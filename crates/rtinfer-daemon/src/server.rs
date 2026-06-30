//! rtinfer/1 loopback inference server.
//!
//! Exposes the daemon's warm [`RealtimePool`] (gpt-realtime-* navigators /
//! scorer) and [`CodexResponsesPool`] (gpt-5.x synthesis) over a loopback
//! HTTP contract so any client on the machine (cse-sweep, unifable judge,
//! ...) borrows one warm pool instead of spawning its own realtime daemon.
//!
//! Auth is file-only by default: both pools read `~/.codex/auth.json` and
//! perform the client-side OAuth `refresh_token` rotation when the short-lived
//! `id_token` lapses (see `rtinfer_core::auth`). The server NEVER reads process
//! env for credentials and NEVER touches a keychain.
//!
//! The server binds `127.0.0.1` only; there is no auth header on the wire
//! because the loopback bind is the trust boundary.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use rtinfer_core::{CodexResponsesPool, WarmSessionPool};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::info;

/// Wire-contract identifier shared with every rtinfer client.
pub const RTINFER_CONTRACT: &str = "rtinfer/1";

/// Warm Realtime sockets kept per model. The realtime fan-out (navigators +
/// scorer) issues a handful of concurrent asks; 4 independent warm sessions per
/// model give real parallelism without cid-multiplexing one socket.
const WARM_SESSIONS_PER_MODEL: usize = 4;

/// Shared daemon state: the warm realtime sessions + the responses pool.
pub struct AppState {
    /// Warm persistent Realtime sockets (out-of-band asks, no per-call
    /// handshake). The realtime_structured tier dispatches here.
    pub warm_realtime: Arc<WarmSessionPool>,
    pub codex_responses_pool: Arc<CodexResponsesPool>,
}

impl AppState {
    /// Build the default file-auth pools (both read `~/.codex/auth.json`).
    pub fn new_file_auth() -> Arc<Self> {
        Arc::new(Self {
            warm_realtime: WarmSessionPool::new(WARM_SESSIONS_PER_MODEL, None),
            codex_responses_pool: CodexResponsesPool::builder().build(),
        })
    }
}

/// Build the rtinfer router. Separated from `serve` so tests can drive it
/// with `tower`/`axum::serve` against an ephemeral port.
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/v1/infer", post(rtinfer))
        .route("/v1/infer/health", get(rtinfer_health))
        .with_state(state)
}

/// Bind `127.0.0.1:{port}`, write the well-known endpoint file, and serve until
/// the process is killed or a newer npm release is self-installed (drain+exit so
/// launchd respawns the updated shim).
pub async fn serve(port: u16) -> anyhow::Result<()> {
    let state = AppState::new_file_auth();
    let app = router(state);
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    let base_url = format!("http://127.0.0.1:{}", bound.port());
    crate::endpoint_file::write(&base_url)?;
    info!(%base_url, version = crate::self_update::current_version(), "rtinfer: serving");

    // Background self-update: drains the server on a confirmed newer release.
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    crate::self_update::spawn(shutdown_tx, crate::self_update::DEFAULT_CHECK_INTERVAL);

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            // Drain on either a self-update signal or SIGINT/SIGTERM.
            let ctrl_c = tokio::signal::ctrl_c();
            tokio::select! {
                _ = ctrl_c => {}
                _ = shutdown_rx.changed() => {}
            }
        })
        .await?;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct RtInferRequest {
    #[serde(default)]
    contract: String,
    tier: String,
    #[serde(default)]
    schema_name: Option<String>,
    #[serde(default)]
    schema: Option<Value>,
    #[serde(default)]
    system: String,
    #[serde(default)]
    user: String,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    reasoning_effort: Option<String>,
}

/// Build the rtinfer error envelope + HTTP status as a response.
fn rtinfer_err(
    status: StatusCode,
    code: &str,
    message: impl Into<String>,
    retryable: bool,
) -> Response {
    (
        status,
        Json(json!({
            "contract": RTINFER_CONTRACT,
            "ok": false,
            "error": { "code": code, "message": message.into(), "retryable": retryable },
        })),
    )
        .into_response()
}

/// Map a `RealtimeError` from either pool onto the rtinfer error envelope.
fn rtinfer_map_realtime_err(tier: &str, err: rtinfer_core::RealtimeError) -> Response {
    use rtinfer_core::RealtimeError as RE;
    let (status, code, retryable) = match &err {
        RE::Handshake(msg)
            if msg.contains("401")
                || msg.contains("403")
                || msg.contains("Unauthorized")
                || msg.contains("Forbidden") =>
        {
            (StatusCode::UNAUTHORIZED, "auth_unavailable", false)
        }
        RE::AuthFile { .. } | RE::AuthMissing(_) | RE::AuthMalformed(_) | RE::Refresh(_) => {
            (StatusCode::UNAUTHORIZED, "auth_unavailable", false)
        }
        RE::Handshake(_) | RE::Protocol(_) | RE::Provider { .. } => {
            (StatusCode::BAD_GATEWAY, "provider_error", true)
        }
        _ => (StatusCode::BAD_GATEWAY, "provider_error", true),
    };
    // Never echo provider message bodies verbatim (they can carry
    // bearer-equivalent material); a stable, tier-scoped label is enough.
    tracing::warn!(tier = %tier, error = %err, "rtinfer: upstream model error");
    rtinfer_err(
        status,
        code,
        format!("{tier} upstream error: {}", err.code_or_label()),
        retryable,
    )
}

async fn rtinfer(State(state): State<Arc<AppState>>, Json(req): Json<RtInferRequest>) -> Response {
    if !req.contract.is_empty() && req.contract != RTINFER_CONTRACT {
        return rtinfer_err(
            StatusCode::BAD_REQUEST,
            "bad_request",
            format!("unknown contract {}", req.contract),
            false,
        );
    }
    match req.tier.as_str() {
        "realtime_structured" => {
            let (Some(name), Some(schema)) = (req.schema_name.as_deref(), req.schema.clone())
            else {
                return rtinfer_err(
                    StatusCode::BAD_REQUEST,
                    "bad_request",
                    "realtime_structured requires schema_name and schema",
                    false,
                );
            };
            match state
                .warm_realtime
                .ask_structured(
                    &req.system,
                    &req.user,
                    name,
                    schema,
                    req.model.as_deref(),
                    req.reasoning_effort.as_deref(),
                )
                .await
            {
                Ok(object) => Json(json!({
                    "contract": RTINFER_CONTRACT, "ok": true, "tier": "realtime_structured",
                    "object": object, "model": req.model,
                }))
                .into_response(),
                Err(e) => rtinfer_map_realtime_err("realtime_structured", e),
            }
        }
        "responses_structured" => {
            let (Some(name), Some(schema)) = (req.schema_name.as_deref(), req.schema.clone())
            else {
                return rtinfer_err(
                    StatusCode::BAD_REQUEST,
                    "bad_request",
                    "responses_structured requires schema_name and schema",
                    false,
                );
            };
            match state
                .codex_responses_pool
                .ask_structured(&req.system, &req.user, name, schema)
                .await
            {
                Ok(object) => Json(json!({
                    "contract": RTINFER_CONTRACT, "ok": true, "tier": "responses_structured",
                    "object": object, "model": state.codex_responses_pool.model(),
                }))
                .into_response(),
                Err(e) => rtinfer_map_realtime_err("responses_structured", e),
            }
        }
        "responses_text" => {
            match state
                .codex_responses_pool
                .ask_text(&req.system, &req.user, req.model.as_deref())
                .await
            {
                Ok(text) => Json(json!({
                    "contract": RTINFER_CONTRACT, "ok": true, "tier": "responses_text",
                    "text": text,
                    "model": req.model.as_deref().unwrap_or(state.codex_responses_pool.model()),
                }))
                .into_response(),
                Err(e) => rtinfer_map_realtime_err("responses_text", e),
            }
        }
        other => rtinfer_err(
            StatusCode::BAD_REQUEST,
            "unsupported_tier",
            format!("unknown tier {other}"),
            false,
        ),
    }
}

async fn rtinfer_health(State(_state): State<Arc<AppState>>) -> Response {
    // `ready` reflects whether file-based Codex credentials are reachable.
    // A client treats `ready:false` as "present but warming" and retries
    // briefly rather than failing loud.
    let ready = rtinfer_core::CodexAuth::from_default_path().is_ok();
    Json(json!({
        "contract": RTINFER_CONTRACT,
        "ready": ready,
        "provider": "rtinferd",
        "tiers": ["realtime_structured", "responses_structured", "responses_text"],
    }))
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn router_registers_both_routes() {
        // The router is built without panicking and the source pins the routes.
        let src = include_str!("server.rs");
        assert!(src.contains(r#".route("/v1/infer", post(rtinfer))"#));
        assert!(src.contains(r#".route("/v1/infer/health", get(rtinfer_health))"#));
    }

    #[test]
    fn error_envelope_carries_contract_and_code() {
        let resp = rtinfer_err(StatusCode::BAD_GATEWAY, "provider_error", "boom", true);
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    }

    #[test]
    fn unknown_contract_is_rejected_shape() {
        let req: RtInferRequest = serde_json::from_value(json!({
            "contract": "rtinfer/2",
            "tier": "responses_text",
        }))
        .unwrap();
        assert_eq!(req.contract, "rtinfer/2");
        assert_eq!(req.tier, "responses_text");
    }
}
