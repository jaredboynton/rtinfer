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
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{rejection::JsonRejection, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use rtinfer_core::{CodexResponsesPool, WarmSessionPool};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::info;

/// Wire-contract identifier shared with every rtinfer client.
pub const RTINFER_CONTRACT: &str = "rtinfer/1";

const OPENAI_REALTIME_MODELS: &[&str] = &[
    "gpt-realtime",
    "gpt-realtime-1.5",
    "gpt-realtime-2",
    "gpt-realtime-mini",
];
const REALTIME_REASONING_EFFORTS: &[&str] = &["none", "minimal", "low", "medium", "high"];
const REASONING_MODEL: &str = "gpt-realtime-2";

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
        .route("/v1/models", get(openai_models))
        .route("/v1/chat/completions", post(openai_chat_completions))
        .route("/v1/responses", post(openai_responses))
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

#[derive(Debug, Deserialize)]
struct OpenAiChatRequest {
    model: String,
    messages: Vec<OpenAiMessage>,
    #[serde(default)]
    stream: bool,
    #[serde(default)]
    reasoning: Option<OpenAiReasoning>,
    #[serde(default)]
    reasoning_effort: Option<String>,
    #[serde(default, rename = "reasoningEffort")]
    reasoning_effort_camel: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OpenAiReasoning {
    #[serde(default)]
    effort: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OpenAiMessage {
    role: String,
    #[serde(default)]
    content: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct OpenAiResponsesRequest {
    model: String,
    input: Value,
    #[serde(default)]
    instructions: Option<String>,
    #[serde(default)]
    stream: bool,
    #[serde(default)]
    reasoning: Option<OpenAiReasoning>,
    #[serde(default)]
    reasoning_effort: Option<String>,
    #[serde(default, rename = "reasoningEffort")]
    reasoning_effort_camel: Option<String>,
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

fn openai_err(status: StatusCode, code: &str, message: impl Into<String>) -> Response {
    (
        status,
        Json(json!({
            "error": {
                "message": message.into(),
                "type": code,
                "code": code,
            }
        })),
    )
        .into_response()
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn openai_message_id() -> String {
    format!("chatcmpl-{}", now_secs())
}

fn normalize_reasoning_effort(effort: Option<&str>) -> Result<Option<String>, String> {
    let Some(effort) = effort.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    let normalized = effort.to_ascii_lowercase();
    if REALTIME_REASONING_EFFORTS.contains(&normalized.as_str()) {
        Ok(Some(normalized))
    } else {
        Err(format!(
            "unsupported reasoning effort {effort}; expected one of {}",
            REALTIME_REASONING_EFFORTS.join(", ")
        ))
    }
}

fn resolve_reasoning_effort(
    model: &str,
    reasoning: Option<&OpenAiReasoning>,
    reasoning_effort: Option<&str>,
    reasoning_effort_camel: Option<&str>,
) -> Result<Option<String>, String> {
    let requested = reasoning
        .and_then(|r| r.effort.as_deref())
        .or(reasoning_effort)
        .or(reasoning_effort_camel);
    if model != REASONING_MODEL && requested.is_some() {
        return Err(format!(
            "reasoning is only supported for {REASONING_MODEL}, not {model}"
        ));
    }
    normalize_reasoning_effort(requested)
}

fn request_reasoning_effort(req: &OpenAiChatRequest) -> Result<Option<String>, String> {
    resolve_reasoning_effort(
        &req.model,
        req.reasoning.as_ref(),
        req.reasoning_effort.as_deref(),
        req.reasoning_effort_camel.as_deref(),
    )
}

fn responses_reasoning_effort(req: &OpenAiResponsesRequest) -> Result<Option<String>, String> {
    resolve_reasoning_effort(
        &req.model,
        req.reasoning.as_ref(),
        req.reasoning_effort.as_deref(),
        req.reasoning_effort_camel.as_deref(),
    )
}

fn openai_content_text(content: &Option<Value>) -> Result<String, String> {
    match content {
        None | Some(Value::Null) => Ok(String::new()),
        Some(Value::String(s)) => Ok(s.clone()),
        Some(Value::Array(parts)) => {
            let mut out = String::new();
            for part in parts {
                match part {
                    Value::String(s) => out.push_str(s),
                    Value::Object(obj) => {
                        let kind = obj.get("type").and_then(Value::as_str).unwrap_or("");
                        match kind {
                            "text" | "input_text" => {
                                if let Some(text) = obj.get("text").and_then(Value::as_str) {
                                    out.push_str(text);
                                }
                            }
                            other => {
                                return Err(format!(
                                    "unsupported content part type {other}; only text is supported"
                                ));
                            }
                        }
                    }
                    _ => return Err("unsupported content part; only text is supported".into()),
                }
            }
            Ok(out)
        }
        _ => Err("unsupported message content; only string or text parts are supported".into()),
    }
}

fn openai_messages_to_prompt(messages: &[OpenAiMessage]) -> Result<(String, String), String> {
    let mut system_parts = Vec::new();
    let mut transcript_parts = Vec::new();
    for msg in messages {
        let role = msg.role.trim().to_ascii_lowercase();
        let text = openai_content_text(&msg.content)?;
        match role.as_str() {
            "system" | "developer" => {
                if !text.is_empty() {
                    system_parts.push(text);
                }
            }
            "user" | "assistant" | "tool" => {
                if !text.is_empty() {
                    transcript_parts.push(format!("{role}: {text}"));
                }
            }
            other => return Err(format!("unsupported message role {other}")),
        }
    }
    if transcript_parts.is_empty() {
        return Err("messages must contain at least one non-empty user/assistant turn".into());
    }
    Ok((system_parts.join("\n\n"), transcript_parts.join("\n\n")))
}

async fn openai_models() -> Response {
    Json(json!({
        "object": "list",
        "data": OPENAI_REALTIME_MODELS.iter().map(|model| json!({
            "id": model,
            "object": "model",
            "created": 0,
            "owned_by": "rtinferd",
        })).collect::<Vec<_>>(),
    }))
    .into_response()
}

fn openai_chat_response(model: &str, text: &str) -> Response {
    Json(json!({
        "id": openai_message_id(),
        "object": "chat.completion",
        "created": now_secs(),
        "model": model,
        "choices": [{
            "index": 0,
            "message": { "role": "assistant", "content": text },
            "finish_reason": "stop",
        }],
        "usage": { "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0 },
    }))
    .into_response()
}

fn openai_chat_stream_response(model: &str, text: &str) -> Response {
    let id = openai_message_id();
    let created = now_secs();
    let start = json!({
        "id": id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [{ "index": 0, "delta": { "role": "assistant" }, "finish_reason": null }],
    });
    let delta = json!({
        "id": id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [{ "index": 0, "delta": { "content": text }, "finish_reason": null }],
    });
    let stop = json!({
        "id": id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [{ "index": 0, "delta": {}, "finish_reason": "stop" }],
    });
    let body = format!("data: {start}\n\ndata: {delta}\n\ndata: {stop}\n\ndata: [DONE]\n\n");
    (
        [
            (header::CONTENT_TYPE, "text/event-stream"),
            (header::CACHE_CONTROL, "no-cache"),
            (header::CONNECTION, "keep-alive"),
        ],
        body,
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

async fn openai_chat_completions(
    State(state): State<Arc<AppState>>,
    body: Result<Json<OpenAiChatRequest>, JsonRejection>,
) -> Response {
    let Json(req) = match body {
        Ok(body) => body,
        Err(e) => {
            return openai_err(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                e.to_string(),
            )
        }
    };
    if !OPENAI_REALTIME_MODELS.contains(&req.model.as_str()) {
        return openai_err(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            format!(
                "unsupported model {}; expected one of {}",
                req.model,
                OPENAI_REALTIME_MODELS.join(", ")
            ),
        );
    }
    if req.messages.is_empty() {
        return openai_err(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "messages must not be empty",
        );
    }
    let (system, user) = match openai_messages_to_prompt(&req.messages) {
        Ok(prompt) => prompt,
        Err(e) => return openai_err(StatusCode::BAD_REQUEST, "invalid_request_error", e),
    };
    let reasoning_effort = match request_reasoning_effort(&req) {
        Ok(effort) => effort,
        Err(e) => return openai_err(StatusCode::BAD_REQUEST, "invalid_request_error", e),
    };
    match state
        .warm_realtime
        .ask_text(
            &system,
            &user,
            Some(&req.model),
            reasoning_effort.as_deref(),
        )
        .await
    {
        Ok(text) if req.stream => openai_chat_stream_response(&req.model, &text),
        Ok(text) => openai_chat_response(&req.model, &text),
        Err(e) => {
            tracing::warn!(model = %req.model, error = %e, "rtinfer: openai chat upstream error");
            openai_err(
                StatusCode::BAD_GATEWAY,
                "provider_error",
                format!("realtime upstream error: {}", e.code_or_label()),
            )
        }
    }
}

/// Flatten the Responses API `input` field (string or array of message items)
/// into a single user-transcript string. `instructions` is handled separately
/// as the system prompt.
fn responses_input_to_text(input: &Value) -> Result<String, String> {
    match input {
        Value::String(s) => Ok(s.clone()),
        Value::Array(items) => {
            let mut out = String::new();
            for item in items {
                match item {
                    Value::String(s) => out.push_str(s),
                    Value::Object(obj) => {
                        let kind = obj.get("type").and_then(Value::as_str).unwrap_or("message");
                        if kind != "message" {
                            return Err(format!(
                                "unsupported input item type {kind}; only message/text is supported"
                            ));
                        }
                        let role = obj.get("role").and_then(Value::as_str).unwrap_or("user");
                        let content = obj.get("content");
                        let text = openai_content_text(&content.cloned())?;
                        if !text.is_empty() {
                            if !out.is_empty() {
                                out.push_str("\n\n");
                            }
                            out.push_str(&format!("{role}: {text}"));
                        }
                    }
                    _ => {
                        return Err("unsupported input item; only message/text is supported".into())
                    }
                }
            }
            if out.is_empty() {
                return Err("responses input must contain at least one non-empty message".into());
            }
            Ok(out)
        }
        _ => Err("unsupported input; only string or array of messages is supported".into()),
    }
}

fn openai_responses_non_stream(model: &str, text: &str) -> Response {
    let id = format!("resp_{}", now_secs());
    let msg_id = format!("msg_{}", now_secs());
    let message = json!({
        "id": msg_id,
        "type": "message",
        "role": "assistant",
        "status": "completed",
        "content": [{ "type": "output_text", "text": text }],
    });
    Json(json!({
        "id": id,
        "object": "response",
        "status": "completed",
        "created_at": now_secs(),
        "model": model,
        "output": [message],
        "output_text": text,
        "usage": { "input_tokens": 0, "output_tokens": 0, "total_tokens": 0 },
    }))
    .into_response()
}

fn openai_responses_stream(model: &str, text: &str) -> Response {
    let id = format!("resp_{}", now_secs());
    let msg_id = format!("msg_{}", now_secs());
    let created_at = now_secs();
    let message_in_progress = json!({
        "id": msg_id, "type": "message", "role": "assistant", "status": "in_progress", "content": []
    });
    let message_done = json!({
        "id": msg_id, "type": "message", "role": "assistant", "status": "completed",
        "content": [{ "type": "output_text", "text": text }]
    });
    let response_done = json!({
        "id": id, "object": "response", "status": "completed", "created_at": created_at,
        "model": model, "output": [message_done.clone()],
        "usage": { "input_tokens": 0, "output_tokens": 0, "total_tokens": 0 }
    });
    let events = [
        (
            "response.created",
            json!({ "type": "response.created", "response": { "id": id, "object": "response", "status": "in_progress", "created_at": created_at, "model": model, "output": [] } }),
        ),
        (
            "response.output_item.added",
            json!({ "type": "response.output_item.added", "output_index": 0, "item": message_in_progress }),
        ),
        (
            "response.content_part.added",
            json!({ "type": "response.content_part.added", "output_index": 0, "content_index": 0, "part": { "type": "output_text", "text": "" } }),
        ),
        (
            "response.output_text.delta",
            json!({ "type": "response.output_text.delta", "output_index": 0, "content_index": 0, "delta": text }),
        ),
        (
            "response.output_text.done",
            json!({ "type": "response.output_text.done", "output_index": 0, "content_index": 0, "text": text }),
        ),
        (
            "response.content_part.done",
            json!({ "type": "response.content_part.done", "output_index": 0, "content_index": 0, "part": { "type": "output_text", "text": text } }),
        ),
        (
            "response.output_item.done",
            json!({ "type": "response.output_item.done", "output_index": 0, "item": message_done }),
        ),
        (
            "response.completed",
            json!({ "type": "response.completed", "response": response_done }),
        ),
    ];
    let mut body = String::new();
    for (event, data) in &events {
        body.push_str(&format!("event: {event}\ndata: {data}\n\n"));
    }
    (
        [
            (header::CONTENT_TYPE, "text/event-stream"),
            (header::CACHE_CONTROL, "no-cache"),
            (header::CONNECTION, "keep-alive"),
        ],
        body,
    )
        .into_response()
}

async fn openai_responses(
    State(state): State<Arc<AppState>>,
    body: Result<Json<OpenAiResponsesRequest>, JsonRejection>,
) -> Response {
    let Json(req) = match body {
        Ok(body) => body,
        Err(e) => {
            return openai_err(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                e.to_string(),
            )
        }
    };
    if !OPENAI_REALTIME_MODELS.contains(&req.model.as_str()) {
        return openai_err(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            format!(
                "unsupported model {}; expected one of {}",
                req.model,
                OPENAI_REALTIME_MODELS.join(", ")
            ),
        );
    }
    let user = match responses_input_to_text(&req.input) {
        Ok(text) => text,
        Err(e) => return openai_err(StatusCode::BAD_REQUEST, "invalid_request_error", e),
    };
    let system = req.instructions.clone().unwrap_or_default();
    let reasoning_effort = match responses_reasoning_effort(&req) {
        Ok(effort) => effort,
        Err(e) => return openai_err(StatusCode::BAD_REQUEST, "invalid_request_error", e),
    };
    match state
        .warm_realtime
        .ask_text(
            &system,
            &user,
            Some(&req.model),
            reasoning_effort.as_deref(),
        )
        .await
    {
        Ok(text) if req.stream => openai_responses_stream(&req.model, &text),
        Ok(text) => openai_responses_non_stream(&req.model, &text),
        Err(e) => {
            tracing::warn!(model = %req.model, error = %e, "rtinfer: openai responses upstream error");
            openai_err(
                StatusCode::BAD_GATEWAY,
                "provider_error",
                format!("realtime upstream error: {}", e.code_or_label()),
            )
        }
    }
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
    use axum::http::StatusCode;

    #[test]
    fn router_registers_both_routes() {
        // The router is built without panicking and the source pins the routes.
        let src = include_str!("server.rs");
        assert!(src.contains(r#".route("/v1/infer", post(rtinfer))"#));
        assert!(src.contains(r#".route("/v1/infer/health", get(rtinfer_health))"#));
        assert!(src.contains(r#".route("/v1/models", get(openai_models))"#));
        assert!(src.contains(r#".route("/v1/chat/completions", post(openai_chat_completions))"#));
        assert!(src.contains(r#".route("/v1/responses", post(openai_responses))"#));
    }

    #[test]
    fn error_envelope_carries_contract_and_code() {
        let resp = rtinfer_err(StatusCode::BAD_GATEWAY, "provider_error", "boom", true);
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    }

    #[test]
    fn openai_error_envelope_uses_requested_status() {
        let resp = openai_err(StatusCode::BAD_REQUEST, "invalid_request_error", "boom");
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
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

    #[test]
    fn openai_request_parses_reasoning_from_all_supported_shapes() {
        let nested: OpenAiChatRequest = serde_json::from_value(json!({
            "model": "gpt-realtime-2",
            "messages": [{"role": "user", "content": "hi"}],
            "reasoning": {"effort": "medium"}
        }))
        .unwrap();
        assert_eq!(
            request_reasoning_effort(&nested).unwrap().as_deref(),
            Some("medium")
        );

        let snake: OpenAiChatRequest = serde_json::from_value(json!({
            "model": "gpt-realtime-2",
            "messages": [{"role": "user", "content": "hi"}],
            "reasoning_effort": "low"
        }))
        .unwrap();
        assert_eq!(
            request_reasoning_effort(&snake).unwrap().as_deref(),
            Some("low")
        );

        let camel: OpenAiChatRequest = serde_json::from_value(json!({
            "model": "gpt-realtime-2",
            "messages": [{"role": "user", "content": "hi"}],
            "reasoningEffort": "high"
        }))
        .unwrap();
        assert_eq!(
            request_reasoning_effort(&camel).unwrap().as_deref(),
            Some("high")
        );
    }

    #[test]
    fn invalid_reasoning_effort_is_rejected() {
        assert!(normalize_reasoning_effort(Some("xhigh")).is_err());
    }

    #[test]
    fn reasoning_is_rejected_for_non_reasoning_models() {
        let req: OpenAiChatRequest = serde_json::from_value(json!({
            "model": "gpt-realtime-mini",
            "messages": [{"role": "user", "content": "hi"}],
            "reasoning": {"effort": "low"}
        }))
        .unwrap();
        let err = request_reasoning_effort(&req).unwrap_err();
        assert!(err.contains("only supported for gpt-realtime-2"));
    }

    #[test]
    fn responses_input_string_flattens_to_user_text() {
        let text = responses_input_to_text(&json!("Say pong.")).unwrap();
        assert_eq!(text, "Say pong.");
    }

    #[test]
    fn responses_input_array_flattens_messages() {
        let input = json!([
            {"type":"message","role":"user","content":[{"type":"input_text","text":"Hello"}]},
            {"type":"message","role":"assistant","content":"Hi there"}
        ]);
        let text = responses_input_to_text(&input).unwrap();
        assert!(text.contains("user: Hello"));
        assert!(text.contains("assistant: Hi there"));
    }

    #[test]
    fn responses_input_rejects_non_message_items() {
        let err = responses_input_to_text(&json!([
            {"type":"function_call","name":"x","arguments":"{}"}
        ]))
        .unwrap_err();
        assert!(err.contains("only message/text is supported"));
    }

    #[test]
    fn openai_messages_flatten_system_and_transcript() {
        let (system, transcript) = openai_messages_to_prompt(&[
            OpenAiMessage {
                role: "system".into(),
                content: Some(Value::String("You are strict".into())),
            },
            OpenAiMessage {
                role: "developer".into(),
                content: Some(Value::Array(vec![
                    json!({"type":"text","text":"Follow policy"}),
                ])),
            },
            OpenAiMessage {
                role: "user".into(),
                content: Some(Value::String("Question".into())),
            },
            OpenAiMessage {
                role: "assistant".into(),
                content: Some(Value::String("Prior answer".into())),
            },
        ])
        .unwrap();
        assert_eq!(system, "You are strict\n\nFollow policy");
        assert_eq!(transcript, "user: Question\n\nassistant: Prior answer");
    }

    #[test]
    fn openai_messages_reject_non_text_parts() {
        let err = openai_messages_to_prompt(&[OpenAiMessage {
            role: "user".into(),
            content: Some(Value::Array(vec![
                json!({"type":"image_url","image_url":{"url":"x"}}),
            ])),
        }])
        .unwrap_err();
        assert!(err.contains("only text is supported"));
    }

    #[test]
    fn openai_messages_require_non_empty_conversation() {
        let err = openai_messages_to_prompt(&[OpenAiMessage {
            role: "system".into(),
            content: Some(Value::String("Rules".into())),
        }])
        .unwrap_err();
        assert!(err.contains("at least one non-empty user/assistant turn"));
    }
}
