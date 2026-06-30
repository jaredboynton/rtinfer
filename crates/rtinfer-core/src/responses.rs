//! codex/responses transport — free gpt-5.5 over Codex OAuth.
//!
//! Distinct from [`crate::RealtimePool`]: the realtime API
//! (`gpt-realtime-2`) and the codex/responses API speak different wire
//! grammars (realtime: `session.update` + `conversation.item.create` +
//! `response.create{output_modalities}`; responses: a single
//! `response.create` with `model` + `instructions` + `input` +
//! `text.format.json_schema`). Conflating them on one pool type would
//! mix two frame vocabularies, so this is its own pool.
//!
//! # Endpoint + cost
//!
//! `wss://chatgpt.com/backend-api/codex/responses` authenticated with
//! the Codex OAuth token (`~/.codex/auth.json` or daemon keychain).
//! On a ChatGPT Business account this is **free** (`codex.rate_limits`
//! frame reports `plan_type:"business"`, `credits.unlimited:true`). The
//! paid `api.openai.com/v1/responses` API-key path is deliberately NOT
//! used.
//!
//! # Stream grammar (captured live 2026-06-07)
//!
//! `codex.rate_limits` -> `response.created` -> `response.in_progress`
//! -> `response.output_item.added` -> `response.content_part.added` ->
//! `response.output_text.delta`* -> `response.output_text.done` (full
//! `.text`) -> `response.content_part.done` -> `response.output_item.done`
//! -> `response.completed` (terminal; `response.output` is `[]` in
//! streaming text mode). The structured payload is the assembled text,
//! NOT an output item. See `tests/responses_protocol.rs`.

use std::path::PathBuf;
use std::sync::{Arc, Once};
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use http::Request;
use serde_json::{json, Value};
use tokio::sync::{RwLock, Semaphore};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::generate_key;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, tungstenite::http::Uri};
use tracing::{debug, trace};

use crate::auth::{CodexAuth, SharedCodexAuthSource, ID_TOKEN_REFRESH_MARGIN_SECS};
use crate::{RealtimeError, DEFAULT_HANDSHAKE_TIMEOUT};

/// codex/responses WebSocket endpoint (free over Codex OAuth).
pub const CODEX_RESPONSES_URL: &str = "wss://chatgpt.com/backend-api/codex/responses";

/// Model id. ChatGPT Business unlimited tokens.
pub const CODEX_RESPONSES_MODEL: &str = "gpt-5.5";

/// `originator` header value (matches the codex CLI; verified live).
pub const CODEX_RESPONSES_ORIGINATOR: &str = "codex_cli_rs";

/// `User-Agent` header value (verified accepted live 2026-06-07).
pub const CODEX_RESPONSES_USER_AGENT: &str = "codex_cli_rs/rtinfer (rtinferd; rust)";

/// `OpenAI-Beta` header required to negotiate the WS responses protocol.
pub const CODEX_RESPONSES_BETA: &str = "responses_websockets=2026-02-06";

/// Reuse the realtime pool's TTL semantics.
pub use crate::DEFAULT_AUTH_TTL;

const UNBOUNDED_CAPACITY: usize = 1 << 31;
const DEFAULT_CAPACITY: usize = 16;

static RUSTLS_PROVIDER: Once = Once::new();

// ---------------------------------------------------------------------------
// Pure stream assembler — shared by the live loop and the replay test.
// ---------------------------------------------------------------------------

enum FrameOutcome {
    Continue,
    Done,
    Error(RealtimeError),
}

#[derive(Default)]
struct ResponsesAssembler {
    delta_buf: String,
    done_text: Option<String>,
}

impl ResponsesAssembler {
    fn on_frame(&mut self, v: &Value) -> FrameOutcome {
        match v.get("type").and_then(Value::as_str).unwrap_or("") {
            "response.output_text.delta" | "response.text.delta" => {
                if let Some(d) = v.get("delta").and_then(Value::as_str) {
                    self.delta_buf.push_str(d);
                }
                FrameOutcome::Continue
            }
            "response.output_text.done" | "response.text.done" => {
                if let Some(t) = v.get("text").and_then(Value::as_str) {
                    self.done_text = Some(t.to_string());
                }
                FrameOutcome::Continue
            }
            "error" => FrameOutcome::Error(provider_error_from_value(v.get("error"))),
            "response.failed" => FrameOutcome::Error(provider_error_from_value(
                v.get("response").and_then(|r| r.get("error")),
            )),
            "response.completed" | "response.done" => FrameOutcome::Done,
            _ => FrameOutcome::Continue,
        }
    }

    /// Authoritative text: prefer the `.done` frame's full text, fall
    /// back to the accumulated deltas.
    fn finish(self) -> String {
        self.done_text.unwrap_or(self.delta_buf)
    }
}

fn provider_error_from_value(err: Option<&Value>) -> RealtimeError {
    let code = err
        .and_then(|e| e.get("code").or_else(|| e.get("type")))
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let message = err
        .and_then(|e| e.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("(no detail)")
        .to_string();
    RealtimeError::Provider { code, message }
}

/// Assemble a codex/responses frame stream into the model's output text.
///
/// Pure and network-free. The live [`CodexResponsesPool`] read loop runs
/// the identical per-frame logic, so the replay test in
/// `tests/responses_protocol.rs` exercises the real parser.
pub fn assemble_codex_responses_text(frames: &[Value]) -> Result<String, RealtimeError> {
    let mut asm = ResponsesAssembler::default();
    for f in frames {
        match asm.on_frame(f) {
            FrameOutcome::Continue => {}
            FrameOutcome::Done => break,
            FrameOutcome::Error(e) => return Err(e),
        }
    }
    let text = asm.finish();
    if text.trim().is_empty() {
        return Err(RealtimeError::Protocol(
            "codex/responses stream produced no text".to_string(),
        ));
    }
    Ok(text)
}

// ---------------------------------------------------------------------------
// Strict-schema normaliser (ported from kepler
// `kepler-jobs/src/codex_kg_worker.rs::require_all_object_properties_for_strict_schema`).
// OpenAI strict mode requires every object to list ALL its properties in
// `required` and set `additionalProperties:false`.
// ---------------------------------------------------------------------------

/// Recursively enforce strict-mode invariants on a JSON Schema in place.
pub fn require_all_object_properties_for_strict_schema(schema: &mut Value) {
    match schema {
        Value::Object(map) => {
            let is_object = map.get("type").and_then(Value::as_str) == Some("object")
                || map.contains_key("properties");
            if is_object {
                if let Some(Value::Object(props)) = map.get("properties") {
                    let keys: Vec<Value> = props.keys().cloned().map(Value::String).collect();
                    map.insert("required".to_string(), Value::Array(keys));
                    map.insert("additionalProperties".to_string(), Value::Bool(false));
                }
            }
            for (_k, v) in map.iter_mut() {
                require_all_object_properties_for_strict_schema(v);
            }
        }
        Value::Array(items) => {
            for v in items.iter_mut() {
                require_all_object_properties_for_strict_schema(v);
            }
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Pool
// ---------------------------------------------------------------------------

/// Builder for [`CodexResponsesPool`].
#[derive(Default)]
pub struct CodexResponsesPoolBuilder {
    endpoint: Option<String>,
    model: Option<String>,
    capacity: Option<usize>,
    auth_ttl: Option<Duration>,
    initial_auth: Option<CodexAuth>,
    auth_path: Option<PathBuf>,
    auth_source: Option<SharedCodexAuthSource>,
}

impl CodexResponsesPoolBuilder {
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }
    pub fn model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }
    pub fn capacity(mut self, cap: usize) -> Self {
        self.capacity = Some(cap);
        self
    }
    pub fn auth_ttl(mut self, ttl: Duration) -> Self {
        self.auth_ttl = Some(ttl);
        self
    }
    /// Seed an explicit [`CodexAuth`] (the daemon passes keychain-derived
    /// auth here so the pool never reads `~/.codex/auth.json`).
    pub fn initial_auth(mut self, auth: CodexAuth) -> Self {
        self.initial_auth = Some(auth);
        self
    }
    pub fn auth_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.auth_path = Some(path.into());
        self
    }

    /// Supply a pluggable [`CodexAuthSource`](crate::CodexAuthSource). When
    /// set, the pool calls it for every cold/stale load and after a handshake
    /// rejection, ignoring `auth_path`. The daemon passes a keychain-backed
    /// source so the pool re-mints through okta-aio instead of reading
    /// `~/.codex/auth.json`.
    pub fn auth_source(mut self, source: SharedCodexAuthSource) -> Self {
        self.auth_source = Some(source);
        self
    }

    pub fn build(self) -> Arc<CodexResponsesPool> {
        let endpoint = self
            .endpoint
            .unwrap_or_else(|| CODEX_RESPONSES_URL.to_owned());
        let model = self
            .model
            .unwrap_or_else(|| CODEX_RESPONSES_MODEL.to_owned());
        let capacity = match self.capacity {
            Some(0) => UNBOUNDED_CAPACITY,
            Some(n) => n,
            None => DEFAULT_CAPACITY,
        };
        let auth_ttl = self.auth_ttl.unwrap_or(DEFAULT_AUTH_TTL);
        // Eager pre-load only for the file path. With an explicit
        // `auth_source` the first ask loads lazily through it (a keychain read
        // / okta-aio re-mint must not run synchronously in `build`).
        let cached = match self.initial_auth.clone() {
            Some(auth) => Some(CachedAuth {
                auth,
                fetched_at: Instant::now(),
            }),
            None if self.auth_source.is_some() => None,
            None => match self.auth_path.as_deref() {
                Some(p) => CodexAuth::from_path(p).ok().map(|auth| CachedAuth {
                    auth,
                    fetched_at: Instant::now(),
                }),
                None => CodexAuth::from_default_path().ok().map(|auth| CachedAuth {
                    auth,
                    fetched_at: Instant::now(),
                }),
            },
        };
        Arc::new(CodexResponsesPool {
            endpoint,
            model,
            semaphore: Semaphore::new(capacity),
            capacity,
            auth_ttl,
            auth_path: self.auth_path,
            auth_source: self.auth_source,
            cached_auth: RwLock::new(cached),
        })
    }
}

#[derive(Clone)]
struct CachedAuth {
    auth: CodexAuth,
    fetched_at: Instant,
}

/// Free gpt-5.5 codex/responses pool. One strict-schema ask per call.
pub struct CodexResponsesPool {
    endpoint: String,
    model: String,
    semaphore: Semaphore,
    capacity: usize,
    auth_ttl: Duration,
    auth_path: Option<PathBuf>,
    auth_source: Option<SharedCodexAuthSource>,
    cached_auth: RwLock<Option<CachedAuth>>,
}

impl CodexResponsesPool {
    pub fn builder() -> CodexResponsesPoolBuilder {
        CodexResponsesPoolBuilder::default()
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn model(&self) -> &str {
        &self.model
    }

    pub fn in_flight(&self) -> usize {
        self.capacity
            .saturating_sub(self.semaphore.available_permits())
    }

    /// Force the next ask to re-read auth.
    pub async fn invalidate_auth(&self) {
        *self.cached_auth.write().await = None;
    }

    /// Ask gpt-5.5 for a single strict-schema structured object.
    ///
    /// `schema` is normalised for strict mode (all props required,
    /// `additionalProperties:false`) before sending. Returns the parsed
    /// JSON object the model produced.
    pub async fn ask_structured(
        &self,
        system: &str,
        user: &str,
        schema_name: &str,
        schema: Value,
    ) -> Result<Value, RealtimeError> {
        let _permit = self.semaphore.acquire().await.map_err(|e| {
            RealtimeError::Protocol(format!("responses pool semaphore closed: {e}"))
        })?;
        let auth = self.fresh_auth().await?;
        let mut schema = schema;
        require_all_object_properties_for_strict_schema(&mut schema);
        let frame = build_structured_frame(&self.model, system, user, schema_name, schema);
        debug!(
            in_flight = self.in_flight(),
            model = %self.model,
            "codex/responses: dispatch ask_structured"
        );
        let text = match run_responses_session(&auth, &self.endpoint, &frame).await {
            Ok(t) => t,
            // Reactive safety net: the server rejected the (seemingly fresh)
            // token at the handshake. Force a refresh of the rotated tokens and
            // retry exactly once.
            Err(e) if is_auth_handshake_error(&e) => {
                debug!(error = %e, "codex/responses: auth rejected at handshake; forcing token refresh + one retry");
                let auth2 = self.force_refresh_after(&auth.access_token).await?;
                run_responses_session(&auth2, &self.endpoint, &frame).await?
            }
            Err(e) => return Err(e),
        };
        serde_json::from_str::<Value>(&text).map_err(|e| {
            RealtimeError::Protocol(format!("codex/responses output is not valid json: {e}"))
        })
    }

    /// Ask for freeform text (no JSON schema). Used by the sweep's synthesis
    /// tier, which produces markdown, not a structured object. `model`
    /// overrides the pool's configured model per call (e.g. the sweep asks
    /// for gpt-5.4). Returns the assembled output text.
    pub async fn ask_text(
        &self,
        system: &str,
        user: &str,
        model: Option<&str>,
    ) -> Result<String, RealtimeError> {
        let _permit = self.semaphore.acquire().await.map_err(|e| {
            RealtimeError::Protocol(format!("responses pool semaphore closed: {e}"))
        })?;
        let auth = self.fresh_auth().await?;
        let model = model
            .filter(|m| !m.trim().is_empty())
            .unwrap_or(&self.model);
        let frame = build_text_frame(model, system, user);
        debug!(
            in_flight = self.in_flight(),
            model = %model,
            "codex/responses: dispatch ask_text"
        );
        match run_responses_session(&auth, &self.endpoint, &frame).await {
            Ok(t) => Ok(t),
            Err(e) if is_auth_handshake_error(&e) => {
                debug!(error = %e, "codex/responses: auth rejected at handshake; forcing token refresh + one retry");
                let auth2 = self.force_refresh_after(&auth.access_token).await?;
                run_responses_session(&auth2, &self.endpoint, &frame).await
            }
            Err(e) => Err(e),
        }
    }

    async fn fresh_auth(&self) -> Result<CodexAuth, RealtimeError> {
        // Fast path: cached, inside TTL, and the id_token isn't near expiry.
        if let Some(cached) = self.cached_auth.read().await.as_ref() {
            if cached.fetched_at.elapsed() < self.auth_ttl
                && !cached.auth.needs_refresh(ID_TOKEN_REFRESH_MARGIN_SECS)
            {
                return Ok(cached.auth.clone());
            }
        }
        let mut guard = self.cached_auth.write().await;
        // Re-check under the write lock so concurrent asks refresh at most once.
        if let Some(cached) = guard.as_ref() {
            if cached.fetched_at.elapsed() < self.auth_ttl
                && !cached.auth.needs_refresh(ID_TOKEN_REFRESH_MARGIN_SECS)
            {
                return Ok(cached.auth.clone());
            }
        }
        // A `CodexAuthSource` owns its own staleness handling: the file source
        // does the client-side OAuth rotation; the keychain source re-mints
        // through okta-aio. The path branch keeps the legacy proactive refresh.
        let auth = match self.auth_source.as_ref() {
            Some(source) => source.load().await?,
            None => {
                let mut auth = match self.auth_path.as_deref() {
                    Some(p) => CodexAuth::from_path(p)?,
                    None => CodexAuth::from_default_path()?,
                };
                // Proactive: the codex/responses WS gates on the short-lived
                // id_token. Refresh it (rotating the token set, persisted to
                // auth.json) before the stale token earns a 403 at the handshake.
                if auth.needs_refresh(ID_TOKEN_REFRESH_MARGIN_SECS) && auth.can_refresh() {
                    match auth.refresh().await {
                        Ok(fresh) => {
                            debug!("codex/responses: proactively refreshed expiring id_token");
                            auth = fresh;
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "codex/responses: proactive token refresh failed; trying existing token");
                        }
                    }
                }
                auth
            }
        };
        *guard = Some(CachedAuth {
            auth: auth.clone(),
            fetched_at: Instant::now(),
        });
        Ok(auth)
    }

    /// Force a token refresh after a handshake auth rejection, collapsing a
    /// thundering herd: if another ask already refreshed (cached access_token
    /// differs from the one that failed), return that instead of refreshing
    /// again.
    async fn force_refresh_after(
        &self,
        stale_access_token: &str,
    ) -> Result<CodexAuth, RealtimeError> {
        let mut guard = self.cached_auth.write().await;
        if let Some(cached) = guard.as_ref() {
            if cached.auth.access_token != stale_access_token {
                return Ok(cached.auth.clone());
            }
        }
        let fresh = match self.auth_source.as_ref() {
            Some(source) => source.force_refresh().await?,
            None => {
                let base = match self.auth_path.as_deref() {
                    Some(p) => CodexAuth::from_path(p)?,
                    None => CodexAuth::from_default_path()?,
                };
                base.refresh().await?
            }
        };
        *guard = Some(CachedAuth {
            auth: fresh.clone(),
            fetched_at: Instant::now(),
        });
        Ok(fresh)
    }
}

/// True when an error is a handshake rejection that a token refresh might fix
/// (HTTP 401/403). The codex/responses WS surfaces a stale token this way.
pub(crate) fn is_auth_handshake_error(e: &RealtimeError) -> bool {
    matches!(
        e,
        RealtimeError::Handshake(msg)
            if msg.contains("403")
                || msg.contains("401")
                || msg.contains("Forbidden")
                || msg.contains("Unauthorized")
    )
}

async fn run_responses_session(
    auth: &CodexAuth,
    endpoint: &str,
    request_frame: &Value,
) -> Result<String, RealtimeError> {
    install_rustls_provider();
    let client_request = build_responses_request(endpoint, auth)?;
    let connect_fut = connect_async(client_request);
    let (mut ws, _resp) = match timeout(DEFAULT_HANDSHAKE_TIMEOUT, connect_fut).await {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => return Err(RealtimeError::Handshake(format!("{e}"))),
        Err(_) => return Err(RealtimeError::Handshake("timeout".to_owned())),
    };

    let body = serde_json::to_string(request_frame)?;
    ws.send(Message::text(body))
        .await
        .map_err(|e| RealtimeError::Protocol(format!("ws send: {e}")))?;

    let mut asm = ResponsesAssembler::default();
    let mut err: Option<RealtimeError> = None;
    // Hard ceiling in case the server never sends a terminal frame.
    let deadline = Duration::from_secs(120);
    let started = Instant::now();
    'read: while let Ok(Some(frame)) = timeout(
        deadline
            .saturating_sub(started.elapsed())
            .max(Duration::from_millis(1)),
        ws.next(),
    )
    .await
    {
        let msg = match frame {
            Ok(m) => m,
            Err(e) => {
                err = Some(RealtimeError::Protocol(format!("ws read: {e}")));
                break 'read;
            }
        };
        match msg {
            Message::Text(text) => match serde_json::from_str::<Value>(&text) {
                Ok(v) => match asm.on_frame(&v) {
                    FrameOutcome::Continue => {}
                    FrameOutcome::Done => break 'read,
                    FrameOutcome::Error(e) => {
                        err = Some(e);
                        break 'read;
                    }
                },
                Err(e) => {
                    // Never log the raw frame: codex frames can carry
                    // bearer-equivalent material.
                    trace!(error = %e, len = text.len(), "codex/responses: skipping unparseable frame");
                }
            },
            Message::Ping(p) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Message::Close(_) => break 'read,
            _ => {}
        }
    }

    let _ = ws
        .send(Message::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "done".into(),
        })))
        .await;

    if let Some(e) = err {
        return Err(e);
    }
    let text = asm.finish();
    if text.trim().is_empty() {
        return Err(RealtimeError::Protocol(
            "codex/responses stream produced no text".to_string(),
        ));
    }
    Ok(text)
}

// ALWAYS request the priority lane. codex-rs grants it server-side (catalog
// `service_tiers` are advisory only), so every Codex /responses request
// unconditionally sends "priority". Mirrors ~/__devlocal/llm
// codex_rs_wire.resolve_codex_service_tier -> SERVICE_TIER_FAST_REQUEST_VALUE.
// codex-rs reference: core/src/client.rs::build_responses_request,
// codex-api/src/common.rs::ResponsesApiRequest. NEVER drop or gate this field.
pub(crate) const CODEX_SERVICE_TIER: &str = "priority";

/// Freeform-text `response.create` frame (markdown synthesis tier). Pure so the
/// wire shape — including the mandatory `service_tier: "priority"` — is unit
/// testable without a live socket.
fn build_text_frame(model: &str, system: &str, user: &str) -> Value {
    json!({
        "type": "response.create",
        "model": model,
        "instructions": system,
        "input": [{
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": user}],
        }],
        "reasoning": {"effort": "low"},
        "store": false,
        "stream": true,
        "include": [],
        "service_tier": CODEX_SERVICE_TIER,
        "text": {"verbosity": "low"},
    })
}

/// Strict-`json_schema` `response.create` frame. `schema` must already be
/// normalised for strict mode. Pure for the same wire-shape pinning reason as
/// `build_text_frame`.
fn build_structured_frame(
    model: &str,
    system: &str,
    user: &str,
    schema_name: &str,
    schema: Value,
) -> Value {
    json!({
        "type": "response.create",
        "model": model,
        "instructions": system,
        "input": [{
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": user}],
        }],
        "reasoning": {"effort": "low"},
        "store": false,
        "stream": true,
        "include": [],
        "service_tier": CODEX_SERVICE_TIER,
        "text": {
            "verbosity": "low",
            "format": {
                "type": "json_schema",
                "name": schema_name,
                "strict": true,
                "schema": schema,
            },
        },
    })
}

fn build_responses_request(endpoint: &str, auth: &CodexAuth) -> Result<Request<()>, RealtimeError> {
    let uri: Uri = endpoint
        .parse()
        .map_err(|e| RealtimeError::Handshake(format!("invalid endpoint {endpoint}: {e}")))?;
    let mut req = uri
        .into_client_request()
        .map_err(|e| RealtimeError::Handshake(format!("client_request: {e}")))?;
    let key = generate_key();
    let headers = req.headers_mut();
    headers.insert(
        "sec-websocket-key",
        key.parse()
            .map_err(|e| RealtimeError::Handshake(format!("sec-websocket-key invalid: {e}")))?,
    );
    headers.insert(
        "authorization",
        format!("Bearer {}", auth.access_token)
            .parse()
            .map_err(|e| RealtimeError::Handshake(format!("authorization invalid: {e}")))?,
    );
    if !auth.account_id.is_empty() {
        headers.insert(
            "chatgpt-account-id",
            auth.account_id.parse().map_err(|e| {
                RealtimeError::Handshake(format!("chatgpt-account-id invalid: {e}"))
            })?,
        );
    }
    headers.insert(
        "originator",
        CODEX_RESPONSES_ORIGINATOR
            .parse()
            .map_err(|e| RealtimeError::Handshake(format!("originator invalid: {e}")))?,
    );
    headers.insert(
        "user-agent",
        CODEX_RESPONSES_USER_AGENT
            .parse()
            .map_err(|e| RealtimeError::Handshake(format!("user-agent invalid: {e}")))?,
    );
    headers.insert(
        "openai-beta",
        CODEX_RESPONSES_BETA
            .parse()
            .map_err(|e| RealtimeError::Handshake(format!("openai-beta invalid: {e}")))?,
    );
    Ok(req)
}

fn install_rustls_provider() {
    RUSTLS_PROVIDER.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normaliser_fills_required_and_blocks_additional() {
        let mut schema = json!({
            "type": "object",
            "properties": {
                "a": {"type": "string"},
                "nested": {
                    "type": "object",
                    "properties": {"b": {"type": "boolean"}}
                }
            }
        });
        require_all_object_properties_for_strict_schema(&mut schema);
        assert_eq!(schema["additionalProperties"], Value::Bool(false));
        let req: Vec<&str> = schema["required"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert!(req.contains(&"a") && req.contains(&"nested"));
        assert_eq!(
            schema["properties"]["nested"]["additionalProperties"],
            Value::Bool(false)
        );
        assert_eq!(schema["properties"]["nested"]["required"], json!(["b"]));
    }

    #[test]
    fn assembler_prefers_done_text() {
        let frames = vec![
            json!({"type":"response.output_text.delta","delta":"par"}),
            json!({"type":"response.output_text.done","text":"full"}),
            json!({"type":"response.completed","response":{"output":[]}}),
        ];
        assert_eq!(assemble_codex_responses_text(&frames).unwrap(), "full");
    }

    #[test]
    fn auth_handshake_error_matches_403_and_401() {
        assert!(is_auth_handshake_error(&RealtimeError::Handshake(
            "HTTP error: 403 Forbidden".into()
        )));
        assert!(is_auth_handshake_error(&RealtimeError::Handshake(
            "401 Unauthorized".into()
        )));
        // Non-auth handshake failures (timeouts, DNS) must NOT trigger a refresh.
        assert!(!is_auth_handshake_error(&RealtimeError::Handshake(
            "timeout".into()
        )));
        assert!(!is_auth_handshake_error(&RealtimeError::Protocol(
            "403 in a protocol frame".into()
        )));
    }

    #[test]
    fn assembler_falls_back_to_deltas() {
        let frames = vec![
            json!({"type":"response.output_text.delta","delta":"a"}),
            json!({"type":"response.output_text.delta","delta":"b"}),
            json!({"type":"response.completed"}),
        ];
        assert_eq!(assemble_codex_responses_text(&frames).unwrap(), "ab");
    }

    // codex-rs envelope contract: every Codex /responses request MUST carry
    // `service_tier: "priority"`. The priority lane is granted server-side, so
    // dropping or gating this field silently downgrades TPS. These tests fail
    // loud if anyone removes it from either frame.
    #[test]
    fn text_frame_always_requests_priority_tier() {
        let frame = build_text_frame("gpt-5.4", "sys", "user");
        assert_eq!(frame["service_tier"], "priority");
        assert_eq!(frame["type"], "response.create");
        assert_eq!(frame["model"], "gpt-5.4");
        assert_eq!(frame["reasoning"]["effort"], "low");
        assert_eq!(frame["text"]["verbosity"], "low");
        assert_eq!(frame["stream"], true);
        assert_eq!(frame["store"], false);
    }

    #[test]
    fn structured_frame_always_requests_priority_tier() {
        let schema = json!({"type":"object","properties":{"a":{"type":"string"}}});
        let frame = build_structured_frame("gpt-5.5", "sys", "user", "result", schema);
        assert_eq!(frame["service_tier"], "priority");
        assert_eq!(frame["type"], "response.create");
        assert_eq!(frame["text"]["format"]["type"], "json_schema");
        assert_eq!(frame["text"]["format"]["strict"], true);
        assert_eq!(frame["text"]["format"]["name"], "result");
    }
}
