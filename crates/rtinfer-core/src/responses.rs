//! codex/responses transport — free gpt-5.4 over Codex OAuth.
//!
//! Distinct from [`crate::RealtimePool`]: the realtime API
//! (`gpt-realtime-2.1`) and the codex/responses API speak different wire
//! grammars (realtime: `session.update` + `conversation.item.create` +
//! `response.create{output_modalities}`; responses: a single
//! `response.create` with `model` + `instructions` + `input` +
//! `text.format.json_schema`). Conflating them on one pool type would
//! mix two frame vocabularies, so this is its own pool.
//!
//! # Endpoint + cost
//!
//! `wss://chatgpt.com/backend-api/codex/responses` authenticated with
//! the Codex OAuth token (`~/.codex/auth.json` or an injected auth source).
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
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::generate_key;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, tungstenite::http::Uri};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tracing::{debug, trace};

use crate::auth::{CodexAuth, SharedCodexAuthSource, ID_TOKEN_REFRESH_MARGIN_SECS};
use crate::{RealtimeError, DEFAULT_HANDSHAKE_TIMEOUT};

/// codex/responses WebSocket endpoint (free over Codex OAuth).
pub const CODEX_RESPONSES_URL: &str = "wss://chatgpt.com/backend-api/codex/responses";

/// Default synthesis model. gpt-5.4 over Codex OAuth (ChatGPT Business
/// unlimited tokens). Callers can override per-request via the `model` field.
pub const CODEX_RESPONSES_MODEL: &str = "gpt-5.4";

/// `originator` header value (matches the codex CLI; verified live).
pub const CODEX_RESPONSES_ORIGINATOR: &str = "codex_cli_rs";

/// `User-Agent` header value (verified accepted live 2026-06-07).
pub const CODEX_RESPONSES_USER_AGENT: &str = "codex_cli_rs/rtinfer (rtinferd; rust)";

/// `OpenAI-Beta` header required to negotiate the WS responses protocol.
pub const CODEX_RESPONSES_BETA: &str = "responses_websockets=2026-02-06";

/// Reuse the realtime pool's TTL semantics.
pub use crate::DEFAULT_AUTH_TTL;

/// A connected codex/responses WebSocket, reusable across sequential asks.
type ResponsesSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Maximum simultaneous fresh TLS+WS handshakes to the codex/responses edge.
/// The edge 403-rejects large concurrent handshake bursts (observed at ~40+),
/// while accepting any number of asks on ALREADY-OPEN sockets. Gating only the
/// handshake staggers cold starts without limiting warm throughput.
const HANDSHAKE_GATE_PERMITS: usize = 8;
static HANDSHAKE_GATE: Semaphore = Semaphore::const_new(HANDSHAKE_GATE_PERMITS);

/// Cheap jitter source (no rand dependency): sub-millisecond clock entropy.
fn rand_like_jitter() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0)
}

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
    /// Seed explicit auth so the pool never reads `~/.codex/auth.json`.
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
    /// rejection, ignoring `auth_path`. Credential-process mode uses this to
    /// prevent any fallback to `~/.codex/auth.json`.
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
        // `auth_source` the first ask loads lazily through it; an external
        // credential operation must not run synchronously in `build`.
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
            idle_sockets: Mutex::new(Vec::new()),
        })
    }
}

#[derive(Clone)]
struct CachedAuth {
    auth: CodexAuth,
    fetched_at: Instant,
}

/// Free gpt-5.4 codex/responses pool. One strict-schema ask per call.
pub struct CodexResponsesPool {
    endpoint: String,
    model: String,
    semaphore: Semaphore,
    capacity: usize,
    auth_ttl: Duration,
    auth_path: Option<PathBuf>,
    auth_source: Option<SharedCodexAuthSource>,
    cached_auth: RwLock<Option<CachedAuth>>,
    /// Warm sockets checked back in after a clean ask. Each socket carries an
    /// exclusive request/response conversation, so a socket is EITHER idle in
    /// this pool OR owned by exactly one in-flight ask. Reuse eliminates the
    /// per-ask TLS+WS handshake (the dominant cost of small asks and the
    /// trigger of upstream 401/403 handshake rate-limiting under fan-out).
    idle_sockets: Mutex<Vec<ResponsesSocket>>,
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

    /// Ask gpt-5.4 for a single strict-schema structured object.
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
        let text = self.run_warm(&auth, &frame).await?;
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
        self.run_warm(&auth, &frame).await
    }

    /// Run one request frame over a warm socket when available, else a fresh
    /// connection. A socket that survives a full ask cleanly is checked back
    /// in for the next ask; any transport error drops the socket and the ask
    /// retries ONCE on a brand-new connection (which also re-handles the
    /// auth-rejected-handshake refresh path). Warm reuse removes the per-ask
    /// TLS+WS handshake that upstream rate-limits under fan-out.
    async fn run_warm(&self, auth: &CodexAuth, frame: &Value) -> Result<String, RealtimeError> {
        if let Some(mut ws) = self.checkout_socket().await {
            match run_on_socket(&mut ws, frame).await {
                Ok(text) => {
                    self.checkin_socket(ws).await;
                    return Ok(text);
                }
                Err(e) => {
                    // Stale/expired warm socket: drop it and fall through to a
                    // fresh connection. Do NOT surface this error; the retry
                    // below is authoritative.
                    debug!(error = %e, "codex/responses: warm socket failed; reconnecting");
                }
            }
        }
        // Acquire a socket with a bounded retry loop. Two supplies race here:
        // (a) warm sockets returned to the idle pool by completing asks, and
        // (b) fresh handshakes. The edge 403-rejects handshake bursts under a
        // rolling rate limit, so a 403 is NOT terminal: refresh auth once (the
        // stale-token case), then back off with jitter and re-poll the idle
        // pool before the next handshake attempt. Under fan-out this converges
        // because every finishing ask donates its warm socket back to the pool.
        let mut refreshed = false;
        let mut auth_current = auth.clone();
        let mut last_err: Option<RealtimeError> = None;
        for attempt in 0u64..8 {
            if attempt > 0 {
                if let Some(mut ws) = self.checkout_socket().await {
                    match run_on_socket(&mut ws, frame).await {
                        Ok(text) => {
                            self.checkin_socket(ws).await;
                            return Ok(text);
                        }
                        Err(e) => {
                            debug!(error = %e, "codex/responses: warm socket failed in retry; dropping");
                        }
                    }
                }
            }
            match connect_socket(&auth_current, &self.endpoint).await {
                Ok(mut ws) => {
                    return match run_on_socket(&mut ws, frame).await {
                        Ok(text) => {
                            self.checkin_socket(ws).await;
                            Ok(text)
                        }
                        Err(e) => Err(e),
                    };
                }
                Err(e) if is_auth_handshake_error(&e) => {
                    debug!(error = %e, attempt, "codex/responses: handshake rejected; backoff + retry");
                    if !refreshed {
                        auth_current = self.force_refresh_after(&auth_current.access_token).await?;
                        refreshed = true;
                    }
                    let backoff_ms = 300 * (attempt + 1) + (rand_like_jitter() % 700);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    last_err = Some(e);
                }
                Err(e) => return Err(e),
            }
        }
        Err(last_err
            .unwrap_or_else(|| RealtimeError::Handshake("handshake retries exhausted".to_owned())))
    }

    /// Open `count` warm sockets in the background and park them in the idle
    /// pool. Handshakes flow through the same gate as on-demand connects, so a
    /// large prewarm staggers instead of bursting into the edge's rolling
    /// handshake rate limit. Failures are logged and skipped; prewarm is a
    /// best-effort latency optimisation, never a correctness dependency.
    pub async fn prewarm(&self, count: usize) {
        for _ in 0..count {
            {
                let idle = self.idle_sockets.lock().await;
                if idle.len() >= count.min(self.capacity) {
                    break;
                }
            }
            let auth = match self.fresh_auth().await {
                Ok(a) => a,
                Err(e) => {
                    debug!(error = %e, "codex/responses: prewarm auth unavailable; stopping");
                    return;
                }
            };
            match connect_socket(&auth, &self.endpoint).await {
                Ok(ws) => self.checkin_socket(ws).await,
                Err(e) => {
                    // Rolling handshake limit: wait it out instead of hammering.
                    debug!(error = %e, "codex/responses: prewarm handshake failed; backing off");
                    tokio::time::sleep(Duration::from_millis(1_500 + (rand_like_jitter() % 3_000)))
                        .await;
                }
            }
        }
    }

    /// Take one idle warm socket, if any.
    async fn checkout_socket(&self) -> Option<ResponsesSocket> {
        self.idle_sockets.lock().await.pop()
    }

    /// Return a healthy socket to the idle pool, bounded by capacity.
    async fn checkin_socket(&self, ws: ResponsesSocket) {
        let mut idle = self.idle_sockets.lock().await;
        // The edge rate-limits HANDSHAKES, not open sockets. Keeping a large
        // idle pool is what lets heavy fan-out (hundreds of concurrent asks)
        // run entirely on warm sockets after the first wave.
        if idle.len() < self.capacity.min(512) {
            idle.push(ws);
        }
        // else: drop the socket; the pool is already warm enough.
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
        // does the client-side OAuth rotation; external sources own refresh.
        // The path branch keeps the legacy proactive refresh.
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
            Some(source) => source.force_refresh(stale_access_token).await?,
            None => {
                let base = match self.auth_path.as_deref() {
                    Some(p) => CodexAuth::from_path(p)?,
                    None => CodexAuth::from_default_path()?,
                };
                // Mirror FileCodexAuthSource: if another writer already rotated
                // past the rejected token, reuse it instead of refreshing again.
                if base.access_token != stale_access_token {
                    base
                } else {
                    base.refresh().await?
                }
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

async fn connect_socket(
    auth: &CodexAuth,
    endpoint: &str,
) -> Result<ResponsesSocket, RealtimeError> {
    install_rustls_provider();
    let _handshake_permit = HANDSHAKE_GATE
        .acquire()
        .await
        .map_err(|e| RealtimeError::Protocol(format!("handshake gate closed: {e}")))?;
    let client_request = build_responses_request(endpoint, auth)?;
    let connect_fut = connect_async(client_request);
    match timeout(DEFAULT_HANDSHAKE_TIMEOUT, connect_fut).await {
        Ok(Ok((ws, _resp))) => Ok(ws),
        Ok(Err(e)) => Err(RealtimeError::Handshake(format!("{e}"))),
        Err(_) => Err(RealtimeError::Handshake("timeout".to_owned())),
    }
}

/// Send one request frame on an already-connected socket and assemble the
/// streamed text. The socket is left OPEN on success so the caller can check
/// it back into the warm pool; every error path leaves the socket to be
/// dropped by the caller.
async fn run_on_socket(
    ws: &mut ResponsesSocket,
    request_frame: &Value,
) -> Result<String, RealtimeError> {
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
            Message::Close(_) => {
                err = Some(RealtimeError::Protocol("socket closed mid-ask".to_owned()));
                break 'read;
            }
            _ => {}
        }
    }

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
    use std::sync::{Arc, Mutex as StdMutex};

    struct RecordingSource {
        rejected: StdMutex<Vec<String>>,
    }

    fn source_auth(access_token: &str) -> CodexAuth {
        CodexAuth {
            access_token: access_token.into(),
            account_id: "acct".into(),
            id_token: String::new(),
            refresh_token: String::new(),
            source_path: None,
        }
    }

    #[async_trait::async_trait]
    impl crate::CodexAuthSource for RecordingSource {
        async fn load(&self) -> Result<CodexAuth, RealtimeError> {
            Ok(source_auth("loaded-access"))
        }

        async fn force_refresh(
            &self,
            rejected_access_token: &str,
        ) -> Result<CodexAuth, RealtimeError> {
            self.rejected
                .lock()
                .unwrap()
                .push(rejected_access_token.into());
            Ok(source_auth("forced-access"))
        }
    }

    #[tokio::test]
    async fn responses_force_passes_rejected_access_to_source() {
        let source = Arc::new(RecordingSource {
            rejected: StdMutex::new(Vec::new()),
        });
        let pool = CodexResponsesPool::builder()
            .auth_source(source.clone())
            .build();

        let loaded = pool.fresh_auth().await.unwrap();
        let forced = pool
            .force_refresh_after(&loaded.access_token)
            .await
            .unwrap();

        assert_eq!(forced.access_token, "forced-access");
        assert_eq!(
            source.rejected.lock().unwrap().as_slice(),
            ["loaded-access"]
        );
    }

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
