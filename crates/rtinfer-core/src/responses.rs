//! Codex `/responses` dual-transport coordinator.
//!
//! [`CodexResponsesClient`] admits each logical ask through one
//! [`AdaptiveConcurrency`] controller, then dispatches exactly once to either
//! a reused HTTP/2+SSE Warpsock client or an exclusive reusable WSS socket.
//! Success requires semantic `response.completed`; post-send replay is forbidden.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Once};
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use http::Request;
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Semaphore};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::generate_key;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, tungstenite::http::Uri};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tracing::{debug, info, warn};
use uuid::Uuid;
use warpsock::{
    CapacityPolicy, Client as WarpsockClient, FingerprintProfile, Headers, HttpVersion,
    RedirectPolicy,
};

use crate::adaptive::{
    AdaptiveConcurrency, AdaptiveConcurrencyConfig, AdaptiveConcurrencySnapshot, AdaptiveLease,
    AdaptiveOutcome, ConcurrencyLimits, EnabledResponsesLanes, ResponsesTransportKind,
};
use crate::auth::{CodexAuth, CodexAuthCache, SharedCodexAuthSource};
use crate::{RealtimeError, DEFAULT_AUTH_TTL, DEFAULT_HANDSHAKE_TIMEOUT};

/// Codex `/responses` WebSocket endpoint (free over Codex OAuth).
pub const CODEX_RESPONSES_URL: &str = "wss://chatgpt.com/backend-api/codex/responses";

/// Codex `/responses` HTTP endpoint (HTTP/2 + SSE).
pub const CODEX_RESPONSES_HTTP_URL: &str = "https://chatgpt.com/backend-api/codex/responses";

/// Default synthesis model.
pub const CODEX_RESPONSES_MODEL: &str = "gpt-5.4";

/// `originator` header / client identity (codex-tui GA wire).
pub const CODEX_RESPONSES_ORIGINATOR: &str = "codex-tui";

/// Pinned codex-tui client version used in headers and user-agent.
pub const CODEX_CLIENT_VERSION: &str = "0.145.0-alpha.24";

/// `User-Agent` header value. MUST NOT contain `reqwest`.
pub const CODEX_RESPONSES_USER_AGENT: &str =
    "codex-tui/0.145.0-alpha.24 (rtinfer; rust) unknown (codex-tui; 0.145.0-alpha.24)";

/// `x-codex-beta-features` value.
pub const CODEX_BETA_FEATURES: &str = "remote_compaction_v2";

/// Always request the priority lane.
pub(crate) const CODEX_SERVICE_TIER: &str = "priority";

type ResponsesSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

const HANDSHAKE_GATE_PERMITS: usize = 8;
static HANDSHAKE_GATE: Semaphore = Semaphore::const_new(HANDSHAKE_GATE_PERMITS);

const HTTP_HARD_MAX: usize = 256;
const WSS_HARD_MAX: usize = 64;
const SSE_EVENT_BUFFER_MAX: usize = 1 << 20;
const HTTP_TOTAL_TIMEOUT: Duration = Duration::from_secs(120);
const WSS_TERMINAL_DEADLINE: Duration = Duration::from_secs(120);
/// Best-effort cleanup is deliberately bounded: a peer that keeps an ignored
/// response body open must not retain an admitted request forever.
const HTTP_BODY_DRAIN_LIMIT: usize = 64 * 1024;
const HTTP_BODY_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

static RUSTLS_PROVIDER: Once = Once::new();

fn rand_like_jitter() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0)
}

fn new_uuid_v4() -> String {
    Uuid::new_v4().to_string()
}

fn install_rustls_provider() {
    RUSTLS_PROVIDER.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

// ---------------------------------------------------------------------------
// Result taxonomy / dispatch phase
// ---------------------------------------------------------------------------

/// Logical result class for one Responses attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum ResponsesResultClass {
    Success,
    LaneOverload,
    SharedThrottle,
    Failure,
    Indeterminate,
}

impl ResponsesResultClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::LaneOverload => "lane_overload",
            Self::SharedThrottle => "shared_throttle",
            Self::Failure => "failure",
            Self::Indeterminate => "indeterminate",
        }
    }
}

/// When a logical send may still be retried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum DispatchPhase {
    PreSend,
    SendAttempted,
    Terminal,
}

#[derive(Debug)]
pub(crate) struct AttemptOutcome {
    class: ResponsesResultClass,
    text: Option<String>,
    error: Option<RealtimeError>,
    terminal_seen: bool,
    retry_after_secs: Option<u64>,
    http_version: Option<String>,
    http_reused: Option<bool>,
    wss_reused: Option<bool>,
    /// Present only when a text delta/done was observed before terminal.
    ttft_ms: Option<u128>,
}

impl AttemptOutcome {
    fn success(text: String) -> Self {
        Self {
            class: ResponsesResultClass::Success,
            text: Some(text),
            error: None,
            terminal_seen: true,
            retry_after_secs: None,
            http_version: None,
            http_reused: None,
            wss_reused: None,
            ttft_ms: None,
        }
    }

    fn err(
        class: ResponsesResultClass,
        error: RealtimeError,
        terminal_seen: bool,
        retry_after_secs: Option<u64>,
    ) -> Self {
        Self {
            class,
            text: None,
            error: Some(error),
            terminal_seen,
            retry_after_secs,
            http_version: None,
            http_reused: None,
            wss_reused: None,
            ttft_ms: None,
        }
    }

    fn into_result(self) -> Result<String, RealtimeError> {
        match (self.class, self.text, self.error) {
            (ResponsesResultClass::Success, Some(text), _) => Ok(text),
            (_, _, Some(err)) => Err(err),
            _ => Err(RealtimeError::Protocol(
                "codex/responses attempt produced no result".into(),
            )),
        }
    }

    fn adaptive_outcome(&self) -> AdaptiveOutcome {
        match self.class {
            ResponsesResultClass::Success => AdaptiveOutcome::Success,
            ResponsesResultClass::LaneOverload => AdaptiveOutcome::LaneOverload,
            ResponsesResultClass::SharedThrottle => AdaptiveOutcome::SharedThrottle {
                retry_after_secs: self.retry_after_secs,
            },
            ResponsesResultClass::Failure => AdaptiveOutcome::Failure,
            ResponsesResultClass::Indeterminate => AdaptiveOutcome::Indeterminate,
        }
    }
}

fn classify_provider_code(code: &str) -> ResponsesResultClass {
    match code {
        "server_is_overloaded" | "slow_down" | "websocket_connection_limit_reached" => {
            ResponsesResultClass::LaneOverload
        }
        "rate_limit_exceeded" => ResponsesResultClass::SharedThrottle,
        _ => ResponsesResultClass::Failure,
    }
}

/// Test seam: exact provider-code taxonomy shared by HTTP and WSS classifiers.
#[doc(hidden)]
pub fn classify_responses_provider_code(code: &str) -> ResponsesResultClass {
    classify_provider_code(code)
}

/// Test seam: classify an HTTP status + optional provider code the same way the
/// HTTP lane does (429 without lane-overload code => SharedThrottle).
#[doc(hidden)]
pub fn classify_responses_http_status(
    status: u16,
    provider_code: Option<&str>,
) -> ResponsesResultClass {
    if status == 401 || status == 403 {
        return ResponsesResultClass::Failure;
    }
    if let Some(code) = provider_code {
        let class = classify_provider_code(code);
        if status == 429 && class != ResponsesResultClass::LaneOverload {
            return ResponsesResultClass::SharedThrottle;
        }
        if class == ResponsesResultClass::LaneOverload {
            return ResponsesResultClass::LaneOverload;
        }
        if status == 429 {
            return ResponsesResultClass::SharedThrottle;
        }
        return class;
    }
    if status == 429 {
        return ResponsesResultClass::SharedThrottle;
    }
    if (200..300).contains(&status) {
        return ResponsesResultClass::Success;
    }
    ResponsesResultClass::Failure
}

/// Test seam for the live endpoint's SSE content-type behavior. The Codex
/// backend currently omits `Content-Type` on successful HTTP/2 streams; an
/// explicit value must still identify SSE.
#[doc(hidden)]
pub fn responses_content_type_allows_sse(content_type: Option<&str>) -> bool {
    content_type.is_none_or(|value| value.to_ascii_lowercase().starts_with("text/event-stream"))
}

fn classify_provider_error(err: &RealtimeError) -> ResponsesResultClass {
    match err {
        RealtimeError::Provider { code, .. } => classify_provider_code(code),
        _ => ResponsesResultClass::Failure,
    }
}

// ---------------------------------------------------------------------------
// Pure stream assembler — shared by WSS, SSE, and replay tests.
// ---------------------------------------------------------------------------

enum FrameOutcome {
    Continue,
    Completed,
    Error(RealtimeError),
}

struct ResponsesAssembler {
    delta_buf: String,
    done_text: Option<String>,
    completed_status: Option<String>,
    terminal_seen: bool,
    started: Instant,
    ttft: Option<Duration>,
}

impl Default for ResponsesAssembler {
    fn default() -> Self {
        Self {
            delta_buf: String::new(),
            done_text: None,
            completed_status: None,
            terminal_seen: false,
            started: Instant::now(),
            ttft: None,
        }
    }
}

impl ResponsesAssembler {
    fn note_first_text(&mut self) {
        if self.ttft.is_none() {
            self.ttft = Some(self.started.elapsed());
        }
    }

    fn attach_ttft(&self, mut outcome: AttemptOutcome) -> AttemptOutcome {
        outcome.ttft_ms = self.ttft.map(|d| d.as_millis());
        outcome
    }

    fn on_frame(&mut self, v: &Value) -> FrameOutcome {
        match v.get("type").and_then(Value::as_str).unwrap_or("") {
            "response.output_text.delta" | "response.text.delta" => {
                self.note_first_text();
                if let Some(d) = v.get("delta").and_then(Value::as_str) {
                    self.delta_buf.push_str(d);
                }
                FrameOutcome::Continue
            }
            "response.output_text.done" | "response.text.done" => {
                self.note_first_text();
                if let Some(t) = v.get("text").and_then(Value::as_str) {
                    self.done_text = Some(t.to_string());
                }
                FrameOutcome::Continue
            }
            "error" => FrameOutcome::Error(provider_error_from_value(v.get("error"))),
            "response.failed" => FrameOutcome::Error(provider_error_from_value(
                v.get("response").and_then(|r| r.get("error")),
            )),
            "response.completed" => {
                self.terminal_seen = true;
                self.completed_status = v
                    .get("response")
                    .and_then(|r| r.get("status"))
                    .and_then(Value::as_str)
                    .map(|s| s.to_string());
                FrameOutcome::Completed
            }
            _ => FrameOutcome::Continue,
        }
    }

    fn assembled_text(&self) -> String {
        self.done_text
            .clone()
            .unwrap_or_else(|| self.delta_buf.clone())
    }

    fn finish_after_completed(self) -> AttemptOutcome {
        let ttft_ms = self.ttft.map(|d| d.as_millis());
        let status_ok = match self.completed_status.as_deref() {
            None | Some("completed") => true,
            Some(other) => {
                let mut outcome = AttemptOutcome::err(
                    ResponsesResultClass::Failure,
                    RealtimeError::Protocol(format!(
                        "codex/responses completed with non-completed status: {other}"
                    )),
                    true,
                    None,
                );
                outcome.ttft_ms = ttft_ms;
                return outcome;
            }
        };
        let text = self.assembled_text();
        if !status_ok || text.trim().is_empty() {
            let mut outcome = AttemptOutcome::err(
                ResponsesResultClass::Failure,
                RealtimeError::Protocol(
                    "codex/responses stream produced no text after response.completed".into(),
                ),
                true,
                None,
            );
            outcome.ttft_ms = ttft_ms;
            return outcome;
        }
        let mut outcome = AttemptOutcome::success(text);
        outcome.ttft_ms = ttft_ms;
        outcome
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
/// Requires semantic `response.completed`. EOF / partial text / done-only
/// streams return [`RealtimeError::Protocol`] with label `protocol`.
pub fn assemble_codex_responses_text(frames: &[Value]) -> Result<String, RealtimeError> {
    let mut asm = ResponsesAssembler::default();
    for f in frames {
        match asm.on_frame(f) {
            FrameOutcome::Continue => {}
            FrameOutcome::Completed => return asm.finish_after_completed().into_result(),
            FrameOutcome::Error(e) => return Err(e),
        }
    }
    Err(RealtimeError::Protocol(
        "codex/responses incomplete stream: missing response.completed".into(),
    ))
}

// ---------------------------------------------------------------------------
// Incremental SSE decoder
// ---------------------------------------------------------------------------

/// Chunk-boundary-safe SSE decoder that feeds [`ResponsesAssembler`].
#[derive(Default)]
pub struct SseDecoder {
    buf: Vec<u8>,
    event_name: String,
    data_lines: Vec<String>,
    assembler: ResponsesAssembler,
    completed: bool,
    error: Option<RealtimeError>,
}

impl SseDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, chunk: &[u8]) -> Result<(), RealtimeError> {
        if self.error.is_some() || self.completed {
            return Ok(());
        }
        if self.buf.len().saturating_add(chunk.len()) > SSE_EVENT_BUFFER_MAX {
            return Err(RealtimeError::Protocol(
                "codex/responses SSE event buffer exceeded".into(),
            ));
        }
        self.buf.extend_from_slice(chunk);
        self.drain_events()
    }

    fn drain_events(&mut self) -> Result<(), RealtimeError> {
        loop {
            let sep = find_event_separator(&self.buf);
            let Some((sep_start, sep_len)) = sep else {
                break;
            };
            let raw = self.buf.drain(..sep_start + sep_len).collect::<Vec<u8>>();
            let event_bytes = &raw[..sep_start];
            self.dispatch_event_block(event_bytes)?;
            if self.completed || self.error.is_some() {
                break;
            }
        }
        if self.buf.len() > SSE_EVENT_BUFFER_MAX {
            return Err(RealtimeError::Protocol(
                "codex/responses SSE event buffer exceeded".into(),
            ));
        }
        Ok(())
    }

    fn dispatch_event_block(&mut self, block: &[u8]) -> Result<(), RealtimeError> {
        let text = std::str::from_utf8(block)
            .map_err(|_| RealtimeError::Protocol("codex/responses SSE: malformed UTF-8".into()))?;
        self.event_name.clear();
        self.data_lines.clear();
        for line in text.split('\n') {
            let line = line.strip_suffix('\r').unwrap_or(line);
            if line.is_empty() || line.starts_with(':') {
                continue;
            }
            if let Some(rest) = line.strip_prefix("event:") {
                self.event_name = rest.trim_start().to_string();
            } else if let Some(rest) = line.strip_prefix("data:") {
                let data = rest.strip_prefix(' ').unwrap_or(rest);
                self.data_lines.push(data.to_string());
            }
        }
        if self.data_lines.is_empty() {
            return Ok(());
        }
        let data = self.data_lines.join("\n");
        if data == "[DONE]" {
            return Err(RealtimeError::Protocol(
                "codex/responses SSE [DONE] is not semantic completion".into(),
            ));
        }
        let v: Value = serde_json::from_str(&data).map_err(|e| {
            RealtimeError::Protocol(format!("codex/responses SSE: malformed JSON: {e}"))
        })?;
        match self.assembler.on_frame(&v) {
            FrameOutcome::Continue => Ok(()),
            FrameOutcome::Completed => {
                self.completed = true;
                Ok(())
            }
            FrameOutcome::Error(e) => {
                self.error = Some(e);
                Ok(())
            }
        }
    }

    pub(crate) fn finish(self) -> AttemptOutcome {
        if let Some(err) = self.error {
            let class = classify_provider_error(&err);
            return self.assembler.attach_ttft(AttemptOutcome::err(
                class,
                err,
                self.assembler.terminal_seen,
                None,
            ));
        }
        if !self.completed {
            return self.assembler.attach_ttft(AttemptOutcome::err(
                ResponsesResultClass::Indeterminate,
                RealtimeError::Protocol(
                    "codex/responses incomplete SSE stream: missing response.completed".into(),
                ),
                false,
                None,
            ));
        }
        self.assembler.finish_after_completed()
    }
}

fn find_event_separator(buf: &[u8]) -> Option<(usize, usize)> {
    let mut i = 0;
    while i + 1 < buf.len() {
        if buf[i] == b'\n' && buf[i + 1] == b'\n' {
            return Some((i, 2));
        }
        if i + 3 < buf.len()
            && buf[i] == b'\r'
            && buf[i + 1] == b'\n'
            && buf[i + 2] == b'\r'
            && buf[i + 3] == b'\n'
        {
            return Some((i, 4));
        }
        i += 1;
    }
    None
}

/// Assemble SSE byte chunks into output text. Requires `response.completed`.
pub fn assemble_codex_responses_sse(chunks: &[&[u8]]) -> Result<String, RealtimeError> {
    let mut dec = SseDecoder::new();
    for chunk in chunks {
        dec.push(chunk)?;
        if dec.error.is_some() || dec.completed {
            break;
        }
    }
    dec.finish().into_result()
}

// ---------------------------------------------------------------------------
// Strict-schema normaliser
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
// Canonical wire
// ---------------------------------------------------------------------------

/// Fresh logical IDs for one ask. Stable across pre-send retries.
#[derive(Debug, Clone)]
pub struct CodexRequestIds {
    pub session_id: String,
    pub thread_id: String,
    pub turn_id: String,
}

impl CodexRequestIds {
    pub fn fresh() -> Self {
        Self {
            session_id: new_uuid_v4(),
            thread_id: new_uuid_v4(),
            turn_id: new_uuid_v4(),
        }
    }

    pub fn window_id(&self) -> String {
        format!("{}:0", self.thread_id)
    }

    pub fn prompt_cache_key(&self) -> &str {
        &self.thread_id
    }
}

/// Canonical codex-tui request representation for HTTP and WSS.
#[derive(Debug, Clone)]
pub struct CodexResponsesWireRequest {
    pub ids: CodexRequestIds,
    pub installation_id: String,
    pub model: String,
    pub body: Value,
}

impl CodexResponsesWireRequest {
    pub fn http_body(&self) -> &Value {
        &self.body
    }

    pub fn wss_frame(&self) -> Value {
        let mut frame = self.body.clone();
        if let Some(obj) = frame.as_object_mut() {
            obj.insert("type".into(), Value::String("response.create".into()));
        }
        frame
    }
}

fn turn_metadata_json() -> String {
    // ASCII-JSON-string for client_metadata / headers.
    json!({
        "request_kind": "turn",
        "thread_source": "user",
        "sandbox": "none",
    })
    .to_string()
}

fn prewarm_turn_metadata_json() -> String {
    json!({
        "request_kind": "prewarm",
        "turn_id": "",
    })
    .to_string()
}

fn build_common_body(
    model: &str,
    system: &str,
    user: &str,
    ids: &CodexRequestIds,
    installation_id: &str,
    text: Value,
) -> Value {
    let turn_meta = turn_metadata_json();
    json!({
        "model": model,
        "instructions": system,
        "input": [{
            "type": "message",
            "role": "user",
            "internal_chat_message_metadata_passthrough": {
                "turn_id": ids.turn_id,
            },
            "content": [{
                "type": "input_text",
                "text": user,
            }],
        }],
        "tools": [],
        "tool_choice": "auto",
        "parallel_tool_calls": false,
        "reasoning": {"effort": "low"},
        "store": false,
        "stream": true,
        "include": [],
        "service_tier": CODEX_SERVICE_TIER,
        "prompt_cache_key": ids.prompt_cache_key(),
        "text": text,
        "client_metadata": {
            "x-codex-installation-id": installation_id,
            "session_id": ids.session_id,
            "thread_id": ids.thread_id,
            "x-codex-window-id": ids.window_id(),
            "turn_id": ids.turn_id,
            "x-codex-turn-metadata": turn_meta,
        },
    })
}

fn build_text_wire(
    model: &str,
    system: &str,
    user: &str,
    ids: CodexRequestIds,
    installation_id: &str,
) -> CodexResponsesWireRequest {
    let body = build_common_body(
        model,
        system,
        user,
        &ids,
        installation_id,
        json!({"verbosity": "low"}),
    );
    CodexResponsesWireRequest {
        ids,
        installation_id: installation_id.to_string(),
        model: model.to_string(),
        body,
    }
}

fn build_structured_wire(
    model: &str,
    system: &str,
    user: &str,
    schema_name: &str,
    schema: Value,
    ids: CodexRequestIds,
    installation_id: &str,
) -> CodexResponsesWireRequest {
    let body = build_common_body(
        model,
        system,
        user,
        &ids,
        installation_id,
        json!({
            "verbosity": "low",
            "format": {
                "type": "json_schema",
                "name": schema_name,
                "strict": true,
                "schema": schema,
            },
        }),
    );
    CodexResponsesWireRequest {
        ids,
        installation_id: installation_id.to_string(),
        model: model.to_string(),
        body,
    }
}

fn build_http_headers(auth: &CodexAuth, ids: &CodexRequestIds) -> Vec<(String, String)> {
    let mut headers = vec![
        (
            "Authorization".into(),
            format!("Bearer {}", auth.access_token),
        ),
        ("Accept".into(), "text/event-stream".into()),
        ("Content-Type".into(), "application/json".into()),
        ("originator".into(), CODEX_RESPONSES_ORIGINATOR.into()),
        ("version".into(), CODEX_CLIENT_VERSION.into()),
        ("User-Agent".into(), CODEX_RESPONSES_USER_AGENT.into()),
        ("x-codex-beta-features".into(), CODEX_BETA_FEATURES.into()),
        ("session-id".into(), ids.session_id.clone()),
        ("thread-id".into(), ids.thread_id.clone()),
        ("x-client-request-id".into(), ids.thread_id.clone()),
        ("x-codex-window-id".into(), ids.window_id()),
    ];
    if !auth.account_id.is_empty() {
        headers.push(("ChatGPT-Account-ID".into(), auth.account_id.clone()));
    }
    headers
}

fn build_wss_handshake_request(
    endpoint: &str,
    auth: &CodexAuth,
    connection_ids: &CodexRequestIds,
) -> Result<Request<()>, RealtimeError> {
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
            "ChatGPT-Account-ID",
            auth.account_id.parse().map_err(|e| {
                RealtimeError::Handshake(format!("ChatGPT-Account-ID invalid: {e}"))
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
        "version",
        CODEX_CLIENT_VERSION
            .parse()
            .map_err(|e| RealtimeError::Handshake(format!("version invalid: {e}")))?,
    );
    headers.insert(
        "user-agent",
        CODEX_RESPONSES_USER_AGENT
            .parse()
            .map_err(|e| RealtimeError::Handshake(format!("user-agent invalid: {e}")))?,
    );
    headers.insert(
        "x-codex-beta-features",
        CODEX_BETA_FEATURES
            .parse()
            .map_err(|e| RealtimeError::Handshake(format!("x-codex-beta-features invalid: {e}")))?,
    );
    headers.insert(
        "session-id",
        connection_ids
            .session_id
            .parse()
            .map_err(|e| RealtimeError::Handshake(format!("session-id invalid: {e}")))?,
    );
    headers.insert(
        "thread-id",
        connection_ids
            .thread_id
            .parse()
            .map_err(|e| RealtimeError::Handshake(format!("thread-id invalid: {e}")))?,
    );
    headers.insert(
        "x-client-request-id",
        connection_ids
            .thread_id
            .parse()
            .map_err(|e| RealtimeError::Handshake(format!("x-client-request-id invalid: {e}")))?,
    );
    headers.insert(
        "x-codex-window-id",
        connection_ids
            .window_id()
            .parse()
            .map_err(|e| RealtimeError::Handshake(format!("x-codex-window-id invalid: {e}")))?,
    );
    headers.insert(
        "x-codex-turn-metadata",
        prewarm_turn_metadata_json()
            .parse()
            .map_err(|e| RealtimeError::Handshake(format!("x-codex-turn-metadata invalid: {e}")))?,
    );
    // Responses WSS is GA: do not set OpenAI-Beta.
    Ok(req)
}

fn resolve_installation_id(
    injected: Option<String>,
    path: Option<&Path>,
) -> Result<String, RealtimeError> {
    if let Some(id) = injected {
        let id = id.trim().to_string();
        if id.is_empty() || Uuid::parse_str(&id).is_err() {
            return Err(RealtimeError::Protocol(
                "responses config: installation_id malformed".into(),
            ));
        }
        return Ok(id);
    }
    let path = match path {
        Some(p) => p.to_path_buf(),
        None => default_installation_id_path().ok_or_else(|| {
            RealtimeError::Protocol("responses config: home directory unavailable".into())
        })?,
    };
    match std::fs::read_to_string(&path) {
        Ok(raw) => {
            let id = raw.trim().to_string();
            if id.is_empty() || Uuid::parse_str(&id).is_err() {
                return Err(RealtimeError::Protocol(
                    "responses config: installation_id malformed".into(),
                ));
            }
            Ok(id)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let id = new_uuid_v4();
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    RealtimeError::Protocol(format!(
                        "responses config: installation_id unwritable: {e}"
                    ))
                })?;
            }
            write_installation_id_atomic(&path, &id).map_err(|e| {
                RealtimeError::Protocol(format!(
                    "responses config: installation_id unwritable: {e}"
                ))
            })?;
            Ok(id)
        }
        Err(e) => Err(RealtimeError::Protocol(format!(
            "responses config: installation_id unreadable: {e}"
        ))),
    }
}

fn default_installation_id_path() -> Option<PathBuf> {
    let auth = crate::default_auth_path()?;
    Some(auth.with_file_name("installation_id"))
}

fn write_installation_id_atomic(path: &Path, contents: &str) -> std::io::Result<()> {
    use std::io::Write;
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    {
        #[cfg(unix)]
        let mut f = {
            use std::os::unix::fs::OpenOptionsExt;
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&tmp)?
        };
        #[cfg(not(unix))]
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(contents.as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

// ---------------------------------------------------------------------------
// Transport mode / runtime config
// ---------------------------------------------------------------------------

/// Runtime transport selection for Responses asks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponsesTransportMode {
    Wss,
    Http,
    Dual,
}

impl ResponsesTransportMode {
    pub fn parse(raw: &str) -> Result<Self, RealtimeError> {
        match raw.trim() {
            "" | "wss" => Ok(Self::Wss),
            "http" => Ok(Self::Http),
            "dual" => Ok(Self::Dual),
            other => Err(RealtimeError::Protocol(format!(
                "responses config: RTINFER_RESPONSES_TRANSPORT invalid value {other:?}"
            ))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Wss => "wss",
            Self::Http => "http",
            Self::Dual => "dual",
        }
    }

    fn enabled_lanes(self) -> EnabledResponsesLanes {
        match self {
            Self::Wss => EnabledResponsesLanes::WebSocket,
            Self::Http => EnabledResponsesLanes::Http,
            Self::Dual => EnabledResponsesLanes::Dual,
        }
    }
}

/// Process-level Responses runtime configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponsesRuntimeConfig {
    pub mode: ResponsesTransportMode,
    pub http_initial: usize,
    pub http_max: usize,
    pub wss_initial: usize,
    pub wss_max: usize,
    pub aggregate_max: usize,
    pub prewarm: usize,
}

impl Default for ResponsesRuntimeConfig {
    fn default() -> Self {
        Self::defaults_for_mode(ResponsesTransportMode::Wss)
    }
}

impl ResponsesRuntimeConfig {
    /// Mode-appropriate defaults: aggregate equals the enabled-lane maxima sum
    /// (`64` WSS, `256` HTTP, `320` Dual).
    pub fn defaults_for_mode(mode: ResponsesTransportMode) -> Self {
        let aggregate_max = match mode {
            ResponsesTransportMode::Wss => WSS_HARD_MAX,
            ResponsesTransportMode::Http => HTTP_HARD_MAX,
            ResponsesTransportMode::Dual => HTTP_HARD_MAX + WSS_HARD_MAX,
        };
        Self {
            mode,
            http_initial: 8.min(HTTP_HARD_MAX),
            http_max: HTTP_HARD_MAX,
            wss_initial: 4.min(WSS_HARD_MAX),
            wss_max: WSS_HARD_MAX,
            aggregate_max,
            prewarm: 0,
        }
    }

    /// Strict cross-field validation for builder-supplied and public configs.
    ///
    /// Invalid values return [`RealtimeError::Protocol`] beginning with
    /// `responses config:`; nothing is clamped or silently normalized.
    pub fn validate(&self) -> Result<(), RealtimeError> {
        if !(1..=HTTP_HARD_MAX).contains(&self.http_max) {
            return Err(RealtimeError::Protocol(format!(
                "responses config: http_max must be in 1..={HTTP_HARD_MAX}"
            )));
        }
        if !(1..=self.http_max).contains(&self.http_initial) {
            return Err(RealtimeError::Protocol(
                "responses config: http_initial must be in 1..=http_max".into(),
            ));
        }
        if !(1..=WSS_HARD_MAX).contains(&self.wss_max) {
            return Err(RealtimeError::Protocol(format!(
                "responses config: wss_max must be in 1..={WSS_HARD_MAX}"
            )));
        }
        if !(1..=self.wss_max).contains(&self.wss_initial) {
            return Err(RealtimeError::Protocol(
                "responses config: wss_initial must be in 1..=wss_max".into(),
            ));
        }
        if self.prewarm > self.wss_max {
            return Err(RealtimeError::Protocol(format!(
                "responses config: prewarm must be in 0..={}",
                self.wss_max
            )));
        }

        let enabled_sum = match self.mode {
            ResponsesTransportMode::Http => self.http_max,
            ResponsesTransportMode::Wss => self.wss_max,
            ResponsesTransportMode::Dual => self.http_max + self.wss_max,
        };
        let (agg_lo, agg_hi) = match self.mode {
            ResponsesTransportMode::Http => (1usize, self.http_max.min(HTTP_HARD_MAX)),
            ResponsesTransportMode::Wss => (1usize, self.wss_max.min(WSS_HARD_MAX)),
            ResponsesTransportMode::Dual => (2usize, enabled_sum.min(320)),
        };
        if self.aggregate_max < agg_lo || self.aggregate_max > agg_hi {
            return Err(RealtimeError::Protocol(format!(
                "responses config: aggregate_max must be in {agg_lo}..={agg_hi}"
            )));
        }
        if self.aggregate_max > enabled_sum {
            return Err(RealtimeError::Protocol(format!(
                "responses config: aggregate_max must not exceed enabled-lane maxima sum ({enabled_sum})"
            )));
        }
        Ok(())
    }

    pub fn from_env() -> Result<Self, RealtimeError> {
        let mode = match std::env::var("RTINFER_RESPONSES_TRANSPORT") {
            Ok(v) => ResponsesTransportMode::parse(&v)?,
            Err(_) => ResponsesTransportMode::Wss,
        };
        let http_max = parse_bound_env(
            "RTINFER_RESPONSES_HTTP_MAX",
            HTTP_HARD_MAX,
            1,
            HTTP_HARD_MAX,
        )?;
        let http_initial = parse_bound_env(
            "RTINFER_RESPONSES_HTTP_INITIAL",
            8.min(http_max),
            1,
            http_max,
        )?;
        let (wss_max, legacy_zero_warned) = parse_wss_max_from_env()?;
        let wss_initial =
            parse_bound_env("RTINFER_RESPONSES_WSS_INITIAL", 4.min(wss_max), 1, wss_max)?;
        let enabled_sum = match mode {
            ResponsesTransportMode::Http => http_max,
            ResponsesTransportMode::Wss => wss_max,
            ResponsesTransportMode::Dual => http_max + wss_max,
        };
        let (agg_lo, agg_hi) = match mode {
            ResponsesTransportMode::Http => (1, http_max.min(HTTP_HARD_MAX)),
            ResponsesTransportMode::Wss => (1, wss_max.min(WSS_HARD_MAX)),
            ResponsesTransportMode::Dual => (2, enabled_sum.min(320)),
        };
        let aggregate_max = parse_bound_env(
            "RTINFER_RESPONSES_AGGREGATE_MAX",
            enabled_sum.min(agg_hi).max(agg_lo),
            agg_lo,
            agg_hi,
        )?;
        if aggregate_max > enabled_sum {
            return Err(RealtimeError::Protocol(format!(
                "responses config: RTINFER_RESPONSES_AGGREGATE_MAX must not exceed enabled-lane maxima sum ({enabled_sum})"
            )));
        }
        let prewarm = parse_bound_env("RTINFER_RESPONSES_PREWARM", 0, 0, wss_max)?;
        let _ = legacy_zero_warned;
        let cfg = Self {
            mode,
            http_initial,
            http_max,
            wss_initial,
            wss_max,
            aggregate_max,
            prewarm,
        };
        cfg.validate()?;
        Ok(cfg)
    }

    fn adaptive_config(&self) -> AdaptiveConcurrencyConfig {
        AdaptiveConcurrencyConfig {
            http: ConcurrencyLimits {
                initial: self.http_initial,
                min: 1,
                max: self.http_max,
            },
            websocket: ConcurrencyLimits {
                initial: self.wss_initial,
                min: 1,
                max: self.wss_max,
            },
            aggregate_max: self.aggregate_max,
        }
    }
}

fn parse_bound_env(
    name: &str,
    default: usize,
    min: usize,
    max: usize,
) -> Result<usize, RealtimeError> {
    match std::env::var(name) {
        Err(_) => Ok(default),
        Ok(raw) => {
            let v: usize = raw.trim().parse().map_err(|_| {
                RealtimeError::Protocol(format!("responses config: {name} invalid integer"))
            })?;
            if v < min || v > max {
                return Err(RealtimeError::Protocol(format!(
                    "responses config: {name} must be in {min}..={max}"
                )));
            }
            Ok(v)
        }
    }
}

fn parse_wss_max_from_env() -> Result<(usize, bool), RealtimeError> {
    let wss_max_set = std::env::var("RTINFER_RESPONSES_WSS_MAX").ok();
    let capacity_set = std::env::var("RTINFER_RESPONSES_CAPACITY").ok();
    match (wss_max_set, capacity_set) {
        (Some(raw), Some(_)) => {
            warn!(
                "RTINFER_RESPONSES_WSS_MAX and RTINFER_RESPONSES_CAPACITY both set; using WSS_MAX"
            );
            let v: usize = raw.trim().parse().map_err(|_| {
                RealtimeError::Protocol(
                    "responses config: RTINFER_RESPONSES_WSS_MAX invalid integer".into(),
                )
            })?;
            if !(1..=WSS_HARD_MAX).contains(&v) {
                return Err(RealtimeError::Protocol(format!(
                    "responses config: RTINFER_RESPONSES_WSS_MAX must be in 1..={WSS_HARD_MAX}"
                )));
            }
            Ok((v, false))
        }
        (Some(raw), None) => {
            let v: usize = raw.trim().parse().map_err(|_| {
                RealtimeError::Protocol(
                    "responses config: RTINFER_RESPONSES_WSS_MAX invalid integer".into(),
                )
            })?;
            if !(1..=WSS_HARD_MAX).contains(&v) {
                return Err(RealtimeError::Protocol(format!(
                    "responses config: RTINFER_RESPONSES_WSS_MAX must be in 1..={WSS_HARD_MAX}"
                )));
            }
            Ok((v, false))
        }
        (None, Some(raw)) => {
            let v: usize = raw.trim().parse().map_err(|_| {
                RealtimeError::Protocol(
                    "responses config: RTINFER_RESPONSES_CAPACITY invalid integer".into(),
                )
            })?;
            if v == 0 {
                warn!(
                    "RTINFER_RESPONSES_CAPACITY=0 is deprecated; mapping to hard cap {WSS_HARD_MAX}"
                );
                Ok((WSS_HARD_MAX, true))
            } else if !(1..=WSS_HARD_MAX).contains(&v) {
                Err(RealtimeError::Protocol(format!(
                    "responses config: RTINFER_RESPONSES_CAPACITY must be in 1..={WSS_HARD_MAX}"
                )))
            } else {
                Ok((v, false))
            }
        }
        (None, None) => Ok((WSS_HARD_MAX, false)),
    }
}

// ---------------------------------------------------------------------------
// HTTP lane
// ---------------------------------------------------------------------------

struct CodexResponsesHttpLane {
    client: WarpsockClient,
    endpoint: String,
    auth: Arc<CodexAuthCache>,
    dispatches: AtomicU64,
}

impl CodexResponsesHttpLane {
    fn new(
        endpoint: String,
        http_max: usize,
        auth: Arc<CodexAuthCache>,
    ) -> Result<Self, RealtimeError> {
        let mut builder = WarpsockClient::builder()
            .fingerprint(FingerprintProfile::Chrome148)
            .prefer_http2(true)
            .h3_upgrade(false)
            .h2_direct_streaming_responses(false)
            .capacity_policy(CapacityPolicy::bounded(http_max.max(1)))
            .total_timeout(HTTP_TOTAL_TIMEOUT)
            .redirect_policy(RedirectPolicy::None)
            .user_agent(CODEX_RESPONSES_USER_AGENT);
        if endpoint.starts_with("http://") {
            builder = builder.http2_prior_knowledge(true);
        }
        let client = builder
            .build()
            .map_err(|e| RealtimeError::Protocol(format!("warpsock client build: {e}")))?;
        Ok(Self {
            client,
            endpoint,
            auth,
            dispatches: AtomicU64::new(0),
        })
    }

    fn connection_reuse_count(&self) -> usize {
        self.client.connection_reuse_count()
    }

    fn dispatch_count(&self) -> u64 {
        self.dispatches.load(Ordering::Relaxed)
    }

    async fn ask(&self, wire: &CodexResponsesWireRequest) -> AttemptOutcome {
        let auth = match self.auth.load().await {
            Ok(a) => a,
            Err(e) => {
                return AttemptOutcome::err(ResponsesResultClass::Failure, e, false, None);
            }
        };
        let reuse_before = self.client.connection_reuse_count();
        let headers = Headers::from_vec(build_http_headers(&auth, &wire.ids));
        let body = match serde_json::to_vec(&wire.body) {
            Ok(b) => b,
            Err(e) => {
                return AttemptOutcome::err(
                    ResponsesResultClass::Failure,
                    RealtimeError::Protocol(format!("encode http body: {e}")),
                    false,
                    None,
                );
            }
        };

        // SendAttempted: request-body write progress is not exposed.
        self.dispatches.fetch_add(1, Ordering::Relaxed);
        let send_result = self
            .client
            .post(&self.endpoint)
            .headers(headers)
            .body(body)
            .version(HttpVersion::Http2)
            .send_streaming()
            .await;

        let mut response = match send_result {
            Ok(r) => r,
            Err(e) => {
                return AttemptOutcome::err(
                    ResponsesResultClass::Indeterminate,
                    RealtimeError::Protocol(format!("http send: {e}")),
                    false,
                    None,
                );
            }
        };

        let status = response.status_code();
        let http_version = response.http_version().to_string();
        let reused = self.client.connection_reuse_count() > reuse_before;
        let retry_after = parse_retry_after_header(response.get_header("retry-after"));

        if status == 401 || status == 403 {
            drain_http_body(response.body_mut()).await;
            let refresh = self.auth.force_refresh_after(&auth.access_token).await;
            let mut outcome = AttemptOutcome::err(
                ResponsesResultClass::Failure,
                http_auth_rejection_error(refresh, status),
                false,
                None,
            );
            outcome.http_version = Some(http_version);
            outcome.http_reused = Some(reused);
            return outcome;
        }

        if status == 429 {
            let body_class = classify_http_error_body(&mut response).await;
            let (class, code) = match body_class {
                Some(ResponsesResultClass::LaneOverload) => {
                    (ResponsesResultClass::LaneOverload, "server_is_overloaded")
                }
                _ => (ResponsesResultClass::SharedThrottle, "rate_limit_exceeded"),
            };
            let mut outcome = AttemptOutcome::err(
                class,
                RealtimeError::Provider {
                    code: code.into(),
                    message: format!("http {status}"),
                },
                false,
                if class == ResponsesResultClass::SharedThrottle {
                    retry_after
                } else {
                    None
                },
            );
            outcome.http_version = Some(http_version);
            outcome.http_reused = Some(reused);
            return outcome;
        }

        if !(200..300).contains(&status) {
            let class = if let Some(c) = classify_http_error_body(&mut response).await {
                c
            } else {
                ResponsesResultClass::Failure
            };
            let mut outcome = AttemptOutcome::err(
                class,
                RealtimeError::Protocol(format!("http status {status}")),
                false,
                None,
            );
            outcome.http_version = Some(http_version);
            outcome.http_reused = Some(reused);
            return outcome;
        }

        if !is_http2_version(&http_version) {
            drain_http_body(response.body_mut()).await;
            let mut outcome = AttemptOutcome::err(
                ResponsesResultClass::Failure,
                RealtimeError::Protocol(format!(
                    "codex/responses expected HTTP/2, got {http_version}"
                )),
                false,
                None,
            );
            outcome.http_version = Some(http_version);
            outcome.http_reused = Some(reused);
            return outcome;
        }

        let content_type = response.content_type().map(str::to_owned);
        if !responses_content_type_allows_sse(content_type.as_deref()) {
            drain_http_body(response.body_mut()).await;
            let mut outcome = AttemptOutcome::err(
                ResponsesResultClass::Failure,
                RealtimeError::Protocol(format!(
                    "codex/responses expected text/event-stream, got {content_type:?}"
                )),
                false,
                None,
            );
            outcome.http_version = Some(http_version);
            outcome.http_reused = Some(reused);
            return outcome;
        }

        let mut decoder = SseDecoder::new();
        let mut body = response.into_body();
        loop {
            match body.chunk().await {
                None => break,
                Some(Ok(chunk)) => {
                    if let Err(e) = decoder.push(&chunk) {
                        drain_http_body(&mut body).await;
                        let mut outcome = decoder.assembler.attach_ttft(AttemptOutcome::err(
                            ResponsesResultClass::Indeterminate,
                            e,
                            false,
                            None,
                        ));
                        outcome.http_version = Some(http_version);
                        outcome.http_reused = Some(reused);
                        return outcome;
                    }
                    if decoder.completed || decoder.error.is_some() {
                        // Keep draining to release the stream cleanly, but stop parsing.
                        drain_http_body(&mut body).await;
                        break;
                    }
                }
                Some(Err(e)) => {
                    let terminal = decoder.assembler.terminal_seen;
                    let mut outcome = decoder.assembler.attach_ttft(AttemptOutcome::err(
                        ResponsesResultClass::Indeterminate,
                        RealtimeError::Protocol(format!("http body: {e}")),
                        terminal,
                        None,
                    ));
                    outcome.http_version = Some(http_version);
                    outcome.http_reused = Some(reused);
                    return outcome;
                }
            }
        }

        let mut outcome = decoder.finish();
        if outcome.class != ResponsesResultClass::Success && !outcome.terminal_seen {
            if outcome.class == ResponsesResultClass::Failure {
                // keep Failure from provider/empty completed
            } else if matches!(
                outcome.class,
                ResponsesResultClass::LaneOverload | ResponsesResultClass::SharedThrottle
            ) {
                // keep
            } else {
                outcome.class = ResponsesResultClass::Indeterminate;
            }
        }
        if let Some(err) = outcome.error.as_ref() {
            if matches!(err, RealtimeError::Provider { .. }) {
                outcome.class = classify_provider_error(err);
            }
        }
        outcome.http_version = Some(http_version);
        outcome.http_reused = Some(reused);
        outcome
    }
}

fn is_http2_version(version: &str) -> bool {
    let v = version.trim().to_ascii_lowercase();
    v == "http/2" || v == "http/2.0" || v == "h2" || v == "2" || v.starts_with("http/2")
}

fn parse_retry_after_header(raw: Option<&str>) -> Option<u64> {
    let raw = raw?.trim();
    raw.parse::<u64>().ok()
}

/// Continue draining only after a successful chunk; stop on EOF or transport error.
#[cfg(test)]
fn continue_http_body_drain<T, E>(chunk: &Option<Result<T, E>>) -> bool {
    matches!(chunk, Some(Ok(_)))
}

/// Drain remaining HTTP chunks only within a small byte/time budget. Dropping
/// this future (including caller cancellation) drops the body immediately, so
/// cleanup cannot outlive the logical request.
async fn drain_http_body(body: &mut warpsock::Body) {
    let deadline = tokio::time::Instant::now() + HTTP_BODY_DRAIN_TIMEOUT;
    let mut drained = 0usize;
    while drained < HTTP_BODY_DRAIN_LIMIT {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match timeout(remaining, body.chunk()).await {
            Ok(Some(Ok(chunk))) => drained = drained.saturating_add(chunk.len()),
            Ok(None | Some(Err(_))) | Err(_) => break,
        }
    }
}

/// Map a forced-refresh result after HTTP 401/403 into the Failure error that
/// production returns: preserve refresh failures verbatim; otherwise surface the
/// auth-rejected protocol error.
fn http_auth_rejection_error(
    refresh: Result<CodexAuth, RealtimeError>,
    status: u16,
) -> RealtimeError {
    match refresh {
        Err(e) => e,
        Ok(_) => RealtimeError::Protocol(format!("http auth rejected: {status}")),
    }
}

const HTTP_ERROR_BODY_LIMIT: usize = 4096;

/// Copy as much of `chunk` as fits under `limit`. Returns `true` when the
/// retained buffer is at capacity and the caller should stop reading.
fn append_bounded(dst: &mut Vec<u8>, chunk: &[u8], limit: usize) -> bool {
    let remaining = limit.saturating_sub(dst.len());
    if remaining == 0 {
        return true;
    }
    if chunk.len() >= remaining {
        dst.extend_from_slice(&chunk[..remaining]);
        true
    } else {
        dst.extend_from_slice(chunk);
        false
    }
}

async fn classify_http_error_body(
    response: &mut warpsock::Response,
) -> Option<ResponsesResultClass> {
    let mut bytes = Vec::new();
    let mut at_limit = false;
    while let Some(chunk) = response.body_mut().chunk().await {
        match chunk {
            Ok(c) => {
                if append_bounded(&mut bytes, &c, HTTP_ERROR_BODY_LIMIT) {
                    at_limit = true;
                    break;
                }
            }
            Err(_) => break,
        }
    }
    if at_limit {
        drain_http_body(response.body_mut()).await;
    }
    let v: Value = serde_json::from_slice(&bytes).ok()?;
    let code = v
        .pointer("/error/code")
        .or_else(|| v.pointer("/response/error/code"))
        .or_else(|| v.get("code"))
        .and_then(Value::as_str)?;
    Some(classify_provider_code(code))
}

// ---------------------------------------------------------------------------
// WSS lane
// ---------------------------------------------------------------------------

/// RAII guard that decrements `active_asks` exactly once on drop (including
/// cancellation). Created immediately after increment so abort cannot leak.
struct ActiveAskGuard<'a> {
    counter: &'a AtomicUsize,
}

impl<'a> ActiveAskGuard<'a> {
    fn enter(counter: &'a AtomicUsize) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self { counter }
    }
}

impl Drop for ActiveAskGuard<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

struct CodexResponsesWssPool {
    endpoint: String,
    auth: Arc<CodexAuthCache>,
    max_idle: usize,
    idle_sockets: Mutex<Vec<ResponsesSocket>>,
    dispatches: AtomicU64,
    handshake_attempts: AtomicU64,
    active_asks: AtomicUsize,
}

impl CodexResponsesWssPool {
    fn new(endpoint: String, max_idle: usize, auth: Arc<CodexAuthCache>) -> Self {
        Self {
            endpoint,
            auth,
            max_idle: max_idle.max(1),
            idle_sockets: Mutex::new(Vec::new()),
            dispatches: AtomicU64::new(0),
            handshake_attempts: AtomicU64::new(0),
            active_asks: AtomicUsize::new(0),
        }
    }

    async fn idle_len(&self) -> usize {
        self.idle_sockets.lock().await.len()
    }

    fn dispatch_count(&self) -> u64 {
        self.dispatches.load(Ordering::Relaxed)
    }

    fn handshake_attempts(&self) -> u64 {
        self.handshake_attempts.load(Ordering::Relaxed)
    }

    fn active_asks(&self) -> usize {
        self.active_asks.load(Ordering::Relaxed)
    }

    async fn checkout_socket(&self) -> Option<ResponsesSocket> {
        self.idle_sockets.lock().await.pop()
    }

    async fn checkin_socket(&self, ws: ResponsesSocket) {
        let mut idle = self.idle_sockets.lock().await;
        if idle.len() < self.max_idle {
            idle.push(ws);
        }
    }

    pub async fn prewarm(&self, count: usize) -> usize {
        let target = count.min(self.max_idle);
        for _ in 0..target {
            if self.idle_len().await >= target {
                break;
            }
            let auth = match self.auth.load().await {
                Ok(a) => a,
                Err(e) => {
                    debug!(error = %e, "codex/responses: prewarm auth unavailable; stopping");
                    return self.idle_len().await;
                }
            };
            match self.connect_socket(&auth).await {
                Ok(ws) => self.checkin_socket(ws).await,
                Err(e) => {
                    debug!(error = %e, "codex/responses: prewarm handshake failed; backing off");
                    tokio::time::sleep(Duration::from_millis(1_500 + (rand_like_jitter() % 3_000)))
                        .await;
                }
            }
        }
        self.idle_len().await
    }

    async fn connect_socket(&self, auth: &CodexAuth) -> Result<ResponsesSocket, RealtimeError> {
        install_rustls_provider();
        let _handshake_permit = HANDSHAKE_GATE
            .acquire()
            .await
            .map_err(|e| RealtimeError::Protocol(format!("handshake gate closed: {e}")))?;
        self.handshake_attempts.fetch_add(1, Ordering::Relaxed);
        let connection_ids = CodexRequestIds::fresh();
        let client_request = build_wss_handshake_request(&self.endpoint, auth, &connection_ids)?;
        let connect_fut = connect_async(client_request);
        match timeout(DEFAULT_HANDSHAKE_TIMEOUT, connect_fut).await {
            Ok(Ok((ws, _resp))) => Ok(ws),
            Ok(Err(e)) => Err(RealtimeError::Handshake(format!("{e}"))),
            Err(_) => Err(RealtimeError::Handshake("timeout".to_owned())),
        }
    }

    async fn acquire_socket(&self) -> Result<(ResponsesSocket, bool), RealtimeError> {
        if let Some(ws) = self.checkout_socket().await {
            return Ok((ws, true));
        }
        let mut refreshed = false;
        let mut auth = self.auth.load().await?;
        let mut last_err: Option<RealtimeError> = None;
        for attempt in 0u64..8 {
            if attempt > 0 {
                if let Some(ws) = self.checkout_socket().await {
                    return Ok((ws, true));
                }
            }
            match self.connect_socket(&auth).await {
                Ok(ws) => return Ok((ws, false)),
                Err(e) if is_auth_handshake_error(&e) => {
                    debug!(error = %e, attempt, "codex/responses: handshake rejected; backoff + retry");
                    if !refreshed {
                        auth = self.auth.force_refresh_after(&auth.access_token).await?;
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

    async fn ask(&self, wire: &CodexResponsesWireRequest) -> AttemptOutcome {
        let (mut ws, reused) = match self.acquire_socket().await {
            Ok(v) => v,
            Err(e) => {
                return AttemptOutcome::err(ResponsesResultClass::Failure, e, false, None);
            }
        };
        // Increment under an owned RAII guard so cancellation decrements exactly
        // once. On abort/drop the checked-out socket stays owned by this future
        // and is dropped without check-in; success-only check-in remains below.
        let _active = ActiveAskGuard::enter(&self.active_asks);
        self.dispatches.fetch_add(1, Ordering::Relaxed);
        let mut outcome = run_on_socket(&mut ws, &wire.wss_frame()).await;
        outcome.wss_reused = Some(reused);
        if outcome.class == ResponsesResultClass::Success {
            self.checkin_socket(ws).await;
        } else {
            drop(ws);
        }
        outcome
    }
}

/// True when an error is a handshake rejection that a token refresh might fix.
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

async fn run_on_socket(ws: &mut ResponsesSocket, request_frame: &Value) -> AttemptOutcome {
    let body = match serde_json::to_string(request_frame) {
        Ok(b) => b,
        Err(e) => {
            return AttemptOutcome::err(
                ResponsesResultClass::Failure,
                RealtimeError::Protocol(format!("encode wss frame: {e}")),
                false,
                None,
            );
        }
    };

    // DispatchPhase::SendAttempted: no post-send retry is permitted after this point.
    let _phase = DispatchPhase::SendAttempted;
    if let Err(e) = ws.send(Message::text(body)).await {
        return AttemptOutcome::err(
            ResponsesResultClass::Indeterminate,
            RealtimeError::Protocol(format!("ws send: {e}")),
            false,
            None,
        );
    }

    let mut asm = ResponsesAssembler::default();
    let deadline = WSS_TERMINAL_DEADLINE;
    let started = Instant::now();
    loop {
        let remaining = deadline.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return asm.attach_ttft(AttemptOutcome::err(
                ResponsesResultClass::Indeterminate,
                RealtimeError::Protocol(
                    "codex/responses wss timeout before response.completed".into(),
                ),
                asm.terminal_seen,
                None,
            ));
        }
        let frame = match timeout(remaining, ws.next()).await {
            Ok(Some(Ok(m))) => m,
            Ok(Some(Err(e))) => {
                return asm.attach_ttft(AttemptOutcome::err(
                    ResponsesResultClass::Indeterminate,
                    RealtimeError::Protocol(format!("ws read: {e}")),
                    asm.terminal_seen,
                    None,
                ));
            }
            Ok(None) => {
                return asm.attach_ttft(AttemptOutcome::err(
                    ResponsesResultClass::Indeterminate,
                    RealtimeError::Protocol("socket closed before response.completed".into()),
                    asm.terminal_seen,
                    None,
                ));
            }
            Err(_) => {
                return asm.attach_ttft(AttemptOutcome::err(
                    ResponsesResultClass::Indeterminate,
                    RealtimeError::Protocol(
                        "codex/responses wss timeout before response.completed".into(),
                    ),
                    asm.terminal_seen,
                    None,
                ));
            }
        };
        match frame {
            Message::Text(text) => match serde_json::from_str::<Value>(&text) {
                Ok(v) => match asm.on_frame(&v) {
                    FrameOutcome::Continue => {}
                    FrameOutcome::Completed => {
                        let _ = _phase;
                        return asm.finish_after_completed();
                    }
                    FrameOutcome::Error(e) => {
                        let class = classify_provider_error(&e);
                        let terminal = asm.terminal_seen;
                        return asm.attach_ttft(AttemptOutcome::err(class, e, terminal, None));
                    }
                },
                Err(e) => {
                    return asm.attach_ttft(AttemptOutcome::err(
                        ResponsesResultClass::Indeterminate,
                        RealtimeError::Protocol(format!("ws malformed JSON: {e}")),
                        asm.terminal_seen,
                        None,
                    ));
                }
            },
            Message::Ping(p) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Message::Close(_) => {
                return asm.attach_ttft(AttemptOutcome::err(
                    ResponsesResultClass::Indeterminate,
                    RealtimeError::Protocol("socket closed mid-ask".into()),
                    asm.terminal_seen,
                    None,
                ));
            }
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Coordinator
// ---------------------------------------------------------------------------

/// Observable coordinator state (non-sensitive).
#[derive(Debug, Clone)]
pub struct ResponsesClientSnapshot {
    pub mode: ResponsesTransportMode,
    pub adaptive: AdaptiveConcurrencySnapshot,
    pub http_connection_reuse_count: usize,
    pub http_dispatches: u64,
    pub wss_idle_sockets: usize,
    pub wss_dispatches: u64,
    pub wss_handshake_attempts: u64,
    pub wss_active_asks: usize,
    pub auth_generation: u64,
}

/// Builder for [`CodexResponsesClient`].
#[derive(Default)]
pub struct CodexResponsesClientBuilder {
    mode: Option<ResponsesTransportMode>,
    runtime: Option<ResponsesRuntimeConfig>,
    model: Option<String>,
    http_endpoint: Option<String>,
    wss_endpoint: Option<String>,
    auth_ttl: Option<Duration>,
    initial_auth: Option<CodexAuth>,
    auth_path: Option<PathBuf>,
    auth_source: Option<SharedCodexAuthSource>,
    installation_id: Option<String>,
    installation_id_path: Option<PathBuf>,
    /// Legacy capacity knob mapped onto wss_max for the pool wrapper.
    legacy_capacity: Option<usize>,
}

impl CodexResponsesClientBuilder {
    pub fn mode(mut self, mode: ResponsesTransportMode) -> Self {
        self.mode = Some(mode);
        self
    }
    pub fn runtime(mut self, runtime: ResponsesRuntimeConfig) -> Self {
        self.runtime = Some(runtime);
        self
    }
    pub fn model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }
    pub fn http_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.http_endpoint = Some(endpoint.into());
        self
    }
    pub fn wss_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.wss_endpoint = Some(endpoint.into());
        self
    }
    pub fn auth_ttl(mut self, ttl: Duration) -> Self {
        self.auth_ttl = Some(ttl);
        self
    }
    pub fn initial_auth(mut self, auth: CodexAuth) -> Self {
        self.initial_auth = Some(auth);
        self
    }
    pub fn auth_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.auth_path = Some(path.into());
        self
    }
    pub fn auth_source(mut self, source: SharedCodexAuthSource) -> Self {
        self.auth_source = Some(source);
        self
    }
    pub fn installation_id(mut self, id: impl Into<String>) -> Self {
        self.installation_id = Some(id.into());
        self
    }
    pub fn installation_id_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.installation_id_path = Some(path.into());
        self
    }

    pub fn build(self) -> Result<Arc<CodexResponsesClient>, RealtimeError> {
        // Mode-derived defaults only when no explicit runtime is supplied.
        // Explicit runtime is validated as-is (after optional mode override) —
        // never normalize aggregate or lane bounds.
        let explicit_runtime = self.runtime.is_some();
        let mut runtime = match self.runtime {
            Some(mut runtime) => {
                if let Some(mode) = self.mode {
                    runtime.mode = mode;
                }
                runtime
            }
            None => ResponsesRuntimeConfig::defaults_for_mode(
                self.mode.unwrap_or(ResponsesTransportMode::Wss),
            ),
        };
        if let Some(cap) = self.legacy_capacity {
            if explicit_runtime {
                return Err(RealtimeError::Protocol(
                    "responses config: legacy capacity cannot be combined with explicit runtime"
                        .into(),
                ));
            }
            // Legacy wrapper knob only: capacity 0 maps to the WSS hard cap.
            // All other values must already be in 1..=WSS_HARD_MAX — never clamp.
            runtime.wss_max = if cap == 0 {
                warn!("legacy capacity 0 mapped to WSS hard cap {WSS_HARD_MAX}");
                WSS_HARD_MAX
            } else if (1..=WSS_HARD_MAX).contains(&cap) {
                cap
            } else {
                return Err(RealtimeError::Protocol(format!(
                    "responses config: capacity must be in 1..={WSS_HARD_MAX}"
                )));
            };
            if runtime.mode == ResponsesTransportMode::Wss {
                runtime.aggregate_max = runtime.wss_max;
            }
        }
        runtime.validate()?;

        let installation_id =
            resolve_installation_id(self.installation_id, self.installation_id_path.as_deref())?;

        let mut auth_builder =
            CodexAuthCache::builder().auth_ttl(self.auth_ttl.unwrap_or(DEFAULT_AUTH_TTL));
        if let Some(auth) = self.initial_auth {
            auth_builder = auth_builder.initial_auth(auth);
        }
        if let Some(path) = self.auth_path {
            auth_builder = auth_builder.auth_path(path);
        }
        if let Some(source) = self.auth_source {
            auth_builder = auth_builder.auth_source(source);
        }
        let auth = auth_builder.build();

        let adaptive = AdaptiveConcurrency::new(runtime.adaptive_config())?;
        let http_endpoint = self
            .http_endpoint
            .unwrap_or_else(|| CODEX_RESPONSES_HTTP_URL.to_owned());
        let wss_endpoint = self
            .wss_endpoint
            .unwrap_or_else(|| CODEX_RESPONSES_URL.to_owned());

        let http = if matches!(
            runtime.mode,
            ResponsesTransportMode::Http | ResponsesTransportMode::Dual
        ) {
            Some(CodexResponsesHttpLane::new(
                http_endpoint,
                runtime.http_max,
                Arc::clone(&auth),
            )?)
        } else {
            None
        };
        let wss = if matches!(
            runtime.mode,
            ResponsesTransportMode::Wss | ResponsesTransportMode::Dual
        ) {
            Some(CodexResponsesWssPool::new(
                wss_endpoint,
                runtime.wss_max,
                Arc::clone(&auth),
            ))
        } else {
            None
        };

        Ok(Arc::new(CodexResponsesClient {
            mode: runtime.mode,
            model: self
                .model
                .unwrap_or_else(|| CODEX_RESPONSES_MODEL.to_owned()),
            installation_id,
            auth,
            adaptive,
            http,
            wss,
            runtime,
        }))
    }
}

/// Production entry point for Codex `/responses` asks.
pub struct CodexResponsesClient {
    mode: ResponsesTransportMode,
    model: String,
    installation_id: String,
    auth: Arc<CodexAuthCache>,
    adaptive: Arc<AdaptiveConcurrency>,
    http: Option<CodexResponsesHttpLane>,
    wss: Option<CodexResponsesWssPool>,
    runtime: ResponsesRuntimeConfig,
}

impl CodexResponsesClient {
    pub fn builder() -> CodexResponsesClientBuilder {
        CodexResponsesClientBuilder::default()
    }

    pub fn model(&self) -> &str {
        &self.model
    }

    pub fn mode(&self) -> ResponsesTransportMode {
        self.mode
    }

    pub fn installation_id(&self) -> &str {
        &self.installation_id
    }

    pub async fn snapshot(&self) -> ResponsesClientSnapshot {
        ResponsesClientSnapshot {
            mode: self.mode,
            adaptive: self.adaptive.snapshot(),
            http_connection_reuse_count: self
                .http
                .as_ref()
                .map(|h| h.connection_reuse_count())
                .unwrap_or(0),
            http_dispatches: self.http.as_ref().map(|h| h.dispatch_count()).unwrap_or(0),
            wss_idle_sockets: match &self.wss {
                Some(w) => w.idle_len().await,
                None => 0,
            },
            wss_dispatches: self.wss.as_ref().map(|w| w.dispatch_count()).unwrap_or(0),
            wss_handshake_attempts: self
                .wss
                .as_ref()
                .map(|w| w.handshake_attempts())
                .unwrap_or(0),
            wss_active_asks: self.wss.as_ref().map(|w| w.active_asks()).unwrap_or(0),
            auth_generation: self.auth.generation().await,
        }
    }

    /// Best-effort WSS idle prewarm. Returns the attained idle socket count.
    /// HTTP mode is a no-op that returns 0.
    pub async fn prewarm(&self, count: usize) -> usize {
        if self.mode == ResponsesTransportMode::Http {
            warn!("RTINFER_RESPONSES_PREWARM ignored in http transport mode");
            return 0;
        }
        if let Some(wss) = &self.wss {
            wss.prewarm(count.min(self.runtime.wss_max)).await
        } else {
            0
        }
    }

    pub async fn ask_text(
        &self,
        system: &str,
        user: &str,
        model: Option<&str>,
    ) -> Result<String, RealtimeError> {
        let model = model
            .filter(|m| !m.trim().is_empty())
            .unwrap_or(&self.model);
        let ids = CodexRequestIds::fresh();
        let wire = build_text_wire(model, system, user, ids, &self.installation_id);
        self.dispatch(wire).await?.into_result()
    }

    pub async fn ask_structured(
        &self,
        system: &str,
        user: &str,
        schema_name: &str,
        schema: Value,
    ) -> Result<Value, RealtimeError> {
        let mut schema = schema;
        require_all_object_properties_for_strict_schema(&mut schema);
        let ids = CodexRequestIds::fresh();
        let wire = build_structured_wire(
            &self.model,
            system,
            user,
            schema_name,
            schema,
            ids,
            &self.installation_id,
        );
        let text = self.dispatch(wire).await?.into_result()?;
        serde_json::from_str::<Value>(&text).map_err(|e| {
            RealtimeError::Protocol(format!("codex/responses output is not valid json: {e}"))
        })
    }

    /// Test/wire seam: build a text wire without dispatching.
    pub fn build_text_wire_for_test(&self, system: &str, user: &str) -> CodexResponsesWireRequest {
        build_text_wire(
            &self.model,
            system,
            user,
            CodexRequestIds::fresh(),
            &self.installation_id,
        )
    }

    /// Test/wire seam: build a structured wire without dispatching.
    pub fn build_structured_wire_for_test(
        &self,
        system: &str,
        user: &str,
        schema_name: &str,
        schema: Value,
    ) -> CodexResponsesWireRequest {
        let mut schema = schema;
        require_all_object_properties_for_strict_schema(&mut schema);
        build_structured_wire(
            &self.model,
            system,
            user,
            schema_name,
            schema,
            CodexRequestIds::fresh(),
            &self.installation_id,
        )
    }

    pub fn build_http_headers_for_test(
        &self,
        auth: &CodexAuth,
        ids: &CodexRequestIds,
    ) -> Vec<(String, String)> {
        build_http_headers(auth, ids)
    }

    pub fn build_wss_handshake_for_test(
        &self,
        auth: &CodexAuth,
        connection_ids: &CodexRequestIds,
    ) -> Result<Request<()>, RealtimeError> {
        let endpoint = self
            .wss
            .as_ref()
            .map(|w| w.endpoint.as_str())
            .unwrap_or(CODEX_RESPONSES_URL);
        build_wss_handshake_request(endpoint, auth, connection_ids)
    }

    async fn dispatch(
        &self,
        wire: CodexResponsesWireRequest,
    ) -> Result<AttemptOutcome, RealtimeError> {
        let admit_started = Instant::now();
        let lease = self.adaptive.acquire(self.mode.enabled_lanes()).await;
        let queue_wait_ms = admit_started.elapsed().as_millis();
        let work_started = Instant::now();
        let transport = lease.transport();
        // Capture before the transport future so cancellation Drop can emit
        // a real generation without awaiting.
        let auth_generation = self.auth.generation().await;
        debug!(
            mode = self.mode.as_str(),
            lane = ?transport,
            "codex/responses: dispatch"
        );
        let mut completion = DispatchCompletionGuard {
            client: self,
            lease: Some(lease),
            admit_started,
            queue_wait_ms,
            work_started,
            transport,
            auth_generation,
            finished: false,
        };
        let outcome = match transport {
            ResponsesTransportKind::Http => match &self.http {
                Some(http) => http.ask(&wire).await,
                None => AttemptOutcome::err(
                    ResponsesResultClass::Failure,
                    RealtimeError::Protocol("http lane disabled".into()),
                    false,
                    None,
                ),
            },
            ResponsesTransportKind::WebSocket => match &self.wss {
                Some(wss) => wss.ask(&wire).await,
                None => AttemptOutcome::err(
                    ResponsesResultClass::Failure,
                    RealtimeError::Protocol("wss lane disabled".into()),
                    false,
                    None,
                ),
            },
        };
        Ok(completion.finish(outcome))
    }
}

/// RAII owner for one admitted logical request's completion telemetry.
///
/// On normal completion, [`finish`](Self::finish) releases the lease then emits
/// exactly one event. On cancellation (Drop without finish), the lease is
/// released first (recording a cancellation and restoring counters), then one
/// `cancelled` event is emitted. Never check-ins a WSS socket.
struct DispatchCompletionGuard<'a> {
    client: &'a CodexResponsesClient,
    lease: Option<AdaptiveLease>,
    admit_started: Instant,
    queue_wait_ms: u128,
    work_started: Instant,
    transport: ResponsesTransportKind,
    auth_generation: u64,
    finished: bool,
}

impl DispatchCompletionGuard<'_> {
    fn lane_str(&self) -> &'static str {
        match self.transport {
            ResponsesTransportKind::Http => "http",
            ResponsesTransportKind::WebSocket => "wss",
        }
    }

    fn post_release_bounds(&self) -> (usize, usize, usize, usize) {
        let snap = self.client.adaptive.snapshot();
        let (lane_limit, lane_in_flight) = match self.transport {
            ResponsesTransportKind::Http => (snap.http.limit, snap.http.in_flight),
            ResponsesTransportKind::WebSocket => (snap.websocket.limit, snap.websocket.in_flight),
        };
        (
            lane_limit,
            lane_in_flight,
            snap.aggregate.limit,
            snap.aggregate.in_flight,
        )
    }

    fn wss_handshake_attempts_if_applicable(&self) -> Option<u64> {
        match self.transport {
            ResponsesTransportKind::WebSocket => Some(
                self.client
                    .wss
                    .as_ref()
                    .map(|w| w.handshake_attempts())
                    .unwrap_or(0),
            ),
            ResponsesTransportKind::Http => None,
        }
    }

    fn finish(&mut self, outcome: AttemptOutcome) -> AttemptOutcome {
        if let Some(lease) = self.lease.take() {
            lease.finish(outcome.adaptive_outcome(), self.work_started.elapsed());
        }
        let total_ms = self.admit_started.elapsed().as_millis();
        let (lane_limit, lane_in_flight, aggregate_limit, aggregate_in_flight) =
            self.post_release_bounds();
        let is_http = matches!(self.transport, ResponsesTransportKind::Http);
        emit_responses_completion(&ResponsesCompletionRecord {
            mode: self.client.mode.as_str(),
            lane: self.lane_str(),
            queue_wait_ms: self.queue_wait_ms,
            total_ms,
            terminal_seen: outcome.terminal_seen,
            result_class: outcome.class.as_str(),
            lane_limit,
            lane_in_flight,
            aggregate_limit,
            aggregate_in_flight,
            auth_generation: self.auth_generation,
            ttft_ms: outcome.ttft_ms,
            http_version: if is_http {
                outcome.http_version.clone()
            } else {
                None
            },
            http_reused: if is_http { outcome.http_reused } else { None },
            wss_reused: if is_http { None } else { outcome.wss_reused },
            wss_handshake_attempts: self.wss_handshake_attempts_if_applicable(),
        });
        self.finished = true;
        outcome
    }
}

impl Drop for DispatchCompletionGuard<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // Cancellation path: release lease first so counters/cancellations settle,
        // then emit exactly one cancelled completion record.
        if let Some(lease) = self.lease.take() {
            drop(lease);
        }
        let total_ms = self.admit_started.elapsed().as_millis();
        let (lane_limit, lane_in_flight, aggregate_limit, aggregate_in_flight) =
            self.post_release_bounds();
        emit_responses_completion(&ResponsesCompletionRecord {
            mode: self.client.mode.as_str(),
            lane: self.lane_str(),
            queue_wait_ms: self.queue_wait_ms,
            total_ms,
            terminal_seen: false,
            result_class: "cancelled",
            lane_limit,
            lane_in_flight,
            aggregate_limit,
            aggregate_in_flight,
            auth_generation: self.auth_generation,
            ttft_ms: None,
            // Reuse/version are only known after a finished transport attempt.
            http_version: None,
            http_reused: None,
            wss_reused: None,
            wss_handshake_attempts: self.wss_handshake_attempts_if_applicable(),
        });
        self.finished = true;
    }
}

/// Bounded, secret-free completion fields for one logical ask (including cancel).
#[derive(Debug, Clone)]
struct ResponsesCompletionRecord {
    mode: &'static str,
    lane: &'static str,
    queue_wait_ms: u128,
    total_ms: u128,
    terminal_seen: bool,
    result_class: &'static str,
    lane_limit: usize,
    lane_in_flight: usize,
    aggregate_limit: usize,
    aggregate_in_flight: usize,
    auth_generation: u64,
    ttft_ms: Option<u128>,
    http_version: Option<String>,
    http_reused: Option<bool>,
    wss_reused: Option<bool>,
    wss_handshake_attempts: Option<u64>,
}

/// Emit exactly one completion tracing event. Optional transport/TTFT fields use
/// `Option` so inapplicable values are absent (tracing skips `None`).
fn emit_responses_completion(record: &ResponsesCompletionRecord) {
    // Never log prompt, instructions, output, schema, provider message,
    // token/account/path, or raw frames.
    info!(
        target: "rtinfer_core::responses_completion",
        mode = record.mode,
        lane = record.lane,
        queue_wait_ms = record.queue_wait_ms as u64,
        total_ms = record.total_ms as u64,
        terminal_seen = record.terminal_seen,
        result_class = record.result_class,
        lane_limit = record.lane_limit,
        lane_in_flight = record.lane_in_flight,
        aggregate_limit = record.aggregate_limit,
        aggregate_in_flight = record.aggregate_in_flight,
        auth_generation = record.auth_generation,
        ttft_ms = record.ttft_ms.map(|v| v as u64),
        http_version = record.http_version.as_deref(),
        http_reused = record.http_reused,
        wss_reused = record.wss_reused,
        wss_handshake_attempts = record.wss_handshake_attempts,
        "responses_completion"
    );
}

// ---------------------------------------------------------------------------
// Compatibility wrapper for existing daemon callers
// ---------------------------------------------------------------------------

/// Temporary compatibility wrapper. New code should use [`CodexResponsesClient`].
#[derive(Default)]
pub struct CodexResponsesPoolBuilder {
    inner: CodexResponsesClientBuilder,
}

impl CodexResponsesPoolBuilder {
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.inner = self.inner.wss_endpoint(endpoint);
        self
    }
    pub fn model(mut self, model: impl Into<String>) -> Self {
        self.inner = self.inner.model(model);
        self
    }
    pub fn capacity(mut self, cap: usize) -> Self {
        self.inner.legacy_capacity = Some(cap);
        self
    }
    pub fn auth_ttl(mut self, ttl: Duration) -> Self {
        self.inner = self.inner.auth_ttl(ttl);
        self
    }
    pub fn initial_auth(mut self, auth: CodexAuth) -> Self {
        self.inner = self.inner.initial_auth(auth);
        self
    }
    pub fn auth_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.auth_path(path);
        self
    }
    pub fn auth_source(mut self, source: SharedCodexAuthSource) -> Self {
        self.inner = self.inner.auth_source(source);
        self
    }

    pub fn build(self) -> Arc<CodexResponsesPool> {
        let mut inner = self.inner;
        if inner.mode.is_none() {
            inner = inner.mode(ResponsesTransportMode::Wss);
        }
        let client = inner.build().unwrap_or_else(|e| {
            panic!("CodexResponsesPool build failed: {e}");
        });
        Arc::new(CodexResponsesPool { inner: client })
    }
}

/// Compatibility facade over [`CodexResponsesClient`] (WSS-default).
pub struct CodexResponsesPool {
    inner: Arc<CodexResponsesClient>,
}

impl CodexResponsesPool {
    pub fn builder() -> CodexResponsesPoolBuilder {
        CodexResponsesPoolBuilder::default()
    }

    pub fn endpoint(&self) -> &str {
        self.inner
            .wss
            .as_ref()
            .map(|w| w.endpoint.as_str())
            .unwrap_or(CODEX_RESPONSES_URL)
    }

    pub fn model(&self) -> &str {
        self.inner.model()
    }

    pub fn in_flight(&self) -> usize {
        self.inner.adaptive.snapshot().aggregate.in_flight
    }

    pub async fn invalidate_auth(&self) {
        self.inner.auth.invalidate().await;
    }

    pub async fn ask_structured(
        &self,
        system: &str,
        user: &str,
        schema_name: &str,
        schema: Value,
    ) -> Result<Value, RealtimeError> {
        self.inner
            .ask_structured(system, user, schema_name, schema)
            .await
    }

    pub async fn ask_text(
        &self,
        system: &str,
        user: &str,
        model: Option<&str>,
    ) -> Result<String, RealtimeError> {
        self.inner.ask_text(system, user, model).await
    }

    pub async fn prewarm(&self, count: usize) {
        self.inner.prewarm(count).await;
    }

    /// Exposed for unit tests that previously called into the pool auth cache.
    pub async fn fresh_auth_for_test(&self) -> Result<CodexAuth, RealtimeError> {
        self.inner.auth.load().await
    }

    pub async fn force_refresh_after_for_test(
        &self,
        stale_access_token: &str,
    ) -> Result<CodexAuth, RealtimeError> {
        self.inner
            .auth
            .force_refresh_after(stale_access_token)
            .await
    }
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

        let loaded = pool.fresh_auth_for_test().await.unwrap();
        let forced = pool
            .force_refresh_after_for_test(&loaded.access_token)
            .await
            .unwrap();

        assert_eq!(forced.access_token, "forced-access");
        assert_eq!(
            source.rejected.lock().unwrap().as_slice(),
            ["loaded-access"]
        );
    }

    #[tokio::test]
    async fn drain_http_body_consumes_buffered_response() {
        let response = warpsock::Response::new(
            200,
            Headers::new(),
            bytes::Bytes::from(vec![1u8, 2, 3, 4, 5]),
            "HTTP/2".into(),
        );
        let mut body = response.into_body();
        drain_http_body(&mut body).await;
        assert!(
            body.chunk().await.is_none(),
            "drain_http_body must leave no remaining chunks"
        );
    }

    #[test]
    fn continue_http_body_drain_stops_on_eof_or_transport_error() {
        assert!(continue_http_body_drain(&Some(Ok::<_, &str>(
            bytes::Bytes::from_static(b"x")
        ))));
        assert!(!continue_http_body_drain(
            &None::<Result<bytes::Bytes, &str>>
        ));
        assert!(!continue_http_body_drain(&Some(Err::<bytes::Bytes, _>(
            "transport"
        ))));
    }

    #[test]
    fn http_auth_rejection_preserves_refresh_and_protocol() {
        let refresh_err = RealtimeError::Refresh("token endpoint returned HTTP 500".into());
        let err = http_auth_rejection_error(Err(refresh_err), 401);
        assert!(
            matches!(err, RealtimeError::Refresh(_)),
            "failed forced refresh must remain Refresh, got {err:?}"
        );

        let err = http_auth_rejection_error(Ok(source_auth("rotated")), 403);
        match err {
            RealtimeError::Protocol(msg) => {
                assert_eq!(msg, "http auth rejected: 403");
            }
            other => panic!("successful refresh must keep Protocol auth rejection, got {other:?}"),
        }
    }

    #[test]
    fn append_bounded_caps_oversized_chunk_at_4096() {
        let mut retained = Vec::new();
        let oversized = vec![0x41u8; HTTP_ERROR_BODY_LIMIT + 1024];
        let stop = append_bounded(&mut retained, &oversized, HTTP_ERROR_BODY_LIMIT);
        assert!(stop, "oversized chunk must signal stop");
        assert_eq!(retained.len(), HTTP_ERROR_BODY_LIMIT);
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
    fn assembler_requires_completed_and_prefers_done_text() {
        let frames = vec![
            json!({"type":"response.output_text.delta","delta":"par"}),
            json!({"type":"response.output_text.done","text":"full"}),
            json!({"type":"response.completed","response":{"output":[]}}),
        ];
        assert_eq!(assemble_codex_responses_text(&frames).unwrap(), "full");
    }

    #[test]
    fn assembler_rejects_incomplete_stream() {
        let frames = vec![
            json!({"type":"response.output_text.delta","delta":"ab"}),
            json!({"type":"response.output_text.done","text":"ab"}),
        ];
        let err = assemble_codex_responses_text(&frames).unwrap_err();
        assert_eq!(err.code_or_label(), "protocol");
    }

    #[test]
    fn auth_handshake_error_matches_403_and_401() {
        assert!(is_auth_handshake_error(&RealtimeError::Handshake(
            "HTTP error: 403 Forbidden".into()
        )));
        assert!(is_auth_handshake_error(&RealtimeError::Handshake(
            "401 Unauthorized".into()
        )));
        assert!(!is_auth_handshake_error(&RealtimeError::Handshake(
            "timeout".into()
        )));
        assert!(!is_auth_handshake_error(&RealtimeError::Protocol(
            "403 in a protocol frame".into()
        )));
    }

    #[test]
    fn text_wire_always_requests_priority_tier_without_type() {
        let ids = CodexRequestIds::fresh();
        let wire = build_text_wire(
            "gpt-5.4",
            "sys",
            "user",
            ids,
            "11111111-1111-4111-8111-111111111111",
        );
        assert_eq!(wire.body["service_tier"], "priority");
        assert!(wire.body.get("type").is_none());
        assert_eq!(wire.wss_frame()["type"], "response.create");
        assert_eq!(wire.body["model"], "gpt-5.4");
        assert_eq!(wire.body["reasoning"]["effort"], "low");
        assert_eq!(wire.body["text"]["verbosity"], "low");
        assert_eq!(wire.body["stream"], true);
        assert_eq!(wire.body["store"], false);
        assert_eq!(wire.body["tools"], json!([]));
        assert!(wire.body.get("max_output_tokens").is_none());
        assert!(wire.body.get("previous_response_id").is_none());
        assert!(wire.body.get("generate").is_none());
    }

    #[test]
    fn structured_wire_always_requests_priority_tier() {
        let schema = json!({"type":"object","properties":{"a":{"type":"string"}}});
        let ids = CodexRequestIds::fresh();
        let wire = build_structured_wire(
            "gpt-5.5",
            "sys",
            "user",
            "result",
            schema,
            ids,
            "11111111-1111-4111-8111-111111111111",
        );
        assert_eq!(wire.body["service_tier"], "priority");
        assert!(wire.body.get("type").is_none());
        assert_eq!(wire.wss_frame()["type"], "response.create");
        assert_eq!(wire.body["text"]["format"]["type"], "json_schema");
        assert_eq!(wire.body["text"]["format"]["strict"], true);
        assert_eq!(wire.body["text"]["format"]["name"], "result");
    }

    #[test]
    fn classify_provider_codes_are_exact() {
        assert_eq!(
            classify_provider_code("server_is_overloaded"),
            ResponsesResultClass::LaneOverload
        );
        assert_eq!(
            classify_provider_code("slow_down"),
            ResponsesResultClass::LaneOverload
        );
        assert_eq!(
            classify_provider_code("websocket_connection_limit_reached"),
            ResponsesResultClass::LaneOverload
        );
        assert_eq!(
            classify_provider_code("rate_limit_exceeded"),
            ResponsesResultClass::SharedThrottle
        );
        assert_eq!(
            classify_provider_code("invalid_request"),
            ResponsesResultClass::Failure
        );
    }

    #[test]
    fn user_agent_has_no_reqwest() {
        assert!(!CODEX_RESPONSES_USER_AGENT
            .to_ascii_lowercase()
            .contains("reqwest"));
        assert!(CODEX_RESPONSES_USER_AGENT.contains("codex-tui"));
    }

    fn expect_config_build_err(
        result: Result<Arc<CodexResponsesClient>, RealtimeError>,
    ) -> RealtimeError {
        match result {
            Ok(_) => panic!("expected responses config construction failure"),
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("responses config:"),
                    "expected responses config error, got {msg}"
                );
                e
            }
        }
    }

    #[test]
    fn builder_rejects_invalid_wss_config() {
        expect_config_build_err(
            CodexResponsesClient::builder()
                .runtime(ResponsesRuntimeConfig {
                    mode: ResponsesTransportMode::Wss,
                    http_initial: 8,
                    http_max: 256,
                    wss_initial: 8,
                    wss_max: 4,
                    aggregate_max: 4,
                    prewarm: 0,
                })
                .installation_id("11111111-1111-4111-8111-111111111111")
                .build(),
        );
    }

    #[test]
    fn builder_rejects_invalid_http_config() {
        expect_config_build_err(
            CodexResponsesClient::builder()
                .runtime(ResponsesRuntimeConfig {
                    mode: ResponsesTransportMode::Http,
                    http_initial: 0,
                    http_max: 16,
                    wss_initial: 1,
                    wss_max: 1,
                    aggregate_max: 16,
                    prewarm: 0,
                })
                .installation_id("11111111-1111-4111-8111-111111111111")
                .build(),
        );
    }

    #[test]
    fn builder_rejects_invalid_dual_aggregate() {
        expect_config_build_err(
            CodexResponsesClient::builder()
                .runtime(ResponsesRuntimeConfig {
                    mode: ResponsesTransportMode::Dual,
                    http_initial: 2,
                    http_max: 8,
                    wss_initial: 2,
                    wss_max: 4,
                    aggregate_max: 1,
                    prewarm: 0,
                })
                .installation_id("11111111-1111-4111-8111-111111111111")
                .build(),
        );

        expect_config_build_err(
            CodexResponsesClient::builder()
                .runtime(ResponsesRuntimeConfig {
                    mode: ResponsesTransportMode::Dual,
                    http_initial: 2,
                    http_max: 8,
                    wss_initial: 2,
                    wss_max: 4,
                    aggregate_max: 100,
                    prewarm: 0,
                })
                .installation_id("11111111-1111-4111-8111-111111111111")
                .build(),
        );
    }

    #[test]
    fn builder_rejects_prewarm_above_wss_max() {
        expect_config_build_err(
            CodexResponsesClient::builder()
                .runtime(ResponsesRuntimeConfig {
                    mode: ResponsesTransportMode::Wss,
                    http_initial: 8,
                    http_max: 256,
                    wss_initial: 4,
                    wss_max: 4,
                    aggregate_max: 4,
                    prewarm: 5,
                })
                .installation_id("11111111-1111-4111-8111-111111111111")
                .build(),
        );
    }

    #[test]
    fn legacy_capacity_rejects_out_of_bounds_without_clamping() {
        ResponsesRuntimeConfig {
            mode: ResponsesTransportMode::Wss,
            http_initial: 8,
            http_max: 256,
            wss_initial: 4,
            wss_max: 65,
            aggregate_max: 64,
            prewarm: 0,
        }
        .validate()
        .expect_err("wss_max above hard bound");

        expect_config_build_err(
            CodexResponsesClientBuilder {
                legacy_capacity: Some(65),
                ..Default::default()
            }
            .installation_id("11111111-1111-4111-8111-111111111111")
            .auth_source(Arc::new(RecordingSource {
                rejected: StdMutex::new(Vec::new()),
            }))
            .build(),
        );
    }

    #[test]
    fn legacy_capacity_does_not_normalize_an_explicit_runtime_aggregate() {
        // A manually supplied runtime is a complete contract. The legacy
        // wrapper must not make an otherwise-invalid aggregate valid by
        // overwriting it to match its WSS capacity.
        expect_config_build_err(
            CodexResponsesClientBuilder {
                runtime: Some(ResponsesRuntimeConfig {
                    mode: ResponsesTransportMode::Wss,
                    http_initial: 8,
                    http_max: 256,
                    wss_initial: 4,
                    wss_max: 4,
                    aggregate_max: 64,
                    prewarm: 0,
                }),
                legacy_capacity: Some(4),
                ..Default::default()
            }
            .installation_id("11111111-1111-4111-8111-111111111111")
            .auth_source(Arc::new(RecordingSource {
                rejected: StdMutex::new(Vec::new()),
            }))
            .build(),
        );
    }

    #[tokio::test]
    async fn builder_mode_defaults_aggregate_enabled_sum() {
        let auth = Arc::new(RecordingSource {
            rejected: StdMutex::new(Vec::new()),
        });
        let install = "11111111-1111-4111-8111-111111111111";

        let wss = CodexResponsesClient::builder()
            .mode(ResponsesTransportMode::Wss)
            .installation_id(install)
            .auth_source(auth.clone())
            .build()
            .expect("wss defaults");
        assert_eq!(wss.snapshot().await.adaptive.aggregate.limit, 64);

        let http = CodexResponsesClient::builder()
            .mode(ResponsesTransportMode::Http)
            .installation_id(install)
            .auth_source(auth.clone())
            .build()
            .expect("http defaults");
        assert_eq!(http.snapshot().await.adaptive.aggregate.limit, 256);

        let dual = CodexResponsesClient::builder()
            .mode(ResponsesTransportMode::Dual)
            .installation_id(install)
            .auth_source(auth.clone())
            .build()
            .expect("dual defaults");
        assert_eq!(dual.snapshot().await.adaptive.aggregate.limit, 320);

        // Explicit runtime above HTTP enabled sum must reject — never normalize.
        expect_config_build_err(
            CodexResponsesClient::builder()
                .mode(ResponsesTransportMode::Http)
                .runtime(ResponsesRuntimeConfig {
                    mode: ResponsesTransportMode::Http,
                    http_initial: 8,
                    http_max: 256,
                    wss_initial: 4,
                    wss_max: 64,
                    aggregate_max: 300,
                    prewarm: 0,
                })
                .installation_id(install)
                .build(),
        );
    }

    #[tokio::test]
    async fn active_ask_guard_decrements_on_cancel() {
        let counter = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&counter);
        let handle = tokio::spawn(async move {
            let _guard = ActiveAskGuard::enter(c.as_ref());
            std::future::pending::<()>().await;
        });
        tokio::task::yield_now().await;
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        handle.abort();
        let _ = handle.await;
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    // -----------------------------------------------------------------------
    // Observability: minimal in-crate tracing capture (no new dependency)
    // -----------------------------------------------------------------------

    type CapturedField = (String, String);
    type CapturedEvent = Vec<CapturedField>;
    type CapturedEvents = Vec<CapturedEvent>;

    #[derive(Default)]
    struct FieldCollector {
        fields: CapturedEvent,
    }

    impl tracing::field::Visit for FieldCollector {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.fields
                .push((field.name().to_string(), format!("{value:?}")));
        }
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.fields
                .push((field.name().to_string(), value.to_string()));
        }
        fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
            self.fields
                .push((field.name().to_string(), value.to_string()));
        }
        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            self.fields
                .push((field.name().to_string(), value.to_string()));
        }
        fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
            self.fields
                .push((field.name().to_string(), value.to_string()));
        }
        fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
            self.fields
                .push((field.name().to_string(), value.to_string()));
        }
    }

    #[derive(Clone, Default)]
    struct CaptureSubscriber {
        events: Arc<StdMutex<CapturedEvents>>,
    }

    impl tracing::Subscriber for CaptureSubscriber {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            metadata.target() == "rtinfer_core::responses_completion"
        }
        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            if event.metadata().target() != "rtinfer_core::responses_completion" {
                return;
            }
            let mut collector = FieldCollector::default();
            event.record(&mut collector);
            self.events.lock().unwrap().push(collector.fields);
        }
        fn enter(&self, _span: &tracing::span::Id) {}
        fn exit(&self, _span: &tracing::span::Id) {}
    }

    fn capture_completion(record: &ResponsesCompletionRecord) -> CapturedEvent {
        let sub = CaptureSubscriber::default();
        let events = Arc::clone(&sub.events);
        tracing::subscriber::with_default(sub, || {
            emit_responses_completion(record);
        });
        let mut guard = events.lock().unwrap();
        assert_eq!(guard.len(), 1, "expected exactly one completion event");
        guard.pop().unwrap()
    }

    fn field_map(fields: &[CapturedField]) -> std::collections::BTreeMap<&str, &str> {
        fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect()
    }

    fn assert_required_fields(map: &std::collections::BTreeMap<&str, &str>) {
        for key in [
            "mode",
            "lane",
            "queue_wait_ms",
            "total_ms",
            "terminal_seen",
            "result_class",
            "lane_limit",
            "lane_in_flight",
            "aggregate_limit",
            "aggregate_in_flight",
            "auth_generation",
        ] {
            assert!(map.contains_key(key), "missing required field {key}");
        }
    }

    #[test]
    fn responses_observability_trace_has_contract_fields_without_content() {
        let completed = ResponsesCompletionRecord {
            mode: "dual",
            lane: "http",
            queue_wait_ms: 3,
            total_ms: 40,
            terminal_seen: true,
            result_class: "success",
            lane_limit: 8,
            lane_in_flight: 0,
            aggregate_limit: 12,
            aggregate_in_flight: 0,
            auth_generation: 1,
            ttft_ms: Some(12),
            http_version: Some("HTTP/2.0".into()),
            http_reused: Some(true),
            wss_reused: None,
            wss_handshake_attempts: None,
        };
        let fields = capture_completion(&completed);
        let map = field_map(&fields);
        assert_required_fields(&map);
        assert_eq!(map.get("result_class"), Some(&"success"));
        assert_eq!(map.get("ttft_ms"), Some(&"12"));
        assert_eq!(map.get("http_version"), Some(&"HTTP/2.0"));
        assert_eq!(map.get("http_reused"), Some(&"true"));
        assert!(!map.contains_key("wss_reused"));
        assert!(!map.contains_key("wss_handshake_attempts"));

        let failed = ResponsesCompletionRecord {
            mode: "wss",
            lane: "wss",
            queue_wait_ms: 0,
            total_ms: 9,
            terminal_seen: true,
            result_class: "failure",
            lane_limit: 4,
            lane_in_flight: 0,
            aggregate_limit: 4,
            aggregate_in_flight: 0,
            auth_generation: 2,
            ttft_ms: None,
            http_version: None,
            http_reused: None,
            wss_reused: Some(false),
            wss_handshake_attempts: Some(1),
        };
        let fields = capture_completion(&failed);
        let map = field_map(&fields);
        assert_required_fields(&map);
        assert_eq!(map.get("result_class"), Some(&"failure"));
        assert!(!map.contains_key("ttft_ms"));
        assert!(!map.contains_key("http_version"));
        assert!(!map.contains_key("http_reused"));
        assert_eq!(map.get("wss_reused"), Some(&"false"));
        assert_eq!(map.get("wss_handshake_attempts"), Some(&"1"));

        let indeterminate = ResponsesCompletionRecord {
            mode: "http",
            lane: "http",
            queue_wait_ms: 1,
            total_ms: 50,
            terminal_seen: false,
            result_class: "indeterminate",
            lane_limit: 8,
            lane_in_flight: 1,
            aggregate_limit: 8,
            aggregate_in_flight: 1,
            auth_generation: 3,
            ttft_ms: Some(5),
            http_version: Some("HTTP/2".into()),
            http_reused: Some(false),
            wss_reused: None,
            wss_handshake_attempts: None,
        };
        let fields = capture_completion(&indeterminate);
        let map = field_map(&fields);
        assert_required_fields(&map);
        assert_eq!(map.get("result_class"), Some(&"indeterminate"));
        assert_eq!(map.get("terminal_seen"), Some(&"false"));
    }

    #[test]
    fn responses_observability_redacts_sensitive_sentinels() {
        const SENTINELS: &[&str] = &[
            "SECRET_PROMPT_SENTINEL_9f3a",
            "SECRET_INSTRUCTIONS_SENTINEL_7c2b",
            "SECRET_OUTPUT_SENTINEL_1d4e",
            "SECRET_SCHEMA_SENTINEL_5a8c",
            "SECRET_PROVIDER_MSG_SENTINEL_3b6d",
            "SECRET_BEARER_TOKEN_SENTINEL_sk-test",
            "SECRET_ACCOUNT_ID_SENTINEL_acct",
            "SECRET_AUTH_PATH_SENTINEL_/tmp/auth.json",
        ];
        let record = ResponsesCompletionRecord {
            mode: "dual",
            lane: "wss",
            queue_wait_ms: 2,
            total_ms: 33,
            terminal_seen: true,
            result_class: "success",
            lane_limit: 4,
            lane_in_flight: 0,
            aggregate_limit: 8,
            aggregate_in_flight: 0,
            auth_generation: 9,
            ttft_ms: Some(7),
            http_version: None,
            http_reused: None,
            wss_reused: Some(true),
            wss_handshake_attempts: Some(2),
        };
        let fields = capture_completion(&record);
        let rendered = fields
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(" ");
        for sentinel in SENTINELS {
            assert!(
                !rendered.contains(sentinel),
                "completion event leaked sentinel {sentinel}: {rendered}"
            );
        }
        // Positive control: required non-sensitive fields are present.
        let map = field_map(&fields);
        assert_required_fields(&map);
        assert_eq!(map.get("auth_generation"), Some(&"9"));
    }

    #[tokio::test]
    async fn responses_observability_cancelled_emits_one_event_and_settles() {
        // Accept TCP then hang so the admitted ask blocks in handshake until aborted.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind hang listener");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            loop {
                let Ok((_sock, _)) = listener.accept().await else {
                    break;
                };
                std::future::pending::<()>().await;
            }
        });

        let client = CodexResponsesClient::builder()
            .mode(ResponsesTransportMode::Wss)
            .runtime(ResponsesRuntimeConfig {
                mode: ResponsesTransportMode::Wss,
                http_initial: 1,
                http_max: 1,
                wss_initial: 1,
                wss_max: 1,
                aggregate_max: 1,
                prewarm: 0,
            })
            .wss_endpoint(format!("ws://{addr}/"))
            .installation_id("11111111-1111-4111-8111-111111111111")
            .auth_source(Arc::new(RecordingSource {
                rejected: StdMutex::new(Vec::new()),
            }))
            .build()
            .expect("hang client");

        let sub = CaptureSubscriber::default();
        let events = Arc::clone(&sub.events);
        let dispatch = tracing::dispatcher::Dispatch::new(sub);
        let _default = tracing::dispatcher::set_default(&dispatch);

        let client_task = Arc::clone(&client);
        let handle = tokio::spawn(async move { client_task.ask_text("s", "u", None).await });

        timeout(Duration::from_secs(2), async {
            loop {
                let snap = client.snapshot().await;
                if snap.adaptive.aggregate.in_flight >= 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("request was never admitted");

        handle.abort();
        let _ = handle.await;
        // Allow the cancelled task's Drop glue (lease release + emit) to run.
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(20)).await;

        let captured = events.lock().unwrap().clone();
        assert_eq!(
            captured.len(),
            1,
            "expected exactly one completion event on cancel, got {captured:?}"
        );
        let map = field_map(&captured[0]);
        assert_required_fields(&map);
        assert_eq!(map.get("result_class"), Some(&"cancelled"));
        assert_eq!(map.get("terminal_seen"), Some(&"false"));
        assert_eq!(map.get("mode"), Some(&"wss"));
        assert_eq!(map.get("lane"), Some(&"wss"));
        assert!(map.contains_key("auth_generation"));
        assert!(!map.contains_key("http_version"));
        assert!(!map.contains_key("http_reused"));
        assert!(!map.contains_key("wss_reused"));
        assert!(map.contains_key("wss_handshake_attempts"));
        assert!(!map.contains_key("ttft_ms"));

        let snap = client.snapshot().await;
        assert_eq!(snap.adaptive.aggregate.in_flight, 0);
        assert_eq!(snap.wss_active_asks, 0);
        assert!(
            snap.adaptive.websocket.cancellations >= 1,
            "lease cancel must increment cancellations"
        );
    }
}
