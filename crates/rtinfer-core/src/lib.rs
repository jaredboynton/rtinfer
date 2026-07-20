//! Realtime client for OpenAI's `gpt-realtime-2.1` WebSocket API.
//!
//! Streams a system `instructions` block, one or more `input_text` context
//! blobs, then a single `QUESTION:` user message, and reads
//! `response.output_text.delta` frames into a single assembled `text` until
//! `response.done`.
//!
//! # Auth
//!
//! Reads `~/.codex/auth.json` produced by `codex login`. The two fields
//! used are `tokens.access_token` (Bearer) and `tokens.account_id` (passed
//! as `chatgpt-account-id`). No refresh, no JWT validation: the file is
//! treated as a short-lived secret managed by the codex CLI.
//!
//! # Errors
//!
//! All failures funnel into [`RealtimeError`]. Provider-side errors that
//! arrive as `{"type":"error","error":{...}}` frames are surfaced via
//! [`RealtimeError::Provider`] with the upstream `code` and `message`
//! preserved verbatim.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

mod adaptive;
mod auth;
mod pool;
mod protocol;
mod responses;
mod thread;
mod warm;

pub use adaptive::{
    AdaptiveConcurrency, AdaptiveConcurrencyConfig, AdaptiveConcurrencySnapshot, AdaptiveLease,
    AdaptiveOutcome, AggregateConcurrencySnapshot, ConcurrencyLimits, EnabledResponsesLanes,
    ResponsesTransportKind, TransportConcurrencySnapshot,
};
pub use auth::{
    CodexAuth, CodexAuthSource, FileCodexAuthSource, SharedCodexAuthSource, StaticCodexAuthSource,
    ID_TOKEN_REFRESH_MARGIN_SECS,
};
pub use pool::{
    global_pool, install_global, RealtimePool, RealtimePoolBuilder, DEFAULT_AUTH_TTL,
    DEFAULT_POOL_CAPACITY,
};
pub use protocol::{run_session, run_session_structured, run_session_with_tools};
pub use responses::{
    assemble_codex_responses_sse, assemble_codex_responses_text, classify_responses_http_status,
    classify_responses_provider_code, require_all_object_properties_for_strict_schema,
    responses_content_type_allows_sse, CodexRequestIds, CodexResponsesClient,
    CodexResponsesClientBuilder, CodexResponsesPool, CodexResponsesPoolBuilder,
    CodexResponsesWireRequest, ResponsesClientSnapshot, ResponsesResultClass,
    ResponsesRuntimeConfig, ResponsesTransportMode, CODEX_BETA_FEATURES, CODEX_CLIENT_VERSION,
    CODEX_RESPONSES_HTTP_URL, CODEX_RESPONSES_MODEL, CODEX_RESPONSES_ORIGINATOR,
    CODEX_RESPONSES_URL, CODEX_RESPONSES_USER_AGENT,
};
pub use thread::{ThreadAskOutcome, ThreadItem, ThreadRegistry};
pub use warm::{WarmSessionPool, WarmToolTurn};

/// Default Realtime endpoint; identical to the JS reference.
pub const REALTIME_URL: &str = "wss://api.openai.com/v1/realtime?model=gpt-realtime-2.1";

/// Default model.  Currently the only model we ever ask for.
pub const DEFAULT_MODEL: &str = "gpt-realtime-2.1";

/// Default WebSocket handshake timeout.  Matches the JS client.
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// One Realtime ask.
///
/// `context_blobs` are sent as a sequence of `conversation.item.create`
/// messages with role `user` and `input_text` content, in order. The
/// `question` is sent last as a separate `QUESTION: <q>` user message.
#[derive(Debug, Clone)]
pub struct RealtimeRequest {
    /// System instructions.  Forwarded as `session.instructions`.
    pub instructions: String,
    /// Free-form context blocks, sent in order before the question.
    pub context_blobs: Vec<String>,
    /// The actual question.  Sent as `QUESTION: {question}`.
    pub question: String,
    /// Override the default model.
    pub model: Option<String>,
    /// Override the default handshake timeout.
    pub handshake_timeout: Option<Duration>,
}

/// A Realtime function tool exposed to the model.
#[derive(Debug, Clone, Serialize)]
pub struct RealtimeTool {
    #[serde(rename = "type")]
    pub kind: String,
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

impl RealtimeTool {
    pub fn function(
        name: impl Into<String>,
        description: impl Into<String>,
        parameters: Value,
    ) -> Self {
        Self {
            kind: "function".to_string(),
            name: name.into(),
            description: description.into(),
            parameters,
        }
    }
}

/// Bounded Realtime tool-loop request.
#[derive(Debug, Clone)]
pub struct RealtimeToolRequest {
    pub instructions: String,
    pub context_blobs: Vec<String>,
    pub question: String,
    pub model: Option<String>,
    pub handshake_timeout: Option<Duration>,
    pub tools: Vec<RealtimeTool>,
    pub options: RealtimeToolLoopOptions,
}

/// Runtime limits for one Realtime function-call loop.
#[derive(Debug, Clone)]
pub struct RealtimeToolLoopOptions {
    pub max_tool_calls: usize,
    pub wall_clock_timeout: Duration,
    pub tool_timeout: Duration,
    pub max_total_tool_result_chars: usize,
}

impl Default for RealtimeToolLoopOptions {
    fn default() -> Self {
        Self {
            max_tool_calls: 12,
            wall_clock_timeout: Duration::from_secs(90),
            tool_timeout: Duration::from_secs(30),
            max_total_tool_result_chars: 40_000,
        }
    }
}

/// One function call requested by the Realtime model.
#[derive(Debug, Clone)]
pub struct RealtimeToolCall {
    pub name: String,
    pub call_id: String,
    pub arguments: Value,
    pub arguments_raw: String,
}

/// Tool output returned to the model.
#[derive(Debug, Clone)]
pub struct RealtimeToolOutput {
    pub output: String,
}

impl RealtimeToolOutput {
    pub fn text(output: impl Into<String>) -> Self {
        Self {
            output: output.into(),
        }
    }

    pub fn json(value: &Value) -> Self {
        Self {
            output: value.to_string(),
        }
    }
}

#[async_trait::async_trait]
pub trait RealtimeToolExecutor {
    async fn execute(&self, call: RealtimeToolCall) -> Result<RealtimeToolOutput, RealtimeError>;
}

/// One structured Realtime ask: a single function tool named after the
/// schema is the only legal output (`tool_choice: "required"`) and the
/// first call's arguments ARE the result. No tool output is returned to
/// the model; the session closes once the arguments arrive.
#[derive(Debug, Clone)]
pub struct RealtimeStructuredRequest {
    pub instructions: String,
    pub context_blobs: Vec<String>,
    pub question: String,
    pub schema_name: String,
    pub schema: Value,
    pub model: Option<String>,
    /// Realtime sampling temperature. `None` uses the server default. The
    /// Realtime API floors this at 0.6 (lower values are rejected), so this is
    /// the lowest-variance setting available for scoring-style asks.
    pub temperature: Option<f64>,
    pub handshake_timeout: Option<Duration>,
    pub wall_clock_timeout: Option<Duration>,
}

/// Result of a successful Realtime exchange.
#[derive(Debug, Clone)]
pub struct RealtimeResponse {
    /// Concatenated `response.output_text.delta` payloads.
    pub text: String,
    /// Wall-clock from the start of [`RealtimeClient::ask`] to the first
    /// `delta` frame.  `0` if no delta arrived (only possible when the
    /// transcript opened with `response.done` and zero output).
    pub first_token_ms: u128,
    /// Wall-clock from the start of [`RealtimeClient::ask`] to graceful
    /// close.
    pub total_ms: u128,
    /// Wall-clock to WebSocket handshake completion.
    pub connected_ms: u128,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum RealtimeError {
    #[error("auth file unreadable at {path}: {source}")]
    AuthFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("auth file missing field: {0}")]
    AuthMissing(&'static str),

    #[error("auth file malformed: {0}")]
    AuthMalformed(String),

    #[error("websocket handshake failed: {0}")]
    Handshake(String),

    #[error("token refresh failed: {0}")]
    Refresh(String),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("provider error: {code}: {message}")]
    Provider { code: String, message: String },

    #[error("tool loop limit exceeded: {0}")]
    ToolLimit(String),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

impl RealtimeError {
    /// A stable, non-sensitive label for this error variant. Used by callers
    /// (e.g. the cockpit rtinfer endpoint) that must surface a category to a
    /// client WITHOUT echoing provider message bodies, which can carry
    /// bearer-equivalent material.
    pub fn code_or_label(&self) -> String {
        match self {
            RealtimeError::AuthFile { .. } => "auth_file".to_string(),
            RealtimeError::AuthMissing(_) => "auth_missing".to_string(),
            RealtimeError::AuthMalformed(_) => "auth_malformed".to_string(),
            RealtimeError::Handshake(_) => "handshake".to_string(),
            RealtimeError::Refresh(_) => "refresh".to_string(),
            RealtimeError::Protocol(_) => "protocol".to_string(),
            RealtimeError::Provider { code, .. } => format!("provider:{code}"),
            RealtimeError::ToolLimit(_) => "tool_limit".to_string(),
            RealtimeError::Io(_) => "io".to_string(),
            RealtimeError::Json(_) => "json".to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// Realtime client.  Holds a [`CodexAuth`] and dispatches one ask at a
/// time over a fresh WebSocket — there is no connection pooling on
/// this type because the underlying `gpt-realtime-2.1` session is
/// request-scoped on the server side.
///
/// **Direct construction is gated.** New callers outside
/// `crates/rtinfer-core/{src,tests}/` must go through
/// [`RealtimePool`] instead. The
/// `crates/rtinfer-core/tests/no_direct_construction.rs` integration
/// test fails CI when an unauthorised file invokes
/// [`RealtimeClient::new`]; see that test's `ALLOWED_PATHS` constant
/// to authorise a new caller.
#[doc(hidden)]
pub struct RealtimeClient {
    auth: CodexAuth,
    endpoint: String,
}

impl RealtimeClient {
    #[doc(hidden)]
    pub fn new(auth: CodexAuth) -> Self {
        Self {
            auth,
            endpoint: REALTIME_URL.to_owned(),
        }
    }

    /// Override the WebSocket endpoint (used by tests).
    #[doc(hidden)]
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }

    /// Run one ask.  Connects, exchanges frames, returns the assembled
    /// response.  Closes the WebSocket gracefully on success or error.
    #[doc(hidden)]
    pub async fn ask(&self, req: RealtimeRequest) -> Result<RealtimeResponse, RealtimeError> {
        protocol::run_session(&self.auth, &self.endpoint, req).await
    }

    /// Run one ask with Realtime function tools.
    #[doc(hidden)]
    pub async fn ask_with_tools(
        &self,
        req: RealtimeToolRequest,
        executor: &(dyn RealtimeToolExecutor + Send + Sync),
    ) -> Result<RealtimeResponse, RealtimeError> {
        protocol::run_session_with_tools(&self.auth, &self.endpoint, req, executor).await
    }
}

// ---------------------------------------------------------------------------
// Wire frames (serialise side only — read side is parsed loosely)
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub(crate) struct SessionUpdate<'a> {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub session: SessionUpdateBody<'a>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SessionUpdateBody<'a> {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub instructions: &'a str,
    pub output_modalities: [&'static str; 1],
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<RealtimeTool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<&'static str>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ConversationItemCreate<'a> {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub item: ConversationItem<'a>,
}

#[derive(Debug, Serialize)]
pub(crate) struct FunctionCallOutputCreate<'a> {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub item: FunctionCallOutputItem<'a>,
}

#[derive(Debug, Serialize)]
pub(crate) struct FunctionCallOutputItem<'a> {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub call_id: &'a str,
    pub output: &'a str,
}

#[derive(Debug, Serialize)]
pub(crate) struct ConversationItem<'a> {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub role: &'static str,
    pub content: Vec<InputContent<'a>>,
}

#[derive(Debug, Serialize)]
pub(crate) struct InputContent<'a> {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub text: &'a str,
}

#[derive(Debug, Serialize)]
pub(crate) struct ResponseCreate {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub response: ResponseCreateBody,
}

#[derive(Debug, Serialize)]
pub(crate) struct ResponseCreateBody {
    pub output_modalities: [&'static str; 1],
}

// ---------------------------------------------------------------------------
// Inbound frame envelopes (loose deserialisation)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub(crate) struct InboundEnvelope {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub delta: Option<String>,
    #[serde(default)]
    pub error: Option<ProviderError>,
    #[serde(default)]
    pub response: Option<RealtimeDoneResponse>,
    #[serde(default)]
    pub output: Option<Vec<RealtimeOutputItem>>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RealtimeDoneResponse {
    #[serde(default)]
    pub output: Vec<RealtimeOutputItem>,
    /// Provider token usage (`response.usage` on `response.done`). Surfaced by
    /// the thread tier so clients can measure prompt-cache effectiveness.
    #[serde(default)]
    pub usage: Option<Value>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct RealtimeOutputItem {
    #[serde(rename = "type", default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub call_id: Option<String>,
    #[serde(default)]
    pub arguments: Option<String>,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub output_text: Option<String>,
    #[serde(default)]
    pub content: Vec<RealtimeContentItem>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct RealtimeContentItem {
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub output_text: Option<String>,
    #[serde(default)]
    pub transcript: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct ProviderError {
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub r#type: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
}

impl ProviderError {
    pub fn code_or_type(&self) -> String {
        self.code
            .clone()
            .or_else(|| self.r#type.clone())
            .unwrap_or_else(|| "unknown".to_owned())
    }
}

/// Helper used by tests + the daemon helper to load auth from `~/.codex/auth.json`.
///
/// Equivalent to `CodexAuth::from_default_path()` but exposed directly
/// so callers don't need to construct a [`CodexAuth`] just to know the
/// canonical path.
pub fn default_auth_path() -> Option<PathBuf> {
    auth::default_auth_path()
}

/// Equivalent of `Path::canonicalize` but tolerant of relative paths;
/// used in error reporting.
fn display_path(p: &Path) -> PathBuf {
    p.canonicalize().unwrap_or_else(|_| p.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_MODEL, REALTIME_URL};

    #[test]
    fn default_realtime_endpoint_uses_the_2_1_model() {
        assert_eq!(DEFAULT_MODEL, "gpt-realtime-2.1");
        assert!(REALTIME_URL.ends_with("model=gpt-realtime-2.1"));
    }
}
