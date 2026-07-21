//! rtinfer/1 loopback inference server.
//!
//! Exposes the daemon's warm [`RealtimePool`] (gpt-realtime-* navigators /
//! scorer) and [`CodexResponsesClient`] (gpt-5.x synthesis) over a loopback
//! HTTP contract so any client on the machine (cse-sweep, unifable judge,
//! ...) borrows one warm pool instead of spawning its own realtime daemon.
//!
//! Auth is file-based by default. With an explicit cse-toold binary, every pool
//! shares one credential-process source and never falls back to `auth.json`.
//!
//! The server binds `127.0.0.1` only; there is no auth header on the wire
//! because the loopback bind is the trust boundary.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{rejection::JsonRejection, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use rtinfer_core::{
    CodexResponsesClient, RealtimeTool, ResponsesRuntimeConfig, SharedCodexAuthSource, ThreadItem,
    ThreadRegistry, TransportConcurrencySnapshot, WarmSessionPool, WarmToolTurn,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::info;

/// Wire-contract identifier shared with every rtinfer client.
pub const RTINFER_CONTRACT: &str = "rtinfer/1";

const OPENAI_REALTIME_MODELS: &[&str] = &[
    "gpt-realtime",
    "gpt-realtime-1.5",
    "gpt-realtime-2.1",
    "gpt-realtime-2.1-mini",
];
const REALTIME_REASONING_EFFORTS: &[&str] = &["none", "minimal", "low", "medium", "high"];
const REASONING_MODEL: &str = "gpt-realtime-2.1";

/// Warm Realtime sockets kept per model. The realtime fan-out (navigators +
/// scorer) issues a handful of concurrent asks; 4 independent warm sessions per
/// model give real parallelism without cid-multiplexing one socket.
const WARM_SESSIONS_PER_MODEL: usize = 4;

/// Shared daemon state: the warm realtime sessions + the responses coordinator.
pub struct AppState {
    /// Warm persistent Realtime sockets (out-of-band asks, no per-call
    /// handshake). The realtime_structured tier dispatches here.
    pub warm_realtime: Arc<WarmSessionPool>,
    /// Single Responses coordinator (WSS / HTTP / dual). Only
    /// `responses_structured` and `responses_text` route here.
    pub codex_responses_client: Arc<CodexResponsesClient>,
    /// Validated `RTINFER_RESPONSES_PREWARM` from the one-shot runtime parse.
    responses_prewarm: usize,
    /// Per-thread pinned sockets with server-side append-only conversations.
    /// The realtime_thread_structured tier dispatches here.
    pub realtime_threads: Arc<ThreadRegistry>,
    /// Present only in credential-process mode. Health loads this exact source;
    /// file mode keeps the legacy direct `auth.json` check.
    auth_source: Option<SharedCodexAuthSource>,
}

/// Parse runtime config once and build the shared Responses coordinator.
fn build_codex_responses_client(
    auth_source: Option<SharedCodexAuthSource>,
) -> Result<(Arc<CodexResponsesClient>, usize), rtinfer_core::RealtimeError> {
    let runtime = ResponsesRuntimeConfig::from_env()?;
    let prewarm = runtime.prewarm;
    let mut builder = CodexResponsesClient::builder().runtime(runtime);
    if let Some(source) = auth_source {
        builder = builder.auth_source(source);
    }
    Ok((builder.build()?, prewarm))
}

impl AppState {
    /// Build the default file-auth pools (both read `~/.codex/auth.json`).
    ///
    /// Invalid Responses runtime configuration fails before the daemon binds.
    pub fn new_file_auth() -> Result<Arc<Self>, rtinfer_core::RealtimeError> {
        let (codex_responses_client, responses_prewarm) = build_codex_responses_client(None)?;
        Ok(Arc::new(Self {
            warm_realtime: WarmSessionPool::new(WARM_SESSIONS_PER_MODEL, None),
            codex_responses_client,
            responses_prewarm,
            realtime_threads: ThreadRegistry::new(None),
            auth_source: None,
        }))
    }

    /// Build all pools around one shared credential-process source.
    pub fn new_cse_toold(bin: PathBuf) -> Result<Arc<Self>, rtinfer_core::RealtimeError> {
        let source: SharedCodexAuthSource =
            crate::cse_toold_auth::CseTooldCodexAuthSource::shared(bin)?;
        Self::new_with_auth_source(source)
    }

    fn new_with_auth_source(
        source: SharedCodexAuthSource,
    ) -> Result<Arc<Self>, rtinfer_core::RealtimeError> {
        let (codex_responses_client, responses_prewarm) =
            build_codex_responses_client(Some(source.clone()))?;
        Ok(Arc::new(Self {
            warm_realtime: WarmSessionPool::new(WARM_SESSIONS_PER_MODEL, Some(source.clone())),
            codex_responses_client,
            responses_prewarm,
            realtime_threads: ThreadRegistry::new(Some(source.clone())),
            auth_source: Some(source),
        }))
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
pub async fn serve(port: u16, cse_toold_bin: Option<PathBuf>) -> anyhow::Result<()> {
    let state = match cse_toold_bin {
        Some(bin) => AppState::new_cse_toold(bin)?,
        None => AppState::new_file_auth()?,
    };
    let app = router(state.clone());
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    let base_url = format!("http://127.0.0.1:{}", bound.port());
    crate::endpoint_file::write(&base_url)?;
    info!(%base_url, version = crate::self_update::current_version(), "rtinfer: serving");

    // Background prewarm from the validated runtime config. HTTP mode skips
    // WSS opens inside the coordinator; zero means off.
    if state.responses_prewarm > 0 {
        let client = state.codex_responses_client.clone();
        let n = state.responses_prewarm;
        tokio::spawn(async move {
            client.prewarm(n).await;
            tracing::info!(sockets = n, "rtinfer: responses prewarm pass complete");
        });
    }

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
    /// realtime_thread_structured: stable client thread id (judge family key).
    #[serde(default)]
    thread_id: Option<String>,
    /// realtime_thread_structured: FULL current transcript window as id'd items;
    /// the daemon appends only what its thread socket has not yet seen.
    #[serde(default)]
    items: Option<Vec<ThreadItem>>,
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
    #[serde(default)]
    tools: Option<Vec<Value>>,
    #[serde(default)]
    tool_choice: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct OpenAiReasoning {
    #[serde(default)]
    effort: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct OpenAiMessage {
    role: String,
    #[serde(default)]
    content: Option<Value>,
    /// Assistant turns replayed by the client carry the tool calls the model
    /// previously requested (OpenAI Chat shape: `{id, function:{name, arguments}}`).
    #[serde(default)]
    tool_calls: Option<Vec<Value>>,
    /// `role:"tool"` turns carry the id of the call they answer.
    #[serde(default)]
    tool_call_id: Option<String>,
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
    #[serde(default)]
    tools: Option<Vec<Value>>,
    #[serde(default)]
    tool_choice: Option<Value>,
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
                            "text" | "input_text" | "output_text" => {
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

// ---------------------------------------------------------------------------
// Tool-calling bridge: OpenAI tool/message shapes <-> Realtime out-of-band items
// ---------------------------------------------------------------------------

/// Translate OpenAI tool definitions into `RealtimeTool`. `nested` selects the
/// Chat Completions shape (`{type:"function", function:{name, ...}}`) vs the flat
/// Responses shape (`{type:"function", name, ...}`).
fn parse_openai_tools(tools: &[Value], nested: bool) -> Result<Vec<RealtimeTool>, String> {
    let mut out = Vec::with_capacity(tools.len());
    for t in tools {
        let spec = if nested {
            t.get("function").unwrap_or(t)
        } else {
            t
        };
        let name = spec
            .get("name")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or("tool definition missing function name")?;
        let description = spec
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let parameters = spec
            .get("parameters")
            .cloned()
            .unwrap_or_else(|| json!({ "type": "object", "properties": {} }));
        out.push(RealtimeTool::function(name, description, parameters));
    }
    Ok(out)
}

/// Map an OpenAI `tool_choice` onto the Realtime `tool_choice` string. Forcing a
/// specific function (object form) maps to `"required"`; `auto` is the default.
fn parse_tool_choice(choice: Option<&Value>) -> String {
    match choice {
        Some(Value::String(s)) if matches!(s.as_str(), "auto" | "none" | "required") => s.clone(),
        Some(Value::Object(_)) => "required".to_string(),
        _ => "auto".to_string(),
    }
}

/// One Realtime `message` input item. Non-user text is labelled (`assistant: ...`)
/// and carried as a user-role `input_text` part — the proven flatten convention,
/// which sidesteps Realtime's stricter assistant-content typing while keeping the
/// turn legible to the model.
fn realtime_text_item(role: &str, text: &str) -> Value {
    let labelled = if role == "user" {
        text.to_string()
    } else {
        format!("{role}: {text}")
    };
    json!({
        "type": "message",
        "role": "user",
        "content": [{ "type": "input_text", "text": labelled }],
    })
}

/// Build a Realtime `function_call` item from an OpenAI Chat `tool_calls` entry.
fn realtime_function_call_from_chat(call: &Value) -> Result<Value, String> {
    let call_id = call.get("id").and_then(Value::as_str).unwrap_or("");
    let func = call
        .get("function")
        .ok_or("tool_call missing function object")?;
    let name = func
        .get("name")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or("tool_call missing function name")?;
    let arguments = func
        .get("arguments")
        .and_then(Value::as_str)
        .unwrap_or("{}");
    Ok(json!({
        "type": "function_call",
        "name": name,
        "call_id": call_id,
        "arguments": arguments,
    }))
}

/// Reconstruct the Realtime out-of-band `input` array from a Chat message list,
/// returning the flattened system prompt plus the interleaved items (text turns,
/// `function_call`, `function_call_output`).
fn chat_messages_to_realtime(messages: &[OpenAiMessage]) -> Result<(String, Vec<Value>), String> {
    let mut system_parts = Vec::new();
    let mut items: Vec<Value> = Vec::new();
    for msg in messages {
        let role = msg.role.trim().to_ascii_lowercase();
        match role.as_str() {
            "system" | "developer" => {
                let text = openai_content_text(&msg.content)?;
                if !text.is_empty() {
                    system_parts.push(text);
                }
            }
            "user" => {
                let text = openai_content_text(&msg.content)?;
                if !text.is_empty() {
                    items.push(realtime_text_item("user", &text));
                }
            }
            "assistant" => {
                let text = openai_content_text(&msg.content)?;
                if !text.is_empty() {
                    items.push(realtime_text_item("assistant", &text));
                }
                if let Some(calls) = &msg.tool_calls {
                    for call in calls {
                        items.push(realtime_function_call_from_chat(call)?);
                    }
                }
            }
            "tool" => {
                let call_id = msg
                    .tool_call_id
                    .clone()
                    .filter(|s| !s.is_empty())
                    .ok_or("tool message missing tool_call_id")?;
                let output = openai_content_text(&msg.content)?;
                items.push(json!({
                    "type": "function_call_output",
                    "call_id": call_id,
                    "output": output,
                }));
            }
            other => return Err(format!("unsupported message role {other}")),
        }
    }
    if items.is_empty() {
        return Err("messages must contain at least one non-empty user/assistant turn".into());
    }
    Ok((system_parts.join("\n\n"), items))
}

/// Coerce a `function_call_output` `output` field (string, or any JSON value) to
/// the string Realtime expects.
fn function_output_to_string(obj: &serde_json::Map<String, Value>) -> String {
    match obj.get("output") {
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
        None => String::new(),
    }
}

/// Reconstruct the Realtime out-of-band `input` array from a Responses `input`
/// field, accepting `message`, `function_call`, and `function_call_output` items.
fn responses_input_to_realtime(input: &Value) -> Result<Vec<Value>, String> {
    match input {
        Value::String(s) => Ok(vec![realtime_text_item("user", s)]),
        Value::Array(arr) => {
            let mut items = Vec::new();
            for it in arr {
                match it {
                    Value::String(s) => items.push(realtime_text_item("user", s)),
                    Value::Object(obj) => {
                        let kind = obj.get("type").and_then(Value::as_str).unwrap_or("message");
                        match kind {
                            "message" => {
                                let role = obj.get("role").and_then(Value::as_str).unwrap_or("user");
                                let text = openai_content_text(&obj.get("content").cloned())?;
                                if !text.is_empty() {
                                    items.push(realtime_text_item(role, &text));
                                }
                            }
                            "function_call" => {
                                let name = obj
                                    .get("name")
                                    .and_then(Value::as_str)
                                    .filter(|s| !s.is_empty())
                                    .ok_or("function_call item missing name")?;
                                let call_id =
                                    obj.get("call_id").and_then(Value::as_str).unwrap_or("");
                                let arguments =
                                    obj.get("arguments").and_then(Value::as_str).unwrap_or("{}");
                                items.push(json!({
                                    "type": "function_call",
                                    "name": name,
                                    "call_id": call_id,
                                    "arguments": arguments,
                                }));
                            }
                            "function_call_output" => {
                                let call_id =
                                    obj.get("call_id").and_then(Value::as_str).unwrap_or("");
                                items.push(json!({
                                    "type": "function_call_output",
                                    "call_id": call_id,
                                    "output": function_output_to_string(obj),
                                }));
                            }
                            other => {
                                return Err(format!(
                                    "unsupported input item type {other}; expected message/function_call/function_call_output"
                                ))
                            }
                        }
                    }
                    _ => return Err("unsupported input item; expected string or object".into()),
                }
            }
            if items.is_empty() {
                return Err("responses input must contain at least one non-empty item".into());
            }
            Ok(items)
        }
        _ => Err("unsupported input; only string or array of items is supported".into()),
    }
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

/// Chat `tool_calls` array (`{index, id, type, function:{name, arguments}}`) from
/// a Realtime tool turn.
fn chat_tool_calls_json(turn: &WarmToolTurn) -> Vec<Value> {
    turn.tool_calls
        .iter()
        .enumerate()
        .map(|(i, c)| {
            json!({
                "index": i,
                "id": c.call_id,
                "type": "function",
                "function": { "name": c.name, "arguments": c.arguments_raw },
            })
        })
        .collect()
}

/// Non-streaming Chat completion carrying tool calls. Falls back to the plain
/// text response when the model returned no calls.
fn openai_chat_tool_response(model: &str, turn: &WarmToolTurn) -> Response {
    if turn.tool_calls.is_empty() {
        return openai_chat_response(model, &turn.text);
    }
    let mut message = json!({ "role": "assistant", "content": Value::Null });
    if !turn.text.is_empty() {
        message["content"] = json!(turn.text);
    }
    message["tool_calls"] = json!(chat_tool_calls_json(turn));
    Json(json!({
        "id": openai_message_id(),
        "object": "chat.completion",
        "created": now_secs(),
        "model": model,
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": "tool_calls",
        }],
        "usage": { "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0 },
    }))
    .into_response()
}

/// Streaming Chat completion carrying tool calls. Falls back to the plain text
/// stream when the model returned no calls.
fn openai_chat_tool_stream_response(model: &str, turn: &WarmToolTurn) -> Response {
    if turn.tool_calls.is_empty() {
        return openai_chat_stream_response(model, &turn.text);
    }
    let id = openai_message_id();
    let created = now_secs();
    let start = json!({
        "id": id, "object": "chat.completion.chunk", "created": created, "model": model,
        "choices": [{ "index": 0, "delta": { "role": "assistant" }, "finish_reason": null }],
    });
    let tool_delta = json!({
        "id": id, "object": "chat.completion.chunk", "created": created, "model": model,
        "choices": [{ "index": 0, "delta": { "tool_calls": chat_tool_calls_json(turn) }, "finish_reason": null }],
    });
    let stop = json!({
        "id": id, "object": "chat.completion.chunk", "created": created, "model": model,
        "choices": [{ "index": 0, "delta": {}, "finish_reason": "tool_calls" }],
    });
    let body = format!("data: {start}\n\ndata: {tool_delta}\n\ndata: {stop}\n\ndata: [DONE]\n\n");
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

/// Bounded, non-sensitive fields for upstream-error observability.
///
/// Never includes provider message bodies, auth/path/token/account material,
/// prompts, outputs, or raw frames — only the tier and a stable `code_or_label`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct UpstreamErrorLogData<'a> {
    tier: &'a str,
    code_or_label: String,
}

impl UpstreamErrorLogData<'_> {
    /// Formatter seam asserted by redaction tests (mirrors production fields).
    #[cfg(test)]
    fn format_log_line(&self) -> String {
        format!("tier={} code_or_label={}", self.tier, self.code_or_label)
    }
}

/// Extract the only fields allowed in daemon upstream-error logs.
fn upstream_error_log_data<'a>(
    tier: &'a str,
    err: &rtinfer_core::RealtimeError,
) -> UpstreamErrorLogData<'a> {
    UpstreamErrorLogData {
        tier,
        code_or_label: err.code_or_label(),
    }
}

/// Map a `RealtimeError` from either pool onto the rtinfer error envelope.
fn rtinfer_map_realtime_err(tier: &str, err: rtinfer_core::RealtimeError) -> Response {
    use rtinfer_core::RealtimeError as RE;
    let (status, code, retryable) = match &err {
        // Handshake 401/403 after the pool's internal refresh-and-retry loop is
        // the edge's rolling handshake rate limit, not a credential outage.
        // Marking it retryable stops clients from latching a global auth
        // freeze that fails every in-flight account for 30s.
        RE::Handshake(msg)
            if msg.contains("401")
                || msg.contains("403")
                || msg.contains("Unauthorized")
                || msg.contains("Forbidden") =>
        {
            (StatusCode::BAD_GATEWAY, "provider_error", true)
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
    let log = upstream_error_log_data(tier, &err);
    tracing::warn!(
        tier = %log.tier,
        code_or_label = %log.code_or_label,
        "rtinfer: upstream model error"
    );
    rtinfer_err(
        status,
        code,
        format!("{tier} upstream error: {}", log.code_or_label),
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
    let reasoning_effort = match request_reasoning_effort(&req) {
        Ok(effort) => effort,
        Err(e) => return openai_err(StatusCode::BAD_REQUEST, "invalid_request_error", e),
    };

    // Tool-calling path: declare the client's tools to the Realtime session and
    // hand back the function calls it requests (client executes + loops).
    let tools = req.tools.clone().unwrap_or_default();
    if !tools.is_empty() {
        let realtime_tools = match parse_openai_tools(&tools, true) {
            Ok(t) => t,
            Err(e) => return openai_err(StatusCode::BAD_REQUEST, "invalid_request_error", e),
        };
        let tool_choice = parse_tool_choice(req.tool_choice.as_ref());
        let (system, items) = match chat_messages_to_realtime(&req.messages) {
            Ok(parts) => parts,
            Err(e) => return openai_err(StatusCode::BAD_REQUEST, "invalid_request_error", e),
        };
        return match state
            .warm_realtime
            .ask_tools(
                &system,
                &items,
                &realtime_tools,
                &tool_choice,
                Some(&req.model),
                reasoning_effort.as_deref(),
            )
            .await
        {
            Ok(turn) if req.stream => openai_chat_tool_stream_response(&req.model, &turn),
            Ok(turn) => openai_chat_tool_response(&req.model, &turn),
            Err(e) => {
                let label = e.code_or_label();
                tracing::warn!(
                    model = %req.model,
                    code_or_label = %label,
                    "rtinfer: openai chat tool upstream error"
                );
                openai_err(
                    StatusCode::BAD_GATEWAY,
                    "provider_error",
                    format!("realtime upstream error: {label}"),
                )
            }
        };
    }

    let (system, user) = match openai_messages_to_prompt(&req.messages) {
        Ok(prompt) => prompt,
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
            let label = e.code_or_label();
            tracing::warn!(
                model = %req.model,
                code_or_label = %label,
                "rtinfer: openai chat upstream error"
            );
            openai_err(
                StatusCode::BAD_GATEWAY,
                "provider_error",
                format!("realtime upstream error: {label}"),
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

/// Responses `function_call` output items from a Realtime tool turn.
fn responses_function_call_items(turn: &WarmToolTurn) -> Vec<Value> {
    turn.tool_calls
        .iter()
        .map(|c| {
            json!({
                "id": format!("fc_{}", c.call_id),
                "type": "function_call",
                "call_id": c.call_id,
                "name": c.name,
                "arguments": c.arguments_raw,
                "status": "completed",
            })
        })
        .collect()
}

/// Non-streaming Responses payload carrying `function_call` output items. Falls
/// back to the plain message response when the model returned no calls.
fn openai_responses_tool_non_stream(model: &str, turn: &WarmToolTurn) -> Response {
    if turn.tool_calls.is_empty() {
        return openai_responses_non_stream(model, &turn.text);
    }
    let mut output = Vec::new();
    if !turn.text.is_empty() {
        output.push(json!({
            "id": format!("msg_{}", now_secs()),
            "type": "message",
            "role": "assistant",
            "status": "completed",
            "content": [{ "type": "output_text", "text": turn.text }],
        }));
    }
    output.extend(responses_function_call_items(turn));
    Json(json!({
        "id": format!("resp_{}", now_secs()),
        "object": "response",
        "status": "completed",
        "created_at": now_secs(),
        "model": model,
        "output": output,
        "usage": { "input_tokens": 0, "output_tokens": 0, "total_tokens": 0 },
    }))
    .into_response()
}

/// Streaming Responses payload carrying `function_call` output items. Falls back
/// to the plain text stream when the model returned no calls.
fn openai_responses_tool_stream(model: &str, turn: &WarmToolTurn) -> Response {
    if turn.tool_calls.is_empty() {
        return openai_responses_stream(model, &turn.text);
    }
    let id = format!("resp_{}", now_secs());
    let created_at = now_secs();
    let items = responses_function_call_items(turn);
    let mut body = String::new();
    let created = json!({
        "type": "response.created",
        "response": { "id": id, "object": "response", "status": "in_progress", "created_at": created_at, "model": model, "output": [] },
    });
    body.push_str(&format!("event: response.created\ndata: {created}\n\n"));
    for (idx, item) in items.iter().enumerate() {
        let added =
            json!({ "type": "response.output_item.added", "output_index": idx, "item": item });
        body.push_str(&format!(
            "event: response.output_item.added\ndata: {added}\n\n"
        ));
        let done =
            json!({ "type": "response.output_item.done", "output_index": idx, "item": item });
        body.push_str(&format!(
            "event: response.output_item.done\ndata: {done}\n\n"
        ));
    }
    let response_done = json!({
        "id": id, "object": "response", "status": "completed", "created_at": created_at,
        "model": model, "output": items,
        "usage": { "input_tokens": 0, "output_tokens": 0, "total_tokens": 0 },
    });
    let completed = json!({ "type": "response.completed", "response": response_done });
    body.push_str(&format!("event: response.completed\ndata: {completed}\n\n"));
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
    let system = req.instructions.clone().unwrap_or_default();
    let reasoning_effort = match responses_reasoning_effort(&req) {
        Ok(effort) => effort,
        Err(e) => return openai_err(StatusCode::BAD_REQUEST, "invalid_request_error", e),
    };

    // Tool-calling path: declare tools, replay any prior function-call turns, and
    // hand back the function calls the model requests.
    let tools = req.tools.clone().unwrap_or_default();
    if !tools.is_empty() {
        let realtime_tools = match parse_openai_tools(&tools, false) {
            Ok(t) => t,
            Err(e) => return openai_err(StatusCode::BAD_REQUEST, "invalid_request_error", e),
        };
        let tool_choice = parse_tool_choice(req.tool_choice.as_ref());
        let items = match responses_input_to_realtime(&req.input) {
            Ok(items) => items,
            Err(e) => return openai_err(StatusCode::BAD_REQUEST, "invalid_request_error", e),
        };
        return match state
            .warm_realtime
            .ask_tools(
                &system,
                &items,
                &realtime_tools,
                &tool_choice,
                Some(&req.model),
                reasoning_effort.as_deref(),
            )
            .await
        {
            Ok(turn) if req.stream => openai_responses_tool_stream(&req.model, &turn),
            Ok(turn) => openai_responses_tool_non_stream(&req.model, &turn),
            Err(e) => {
                let label = e.code_or_label();
                tracing::warn!(
                    model = %req.model,
                    code_or_label = %label,
                    "rtinfer: openai responses tool upstream error"
                );
                openai_err(
                    StatusCode::BAD_GATEWAY,
                    "provider_error",
                    format!("realtime upstream error: {label}"),
                )
            }
        };
    }

    let user = match responses_input_to_text(&req.input) {
        Ok(text) => text,
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
            let label = e.code_or_label();
            tracing::warn!(
                model = %req.model,
                code_or_label = %label,
                "rtinfer: openai responses upstream error"
            );
            openai_err(
                StatusCode::BAD_GATEWAY,
                "provider_error",
                format!("realtime upstream error: {label}"),
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
        "realtime_thread_structured" => {
            let (Some(name), Some(schema)) = (req.schema_name.as_deref(), req.schema.clone())
            else {
                return rtinfer_err(
                    StatusCode::BAD_REQUEST,
                    "bad_request",
                    "realtime_thread_structured requires schema_name and schema",
                    false,
                );
            };
            let Some(thread_id) = req.thread_id.as_deref().filter(|t| !t.trim().is_empty()) else {
                return rtinfer_err(
                    StatusCode::BAD_REQUEST,
                    "bad_request",
                    "realtime_thread_structured requires thread_id",
                    false,
                );
            };
            let items = req.items.clone().unwrap_or_default();
            match state
                .realtime_threads
                .ask_structured(
                    thread_id,
                    &req.system,
                    &req.user,
                    name,
                    schema,
                    items,
                    req.model.as_deref(),
                    req.reasoning_effort.as_deref(),
                )
                .await
            {
                Ok(outcome) => Json(json!({
                    "contract": RTINFER_CONTRACT, "ok": true, "tier": "realtime_thread_structured",
                    "object": outcome.object, "model": req.model,
                    "thread": {
                        "appended": outcome.appended,
                        "replayed": outcome.replayed,
                        "total_items": outcome.total_items,
                    },
                    "usage": outcome.usage,
                }))
                .into_response(),
                Err(e) => rtinfer_map_realtime_err("realtime_thread_structured", e),
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
                .codex_responses_client
                .ask_structured(&req.system, &req.user, name, schema)
                .await
            {
                Ok(object) => Json(json!({
                    "contract": RTINFER_CONTRACT, "ok": true, "tier": "responses_structured",
                    "object": object, "model": state.codex_responses_client.model(),
                }))
                .into_response(),
                Err(e) => rtinfer_map_realtime_err("responses_structured", e),
            }
        }
        "responses_text" => {
            match state
                .codex_responses_client
                .ask_text(&req.system, &req.user, req.model.as_deref())
                .await
            {
                Ok(text) => Json(json!({
                    "contract": RTINFER_CONTRACT, "ok": true, "tier": "responses_text",
                    "text": text,
                    "model": req.model.as_deref().unwrap_or(state.codex_responses_client.model()),
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

fn responses_lane_health_json(lane: &TransportConcurrencySnapshot) -> Value {
    let mut obj = json!({
        "limit": lane.limit,
        "in_flight": lane.in_flight,
        "min": lane.min,
        "max": lane.max,
        "slow_start": lane.slow_start,
        "sample_count": lane.sample_count,
        "successes": lane.successes,
        "lane_overloads": lane.lane_overloads,
        "shared_throttles": lane.shared_throttles,
        "failures": lane.failures,
        "indeterminate": lane.indeterminate,
        "cancellations": lane.cancellations,
    });
    // sample_count=0 means unmeasured; never report latency_ewma_ms:0 as a sample.
    if lane.sample_count > 0 {
        obj.as_object_mut()
            .expect("lane health object")
            .insert("latency_ewma_ms".into(), json!(lane.latency_ewma_ms));
    }
    obj
}

async fn rtinfer_health(State(state): State<Arc<AppState>>) -> Response {
    // Validate the configured source. Provider mode must not fall back to a
    // readable auth.json when its credential process fails.
    // A client treats `ready:false` as "present but warming" and retries
    // briefly rather than failing loud.
    let (ready, auth_source) = match state.auth_source.as_ref() {
        Some(source) => (source.load().await.is_ok(), "cse-toold"),
        None => (
            rtinfer_core::CodexAuth::from_default_path().is_ok(),
            "auth.json",
        ),
    };
    let snap = state.codex_responses_client.snapshot().await;
    let mut http = responses_lane_health_json(&snap.adaptive.http);
    {
        let http_obj = http.as_object_mut().expect("http health object");
        http_obj.insert(
            "connection_reuse_count".into(),
            json!(snap.http_connection_reuse_count),
        );
        http_obj.insert("dispatches".into(), json!(snap.http_dispatches));
    }
    let mut websocket = responses_lane_health_json(&snap.adaptive.websocket);
    {
        let ws_obj = websocket.as_object_mut().expect("websocket health object");
        ws_obj.insert("idle_sockets".into(), json!(snap.wss_idle_sockets));
        ws_obj.insert("dispatches".into(), json!(snap.wss_dispatches));
        ws_obj.insert(
            "handshake_attempts".into(),
            json!(snap.wss_handshake_attempts),
        );
        ws_obj.insert("active_asks".into(), json!(snap.wss_active_asks));
    }
    Json(json!({
        "contract": RTINFER_CONTRACT,
        "ready": ready,
        "provider": "rtinferd",
        "auth_source": auth_source,
        "tiers": ["realtime_structured", "realtime_thread_structured", "responses_structured", "responses_text"],
        "responses": {
            "mode": snap.mode.as_str(),
            "aggregate": {
                "limit": snap.adaptive.aggregate.limit,
                "in_flight": snap.adaptive.aggregate.in_flight,
                "waiting": snap.adaptive.aggregate.waiting,
                "throttled": snap.adaptive.aggregate.throttled,
            },
            "http": http,
            "websocket": websocket,
            "auth_generation": snap.auth_generation,
        },
    }))
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use axum::body::to_bytes;
    use axum::http::StatusCode;
    use rtinfer_core::{
        CodexAuth, CodexAuthSource, RealtimeError, ResponsesRuntimeConfig, ResponsesTransportMode,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::OnceLock;
    use tokio::sync::{Mutex, MutexGuard};

    /// Far-future unsigned JWT so `initial_auth` stays a cache hit without
    /// falling through to `~/.codex/auth.json` or the OAuth refresh path.
    fn fixture_codex_auth() -> CodexAuth {
        CodexAuth {
            access_token: "fixture-access".into(),
            account_id: "fixture-acct".into(),
            id_token: ["eyJhbGciOiJub25lIn0", "eyJleHAiOjQxMDI0NDQ4MDB9", ""].join("."),
            refresh_token: String::new(),
            source_path: None,
        }
    }

    const RESPONSES_ENV_KEYS: &[&str] = &[
        "RTINFER_RESPONSES_TRANSPORT",
        "RTINFER_RESPONSES_HTTP_INITIAL",
        "RTINFER_RESPONSES_HTTP_MAX",
        "RTINFER_RESPONSES_WSS_INITIAL",
        "RTINFER_RESPONSES_WSS_MAX",
        "RTINFER_RESPONSES_AGGREGATE_MAX",
        "RTINFER_RESPONSES_PREWARM",
        "RTINFER_RESPONSES_CAPACITY",
    ];

    /// Serializes process-wide Responses env mutation across tests.
    fn responses_env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    /// Dedicated runtime for synchronous tests that must acquire the env lock
    /// without holding a `std` mutex across await (and without nesting in the
    /// caller's Tokio runtime).
    fn responses_env_sync_runtime() -> &'static tokio::runtime::Runtime {
        static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
        RT.get_or_init(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("responses env sync lock runtime")
        })
    }

    /// Synchronous acquisition for `#[test]` (not `#[tokio::test]`).
    fn lock_responses_env_sync() -> MutexGuard<'static, ()> {
        responses_env_sync_runtime().block_on(responses_env_lock().lock())
    }

    /// Restores captured Responses env keys on drop.
    struct ResponsesEnvGuard {
        saved: Vec<(String, Option<String>)>,
    }

    impl ResponsesEnvGuard {
        fn capture() -> Self {
            Self {
                saved: RESPONSES_ENV_KEYS
                    .iter()
                    .map(|k| ((*k).to_string(), std::env::var(k).ok()))
                    .collect(),
            }
        }

        fn clear_all() {
            for key in RESPONSES_ENV_KEYS {
                std::env::remove_var(key);
            }
        }

        fn set(key: &str, value: &str) {
            std::env::set_var(key, value);
        }
    }

    impl Drop for ResponsesEnvGuard {
        fn drop(&mut self) {
            for (key, value) in &self.saved {
                match value {
                    Some(v) => std::env::set_var(key, v),
                    None => std::env::remove_var(key),
                }
            }
        }
    }

    struct HealthAuthSource {
        calls: AtomicUsize,
        succeeds: bool,
    }

    #[async_trait]
    impl CodexAuthSource for HealthAuthSource {
        async fn load(&self) -> Result<CodexAuth, RealtimeError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if !self.succeeds {
                return Err(RealtimeError::Refresh("provider unavailable".into()));
            }
            Ok(CodexAuth {
                access_token: "provider-access".into(),
                account_id: "acct".into(),
                id_token: "provider-id".into(),
                refresh_token: String::new(),
                source_path: None,
            })
        }

        async fn force_refresh(
            &self,
            _rejected_access_token: &str,
        ) -> Result<CodexAuth, RealtimeError> {
            self.load().await
        }
    }

    async fn health_json(state: Arc<AppState>) -> Value {
        let response = rtinfer_health(State(state)).await;
        let bytes = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    async fn response_json(response: Response) -> (StatusCode, Value) {
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
        let body = serde_json::from_slice(&bytes).unwrap();
        (status, body)
    }

    #[tokio::test]
    async fn provider_source_is_shared_by_all_pools_and_health() {
        let _lock = responses_env_lock().lock().await;
        let _env = ResponsesEnvGuard::capture();
        ResponsesEnvGuard::clear_all();

        let source = Arc::new(HealthAuthSource {
            calls: AtomicUsize::new(0),
            succeeds: true,
        });
        let shared: SharedCodexAuthSource = source.clone();

        let state = AppState::new_with_auth_source(shared).unwrap();

        // Caller + warm pool + responses auth cache + thread registry + health field.
        assert_eq!(Arc::strong_count(&source), 5);
        let health = health_json(state).await;
        assert_eq!(health["ready"], true);
        assert_eq!(health["auth_source"], "cse-toold");
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn provider_health_failure_is_not_masked_by_file_auth() {
        let _lock = responses_env_lock().lock().await;
        let _env = ResponsesEnvGuard::capture();
        ResponsesEnvGuard::clear_all();

        let source: SharedCodexAuthSource = Arc::new(HealthAuthSource {
            calls: AtomicUsize::new(0),
            succeeds: false,
        });
        let health = health_json(AppState::new_with_auth_source(source).unwrap()).await;
        assert_eq!(health["ready"], false);
        assert_eq!(health["auth_source"], "cse-toold");
    }

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

    #[tokio::test]
    async fn responses_transport_defaults_to_wss() {
        let _lock = responses_env_lock().lock().await;
        let _env = ResponsesEnvGuard::capture();
        ResponsesEnvGuard::clear_all();

        let state = AppState::new_file_auth().unwrap();
        assert_eq!(
            state.codex_responses_client.mode(),
            ResponsesTransportMode::Wss
        );
        let snap = state.codex_responses_client.snapshot().await;
        assert_eq!(snap.mode, ResponsesTransportMode::Wss);
        assert_eq!(snap.http_dispatches, 0);
        assert_eq!(snap.wss_handshake_attempts, 0);

        let health = health_json(state).await;
        assert_eq!(health["responses"]["mode"], "wss");
        assert_eq!(health["responses"]["http"]["dispatches"], 0);
    }

    #[tokio::test]
    async fn responses_transport_explicit_modes() {
        let _lock = responses_env_lock().lock().await;
        let _env = ResponsesEnvGuard::capture();

        ResponsesEnvGuard::clear_all();
        ResponsesEnvGuard::set("RTINFER_RESPONSES_TRANSPORT", "http");
        let http_state = AppState::new_file_auth().unwrap();
        assert_eq!(
            http_state.codex_responses_client.mode(),
            ResponsesTransportMode::Http
        );
        let http_snap = http_state.codex_responses_client.snapshot().await;
        assert_eq!(http_snap.mode, ResponsesTransportMode::Http);
        assert_eq!(http_snap.wss_handshake_attempts, 0);
        assert_eq!(http_snap.wss_dispatches, 0);
        assert_eq!(http_snap.wss_idle_sockets, 0);
        // HTTP mode must not schedule WSS prewarm opens.
        http_state.codex_responses_client.prewarm(2).await;
        let after = http_state.codex_responses_client.snapshot().await;
        assert_eq!(after.wss_handshake_attempts, 0);
        assert_eq!(after.wss_idle_sockets, 0);

        ResponsesEnvGuard::clear_all();
        ResponsesEnvGuard::set("RTINFER_RESPONSES_TRANSPORT", "dual");
        let dual_state = AppState::new_file_auth().unwrap();
        assert_eq!(
            dual_state.codex_responses_client.mode(),
            ResponsesTransportMode::Dual
        );
        let dual_health = health_json(dual_state).await;
        assert_eq!(dual_health["responses"]["mode"], "dual");

        ResponsesEnvGuard::clear_all();
        ResponsesEnvGuard::set("RTINFER_RESPONSES_TRANSPORT", "wss");
        let wss_state = AppState::new_file_auth().unwrap();
        assert_eq!(
            wss_state.codex_responses_client.mode(),
            ResponsesTransportMode::Wss
        );
    }

    fn expect_responses_config_err(result: Result<Arc<AppState>, RealtimeError>) -> RealtimeError {
        match result {
            Ok(_) => panic!("expected responses config construction failure"),
            Err(e) => e,
        }
    }

    #[test]
    fn responses_runtime_config_rejects_invalid_values() {
        let _lock = lock_responses_env_sync();
        let _env = ResponsesEnvGuard::capture();

        let cases: &[(&str, &str)] = &[
            ("RTINFER_RESPONSES_TRANSPORT", "udp"),
            ("RTINFER_RESPONSES_HTTP_INITIAL", "0"),
            ("RTINFER_RESPONSES_HTTP_MAX", "0"),
            ("RTINFER_RESPONSES_WSS_INITIAL", "0"),
            ("RTINFER_RESPONSES_WSS_MAX", "0"),
            ("RTINFER_RESPONSES_HTTP_MAX", "257"),
            ("RTINFER_RESPONSES_WSS_MAX", "65"),
        ];
        for (key, value) in cases {
            ResponsesEnvGuard::clear_all();
            ResponsesEnvGuard::set(key, value);
            let err = expect_responses_config_err(AppState::new_file_auth());
            let msg = err.to_string();
            assert!(
                msg.starts_with("protocol error: responses config:"),
                "key={key} value={value} got {msg}"
            );
        }

        // initial > max
        ResponsesEnvGuard::clear_all();
        ResponsesEnvGuard::set("RTINFER_RESPONSES_HTTP_MAX", "4");
        ResponsesEnvGuard::set("RTINFER_RESPONSES_HTTP_INITIAL", "8");
        let err = expect_responses_config_err(AppState::new_file_auth());
        assert!(err
            .to_string()
            .starts_with("protocol error: responses config:"));

        ResponsesEnvGuard::clear_all();
        ResponsesEnvGuard::set("RTINFER_RESPONSES_WSS_MAX", "4");
        ResponsesEnvGuard::set("RTINFER_RESPONSES_WSS_INITIAL", "8");
        let err = expect_responses_config_err(AppState::new_file_auth());
        assert!(err
            .to_string()
            .starts_with("protocol error: responses config:"));

        // dual aggregate below 2
        ResponsesEnvGuard::clear_all();
        ResponsesEnvGuard::set("RTINFER_RESPONSES_TRANSPORT", "dual");
        ResponsesEnvGuard::set("RTINFER_RESPONSES_AGGREGATE_MAX", "1");
        let err = expect_responses_config_err(AppState::new_file_auth());
        assert!(err
            .to_string()
            .starts_with("protocol error: responses config:"));

        // aggregate above enabled maxima sum
        ResponsesEnvGuard::clear_all();
        ResponsesEnvGuard::set("RTINFER_RESPONSES_TRANSPORT", "wss");
        ResponsesEnvGuard::set("RTINFER_RESPONSES_WSS_MAX", "4");
        ResponsesEnvGuard::set("RTINFER_RESPONSES_AGGREGATE_MAX", "5");
        let err = expect_responses_config_err(AppState::new_file_auth());
        assert!(err
            .to_string()
            .starts_with("protocol error: responses config:"));
    }

    #[tokio::test]
    async fn legacy_zero_capacity_maps_to_hard_cap() {
        let _lock = responses_env_lock().lock().await;
        let _env = ResponsesEnvGuard::capture();
        ResponsesEnvGuard::clear_all();
        // Legacy alias only when WSS_MAX is absent.
        ResponsesEnvGuard::set("RTINFER_RESPONSES_CAPACITY", "0");

        let state = AppState::new_file_auth().unwrap();
        let snap = state.codex_responses_client.snapshot().await;
        assert_eq!(snap.adaptive.websocket.max, 64);
        assert_eq!(snap.adaptive.websocket.limit, 4);
        assert_eq!(snap.adaptive.aggregate.limit, 64);
        let health = health_json(state).await;
        assert_eq!(health["responses"]["websocket"]["max"], 64);
    }

    #[test]
    fn responses_tiers_route_through_coordinator() {
        let src = include_str!("server.rs");
        assert!(src.contains("codex_responses_client"));
        assert!(src.contains(r#""responses_structured" =>"#));
        assert!(src.contains(r#""responses_text" =>"#));
        assert!(src.contains(".codex_responses_client\n                .ask_structured"));
        assert!(src.contains(".codex_responses_client\n                .ask_text"));
        // Daemon handlers must not select lanes or acquire adaptive leases.
        let structured_start = src.find(r#""responses_structured" =>"#).unwrap();
        let text_start = src.find(r#""responses_text" =>"#).unwrap();
        let end = src.find("other => rtinfer_err(").unwrap();
        let handlers = &src[structured_start..end];
        assert!(!handlers.contains("AdaptiveConcurrency"));
        assert!(!handlers.contains("ResponsesTransportKind"));
        assert!(!handlers.contains("acquire("));
        assert!(text_start > structured_start);
    }

    /// C5/Q2: both Responses tiers reach the coordinator and attempt a real
    /// WSS handshake; observable handshake-attempt delta replaces source text
    /// as proof of transport activity.
    #[tokio::test]
    async fn responses_tiers_execute_coordinator_handshake_side_effects() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind drop listener");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            for _ in 0..2 {
                let Ok((sock, _)) = listener.accept().await else {
                    break;
                };
                drop(sock);
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
            .initial_auth(fixture_codex_auth())
            .build()
            .expect("wss fixture client");

        let state = Arc::new(AppState {
            warm_realtime: WarmSessionPool::new(WARM_SESSIONS_PER_MODEL, None),
            codex_responses_client: client,
            responses_prewarm: 0,
            realtime_threads: ThreadRegistry::new(None),
            auth_source: None,
        });

        let before = state.codex_responses_client.snapshot().await;
        assert_eq!(before.wss_handshake_attempts, 0);
        assert_eq!(before.http_dispatches, 0);

        let text_req: RtInferRequest = serde_json::from_value(json!({
            "contract": RTINFER_CONTRACT,
            "tier": "responses_text",
            "system": "sys",
            "user": "ask",
        }))
        .unwrap();
        let (text_status, text_body) =
            response_json(rtinfer(State(state.clone()), Json(text_req)).await).await;
        assert_eq!(text_status, StatusCode::BAD_GATEWAY);
        assert_eq!(text_body["contract"], RTINFER_CONTRACT);
        assert_eq!(text_body["ok"], false);
        assert_eq!(text_body["error"]["code"], "provider_error");
        assert_eq!(text_body["error"]["retryable"], true);
        assert!(text_body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("responses_text upstream error: handshake"));

        let structured_req: RtInferRequest = serde_json::from_value(json!({
            "contract": RTINFER_CONTRACT,
            "tier": "responses_structured",
            "system": "sys",
            "user": "ask",
            "schema_name": "Answer",
            "schema": {
                "type": "object",
                "properties": { "ok": { "type": "boolean" } },
                "required": ["ok"],
                "additionalProperties": false
            },
        }))
        .unwrap();
        let (structured_status, structured_body) =
            response_json(rtinfer(State(state.clone()), Json(structured_req)).await).await;
        assert_eq!(structured_status, StatusCode::BAD_GATEWAY);
        assert_eq!(structured_body["contract"], RTINFER_CONTRACT);
        assert_eq!(structured_body["ok"], false);
        assert_eq!(structured_body["error"]["code"], "provider_error");
        assert_eq!(structured_body["error"]["retryable"], true);
        assert!(structured_body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("responses_structured upstream error: handshake"));

        let after = state.codex_responses_client.snapshot().await;
        assert_eq!(
            after.wss_handshake_attempts - before.wss_handshake_attempts,
            2,
            "both Responses tiers must each attempt one WSS handshake"
        );
        assert_eq!(after.http_dispatches, 0);
    }

    /// Executable proof that the rtinfer handler selects the Responses tier
    /// arm and returns its stable validation envelope without contacting upstream.
    #[tokio::test]
    async fn responses_structured_validation_error_via_handler() {
        let _lock = responses_env_lock().lock().await;
        let _env = ResponsesEnvGuard::capture();
        ResponsesEnvGuard::clear_all();

        let state = AppState::new_file_auth().unwrap();
        // Router construction is exercised; handler is invoked for the
        // responses_structured validation path (no live network).
        let _app = router(state.clone());
        let req: RtInferRequest = serde_json::from_value(json!({
            "contract": RTINFER_CONTRACT,
            "tier": "responses_structured",
            "system": "sys",
            "user": "ask",
        }))
        .unwrap();
        let (status, body) = response_json(rtinfer(State(state), Json(req)).await).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["contract"], RTINFER_CONTRACT);
        assert_eq!(body["ok"], false);
        assert_eq!(body["error"]["code"], "bad_request");
        assert_eq!(body["error"]["retryable"], false);
        let message = body["error"]["message"].as_str().unwrap();
        assert!(
            message.contains("responses_structured requires schema_name and schema"),
            "unexpected message: {message}"
        );
    }

    /// Same envelope/route selection for responses_text unknown-tier vs known
    /// tier: missing fields on a known Responses tier still hits that arm.
    #[tokio::test]
    async fn responses_text_unknown_tier_rejected_via_handler() {
        let _lock = responses_env_lock().lock().await;
        let _env = ResponsesEnvGuard::capture();
        ResponsesEnvGuard::clear_all();

        let state = AppState::new_file_auth().unwrap();
        let _app = router(state.clone());
        let req: RtInferRequest = serde_json::from_value(json!({
            "contract": RTINFER_CONTRACT,
            "tier": "responses_not_a_real_tier",
        }))
        .unwrap();
        let (status, body) = response_json(rtinfer(State(state), Json(req)).await).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["contract"], RTINFER_CONTRACT);
        assert_eq!(body["ok"], false);
        assert_eq!(body["error"]["code"], "unsupported_tier");
        assert!(body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("responses_not_a_real_tier"));
    }

    #[test]
    fn openai_responses_remains_realtime_backed() {
        let src = include_str!("server.rs");
        let start = src
            .find("async fn openai_responses(")
            .expect("openai_responses");
        let end = src.find("async fn rtinfer(").expect("rtinfer");
        let body = &src[start..end];
        assert!(body.contains("warm_realtime"));
        assert!(body.contains(".ask_text(") || body.contains(".ask_tools("));
        assert!(
            !body.contains("codex_responses_client"),
            "/v1/responses must stay Realtime-backed"
        );
        assert!(!body.contains("ask_structured"));
    }

    #[tokio::test]
    async fn responses_health_exposes_bounded_snapshot() {
        let _lock = responses_env_lock().lock().await;
        let _env = ResponsesEnvGuard::capture();
        ResponsesEnvGuard::clear_all();

        let state = AppState::new_file_auth().unwrap();
        let health = health_json(state).await;

        // Existing top-level fields preserved.
        assert_eq!(health["contract"], RTINFER_CONTRACT);
        assert!(health["ready"].is_boolean());
        assert_eq!(health["provider"], "rtinferd");
        assert!(health["auth_source"].is_string());
        assert_eq!(
            health["tiers"],
            json!([
                "realtime_structured",
                "realtime_thread_structured",
                "responses_structured",
                "responses_text"
            ])
        );

        let responses = &health["responses"];
        assert_eq!(responses["mode"], "wss");
        assert!(responses["auth_generation"].is_number());

        let aggregate = &responses["aggregate"];
        assert!(aggregate["limit"].as_u64().unwrap() <= 64);
        assert_eq!(aggregate["in_flight"], 0);
        assert_eq!(aggregate["waiting"], 0);
        assert_eq!(aggregate["throttled"], false);

        for lane_name in ["http", "websocket"] {
            let lane = &responses[lane_name];
            let limit = lane["limit"].as_u64().unwrap() as usize;
            let max = lane["max"].as_u64().unwrap() as usize;
            let in_flight = lane["in_flight"].as_u64().unwrap() as usize;
            let min = lane["min"].as_u64().unwrap() as usize;
            assert!(min >= 1);
            assert!(limit >= min && limit <= max);
            assert!(in_flight <= limit);
            assert!(lane["sample_count"].as_u64().unwrap() == 0);
            assert!(lane.get("latency_ewma_ms").is_none());
            assert!(lane["dispatches"].is_number());
        }
        assert!(responses["http"]["connection_reuse_count"].is_number());
        assert!(responses["websocket"]["handshake_attempts"].is_number());
        assert!(responses["websocket"]["idle_sockets"].is_number());
        assert!(responses["websocket"]["active_asks"].is_number());
        assert!(responses["websocket"]["max"].as_u64().unwrap() <= 64);
        assert!(responses["http"]["max"].as_u64().unwrap() <= 256);
    }

    #[tokio::test]
    async fn error_envelope_carries_contract_and_code() {
        let resp = rtinfer_err(StatusCode::BAD_GATEWAY, "provider_error", "boom", true);
        let (status, body) = response_json(resp).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(body["contract"], RTINFER_CONTRACT);
        assert_eq!(body["ok"], false);
        assert_eq!(body["error"]["code"], "provider_error");
        assert_eq!(body["error"]["message"], "boom");
        assert_eq!(body["error"]["retryable"], true);
    }

    #[tokio::test]
    async fn openai_error_envelope_uses_requested_status() {
        let resp = openai_err(StatusCode::BAD_REQUEST, "invalid_request_error", "boom");
        let (status, body) = response_json(resp).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "invalid_request_error");
        assert_eq!(body["error"]["type"], "invalid_request_error");
        assert_eq!(body["error"]["message"], "boom");
    }

    /// Provider-message sentinels must not appear in the bounded log seam or
    /// the stable HTTP envelope produced by `rtinfer_map_realtime_err`.
    #[tokio::test]
    async fn upstream_error_log_and_envelope_redact_provider_message_sentinel() {
        const SENTINEL: &str = "SECRET_PROVIDER_MSG_sentinel_bearer_sk-test-do-not-log-xyzzy";
        let err = RealtimeError::Provider {
            code: "rate_limit_exceeded".into(),
            message: SENTINEL.into(),
        };
        // Fixture sanity: Display still carries the body (what we must not log).
        assert!(
            err.to_string().contains(SENTINEL),
            "fixture Display must contain sentinel so redaction is meaningful"
        );

        let log = upstream_error_log_data("responses_structured", &err);
        assert_eq!(log.tier, "responses_structured");
        assert_eq!(log.code_or_label, "provider:rate_limit_exceeded");
        let formatted = log.format_log_line();
        assert!(
            !formatted.contains(SENTINEL),
            "log formatter seam leaked sentinel: {formatted}"
        );
        assert!(!log.code_or_label.contains(SENTINEL));
        assert!(!format!("{log:?}").contains(SENTINEL));

        let (status, body) =
            response_json(rtinfer_map_realtime_err("responses_structured", err)).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(body["contract"], RTINFER_CONTRACT);
        assert_eq!(body["ok"], false);
        assert_eq!(body["error"]["code"], "provider_error");
        assert_eq!(body["error"]["retryable"], true);
        let envelope = body.to_string();
        assert!(
            !envelope.contains(SENTINEL),
            "HTTP envelope leaked sentinel: {envelope}"
        );
        let message = body["error"]["message"].as_str().unwrap();
        assert!(
            message.contains("responses_structured upstream error: provider:rate_limit_exceeded")
        );
        assert!(!message.contains(SENTINEL));
    }

    /// C7/Q3: every named auth / handshake / provider taxonomy branch maps to
    /// the stable rtinfer/1 envelope with code_or_label-only messages.
    #[tokio::test]
    async fn rtinfer_map_realtime_err_taxonomy_branches() {
        const TIER: &str = "responses_text";

        struct MapCase {
            name: &'static str,
            err: RealtimeError,
            status: StatusCode,
            code: &'static str,
            retryable: bool,
            label: &'static str,
            forbidden: &'static [&'static str],
        }

        let json_err: RealtimeError = serde_json::from_str::<Value>("not-json-fixture")
            .unwrap_err()
            .into();

        let cases = vec![
            MapCase {
                name: "auth_file",
                err: RealtimeError::AuthFile {
                    path: PathBuf::from("/tmp/rtinfer-fixture-auth.json"),
                    source: std::io::Error::new(std::io::ErrorKind::NotFound, "fixture-not-found"),
                },
                status: StatusCode::UNAUTHORIZED,
                code: "auth_unavailable",
                retryable: false,
                label: "auth_file",
                forbidden: &["fixture-not-found", "/tmp/rtinfer-fixture-auth.json"],
            },
            MapCase {
                name: "auth_missing",
                err: RealtimeError::AuthMissing("tokens"),
                status: StatusCode::UNAUTHORIZED,
                code: "auth_unavailable",
                retryable: false,
                label: "auth_missing",
                forbidden: &["tokens"],
            },
            MapCase {
                name: "auth_malformed",
                err: RealtimeError::AuthMalformed("fixture-malformed-auth".into()),
                status: StatusCode::UNAUTHORIZED,
                code: "auth_unavailable",
                retryable: false,
                label: "auth_malformed",
                forbidden: &["fixture-malformed-auth"],
            },
            MapCase {
                name: "refresh",
                err: RealtimeError::Refresh("token endpoint returned HTTP 500".into()),
                status: StatusCode::UNAUTHORIZED,
                code: "auth_unavailable",
                retryable: false,
                label: "refresh",
                forbidden: &["token endpoint returned HTTP 500"],
            },
            MapCase {
                name: "handshake_401",
                err: RealtimeError::Handshake("HTTP 401 Unauthorized".into()),
                status: StatusCode::BAD_GATEWAY,
                code: "provider_error",
                retryable: true,
                label: "handshake",
                forbidden: &["HTTP 401 Unauthorized", "Unauthorized"],
            },
            MapCase {
                name: "handshake_403",
                err: RealtimeError::Handshake("HTTP 403 Forbidden".into()),
                status: StatusCode::BAD_GATEWAY,
                code: "provider_error",
                retryable: true,
                label: "handshake",
                forbidden: &["HTTP 403 Forbidden", "Forbidden"],
            },
            MapCase {
                name: "handshake_ordinary",
                err: RealtimeError::Handshake("connection reset by peer".into()),
                status: StatusCode::BAD_GATEWAY,
                code: "provider_error",
                retryable: true,
                label: "handshake",
                forbidden: &["connection reset by peer"],
            },
            MapCase {
                name: "protocol",
                err: RealtimeError::Protocol("fixture protocol detail".into()),
                status: StatusCode::BAD_GATEWAY,
                code: "provider_error",
                retryable: true,
                label: "protocol",
                forbidden: &["fixture protocol detail"],
            },
            MapCase {
                name: "provider",
                err: RealtimeError::Provider {
                    code: "rate_limit_exceeded".into(),
                    message: "PROVIDER_BODY_fixture".into(),
                },
                status: StatusCode::BAD_GATEWAY,
                code: "provider_error",
                retryable: true,
                label: "provider:rate_limit_exceeded",
                forbidden: &["PROVIDER_BODY_fixture"],
            },
            MapCase {
                name: "tool_limit",
                err: RealtimeError::ToolLimit("wall clock timeout fixture".into()),
                status: StatusCode::BAD_GATEWAY,
                code: "provider_error",
                retryable: true,
                label: "tool_limit",
                forbidden: &["wall clock timeout fixture"],
            },
            MapCase {
                name: "io",
                err: RealtimeError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "io-fixture-body",
                )),
                status: StatusCode::BAD_GATEWAY,
                code: "provider_error",
                retryable: true,
                label: "io",
                forbidden: &["io-fixture-body"],
            },
            MapCase {
                name: "json",
                err: json_err,
                status: StatusCode::BAD_GATEWAY,
                code: "provider_error",
                retryable: true,
                label: "json",
                forbidden: &["not-json-fixture"],
            },
        ];

        for case in cases {
            assert_eq!(
                case.err.code_or_label(),
                case.label,
                "case {}: unexpected code_or_label",
                case.name
            );
            let expected_message = format!("{TIER} upstream error: {}", case.label);
            let (got_status, body) = response_json(rtinfer_map_realtime_err(TIER, case.err)).await;
            assert_eq!(got_status, case.status, "case {}: status", case.name);
            assert_eq!(
                body["contract"], RTINFER_CONTRACT,
                "case {}: contract",
                case.name
            );
            assert_eq!(body["ok"], false, "case {}: ok", case.name);
            assert_eq!(body["error"]["code"], case.code, "case {}: code", case.name);
            assert_eq!(
                body["error"]["retryable"], case.retryable,
                "case {}: retryable",
                case.name
            );
            let message = body["error"]["message"].as_str().unwrap();
            assert_eq!(message, expected_message, "case {}: message", case.name);
            for needle in case.forbidden {
                assert!(
                    !message.contains(needle),
                    "case {}: message leaked body fragment {needle:?}: {message}",
                    case.name
                );
            }
        }
    }

    #[tokio::test]
    async fn unknown_contract_is_rejected_via_handler() {
        let _lock = responses_env_lock().lock().await;
        let _env = ResponsesEnvGuard::capture();
        ResponsesEnvGuard::clear_all();

        let state = AppState::new_file_auth().unwrap();
        let req: RtInferRequest = serde_json::from_value(json!({
            "contract": "rtinfer/2",
            "tier": "responses_text",
        }))
        .unwrap();
        assert_eq!(req.contract, "rtinfer/2");
        assert_eq!(req.tier, "responses_text");
        let (status, body) = response_json(rtinfer(State(state), Json(req)).await).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["contract"], RTINFER_CONTRACT);
        assert_eq!(body["ok"], false);
        assert_eq!(body["error"]["code"], "bad_request");
        assert!(body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("unknown contract rtinfer/2"));
    }

    #[test]
    fn openai_request_parses_reasoning_from_all_supported_shapes() {
        let nested: OpenAiChatRequest = serde_json::from_value(json!({
            "model": "gpt-realtime-2.1",
            "messages": [{"role": "user", "content": "hi"}],
            "reasoning": {"effort": "medium"}
        }))
        .unwrap();
        assert_eq!(
            request_reasoning_effort(&nested).unwrap().as_deref(),
            Some("medium")
        );

        let snake: OpenAiChatRequest = serde_json::from_value(json!({
            "model": "gpt-realtime-2.1",
            "messages": [{"role": "user", "content": "hi"}],
            "reasoning_effort": "low"
        }))
        .unwrap();
        assert_eq!(
            request_reasoning_effort(&snake).unwrap().as_deref(),
            Some("low")
        );

        let camel: OpenAiChatRequest = serde_json::from_value(json!({
            "model": "gpt-realtime-2.1",
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
    fn realtime_model_catalog_uses_the_2_1_family() {
        assert!(OPENAI_REALTIME_MODELS.contains(&"gpt-realtime-2.1"));
        assert!(OPENAI_REALTIME_MODELS.contains(&"gpt-realtime-2.1-mini"));
        assert!(!OPENAI_REALTIME_MODELS.contains(&"gpt-realtime-2"));
        assert!(!OPENAI_REALTIME_MODELS.contains(&"gpt-realtime-mini"));
        assert_eq!(REASONING_MODEL, "gpt-realtime-2.1");
    }

    #[test]
    fn reasoning_is_rejected_for_non_reasoning_models() {
        let req: OpenAiChatRequest = serde_json::from_value(json!({
            "model": "gpt-realtime-2.1-mini",
            "messages": [{"role": "user", "content": "hi"}],
            "reasoning": {"effort": "low"}
        }))
        .unwrap();
        let err = request_reasoning_effort(&req).unwrap_err();
        assert!(err.contains("only supported for gpt-realtime-2.1"));
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
                ..Default::default()
            },
            OpenAiMessage {
                role: "developer".into(),
                content: Some(Value::Array(vec![
                    json!({"type":"text","text":"Follow policy"}),
                ])),
                ..Default::default()
            },
            OpenAiMessage {
                role: "user".into(),
                content: Some(Value::String("Question".into())),
                ..Default::default()
            },
            OpenAiMessage {
                role: "assistant".into(),
                content: Some(Value::String("Prior answer".into())),
                ..Default::default()
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
            ..Default::default()
        }])
        .unwrap_err();
        assert!(err.contains("only text is supported"));
    }

    #[test]
    fn openai_content_text_accepts_output_text_for_assistant_turns() {
        // The Responses API sends prior assistant messages back with content
        // parts of type "output_text" (not "input_text"); both must flatten.
        let text = openai_content_text(&Some(Value::Array(vec![
            json!({"type":"output_text","text":"previous answer"}),
        ])))
        .unwrap();
        assert_eq!(text, "previous answer");
    }

    #[test]
    fn openai_messages_require_non_empty_conversation() {
        let err = openai_messages_to_prompt(&[OpenAiMessage {
            role: "system".into(),
            content: Some(Value::String("Rules".into())),
            ..Default::default()
        }])
        .unwrap_err();
        assert!(err.contains("at least one non-empty user/assistant turn"));
    }

    #[test]
    fn parse_tools_handles_chat_nested_and_responses_flat() {
        let chat = json!([{
            "type": "function",
            "function": {
                "name": "get_weather",
                "description": "Get weather",
                "parameters": {"type":"object","properties":{"city":{"type":"string"}}},
            },
        }]);
        let nested = parse_openai_tools(chat.as_array().unwrap(), true).unwrap();
        assert_eq!(nested.len(), 1);
        assert_eq!(nested[0].name, "get_weather");
        assert_eq!(nested[0].parameters["properties"]["city"]["type"], "string");

        let flat = json!([{
            "type": "function",
            "name": "get_weather",
            "description": "Get weather",
            "parameters": {"type":"object"},
        }]);
        let flat = parse_openai_tools(flat.as_array().unwrap(), false).unwrap();
        assert_eq!(flat[0].name, "get_weather");
    }

    #[test]
    fn parse_tools_rejects_missing_name() {
        let bad = json!([{ "type": "function", "function": { "description": "x" } }]);
        assert!(parse_openai_tools(bad.as_array().unwrap(), true).is_err());
    }

    #[test]
    fn tool_choice_maps_string_and_object_forms() {
        assert_eq!(parse_tool_choice(Some(&json!("auto"))), "auto");
        assert_eq!(parse_tool_choice(Some(&json!("none"))), "none");
        assert_eq!(parse_tool_choice(Some(&json!("required"))), "required");
        assert_eq!(
            parse_tool_choice(Some(&json!({"type":"function","function":{"name":"x"}}))),
            "required"
        );
        assert_eq!(parse_tool_choice(None), "auto");
    }

    #[test]
    fn chat_history_reconstructs_tool_round_trip() {
        let messages = vec![
            OpenAiMessage {
                role: "system".into(),
                content: Some(Value::String("Be terse".into())),
                ..Default::default()
            },
            OpenAiMessage {
                role: "user".into(),
                content: Some(Value::String("weather in SF?".into())),
                ..Default::default()
            },
            OpenAiMessage {
                role: "assistant".into(),
                content: None,
                tool_calls: Some(vec![json!({
                    "id": "call_1",
                    "type": "function",
                    "function": { "name": "get_weather", "arguments": "{\"city\":\"SF\"}" },
                })]),
                tool_call_id: None,
            },
            OpenAiMessage {
                role: "tool".into(),
                content: Some(Value::String("{\"temp\":62}".into())),
                tool_call_id: Some("call_1".into()),
                ..Default::default()
            },
        ];
        let (system, items) = chat_messages_to_realtime(&messages).unwrap();
        assert_eq!(system, "Be terse");
        assert_eq!(items.len(), 3);
        assert_eq!(items[0]["content"][0]["text"], "weather in SF?");
        assert_eq!(items[1]["type"], "function_call");
        assert_eq!(items[1]["name"], "get_weather");
        assert_eq!(items[1]["call_id"], "call_1");
        assert_eq!(items[2]["type"], "function_call_output");
        assert_eq!(items[2]["call_id"], "call_1");
        assert_eq!(items[2]["output"], "{\"temp\":62}");
    }

    #[test]
    fn responses_input_accepts_function_call_items() {
        let input = json!([
            {"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]},
            {"type":"function_call","name":"f","call_id":"c1","arguments":"{}"},
            {"type":"function_call_output","call_id":"c1","output":"done"},
        ]);
        let items = responses_input_to_realtime(&input).unwrap();
        assert_eq!(items.len(), 3);
        assert_eq!(items[1]["type"], "function_call");
        assert_eq!(items[2]["type"], "function_call_output");
        assert_eq!(items[2]["output"], "done");
    }

    fn turn_with_call() -> WarmToolTurn {
        WarmToolTurn {
            text: String::new(),
            tool_calls: vec![rtinfer_core::RealtimeToolCall {
                name: "get_weather".into(),
                call_id: "call_1".into(),
                arguments: json!({"city":"SF"}),
                arguments_raw: "{\"city\":\"SF\"}".into(),
            }],
        }
    }

    #[test]
    fn chat_tool_response_sets_finish_reason_and_tool_calls() {
        let turn = turn_with_call();
        let calls = chat_tool_calls_json(&turn);
        assert_eq!(calls[0]["id"], "call_1");
        assert_eq!(calls[0]["type"], "function");
        assert_eq!(calls[0]["function"]["name"], "get_weather");
        assert_eq!(calls[0]["function"]["arguments"], "{\"city\":\"SF\"}");
        // Builder returns a 200 with the tool_calls choice.
        let resp = openai_chat_tool_response("gpt-realtime-2.1", &turn);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn responses_tool_items_are_function_calls() {
        let items = responses_function_call_items(&turn_with_call());
        assert_eq!(items[0]["type"], "function_call");
        assert_eq!(items[0]["name"], "get_weather");
        assert_eq!(items[0]["call_id"], "call_1");
        assert_eq!(items[0]["status"], "completed");
    }
}
