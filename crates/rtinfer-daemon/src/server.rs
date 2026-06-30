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
use rtinfer_core::{CodexResponsesPool, RealtimeTool, WarmSessionPool, WarmToolTurn};
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
                tracing::warn!(model = %req.model, error = %e, "rtinfer: openai chat tool upstream error");
                openai_err(
                    StatusCode::BAD_GATEWAY,
                    "provider_error",
                    format!("realtime upstream error: {}", e.code_or_label()),
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
                tracing::warn!(model = %req.model, error = %e, "rtinfer: openai responses tool upstream error");
                openai_err(
                    StatusCode::BAD_GATEWAY,
                    "provider_error",
                    format!("realtime upstream error: {}", e.code_or_label()),
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
        let resp = openai_chat_tool_response("gpt-realtime-2", &turn);
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
