//! WebSocket session driver.
//!
//! Connects to `wss://api.openai.com/v1/realtime?model=gpt-realtime-2`
//! (or test override), runs the four-step handshake from the JS
//! reference, then assembles `response.output_text.delta` into a single
//! string until `response.done`.

use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use http::Request;
use serde_json::{json, Value};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::generate_key;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, tungstenite::http::Uri};
use tracing::{debug, trace};

use crate::auth::CodexAuth;
use crate::{
    ConversationItem, ConversationItemCreate, FunctionCallOutputCreate, FunctionCallOutputItem,
    InboundEnvelope, InputContent, RealtimeError, RealtimeRequest, RealtimeResponse,
    RealtimeStructuredRequest, RealtimeTool, RealtimeToolCall, RealtimeToolExecutor,
    RealtimeToolRequest, ResponseCreate, ResponseCreateBody, SessionUpdate, SessionUpdateBody,
    DEFAULT_HANDSHAKE_TIMEOUT,
};

/// Wall-clock ceiling for one structured ask. Generous because the user
/// blob can be a full transcript; the caller can override per request.
const STRUCTURED_WALL_CLOCK: Duration = Duration::from_secs(180);

static RUSTLS_PROVIDER: std::sync::Once = std::sync::Once::new();

pub(crate) type RealtimeWs =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

enum FrameEvent {
    Envelope(InboundEnvelope),
    Continue,
    Closed,
}

enum LoopAction {
    Envelope(InboundEnvelope),
    Continue,
    Stop,
}

/// Drive one full Realtime exchange to completion.
pub async fn run_session(
    auth: &CodexAuth,
    endpoint: &str,
    req: RealtimeRequest,
) -> Result<RealtimeResponse, RealtimeError> {
    let (mut ws, t0, connected_ms) =
        open_realtime_session(auth, endpoint, req.handshake_timeout).await?;
    send_session_update(&mut ws, &req.instructions, None, None, None).await?;
    send_context_and_question(&mut ws, &req.context_blobs, &req.question).await?;
    send_response_create(&mut ws).await?;

    // Read loop
    let mut buf = String::new();
    let mut first_token_ms: u128 = 0;
    let mut provider_err: Option<RealtimeError> = None;

    while let Some(frame) = ws.next().await {
        let env = match loop_action(decode_frame(&mut ws, frame).await, &mut provider_err) {
            LoopAction::Envelope(env) => env,
            LoopAction::Continue => continue,
            LoopAction::Stop => break,
        };
        match env.kind.as_str() {
            "response.output_text.delta" => {
                if first_token_ms == 0 {
                    first_token_ms = t0.elapsed().as_millis();
                }
                if let Some(d) = env.delta {
                    buf.push_str(&d);
                }
            }
            "response.done" => {
                break;
            }
            "error" => {
                provider_err = Some(provider_error(env));
                break;
            }
            other => {
                trace!(kind = %other, "realtime: ignoring frame");
            }
        }
    }

    // Best-effort graceful close.
    let _ = ws
        .send(Message::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "done".into(),
        })))
        .await;

    let total_ms = t0.elapsed().as_millis();
    if let Some(e) = provider_err {
        return Err(e);
    }
    Ok(RealtimeResponse {
        text: buf,
        first_token_ms,
        total_ms,
        connected_ms,
    })
}

/// Drive a Realtime exchange with model-requested function calls.
pub async fn run_session_with_tools(
    auth: &CodexAuth,
    endpoint: &str,
    req: RealtimeToolRequest,
    executor: &(dyn RealtimeToolExecutor + Send + Sync),
) -> Result<RealtimeResponse, RealtimeError> {
    let (mut ws, t0, connected_ms) =
        open_realtime_session(auth, endpoint, req.handshake_timeout).await?;
    send_session_update(
        &mut ws,
        &req.instructions,
        None,
        Some(req.tools.clone()),
        Some("auto"),
    )
    .await?;
    send_context_and_question(&mut ws, &req.context_blobs, &req.question).await?;
    send_response_create(&mut ws).await?;

    let mut buf = String::new();
    let mut first_token_ms: u128 = 0;
    let mut provider_err: Option<RealtimeError> = None;
    let mut tool_calls = 0usize;
    let mut total_tool_chars = 0usize;

    loop {
        if t0.elapsed() > req.options.wall_clock_timeout {
            provider_err = Some(RealtimeError::ToolLimit("wall clock timeout".to_string()));
            break;
        }
        let remaining = req
            .options
            .wall_clock_timeout
            .checked_sub(t0.elapsed())
            .unwrap_or_else(|| Duration::from_millis(1));
        let frame = match timeout(remaining, ws.next()).await {
            Ok(Some(frame)) => frame,
            Ok(None) => break,
            Err(_) => {
                provider_err = Some(RealtimeError::ToolLimit("wall clock timeout".to_string()));
                break;
            }
        };
        let env = match loop_action(decode_frame(&mut ws, frame).await, &mut provider_err) {
            LoopAction::Envelope(env) => env,
            LoopAction::Continue => continue,
            LoopAction::Stop => break,
        };
        match env.kind.as_str() {
            "response.output_text.delta" => {
                if first_token_ms == 0 {
                    first_token_ms = t0.elapsed().as_millis();
                }
                if let Some(d) = env.delta {
                    buf.push_str(&d);
                }
            }
            "response.done" => {
                let calls = extract_tool_calls(&env)?;
                if calls.is_empty() {
                    if buf.is_empty() {
                        buf.push_str(&collect_done_text(&env));
                    }
                    break;
                }
                if tool_calls + calls.len() > req.options.max_tool_calls {
                    provider_err = Some(RealtimeError::ToolLimit(format!(
                        "max tool calls exceeded: {} > {}",
                        tool_calls + calls.len(),
                        req.options.max_tool_calls
                    )));
                    break;
                }
                for call in calls {
                    tool_calls += 1;
                    let call_id = call.call_id.clone();
                    let output = timeout(req.options.tool_timeout, executor.execute(call))
                        .await
                        .map_err(|_| RealtimeError::ToolLimit("tool timeout".to_string()))??;
                    let mut output_text = output.output;
                    let remaining_budget = req
                        .options
                        .max_total_tool_result_chars
                        .saturating_sub(total_tool_chars);
                    if output_text.len() > remaining_budget {
                        output_text = if remaining_budget == 0 {
                            r#"{"truncated":true,"hint":"context budget exhausted; commit now"}"#
                                .to_string()
                        } else {
                            let preview: String =
                                output_text.chars().take(remaining_budget).collect();
                            serde_json::json!({
                                "truncated": true,
                                "hint": "context budget exhausted; commit now",
                                "preview": preview
                            })
                            .to_string()
                        };
                    }
                    total_tool_chars += output_text.len();
                    let item = serde_json::to_string(&FunctionCallOutputCreate {
                        kind: "conversation.item.create",
                        item: FunctionCallOutputItem {
                            kind: "function_call_output",
                            call_id: &call_id,
                            output: &output_text,
                        },
                    })?;
                    ws.send(Message::text(item)).await.map_err(io_err)?;
                }
                send_response_create(&mut ws).await?;
            }
            "error" => {
                provider_err = Some(provider_error(env));
                break;
            }
            other => {
                trace!(kind = %other, "realtime: ignoring frame");
            }
        }
    }

    let _ = ws
        .send(Message::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "done".into(),
        })))
        .await;

    let total_ms = t0.elapsed().as_millis();
    if let Some(e) = provider_err {
        return Err(e);
    }
    Ok(RealtimeResponse {
        text: buf,
        first_token_ms,
        total_ms,
        connected_ms,
    })
}

/// Drive one structured ask. The schema rides a single function tool and
/// `tool_choice: "required"` makes calling it the only legal output; the
/// first call's arguments are returned as the structured object. Single
/// tool output by contract: no function result goes back to the model.
/// A model that answers in text anyway is salvaged when the text parses
/// as JSON, and is a protocol error otherwise.
pub async fn run_session_structured(
    auth: &CodexAuth,
    endpoint: &str,
    req: RealtimeStructuredRequest,
) -> Result<Value, RealtimeError> {
    // The realtime model is carried in the endpoint's `model=` query param,
    // not in any wire frame. A per-request `model` override therefore rewrites
    // that param so one pool can serve gpt-realtime-mini and gpt-realtime-2.
    let endpoint = endpoint_for_model(endpoint, req.model.as_deref());
    let (mut ws, t0, _connected_ms) =
        open_realtime_session(auth, &endpoint, req.handshake_timeout).await?;
    let tool = RealtimeTool::function(
        req.schema_name.clone(),
        "Return the structured result. Call exactly once with the complete object.",
        req.schema.clone(),
    );
    send_session_update(
        &mut ws,
        &req.instructions,
        req.temperature,
        Some(vec![tool]),
        Some("required"),
    )
    .await?;
    send_context_and_question(&mut ws, &req.context_blobs, &req.question).await?;
    send_response_create(&mut ws).await?;

    let wall = req.wall_clock_timeout.unwrap_or(STRUCTURED_WALL_CLOCK);
    let mut provider_err: Option<RealtimeError> = None;
    let mut result: Option<Value> = None;
    let mut text_fallback = String::new();
    loop {
        let remaining = match wall.checked_sub(t0.elapsed()) {
            Some(r) => r,
            None => {
                provider_err = Some(RealtimeError::Protocol(
                    "structured ask wall clock timeout".to_string(),
                ));
                break;
            }
        };
        let frame = match timeout(remaining, ws.next()).await {
            Ok(Some(frame)) => frame,
            Ok(None) => break,
            Err(_) => {
                provider_err = Some(RealtimeError::Protocol(
                    "structured ask wall clock timeout".to_string(),
                ));
                break;
            }
        };
        let env = match loop_action(decode_frame(&mut ws, frame).await, &mut provider_err) {
            LoopAction::Envelope(env) => env,
            LoopAction::Continue => continue,
            LoopAction::Stop => break,
        };
        match env.kind.as_str() {
            "response.output_text.delta" => {
                if let Some(d) = env.delta {
                    text_fallback.push_str(&d);
                }
            }
            "response.done" => {
                let mut calls = extract_tool_calls(&env)?;
                if !calls.is_empty() {
                    result = Some(calls.remove(0).arguments);
                } else if text_fallback.is_empty() {
                    text_fallback = collect_done_text(&env);
                }
                break;
            }
            "error" => {
                provider_err = Some(provider_error(env));
                break;
            }
            other => {
                trace!(kind = %other, "realtime: ignoring frame");
            }
        }
    }

    let _ = ws
        .send(Message::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "done".into(),
        })))
        .await;

    if let Some(e) = provider_err {
        return Err(e);
    }
    if let Some(v) = result {
        return Ok(v);
    }
    serde_json::from_str::<Value>(text_fallback.trim()).map_err(|_| {
        RealtimeError::Protocol(
            "structured ask returned no tool call and non-JSON text".to_string(),
        )
    })
}

pub(crate) async fn open_realtime_session(
    auth: &CodexAuth,
    endpoint: &str,
    handshake_timeout: Option<Duration>,
) -> Result<(RealtimeWs, Instant, u128), RealtimeError> {
    install_rustls_provider();
    let started_at = Instant::now();
    let handshake_timeout = handshake_timeout.unwrap_or(DEFAULT_HANDSHAKE_TIMEOUT);
    let client_request = build_request(endpoint, auth)?;
    let connect_fut = connect_async(client_request);
    let (ws, _resp) = match timeout(handshake_timeout, connect_fut).await {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => return Err(RealtimeError::Handshake(format!("{e}"))),
        Err(_) => return Err(RealtimeError::Handshake("timeout".to_owned())),
    };
    let connected_ms = started_at.elapsed().as_millis();
    debug!(connected_ms, "realtime: ws connected");
    Ok((ws, started_at, connected_ms))
}

async fn send_session_update(
    ws: &mut RealtimeWs,
    instructions: &str,
    temperature: Option<f64>,
    tools: Option<Vec<RealtimeTool>>,
    tool_choice: Option<&'static str>,
) -> Result<(), RealtimeError> {
    let session_msg = serde_json::to_string(&SessionUpdate {
        kind: "session.update",
        session: SessionUpdateBody {
            kind: "realtime",
            instructions,
            output_modalities: ["text"],
            temperature,
            tools,
            tool_choice,
        },
    })?;
    ws.send(Message::text(session_msg)).await.map_err(io_err)?;
    Ok(())
}

async fn send_context_and_question(
    ws: &mut RealtimeWs,
    context_blobs: &[String],
    question: &str,
) -> Result<(), RealtimeError> {
    for blob in context_blobs {
        let item = serde_json::to_string(&ConversationItemCreate {
            kind: "conversation.item.create",
            item: ConversationItem {
                kind: "message",
                role: "user",
                content: vec![InputContent {
                    kind: "input_text",
                    text: blob,
                }],
            },
        })?;
        ws.send(Message::text(item)).await.map_err(io_err)?;
    }
    let question_text = format!("QUESTION: {question}");
    let q_item = serde_json::to_string(&ConversationItemCreate {
        kind: "conversation.item.create",
        item: ConversationItem {
            kind: "message",
            role: "user",
            content: vec![InputContent {
                kind: "input_text",
                text: &question_text,
            }],
        },
    })?;
    ws.send(Message::text(q_item)).await.map_err(io_err)?;
    Ok(())
}

async fn send_response_create(ws: &mut RealtimeWs) -> Result<(), RealtimeError> {
    let go = serde_json::to_string(&ResponseCreate {
        kind: "response.create",
        response: ResponseCreateBody {
            output_modalities: ["text"],
        },
    })?;
    ws.send(Message::text(go)).await.map_err(io_err)?;
    Ok(())
}

async fn decode_frame(
    ws: &mut RealtimeWs,
    frame: Result<Message, tokio_tungstenite::tungstenite::Error>,
) -> Result<FrameEvent, RealtimeError> {
    match frame.map_err(|e| RealtimeError::Protocol(format!("ws read: {e}")))? {
        Message::Text(text) => match serde_json::from_str(&text) {
            Ok(env) => Ok(FrameEvent::Envelope(env)),
            Err(e) => {
                // Never log raw frame body — Realtime frames may contain
                // bearer-equivalent material we don't control.
                trace!(error = %e, len = text.len(), "realtime: skipping unparseable frame");
                Ok(FrameEvent::Continue)
            }
        },
        Message::Close(_) => Ok(FrameEvent::Closed),
        Message::Ping(p) => {
            ws.send(Message::Pong(p)).await.map_err(io_err)?;
            Ok(FrameEvent::Continue)
        }
        Message::Pong(_) | Message::Binary(_) | Message::Frame(_) => Ok(FrameEvent::Continue),
    }
}

fn loop_action(
    event: Result<FrameEvent, RealtimeError>,
    provider_err: &mut Option<RealtimeError>,
) -> LoopAction {
    match event {
        Ok(FrameEvent::Envelope(env)) => LoopAction::Envelope(env),
        Ok(FrameEvent::Continue) => LoopAction::Continue,
        Ok(FrameEvent::Closed) => LoopAction::Stop,
        Err(e) => {
            *provider_err = Some(e);
            LoopAction::Stop
        }
    }
}

pub(crate) fn provider_error(env: InboundEnvelope) -> RealtimeError {
    provider_error_from(env.error)
}

/// Build a provider error from an optional error payload (borrow-friendly so a
/// warm read loop holding `&InboundEnvelope` can surface it without consuming).
pub(crate) fn provider_error_from(err: Option<crate::ProviderError>) -> RealtimeError {
    let pe = err.unwrap_or_else(|| crate::ProviderError {
        code: None,
        r#type: None,
        message: Some("(no detail)".into()),
    });
    RealtimeError::Provider {
        code: pe.code_or_type(),
        message: pe.message.unwrap_or_default(),
    }
}

pub(crate) fn extract_tool_calls(
    env: &InboundEnvelope,
) -> Result<Vec<RealtimeToolCall>, RealtimeError> {
    let mut items = Vec::new();
    if let Some(response) = &env.response {
        items.extend(response.output.clone());
    }
    if let Some(output) = &env.output {
        items.extend(output.clone());
    }
    let mut calls = Vec::new();
    for item in items {
        if item.kind.as_deref() != Some("function_call") {
            continue;
        }
        let name = item
            .name
            .ok_or_else(|| RealtimeError::Protocol("function call missing name".to_string()))?;
        let call_id = item.call_id.unwrap_or_else(|| name.clone());
        let arguments_raw = item.arguments.unwrap_or_else(|| "{}".to_string());
        let arguments = serde_json::from_str::<Value>(&arguments_raw).map_err(|e| {
            RealtimeError::Protocol(format!("function call arguments invalid json: {e}"))
        })?;
        calls.push(RealtimeToolCall {
            name,
            call_id,
            arguments,
            arguments_raw,
        });
    }
    Ok(calls)
}

pub(crate) fn collect_done_text(env: &InboundEnvelope) -> String {
    let mut items = Vec::new();
    if let Some(response) = &env.response {
        items.extend(response.output.clone());
    }
    if let Some(output) = &env.output {
        items.extend(output.clone());
    }
    let mut out = String::new();
    for item in items {
        if let Some(text) = item.text {
            out.push_str(&text);
        }
        if let Some(text) = item.output_text {
            out.push_str(&text);
        }
        for content in item.content {
            if let Some(text) = content.text {
                out.push_str(&text);
            }
            if let Some(text) = content.output_text {
                out.push_str(&text);
            }
            if let Some(text) = content.transcript {
                out.push_str(&text);
            }
        }
    }
    out
}

pub(crate) fn io_err(e: tokio_tungstenite::tungstenite::Error) -> RealtimeError {
    RealtimeError::Protocol(format!("ws send: {e}"))
}

/// Send a JSON value as a single WS text frame. Used by the warm-session layer
/// to dispatch an out-of-band `response.create` on an already-open socket.
pub(crate) async fn send_value(ws: &mut RealtimeWs, value: &Value) -> Result<(), RealtimeError> {
    let text = serde_json::to_string(value)?;
    ws.send(Message::text(text)).await.map_err(io_err)
}

/// Read+decode the next inbound envelope on a warm socket, answering pings and
/// skipping unparseable/non-envelope frames. `Ok(None)` on a close frame.
pub(crate) async fn next_envelope(
    ws: &mut RealtimeWs,
) -> Result<Option<InboundEnvelope>, RealtimeError> {
    loop {
        let Some(frame) = ws.next().await else {
            return Ok(None);
        };
        match decode_frame(ws, frame).await? {
            FrameEvent::Envelope(env) => return Ok(Some(env)),
            FrameEvent::Continue => continue,
            FrameEvent::Closed => return Ok(None),
        }
    }
}

/// Prime a freshly-opened warm socket with a bare `session.update` (text output,
/// no tools): tools + instructions ride each out-of-band `response.create`, so
/// the session itself stays generic and reusable across asks.
pub(crate) async fn prime_session(ws: &mut RealtimeWs) -> Result<(), RealtimeError> {
    let msg = json!({
        "type": "session.update",
        "session": { "type": "realtime", "output_modalities": ["text"] },
    });
    send_value(ws, &msg).await
}

/// Best-effort graceful close of a warm socket being retired.
pub(crate) async fn graceful_close(ws: &mut RealtimeWs) -> Result<(), RealtimeError> {
    ws.send(Message::Close(Some(CloseFrame {
        code: CloseCode::Normal,
        reason: "rotate".into(),
    })))
    .await
    .map_err(io_err)
}

/// Outcome of folding one inbound envelope into an in-progress structured ask.
pub(crate) enum AskOutcome {
    /// Keep reading (delta or ignorable frame).
    Pending,
    /// The forced tool call's arguments (the structured result).
    Object(Value),
    /// `response.done` with no tool call: caller salvages accumulated text.
    Done,
}

/// Fold one envelope into a warm structured ask: accumulate text deltas, return
/// the tool-call arguments on `response.done`, or surface a provider error.
pub(crate) fn warm_envelope_outcome(
    env: &InboundEnvelope,
    text_fallback: &mut String,
) -> Result<AskOutcome, RealtimeError> {
    match env.kind.as_str() {
        "response.output_text.delta" => {
            if let Some(d) = &env.delta {
                text_fallback.push_str(d);
            }
            Ok(AskOutcome::Pending)
        }
        "response.done" | "response.completed" => {
            let mut calls = extract_tool_calls(env)?;
            if !calls.is_empty() {
                Ok(AskOutcome::Object(calls.remove(0).arguments))
            } else {
                if text_fallback.is_empty() {
                    text_fallback.push_str(&collect_done_text(env));
                }
                Ok(AskOutcome::Done)
            }
        }
        "response.failed" => Err(provider_error_from(env.error.clone())),
        "error" => Err(provider_error_from(env.error.clone())),
        _ => Ok(AskOutcome::Pending),
    }
}

pub(crate) fn install_rustls_provider() {
    RUSTLS_PROVIDER.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Rewrite the `model=` query parameter of a realtime WebSocket endpoint.
///
/// The realtime API selects the model via the URL query string, so a
/// per-request model override (e.g. `gpt-realtime-mini` for navigators vs
/// `gpt-realtime-2` for scoring) is applied here. `None`, an empty string, or
/// a model equal to the existing param leaves the endpoint untouched. Any
/// existing `model` param is replaced; other params are preserved in order.
pub(crate) fn endpoint_for_model(endpoint: &str, model: Option<&str>) -> String {
    let model = match model {
        Some(m) if !m.trim().is_empty() => m.trim(),
        _ => return endpoint.to_owned(),
    };
    let (base, query) = match endpoint.split_once('?') {
        Some((b, q)) => (b, q),
        None => return format!("{endpoint}?model={model}"),
    };
    let mut parts: Vec<String> = Vec::new();
    let mut replaced = false;
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let key = pair.split('=').next().unwrap_or("");
        if key == "model" {
            parts.push(format!("model={model}"));
            replaced = true;
        } else {
            parts.push(pair.to_owned());
        }
    }
    if !replaced {
        parts.push(format!("model={model}"));
    }
    format!("{base}?{}", parts.join("&"))
}

/// Build the WebSocket upgrade request with all the headers the JS
/// client sends.  Custom headers can't ride on `IntoClientRequest`
/// directly, so we hand-assemble an `http::Request`.
fn build_request(endpoint: &str, auth: &CodexAuth) -> Result<Request<()>, RealtimeError> {
    let uri: Uri = endpoint
        .parse()
        .map_err(|e| RealtimeError::Handshake(format!("invalid endpoint {endpoint}: {e}")))?;

    // Use `IntoClientRequest` to seed a baseline (Sec-WebSocket-Key, Host,
    // Connection: Upgrade, etc.), then layer custom headers on top.
    let mut req = uri
        .into_client_request()
        .map_err(|e| RealtimeError::Handshake(format!("client_request: {e}")))?;

    // Replace the WS-Key with one we control (defensive — `into_client_request`
    // already populates a fresh one).
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
        "codex_cli_rs"
            .parse()
            .map_err(|e| RealtimeError::Handshake(format!("originator invalid: {e}")))?,
    );

    Ok(req)
}

/// Sane default for tests that need a deterministic upper bound on the
/// read loop in case the server forgets to send `response.done`.
#[allow(dead_code)]
pub(crate) const READ_DEADLINE: Duration = Duration::from_secs(120);

#[cfg(test)]
mod tests {
    use super::endpoint_for_model;
    use super::{warm_envelope_outcome, AskOutcome};
    use crate::InboundEnvelope;

    fn env(json_str: &str) -> InboundEnvelope {
        serde_json::from_str(json_str).expect("valid envelope")
    }

    #[test]
    fn warm_outcome_accumulates_text_then_salvages_json_on_done() {
        let mut buf = String::new();
        let d = env(r#"{"type":"response.output_text.delta","delta":"{\"a\":"}"#);
        assert!(matches!(
            warm_envelope_outcome(&d, &mut buf).unwrap(),
            AskOutcome::Pending
        ));
        let d2 = env(r#"{"type":"response.output_text.delta","delta":"1}"}"#);
        assert!(matches!(
            warm_envelope_outcome(&d2, &mut buf).unwrap(),
            AskOutcome::Pending
        ));
        // done with no tool call -> caller salvages the accumulated JSON text.
        let done = env(r#"{"type":"response.done","response":{"output":[]}}"#);
        assert!(matches!(
            warm_envelope_outcome(&done, &mut buf).unwrap(),
            AskOutcome::Done
        ));
        assert_eq!(buf, "{\"a\":1}");
    }

    #[test]
    fn warm_outcome_returns_tool_call_arguments() {
        let mut buf = String::new();
        let done = env(r#"{"type":"response.done","response":{"output":[
                {"type":"function_call","name":"result","call_id":"c1","arguments":"{\"score\":9}"}
            ]}}"#);
        match warm_envelope_outcome(&done, &mut buf).unwrap() {
            AskOutcome::Object(v) => assert_eq!(v["score"], 9),
            _ => panic!("expected tool-call arguments"),
        }
    }

    #[test]
    fn warm_outcome_surfaces_provider_error() {
        let mut buf = String::new();
        let err = env(r#"{"type":"error","error":{"code":"rate_limit","message":"slow down"}}"#);
        let out = warm_envelope_outcome(&err, &mut buf);
        assert!(out.is_err(), "an error frame must surface as Err");
    }

    #[test]
    fn endpoint_for_model_replaces_existing_param() {
        let got = endpoint_for_model(
            "wss://api.openai.com/v1/realtime?model=gpt-realtime-2",
            Some("gpt-realtime-mini"),
        );
        assert_eq!(
            got,
            "wss://api.openai.com/v1/realtime?model=gpt-realtime-mini"
        );
    }

    #[test]
    fn endpoint_for_model_appends_when_absent() {
        let got = endpoint_for_model(
            "wss://api.openai.com/v1/realtime",
            Some("gpt-realtime-mini"),
        );
        assert_eq!(
            got,
            "wss://api.openai.com/v1/realtime?model=gpt-realtime-mini"
        );
    }

    #[test]
    fn endpoint_for_model_preserves_other_params() {
        let got = endpoint_for_model(
            "wss://api.openai.com/v1/realtime?foo=1&model=gpt-realtime-2&bar=2",
            Some("gpt-realtime-mini"),
        );
        assert_eq!(
            got,
            "wss://api.openai.com/v1/realtime?foo=1&model=gpt-realtime-mini&bar=2"
        );
    }

    #[test]
    fn endpoint_for_model_noop_on_none_or_empty() {
        let base = "wss://api.openai.com/v1/realtime?model=gpt-realtime-2";
        assert_eq!(endpoint_for_model(base, None), base);
        assert_eq!(endpoint_for_model(base, Some("   ")), base);
    }
}
