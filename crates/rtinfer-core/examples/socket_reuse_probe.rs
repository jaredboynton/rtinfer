//! Probe: does one codex/responses WebSocket accept multiple sequential
//! response.create frames? Run: cargo run --release -p rtinfer-core --example socket_reuse_probe
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::generate_key;
use tokio_tungstenite::tungstenite::http::Uri;
use tokio_tungstenite::tungstenite::Message;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let auth = rtinfer_core::CodexAuth::from_default_path()?;

    let uri: Uri = rtinfer_core::CODEX_RESPONSES_URL.parse()?;
    let mut req = uri.into_client_request()?;
    let key = generate_key();
    let headers = req.headers_mut();
    headers.insert("sec-websocket-key", key.parse()?);
    headers.insert(
        "authorization",
        format!("Bearer {}", auth.access_token).parse()?,
    );
    if !auth.account_id.is_empty() {
        headers.insert("chatgpt-account-id", auth.account_id.parse()?);
    }
    headers.insert(
        "originator",
        rtinfer_core::CODEX_RESPONSES_ORIGINATOR.parse()?,
    );
    headers.insert(
        "user-agent",
        rtinfer_core::CODEX_RESPONSES_USER_AGENT.parse()?,
    );
    headers.insert("openai-beta", rtinfer_core::CODEX_RESPONSES_BETA.parse()?);

    let (mut ws, _resp) = timeout(
        Duration::from_secs(15),
        tokio_tungstenite::connect_async(req),
    )
    .await??;
    println!("handshake ok");

    let schema = json!({"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"],"additionalProperties":false});
    let frame = |i: usize| {
        json!({
            "type": "response.create",
            "model": "gpt-5.4",
            "instructions": "Return JSON only.",
            "input": [{"type":"message","role":"user","content":[{"type":"input_text","text":format!("Return {{\"ok\":true}} probe #{i}")}]}],
            "reasoning": {"effort":"low"},
            "store": false,
            "stream": true,
            "include": [],
            "service_tier": "priority",
            "text": {"verbosity":"low","format": {"type":"json_schema","name":"p","strict":true,"schema": schema.clone()}}
        })
    };

    let started = Instant::now();
    let mut completed = 0usize;
    ws.send(Message::text(frame(1).to_string())).await?;
    loop {
        let msg = match timeout(Duration::from_secs(60), ws.next()).await {
            Ok(Some(Ok(m))) => m,
            Ok(Some(Err(e))) => {
                println!("ws error after {}ms: {e}", started.elapsed().as_millis());
                break;
            }
            Ok(None) => {
                println!(
                    "socket closed by server after {}ms (completed={completed})",
                    started.elapsed().as_millis()
                );
                break;
            }
            Err(_) => {
                println!("read timeout (completed={completed})");
                break;
            }
        };
        if let Message::Text(text) = msg {
            if let Ok(v) = serde_json::from_str::<Value>(&text) {
                match v.get("type").and_then(Value::as_str) {
                    Some("response.completed") => {
                        completed += 1;
                        println!(
                            "response.completed #{completed} at {}ms",
                            started.elapsed().as_millis()
                        );
                        if completed >= 3 {
                            println!("SOCKET REUSE WORKS: 3 responses on one connection");
                            break;
                        }
                        ws.send(Message::text(frame(completed + 1).to_string()))
                            .await?;
                    }
                    Some("error") => {
                        println!(
                            "server error frame: {}",
                            text.chars().take(300).collect::<String>()
                        );
                        break;
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}
