//! Probe: does one codex/responses WebSocket accept multiple sequential
//! response.create frames via CodexResponsesClient?
//!
//! Run: cargo run --release -p rtinfer-core --example socket_reuse_probe

use rtinfer_core::{CodexResponsesClient, ResponsesTransportMode};
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let client = CodexResponsesClient::builder()
        .mode(ResponsesTransportMode::Wss)
        .build()?;

    let started = Instant::now();
    for i in 1..=3 {
        let text = client
            .ask_text(
                "Return the single word ok.",
                &format!("Probe #{i}: reply with ok"),
                None,
            )
            .await?;
        println!(
            "ask #{i} ok ({} chars) at {}ms",
            text.len(),
            started.elapsed().as_millis()
        );
    }
    let snap = client.snapshot().await;
    println!(
        "SOCKET REUSE PROBE: handshakes={} idle={} dispatches={}",
        snap.wss_handshake_attempts, snap.wss_idle_sockets, snap.wss_dispatches
    );
    if snap.wss_handshake_attempts == 1 && snap.wss_dispatches == 3 {
        println!("SOCKET REUSE WORKS: 3 responses on one connection");
    } else {
        println!(
            "note: handshake_attempts={} (reuse expected when edge keeps the socket)",
            snap.wss_handshake_attempts
        );
    }
    Ok(())
}
