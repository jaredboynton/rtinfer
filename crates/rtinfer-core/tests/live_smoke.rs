//! Manual-only smoke test against the real `gpt-realtime-2.1` endpoint.
//!
//! Skipped by default (`#[ignore]`).  Run explicitly with:
//!
//! ```sh
//! cargo test -p rtinfer-core --test live_smoke -- --ignored --nocapture
//! ```
//!
//! Requires `~/.codex/auth.json` populated by `codex login`.

use std::time::Duration;

use rtinfer_core::{CodexAuth, RealtimeClient, RealtimeRequest};

#[tokio::test]
#[ignore]
async fn live_realtime_returns_non_empty_answer() {
    let auth = match CodexAuth::from_default_path() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("skipping live smoke: {e}");
            return;
        }
    };

    let req = RealtimeRequest {
        instructions: "Answer in one short sentence.".into(),
        context_blobs: vec![],
        question: "What is 2 + 2?".into(),
        model: None,
        handshake_timeout: Some(Duration::from_secs(15)),
    };

    let resp = RealtimeClient::new(auth)
        .ask(req)
        .await
        .expect("live realtime ask failed");
    eprintln!("answer: {}", resp.text);
    eprintln!(
        "timing: connect={}ms first_token={}ms total={}ms",
        resp.connected_ms, resp.first_token_ms, resp.total_ms
    );
    assert!(!resp.text.trim().is_empty(), "live answer was empty");
}
