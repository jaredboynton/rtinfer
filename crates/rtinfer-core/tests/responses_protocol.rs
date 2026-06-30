//! codex/responses protocol replay — NON-CIRCULAR fixtures.
//!
//! The frames in `fixtures/codex_responses_worktime.jsonl` were captured
//! from a REAL live round-trip against
//! `wss://chatgpt.com/backend-api/codex/responses` (gpt-5.5, strict
//! json_schema) on 2026-06-07 using the worktime-adjudication schema and
//! the Jared 11:06PM->12:13AM demo-sandbox prompt. They are the ground
//! truth for the stream grammar, not frames we authored from assumptions.
//!
//! The assembler under test (`assemble_codex_responses_text`) is the SAME
//! per-frame logic the live `CodexResponsesPool` read loop runs, so a pass
//! here proves the parser matches reality.

use serde_json::Value;

fn load_frames() -> Vec<Value> {
    let raw = include_str!("fixtures/codex_responses_worktime.jsonl");
    raw.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str::<Value>(l).expect("fixture line is valid json"))
        .collect()
}

#[test]
fn real_frames_assemble_to_worktime_adjudication() {
    let frames = load_frames();
    assert!(frames.len() > 50, "fixture should hold the full stream");

    let text = rtinfer_core::assemble_codex_responses_text(&frames)
        .expect("real stream must assemble without a provider error");

    // The assembled text is the strict-schema object the model produced.
    let obj: Value = serde_json::from_str(&text).expect("assembled text must be valid JSON");
    assert_eq!(obj["is_real_work"], Value::Bool(true));
    assert_eq!(obj["start"], "2026-06-05T23:06:00");
    assert_eq!(obj["end"], "2026-06-06T00:13:00");
    assert_eq!(obj["kind"], "enablement");
    assert!(
        obj["rationale"].as_str().unwrap_or("").len() > 10,
        "rationale should be present"
    );
}

#[test]
fn terminal_frame_is_response_completed_not_done() {
    // Guards the corrected assumption: the codex/responses stream
    // terminates on `response.completed`, and `response.output` is EMPTY
    // in streaming text mode (the text lives only in the delta/.done
    // frames). If a future capture changes this, this test flags it.
    let frames = load_frames();
    let has_completed = frames.iter().any(|f| f["type"] == "response.completed");
    let has_done = frames.iter().any(|f| f["type"] == "response.done");
    assert!(has_completed, "stream must contain response.completed");
    assert!(!has_done, "codex/responses uses .completed, not .done");

    let completed = frames
        .iter()
        .find(|f| f["type"] == "response.completed")
        .unwrap();
    let output = &completed["response"]["output"];
    assert!(
        output.as_array().map(|a| a.is_empty()).unwrap_or(true),
        "response.output is empty in streaming text mode; text comes from deltas"
    );
}

#[test]
fn error_frame_surfaces_typed_error() {
    // A synthetic error frame in the position of a real stream must
    // produce a typed provider error, not a silent empty string.
    let frames = vec![
        serde_json::json!({"type": "response.created"}),
        serde_json::json!({"type": "error", "error": {"code": "rate_limit_exceeded", "message": "slow down"}}),
    ];
    let err = rtinfer_core::assemble_codex_responses_text(&frames)
        .expect_err("error frame must produce an Err");
    let msg = format!("{err}");
    assert!(msg.contains("rate_limit_exceeded"), "got: {msg}");
}

/// Live smoke: drives the REAL `CodexResponsesPool` against
/// `wss://chatgpt.com/backend-api/codex/responses` using `~/.codex/auth.json`.
/// Gated `#[ignore]` (network + creds). Run with:
///   cargo test -p rtinfer-core --test responses_protocol -- --ignored live_smoke
#[tokio::test]
#[ignore]
async fn live_smoke_ask_structured_round_trip() {
    let auth_path = rtinfer_core::default_auth_path().expect("auth path");
    let pool = rtinfer_core::CodexResponsesPool::builder()
        .auth_path(auth_path)
        .build();
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "is_real_work": {"type": "boolean"},
            "start": {"type": "string"},
            "end": {"type": "string"},
            "label": {"type": "string"},
            "kind": {"type": "string", "enum": ["collaboration","deliverable","investigation","enablement","other"]},
            "rationale": {"type": "string"}
        }
    });
    let out = pool
        .ask_structured(
            "You estimate real work intervals. Reply only with the structured object.",
            "At 23:06 a teammate said 'working with James to get the demo sandbox up'. At 00:13 they posted a Loom 'Everything is set up'. Estimate the work interval (ISO8601 local, 2026-06-05/06).",
            "worktime_adjudication",
            schema,
        )
        .await
        .expect("live round-trip must succeed");
    assert_eq!(out["is_real_work"], serde_json::Value::Bool(true));
    assert!(out["start"].as_str().unwrap().starts_with("2026-06-05T23"));
    eprintln!("LIVE OK: {out}");
}

#[test]
fn done_text_authoritative_over_partial_deltas() {
    // If output_text.done carries the full text, it wins even when the
    // deltas were (hypothetically) dropped.
    let frames = vec![
        serde_json::json!({"type": "response.output_text.delta", "delta": "{\"ok\":"}),
        serde_json::json!({"type": "response.output_text.done", "text": "{\"ok\":true}"}),
        serde_json::json!({"type": "response.completed", "response": {"output": []}}),
    ];
    let text = rtinfer_core::assemble_codex_responses_text(&frames).unwrap();
    assert_eq!(text, "{\"ok\":true}");
}
