//! End-to-end protocol tests against a local in-process WebSocket server.
//!
//! Spins up `tokio_tungstenite::accept_async` over `TcpListener` on
//! 127.0.0.1, captures every text frame the client sends, replays
//! canned `response.output_text.delta` + `response.done` (or `error`)
//! frames, and asserts the assembled response.

use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use rtinfer_core::{
    CodexAuth, RealtimeClient, RealtimeError, RealtimeRequest, RealtimeToolCall,
    RealtimeToolExecutor, RealtimeToolOutput, RealtimeToolRequest,
};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

#[derive(Clone)]
struct ServerHandle {
    /// Frames the client sent to the server, in order.
    captured: Arc<Mutex<Vec<String>>>,
    addr: std::net::SocketAddr,
}

/// Spawn a one-shot WS server: it accepts a single connection, reads
/// every text frame from the client, and sends `replies` in order.
async fn spawn_server(replies: Vec<&'static str>) -> ServerHandle {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let captured: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    let cap = Arc::clone(&captured);
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();

        // Drain everything the client sends until we've absorbed at least 4
        // frames (session.update + 1 ctx + question + response.create) or
        // 250ms idle, whichever comes first — then start the reply stream.
        let drain_deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        let mut received = 0;
        while received < 4 && tokio::time::Instant::now() < drain_deadline {
            let next = tokio::time::timeout(Duration::from_millis(200), ws.next()).await;
            match next {
                Ok(Some(Ok(Message::Text(t)))) => {
                    cap.lock().await.push(t.to_string());
                    received += 1;
                }
                Ok(Some(Ok(_))) => continue,
                Ok(Some(Err(_))) | Ok(None) => break,
                Err(_) => continue, // idle tick
            }
        }

        // Replay canned responses.
        for frame in replies {
            let _ = ws.send(Message::text(frame.to_string())).await;
        }

        // Wait briefly for the client to read everything before closing.
        let _ = tokio::time::timeout(Duration::from_millis(500), ws.next()).await;
        let _ = ws.close(None).await;
    });

    ServerHandle { captured, addr }
}

async fn spawn_tool_server(replies: Vec<(&'static str, usize)>, idle_ms: u64) -> ServerHandle {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let captured: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    let cap = Arc::clone(&captured);
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        let mut received = 0;
        for (frame, wait_for_frames) in replies {
            while received < wait_for_frames {
                let next = tokio::time::timeout(Duration::from_millis(idle_ms), ws.next()).await;
                match next {
                    Ok(Some(Ok(Message::Text(t)))) => {
                        cap.lock().await.push(t.to_string());
                        received += 1;
                    }
                    Ok(Some(Ok(_))) => continue,
                    Ok(Some(Err(_))) | Ok(None) => break,
                    Err(_) => break,
                }
            }
            let _ = ws.send(Message::text(frame.to_string())).await;
        }
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        while tokio::time::Instant::now() < deadline {
            let next = tokio::time::timeout(Duration::from_millis(50), ws.next()).await;
            match next {
                Ok(Some(Ok(Message::Text(t)))) => cap.lock().await.push(t.to_string()),
                Ok(Some(Ok(_))) => continue,
                _ => break,
            }
        }
        let _ = ws.close(None).await;
    });

    ServerHandle { captured, addr }
}

fn fake_auth() -> CodexAuth {
    CodexAuth {
        access_token: "test-token".into(),
        account_id: "test-account".into(),
        id_token: String::new(),
        refresh_token: String::new(),
        source_path: None,
    }
}

#[derive(Default)]
struct EchoExecutor {
    calls: Arc<Mutex<Vec<RealtimeToolCall>>>,
}

#[async_trait::async_trait]
impl RealtimeToolExecutor for EchoExecutor {
    async fn execute(&self, call: RealtimeToolCall) -> Result<RealtimeToolOutput, RealtimeError> {
        self.calls.lock().await.push(call);
        Ok(RealtimeToolOutput::text(
            r#"{"ok":true,"value":"tool-result"}"#,
        ))
    }
}

#[tokio::test]
async fn assembles_deltas_into_text_response() {
    let server = spawn_server(vec![
        r#"{"type":"response.output_text.delta","delta":"Hel"}"#,
        r#"{"type":"response.output_text.delta","delta":"lo, "}"#,
        r#"{"type":"response.output_text.delta","delta":"world."}"#,
        r#"{"type":"response.done"}"#,
    ])
    .await;
    let url = format!("ws://{}/", server.addr);

    let client = RealtimeClient::new(fake_auth()).with_endpoint(url);
    let req = RealtimeRequest {
        instructions: "be concise".into(),
        context_blobs: vec!["CTX_BLOB".into()],
        question: "say hi".into(),
        model: None,
        handshake_timeout: Some(Duration::from_secs(5)),
    };

    let resp = client.ask(req).await.expect("ask must succeed");
    assert_eq!(resp.text, "Hello, world.");
    // `connected_ms` may be 0 on a fast loopback; we just want to confirm
    // total timing is at least as large as connect timing.
    assert!(resp.total_ms >= resp.connected_ms);

    // Verify frame order: session.update, conversation.item.create (ctx),
    // conversation.item.create (question), response.create.
    let captured = server.captured.lock().await.clone();
    assert!(captured.len() >= 4, "got {} frames", captured.len());
    assert!(captured[0].contains(r#""type":"session.update""#));
    assert!(captured[0].contains(r#""instructions":"be concise""#));
    assert!(captured[1].contains(r#""type":"conversation.item.create""#));
    assert!(captured[1].contains("CTX_BLOB"));
    assert!(captured[2].contains(r#""type":"conversation.item.create""#));
    assert!(captured[2].contains("QUESTION: say hi"));
    assert!(captured[3].contains(r#""type":"response.create""#));
}

#[tokio::test]
async fn provider_error_frame_surfaces_typed_error() {
    let server = spawn_server(vec![
        r#"{"type":"error","error":{"code":"invalid_request","type":"invalid_request_error","message":"bad model"}}"#,
    ])
    .await;
    let url = format!("ws://{}/", server.addr);

    let client = RealtimeClient::new(fake_auth()).with_endpoint(url);
    let req = RealtimeRequest {
        instructions: "x".into(),
        context_blobs: vec![],
        question: "y".into(),
        model: None,
        handshake_timeout: Some(Duration::from_secs(5)),
    };

    let err = client.ask(req).await.expect_err("must fail");
    match err {
        RealtimeError::Provider { code, message } => {
            assert_eq!(code, "invalid_request");
            assert_eq!(message, "bad model");
        }
        other => panic!("wrong error: {other:?}"),
    }
}

#[tokio::test]
async fn ignores_unknown_frame_types() {
    let server = spawn_server(vec![
        r#"{"type":"response.created"}"#,
        r#"{"type":"rate_limits.updated","limits":[]}"#,
        r#"{"type":"response.output_text.delta","delta":"ok"}"#,
        r#"{"type":"response.done"}"#,
    ])
    .await;
    let url = format!("ws://{}/", server.addr);

    let client = RealtimeClient::new(fake_auth()).with_endpoint(url);
    let req = RealtimeRequest {
        instructions: "x".into(),
        context_blobs: vec![],
        question: "y".into(),
        model: None,
        handshake_timeout: Some(Duration::from_secs(5)),
    };

    let resp = client.ask(req).await.expect("ok");
    assert_eq!(resp.text, "ok");
}

#[tokio::test]
async fn no_context_blobs_still_sends_question_and_create() {
    let server = spawn_server(vec![
        r#"{"type":"response.output_text.delta","delta":"empty-ctx"}"#,
        r#"{"type":"response.done"}"#,
    ])
    .await;
    let url = format!("ws://{}/", server.addr);

    let client = RealtimeClient::new(fake_auth()).with_endpoint(url);
    let req = RealtimeRequest {
        instructions: "be terse".into(),
        context_blobs: vec![],
        question: "hi".into(),
        model: None,
        handshake_timeout: Some(Duration::from_secs(5)),
    };

    let resp = client.ask(req).await.expect("ok");
    assert_eq!(resp.text, "empty-ctx");

    let captured = server.captured.lock().await.clone();
    // session.update, conversation.item.create (question), response.create
    assert!(captured.len() >= 3, "got {} frames", captured.len());
    assert!(captured[0].contains("session.update"));
    assert!(captured[1].contains("QUESTION: hi"));
    assert!(captured[2].contains("response.create"));
}

#[tokio::test]
async fn tool_loop_sends_function_output_and_followup_response() {
    let server = spawn_tool_server(
        vec![
            (
                r#"{"type":"response.done","response":{"output":[{"type":"function_call","name":"slack_search_readonly","call_id":"call_1","arguments":"{\"query\":\"from:jared\",\"limit\":1}"}]}}"#,
                4,
            ),
            (
                r#"{"type":"response.done","response":{"output":[{"type":"message","content":[{"type":"output_text","text":"{\"ok\":true}"}]}]}}"#,
                6,
            ),
        ],
        200,
    )
    .await;
    let url = format!("ws://{}/", server.addr);
    let client = RealtimeClient::new(fake_auth()).with_endpoint(url);
    let executor = EchoExecutor::default();

    let resp = client
        .ask_with_tools(
            RealtimeToolRequest {
                instructions: "use tools".into(),
                context_blobs: vec!["CTX".into()],
                question: "refresh".into(),
                model: None,
                handshake_timeout: Some(Duration::from_secs(5)),
                tools: vec![rtinfer_core::RealtimeTool::function(
                    "slack_search_readonly",
                    "Search Slack",
                    serde_json::json!({"type":"object","properties":{"query":{"type":"string"}}}),
                )],
                options: Default::default(),
            },
            &executor,
        )
        .await
        .expect("tool loop succeeds");

    assert_eq!(resp.text, r#"{"ok":true}"#);
    let calls = executor.calls.lock().await.clone();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].name, "slack_search_readonly");
    assert_eq!(calls[0].arguments["query"], "from:jared");

    let captured = server.captured.lock().await.clone();
    assert!(captured[0].contains(r#""tools":[{"type":"function""#));
    assert!(
        captured
            .iter()
            .any(|f| f.contains(r#""type":"function_call_output""#) && f.contains("call_1")),
        "missing function output in frames: {captured:?}"
    );
    assert!(
        captured
            .iter()
            .filter(|f| f.contains("response.create"))
            .count()
            >= 2,
        "expected initial and follow-up response.create frames: {captured:?}"
    );
}
