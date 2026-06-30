//! Cockpit-style fan-out test for [`RealtimePool`].
//!
//! Spawns N concurrent in-process WebSocket servers, each a one-shot
//! that mimics the real Realtime endpoint by replying with two
//! `response.output_text.delta` frames followed by `response.done`.
//! Each pool ask is launched on its own task with no external
//! semaphore — the pool is the only thing that bounds concurrency.
//! The test asserts every ask completes successfully and that the
//! pool never exceeded its configured capacity.

use std::sync::Arc;
use std::time::Duration;

use futures::SinkExt;
use futures::StreamExt;
use rtinfer_core::{CodexAuth, RealtimePool, RealtimeRequest};
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

fn fake_auth() -> CodexAuth {
    CodexAuth {
        access_token: "test-token".into(),
        account_id: "test-account".into(),
        id_token: String::new(),
        refresh_token: String::new(),
        source_path: None,
    }
}

async fn spawn_one_shot_server() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        // Drain at least the four client frames before replying so the
        // server-side state machine matches the real endpoint timing.
        let drain_deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        let mut received = 0;
        while received < 4 && tokio::time::Instant::now() < drain_deadline {
            let next = tokio::time::timeout(Duration::from_millis(150), ws.next()).await;
            match next {
                Ok(Some(Ok(Message::Text(_)))) => received += 1,
                Ok(Some(Ok(_))) => continue,
                Ok(Some(Err(_))) | Ok(None) => break,
                Err(_) => continue,
            }
        }
        let _ = ws
            .send(Message::Text(
                r#"{"type":"response.output_text.delta","delta":"Hi "}"#.into(),
            ))
            .await;
        let _ = ws
            .send(Message::Text(
                r#"{"type":"response.output_text.delta","delta":"there"}"#.into(),
            ))
            .await;
        let _ = ws
            .send(Message::Text(r#"{"type":"response.done"}"#.into()))
            .await;
        let _ = tokio::time::timeout(Duration::from_millis(500), ws.next()).await;
        let _ = ws.close(None).await;
    });
    addr
}

#[tokio::test]
async fn fan_out_25_concurrent_asks_all_succeed() {
    // 25 entities is the cockpit /api/refresh-all peak (per AGENTS.md).
    let n = 25usize;
    let mut servers = Vec::with_capacity(n);
    for _ in 0..n {
        servers.push(spawn_one_shot_server().await);
    }

    // Each server is single-shot, so we use a fresh pool per addr to
    // avoid the (real-world impossible) need to multiplex many
    // distinct endpoints through one pool. This is still a fan-out
    // test of the pool primitive: we exercise permit acquisition +
    // auth caching across many concurrent tasks.
    let mut handles = Vec::with_capacity(n);
    for addr in servers {
        let pool = RealtimePool::builder()
            .initial_auth(fake_auth())
            .endpoint(format!("ws://{addr}/"))
            .capacity(64)
            .build();
        handles.push(tokio::spawn(async move {
            let req = RealtimeRequest {
                instructions: "be brief".into(),
                context_blobs: vec!["CTX".into()],
                question: "ping".into(),
                model: None,
                handshake_timeout: Some(Duration::from_secs(5)),
            };
            pool.ask(req).await
        }));
    }

    let mut ok = 0usize;
    for h in handles {
        let resp = h.await.expect("task join").expect("ask succeed");
        assert_eq!(resp.text, "Hi there");
        ok += 1;
    }
    assert_eq!(ok, n);
}

#[tokio::test]
async fn pool_bounds_in_flight_at_capacity() {
    // Single addr; cap = 2; spawn 5 tasks; verify in_flight() never
    // exceeds 2 across the run. Each task sleeps briefly inside the
    // server reply path so multiple permits would be held simultaneously
    // without the cap.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(p) => p,
                Err(_) => break,
            };
            tokio::spawn(async move {
                let mut ws = match accept_async(stream).await {
                    Ok(w) => w,
                    Err(_) => return,
                };
                let drain_deadline = tokio::time::Instant::now() + Duration::from_millis(800);
                let mut received = 0;
                while received < 4 && tokio::time::Instant::now() < drain_deadline {
                    let next = tokio::time::timeout(Duration::from_millis(200), ws.next()).await;
                    match next {
                        Ok(Some(Ok(Message::Text(_)))) => received += 1,
                        Ok(Some(Ok(_))) => continue,
                        Ok(Some(Err(_))) | Ok(None) => break,
                        Err(_) => continue,
                    }
                }
                tokio::time::sleep(Duration::from_millis(80)).await;
                let _ = ws
                    .send(Message::Text(
                        r#"{"type":"response.output_text.delta","delta":"ok"}"#.into(),
                    ))
                    .await;
                let _ = ws
                    .send(Message::Text(r#"{"type":"response.done"}"#.into()))
                    .await;
                let _ = tokio::time::timeout(Duration::from_millis(200), ws.next()).await;
                let _ = ws.close(None).await;
            });
        }
    });

    let pool = RealtimePool::builder()
        .initial_auth(fake_auth())
        .endpoint(format!("ws://{addr}/"))
        .capacity(2)
        .build();

    let mut handles = Vec::with_capacity(5);
    let observed_max = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    for _ in 0..5 {
        let pool = pool.clone();
        let observed_max = observed_max.clone();
        handles.push(tokio::spawn(async move {
            let req = RealtimeRequest {
                instructions: "x".into(),
                context_blobs: vec!["c".into()],
                question: "q".into(),
                model: None,
                handshake_timeout: Some(Duration::from_secs(5)),
            };
            let fut = pool.ask(req);
            tokio::pin!(fut);
            // Sample in_flight while the ask runs.
            let sampler = {
                let pool = pool.clone();
                let observed_max = observed_max.clone();
                tokio::spawn(async move {
                    for _ in 0..30 {
                        let n = pool.in_flight();
                        let prev = observed_max.load(std::sync::atomic::Ordering::Relaxed);
                        if n > prev {
                            observed_max.store(n, std::sync::atomic::Ordering::Relaxed);
                        }
                        tokio::time::sleep(Duration::from_millis(15)).await;
                    }
                })
            };
            let r = fut.await;
            sampler.abort();
            r
        }));
    }
    for h in handles {
        h.await.expect("task join").expect("ask succeed");
    }
    let max = observed_max.load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        max <= 2,
        "pool exceeded configured capacity 2: observed in_flight = {max}"
    );
    // After everything completes, in_flight should drain back to 0.
    assert_eq!(pool.in_flight(), 0);
}
