//! Ignored credentialed live proofs for [`CodexResponsesClient`].
//!
//! Opt-in only. These tests fail clearly when ChatGPT auth is missing or
//! unusable; they never silently skip.
//!
//! ```sh
//! cargo test -p rtinfer-core --test responses_dual_live -- --ignored --nocapture
//! ```
//!
//! Safety: prompts are tiny and deterministic; tests never print prompts,
//! model output, tokens, auth/account data, or raw frames.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use rtinfer_core::{
    CodexAuth, CodexResponsesClient, ResponsesTransportMode, CODEX_RESPONSES_MODEL,
};
use tracing::instrument::WithSubscriber;

/// Tiny deterministic ask; never logged or asserted by content.
const SYSTEM: &str = "Reply with the single digit 1.";
const USER: &str = "1";

type FieldMap = BTreeMap<String, String>;

#[derive(Default)]
struct FieldCollector {
    fields: Vec<(String, String)>,
}

impl tracing::field::Visit for FieldCollector {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.fields
            .push((field.name().to_string(), format!("{value:?}")));
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }
    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }
}

/// Captures only `rtinfer_core::responses_completion` events (no global subscriber).
#[derive(Clone, Default)]
struct CaptureSubscriber {
    events: Arc<Mutex<Vec<FieldMap>>>,
}

impl tracing::Subscriber for CaptureSubscriber {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target() == "rtinfer_core::responses_completion"
    }
    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        if event.metadata().target() != "rtinfer_core::responses_completion" {
            return;
        }
        let mut collector = FieldCollector::default();
        event.record(&mut collector);
        let map: FieldMap = collector.fields.into_iter().collect();
        self.events.lock().unwrap().push(map);
    }
    fn enter(&self, _span: &tracing::span::Id) {}
    fn exit(&self, _span: &tracing::span::Id) {}
}

fn new_capture() -> (tracing::Dispatch, Arc<Mutex<Vec<FieldMap>>>) {
    let sub = CaptureSubscriber::default();
    let events = Arc::clone(&sub.events);
    (tracing::Dispatch::new(sub), events)
}

/// Same acceptance set as production `is_http2_version` (Warpsock forms).
fn is_http2_version(version: &str) -> bool {
    let v = version.trim().to_ascii_lowercase();
    v == "http/2" || v == "http/2.0" || v == "h2" || v == "2" || v.starts_with("http/2")
}

fn install_rustls() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Load usable ChatGPT auth or panic with a non-sensitive failure reason.
fn require_usable_auth() -> CodexAuth {
    match CodexAuth::from_default_path() {
        Ok(auth) => auth,
        Err(e) => panic!(
            "usable ChatGPT auth required (run `codex login`); auth load failed: {}",
            e.code_or_label()
        ),
    }
}

fn build_client(mode: ResponsesTransportMode, auth: CodexAuth) -> Arc<CodexResponsesClient> {
    CodexResponsesClient::builder()
        .mode(mode)
        .initial_auth(auth)
        .model(CODEX_RESPONSES_MODEL)
        .build()
        .unwrap_or_else(|e| panic!("CodexResponsesClient build failed: {}", e.code_or_label()))
}

/// Semantic success requires non-empty assembled text after `response.completed`.
async fn ask_ok(client: &CodexResponsesClient, dispatch: tracing::Dispatch) -> String {
    match client
        .ask_text(SYSTEM, USER, None)
        .with_subscriber(dispatch)
        .await
    {
        Ok(text) => {
            assert!(
                !text.is_empty(),
                "semantic success requires non-empty assembled text"
            );
            text
        }
        Err(e) => panic!("semantic ask failed: {}", e.code_or_label()),
    }
}

fn assert_success_completion(map: &FieldMap) {
    assert_eq!(
        map.get("result_class").map(String::as_str),
        Some("success"),
        "expected result_class=success, got {map:?}"
    );
    assert_eq!(
        map.get("terminal_seen").map(String::as_str),
        Some("true"),
        "expected terminal_seen=true, got {map:?}"
    );
}

fn assert_http2_lane(map: &FieldMap) {
    assert_eq!(
        map.get("lane").map(String::as_str),
        Some("http"),
        "expected lane=http, got {map:?}"
    );
    let hv = map
        .get("http_version")
        .map(String::as_str)
        .unwrap_or("<missing>");
    assert!(
        is_http2_version(hv),
        "expected HTTP/2 http_version (HTTP/2, HTTP/2.0, h2, 2, or HTTP/2-prefixed), got {hv:?}"
    );
}

#[tokio::test]
#[ignore = "credentialed live proof; requires ChatGPT auth"]
async fn live_http_endpoint_is_http2() {
    install_rustls();
    let auth = require_usable_auth();
    let client = build_client(ResponsesTransportMode::Http, auth);
    let before = client.snapshot().await;
    let (dispatch, events) = new_capture();

    let text = match client
        .ask_text(SYSTEM, USER, None)
        .with_subscriber(dispatch)
        .await
    {
        Ok(t) => t,
        Err(e) => {
            panic!(
                "HTTP/2 live proof failed (expected semantic success on HTTP/2): {}",
                e.code_or_label()
            );
        }
    };
    assert!(
        !text.is_empty(),
        "semantic success requires non-empty assembled text"
    );

    let captured = events.lock().unwrap().clone();
    assert_eq!(
        captured.len(),
        1,
        "expected exactly one completion event for the ask, got {}",
        captured.len()
    );
    assert_success_completion(&captured[0]);
    assert_http2_lane(&captured[0]);

    let after = client.snapshot().await;
    assert_eq!(
        after.http_dispatches.saturating_sub(before.http_dispatches),
        1,
        "HTTP dispatch delta must be exactly 1"
    );
    assert_eq!(
        after
            .adaptive
            .http
            .successes
            .saturating_sub(before.adaptive.http.successes),
        1,
        "HTTP success delta must be exactly 1"
    );
}

#[tokio::test]
#[ignore = "credentialed live proof; requires ChatGPT auth"]
async fn live_sequential_http_reuses_connection() {
    install_rustls();
    let auth = require_usable_auth();
    let client = build_client(ResponsesTransportMode::Http, auth);
    let before = client.snapshot().await;
    let (dispatch, events) = new_capture();

    let a = ask_ok(&client, dispatch.clone()).await;
    let b = ask_ok(&client, dispatch).await;
    assert!(!a.is_empty() && !b.is_empty());

    let captured = events.lock().unwrap().clone();
    assert_eq!(
        captured.len(),
        2,
        "expected one completion event per successful ask, got {}",
        captured.len()
    );
    for map in &captured {
        assert_success_completion(map);
        assert_http2_lane(map);
    }

    let after = client.snapshot().await;
    assert_eq!(
        after.http_dispatches.saturating_sub(before.http_dispatches),
        2
    );
    assert_eq!(
        after
            .adaptive
            .http
            .successes
            .saturating_sub(before.adaptive.http.successes),
        2
    );
    assert!(
        after.http_connection_reuse_count > before.http_connection_reuse_count,
        "two sequential HTTP asks must increase connection_reuse_count \
         (before={}, after={})",
        before.http_connection_reuse_count,
        after.http_connection_reuse_count
    );
}

#[tokio::test]
#[ignore = "credentialed live proof; requires ChatGPT auth"]
async fn live_sequential_wss_reuses_one_handshake() {
    install_rustls();
    let auth = require_usable_auth();
    let client = build_client(ResponsesTransportMode::Wss, auth);
    let before = client.snapshot().await;
    let (dispatch, events) = new_capture();

    let a = ask_ok(&client, dispatch.clone()).await;
    let b = ask_ok(&client, dispatch).await;
    assert!(!a.is_empty() && !b.is_empty());

    let captured = events.lock().unwrap().clone();
    assert_eq!(
        captured.len(),
        2,
        "expected one completion event per successful ask, got {}",
        captured.len()
    );
    for map in &captured {
        assert_success_completion(map);
        assert_eq!(
            map.get("lane").map(String::as_str),
            Some("wss"),
            "expected lane=wss, got {map:?}"
        );
    }

    let after = client.snapshot().await;
    let handshake_delta = after
        .wss_handshake_attempts
        .saturating_sub(before.wss_handshake_attempts);
    let dispatch_delta = after.wss_dispatches.saturating_sub(before.wss_dispatches);
    let success_delta = after
        .adaptive
        .websocket
        .successes
        .saturating_sub(before.adaptive.websocket.successes);
    assert_eq!(dispatch_delta, 2, "expected two WSS dispatches");
    assert_eq!(success_delta, 2, "expected two WSS semantic successes");
    assert_eq!(
        handshake_delta, 1,
        "two sequential WSS asks must use exactly one handshake \
         (handshake_delta={handshake_delta}, dispatch_delta={dispatch_delta})"
    );
}

#[tokio::test]
#[ignore = "credentialed live proof; requires ChatGPT auth"]
async fn live_dual_records_semantic_successes_on_each_lane() {
    install_rustls();
    let auth = require_usable_auth();
    let client = build_client(ResponsesTransportMode::Dual, auth);
    let before = client.snapshot().await;
    let (dispatch, events) = new_capture();

    const N: usize = 12;
    let mut joins = Vec::with_capacity(N);
    for _ in 0..N {
        let c = Arc::clone(&client);
        let d = dispatch.clone();
        joins.push(tokio::spawn(async move {
            c.ask_text(SYSTEM, USER, None).with_subscriber(d).await
        }));
    }

    let mut semantic_ok = 0u64;
    let mut failures = 0u64;
    for join in joins {
        match join.await {
            Ok(Ok(text)) => {
                assert!(!text.is_empty(), "semantic success requires non-empty text");
                semantic_ok += 1;
            }
            Ok(Err(e)) => {
                failures += 1;
                let _ = e.code_or_label();
            }
            Err(_) => failures += 1,
        }
    }

    let captured = events.lock().unwrap().clone();
    let success_events: Vec<&FieldMap> = captured
        .iter()
        .filter(|m| m.get("result_class").map(String::as_str) == Some("success"))
        .collect();
    assert_eq!(
        success_events.len() as u64,
        semantic_ok,
        "success completion events must equal semantic_ok \
         (events={}, semantic_ok={semantic_ok}, failures={failures})",
        success_events.len()
    );
    for map in &success_events {
        assert_eq!(
            map.get("terminal_seen").map(String::as_str),
            Some("true"),
            "every success event must have terminal_seen=true, got {map:?}"
        );
    }
    let http2_successes = success_events
        .iter()
        .filter(|m| {
            m.get("lane").map(String::as_str) == Some("http")
                && m.get("http_version")
                    .map(|v| is_http2_version(v))
                    .unwrap_or(false)
        })
        .count();
    let wss_successes = success_events
        .iter()
        .filter(|m| m.get("lane").map(String::as_str) == Some("wss"))
        .count();
    assert!(
        http2_successes >= 3,
        "dual mode need >=3 success events with lane=http and HTTP/2 \
         (http2_successes={http2_successes}, wss_successes={wss_successes})"
    );
    assert!(
        wss_successes >= 3,
        "dual mode need >=3 success events with lane=wss \
         (http2_successes={http2_successes}, wss_successes={wss_successes})"
    );

    let after = client.snapshot().await;
    let http_ok = after
        .adaptive
        .http
        .successes
        .saturating_sub(before.adaptive.http.successes);
    let wss_ok = after
        .adaptive
        .websocket
        .successes
        .saturating_sub(before.adaptive.websocket.successes);
    let http_disp = after.http_dispatches.saturating_sub(before.http_dispatches);
    let wss_disp = after.wss_dispatches.saturating_sub(before.wss_dispatches);

    assert!(
        semantic_ok >= 6,
        "dual cohort needed semantic successes; ok={semantic_ok} failures={failures}"
    );
    assert!(
        http_ok >= 3 && http_disp >= 3,
        "dual mode must record >=3 HTTP semantic successes \
         (http_success_delta={http_ok}, http_dispatch_delta={http_disp}, \
          wss_success_delta={wss_ok}, wss_dispatch_delta={wss_disp})"
    );
    assert!(
        wss_ok >= 3 && wss_disp >= 3,
        "dual mode must record >=3 WSS semantic successes \
         (http_success_delta={http_ok}, http_dispatch_delta={http_disp}, \
          wss_success_delta={wss_ok}, wss_dispatch_delta={wss_disp})"
    );
    assert_eq!(
        after.adaptive.aggregate.in_flight, 0,
        "aggregate in_flight must settle to 0"
    );
    assert_eq!(
        after.adaptive.aggregate.waiting, 0,
        "aggregate waiting must settle to 0"
    );
}
