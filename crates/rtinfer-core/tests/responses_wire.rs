//! Canonical codex-tui wire contract for HTTP and WSS Responses.

use rtinfer_core::{
    CodexAuth, CodexRequestIds, CodexResponsesClient, ResponsesTransportMode,
    StaticCodexAuthSource, CODEX_BETA_FEATURES, CODEX_CLIENT_VERSION, CODEX_RESPONSES_ORIGINATOR,
    CODEX_RESPONSES_USER_AGENT,
};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;

fn assert_uuid_v4(s: &str) {
    let parts: Vec<_> = s.split('-').collect();
    assert_eq!(parts.len(), 5, "uuid shape: {s}");
    assert_eq!(parts[0].len(), 8);
    assert_eq!(parts[1].len(), 4);
    assert_eq!(parts[2].len(), 4);
    assert_eq!(parts[3].len(), 4);
    assert_eq!(parts[4].len(), 12);
    assert!(parts[2].starts_with('4'), "version nibble must be 4: {s}");
    let variant = u8::from_str_radix(&parts[3][..1], 16).unwrap();
    assert!((8..12).contains(&variant), "RFC4122 variant nibble: {s}");
    assert!(s.chars().all(|c| c.is_ascii_hexdigit() || c == '-'));
}

fn test_auth() -> CodexAuth {
    CodexAuth {
        access_token: "test-access".into(),
        account_id: "acct_test".into(),
        id_token: "id".into(),
        refresh_token: String::new(),
        source_path: None,
    }
}

fn test_client() -> Arc<CodexResponsesClient> {
    CodexResponsesClient::builder()
        .mode(ResponsesTransportMode::Wss)
        .installation_id("11111111-1111-4111-8111-111111111111")
        .auth_source(Arc::new(StaticCodexAuthSource(test_auth())))
        .build()
        .expect("client")
}

#[test]
fn http_and_wss_share_one_common_body() {
    let client = test_client();
    let wire = client.build_text_wire_for_test("sys", "user");
    let http = wire.http_body().clone();
    let wss = wire.wss_frame();

    assert!(http.get("type").is_none(), "HTTP must not send type");
    assert_eq!(wss["type"], "response.create");

    let mut wss_without_type = wss.clone();
    wss_without_type.as_object_mut().unwrap().remove("type");
    assert_eq!(http, wss_without_type);
}

#[test]
fn http_wire_matches_codex_tui_contract() {
    let client = test_client();
    let wire = client.build_structured_wire_for_test(
        "sys",
        "user",
        "result",
        serde_json::json!({"type":"object","properties":{"ok":{"type":"boolean"}}}),
    );
    let body = wire.http_body();
    assert_eq!(body["model"], "gpt-5.4");
    assert_eq!(body["reasoning"]["effort"], "low");
    assert_eq!(body["service_tier"], "priority");
    assert_eq!(body["store"], false);
    assert_eq!(body["stream"], true);
    assert_eq!(body["include"], serde_json::json!([]));
    assert_eq!(body["tools"], serde_json::json!([]));
    assert_eq!(body["tool_choice"], "auto");
    assert_eq!(body["parallel_tool_calls"], false);
    assert_eq!(body["prompt_cache_key"], wire.ids.thread_id);
    assert_eq!(
        body["input"][0]["internal_chat_message_metadata_passthrough"]["turn_id"],
        wire.ids.turn_id
    );
    assert!(body["input"][0]["content"][0]
        .get("internal_chat_message_metadata_passthrough")
        .is_none());
    assert_eq!(
        body["client_metadata"]["x-codex-installation-id"],
        "11111111-1111-4111-8111-111111111111"
    );
    assert_eq!(body["client_metadata"]["session_id"], wire.ids.session_id);
    assert_eq!(body["client_metadata"]["thread_id"], wire.ids.thread_id);
    assert_eq!(body["client_metadata"]["turn_id"], wire.ids.turn_id);
    assert_eq!(
        body["client_metadata"]["x-codex-window-id"],
        format!("{}:0", wire.ids.thread_id)
    );
    let turn_meta: Value = serde_json::from_str(
        body["client_metadata"]["x-codex-turn-metadata"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(turn_meta["request_kind"], "turn");
    assert_eq!(turn_meta["thread_source"], "user");
    assert_eq!(turn_meta["sandbox"], "none");

    assert!(body.get("max_output_tokens").is_none());
    assert!(body.get("previous_response_id").is_none());
    assert!(body.get("generate").is_none());
    assert!(body.get("type").is_none());

    let headers = client.build_http_headers_for_test(&test_auth(), &wire.ids);
    let map: std::collections::HashMap<_, _> = headers.into_iter().collect();
    assert_eq!(map["Authorization"], "Bearer test-access");
    assert_eq!(map["ChatGPT-Account-ID"], "acct_test");
    assert_eq!(map["Accept"], "text/event-stream");
    assert_eq!(map["Content-Type"], "application/json");
    assert_eq!(map["originator"], CODEX_RESPONSES_ORIGINATOR);
    assert_eq!(map["version"], CODEX_CLIENT_VERSION);
    assert_eq!(map["User-Agent"], CODEX_RESPONSES_USER_AGENT);
    assert_eq!(map["x-codex-beta-features"], CODEX_BETA_FEATURES);
    assert_eq!(map["session-id"], wire.ids.session_id);
    assert_eq!(map["thread-id"], wire.ids.thread_id);
    assert_eq!(map["x-client-request-id"], wire.ids.thread_id);
    assert_eq!(
        map["x-codex-window-id"],
        format!("{}:0", wire.ids.thread_id)
    );
    assert!(!map.contains_key("OpenAI-Beta"));
    assert!(!map.contains_key("openai-beta"));
    assert!(!CODEX_RESPONSES_USER_AGENT
        .to_ascii_lowercase()
        .contains("reqwest"));
}

#[test]
fn wss_handshake_matches_ga_codex_tui_contract() {
    let client = test_client();
    let ids = CodexRequestIds::fresh();
    let req = client
        .build_wss_handshake_for_test(&test_auth(), &ids)
        .unwrap();
    let h = req.headers();

    assert_eq!(
        h.get("authorization").unwrap().to_str().unwrap(),
        "Bearer test-access"
    );
    assert_eq!(
        h.get("ChatGPT-Account-ID").unwrap().to_str().unwrap(),
        "acct_test"
    );
    assert_eq!(
        h.get("user-agent").unwrap().to_str().unwrap(),
        CODEX_RESPONSES_USER_AGENT
    );
    assert_eq!(
        h.get("originator").unwrap().to_str().unwrap(),
        CODEX_RESPONSES_ORIGINATOR
    );
    assert_eq!(
        h.get("x-codex-beta-features").unwrap().to_str().unwrap(),
        CODEX_BETA_FEATURES
    );
    assert_eq!(
        h.get("version").unwrap().to_str().unwrap(),
        CODEX_CLIENT_VERSION
    );
    assert_eq!(
        h.get("session-id").unwrap().to_str().unwrap(),
        ids.session_id
    );
    assert_eq!(h.get("thread-id").unwrap().to_str().unwrap(), ids.thread_id);
    assert_eq!(
        h.get("x-client-request-id").unwrap().to_str().unwrap(),
        ids.thread_id
    );
    assert_eq!(
        h.get("x-codex-window-id").unwrap().to_str().unwrap(),
        ids.window_id()
    );
    let meta: Value =
        serde_json::from_str(h.get("x-codex-turn-metadata").unwrap().to_str().unwrap()).unwrap();
    assert_eq!(meta["request_kind"], "prewarm");
    assert_eq!(meta["turn_id"], "");
    assert!(h.get("openai-beta").is_none());
    assert!(h.get("OpenAI-Beta").is_none());
}

#[test]
fn logical_request_ids_are_unique_v4() {
    let client = test_client();
    let mut seen = HashSet::new();
    for _ in 0..100 {
        let wire = client.build_text_wire_for_test("s", "u");
        for id in [&wire.ids.session_id, &wire.ids.thread_id, &wire.ids.turn_id] {
            assert_uuid_v4(id);
        }
        let key = (
            wire.ids.session_id.clone(),
            wire.ids.thread_id.clone(),
            wire.ids.turn_id.clone(),
            wire.body["prompt_cache_key"].as_str().unwrap().to_string(),
        );
        assert!(seen.insert(key), "duplicate logical id tuple");
    }
}

#[test]
fn installation_id_is_stable_and_strict() {
    let client = test_client();
    let a = client.build_text_wire_for_test("s", "u");
    let b = client.build_text_wire_for_test("s", "u");
    assert_eq!(
        a.body["client_metadata"]["x-codex-installation-id"],
        b.body["client_metadata"]["x-codex-installation-id"]
    );
    assert_eq!(
        a.body["client_metadata"]["x-codex-installation-id"],
        "11111111-1111-4111-8111-111111111111"
    );

    let err = match CodexResponsesClient::builder()
        .mode(ResponsesTransportMode::Wss)
        .installation_id("not-a-uuid")
        .auth_source(Arc::new(StaticCodexAuthSource(test_auth())))
        .build()
    {
        Ok(_) => panic!("malformed installation_id must fail construction"),
        Err(e) => e,
    };
    assert_eq!(err.code_or_label(), "protocol");
}
