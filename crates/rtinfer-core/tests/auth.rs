//! Tests for `CodexAuth` JSON loading.

use rtinfer_core::{CodexAuth, RealtimeError};
use std::fs;

#[test]
fn loads_well_formed_auth_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("auth.json");
    fs::write(
        &path,
        serde_json::json!({
            "tokens": {
                "access_token": "sk-test-abc",
                "account_id": "acct_123",
                "id_token": "ignored",
                "refresh_token": "ignored"
            }
        })
        .to_string(),
    )
    .unwrap();

    let auth = CodexAuth::from_path(&path).expect("auth must load");
    assert_eq!(auth.access_token, "sk-test-abc");
    assert_eq!(auth.account_id, "acct_123");
}

#[test]
fn account_id_optional_defaults_empty() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("auth.json");
    fs::write(
        &path,
        serde_json::json!({
            "tokens": { "access_token": "sk-test-xyz" }
        })
        .to_string(),
    )
    .unwrap();

    let auth = CodexAuth::from_path(&path).expect("auth must load");
    assert_eq!(auth.access_token, "sk-test-xyz");
    assert!(auth.account_id.is_empty());
}

#[test]
fn missing_access_token_returns_typed_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("auth.json");
    fs::write(
        &path,
        serde_json::json!({
            "tokens": { "account_id": "acct_only" }
        })
        .to_string(),
    )
    .unwrap();

    let err = CodexAuth::from_path(&path).expect_err("must fail");
    assert!(
        matches!(err, RealtimeError::AuthMissing(field) if field == "tokens.access_token"),
        "got {err:?}"
    );
}

#[test]
fn missing_tokens_block_returns_typed_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("auth.json");
    fs::write(&path, "{}").unwrap();

    let err = CodexAuth::from_path(&path).expect_err("must fail");
    assert!(
        matches!(err, RealtimeError::AuthMissing(field) if field == "tokens"),
        "got {err:?}"
    );
}

#[test]
fn missing_file_returns_typed_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("does-not-exist.json");

    let err = CodexAuth::from_path(&path).expect_err("must fail");
    assert!(matches!(err, RealtimeError::AuthFile { .. }), "got {err:?}");
}

#[test]
fn malformed_json_returns_typed_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("auth.json");
    fs::write(&path, "{not json").unwrap();

    let err = CodexAuth::from_path(&path).expect_err("must fail");
    assert!(
        matches!(err, RealtimeError::AuthMalformed(_)),
        "got {err:?}"
    );
}

#[test]
fn empty_access_token_treated_as_missing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("auth.json");
    fs::write(
        &path,
        serde_json::json!({
            "tokens": { "access_token": "", "account_id": "acct" }
        })
        .to_string(),
    )
    .unwrap();

    let err = CodexAuth::from_path(&path).expect_err("must fail");
    assert!(
        matches!(err, RealtimeError::AuthMissing(field) if field == "tokens.access_token"),
        "got {err:?}"
    );
}
