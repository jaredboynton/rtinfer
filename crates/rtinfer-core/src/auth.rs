//! Codex JWT loader + OAuth refresh.
//!
//! Reads `~/.codex/auth.json` and pulls `tokens.{access_token,id_token,
//! refresh_token,account_id}`. The codex/responses WebSocket validates the
//! short-lived `id_token` (1-hour life), so the access_token's far-future
//! `exp` is not enough — a stale `id_token` is rejected at the handshake
//! with HTTP 403. [`CodexAuth::refresh`] mints a fresh token set through the
//! OpenAI OAuth token endpoint (the same `refresh_token` grant the codex CLI
//! uses) and writes the rotated tokens back to `auth.json` atomically, so the
//! daemon self-heals without shelling out to `codex`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use serde::Deserialize;
use serde_json::{json, Value};
use warpsock::{Client, FingerprintProfile, Headers};

use crate::{display_path, RealtimeError};

/// Refresh the `id_token` when it expires within this many seconds. The
/// codex/responses WS rejects a stale `id_token` (1-hour life) with a 403
/// handshake, so a source refreshes proactively just before it lapses. Lives
/// here (rather than in `responses`) so every [`CodexAuthSource`] shares one
/// staleness threshold.
pub const ID_TOKEN_REFRESH_MARGIN_SECS: i64 = 120;

/// OpenAI OAuth token endpoint (verified live 2026-06-08; JSON body, not form).
const OAUTH_TOKEN_URL: &str = "https://auth.openai.com/oauth/token";
/// Codex CLI public OAuth client id (`aud` of every codex-minted token).
const OAUTH_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
/// Scope sent on the refresh grant (mirrors the codex CLI).
const OAUTH_REFRESH_SCOPE: &str = "openid profile email";
/// User-Agent for the refresh POST (codex's originator; verified accepted).
const OAUTH_USER_AGENT: &str = "codex_cli_rs";

/// Resolved Codex authentication.
///
/// `Debug` is implemented manually so the bearer-equivalent `access_token`,
/// `id_token`, and `refresh_token` are never printed in logs or panic
/// backtraces.
#[derive(Clone)]
pub struct CodexAuth {
    pub access_token: String,
    /// May be empty when the account hasn't been bound to a ChatGPT
    /// workspace; the Realtime API still accepts the request without
    /// `chatgpt-account-id` in that case but we forward an empty header
    /// to mirror the JS client.
    pub account_id: String,
    /// Short-lived OpenID token (1-hour life). The codex/responses WS gates
    /// on this; an expired `id_token` is the real cause of handshake 403s.
    /// Empty when auth was seeded from the daemon keychain (access-token only).
    pub id_token: String,
    /// Long-lived refresh token used to mint a new token set. Rotated on every
    /// successful refresh. Empty when seeded from keychain.
    pub refresh_token: String,
    /// Source `auth.json` path, when loaded from a file. Refreshed tokens are
    /// written back here. `None` for keychain-seeded auth (no write-back).
    pub source_path: Option<PathBuf>,
}

impl std::fmt::Debug for CodexAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodexAuth")
            .field(
                "access_token",
                &format!("<redacted:len={}>", self.access_token.len()),
            )
            .field("account_id", &self.account_id)
            .field(
                "id_token",
                &format!("<redacted:len={}>", self.id_token.len()),
            )
            .field(
                "refresh_token",
                &format!("<redacted:len={}>", self.refresh_token.len()),
            )
            .field("source_path", &self.source_path)
            .finish()
    }
}

#[derive(Debug, Deserialize)]
struct AuthFile {
    #[serde(default)]
    tokens: Option<AuthTokens>,
}

#[derive(Debug, Deserialize)]
struct AuthTokens {
    #[serde(default)]
    access_token: Option<String>,
    #[serde(default)]
    account_id: Option<String>,
    #[serde(default)]
    id_token: Option<String>,
    #[serde(default)]
    refresh_token: Option<String>,
}

impl CodexAuth {
    /// Load from the default `~/.codex/auth.json` location.
    pub fn from_default_path() -> Result<Self, RealtimeError> {
        let path = default_auth_path().ok_or(RealtimeError::AuthMissing(
            "home directory (~/.codex/auth.json)",
        ))?;
        Self::from_path(&path)
    }

    /// Load from an explicit path.
    pub fn from_path(path: &Path) -> Result<Self, RealtimeError> {
        let raw = std::fs::read_to_string(path).map_err(|e| RealtimeError::AuthFile {
            path: display_path(path),
            source: e,
        })?;
        let parsed: AuthFile = serde_json::from_str(&raw)
            .map_err(|e| RealtimeError::AuthMalformed(format!("{path:?}: {e}")))?;
        let tokens = parsed.tokens.ok_or(RealtimeError::AuthMissing("tokens"))?;
        let access_token = tokens
            .access_token
            .filter(|s| !s.is_empty())
            .ok_or(RealtimeError::AuthMissing("tokens.access_token"))?;
        let account_id = tokens.account_id.unwrap_or_default();
        let id_token = tokens.id_token.unwrap_or_default();
        let refresh_token = tokens.refresh_token.unwrap_or_default();
        Ok(Self {
            access_token,
            account_id,
            id_token,
            refresh_token,
            source_path: Some(path.to_path_buf()),
        })
    }

    /// True when the `id_token` is missing/unparseable or expires within
    /// `margin_secs`. The codex/responses WS rejects a stale `id_token` with
    /// 403, so this drives proactive refresh before the handshake fails.
    pub fn needs_refresh(&self, margin_secs: i64) -> bool {
        match jwt_exp_unix(&self.id_token) {
            Some(exp) => exp - now_unix() <= margin_secs,
            None => true,
        }
    }

    /// True when a refresh can both run and persist its rotated tokens. We
    /// require a write-back path so the rotated `refresh_token` is never lost
    /// (losing it would brick auth on the next refresh).
    pub fn can_refresh(&self) -> bool {
        !self.refresh_token.is_empty() && self.source_path.is_some()
    }

    /// Mint a fresh token set via the OAuth `refresh_token` grant and write
    /// the rotated tokens back to `source_path` atomically (preserving all
    /// other fields in the file). Returns the refreshed [`CodexAuth`].
    pub async fn refresh(&self) -> Result<CodexAuth, RealtimeError> {
        if self.refresh_token.is_empty() {
            return Err(RealtimeError::Refresh("no refresh_token available".into()));
        }
        let client = Client::builder()
            .fingerprint(FingerprintProfile::Chrome148)
            .prefer_http2(true)
            .user_agent(OAUTH_USER_AGENT)
            .total_timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| RealtimeError::Refresh(format!("warpsock client build: {e}")))?;

        let body = serde_json::to_vec(&json!({
            "client_id": OAUTH_CLIENT_ID,
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "scope": OAUTH_REFRESH_SCOPE,
        }))
        .map_err(|e| RealtimeError::Refresh(format!("encode request: {e}")))?;

        let headers = Headers::from_vec(vec![(
            "content-type".to_string(),
            "application/json".to_string(),
        )]);
        let resp = client
            .post(OAUTH_TOKEN_URL)
            .headers(headers)
            .body(body)
            .send()
            .await
            .map_err(|e| RealtimeError::Refresh(format!("token endpoint POST: {e}")))?;
        let status = resp.status_code();
        let bytes = resp
            .bytes()
            .map_err(|e| RealtimeError::Refresh(format!("read token response: {e}")))?
            .to_vec();
        if status != 200 {
            // Body may name the failure (e.g. invalid_grant) but can also carry
            // sensitive material; include only a short, bounded snippet.
            let snippet: String = String::from_utf8_lossy(&bytes).chars().take(200).collect();
            return Err(RealtimeError::Refresh(format!(
                "token endpoint returned HTTP {status}: {snippet}"
            )));
        }

        let v: Value = serde_json::from_slice(&bytes)
            .map_err(|e| RealtimeError::Refresh(format!("response not json: {e}")))?;
        let access = v
            .get("access_token")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| RealtimeError::Refresh("response missing access_token".into()))?
            .to_string();
        let id = v
            .get("id_token")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        // refresh_token rotates; if the response omits it, keep the current one.
        let new_refresh = v
            .get("refresh_token")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| self.refresh_token.clone());

        if let Some(path) = self.source_path.as_deref() {
            let raw = std::fs::read_to_string(path)
                .map_err(|e| RealtimeError::Refresh(format!("re-read {}: {e}", path.display())))?;
            let patched = patch_auth_json(&raw, &access, &id, &new_refresh, &now_rfc3339())?;
            write_auth_file_atomic(path, &patched)
                .map_err(|e| RealtimeError::Refresh(format!("write {}: {e}", path.display())))?;
        }

        Ok(CodexAuth {
            access_token: access,
            account_id: self.account_id.clone(),
            id_token: id,
            refresh_token: new_refresh,
            source_path: self.source_path.clone(),
        })
    }
}

/// Pluggable provider of [`CodexAuth`] for the pools.
///
/// The pools own no knowledge of where credentials come from; they call
/// [`load`](CodexAuthSource::load) when their cache is cold/stale and
/// [`force_refresh`](CodexAuthSource::force_refresh) after a handshake
/// rejection. The standalone/dev path uses [`FileCodexAuthSource`]
/// (`auth.json` + client-side OAuth rotation); the daemon supplies a
/// keychain-backed source that re-mints through okta-aio and never writes
/// `auth.json`, so the two refresh owners (the worker's server-side
/// `refresh_token` and this client's) can never rotate the same token and
/// invalidate each other.
#[async_trait]
pub trait CodexAuthSource: Send + Sync + 'static {
    /// Return currently-valid auth, refreshing/re-minting upstream if the
    /// backing token is stale.
    async fn load(&self) -> Result<CodexAuth, RealtimeError>;
    /// Force an upstream refresh/re-mint, bypassing any "still fresh" check.
    /// Called after the WS rejects a seemingly-fresh token at the handshake.
    async fn force_refresh(&self) -> Result<CodexAuth, RealtimeError>;
}

/// Reads [`CodexAuth`] from an `auth.json` file (explicit path or the default
/// `~/.codex/auth.json`) and performs the client-side OAuth `refresh_token`
/// rotation when the `id_token` is stale. This is the standalone/dev/test
/// path. The daemon's keychain source uses this as its fallback when keychain
/// holds no codex tokens (operator ran `codex login` but never worker-enrolled).
pub struct FileCodexAuthSource {
    path: Option<PathBuf>,
}

impl FileCodexAuthSource {
    /// `None` resolves to `~/.codex/auth.json`.
    pub fn new(path: Option<PathBuf>) -> Self {
        Self { path }
    }

    fn read(&self) -> Result<CodexAuth, RealtimeError> {
        match self.path.as_deref() {
            Some(p) => CodexAuth::from_path(p),
            None => CodexAuth::from_default_path(),
        }
    }
}

#[async_trait]
impl CodexAuthSource for FileCodexAuthSource {
    async fn load(&self) -> Result<CodexAuth, RealtimeError> {
        let base = self.read()?;
        if base.needs_refresh(ID_TOKEN_REFRESH_MARGIN_SECS) && base.can_refresh() {
            base.refresh().await
        } else {
            Ok(base)
        }
    }

    async fn force_refresh(&self) -> Result<CodexAuth, RealtimeError> {
        let base = self.read()?;
        if base.can_refresh() {
            base.refresh().await
        } else {
            Ok(base)
        }
    }
}

/// A fixed [`CodexAuth`] that never refreshes. Tests seed a pool's source with
/// this so it neither reads `auth.json` nor touches keychain.
pub struct StaticCodexAuthSource(pub CodexAuth);

#[async_trait]
impl CodexAuthSource for StaticCodexAuthSource {
    async fn load(&self) -> Result<CodexAuth, RealtimeError> {
        Ok(self.0.clone())
    }

    async fn force_refresh(&self) -> Result<CodexAuth, RealtimeError> {
        Ok(self.0.clone())
    }
}

/// Shared `Arc<dyn CodexAuthSource>` alias used by the pool builders.
pub type SharedCodexAuthSource = Arc<dyn CodexAuthSource>;

/// Patch `tokens.{access_token,id_token,refresh_token}` + top-level
/// `last_refresh` in a raw `auth.json` string, preserving every other field
/// (auth_mode, OPENAI_API_KEY, account_id, ...). Pure so it is unit-testable.
fn patch_auth_json(
    raw: &str,
    access: &str,
    id: &str,
    refresh: &str,
    last_refresh: &str,
) -> Result<String, RealtimeError> {
    let mut v: Value = serde_json::from_str(raw)
        .map_err(|e| RealtimeError::Refresh(format!("parse for patch: {e}")))?;
    let obj = v
        .as_object_mut()
        .ok_or_else(|| RealtimeError::Refresh("auth.json is not a JSON object".into()))?;
    let tokens = obj.entry("tokens").or_insert_with(|| json!({}));
    let tobj = tokens
        .as_object_mut()
        .ok_or_else(|| RealtimeError::Refresh("auth.json tokens is not an object".into()))?;
    tobj.insert("access_token".into(), json!(access));
    if !id.is_empty() {
        tobj.insert("id_token".into(), json!(id));
    }
    tobj.insert("refresh_token".into(), json!(refresh));
    obj.insert("last_refresh".into(), json!(last_refresh));
    serde_json::to_string_pretty(&v)
        .map_err(|e| RealtimeError::Refresh(format!("serialize patched: {e}")))
}

/// Write `contents` to `path` atomically with `0600` perms (tmp + rename).
fn write_auth_file_atomic(path: &Path, contents: &str) -> std::io::Result<()> {
    use std::io::Write;
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".refresh.tmp");
    let tmp = PathBuf::from(tmp);
    {
        #[cfg(unix)]
        let mut f = {
            use std::os::unix::fs::OpenOptionsExt;
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&tmp)?
        };
        #[cfg(not(unix))]
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(contents.as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

/// Decode the `exp` (seconds since epoch) from a JWT's payload segment.
fn jwt_exp_unix(token: &str) -> Option<i64> {
    let payload = token.split('.').nth(1)?;
    let payload = payload.trim_end_matches('=');
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    let v: Value = serde_json::from_slice(&bytes).ok()?;
    v.get("exp").and_then(Value::as_i64)
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
}

/// Resolve `~/.codex/auth.json` for the current user.
pub fn default_auth_path() -> Option<PathBuf> {
    let mut p = home_dir()?;
    p.push(".codex");
    p.push("auth.json");
    Some(p)
}

#[cfg(unix)]
fn home_dir() -> Option<PathBuf> {
    use std::ffi::CStr;
    use std::os::raw::{c_char, c_uint};

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    #[repr(C)]
    struct Passwd {
        pw_name: *mut c_char,
        pw_passwd: *mut c_char,
        pw_uid: c_uint,
        pw_gid: c_uint,
        pw_change: isize,
        pw_class: *mut c_char,
        pw_gecos: *mut c_char,
        pw_dir: *mut c_char,
        pw_shell: *mut c_char,
        pw_expire: isize,
    }

    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    #[repr(C)]
    struct Passwd {
        pw_name: *mut c_char,
        pw_passwd: *mut c_char,
        pw_uid: c_uint,
        pw_gid: c_uint,
        pw_gecos: *mut c_char,
        pw_dir: *mut c_char,
        pw_shell: *mut c_char,
    }

    unsafe extern "C" {
        fn getuid() -> c_uint;
        fn getpwuid(uid: c_uint) -> *mut Passwd;
    }

    let uid = unsafe { getuid() };
    let passwd = unsafe { getpwuid(uid) };
    if passwd.is_null() {
        return None;
    }

    let dir = unsafe { (*passwd).pw_dir };
    if dir.is_null() {
        return None;
    }

    let path = unsafe { CStr::from_ptr(dir) };
    Some(PathBuf::from(path.to_string_lossy().into_owned()))
}

#[cfg(not(unix))]
fn home_dir() -> Option<PathBuf> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn jwt_with_exp(exp: i64) -> String {
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"{\"alg\":\"none\"}");
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(format!("{{\"exp\":{exp}}}").as_bytes());
        format!("{header}.{payload}.")
    }

    #[test]
    fn debug_redacts_all_secrets() {
        let auth = CodexAuth {
            access_token: "sk-secret-bearer-aaaa".into(),
            account_id: "acct_123".into(),
            id_token: "id-secret-zzzz".into(),
            refresh_token: "rt-secret-yyyy".into(),
            source_path: None,
        };
        let s = format!("{auth:?}");
        assert!(!s.contains("sk-secret"), "must not leak access_token: {s}");
        assert!(!s.contains("id-secret"), "must not leak id_token: {s}");
        assert!(!s.contains("rt-secret"), "must not leak refresh_token: {s}");
        assert!(s.contains("redacted"), "expected redaction marker: {s}");
        assert!(
            s.contains("acct_123"),
            "account_id should still appear: {s}"
        );
    }

    #[test]
    fn jwt_exp_parses() {
        assert_eq!(
            jwt_exp_unix(&jwt_with_exp(1_900_000_000)),
            Some(1_900_000_000)
        );
        assert_eq!(jwt_exp_unix("garbage"), None);
        assert_eq!(jwt_exp_unix(""), None);
    }

    #[test]
    fn needs_refresh_on_expiry() {
        let future = CodexAuth {
            access_token: "a".into(),
            account_id: String::new(),
            id_token: jwt_with_exp(now_unix() + 3600),
            refresh_token: "rt".into(),
            source_path: Some(PathBuf::from("/x")),
        };
        assert!(
            !future.needs_refresh(120),
            "1h-out id_token should not refresh"
        );

        let stale = CodexAuth {
            id_token: jwt_with_exp(now_unix() - 10),
            ..future.clone()
        };
        assert!(stale.needs_refresh(120), "expired id_token must refresh");

        let no_id = CodexAuth {
            id_token: String::new(),
            ..future.clone()
        };
        assert!(no_id.needs_refresh(120), "missing id_token must refresh");
    }

    #[test]
    fn can_refresh_requires_token_and_path() {
        let base = CodexAuth {
            access_token: "a".into(),
            account_id: String::new(),
            id_token: String::new(),
            refresh_token: "rt".into(),
            source_path: Some(PathBuf::from("/x")),
        };
        assert!(base.can_refresh());
        assert!(!CodexAuth {
            refresh_token: String::new(),
            ..base.clone()
        }
        .can_refresh());
        assert!(!CodexAuth {
            source_path: None,
            ..base.clone()
        }
        .can_refresh());
    }

    #[test]
    fn patch_preserves_unknown_fields_and_rotates_tokens() {
        let raw = r#"{
  "auth_mode": "chatgpt",
  "OPENAI_API_KEY": "sk-keep-me",
  "tokens": {
    "access_token": "old-access",
    "id_token": "old-id",
    "refresh_token": "old-refresh",
    "account_id": "acct-keep"
  },
  "last_refresh": "2026-01-01T00:00:00Z"
}"#;
        let out = patch_auth_json(
            raw,
            "new-access",
            "new-id",
            "new-refresh",
            "2026-06-08T09:00:00Z",
        )
        .unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["auth_mode"], "chatgpt");
        assert_eq!(v["OPENAI_API_KEY"], "sk-keep-me", "unknown field preserved");
        assert_eq!(
            v["tokens"]["account_id"], "acct-keep",
            "account_id preserved"
        );
        assert_eq!(v["tokens"]["access_token"], "new-access");
        assert_eq!(v["tokens"]["id_token"], "new-id");
        assert_eq!(
            v["tokens"]["refresh_token"], "new-refresh",
            "rotated token persisted"
        );
        assert_eq!(v["last_refresh"], "2026-06-08T09:00:00Z");
    }

    #[test]
    fn from_path_loads_all_tokens() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");
        std::fs::write(
            &path,
            r#"{"tokens":{"access_token":"acc","id_token":"idt","refresh_token":"rft","account_id":"acct"}}"#,
        )
        .unwrap();
        let auth = CodexAuth::from_path(&path).unwrap();
        assert_eq!(auth.access_token, "acc");
        assert_eq!(auth.id_token, "idt");
        assert_eq!(auth.refresh_token, "rft");
        assert_eq!(auth.account_id, "acct");
        assert_eq!(auth.source_path.as_deref(), Some(path.as_path()));
        assert!(auth.can_refresh());
    }

    #[test]
    fn write_atomic_sets_0600() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");
        write_auth_file_atomic(&path, "{\"k\":1}").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "{\"k\":1}");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "auth.json must be owner-only");
        }
    }

    #[test]
    fn clone_still_works() {
        let auth = CodexAuth {
            access_token: "tok".into(),
            account_id: "acct".into(),
            id_token: "id".into(),
            refresh_token: "rt".into(),
            source_path: Some(PathBuf::from("/p")),
        };
        let c = auth.clone();
        assert_eq!(c.access_token, auth.access_token);
        assert_eq!(c.account_id, auth.account_id);
        assert_eq!(c.refresh_token, auth.refresh_token);
    }

    #[tokio::test]
    async fn static_source_returns_fixed_auth() {
        let auth = CodexAuth {
            access_token: "static-acc".into(),
            account_id: "acct".into(),
            id_token: "id".into(),
            refresh_token: String::new(),
            source_path: None,
        };
        let source = StaticCodexAuthSource(auth);
        assert_eq!(source.load().await.unwrap().access_token, "static-acc");
        assert_eq!(
            source.force_refresh().await.unwrap().access_token,
            "static-acc"
        );
    }

    #[tokio::test]
    async fn file_source_loads_fresh_token_without_refresh() {
        // A fresh id_token => needs_refresh is false => load returns the file
        // contents verbatim without any network refresh.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");
        let id = jwt_with_exp(now_unix() + 3600);
        std::fs::write(
            &path,
            format!(
                r#"{{"tokens":{{"access_token":"acc","id_token":"{id}","refresh_token":"rft","account_id":"acct"}}}}"#
            ),
        )
        .unwrap();
        let source = FileCodexAuthSource::new(Some(path));
        let auth = source.load().await.unwrap();
        assert_eq!(auth.access_token, "acc");
        assert_eq!(auth.refresh_token, "rft");
    }

    #[tokio::test]
    async fn file_source_returns_stale_token_when_cannot_refresh() {
        // Stale id_token but empty refresh_token => can_refresh is false => load
        // returns the stale token rather than attempting a (doomed) refresh.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");
        let id = jwt_with_exp(now_unix() - 10);
        std::fs::write(
            &path,
            format!(
                r#"{{"tokens":{{"access_token":"stale-acc","id_token":"{id}","account_id":"acct"}}}}"#
            ),
        )
        .unwrap();
        let source = FileCodexAuthSource::new(Some(path));
        let auth = source.load().await.unwrap();
        assert_eq!(auth.access_token, "stale-acc");
        assert!(auth.needs_refresh(120));
        assert!(!auth.can_refresh());
    }
}
