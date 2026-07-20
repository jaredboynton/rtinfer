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
use std::time::{Duration, Instant};

use async_trait::async_trait;
use base64::Engine;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::RwLock;
use tracing::{debug, warn};
use warpsock::{Client, FingerprintProfile, Headers};

use crate::{display_path, RealtimeError, DEFAULT_AUTH_TTL};

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
/// `id_token`, `refresh_token`, and `account_id` are never printed in logs or
/// panic backtraces — only bounded redaction markers/lengths.
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
    /// Empty only for access-token-only sources.
    pub id_token: String,
    /// Long-lived refresh token used to mint a new token set. Rotated on every
    /// successful refresh. Empty for externally refreshed sources.
    pub refresh_token: String,
    /// Source `auth.json` path, when loaded from a file. Refreshed tokens are
    /// written back here. `None` for non-file auth (no write-back).
    pub source_path: Option<PathBuf>,
}

impl std::fmt::Debug for CodexAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodexAuth")
            .field(
                "access_token",
                &format!("<redacted:len={}>", self.access_token.len()),
            )
            .field(
                "account_id",
                &format!("<redacted:len={}>", self.account_id.len()),
            )
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
            // sensitive material; report bounded HTTP status only.
            return Err(token_endpoint_http_error(status));
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
/// (`auth.json` + client-side OAuth rotation); the daemon can supply a
/// cse-toold credential-process source that never writes `auth.json`.
#[async_trait]
pub trait CodexAuthSource: Send + Sync + 'static {
    /// Return currently-valid auth, refreshing/re-minting upstream if the
    /// backing token is stale.
    async fn load(&self) -> Result<CodexAuth, RealtimeError>;
    /// Force an upstream refresh/re-mint, bypassing any "still fresh" check.
    /// Called after the WS rejects a seemingly-fresh token at the handshake.
    ///
    /// `rejected_access_token` is the access token that just failed (or empty
    /// when the caller has no prior generation). Implementations use it to
    /// collapse concurrent force calls and to avoid a redundant rotation when
    /// another writer already replaced the rejected token.
    async fn force_refresh(&self, rejected_access_token: &str) -> Result<CodexAuth, RealtimeError>;
}

/// Reads [`CodexAuth`] from an `auth.json` file (explicit path or the default
/// `~/.codex/auth.json`) and performs the client-side OAuth `refresh_token`
/// rotation when the `id_token` is stale. This is the standalone/dev/test
/// path. Daemon credential-process mode supplies a different source and never
/// falls back here.
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

    async fn force_refresh(&self, rejected_access_token: &str) -> Result<CodexAuth, RealtimeError> {
        let base = self.read()?;
        // Another writer (or a concurrent force) may already have rotated the
        // file. Re-read and skip our own refresh when the rejected token is no
        // longer current — rotating again would invalidate the newer grant.
        if !rejected_access_token.is_empty() && base.access_token != rejected_access_token {
            return Ok(base);
        }
        if base.can_refresh() {
            base.refresh().await
        } else {
            Ok(base)
        }
    }
}

/// A fixed [`CodexAuth`] that never refreshes. Tests seed a pool's source with
/// this so it never reads or refreshes `auth.json`.
pub struct StaticCodexAuthSource(pub CodexAuth);

#[async_trait]
impl CodexAuthSource for StaticCodexAuthSource {
    async fn load(&self) -> Result<CodexAuth, RealtimeError> {
        Ok(self.0.clone())
    }

    async fn force_refresh(
        &self,
        _rejected_access_token: &str,
    ) -> Result<CodexAuth, RealtimeError> {
        Ok(self.0.clone())
    }
}

/// Shared `Arc<dyn CodexAuthSource>` alias used by the pool builders.
pub type SharedCodexAuthSource = Arc<dyn CodexAuthSource>;

/// Cached [`CodexAuth`] entry with the instant it was stored.
#[derive(Clone)]
struct CachedAuth {
    auth: CodexAuth,
    fetched_at: Instant,
}

/// Interior of [`CodexAuthCache`]. `generation` and `last_access_token` survive
/// [`CodexAuthCache::invalidate`] so a reload of the same access token does not
/// bump generation; only a changed access token increments it.
struct CacheState {
    entry: Option<CachedAuth>,
    generation: u64,
    last_access_token: Option<String>,
}

impl CacheState {
    fn empty() -> Self {
        Self {
            entry: None,
            generation: 0,
            last_access_token: None,
        }
    }

    /// Store `auth`, bumping `generation` only when the access token differs
    /// from the last committed value (including the first load).
    fn commit(&mut self, auth: CodexAuth) {
        let changed = self
            .last_access_token
            .as_deref()
            .map(|prev| prev != auth.access_token.as_str())
            .unwrap_or(true);
        if changed {
            self.generation = self.generation.saturating_add(1);
            self.last_access_token = Some(auth.access_token.clone());
        }
        self.entry = Some(CachedAuth {
            auth,
            fetched_at: Instant::now(),
        });
    }
}

/// Builder for [`CodexAuthCache`].
#[derive(Default)]
pub(crate) struct CodexAuthCacheBuilder {
    auth_ttl: Option<Duration>,
    initial_auth: Option<CodexAuth>,
    auth_path: Option<PathBuf>,
    auth_source: Option<SharedCodexAuthSource>,
}

impl CodexAuthCacheBuilder {
    pub(crate) fn auth_ttl(mut self, ttl: Duration) -> Self {
        self.auth_ttl = Some(ttl);
        self
    }

    /// Seed explicit auth so the cache never reads `~/.codex/auth.json` at build.
    pub(crate) fn initial_auth(mut self, auth: CodexAuth) -> Self {
        self.initial_auth = Some(auth);
        self
    }

    pub(crate) fn auth_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.auth_path = Some(path.into());
        self
    }

    /// Supply a pluggable [`CodexAuthSource`]. When set, the cache calls it for
    /// every cold/stale load and after a rejection, ignoring `auth_path`.
    /// Credential-process mode uses this to prevent any fallback to
    /// `~/.codex/auth.json`.
    pub(crate) fn auth_source(mut self, source: SharedCodexAuthSource) -> Self {
        self.auth_source = Some(source);
        self
    }

    pub(crate) fn build(self) -> Arc<CodexAuthCache> {
        let auth_ttl = self.auth_ttl.unwrap_or(DEFAULT_AUTH_TTL);
        // Eager pre-load only for the file path. With an explicit `auth_source`
        // the first load runs lazily through it; an external credential
        // operation must not run synchronously in `build`.
        let mut state = CacheState::empty();
        match self.initial_auth {
            Some(auth) => state.commit(auth),
            None if self.auth_source.is_some() => {}
            None => {
                let maybe = match self.auth_path.as_deref() {
                    Some(p) => CodexAuth::from_path(p).ok(),
                    None => CodexAuth::from_default_path().ok(),
                };
                if let Some(auth) = maybe {
                    state.commit(auth);
                }
            }
        }
        Arc::new(CodexAuthCache {
            auth_ttl,
            auth_path: self.auth_path,
            auth_source: self.auth_source,
            state: RwLock::new(state),
        })
    }
}

/// Process-shared Codex auth cache for HTTP and WSS Responses lanes.
///
/// Owns TTL, optional file path, optional [`SharedCodexAuthSource`], the cached
/// token set, a monotonic non-secret `generation`, proactive file refresh, and
/// generation-aware [`force_refresh_after`](Self::force_refresh_after). Both
/// transports should share one `Arc<CodexAuthCache>`; neither lane should own
/// a second cache or read `auth.json` directly.
pub(crate) struct CodexAuthCache {
    auth_ttl: Duration,
    auth_path: Option<PathBuf>,
    auth_source: Option<SharedCodexAuthSource>,
    state: RwLock<CacheState>,
}

impl CodexAuthCache {
    pub(crate) fn builder() -> CodexAuthCacheBuilder {
        CodexAuthCacheBuilder::default()
    }

    /// Monotonic generation, incremented only when the cached access token
    /// changes. Safe to log; never a secret.
    pub(crate) async fn generation(&self) -> u64 {
        self.state.read().await.generation
    }

    /// Force the next [`load`](Self::load) to re-fetch. Does not change
    /// `generation`.
    pub(crate) async fn invalidate(&self) {
        self.state.write().await.entry = None;
    }

    /// True when a cached entry can be returned without reloading.
    ///
    /// With an explicit [`auth_source`](CodexAuthCacheBuilder::auth_source),
    /// TTL alone gates the fast path — the source owns refresh, including
    /// access-token-only credentials whose empty `id_token` would otherwise
    /// look perpetually stale. For the file-auth path, also require the
    /// `id_token` not be near expiry so proactive OAuth refresh still runs.
    fn is_fresh_cache_hit(&self, cached: &CachedAuth) -> bool {
        if cached.fetched_at.elapsed() >= self.auth_ttl {
            return false;
        }
        if self.auth_source.is_some() {
            return true;
        }
        !cached.auth.needs_refresh(ID_TOKEN_REFRESH_MARGIN_SECS)
    }

    /// Return currently-valid auth, refreshing through the configured source
    /// (or file path) when the cache is cold, past TTL, or — for file auth —
    /// the `id_token` is near expiry. Concurrent stale loads collapse on one
    /// write lock.
    pub(crate) async fn load(&self) -> Result<CodexAuth, RealtimeError> {
        // Fast path: cached and still fresh for this cache mode.
        {
            let guard = self.state.read().await;
            if let Some(cached) = guard.entry.as_ref() {
                if self.is_fresh_cache_hit(cached) {
                    return Ok(cached.auth.clone());
                }
            }
        }
        let mut guard = self.state.write().await;
        // Re-check under the write lock so concurrent asks refresh at most once.
        if let Some(cached) = guard.entry.as_ref() {
            if self.is_fresh_cache_hit(cached) {
                return Ok(cached.auth.clone());
            }
        }
        // A `CodexAuthSource` owns its own staleness handling: the file source
        // does the client-side OAuth rotation; external sources own refresh.
        // The path branch keeps the legacy proactive refresh.
        let auth = match self.auth_source.as_ref() {
            Some(source) => source.load().await?,
            None => {
                let mut auth = match self.auth_path.as_deref() {
                    Some(p) => CodexAuth::from_path(p)?,
                    None => CodexAuth::from_default_path()?,
                };
                // Proactive: the codex/responses WS gates on the short-lived
                // id_token. Refresh it before the stale token earns a 403.
                if auth.needs_refresh(ID_TOKEN_REFRESH_MARGIN_SECS) && auth.can_refresh() {
                    match auth.refresh().await {
                        Ok(fresh) => {
                            debug!("codex auth cache: proactively refreshed expiring id_token");
                            auth = fresh;
                        }
                        Err(e) => {
                            warn!(
                                error = %e,
                                "codex auth cache: proactive token refresh failed; trying existing token"
                            );
                        }
                    }
                }
                auth
            }
        };
        guard.commit(auth.clone());
        Ok(auth)
    }

    /// Force a token refresh after an auth rejection, collapsing a thundering
    /// herd: if another caller already refreshed (cached access_token differs
    /// from the rejected one), return that instead of refreshing again.
    pub(crate) async fn force_refresh_after(
        &self,
        rejected_access_token: &str,
    ) -> Result<CodexAuth, RealtimeError> {
        let mut guard = self.state.write().await;
        if let Some(cached) = guard.entry.as_ref() {
            if cached.auth.access_token != rejected_access_token {
                return Ok(cached.auth.clone());
            }
        }
        let fresh = match self.auth_source.as_ref() {
            Some(source) => source.force_refresh(rejected_access_token).await?,
            None => {
                let base = match self.auth_path.as_deref() {
                    Some(p) => CodexAuth::from_path(p)?,
                    None => CodexAuth::from_default_path()?,
                };
                // Mirror FileCodexAuthSource: if another writer already rotated
                // past the rejected token, reuse it instead of refreshing again.
                if !rejected_access_token.is_empty() && base.access_token != rejected_access_token {
                    base
                } else if base.can_refresh() {
                    base.refresh().await?
                } else {
                    base
                }
            }
        };
        guard.commit(fresh.clone());
        Ok(fresh)
    }
}

/// Non-200 token-endpoint status as a [`RealtimeError::Refresh`] without any
/// response-body snippet (bodies can carry sensitive material).
fn token_endpoint_http_error(status: u16) -> RealtimeError {
    RealtimeError::Refresh(format!("token endpoint returned HTTP {status}"))
}

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
            account_id: "acct_sentinel_do_not_leak".into(),
            id_token: "id-secret-zzzz".into(),
            refresh_token: "rt-secret-yyyy".into(),
            source_path: None,
        };
        let s = format!("{auth:?}");
        assert!(!s.contains("sk-secret"), "must not leak access_token: {s}");
        assert!(!s.contains("id-secret"), "must not leak id_token: {s}");
        assert!(!s.contains("rt-secret"), "must not leak refresh_token: {s}");
        assert!(
            !s.contains("acct_sentinel_do_not_leak"),
            "must not leak account_id: {s}"
        );
        assert!(s.contains("redacted"), "expected redaction marker: {s}");
        assert!(
            s.contains("account_id: \"<redacted:len="),
            "account_id must use bounded redaction marker: {s}"
        );
    }

    #[test]
    fn token_endpoint_http_error_omits_body() {
        // Unit-testable without network: the Refresh payload is status-only.
        let err = token_endpoint_http_error(401);
        match &err {
            RealtimeError::Refresh(msg) => {
                assert_eq!(msg, "token endpoint returned HTTP 401");
                assert!(
                    !msg.contains('{') && !msg.contains("invalid_grant"),
                    "must not surface provider body: {msg}"
                );
            }
            other => panic!("expected Refresh, got {other:?}"),
        }
        let s = err.to_string();
        assert!(
            s.contains("HTTP 401"),
            "must report bounded HTTP status: {s}"
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
            source.force_refresh("").await.unwrap().access_token,
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

    #[tokio::test]
    async fn file_force_rereads_and_reuses_newer_generation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");
        let id = jwt_with_exp(now_unix() + 3600);
        let raw = format!(
            r#"{{"tokens":{{"access_token":"new-access","id_token":"{id}","refresh_token":"do-not-rotate","account_id":"acct"}}}}"#
        );
        std::fs::write(&path, &raw).unwrap();
        let source = FileCodexAuthSource::new(Some(path.clone()));

        let auth = source.force_refresh("rejected-old-access").await.unwrap();

        assert_eq!(auth.access_token, "new-access");
        assert_eq!(std::fs::read_to_string(path).unwrap(), raw);
    }

    // -----------------------------------------------------------------------
    // CodexAuthCache
    // -----------------------------------------------------------------------

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex as StdMutex;

    fn cache_auth(access_token: &str) -> CodexAuth {
        CodexAuth {
            access_token: access_token.into(),
            account_id: "acct".into(),
            id_token: jwt_with_exp(now_unix() + 3600),
            refresh_token: String::new(),
            source_path: None,
        }
    }

    struct RecordingAuthSource {
        load_calls: AtomicUsize,
        force_calls: AtomicUsize,
        rejected: StdMutex<Vec<String>>,
        load_token: StdMutex<String>,
        force_token: StdMutex<String>,
    }

    impl RecordingAuthSource {
        fn new(load_token: &str, force_token: &str) -> Self {
            Self {
                load_calls: AtomicUsize::new(0),
                force_calls: AtomicUsize::new(0),
                rejected: StdMutex::new(Vec::new()),
                load_token: StdMutex::new(load_token.into()),
                force_token: StdMutex::new(force_token.into()),
            }
        }
    }

    #[async_trait]
    impl CodexAuthSource for RecordingAuthSource {
        async fn load(&self) -> Result<CodexAuth, RealtimeError> {
            self.load_calls.fetch_add(1, Ordering::SeqCst);
            Ok(cache_auth(&self.load_token.lock().unwrap()))
        }

        async fn force_refresh(
            &self,
            rejected_access_token: &str,
        ) -> Result<CodexAuth, RealtimeError> {
            self.force_calls.fetch_add(1, Ordering::SeqCst);
            self.rejected
                .lock()
                .unwrap()
                .push(rejected_access_token.into());
            // Yield so concurrent force_refresh_after callers pile up behind
            // the cache write lock and exercise singleflight.
            tokio::task::yield_now().await;
            Ok(cache_auth(&self.force_token.lock().unwrap()))
        }
    }

    #[tokio::test]
    async fn cache_loads_once_inside_ttl() {
        let source = Arc::new(RecordingAuthSource::new("loaded-access", "forced-access"));
        let cache = CodexAuthCache::builder()
            .auth_ttl(Duration::from_secs(60))
            .auth_source(source.clone())
            .build();

        let a = cache.load().await.unwrap();
        let b = cache.load().await.unwrap();
        let gen = cache.generation().await;

        assert_eq!(a.access_token, "loaded-access");
        assert_eq!(b.access_token, "loaded-access");
        assert_eq!(source.load_calls.load(Ordering::SeqCst), 1);
        assert_eq!(gen, 1);
    }

    /// Access-token-only sources return an empty `id_token`, which makes
    /// [`CodexAuth::needs_refresh`] true. With an explicit `auth_source`, the
    /// TTL fast path must still return the cached entry (source owns refresh).
    #[tokio::test]
    async fn source_mode_ttl_loads_once_with_empty_id_token() {
        struct AccessOnlySource {
            load_calls: AtomicUsize,
        }

        #[async_trait]
        impl CodexAuthSource for AccessOnlySource {
            async fn load(&self) -> Result<CodexAuth, RealtimeError> {
                self.load_calls.fetch_add(1, Ordering::SeqCst);
                Ok(CodexAuth {
                    access_token: "access-only".into(),
                    account_id: "acct_source_only".into(),
                    id_token: String::new(),
                    refresh_token: String::new(),
                    source_path: None,
                })
            }

            async fn force_refresh(
                &self,
                _rejected_access_token: &str,
            ) -> Result<CodexAuth, RealtimeError> {
                self.load().await
            }
        }

        let source = Arc::new(AccessOnlySource {
            load_calls: AtomicUsize::new(0),
        });
        let cache = CodexAuthCache::builder()
            .auth_ttl(Duration::from_secs(60))
            .auth_source(source.clone())
            .build();

        let a = cache.load().await.unwrap();
        let b = cache.load().await.unwrap();
        let c = cache.load().await.unwrap();

        assert_eq!(a.access_token, "access-only");
        assert_eq!(b.access_token, "access-only");
        assert_eq!(c.access_token, "access-only");
        assert!(
            a.id_token.is_empty() && a.needs_refresh(ID_TOKEN_REFRESH_MARGIN_SECS),
            "fixture must look stale via id_token freshness"
        );
        assert_eq!(
            source.load_calls.load(Ordering::SeqCst),
            1,
            "empty id_token must not force a reload on every ask inside TTL"
        );
    }

    #[tokio::test]
    async fn rejected_generation_refresh_is_singleflight() {
        let source = Arc::new(RecordingAuthSource::new("loaded-access", "forced-access"));
        let cache = CodexAuthCache::builder()
            .auth_ttl(Duration::from_secs(60))
            .auth_source(source.clone())
            .build();

        let loaded = cache.load().await.unwrap();
        let gen_before = cache.generation().await;
        assert_eq!(loaded.access_token, "loaded-access");
        assert_eq!(gen_before, 1);

        let rejected = loaded.access_token.clone();
        let mut joins = Vec::new();
        for _ in 0..32 {
            let cache = Arc::clone(&cache);
            let rejected = rejected.clone();
            joins.push(tokio::spawn(async move {
                cache.force_refresh_after(&rejected).await.unwrap()
            }));
        }
        let mut tokens = Vec::new();
        for join in joins {
            let auth = join.await.unwrap();
            tokens.push(auth.access_token);
        }
        let gen_after = cache.generation().await;

        assert_eq!(source.force_calls.load(Ordering::SeqCst), 1);
        assert!(tokens.iter().all(|t| t == "forced-access"));
        assert_eq!(gen_after, gen_before + 1);
        assert_eq!(
            source.rejected.lock().unwrap().as_slice(),
            ["loaded-access"]
        );
    }

    #[tokio::test]
    async fn generation_increments_only_when_access_token_changes() {
        let source = Arc::new(RecordingAuthSource::new("tok-a", "tok-a"));
        let cache = CodexAuthCache::builder()
            .auth_ttl(Duration::from_secs(60))
            .auth_source(source.clone())
            .build();

        cache.load().await.unwrap();
        assert_eq!(cache.generation().await, 1);

        // Force refresh returns the same access token => generation unchanged.
        let forced = cache.force_refresh_after("tok-a").await.unwrap();
        assert_eq!(forced.access_token, "tok-a");
        assert_eq!(cache.generation().await, 1);

        *source.force_token.lock().unwrap() = "tok-b".into();
        let forced2 = cache.force_refresh_after("tok-a").await.unwrap();
        assert_eq!(forced2.access_token, "tok-b");
        assert_eq!(cache.generation().await, 2);

        // After invalidate, a reload whose access token differs from the last
        // committed value still bumps generation.
        cache.invalidate().await;
        assert!(cache.state.read().await.entry.is_none());
        let reloaded = cache.load().await.unwrap();
        assert_eq!(reloaded.access_token, "tok-a");
        assert_eq!(cache.generation().await, 3);
    }

    #[tokio::test]
    async fn credential_source_never_falls_back_to_file() {
        let dir = tempfile::tempdir().unwrap();
        let bad_path = dir.path().join("unreadable-auth.json");
        // Missing file would be used by the path branch; source must win.
        let source = Arc::new(RecordingAuthSource::new("source-only", "forced"));
        let cache = CodexAuthCache::builder()
            .auth_ttl(Duration::from_secs(60))
            .auth_path(bad_path)
            .auth_source(source.clone())
            .build();

        // Build must not have eagerly read the file (source mode is lazy).
        assert_eq!(cache.generation().await, 0);
        assert!(cache.state.read().await.entry.is_none());

        let auth = cache.load().await.unwrap();
        assert_eq!(auth.access_token, "source-only");
        assert_eq!(source.load_calls.load(Ordering::SeqCst), 1);

        let forced = cache.force_refresh_after("source-only").await.unwrap();
        assert_eq!(forced.access_token, "forced");
        assert_eq!(source.force_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn invalidate_clears_entry_without_touching_generation() {
        let source = Arc::new(RecordingAuthSource::new("loaded-access", "forced-access"));
        let cache = CodexAuthCache::builder()
            .auth_ttl(Duration::from_secs(60))
            .auth_source(source.clone())
            .build();

        cache.load().await.unwrap();
        let gen = cache.generation().await;
        cache.invalidate().await;
        assert!(cache.state.read().await.entry.is_none());
        assert_eq!(cache.generation().await, gen);

        // Next load hits the source again.
        cache.load().await.unwrap();
        assert_eq!(source.load_calls.load(Ordering::SeqCst), 2);
        // Same access token as last commit => generation unchanged.
        assert_eq!(cache.generation().await, gen);
    }

    #[tokio::test]
    async fn file_cache_loads_fresh_token_without_network_refresh() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");
        let id = jwt_with_exp(now_unix() + 3600);
        std::fs::write(
            &path,
            format!(
                r#"{{"tokens":{{"access_token":"file-acc","id_token":"{id}","refresh_token":"rft","account_id":"acct"}}}}"#
            ),
        )
        .unwrap();

        let cache = CodexAuthCache::builder()
            .auth_ttl(Duration::from_secs(60))
            .auth_path(path)
            .build();

        // Eager build preload + second load inside TTL share one generation.
        assert_eq!(cache.generation().await, 1);
        let a = cache.load().await.unwrap();
        let b = cache.load().await.unwrap();
        assert_eq!(a.access_token, "file-acc");
        assert_eq!(b.access_token, "file-acc");
        assert_eq!(cache.generation().await, 1);
    }

    #[tokio::test]
    async fn file_cache_returns_stale_when_cannot_refresh() {
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

        let cache = CodexAuthCache::builder()
            .auth_ttl(Duration::from_secs(60))
            .auth_path(path)
            .build();

        // Stale id_token with no refresh_token: load still returns the file
        // token (no network) and does not error.
        let auth = cache.load().await.unwrap();
        assert_eq!(auth.access_token, "stale-acc");
        assert!(auth.needs_refresh(120));
        assert!(!auth.can_refresh());

        let forced = cache.force_refresh_after("stale-acc").await.unwrap();
        assert_eq!(forced.access_token, "stale-acc");
    }

    /// File-auth path must keep applying local `id_token` freshness: a cached
    /// entry with an empty/unparseable `id_token` is not a TTL fast-path hit
    /// even when still inside TTL (unlike source mode).
    #[tokio::test]
    async fn file_path_applies_id_token_freshness_inside_ttl() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");
        // Empty id_token => needs_refresh true; no refresh_token => no network.
        std::fs::write(
            &path,
            r#"{"tokens":{"access_token":"file-empty-id","account_id":"acct"}}"#,
        )
        .unwrap();

        let cache = CodexAuthCache::builder()
            .auth_ttl(Duration::from_secs(60))
            .auth_path(path.clone())
            .build();

        // Eager preload committed an entry that looks stale via id_token.
        {
            let guard = cache.state.read().await;
            let cached = guard.entry.as_ref().expect("eager preload");
            assert!(
                cached.fetched_at.elapsed() < Duration::from_secs(60),
                "still inside TTL"
            );
            assert!(
                cached.auth.needs_refresh(ID_TOKEN_REFRESH_MARGIN_SECS),
                "empty id_token must be treated as stale"
            );
            assert!(
                !cache.is_fresh_cache_hit(cached),
                "file path must not TTL-short-circuit past id_token freshness"
            );
        }

        let auth = cache.load().await.unwrap();
        assert_eq!(auth.access_token, "file-empty-id");
        assert!(auth.needs_refresh(ID_TOKEN_REFRESH_MARGIN_SECS));
        assert!(!auth.can_refresh());
        // File was re-read (proactive path attempted); contents unchanged.
        assert!(std::fs::read_to_string(&path)
            .unwrap()
            .contains("file-empty-id"));
    }

    #[tokio::test]
    async fn cache_debug_and_errors_do_not_leak_secrets() {
        let auth = cache_auth("sk-secret-bearer-leak-test");
        let cache = CodexAuthCache::builder()
            .auth_ttl(Duration::from_secs(60))
            .initial_auth(auth)
            .build();
        let loaded = cache.load().await.unwrap();
        let dbg = format!("{loaded:?}");
        assert!(
            !dbg.contains("sk-secret-bearer-leak-test"),
            "must not leak access_token in Debug: {dbg}"
        );
    }
}
