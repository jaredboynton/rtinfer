//! Connection / handshake amortization layer for the Realtime API.
//!
//! # Why a "pool" when sessions are request-scoped
//!
//! OpenAI's `gpt-realtime-2` WebSocket session is request-scoped on the
//! server side: each `ask()` / `ask_with_tools()` must own its own
//! WebSocket from `session.update` through `response.done`. That means
//! we cannot reuse a WebSocket across asks — the abstraction below is
//! deliberately *not* a connection pool in the HTTP/2 sense.
//!
//! What [`RealtimePool`] actually amortizes:
//!
//! - **`CodexAuth` fetch.** Today every cockpit worker calls
//!   [`CodexAuth::from_default_path`] per refresh, which hits
//!   `~/.codex/auth.json` from a tokio task and re-parses the file.
//!   The pool reads it once per process (with a short TTL so a fresh
//!   `codex login` is picked up without restarting the daemon).
//! - **rustls crypto provider install.** Already a `std::sync::Once`
//!   inside `protocol::run_session`; sharing the pool means the
//!   `Once` fires exactly once per process regardless of how many
//!   asks run concurrently.
//! - **Endpoint resolution.** The default endpoint string is materialised
//!   once instead of being cloned on every `RealtimeClient::new` call
//!   site.
//! - **In-flight observability.** The pool maintains a counter of
//!   active asks behind a `Semaphore::available_permits` view that
//!   tracing/metrics can read without changing call semantics.
//!
//! What the pool **does not** do:
//!
//! - It does not gate work on its in-flight count for the cockpit
//!   workload. The OpenAI Realtime quota on this account is
//!   **10k RPM / 10M TPM**, which is not a binding constraint for the
//!   cockpit fan-out (peak ~25 entities, each capped at 18 tool calls
//!   in 150s — well under either ceiling). The pool keeps a very high
//!   ceiling ([`DEFAULT_POOL_CAPACITY`] = 64) only as a runaway-leak
//!   guard, never as a throttling mechanism.
//! - It does not change retry semantics. Cockpit's
//!   `MAX_REPAIR_ATTEMPTS = 2` repair loop runs above the pool, not
//!   inside it.
//! - It does not throttle Slack / Kepler / Atlassian tool calls. Those
//!   downstream rate limits are enforced by the daemon's HTTP runner
//!   and the cockpit `dispatch_alias` path; the pool only saves on
//!   websocket handshake + auth fetch overhead.
//!
//! # Single shared instance
//!
//! The cockpit `AppState` and the orgchart MCP runtime share one pool
//! handle ([`Arc<RealtimePool>`]) so a high-fan-out `/api/refresh-all`
//! plus an org-chart `slack_get_orgchart` ask do not compete for
//! connection-establishment work. See [`global_pool`] for the
//! process-singleton accessor used by orgchart-runtime when no
//! explicit pool is plumbed in.
//!
//! # Lint guard authorisation
//!
//! `RealtimeClient::new` is `#[doc(hidden)]`. Any new caller outside
//! `crates/rtinfer-core/{src,tests}/` must go through
//! [`RealtimePool::ask`] or [`RealtimePool::ask_with_tools`]. The
//! `crates/rtinfer-core/tests/no_direct_construction.rs` integration
//! test walks every workspace `src` tree and fails if it finds a
//! direct `RealtimeClient::new` call from an unauthorised file. To
//! authorise a new caller, edit the `ALLOWED_PATHS` constant in that
//! test (the failure message points there explicitly).

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use tokio::sync::{RwLock, Semaphore};
use tracing::debug;

use crate::auth::{CodexAuth, SharedCodexAuthSource};
use crate::{
    protocol, RealtimeError, RealtimeRequest, RealtimeResponse, RealtimeStructuredRequest,
    RealtimeToolExecutor, RealtimeToolRequest, REALTIME_URL,
};

/// Maximum simultaneous in-flight Realtime asks the pool will admit.
///
/// Picked deliberately high (64) because the OpenAI Realtime quota on
/// this account (10k RPM / 10M TPM) is not a binding constraint for
/// cockpit fan-out workloads. This cap exists only as a runaway-leak
/// guard — if a future caller accidentally spawns thousands of tasks,
/// the pool back-pressures them rather than letting them all open
/// websockets simultaneously. It is **not** a throttle.
pub const DEFAULT_POOL_CAPACITY: usize = 64;

/// How long a cached [`CodexAuth`] is reused before being re-read from
/// `~/.codex/auth.json`. The codex CLI rotates `tokens.access_token`
/// roughly every 60 minutes; 30 minutes keeps the cache fresh enough
/// that we never hand out a token within 30 min of expiry.
pub const DEFAULT_AUTH_TTL: Duration = Duration::from_secs(30 * 60);

/// Sentinel value the builder uses when callers pass `0` to mean
/// "unbounded". Tokio's `Semaphore` rejects construction with
/// `usize::MAX`, so we settle for a value vastly larger than any
/// realistic cockpit / orgchart fan-out (2^31 is roughly 2.1 billion
/// permits — the cockpit will never spawn that many tasks).
const UNBOUNDED_CAPACITY: usize = 1 << 31;

/// Builder for [`RealtimePool`]. All fields are optional; reasonable
/// defaults match the production cockpit configuration.
#[derive(Default)]
pub struct RealtimePoolBuilder {
    endpoint: Option<String>,
    capacity: Option<usize>,
    auth_ttl: Option<Duration>,
    initial_auth: Option<CodexAuth>,
    auth_path: Option<PathBuf>,
    auth_source: Option<SharedCodexAuthSource>,
}

impl RealtimePoolBuilder {
    /// Override the WebSocket endpoint (used by tests).
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Override the in-flight ask cap. `0` is treated as "no observable
    /// cap" by clamping to `usize::MAX`. Default is
    /// [`DEFAULT_POOL_CAPACITY`].
    pub fn capacity(mut self, cap: usize) -> Self {
        self.capacity = Some(cap);
        self
    }

    /// Override how long [`CodexAuth`] is cached before being re-read
    /// from disk.
    pub fn auth_ttl(mut self, ttl: Duration) -> Self {
        self.auth_ttl = Some(ttl);
        self
    }

    /// Seed the pool with an explicit [`CodexAuth`] (tests use this so
    /// the pool never touches `~/.codex/auth.json`).
    pub fn initial_auth(mut self, auth: CodexAuth) -> Self {
        self.initial_auth = Some(auth);
        self
    }

    /// Override the path the pool reads `CodexAuth` from when its
    /// cache is stale or empty. Defaults to `~/.codex/auth.json`.
    pub fn auth_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.auth_path = Some(path.into());
        self
    }

    /// Supply a pluggable [`CodexAuthSource`](crate::CodexAuthSource). When
    /// set, the pool loads auth through it (ignoring `auth_path`); the daemon
    /// passes a keychain-backed source so the pool never reads
    /// `~/.codex/auth.json`.
    pub fn auth_source(mut self, source: SharedCodexAuthSource) -> Self {
        self.auth_source = Some(source);
        self
    }

    /// Build the pool. Pre-loads [`CodexAuth`] from disk eagerly so the
    /// first `ask` does not pay the file read; if loading fails, the
    /// pool still constructs and `ask` will surface the error on first
    /// use.
    pub fn build(self) -> Arc<RealtimePool> {
        let endpoint = self.endpoint.unwrap_or_else(|| REALTIME_URL.to_owned());
        let capacity = match self.capacity {
            Some(0) => UNBOUNDED_CAPACITY,
            Some(n) => n,
            None => DEFAULT_POOL_CAPACITY,
        };
        let auth_ttl = self.auth_ttl.unwrap_or(DEFAULT_AUTH_TTL);

        // Eager pre-load only for the file path. With an explicit
        // `auth_source` the first ask loads lazily through it (a keychain read
        // / okta-aio re-mint must not run synchronously in `build`).
        let cached = match self.initial_auth.clone() {
            Some(auth) => Some(CachedAuth {
                auth,
                fetched_at: Instant::now(),
            }),
            None if self.auth_source.is_some() => None,
            None => match self.auth_path.as_deref() {
                Some(p) => CodexAuth::from_path(p).ok().map(|auth| CachedAuth {
                    auth,
                    fetched_at: Instant::now(),
                }),
                None => CodexAuth::from_default_path().ok().map(|auth| CachedAuth {
                    auth,
                    fetched_at: Instant::now(),
                }),
            },
        };

        Arc::new(RealtimePool {
            endpoint,
            semaphore: Arc::new(Semaphore::new(capacity)),
            capacity,
            auth_ttl,
            auth_path: self.auth_path,
            auth_source: self.auth_source,
            cached_auth: RwLock::new(cached),
        })
    }
}

#[derive(Clone)]
struct CachedAuth {
    auth: CodexAuth,
    fetched_at: Instant,
}

/// Single shared handle that amortizes Realtime auth + handshake setup
/// across many concurrent asks. See module docs.
pub struct RealtimePool {
    endpoint: String,
    semaphore: Arc<Semaphore>,
    capacity: usize,
    auth_ttl: Duration,
    auth_path: Option<PathBuf>,
    auth_source: Option<SharedCodexAuthSource>,
    cached_auth: RwLock<Option<CachedAuth>>,
}

impl RealtimePool {
    /// New pool with all defaults (endpoint = OpenAI prod, capacity =
    /// [`DEFAULT_POOL_CAPACITY`], auth TTL = [`DEFAULT_AUTH_TTL`],
    /// reads `~/.codex/auth.json`). Equivalent to
    /// `RealtimePoolBuilder::default().build()`.
    pub fn new() -> Arc<Self> {
        RealtimePoolBuilder::default().build()
    }

    /// Start a new builder.
    pub fn builder() -> RealtimePoolBuilder {
        RealtimePoolBuilder::default()
    }

    /// Number of asks currently in-flight through this pool. Exposed
    /// for metrics/tracing only — *do not* gate work on this.
    pub fn in_flight(&self) -> usize {
        self.capacity
            .saturating_sub(self.semaphore.available_permits())
    }

    /// Configured maximum in-flight (the runaway-leak guard).
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The endpoint this pool dispatches against. Useful for tracing.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Run one Realtime ask. Acquires an in-flight permit, refreshes
    /// auth if stale, and dispatches the underlying protocol session
    /// directly — no `RealtimeClient` instance is constructed (the
    /// lint guard depends on this).
    pub async fn ask(&self, req: RealtimeRequest) -> Result<RealtimeResponse, RealtimeError> {
        let _permit =
            self.semaphore.acquire().await.map_err(|e| {
                RealtimeError::Protocol(format!("realtime pool semaphore closed: {e}"))
            })?;
        let auth = self.fresh_auth().await?;
        debug!(
            in_flight = self.in_flight(),
            capacity = self.capacity,
            "realtime: pool dispatch ask"
        );
        protocol::run_session(&auth, &self.endpoint, req).await
    }

    /// Run one Realtime ask with model-side function tools.
    pub async fn ask_with_tools(
        &self,
        req: RealtimeToolRequest,
        executor: &(dyn RealtimeToolExecutor + Send + Sync),
    ) -> Result<RealtimeResponse, RealtimeError> {
        let _permit =
            self.semaphore.acquire().await.map_err(|e| {
                RealtimeError::Protocol(format!("realtime pool semaphore closed: {e}"))
            })?;
        let auth = self.fresh_auth().await?;
        debug!(
            in_flight = self.in_flight(),
            capacity = self.capacity,
            "realtime: pool dispatch ask_with_tools"
        );
        protocol::run_session_with_tools(&auth, &self.endpoint, req, executor).await
    }

    /// One structured ask over gpt-realtime-2: the (strict-normalised)
    /// schema rides a single forced function tool and that call's
    /// arguments are the result. The signature mirrors
    /// `CodexResponsesPool::ask_structured` so call sites swap pools
    /// without reshaping. Retries exactly once on an auth-rejected
    /// handshake, mirroring the responses pool's reactive refresh.
    pub async fn ask_structured(
        &self,
        system: &str,
        user: &str,
        schema_name: &str,
        schema: serde_json::Value,
    ) -> Result<serde_json::Value, RealtimeError> {
        self.ask_structured_with_model(system, user, schema_name, schema, None)
            .await
    }

    /// Like [`RealtimePool::ask_structured`] but selects the realtime model
    /// per call (e.g. `gpt-realtime-mini` for navigators, `gpt-realtime-2`
    /// for scoring). `None` uses the pool's configured endpoint model. The
    /// model rewrites the endpoint's `model=` query param inside
    /// `run_session_structured`, so one pool serves both tiers.
    pub async fn ask_structured_with_model(
        &self,
        system: &str,
        user: &str,
        schema_name: &str,
        schema: serde_json::Value,
        model: Option<&str>,
    ) -> Result<serde_json::Value, RealtimeError> {
        self.ask_structured_with_opts(system, user, schema_name, schema, model, None)
            .await
    }

    /// Full-control structured ask: selects the realtime model AND the
    /// sampling temperature per call. `temperature` is floored at 0.6 by the
    /// Realtime API; pass `Some(0.6)` for the lowest-variance scoring asks.
    /// `None` uses the server default.
    pub async fn ask_structured_with_opts(
        &self,
        system: &str,
        user: &str,
        schema_name: &str,
        schema: serde_json::Value,
        model: Option<&str>,
        temperature: Option<f64>,
    ) -> Result<serde_json::Value, RealtimeError> {
        let _permit =
            self.semaphore.acquire().await.map_err(|e| {
                RealtimeError::Protocol(format!("realtime pool semaphore closed: {e}"))
            })?;
        let auth = self.fresh_auth().await?;
        let mut schema = schema;
        crate::responses::require_all_object_properties_for_strict_schema(&mut schema);
        let req = RealtimeStructuredRequest {
            instructions: system.to_owned(),
            context_blobs: vec![user.to_owned()],
            question: format!("Call the {schema_name} function now with the complete result."),
            schema_name: schema_name.to_owned(),
            schema,
            model: model.map(str::to_owned),
            temperature,
            handshake_timeout: None,
            wall_clock_timeout: None,
        };
        debug!(
            in_flight = self.in_flight(),
            capacity = self.capacity,
            model = %model.unwrap_or("(pool default)"),
            "realtime: pool dispatch ask_structured"
        );
        match protocol::run_session_structured(&auth, &self.endpoint, req.clone()).await {
            Err(e) if crate::responses::is_auth_handshake_error(&e) => {
                debug!(error = %e, "realtime: auth rejected at handshake; refreshing + one retry");
                self.invalidate_auth().await;
                let auth2 = self.fresh_auth().await?;
                protocol::run_session_structured(&auth2, &self.endpoint, req).await
            }
            other => other,
        }
    }

    /// Force-refresh the cached auth on the next ask. Used when a
    /// caller knows `~/.codex/auth.json` rotated (e.g. after a fresh
    /// `codex login`).
    pub async fn invalidate_auth(&self) {
        let mut guard = self.cached_auth.write().await;
        *guard = None;
    }

    async fn fresh_auth(&self) -> Result<CodexAuth, RealtimeError> {
        if let Some(cached) = self.cached_auth.read().await.as_ref() {
            if cached.fetched_at.elapsed() < self.auth_ttl {
                return Ok(cached.auth.clone());
            }
        }
        let mut guard = self.cached_auth.write().await;
        if let Some(cached) = guard.as_ref() {
            if cached.fetched_at.elapsed() < self.auth_ttl {
                return Ok(cached.auth.clone());
            }
        }
        let auth = match self.auth_source.as_ref() {
            Some(source) => source.load().await?,
            None => match self.auth_path.as_deref() {
                Some(p) => CodexAuth::from_path(p)?,
                None => CodexAuth::from_default_path()?,
            },
        };
        *guard = Some(CachedAuth {
            auth: auth.clone(),
            fetched_at: Instant::now(),
        });
        Ok(auth)
    }
}

static GLOBAL_POOL: OnceLock<Arc<RealtimePool>> = OnceLock::new();

/// Return the process-wide singleton pool, initialising it lazily with
/// defaults on first call. Used by call sites (e.g. orgchart MCP tool
/// handler) that don't have a parent struct to thread an
/// `Arc<RealtimePool>` through.
///
/// Cockpit code should construct its pool explicitly and stash it in
/// `AppState`; the singleton is the same instance because the first
/// caller wins and subsequent calls return the cached `Arc`.
pub fn global_pool() -> Arc<RealtimePool> {
    GLOBAL_POOL.get_or_init(RealtimePool::new).clone()
}

/// Install an explicit pool as the process singleton. Returns the
/// installed pool (which may be a previously-installed pool — the
/// singleton is set-once). Cockpit calls this at startup so any
/// orgchart-MCP ask later in the process shares its pool.
pub fn install_global(pool: Arc<RealtimePool>) -> Arc<RealtimePool> {
    let _ = GLOBAL_POOL.set(pool.clone());
    GLOBAL_POOL.get().expect("global pool set above").clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_auth() -> CodexAuth {
        CodexAuth {
            access_token: "tok".into(),
            account_id: "acct".into(),
            id_token: String::new(),
            refresh_token: String::new(),
            source_path: None,
        }
    }

    #[tokio::test]
    async fn fresh_auth_uses_cached_value() {
        let pool = RealtimePool::builder()
            .initial_auth(fake_auth())
            .auth_ttl(Duration::from_secs(60))
            .build();
        let a = pool.fresh_auth().await.unwrap();
        let b = pool.fresh_auth().await.unwrap();
        assert_eq!(a.access_token, b.access_token);
    }

    #[tokio::test]
    async fn capacity_zero_means_unbounded() {
        let pool = RealtimePool::builder()
            .initial_auth(fake_auth())
            .capacity(0)
            .build();
        assert_eq!(pool.capacity(), UNBOUNDED_CAPACITY);
    }

    #[tokio::test]
    async fn invalidate_auth_clears_cache() {
        let pool = RealtimePool::builder().initial_auth(fake_auth()).build();
        assert!(pool.cached_auth.read().await.is_some());
        pool.invalidate_auth().await;
        assert!(pool.cached_auth.read().await.is_none());
    }

    #[test]
    fn in_flight_starts_at_zero() {
        let pool = RealtimePool::builder().initial_auth(fake_auth()).build();
        assert_eq!(pool.in_flight(), 0);
    }

    #[tokio::test]
    async fn fresh_auth_loads_through_source_when_cache_cold() {
        // No initial_auth => cache is cold => fresh_auth must pull from the
        // injected source rather than reading ~/.codex/auth.json.
        let source = crate::auth::StaticCodexAuthSource(CodexAuth {
            access_token: "from-source".into(),
            account_id: "acct".into(),
            id_token: String::new(),
            refresh_token: String::new(),
            source_path: None,
        });
        let pool = RealtimePool::builder()
            .auth_source(std::sync::Arc::new(source))
            .build();
        let auth = pool.fresh_auth().await.unwrap();
        assert_eq!(auth.access_token, "from-source");
    }
}
