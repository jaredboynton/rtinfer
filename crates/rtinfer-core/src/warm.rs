//! Warm-session layer: persistent Realtime WebSockets reused across asks.
//!
//! # Why this exists
//!
//! [`RealtimePool`](crate::RealtimePool) and the bare `protocol::run_session_*`
//! functions open a FRESH WebSocket handshake (TLS + WS upgrade + `session.update`)
//! on every ask. For tiny per-query scoring calls that handshake is the dominant
//! latency (~800ms), so a remote-daemon borrow ends up ~2x slower than a local
//! warm socket even though the model turn is identical.
//!
//! The fix, proven in unifable's Python judge daemon: hold the socket OPEN and run
//! each ask as an OUT-OF-BAND response (`response.create` with
//! `"conversation":"none"`) carrying its own per-request `instructions` + `tools` +
//! `input`. The session prefix stays on one connection (maximising gpt-realtime-2
//! prompt-cache stickiness) and there is no per-call handshake.
//!
//! # Concurrency model
//!
//! A single Realtime session SERIALIZES responses on the server side, so true
//! parallelism comes from running N independent warm sessions. [`WarmSessionPool`]
//! keeps a small ring of [`WarmSession`] handles per model; each handle owns one
//! socket and a mutex that serializes its single in-flight out-of-band ask. A batch
//! fans out across handles. This deliberately avoids cid-multiplexing a single
//! socket: one out-of-band response per socket at a time keeps the read loop
//! trivially correct and still removes the handshake, which is the whole win.
//!
//! # Reconnect
//!
//! A session reconnects on WS close / read error / the 60-minute Realtime session
//! cap, refreshing auth when the handshake is rejected (401/403). Any ask error is
//! surfaced to the caller, which (per the rtinfer contract) maps it to a retryable
//! `provider_error` envelope so the client falls open.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::debug;

use crate::auth::SharedCodexAuthSource;
use crate::{protocol, CodexAuth, RealtimeError, RealtimeTool, REALTIME_URL};

/// Reconnect a warm session open at least this long, before the server-side
/// 60-minute Realtime cap forces a mid-ask close. Checked lazily at ask start.
const SESSION_MAX_AGE: Duration = Duration::from_secs(50 * 60);

/// Sticky-routing overflow threshold. When a family's pinned session has at
/// least this many in-flight asks, the next same-family ask overflows to the
/// least-loaded session WITHOUT re-pinning, so a parallel burst spreads across
/// the pool while the pinned session remains the cache home for the next serial
/// call. Default 1: a single in-flight ask overflows (maximises parallelism).
fn sticky_overflow_inflight() -> usize {
    std::env::var("UNIFABLE_STICKY_OVERFLOW_INFLIGHT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n >= 1)
        .unwrap_or(1)
}

/// Kill-switch for family-sticky routing. Default ON. When OFF, `pick` ignores
/// the system-prompt family hash and falls back to idle/grow/least-loaded (the
/// pre-sticky behavior). Mirrors the Python judge daemon's STICKY_ROUTING.
/// Read once at pool construction so per-pool tests do not race on the global
/// env (env vars are process-global and parallel tests would otherwise
/// interfere).
fn sticky_routing_enabled_at_build() -> bool {
    !matches!(
        std::env::var("UNIFABLE_STICKY_ROUTING")
            .ok()
            .as_deref()
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("0" | "false" | "no" | "off")
    )
}

/// Wall-clock ceiling for one out-of-band structured ask on a warm socket.
const ASK_WALL_CLOCK: Duration = Duration::from_secs(180);

/// One persistent Realtime socket bound to a single model endpoint. The mutex
/// serializes the one in-flight out-of-band response this socket carries.
struct WarmSession {
    endpoint: String,
    /// In-flight asks routed to this socket. Selection prefers the
    /// lowest-load (ideally idle, already-warm) socket so sequential callers
    /// reuse one warm connection instead of fanning onto cold sockets.
    load: AtomicUsize,
    inner: Mutex<Option<LiveSocket>>,
}

struct LiveSocket {
    ws: protocol::RealtimeWs,
    opened_at: Instant,
}

impl WarmSession {
    fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            load: AtomicUsize::new(0),
            inner: Mutex::new(None),
        }
    }

    fn load(&self) -> usize {
        self.load.load(Ordering::Relaxed)
    }

    /// Whether this socket is currently primed (a live connection exists). Used
    /// only to bias selection toward already-warm sockets; the ask path still
    /// (re)connects under its own mutex when needed.
    fn is_warm(&self) -> bool {
        self.inner.try_lock().map(|g| g.is_some()).unwrap_or(true)
    }

    /// Run one structured ask on this socket, (re)connecting + priming as needed.
    /// On an auth-rejected handshake the auth is refreshed and the connect retried
    /// once. The schema rides a single forced function tool; that call's arguments
    /// are the result (text that parses as JSON is salvaged as a fallback).
    async fn ask_structured(
        &self,
        auth_loader: &AuthLoader,
        system: &str,
        user: &str,
        schema_name: &str,
        schema: Value,
        reasoning_effort: Option<&str>,
    ) -> Result<Value, RealtimeError> {
        let mut guard = self.inner.lock().await;

        // Connect (with one auth-refresh retry on a rejected handshake).
        let need_connect = guard
            .as_ref()
            .map(|s| s.opened_at.elapsed() >= SESSION_MAX_AGE)
            .unwrap_or(true);
        if need_connect {
            if let Some(old) = guard.take() {
                let mut ws = old.ws;
                let _ = protocol::graceful_close(&mut ws).await;
            }
            let auth = auth_loader.load(false).await?;
            match self.connect_and_prime(&auth).await {
                Ok(live) => *guard = Some(live),
                Err(e) if crate::responses::is_auth_handshake_error(&e) => {
                    debug!(error = %e, "warm: auth rejected at handshake; refreshing + one retry");
                    let auth2 = auth_loader.load(true).await?;
                    *guard = Some(self.connect_and_prime(&auth2).await?);
                }
                Err(e) => return Err(e),
            }
        }

        let live = guard
            .as_mut()
            .ok_or_else(|| RealtimeError::Protocol("warm: no live socket after connect".into()))?;

        // Dispatch the out-of-band response and read it to completion. On any
        // socket error, drop the session so the next ask reconnects.
        match Self::run_one(
            &mut live.ws,
            system,
            user,
            schema_name,
            schema,
            reasoning_effort,
        )
        .await
        {
            Ok(v) => Ok(v),
            Err(e) => {
                *guard = None;
                Err(e)
            }
        }
    }

    async fn connect_and_prime(&self, auth: &CodexAuth) -> Result<LiveSocket, RealtimeError> {
        let (mut ws, _t0, connected_ms) =
            protocol::open_realtime_session(auth, &self.endpoint, None).await?;
        protocol::prime_session(&mut ws).await?;
        debug!(connected_ms, endpoint = %self.endpoint, "warm: session (re)connected");
        Ok(LiveSocket {
            ws,
            opened_at: Instant::now(),
        })
    }

    /// Send one out-of-band `response.create` and assemble its structured result.
    async fn run_one(
        ws: &mut protocol::RealtimeWs,
        system: &str,
        user: &str,
        schema_name: &str,
        mut schema: Value,
        reasoning_effort: Option<&str>,
    ) -> Result<Value, RealtimeError> {
        crate::responses::require_all_object_properties_for_strict_schema(&mut schema);
        let tool = RealtimeTool::function(
            schema_name.to_string(),
            "Return the structured result. Call exactly once with the complete object.",
            schema,
        );
        let frame = response_create_frame(system, user, &tool, reasoning_effort);
        protocol::send_value(ws, &frame).await?;

        let deadline = Instant::now() + ASK_WALL_CLOCK;
        let mut text_fallback = String::new();
        loop {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| RealtimeError::Protocol("warm: ask wall clock timeout".into()))?;
            let env = match timeout(remaining, protocol::next_envelope(ws)).await {
                Ok(Ok(Some(env))) => env,
                Ok(Ok(None)) => {
                    return Err(RealtimeError::Protocol(
                        "warm: socket closed mid-ask".into(),
                    ))
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    return Err(RealtimeError::Protocol(
                        "warm: ask wall clock timeout".into(),
                    ))
                }
            };
            match protocol::warm_envelope_outcome(&env, &mut text_fallback)? {
                protocol::AskOutcome::Pending => continue,
                protocol::AskOutcome::Object(v) => return Ok(v),
                protocol::AskOutcome::Done => {
                    return serde_json::from_str::<Value>(text_fallback.trim()).map_err(|_| {
                        RealtimeError::Protocol(
                            "warm: response had no tool call and non-JSON text".into(),
                        )
                    })
                }
            }
        }
    }
}

/// Build an out-of-band `response.create` frame (`conversation:"none"`) carrying
/// its own instructions + forced tool + the user turn. Mirrors the Python judge
/// daemon's `_response_create`.
fn response_create_frame(
    system: &str,
    user: &str,
    tool: &RealtimeTool,
    reasoning_effort: Option<&str>,
) -> Value {
    let mut response = json!({
        "conversation": "none",
        "output_modalities": ["text"],
        "instructions": system,
        "tools": [tool],
        "tool_choice": "required",
        "input": [{
            "type": "message",
            "role": "user",
            "content": [{ "type": "input_text", "text": format!("QUESTION: {user}") }],
        }],
    });
    if let Some(effort) = reasoning_effort {
        let effort = effort.trim().to_lowercase();
        if matches!(
            effort.as_str(),
            "none" | "minimal" | "low" | "medium" | "high"
        ) {
            response["reasoning"] = json!({ "effort": effort });
        }
    }
    json!({ "type": "response.create", "response": response })
}

/// Lazily loads + caches auth for the pool, refreshing through the configured
/// source (or `~/.codex/auth.json`) on demand.
struct AuthLoader {
    auth_source: Option<SharedCodexAuthSource>,
    cached: Mutex<Option<CodexAuth>>,
}

impl AuthLoader {
    fn new(auth_source: Option<SharedCodexAuthSource>) -> Self {
        Self {
            auth_source,
            cached: Mutex::new(None),
        }
    }

    async fn load(&self, force: bool) -> Result<CodexAuth, RealtimeError> {
        let mut guard = self.cached.lock().await;
        if !force {
            if let Some(a) = guard.as_ref() {
                return Ok(a.clone());
            }
        }
        let auth = match self.auth_source.as_ref() {
            Some(source) => source.load().await?,
            None => CodexAuth::from_default_path()?,
        };
        *guard = Some(auth.clone());
        Ok(auth)
    }
}

/// Process-shared pool of warm sessions, lazily grown per model up to `per_model`.
///
/// # Prompt-cache sticky routing
///
/// gpt-realtime caches every prefix it has seen on a given session. To maximise
/// cache hits, `pick` hashes the system prompt to a "family" and pins each
/// family to a specific session so repeated calls with the same instructions
/// land on the same warm socket. When the pinned session is busy (>=
/// `STICKY_OVERFLOW_INFLIGHT` in-flight), the next same-family ask overflows to
/// the least-loaded session WITHOUT re-pinning, so a parallel burst spreads
/// across the pool while the pinned session stays the cache home for the next
/// serial call. A dead (dropped-socket) pinned session triggers a cold re-pin
/// to the least-loaded live session. This mirrors the Python judge daemon's
/// `_pick_worker` / `_family_to_worker` sticky routing.
pub struct WarmSessionPool {
    base_endpoint: String,
    per_model: usize,
    sticky: bool,
    auth: AuthLoader,
    sessions: Mutex<HashMap<String, Vec<Arc<WarmSession>>>>,
    /// family_hash -> pinned session (Weak so a dropped session is GC-able and
    /// the upgrade check doubles as the "still alive" readiness gate).
    family_pins: Mutex<HashMap<(String, u64), Weak<WarmSession>>>,
}

impl WarmSessionPool {
    /// New pool keeping up to `per_model` warm sockets per distinct model. When
    /// `auth_source` is `None`, sessions load `~/.codex/auth.json`. Sticky
    /// routing (system-prompt -> pinned session) is ON by default; disable via
    /// `UNIFABLE_STICKY_ROUTING=0` in the environment at construction time.
    pub fn new(per_model: usize, auth_source: Option<SharedCodexAuthSource>) -> Arc<Self> {
        Arc::new(Self {
            base_endpoint: REALTIME_URL.to_owned(),
            per_model: per_model.max(1),
            sticky: sticky_routing_enabled_at_build(),
            auth: AuthLoader::new(auth_source),
            sessions: Mutex::new(HashMap::new()),
            family_pins: Mutex::new(HashMap::new()),
        })
    }

    /// Test-only constructor with explicit sticky-routing control so parallel
    /// tests do not race on the global env var.
    #[cfg(test)]
    fn new_with_sticky(per_model: usize, sticky: bool) -> Arc<Self> {
        Arc::new(Self {
            base_endpoint: REALTIME_URL.to_owned(),
            per_model: per_model.max(1),
            sticky,
            auth: AuthLoader::new(None),
            sessions: Mutex::new(HashMap::new()),
            family_pins: Mutex::new(HashMap::new()),
        })
    }

    /// One structured ask over a warm socket for `model` (None = pool default).
    pub async fn ask_structured(
        &self,
        system: &str,
        user: &str,
        schema_name: &str,
        schema: Value,
        model: Option<&str>,
        reasoning_effort: Option<&str>,
    ) -> Result<Value, RealtimeError> {
        let session = self.pick(model, system).await;
        session.load.fetch_add(1, Ordering::Relaxed);
        let out = session
            .ask_structured(
                &self.auth,
                system,
                user,
                schema_name,
                schema,
                reasoning_effort,
            )
            .await;
        session.load.fetch_sub(1, Ordering::Relaxed);
        out
    }

    /// Pick a session for `model` + `system` (the system prompt seeds the
    /// sticky-routing family hash). Selection strategy:
    ///
    /// When sticky routing is ON (default):
    ///   1. Look up the family's pinned session. If it is alive (upgradeable
    ///      Weak) and its load is below `STICKY_OVERFLOW_INFLIGHT`, return it ->
    ///      same socket -> prompt-cache hit on the instructions+tools prefix.
    ///   2. If the pinned session is alive but busy (>= overflow), return the
    ///      least-loaded session WITHOUT re-pinning, so a same-family parallel
    ///      burst spreads across the pool and the pinned session stays the
    ///      cache home for the next serial call.
    ///   3. Cold start or dead (dropped) pinned session: pick the least-loaded
    ///      session (growing the ring if under capacity) and PIN it as the new
    ///      cache home for this family.
    ///
    /// When sticky routing is OFF: fall back to idle/grow/least-loaded (the
    /// pre-sticky behavior), ignoring the family hash.
    async fn pick(&self, model: Option<&str>, system: &str) -> Arc<WarmSession> {
        let endpoint = protocol::endpoint_for_model(&self.base_endpoint, model);
        let key = model.unwrap_or("default").to_string();
        let family = family_hash(system);

        // (Sticky) Check the family pin BEFORE taking the sessions lock so a
        // hot cache hit does not serialize behind ring growth. The Weak upgrade
        // is the readiness gate: a dropped session fails to upgrade and falls
        // through to the cold path.
        if self.sticky {
            let pin_key = (key.clone(), family);
            let sticky = {
                let pins = self.family_pins.lock().await;
                pins.get(&pin_key).and_then(Weak::upgrade)
            };
            if let Some(s) = sticky {
                if s.load() < sticky_overflow_inflight() {
                    return s; // cache hit: same family -> same session
                }
                // Overflow: least-loaded WITHOUT re-pin. The pinned session
                // stays the cache home for the next serial call.
                return self.select_cold(&endpoint, &key).await;
            }
            // Cold or dead-sticky: select least-loaded and pin it.
            let chosen = self.select_cold(&endpoint, &key).await;
            self.family_pins
                .lock()
                .await
                .insert(pin_key, Arc::downgrade(&chosen));
            return chosen;
        }

        // Sticky off: plain idle/grow/least-loaded, no pinning.
        self.select_cold(&endpoint, &key).await
    }

    /// Least-loaded selection with lazy ring growth:
    ///   1. An idle socket (load 0): prefer one that is already warm.
    ///   2. Otherwise, if the ring is below `per_model`, open a new socket.
    ///   3. Otherwise, route to the least-loaded existing socket (warm wins
    ///      ties).
    async fn select_cold(&self, endpoint: &str, key: &str) -> Arc<WarmSession> {
        let mut map = self.sessions.lock().await;
        let ring = map.entry(key.to_string()).or_default();

        // (1) An idle socket: prefer one that is already warm (live connection).
        if let Some(s) = ring
            .iter()
            .filter(|s| s.load() == 0)
            .min_by_key(|s| u8::from(!s.is_warm()))
        {
            return s.clone();
        }
        // (2) Grow the ring under concurrency.
        if ring.len() < self.per_model {
            let s = Arc::new(WarmSession::new(endpoint.to_string()));
            ring.push(s.clone());
            return s;
        }
        // (3) Least-loaded existing socket; warm wins ties.
        ring.iter()
            .min_by_key(|s| (s.load(), u8::from(!s.is_warm())))
            .expect("ring is non-empty once per_model >= 1")
            .clone()
    }
}

/// Stable per-process family hash of a system prompt. The Realtime model caches
/// every prefix it has seen on a session, so two calls with the same
/// instructions share a cache home when routed to the same session.
fn family_hash(system: &str) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    system.hash(&mut h);
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_is_out_of_band_with_forced_tool_and_question() {
        let tool = RealtimeTool::function("result", "desc", json!({"type": "object"}));
        let f = response_create_frame("SYS", "find the auth fn", &tool, None);
        assert_eq!(f["type"], "response.create");
        let r = &f["response"];
        // Out-of-band: must NOT touch the persistent conversation.
        assert_eq!(r["conversation"], "none");
        assert_eq!(r["instructions"], "SYS");
        assert_eq!(r["tool_choice"], "required");
        assert_eq!(r["tools"][0]["name"], "result");
        assert_eq!(
            r["input"][0]["content"][0]["text"], "QUESTION: find the auth fn",
            "user turn must ride the response, prefixed like the python daemon"
        );
        // No reasoning block when effort is omitted.
        assert!(r.get("reasoning").is_none());
    }

    #[test]
    fn frame_includes_reasoning_only_for_known_efforts() {
        let tool = RealtimeTool::function("result", "desc", json!({"type": "object"}));
        let lo = response_create_frame("s", "u", &tool, Some("low"));
        assert_eq!(lo["response"]["reasoning"]["effort"], "low");
        // An unknown effort is dropped rather than forwarded verbatim.
        let bad = response_create_frame("s", "u", &tool, Some("turbo"));
        assert!(bad["response"].get("reasoning").is_none());
    }

    #[tokio::test]
    async fn pick_reuses_one_idle_socket_for_sequential_asks() {
        let pool = WarmSessionPool::new(4, None);
        // Sequential picks (each released before the next) must return the SAME
        // socket: an idle one is reused rather than opening a cold connection.
        let a = pool.pick(Some("gpt-realtime-2"), "SYS").await;
        let b = pool.pick(Some("gpt-realtime-2"), "SYS").await;
        assert!(
            Arc::ptr_eq(&a, &b),
            "idle socket must be reused, not multiplied"
        );
    }

    #[tokio::test]
    async fn pick_grows_ring_only_under_concurrency() {
        let pool = WarmSessionPool::new(2, None);
        // Simulate an in-flight ask pinning the first socket.
        let a = pool.pick(Some("gpt-realtime-2"), "SYS").await;
        a.load.fetch_add(1, Ordering::Relaxed);
        // Same family + sticky busy -> overflow grows the ring (no re-pin).
        let b = pool.pick(Some("gpt-realtime-2"), "SYS").await;
        assert!(
            !Arc::ptr_eq(&a, &b),
            "busy sticky forces an overflow socket"
        );
        b.load.fetch_add(1, Ordering::Relaxed);
        // Ring full and both busy -> route to least-loaded (here, a tie on a).
        let c = pool.pick(Some("gpt-realtime-2"), "SYS").await;
        assert!(
            Arc::ptr_eq(&a, &c) || Arc::ptr_eq(&b, &c),
            "full busy ring routes to an existing least-loaded socket"
        );
    }

    #[tokio::test]
    async fn pick_keys_sessions_per_model() {
        let pool = WarmSessionPool::new(1, None);
        let mini = pool.pick(Some("gpt-realtime-mini"), "SYS").await;
        let full = pool.pick(Some("gpt-realtime-2"), "SYS").await;
        // Different models get independent rings (and endpoints).
        assert!(!Arc::ptr_eq(&mini, &full));
        assert!(mini.endpoint.contains("gpt-realtime-mini"));
        assert!(full.endpoint.contains("gpt-realtime-2"));
    }

    #[tokio::test]
    async fn pick_sticks_same_family_to_one_session() {
        // The core sticky property: two sequential asks with the SAME system
        // prompt land on the SAME session (cache home). Distinct cold families
        // may share an idle session (the machine caches every prefix it has
        // seen), but once a family is pinned it sticks to that session even
        // after other activity grows the ring.
        let pool = WarmSessionPool::new(3, None);
        let fa = pool.pick(Some("gpt-realtime-2"), "GRADE").await;
        // Grow the ring by holding fa busy so a different family cold-pins to a
        // new session, then release fa.
        fa.load.fetch_add(1, Ordering::Relaxed);
        let fb = pool.pick(Some("gpt-realtime-2"), "GROUNDED").await;
        assert!(
            !Arc::ptr_eq(&fa, &fb),
            "distinct families under load get distinct sessions"
        );
        fa.load.fetch_sub(1, Ordering::Relaxed);
        // The first family sticks to its original pinned session (cache hit).
        let fa2 = pool.pick(Some("gpt-realtime-2"), "GRADE").await;
        assert!(
            Arc::ptr_eq(&fa, &fa2),
            "same family must stick to its pinned cache home"
        );
        // The second family sticks to its own pinned session, not fa's.
        let fb2 = pool.pick(Some("gpt-realtime-2"), "GROUNDED").await;
        assert!(
            Arc::ptr_eq(&fb, &fb2),
            "second family sticks to its own cache home"
        );
    }

    #[tokio::test]
    async fn pick_overflow_does_not_repin() {
        // When the sticky session is busy (>= STICKY_OVERFLOW_INFLIGHT), the
        // next same-family ask overflows to another session WITHOUT re-pinning:
        // after the sticky session drains, the next serial call sticks back.
        let pool = WarmSessionPool::new(3, None);
        let fa = pool.pick(Some("gpt-realtime-2"), "ARM").await;
        fa.load.fetch_add(1, Ordering::Relaxed); // simulate in-flight
                                                 // Overflow: same family, but sticky is busy -> a different session.
        let over = pool.pick(Some("gpt-realtime-2"), "ARM").await;
        assert!(
            !Arc::ptr_eq(&fa, &over),
            "busy sticky overflows to another session"
        );
        // Drain the sticky session.
        fa.load.fetch_sub(1, Ordering::Relaxed);
        // Next serial same-family call sticks back to the original cache home.
        let fa2 = pool.pick(Some("gpt-realtime-2"), "ARM").await;
        assert!(
            Arc::ptr_eq(&fa, &fa2),
            "overflow must not re-pin; sticky home survives a burst"
        );
    }

    #[tokio::test]
    async fn pick_dead_sticky_repins_cold() {
        // A dropped (dead) pinned session fails the Weak upgrade and falls
        // through to the cold path, which re-pins to a live session.
        let pool = WarmSessionPool::new(3, None);
        let fa = pool.pick(Some("gpt-realtime-2"), "ZONE").await;
        // Drop the strong ref in the ring by simulating a dead pin: replace the
        // family pin with a Weak to a session no longer in the ring.
        let orphan = Arc::new(WarmSession::new("wss://orphan".to_string()));
        let pin_key = ("gpt-realtime-2".to_string(), family_hash("ZONE"));
        pool.family_pins
            .lock()
            .await
            .insert(pin_key, Arc::downgrade(&orphan));
        drop(orphan);
        // Cold re-pin: the next ZONE call must NOT return the orphan (it cannot
        // upgrade) and must pin a live ring session instead.
        let fa2 = pool.pick(Some("gpt-realtime-2"), "ZONE").await;
        assert!(
            !Arc::ptr_eq(&fa, &fa2) || ring_has(&pool, "gpt-realtime-2", &fa2).await,
            "dead sticky repins to a live ring session"
        );
        // The new pin is live: a follow-up sticks to it.
        let fa3 = pool.pick(Some("gpt-realtime-2"), "ZONE").await;
        assert!(
            Arc::ptr_eq(&fa2, &fa3),
            "repinned session is the new sticky cache home"
        );
    }

    #[tokio::test]
    async fn pick_sticky_off_falls_back_to_least_loaded() {
        // With sticky routing disabled at construction, family pinning is off:
        // two families share sessions by idle/least-loaded, and no pins are
        // recorded. Uses the per-pool constructor so parallel tests do not race
        // on the global env var.
        let pool = WarmSessionPool::new_with_sticky(2, false);
        let a = pool.pick(Some("gpt-realtime-2"), "GRADE").await;
        let b = pool.pick(Some("gpt-realtime-2"), "GROUNDED").await;
        // With sticky off and both idle, both families land on the SAME idle
        // session (the pre-sticky idle-reuse behavior).
        assert!(
            Arc::ptr_eq(&a, &b),
            "sticky off reuses the idle socket regardless of family"
        );
        assert!(
            pool.family_pins.lock().await.is_empty(),
            "no pins recorded when sticky is off"
        );
    }

    async fn ring_has(pool: &WarmSessionPool, model: &str, s: &Arc<WarmSession>) -> bool {
        let map = pool.sessions.lock().await;
        map.get(model)
            .map(|ring| ring.iter().any(|r| Arc::ptr_eq(r, s)))
            .unwrap_or(false)
    }

    #[test]
    fn family_hash_is_deterministic_and_distinct() {
        assert_eq!(family_hash("GRADE"), family_hash("GRADE"));
        assert_ne!(family_hash("GRADE"), family_hash("GROUNDED"));
        assert_ne!(family_hash(""), family_hash("GRADE"));
    }
}
