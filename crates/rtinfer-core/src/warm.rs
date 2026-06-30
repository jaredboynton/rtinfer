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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
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
pub struct WarmSessionPool {
    base_endpoint: String,
    per_model: usize,
    auth: AuthLoader,
    sessions: Mutex<HashMap<String, Vec<Arc<WarmSession>>>>,
}

impl WarmSessionPool {
    /// New pool keeping up to `per_model` warm sockets per distinct model. When
    /// `auth_source` is `None`, sessions load `~/.codex/auth.json`.
    pub fn new(per_model: usize, auth_source: Option<SharedCodexAuthSource>) -> Arc<Self> {
        Arc::new(Self {
            base_endpoint: REALTIME_URL.to_owned(),
            per_model: per_model.max(1),
            auth: AuthLoader::new(auth_source),
            sessions: Mutex::new(HashMap::new()),
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
        let session = self.pick(model).await;
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

    /// Pick a session for `model`. Selection strategy:
    ///   1. If any existing socket is idle (load 0), reuse it - this keeps
    ///      sequential callers pinned to ONE warm connection (no fresh
    ///      handshake, maximal prompt-cache stickiness).
    ///   2. Otherwise, if the ring is below `per_model`, open a new socket so
    ///      concurrent asks get real parallelism.
    ///   3. Otherwise, route to the least-loaded existing socket.
    ///
    /// A warm (already-connected) socket wins ties so we never resurrect a
    /// cold socket while a warm one sits idle.
    async fn pick(&self, model: Option<&str>) -> Arc<WarmSession> {
        let endpoint = protocol::endpoint_for_model(&self.base_endpoint, model);
        let key = model.unwrap_or("default").to_string();
        let mut map = self.sessions.lock().await;
        let ring = map.entry(key).or_default();

        // (1) An idle socket: prefer one that is already warm.
        let idle = ring
            .iter()
            .filter(|s| s.load() == 0)
            .min_by_key(|s| u8::from(!s.is_warm()));
        if let Some(s) = idle {
            return s.clone();
        }

        // (2) Grow the ring under concurrency.
        if ring.len() < self.per_model {
            let s = Arc::new(WarmSession::new(endpoint));
            ring.push(s.clone());
            return s;
        }

        // (3) Least-loaded existing socket.
        ring.iter()
            .min_by_key(|s| s.load())
            .expect("ring is non-empty once per_model >= 1")
            .clone()
    }
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
        let a = pool.pick(Some("gpt-realtime-2")).await;
        let b = pool.pick(Some("gpt-realtime-2")).await;
        assert!(
            Arc::ptr_eq(&a, &b),
            "idle socket must be reused, not multiplied"
        );
    }

    #[tokio::test]
    async fn pick_grows_ring_only_under_concurrency() {
        let pool = WarmSessionPool::new(2, None);
        // Simulate an in-flight ask pinning the first socket.
        let a = pool.pick(Some("gpt-realtime-2")).await;
        a.load.fetch_add(1, Ordering::Relaxed);
        // No idle socket -> grow the ring.
        let b = pool.pick(Some("gpt-realtime-2")).await;
        assert!(!Arc::ptr_eq(&a, &b), "busy socket forces a new one");
        b.load.fetch_add(1, Ordering::Relaxed);
        // Ring full and both busy -> route to least-loaded (here, a tie on a).
        let c = pool.pick(Some("gpt-realtime-2")).await;
        assert!(
            Arc::ptr_eq(&a, &c) || Arc::ptr_eq(&b, &c),
            "full busy ring routes to an existing least-loaded socket"
        );
    }

    #[tokio::test]
    async fn pick_keys_sessions_per_model() {
        let pool = WarmSessionPool::new(1, None);
        let mini = pool.pick(Some("gpt-realtime-mini")).await;
        let full = pool.pick(Some("gpt-realtime-2")).await;
        // Different models get independent rings (and endpoints).
        assert!(!Arc::ptr_eq(&mini, &full));
        assert!(mini.endpoint.contains("gpt-realtime-mini"));
        assert!(full.endpoint.contains("gpt-realtime-2"));
    }
}
