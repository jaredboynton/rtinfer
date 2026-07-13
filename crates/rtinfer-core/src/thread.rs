//! Thread layer: per-thread pinned Realtime sockets with a server-side
//! append-only conversation, for transcript-shaped judge workloads.
//!
//! # Why this exists
//!
//! [`WarmSessionPool`](crate::WarmSessionPool) runs every ask OUT-OF-BAND
//! (`conversation:"none"`), so the full transcript context is resent inside the
//! user message on every call. For judge workloads that re-ask over one growing
//! host transcript, that resend dominates cost: the transcript prefix is
//! byte-stable across calls but never lands in the provider's prompt cache
//! because it rides a fresh single message each time.
//!
//! A thread instead appends transcript records as individual
//! `conversation.item.create` items on a dedicated socket's DEFAULT
//! conversation, and runs each ask OUT-OF-BAND (`response.create` with
//! `conversation:"none"`) whose `input` carries `item_reference` entries
//! pointing at the appended transcript items plus one inline question message.
//! The conversation holds ONLY the transcript items — a byte-stable, append-only
//! prefix that lands in the provider's per-session prompt cache. Each ask pays
//! for only the new items (appended since the last ask) plus the question; the
//! cached prefix is never re-processed.
//!
//! # Wire statelessness
//!
//! The client stays stateless: every request carries the FULL current item
//! window (loopback resend is free). The daemon diffs the request's item ids
//! against what it already appended on the thread's socket:
//!
//! - ids match a prefix -> append only the new items (cache hit path)
//! - mismatch (client window slid, config changed, socket aged out or dropped)
//!   -> reconnect and replay the whole window (one re-prefill, which is exactly
//!   what every single stateless ask costs)
//!
//! So the worst case degenerates to the stateless path, never below it.
//!
//! # Conversation discipline
//!
//! session.update pins instructions + the forced result tool for the thread's
//! lifetime (a thread is per judge-family, so these are constant). Each ask
//! appends new transcript items to the conversation, then sends an out-of-band
//! `response.create` (`conversation:"none"`) whose `input` references every
//! appended item plus the inline question. The response (function call + output)
//! is out-of-band: nothing is added to the conversation, so the prefix stays
//! stable and the prompt cache survives across asks. Nothing is ever deleted:
//! deletion would shift the cached prefix. When the appended volume exceeds
//! [`thread_max_chars`], the next ask reconnects and replays the
//! (client-truncated) window instead.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::debug;

use crate::auth::SharedCodexAuthSource;
use crate::warm::AuthLoader;
use crate::{protocol, RealtimeError, RealtimeTool, REALTIME_URL};

/// Reconnect before the server-side 60-minute Realtime session cap.
const SESSION_MAX_AGE: Duration = Duration::from_secs(50 * 60);

/// Wall-clock ceiling for one thread ask (append + response read).
const ASK_WALL_CLOCK: Duration = Duration::from_secs(120);

/// Idle threads older than this are evictable.
const THREAD_IDLE_TTL: Duration = Duration::from_secs(2 * 60 * 60);

/// Max distinct threads held before LRU eviction.
const DEFAULT_MAX_THREADS: usize = 64;

/// Ceiling on total appended item chars per socket before a forced
/// reconnect+replay. A safety net under the model context window; the client's
/// own window truncation normally triggers replay first.
fn thread_max_chars() -> usize {
    std::env::var("RTINFER_THREAD_MAX_CHARS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n >= 10_000)
        .unwrap_or(480_000)
}

/// One transcript item in a thread window: a client-stable `id` (content-hash
/// derived, so an unchanged record keeps its id across calls) plus its
/// rendered text.
#[derive(Debug, Clone, Deserialize)]
pub struct ThreadItem {
    pub id: String,
    pub text: String,
}

/// Result of one thread ask: the structured object plus append/replay
/// accounting and any provider usage from `response.done`.
#[derive(Debug)]
pub struct ThreadAskOutcome {
    pub object: Value,
    pub usage: Option<Value>,
    pub appended: usize,
    pub replayed: bool,
    pub total_items: usize,
}

struct LiveThreadSocket {
    ws: protocol::RealtimeWs,
    opened_at: Instant,
}

#[derive(Default)]
struct ThreadState {
    live: Option<LiveThreadSocket>,
    /// Ids of transcript items already appended to the live socket, in order.
    appended_ids: Vec<String>,
    appended_chars: usize,
    /// Hash of (system, schema_name, schema, model, reasoning_effort); a
    /// change means the session.update prefix changed -> reset.
    config_hash: u64,
}

/// One thread: a dedicated socket + its append-only item log, serialized by a
/// mutex (judge asks on one thread are inherently sequential).
struct ThreadSession {
    endpoint: String,
    state: Mutex<ThreadState>,
    last_used: Mutex<Instant>,
}

fn config_hash(
    system: &str,
    schema_name: &str,
    schema: &Value,
    model: Option<&str>,
    reasoning_effort: Option<&str>,
) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    system.hash(&mut h);
    schema_name.hash(&mut h);
    schema.to_string().hash(&mut h);
    model.unwrap_or("").hash(&mut h);
    reasoning_effort.unwrap_or("").hash(&mut h);
    h.finish()
}

/// True when `appended` is exactly the leading ids of `items` (the request
/// window strictly extends what the socket already holds).
fn ids_extend(appended: &[String], items: &[ThreadItem]) -> bool {
    appended.len() <= items.len() && appended.iter().zip(items.iter()).all(|(a, b)| *a == b.id)
}

fn item_create_frame(item: &ThreadItem) -> Value {
    json!({
        "type": "conversation.item.create",
        "item": {
            "id": item.id,
            "type": "message",
            "role": "user",
            "content": [{ "type": "input_text", "text": item.text }],
        },
    })
}

/// Build an out-of-band `response.create` (`conversation:"none"`) whose `input`
/// references every appended transcript item via `item_reference` (pulling them
/// from the server-side conversation's prompt cache) plus one inline question
/// message. The response is out-of-band: nothing is added to the conversation,
/// so the transcript-only prefix stays stable and the prompt cache survives.
fn response_create_frame(
    appended_ids: &[String],
    user: &str,
    reasoning_effort: Option<&str>,
) -> Value {
    let mut input: Vec<Value> = appended_ids
        .iter()
        .map(|id| json!({ "type": "item_reference", "id": id }))
        .collect();
    input.push(json!({
        "type": "message",
        "role": "user",
        "content": [{ "type": "input_text", "text": format!("QUESTION: {user}") }],
    }));
    let mut response = json!({
        "conversation": "none",
        "tool_choice": "required",
        "input": input,
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

impl ThreadSession {
    fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            state: Mutex::new(ThreadState::default()),
            last_used: Mutex::new(Instant::now()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn ask(
        &self,
        auth: &AuthLoader,
        system: &str,
        user: &str,
        schema_name: &str,
        mut schema: Value,
        items: &[ThreadItem],
        reasoning_effort: Option<&str>,
        model: Option<&str>,
    ) -> Result<ThreadAskOutcome, RealtimeError> {
        *self.last_used.lock().await = Instant::now();
        let mut st = self.state.lock().await;

        crate::responses::require_all_object_properties_for_strict_schema(&mut schema);
        let cfg = config_hash(system, schema_name, &schema, model, reasoning_effort);
        let new_chars: usize = items
            .iter()
            .skip(st.appended_ids.len())
            .map(|i| i.text.len())
            .sum();

        let need_reset = st.live.is_none()
            || st
                .live
                .as_ref()
                .map(|l| l.opened_at.elapsed() >= SESSION_MAX_AGE)
                .unwrap_or(true)
            || st.config_hash != cfg
            || !ids_extend(&st.appended_ids, items)
            || st.appended_chars + new_chars > thread_max_chars();

        let replayed = need_reset;
        if need_reset {
            if let Some(old) = st.live.take() {
                let mut ws = old.ws;
                let _ = protocol::graceful_close(&mut ws).await;
            }
            st.appended_ids.clear();
            st.appended_chars = 0;
            let tool = RealtimeTool::function(
                schema_name.to_string(),
                "Return the structured result. Call exactly once with the complete object."
                    .to_string(),
                schema.clone(),
            );
            let live = self.connect(auth, system, &tool).await?;
            st.live = Some(live);
            st.config_hash = cfg;
        }

        // Append the delta (or, after a reset, the whole window).
        let start = st.appended_ids.len();
        let to_append: Vec<ThreadItem> = items[start..].to_vec();
        let appended = to_append.len();
        {
            let live = st
                .live
                .as_mut()
                .ok_or_else(|| RealtimeError::Protocol("thread: no live socket".into()))?;
            for item in &to_append {
                if let Err(e) = protocol::send_value(&mut live.ws, &item_create_frame(item)).await {
                    st.live = None;
                    return Err(e);
                }
            }
        }
        for item in &to_append {
            st.appended_chars += item.text.len();
            st.appended_ids.push(item.id.clone());
        }
        let total_items = st.appended_ids.len();

        // Out-of-band response: references the appended transcript items via
        // item_reference + inline question. Nothing added to the conversation.
        // Any error drops the socket so the next ask reconnects and replays.
        // Clone the id list before the mutable borrow of st.live.
        let appended_ids = st.appended_ids.clone();
        let live = st
            .live
            .as_mut()
            .ok_or_else(|| RealtimeError::Protocol("thread: no live socket".into()))?;
        match Self::run_ask(&mut live.ws, &appended_ids, user, reasoning_effort).await {
            Ok((object, usage)) => Ok(ThreadAskOutcome {
                object,
                usage,
                appended,
                replayed,
                total_items,
            }),
            Err(e) => {
                st.live = None;
                Err(e)
            }
        }
    }

    async fn connect(
        &self,
        auth: &AuthLoader,
        system: &str,
        tool: &RealtimeTool,
    ) -> Result<LiveThreadSocket, RealtimeError> {
        let creds = auth.load(false).await?;
        let attempt = protocol::open_realtime_session(&creds, &self.endpoint, None).await;
        let (mut ws, _t0, connected_ms) = match attempt {
            Ok(v) => v,
            Err(e) if crate::responses::is_auth_handshake_error(&e) => {
                debug!(error = %e, "thread: auth rejected at handshake; refreshing + one retry");
                let creds = auth.load(true).await?;
                protocol::open_realtime_session(&creds, &self.endpoint, None).await?
            }
            Err(e) => return Err(e),
        };
        let session_update = json!({
            "type": "session.update",
            "session": {
                "type": "realtime",
                "output_modalities": ["text"],
                "instructions": system,
                "tools": [tool],
                "tool_choice": "required",
            },
        });
        protocol::send_value(&mut ws, &session_update).await?;
        debug!(connected_ms, endpoint = %self.endpoint, "thread: session (re)connected");
        Ok(LiveThreadSocket {
            ws,
            opened_at: Instant::now(),
        })
    }

    /// Send one out-of-band `response.create` referencing the appended
    /// transcript items + inline question, read to `response.done`, and return
    /// the structured object. The response is out-of-band: no function-call
    /// closure is needed because nothing is added to the conversation.
    async fn run_ask(
        ws: &mut protocol::RealtimeWs,
        appended_ids: &[String],
        user: &str,
        reasoning_effort: Option<&str>,
    ) -> Result<(Value, Option<Value>), RealtimeError> {
        protocol::send_value(
            ws,
            &response_create_frame(appended_ids, user, reasoning_effort),
        )
        .await?;

        let deadline = Instant::now() + ASK_WALL_CLOCK;
        let mut text_fallback = String::new();
        loop {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| RealtimeError::Protocol("thread: ask wall clock timeout".into()))?;
            let env = match timeout(remaining, protocol::next_envelope(ws)).await {
                Ok(Ok(Some(env))) => env,
                Ok(Ok(None)) => {
                    return Err(RealtimeError::Protocol(
                        "thread: socket closed mid-ask".into(),
                    ))
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    return Err(RealtimeError::Protocol(
                        "thread: ask wall clock timeout".into(),
                    ))
                }
            };
            match env.kind.as_str() {
                "response.output_text.delta" => {
                    if let Some(d) = &env.delta {
                        text_fallback.push_str(d);
                    }
                }
                "response.done" | "response.completed" => {
                    let usage = env.response.as_ref().and_then(|r| r.usage.clone());
                    let mut calls = protocol::extract_tool_calls(&env)?;
                    if let Some(call) = calls.drain(..).next() {
                        // Out-of-band: the function call is NOT in the
                        // conversation, so no function_call_output is needed.
                        return Ok((call.arguments, usage));
                    }
                    if text_fallback.is_empty() {
                        text_fallback = protocol::collect_done_text(&env);
                    }
                    let parsed =
                        serde_json::from_str::<Value>(text_fallback.trim()).map_err(|_| {
                            RealtimeError::Protocol(
                                "thread: response had no tool call and non-JSON text".into(),
                            )
                        })?;
                    return Ok((parsed, usage));
                }
                "response.failed" | "error" => {
                    return Err(protocol::provider_error_from(env.error))
                }
                _ => continue,
            }
        }
    }
}

/// Registry of live threads, keyed by client thread id. LRU-bounded; idle
/// threads are evicted when the registry is full or on periodic sweep.
pub struct ThreadRegistry {
    base_endpoint: String,
    max_threads: usize,
    auth: AuthLoader,
    threads: Mutex<HashMap<String, Arc<ThreadSession>>>,
}

impl ThreadRegistry {
    pub fn new(auth_source: Option<SharedCodexAuthSource>) -> Arc<Self> {
        Arc::new(Self {
            base_endpoint: REALTIME_URL.to_owned(),
            max_threads: DEFAULT_MAX_THREADS,
            auth: AuthLoader::new(auth_source),
            threads: Mutex::new(HashMap::new()),
        })
    }

    /// One structured ask on `thread_id`'s pinned socket. `items` is the FULL
    /// current transcript window; the registry appends only what is new.
    #[allow(clippy::too_many_arguments)]
    pub async fn ask_structured(
        &self,
        thread_id: &str,
        system: &str,
        user: &str,
        schema_name: &str,
        schema: Value,
        items: Vec<ThreadItem>,
        model: Option<&str>,
        reasoning_effort: Option<&str>,
    ) -> Result<ThreadAskOutcome, RealtimeError> {
        let session = self.session_for(thread_id, model).await;
        session
            .ask(
                &self.auth,
                system,
                user,
                schema_name,
                schema,
                &items,
                reasoning_effort,
                model,
            )
            .await
    }

    async fn session_for(&self, thread_id: &str, model: Option<&str>) -> Arc<ThreadSession> {
        let endpoint = protocol::endpoint_for_model(&self.base_endpoint, model);
        // The model participates in the key so one thread id asked under two
        // models never shares a socket.
        let key = format!("{}::{}", model.unwrap_or("default"), thread_id);
        let mut map = self.threads.lock().await;
        if let Some(s) = map.get(&key) {
            return s.clone();
        }
        if map.len() >= self.max_threads {
            Self::evict_idle(&mut map).await;
        }
        let s = Arc::new(ThreadSession::new(endpoint));
        map.insert(key, s.clone());
        s
    }

    /// Drop idle threads (oldest first) until under capacity. Sessions with an
    /// in-flight ask hold their own Arc, so dropping the map entry is safe.
    async fn evict_idle(map: &mut HashMap<String, Arc<ThreadSession>>) {
        let mut candidates: Vec<(String, Instant)> = Vec::with_capacity(map.len());
        for (k, s) in map.iter() {
            let used = *s.last_used.lock().await;
            candidates.push((k.clone(), used));
        }
        candidates.sort_by_key(|(_, used)| *used);
        let target = DEFAULT_MAX_THREADS.saturating_sub(1).max(1);
        for (k, used) in candidates {
            if map.len() <= target {
                break;
            }
            if used.elapsed() >= THREAD_IDLE_TTL || map.len() > DEFAULT_MAX_THREADS {
                map.remove(&k);
            }
        }
        // Registry full of hot threads: shed the least-recently-used anyway so
        // a new thread can always start.
        if map.len() > target {
            if let Some(oldest) = {
                let mut best: Option<(String, Instant)> = None;
                for (k, s) in map.iter() {
                    let used = *s.last_used.lock().await;
                    if best.as_ref().map(|(_, u)| used < *u).unwrap_or(true) {
                        best = Some((k.clone(), used));
                    }
                }
                best.map(|(k, _)| k)
            } {
                map.remove(&oldest);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(id: &str, text: &str) -> ThreadItem {
        ThreadItem {
            id: id.to_string(),
            text: text.to_string(),
        }
    }

    #[test]
    fn ids_extend_accepts_pure_extension() {
        let appended = vec!["a".to_string(), "b".to_string()];
        let items = vec![item("a", "1"), item("b", "2"), item("c", "3")];
        assert!(ids_extend(&appended, &items));
    }

    #[test]
    fn ids_extend_accepts_identical_window() {
        let appended = vec!["a".to_string(), "b".to_string()];
        let items = vec![item("a", "1"), item("b", "2")];
        assert!(ids_extend(&appended, &items));
    }

    #[test]
    fn ids_extend_rejects_slid_window() {
        // Client truncated its window start: "a" fell off.
        let appended = vec!["a".to_string(), "b".to_string()];
        let items = vec![item("b", "2"), item("c", "3")];
        assert!(!ids_extend(&appended, &items));
    }

    #[test]
    fn ids_extend_rejects_shrunk_window() {
        let appended = vec!["a".to_string(), "b".to_string()];
        let items = vec![item("a", "1")];
        assert!(!ids_extend(&appended, &items));
    }

    #[test]
    fn config_hash_varies_on_every_input() {
        let schema = json!({"type": "object", "properties": {}});
        let base = config_hash("sys", "result", &schema, None, None);
        assert_ne!(base, config_hash("sys2", "result", &schema, None, None));
        assert_ne!(base, config_hash("sys", "other", &schema, None, None));
        assert_ne!(
            base,
            config_hash("sys", "result", &json!({"type": "object"}), None, None)
        );
        assert_ne!(
            base,
            config_hash(
                "sys",
                "result",
                &schema,
                Some("gpt-realtime-2.1-mini"),
                None
            )
        );
        assert_ne!(
            base,
            config_hash("sys", "result", &schema, None, Some("low"))
        );
    }

    #[test]
    fn item_frame_carries_client_id() {
        let f = item_create_frame(&item("u000001_abcd", "hello"));
        assert_eq!(f["item"]["id"], "u000001_abcd");
        assert_eq!(f["item"]["content"][0]["text"], "hello");
    }

    #[test]
    fn response_frame_is_out_of_band_with_item_references() {
        let ids = vec!["u000001_abcd".to_string(), "u000002_efgh".to_string()];
        let f = response_create_frame(&ids, "is it grounded?", None);
        assert_eq!(f["response"]["conversation"], "none");
        assert_eq!(f["response"]["tool_choice"], "required");
        // input = [item_reference...] + inline question
        assert_eq!(f["response"]["input"][0]["type"], "item_reference");
        assert_eq!(f["response"]["input"][0]["id"], "u000001_abcd");
        assert_eq!(f["response"]["input"][1]["type"], "item_reference");
        assert_eq!(f["response"]["input"][1]["id"], "u000002_efgh");
        assert_eq!(f["response"]["input"][2]["type"], "message");
        assert_eq!(
            f["response"]["input"][2]["content"][0]["text"],
            "QUESTION: is it grounded?"
        );
    }

    #[test]
    fn response_frame_empty_items_just_question() {
        let f = response_create_frame(&[], "hello", None);
        assert_eq!(f["response"]["conversation"], "none");
        assert_eq!(f["response"]["input"].as_array().unwrap().len(), 1);
        assert_eq!(f["response"]["input"][0]["type"], "message");
    }

    #[test]
    fn response_frame_validates_effort() {
        let ids = vec!["x".to_string()];
        let f = response_create_frame(&ids, "q", Some("low"));
        assert_eq!(f["response"]["reasoning"]["effort"], "low");
        let f = response_create_frame(&ids, "q", Some("bogus"));
        assert!(f["response"].get("reasoning").is_none());
    }
}
