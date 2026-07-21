# Adaptive dual-transport Codex Responses client

**Status:** Implementation PRD  
**Date:** 2026-07-20  
**Target release:** `0.1.15`  

## Summary

`rtinfer` currently sends Codex Responses work only through `CodexResponsesPool`, while an unintegrated adaptive controller and a red HTTP/SSE parser test exist beside it. Build one `CodexResponsesClient` that can dispatch each logical request to either the confirmed free HTTP/2+SSE endpoint or an exclusive reusable WSS socket, while sharing auth, enforcing semantic completion, and admitting work through one cancellation-safe controller. The user-visible `rtinfer/1` responses tiers remain unchanged; rollout defaults to WSS and enables dual mode explicitly after deterministic and credentialed goodput evidence.

## Current-state evidence

- `crates/rtinfer-core/src/adaptive.rs` exists but returns a bare `ResponsesTransportKind`; cancellation can leak `in_flight`, and no production caller uses it.
- `crates/rtinfer-core/tests/adaptive_concurrency.rs` has two passing controller tests only.
- `crates/rtinfer-core/tests/responses_http_sse.rs` is intentionally red because `assemble_codex_responses_sse` does not exist.
- `crates/rtinfer-core/src/responses.rs::run_on_socket` can return text without observing `response.completed`; `CodexResponsesPool::run_warm` can then resend an already-attempted logical request.
- `crates/rtinfer-core/src/responses.rs::CodexResponsesPool` owns a second semaphore and its own auth cache. No HTTP Responses client or daemon selector exists.
- `crates/rtinfer-daemon/src/server.rs::AppState` owns `Arc<CodexResponsesPool>` and routes only `responses_structured` and `responses_text` through it. `/v1/responses` remains a separate Realtime-backed compatibility endpoint.
- `Cargo.lock` resolves `warpsock 4.2.8`, whose `Client`, `CapacityPolicy`, `HttpVersion::Http2`, `RequestBuilder::send_streaming`, `Response::http_version`, `Body::chunk`, and `Client::connection_reuse_count` are the required HTTP seams.

## Goals / Non-goals

### Goals

- Increase aggregate semantically completed Responses requests per second by using independent HTTP and WSS windows under one fixed aggregate ceiling.
- Discover each lane's safe concurrency quickly with slow start followed by AIMD on authoritative lane-overload signals only.
- Reuse one process-lifetime Warpsock `Client` for multiplexed HTTP/2 streams and reuse healthy WSS sockets sequentially with one in-flight ask per socket.
- Make permit ownership, cancellation, terminal completion, and replay eligibility mechanically unambiguous.
- Preserve one shared auth cache/source and generation-aware refresh behavior across both lanes.
- Preserve the `rtinfer/1` request/success/error envelope and WSS-only behavior when the transport variable is absent.
- Ship deterministic local tests, ignored credentialed proof, an aggregate-goodput benchmark, an explicit canary, release verification, launchd reload, and rollback commands.

### Non-goals

- **OUT OF SCOPE:** changing `/v1/responses`; it stays on `openai_responses` and `WarmSessionPool` in `crates/rtinfer-daemon/src/server.rs`.
- **OUT OF SCOPE:** changing model selection, reasoning policy, schemas, `service_tier: "priority"`, or the `rtinfer/1` client contract.
- **OUT OF SCOPE:** tool loops, `previous_response_id`, cross-request conversation state, or WSS continuation recovery.
- **OUT OF SCOPE:** request replay after any HTTP body send attempt or WSS `response.create` send attempt. No idempotency contract is available.
- **OUT OF SCOPE:** weighted fairness, per-tenant scheduling, Gradient2/Vegas-style latency control, or a second transport queue. V1 uses least-utilized lane selection and one admission wait set.
- **OUT OF SCOPE:** adapting from raw latency. TTFT and total time are observations until a cohort-normalized queue/service split exists.
- **OUT OF SCOPE:** adding a second HTTP library or replacing `tokio-tungstenite` for WSS.

## Users & scenarios

### Users

- Local `rtinfer/1` callers using `responses_structured` or `responses_text` under concurrent fan-out.
- The daemon operator selecting `wss`, `dual`, or `http`, inspecting health/logs, canarying a release, and rolling back without changing callers.
- Maintainers verifying wire compatibility, conservation, reuse, throughput, and release artifacts.

### Scenarios

1. With no new environment variables, existing Responses calls use only reusable WSS sockets and return the same success/error envelopes as `0.1.14`.
2. In `dual` mode, a burst fills both independent lane windows without exceeding the aggregate ceiling; completed requests grow only the saturated successful lane.
3. A caller future is aborted while queued or while holding admission. All lane and aggregate counters return to their pre-request values, and another waiter proceeds.
4. HTTP SSE or WSS closes after partial text but before `response.completed`. The call fails as indeterminate, returns no partial success, and emits no second logical send on either lane.
5. A credentialed benchmark warms both lanes, runs identical cohorts in rotating order, and proves dual completed-goodput exceeds the best single lane or records observed provider signals that prevent it.

## Decisions and invariants

### Architecture

- `CodexResponsesClient` is the only production entry point for `ask_text` and `ask_structured`.
- It owns one `Arc<CodexAuthCache>`, one `Arc<AdaptiveConcurrency>`, one private `CodexResponsesHttpLane`, and one private `CodexResponsesWssPool`.
- Lane selection occurs once, after admission and before transport dispatch. Daemon handlers do not select lanes.
- The adaptive controller is the only request-admission owner. `CodexResponsesWssPool` MUST NOT retain its request semaphore. Warpsock's local stream capacity is configured to the HTTP hard maximum so it does not impose a smaller local queue.
- WSS idle-socket inventory and the global eight-permit fresh-handshake gate are resource-management seams, not request-admission seams.

### Configuration

| Variable | Default | Valid values / hard bound | Behavior |
|---|---:|---|---|
| `RTINFER_RESPONSES_TRANSPORT` | `wss` | `wss`, `dual`, `http` only | Unset or blank means `wss`; any other value fails daemon construction. |
| `RTINFER_RESPONSES_HTTP_INITIAL` | `32` | integer `1..=HTTP_MAX` | Initial HTTP window. |
| `RTINFER_RESPONSES_HTTP_MAX` | `48` | integer `1..=256` | HTTP window maximum; `256` remains the explicit hard bound. |
| `RTINFER_RESPONSES_WSS_INITIAL` | `32` | integer `1..=WSS_MAX` | Initial WSS window. |
| `RTINFER_RESPONSES_WSS_MAX` | `48` | integer `1..=64` | WSS window and idle-pool maximum; `64` remains the explicit hard bound. |
| `RTINFER_RESPONSES_AGGREGATE_MAX` | `48` | `1..=256` in `http`, `1..=64` in `wss`, `2..=320` in `dual`; MUST NOT exceed the enabled-lane maxima sum | Fixed process ceiling; it does not grow adaptively. |
| `RTINFER_RESPONSES_PREWARM` | `0` | integer `0..=WSS_MAX` | Existing name retained; ignored with one warning in `http`, opens that many idle WSS sockets otherwise. |

- Lane minimum is fixed at `1`; zero-start and total lane collapse are not v1 behaviors.
- `RTINFER_RESPONSES_CAPACITY` is a deprecated WSS-max alias only when `RTINFER_RESPONSES_WSS_MAX` is absent. Values `1..=64` are used; `0` maps to the safe hard cap `64` with a warning instead of remaining unbounded. If both variables are set, `RTINFER_RESPONSES_WSS_MAX` wins and one warning is emitted.
- All numeric parsing and cross-field validation happens once in `ResponsesRuntimeConfig::from_env`. Invalid configuration returns `RealtimeError::Protocol` with `responses config: <variable> <reason>` and prevents serving.

### Admission and adaptation

- `AdaptiveConcurrency::acquire(&Arc<Self>, enabled_lanes).await` returns an `AdaptiveLease`; no production method returns a bare transport enum.
- A lease reserves exactly one lane slot and one aggregate slot atomically under the controller mutex.
- `AdaptiveLease::finish(self, outcome, elapsed)` consumes the lease. `Drop` without `finish` releases both counters, increments `cancellations`, changes no window, records no latency sample, and wakes one waiter.
- Cancellation while waiting owns no capacity. A waiting-count guard decrements on future cancellation.
- `acquire` uses one `tokio::sync::Notify` wait set with register-before-recheck ordering. It MUST NOT add a semaphore, FIFO scheduler, weighted fairness policy, lane-local waiter queue, or polling sleep.
- Selection chooses the enabled open lane with the lower `in_flight / limit` ratio, then greater headroom, then HTTP. This deterministic tie-break is not a fairness scheduler.
- Saturated `Success` adds one permit per completion during slow start. After the first `LaneOverload`, saturated success adds one permit after `limit` successful completions. `LaneOverload` halves only that lane, floored at `1`. `Failure`, `Indeterminate`, `Cancelled`, and `SharedThrottle` do not change a lane window.
- `SharedThrottle` pauses admission to both lanes. Integer `Retry-After` seconds is clamped to `100ms..=30s`; absent, malformed, or HTTP-date values use `1s`. A later throttle extends but never shortens the current pause.
- Raw queue wait, TTFT, and total latency are recorded. None participates in selection, increase, decrease, or throttle duration.

### Logical and transport result taxonomy

| Result class | Literal meaning | Window action | Replay/failover |
|---|---|---|---|
| `Success` | `response.completed` observed, status absent or `completed`, and assembled text is non-empty | Success update on selected lane | None |
| `LaneOverload` | Provider code is exactly `server_is_overloaded`, `slow_down`, or `websocket_connection_limit_reached` | Halve selected lane | None |
| `SharedThrottle` | Provider code is exactly `rate_limit_exceeded`, or HTTP status is `429` without a lane-overload code | Pause both lanes; do not change windows | None |
| `Failure` | Explicit provider rejection not listed above, auth/config error, non-429 4xx/5xx, schema/output parse error after terminal, or connection/handshake failure before a logical send attempt | Release only | Handshake/auth work may retry before send; no logical-send retry |
| `Indeterminate` | EOF, timeout, malformed frame/event, or transport reset after a logical send attempt and before semantic terminal completion | Release only | Forbidden |
| `Cancelled` | Lease dropped before a terminal result; includes caller abort before or after send | Release only | Forbidden |

- `DispatchPhase` has exactly `PreSend`, `SendAttempted`, and `Terminal`.
- For WSS, phase changes to `SendAttempted` immediately before calling `ws.send(response.create)`. For HTTP, it changes immediately before calling Warpsock `.send_streaming()` because request-body write progress is not exposed.
- Once phase is `SendAttempted`, the same logical request MUST NOT be sent again on the same lane or the other lane, even when the send future itself returns an error.
- WSS handshake 401/403 refresh and reconnect attempts are pre-send and may repeat without emitting `response.create`. HTTP 401/403 is post-send: refresh the shared cache for a future caller, return `Failure`, and do not replay the current request.
- `[DONE]`, `response.output_text.done`, EOF, and non-empty accumulated text are not terminal success.

### Auth ownership

- `CodexAuthCache` moves the current cache, TTL, source, file-path, proactive refresh, and generation-aware `force_refresh_after(rejected_access_token)` logic from `responses.rs` to `auth.rs`.
- Both lanes receive the same `Arc<CodexAuthCache>` from `CodexResponsesClientBuilder`; neither lane owns another cache or reads `~/.codex/auth.json` directly.
- `CodexAuthSource` continues to own credential origin and refresh. Credential-process mode never falls back to file auth.
- The cache exposes a monotonic, non-secret `generation: u64`, incremented only when the cached access token changes. Logs expose generation, never token/account values.
- Concurrent refreshes for the same rejected access token invoke `CodexAuthSource::force_refresh` once; later callers reuse the changed generation.

### Codex `/responses` wire

- Endpoint constants are `https://chatgpt.com/backend-api/codex/responses` for HTTP and `wss://chatgpt.com/backend-api/codex/responses` for WSS.
- Shared identity constants are `originator: codex-tui`, `version: 0.143.0-alpha.29`, `x-codex-beta-features: remote_compaction_v2`, and user agent `codex-tui/0.143.0-alpha.29 (rtinfer; rust) unknown (codex-tui; 0.143.0-alpha.29)`. The user agent MUST NOT contain `reqwest`.
- Both transports send `Authorization: Bearer <access_token>` and `ChatGPT-Account-ID`. HTTP headers use the logical request's `session-id`, `thread-id`, `x-client-request-id` equal to `thread-id`, and `x-codex-window-id` equal to `<thread-id>:0`; HTTP also sends `Accept: text/event-stream` and `Content-Type: application/json`.
- Each newly connected WSS socket creates separate connection-lifetime UUID-v4 `session-id`/`thread-id` header values, sets `x-client-request-id` to that connection thread ID and `x-codex-window-id` to `<connection-thread-id>:0`, and sends `x-codex-turn-metadata` with `request_kind:"prewarm"` and empty `turn_id`. These connection headers remain fixed while the socket is reused; each inference body's logical request IDs remain fresh. Responses WSS is GA: it MUST NOT send `OpenAI-Beta`.
- Every logical ask creates fresh UUID-v4 `session_id`, `thread_id`, and `turn_id` before lane selection. The IDs, `prompt_cache_key`, and metadata remain stable for all pre-send auth/connection attempts and are never reused by another logical ask.
- Installation ID is read from `~/.codex/installation_id`; absent means create one UUID-v4 atomically with mode `0600`. Builder tests may inject it. Malformed or unwritable installation state fails client construction; no random per-request fallback is permitted.
- The common POST body includes existing `model`, `instructions`, `input`, `reasoning:{"effort":"low"}`, `store:false`, `stream:true`, `include:[]`, `service_tier:"priority"`, and existing text/strict-schema shape, plus `tools:[]`, `tool_choice:"auto"`, `parallel_tool_calls:false`, `prompt_cache_key:<thread_id>`, input `internal_chat_message_metadata_passthrough:{"turn_id":<turn_id>}`, and `client_metadata` fields `x-codex-installation-id`, `session_id`, `thread_id`, `x-codex-window-id`, `turn_id`, and ASCII-JSON-string `x-codex-turn-metadata` with `request_kind:"turn"`, `thread_source:"user"`, and `sandbox:"none"`.
- WSS adds only top-level `"type":"response.create"` to the common HTTP body. HTTP MUST NOT send that top-level field.
- Neither transport sends `max_output_tokens`, `previous_response_id`, `generate`, `OpenAI-Beta`, or `x-oai-web-search-eligible`.

## Requirements

### R1 — Semantic completion and replay safety

**Statement:** Both parsers and both live lanes MUST return success only after the canonical terminal event and MUST make post-send replay impossible.

**Priority:** P0

**Seams:**
- `crates/rtinfer-core/src/responses.rs` — `ResponsesAssembler`, `FrameOutcome`, `assemble_codex_responses_text`, `run_on_socket`; add `SseDecoder`, `assemble_codex_responses_sse`, `DispatchPhase`, `ResponsesResultClass`, and private attempt-result types beside the assembler.
- `crates/rtinfer-core/src/lib.rs` — export `assemble_codex_responses_sse` only for integration tests/consumers.
- `crates/rtinfer-core/tests/responses_protocol.rs` and `crates/rtinfer-core/tests/responses_http_sse.rs` — extend.

**Negative space:**
- MUST NOT treat EOF, timeout, `[DONE]`, `response.output_text.done`, or accumulated text as success.
- MUST NOT skip malformed JSON after a send attempt.
- MUST NOT retry or cross-lane fail over after `SendAttempted`.
- MUST NOT delete the captured JSONL fixture or weaken its field assertions.

**Acceptance criteria:**
- Given deltas, `response.output_text.done`, and EOF without `response.completed`, when either assembler finishes, then it returns `RealtimeError::Protocol` with label `protocol` and no text.
- Given partial text followed by WSS close, timeout, malformed JSON, or read error, when one logical ask runs, then its class is `Indeterminate`, `terminal_seen=false`, and the harness observes exactly one `response.create` across all sockets.
- Given HTTP SSE partial text followed by body EOF, reset, malformed UTF-8/JSON, or idle timeout, when one logical ask runs, then its class is `Indeterminate` and the H2 harness observes exactly one POST.
- Given `response.completed` with absent status or `response.status:"completed"` and non-empty assembled text, when parsing finishes, then class is `Success` and the exact done text wins over partial deltas.
- Given `response.completed` with empty text or non-`completed` status, when parsing finishes, then class is `Failure` and no partial success is returned.

**Assert:**
- Extend `responses_protocol.rs` with `incomplete_wss_stream_is_rejected`, `completed_status_and_text_are_required`, and `post_send_wss_failure_is_not_replayed`.
- Extend `responses_http_sse.rs` with `incomplete_sse_is_rejected`, `malformed_sse_data_is_rejected`, `done_marker_is_not_semantic_completion`, and `post_send_http_failure_is_not_replayed`.
- Run `cargo test -p rtinfer-core --test responses_protocol --test responses_http_sse`; expect exit `0`.

### R2 — RAII admission, independent windows, and aggregate ceiling

**Statement:** Replace manual completion with a cancellation-safe lease and enforce independent bounded lane windows plus one fixed aggregate ceiling.

**Priority:** P0

**Seams:**
- `crates/rtinfer-core/src/adaptive.rs` — replace `try_acquire`/public `complete`; add `AdaptiveLease`, `EnabledResponsesLanes`, aggregate/throttle state, `Notify`, waiting guard, snapshots, and result counters.
- `crates/rtinfer-core/src/lib.rs` — update adaptive exports.
- `crates/rtinfer-core/tests/adaptive_concurrency.rs` — replace bare-enum tests with lease tests while preserving their slow-start and isolated-overload intent.

**Negative space:**
- MUST NOT expose a production acquire API that requires a later lane argument.
- MUST NOT add semaphores, polling sleeps, a fairness scheduler, Gradient2, or latency-based window changes.
- MUST NOT silently normalize invalid limits; constructors/config parsing return an error.
- MUST NOT let any snapshot exceed configured lane or aggregate maxima.

**Acceptance criteria:**
- Given a held lease, when it is dropped or its task is aborted, then lane and aggregate `in_flight` decrement exactly once, `cancellations` increments once, and a waiter completes acquisition within `100ms` under paused Tokio time.
- Given a cancelled acquire future that never received a lease, when state is sampled, then all `in_flight` counts are unchanged and `waiting` returns to its prior value.
- Given 1,000 tasks with deterministic mixes of success, overload, failure, and cancellation, when all tasks settle, then both lane and aggregate `in_flight` are `0`, no observed count exceeds its max, and successful acquisition resumes.
- Given a lane-overload completion on HTTP, when snapshots are compared, then HTTP limit is halved to at least `1`, WSS limit is unchanged, and aggregate max is unchanged.
- Given `SharedThrottle` with absent retry-after, when an acquire starts at `t=0`, then neither lane admits before `t=1s`; at `t=1s` one enabled lane admits and neither lane limit changed.
- Given equal normalized utilization/headroom, when dual acquisition selects, then it chooses HTTP; after HTTP utilization rises, a subsequent acquisition chooses WSS.
- Given two otherwise identical runs with elapsed values `10ms` and `10s`, when outcomes are identical, then all selected lanes and limits are identical while latency observations differ.

**Assert:**
- Tests named `dropped_lease_releases_lane_and_aggregate`, `aborted_holder_conserves_capacity`, `cancelled_waiter_owns_no_capacity`, `wake_registration_has_no_lost_notification`, `thousand_task_conservation_respects_all_maxima`, `shared_throttle_pauses_both_lanes_without_halving`, and `latency_is_observational_only` in `adaptive_concurrency.rs`.
- Run `cargo test -p rtinfer-core --test adaptive_concurrency`; expect exit `0`.

### R3 — One shared auth owner

**Statement:** Extract one cache used by both transports and preserve generation-aware, singleflight source refresh.

**Priority:** P0

**Seams:**
- `crates/rtinfer-core/src/auth.rs` — `CodexAuthSource`; add `CodexAuthCache`, `CachedAuth`, builder/constructors, `load`, `force_refresh_after`, `invalidate`, and `generation`.
- `crates/rtinfer-core/src/responses.rs` — remove `CachedAuth`, `fresh_auth`, `force_refresh_after`, `auth_ttl`, `auth_path`, `auth_source`, and `cached_auth` from WSS pool; inject `Arc<CodexAuthCache>` into both lanes.
- `crates/rtinfer-core/tests/auth.rs` and `responses.rs` unit tests — extend.

**Negative space:**
- MUST NOT create one cache per lane.
- MUST NOT let credential-process mode read or refresh `auth.json`.
- MUST NOT log, debug-print, or place bearer, ID, refresh token, account ID, prompt, or output in observability.
- MUST NOT replay an HTTP request after a 401/403.

**Acceptance criteria:**
- Given 32 concurrent calls rejecting the same access token, when all invoke `force_refresh_after`, then the fake source records exactly one force call and all receive the same new generation/token.
- Given HTTP and WSS requests on one client, when both load auth inside TTL, then the fake source records one load and snapshots expose one shared generation.
- Given credential-process mode and an unreadable/mismatched auth file, when both lanes ask, then only the supplied source is called.
- Given HTTP 401, when the response is classified, then the source refreshes at most once for future calls, the current logical request records one POST, and the caller receives `Failure`.

**Assert:**
- Add tests `shared_cache_loads_once_across_lanes`, `rejected_generation_refresh_is_singleflight`, `credential_source_never_falls_back_to_file`, and `http_auth_rejection_refreshes_without_replay`.
- Run `cargo test -p rtinfer-core auth responses_force`; expect exit `0`.

### R4 — Canonical common request wire

**Statement:** Build each logical request once with the specified codex-tui headers, identifiers, metadata, and body; derive HTTP and WSS envelopes without changing semantic fields.

**Priority:** P0

**Seams:**
- `crates/rtinfer-core/src/responses.rs` — replace `build_text_frame`, `build_structured_frame`, and `build_responses_request` with `CodexRequestIds`, `CodexResponsesWireRequest`, `build_common_body`, `build_http_headers`, `build_wss_handshake_request`, and `build_wss_frame` beside existing schema normalization.
- `crates/rtinfer-core/examples/socket_reuse_probe.rs` — rewrite the probe to construct `CodexResponsesClientBuilder` in `ResponsesTransportMode::Wss`, issue three sequential asks, and remove its duplicated header/frame construction.
- `crates/rtinfer-core/Cargo.toml`, root `Cargo.toml`, and `Cargo.lock` — add only `uuid = { version = "1", features = ["v4"] }`.
- `crates/rtinfer-core/tests/responses_wire.rs` — **NEW:** beside protocol tests.

**Negative space:**
- MUST NOT retain `codex_cli_rs`, `CODEX_RESPONSES_BETA`, `OpenAI-Beta`, lowercase-only account-header assertions, a reqwest UA suffix, or per-lane body builders.
- MUST NOT add `max_output_tokens`, `previous_response_id`, `generate`, tools beyond `[]`, or change existing model/reasoning/schema/service-tier values.
- MUST NOT reuse logical `session_id`, `thread_id`, `turn_id`, or prompt cache key across asks.
- MUST NOT add any dependency other than `uuid` for production or `h2`/`bytes` for local test fixtures.

**Acceptance criteria:**
- Given a text or structured request, when HTTP and WSS representations are serialized, then all common fields are byte-equivalent as JSON values, WSS alone has `type:"response.create"`, and HTTP lacks `type`.
- Given the HTTP representation, then literal headers and body fields match the Decisions table, `http_version` request preference is forced to HTTP/2, and forbidden fields/headers are absent.
- Given the WSS handshake, then `originator=codex-tui`, beta features are `remote_compaction_v2`, turn metadata says prewarm with empty turn ID, and `OpenAI-Beta` is absent.
- Given 100 generated logical requests, then all session/thread/turn UUID strings parse as v4 UUIDs and no ID/prompt-cache-key tuple repeats.
- Given an existing valid installation ID, then all requests in one client use it; given a malformed file, client construction fails with `protocol` before network work.

**Assert:**
- Add tests `http_and_wss_share_one_common_body`, `http_wire_matches_codex_tui_contract`, `wss_handshake_matches_ga_codex_tui_contract`, `logical_request_ids_are_unique_v4`, and `installation_id_is_stable_and_strict` in `responses_wire.rs`.
- Run `cargo test -p rtinfer-core --test responses_wire`; expect exit `0`.

### R5 — Reused multiplexed Warpsock HTTP/2 lane

**Statement:** Add one process-lifetime Warpsock client that forces HTTP/2, streams SSE incrementally, and exposes reuse/protocol evidence.

**Priority:** P0

**Seams:**
- `crates/rtinfer-core/src/responses.rs` — **NEW symbols:** `CodexResponsesHttpLane`, `CodexResponsesHttpLane::new`, `ask`, and HTTP response classifier beside `CodexResponsesWssPool`.
- Warpsock reuse: `warpsock::Client::builder`, `CapacityPolicy::bounded(http_max)`, `prefer_http2(true)`, `h3_upgrade(false)`, `h2_direct_streaming_responses(false)`, `total_timeout(120s)`, per-request `version(HttpVersion::Http2)`, `send_streaming`, `Response::http_version`, `Body::chunk`, and `connection_reuse_count`.
- `crates/rtinfer-core/tests/responses_dual_transport.rs` — **NEW:** cleartext local H2+WSS harness.
- `crates/rtinfer-core/Cargo.toml` dev-dependencies — add only `h2 = "0.4"` and `bytes = "1"` for the local H2 fixture.

**Negative space:**
- MUST NOT construct a Warpsock client per request, allow H1/H3 fallback, buffer the full successful SSE body, follow redirects, or use Warpsock's direct exclusive H2 streaming path.
- MUST NOT configure Warpsock's local stream cap below `RTINFER_RESPONSES_HTTP_MAX`.
- MUST NOT infer overload from latency, generic timeout, EOF, malformed SSE, 401/403, or arbitrary error-string matching.

**Acceptance criteria:**
- Given eight concurrent HTTP asks to a local H2 fixture advertising at least eight streams, when all complete, then the fixture accepts exactly one TCP connection, observes at least two overlapping streams, every response reports `HTTP/2`, and client reuse-count delta is at least `1`.
- Given a successful status with content type not beginning `text/event-stream`, when classified, then result is `Failure`, label is `protocol`, and no body is returned as success.
- Given provider codes/statuses in the taxonomy, when classified, then only the exact lane-overload codes yield `LaneOverload`; `rate_limit_exceeded` or unqualified 429 yields `SharedThrottle`; 401/403 and 500 yield `Failure`.
- Given arbitrary SSE chunk boundaries including CRLF, split UTF-8, multiple `data:` lines, comments, and event fields, when decoded, then the assembled text and terminal result equal the unsplit fixture.

**Assert:**
- Tests `http_client_is_single_reused_h2_multiplexer`, `http_rejects_non_sse_or_non_h2_success`, `http_result_taxonomy_is_exact`, and `sse_decoder_is_chunk_boundary_invariant`.
- Run `cargo test -p rtinfer-core --test responses_dual_transport --test responses_http_sse`; expect exit `0`.

### R6 — Corrected reusable exclusive WSS lane

**Statement:** Retain exclusive socket reuse, prewarm, and the eight-handshake gate while removing the hidden request semaphore and unsafe resend behavior.

**Priority:** P0

**Seams:**
- `crates/rtinfer-core/src/responses.rs` — rename `CodexResponsesPool` to private `CodexResponsesWssPool`, remove the public pool builder, and construct it only from `CodexResponsesClientBuilder`; retain `HANDSHAKE_GATE`, `connect_socket`, `checkout_socket`, `checkin_socket`, and `prewarm`; rewrite `run_warm` and `run_on_socket` to return typed attempts.
- `crates/rtinfer-core/tests/responses_dual_transport.rs` and `responses_protocol.rs`.
- `crates/rtinfer-core/examples/socket_reuse_probe.rs`.

**Negative space:**
- MUST NOT multiplex two active asks on one WSS socket.
- MUST NOT retry a request after entering `SendAttempted`, including a warm-socket send/read failure.
- MUST NOT reduce `HANDSHAKE_GATE_PERMITS` from `8`, expand it without new live evidence, or count idle sockets as in-flight requests.
- MUST NOT check a socket back in after any non-success result or before semantic terminal completion.

**Acceptance criteria:**
- Given three sequential successful asks and WSS max at least one, when executed, then the fixture observes one handshake, exactly three `response.create` frames, and never more than one active ask on that socket.
- Given two concurrent asks and two sockets, then each socket owns at most one ask, both complete, and both healthy sockets return to idle inventory.
- Given a partial response then close/timeout/malformed frame, then the socket is dropped, the result is `Indeterminate`, and all fixture sockets together observe exactly one `response.create` for that logical ID.
- Given 32 simultaneous cold asks, then at no point are more than eight fresh handshakes active; completed sockets may be reused while other handshakes wait.
- Given prewarm `N`, then idle sockets never exceed `min(N, WSS_MAX)` and no adaptive request lease is consumed by prewarm.

**Assert:**
- Tests `wss_reuses_one_socket_for_sequential_completed_asks`, `wss_socket_ownership_is_exclusive`, `wss_incomplete_attempt_is_dropped_not_replayed`, `wss_handshakes_never_exceed_eight`, and `prewarm_does_not_consume_admission`.
- Run `cargo test -p rtinfer-core --test responses_dual_transport --test responses_protocol`; expect exit `0`.

### R7 — Coordinator, runtime mode, and daemon integration

**Statement:** Route the two existing Responses tiers through one coordinator with strict runtime configuration and no nested admission.

**Priority:** P0

**Seams:**
- `crates/rtinfer-core/src/responses.rs` — **NEW:** `ResponsesTransportMode`, `ResponsesClientConfig`, `CodexResponsesClientBuilder`, `CodexResponsesClient::{ask_text,ask_structured,model,snapshot,prewarm}`.
- `crates/rtinfer-core/src/lib.rs` — export coordinator/config public surface; stop exporting the old pool as the daemon entry point.
- `crates/rtinfer-daemon/src/server.rs` — `AppState.codex_responses_pool` becomes `codex_responses_client`; replace `responses_capacity_from_env`/`responses_pool_builder` with `ResponsesRuntimeConfig::from_env` and a client builder; update `new_file_auth`, `new_with_auth_source`, `serve`, `responses_structured`, `responses_text`, health, and tests.
- `README.md` — document the exact variables/defaults and rollback mode.

**Negative space:**
- MUST NOT change `/v1/responses`, Realtime tiers, request/response payload fields, client libraries, or `clients/contract.json`.
- MUST NOT select a transport in a daemon handler or acquire a lane permit before entering the coordinator.
- MUST NOT keep `RTINFER_RESPONSES_CAPACITY=0` as unbounded.
- MUST NOT silently accept an invalid mode/number/cross-field relation.

**Acceptance criteria:**
- Given no transport variable, when the daemon builds and handles Responses calls, then snapshot mode is `wss`, HTTP dispatch count is `0`, WSS output/error envelopes match the current contract, and `/v1/responses` still uses `openai_responses`.
- Given `http`, then every admitted Responses tier request uses HTTP and no WSS handshake/prewarm occurs.
- Given `dual` and a 12-request blocking local burst with defaults scaled to fixture limits HTTP `2`, WSS `2`, aggregate `4`, then both lanes observe at least one request, aggregate active never exceeds `4`, each lane active never exceeds `2`, and there is no pool semaphore wait after lease acquisition.
- Given invalid mode, zero initial/max, initial greater than max, out-of-bound max, dual aggregate below `2`, or aggregate above enabled maxima sum, then daemon construction fails before bind and the error begins `protocol error: responses config:`.
- Given legacy capacity `0` with no new WSS max, then effective WSS max is `64`, a warning is emitted once, and no unbounded sentinel exists.

**Assert:**
- Add core tests `mode_routes_only_enabled_lanes`, `dual_burst_uses_both_lanes_under_all_bounds`, and `coordinator_has_one_admission_wait`.
- Add daemon tests `responses_transport_defaults_to_wss`, `responses_runtime_config_rejects_invalid_values`, `legacy_zero_capacity_maps_to_hard_cap`, and retain the route-source guard for `/v1/responses`.
- Run `cargo test -p rtinfer-core --test responses_dual_transport && cargo test -p rtinfer-daemon server::tests`; expect exit `0`.

### R8 — Observability without sensitive content

**Statement:** Emit enough state to prove selection, completion, reuse, bounds, and auth generation without recording request or model content.

**Priority:** P1

**Seams:**
- `crates/rtinfer-core/src/adaptive.rs` — snapshots add aggregate `limit`, `in_flight`, `waiting`, throttle state, and per-lane `sample_count`, `successes`, `lane_overloads`, `shared_throttles`, `failures`, `indeterminate`, `cancellations`, and latency observations.
- `crates/rtinfer-core/src/responses.rs` — one completion tracing event per logical request and reuse/handshake counters.
- `crates/rtinfer-daemon/src/server.rs::rtinfer_health` — add `responses` object without changing existing fields.

**Negative space:**
- MUST NOT log prompt, instructions, output, schema body, provider message body, bearer/ID/refresh token, account ID, full auth path contents, or SSE/WSS raw frames.
- MUST NOT report `latency_ewma_ms:0` as a measured sample; `sample_count=0` means unmeasured.
- MUST NOT make health readiness depend on either lane having prior samples.

**Acceptance criteria:**
- Given a completed, failed, indeterminate, and cancelled request, when traces are captured, then each logical request has one event with fields `mode`, `lane`, `queue_wait_ms`, `ttft_ms`, `total_ms`, `terminal_seen`, `result_class`, `lane_limit`, `lane_in_flight`, `aggregate_limit`, `aggregate_in_flight`, `http_version`, `http_reused`, `wss_reused`, `wss_handshake_attempts`, and `auth_generation`; inapplicable fields are absent, not fabricated zeroes.
- Given health, then existing `contract`, `ready`, `provider`, `auth_source`, and `tiers` fields are unchanged, and `responses` contains mode plus aggregate/http/websocket snapshots with numeric bounds/counters.
- Given secret-like sentinel strings injected as auth/prompt/output/provider message, when logs and health JSON are searched, then none of the sentinel strings appears.

**Assert:**
- Add unit tests `responses_trace_has_contract_fields_without_content`, `responses_health_exposes_bounded_snapshot`, and `responses_observability_redacts_sensitive_sentinels`.
- Run `cargo test -p rtinfer-core responses_observability && cargo test -p rtinfer-daemon responses_health`; expect exit `0`.

### R9 — Credentialed proof, goodput, release, launchd rollout, and rollback

**Statement:** Add opt-in live proof and execute the repository's release/launchd path with WSS default, explicit dual canary, and command-level rollback.

**Priority:** P1

**Seams:**
- `crates/rtinfer-core/tests/responses_dual_live.rs` — **NEW:** ignored credentialed live tests.
- `crates/rtinfer-core/examples/responses_goodput.rs` — **NEW:** credentialed benchmark CLI using the production coordinator.
- `.plans/evidence/` — **NEW at execution time:** benchmark JSON and markdown verdict; no credentials/content.
- `Cargo.toml`, package manifests, and `Cargo.lock` via `scripts/set-version.sh 0.1.15`.
- Existing `.github/workflows/ci.yml`, `.github/workflows/auto-tag.yml`, `.github/workflows/release.yml`, `crates/rtinfer-daemon/src/install.rs`, and `self_update.rs` are reused, not redesigned.

**Negative space:**
- MUST NOT run credentialed tests in CI, silently pass when auth is missing, include prompts/outputs/tokens in evidence, publish before full gates, change default mode from WSS, edit the plist's stable npm shim, or claim goodput from non-terminal responses.
- MUST NOT call a benchmark pass from one noisy sample. Use the cohort protocol below.
- MUST NOT delete a failed benchmark artifact; a blocked verdict must retain raw counters and the qualifying provider signal.

**Acceptance criteria:**
- Given valid ChatGPT auth, when the ignored live proof runs, then one HTTP request reports `HTTP/2`, two sequential HTTP requests increase `connection_reuse_count`, two sequential WSS asks use one handshake, dual mode records at least three semantic successes on each lane, and every successful ask observed `response.completed`.
- Given three repetitions of 24 identical short requests per mode, with run order rotating `wss,http,dual`; `http,dual,wss`; `dual,wss,http`, and four unmeasured warmups per mode, when `completed / wall_seconds` is calculated per run, then the median dual goodput is at least `1.05 * max(median_http, median_wss)`.
- If that threshold is not met, then `.plans/evidence/adaptive-dual-transport-goodput-2026-07-20.md` has `verdict: blocked`, raw per-run completed/failure/class counts and wall times, lane counts proving both lanes carried load, version/config, and at least one observed qualifying cause: shared `rate_limit_exceeded`/HTTP 429, one lane's canonical endpoint failing its live protocol/reuse gate, or the fixed aggregate ceiling saturating before either lane window. A latency opinion or generic noise statement is not qualifying evidence.
- Given implementation gates and live evidence, when `0.1.15` is released, then all four npm packages report `0.1.15`, installed `rtinferd --version` prints `rtinferd 0.1.15`, and the release workflow is green.
- Given canary launchd commands, when the job is reloaded, then `/v1/infer/health.responses.mode` is `dual`; after rollback commands, it is `wss` and callers require no restart/config change.

**Assert:**
- Live proof: `cargo test -p rtinfer-core --test responses_dual_live -- --ignored --nocapture`; expect exit `0` with auth present.
- Benchmark: `cargo run --release -p rtinfer-core --example responses_goodput -- --requests 24 --repetitions 3 --warmups 4 --output .plans/evidence/adaptive-dual-transport-goodput-2026-07-20.json`; expect exit `0` and JSON field `verdict` equal to `pass` or `blocked` under the qualifying rule.
- Release/launchd checks are listed verbatim in the Execution contract.

## Validation & test plan

### Requirement-to-check mapping

| Requirement | Unit | Integration | E2E/live | Manual/operational |
|---|---|---|---|---|
| R1 | assembler/parser terminal tests | local H2+WSS partial/reset harness | live terminal proof | — |
| R2 | lease/state/paused-time tests | 1,000-task Tokio conservation | dual live concurrency | snapshot review |
| R3 | fake-source generation tests | both lanes on one cache | credentialed auth load | secret scan |
| R4 | wire golden assertions | local fixtures inspect requests | canonical endpoint acceptance | version constant review |
| R5 | SSE/status taxonomy | local cleartext H2 multiplex fixture | HTTP protocol/reuse proof | reuse counters |
| R6 | WSS terminal/replay tests | local WSS handshake/reuse fixture | WSS reuse proof | prewarm counters |
| R7 | config parser | daemon/core dual burst | loopback daemon canary | default/rollback check |
| R8 | snapshot/redaction | health and tracing capture | canary health | log sentinel review |
| R9 | benchmark calculations | release contract tests | ignored proof + benchmark | npm/launchd verification |

### Default-FAIL

- All R1–R9 criteria start unmet.
- Existing green tests do not mark a requirement complete.
- Mark a requirement complete only after every named assertion for that requirement passes on the current combined worktree.
- R9 remains unmet until live evidence and immutable npm/launchd verification exist; a skipped ignored test is not a pass.

### Anti-gaming

- Do not delete, ignore, rename away, loosen, reduce iterations/concurrency, replace exact equality with non-empty assertions, or narrow fixture coverage to obtain green.
- Do not classify an incomplete stream as failure before its send marker merely to permit replay.
- Do not make local fixtures serialize requests; overlap and max-active assertions must remain.
- Do not lower configured maxima in tests after the controller starts to hide counter leaks.
- Do not count transport acceptance, `output_text.done`, or partial text as benchmark completion.

### Edge cases and expected observables

| Edge/failure | Expected observable |
|---|---|
| Caller cancelled while waiting | `waiting` decrements; no lane/aggregate capacity changes. |
| Caller cancelled after lease | selected and aggregate in-flight decrement once; `cancellations +1`; no window change. |
| WSS/HTTP partial then EOF | `Indeterminate`, `terminal_seen=false`, exactly one logical send. |
| Terminal with empty text | `Failure(protocol)`, no success/window growth. |
| `rate_limit_exceeded` or unqualified 429 | global pause; lane limits unchanged. |
| `server_is_overloaded` on HTTP | only HTTP window halves. |
| WSS handshake 401/403 | shared refresh before any frame; handshake may retry; request send count remains one or zero. |
| HTTP 401/403 | refresh for future generation; current POST count one; no replay. |
| Both windows open but aggregate full | one admission wait; transport methods not called. |
| Disabled lane | zero dispatches/handshakes on that lane. |
| Bad numeric/mode env | daemon does not bind; exact config error prefix. |
| Warpsock negotiates non-H2 | `Failure(protocol)`; no fallback. |
| WSS socket succeeds | socket checked in only after completed terminal. |
| WSS socket fails | socket dropped; idle count does not rise. |
| No latency samples | `sample_count=0`; latency field omitted or explicitly null, never interpreted as zero latency. |

### Regression risks

- Correctly rejecting incomplete WSS streams will expose failures previously returned as truncated success.
- Removing WSS post-send retry can reduce apparent success rate while eliminating duplicate generations; live evidence must measure this honestly.
- Canonical codex-tui wire changes WSS headers currently using `codex_cli_rs` and `OpenAI-Beta`; local goldens and live proof gate release.
- A forced HTTP/2-only policy makes endpoint/proxy downgrade visible rather than silently using H1.
- Shared global throttling can temporarily leave available lane slots idle; this is required for authoritative account-level rate limits.
- Launchd may keep the old process image after npm install; explicit `kickstart -k` is mandatory.

## Ordered implementation waves

1. **Terminal truth:** make `ResponsesAssembler` require `response.completed`; add incremental SSE decoder and red incomplete-stream/no-replay tests before network integration.
2. **Admission ownership:** replace the bare-enum controller API with `AdaptiveLease`, aggregate bounds, shared throttle, cancellation counters, and deterministic paused-time conservation tests.
3. **Shared identity/auth:** extract `CodexAuthCache`; add installation/request IDs and one common codex-tui wire builder with exact golden tests.
4. **WSS safety:** correct WSS handshake/body wire, remove its request semaphore, preserve gate/idle pool, and prohibit all post-send retry.
5. **HTTP lane:** construct one bounded forced-H2 Warpsock client, stream body chunks into the SSE decoder, and classify statuses/provider events.
6. **Coordinator:** add `CodexResponsesClient`, one acquisition/select/dispatch/finish path, strict mode/config parsing, and local dual-lane harness tests.
7. **Daemon and observability:** replace `AppState` pool, retain `/v1/responses`, add health snapshot/traces/config docs, and prove sensitive sentinel absence.
8. **Live/benchmark:** run ignored canonical endpoint/reuse proof and three-cohort goodput benchmark; retain pass or qualifying blocked evidence.
9. **Release/canary:** run full gates, bump to `0.1.15`, publish through existing workflows, install, set launchd dual canary, kickstart, verify, and retain WSS rollback.

## Constraints

### Technical

- Rust 2021 workspace; Tokio runtime; Warpsock remains lock-resolved to `4.2.8` unless a separate dependency change is approved.
- HTTP response timeout remains `120s`; WSS terminal deadline remains `120s`.
- HTTP hard max `256`, WSS hard max `64`, dual aggregate hard max `320`, handshake concurrency `8`.
- One WSS socket carries one active ask. One Warpsock `Client` may carry many H2 streams.
- Only `uuid` production and `h2`/`bytes` test dependencies are approved by this PRD.

### Security

- Preserve loopback-only daemon binding and current credential-source trust boundary.
- Installation ID file is mode `0600`; auth file behavior remains atomic.
- No raw upstream body/frame or sensitive request/auth content in logs, health, benchmark evidence, panic messages, or test snapshots.

### Performance

- The only v1 adaptation inputs are saturation plus authoritative overload classes.
- Local deterministic tests prove multiplexing/reuse and bounds; live benchmark judges terminal completed-goodput.
- Raw latency is observational. No release claim may state latency improvement from this work without separate cohort-normalized TTFT evidence.

### Compatibility

- Default mode is WSS. Existing `responses_structured`/`responses_text` request/success/error payloads and `/v1/responses` behavior do not change.
- Existing `RTINFER_RESPONSES_PREWARM` remains valid. Legacy capacity receives the bounded alias behavior specified above.
- `CODEX_RESPONSES_MODEL`, strict-schema normalization, reasoning effort, verbosity, and priority service tier remain unchanged.

### Rollout

1. Local deterministic gates.
2. Credentialed single-lane proof.
3. Explicit dual goodput benchmark.
4. Release with default WSS.
5. Launchd dual canary via environment plus forced job restart.
6. Observe health/result classes for at least one production fan-out before any future default-mode proposal.

## Open questions

- No P0 product decision remains open.
- `OPEN (P2, non-blocking):` refresh `CODEX_CLIENT_VERSION` and UA platform/terminal segments only from a new captured canonical Codex CLI wire. The pinned current value is part of v1 wire tests; an executor MUST NOT guess a newer value.
- `OPEN (P2, non-blocking):` whether live cohort-normalized TTFT warrants a latency-sensitive controller after v1. Raw latency data alone cannot authorize it.
- `OPEN (P2, non-blocking):` whether a weighted/fair scheduler is necessary. Require demonstrated starvation under the v1 selector before adding one.

## Execution contract

### Atomic steps (ordered)

1. Extend terminal parser tests so incomplete WSS and SSE streams fail; run the targeted tests and confirm red before implementation.
2. Implement terminal-required shared assembly and incremental SSE decoding in `responses.rs`; export `assemble_codex_responses_sse`; make parser tests green.
3. Replace `AdaptiveConcurrency::try_acquire/complete` with `AdaptiveLease`, aggregate ceiling, throttle state, notification wait, and snapshots; make all named adaptive tests green.
4. Move cache ownership into `auth.rs::CodexAuthCache`; add fake-source cross-lane and singleflight tests.
5. Add `uuid`, request/installation IDs, exact common wire builders, and `responses_wire.rs`; remove obsolete Responses beta/codex_cli_rs wire.
6. Rewrite the WSS lane around typed send phase/result, no request semaphore, no post-send retry, and healthy-only checkin; pass WSS fixture tests.
7. Add one forced-H2 Warpsock lane and incremental SSE body consumption; add `h2`/`bytes` dev fixtures; pass HTTP multiplex/reuse/taxonomy tests.
8. Add `CodexResponsesClient` and strict runtime configuration; route daemon Responses tiers through it; leave `/v1/responses` untouched.
9. Add snapshots/traces/health fields and README configuration; pass redaction and compatibility tests.
10. Add ignored live proof and benchmark; run both with credentials and write non-sensitive evidence.
11. Run every local/full gate below on the current combined worktree.
12. Run `bash scripts/set-version.sh 0.1.15`, rerun version/full gates, commit only intended implementation/PRD/version files, and push the current branch.
13. After human approval to merge/release, merge to `main`; verify auto-tag/release, four npm packages, and installed binary version.
14. Canary dual under launchd: `launchctl setenv RTINFER_RESPONSES_TRANSPORT dual`; `launchctl kickstart -k gui/$UID/com.jaredboynton.rtinferd`; poll health until `responses.mode` is `dual`.
15. After at least one real fan-out, retain dual only if health/log counters show both lane successes, no bound violation, no duplicate send evidence, and acceptable result classes. Otherwise run rollback.

### must_pass

- name: terminal parsers
  run: `cargo test -p rtinfer-core --test responses_protocol --test responses_http_sse`
  expect: exit 0
- name: adaptive conservation
  run: `cargo test -p rtinfer-core --test adaptive_concurrency`
  expect: exit 0
- name: exact wire
  run: `cargo test -p rtinfer-core --test responses_wire`
  expect: exit 0
- name: local dual transport harness
  run: `cargo test -p rtinfer-core --test responses_dual_transport`
  expect: exit 0
- name: daemon integration
  run: `cargo test -p rtinfer-daemon`
  expect: exit 0
- name: formatting
  run: `cargo fmt --all -- --check`
  expect: exit 0
- name: clippy
  run: `cargo clippy --workspace --all-targets -- -D warnings`
  expect: exit 0
- name: workspace tests
  run: `cargo test --workspace`
  expect: exit 0
- name: JS client tests
  run: `node --test clients/js/rtinfer-client.test.mjs`
  expect: exit 0
- name: Python client tests
  run: `python3 clients/python/test_rtinfer_client.py`
  expect: exit 0
- name: release contract
  run: `node --test tests/release-contract.test.mjs`
  expect: exit 0
- name: version coupling
  run: `bash scripts/set-version.sh --check`
  expect: exit 0
- name: release build
  run: `cargo build --release -p rtinfer-daemon`
  expect: exit 0
- name: credentialed live proof
  run: `cargo test -p rtinfer-core --test responses_dual_live -- --ignored --nocapture`
  expect: exit 0
- name: aggregate goodput evidence
  run: `cargo run --release -p rtinfer-core --example responses_goodput -- --requests 24 --repetitions 3 --warmups 4 --output .plans/evidence/adaptive-dual-transport-goodput-2026-07-20.json`
  expect: exit 0 and JSON `verdict` is `pass`, or `blocked` with the qualifying evidence required by R9
- name: npm versions after release
  run: `npm view @jaredboynton/rtinfer@0.1.15 version && npm view @jaredboynton/rtinfer-darwin-arm64@0.1.15 version && npm view @jaredboynton/rtinfer-linux-arm64@0.1.15 version && npm view @jaredboynton/rtinfer-linux-x64@0.1.15 version`
  expect: exit 0 and four lines equal `0.1.15`
- name: installed immutable version
  run: `npm i -g @jaredboynton/rtinfer@0.1.15 && rtinferd --version`
  expect: exit 0 and final line equals `rtinferd 0.1.15`
- name: launchd dual canary
  run: `launchctl setenv RTINFER_RESPONSES_TRANSPORT dual && launchctl kickstart -k gui/$UID/com.jaredboynton.rtinferd && curl -fsS http://127.0.0.1:8765/v1/infer/health`
  expect: HTTP 200 and JSON `.responses.mode` equals `dual`

### must_not

- name: no PRD publication outside plans
  rule: implementation PRD remains under `.plans/`; product code does not depend on this file
- name: realtime compatibility path
  rule: `crates/rtinfer-daemon/src/server.rs::openai_responses` and route `/v1/responses` remain Realtime-backed
- name: no nested request admission
  rule: no Responses WSS request semaphore and no per-lane admission queue; only `AdaptiveConcurrency::acquire` admits logical work
- name: no post-send replay
  rule: no code path invokes HTTP POST or WSS `response.create` twice for one logical request after `DispatchPhase::SendAttempted`
- name: no incomplete success
  rule: all success paths require `response.completed` and non-empty assembled text
- name: no unbounded knobs
  rule: no zero/unbounded sentinel for HTTP, WSS, or aggregate request capacity
- name: no transport drift
  rule: HTTP is forced `HttpVersion::Http2`; WSS uses codex-tui GA headers without `OpenAI-Beta`
- name: no sensitive observability
  rule: prompts, outputs, schemas, raw frames/bodies, tokens, and account IDs absent from logs/health/evidence
- name: no adaptive complexity
  rule: no latency-driven decrease/increase, Gradient2, weighted fairness, or new scheduler dependency
- name: no test weakening
  rule: existing and added tests remain present with equal-or-stronger assertions and iteration/concurrency counts
- name: no unapproved dependencies
  rule: no new production dependency except `uuid`; no new fixture dependency except `h2` and `bytes`
- name: WSS default
  rule: unset `RTINFER_RESPONSES_TRANSPORT` resolves to `wss` in code, docs, and tests

### Boundaries

- Always: build the common logical request and IDs once; acquire once; send once; require semantic terminal; finish/drop the lease once; retain exact evidence.
- Always: preserve the eight-handshake gate, WSS exclusive socket ownership, one shared auth cache, and fixed hard maxima.
- Ask first: changing model/reasoning/service tier, wire version/UA constant, default mode, public `rtinfer/1` payloads, dependency allowlist, release version, or qualifying blocked-evidence rules.
- Ask first: merging to `main`, publishing/tagging, or changing persistent launchd environment.
- Never: replay post-send, return partial success, expose secrets/content, silently clamp invalid new config, weaken tests, or edit the read-only upstream `./opencode` tree.

## Rollback

### Runtime rollback (preferred)

```sh
launchctl unsetenv RTINFER_RESPONSES_TRANSPORT
launchctl kickstart -k gui/$UID/com.jaredboynton.rtinferd
curl -fsS http://127.0.0.1:8765/v1/infer/health
```

Pass signal: HTTP `200`, `responses.mode` equals `wss`, and a `responses_text` smoke reaches `response.completed`. This requires no caller or package rollback.

### Binary rollback (only if WSS mode is regressed)

```sh
launchctl setenv RTINFER_SKIP_SELF_UPDATE 1
launchctl unsetenv RTINFER_RESPONSES_TRANSPORT
npm i -g @jaredboynton/rtinfer@0.1.14
launchctl kickstart -k gui/$UID/com.jaredboynton.rtinferd
rtinferd --version
```

Pass signal: final line is `rtinferd 0.1.14` and health is ready. After a fixed version is installed, run `launchctl unsetenv RTINFER_SKIP_SELF_UPDATE` and kickstart once.

## Dumb-model readiness audit

- **Next seam identifiable without invention:** yes; every wave names files and symbols, including all new placements.
- **Every P0 criterion has a named check:** yes; R1–R7 map to exact tests/commands.
- **Likely overbuild is forbidden:** yes; no extra transport, scheduler, latency control, API change, replay, or dependency.
- **Ordering is dependency-complete:** yes; terminal/admission/auth/wire precede lanes, coordinator, daemon, and release.
- **Criteria-only judge can pass/fail:** yes; terminal events, counts, headers, modes, bounds, commands, and release signals are literal.

## Done when

- [ ] R1–R9 acceptance criteria pass on the current combined worktree.
- [ ] All `must_pass` checks are green, including credentialed proof, benchmark verdict, release artifacts, and launchd canary.
- [ ] All `must_not` rules remain intact.
- [ ] No open `OPEN:` blocks P0.
- [ ] Dual live goodput is at least 1.05× the best single-lane median, or the qualifying blocked-evidence artifact exists and is accepted by a human before launch.
- [ ] Unset transport is proven WSS-only and runtime rollback is exercised once.
- [ ] `0.1.15` is published for all platforms, installed bytes report `0.1.15`, and launchd runs the intended image.
- [ ] Dumb-model readiness self-check remains passed after implementation changes.
