# Realtime navigator reliability — findings (2026-07-14)

Investigation: cse-sweep navigators (gpt-realtime-2.1-mini fan-out over the
rtinferd `realtime_structured` tier) were falling back to seed shards instead
of executing their declarative tool plans. Live sweeps ended the day at
**24/24 navigator shards (100%, zero fallbacks)** across three representative
accounts at the production nav count (8). Fixes landed in three layers.

## Finding 1 — stale running daemon, not a schema problem

The first 4/4 fallback wave (`fallback: navigator returned no plan`, 502
`provider_error: realtime_structured upstream error: protocol` in ~4ms) was a
**stale rtinferd process**: launchd had spawned the daemon before the
`feat/realtime-2-1` npm release was installed, so the running binary rejected
`gpt-realtime-2.1*` requests with `invalid_value: Unsupported option for this
model`.

* The launchd plist (`com.jaredboynton.rtinferd`) execs the **stable npm bin
  shim**; `npm i -g` rewrites it in place, but an already-running process
  keeps its old image. `launchctl kickstart -k gui/$UID/com.jaredboynton.rtinferd`
  is the correct reload.
* In-daemon self-update (`self_update.rs`) polls the registry every 30 min and
  drains+exits on a confirmed newer version, so steady-state drift heals
  itself; a *just-published* release still benefits from a manual kickstart.
* Local dev deploy: `cargo build --release`, copy over the npm platform
  package binary, kickstart. The plist needs no `@latest` change — the shim
  path is already version-stable.

## Finding 2 — provider closes idle realtime sockets ("socket closed mid-ask")

After the restart, residual failures were `protocol error: warm: socket
closed mid-ask`. The provider half-closes idle Realtime WebSockets **well
before** the 60-minute session cap; a warm socket reused after sitting idle
(e.g. between the seed/hydrate phase and the nav fan-out, or between two
sweeps) failed its first dispatch and the ask was lost.

Architectural fix in `crates/rtinfer-core/src/warm.rs` (no client-side
retries):

* `LiveSocket.last_used` + `SESSION_IDLE_MAX` (75s): `ensure_live()`
  proactively reconnects a socket that sat idle past the threshold or crossed
  `SESSION_MAX_AGE`, replacing three duplicated connect blocks.
* Stale-reuse re-dispatch: if a **reused** socket still fails with a
  stale-socket error (`socket closed mid-ask`, `ws send:`, `ws read:`), the
  daemon reconnects and re-dispatches the identical ask **exactly once**.
  The ask never started server-side, so callers keep one-request → one-result
  semantics. Fresh sockets never re-dispatch — a failure there is real.
* Provider errors and wall-clock timeouts are excluded from the stale
  classifier on purpose: those asks may have started server-side.

Clients (cse-sweep `daemon-client.mjs`) stay retry-free; the daemon owns the
socket lifecycle.

## Finding 3 — double-encoded arguments_json breaks structured outputs

The last fallback class was shape corruption in the navigator's forced
`cse_query` function call. The old contract nested the tool arguments as a
**JSON string** (`arguments_json`), i.e. JSON inside JSON. On long prompts
(3KB+ seed excerpts) gpt-realtime-2.1-mini reliably mangled the escaping —
keys and quotes leaked between fields (`"rationale\":\"...\",\"tool\":..."`
fused into a single key) and the plan failed host validation.

Fix in cse-tools `cse-sweep` (`buildCseToolSchemas` / `dispatchCsePlan`):

* `arguments` is now a **real JSON object** in the function schema
  (`type: "object", additionalProperties: true`). Realtime function calling
  emits the whole call as one JSON document, so nesting is loss-free.
* Host validation unchanged: exact tool-name enum, read-only allowlist,
  person-scope arg checks, then MCP dispatch. `arguments_json` (string)
  remains accepted as a legacy fallback.
* Empirical: 20/20 probes with object-typed arguments vs repeated escape
  mangling with string-typed on the same prompts.

Note: rtinferd's strict-schema pass (`require_all_object_properties_for_
strict_schema`) forces `additionalProperties:false` and full `required` on
every object **that lists `properties`**; a free-form `arguments` object with
no `properties` key passes through unmodified, which is exactly what the
navigator needs.

## Model/tier map after this work (cse-sweep)

| Stage | Model | Reasoning |
|---|---|---|
| Navigators (fan-out, K=8) | gpt-realtime-2.1-mini | effort omitted (mini rejects the option) |
| Score + dedup | gpt-realtime-2.1 | low |
| Final synthesis | gpt-5.6-sol (Codex Responses) | default |

## Verification

* `cargo test` (rtinfer workspace): green.
* cse-tools `node --test tests/cse-sweep-*.test.mjs`: 42 pass.
* Live sweeps via local cse-toold MCP (`127.0.0.1:9901/mcp`):
  Guardian Life / Twilio / Visa × `EXPLORE_RT_NAV_COUNT=8` →
  **24/24 `source:"navigator"` shards, 0 seed-fallback, 0 navigator-error**;
  earlier 4-nav rounds also 12/12 after the contract change.

## Related commits

* rtinfer `feat/warm-socket-lifecycle` — warm.rs lifecycle rework.
* cse-tools `feat/cse-sweep-declarative-nav` — object-typed navigator plans,
  nav debug dump (`CSE_SWEEP_NAV_DEBUG`), synthesis pinned to gpt-5.6-sol.
