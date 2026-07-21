# rtinfer

Always-on loopback inference daemon serving the stable **`rtinfer/1`**
contract over `127.0.0.1`. Realtime tiers retain warm-session pools.
`responses_structured` and `responses_text` instead share one
`CodexResponsesClient`: it coordinates a reused HTTP/2+SSE client and
exclusive reusable WSS sockets through cancellation-safe adaptive lane windows
under an aggregate ceiling. Local clients (cse-tools' `cse-sweep`, unifable's
judge, ...) borrow the daemon rather than each starting their own connections.

Auth defaults to `~/.codex/auth.json`, including client-side rotation of its
short-lived tokens. Production callers can instead configure an absolute
`cse-toold` binary as a credential process. In that mode one shared source feeds
all pools, `auth.json` is never a fallback, and rtinfer never receives a refresh
token. The loopback bind is the trust boundary; there is no wire auth header.

## Layout

```
crates/rtinfer-core      forked inference engine (realtime + codex/responses + auth)
crates/rtinfer-daemon    rtinferd: axum /v1/infer server, endpoint-file writer, LaunchAgent
clients/js               canonical JS client (rtinfer-client.mjs) + tests
clients/python           canonical Python client (rtinfer_client.py) + tests
clients/contract.json    the rtinfer/1 wire fixture
packages/                npm meta + platform packages
scripts/                 sync-clients.sh (release), dev-link.sh (local symlinks)
```

## Contract: `rtinfer/1`

```
GET  /v1/infer/health  -> { contract, ready, provider, auth_source, tiers }
POST /v1/infer         { contract, tier, system, user, schema?, schema_name?, model? }
```

Tiers:
- `realtime_structured` — `gpt-realtime-*`, returns `{ object }`
- `responses_structured` — `gpt-5.x` codex/responses strict schema, returns `{ object }`
- `responses_text` — `gpt-5.x` codex/responses freeform, returns `{ text }`

Errors use a stable envelope: `{ contract, ok:false, error:{ code, message, retryable } }`.

The OpenAI-compatible `POST /v1/responses` endpoint is separate: it remains
Realtime-backed and is unaffected by the Responses transport settings below.

## Responses transport, safety, and health

`responses_structured` and `responses_text` select the enabled lane(s) within
the shared coordinator. A successful Responses ask requires the semantic
`response.completed` event. Partial output, `response.done`, or EOF are
failures; after sending, rtinfer does not replay an ask or fail it over to the
other lane.

| Setting | Default / accepted values |
| --- | --- |
| `RTINFER_RESPONSES_TRANSPORT` | `wss`; `wss`, `http`, or `dual` |
| `RTINFER_RESPONSES_HTTP_INITIAL`, `RTINFER_RESPONSES_HTTP_MAX` | `32`, `48` (initial cannot exceed max; explicit max hard bound `256`) |
| `RTINFER_RESPONSES_WSS_INITIAL`, `RTINFER_RESPONSES_WSS_MAX` | `32`, `48` (initial cannot exceed max; explicit max hard bound `64`) |
| `RTINFER_RESPONSES_AGGREGATE_MAX` | `48` in every mode; cannot exceed the enabled-lane maxima sum |
| `RTINFER_RESPONSES_PREWARM` | `0`; opens WSS sockets only, so it is ignored in HTTP-only mode |
| `RTINFER_RESPONSES_CAPACITY` | Deprecated WSS-max alias. `0` maps to the bounded hard maximum, `64`; use `RTINFER_RESPONSES_WSS_MAX` instead. |

Invalid transport or bound values fail daemon startup. If both WSS max and the
deprecated capacity alias are set, WSS max wins.

`GET /v1/infer/health` retains its top-level `contract`, `ready`, `provider`,
`auth_source`, and `tiers` fields. Its additive `responses` object reports the
mode; aggregate bound, in-flight, waiting, and throttle state; per-lane bounds,
in-flight state, adaptive counters, and samples; HTTP connection reuse and
dispatches; WSS idle sockets, dispatches, handshake attempts, and active asks;
and a non-secret auth generation. Readiness does not require previous adaptive
samples.

## Install

```sh
npm i -g @jaredboynton/rtinfer    # postinstall runs `rtinferd install`
# or manually:
rtinferd install                  # macOS LaunchAgent, KeepAlive, RunAtLoad
rtinferd serve --port 8765        # foreground
rtinferd serve --cse-toold-bin /absolute/path/to/cse-toold
rtinferd status                   # show ~/.cse-rtinfer/endpoint.json
rtinferd uninstall
```

Optional platform packages ship the native binary for `darwin-arm64`,
`linux-arm64`, and `linux-x64`. The daemon owns its own port (default **8765**)
and advertises the live URL in `~/.cse-rtinfer/endpoint.json` on boot.
Linux releases enforce a GLIBC 2.35 ceiling (Ubuntu 22.04 or equivalent).
cse-tools' cockpit keeps 8787.

### macOS LaunchAgent canary and rollback

The LaunchAgent label is `com.jaredboynton.rtinferd`. A global npm install pins
the agent to npm's stable `rtinferd` shim; npm rewrites that shim in place, but
an already-running process needs a kickstart to adopt it.

```sh
# Canary both Responses lanes, then replace the running process.
launchctl setenv RTINFER_RESPONSES_TRANSPORT dual
launchctl kickstart -k "gui/$UID/com.jaredboynton.rtinferd"

# Verify both daemon readiness and the selected mode.
curl -fsS http://127.0.0.1:8765/v1/infer/health | python3 -c \
  'import json,sys; h=json.load(sys.stdin); assert h["ready"] and h["responses"]["mode"] == "dual"; print("ready dual")'
```

Preferred rollback is transport-only: restore the default WSS mode and
kickstart, then verify health again.

```sh
launchctl unsetenv RTINFER_RESPONSES_TRANSPORT
launchctl kickstart -k "gui/$UID/com.jaredboynton.rtinferd"
curl -fsS http://127.0.0.1:8765/v1/infer/health
```

Use a binary rollback only exceptionally, after the WSS rollback fails: install
an explicitly selected known-good published version, run `rtinferd --version`,
and verify that output is the selected version before relying on the restarted
agent.

`RTINFER_CSE_TOOLD_BIN` is the environment equivalent of
`--cse-toold-bin`. rtinfer executes that path directly (never through a shell)
as `codex-lease --min-valid-for-seconds 300`; health reports `ready:false` if
the configured provider cannot return a valid v1 lease. Omit both settings to
keep the default file-auth behavior.

For the cse-toold lease provider, only positive credential refusal evidence —
`invalid_grant`, unenrollment, or refresh-token rejection/revocation — maps to
non-retryable `auth_unavailable`. Spawn, timeout, malformed lease output, and
a nonzero lease command without that refusal evidence map to retryable
`provider_error`; callers must not latch those lease-plane failures as dead
credentials.

## Sticky Routing

`realtime_structured` uses a warm-session pool with prompt-cache sticky routing:
the system prompt hashes to a session family, repeated same-family calls stay on
the same socket for cache hits, and same-family parallel bursts overflow without
re-pinning so the next serial call still lands on the cache home.

Tuning knobs:
- `RTINFER_STICKY_ROUTING=0` disables sticky routing for A/B or rollback.
- `RTINFER_STICKY_OVERFLOW_INFLIGHT=N` changes the overflow threshold. Default
  `1` means any overlapping same-family call spills to another session.
- Legacy `UNIFABLE_STICKY_ROUTING` and `UNIFABLE_STICKY_OVERFLOW_INFLIGHT`
  remain accepted during the cutover.

## Clients (discovery order)

1. `$CSE_RTINFER_URL` — explicit override / tests (`CSE_RTINFER_STRICT_URL=1` to trust only this)
2. `~/.cse-rtinfer/endpoint.json` — rtinferd advertises here (authoritative)
3. `http://127.0.0.1:8787` — legacy cse-toold cockpit (transitional)

The clients in `clients/` are the **source of truth**. Consumers vendor a copy
at release (`scripts/sync-clients.sh`) and symlink for local dev
(`scripts/dev-link.sh`). Edit JS and Python in lockstep when the contract
changes; the health gate accepts any `rtinfer/1.x` so a minor bump never
dark-fails and a true `rtinfer/2` cleanly falls open.

Per-request client wall clocks default to **300s** so saturated local fan-out
does not abort while a healthy warm pool is still working. Override with
`EXPLORE_SEARCH_DAEMON_REQUEST` / `EXPLORE_SEARCH_DAEMON_SYNTH_REQUEST` (JS)
or `CSE_RTINFER_REQUEST_TIMEOUT` (Python).

## Develop

```sh
cargo test --workspace
node --test clients/js/rtinfer-client.test.mjs
python3 clients/python/test_rtinfer_client.py
```

For Responses-specific, non-network validation:

```sh
cargo test -p rtinfer-core responses::tests::assembler_requires_completed_and_prefers_done_text
cargo test -p rtinfer-core responses::tests::builder_mode_defaults_aggregate_enabled_sum
cargo test -p rtinfer-daemon server::tests::responses_transport_explicit_modes
cargo test -p rtinfer-daemon server::tests::responses_health_exposes_bounded_snapshot
```

Sanitized v0.1.15 benchmark evidence recorded dual at **4.6182 completed
requests/s** versus **4.1715** for the best single transport (threshold
**4.3800**), with **216/216** successes across both lanes. The record contains
no prompts, outputs, tokens, or raw frames.
