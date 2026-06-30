# rtinfer

Always-on loopback inference daemon serving the **`rtinfer/1`** contract over
`127.0.0.1`. It runs one warm pool of Codex-OAuth models (`gpt-realtime-*` for
structured navigation/scoring, `gpt-5.x` codex/responses for synthesis) and
lends them to any local client (cse-tools' `cse-sweep`, unifable's judge, ...)
so each tool borrows one warm daemon instead of spawning its own.

Auth is **file-only**: both pools read `~/.codex/auth.json` and rotate the
short-lived `id_token` through the OAuth `refresh_token` grant. The daemon never
reads process env for credentials and never touches a keychain. The loopback
bind is the trust boundary; there is no wire auth header.

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
GET  /v1/infer/health  -> { contract, ready, provider, tiers }
POST /v1/infer         { contract, tier, system, user, schema?, schema_name?, model? }
```

Tiers:
- `realtime_structured` — `gpt-realtime-*`, returns `{ object }`
- `responses_structured` — `gpt-5.x` codex/responses strict schema, returns `{ object }`
- `responses_text` — `gpt-5.x` codex/responses freeform, returns `{ text }`

Errors use a stable envelope: `{ contract, ok:false, error:{ code, message, retryable } }`.

## Install

```sh
npm i -g @jaredboynton/rtinfer    # postinstall runs `rtinferd install`
# or manually:
rtinferd install                  # macOS LaunchAgent, KeepAlive, RunAtLoad
rtinferd serve --port 8765        # foreground
rtinferd status                   # show ~/.cse-rtinfer/endpoint.json
rtinferd uninstall
```

The daemon owns its own port (default **8765**) and advertises the live URL in
`~/.cse-rtinfer/endpoint.json` on boot. cse-tools' cockpit keeps 8787.

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

## Develop

```sh
cargo test --workspace
node --test clients/js/rtinfer-client.test.mjs
python3 clients/python/test_rtinfer_client.py
```
