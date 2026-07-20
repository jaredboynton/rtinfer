// Canonical rtinfer/1 HTTP client (JS).
//
// This is the SOURCE OF TRUTH for the JS client. Consumers vendor or symlink
// this file; do not fork it. The matching Python client lives at
// clients/python/rtinfer_client.py and MUST be edited in lockstep when the
// wire contract changes.
//
// The daemon (rtinferd) serves the `rtinfer/1` loopback contract:
//   POST /v1/infer            { contract, tier, system, user, schema?, schema_name?, model?,
//                               thread_id?, items?, reasoning_effort? }
//   GET  /v1/infer/health     -> { contract, ready, provider, tiers }
//
// The daemon is the ONLY inference path: there is no live-realtime fallback.
// When no daemon is reachable, calls reject with DaemonUnreachable so the
// orchestrator fails loud (non-zero exit) rather than silently producing
// zero output.
//
// Discovery order (deterministic; no repo imports another):
//   1. $CSE_RTINFER_URL              explicit override / tests
//   2. ~/.cse-rtinfer/endpoint.json  rtinferd advertises here on boot (authoritative)
//   3. http://127.0.0.1:8787         legacy cse-toold cockpit default (transitional)
//   else: unreachable -> fail loud.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

export const RTINFER_CONTRACT = "rtinfer/1";
const CONTRACT_MAJOR = 1;
const WELL_KNOWN = path.join(os.homedir(), ".cse-rtinfer", "endpoint.json");
const LEGACY_COCKPIT_DEFAULT = "http://127.0.0.1:8787";
const POOL_SIZE = Math.max(1, parseInt(process.env.EXPLORE_SEARCH_DAEMON_POOL || "4", 10));
const SCORER_MODEL = (process.env.EXPLORE_SEARCH_SCORER_MODEL || "gpt-realtime-2.1").trim();
const HEALTH_TIMEOUT_MS = Math.round(
  (parseFloat(process.env.EXPLORE_SEARCH_DAEMON_CONNECT || "") || 0.5) * 1000,
);
const REQUEST_TIMEOUT_MS = Math.round(
  (parseFloat(process.env.EXPLORE_SEARCH_DAEMON_REQUEST || "") || 20.0) * 1000,
);
// Synthesis (responses_text, gpt-5.x map-reduce) runs ~30s on a full pack and
// far longer on a map-reduce, so it gets its own higher ceiling. The realtime
// navigator/scorer tiers keep the tight 20s timeout above.
const SYNTH_REQUEST_TIMEOUT_MS = Math.round(
  (parseFloat(process.env.EXPLORE_SEARCH_DAEMON_SYNTH_REQUEST || "") || 90.0) * 1000,
);

let _resolved = false;
let _base = null;

export class DaemonUnreachable extends Error {
  constructor(message = "no rtinfer daemon reachable") {
    super(message);
    this.name = "DaemonUnreachable";
  }
}

export function daemonPoolSize() {
  return POOL_SIZE;
}

export function scorerModel() {
  return SCORER_MODEL;
}

// Accept any rtinfer/<major>.* matching CONTRACT_MAJOR so a minor bump does
// not dark-fail; a true rtinfer/2 cleanly falls open.
function contractMajorOk(contract) {
  if (typeof contract !== "string") return false;
  const m = /^rtinfer\/(\d+)/.exec(contract);
  return !!m && parseInt(m[1], 10) === CONTRACT_MAJOR;
}

function envBool(name) {
  return ["1", "true", "yes", "on"].includes((process.env[name] || "").trim().toLowerCase());
}

function healthUrl(base) {
  return `${base.replace(/\/$/, "")}/v1/infer/health`;
}

function inferUrl(base) {
  return `${base.replace(/\/$/, "")}/v1/infer`;
}

// A daemon is usable only if it answers health with our contract and reports
// ready (codex auth reachable). `ready:false` means "present but warming" -> we
// keep probing the next candidate, then the caller retries briefly.
async function probe(base) {
  try {
    const r = await fetch(healthUrl(base), { signal: AbortSignal.timeout(HEALTH_TIMEOUT_MS) });
    if (!r.ok) return false;
    const d = await r.json();
    return d && contractMajorOk(d.contract) && d.ready === true;
  } catch {
    return false;
  }
}

function candidates() {
  const out = [];
  if (process.env.CSE_RTINFER_URL) out.push(process.env.CSE_RTINFER_URL);
  // Strict mode: trust ONLY the explicit override, no well-known / cockpit
  // fallback. Default off keeps the documented discovery order. Mirrors the
  // Python client's CSE_RTINFER_STRICT_URL.
  if (process.env.CSE_RTINFER_URL && envBool("CSE_RTINFER_STRICT_URL")) return out;
  try {
    const d = JSON.parse(fs.readFileSync(WELL_KNOWN, "utf8"));
    if (d && contractMajorOk(d.contract) && d.base_url) out.push(d.base_url);
  } catch {
    /* no well-known file */
  }
  out.push(LEGACY_COCKPIT_DEFAULT);
  return out;
}

// Resolve the daemon base URL once per process. Returns null when nothing is
// reachable (the caller decides whether that is fatal).
export async function discoverEndpoint({ refresh = false } = {}) {
  if (_resolved && !refresh) return _base;
  for (const base of candidates()) {
    // eslint-disable-next-line no-await-in-loop
    if (await probe(base)) {
      _base = base;
      _resolved = true;
      return _base;
    }
  }
  _base = null;
  _resolved = true;
  return null;
}

// True when a daemon is reachable. Async (was a sync gate-dir check before).
export async function daemonEnabled() {
  if (process.env.EXPLORE_SEARCH_DAEMON === "0") return false;
  return (await discoverEndpoint()) != null;
}

// Warm-up = discovery probe; the server pool is already hot, so there is no
// per-process socket to spawn. Returns the base URL or null.
export async function warmDaemonPool() {
  return discoverEndpoint();
}

// POST one rtinfer request. Throws DaemonUnreachable when no daemon is
// reachable; returns null on a per-request error (so a single bad ask degrades
// without aborting a batch). `tier` selects the model arm.
export async function postInfer(tier, body, {returnEnvelope = false} = {}) {
  const base = await discoverEndpoint();
  if (!base) throw new DaemonUnreachable();
  const timeoutMs = tier === "responses_text" ? SYNTH_REQUEST_TIMEOUT_MS : REQUEST_TIMEOUT_MS;
  let resp;
  try {
    resp = await fetch(inferUrl(base), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ contract: RTINFER_CONTRACT, tier, ...body }),
      signal: AbortSignal.timeout(timeoutMs),
    });
  } catch (e) {
    // Transport failure after a successful health probe: the daemon went away
    // mid-run. Treat as unreachable so the orchestrator fails loud.
    throw new DaemonUnreachable(`rtinfer request failed: ${e.message}`);
  }
  let json = null;
  try {
    json = await resp.json();
  } catch {
    return null;
  }
  if (!resp.ok || !json || json.ok !== true) return null;
  if (tier === "responses_text") return json.text;
  return returnEnvelope ? {object: json.object, usage: json.usage ?? null} : json.object;
}

// One structured realtime ask (navigator / scorer). Returns the parsed object
// or null on a per-request error.
export async function daemonAsk(namespace, req, { model = SCORER_MODEL } = {}) {
  return postInfer("realtime_structured", {
    system: req.system,
    user: req.user,
    schema: req.schema,
    schema_name: req.schemaName || req.schema_name || "result",
    model,
    reasoning_effort: req.reasoningEffort,
  });
}

// One structured Responses ask through the daemon's shared WebSocket pool.
export async function daemonAskResponsesStructured(req, { model = "gpt-5.4" } = {}) {
  return postInfer("responses_structured", {
    system: req.system,
    user: req.user,
    schema: req.schema,
    schema_name: req.schemaName || req.schema_name || "result",
    model,
    reasoning_effort: req.reasoningEffort,
  });
}

// Structured Responses ask with provider usage retained for evaluation and
// accounting callers. Normal product callers use the object-only helper above.
export async function daemonAskResponsesStructuredDetailed(req, {model = "gpt-5.4"} = {}) {
  return postInfer(
    "responses_structured",
    {
      system: req.system,
      user: req.user,
      schema: req.schema,
      schema_name: req.schemaName || req.schema_name || "result",
      model,
      reasoning_effort: req.reasoningEffort,
    },
    {returnEnvelope: true},
  );
}

// One structured thread ask (realtime_thread_structured): server-side pinned
// conversation keyed by threadId. `items` is the FULL current transcript
// window as { id, text } objects with client-stable ids; the daemon appends
// only the unseen suffix (or replays on mismatch). Returns
// { object, usage, thread } or null on a per-request error.
export async function daemonAskThread(threadId, req, { model = SCORER_MODEL } = {}) {
  const base = await discoverEndpoint();
  if (!base) throw new DaemonUnreachable();
  let resp;
  try {
    resp = await fetch(inferUrl(base), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        contract: RTINFER_CONTRACT,
        tier: "realtime_thread_structured",
        thread_id: threadId,
        system: req.system,
        user: req.user,
        schema: req.schema,
        schema_name: req.schemaName || req.schema_name || "result",
        items: req.items || [],
        model,
        reasoning_effort: req.reasoningEffort,
      }),
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });
  } catch (e) {
    throw new DaemonUnreachable(`rtinfer thread request failed: ${e.message}`);
  }
  let json = null;
  try {
    json = await resp.json();
  } catch {
    return null;
  }
  if (!resp.ok || !json || json.ok !== true || typeof json.object !== "object" || json.object === null) return null;
  return { object: json.object, usage: json.usage ?? null, thread: json.thread ?? null };
}

// Batch of structured realtime asks. True parallelism comes from the server
// pool's semaphore; the client just fans out concurrent fetches. Per-request
// errors surface as null elements; total unreachability throws.
export async function daemonAskBatch(namespace, requests, { model = SCORER_MODEL } = {}) {
  // Probe once up front so an unreachable daemon throws before fan-out instead
  // of N times in parallel.
  const base = await discoverEndpoint();
  if (!base) throw new DaemonUnreachable();
  return Promise.all(requests.map((req) => daemonAsk(namespace, req, { model })));
}
