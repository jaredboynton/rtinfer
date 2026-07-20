// Loopback smoke tests for the canonical JS rtinfer client.
//
// Spins a fake rtinfer/1 server on an ephemeral port, points the client at it
// via $CSE_RTINFER_URL, and asserts discovery, ready-gating, fail-loud, and
// the POST envelope shape. No live model calls.
//
// Run: node --test clients/js/rtinfer-client.test.mjs
import assert from "node:assert/strict";
import http from "node:http";
import test from "node:test";

async function withServer(handler, fn) {
  const server = http.createServer(handler);
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  const { port } = server.address();
  const base = `http://127.0.0.1:${port}`;
  try {
    return await fn(base);
  } finally {
    await new Promise((r) => server.close(r));
  }
}

function readBody(req) {
  return new Promise((resolve) => {
    let b = "";
    req.on("data", (c) => (b += c));
    req.on("end", () => resolve(b));
  });
}

// Fresh module per test so the per-process discovery cache does not leak.
// Strict mode keeps the test hermetic: only $CSE_RTINFER_URL is trusted, never
// a real daemon that happens to be running on this machine.
async function freshClient(base) {
  process.env.CSE_RTINFER_URL = base || "";
  process.env.CSE_RTINFER_STRICT_URL = "1";
  return import(`./rtinfer-client.mjs?ts=${Date.now()}_${Math.random()}`);
}

test("discovers a ready daemon and posts responses_text", async () => {
  await withServer(
    async (req, res) => {
      if (req.url === "/v1/infer/health") {
        res.setHeader("content-type", "application/json");
        res.end(JSON.stringify({ contract: "rtinfer/1", ready: true, tiers: [] }));
        return;
      }
      if (req.url === "/v1/infer" && req.method === "POST") {
        const body = JSON.parse(await readBody(req));
        assert.equal(body.contract, "rtinfer/1");
        assert.equal(body.tier, "responses_text");
        res.setHeader("content-type", "application/json");
        res.end(JSON.stringify({ contract: "rtinfer/1", ok: true, tier: "responses_text", text: "hi" }));
        return;
      }
      res.statusCode = 404;
      res.end("{}");
    },
    async (base) => {
      const c = await freshClient(base);
      assert.equal(await c.daemonEnabled(), true);
      const out = await c.postInfer("responses_text", { system: "s", user: "u" });
      assert.equal(out, "hi");
    },
  );
});

test("daemonAskResponsesStructured posts exact terra high envelope", async () => {
  await withServer(
    async (req, res) => {
      if (req.url === "/v1/infer/health") {
        res.setHeader("content-type", "application/json");
        res.end(JSON.stringify({ contract: "rtinfer/1", ready: true }));
        return;
      }
      const body = JSON.parse(await readBody(req));
      assert.deepEqual(body, {
        contract: "rtinfer/1",
        tier: "responses_structured",
        system: "judge",
        user: "payload",
        schema: { type: "object", properties: { choice_id: { type: "string" } } },
        schema_name: "capsule_gate",
        model: "gpt-5.6-terra",
        reasoning_effort: "high",
      });
      res.setHeader("content-type", "application/json");
      res.end(JSON.stringify({ contract: "rtinfer/1", ok: true, object: { choice_id: "a" } }));
    },
    async (base) => {
      const c = await freshClient(base);
      const out = await c.daemonAskResponsesStructured(
        {
          system: "judge",
          user: "payload",
          schema: { type: "object", properties: { choice_id: { type: "string" } } },
          schemaName: "capsule_gate",
          reasoningEffort: "high",
        },
        { model: "gpt-5.6-terra" },
      );
      assert.deepEqual(out, { choice_id: "a" });
    },
  );
});

test("ready:false is treated as not reachable", async () => {
  await withServer(
    (req, res) => {
      res.setHeader("content-type", "application/json");
      res.end(JSON.stringify({ contract: "rtinfer/1", ready: false }));
    },
    async (base) => {
      const c = await freshClient(base);
      assert.equal(await c.discoverEndpoint(), null);
    },
  );
});

test("major contract mismatch falls open", async () => {
  await withServer(
    (req, res) => {
      res.setHeader("content-type", "application/json");
      res.end(JSON.stringify({ contract: "rtinfer/2", ready: true }));
    },
    async (base) => {
      const c = await freshClient(base);
      assert.equal(await c.discoverEndpoint(), null);
    },
  );
});

test("unreachable daemon throws DaemonUnreachable", async () => {
  // Point at a closed port (nothing listening).
  const c = await freshClient("http://127.0.0.1:1");
  await assert.rejects(() => c.postInfer("responses_text", { system: "s", user: "u" }), {
    name: "DaemonUnreachable",
  });
});

test("defaults realtime scoring to gpt-realtime-2.1", async () => {
  const previous = process.env.EXPLORE_SEARCH_SCORER_MODEL;
  delete process.env.EXPLORE_SEARCH_SCORER_MODEL;
  try {
    const c = await freshClient("http://127.0.0.1:1");
    assert.equal(c.scorerModel(), "gpt-realtime-2.1");
  } finally {
    if (previous === undefined) delete process.env.EXPLORE_SEARCH_SCORER_MODEL;
    else process.env.EXPLORE_SEARCH_SCORER_MODEL = previous;
  }
});
