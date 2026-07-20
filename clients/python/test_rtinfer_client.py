#!/usr/bin/env python3
"""Loopback smoke tests for the canonical Python rtinfer client.

Spins a fake rtinfer/1 server on an ephemeral port, points the client at it via
$CSE_RTINFER_URL, and asserts discovery, ready-gating, contract major-gating,
and the POST envelope. No live model calls.

Run: python3 -m pytest clients/python/test_rtinfer_client.py
"""

from __future__ import annotations

import importlib
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import rtinfer_client


def _make_handler(health_body, infer_body, received=None):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *_a):  # silence
            pass

        def _send(self, obj):
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(obj).encode())

        def do_GET(self):
            if self.path == "/v1/infer/health":
                self._send(health_body)
            else:
                self.send_response(404)
                self.end_headers()

        def do_POST(self):
            length = int(self.headers.get("content-length", 0))
            body = json.loads(self.rfile.read(length) or b"{}")
            assert body.get("contract") == "rtinfer/1"
            if received is not None:
                received.append(body)
            self._send(infer_body)

    return H


def _serve(health_body, infer_body, received=None):
    server = HTTPServer(("127.0.0.1", 0), _make_handler(health_body, infer_body, received))
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    return server, base


def _fresh(base: str | None):
    # Strict mode keeps the test hermetic: only $CSE_RTINFER_URL is trusted,
    # never a real daemon running on this machine (cse-toold on 8787, etc).
    os.environ["CSE_RTINFER_STRICT_URL"] = "1"
    if base:
        os.environ["CSE_RTINFER_URL"] = base
    else:
        os.environ.pop("CSE_RTINFER_URL", None)
    importlib.reload(rtinfer_client)
    return rtinfer_client


def test_discovers_and_asks_structured():
    server, base = _serve(
        {"contract": "rtinfer/1", "ready": True, "tiers": []},
        {"contract": "rtinfer/1", "ok": True, "tier": "realtime_structured", "object": {"x": 1}},
    )
    try:
        c = _fresh(base)
        obj, usage = c.ask_structured("s", "u", {"type": "object"})
        assert obj == {"x": 1}
        assert usage is None
    finally:
        server.shutdown()


def test_ask_text_tier():
    server, base = _serve(
        {"contract": "rtinfer/1", "ready": True},
        {"contract": "rtinfer/1", "ok": True, "tier": "responses_text", "text": "hello"},
    )
    try:
        c = _fresh(base)
        assert c.ask_text("s", "u") == "hello"
    finally:
        server.shutdown()


def test_ask_responses_structured_envelope():
    received = []
    server, base = _serve(
        {"contract": "rtinfer/1", "ready": True},
        {"contract": "rtinfer/1", "ok": True, "tier": "responses_structured", "object": {"choice_id": "a"}},
        received,
    )
    try:
        c = _fresh(base)
        obj, usage = c.ask_responses_structured(
            "judge",
            "payload",
            {"type": "object", "properties": {"choice_id": {"type": "string"}}},
            schema_name="capsule_gate",
            model="gpt-5.6-terra",
            reasoning_effort="high",
        )
        assert obj == {"choice_id": "a"}
        assert usage is None
        assert received == [{
            "contract": "rtinfer/1",
            "tier": "responses_structured",
            "system": "judge",
            "user": "payload",
            "schema": {"type": "object", "properties": {"choice_id": {"type": "string"}}},
            "schema_name": "capsule_gate",
            "model": "gpt-5.6-terra",
            "reasoning_effort": "high",
        }]
    finally:
        server.shutdown()


def test_ready_false_falls_open():
    server, base = _serve({"contract": "rtinfer/1", "ready": False}, {})
    try:
        c = _fresh(base)
        assert c.discover(refresh=True) is None
        assert c.ask_structured("s", "u", {}) == (None, None)
    finally:
        server.shutdown()


def test_major_mismatch_falls_open():
    server, base = _serve({"contract": "rtinfer/2", "ready": True}, {})
    try:
        c = _fresh(base)
        assert c.discover(refresh=True) is None
    finally:
        server.shutdown()


if __name__ == "__main__":
    test_discovers_and_asks_structured()
    test_ask_text_tier()
    test_ask_responses_structured_envelope()
    test_ready_false_falls_open()
    test_major_mismatch_falls_open()
    print("all rtinfer python client tests passed")
