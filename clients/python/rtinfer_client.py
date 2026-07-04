#!/usr/bin/env python3
"""Canonical rtinfer/1 HTTP client (Python).

SOURCE OF TRUTH for the Python client. Consumers vendor or symlink this file;
do not fork it. The matching JS client lives at clients/js/rtinfer-client.mjs
and MUST be edited in lockstep when the wire contract changes.

The daemon (rtinferd) serves the `rtinfer/1` loopback contract:
  POST /v1/infer            {contract, tier, system, user, schema?, schema_name?, model?,
                             thread_id?, items?, reasoning_effort?}
  GET  /v1/infer/health     -> {contract, ready, provider, tiers}

This is a *preferred* path for borrowing the shared daemon, never a required
one when used as a fallback layer: any unreachability, timeout, or non-OK
envelope returns ``(None, None)`` so callers can fall through to their own
inference path.

Discovery order (matches clients/js/rtinfer-client.mjs):
  1. $CSE_RTINFER_URL              explicit override / tests
  2. ~/.cse-rtinfer/endpoint.json  rtinferd advertises here on boot (authoritative)
  3. http://127.0.0.1:8787         legacy cse-toold cockpit default (transitional)

The health gate accepts any rtinfer/1.x (major-1 match), so a minor bump does
not dark-fail; a true rtinfer/2 cleanly falls open.

Stdlib only: urllib + json.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

CONTRACT = "rtinfer/1"
_CONTRACT_MAJOR = 1
_LEGACY_COCKPIT_DEFAULT = "http://127.0.0.1:8787"
_WELL_KNOWN = Path.home() / ".cse-rtinfer" / "endpoint.json"


def _contract_major_ok(contract: Any) -> bool:
    """True when ``contract`` is rtinfer/<major>.* matching _CONTRACT_MAJOR."""
    if not isinstance(contract, str):
        return False
    m = re.match(r"^rtinfer/(\d+)", contract)
    return bool(m) and int(m.group(1)) == _CONTRACT_MAJOR


def _debug_log(msg: str) -> None:
    if (os.environ.get("UNIFABLE_DEBUG") or os.environ.get("DEBUG") or "").strip():
        try:
            sys.stderr.write(f"[rtinfer] {msg}\n")
        except OSError:
            pass


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name) or default)
    except (TypeError, ValueError):
        return default


HEALTH_TIMEOUT = _env_float("CSE_RTINFER_HEALTH_TIMEOUT", 0.5)
REQUEST_TIMEOUT = _env_float("CSE_RTINFER_REQUEST_TIMEOUT", 95.0)
# Re-discovery is cheap but not free; cache the resolved base for this process.
_DISCOVERY_TTL = _env_float("CSE_RTINFER_DISCOVERY_TTL", 30.0)

_resolved_at = 0.0
_resolved_base: str | None = None


def _env_bool(name: str) -> bool:
    return (os.environ.get(name) or "").strip().lower() in ("1", "true", "yes", "on")


def _candidates() -> list[str]:
    out: list[str] = []
    override = os.environ.get("CSE_RTINFER_URL")
    if override:
        out.append(override.strip())
    # Strict mode: trust ONLY the explicit override, no well-known / cockpit
    # fallback. Default off keeps the documented discovery order.
    if override and _env_bool("CSE_RTINFER_STRICT_URL"):
        return out
    try:
        data = json.loads(_WELL_KNOWN.read_text("utf-8"))
        if isinstance(data, dict) and _contract_major_ok(data.get("contract")) and data.get("base_url"):
            out.append(str(data["base_url"]).strip())
    except (OSError, ValueError):
        pass
    out.append(_LEGACY_COCKPIT_DEFAULT)
    return out


def _health_ok(base: str) -> bool:
    url = base.rstrip("/") + "/v1/infer/health"
    try:
        with urllib.request.urlopen(url, timeout=HEALTH_TIMEOUT) as resp:  # noqa: S310 (loopback only)
            if resp.status != 200:
                return False
            data = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, OSError, ValueError, TimeoutError):
        return False
    if not isinstance(data, dict):
        return False
    if not _contract_major_ok(data.get("contract")):
        if data.get("contract"):
            _debug_log(f"contract mismatch at {base}: {data.get('contract')} (want rtinfer/{_CONTRACT_MAJOR}.x)")
        return False
    return data.get("ready") is True


def discover(refresh: bool = False) -> str | None:
    """Resolve a ready rtinfer base URL, or None. Cached for _DISCOVERY_TTL."""
    global _resolved_at, _resolved_base
    now = time.monotonic()
    if not refresh and _resolved_base is not None and (now - _resolved_at) < _DISCOVERY_TTL:
        return _resolved_base
    for base in _candidates():
        if _health_ok(base):
            _resolved_base = base.rstrip("/")
            _resolved_at = now
            return _resolved_base
    _resolved_base = None
    _resolved_at = now
    return None


def ask_structured(
    system: str,
    user: str,
    schema: dict[str, Any],
    *,
    schema_name: str = "result",
    model: str | None = None,
    timeout: float = REQUEST_TIMEOUT,
) -> tuple[dict[str, Any] | None, dict[str, int] | None]:
    """One structured ask over the shared daemon's realtime tier. Returns
    ``(object, usage)`` on success, ``(None, None)`` to signal fallback.

    ``usage`` is always None: the loopback endpoint does not surface token
    counts, and the borrow path is off the correctness/measurement path."""
    base = discover()
    if base is None:
        return None, None
    body = {
        "contract": CONTRACT,
        "tier": "realtime_structured",
        "system": system,
        "user": user,
        "schema": schema,
        "schema_name": schema_name,
    }
    if model:
        body["model"] = model
    payload = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        base + "/v1/infer",
        data=payload,
        headers={"content-type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 (loopback only)
            data = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, OSError, ValueError, TimeoutError):
        # Daemon went away mid-run: invalidate so the next call re-discovers.
        _invalidate()
        return None, None
    if not isinstance(data, dict) or data.get("ok") is not True:
        return None, None
    obj = data.get("object")
    if not isinstance(obj, dict):
        return None, None
    return obj, None


def ask_thread_structured(
    thread_id: str,
    system: str,
    user: str,
    schema: dict[str, Any],
    items: list[dict[str, str]],
    *,
    schema_name: str = "result",
    model: str | None = None,
    reasoning_effort: str | None = None,
    timeout: float = REQUEST_TIMEOUT,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """One structured ask over the daemon's realtime_thread_structured tier.

    ``items`` is the FULL current transcript window as ``{"id", "text"}`` dicts
    with client-stable ids; the daemon appends only the suffix it has not seen
    on ``thread_id``'s pinned socket (or replays the whole window on mismatch).

    Returns ``(object, meta)`` on success where ``meta`` holds the daemon's
    ``usage`` (provider token counts, may be None) and ``thread`` accounting
    (``appended`` / ``replayed`` / ``total_items``); ``(None, None)`` signals
    fallback to the stateless path."""
    base = discover()
    if base is None:
        return None, None
    body: dict[str, Any] = {
        "contract": CONTRACT,
        "tier": "realtime_thread_structured",
        "thread_id": thread_id,
        "system": system,
        "user": user,
        "schema": schema,
        "schema_name": schema_name,
        "items": items,
    }
    if model:
        body["model"] = model
    if reasoning_effort:
        body["reasoning_effort"] = reasoning_effort
    payload = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        base + "/v1/infer",
        data=payload,
        headers={"content-type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 (loopback only)
            data = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, OSError, ValueError, TimeoutError):
        _invalidate()
        return None, None
    if not isinstance(data, dict) or data.get("ok") is not True:
        return None, None
    obj = data.get("object")
    if not isinstance(obj, dict):
        return None, None
    meta = {
        "usage": data.get("usage") if isinstance(data.get("usage"), dict) else None,
        "thread": data.get("thread") if isinstance(data.get("thread"), dict) else None,
    }
    return obj, meta

def ask_text(
    system: str,
    user: str,
    *,
    model: str | None = None,
    timeout: float = REQUEST_TIMEOUT,
) -> str | None:
    """One freeform-text ask over the shared daemon's responses_text tier.
    Returns the assembled text on success, ``None`` to signal fallback."""
    base = discover()
    if base is None:
        return None
    body: dict[str, Any] = {
        "contract": CONTRACT,
        "tier": "responses_text",
        "system": system,
        "user": user,
    }
    if model:
        body["model"] = model
    payload = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        base + "/v1/infer",
        data=payload,
        headers={"content-type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 (loopback only)
            data = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, OSError, ValueError, TimeoutError):
        _invalidate()
        return None
    if not isinstance(data, dict) or data.get("ok") is not True:
        return None
    text = data.get("text")
    return text if isinstance(text, str) else None


def _invalidate() -> None:
    global _resolved_at, _resolved_base
    _resolved_base = None
    _resolved_at = 0.0
