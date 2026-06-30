#!/usr/bin/env bash
# Sync canonical clients into the npm js-wrapper package and into the consumer
# repos (cse-tools, unifable). Run at release time, or with --check in CI to
# assert vendored copies are byte-identical to the canonical sources.
#
# Canonical sources (SOURCE OF TRUTH):
#   clients/js/rtinfer-client.mjs
#   clients/python/rtinfer_client.py
#
# Vendored destinations:
#   packages/js-wrapper/clients/rtinfer-client.mjs        (published with npm pkg)
#   <cse-tools>/.../cse-sweep/scripts/lib/daemon-client.mjs (consumer, JS)
#   <unifable>/scripts/gate/rtinfer_client.py              (consumer, Python)
#
# Local dev uses symlinks (scripts/dev-link.sh); release uses real copies so
# the published artifacts are self-contained.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JS_SRC="$ROOT/clients/js/rtinfer-client.mjs"
PY_SRC="$ROOT/clients/python/rtinfer_client.py"

CSE_TOOLS="${CSE_TOOLS_DIR:-$HOME/__devlocal/cse-tools}"
UNIFABLE="${UNIFABLE_DIR:-$HOME/__devlocal/unifable}"

CSE_SWEEP_JS="$CSE_TOOLS/plugins/cse-tools/.agents/skills/cse-sweep/scripts/lib/daemon-client.mjs"
UNIFABLE_PY="$UNIFABLE/scripts/gate/rtinfer_client.py"
PKG_JS="$ROOT/packages/js-wrapper/clients/rtinfer-client.mjs"
PKG_PY="$ROOT/packages/js-wrapper/clients/rtinfer_client.py"

CHECK=0
[ "${1:-}" = "--check" ] && CHECK=1

sync_one() {
  local src="$1" dst="$2"
  if [ "$CHECK" = "1" ]; then
    if [ ! -f "$dst" ]; then
      echo "MISSING: $dst" >&2
      return 1
    fi
    if ! diff -q "$src" "$dst" >/dev/null; then
      echo "DRIFT: $dst differs from canonical $src" >&2
      return 1
    fi
    echo "ok: $dst"
  else
    mkdir -p "$(dirname "$dst")"
    cp "$src" "$dst"
    echo "synced: $dst"
  fi
}

rc=0
sync_one "$JS_SRC" "$PKG_JS" || rc=1
sync_one "$PY_SRC" "$PKG_PY" || rc=1
[ -d "$CSE_TOOLS" ] && { sync_one "$JS_SRC" "$CSE_SWEEP_JS" || rc=1; }
[ -d "$UNIFABLE" ] && { sync_one "$PY_SRC" "$UNIFABLE_PY" || rc=1; }
exit $rc
