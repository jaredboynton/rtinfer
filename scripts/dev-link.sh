#!/usr/bin/env bash
# Local dev: symlink the canonical clients into the consumer repos so edits to
# the canonical source are picked up live, without a release. Release uses real
# copies (scripts/sync-clients.sh); this is the dev-loop equivalent.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JS_SRC="$ROOT/clients/js/rtinfer-client.mjs"
PY_SRC="$ROOT/clients/python/rtinfer_client.py"

CSE_TOOLS="${CSE_TOOLS_DIR:-$HOME/__devlocal/cse-tools}"
UNIFABLE="${UNIFABLE_DIR:-$HOME/__devlocal/unifable}"

CSE_SWEEP_JS="$CSE_TOOLS/plugins/cse-tools/.agents/skills/cse-sweep/scripts/lib/daemon-client.mjs"
UNIFABLE_PY="$UNIFABLE/scripts/gate/rtinfer_client.py"

link_one() {
  local src="$1" dst="$2"
  mkdir -p "$(dirname "$dst")"
  ln -sf "$src" "$dst"
  echo "linked: $dst -> $src"
}

[ -d "$CSE_TOOLS" ] && link_one "$JS_SRC" "$CSE_SWEEP_JS"
[ -d "$UNIFABLE" ] && link_one "$PY_SRC" "$UNIFABLE_PY"
