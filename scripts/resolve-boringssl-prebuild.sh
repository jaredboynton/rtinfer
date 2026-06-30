#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PRINT_ENV=0
if [ "${1:-}" = "--print-env" ]; then
  PRINT_ENV=1
  shift
fi

TARGET="${1:-}"
if [ -z "$TARGET" ]; then
  echo "usage: $0 [--print-env] <rust-target-triple>" >&2
  exit 2
fi

BSSL_VERSION="$(
  CARGO_LOCK="$ROOT/Cargo.lock" python3 - <<'PY'
import os, pathlib, re
text = pathlib.Path(os.environ["CARGO_LOCK"]).read_text()
m = re.search(r'\[\[package\]\]\s+name = "boring-sys"\s+version = "([^"]+)"', text)
if not m:
    raise SystemExit("could not resolve boring-sys version from Cargo.lock")
print(m.group(1))
PY
)"

CACHE_ROOT="${BORING_BSSL_PREBUILT_ROOT:-$HOME/boringssl}"
DEST="$CACHE_ROOT/$TARGET/$BSSL_VERSION"
PKG="@jaredboynton/bssl-prebuild-$TARGET@$BSSL_VERSION"

valid_prebuild() {
  { [ -f "$1/lib/libcrypto.a" ] || [ -f "$1/lib/crypto.lib" ]; } &&
    { [ -f "$1/lib/libssl.a" ] || [ -f "$1/lib/ssl.lib" ]; } &&
    [ -f "$1/include/openssl/ssl.h" ]
}

if ! valid_prebuild "$DEST"; then
  TMP="$(mktemp -d "${TMPDIR:-/tmp}/rtinfer-bssl.XXXXXX")"
  trap 'rm -rf "$TMP"' EXIT
  echo "rtinfer: fetching BoringSSL prebuild $PKG" >&2
  PACK_NAME="$TMP/pack-name"
  (cd "$TMP" && npm pack "$PKG" --silent >"$PACK_NAME")
  TARBALL="$TMP/$(cat "$PACK_NAME")"
  rm -rf "$TMP/package" "$DEST"
  tar -xzf "$TARBALL" -C "$TMP"
  mkdir -p "$(dirname "$DEST")"
  mv "$TMP/package" "$DEST"
fi

if ! valid_prebuild "$DEST"; then
  echo "rtinfer: invalid BoringSSL prebuild at $DEST" >&2
  exit 1
fi

if [ "$PRINT_ENV" = "1" ]; then
  printf 'export BORING_BSSL_PATH=%q\n' "$DEST"
  printf 'export BORING_BSSL_INCLUDE_PATH=%q\n' "$DEST/include"
else
  echo "$DEST"
fi
