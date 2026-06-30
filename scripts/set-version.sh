#!/usr/bin/env bash
# Single source of truth for the project version. Writes <X.Y.Z> into every
# managed manifest so the built binary's CARGO_PKG_VERSION always equals the
# published npm version (otherwise the in-daemon self-update loops forever).
#
#   scripts/set-version.sh 0.1.3     # write the version everywhere
#   scripts/set-version.sh --check   # exit non-zero if any manifest drifts
#
# Managed manifests:
#   - Cargo.toml                       [workspace.package] version
#   - packages/js-wrapper/package.json version + optionalDependencies pins
#   - packages/darwin-arm64/package.json version
#   - packages/linux-arm64/package.json version
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

CARGO_TOML="$ROOT/Cargo.toml"
JS_WRAPPER="$ROOT/packages/js-wrapper/package.json"
DARWIN="$ROOT/packages/darwin-arm64/package.json"
LINUX="$ROOT/packages/linux-arm64/package.json"

cargo_version() {
  awk '/^\[workspace\.package\]/{f=1} f&&/^version *= *"/{gsub(/^version *= *"|".*$/,"");print;exit}' "$CARGO_TOML"
}

pkg_version() {
  awk -F'"' '/"version"/{print $4;exit}' "$1"
}

set_cargo_version() {
  local v="$1"
  awk -v v="$v" '
    /^\[/{inpkg=($0=="[workspace.package]")}
    inpkg && /^version *= *"/{print "version = \"" v "\"";next}
    {print}
  ' "$CARGO_TOML" >"$CARGO_TOML.tmp"
  mv "$CARGO_TOML.tmp" "$CARGO_TOML"
}

set_pkg_version() {
  local file="$1" v="$2"
  # Top-level "version": "..." (first occurrence) and the two optionalDependencies pins.
  awk -v v="$v" '
    !done_version && /"version"[[:space:]]*:/ {
      sub(/"version"[[:space:]]*:[[:space:]]*"[^"]*"/, "\"version\": \"" v "\"")
      done_version=1
    }
    /@jaredboynton\/rtinfer-(darwin-arm64|linux-arm64)"[[:space:]]*:/ {
      sub(/:[[:space:]]*"[^"]*"/, ": \"" v "\"")
    }
    { print }
  ' "$file" >"$file.tmp"
  mv "$file.tmp" "$file"
}

check() {
  local cargo js darwin linux
  cargo="$(cargo_version)"
  js="$(pkg_version "$JS_WRAPPER")"
  darwin="$(pkg_version "$DARWIN")"
  linux="$(pkg_version "$LINUX")"
  local opt_darwin opt_linux
  opt_darwin="$(awk -F'"' '/@jaredboynton\/rtinfer-darwin-arm64"/{print $4;exit}' "$JS_WRAPPER")"
  opt_linux="$(awk -F'"' '/@jaredboynton\/rtinfer-linux-arm64"/{print $4;exit}' "$JS_WRAPPER")"

  local ok=1
  for pair in \
    "Cargo.toml=$cargo" \
    "js-wrapper=$js" \
    "darwin-arm64=$darwin" \
    "linux-arm64=$linux" \
    "optionalDependencies.darwin=$opt_darwin" \
    "optionalDependencies.linux=$opt_linux"; do
    echo "  ${pair%%=*}: ${pair#*=}"
  done

  if [ "$cargo" = "$js" ] && [ "$js" = "$darwin" ] && [ "$darwin" = "$linux" ] \
     && [ "$opt_darwin" = "$cargo" ] && [ "$opt_linux" = "$cargo" ]; then
    echo "version: all manifests agree on $cargo"
  else
    echo "version drift detected" >&2
    ok=0
  fi
  [ "$ok" = 1 ]
}

main() {
  [ $# -eq 1 ] || { echo "usage: set-version.sh <X.Y.Z|--check>" >&2; exit 2; }

  if [ "$1" = "--check" ]; then
    check
    return
  fi

  local version="$1"
  echo "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' || {
    echo "invalid version: $version (expected X.Y.Z)" >&2; exit 2; }

  set_cargo_version "$version"
  set_pkg_version "$JS_WRAPPER" "$version"
  set_pkg_version "$DARWIN" "$version"
  set_pkg_version "$LINUX" "$version"

  # Keep Cargo.lock in lockstep so the build is deterministic.
  if command -v cargo >/dev/null 2>&1; then
    (cd "$ROOT" && cargo update -p rtinfer-core -p rtinfer-daemon --precise "$version" >/dev/null 2>&1) || true
  fi

  echo "set version to $version"
  check
}

main "$@"
