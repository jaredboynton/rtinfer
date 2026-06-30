#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

rust_target_for_package() {
  case "$1" in
    darwin-arm64) echo "aarch64-apple-darwin" ;;
    darwin-x64) echo "x86_64-apple-darwin" ;;
    linux-arm64) echo "aarch64-unknown-linux-gnu" ;;
    linux-x64) echo "x86_64-unknown-linux-gnu" ;;
    win32-arm64) echo "aarch64-pc-windows-msvc" ;;
    win32-x64) echo "x86_64-pc-windows-msvc" ;;
    *)
      echo "unknown package platform '$1'" >&2
      return 1
      ;;
  esac
}

bin_name_for_package() {
  case "$1" in
    win32-*) echo "rtinferd.exe" ;;
    *) echo "rtinferd" ;;
  esac
}

build_for_target() {
  local package_dir="$1"
  local target="$2"
  local bin_name="$3"
  local out_dir="$ROOT/packages/$package_dir/bin"
  local rustc_bin=""

  if command -v rustup >/dev/null 2>&1; then
    rustc_bin="$(rustup which --toolchain stable rustc 2>/dev/null || true)"
  fi
  [ -n "$rustc_bin" ] || rustc_bin="$(command -v rustc)"

  local -a env_prefix=(env "RUSTC=$rustc_bin")
  case "$target" in
    *linux*|*windows*)
      eval "$("$ROOT/scripts/resolve-boringssl-prebuild.sh" --print-env "$target")"
      env_prefix+=(BORING_BSSL_PATH="$BORING_BSSL_PATH")
      env_prefix+=(BORING_BSSL_INCLUDE_PATH="$BORING_BSSL_INCLUDE_PATH")
      ;;
  esac

  if [[ "$target" == *linux* ]]; then
    "${env_prefix[@]}" cargo zigbuild --release --target "$target" -p rtinfer-daemon
  else
    "${env_prefix[@]}" cargo build --release --target "$target" -p rtinfer-daemon
  fi

  mkdir -p "$out_dir"
  cp "$ROOT/target/$target/release/$bin_name" "$out_dir/$bin_name"
  chmod 755 "$out_dir/$bin_name"

  local host
  host="$(rustc -vV | awk '/^host: / {print $2}')"
  if [ "$target" = "$host" ]; then
    "$out_dir/$bin_name" --version
  else
    file "$out_dir/$bin_name"
  fi
}

main() {
  cd "$ROOT"
  "$ROOT/scripts/sync-clients.sh" --package-only

  local package_json package_dir target bin_name
  for package_json in "$ROOT"/packages/*/package.json; do
    package_dir="$(basename "$(dirname "$package_json")")"
    [ "$package_dir" = "js-wrapper" ] && continue
    target="$(rust_target_for_package "$package_dir")"
    bin_name="$(bin_name_for_package "$package_dir")"
    echo "rtinfer: building $package_dir ($target)"
    build_for_target "$package_dir" "$target" "$bin_name"
  done
}

main "$@"
