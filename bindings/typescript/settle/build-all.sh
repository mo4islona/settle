#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG_DIR="$SCRIPT_DIR"
CARGO_CWD="../../.."

build_target() {
  local target="$1"
  local use_zigbuild="${2:-false}"

  echo "--- Building $target ---"
  rustup target add "$target" 2>/dev/null || true

  cd "$PKG_DIR"
  if [ "$use_zigbuild" = true ]; then
    npx napi build --cargo-cwd "$CARGO_CWD" --features napi --release --platform \
      --target "$target" --dts native.d.ts --js false --zig src/native
  else
    npx napi build --cargo-cwd "$CARGO_CWD" --features napi --release --platform \
      --target "$target" --dts native.d.ts --js false src/native
  fi
  echo "Done: $target"
  echo ""
}

# macOS targets (native)
build_target "aarch64-apple-darwin"
build_target "x86_64-apple-darwin"

# Linux targets (zigbuild cross-compile from macOS)
build_target "x86_64-unknown-linux-gnu" true
build_target "aarch64-unknown-linux-gnu" true

echo "=== All builds complete ==="
ls -lh "$PKG_DIR/src/native"/*.node
