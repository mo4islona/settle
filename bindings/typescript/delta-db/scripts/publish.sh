#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
CARGO_CWD="../../.."

VERSION=$(node -p "require('./package.json').version")
TAG="alpha"
PUBLISHED=0
SKIPPED=0

echo "=== @sqd-pipes/delta-db v${VERSION} ==="
echo ""

# ─── Platform → Rust target mapping ────────────────────────────
PLATFORMS="darwin-arm64 darwin-x64 linux-x64-gnu linux-arm64-gnu"

rust_target() {
  case "$1" in
    darwin-arm64)    echo "aarch64-apple-darwin" ;;
    darwin-x64)      echo "x86_64-apple-darwin" ;;
    linux-x64-gnu)   echo "x86_64-unknown-linux-gnu" ;;
    linux-arm64-gnu) echo "aarch64-unknown-linux-gnu" ;;
  esac
}

# ─── 1. Build native for ALL platforms ──────────────────────────
echo "--- Build native (all platforms) ---"
for platform in $PLATFORMS; do
  target=$(rust_target "$platform")
  echo -n "  ${platform} (${target}) ... "
  npx napi build \
    --cargo-cwd "$CARGO_CWD" \
    --features napi \
    --release \
    --platform \
    --target "$target" \
    --dts native.d.ts \
    --js false \
    src/native \
    2>&1 | tail -1
done
echo ""

# ─── 2. Build wasm ──────────────────────────────────────────────
echo "--- Build wasm ---"
pnpm run build:wasm
echo ""

# ─── 3. Build TypeScript ────────────────────────────────────────
echo "--- Build TypeScript ---"
pnpm run build:ts
echo ""

# ─── 4. Test ────────────────────────────────────────────────────
echo "--- Test ---"
pnpm run test
echo ""

# ─── 5. Copy binaries → npm packages ───────────────────────────
echo "--- Prepare packages ---"
for platform in $PLATFORMS; do
  src="src/native/delta-db.${platform}.node"
  if [ -f "$src" ]; then
    cp "$src" "npm/${platform}/"
    echo "  ✓ ${platform} ($(du -h "$src" | cut -f1))"
  else
    echo "  ✗ ${platform} (build failed)"
  fi
done

if [ -f "src/wasm/delta_db_bg.wasm" ]; then
  cp src/wasm/delta_db.js npm/wasm/delta-db.js
  cp src/wasm/delta_db.d.ts npm/wasm/delta-db.d.ts
  cp src/wasm/delta_db_bg.wasm npm/wasm/delta-db_bg.wasm
  echo "  ✓ wasm ($(du -h src/wasm/delta_db_bg.wasm | cut -f1))"
fi
echo ""

# ─── 6. Publish ─────────────────────────────────────────────────
echo "--- Publish ---"
for platform in $PLATFORMS; do
  if [ -f "npm/${platform}/delta-db.${platform}.node" ]; then
    echo "  @sqd-pipes/delta-db-${platform}"
    (cd "npm/${platform}" && npm publish --access public --tag "$TAG") && PUBLISHED=$((PUBLISHED + 1))
    echo ""
  else
    echo "  ✗ @sqd-pipes/delta-db-${platform} (no binary)"
    SKIPPED=$((SKIPPED + 1))
  fi
done

if [ -f "npm/wasm/delta-db_bg.wasm" ]; then
  echo "  @sqd-pipes/delta-db-wasm"
  (cd npm/wasm && npm publish --access public --tag "$TAG") && PUBLISHED=$((PUBLISHED + 1))
  echo ""
fi

echo "  @sqd-pipes/delta-db"
npm publish --access public --tag "$TAG" && PUBLISHED=$((PUBLISHED + 1))

echo ""
echo "=== Done: ${PUBLISHED} published, ${SKIPPED} skipped ==="
