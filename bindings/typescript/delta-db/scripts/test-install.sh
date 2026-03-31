#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "=== Test: simulate npm install + require (like a real consumer) ==="
echo ""

# 1. Build TS to ensure dist/ is fresh
echo "--- Building TypeScript ---"
pnpm run build:ts
echo ""

# 2. Pack main + platform packages
echo "--- Packing ---"
PLATFORM="${OSTYPE}-$(uname -m)"
case "$PLATFORM" in
  darwin*-arm64)  SUFFIX="darwin-arm64" ;;
  darwin*-x86_64) SUFFIX="darwin-x64" ;;
  linux*-x86_64)  SUFFIX="linux-x64-gnu" ;;
  linux*-aarch64) SUFFIX="linux-arm64-gnu" ;;
  *) echo "Unknown platform: $PLATFORM"; exit 1 ;;
esac

NODE_FILE="src/native/delta-db.${SUFFIX}.node"
if [ ! -f "$NODE_FILE" ]; then
  echo "Native binary not found: $NODE_FILE"
  echo "Run 'pnpm run build:native' first"
  exit 1
fi
cp "$NODE_FILE" "npm/${SUFFIX}/"

MAIN_TGZ=$(npm pack --pack-destination /tmp 2>/dev/null | tail -1)
PLATFORM_TGZ=$(cd "npm/${SUFFIX}" && npm pack --pack-destination /tmp 2>/dev/null | tail -1)
echo "  main:     /tmp/${MAIN_TGZ}"
  echo "  platform: /tmp/${PLATFORM_TGZ}"
echo ""

# 3. Install in temp project
TMPDIR=$(mktemp -d)
echo "--- Installing in ${TMPDIR} ---"
cd "$TMPDIR"
npm init -y --silent > /dev/null 2>&1
npm install "/tmp/${MAIN_TGZ}" "/tmp/${PLATFORM_TGZ}" --no-save 2>&1 | grep -E "added|npm warn" || true
echo ""

# 4. Test: load via dist/ entry point (how real consumers load it)
echo "--- Test: require via dist/ entry point ---"
node -e "
  // This is how a real consumer loads delta-db:
  // import { DeltaDb } from '@sqd-pipes/delta-db'
  // which resolves to dist/index.js → dist/delta-db.js → native loader
  const { DeltaDb } = require('@sqd-pipes/delta-db');
  console.log('  ✓ require(@sqd-pipes/delta-db) works');
  console.log('  ✓ DeltaDb:', typeof DeltaDb);
"
echo ""

# 5. Test: DeltaDb.open() works
echo "--- Test: DeltaDb.open() ---"
node -e "
  const { DeltaDb } = require('@sqd-pipes/delta-db');
  const db = DeltaDb.open({ schema: 'CREATE TABLE t (block_number UInt64, x Float64);' });
  console.log('  ✓ DeltaDb.open() works');
"
echo ""

# 6. Test: ingest works
echo "--- Test: ingest() ---"
node -e "
  const { DeltaDb } = require('@sqd-pipes/delta-db');
  const db = DeltaDb.open({ schema: 'CREATE TABLE t (block_number UInt64, x Float64);' });
  const batch = db.ingest({
    data: { t: [{ block_number: 1, x: 42.0 }] },
    finalizedHead: { number: 1, hash: '0x1' },
  });
  console.log('  ✓ ingest() returned:', batch ? 'batch' : 'null');
"
echo ""

# 7. Same test with pnpm (strict node_modules)
echo "--- Test: pnpm install + require ---"
TMPDIR2=$(mktemp -d)
cd "$TMPDIR2"
npm init -y --silent > /dev/null 2>&1
pnpm install "/tmp/${MAIN_TGZ}" "/tmp/${PLATFORM_TGZ}" --no-lockfile 2>&1 | grep -E "done|packages" || true

node -e "
  const { DeltaDb } = require('@sqd-pipes/delta-db');
  const db = DeltaDb.open({ schema: 'CREATE TABLE t (block_number UInt64, x Float64);' });
  const batch = db.ingest({
    data: { t: [{ block_number: 1, x: 42.0 }] },
    finalizedHead: { number: 1, hash: '0x1' },
  });
  console.log('  ✓ pnpm: require + open + ingest works');
"
echo ""

# 8. Cleanup
rm -rf "$TMPDIR" "$TMPDIR2"
rm -f "/tmp/${MAIN_TGZ}" "/tmp/${PLATFORM_TGZ}"

echo "=== All checks passed ==="
