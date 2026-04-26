/**
 * Polymarket SettleStream — standalone example
 *
 * Demonstrates the full pipeline: feeding raw orders into settle-stream,
 * processing through reducers (market_stats, insider_classifier) and
 * materialized views (token_summary, insider_positions), then decoding
 * the resulting change batches.
 *
 * Run with: npx tsx examples/polymarket-pipe.example.ts
 */

import { type ChangeBatch, SettleStream } from '../src/index'

// ── Types ──────────────────────────────────────────────────────────

enum SIDE {
  BUY = 0,
  SELL = 1,
}

interface ParsedOrder {
  blockNumber: number
  trader: string
  assetId: string
  usdc: number
  shares: number
  side: SIDE
  timestamp: number
}

// ── Schema ─────────────────────────────────────────────────────────

const SCHEMA = `
CREATE VIRTUAL TABLE orders (
    block_number UInt64,
    timestamp    UInt64,
    trader       String,
    asset_id     String,
    usdc         UInt64,
    shares       UInt64,
    side         UInt64
);

CREATE REDUCER market_stats
SOURCE orders
GROUP BY asset_id
STATE (
    volume      Float64 DEFAULT 0,
    trades      UInt64  DEFAULT 0,
    sum_price   Float64 DEFAULT 0,
    sum_price_sq Float64 DEFAULT 0,
    first_seen  UInt64  DEFAULT 0,
    last_seen   UInt64  DEFAULT 0
)
LANGUAGE lua
PROCESS $$
    if row.shares == 0 then return end

    local price = row.usdc / row.shares
    local vol = row.usdc / 1000000

    state.volume = state.volume + vol
    state.trades = state.trades + 1
    state.sum_price = state.sum_price + price
    state.sum_price_sq = state.sum_price_sq + price * price
    if state.first_seen == 0 then state.first_seen = row.timestamp end
    state.last_seen = row.timestamp

    emit.asset_id = row.asset_id
    emit.volume = vol
    emit.price = price
    emit.price_sq = price * price
$$;

CREATE MATERIALIZED VIEW token_summary AS
SELECT
    asset_id,
    sum(volume)    AS total_volume,
    count()        AS trade_count,
    last(price)    AS last_price,
    sum(price)     AS sum_price,
    sum(price_sq)  AS sum_price_sq
FROM market_stats
GROUP BY asset_id;

CREATE REDUCER insider_classifier
SOURCE orders
GROUP BY trader
STATE (
    status       String  DEFAULT 'unknown',
    window_start UInt64  DEFAULT 0,
    window_vol   UInt64  DEFAULT 0,
    window_trades UInt64 DEFAULT 0,
    positions    Json    DEFAULT '{}'
)
LANGUAGE lua
PROCESS $$
    if row.shares == 0 then return end

    local FIFTEEN_MIN = 900
    local VOLUME_THRESHOLD = 4000000000
    local MIN_PRICE_BPS = 9500
    local BPS_SCALE = 10000

    if row.side ~= 0 then return end
    if row.usdc * BPS_SCALE >= row.shares * MIN_PRICE_BPS then return end

    if state.status ~= "unknown" then
        if state.status == "insider" then
            local price = row.usdc / row.shares
            emit {
                trader = row.trader,
                asset_id = row.asset_id,
                volume = row.usdc / 1000000,
                price = price,
                price_sq = price * price,
                timestamp = row.timestamp,
                detected_at = row.timestamp
            }
        end
        return
    end

    if state.window_start == 0 then
        state.window_start = row.timestamp
    elseif row.timestamp - state.window_start > FIFTEEN_MIN then
        state.status = "clean"
        return
    end

    state.window_vol = state.window_vol + row.usdc
    state.window_trades = state.window_trades + 1

    local token = row.asset_id
    local price = row.usdc / row.shares
    local vol = row.usdc / 1000000
    local pos = state.positions[token]
    if not pos then
        pos = { volume = 0, trades = 0, sum_price = 0, sum_price_sq = 0,
                first_seen = row.timestamp, last_seen = row.timestamp }
    end
    pos.volume = pos.volume + vol
    pos.trades = pos.trades + 1
    pos.sum_price = pos.sum_price + price
    pos.sum_price_sq = pos.sum_price_sq + price * price
    if row.timestamp < pos.first_seen then pos.first_seen = row.timestamp end
    if row.timestamp > pos.last_seen then pos.last_seen = row.timestamp end
    state.positions[token] = pos

    if state.window_vol >= VOLUME_THRESHOLD then
        state.status = "insider"
        for tid, p in pairs(state.positions) do
            emit {
                trader = row.trader,
                asset_id = tid,
                volume = p.volume,
                price = p.sum_price / p.trades,
                price_sq = p.sum_price_sq / p.trades,
                timestamp = p.first_seen,
                detected_at = row.timestamp
            }
        end
    end
$$;

CREATE MATERIALIZED VIEW insider_positions AS
SELECT
    trader,
    asset_id,
    sum(volume)      AS total_volume,
    count()          AS trade_count,
    sum(price)       AS sum_price,
    sum(price_sq)    AS sum_price_sq,
    first(timestamp) AS first_seen,
    last(timestamp)  AS last_seen,
    first(detected_at) AS detected_at
FROM insider_classifier
GROUP BY trader, asset_id;
`

// ── Transform: ParsedOrder[] → schema rows ─────────────────────────

function transformOrders(orders: ParsedOrder[]): Record<string, any>[] {
  return orders.map((order) => ({
    block_number: order.blockNumber,
    timestamp: order.timestamp,
    trader: order.trader,
    asset_id: String(order.assetId),
    usdc: order.usdc,
    shares: order.shares,
    side: order.side,
  }))
}

// ── Change batch decoder ────────────────────────────────────────────

interface TokenSummary {
  assetId: string
  totalVolume: number
  tradeCount: number
  lastPrice: number
  meanPrice: number
  stdDev: number
}

interface InsiderPosition {
  trader: string
  assetId: string
  totalVolume: number
  tradeCount: number
  avgPrice: number
  firstSeen: number
  lastSeen: number
  detectedAt: number
}

interface DecodedChanges {
  tokenSummaries: TokenSummary[]
  insiderPositions: InsiderPosition[]
  rawOrderCount: number
}

function decodeBatch(batch: ChangeBatch): DecodedChanges {
  const tokenSummaries: TokenSummary[] = []
  const insiderPositions: InsiderPosition[] = []
  let rawOrderCount = 0

  for (const record of batch.tables.token_summary ?? []) {
    if (record.operation === 'delete') continue
    const row = { ...record.key, ...record.values }

    const tradeCount = row.trade_count as number
    const sumPrice = row.sum_price as number
    const sumPriceSq = row.sum_price_sq as number
    const mean = tradeCount > 0 ? sumPrice / tradeCount : 0
    const variance = tradeCount > 0 ? sumPriceSq / tradeCount - mean * mean : 0

    tokenSummaries.push({
      assetId: row.asset_id as string,
      totalVolume: row.total_volume as number,
      tradeCount,
      lastPrice: row.last_price as number,
      meanPrice: mean,
      stdDev: Math.sqrt(Math.max(0, variance)),
    })
  }

  for (const record of batch.tables.insider_positions ?? []) {
    if (record.operation === 'delete') continue
    const row = { ...record.key, ...record.values }

    const tc = row.trade_count as number
    insiderPositions.push({
      trader: row.trader as string,
      assetId: row.asset_id as string,
      totalVolume: row.total_volume as number,
      tradeCount: tc,
      avgPrice: tc > 0 ? (row.sum_price as number) / tc : 0,
      firstSeen: row.first_seen as number,
      lastSeen: row.last_seen as number,
      detectedAt: row.detected_at as number,
    })
  }

  rawOrderCount = (batch.tables.orders ?? []).length

  return { tokenSummaries, insiderPositions, rawOrderCount }
}

// ── Sample data generator ──────────────────────────────────────────

function generateOrders(blockNumber: number, timestamp: number, count: number): ParsedOrder[] {
  const orders: ParsedOrder[] = []
  for (let i = 0; i < count; i++) {
    const traderIdx = (blockNumber * count + i) % 50
    const tokenIdx = i % 5
    const isBuy = i % 3 !== 2
    // Low price (< 0.95) for insider detection eligibility
    const priceBps = 3000 + (i % 6000)
    const shares = 1_000_000_000
    const usdc = Math.floor((shares * priceBps) / 10_000)

    orders.push({
      blockNumber,
      trader: `0xtrader_${traderIdx.toString(16).padStart(4, '0')}`,
      assetId: `token_${tokenIdx.toString().padStart(4, '0')}`,
      usdc,
      shares,
      side: isBuy ? SIDE.BUY : SIDE.SELL,
      timestamp,
    })
  }
  return orders
}

// ── Main ───────────────────────────────────────────────────────────

function main() {
  // Open settle-stream with in-memory storage (no dataDir = no persistence)
  const db = SettleStream.open({ schema: SCHEMA })

  console.log('=== Polymarket SettleStream Example ===\n')

  // Process 10 blocks, 20 orders each
  const numBlocks = 10
  const ordersPerBlock = 20
  const allBatches: ChangeBatch[] = []

  for (let i = 0; i < numBlocks; i++) {
    const blockNumber = 1000 + i
    const timestamp = 1_700_000_000 + i * 12 // ~12s per block

    const orders = generateOrders(blockNumber, timestamp, ordersPerBlock)
    const rows = transformOrders(orders)

    // Feed rows into settle-stream
    db.processBatch('orders', blockNumber, rows)

    // Finalize older blocks (keep last 3 unfinalized for rollback)
    if (blockNumber > 1002) {
      db.finalize(blockNumber - 3)
    }
  }

  // Flush all pending changes
  const batch = db.flush()
  if (batch) {
    allBatches.push(batch)
    db.ack(batch.sequence)
  }

  // Decode and display results
  console.log(
    `Processed ${numBlocks} blocks × ${ordersPerBlock} orders = ${numBlocks * ordersPerBlock} total orders\n`,
  )

  if (batch) {
    const totalRecords = Object.values(batch.tables).reduce((s, r) => s + r.length, 0)
    console.log(`Change batch: sequence=${batch.sequence}, ${totalRecords} records`)

    // Count records by table and operation
    console.log('\nRecords by table:')
    for (const [table, records] of Object.entries(batch.tables)) {
      const counts: Record<string, number> = {}
      for (const r of records) {
        counts[r.operation] = (counts[r.operation] ?? 0) + 1
      }
      const parts = Object.entries(counts)
        .map(([op, n]) => `${op}=${n}`)
        .join(', ')
      console.log(`  ${table}: ${parts}`)
    }

    // Decode into typed structures
    const decoded = decodeBatch(batch)

    console.log(`\n--- Token Summaries (${decoded.tokenSummaries.length}) ---`)
    for (const ts of decoded.tokenSummaries) {
      console.log(
        `  ${ts.assetId}: vol=${ts.totalVolume.toFixed(2)}, ` +
          `trades=${ts.tradeCount}, last=${ts.lastPrice.toFixed(4)}, ` +
          `mean=${ts.meanPrice.toFixed(4)}, std=${ts.stdDev.toFixed(4)}`,
      )
    }

    if (decoded.insiderPositions.length > 0) {
      console.log(`\n--- Insider Positions (${decoded.insiderPositions.length}) ---`)
      for (const ip of decoded.insiderPositions) {
        console.log(
          `  ${ip.trader} → ${ip.assetId}: vol=${ip.totalVolume.toFixed(2)}, ` +
            `trades=${ip.tradeCount}, avg_price=${ip.avgPrice.toFixed(4)}, ` +
            `detected_at=${ip.detectedAt}`,
        )
      }
    } else {
      console.log('\n--- No insiders detected ---')
    }
  }

  // Demonstrate rollback
  console.log('\n=== Rollback Demo ===\n')

  // Process one more block
  const extraOrders = generateOrders(1010, 1_700_000_120, 10)
  db.processBatch('orders', 1010, transformOrders(extraOrders))

  const beforeRollback = db.flush()
  if (beforeRollback) {
    const count = Object.values(beforeRollback.tables).reduce((s, r) => s + r.length, 0)
    console.log(`Before rollback: ${count} new records`)
    db.ack(beforeRollback.sequence)
  }

  // Rollback to block 1008 (undoes blocks 1009 and 1010)
  db.rollback(1008)
  const rollbackBatch = db.flush()
  if (rollbackBatch) {
    const allRecords = Object.values(rollbackBatch.tables).flat()
    const deletes = allRecords.filter((r) => r.operation === 'delete')
    const updates = allRecords.filter((r) => r.operation === 'update')
    console.log(
      `After rollback to 1008: ${allRecords.length} compensating changes ` +
        `(${deletes.length} deletes, ${updates.length} updates)`,
    )
    db.ack(rollbackBatch.sequence)

    // Show prevValues on updates (allows downstream to diff)
    const updatesWithPrev = updates.filter((r) => r.prevValues != null)
    if (updatesWithPrev.length > 0) {
      console.log('\nSample compensating update (with prevValues):')
      const sample = updatesWithPrev[0]
      console.log(`  table: ${sample.table}`)
      console.log(`  key:`, sample.key)
      console.log(`  values:`, sample.values)
      console.log(`  prevValues:`, sample.prevValues)
    }
  }

  // Re-ingest corrected data after rollback
  const correctedOrders = generateOrders(1009, 1_700_000_108, 5)
  db.processBatch('orders', 1009, transformOrders(correctedOrders))

  const afterFix = db.flush()
  if (afterFix) {
    const decoded = decodeBatch(afterFix)
    console.log(
      `\nAfter re-ingest: ${decoded.tokenSummaries.length} token summaries, ` +
        `${decoded.insiderPositions.length} insider positions`,
    )
    db.ack(afterFix.sequence)
  }

  console.log('\n=== Done ===')
}

main()
