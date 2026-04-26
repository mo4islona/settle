/**
 * Three-way benchmark: Lua vs EventRules vs External (JS callback) reducers.
 *
 * All three use the same PnL workload:
 * - 100K rows, 100 users, buy/sell trades
 * - Same state: {quantity, cost_basis}
 * - Same emit: {trade_pnl, position_size}
 * - Same downstream MV: position_summary
 *
 * Run: npx tsx examples/benchmark-reducers.ts
 */

import { Settle } from '../src/index'

// ─── Schemas ─────────────────────────────────────────────────────────

const TABLE_SCHEMA = `
  CREATE TABLE trades (
    block_number UInt64,
    user         String,
    side         String,
    amount       Float64,
    price        Float64
  );
`

const MV_SCHEMA = `
  CREATE MATERIALIZED VIEW position_summary AS
  SELECT
    user,
    sum(trade_pnl)       AS total_pnl,
    last(position_size)  AS current_position,
    count()              AS trade_count
  FROM pnl
  GROUP BY user;
`

const EVENT_RULES_REDUCER = `
  CREATE REDUCER pnl
  SOURCE trades
  GROUP BY user
  STATE (
    quantity   Float64 DEFAULT 0,
    cost_basis Float64 DEFAULT 0
  )
    WHEN row.side = 'buy' THEN
      SET state.quantity = state.quantity + row.amount
      SET state.cost_basis = state.cost_basis + row.amount * row.price
      EMIT trade_pnl = 0
    WHEN row.side = 'sell' THEN
      LET avg_cost = state.cost_basis / state.quantity
      SET state.quantity = state.quantity - row.amount
      SET state.cost_basis = state.cost_basis - row.amount * avg_cost
      EMIT trade_pnl = row.amount * (row.price - avg_cost)
    ALWAYS EMIT
      state.quantity AS position_size
  END;
`

const LUA_REDUCER = `
  CREATE REDUCER pnl
  SOURCE trades
  GROUP BY user
  STATE (
    quantity   Float64 DEFAULT 0,
    cost_basis Float64 DEFAULT 0
  )
  LANGUAGE lua
  PROCESS $$
    if row.side == "buy" then
      state.quantity = state.quantity + row.amount
      state.cost_basis = state.cost_basis + row.amount * row.price
      emit.trade_pnl = 0
    else
      local avg_cost = state.cost_basis / state.quantity
      emit.trade_pnl = row.amount * (row.price - avg_cost)
      state.quantity = state.quantity - row.amount
      state.cost_basis = state.cost_basis - row.amount * avg_cost
    end
    emit.position_size = state.quantity
  $$;
`

const EXTERNAL_REDUCER_PLACEHOLDER = `
  CREATE REDUCER pnl
  SOURCE trades
  GROUP BY user
  STATE (
    quantity   Float64 DEFAULT 0,
    cost_basis Float64 DEFAULT 0
  )
  LANGUAGE EXTERNAL;
`

// ─── Data generation ─────────────────────────────────────────────────

interface Trade {
  user: string
  side: string
  amount: number
  price: number
}

function generateTrades(total: number, numUsers: number): Trade[] {
  const rows: Trade[] = []
  for (let i = 0; i < total; i++) {
    const user = `user${i % numUsers}`
    const side = Math.floor(i / numUsers) < 5 ? 'buy' : i % 3 === 0 ? 'sell' : 'buy'
    rows.push({
      user,
      side,
      amount: 1.0 + i * 0.01,
      price: 2000.0 + i * 0.1,
    })
  }
  return rows
}

// ─── Benchmark runner ────────────────────────────────────────────────

interface BenchResult {
  name: string
  totalRows: number
  elapsedMs: number
  rowsPerSec: number
}

async function runBenchmark(name: string, db: Settle, rows: Trade[], batchSize: number): Promise<BenchResult> {
  const totalRows = rows.length

  const start = performance.now()
  for (let block = 0; block * batchSize < totalRows; block++) {
    const batch = rows.slice(block * batchSize, (block + 1) * batchSize)
    await db.ingest({
      data: { trades: batch },
      finalizedHead: { number: block, hash: `0x${block}` },
    })
  }
  const elapsed = performance.now() - start

  return {
    name,
    totalRows,
    elapsedMs: elapsed,
    rowsPerSec: totalRows / (elapsed / 1000),
  }
}

function printResult(r: BenchResult) {
  const rps = r.rowsPerSec.toFixed(0).padStart(10)
  const ms = r.elapsedMs.toFixed(1)
  console.log(`  ${r.name.padEnd(45)} ${rps} rows/s  (${r.totalRows} rows in ${ms}ms)`)
}

// ─── Main ────────────────────────────────────────────────────────────

const TOTAL_ROWS = 100_000
const BATCH_SIZE = 50
const NUM_USERS = 100

async function main() {

console.log(
  `\n=== Reducer Benchmark (${TOTAL_ROWS} rows, ${NUM_USERS} users, batch=${BATCH_SIZE}) ===\n`,
)

const trades = generateTrades(TOTAL_ROWS, NUM_USERS)

// 1. Event Rules
{
  const db = Settle.open({ schema: TABLE_SCHEMA + EVENT_RULES_REDUCER + MV_SCHEMA })
  const r = await runBenchmark('Full pipeline — Event Rules', db, trades, BATCH_SIZE)
  printResult(r)
}

// 2. Lua
{
  const db = Settle.open({ schema: TABLE_SCHEMA + LUA_REDUCER + MV_SCHEMA })
  const r = await runBenchmark('Full pipeline — Lua', db, trades, BATCH_SIZE)
  printResult(r)
}

// 3. External (JS callback)
{
  const db = Settle.open({ schema: TABLE_SCHEMA + EXTERNAL_REDUCER_PLACEHOLDER + MV_SCHEMA })

  interface PnlState {
    quantity: number
    cost_basis: number
  }

  db.registerReducer<PnlState, Trade, { trade_pnl: number; position_size: number }>({
    name: 'pnl',
    source: 'trades',
    groupBy: ['user'],
    state: [
      { name: 'quantity', columnType: 'Float64', defaultValue: '0' },
      { name: 'cost_basis', columnType: 'Float64', defaultValue: '0' },
    ],
    reduce(state, row) {
      if (row.side === 'buy') {
        const newState = {
          quantity: state.quantity + row.amount,
          cost_basis: state.cost_basis + row.amount * row.price,
        }
        state.update(newState)
        state.emit({ trade_pnl: 0, position_size: newState.quantity })
      } else {
        const avgCost = state.cost_basis / state.quantity
        const newState = {
          quantity: state.quantity - row.amount,
          cost_basis: state.cost_basis - row.amount * avgCost,
        }
        state.update(newState)
        state.emit({
          trade_pnl: row.amount * (row.price - avgCost),
          position_size: newState.quantity,
        })
      }
    },
  })

  const r = await runBenchmark('Full pipeline — External (JS callback)', db, trades, BATCH_SIZE)
  printResult(r)
}

console.log()

} // end main
main()
