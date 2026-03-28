/**
 * Correctness test for external (JS callback) reducers.
 * Verifies that the External reducer produces the same results as Lua.
 */

import { describe, expect, it } from 'vitest'
import { DeltaDb } from '../src/index'

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

const EXTERNAL_REDUCER = `
  CREATE REDUCER pnl
  SOURCE trades
  GROUP BY user
  STATE (
    quantity   Float64 DEFAULT 0,
    cost_basis Float64 DEFAULT 0
  )
  LANGUAGE EXTERNAL;
`

interface PnlState {
  quantity: number
  cost_basis: number
}

function registerPnlReducer(db: DeltaDb) {
  db.registerReducer<PnlState>({
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
}

describe('External Reducer', () => {
  it('produces same MV output as Lua for PnL workload', async () => {
    // Lua version
    const luaDb = DeltaDb.open({ schema: TABLE_SCHEMA + LUA_REDUCER + MV_SCHEMA })
    const luaBatch = await luaDb.ingest({
      data: {
        trades: [
          { block_number: 1000, user: 'alice', side: 'buy', amount: 10, price: 2000 },
          { block_number: 1000, user: 'alice', side: 'buy', amount: 5, price: 2100 },
          { block_number: 1001, user: 'alice', side: 'sell', amount: 5, price: 2200 },
        ],
      },
      finalizedHead: { number: 1001, hash: '0x1001' },
      rollbackChain: [{ number: 1001, hash: '0x1001' }, { number: 1000, hash: '0x1000' }],
    })

    // External version
    const extDb = DeltaDb.open({ schema: TABLE_SCHEMA + EXTERNAL_REDUCER + MV_SCHEMA })
    registerPnlReducer(extDb)
    const extBatch = await extDb.ingest({
      data: {
        trades: [
          { block_number: 1000, user: 'alice', side: 'buy', amount: 10, price: 2000 },
          { block_number: 1000, user: 'alice', side: 'buy', amount: 5, price: 2100 },
          { block_number: 1001, user: 'alice', side: 'sell', amount: 5, price: 2200 },
        ],
      },
      finalizedHead: { number: 1001, hash: '0x1001' },
      rollbackChain: [{ number: 1001, hash: '0x1001' }, { number: 1000, hash: '0x1000' }],
    })

    // Compare MV records
    expect(luaBatch).toBeTruthy()
    expect(extBatch).toBeTruthy()
    const luaMv = luaBatch!.tables.position_summary
    const extMv = extBatch!.tables.position_summary

    expect(extMv).toHaveLength(luaMv.length)
    expect(extMv[0].values.trade_count).toBe(luaMv[0].values.trade_count)
    expect(extMv[0].values.current_position).toBeCloseTo(luaMv[0].values.current_position, 6)
    expect(extMv[0].values.total_pnl).toBeCloseTo(luaMv[0].values.total_pnl, 2)
  })

  it('handles multiple groups correctly', async () => {
    const db = DeltaDb.open({ schema: TABLE_SCHEMA + EXTERNAL_REDUCER + MV_SCHEMA })
    registerPnlReducer(db)

    const batch = await db.ingest({
      data: {
        trades: [
          { block_number: 1000, user: 'alice', side: 'buy', amount: 10, price: 2000 },
          { block_number: 1000, user: 'bob', side: 'buy', amount: 5, price: 3000 },
        ],
      },
      finalizedHead: { number: 1000, hash: '0x1000' },
      rollbackChain: [{ number: 1000, hash: '0x1000' }],
    })

    expect(batch).toBeTruthy()
    const mvRecords = batch!.tables.position_summary
    expect(mvRecords).toHaveLength(2)

    const alice = mvRecords.find((r: any) => r.key.user === 'alice')
    const bob = mvRecords.find((r: any) => r.key.user === 'bob')

    expect(alice?.values.current_position).toBe(10)
    expect(bob?.values.current_position).toBe(5)
  })

  it('supports rollback', async () => {
    const db = DeltaDb.open({ schema: TABLE_SCHEMA + EXTERNAL_REDUCER + MV_SCHEMA })
    registerPnlReducer(db)

    // Ingest blocks 1000 and 1001
    await db.ingest({
      data: {
        trades: [
          { block_number: 1000, user: 'alice', side: 'buy', amount: 10, price: 2000 },
          { block_number: 1001, user: 'alice', side: 'buy', amount: 5, price: 2100 },
        ],
      },
      finalizedHead: { number: 1000, hash: '0x1000' },
      rollbackChain: [{ number: 1001, hash: '0x1001' }, { number: 1000, hash: '0x1000' }],
    })

    // Rollback block 1001: ingest with rollbackChain that only includes block 1000
    const batch = await db.ingest({
      data: {},
      finalizedHead: { number: 1000, hash: '0x1000' },
      rollbackChain: [{ number: 1000, hash: '0x1000' }],
    })

    expect(batch).toBeTruthy()
    const mvRecords = batch!.tables.position_summary
    expect(mvRecords).toHaveLength(1)
    expect(mvRecords[0].values.current_position).toBe(10) // back to 10, not 15
  })
})
