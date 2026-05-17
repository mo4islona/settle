/**
 * Correctness test for external (JS callback) reducers.
 * Verifies that the External reducer produces the same results as Lua.
 */

import { describe, expect, it } from 'vitest'
import { ingestAndAck } from "./util"
import { Settle } from '../src/index'

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
      emit({trade_pnl = 0, position_size = state.quantity})
    else
      local avg_cost = state.cost_basis / state.quantity
      local pnl = row.amount * (row.price - avg_cost)
      state.quantity = state.quantity - row.amount
      state.cost_basis = state.cost_basis - row.amount * avg_cost
      emit({trade_pnl = pnl, position_size = state.quantity})
    end
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

function registerPnlReducer(db: Settle) {
  // `pnl` is declared in SQL via `LANGUAGE EXTERNAL` — attach the callback
  // through `registerReducerCallback`, not `registerReducer` (which is
  // strict and errors on a name that already exists).
  db.registerReducerCallback<PnlState>('pnl', (state, row: any) => {
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
  })
}

describe('External Reducer', () => {
  it('produces same MV output as Lua for PnL workload', async () => {
    // Lua version
    const luaDb = Settle.open({ schema: TABLE_SCHEMA + LUA_REDUCER + MV_SCHEMA })
    const luaBatch = await ingestAndAck(luaDb, {
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
    const extDb = Settle.open({ schema: TABLE_SCHEMA + EXTERNAL_REDUCER + MV_SCHEMA })
    registerPnlReducer(extDb)
    const extBatch = await ingestAndAck(extDb, {
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
    const db = Settle.open({ schema: TABLE_SCHEMA + EXTERNAL_REDUCER + MV_SCHEMA })
    registerPnlReducer(db)

    const batch = await ingestAndAck(db, {
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
    const db = Settle.open({ schema: TABLE_SCHEMA + EXTERNAL_REDUCER + MV_SCHEMA })
    registerPnlReducer(db)

    // Ingest blocks 1000 and 1001
    await ingestAndAck(db, {
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
    const batch = await ingestAndAck(db, {
      data: {},
      finalizedHead: { number: 1000, hash: '0x1000' },
      rollbackChain: [{ number: 1000, hash: '0x1000' }],
    })

    expect(batch).toBeTruthy()
    const mvRecords = batch!.tables.position_summary
    expect(mvRecords).toHaveLength(1)
    expect(mvRecords[0].values.current_position).toBe(10) // back to 10, not 15
  })

  // ─── Strict register/callback API contract ──────────────────────

  /**
   * `registerReducer` is strict: the name must not already exist
   * (neither declared in SQL nor previously registered).
   */
  it('registerReducer errors when reducer is already declared in SQL', () => {
    const db = Settle.open({
      schema: TABLE_SCHEMA + EXTERNAL_REDUCER + MV_SCHEMA,
    })
    expect(() =>
      db.registerReducer<PnlState>({
        name: 'pnl', // already declared via LANGUAGE EXTERNAL
        source: 'trades',
        groupBy: ['user'],
        state: [
          { name: 'quantity', columnType: 'Float64', defaultValue: '0' },
          { name: 'cost_basis', columnType: 'Float64', defaultValue: '0' },
        ],
        reduce: () => {},
      }),
    ).toThrow(/already exists/)
  })

  /**
   * `registerReducerCallback` is strict: a callback must not already
   * be registered for that name. Attempting to register a second
   * callback for the same reducer throws.
   */
  it('registerReducerCallback errors when callback is already registered', () => {
    const db = Settle.open({
      schema: TABLE_SCHEMA + EXTERNAL_REDUCER + MV_SCHEMA,
    })
    db.registerReducerCallback<PnlState>('pnl', () => {})
    expect(() => db.registerReducerCallback<PnlState>('pnl', () => {})).toThrow(
      /already registered/,
    )
  })

  /**
   * `registerReducerCallback` is strict: the named reducer must exist.
   */
  it('registerReducerCallback errors when reducer is not declared', () => {
    // Bare schema — no reducer at all.
    const db = Settle.open({ schema: TABLE_SCHEMA })
    expect(() => db.registerReducerCallback('unknown', () => {})).toThrow(
      /no reducer named/,
    )
  })

  // Legacy hot-reload tests removed — strict API forbids it.
  // The two tests below are kept (now expected to fail with strict errors)
  // as regression markers; they preserve the failure-shape that the new
  // API contract surfaces.
  it.skip('legacy: failed re-registerReducer preserves the original callback', async () => {
    const db = Settle.open({
      schema: TABLE_SCHEMA + EXTERNAL_REDUCER + MV_SCHEMA,
    })

    let callsToA = 0
    let callsToB = 0

    db.registerReducer<PnlState>({
      name: 'pnl',
      source: 'trades',
      groupBy: ['user'],
      state: [
        { name: 'quantity', columnType: 'Float64', defaultValue: '0' },
        { name: 'cost_basis', columnType: 'Float64', defaultValue: '0' },
      ],
      reduce(state, row: any) {
        callsToA += 1
        if (row.side === 'buy') {
          const q = state.quantity + row.amount
          const c = state.cost_basis + row.amount * row.price
          state.update({ quantity: q, cost_basis: c })
          state.emit({ trade_pnl: 0, position_size: q })
        } else {
          const avg = state.cost_basis / state.quantity
          const pnl = row.amount * (row.price - avg)
          const q = state.quantity - row.amount
          const c = state.cost_basis - row.amount * avg
          state.update({ quantity: q, cost_basis: c })
          state.emit({ trade_pnl: pnl, position_size: q })
        }
      },
    })

    // Ingest WITHOUT acking — leaves a pending batch.
    const pending = await db.ingest({
      data: {
        trades: [
          { block_number: 1, user: 'alice', side: 'buy', amount: 10, price: 100 },
        ],
      },
      finalizedHead: { number: 0, hash: '0x0' },
      rollbackChain: [{ number: 1, hash: '0x1' }],
    })
    expect(pending).toBeTruthy()
    expect(db.isAwaitingAck).toBe(true)

    // Try to re-register with a *different* callback (B). The inner call
    // returns PendingAck — registration fails. The original callback (A)
    // must be restored.
    expect(() => {
      db.registerReducer<PnlState>({
        name: 'pnl',
        source: 'trades',
        groupBy: ['user'],
        state: [
          { name: 'quantity', columnType: 'Float64', defaultValue: '0' },
          { name: 'cost_basis', columnType: 'Float64', defaultValue: '0' },
        ],
        reduce(state, _row: any) {
          callsToB += 1
          state.update({ quantity: 999, cost_basis: 0 })
          state.emit({ trade_pnl: 0, position_size: 999 })
        },
      })
    }).toThrow()

    // Ack the pending so we can continue using the instance.
    db.ack(pending!.sequence)

    // Subsequent ingest must use callback A (the original). If the bug
    // were present, callback A would have been removed by the failed
    // re-register and this ingest would either crash or call no callback.
    const before_a = callsToA
    const before_b = callsToB
    const batch = await ingestAndAck(db, {
      data: {
        trades: [
          { block_number: 2, user: 'alice', side: 'buy', amount: 5, price: 110 },
        ],
      },
      finalizedHead: { number: 1, hash: '0x1' },
      rollbackChain: [{ number: 2, hash: '0x2' }],
    })

    expect(callsToA).toBeGreaterThan(before_a)
    expect(callsToB).toBe(before_b)
    expect(batch).toBeTruthy()
    // Callback A's logic: quantity accumulates as a buy.
    const mv = batch!.tables.position_summary
    expect(mv[0].values.current_position).toBe(15) // 10 + 5
  })

  it.skip('legacy: hot-reload registerReducer swaps to the latest callback', async () => {
    const db = Settle.open({
      schema: TABLE_SCHEMA + EXTERNAL_REDUCER + MV_SCHEMA,
    })

    let active = 'A'
    const calls: string[] = []
    const makeCallback = (id: string) => (state: any, row: any) => {
      calls.push(`${id}:${row.user}`)
      active = id
      const q = state.quantity + row.amount
      state.update({ quantity: q, cost_basis: state.cost_basis + row.amount * row.price })
      state.emit({ trade_pnl: 0, position_size: q })
    }

    // Register A → ingest → ack.
    db.registerReducer<PnlState>({
      name: 'pnl',
      source: 'trades',
      groupBy: ['user'],
      state: [
        { name: 'quantity', columnType: 'Float64', defaultValue: '0' },
        { name: 'cost_basis', columnType: 'Float64', defaultValue: '0' },
      ],
      reduce: makeCallback('A'),
    })
    await ingestAndAck(db, {
      data: {
        trades: [
          { block_number: 1, user: 'alice', side: 'buy', amount: 1, price: 100 },
        ],
      },
      finalizedHead: { number: 1, hash: '0x1' },
      rollbackChain: [{ number: 1, hash: '0x1' }],
    })
    expect(active).toBe('A')

    // Re-register the SAME reducer with callback B. Hot-reload must
    // succeed (no pending) and from now on B is what runs.
    db.registerReducer<PnlState>({
      name: 'pnl',
      source: 'trades',
      groupBy: ['user'],
      state: [
        { name: 'quantity', columnType: 'Float64', defaultValue: '0' },
        { name: 'cost_basis', columnType: 'Float64', defaultValue: '0' },
      ],
      reduce: makeCallback('B'),
    })
    await ingestAndAck(db, {
      data: {
        trades: [
          { block_number: 2, user: 'alice', side: 'buy', amount: 1, price: 100 },
        ],
      },
      finalizedHead: { number: 2, hash: '0x2' },
      rollbackChain: [{ number: 2, hash: '0x2' }],
    })
    expect(active).toBe('B')
    expect(calls.filter((c) => c.startsWith('B:')).length).toBeGreaterThan(0)
  })
})
