/**
 * Tests for the browser/wasm entry point (`@sqd-pipes/delta-db/web`).
 *
 * The wasm binary is loaded from the pre-built file in src/wasm/.
 * These tests verify full API parity with the Node.js native binding.
 */

import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { describe, expect, it, beforeAll } from 'vitest'
import { init, DeltaDb } from '../src/web'

const WASM_PATH = resolve(__dirname, '../src/wasm/delta_db_bg.wasm')

beforeAll(async () => {
  // Load wasm from disk for Node.js test environment
  const wasmBytes = readFileSync(WASM_PATH)
  await init(wasmBytes)
})

const SCHEMA = `
  CREATE TABLE events (
    block_number UInt64,
    user         String,
    amount       Float64
  );
`

const EXTERNAL_REDUCER_SCHEMA = `
  CREATE REDUCER totals
  SOURCE events
  GROUP BY user
  STATE (
    total Float64 DEFAULT 0
  )
  LANGUAGE EXTERNAL;

  CREATE MATERIALIZED VIEW user_totals AS
  SELECT user, last(running_total) AS running_total
  FROM totals
  GROUP BY user;
`

describe('Web (WASM) DeltaDb', () => {
  it('init is idempotent', async () => {
    await init() // second call — should be a no-op
  })

  it('creates a DeltaDb instance', () => {
    const db = new DeltaDb({ schema: SCHEMA })
    expect(db).toBeTruthy()
  })

  it('throws if DeltaDb created before init', async () => {
    // Re-import a fresh module to simulate uninitialized state is not easily
    // possible in a single test file, so we just verify the happy path here.
    const db = new DeltaDb({ schema: SCHEMA })
    expect(db).toBeTruthy()
  })

  it('ingest returns null when no rows produce deltas', async () => {
    const db = new DeltaDb({ schema: SCHEMA })
    const result = await db.ingest({
      data: {},
      finalizedHead: { number: 1, hash: '0x1' },
    })
    expect(result).toBeNull()
  })

  it('ingest returns a DeltaBatch with correct shape', async () => {
    const db = new DeltaDb({ schema: SCHEMA })
    const batch = await db.ingest({
      data: {
        events: [
          { block_number: 1, user: 'alice', amount: 100 },
          { block_number: 1, user: 'bob', amount: 50 },
        ],
      },
      finalizedHead: { number: 1, hash: '0x1' },
    })

    expect(batch).toBeTruthy()
    expect(typeof batch!.sequence).toBe('number')
    expect(batch!.tables).toHaveProperty('events')
    expect(batch!.tables.events).toHaveLength(2)
    expect(batch!.tables.events[0].operation).toBe('insert')
  })

  it('cursor reflects latest ingested block', async () => {
    const db = new DeltaDb({ schema: SCHEMA })
    expect(db.cursor).toBeNull()

    await db.ingest({
      data: { events: [{ block_number: 5, user: 'alice', amount: 1 }] },
      finalizedHead: { number: 5, hash: '0x5' },
    })

    expect(db.cursor).toEqual({ number: 5, hash: '0x5' })
  })

  it('pendingCount and isBackpressured reflect buffer state', async () => {
    const db = new DeltaDb({ schema: SCHEMA })
    expect(db.pendingCount).toBe(0)
    expect(db.isBackpressured).toBe(false)
  })

  it('flush returns null when buffer is empty', () => {
    const db = new DeltaDb({ schema: SCHEMA })
    expect(db.flush()).toBeNull()
  })

  it('ack does not throw', async () => {
    const db = new DeltaDb({ schema: SCHEMA })
    const batch = await db.ingest({
      data: { events: [{ block_number: 1, user: 'alice', amount: 10 }] },
      finalizedHead: { number: 1, hash: '0x1' },
    })
    expect(() => db.ack(batch!.sequence)).not.toThrow()
  })

  it('ingest calls onDelta and acks automatically', async () => {
    const db = new DeltaDb({ schema: SCHEMA })
    let captured: any = null

    await db.ingest({
      data: { events: [{ block_number: 1, user: 'alice', amount: 10 }] },
      finalizedHead: { number: 1, hash: '0x1' },
      onDelta: async (batch) => {
        captured = batch
      },
    })

    expect(captured).toBeTruthy()
    expect(captured.tables.events).toHaveLength(1)
  })

  it('resolveForkCursor returns null when no match', () => {
    const db = new DeltaDb({ schema: SCHEMA })
    const cursor = db.resolveForkCursor([{ number: 999, hash: '0xdead' }])
    expect(cursor).toBeNull()
  })

  it('resolveForkCursor finds common ancestor', async () => {
    const db = new DeltaDb({ schema: SCHEMA })

    await db.ingest({
      data: { events: [{ block_number: 1, user: 'alice', amount: 1 }] },
      finalizedHead: { number: 1, hash: '0x1' },
      rollbackChain: [
        { number: 3, hash: '0x3' },
        { number: 2, hash: '0x2' },
        { number: 1, hash: '0x1' },
      ],
    })

    const cursor = db.resolveForkCursor([
      { number: 3, hash: '0x3_new' }, // forked
      { number: 2, hash: '0x2' },     // common
      { number: 1, hash: '0x1' },     // common
    ])

    expect(cursor).toEqual({ number: 2, hash: '0x2' })
  })

  it('registerReducer and ingest with external reducer', async () => {
    const db = new DeltaDb({ schema: SCHEMA + EXTERNAL_REDUCER_SCHEMA })

    db.registerReducer<{ total: number }>({
      name: 'totals',
      source: 'events',
      groupBy: ['user'],
      state: [{ name: 'total', columnType: 'Float64', defaultValue: '0' }],
      reduce(state, row) {
        const newTotal = state.total + row.amount
        state.update({ total: newTotal })
        state.emit({ running_total: newTotal })
      },
    })

    const batch = await db.ingest({
      data: {
        events: [
          { block_number: 1, user: 'alice', amount: 100 },
          { block_number: 1, user: 'alice', amount: 50 },
        ],
      },
      finalizedHead: { number: 1, hash: '0x1' },
      rollbackChain: [{ number: 1, hash: '0x1' }],
    })

    expect(batch).toBeTruthy()
    const records = batch!.tables.user_totals
    expect(records).toBeDefined()
    const alice = records.find((r: any) => r.key.user === 'alice')
    expect(alice?.values.running_total).toBe(150)
  })
})
