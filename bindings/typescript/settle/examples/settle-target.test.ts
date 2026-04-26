import { describe, expect, it } from 'vitest'

import {
  type ChangeBatch,
  Settle,
  type SettleCursor,
  type ChangeRecord,
  settleTarget,
} from '../examples/settle-target'

// ─── Helpers ───────────────────────────────────────────────────────

function mockRead(
  blocks: Array<{
    blockNumber: number
    data: Record<string, Record<string, unknown>[]>
    finalized?: number
  }>,
) {
  return function read() {
    const seenBlocks: Array<SettleCursor> = []

    return (async function* () {
      for (const block of blocks) {
        const finalized = block.finalized ?? block.blockNumber
        const hash = `0x${block.blockNumber.toString(16)}`

        seenBlocks.push({ number: block.blockNumber, hash })
        const rollbackChain = seenBlocks.filter((b) => b.number > finalized)

        yield {
          data: block.data,
          ctx: {
            head: {
              finalized: { number: finalized, hash: `0x${finalized.toString(16)}` },
            },
            state: {
              current: { number: block.blockNumber, hash },
              rollbackChain,
            },
          },
        }
      }
    })()
  }
}

function allRecords(batch: ChangeBatch): ChangeRecord[] {
  return Object.values(batch.tables).flat()
}

function findRecords(batch: ChangeBatch, table: string, op?: string): ChangeRecord[] {
  const records = batch.tables[table] ?? []
  return op ? records.filter((r) => r.operation === op) : records
}

// ─── Schema Definitions ────────────────────────────────────────────

const SIMPLE_SCHEMA = `
  CREATE TABLE transfers (
    block_number UInt64,
    tx_index     UInt64,
    from_addr    String,
    to_addr      String,
    value        Float64
  );
`

// Reducer + downstream MV so reducer output is visible
const REDUCER_SCHEMA = `
  CREATE TABLE trades (
    block_number UInt64,
    user         String,
    side         String,
    amount       Float64,
    price        Float64
  );

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

  CREATE MATERIALIZED VIEW position_summary AS
    SELECT
      user,
      sum(trade_pnl) AS total_pnl,
      last(position_size) AS current_position,
      count() AS trade_count
    FROM pnl
    GROUP BY user;
`

const MV_SCHEMA = `
  CREATE TABLE swaps (
    block_number UInt64,
    pool         String,
    amount       Float64
  );

  CREATE MATERIALIZED VIEW volume_by_pool AS
    SELECT
      pool,
      sum(amount) AS total_volume,
      count() AS swap_count
    FROM swaps
    GROUP BY pool;
`

// ─── Low-level Settle Tests ──────────────────────────────────────

describe('Settle', () => {
  it('should open with a valid schema', () => {
    const db = Settle.open({ schema: SIMPLE_SCHEMA })
    expect(db).toBeDefined()
    expect(db.cursor).toBeNull()
    expect(db.pendingCount).toBe(0)
  })

  it('should reject invalid schema', () => {
    expect(() => Settle.open({ schema: 'NOT VALID SQL' })).toThrow()
  })

  it('should process a batch and flush changes', () => {
    const db = Settle.open({ schema: SIMPLE_SCHEMA })

    db.processBatch('transfers', 100, [
      { block_number: 100, tx_index: 0, from_addr: 'alice', to_addr: 'bob', value: 10.5 },
      { block_number: 100, tx_index: 1, from_addr: 'bob', to_addr: 'carol', value: 5.0 },
    ])

    expect(db.cursor).toEqual({ number: 100, hash: '' })
    expect(db.pendingCount).toBeGreaterThan(0)

    const batch = db.flush()
    expect(batch).not.toBeNull()
    expect(allRecords(batch!).length).toBe(2)

    for (const record of allRecords(batch!)) {
      expect(record.table).toBe('transfers')
      expect(record.operation).toBe('insert')
    }

    expect(db.pendingCount).toBe(0)
    expect(db.flush()).toBeNull()
  })

  it('should handle multiple blocks', () => {
    const db = Settle.open({ schema: SIMPLE_SCHEMA })

    db.processBatch('transfers', 100, [
      { block_number: 100, tx_index: 0, from_addr: 'alice', to_addr: 'bob', value: 10 },
    ])
    db.processBatch('transfers', 101, [
      { block_number: 101, tx_index: 0, from_addr: 'bob', to_addr: 'carol', value: 5 },
    ])

    const batch = db.flush()
    expect(allRecords(batch!).length).toBe(2)
  })

  it('should rollback and produce compensating changes', () => {
    const db = Settle.open({ schema: SIMPLE_SCHEMA })

    db.processBatch('transfers', 100, [
      { block_number: 100, tx_index: 0, from_addr: 'alice', to_addr: 'bob', value: 10 },
    ])
    db.processBatch('transfers', 101, [
      { block_number: 101, tx_index: 0, from_addr: 'bob', to_addr: 'carol', value: 5 },
    ])

    const batch1 = db.flush()
    db.ack(batch1!.sequence)

    db.rollback(100)

    const rollbackBatch = db.flush()
    expect(rollbackBatch).not.toBeNull()

    const deletes = allRecords(rollbackBatch!).filter((r) => r.operation === 'delete')
    expect(deletes.length).toBeGreaterThan(0)
    expect(deletes[0].table).toBe('transfers')
  })

  it('should finalize blocks', () => {
    const db = Settle.open({ schema: SIMPLE_SCHEMA })

    db.processBatch('transfers', 100, [
      { block_number: 100, tx_index: 0, from_addr: 'alice', to_addr: 'bob', value: 10 },
    ])
    db.processBatch('transfers', 101, [
      { block_number: 101, tx_index: 0, from_addr: 'bob', to_addr: 'carol', value: 5 },
    ])

    db.finalize(100)

    // Rollback to 100 should still work (it's the finalized point)
    db.rollback(100)
  })

  it('should track backpressure', () => {
    const db = Settle.open({ schema: SIMPLE_SCHEMA, maxBufferSize: 2 })

    expect(db.isBackpressured).toBe(false)

    db.processBatch('transfers', 100, [
      { block_number: 100, tx_index: 0, from_addr: 'a', to_addr: 'b', value: 1 },
      { block_number: 100, tx_index: 1, from_addr: 'b', to_addr: 'c', value: 2 },
      { block_number: 100, tx_index: 2, from_addr: 'c', to_addr: 'd', value: 3 },
    ])

    expect(db.isBackpressured).toBe(true)

    const batch = db.flush()
    db.ack(batch!.sequence)
    expect(db.isBackpressured).toBe(false)
  })
})

// ─── Ingest + Fork Resolution Tests ──────────────────────────────

describe('Settle ingest', () => {
  it('should ingest data and return batch with cursor', async () => {
    const db = Settle.open({ schema: SIMPLE_SCHEMA })

    const batch = await db.ingest({
      data: {
        transfers: [
          { block_number: 100, tx_index: 0, from_addr: 'alice', to_addr: 'bob', value: 10 },
        ],
      },
      rollbackChain: [{ number: 100, hash: '0x64' }],
      finalizedHead: { number: 99, hash: '0x63' },
    })

    expect(batch).not.toBeNull()
    expect(allRecords(batch!).length).toBe(1)
    expect(batch!.latestHead?.number).toBe(100)
    expect(batch!.latestHead?.hash).toBe('0x64')
    expect(batch!.finalizedHead?.number).toBe(99)

    expect(db.cursor?.number).toBe(100)
    expect(db.cursor?.hash).toBe('0x64')
  })

  it('should resolve fork cursor from stored hashes', async () => {
    const db = Settle.open({ schema: SIMPLE_SCHEMA })

    await db.ingest({
      data: {
        transfers: [
          { block_number: 100, tx_index: 0, from_addr: 'a', to_addr: 'b', value: 1 },
          { block_number: 101, tx_index: 0, from_addr: 'b', to_addr: 'c', value: 2 },
        ],
      },
      rollbackChain: [
        { number: 100, hash: '0x64' },
        { number: 101, hash: '0x65' },
      ],
      finalizedHead: { number: 99, hash: '0x63' },
    })

    // Block 100 matches
    const cursor = db.resolveForkCursor([
      { number: 101, hash: '0xdifferent' },
      { number: 100, hash: '0x64' },
    ])
    expect(cursor).toEqual({ number: 100, hash: '0x64' })

    // No match
    const none = db.resolveForkCursor([
      { number: 101, hash: '0xnope' },
      { number: 100, hash: '0xnope' },
    ])
    expect(none).toBeNull()
  })
})

// ─── Reducer + MV Pipeline Tests ───────────────────────────────────

describe('Settle with reducer pipeline', () => {
  it('should produce changes for raw table and downstream MV', () => {
    const db = Settle.open({ schema: REDUCER_SCHEMA })

    db.processBatch('trades', 100, [
      { block_number: 100, user: 'alice', side: 'buy', amount: 10, price: 2000 },
    ])

    const batch = db.flush()
    expect(batch).not.toBeNull()

    const tradeRecords = findRecords(batch!, 'trades')
    const mvRecords = findRecords(batch!, 'position_summary')

    expect(tradeRecords.length).toBe(1)
    expect(tradeRecords[0].operation).toBe('insert')

    expect(mvRecords.length).toBe(1)
    expect(mvRecords[0].operation).toBe('insert')

    const mvValues = { ...mvRecords[0].key, ...mvRecords[0].values } as Record<string, unknown>
    expect(mvValues.trade_count).toBe(1)
    expect(mvValues.current_position).toBe(10)
    expect(mvValues.total_pnl).toBe(0)
  })

  it('should update MV state across blocks (buy then sell)', () => {
    const db = Settle.open({ schema: REDUCER_SCHEMA })

    db.processBatch('trades', 100, [
      { block_number: 100, user: 'alice', side: 'buy', amount: 10, price: 2000 },
    ])
    db.processBatch('trades', 101, [
      { block_number: 101, user: 'alice', side: 'sell', amount: 5, price: 2200 },
    ])

    const batch = db.flush()
    const mvRecords = findRecords(batch!, 'position_summary')

    expect(mvRecords.length).toBe(1)
    const values = { ...mvRecords[0].key, ...mvRecords[0].values } as Record<string, unknown>
    expect(values.trade_count).toBe(2)
    expect(values.current_position).toBe(5) // 10 - 5
    // PnL: sell 5 @ 2200, avg cost 2000 → 5*(2200-2000) = 1000
    expect(Math.abs((values.total_pnl as number) - 1000)).toBeLessThan(0.01)
  })

  it('should rollback reducer state correctly', () => {
    const db = Settle.open({ schema: REDUCER_SCHEMA })

    db.processBatch('trades', 100, [
      { block_number: 100, user: 'alice', side: 'buy', amount: 10, price: 2000 },
    ])
    db.processBatch('trades', 101, [
      { block_number: 101, user: 'alice', side: 'buy', amount: 5, price: 2100 },
    ])
    db.processBatch('trades', 102, [
      { block_number: 102, user: 'alice', side: 'sell', amount: 8, price: 2200 },
    ])
    db.flush()

    // Rollback block 102 (the sell)
    db.rollback(101)
    db.flush()

    // Re-ingest with different sell
    db.processBatch('trades', 102, [
      { block_number: 102, user: 'alice', side: 'sell', amount: 3, price: 2300 },
    ])

    const batch = db.flush()
    expect(batch).not.toBeNull()

    const mvRecords = findRecords(batch!, 'position_summary')
    expect(mvRecords.length).toBe(1)

    const values = { ...mvRecords[0].key, ...mvRecords[0].values } as Record<string, unknown>
    expect(values.trade_count).toBe(3)
    expect(values.current_position).toBe(12) // 10 + 5 - 3
  })
})

// ─── Materialized View Tests ───────────────────────────────────────

describe('Settle with materialized view', () => {
  it('should produce MV changes from raw table inserts', () => {
    const db = Settle.open({ schema: MV_SCHEMA })

    db.processBatch('swaps', 100, [
      { block_number: 100, pool: 'ETH/USDC', amount: 1000 },
      { block_number: 100, pool: 'ETH/USDC', amount: 500 },
      { block_number: 100, pool: 'BTC/USDC', amount: 2000 },
    ])

    const batch = db.flush()
    expect(batch).not.toBeNull()

    const mvRecords = findRecords(batch!, 'volume_by_pool')
    expect(mvRecords.length).toBeGreaterThan(0)

    const ethRecord = mvRecords.find(
      (r) => (r.values as any).pool === 'ETH/USDC' || (r.key as any).pool === 'ETH/USDC',
    )
    expect(ethRecord).toBeDefined()

    const ethValues = { ...ethRecord!.key, ...ethRecord!.values } as Record<string, unknown>
    expect(ethValues.total_volume).toBe(1500)
    expect(ethValues.swap_count).toBe(2)
  })

  it('should update MV on new block', () => {
    const db = Settle.open({ schema: MV_SCHEMA })

    db.processBatch('swaps', 100, [{ block_number: 100, pool: 'ETH/USDC', amount: 1000 }])
    const batch1 = db.flush()
    db.ack(batch1!.sequence)

    db.processBatch('swaps', 101, [{ block_number: 101, pool: 'ETH/USDC', amount: 500 }])

    const batch2 = db.flush()
    const mvUpdates = findRecords(batch2!, 'volume_by_pool', 'update')

    expect(mvUpdates.length).toBeGreaterThan(0)
    const values = { ...mvUpdates[0].key, ...mvUpdates[0].values } as Record<string, unknown>
    expect(values.total_volume).toBe(1500)
    expect(values.swap_count).toBe(2)
  })
})

// ─── settleTarget (Pipes SDK integration) Tests ───────────────────

describe('settleTarget', () => {
  it('should process blocks and call onChange with change batches', async () => {
    const batches: ChangeBatch[] = []

    const target = settleTarget({
      schema: SIMPLE_SCHEMA,
      onChange: ({ batch }: any) => {
        batches.push(batch)
      },
    })

    await target.write({
      read: mockRead([
        {
          blockNumber: 100,
          data: {
            transfers: [
              { block_number: 100, tx_index: 0, from_addr: 'alice', to_addr: 'bob', value: 10 },
            ],
          },
        },
        {
          blockNumber: 101,
          data: {
            transfers: [
              { block_number: 101, tx_index: 0, from_addr: 'bob', to_addr: 'carol', value: 5 },
            ],
          },
        },
      ]),
      logger: null,
    })

    expect(batches.length).toBe(2)
    expect(allRecords(batches[0]).length).toBe(1)
    expect(allRecords(batches[0])[0].table).toBe('transfers')
    expect(allRecords(batches[0])[0].operation).toBe('insert')
    expect(allRecords(batches[1]).length).toBe(1)
    expect(batches[1].latestHead?.number).toBe(101)
  })

  it('should propagate finalized block from ctx', async () => {
    const batches: ChangeBatch[] = []

    const target = settleTarget({
      schema: SIMPLE_SCHEMA,
      onChange: ({ batch }: any) => {
        batches.push(batch)
      },
    })

    const blocks = []
    for (let i = 1; i <= 1000; i++) {
      blocks.push({
        blockNumber: i,
        finalized: i >= 950 ? 950 : undefined,
        data: {
          transfers: [{ block_number: i, tx_index: 0, from_addr: 'a', to_addr: 'b', value: 1 }],
        },
      })
    }

    await target.write({
      read: mockRead(blocks),
      logger: null,
    })

    expect(batches.length).toBe(1000)
    // The last batch should have finalized at 950
    expect(batches[batches.length - 1].finalizedHead?.number).toBe(950)
  })

  it('should handle fork and produce compensating changes via onChange', async () => {
    const allBatches: ChangeBatch[] = []

    const target = settleTarget({
      schema: SIMPLE_SCHEMA,
      onChange: ({ batch }: any) => {
        allBatches.push(batch)
      },
    })

    // Process initial blocks (keep unfinalized so hashes are preserved for fork resolution)
    await target.write({
      read: mockRead([
        {
          blockNumber: 100,
          finalized: 99,
          data: {
            transfers: [
              { block_number: 100, tx_index: 0, from_addr: 'alice', to_addr: 'bob', value: 10 },
            ],
          },
        },
        {
          blockNumber: 101,
          finalized: 99,
          data: {
            transfers: [
              { block_number: 101, tx_index: 0, from_addr: 'bob', to_addr: 'carol', value: 5 },
            ],
          },
        },
        {
          blockNumber: 102,
          finalized: 99,
          data: {
            transfers: [
              { block_number: 102, tx_index: 0, from_addr: 'carol', to_addr: 'dave', value: 3 },
            ],
          },
        },
      ]),
      logger: null,
    })

    expect(allBatches.length).toBe(3)

    // Simulate fork: rollback to block 100 (blocks 101, 102 are bad)
    const safeCursor = await target.fork([{ number: 100, hash: '0x64' }])

    expect(safeCursor).toEqual({ number: 100, hash: '0x64' })

    // Compensating changes delivered through onChange (batch 4)
    expect(allBatches.length).toBe(4)
    const compensating = allBatches[3]
    const deletes = allRecords(compensating).filter((r) => r.operation === 'delete')
    expect(deletes.length).toBe(2) // one delete per undone transfer
  })

  it('should handle fork with empty previousBlocks', async () => {
    const target = settleTarget({
      schema: SIMPLE_SCHEMA,
      onChange: () => {},
    })

    const safeCursor = await target.fork([])
    expect(safeCursor).toBeNull()
  })

  it('should process full reducer pipeline through pipes target', async () => {
    const batches: ChangeBatch[] = []

    const target = settleTarget({
      schema: REDUCER_SCHEMA,
      onChange: ({ batch }: any) => {
        batches.push(batch)
      },
    })

    await target.write({
      read: mockRead([
        {
          blockNumber: 100,
          data: {
            trades: [{ block_number: 100, user: 'alice', side: 'buy', amount: 10, price: 2000 }],
          },
        },
        {
          blockNumber: 101,
          data: {
            trades: [{ block_number: 101, user: 'alice', side: 'sell', amount: 5, price: 2200 }],
          },
        },
      ]),
      logger: null,
    })

    expect(batches.length).toBe(2)

    // Block 100: insert trade + insert MV
    const b100trades = findRecords(batches[0], 'trades', 'insert')
    const b100mv = findRecords(batches[0], 'position_summary', 'insert')
    expect(b100trades.length).toBe(1)
    expect(b100mv.length).toBe(1)

    // Block 101: insert trade + update MV
    const b101trades = findRecords(batches[1], 'trades', 'insert')
    const b101mv = findRecords(batches[1], 'position_summary', 'update')
    expect(b101trades.length).toBe(1)
    expect(b101mv.length).toBe(1)

    const values = { ...b101mv[0].key, ...b101mv[0].values } as Record<string, unknown>
    expect(values.trade_count).toBe(2)
    expect(values.current_position).toBe(5) // 10 - 5
  })

  it('should handle multiple tables in same block', async () => {
    const schema = `
      CREATE TABLE events_a (
        block_number UInt64,
        name String
      );
      CREATE TABLE events_b (
        block_number UInt64,
        value Float64
      );
    `

    const batches: ChangeBatch[] = []

    const target = settleTarget({
      schema,
      onChange: ({ batch }: any) => {
        batches.push(batch)
      },
    })

    await target.write({
      read: mockRead([
        {
          blockNumber: 100,
          data: {
            events_a: [
              { block_number: 100, name: 'foo' },
              { block_number: 100, name: 'bar' },
            ],
            events_b: [{ block_number: 100, value: 42.0 }],
          },
        },
      ]),
      logger: null,
    })

    expect(batches.length).toBe(1)

    const aRecords = findRecords(batches[0], 'events_a')
    const bRecords = findRecords(batches[0], 'events_b')

    expect(aRecords.length).toBe(2)
    expect(bRecords.length).toBe(1)
  })

  it('should handle fork then re-ingest (full reorg scenario)', async () => {
    const allBatches: ChangeBatch[] = []

    const target = settleTarget({
      schema: REDUCER_SCHEMA,
      onChange: ({ batch }: any) => {
        allBatches.push(batch)
      },
    })

    // Phase 1: Process blocks 100-101 (keep unfinalized for fork resolution)
    await target.write({
      read: mockRead([
        {
          blockNumber: 100,
          finalized: 99,
          data: {
            trades: [{ block_number: 100, user: 'alice', side: 'buy', amount: 10, price: 2000 }],
          },
        },
        {
          blockNumber: 101,
          finalized: 99,
          data: {
            trades: [{ block_number: 101, user: 'alice', side: 'buy', amount: 5, price: 2100 }],
          },
        },
      ]),
      logger: null,
    })

    expect(allBatches.length).toBe(2)

    // Phase 2: Fork — rollback to block 100
    await target.fork([{ number: 100, hash: '0x64' }])
    // Compensating changes delivered via onChange
    expect(allBatches.length).toBe(3)

    // Phase 3: Re-ingest corrected blocks
    await target.write({
      read: mockRead([
        {
          blockNumber: 101,
          data: {
            trades: [{ block_number: 101, user: 'alice', side: 'sell', amount: 3, price: 2300 }],
          },
        },
      ]),
      logger: null,
    })

    expect(allBatches.length).toBe(4)

    // Last batch: alice sells instead of buys
    const lastBatch = allBatches[3]
    const tradeInsert = findRecords(lastBatch, 'trades', 'insert')[0]
    expect((tradeInsert.values as any).side).toBe('sell')

    // MV should reflect corrected state
    const mvRecords = findRecords(lastBatch, 'position_summary')
    expect(mvRecords.length).toBe(1)
    const values = { ...mvRecords[0].key, ...mvRecords[0].values } as Record<string, unknown>
    expect(values.current_position).toBe(7) // 10 - 3
  })
})

// ─── Sequence / Ack Tests ──────────────────────────────────────────

describe('settleTarget sequence tracking', () => {
  it('should produce monotonically increasing sequence numbers', async () => {
    const sequences: number[] = []

    const target = settleTarget({
      schema: SIMPLE_SCHEMA,
      onChange: ({ batch }: any) => {
        sequences.push(batch.sequence)
      },
    })

    await target.write({
      read: mockRead([
        {
          blockNumber: 100,
          data: {
            transfers: [{ block_number: 100, tx_index: 0, from_addr: 'a', to_addr: 'b', value: 1 }],
          },
        },
        {
          blockNumber: 101,
          data: {
            transfers: [{ block_number: 101, tx_index: 0, from_addr: 'b', to_addr: 'c', value: 2 }],
          },
        },
        {
          blockNumber: 102,
          data: {
            transfers: [{ block_number: 102, tx_index: 0, from_addr: 'c', to_addr: 'd', value: 3 }],
          },
        },
      ]),
      logger: null,
    })

    expect(sequences.length).toBe(3)
    for (let i = 1; i < sequences.length; i++) {
      expect(sequences[i]).toBeGreaterThan(sequences[i - 1])
    }
  })
})

// ─── Edge Cases ────────────────────────────────────────────────────

describe('settleTarget edge cases', () => {
  it('should handle blocks with empty table arrays', async () => {
    const batches: ChangeBatch[] = []

    const target = settleTarget({
      schema: SIMPLE_SCHEMA,
      onChange: ({ batch }: any) => {
        batches.push(batch)
      },
    })

    await target.write({
      read: mockRead([
        {
          blockNumber: 100,
          data: {
            transfers: [],
          },
        },
      ]),
      logger: null,
    })

    expect(batches.length).toBe(0)
  })

  it('should reject unknown table names', async () => {
    const target = settleTarget({
      schema: SIMPLE_SCHEMA,
      onChange: () => {},
    })

    await expect(
      target.write({
        read: mockRead([
          {
            blockNumber: 100,
            data: {
              nonexistent: [{ block_number: 100, x: 1 }],
            },
          },
        ]),
        logger: null,
      }),
    ).rejects.toThrow()
  })
})
