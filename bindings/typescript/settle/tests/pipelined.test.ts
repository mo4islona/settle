/**
 * Pipelined acks: with `maxPendingBatches` > 1, several change batches wait
 * for their ack while the caller ingests the next blocks.
 */

import { describe, expect, it } from 'vitest'
import {
  type IngestInput,
  type ISettle,
  Settle,
  SettlePendingAckError,
  SettleWrongAckSequenceError,
} from '../src/index'
import { Settle as WebSettle, init } from '../src/web'
import { mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

const SCHEMA = `
  CREATE TABLE events (
    block_number UInt64,
    user         String,
    amount       Float64
  );

  CREATE MATERIALIZED VIEW user_totals AS
  SELECT user, sum(amount) AS total
  FROM events
  GROUP BY user;
`

function block(n: number): IngestInput {
  return {
    data: { events: [{ block_number: n, user: 'alice', amount: n }] },
    finalizedHead: { number: 0, hash: '0x0' },
    rollbackChain: [{ number: n, hash: `0x${n}` }],
  }
}

async function checkPipelinedAcks(db: ISettle) {
  const b1 = await db.ingest(block(1))
  const b2 = await db.ingest(block(2))
  expect(db.unackedBatches).toBe(2)

  await expect(db.ingest(block(3))).rejects.toBeInstanceOf(SettlePendingAckError)
  expect(() => db.ack(b2!.sequence + 1)).toThrow(SettleWrongAckSequenceError)

  // Acks are cumulative: the second ack commits the first batch too.
  db.ack(b2!.sequence)
  expect(db.unackedBatches).toBe(0)
  db.ack(b1!.sequence)

  const b3 = await db.ingest(block(3))
  expect(b3!.tables.user_totals[0].values.total).toBe(6)
  db.ack(b3!.sequence)
  expect(db.isAwaitingAck).toBe(false)
}

describe('pipelined acks', () => {
  it('native', async () => {
    await checkPipelinedAcks(Settle.open({ schema: SCHEMA, maxPendingBatches: 2 }))
  })

  it('wasm', async () => {
    await init(readFileSync(resolve(__dirname, '../src/wasm/settle_bg.wasm')))
    await checkPipelinedAcks(WebSettle.open({ schema: SCHEMA, maxPendingBatches: 2 }))
  })

  it('two unacked batches by default', async () => {
    const db = Settle.open({ schema: SCHEMA })
    await db.ingest(block(1))
    await db.ingest(block(2))
    await expect(db.ingest(block(3))).rejects.toBeInstanceOf(SettlePendingAckError)
  })

  it('one unacked batch when set to 1', async () => {
    const db = Settle.open({ schema: SCHEMA, maxPendingBatches: 1 })
    await db.ingest(block(1))
    await expect(db.ingest(block(2))).rejects.toBeInstanceOf(SettlePendingAckError)
  })
})

describe('applied sequence', () => {
  it('reaches the native open', async () => {
    const dataDir = mkdtempSync(join(tmpdir(), 'settle-applied-'))
    try {
      const open = (appliedSequence: number) => () =>
        Settle.open({ schema: SCHEMA, dataDir, appliedSequence })
      expect(open(-1)).toThrow(/non-negative/)
      // A new database has no batch that the target can have applied.
      expect(open(3)).toThrow(/no record of batch 1/)

      const db = Settle.open({ schema: SCHEMA, dataDir, appliedSequence: 0 })
      const batch = await db.ingest(block(1))
      db.ack(batch!.sequence)
      expect(db.isAwaitingAck).toBe(false)
    } finally {
      rmSync(dataDir, { recursive: true, force: true })
    }
  })
})
