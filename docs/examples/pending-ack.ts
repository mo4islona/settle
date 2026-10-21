import { Pipeline, SettlePendingAckError, float64, string, uint64 } from '@settle/stream'

const p = new Pipeline()

p.table('swaps', { block_number: uint64(), pool: string(), amount: float64() }).createView(
  'pool_volume',
  {
    groupBy: 'pool',
    select: (agg) => ({ pool: agg.key.pool, volume: agg.sum('amount') }),
  },
)

const db = p.build({ maxPendingBatches: 1 })

function block(n: number) {
  return {
    data: { swaps: [{ block_number: n, pool: 'ETH/USDC', amount: 100 }] },
    rollbackChain: [{ number: n, hash: `0x${n}` }],
    finalizedHead: { number: 0, hash: '0x0' },
  }
}

const first = await db.ingest(block(1))

// [!region pending]
try {
  await db.ingest(block(2))
} catch (error) {
  if (!(error instanceof SettlePendingAckError)) throw error

  // Apply and ack the oldest batch, then send the same blocks again.
  console.log('wait for the ack of batch', error.sequence)
  db.ack(error.sequence)
  await db.ingest(block(2))
}
// [!endregion pending]

console.log('first batch:', first?.sequence, 'unacked batches:', db.unackedBatches)
