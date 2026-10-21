import { Pipeline, float64, string, uint64 } from '@settle/stream'

// [!region table]
const p = new Pipeline()

const swaps = p.table('swaps', {
  block_number: uint64(),
  pool: string(),
  amount: float64(),
})
// [!endregion table]

swaps.createView('pool_volume', {
  groupBy: 'pool',
  select: (agg) => ({ pool: agg.key.pool, volume: agg.sum('amount') }),
})

const db = p.build()

// [!region ingest]
const batch = await db.ingest({
  data: {
    swaps: [
      { block_number: 7, pool: 'ETH/USDC', amount: 100 },
      { block_number: 7, pool: 'BTC/USDC', amount: 50 },
    ],
  },
  rollbackChain: [{ number: 7, hash: '0x7a' }],
  finalizedHead: { number: 0, hash: '0x00' },
})

for (const record of batch?.tables.swaps ?? []) {
  console.log(record.operation, record.key, record.values)
}
// [!endregion ingest]

if (batch) db.ack(batch.sequence)

// Not shown on a page: a fork removes the rows of block 7.
const fork = db.handleFork([{ number: 7, hash: '0x7b' }, { number: 0, hash: '0x00' }])
for (const record of fork.batch?.tables.swaps ?? []) {
  console.log(record.operation, record.key, record.values, record.prevValues)
}
