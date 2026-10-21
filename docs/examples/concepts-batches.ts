import { Pipeline, float64, string, uint64 } from '@settle/stream'

const p = new Pipeline()

const swaps = p.table(
  'swaps',
  { block_number: uint64(), pool: string(), amount: float64() },
  { virtual: true },
)

swaps.createView('pool_volume', {
  groupBy: 'pool',
  select: (agg) => ({ pool: agg.key.pool, volume: agg.sum('amount'), swaps: agg.count() }),
})

const db = p.build()

const first = await db.ingest({
  data: { swaps: [{ block_number: 1, pool: 'ETH/USDC', amount: 100 }] },
  rollbackChain: [{ number: 1, hash: '0x1a' }],
  finalizedHead: { number: 0, hash: '0x00' },
})
if (first) db.ack(first.sequence)

// [!region update]
const batch = await db.ingest({
  data: {
    swaps: [
      { block_number: 2, pool: 'ETH/USDC', amount: 250 },
      { block_number: 3, pool: 'ETH/USDC', amount: 50 },
    ],
  },
  rollbackChain: [
    { number: 2, hash: '0x2a' },
    { number: 3, hash: '0x3a' },
  ],
  finalizedHead: { number: 1, hash: '0x1a' },
})

console.log('sequence:', batch?.sequence)
console.dir(batch?.tables, { depth: null })
// [!endregion update]

if (batch) db.ack(batch.sequence)

// [!region heartbeat]
const empty = await db.ingest({
  data: {},
  finalizedHead: { number: 3, hash: '0x3a' },
})

console.log(empty) // null: no output table changed
// [!endregion heartbeat]
