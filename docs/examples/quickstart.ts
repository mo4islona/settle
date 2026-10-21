// [!region pipeline]
import { type ChangeBatch, Pipeline, float64, string, uint64 } from '@settle/stream'

const p = new Pipeline()

const swaps = p.table(
  'swaps',
  {
    block_number: uint64(),
    pool: string(),
    amount: float64(),
  },
  { virtual: true },
)

swaps.createView('pool_volume', {
  groupBy: 'pool',
  select: (agg) => ({
    pool: agg.key.pool,
    volume: agg.sum('amount'),
    swaps: agg.count(),
  }),
})

const db = p.build()
// [!endregion pipeline]

// [!region apply]
function apply(batch: ChangeBatch | null) {
  if (!batch) return

  for (const record of batch.tables.pool_volume ?? []) {
    console.log(record.operation, record.values)
  }

  db.ack(batch.sequence)
}
// [!endregion apply]

// [!region ingest]
const batch = await db.ingest({
  data: {
    swaps: [
      { block_number: 1, pool: 'ETH/USDC', amount: 100 },
      { block_number: 2, pool: 'ETH/USDC', amount: 250 },
      { block_number: 2, pool: 'BTC/USDC', amount: 50 },
    ],
  },
  rollbackChain: [
    { number: 1, hash: '0x1a' },
    { number: 2, hash: '0x2a' },
  ],
  finalizedHead: { number: 0, hash: '0x00' },
})

apply(batch)
// [!endregion ingest]

// [!region fork]
const fork = db.handleFork([
  { number: 2, hash: '0x2b' },
  { number: 1, hash: '0x1a' },
])

console.log('fork point:', fork.cursor.number)
apply(fork.batch)
// [!endregion fork]

// [!region new-block]
const next = await db.ingest({
  data: {
    swaps: [{ block_number: 2, pool: 'ETH/USDC', amount: 300 }],
  },
  rollbackChain: [{ number: 2, hash: '0x2b' }],
  finalizedHead: { number: 1, hash: '0x1a' },
})

apply(next)
// [!endregion new-block]
