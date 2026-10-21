// [!region open]
import { Pipeline, Settle, float64, init, openCompiled, string, uint64 } from '@settle/stream/web'

await init()

const p = new Pipeline()

p.table(
  'swaps',
  { block_number: uint64(), pool: string(), amount: float64() },
  { virtual: true },
).createView('pool_volume', {
  groupBy: 'pool',
  select: (agg) => ({
    pool: agg.key.pool,
    volume: agg.sum('amount'),
    swaps: agg.count(),
  }),
})

const db = openCompiled(Settle, p.compile())
// [!endregion open]

// [!region ingest]
const batch = await db.ingest({
  data: {
    swaps: [
      { block_number: 1, pool: 'ETH/USDC', amount: 100 },
      { block_number: 2, pool: 'BTC/USDC', amount: 50 },
    ],
  },
  rollbackChain: [
    { number: 1, hash: '0x1a' },
    { number: 2, hash: '0x2a' },
  ],
  finalizedHead: { number: 0, hash: '0x00' },
})

for (const record of batch?.tables.pool_volume ?? []) {
  console.log(record.operation, record.values)
}

if (batch) db.ack(batch.sequence)
// [!endregion ingest]

// [!region options]
try {
  openCompiled(Settle, p.compile(), { dataDir: './data' })
} catch (error) {
  console.log((error as Error).message)
}
// [!endregion options]
