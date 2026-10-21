import { Pipeline, float64, string, uint64 } from '@settle/stream'

// [!region open]
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

const db = p.build({ dataDir: './settle-data' })

console.log('continue after:', db.cursor)
// [!endregion open]

// [!region next-block]
const next = (db.cursor?.number ?? 0) + 1

const batch = await db.ingest({
  data: {
    swaps: [{ block_number: next, pool: 'ETH/USDC', amount: 100 }],
  },
  rollbackChain: [{ number: next, hash: `0x${next}` }],
  finalizedHead: { number: next - 1, hash: `0x${next - 1}` },
})
// [!endregion next-block]

// [!region apply]
if (batch) {
  for (const record of batch.tables.pool_volume ?? []) {
    console.log(`batch ${batch.sequence}:`, record.operation, record.values)
  }

  // A crash before the ack loses the batch in Settle, but not in your database.
  if (!process.argv.includes('--crash')) {
    db.ack(batch.sequence)
  }
}
// [!endregion apply]
