import { type ChangeBatch, Pipeline, float64, string, uint64 } from '@settle/stream'

const p = new Pipeline()

const swaps = p.table(
  'swaps',
  { block_number: uint64(), pool: string(), amount: float64() },
  { virtual: true },
)

swaps.createView('pool_volume', {
  groupBy: 'pool',
  select: (agg) => ({ pool: agg.key.pool, volume: agg.sum('amount') }),
})

const db = p.build()

function apply(batch: ChangeBatch | null) {
  for (const record of batch?.tables.pool_volume ?? []) {
    console.log(record.operation, record.values)
  }
  if (batch) db.ack(batch.sequence)
}

apply(
  await db.ingest({
    data: {
      swaps: [
        { block_number: 1, pool: 'ETH/USDC', amount: 100 },
        { block_number: 2, pool: 'ETH/USDC', amount: 250 },
        { block_number: 3, pool: 'ETH/USDC', amount: 50 },
      ],
    },
    rollbackChain: [
      { number: 1, hash: '0x1a' },
      { number: 2, hash: '0x2a' },
      { number: 3, hash: '0x3a' },
    ],
    finalizedHead: { number: 1, hash: '0x1a' },
  }),
)

console.log('--- in-line fork')

// [!region inline]
// Block 2 and block 3 have new hashes. Block 1 does not change.
const batch = await db.ingest({
  data: {
    swaps: [
      { block_number: 2, pool: 'ETH/USDC', amount: 10 },
      { block_number: 3, pool: 'ETH/USDC', amount: 20 },
    ],
  },
  rollbackChain: [
    { number: 1, hash: '0x1a' },
    { number: 2, hash: '0x2b' },
    { number: 3, hash: '0x3b' },
  ],
  finalizedHead: { number: 1, hash: '0x1a' },
})

apply(batch)
// [!endregion inline]

console.log('--- no common ancestor')

// [!region no-ancestor]
try {
  db.handleFork([{ number: 3, hash: '0x3c' }])
} catch (error) {
  console.log((error as Error).message)
}
// [!endregion no-ancestor]
