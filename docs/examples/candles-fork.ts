import { type ChangeBatch, Pipeline, datetime, float64, interval, string, uint64 } from '@settle/stream'

const p = new Pipeline()

p.table(
  'trades',
  { block_number: uint64(), timestamp: datetime(), pool: string(), price: float64() },
  { virtual: true },
).createView('candles_1m', {
  groupBy: ['pool', interval('timestamp', '1 minute').as('minute')],
  select: (agg) => ({
    pool: agg.key.pool,
    minute: agg.key.minute,
    open: agg.first('price'),
    high: agg.max('price'),
    low: agg.min('price'),
    close: agg.last('price'),
  }),
})

const db = p.build()

function apply(batch: ChangeBatch | null) {
  if (!batch) return

  for (const { operation, values } of batch.tables.candles_1m ?? []) {
    console.log(operation, { open: values.open, high: values.high, low: values.low, close: values.close })
  }

  db.ack(batch.sequence)
}

const t0 = Date.UTC(2026, 0, 1, 12, 0)

// [!region blocks]
apply(
  await db.ingest({
    data: {
      trades: [
        { block_number: 1, timestamp: t0 + 10_000, pool: 'ETH/USDC', price: 2000 },
        { block_number: 2, timestamp: t0 + 20_000, pool: 'ETH/USDC', price: 2050 },
        { block_number: 3, timestamp: t0 + 30_000, pool: 'ETH/USDC', price: 2010 },
      ],
    },
    rollbackChain: [
      { number: 1, hash: '0x1a' },
      { number: 2, hash: '0x2a' },
      { number: 3, hash: '0x3a' },
    ],
    finalizedHead: { number: 0, hash: '0x00' },
  }),
)
// [!endregion blocks]

console.log('---')

// [!region fork]
// The chain replaces blocks 2 and 3. Block 2 had the highest price.
const fork = db.handleFork([
  { number: 3, hash: '0x3b' },
  { number: 2, hash: '0x2b' },
  { number: 1, hash: '0x1a' },
])
apply(fork.batch)

apply(
  await db.ingest({
    data: {
      trades: [{ block_number: 2, timestamp: t0 + 20_000, pool: 'ETH/USDC', price: 1995 }],
    },
    rollbackChain: [{ number: 2, hash: '0x2b' }],
    finalizedHead: { number: 1, hash: '0x1a' },
  }),
)
// [!endregion fork]
