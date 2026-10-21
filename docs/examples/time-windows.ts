import { Pipeline, datetime, float64, interval, string, uint64 } from '@settle/stream'

const p = new Pipeline()

// [!region table]
const trades = p.table(
  'trades',
  {
    block_number: uint64(),
    timestamp: datetime(), // milliseconds since 1970-01-01
    pool: string(),
    price: float64(),
    amount: float64(),
  },
  { virtual: true },
)
// [!endregion table]

// [!region candles]
trades.createView('candles_1m', {
  groupBy: ['pool', interval('timestamp', '1 minute').as('minute')],
  select: (agg) => ({
    pool: agg.key.pool,
    minute: agg.key.minute,
    open: agg.first('price'),
    high: agg.max('price'),
    low: agg.min('price'),
    close: agg.last('price'),
    volume: agg.sum('amount'),
  }),
})
// [!endregion candles]

// [!region sliding]
trades.createView('volume_1h', {
  groupBy: 'pool',
  select: (agg) => ({
    pool: agg.key.pool,
    volume: agg.sum('amount'),
    trades: agg.count(),
  }),
  slidingWindow: { interval: '1 hour', timeColumn: 'timestamp' },
})
// [!endregion sliding]

const db = p.build()

const MINUTE = 60_000
const t0 = Date.UTC(2026, 0, 1, 12, 0)

const batch = await db.ingest({
  data: {
    trades: [
      { block_number: 1, timestamp: t0 + 5_000, pool: 'ETH/USDC', price: 2000, amount: 1 },
      { block_number: 2, timestamp: t0 + 30_000, pool: 'ETH/USDC', price: 2010, amount: 2 },
      { block_number: 3, timestamp: t0 + 50_000, pool: 'ETH/USDC', price: 1990, amount: 1 },
      { block_number: 4, timestamp: t0 + MINUTE + 10_000, pool: 'ETH/USDC', price: 2005, amount: 3 },
    ],
  },
  rollbackChain: [
    { number: 1, hash: '0x1a' },
    { number: 2, hash: '0x2a' },
    { number: 3, hash: '0x3a' },
    { number: 4, hash: '0x4a' },
  ],
  finalizedHead: { number: 0, hash: '0x00' },
})

for (const record of batch?.tables.candles_1m ?? []) {
  console.log(record.operation, record.values)
}
for (const record of batch?.tables.volume_1h ?? []) {
  console.log(record.operation, record.values)
}

if (batch) db.ack(batch.sequence)

// Two hours later: the trades of block 1 to 4 leave the 1-hour window.
const later = await db.ingest({
  data: {
    trades: [
      { block_number: 5, timestamp: t0 + 120 * MINUTE, pool: 'ETH/USDC', price: 2100, amount: 4 },
    ],
  },
  rollbackChain: [{ number: 5, hash: '0x5a' }],
  finalizedHead: { number: 4, hash: '0x4a' },
})

console.log('---')
for (const record of later?.tables.volume_1h ?? []) {
  console.log(record.operation, record.values)
}

if (later) db.ack(later.sequence)
