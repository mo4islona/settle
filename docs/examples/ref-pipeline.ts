// Checks the Pipeline builder: the generated schema, the state types that
// initialState gives, and the effect of update().
import { rmSync } from 'node:fs'
import { Pipeline, Settle, type StateMap, datetime, float64, interval, string, uint64 } from '@settle/stream'

// [!region compile]
const p = new Pipeline()

const trades = p.table('trades', {
  block_number: uint64(),
  timestamp: datetime(),
  trader: string(),
  amount: float64(),
})

type Stats = { volume: number; trades: bigint; last_side: string; active: boolean; tags: object }
type StatsEmit = { volume: number; timestamp: number }

const stats = trades.createReducer<Stats, StatsEmit>('stats', {
  groupBy: 'trader',
  initialState: { volume: 0, trades: 0n, last_side: 'none', active: false, tags: {} },
  reduce(state, row) {
    const volume = state.volume + row.amount
    state.update({ ...state, trades: BigInt(state.trades) + 1n, volume })
    state.emit({ volume, timestamp: row.timestamp })
  },
})

stats.createView('trader_volume', {
  groupBy: ['trader', interval('timestamp', '1 hour').as('hour')],
  select: (agg) => ({
    trader: agg.key.trader,
    hour: agg.key.hour,
    volume: agg.last('volume'),
  }),
})

trades.createView('volume_24h', {
  groupBy: 'trader',
  select: (agg) => ({ trader: agg.key.trader, volume: agg.sum('amount') }),
  slidingWindow: { interval: '24 hours', timeColumn: 'timestamp' },
})

console.log(p.compile().schema.replaceAll(';', ';\n'))
// [!endregion compile]

console.log('--- update() sets the absent fields to null')
const q = new Pipeline()
q.table('t', { block_number: uint64(), k: string(), n: float64() })
  .createReducer<{ a: number; b: number }, { a: number; b: number }>('r', {
    groupBy: 'k',
    initialState: { a: 0, b: 0 },
    reduce(state, row) {
      console.log('  row', row.n, 'sees a =', state.a, 'b =', state.b)
      if (row.n === 1) state.update({ a: 1, b: 1 })
      else state.update({ a: 2 } as any)
      state.emit({ a: state.a, b: state.b })
    },
  })
  .createView('v', { groupBy: 'k', select: (agg) => ({ k: agg.key.k, a: agg.last('a'), b: agg.last('b') }) })
const db = q.build({ dataDir: ':memory:', maxPendingBatches: 3 })
for (const n of [1, 2, 3]) {
  const b = await db.ingest({
    data: { t: [{ block_number: n, k: 'x', n }] },
    rollbackChain: [{ number: n, hash: `0x${n}a` }],
    finalizedHead: { number: 0, hash: '0x00' },
  })
  console.log(JSON.stringify(b?.tables.v?.[0]?.values))
}
console.log('three unacked:', db.unackedBatches)

console.log('--- the state that the reducer sees')
const r = new Pipeline()
r.table('t', { block_number: uint64(), k: string() }).createReducer('r', {
  groupBy: 'k',
  initialState: { count: 0n, label: 'x' },
  reduce(state, row) {
    console.log('state:', typeof state.count, state.count, state.label, 'row:', JSON.stringify(row))
    state.update({ count: BigInt(state.count) + 1n, label: 'y' })
  },
})
const rdb = r.build()
await rdb.ingest({
  data: { t: [{ block_number: 1, k: 'a' }, { block_number: 1, k: 'a' }] },
  rollbackChain: [{ number: 1, hash: '0x1a' }],
  finalizedHead: { number: 0, hash: '0x00' },
})

console.log('--- interval units in SQL')
for (const unit of ['5 MINUTE', '5 MINUTES', '1 HOUR', '1 DAY', '1 WEEK', '30 SECOND']) {
  try {
    Settle.open({
      schema: `CREATE TABLE w (block_number UInt64, ts DateTime, k String, n Float64);
        CREATE MATERIALIZED VIEW v AS SELECT k, toStartOfInterval(ts, INTERVAL ${unit}) AS bucket, sum(n) AS s FROM w GROUP BY k, bucket;`,
    })
    console.log(unit, 'ok')
  } catch (e) {
    console.log(unit, (e as Error).message)
  }
}
for (const d of ['90 s', '5 min', '2 h', '1 d', '1 week', 'one hour']) {
  try {
    interval('ts', d)
    console.log(`interval '${d}' ok`)
  } catch (e) {
    console.log(`interval '${d}':`, (e as Error).message)
  }
}

console.log('--- more options')
{
  // [!region options]
  const p = new Pipeline()
  p.table('swaps', { block_number: uint64(), pool: string(), amount: float64() }).createReducer('totals', {
    groupBy: 'pool',
    initialState: { volume: 0 },
    reduce(state, row) {
      state.update({ volume: state.volume + row.amount })
    },
  })

  const db = p.build({
    dataDir: './settle-data',
    maxPendingBatches: 1,
    appliedSequence: 0,
  })
  // [!endregion options]
  console.log('opened, unacked:', db.unackedBatches)
}

console.log('--- a Map field')
{
  // [!region map]
  const db = Settle.open({
    schema: `
      CREATE TABLE moves (block_number UInt64, user String, token String, amount Float64);
      CREATE REDUCER books SOURCE moves GROUP BY user
      STATE (book Map(String, Float64))
      LANGUAGE EXTERNAL;
      CREATE MATERIALIZED VIEW holdings AS
      SELECT user, token, last(qty) AS qty FROM books GROUP BY user, token;
    `,
  })

  db.registerReducerCallback<{ book: StateMap<number> }>('books', (state, row) => {
    const qty = (state.book.get(row.token) ?? 0) + row.amount

    if (qty === 0) state.book.delete(row.token)
    else state.book.set(row.token, qty)

    state.emit({ token: row.token, qty })
  })
  // [!endregion map]

  const move = (block_number: number, amount: number) => ({ block_number, user: 'a', token: 'X', amount })
  const b = await db.ingest({
    data: { moves: [move(1, 5), move(1, 2), move(2, -7), move(2, 1)] },
    rollbackChain: [{ number: 1, hash: '0x1a' }, { number: 2, hash: '0x2a' }],
    finalizedHead: { number: 0, hash: '0x00' },
  })
  for (const r of b!.tables.holdings) console.log(r.operation, 'qty=' + r.values.qty)
}
