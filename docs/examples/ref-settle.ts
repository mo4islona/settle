// Checks the Settle class: open with a schema text, external reducers, the
// batch fields, the properties and the fork helper.
import { Settle, SettlePendingAckError, SettleWrongAckSequenceError } from '@settle/stream'

// [!region open]
const db = Settle.open({
  schema: `
    CREATE TABLE swaps (block_number UInt64, pool String, amount Float64);

    CREATE REDUCER totals
    SOURCE swaps
    GROUP BY pool
    STATE (volume Float64 DEFAULT 0)
    LANGUAGE EXTERNAL;

    CREATE MATERIALIZED VIEW pool_volume AS
    SELECT pool, last(volume) AS volume
    FROM totals
    GROUP BY pool;
  `,
})

db.registerReducerCallback<{ volume: number }>('totals', (state, row) => {
  const volume = state.volume + row.amount
  state.update({ volume })
  state.emit({ volume })
})
// [!endregion open]

console.log('cursor before:', db.cursor)

const batch = await db.ingest({
  data: {
    swaps: [
      { block_number: 10, pool: 'ETH/USDC', amount: 100 },
      { block_number: 11, pool: 'ETH/USDC', amount: 50 },
    ],
  },
  rollbackChain: [
    { number: 10, hash: '0x10a' },
    { number: 11, hash: '0x11a' },
  ],
  finalizedHead: { number: 9, hash: '0x09a' },
})

// [!region batch]
console.log('sequence:', batch!.sequence)
console.log('latestHead:', batch!.latestHead)
console.log('finalizedHead:', batch!.finalizedHead)
console.log('tables:', Object.keys(batch!.tables))
console.log('raw record:', batch!.tables.swaps[0])
console.log('view record:', batch!.tables.pool_volume[0])
// [!endregion batch]
console.log('perf kinds:', batch!.perf.map((n) => `${n.kind}:${n.name}`).join(' '))

console.log('unackedBatches:', db.unackedBatches, 'isAwaitingAck:', db.isAwaitingAck)
db.ack(batch!.sequence)
console.log('after ack:', db.unackedBatches, db.isAwaitingAck)
console.log('cursor after:', db.cursor)
console.log('pendingCount:', db.pendingCount, 'isBackpressured:', db.isBackpressured, 'isPoisoned:', db.isPoisoned)

console.log('--- update record with prevValues')
const b2 = await db.ingest({
  data: { swaps: [{ block_number: 12, pool: 'ETH/USDC', amount: 25 }] },
  rollbackChain: [{ number: 12, hash: '0x12a' }],
  finalizedHead: { number: 10, hash: '0x10a' },
})
console.log(b2!.tables.pool_volume[0])
db.ack(b2!.sequence)

console.log('--- a call with no change')
const b3 = await db.ingest({
  data: {},
  rollbackChain: [],
  finalizedHead: { number: 11, hash: '0x11a' },
})
console.log('batch:', b3)

console.log('--- resolveForkCursor')
// [!region resolve]
const point = db.resolveForkCursor([
  { number: 12, hash: '0x12b' },
  { number: 11, hash: '0x11a' },
])
console.log(point)
// [!endregion resolve]
console.log('unknown chain:', db.resolveForkCursor([{ number: 12, hash: '0xzz' }]))

console.log('--- handleFork')
const fork = db.handleFork([
  { number: 12, hash: '0x12b' },
  { number: 11, hash: '0x11a' },
])
console.log('cursor:', fork.cursor)
for (const t of Object.keys(fork.batch!.tables)) {
  for (const r of fork.batch!.tables[t]) console.log(t, r.operation, JSON.stringify(r.key), JSON.stringify(r.values), JSON.stringify(r.prevValues))
}
db.ack(fork.batch!.sequence)
console.log('cursor after fork:', db.cursor)

console.log('--- registerReducer')
const other = Settle.open({
  schema: `CREATE TABLE swaps (block_number UInt64, pool String, amount Float64);`,
})
// [!region register]
other.registerReducer<{ count: bigint }>({
  name: 'counts',
  source: 'swaps',
  groupBy: ['pool'],
  state: [{ name: 'count', columnType: 'UInt64', defaultValue: '0' }],
  reduce(state) {
    state.update({ count: BigInt(state.count) + 1n })
  },
})
// [!endregion register]
console.log('registered')
try {
  other.registerReducer({ name: 'counts', source: 'swaps', groupBy: ['pool'], state: [], reduce() {} })
} catch (e) {
  console.log('twice:', (e as Error).message)
}
try {
  db.registerReducerCallback('totals', () => {})
} catch (e) {
  console.log('callback twice:', (e as Error).message)
}
try {
  db.registerReducerCallback('nothing', () => {})
} catch (e) {
  console.log('unknown name:', (e as Error).message)
}

console.log('--- pending acks')
const q = Settle.open({
  schema: 'CREATE TABLE t (block_number UInt64, k String);',
  maxPendingBatches: 1,
})
const first = await q.ingest({
  data: { t: [{ block_number: 1, k: 'a' }] },
  rollbackChain: [{ number: 1, hash: '0x1a' }],
  finalizedHead: { number: 0, hash: '0x00' },
})
try {
  await q.ingest({
    data: { t: [{ block_number: 2, k: 'a' }] },
    rollbackChain: [{ number: 2, hash: '0x2a' }],
    finalizedHead: { number: 0, hash: '0x00' },
  })
} catch (e) {
  console.log(e instanceof SettlePendingAckError, (e as SettlePendingAckError).sequence, (e as Error).message.replace(/\d+ms/, 'Nms'))
}
try {
  q.ack(5)
} catch (e) {
  console.log(e instanceof SettleWrongAckSequenceError, (e as SettleWrongAckSequenceError).expected, (e as SettleWrongAckSequenceError).got)
}
q.ack(first!.sequence)
q.ack(first!.sequence)
console.log('double ack: ok')
try {
  q.ack(-1)
} catch (e) {
  console.log((e as Error).name, (e as Error).message)
}

console.log('--- heartbeat joins an unacked batch')
const h = Settle.open({ schema: 'CREATE TABLE t (block_number UInt64, k String);' })
const hb = await h.ingest({
  data: { t: [{ block_number: 1, k: 'a' }] },
  rollbackChain: [{ number: 1, hash: '0x1a' }],
  finalizedHead: { number: 0, hash: '0x00' },
})
await h.ingest({ data: {}, finalizedHead: { number: 1, hash: '0x1a' } })
console.log('unackedBatches:', h.unackedBatches)
try {
  await h.ingest({ data: {}, finalizedHead: { number: 1, hash: '0x1a' } })
} catch (e) {
  console.log('third call:', (e as Error).name)
}
h.ack(hb!.sequence)
console.log('after ack:', h.unackedBatches)
