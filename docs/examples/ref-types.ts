// Checks the column types: the schema names, the builder functions, the JS
// values that go in and the JS values that come out.
import { Pipeline, Settle, base58, boolean, bytes, datetime, float64, int64, json, string, uint256, uint64 } from '@settle/stream'

function describe(value: unknown): string {
  if (value instanceof Uint8Array) return `Uint8Array(${Array.from(value).join(',')})`
  if (typeof value === 'bigint') return `${value}n (bigint)`
  return `${JSON.stringify(value)} (${typeof value})`
}

async function tryOpen(label: string, schema: string) {
  try {
    Settle.open({ schema })
    console.log(`${label}: ok`)
  } catch (e) {
    console.log(`${label}: ${(e as Error).message}`)
  }
}

// [!region builder]
const p = new Pipeline()

p.table('values', {
  block_number: uint64(),
  u64: uint64(),
  i64: int64(),
  f64: float64(),
  u256: uint256(),
  text: string(),
  time: datetime(),
  flag: boolean(),
  b58: base58(),
})

const db = p.build()

const batch = await db.ingest({
  data: {
    values: [
      {
        block_number: 1,
        u64: 2n ** 60n,
        i64: -5,
        f64: 1.5,
        u256: 2n ** 200n,
        text: 'hello',
        time: Date.UTC(2026, 0, 1),
        flag: true,
        b58: 'abc',
      },
    ],
  },
  rollbackChain: [{ number: 1, hash: '0x1a' }],
  finalizedHead: { number: 0, hash: '0x00' },
})

const record = batch!.tables.values[0]
for (const [name, value] of Object.entries(record.values)) {
  console.log(name.padEnd(12), describe(value))
}
console.log('key'.padEnd(12), JSON.stringify(record.key))
// [!endregion builder]
db.ack(batch!.sequence)

console.log('--- small integers')
{
  const s = Settle.open({ schema: 'CREATE TABLE t (block_number UInt64, n UInt64);' })
  const b = await s.ingest({
    data: { t: [{ block_number: 1, n: 7 }, { block_number: 1, n: 2n ** 53n }] },
    rollbackChain: [{ number: 1, hash: '0x1a' }],
    finalizedHead: { number: 0, hash: '0x00' },
  })
  for (const r of b!.tables.t) console.log(describe(r.values.n))
}

console.log('--- builder bytes() and json()')
{
  const q = new Pipeline()
  q.table('blobs', { block_number: uint64(), data: bytes() })
  try {
    q.build()
    console.log('bytes(): ok')
  } catch (e) {
    console.log(`bytes(): ${(e as Error).message}`)
  }
  const r = new Pipeline()
  r.table('docs', { block_number: uint64(), doc: json() })
  try {
    r.build()
    console.log('json(): ok')
  } catch (e) {
    console.log(`json(): ${(e as Error).message}`)
  }
}

console.log('--- schema type names in CREATE TABLE')
for (const type of [
  'UInt64', 'Int64', 'BIGINT', 'Float64', 'DOUBLE', 'UInt256', 'Uint256', 'String', 'VARCHAR', 'TEXT',
  'DateTime', 'TIMESTAMP', 'Boolean', 'Bytes', 'BYTEA', 'VARBINARY', 'BLOB', 'Base58', 'JSON', 'JSONB', 'Json', 'uint64',
]) {
  await tryOpen(type, `CREATE TABLE t (block_number UInt64, c ${type});`)
}

console.log('--- schema type names in STATE')
for (const type of ['UInt64', 'BIGINT', 'Float64', 'DOUBLE', 'String', 'TEXT', 'Json', 'Boolean', 'DateTime']) {
  const def = type === 'String' || type === 'TEXT' || type === 'Json' ? "''" : type === 'Boolean' ? 'false' : '0'
  await tryOpen(
    `state ${type}`,
    `CREATE TABLE t (block_number UInt64, k String);
     CREATE REDUCER r SOURCE t GROUP BY k STATE (x ${type} DEFAULT ${def}) LANGUAGE EXTERNAL;`,
  )
}

console.log('--- bytes through SQL')
{
  const s = Settle.open({ schema: 'CREATE TABLE t (block_number UInt64, b BYTEA);' })
  const b = await s.ingest({
    data: { t: [{ block_number: 1, b: new Uint8Array([1, 2, 255]) }] },
    rollbackChain: [{ number: 1, hash: '0x1a' }],
    finalizedHead: { number: 0, hash: '0x00' },
  })
  console.log('b', describe(b!.tables.t[0].values.b))
}

console.log('--- json through a reducer state and a view')
{
  const q = new Pipeline()
  q.table('t', { block_number: uint64(), k: string(), tag: string() })
    .createReducer<{ tags: Record<string, number> }, { tags: Record<string, number>; big: bigint }>('r', {
      groupBy: 'k',
      initialState: { tags: {} as Record<string, number> },
      reduce(state, row) {
        const tags = { ...state.tags, [row.tag]: (state.tags[row.tag] ?? 0) + 1 }
        state.update({ tags })
        state.emit({ tags, big: 2n ** 70n })
      },
    })
    .createView('v', {
      groupBy: 'k',
      select: (agg) => ({ k: agg.key.k, tags: agg.last('tags'), big: agg.last('big') }),
    })
  const s = q.build()
  const b = await s.ingest({
    data: { t: [{ block_number: 1, k: 'a', tag: 'x' }, { block_number: 1, k: 'a', tag: 'x' }] },
    rollbackChain: [{ number: 1, hash: '0x1a' }],
    finalizedHead: { number: 0, hash: '0x00' },
  })
  const v = b!.tables.v[0].values
  console.log('tags', describe(v.tags))
  console.log('big', describe(v.big))
}

console.log('--- a string in a UInt64 column')
{
  const s = Settle.open({ schema: 'CREATE TABLE t (block_number UInt64, n UInt64);' })
  try {
    const b = await s.ingest({
      data: { t: [{ block_number: 1, n: '12' }] },
      rollbackChain: [{ number: 1, hash: '0x1a' }],
      finalizedHead: { number: 0, hash: '0x00' },
    })
    console.log('accepted:', describe(b!.tables.t[0].values.n))
  } catch (e) {
    console.log('refused:', (e as Error).message)
  }
}

console.log('--- null and missing values')
{
  const s = Settle.open({ schema: 'CREATE TABLE t (block_number UInt64, a String, n Float64);' })
  const b = await s.ingest({
    data: { t: [{ block_number: 1, a: null }] },
    rollbackChain: [{ number: 1, hash: '0x1a' }],
    finalizedHead: { number: 0, hash: '0x00' },
  })
  console.log(JSON.stringify(b!.tables.t[0].values))
}
