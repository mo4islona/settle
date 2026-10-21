// Small checks for the reference pages.
import { Pipeline, Settle, float64, string, uint64 } from '@settle/stream'

const c1 = [{ number: 1, hash: '0x1a' }]
const f0 = { number: 0, hash: '0x00' }

console.log('--- a table without a block_number column')
try {
  const db = Settle.open({ schema: 'CREATE TABLE t (k String);' })
  const b = await db.ingest({ data: { t: [{ block_number: 1, k: 'a' }] }, rollbackChain: c1, finalizedHead: f0 })
  console.log(JSON.stringify(b!.tables.t[0]))
} catch (e) {
  console.log((e as Error).message)
}

console.log('--- an alias for a key column')
{
  const p = new Pipeline()
  p.table('t', { block_number: uint64(), to: string(), n: float64() }).createView('v', {
    groupBy: 'to',
    select: (agg) => ({ account: agg.key.to, total: agg.sum('n') }),
  })
  console.log(p.compile().schema)
  try {
    const db = p.build()
    const b = await db.ingest({ data: { t: [{ block_number: 1, to: 'a', n: 1 }] }, rollbackChain: c1, finalizedHead: f0 })
    console.log(JSON.stringify(b!.tables.v[0]))
  } catch (e) {
    console.log((e as Error).message)
  }
}

console.log('--- a groupBy column that select leaves out')
{
  const p = new Pipeline()
  p.table('t', { block_number: uint64(), k: string(), n: float64() }, { virtual: true }).createView('v', {
    groupBy: 'k',
    select: (agg) => ({ total: agg.sum('n') }),
  })
  try {
    const db = p.build()
    const b = await db.ingest({ data: { t: [{ block_number: 1, k: 'a', n: 1 }, { block_number: 1, k: 'b', n: 2 }] }, rollbackChain: c1, finalizedHead: f0 })
    for (const r of b!.tables.v) console.log(JSON.stringify(r))
  } catch (e) {
    console.log((e as Error).message)
  }
}

console.log('--- a reducer with groupBy []')
{
  const p = new Pipeline()
  p.table('t', { block_number: uint64(), k: string() }).createReducer('r', {
    groupBy: [],
    initialState: { n: 0 },
    reduce(state) {
      state.update({ n: state.n + 1 })
    },
  })
  try {
    p.build()
    console.log('ok')
  } catch (e) {
    console.log((e as Error).message)
  }
}

console.log('--- an emit of a value with another type than the state')
{
  const db = Settle.open({
    schema: `CREATE TABLE t (block_number UInt64, k String);
      CREATE REDUCER r SOURCE t GROUP BY k STATE (n UInt64 DEFAULT 0) LANGUAGE EXTERNAL;
      CREATE MATERIALIZED VIEW v AS SELECT k, last(n) AS n, sum(n) AS s FROM r GROUP BY k;`,
  })
  db.registerReducerCallback('r', (state: any) => {
    state.update({ n: state.n + 1 })
    state.emit({ n: state.n })
  })
  const b = await db.ingest({ data: { t: [{ block_number: 1, k: 'a' }, { block_number: 1, k: 'a' }] }, rollbackChain: c1, finalizedHead: f0 })
  console.log(JSON.stringify(b!.tables.v[0].values))
}
