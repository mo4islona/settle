// An emit column that has the name of an integer state field.
import { Settle } from '@settle/stream'

const c1 = [{ number: 1, hash: '0x1a' }]
const f0 = { number: 0, hash: '0x00' }

for (const [label, emit] of [
  ['number, same name', (s: any) => ({ n: s.n })],
  ['bigint, same name', (s: any) => ({ n: BigInt(s.n) })],
  ['number, other name', (s: any) => ({ count: s.n })],
] as const) {
  const db = Settle.open({
    schema: `CREATE TABLE t (block_number UInt64, k String);
      CREATE REDUCER r SOURCE t GROUP BY k STATE (n UInt64 DEFAULT 0) LANGUAGE EXTERNAL;
      CREATE MATERIALIZED VIEW v AS SELECT k, sum(${label.includes('other') ? 'count' : 'n'}) AS s, max(${label.includes('other') ? 'count' : 'n'}) AS m FROM r GROUP BY k;`,
  })
  db.registerReducerCallback('r', (state: any) => {
    state.update({ n: state.n + 1 })
    state.emit(emit(state))
  })
  const b = await db.ingest({ data: { t: [{ block_number: 1, k: 'a' }, { block_number: 1, k: 'a' }] }, rollbackChain: c1, finalizedHead: f0 })
  console.log(label.padEnd(20), JSON.stringify(b?.tables.v?.[0]?.values ?? "no row"))
}

const lua = Settle.open({
  schema: `CREATE TABLE t (block_number UInt64, k String);
    CREATE REDUCER r SOURCE t GROUP BY k STATE (n UInt64 DEFAULT 0) LANGUAGE lua PROCESS $$
      state.n = state.n + 1
      emit({ n = state.n })
    $$;
    CREATE MATERIALIZED VIEW v AS SELECT k, sum(n) AS s, max(n) AS m FROM r GROUP BY k;`,
})
const b = await lua.ingest({ data: { t: [{ block_number: 1, k: 'a' }, { block_number: 1, k: 'a' }] }, rollbackChain: c1, finalizedHead: f0 })
console.log('lua, same name'.padEnd(20), JSON.stringify(b?.tables.v?.[0]?.values ?? "no row"))
