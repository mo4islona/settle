// A JSON value in a Lua emit, and a direct state assignment in JS.
import { Settle } from '@settle/stream'

const f0 = { number: 0, hash: '0x00' }
for (const body of ['emit({ doc = state.doc })', 'emit({ doc = state.doc, n = 1 })', 'emit({ doc = json.encode(state.doc) })']) {
  const db = Settle.open({
    schema: `CREATE TABLE t (block_number UInt64, k String);
      CREATE REDUCER r SOURCE t GROUP BY k STATE (doc JSON DEFAULT '{"count": 0}') LANGUAGE lua PROCESS $$
        state.doc.count = state.doc.count + 1
        ${body}
      $$;
      CREATE MATERIALIZED VIEW v AS SELECT k, last(doc) AS doc, count() AS rows FROM r GROUP BY k;`,
  })
  const b = await db.ingest({ data: { t: [{ block_number: 1, k: 'a' }] }, rollbackChain: [{ number: 1, hash: '0x1a' }], finalizedHead: f0 })
  console.log(body, '->', JSON.stringify(b?.tables ?? null))
}

const js = Settle.open({
  schema: `CREATE TABLE t (block_number UInt64, k String);
    CREATE REDUCER r SOURCE t GROUP BY k STATE (n Float64 DEFAULT 0) LANGUAGE EXTERNAL;
    CREATE MATERIALIZED VIEW v AS SELECT k, last(n) AS n FROM r GROUP BY k;`,
})
js.registerReducerCallback('r', (state: any) => {
  state.n = state.n + 1
  state.emit({ n: state.n })
})
for (const n of [1, 2]) {
  const b = await js.ingest({ data: { t: [{ block_number: n, k: 'a' }] }, rollbackChain: [{ number: n, hash: `0x${n}a` }], finalizedHead: f0 })
  console.log('direct assignment, block', n, '->', JSON.stringify(b?.tables.v?.[0]?.values ?? null))
  if (b) js.ack(b.sequence)
}
