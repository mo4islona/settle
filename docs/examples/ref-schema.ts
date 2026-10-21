// Checks the schema language: the statements, the Lua body, the default
// output names and the constructs that open refuses.
import { Settle } from '@settle/stream'

const cursor = (n: number) => ({ number: n, hash: `0x${n}a` })

function tryOpen(label: string, schema: string) {
  try {
    Settle.open({ schema })
    console.log(`${label}: ok`)
  } catch (e) {
    console.log(`${label}: ${(e as Error).message}`)
  }
}

// [!region schema]
const schema = `
CREATE VIRTUAL TABLE trades (
    block_number UInt64,
    pool         String,
    price        Float64,
    amount       Float64
);

CREATE MODULE fees LANGUAGE lua AS $$
    return { rate = 0.003 }
$$;

CREATE REDUCER pool_fees
SOURCE trades
GROUP BY pool
STATE (
    total Float64 DEFAULT 0,
    seen  Map(String, Float64)
)
REQUIRE fees
LANGUAGE lua
PROCESS $$
    if row.amount <= 0 then return end

    local fee = row.amount * row.price * fees.rate
    state.total = state.total + fee
    state.seen:set(tostring(row.price), fee)

    emit({ fee = fee, total = state.total })
$$;

CREATE MATERIALIZED VIEW pool_summary AS
SELECT
    pool,
    sum(fee),
    count(),
    max(total) AS max_total
FROM pool_fees
GROUP BY pool;
`
// [!endregion schema]

const db = Settle.open({ schema })
const batch = await db.ingest({
  data: {
    trades: [
      { block_number: 1, pool: 'ETH/USDC', price: 2000, amount: 1 },
      { block_number: 1, pool: 'ETH/USDC', price: 2000, amount: 0 },
      { block_number: 2, pool: 'ETH/USDC', price: 1000, amount: 2 },
    ],
  },
  rollbackChain: [cursor(1), cursor(2)],
  finalizedHead: { number: 0, hash: '0x00' },
})
// [!region output]
for (const record of batch!.tables.pool_summary) {
  console.log(record.operation, record.values)
}
// [!endregion output]
db.ack(batch!.sequence)

console.log('--- emit forms')
const base = `CREATE TABLE t (block_number UInt64, k String, n Float64);`
for (const [label, body] of [
  ['emit({...})', 'emit({ n = row.n })'],
  ['emit {...}', 'emit { n = row.n }'],
  ['emit.n = ...', 'emit.n = row.n'],
  ['two emits', 'emit({ n = row.n }) emit({ n = row.n * 2 })'],
  ['empty emit', 'emit({})'],
  ['integer emit', 'emit({ n = 3 })'],
]) {
  const s = Settle.open({
    schema: `${base}
      CREATE REDUCER r SOURCE t GROUP BY k STATE (x Float64 DEFAULT 0) LANGUAGE lua PROCESS $$ ${body} $$;
      CREATE MATERIALIZED VIEW v AS SELECT k, count() AS rows, sum(n) AS total FROM r GROUP BY k;`,
  })
  try {
    const b = await s.ingest({
      data: { t: [{ block_number: 1, k: 'a', n: 1.5 }] },
      rollbackChain: [cursor(1)],
      finalizedHead: { number: 0, hash: '0x00' },
    })
    console.log(label, '->', JSON.stringify(b?.tables.v?.[0]?.values ?? null))
  } catch (e) {
    console.log(label, '->', (e as Error).message)
  }
}

console.log('--- sandbox')
for (const name of ['os', 'io', 'require', 'load', 'string', 'math', 'table', 'json']) {
  const s = Settle.open({
    schema: `${base}
      CREATE REDUCER r SOURCE t GROUP BY k STATE (x Float64 DEFAULT 0) LANGUAGE lua PROCESS $$ emit({ k = row.k, ok = (${name} ~= nil) and 1 or 0 }) $$;
      CREATE MATERIALIZED VIEW v AS SELECT k, last(ok) AS ok FROM r GROUP BY k;`,
  })
  const b = await s.ingest({
    data: { t: [{ block_number: 1, k: 'a', n: 1 }] },
    rollbackChain: [cursor(1)],
    finalizedHead: { number: 0, hash: '0x00' },
  })
  console.log(name, b!.tables.v[0].values.ok === 1 ? 'present' : 'absent')
}

console.log('--- a reducer without GROUP BY')
{
  const s = Settle.open({
    schema: `${base}
      CREATE REDUCER r SOURCE t STATE (total Float64 DEFAULT 0) LANGUAGE lua PROCESS $$
        state.total = state.total + row.n
        emit({ total = state.total })
      $$;
      CREATE MATERIALIZED VIEW v AS SELECT k, last(total) AS total FROM r GROUP BY k;`,
  })
  const b = await s.ingest({
    data: { t: [{ block_number: 1, k: 'a', n: 1 }, { block_number: 1, k: 'b', n: 2 }] },
    rollbackChain: [cursor(1)],
    finalizedHead: { number: 0, hash: '0x00' },
  })
  for (const r of b!.tables.v) console.log(JSON.stringify(r.values))
}

console.log('--- a group-by column that the emit leaves out')
{
  const s = Settle.open({
    schema: `${base}
      CREATE REDUCER r SOURCE t GROUP BY k STATE (x Float64 DEFAULT 0) LANGUAGE lua PROCESS $$ emit({ n = row.n }) $$;
      CREATE MATERIALIZED VIEW v AS SELECT k, sum(n) AS total FROM r GROUP BY k;`,
  })
  const b = await s.ingest({
    data: { t: [{ block_number: 1, k: 'a', n: 1 }] },
    rollbackChain: [cursor(1)],
    finalizedHead: { number: 0, hash: '0x00' },
  })
  console.log(JSON.stringify(b!.tables.v[0].values))
}

console.log('--- json in Lua state')
{
  const s = Settle.open({
    schema: `${base}
      CREATE REDUCER r SOURCE t GROUP BY k STATE (doc Json DEFAULT '{"count": 0}') LANGUAGE lua PROCESS $$
        state.doc.count = state.doc.count + 1
        emit({ text = json.encode(state.doc) })
      $$;
      CREATE MATERIALIZED VIEW v AS SELECT k, last(text) AS text FROM r GROUP BY k;`,
  })
  const b = await s.ingest({
    data: { t: [{ block_number: 1, k: 'a', n: 1 }, { block_number: 1, k: 'a', n: 1 }] },
    rollbackChain: [cursor(1)],
    finalizedHead: { number: 0, hash: '0x00' },
  })
  console.log(JSON.stringify(b!.tables.v[0].values))
}

console.log('--- open errors')
tryOpen('WHERE', `${base} CREATE MATERIALIZED VIEW v AS SELECT k, sum(n) AS s FROM t WHERE n > 0 GROUP BY k;`)
tryOpen('JOIN', `${base} CREATE TABLE u (block_number UInt64, k String); CREATE MATERIALIZED VIEW v AS SELECT t.k, sum(n) AS s FROM t JOIN u ON t.k = u.k GROUP BY t.k;`)
tryOpen('ORDER BY', `${base} CREATE MATERIALIZED VIEW v AS SELECT k, sum(n) AS s FROM t GROUP BY k ORDER BY k;`)
tryOpen('no aggregate', `${base} CREATE MATERIALIZED VIEW v AS SELECT k FROM t GROUP BY k;`)
tryOpen('key not in GROUP BY', `${base} CREATE MATERIALIZED VIEW v AS SELECT k, n, sum(n) AS s FROM t GROUP BY k;`)
tryOpen('sum of String', `${base} CREATE MATERIALIZED VIEW v AS SELECT k, sum(k) AS s FROM t GROUP BY k;`)
tryOpen('view over view', `${base} CREATE MATERIALIZED VIEW v AS SELECT k, sum(n) AS s FROM t GROUP BY k; CREATE MATERIALIZED VIEW w AS SELECT k, sum(s) AS s2 FROM v GROUP BY k;`)
tryOpen('unknown source', `${base} CREATE MATERIALIZED VIEW v AS SELECT k, sum(n) AS s FROM nothing GROUP BY k;`)
tryOpen('unknown column', `${base} CREATE MATERIALIZED VIEW v AS SELECT k, sum(m) AS s FROM t GROUP BY k;`)
tryOpen('duplicate name', `${base} CREATE TABLE t (block_number UInt64);`)
tryOpen('WHEN body', `${base} CREATE REDUCER r SOURCE t GROUP BY k STATE (x Float64 DEFAULT 0) WHEN true THEN SET x = 1;`)
tryOpen('bad default', `${base} CREATE REDUCER r SOURCE t GROUP BY k STATE (x Boolean DEFAULT yes) LANGUAGE EXTERNAL;`)
tryOpen('Map with DEFAULT', `${base} CREATE REDUCER r SOURCE t GROUP BY k STATE (m Map(String, Float64) DEFAULT 0) LANGUAGE EXTERNAL;`)
tryOpen('missing module', `${base} CREATE REDUCER r SOURCE t GROUP BY k STATE (x Float64 DEFAULT 0) REQUIRE nope LANGUAGE lua PROCESS $$ $$;`)
tryOpen('reducer cycle', `${base} CREATE REDUCER a SOURCE b STATE (x Float64 DEFAULT 0) LANGUAGE EXTERNAL; CREATE REDUCER b SOURCE a STATE (x Float64 DEFAULT 0) LANGUAGE EXTERNAL;`)
tryOpen('sliding on String', `${base} CREATE MATERIALIZED VIEW v AS SELECT k, sum(n) AS s FROM t GROUP BY k WINDOW SLIDING INTERVAL 1 HOUR BY k;`)
tryOpen('sliding ok', `${base} CREATE TABLE w (block_number UInt64, ts DateTime, k String, n Float64); CREATE MATERIALIZED VIEW v AS SELECT k, sum(n) AS s FROM w GROUP BY k WINDOW SLIDING INTERVAL 2 HOURS BY ts;`)
tryOpen('tumbling', `${base} CREATE TABLE w (block_number UInt64, ts DateTime, k String, n Float64); CREATE MATERIALIZED VIEW v AS SELECT k, toStartOfInterval(ts, INTERVAL 5 MINUTE) AS bucket, sum(n) AS s FROM w GROUP BY k, bucket;`)
tryOpen('count(col)', `${base} CREATE MATERIALIZED VIEW v AS SELECT k, count(n) AS c FROM t GROUP BY k;`)
tryOpen('other statement', `${base} DROP TABLE t;`)
tryOpen('column options', `CREATE TABLE t (block_number UInt64 NOT NULL PRIMARY KEY, k String);`)
