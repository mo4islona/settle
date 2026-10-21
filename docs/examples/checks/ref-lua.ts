// Runs the Lua snippets of the Lua reference page.
import { Settle } from '@settle/stream'

const f0 = { number: 0, hash: '0x00' }
const c = (n: number) => ({ number: n, hash: `0x${n}a` })

async function run(label: string, schema: string, data: Record<string, any[]>, view = 'v') {
  try {
    const db = Settle.open({ schema })
    const blocks = [...new Set(Object.values(data).flat().map((r) => r.block_number))]
    const b = await db.ingest({ data, rollbackChain: blocks.map(c), finalizedHead: f0 })
    for (const r of b?.tables[view] ?? []) console.log(label, JSON.stringify(r.values, (_, v) => (typeof v === 'bigint' ? `${v}n` : v)))
  } catch (e) {
    console.log(label, 'error:', (e as Error).message.split('\n')[0])
  }
}

await run('market_stats', `
CREATE TABLE orders (block_number UInt64, asset_id String, usdc UInt64, shares UInt64);
CREATE REDUCER market_stats
SOURCE orders
GROUP BY asset_id
STATE (
    volume Float64 DEFAULT 0,
    trades UInt64  DEFAULT 0
)
LANGUAGE lua
PROCESS $$
    if row.shares == 0 then return end

    local volume = row.usdc / 1000000
    state.volume = state.volume + volume
    state.trades = state.trades + 1

    emit({ volume = volume, trades = state.trades })
$$;
CREATE MATERIALIZED VIEW v AS SELECT asset_id, sum(volume) AS volume, max(trades) AS trades FROM market_stats GROUP BY asset_id;`,
  { orders: [
    { block_number: 1, asset_id: 'x', usdc: 2_500_000, shares: 5 },
    { block_number: 1, asset_id: 'x', usdc: 1_000_000, shares: 0 },
    { block_number: 2, asset_id: 'x', usdc: 500_000, shares: 1 },
  ] })

await run('numbers', `
CREATE TABLE t (block_number UInt64, k String, n UInt64);
CREATE REDUCER r SOURCE t GROUP BY k STATE (x Float64 DEFAULT 0) LANGUAGE lua PROCESS $$
  emit { a = row.n // 2, b = row.n / 2, c = 3, d = math.type(row.n) }
$$;
CREATE MATERIALIZED VIEW v AS SELECT k, last(a) AS a, last(b) AS b, last(c) AS c, last(d) AS d FROM r GROUP BY k;`,
  { t: [{ block_number: 1, k: 'a', n: 7 }] })

await run('json emit', `
CREATE TABLE t (block_number UInt64, k String);
CREATE REDUCER r SOURCE t GROUP BY k STATE (doc JSON DEFAULT '{"count": 0}') LANGUAGE lua PROCESS $$
  state.doc.count = state.doc.count + 1
  emit({ doc = state.doc })
$$;
CREATE MATERIALIZED VIEW v AS SELECT k, last(doc) AS doc FROM r GROUP BY k;`,
  { t: [{ block_number: 1, k: 'a' }, { block_number: 1, k: 'a' }] })

await run('map', `
CREATE TABLE t (block_number UInt64, user String, token String, amount Float64);
CREATE REDUCER r SOURCE t GROUP BY user STATE (book Map(String, JSON)) LANGUAGE lua PROCESS $$
  local p = state.book:get(row.token) or { qty = 0 }
  p.qty = p.qty + row.amount
  if p.qty == 0 then state.book:delete(row.token) else state.book:set(row.token, p) end
  emit({ token = row.token, qty = p.qty })
$$;
CREATE MATERIALIZED VIEW v AS SELECT user, token, last(qty) AS qty FROM r GROUP BY user, token;`,
  { t: [
    { block_number: 1, user: 'a', token: 'X', amount: 5 },
    { block_number: 1, user: 'a', token: 'X', amount: 2 },
    { block_number: 2, user: 'a', token: 'X', amount: -7 },
    { block_number: 2, user: 'a', token: 'X', amount: 1 },
  ] })

await run('map index', `
CREATE TABLE t (block_number UInt64, user String, token String);
CREATE REDUCER r SOURCE t GROUP BY user STATE (book Map(String, Float64)) LANGUAGE lua PROCESS $$
  local x = state.book[row.token]
  emit({ token = row.token })
$$;
CREATE MATERIALIZED VIEW v AS SELECT user, count() AS n FROM r GROUP BY user;`,
  { t: [{ block_number: 1, user: 'a', token: 'X' }] })

await run('map table value', `
CREATE TABLE t (block_number UInt64, user String, token String);
CREATE REDUCER r SOURCE t GROUP BY user STATE (book Map(String, Float64)) LANGUAGE lua PROCESS $$
  state.book:set(row.token, { a = 1 })
  emit({ token = row.token })
$$;
CREATE MATERIALIZED VIEW v AS SELECT user, count() AS n FROM r GROUP BY user;`,
  { t: [{ block_number: 1, user: 'a', token: 'X' }] })

await run('module', `
CREATE TABLE trades (block_number UInt64, pool String, amount Float64);
CREATE MODULE fees LANGUAGE lua AS $$
    return { rate = 0.003 }
$$;

CREATE REDUCER pool_fees
SOURCE trades
GROUP BY pool
STATE (total Float64 DEFAULT 0)
REQUIRE fees
LANGUAGE lua
PROCESS $$
    state.total = state.total + row.amount * fees.rate
    emit({ total = state.total })
$$;
CREATE MATERIALIZED VIEW v AS SELECT pool, last(total) AS total FROM pool_fees GROUP BY pool;`,
  { trades: [{ block_number: 1, pool: 'p', amount: 1000 }] })

await run('error', `
CREATE TABLE t (block_number UInt64, k String);
CREATE REDUCER r SOURCE t GROUP BY k STATE (x Float64 DEFAULT 0) LANGUAGE lua PROCESS $$
  local y = row.missing.field
$$;
CREATE MATERIALIZED VIEW v AS SELECT k, count() AS n FROM r GROUP BY k;`,
  { t: [{ block_number: 1, k: 'a' }] })
