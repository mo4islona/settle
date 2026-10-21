// Checks the aggregate functions: the result of each one, its type, and the
// effect of Null values.
import { Settle } from '@settle/stream'

const cursor = (n: number) => ({ number: n, hash: `0x${n}a` })

// [!region schema]
const db = Settle.open({
  schema: `
    CREATE VIRTUAL TABLE transfers (
        block_number UInt64,
        token        String,
        amount       UInt64,
        price        Float64,
        memo         String
    );

    CREATE MATERIALIZED VIEW token_stats AS
    SELECT
        token,
        count()       AS rows,
        count(memo)   AS memos,
        sum(amount)   AS total,
        avg(amount)   AS mean,
        min(amount)   AS smallest,
        max(price)    AS top_price,
        first(memo)   AS first_memo,
        last(memo)    AS last_memo
    FROM transfers
    GROUP BY token;
  `,
})
// [!endregion schema]

const batch = await db.ingest({
  data: {
    transfers: [
      { block_number: 1, token: 'USDC', amount: 10, price: 1.0, memo: null },
      { block_number: 1, token: 'USDC', amount: 25, price: 1.01, memo: 'rent' },
      { block_number: 2, token: 'USDC', amount: 5, price: 0.99, memo: 'tip' },
      { block_number: 2, token: 'USDC', amount: 7, price: 1.0, memo: null },
    ],
  },
  rollbackChain: [cursor(1), cursor(2)],
  finalizedHead: { number: 0, hash: '0x00' },
})

// [!region output]
const values = batch!.tables.token_stats[0].values
for (const [name, value] of Object.entries(values)) {
  console.log(name.padEnd(10), value)
}
// [!endregion output]
db.ack(batch!.sequence)

console.log('--- the max after a fork')
const fork = db.handleFork([{ number: 2, hash: '0x2b' }, cursor(1)])
console.log(fork.batch!.tables.token_stats[0].values)
db.ack(fork.batch!.sequence)

console.log('--- big sums')
const big = Settle.open({
  schema: `
    CREATE TABLE t (block_number UInt64, k String, n UInt64, u UInt256);
    CREATE MATERIALIZED VIEW v AS SELECT k, sum(n) AS n, sum(u) AS u, avg(u) AS avg_u FROM t GROUP BY k;`,
})
const b = await big.ingest({
  data: {
    t: [
      { block_number: 1, k: 'a', n: 2n ** 63n, u: 2n ** 70n },
      { block_number: 1, k: 'a', n: 2n ** 63n, u: 2n ** 70n },
    ],
  },
  rollbackChain: [cursor(1)],
  finalizedHead: { number: 0, hash: '0x00' },
})
console.log(b!.tables.v[0].values)

console.log('--- an Int64 sum below zero and a mix')
const neg = Settle.open({
  schema: `
    CREATE TABLE t (block_number UInt64, k String, n Int64);
    CREATE MATERIALIZED VIEW v AS SELECT k, sum(n) AS n, min(n) AS lo FROM t GROUP BY k;`,
})
const c = await neg.ingest({
  data: { t: [{ block_number: 1, k: 'a', n: -5 }, { block_number: 1, k: 'a', n: 2 }] },
  rollbackChain: [cursor(1)],
  finalizedHead: { number: 0, hash: '0x00' },
})
console.log(c!.tables.v[0].values)
