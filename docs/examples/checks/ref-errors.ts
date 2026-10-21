// Checks the errors that each call gives, and the state after them.
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Settle } from '@settle/stream'

const SCHEMA = `
  CREATE TABLE t (block_number UInt64, k String, n Float64);
  CREATE MATERIALIZED VIEW v AS SELECT k, sum(n) AS total FROM t GROUP BY k;
`
const cursor = (n: number, s = 'a') => ({ number: n, hash: `0x${n}${s}` })

async function attempt(label: string, call: () => unknown) {
  try {
    const r = await call()
    console.log(`${label}: ok`, r && typeof r === 'object' && 'sequence' in r ? `(batch ${(r as any).sequence})` : '')
    if (r && typeof r === 'object' && 'sequence' in r) lastSequence = (r as any).sequence
  } catch (e) {
    const err = e as Error & { code?: string }
    console.log(`${label}: ${err.name}${err.code ? ` [${err.code}]` : ''}: ${err.message.split('\n')[0]}`)
  }
}

let lastSequence = 0
const db = Settle.open({ schema: SCHEMA })

await attempt('first ingest', () =>
  db.ingest({
    data: { t: [{ block_number: 5, k: 'a', n: 1 }] },
    rollbackChain: [cursor(4), cursor(5)],
    finalizedHead: cursor(2),
  }),
)
db.ack(lastSequence)

await attempt('block not above latest', () =>
  db.ingest({ data: { t: [{ block_number: 5, k: 'a', n: 1 }] }, rollbackChain: [cursor(5)], finalizedHead: cursor(2) }),
)
await attempt('finality goes back', () =>
  db.ingest({ data: { t: [{ block_number: 6, k: 'a', n: 1 }] }, rollbackChain: [cursor(6)], finalizedHead: cursor(1) }),
)
await attempt('unknown table with rows', () =>
  db.ingest({ data: { nope: [{ block_number: 6 }] }, rollbackChain: [cursor(6)], finalizedHead: cursor(2) }),
)
await attempt('unknown table without rows', () =>
  db.ingest({ data: { nope: [] }, rollbackChain: [], finalizedHead: cursor(2) }),
)
await attempt('row without block_number', () =>
  db.ingest({ data: { t: [{ k: 'a', n: 1 }] }, rollbackChain: [cursor(6)], finalizedHead: cursor(2) }),
)
await attempt('unknown column in a row', () =>
  db.ingest({ data: { t: [{ block_number: 6, k: 'a', n: 1, extra: 3 }] }, rollbackChain: [cursor(6)], finalizedHead: cursor(2) }),
)
if (db.unackedBatches) db.ack(lastSequence)

await attempt('fork with no common block', () => db.handleFork([cursor(6, 'x'), cursor(5, 'x')]))
await attempt('fork with an empty chain', () => db.handleFork([]))
await attempt('fork below finality (ingest)', () =>
  db.ingest({ data: { t: [{ block_number: 7, k: 'a', n: 1 }] }, rollbackChain: [cursor(2, 'x'), cursor(7)], finalizedHead: cursor(2) }),
)
if (db.unackedBatches) db.ack(lastSequence)

console.log('--- a fork inside ingest')
await attempt('ingest with a new hash for block 6', () =>
  db.ingest({ data: { t: [{ block_number: 6, k: 'a', n: 10 }] }, rollbackChain: [cursor(5), cursor(6, 'b')], finalizedHead: cursor(2) }),
)
db.ack(lastSequence)
console.log('cursor:', db.cursor)

console.log('--- bad config')
await attempt('compression gzip', () => Settle.open({ schema: SCHEMA, dataDir: join(tmpdir(), 'settle-bad'), compression: 'gzip' as any }))
await attempt('negative cacheSize', () => Settle.open({ schema: SCHEMA, cacheSize: -1 }))
await attempt('bad schema', () => Settle.open({ schema: 'CREATE TABLE t (block_number UInt64, c Nope);' }))

console.log('--- a reducer that throws')
{
  const r = Settle.open({
    schema: `
      CREATE TABLE t (block_number UInt64, k String, n Float64);
      CREATE REDUCER r SOURCE t GROUP BY k STATE (x Float64 DEFAULT 0) LANGUAGE EXTERNAL;
      CREATE MATERIALIZED VIEW v AS SELECT k, last(x) AS x FROM r GROUP BY k;`,
  })
  r.registerReducerCallback('r', (state: any, row: any) => {
    if (row.n < 0) throw new Error('negative amount')
    state.update({ x: row.n })
    state.emit({ x: row.n })
  })
  await attempt('throw in reduce', () =>
    r.ingest({ data: { t: [{ block_number: 1, k: 'a', n: -1 }] }, rollbackChain: [cursor(1)], finalizedHead: cursor(0) }),
  )
  console.log('isPoisoned:', r.isPoisoned)
  await attempt('next ingest', () =>
    r.ingest({ data: { t: [{ block_number: 1, k: 'a', n: 2 }] }, rollbackChain: [cursor(1)], finalizedHead: cursor(0) }),
  )
}

console.log('--- external reducer without a callback')
{
  const r = Settle.open({
    schema: `
      CREATE TABLE t (block_number UInt64, k String, n Float64);
      CREATE REDUCER r SOURCE t GROUP BY k STATE (x Float64 DEFAULT 0) LANGUAGE EXTERNAL;`,
  })
  await attempt('ingest', () =>
    r.ingest({ data: { t: [{ block_number: 1, k: 'a', n: 1 }] }, rollbackChain: [cursor(1)], finalizedHead: cursor(0) }),
  )
}

console.log('--- dataDir :memory: with Settle.open')
{
  const before = process.cwd()
  const dir = mkdtempSync(join(tmpdir(), 'settle-mem-'))
  process.chdir(dir)
  Settle.open({ schema: SCHEMA, dataDir: ':memory:' })
  const { readdirSync } = await import('node:fs')
  console.log('directory entries:', readdirSync(dir))
  process.chdir(before)
  rmSync(dir, { recursive: true, force: true })
}
