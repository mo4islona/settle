// Run twice with the same directory: `write` stores data, `change` and
// `same` open the directory again with another or the same schema.
import { Settle } from '@settle/stream'

const [phase, dir] = process.argv.slice(2)
const schemas: Record<string, string> = {
  write: 'CREATE TABLE t (block_number UInt64, k String); CREATE MATERIALIZED VIEW v AS SELECT k, count() AS c FROM t GROUP BY k;',
  same: 'CREATE TABLE t (block_number UInt64, k String); CREATE MATERIALIZED VIEW v AS SELECT k, count() AS rows FROM t GROUP BY k; CREATE MATERIALIZED VIEW w AS SELECT k, count() AS c FROM t GROUP BY k;',
  change: 'CREATE TABLE t (block_number UInt64, k String, n Float64); CREATE MATERIALIZED VIEW v AS SELECT k, count() AS c FROM t GROUP BY k;',
}

try {
  const db = Settle.open({ schema: schemas[phase], dataDir: dir })
  console.log(phase, 'open: ok, cursor', db.cursor)
  if (phase === 'write') {
    const b = await db.ingest({
      data: { t: [{ block_number: 1, k: 'a' }] },
      rollbackChain: [{ number: 1, hash: '0x1a' }],
      finalizedHead: { number: 0, hash: '0x00' },
    })
    db.ack(b!.sequence)
  }
} catch (e) {
  console.log(phase, 'open:', (e as Error).message)
}
