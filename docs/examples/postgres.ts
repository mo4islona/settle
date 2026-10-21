import pg from 'pg'
import { type ChangeBatch, Pipeline, float64, string, uint64 } from '@settle/stream'

// [!region tables]
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL })

await pool.query(`
  CREATE TABLE IF NOT EXISTS pool_volume (
    pool   text PRIMARY KEY,
    volume double precision NOT NULL,
    swaps  bigint NOT NULL
  );

  CREATE TABLE IF NOT EXISTS settle_sequence (
    id       int PRIMARY KEY,
    sequence bigint NOT NULL
  );

  INSERT INTO settle_sequence VALUES (1, 0) ON CONFLICT DO NOTHING;
`)
// [!endregion tables]

// [!region open]
const p = new Pipeline()

p.table(
  'swaps',
  { block_number: uint64(), pool: string(), amount: float64() },
  { virtual: true },
).createView('pool_volume', {
  groupBy: 'pool',
  select: (agg) => ({
    pool: agg.key.pool,
    volume: agg.sum('amount'),
    swaps: agg.count(),
  }),
})

const { rows } = await pool.query('SELECT sequence FROM settle_sequence WHERE id = 1')

const db = p.build({
  dataDir: './settle-data',
  appliedSequence: Number(rows[0].sequence),
})
// [!endregion open]

// [!region apply]
async function apply(batch: ChangeBatch | null) {
  if (!batch) return

  const client = await pool.connect()
  try {
    await client.query('BEGIN')

    for (const record of batch.tables.pool_volume ?? []) {
      if (record.operation === 'delete') {
        await client.query('DELETE FROM pool_volume WHERE pool = $1', [record.key.pool])
        continue
      }

      const { pool: name, volume, swaps } = record.values
      await client.query(
        `INSERT INTO pool_volume (pool, volume, swaps) VALUES ($1, $2, $3)
         ON CONFLICT (pool) DO UPDATE SET volume = $2, swaps = $3`,
        [name, volume, swaps],
      )
    }

    await client.query('UPDATE settle_sequence SET sequence = $1 WHERE id = 1', [batch.sequence])
    await client.query('COMMIT')
  } catch (error) {
    await client.query('ROLLBACK')
    throw error
  } finally {
    client.release()
  }

  if (process.argv.includes('--crash')) {
    console.log(`batch ${batch.sequence} is in Postgres, stop before the ack`)
    process.exit(1)
  }

  db.ack(batch.sequence)
}
// [!endregion apply]

// A source of three blocks for each run. A real app reads them from the chain.
function swapsOf(block: number) {
  const pools = ['ETH/USDC', 'BTC/USDC']
  return [{ block_number: block, pool: pools[block % 2], amount: 100 }]
}

// [!region loop]
console.log('continue after:', db.cursor)

const from = (db.cursor?.number ?? 0) + 1

for (let block = from; block < from + 3; block++) {
  const batch = await db.ingest({
    data: { swaps: swapsOf(block) },
    rollbackChain: [{ number: block, hash: `0x${block}` }],
    finalizedHead: { number: block - 1, hash: `0x${block - 1}` },
  })

  await apply(batch)
}
// [!endregion loop]

const sequence = await pool.query('SELECT sequence::int FROM settle_sequence')
const volumes = await pool.query('SELECT pool, volume, swaps::int FROM pool_volume ORDER BY pool')
console.log('sequence in Postgres:', sequence.rows[0].sequence)
console.log(volumes.rows)

await pool.end()
