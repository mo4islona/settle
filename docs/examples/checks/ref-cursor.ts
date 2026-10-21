// A block number above 2^32 in a cursor.
import { Settle } from '@settle/stream'

const db = Settle.open({ schema: 'CREATE TABLE t (block_number UInt64, k String);' })
const n = 5_000_000_000
const b = await db.ingest({
  data: { t: [{ block_number: n, k: 'a' }] },
  rollbackChain: [{ number: n, hash: '0xa' }],
  finalizedHead: { number: n - 10, hash: '0xb' },
})
console.log('row block_number:', b!.tables.t[0].values.block_number)
console.log('latestHead:', b!.latestHead, 'finalizedHead:', b!.finalizedHead)
db.ack(b!.sequence)
console.log('cursor:', db.cursor)
