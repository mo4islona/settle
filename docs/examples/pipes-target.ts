import { createTarget } from '@subsquid/pipes'
import type { ChangeBatch, Settle, SettleCursor } from '@settle/stream'

type Rows = Record<string, Record<string, unknown>[]>

// [!region target]
export function settleTarget<T>(options: {
  db: Settle
  toRows: (data: T) => Rows
  apply: (batch: ChangeBatch) => Promise<void>
}) {
  const { db, toRows, apply } = options

  const cursor = (block: { number: number; hash?: string }): SettleCursor => ({
    number: block.number,
    hash: block.hash ?? '',
  })

  async function applyAndAck(batch: ChangeBatch | null) {
    if (!batch) return

    await apply(batch)
    db.ack(batch.sequence)
  }

  return createTarget<T>({
    write: async ({ read }) => {
      const latest = db.cursor
      const start = latest ? { latest, finalized: null } : undefined

      for await (const { data, ctx } of read(start)) {
        const finalized = ctx.stream.head.finalized
        if (!finalized) {
          throw new Error('Settle needs a dataset with a finalized head')
        }

        const batch = await db.ingest({
          data: toRows(data),
          rollbackChain: ctx.stream.state.rollbackChain.map(cursor),
          finalizedHead: cursor(finalized),
        })

        await applyAndAck(batch)
      }
    },

    resolveFork: async (canonicalBlocks) => {
      const fork = db.handleFork(canonicalBlocks.map(cursor))

      await applyAndAck(fork.batch)
      return fork.cursor
    },
  })
}
// [!endregion target]
