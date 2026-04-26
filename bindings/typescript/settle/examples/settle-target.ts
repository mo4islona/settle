import { type ChangeBatch, Settle, type SettleCursor } from '../src/index'

export * from '../src/index'

export type Row = Record<string, any>

export interface SettleTargetOptions<TInput = Record<string, any[]>> {
  /** SQL schema definition (CREATE TABLE, CREATE REDUCER, CREATE MATERIALIZED VIEW). */
  schema: string
  /** RocksDB data directory. Enables persistence and resumption. */
  dataDir?: string
  /** Maximum change buffer size before backpressure. Default: 10000. */
  maxBufferSize?: number
  /**
   * Map decoder output to schema tables.
   * Returns Record<string, Row[]> where keys are table names from the schema.
   * Rows must contain `block_number`.
   * If omitted, data is passed through directly (keys must match table names).
   */
  transform?: (data: TInput) => Record<string, Row[]>
  /**
   * Called with each change batch (including rollback compensating changes).
   * Apply records to your downstream store.
   */
  onChange: (ctx: { batch: ChangeBatch; ctx: any }) => unknown | Promise<unknown>
}

/**
 * Creates a Pipes SDK Target that routes decoded blockchain data
 * through Settle's computation pipeline (raw tables → reducers → MVs)
 * and flushes change batches to a downstream store.
 *
 * Each iteration is atomic — `db.ingest()` processes all tables, stores
 * block hashes, finalizes, and flushes in a single RocksDB WriteBatch.
 */
export function settleTarget<TInput = Record<string, any[]>>({
  schema,
  dataDir,
  maxBufferSize,
  transform,
  onChange,
}: SettleTargetOptions<TInput>): {
  write: (writer: {
    read: (cursor?: SettleCursor) => AsyncIterableIterator<{ data: TInput; ctx: any }>
    logger: any
  }) => Promise<void>
  fork: (previousBlocks: SettleCursor[]) => Promise<SettleCursor | null>
} {
  const db = Settle.open({ schema, dataDir, maxBufferSize })

  return {
    write: async ({ read }) => {
      for await (const { data, ctx } of read(db.cursor ?? undefined)) {
        const mapped = transform ? transform(data) : (data as Record<string, any[]>)

        if (!ctx.head.finalized) {
          throw new Error('ctx.head.finalized is required — source must provide finalization info')
        }

        const batch = await db.ingest({
          data: mapped,
          rollbackChain: ctx.state.rollbackChain,
          finalizedHead: ctx.head.finalized,
        })

        if (batch) {
          await onChange({ batch, ctx })
          db.ack(batch.sequence)
        }
      }
    },

    fork: async (previousBlocks) => {
      if (!previousBlocks.length) return null

      const forkCursor = db.resolveForkCursor(previousBlocks)
      if (forkCursor == null) {
        throw new Error('Fork too deep: no common ancestor found in block hashes')
      }

      db.rollback(forkCursor.number)

      const batch = db.flush()
      if (batch) {
        await onChange({ batch, ctx: null })
        db.ack(batch.sequence)
      }

      return forkCursor
    },
  }
}
