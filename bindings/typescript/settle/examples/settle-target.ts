import { type ChangeBatch, Settle, type SettleCursor } from '../src/index'

export * from '../src/index'

export type Row = Record<string, any>

// The batch that `onChange` applies, and the one ingested next.
const MAX_PENDING_BATCHES = 2

/**
 * `promise`, or a rejection with the reason once `signal` aborts. The abort
 * listener goes when `promise` settles: a handler on a promise that lives as
 * long as the loop would keep every read, and its data, alive.
 */
function unlessAborted<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(signal.reason)
    const settled = () => signal.removeEventListener('abort', onAbort)

    // Attached first, also when `signal` is already aborted: a rejection of
    // `promise` must never go unhandled.
    promise.then(
      (value) => {
        settled()
        resolve(value)
      },
      (error) => {
        settled()
        reject(error)
      },
    )

    if (signal.aborted) {
      onAbort()
    } else {
      signal.addEventListener('abort', onAbort, { once: true })
    }
  })
}

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
   * Apply records to your downstream store. Calls never overlap and come in
   * batch order; the next batch is ingested while one runs.
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
 *
 * Settle ingests the next data while `onChange` applies the batch before, so
 * neither side waits for the other. Each batch is acked after its
 * `onChange`; a restart resumes after the last acked one.
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
  const db = Settle.open({ schema, dataDir, maxBufferSize, maxPendingBatches: MAX_PENDING_BATCHES })

  // The newest batch handed to `onChange`, applied after all earlier ones.
  let applying: Promise<void> = Promise.resolve()

  // Aborted with the first failed `onChange` or ack, also while `write`
  // waits for the source.
  const failure = new AbortController()

  // Queue `batch` behind the batches before it, in the order Settle returned
  // them, and return the batch before. Call it as soon as the Settle call
  // returns: `fork` waits for `applying` before `handleFork`, so its batch
  // lands behind this one.
  const apply = (batch: ChangeBatch, ctx: any): Promise<void> => {
    const previous = applying
    applying = (async () => {
      await previous
      await onChange({ batch, ctx })
      db.ack(batch.sequence)
    })()
    // Marked handled here; the next `await` on it rethrows the error.
    applying.catch((error) => failure.abort(error))
    return previous
  }

  return {
    write: async ({ read }) => {
      const source = read(db.cursor ?? undefined)[Symbol.asyncIterator]()
      try {
        for (;;) {
          const next = await unlessAborted(source.next(), failure.signal)
          if (next.done) break

          const { data, ctx } = next.value
          const mapped = transform ? transform(data) : (data as Record<string, any[]>)

          if (!ctx.head.finalized) {
            throw new Error('ctx.head.finalized is required — source must provide finalization info')
          }

          // A call that returns no batch takes a place behind an unacked one.
          if (db.unackedBatches >= MAX_PENDING_BATCHES) {
            await applying
          }

          const batch = await db.ingest({
            data: mapped,
            rollbackChain: ctx.state.rollbackChain,
            finalizedHead: ctx.head.finalized,
          })

          // Wait for the batch before, so at most this one and the next are
          // unacked.
          if (batch) {
            await apply(batch, ctx)
          }
        }
      } catch (error) {
        // Not awaited: a source that waits for data would hold the error.
        source.return?.().catch(() => {})
        // The batches handed to `onChange` get their acks before the caller
        // can reopen, also on a poisoned instance: a reopen goes back to the
        // last acked batch and cannot undo the others in the store.
        await applying.catch(() => {})
        throw error
      }
      await applying
    },

    fork: async (previousBlocks) => {
      if (!previousBlocks.length) return null

      // The compensating batch undoes the batches before it, so they must
      // reach the store first.
      await applying

      // handleFork() resolves the highest common ancestor, atomically rolls
      // back state above it, and returns the compensating change batch.
      const { cursor: forkCursor, batch } = db.handleFork(previousBlocks)

      if (batch) {
        apply(batch, null)
        await applying
      }

      return forkCursor
    },
  }
}
