import { readBatch, TableWriter } from './codec'
import type { ReducerCtx } from './ddl'
// Import from src/native/ directly — not copied to dist/ to avoid stale binaries
import { mapDelete, mapGet, mapSet, Settle as NativeSettle } from '../src/native/native.js'
import {
  rethrowSettleError,
  SettlePendingAckError,
  SettleWrongAckSequenceError,
} from './errors'
import { nativeBatchCallback } from './reducer-callback'
import type { MapAccess } from './state-map'

const nativeMaps: MapAccess = { mapGet, mapSet, mapDelete }

/** A change batch as native code returns it: `[buffer, strings, others]`. */
type EncodedBatch = [ArrayBuffer, string, unknown[] | null]

/**
 * Decode a change batch. An integer beyond `Number.MAX_SAFE_INTEGER`
 * arrives as a BigInt, so none loses precision.
 */
function decodeBatch(encoded: object): ChangeBatch {
  const [buffer, strings, others] = encoded as EncodedBatch
  return readBatch(buffer, strings, others)
}

// ─── Types ───────────────────────────────────────────────────────

export interface SettleConfig {
  schema: string
  dataDir?: string
  maxBufferSize?: number
  /**
   * Most change batches that may wait for `ack()` at a time (default 2).
   * With 2, `ingest()` can run while the target still applies the batch
   * before; 1 makes it wait for that ack. A call that returns no batch while
   * batches wait takes a place too. Each batch reaches disk only when it is
   * acked, in sequence order, so a restart goes back to the last acked
   * batch (with `appliedSequence`, to that batch). The target must apply
   * every batch in sequence order, also a fork's compensating batch.
   */
  maxPendingBatches?: number
  /** Compression algorithm for RocksDB: "none", "snappy" (default), "zstd", "lz4". */
  compression?: 'none' | 'snappy' | 'zstd' | 'lz4'
  /** Block cache size in bytes. Omit for RocksDB default (~8MB per CF), 0 to disable. */
  cacheSize?: number
  /**
   * The `sequence` of the newest change batch that the target applied; 0
   * before the first. The target stores it in the same transaction as the
   * data of that batch. Set, it turns on the journal: each batch that
   * `ingest()` or `handleFork()` returns is on disk first, and open brings
   * the database to the state after this batch, acked or not. Without it, a
   * restart goes back to the last acked batch, and a fork of blocks that the
   * target applied but did not ack leaves their changes in the target. The
   * web build keeps no data across restarts and ignores it.
   */
  appliedSequence?: number
}

export interface SettleCursor {
  number: number
  hash: string
}

export type ChangeOp = 'insert' | 'update' | 'delete'

/**
 * One changed row. An integer beyond `Number.MAX_SAFE_INTEGER` arrives as a
 * `bigint`, so that it keeps every digit; smaller ones arrive as numbers.
 */
export interface ChangeRecord {
  table: string
  operation: ChangeOp
  key: Record<string, any>
  values: Record<string, any>
  prevValues: Record<string, any> | null
}

export type PerfNodeKind = 'pipeline' | 'raw_table' | 'reducer' | 'mv' | 'parallel'

export interface PerfNode {
  kind: PerfNodeKind
  name: string
  durationMs: number
  children: PerfNode[]
}

export interface ChangeBatch {
  sequence: number
  finalizedHead: SettleCursor | null
  latestHead: SettleCursor | null
  tables: Record<string, ChangeRecord[]>
  perf: PerfNode[]
}

export interface IngestInput {
  data: Record<string, Record<string, any>[]>
  rollbackChain?: SettleCursor[]
  finalizedHead: SettleCursor
}

export interface StateFieldDef {
  name: string
  columnType: string
  defaultValue: string
}

export interface ExternalReducerOptions<TState = any, TRow = any, TEmit = any> {
  name: string
  source: string
  groupBy: string[]
  state: StateFieldDef[]
  reduce: (state: ReducerCtx<TState, TEmit>, row: TRow) => void
}

export type { ReducerCtx, StateMap } from './ddl'
export { SettlePendingAckError, SettleWrongAckSequenceError } from './errors'

/**
 * Common surface implemented by both the Node (NAPI) and Web (WASM) `Settle`
 * classes. Use this as the parameter / return type when writing code that
 * should run in either environment.
 *
 * Durability contract: `ingest()` and `handleFork()` may return a `ChangeBatch`;
 * the caller MUST apply it to the target (atomically or idempotently), in
 * sequence order, and then call `ack(batch.sequence)`. While
 * `unackedBatches` is `maxPendingBatches`, `ingest()` and `handleFork()`
 * throw `SettlePendingAckError`; while any batch is unacked, so do
 * `registerReducer` and `registerReducerCallback`.
 */
export interface ISettle {
  ingest(input: IngestInput): Promise<ChangeBatch | null>
  /**
   * Commit the writes of every batch up to `sequence` durably, oldest
   * first. Acks are cumulative. Throws:
   * - `SettleWrongAckSequenceError` if `sequence` is above the newest
   *   unacked batch (`expected`) — protocol bug on the caller side.
   *   Nothing is committed.
   * - A generic `Error` on storage failure (disk full, I/O). On this path
   *   the failed batch and all later ones stay unacked; caller MUST retry
   *   by calling `ack(sequence)` again (NOT by calling `ingest()`, which can
   *   throw `SettlePendingAckError` until the ack succeeds).
   * No-op (silently `Ok`) when no batch up to `sequence` is unacked —
   * covers double-ack after success and stale acks on startup.
   *
   * `sequence` must be a non-negative integer ≤ `Number.MAX_SAFE_INTEGER`.
   */
  ack(sequence: number): void
  /**
   * Register a brand-new external reducer + JS callback. **Strict**:
   * throws if a reducer with this name already exists (whether declared
   * in SQL via `LANGUAGE EXTERNAL` or registered previously). To attach
   * a callback to a reducer declared in SQL, use
   * `registerReducerCallback(name, callback)`. To change a registered
   * callback, drop and reopen the instance — silent hot-reload is not
   * supported.
   */
  registerReducer<TState = any, TRow = any, TEmit = any>(
    options: ExternalReducerOptions<TState, TRow, TEmit>,
  ): void
  /**
   * Attach a JS callback to an existing reducer declared in SQL with
   * `LANGUAGE EXTERNAL`, then replay any unfinalized blocks through it.
   * **Strict**: throws if no such reducer exists, AND throws if a
   * callback is already registered for that name. To change a registered
   * callback, drop and reopen the instance.
   */
  registerReducerCallback<TState = any, TRow = any, TEmit = any>(
    name: string,
    reduce: (state: ReducerCtx<TState, TEmit>, row: TRow) => void,
  ): void
  resolveForkCursor(previousBlocks: SettleCursor[]): SettleCursor | null
  handleFork(previousBlocks: SettleCursor[]): { cursor: SettleCursor; batch: ChangeBatch | null }
  readonly pendingCount: number
  readonly isBackpressured: boolean
  readonly isAwaitingAck: boolean
  /**
   * Places in use in the ack queue: each change batch that waits for
   * `ack()`, and each call without a batch whose writes wait with one.
   */
  readonly unackedBatches: number
  /**
   * `true` when a previous failure left memory in a state that no returned
   * batch describes. Once set, every call but `ack()` rejects. Ack the
   * batches that the target applied, then drop this instance and reopen.
   */
  readonly isPoisoned: boolean
  readonly cursor: SettleCursor | null
}

// ─── Settle class ───────────────────────────────────────────────

export class Settle implements ISettle {
  #native: InstanceType<typeof NativeSettle>
  #rows = new TableWriter()
  /** The columns of each table name, or null for a name that the schema lacks. */
  #columns = new Map<string, readonly string[] | null>()

  private constructor(native: InstanceType<typeof NativeSettle>) {
    this.#native = native
  }

  static open(config: SettleConfig): Settle {
    return new Settle(NativeSettle.open(config))
  }

  async ingest(input: IngestInput): Promise<ChangeBatch | null> {
    const t0 = performance.now()
    const rows = this.#rows.encode(input.data, (table) => this.#tableColumns(table))
    const encodeMs = performance.now() - t0

    let encoded: ReturnType<InstanceType<typeof NativeSettle>['ingest']>
    try {
      encoded = this.#native.ingest(rows, {
        rollbackChain: input.rollbackChain,
        finalizedHead: input.finalizedHead,
      })
    } catch (e) {
      rethrowSettleError(e)
    }

    const t1 = performance.now()
    const batch = encoded ? decodeBatch(encoded) : null
    const decodeMs = performance.now() - t1

    if (batch) {
      batch.perf.unshift({ kind: 'pipeline', name: 'encode', durationMs: encodeMs, children: [] })
      batch.perf.push({ kind: 'pipeline', name: 'decode', durationMs: decodeMs, children: [] })
    }

    return batch
  }

  #tableColumns(table: string): readonly string[] | null {
    let columns = this.#columns.get(table)
    if (columns === undefined) {
      columns = this.#native.tableColumns(table) ?? null
      this.#columns.set(table, columns)
    }
    return columns
  }

  resolveForkCursor(previousBlocks: SettleCursor[]): SettleCursor | null {
    return this.#native.resolveForkCursor(previousBlocks)
  }

  handleFork(previousBlocks: SettleCursor[]): { cursor: SettleCursor; batch: ChangeBatch | null } {
    // NAPI exposes `batch` as `Buffer | undefined` (Option<Buffer>); let
    // TS infer the native shape rather than assert `Buffer | null`.
    let result: ReturnType<InstanceType<typeof NativeSettle>['handleFork']>
    try {
      result = this.#native.handleFork(previousBlocks)
    } catch (e) {
      rethrowSettleError(e)
    }
    const batch = result.batch ? decodeBatch(result.batch) : null
    return { cursor: result.cursor, batch }
  }

  ack(sequence: number): void {
    if (!Number.isInteger(sequence) || sequence < 0 || sequence > Number.MAX_SAFE_INTEGER) {
      throw new RangeError(
        `ack sequence must be a non-negative safe integer, got ${sequence}`,
      )
    }
    try {
      // Native ack takes i64 in Rust; pass a JS number (precision exact up to 2^53).
      this.#native.ack(sequence)
    } catch (e) {
      rethrowSettleError(e)
    }
  }

  get pendingCount(): number {
    return this.#native.pendingCount
  }

  get isBackpressured(): boolean {
    return this.#native.isBackpressured
  }

  get isAwaitingAck(): boolean {
    return this.#native.isAwaitingAck
  }

  get unackedBatches(): number {
    return this.#native.unackedBatches
  }

  get isPoisoned(): boolean {
    return this.#native.isPoisoned
  }

  get cursor(): SettleCursor | null {
    return this.#native.cursor
  }

  registerReducer<TState = any, TRow = any, TEmit = any>(
    options: ExternalReducerOptions<TState, TRow, TEmit>,
  ): void {
    const { reduce } = options

    const batchFn = nativeBatchCallback<TState, TRow, TEmit>(reduce, nativeMaps)

    try {
      this.#native.registerReducer(
        {
          name: options.name,
          source: options.source,
          groupBy: options.groupBy,
          state: options.state,
        },
        batchFn,
      )
    } catch (e) {
      rethrowSettleError(e)
    }
  }

  registerReducerCallback<TState = any, TRow = any, TEmit = any>(
    name: string,
    reduce: (state: ReducerCtx<TState, TEmit>, row: TRow) => void,
  ): void {
    const batchFn = nativeBatchCallback<TState, TRow, TEmit>(reduce, nativeMaps)

    try {
      this.#native.registerReducerCallback(name, batchFn)
    } catch (e) {
      rethrowSettleError(e)
    }
  }
}
