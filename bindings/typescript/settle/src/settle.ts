import { Decoder, Encoder } from '@msgpack/msgpack'
import type { ReducerCtx } from './ddl'
// Import from src/native/ directly — not copied to dist/ to avoid stale binaries
import { Settle as NativeSettle } from '../src/native/native.js'
import {
  rethrowSettleError,
  SettlePendingAckError,
  SettleWrongAckSequenceError,
} from './errors'
import { groupBatchCallback } from './reducer-callback'

const encoder = new Encoder({ useBigInt64: true })
const decoder = new Decoder({ useBigInt64: true })

const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER)

/**
 * Decode a change batch. 64-bit integers decode as BigInt, so none loses
 * precision; each one that a number holds exactly becomes a number again.
 */
function decodeBatch(buf: Uint8Array): ChangeBatch {
  return toSafeNumbers(decoder.decode(buf)) as ChangeBatch
}

function toSafeNumbers(value: unknown): unknown {
  if (typeof value === 'bigint') {
    return value >= -MAX_SAFE && value <= MAX_SAFE ? Number(value) : value
  }
  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i++) {
      value[i] = toSafeNumbers(value[i])
    }
  } else if (value !== null && typeof value === 'object' && !ArrayBuffer.isView(value)) {
    const fields = value as Record<string, unknown>
    for (const key in fields) {
      fields[key] = toSafeNumbers(fields[key])
    }
  }
  return value
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
  /** Disable RocksDB automatic background compactions. */
  disableCompaction?: boolean
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

export type { ReducerCtx } from './ddl'
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

  private constructor(native: InstanceType<typeof NativeSettle>) {
    this.#native = native
  }

  static open(config: SettleConfig): Settle {
    return new Settle(NativeSettle.open(config))
  }

  async ingest(input: IngestInput): Promise<ChangeBatch | null> {
    const t0 = performance.now()
    const encoded = Buffer.from(encoder.encode(input.data))
    const encodeMs = performance.now() - t0

    let buf: ReturnType<InstanceType<typeof NativeSettle>['ingest']>
    try {
      buf = this.#native.ingest({
        data: encoded,
        rollbackChain: input.rollbackChain,
        finalizedHead: input.finalizedHead,
      })
    } catch (e) {
      rethrowSettleError(e)
    }

    const t1 = performance.now()
    const batch = buf ? decodeBatch(buf) : null
    const decodeMs = performance.now() - t1

    if (batch) {
      batch.perf.unshift(
        { kind: 'pipeline', name: 'msgpack_encode', durationMs: encodeMs, children: [] },
      )
      batch.perf.push(
        { kind: 'pipeline', name: 'msgpack_decode', durationMs: decodeMs, children: [] },
      )
    }

    return batch
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

    const batchFn = groupBatchCallback<TState, TRow, TEmit>(reduce)

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
    const batchFn = groupBatchCallback<TState, TRow, TEmit>(reduce)

    try {
      this.#native.registerReducerCallback(name, batchFn)
    } catch (e) {
      rethrowSettleError(e)
    }
  }
}
