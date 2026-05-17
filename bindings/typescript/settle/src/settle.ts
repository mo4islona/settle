import { decode, Encoder } from '@msgpack/msgpack'
import type { ReducerCtx } from './ddl'
// Import from src/native/ directly — not copied to dist/ to avoid stale binaries
import { Settle as NativeSettle } from '../src/native/native.js'
import {
  rethrowSettleError,
  SettlePendingAckError,
  SettleWrongAckSequenceError,
} from './errors'

const encoder = new Encoder({ useBigInt64: true })

// ─── Types ───────────────────────────────────────────────────────

export interface SettleConfig {
  schema: string
  dataDir?: string
  maxBufferSize?: number
  /** Compression algorithm for RocksDB: "none", "snappy" (default), "zstd", "lz4". */
  compression?: 'none' | 'snappy' | 'zstd' | 'lz4'
  /** Disable RocksDB automatic background compactions. */
  disableCompaction?: boolean
  /** Block cache size in bytes. Omit for RocksDB default (~8MB per CF), 0 to disable. */
  cacheSize?: number
}

export interface SettleCursor {
  number: number
  hash: string
}

export type ChangeOp = 'insert' | 'update' | 'delete'

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
 * the caller MUST `await` apply it to the target (atomically or idempotently)
 * and then call `ack(batch.sequence)`. While a previously-returned batch
 * remains unacked, every mutating call throws `SettlePendingAckError`.
 */
export interface ISettle {
  ingest(input: IngestInput): Promise<ChangeBatch | null>
  /**
   * Commit the pending batch's writes durably. Throws:
   * - `SettleWrongAckSequenceError` if a pending batch exists but its
   *   sequence differs from `sequence` — protocol bug on the caller side.
   *   `isAwaitingAck` stays `true`.
   * - A generic `Error` on storage failure (disk full, I/O). On this path
   *   the pending slot is **preserved** and `isAwaitingAck` stays `true`;
   *   caller MUST retry by calling `ack(sequence)` with the SAME sequence
   *   (NOT by calling `ingest()`, which would throw `SettlePendingAckError`
   *   until the ack succeeds).
   * No-op (silently `Ok`) when `isAwaitingAck` is `false` — covers
   * double-ack after success and stale acks on startup.
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
   * `true` when a previous immediate-commit failure (e.g. heartbeat-style
   * ingest or fork that committed in one shot) corrupted the instance's
   * in-memory ↔ disk invariant. Once set, all mutating calls reject — the
   * only recovery is to drop this instance and reopen.
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
    const batch = buf ? (decode(buf) as ChangeBatch) : null
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
    const batch = result.batch ? (decode(result.batch) as ChangeBatch) : null
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

    const batchFn = (groups: { state: TState; rows: TRow[] }[]) => {
      return groups.map(({ state, rows }) => {
        let s = state
        const emits: any[] = []
        const ctx = Object.create(null)
        ctx.update = (newState: TState) => {
          s = newState
          for (const k of Object.keys(newState as any)) {
            ctx[k] = (newState as any)[k]
          }
        }
        ctx.emit = (row: TEmit) => {
          if (row != null) emits.push(row)
        }
        for (const k of Object.keys(state as any)) {
          ctx[k] = (state as any)[k]
        }
        for (const row of rows) {
          reduce(ctx, row)
        }
        return { state: s, emits }
      })
    }

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
    const batchFn = (groups: { state: TState; rows: TRow[] }[]) => {
      return groups.map(({ state, rows }) => {
        let s = state
        const emits: any[] = []
        const ctx = Object.create(null)
        ctx.update = (newState: TState) => {
          s = newState
          for (const k of Object.keys(newState as any)) {
            ctx[k] = (newState as any)[k]
          }
        }
        ctx.emit = (row: TEmit) => {
          if (row != null) emits.push(row)
        }
        for (const k of Object.keys(state as any)) {
          ctx[k] = (state as any)[k]
        }
        for (const row of rows) {
          reduce(ctx, row)
        }
        return { state: s, emits }
      })
    }

    try {
      this.#native.registerReducerCallback(name, batchFn)
    } catch (e) {
      rethrowSettleError(e)
    }
  }
}
