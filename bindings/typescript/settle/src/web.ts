/**
 * Browser entry point for @settle/stream.
 *
 * Usage:
 *   import { init, Settle } from '@settle/stream/web'
 *   await init()
 *   const db = Settle.open({ schema: '...' })
 *
 * Uses wasm backend — memory-only storage, no Lua, no RocksDB.
 * External reducers work via JS callbacks (same API as Node.js).
 *
 * Persistence options on `SettleConfig` (`dataDir`, `compression`,
 * `disableCompaction`, `cacheSize`) are not supported in WASM and will
 * cause `Settle.open` to throw — the WASM build is memory-only by design.
 */

// Re-export types that are shared with Node.js entry point
export type {
  ChangeBatch,
  ISettle,
  SettleConfig,
  SettleCursor,
  ChangeOp,
  ChangeRecord,
  ExternalReducerOptions,
  IngestInput,
  PerfNode,
  PerfNodeKind,
  StateFieldDef,
} from './settle'

export { SettlePendingAckError, SettleWrongAckSequenceError } from './errors'

// Re-export builder API (pure TS, works in both environments)
export * from './column'
export {
  type AggExpr,
  type AggProxy,
  type GroupByItem,
  type IntervalExpr,
  interval,
  type KeyRef,
  type ReducerCtx,
  type ReducerOptions,
  type SlidingWindowOptions,
  type ViewOptions,
} from './ddl'
export {
  type BuildOptions,
  type CompiledPipeline,
  type CompiledReducer,
  openCompiled,
  Pipeline,
  ReducerHandle,
  TableHandle,
  ViewHandle,
} from './pipeline'

// ─── WASM Settle wrapper ────────────────────────────────────────

import type {
  ChangeBatch,
  ExternalReducerOptions,
  IngestInput,
  ISettle,
  SettleConfig,
  SettleCursor,
} from './settle'
import { rethrowSettleError } from './errors'

// The wasm module is loaded lazily via init()
let wasmReady = false
let WasmSettle: typeof import('./wasm/settle.js').Settle

/**
 * Initialize the wasm module. Must be called once before creating Settle instances.
 *
 * @example
 * ```ts
 * import { init, Settle } from '@settle/stream/web'
 * await init()
 * const db = Settle.open({ schema: '...' })
 * ```
 */
export async function init(wasmUrl?: URL | string): Promise<void> {
  if (wasmReady) return
  const mod: any = await import('./wasm/settle.js')
  await mod.default(wasmUrl)
  WasmSettle = mod.Settle
  wasmReady = true
}

export class Settle implements ISettle {
  #native: any

  private constructor(native: any) {
    this.#native = native
  }

  /**
   * Open a Settle instance. `init()` must be awaited first.
   *
   * Mirrors the Node-side `Settle.open(config)` so the same code can target
   * both environments. WASM is memory-only — supplying any persistence option
   * (`dataDir`, `compression`, `disableCompaction`, `cacheSize`) throws.
   */
  static open(config: SettleConfig): Settle {
    if (!wasmReady) {
      throw new Error(
        'WASM module not initialized. Call `await init()` before `Settle.open(...)`.',
      )
    }
    if (
      config.dataDir !== undefined ||
      config.compression !== undefined ||
      config.disableCompaction !== undefined ||
      config.cacheSize !== undefined ||
      config.maxBufferSize !== undefined
    ) {
      throw new Error(
        'WASM build is memory-only and does not expose buffer tuning — `dataDir`, `compression`, `disableCompaction`, `cacheSize`, and `maxBufferSize` are not supported.',
      )
    }
    return new Settle(new WasmSettle(config.schema))
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
        options.name,
        options.source,
        options.groupBy,
        options.state,
        batchFn,
      )
    } catch (e) {
      rethrowSettleError(e)
    }
  }

  registerReducerCallback<TState = any, TRow = any, TEmit = any>(
    name: string,
    reduce: (state: any, row: TRow) => void,
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

  async ingest(input: IngestInput): Promise<ChangeBatch | null> {
    let result: ChangeBatch | null | undefined
    try {
      result = this.#native.ingest({
        data: input.data,
        rollbackChain: input.rollbackChain,
        finalizedHead: input.finalizedHead,
      })
    } catch (e) {
      rethrowSettleError(e)
    }
    return result ?? null
  }

  ack(sequence: number): void {
    if (!Number.isInteger(sequence) || sequence < 0 || sequence > Number.MAX_SAFE_INTEGER) {
      throw new RangeError(
        `ack sequence must be a non-negative safe integer, got ${sequence}`,
      )
    }
    try {
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
    return this.#native.cursor ?? null
  }

  resolveForkCursor(previousBlocks: SettleCursor[]): SettleCursor | null {
    return this.#native.resolveForkCursor(previousBlocks) ?? null
  }

  handleFork(previousBlocks: SettleCursor[]): { cursor: SettleCursor; batch: ChangeBatch | null } {
    let result: { cursor: SettleCursor; batch: ChangeBatch | null | undefined }
    try {
      result = this.#native.handleFork(previousBlocks)
    } catch (e) {
      rethrowSettleError(e)
    }
    return { cursor: result.cursor, batch: result.batch ?? null }
  }
}
