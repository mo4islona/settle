/**
 * Browser entry point for @settle/stream.
 *
 * Usage:
 *   import { Settle } from '@settle/stream/web'
 *
 * Uses wasm backend — memory-only storage, no Lua, no RocksDB.
 * External reducers work via JS callbacks (same API as Node.js).
 */

// Re-export types that are shared with Node.js entry point
export type {
  ChangeBatch,
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
export { Pipeline, ReducerHandle, TableHandle, ViewHandle } from './pipeline'

// ─── WASM Settle wrapper ────────────────────────────────────────

import type { ChangeBatch, SettleCursor, ExternalReducerOptions, IngestInput } from './settle'

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
 * const db = new Settle({ schema: '...' })
 * ```
 */
export async function init(wasmUrl?: URL | string): Promise<void> {
  if (wasmReady) return
  const mod: any = await import('./wasm/settle.js')
  await mod.default(wasmUrl)
  WasmSettle = mod.Settle
  wasmReady = true
}

export class Settle {
  #native: any

  constructor(config: { schema: string }) {
    if (!wasmReady) {
      throw new Error(
        'WASM module not initialized. Call `await init()` before creating Settle instances.',
      )
    }
    this.#native = new WasmSettle(config.schema)
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

    this.#native.register_reducer(
      options.name,
      options.source,
      options.groupBy,
      options.state,
      batchFn,
    )
  }

  async ingest(input: IngestInput): Promise<ChangeBatch | null> {
    const result = this.#native.ingest({
      data: input.data,
      rollbackChain: input.rollbackChain,
      finalizedHead: input.finalizedHead,
    })
    if (result && input.onChange) {
      try {
        await input.onChange(result)
      } finally {
        this.#native.ack(result.sequence)
      }
    }
    return result ?? null
  }

  flush(): ChangeBatch | null {
    return this.#native.flush() ?? null
  }

  ack(sequence: number): void {
    this.#native.ack(sequence)
  }

  get pendingCount(): number {
    return this.#native.pendingCount
  }

  get isBackpressured(): boolean {
    return this.#native.isBackpressured
  }

  get cursor(): SettleCursor | null {
    return this.#native.cursor ?? null
  }

  resolveForkCursor(previousBlocks: SettleCursor[]): SettleCursor | null {
    return this.#native.resolve_fork_cursor(previousBlocks) ?? null
  }

  handleFork(previousBlocks: SettleCursor[]): { cursor: SettleCursor; batch: ChangeBatch | null } {
    const result = this.#native.handle_fork(previousBlocks)
    return { cursor: result.cursor, batch: result.batch ?? null }
  }
}

