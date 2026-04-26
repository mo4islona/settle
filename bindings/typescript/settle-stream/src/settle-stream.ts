import { decode, Encoder } from '@msgpack/msgpack'
import type { ReducerCtx } from './ddl'
import { SettleStream as NativeSettleStream } from './native/native.js'

const encoder = new Encoder({ useBigInt64: true })

// ─── Types ───────────────────────────────────────────────────────

export interface SettleStreamConfig {
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

export interface SettleStreamCursor {
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
  finalizedHead: SettleStreamCursor | null
  latestHead: SettleStreamCursor | null
  tables: Record<string, ChangeRecord[]>
  perf: PerfNode[]
}

export interface IngestInput {
  data: Record<string, Record<string, any>[]>
  rollbackChain?: SettleStreamCursor[]
  finalizedHead: SettleStreamCursor
  onChange?: (batch: ChangeBatch) => void | Promise<void>
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

// ─── SettleStream class ───────────────────────────────────────────────

export class SettleStream {
  #native: InstanceType<typeof NativeSettleStream>

  private constructor(native: InstanceType<typeof NativeSettleStream>) {
    this.#native = native
  }

  static open(config: SettleStreamConfig): SettleStream {
    return new SettleStream(NativeSettleStream.open(config))
  }

  async ingest(input: IngestInput): Promise<ChangeBatch | null> {
    const buf = this.#native.ingest({
      data: Buffer.from(encoder.encode(input.data)),
      rollbackChain: input.rollbackChain,
      finalizedHead: input.finalizedHead,
    })
    const batch = buf ? (decode(buf) as ChangeBatch) : null
    if (batch && input.onChange) {
      await input.onChange(batch)
      this.#native.ack(batch.sequence)
    }
    return batch
  }

  resolveForkCursor(previousBlocks: SettleStreamCursor[]): SettleStreamCursor | null {
    return this.#native.resolveForkCursor(previousBlocks)
  }

  flush(): ChangeBatch | null {
    const buf = this.#native.flush()
    return buf ? (decode(buf) as ChangeBatch) : null
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

  get cursor(): SettleStreamCursor | null {
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
        // Initialize readable state properties
        for (const k of Object.keys(state as any)) {
          ctx[k] = (state as any)[k]
        }
        for (const row of rows) {
          reduce(ctx, row)
        }
        return { state: s, emits }
      })
    }

    this.#native.registerReducer(
      {
        name: options.name,
        source: options.source,
        groupBy: options.groupBy,
        state: options.state,
      },
      batchFn,
    )
  }
}
