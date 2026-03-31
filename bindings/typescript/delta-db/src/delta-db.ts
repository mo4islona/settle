import { decode, Encoder } from '@msgpack/msgpack'
import type { ReducerCtx } from './ddl'
// Import from src/native/ directly — not copied to dist/ to avoid stale binaries
import { DeltaDb as NativeDeltaDb } from '../src/native/native.js'

const encoder = new Encoder({ useBigInt64: true })

// ─── Types ───────────────────────────────────────────────────────

export interface DeltaDbConfig {
  schema: string
  dataDir?: string
  maxBufferSize?: number
}

export interface DeltaDbCursor {
  number: number
  hash: string
}

export type DeltaOperation = 'insert' | 'update' | 'delete'

export interface DeltaRecord {
  table: string
  operation: DeltaOperation
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

export interface DeltaBatch {
  sequence: number
  finalizedHead: DeltaDbCursor | null
  latestHead: DeltaDbCursor | null
  tables: Record<string, DeltaRecord[]>
  perf: PerfNode[]
}

export interface IngestInput {
  data: Record<string, Record<string, any>[]>
  rollbackChain?: DeltaDbCursor[]
  finalizedHead: DeltaDbCursor
  onDelta?: (batch: DeltaBatch) => void | Promise<void>
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

// ─── DeltaDb class ───────────────────────────────────────────────

export class DeltaDb {
  #native: InstanceType<typeof NativeDeltaDb>

  private constructor(native: InstanceType<typeof NativeDeltaDb>) {
    this.#native = native
  }

  static open(config: DeltaDbConfig): DeltaDb {
    return new DeltaDb(NativeDeltaDb.open(config))
  }

  async ingest(input: IngestInput): Promise<DeltaBatch | null> {
    const t0 = performance.now()
    const encoded = Buffer.from(encoder.encode(input.data))
    const encodeMs = performance.now() - t0

    const buf = this.#native.ingest({
      data: encoded,
      rollbackChain: input.rollbackChain,
      finalizedHead: input.finalizedHead,
    })

    const t1 = performance.now()
    const batch = buf ? (decode(buf) as DeltaBatch) : null
    const decodeMs = performance.now() - t1

    if (batch) {
      // Inject encode/decode timing into perf tree (encode first, decode last)
      batch.perf.unshift(
        { kind: 'pipeline', name: 'msgpack_encode', durationMs: encodeMs, children: [] },
      )
      batch.perf.push(
        { kind: 'pipeline', name: 'msgpack_decode', durationMs: decodeMs, children: [] },
      )
    }

    if (batch && input.onDelta) {
      try {
        await input.onDelta(batch)
      } finally {
        this.#native.ack(batch.sequence)
      }
    }
    return batch
  }

  resolveForkCursor(previousBlocks: DeltaDbCursor[]): DeltaDbCursor | null {
    return this.#native.resolveForkCursor(previousBlocks)
  }

  handleFork(previousBlocks: DeltaDbCursor[]): { cursor: DeltaDbCursor; batch: DeltaBatch | null } {
    const result = this.#native.handleFork(previousBlocks)
    const batch = result.batch ? (decode(result.batch) as DeltaBatch) : null
    return { cursor: result.cursor, batch }
  }

  flush(): DeltaBatch | null {
    const buf = this.#native.flush()
    return buf ? (decode(buf) as DeltaBatch) : null
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

  get cursor(): DeltaDbCursor | null {
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
