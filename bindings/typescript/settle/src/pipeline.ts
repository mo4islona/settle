import type { ColumnType, InferRow } from './column'
import {
  inferStateFields,
  type ReducerOptions,
  reducerToSql,
  tableToSql,
  type ViewOptions,
  viewToSql,
} from './ddl'
import { Settle, type StateFieldDef } from './settle'

// ─── Internal types ──────────────────────────────────────────────

interface TableDef {
  name: string
  columns: Record<string, ColumnType>
  virtual: boolean
}

interface ReducerDef {
  name: string
  source: string
  groupBy: string[]
  stateFields: StateFieldDef[]
  reduce: (state: any, row: any) => void
}

interface ViewDef {
  sql: string
}

// ─── Pipeline ────────────────────────────────────────────────────

export class Pipeline {
  #tables: TableDef[] = []
  #reducers: ReducerDef[] = []
  #views: ViewDef[] = []

  table<TCols extends Record<string, ColumnType>>(
    name: string,
    columns: TCols,
    opts?: { virtual?: boolean },
  ): TableHandle<InferRow<TCols>> {
    this.#tables.push({ name, columns, virtual: opts?.virtual ?? false })
    return new TableHandle<InferRow<TCols>>(this, name)
  }

  /** @internal */
  _addReducer<TState, TRow, TEmit>(
    name: string,
    source: string,
    opts: ReducerOptions<TState, TRow, TEmit>,
  ): ReducerHandle<TEmit> {
    const groupBy = Array.isArray(opts.groupBy) ? opts.groupBy : [opts.groupBy]
    const stateFields = inferStateFields(opts.initialState as Record<string, unknown>)
    this.#reducers.push({ name, source, groupBy, stateFields, reduce: opts.reduce as any })
    return new ReducerHandle<TEmit>(this, name)
  }

  /** @internal */
  _addView<TSource>(name: string, source: string, opts: ViewOptions<TSource>): ViewHandle {
    const groupByItems = Array.isArray(opts.groupBy) ? opts.groupBy : [opts.groupBy]
    const sql = viewToSql(name, source, groupByItems, opts.select as any, opts.slidingWindow)
    this.#views.push({ sql })
    return new ViewHandle(name)
  }

  build(opts?: { dataDir?: string; maxBufferSize?: number }): Settle {
    const ddl: string[] = []
    for (const t of this.#tables) {
      ddl.push(tableToSql(t.name, t.columns, t.virtual))
    }
    for (const r of this.#reducers) {
      ddl.push(reducerToSql(r.name, r.source, r.groupBy, r.stateFields))
    }
    for (const v of this.#views) {
      ddl.push(v.sql)
    }

    // ':memory:' (or omitted) uses in-memory storage — same convention as SQLite
    const dataDir = opts?.dataDir === ':memory:' ? undefined : opts?.dataDir

    const db = Settle.open({
      schema: ddl.join('\n'),
      dataDir,
      maxBufferSize: opts?.maxBufferSize,
    })

    for (const r of this.#reducers) {
      db.registerReducer({
        name: r.name,
        source: r.source,
        groupBy: r.groupBy,
        state: r.stateFields,
        reduce: r.reduce,
      })
    }

    return db
  }
}

// ─── Handles ─────────────────────────────────────────────────────

export class TableHandle<TRow = any> {
  #pipeline: Pipeline
  #name: string

  constructor(pipeline: Pipeline, name: string) {
    this.#pipeline = pipeline
    this.#name = name
  }

  get name(): string {
    return this.#name
  }

  createReducer<TState, TEmit>(
    name: string,
    opts: ReducerOptions<TState, TRow, TEmit>,
  ): ReducerHandle<TEmit> {
    return this.#pipeline._addReducer(name, this.#name, opts)
  }

  createView(name: string, opts: ViewOptions<TRow>): ViewHandle {
    return this.#pipeline._addView(name, this.#name, opts)
  }
}

export class ReducerHandle<TOutput = any> {
  #pipeline: Pipeline
  #name: string

  constructor(pipeline: Pipeline, name: string) {
    this.#pipeline = pipeline
    this.#name = name
  }

  get name(): string {
    return this.#name
  }

  createReducer<TState, TEmit>(
    name: string,
    opts: ReducerOptions<TState, TOutput, TEmit>,
  ): ReducerHandle<TEmit> {
    return this.#pipeline._addReducer(name, this.#name, opts)
  }

  createView(name: string, opts: ViewOptions<TOutput>): ViewHandle {
    return this.#pipeline._addView(name, this.#name, opts)
  }
}

export class ViewHandle {
  readonly #name: string

  constructor(name: string) {
    this.#name = name
  }

  get name(): string {
    return this.#name
  }
}
