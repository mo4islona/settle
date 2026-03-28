import { DeltaDb, type StateFieldDef } from './delta-db'

// ─── Duration parsing ────────────────────────────────────────────

const DURATION_UNITS: Record<string, number> = {
  second: 1,
  seconds: 1,
  sec: 1,
  s: 1,
  minute: 60,
  minutes: 60,
  min: 60,
  m: 60,
  hour: 3600,
  hours: 3600,
  hr: 3600,
  h: 3600,
  day: 86400,
  days: 86400,
  d: 86400,
}

function parseDuration(s: string): number {
  const match = s.trim().match(/^(\d+)\s*(\w+)$/)
  if (!match) throw new Error(`invalid duration: '${s}'`)
  const n = parseInt(match[1], 10)
  const unit = match[2].toLowerCase()
  const mult = DURATION_UNITS[unit]
  if (!mult) throw new Error(`unknown duration unit: '${unit}'`)
  return n * mult
}

// ─── Interval helper ─────────────────────────────────────────────

export interface IntervalExpr {
  _type: 'interval'
  column: string
  seconds: number
  alias?: string
  as(alias: string): IntervalExpr
}

export function interval(column: string, duration: string): IntervalExpr {
  const seconds = parseDuration(duration)
  return {
    _type: 'interval',
    column,
    seconds,
    as(alias: string): IntervalExpr {
      return { _type: 'interval', column, seconds, alias, as: (a: string) => interval(column, duration).as(a) }
    },
  }
}

// ─── Aggregation types ───────────────────────────────────────────

export interface AggExpr {
  _type: 'agg'
  func: string
  column: string | null
}

export interface KeyRef {
  _type: 'key'
  column: string
}

export interface AggProxy<TKeys extends string = string> {
  key: Record<TKeys, KeyRef>
  sum(column: string): AggExpr
  count(): AggExpr
  first(column: string): AggExpr
  last(column: string): AggExpr
  min(column: string): AggExpr
  max(column: string): AggExpr
  avg(column: string): AggExpr
}

// ─── Options types ───────────────────────────────────────────────

export type GroupByItem = string | IntervalExpr

export interface ReducerOptions<TState, TRow, TEmit> {
  groupBy: string | string[]
  initialState: TState
  reduce: (state: TState, row: TRow) => [TState, TEmit | TEmit[] | null]
}

export interface ViewOptions {
  groupBy: GroupByItem | GroupByItem[]
  select: (agg: AggProxy<any>) => Record<string, AggExpr | KeyRef>
}

// ─── Type inference from initialState ────────────────────────────

function inferStateFields(initialState: Record<string, unknown>): StateFieldDef[] {
  const fields: StateFieldDef[] = []
  for (const [name, value] of Object.entries(initialState)) {
    let columnType: string
    let defaultValue: string
    switch (typeof value) {
      case 'number':
        columnType = 'Float64'
        defaultValue = String(value)
        break
      case 'bigint':
        columnType = 'UInt64'
        defaultValue = String(value)
        break
      case 'string':
        columnType = 'String'
        defaultValue = `'${value}'`
        break
      case 'boolean':
        columnType = 'Boolean'
        defaultValue = value ? 'true' : 'false'
        break
      case 'object':
        columnType = 'Json'
        defaultValue = `'${JSON.stringify(value)}'`
        break
      default:
        columnType = 'String'
        defaultValue = `'${String(value)}'`
    }
    fields.push({ name, columnType, defaultValue })
  }
  return fields
}

// ─── DDL generators ──────────────────────────────────────────────

function tableToSql(name: string, columns: Record<string, string>, virtual: boolean): string {
  const prefix = virtual ? 'CREATE VIRTUAL TABLE' : 'CREATE TABLE'
  const cols = Object.entries(columns)
    .map(([col, type]) => `${col} ${type}`)
    .join(', ')
  return `${prefix} ${name} (${cols});`
}

function reducerToSql(
  name: string,
  source: string,
  groupBy: string[],
  stateFields: StateFieldDef[],
): string {
  const gb = groupBy.join(', ')
  const state = stateFields
    .map((f) => `${f.name} ${f.columnType} DEFAULT ${f.defaultValue}`)
    .join(', ')
  return `CREATE REDUCER ${name} SOURCE ${source} GROUP BY ${gb} STATE (${state}) LANGUAGE EXTERNAL;`
}

function viewToSql(
  name: string,
  source: string,
  groupByItems: GroupByItem[],
  selectFn: (agg: AggProxy<any>) => Record<string, AggExpr | KeyRef>,
): string {
  const groupByCols: string[] = []
  const intervalDefs: { column: string; seconds: number; alias: string }[] = []

  for (const item of groupByItems) {
    if (typeof item === 'string') {
      groupByCols.push(item)
    } else if (item._type === 'interval') {
      const alias = item.alias || `${item.column}_interval`
      intervalDefs.push({ column: item.column, seconds: item.seconds, alias })
      groupByCols.push(alias)
    }
  }

  // Build aggregation proxy
  const keyProxy = new Proxy({} as Record<string, KeyRef>, {
    get(_target, prop: string): KeyRef {
      return { _type: 'key', column: prop }
    },
  })

  const aggProxy: AggProxy<any> = {
    key: keyProxy,
    sum(col: string) {
      return { _type: 'agg', func: 'sum', column: col }
    },
    count() {
      return { _type: 'agg', func: 'count', column: null }
    },
    first(col: string) {
      return { _type: 'agg', func: 'first', column: col }
    },
    last(col: string) {
      return { _type: 'agg', func: 'last', column: col }
    },
    min(col: string) {
      return { _type: 'agg', func: 'min', column: col }
    },
    max(col: string) {
      return { _type: 'agg', func: 'max', column: col }
    },
    avg(col: string) {
      return { _type: 'agg', func: 'avg', column: col }
    },
  }

  const selectResult = selectFn(aggProxy)
  const selectItems: string[] = []

  for (const [alias, expr] of Object.entries(selectResult)) {
    if (expr._type === 'key') {
      const intv = intervalDefs.find((d) => d.alias === expr.column)
      if (intv) {
        selectItems.push(
          `toStartOfInterval(${intv.column}, INTERVAL ${intv.seconds} SECOND) AS ${alias}`,
        )
      } else {
        selectItems.push(alias === expr.column ? alias : `${expr.column} AS ${alias}`)
      }
    } else if (expr._type === 'agg') {
      const arg = expr.column ? `(${expr.column})` : '()'
      selectItems.push(`${expr.func}${arg} AS ${alias}`)
    }
  }

  return `CREATE MATERIALIZED VIEW ${name} AS SELECT ${selectItems.join(', ')} FROM ${source} GROUP BY ${groupByCols.join(', ')};`
}

// ─── Internal types ──────────────────────────────────────────────

interface TableDef {
  name: string
  columns: Record<string, string>
  virtual: boolean
}

interface ReducerDef {
  name: string
  source: string
  groupBy: string[]
  stateFields: StateFieldDef[]
  reduce: (state: any, row: any) => [any, any]
}

interface ViewDef {
  sql: string
}

// ─── Pipeline (builder) ──────────────────────────────────────────

export class Pipeline {
  #tables: TableDef[] = []
  #reducers: ReducerDef[] = []
  #views: ViewDef[] = []

  table<TRow = any>(
    name: string,
    columns: Record<string, string>,
    opts?: { virtual?: boolean },
  ): TableHandle<TRow> {
    this.#tables.push({ name, columns, virtual: opts?.virtual ?? false })
    return new TableHandle<TRow>(this, name)
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
  _addView(name: string, source: string, opts: ViewOptions): ViewHandle {
    const groupByItems = Array.isArray(opts.groupBy) ? opts.groupBy : [opts.groupBy]
    const sql = viewToSql(name, source, groupByItems, opts.select)
    this.#views.push({ sql })
    return new ViewHandle(name)
  }

  build(opts?: { dataDir?: string; maxBufferSize?: number }): DeltaDb {
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

    const db = DeltaDb.open({
      schema: ddl.join('\n'),
      dataDir: opts?.dataDir,
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

  createView(name: string, opts: ViewOptions): ViewHandle {
    return this.#pipeline._addView(name, this.#name, opts)
  }
}

export class ReducerHandle<TEmit = any> {
  #pipeline: Pipeline
  #name: string

  constructor(pipeline: Pipeline, name: string) {
    this.#pipeline = pipeline
    this.#name = name
  }

  get name(): string {
    return this.#name
  }

  createReducer<TState, TEmit2>(
    name: string,
    opts: ReducerOptions<TState, TEmit, TEmit2>,
  ): ReducerHandle<TEmit2> {
    return this.#pipeline._addReducer(name, this.#name, opts)
  }

  createView(name: string, opts: ViewOptions): ViewHandle {
    return this.#pipeline._addView(name, this.#name, opts)
  }
}

export class ViewHandle {
  #name: string

  constructor(name: string) {
    this.#name = name
  }

  get name(): string {
    return this.#name
  }
}
