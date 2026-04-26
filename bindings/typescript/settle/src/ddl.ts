import type { ColumnType } from './column'
import type { StateFieldDef } from './settle'

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

export function parseDuration(s: string): number {
  const match = s.trim().match(/^(\d+)\s*(\w+)$/)
  if (!match) {
    throw new Error(`invalid duration: '${s}'`)
  }

  const n = parseInt(match[1], 10)
  const unit = match[2].toLowerCase()
  const mult = DURATION_UNITS[unit]
  if (!mult) {
    throw new Error(`unknown duration unit: '${unit}'`)
  }

  return n * mult
}

// ─── SQL escaping ───────────────────────────────────────────────

function sqlEscape(s: string): string {
  return s.replace(/'/g, "''")
}

// ─── Interval helper ─────────────────────────────────────────────

export interface IntervalExpr {
  _type: 'interval'
  column: string
  seconds: number
  alias?: string
  as(alias: string): IntervalExpr
}

function makeInterval(column: string, seconds: number, alias?: string): IntervalExpr {
  return {
    _type: 'interval',
    column,
    seconds,
    alias,
    as(a: string): IntervalExpr {
      return makeInterval(column, seconds, a)
    },
  }
}

export function interval(column: string, duration: string): IntervalExpr {
  return makeInterval(column, parseDuration(duration))
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

export interface AggProxy<TSource = any> {
  key: Record<string, KeyRef>
  sum(column: string & keyof TSource): AggExpr
  count(): AggExpr
  first(column: string & keyof TSource): AggExpr
  last(column: string & keyof TSource): AggExpr
  min(column: string & keyof TSource): AggExpr
  max(column: string & keyof TSource): AggExpr
  avg(column: string & keyof TSource): AggExpr
}

export type GroupByItem = string | IntervalExpr

// ─── Options types ───────────────────────────────────────────────

export type ReducerCtx<TState, TEmit> = Readonly<TState> & {
  update(newState: TState): void
  emit(row: TEmit): void
}

export interface ReducerOptions<TState, TRow, TEmit> {
  groupBy: (string & keyof TRow) | (string & keyof TRow)[]
  initialState: TState
  reduce: (state: ReducerCtx<TState, TEmit>, row: TRow) => void
}

export interface SlidingWindowOptions<TSource = any> {
  /** Window duration, e.g. "1 hour", "30 minutes", "86400 seconds" */
  interval: string
  /** Column containing row timestamps (DateTime in milliseconds).
   *  Must be a numeric/DateTime column from the source table or reducer output. */
  timeColumn: string & keyof TSource
}

export interface ViewOptions<TSource = any> {
  groupBy: GroupByItem | GroupByItem[]
  select: (agg: AggProxy<TSource>) => Record<string, AggExpr | KeyRef>
  /** When set, the MV computes rolling aggregations over the given time window. */
  slidingWindow?: SlidingWindowOptions<TSource>
}

// ─── Type inference from initialState ────────────────────────────

export function inferStateFields(initialState: Record<string, unknown>): StateFieldDef[] {
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
        defaultValue = `'${sqlEscape(value as string)}'`
        break
      case 'boolean':
        columnType = 'Boolean'
        defaultValue = value ? 'true' : 'false'
        break
      case 'object':
        columnType = 'Json'
        defaultValue = `'${sqlEscape(JSON.stringify(value))}'`
        break
      default:
        columnType = 'String'
        defaultValue = `'${sqlEscape(String(value))}'`
    }
    fields.push({ name, columnType, defaultValue })
  }
  return fields
}

// ─── DDL generators ──────────────────────────────────────────────

export function tableToSql(
  name: string,
  columns: Record<string, ColumnType>,
  virtual: boolean,
): string {
  const prefix = virtual ? 'CREATE VIRTUAL TABLE' : 'CREATE TABLE'
  const cols = Object.entries(columns)
    .map(([name, ct]) => `${name} ${ct._sql}`)
    .join(', ')
  return `${prefix} ${name} (${cols});`
}

export function reducerToSql(
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

export function viewToSql(
  name: string,
  source: string,
  groupByItems: GroupByItem[],
  selectFn: (agg: AggProxy<any>) => Record<string, AggExpr | KeyRef>,
  slidingWindow?: SlidingWindowOptions,
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

  let sql = `CREATE MATERIALIZED VIEW ${name} AS SELECT ${selectItems.join(', ')} FROM ${source} GROUP BY ${groupByCols.join(', ')}`

  if (slidingWindow) {
    const seconds = parseDuration(slidingWindow.interval)
    sql += ` WINDOW SLIDING INTERVAL ${seconds} SECOND BY ${slidingWindow.timeColumn}`
  }

  return sql + ';'
}
