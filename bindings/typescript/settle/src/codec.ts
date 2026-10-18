/**
 * The binary layout that moves the values of a whole batch across the native
 * boundary in one call. `src/js_codec/mod.rs` describes it; the tags and the
 * order of the ints and slots here must match it.
 *
 * Objects are built here by generated code, one function per set of field
 * names, so that each object of a set has the same shape.
 */

import type {
  ChangeBatch,
  ChangeOp,
  ChangeRecord,
  PerfNode,
  PerfNodeKind,
  SettleCursor,
} from './settle'

export const NULL = 0
export const NUMBER = 1
export const STRING = 2
export const TRUE = 3
export const FALSE = 4
export const OTHER = 5

/** The u32 header of a buffer from native code: slot, int and string counts. */
const HEADER_BYTES = 32

type Others = unknown[] | null

/** The slots, the ints and the string table of a buffer from native code. */
export interface Input {
  tags: Uint8Array
  nums: Float64Array
  ints: Uint32Array
  strings: string[]
  others: Others
}

export function readInput(buffer: ArrayBuffer, joined: string, others: Others): Input {
  const header = new Uint32Array(buffer, 0, 8)
  const slotCount = header[0]
  const intCount = header[1]
  const stringCount = header[2]

  let at = HEADER_BYTES
  const nums = new Float64Array(buffer, at, slotCount)
  at += 8 * slotCount
  const ints = new Uint32Array(buffer, at, intCount)
  at += 4 * intCount
  const ends = new Uint32Array(buffer, at, stringCount)
  at += 4 * stringCount
  const tags = new Uint8Array(buffer, at, slotCount)

  const strings: string[] = []
  let start = 0
  for (let i = 0; i < stringCount; i++) {
    const end = ends[i]
    strings.push(joined.slice(start, end))
    start = end
  }
  return { tags, nums, ints, strings, others }
}

/** Slot `i` as a value; Null is `null`. */
export function value(t: Uint8Array, n: Float64Array, s: string[], o: Others, i: number): unknown {
  switch (t[i]) {
    case NUMBER:
      return n[i]
    case STRING:
      return s[n[i]]
    case TRUE:
      return true
    case FALSE:
      return false
    case OTHER:
      return (o as unknown[])[n[i]]
    default:
      return null
  }
}

/** Slot `i` as a field of an input row: Null is `undefined`, as a field that a row lacks. */
export function field(t: Uint8Array, n: Float64Array, s: string[], o: Others, i: number): unknown {
  switch (t[i]) {
    case NUMBER:
      return n[i]
    case STRING:
      return s[n[i]]
    case TRUE:
      return true
    case FALSE:
      return false
    case OTHER:
      return (o as unknown[])[n[i]]
    default:
      return undefined
  }
}

type Read = typeof value

/** Builds an object with fields from the slots that start at `base`. */
export type Build = (
  t: Uint8Array,
  n: Float64Array,
  s: string[],
  o: Others,
  base: number,
) => Record<string, any>

// ─── Generated code ──────────────────────────────────────────────

/** The key of `name` in an object literal; a computed `__proto__` is an own field. */
function literalKey(name: string): string {
  return name === '__proto__' ? '["__proto__"]' : JSON.stringify(name)
}

/** Code that reads field `name` of `obj`, own `__proto__` only. */
function readCode(obj: string, name: string): string {
  if (name === '__proto__') {
    return `(Object.prototype.hasOwnProperty.call(${obj}, "__proto__") ? ${obj}["__proto__"] : undefined)`
  }
  return `${obj}[${JSON.stringify(name)}]`
}

/** Code that sets field `name` of `obj` to `expr`. */
function assignCode(obj: string, name: string, expr: string): string {
  if (name === '__proto__') {
    return `Object.defineProperty(${obj}, "__proto__", { value: ${expr}, enumerable: true, writable: true, configurable: true })`
  }
  return `${obj}[${JSON.stringify(name)}] = ${expr}`
}

/** Distinct name lists are few (tables, views, reducers); the cap only bounds a misuse. */
const MAX_COMPILED = 1024
const compiled = new Map<string, unknown>()

function cached<T>(key: string, compile: () => T): T {
  let fn = compiled.get(key) as T | undefined
  if (fn === undefined) {
    if (compiled.size >= MAX_COMPILED) compiled.clear()
    fn = compile()
    compiled.set(key, fn)
  }
  return fn
}

/** A `Build` for objects with the fields `names`, read by `read`. */
export function objectBuilder(names: readonly string[], read: Read): Build {
  const key = `build:${read === value ? 'value' : 'field'}:${JSON.stringify(names)}`
  return cached(key, () => {
    const fields = names.map((name, i) => `${literalKey(name)}: read(t, n, s, o, b + ${i})`)
    const source = `return function build(t, n, s, o, b) { return { ${fields.join(', ')} } }`
    return new Function('read', source)(read) as Build
  })
}

/** Writes the fields `names` of a row, `block_number` first, into a `Writer`. */
type WriteRow = (w: Writer, row: Record<string, unknown>) => void

function rowWriter(columns: readonly string[]): WriteRow {
  return cached(`row:${JSON.stringify(columns)}`, () => {
    const puts = ['block_number', ...columns].map((name) => `w.put(${readCode('r', name)})`)
    const source = `return function write(w, r) { ${puts.join('; ')} }`
    return new Function(source)() as WriteRow
  })
}

// ─── JS → native ─────────────────────────────────────────────────

/**
 * Slots and ints for native code. The arrays are kept and grow as needed:
 * native code reads them before the call that returns them ends.
 */
export class Writer {
  tags = new Uint8Array(256)
  nums = new Float64Array(256)
  ints = new Uint32Array(256)
  slots = 0
  intCount = 0
  /** String values in slot order; each slot holds the length. */
  strings: string[] = []
  others: unknown[] = []

  /** Start over, with `header` ints for the counts. */
  reset(header: number): void {
    this.slots = 0
    this.intCount = header
    this.strings = []
    this.others = []
  }

  int(n: number): void {
    if (this.intCount === this.ints.length) this.growInts()
    this.ints[this.intCount++] = n
  }

  put(v: unknown): void {
    const i = this.slots
    if (i === this.tags.length) this.growSlots()
    this.slots = i + 1
    switch (typeof v) {
      case 'number':
        this.tags[i] = NUMBER
        this.nums[i] = v
        return
      case 'string':
        this.tags[i] = STRING
        this.nums[i] = v.length
        this.strings.push(v)
        return
      case 'boolean':
        this.tags[i] = v ? TRUE : FALSE
        return
      case 'bigint':
        this.other(i, v)
        return
      case 'object':
        if (v === null) this.tags[i] = NULL
        else this.other(i, v)
        return
      default:
        this.tags[i] = NULL
    }
  }

  private other(i: number, v: unknown): void {
    this.tags[i] = OTHER
    this.nums[i] = this.others.length
    this.others.push(v)
  }

  private growSlots(): void {
    const tags = new Uint8Array(this.tags.length * 2)
    tags.set(this.tags)
    this.tags = tags
    const nums = new Float64Array(this.nums.length * 2)
    nums.set(this.nums)
    this.nums = nums
  }

  private growInts(): void {
    const ints = new Uint32Array(this.ints.length * 2)
    ints.set(this.ints)
    this.ints = ints
  }
}

/** The rows of an ingest, as `ingest` in `src/napi.rs` reads them. */
export type EncodedRows = [Uint32Array, Uint8Array, Float64Array, string, Others]

/**
 * Encodes the rows of an ingest by the columns of each table
 * (`decode_ingest` in `src/js_codec/mod.rs`).
 */
export class TableWriter extends Writer {
  encode(
    data: Record<string, readonly Record<string, unknown>[]>,
    columnsOf: (table: string) => readonly string[] | null,
  ): EncodedRows {
    this.reset(2)

    const tables: {
      name: string
      rows: readonly Record<string, unknown>[]
      columns: readonly string[]
    }[] = []
    for (const name of Object.keys(data)) {
      const rows = data[name]
      const columns = columnsOf(name)
      if (columns === null) {
        if (rows.length > 0) throw new Error(`unknown table: ${name}`)
        continue
      }
      tables.push({ name, rows, columns })
    }

    this.int(tables.length)
    for (const { name, rows, columns } of tables) {
      this.int(name.length)
      this.int(rows.length)
      this.int(columns.length + 1)
      this.strings.push(name)
    }
    for (const { rows, columns } of tables) {
      const write = rowWriter(columns)
      for (const row of rows) write(this, row)
    }

    this.ints[0] = this.intCount
    this.ints[1] = this.slots
    return [
      this.ints,
      this.tags,
      this.nums,
      this.strings.join(''),
      this.others.length ? this.others : null,
    ]
  }
}

/** The output of a reducer callback, as `call_js_batch` in `src/reducer_runtime/external.rs` reads it. */
export type GroupResults = [Uint32Array, Uint8Array, Float64Array, string, string, Others]

/**
 * The states and emits of the groups of a block
 * (`decode_group_results` in `src/js_codec/mod.rs`).
 */
export class GroupWriter extends Writer {
  /** The index in its group of the row that `reduce` runs on. */
  row = 0
  /** The context of the current group: `update` and `emit` use no `this`, so that they work destructured. */
  ctx: Record<string, unknown> | undefined = undefined
  changed = false
  next: unknown = undefined
  private groupAt = 0
  private emitCount = 0
  private columns: string[] = []
  private columnIds = new Map<string, number>()

  begin(): void {
    this.reset(3)
    this.columns = []
    this.columnIds.clear()
  }

  beginGroup(): void {
    this.groupAt = this.intCount
    this.int(0)
    this.int(0)
    this.emitCount = 0
    this.changed = false
    this.next = undefined
  }

  emit(row: object): void {
    const at = this.intCount
    this.int(this.row)
    this.int(0)

    let fields = 0
    for (const key in row) {
      let id = this.columnIds.get(key)
      if (id === undefined) {
        id = this.columns.length
        this.columns.push(key)
        this.columnIds.set(key, id)
      }
      this.int(id)
      this.put((row as Record<string, unknown>)[key])
      fields++
    }
    this.ints[at + 1] = fields
    this.emitCount++
  }

  endGroup(writeState: (w: Writer, state: any) => void): void {
    this.ints[this.groupAt] = this.changed ? 1 : 0
    this.ints[this.groupAt + 1] = this.emitCount
    if (this.changed) writeState(this, this.next)
  }

  finish(): GroupResults {
    for (const column of this.columns) this.int(column.length)
    this.ints[0] = this.intCount
    this.ints[1] = this.slots
    this.ints[2] = this.columns.length
    return [
      this.ints,
      this.tags,
      this.nums,
      this.strings.join(''),
      this.columns.join(''),
      this.others.length ? this.others : null,
    ]
  }
}

// ─── Reducer layout ──────────────────────────────────────────────

/** A reducer context class: the state fields, `update` and `emit`. */
export type CtxClass = new (
  t: Uint8Array,
  n: Float64Array,
  s: string[],
  o: Others,
  base: number,
) => any

/** The state fields and row columns of the groups of a reducer, with code for them. */
export class ReducerLayout {
  readonly stateNames: string[]
  readonly columnNames: string[]
  readonly Ctx: CtxClass
  readonly row: Build
  readonly writeState: (w: Writer, state: any) => void

  constructor(input: Input, out: GroupWriter) {
    const { ints, strings } = input
    const stateCount = ints[1]
    const columnCount = ints[2]
    this.stateNames = []
    for (let i = 0; i < stateCount; i++) this.stateNames.push(strings[ints[3 + i]])
    this.columnNames = []
    for (let i = 0; i < columnCount; i++) this.columnNames.push(strings[ints[3 + stateCount + i]])

    this.row = objectBuilder(this.columnNames, field)
    this.Ctx = ctxClass(this.stateNames, out)
    this.writeState = stateWriter(this.stateNames)
  }

  matches(input: Input): boolean {
    const { ints, strings } = input
    const stateCount = ints[1]
    const columnCount = ints[2]
    if (stateCount !== this.stateNames.length || columnCount !== this.columnNames.length)
      return false
    for (let i = 0; i < stateCount; i++) {
      if (strings[ints[3 + i]] !== this.stateNames[i]) return false
    }
    for (let i = 0; i < columnCount; i++) {
      if (strings[ints[3 + stateCount + i]] !== this.columnNames[i]) return false
    }
    return true
  }
}

/**
 * A class whose objects hold the state fields of a group, read from slots.
 * `update(next)` makes `next` the state that goes back to the engine and
 * shows its fields on the context of the current group; `emit(row)` writes
 * an emit of the current row.
 */
function ctxClass(names: readonly string[], out: GroupWriter): CtxClass {
  const init = names.map((name, i) => assignCode('this', name, `value(t, n, s, o, b + ${i})`))
  const update = names.map((name) => assignCode('ctx', name, readCode('next', name)))
  const source = `
    "use strict"
    function Ctx(t, n, s, o, b) { ${init.join('; ')} }
    Ctx.prototype.update = function update(next) {
      const ctx = out.ctx
      out.changed = true
      out.next = next
      ${update.join('; ')}
    }
    Ctx.prototype.emit = function emit(row) {
      if (row != null) out.emit(row)
    }
    return Ctx`
  return new Function('value', 'out', source)(value, out) as CtxClass
}

function stateWriter(names: readonly string[]): (w: Writer, state: any) => void {
  return cached(`state:${JSON.stringify(names)}`, () => {
    const puts = names.map((name) => `w.put(${readCode('state', name)})`)
    return new Function(`return function writeState(w, state) { ${puts.join('; ')} }`)() as (
      w: Writer,
      state: any,
    ) => void
  })
}

// ─── Change batch ────────────────────────────────────────────────

const OPERATIONS: ChangeOp[] = ['insert', 'update', 'delete']
const PERF_KINDS: PerfNodeKind[] = ['pipeline', 'raw_table', 'reducer', 'mv', 'parallel']

interface RecordLayout {
  table: string
  keyCount: number
  columnCount: number
  key: Build
  values: Build
}

function recordLayout(table: string, keyNames: string[], columnNames: string[]): RecordLayout {
  return {
    table,
    keyCount: keyNames.length,
    columnCount: columnNames.length,
    key: objectBuilder(keyNames, value),
    values: objectBuilder(columnNames, value),
  }
}

/** A change batch from `[buffer, strings, others]` (`encode_batch` in `src/js_codec/mod.rs`). */
export function readBatch(buffer: ArrayBuffer, joined: string, others: Others): ChangeBatch {
  const input = readInput(buffer, joined, others)
  const { tags: t, nums: n, ints, strings: s } = input
  const o = input.others
  let slot = 0
  let at = 0

  const sequence = n[slot++]
  const heads: (SettleCursor | null)[] = []
  for (let h = 0; h < 2; h++) {
    if (ints[at++] === 1) {
      heads.push({ number: n[slot], hash: s[n[slot + 1]] })
      slot += 2
    } else {
      heads.push(null)
    }
  }

  const tables: Record<string, ChangeRecord[]> = {}
  const tableCount = ints[at++]
  for (let ti = 0; ti < tableCount; ti++) {
    const name = s[n[slot++]]
    const count = ints[at++]
    const records: ChangeRecord[] = []
    let layout: RecordLayout | undefined
    for (let i = 0; i < count; i++) {
      const flags = ints[at++]
      if (flags & 8) {
        const keyCount = ints[at++]
        const columnCount = ints[at++]
        const table = s[n[slot++]]
        const keyNames: string[] = []
        for (let k = 0; k < keyCount; k++) keyNames.push(s[n[slot++]])
        const columnNames: string[] = []
        for (let c = 0; c < columnCount; c++) columnNames.push(s[n[slot++]])
        layout = recordLayout(table, keyNames, columnNames)
      }
      const l = layout as RecordLayout

      const key = l.key(t, n, s, o, slot)
      slot += l.keyCount
      const values = l.values(t, n, s, o, slot)
      slot += l.columnCount
      let prevValues = null
      if (flags & 4) {
        prevValues = l.values(t, n, s, o, slot)
        slot += l.columnCount
      }
      records.push({ table: l.table, operation: OPERATIONS[flags & 3], key, values, prevValues })
    }
    tables[name] = records
  }

  const cursor = { slot, at }
  const perf = readPerfNodes(input, cursor, ints[cursor.at++])
  return { sequence, finalizedHead: heads[0], latestHead: heads[1], tables, perf }
}

function readPerfNodes(
  input: Input,
  cursor: { slot: number; at: number },
  count: number,
): PerfNode[] {
  const { nums: n, ints, strings: s } = input
  const nodes: PerfNode[] = []
  for (let i = 0; i < count; i++) {
    const kind = PERF_KINDS[ints[cursor.at++]]
    const childCount = ints[cursor.at++]
    const name = s[n[cursor.slot++]]
    const durationMs = n[cursor.slot++]
    const children = readPerfNodes(input, cursor, childCount)
    nodes.push({ kind, name, durationMs, children })
  }
  return nodes
}
