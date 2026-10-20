import { GROUP_HEADER, type GroupResults, GroupWriter, ReducerLayout, readInput } from './codec'
import type { ReducerCtx } from './ddl'
import { type MapAccess, nextEpoch, StateMapHandle } from './state-map'

/** The rows of one group in one block, with the group's state before them. */
export interface ReducerGroup<TState, TRow> {
  state: TState
  rows: TRow[]
}

/**
 * The callback that the WASM engine calls for an external reducer, once per
 * block: it runs `reduce` on each row of each group, in order. `emitRows[i]` is the
 * index in `rows` of the row that emitted `emits[i]`, so that the engine can
 * put the emits of all groups back in input order. `mapNames` are the Map
 * fields, which `access` reaches.
 */
export function groupBatchCallback<TState, TRow, TEmit>(
  reduce: (state: ReducerCtx<TState, TEmit>, row: TRow) => void,
  access: MapAccess,
) {
  return (groups: ReducerGroup<TState, TRow>[], mapNames: string[]) =>
    groups.map(({ state, rows }, group) => {
      let s = state
      const emits: TEmit[] = []
      const emitRows: number[] = []
      let rowIndex = 0

      const ctx = Object.create(null)
      ctx.update = (newState: TState) => {
        s = newState
        for (const k of Object.keys(newState as any)) {
          ctx[k] = (newState as any)[k]
        }
      }
      ctx.emit = (row: TEmit) => {
        if (row != null) {
          emits.push(row)
          emitRows.push(rowIndex)
        }
      }
      for (const k of Object.keys(state as any)) {
        ctx[k] = (state as any)[k]
      }
      nextEpoch()
      mapNames.forEach((name, map) => {
        ctx[name] = new StateMapHandle(access, group, map, name)
      })

      for (; rowIndex < rows.length; rowIndex++) {
        reduce(ctx, rows[rowIndex])
      }
      return { state: s, emits, emitRows }
    })
}

/**
 * The callback that the native engine calls for an external reducer, once
 * per block, with the groups in the layout of `codec.ts`: it runs `reduce`
 * on each row of each group, in order, and returns the new states and the
 * emits in one set of arrays. A group whose `update` never ran keeps its
 * state. `access` reaches the Map fields.
 */
export function nativeBatchCallback<TState, TRow, TEmit>(
  reduce: (state: ReducerCtx<TState, TEmit>, row: TRow) => void,
  access: MapAccess,
) {
  const out = new GroupWriter()
  let layout: ReducerLayout | undefined

  return (buffer: ArrayBuffer, joined: string, others: unknown[] | null): GroupResults => {
    const input = readInput(buffer, joined, others)
    if (layout === undefined || !layout.matches(input)) {
      layout = new ReducerLayout(input, out, access)
    }
    const { Ctx, row, writeState } = layout
    const { tags, nums, ints, strings } = input
    const o = input.others

    const groupCount = ints[0]
    const stateCount = ints[1]
    const columnCount = ints[2]
    const mapCount = ints[3]
    const sizesAt = GROUP_HEADER + stateCount + columnCount + mapCount

    out.begin()
    let slot = groupCount * stateCount
    for (let g = 0; g < groupCount; g++) {
      const rowCount = ints[sizesAt + g]
      nextEpoch()
      const ctx = new Ctx(tags, nums, strings, o, g * stateCount, g)
      out.ctx = ctx
      out.beginGroup()
      for (let r = 0; r < rowCount; r++) {
        out.row = r
        reduce(ctx, row(tags, nums, strings, o, slot) as TRow)
        slot += columnCount
      }
      out.endGroup(writeState)
    }
    return out.finish()
  }
}
