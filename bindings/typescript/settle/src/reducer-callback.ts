import { type GroupResults, GroupWriter, ReducerLayout, readInput } from './codec'
import type { ReducerCtx } from './ddl'

/** The rows of one group in one block, with the group's state before them. */
export interface ReducerGroup<TState, TRow> {
  state: TState
  rows: TRow[]
}

/**
 * The callback that the WASM engine calls for an external reducer, once per
 * block: it runs `reduce` on each row of each group, in order. `emitRows[i]` is the
 * index in `rows` of the row that emitted `emits[i]`, so that the engine can
 * put the emits of all groups back in input order.
 */
export function groupBatchCallback<TState, TRow, TEmit>(
  reduce: (state: ReducerCtx<TState, TEmit>, row: TRow) => void,
) {
  return (groups: ReducerGroup<TState, TRow>[]) =>
    groups.map(({ state, rows }) => {
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
 * state.
 */
export function nativeBatchCallback<TState, TRow, TEmit>(
  reduce: (state: ReducerCtx<TState, TEmit>, row: TRow) => void,
) {
  const out = new GroupWriter()
  let layout: ReducerLayout | undefined

  return (buffer: ArrayBuffer, joined: string, others: unknown[] | null): GroupResults => {
    const input = readInput(buffer, joined, others)
    if (layout === undefined || !layout.matches(input)) layout = new ReducerLayout(input, out)
    const { Ctx, row, writeState } = layout
    const { tags, nums, ints, strings } = input
    const o = input.others

    const groupCount = ints[0]
    const stateCount = ints[1]
    const columnCount = ints[2]
    const sizesAt = 3 + stateCount + columnCount

    out.begin()
    let slot = groupCount * stateCount
    for (let g = 0; g < groupCount; g++) {
      const rowCount = ints[sizesAt + g]
      const ctx = new Ctx(tags, nums, strings, o, g * stateCount)
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
