import type { ReducerCtx } from './ddl'

/** The rows of one group in one block, with the group's state before them. */
export interface ReducerGroup<TState, TRow> {
  state: TState
  rows: TRow[]
}

/**
 * The callback that the engine calls for an external reducer, once per block:
 * it runs `reduce` on each row of each group, in order. `emitRows[i]` is the
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
