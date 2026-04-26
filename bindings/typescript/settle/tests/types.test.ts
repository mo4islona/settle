/**
 * Compile-time type tests for the builder API.
 * Each test uses @ts-expect-error to assert that invalid code IS rejected.
 */

import { describe, expect, it } from 'vitest'
import { float64, Pipeline, string } from '../src'

describe('Type safety', () => {
  it('rejects raw strings as column types', () => {
    const p = new Pipeline()
    // @ts-expect-error — string literals not allowed, must use uint64(), string(), etc.
    p.table('t', { id: 'UInt64' })
  })

  it('infers row type from column definitions', () => {
    const p = new Pipeline()
    const t = p.table('t', { user: string(), amount: float64() })

    t.createReducer('r', {
      groupBy: 'user',
      initialState: { total: 0 },
      reduce(state, row) {
        // row.user is string, row.amount is number — inferred from columns
        const _u: string = row.user
        const _a: number = row.amount
        // @ts-expect-error — 'nonexistent' is not a column
        const _x = row.nonexistent
        state.update({ total: state.total + row.amount })
      },
    })
  })

  it('validates groupBy against inferred row keys', () => {
    const p = new Pipeline()
    const t = p.table('t', { user: string(), amount: float64() })

    t.createReducer('r', {
      groupBy: 'user', // valid
      initialState: {},
      reduce(_state, _row) {},
    })

    t.createReducer('r2', {
      // @ts-expect-error — 'nonexistent' is not a column
      groupBy: 'nonexistent',
      initialState: {},
      reduce(_state, _row) {},
    })
  })

  it('infers state type from initialState', () => {
    const p = new Pipeline()
    const t = p.table('t', { v: float64() })

    t.createReducer('r', {
      groupBy: [],
      initialState: { count: 0, label: 'hello' },
      reduce(state, _row) {
        const _n: number = state.count
        const _s: string = state.label
        // @ts-expect-error — no 'missing' property on state
        const _x = state.missing
      },
    })
  })

  it('types chained reducer row from parent emit', () => {
    const p = new Pipeline()
    const t = p.table('t', { v: float64() })

    interface Enriched {
      value: number
      doubled: number
    }

    const enriched = t.createReducer<{ sum: number }, Enriched>('enriched', {
      groupBy: [],
      initialState: { sum: 0 },
      reduce(state, row) {
        const sum = state.sum + row.v
        state.update({ sum })
        state.emit({ value: row.v, doubled: row.v * 2 })
      },
    })

    enriched.createReducer('chained', {
      groupBy: [],
      initialState: { prev: 0 },
      reduce(state, row) {
        const _v: number = row.value
        const _d: number = row.doubled
        // @ts-expect-error — 'nonexistent' is not a key of Enriched
        const _x = row.nonexistent
        state.update({ prev: row.value })
      },
    })
  })

  it('types agg.sum/first/last against emit columns', () => {
    const p = new Pipeline()
    const t = p.table('t', { k: string(), v: float64() })

    interface Emit {
      group: string
      price: number
      volume: number
    }

    const reducer = t.createReducer<{ n: number }, Emit>('r', {
      groupBy: 'k',
      initialState: { n: 0 },
      reduce(state, row) {
        state.update({ n: state.n + 1 })
        state.emit({ group: row.k, price: row.v, volume: row.v * 2 })
      },
    })

    reducer.createView('mv', {
      groupBy: ['group'],
      select: (agg) => ({
        group: agg.key.group,
        avgPrice: agg.avg('price'),
        totalVolume: agg.sum('volume'),
        count: agg.count(),
        // @ts-expect-error — 'nonexistent' is not a key of Emit
        bad: agg.sum('nonexistent'),
      }),
    })
  })

  it('agg.key accepts any string (group-by columns, interval aliases)', () => {
    const p = new Pipeline()
    const t = p.table('t', { x: float64() })

    interface Emit {
      pool: string
      price: number
    }

    const r = t.createReducer<{ n: number }, Emit>('r', {
      groupBy: [],
      initialState: { n: 0 },
      reduce(state, row) {
        state.update({ n: state.n + 1 })
        state.emit({ pool: 'a', price: row.x })
      },
    })

    r.createView('mv', {
      groupBy: ['pool'],
      select: (agg) => ({
        pool: agg.key.pool,
        alias: agg.key.window_start, // interval aliases work
      }),
    })
  })

  it('preserves types through table → reducer → reducer → view', () => {
    const p = new Pipeline()

    interface EnrichedEmit {
      user: string
      runningTotal: number
    }

    interface AlertEmit {
      user: string
      spikeValue: number
    }

    const events = p.table('events', { user: string(), amount: float64() })

    const enriched = events.createReducer<{ total: number }, EnrichedEmit>('enriched', {
      groupBy: 'user',
      initialState: { total: 0 },
      reduce(state, row) {
        const total = state.total + row.amount
        state.update({ total })
        state.emit({ user: row.user, runningTotal: total })
      },
    })

    const alerts = enriched.createReducer<{ prev: number }, AlertEmit>('alerts', {
      groupBy: 'user',
      initialState: { prev: 0 },
      reduce(state, row) {
        const _rt: number = row.runningTotal
        const _u: string = row.user
        // @ts-expect-error — 'amount' is on Event, NOT on EnrichedEmit
        const _a = row.amount
        state.update({ prev: row.runningTotal })
        state.emit({ user: row.user, spikeValue: row.runningTotal })
      },
    })

    alerts.createView('summary', {
      groupBy: ['user'],
      select: (agg) => ({
        user: agg.key.user,
        lastSpike: agg.last('spikeValue'),
        count: agg.count(),
        // @ts-expect-error — 'runningTotal' is on EnrichedEmit, NOT on AlertEmit
        bad: agg.sum('runningTotal'),
      }),
    })

    expect(() => p.build()).not.toThrow()
  })
})
