import {describe, expect, it} from 'vitest'
import {datetime, float64, interval, Pipeline, string, uint64, type SlidingWindowOptions} from '../src'

describe('Pipeline builder', () => {
    it('builds a PnL pipeline and produces correct MV output', async () => {
        const p = new Pipeline()

        const trades = p.table('trades', {
            block_number: uint64(),
            user: string(),
            side: string(),
            amount: float64(),
            price: float64(),
        })

        const pnl = trades.createReducer('pnl', {
            groupBy: 'user',
            initialState: {quantity: 0, cost_basis: 0},
            reduce(state, row) {
                if (row.side === 'buy') {
                    const s = {
                        quantity: state.quantity + row.amount,
                        cost_basis: state.cost_basis + row.amount * row.price,
                    }
                    state.update(s)
                    state.emit({trade_pnl: 0, position_size: s.quantity})
                } else {
                    const avg = state.cost_basis / state.quantity
                    const s = {
                        quantity: state.quantity - row.amount,
                        cost_basis: state.cost_basis - row.amount * avg,
                    }
                    state.update(s)
                    state.emit({trade_pnl: row.amount * (row.price - avg), position_size: s.quantity})
                }
            },
        })

        pnl.createView('position_summary', {
            groupBy: ['user'],
            select: (agg) => ({
                user: agg.key.user,
                totalPnl: agg.sum('trade_pnl'),
                currentPosition: agg.last('position_size'),
                tradeCount: agg.count(),
            }),
        })

        const db = p.build()
        const batch = await db.ingest({
            data: {
                trades: [
                    {block_number: 1000, user: 'alice', side: 'buy', amount: 10, price: 2000},
                    {block_number: 1001, user: 'alice', side: 'sell', amount: 5, price: 2200},
                ],
            },
            finalizedHead: {number: 1001, hash: '0x1001'},
            rollbackChain: [{number: 1001, hash: '0x1001'}, {number: 1000, hash: '0x1000'}],
        })

        expect(batch).toBeTruthy()
        const mv = batch!.tables.position_summary
        expect(mv).toHaveLength(1)
        expect(mv[0].values.tradeCount).toBe(2)
        expect(mv[0].values.currentPosition).toBeCloseTo(5, 6)
        expect(mv[0].values.totalPnl).toBeCloseTo(1000, 2)
    })

    it('handles multiple groups', async () => {
        const p = new Pipeline()
        const trades = p.table('trades', {
            block_number: uint64(),
            user: string(),
            side: string(),
            amount: float64(),
            price: float64(),
        })

        trades
            .createReducer('pnl', {
                groupBy: 'user',
                initialState: {quantity: 0},
                reduce(state, row) {
                    const q = state.quantity + (row.side === 'buy' ? row.amount : -row.amount)
                    state.update({quantity: q})
                    state.emit({position_size: q})
                },
            })
            .createView('summary', {
                groupBy: ['user'],
                select: (agg) => ({
                    user: agg.key.user,
                    position: agg.last('position_size'),
                    trades: agg.count(),
                }),
            })

        const db = p.build()
        const batch = await db.ingest({
            data: {
                trades: [
                    {block_number: 1, user: 'alice', side: 'buy', amount: 10, price: 100},
                    {block_number: 1, user: 'bob', side: 'buy', amount: 5, price: 200},
                    {block_number: 1, user: 'alice', side: 'sell', amount: 3, price: 110},
                ],
            },
            finalizedHead: {number: 1, hash: '0x1'},
            rollbackChain: [{number: 1, hash: '0x1'}],
        })

        expect(batch).toBeTruthy()
        const mv = batch!.tables.summary
        expect(mv).toHaveLength(2)

        const alice = mv.find((r) => r.key.user === 'alice')
        const bob = mv.find((r) => r.key.user === 'bob')
        expect(alice?.values.position).toBe(7)
        expect(alice?.values.trades).toBe(2)
        expect(bob?.values.position).toBe(5)
        expect(bob?.values.trades).toBe(1)
    })

    it('supports rollback', async () => {
        const p = new Pipeline()
        const t = p.table('t', {block_number: uint64(), k: string(), v: float64()})

        t.createReducer('r', {
            groupBy: 'k',
            initialState: {total: 0},
            reduce(state, row) {
                const total = state.total + row.v
                state.update({total})
                state.emit({total})
            },
        }).createView('mv', {
            groupBy: ['k'],
            select: (agg) => ({
                k: agg.key.k,
                total: agg.last('total'),
            }),
        })

        const db = p.build()

        // Ingest blocks 1 and 2
        await db.ingest({
            data: {
                t: [
                    {block_number: 1, k: 'a', v: 10},
                    {block_number: 2, k: 'a', v: 20},
                ],
            },
            finalizedHead: {number: 1, hash: '0x1'},
            rollbackChain: [{number: 2, hash: '0x2'}, {number: 1, hash: '0x1'}],
        })

        // Rollback block 2: ingest with rollbackChain that only includes block 1
        const batch = await db.ingest({
            data: {},
            finalizedHead: {number: 1, hash: '0x1'},
            rollbackChain: [{number: 1, hash: '0x1'}],
        })

        expect(batch).toBeTruthy()
        const mv = batch!.tables.mv
        expect(mv[0].values.total).toBe(10)
    })

    it('chains reducers (reducer → reducer → view)', async () => {
        const p = new Pipeline()
        const events = p.table('events', {
            block_number: uint64(),
            user: string(),
            amount: float64(),
        })

        const enriched = events.createReducer('enriched', {
            groupBy: 'user',
            initialState: {total: 0},
            reduce(state, row) {
                const total = state.total + row.amount
                state.update({total})
                state.emit({user: row.user, amount: row.amount, running_total: total})
            },
        })

        const alerts = enriched.createReducer('alerts', {
            groupBy: 'user',
            initialState: {prev: 0},
            reduce(state, row) {
                const spike = state.prev > 0 && row.running_total > state.prev * 2
                state.update({prev: row.running_total})
                if (spike) {
                    state.emit({user: row.user, spike_total: row.running_total})
                }
            },
        })

        alerts.createView('spike_summary', {
            groupBy: ['user'],
            select: (agg) => ({
                user: agg.key.user,
                spikes: agg.count(),
                lastSpike: agg.last('spike_total'),
            }),
        })

        const db = p.build()
        const batch = await db.ingest({
            data: {
                events: [
                    {block_number: 1, user: 'alice', amount: 10},
                    {block_number: 2, user: 'alice', amount: 15},
                ],
            },
            finalizedHead: {number: 2, hash: '0x2'},
            rollbackChain: [{number: 2, hash: '0x2'}, {number: 1, hash: '0x1'}],
        })

        expect(batch).toBeTruthy()
        const mv = batch!.tables.spike_summary
        expect(mv).toHaveLength(1)
        expect(mv[0].values.spikes).toBe(1)
        expect(mv[0].values.lastSpike).toBe(25)
    })

    it('creates a view with time-window grouping', async () => {
        const p = new Pipeline()
        const swaps = p.table('swaps', {
            block_number: uint64(),
            pool: string(),
            block_time: datetime(),
            price: float64(),
            volume: float64(),
        })

        swaps
            .createReducer('prices', {
                groupBy: 'pool',
                initialState: {count: 0},
                reduce(state, row) {
                    state.update({count: state.count + 1})
                    state.emit({pool: row.pool, block_time: row.block_time, price: row.price, volume: row.volume})
                },
            })
            .createView('candles_5m', {
                groupBy: ['pool', interval('block_time', '5 minutes').as('window_start')],
                select: (agg) => ({
                    pool: agg.key.pool,
                    windowStart: agg.key.window_start,
                    open: agg.first('price'),
                    high: agg.max('price'),
                    low: agg.min('price'),
                    close: agg.last('price'),
                    volume: agg.sum('volume'),
                    tradeCount: agg.count(),
                }),
            })

        const db = p.build()
        const batch = await db.ingest({
            data: {
                swaps: [
                    {block_number: 1, pool: 'ETH/USDC', block_time: 60000, price: 2000, volume: 100},
                    {block_number: 1, pool: 'ETH/USDC', block_time: 120000, price: 2100, volume: 200},
                    {block_number: 2, pool: 'ETH/USDC', block_time: 360000, price: 2050, volume: 150},
                ],
            },
            finalizedHead: {number: 2, hash: '0x2'},
            rollbackChain: [{number: 2, hash: '0x2'}, {number: 1, hash: '0x1'}],
        })

        expect(batch).toBeTruthy()
        const candles = batch!.tables.candles_5m
        expect(candles).toHaveLength(2)
        candles.sort((a, b) => a.values.windowStart - b.values.windowStart)

        expect(candles[0].values.open).toBe(2000)
        expect(candles[0].values.high).toBe(2100)
        expect(candles[0].values.close).toBe(2100)
        expect(candles[0].values.volume).toBe(300)
        expect(candles[0].values.tradeCount).toBe(2)

        expect(candles[1].values.open).toBe(2050)
        expect(candles[1].values.close).toBe(2050)
        expect(candles[1].values.volume).toBe(150)
        expect(candles[1].values.tradeCount).toBe(1)
    })

    it('supports virtual tables (no deltas emitted for raw rows)', async () => {
        const p = new Pipeline()
        const orders = p.table(
            'orders',
            {block_number: uint64(), trader: string(), amount: float64()},
            {virtual: true},
        )

        orders
            .createReducer('stats', {
                groupBy: 'trader',
                initialState: {total: 0},
                reduce(state, row) {
                    const total = state.total + row.amount
                    state.update({total})
                    state.emit({trader: row.trader, total})
                },
            })
            .createView('summary', {
                groupBy: ['trader'],
                select: (agg) => ({
                    trader: agg.key.trader,
                    total: agg.last('total'),
                }),
            })

        const db = p.build()
        const batch = await db.ingest({
            data: {
                orders: [{block_number: 1, trader: 'alice', amount: 100}],
            },
            finalizedHead: {number: 1, hash: '0x1'},
            rollbackChain: [{number: 1, hash: '0x1'}],
        })

        expect(batch).toBeTruthy()
        expect(batch!.tables.orders).toBeUndefined()
        expect(batch!.tables.summary).toHaveLength(1)
        expect(batch!.tables.summary[0].values.total).toBe(100)
    })

    it('creates a sliding window view with time-based expiry', async () => {
        const p = new Pipeline()
        const trades = p.table('trades', {
            block_number: uint64(),
            block_time: datetime(),
            pair: string(),
            volume: float64(),
        })

        trades.createView('volume_1h', {
            groupBy: ['pair'],
            select: (agg) => ({
                pair: agg.key.pair,
                totalVolume: agg.sum('volume'),
                tradeCount: agg.count(),
            }),
            slidingWindow: {
                interval: '1 hour',
                timeColumn: 'block_time',
            },
        })

        const db = p.build()

        // Ingest blocks 1 and 2
        const batch1 = await db.ingest({
            data: {
                trades: [
                    {block_number: 1, pair: 'ETH', volume: 100, block_time: 0},
                    {block_number: 2, pair: 'ETH', volume: 200, block_time: 1_800_000},
                ],
            },
            finalizedHead: {number: 2, hash: '0x2'},
            rollbackChain: [{number: 2, hash: '0x2'}, {number: 1, hash: '0x1'}],
        })

        expect(batch1).toBeTruthy()
        const vol1 = batch1!.tables.volume_1h
        // Should have Insert + Update for ETH
        const latest1 = vol1[vol1.length - 1]
        expect(latest1.values.totalVolume).toBe(300)
        expect(latest1.values.tradeCount).toBe(2)

        // Block 3: ETH volume=50 at t=1hr+1s → block 1 expires
        const batch2 = await db.ingest({
            data: {
                trades: [
                    {block_number: 3, pair: 'ETH', volume: 50, block_time: 3_601_000},
                ],
            },
            finalizedHead: {number: 3, hash: '0x3'},
            rollbackChain: [{number: 3, hash: '0x3'}, {number: 2, hash: '0x2'}, {number: 1, hash: '0x1'}],
        })

        expect(batch2).toBeTruthy()
        const vol2 = batch2!.tables.volume_1h
        expect(vol2).toHaveLength(1)
        // After expiry: 200 + 50 = 250
        expect(vol2[0].values.totalVolume).toBe(250)
        expect(vol2[0].values.tradeCount).toBe(2)
    })

    it('sliding window emits Delete when group fully expires', async () => {
        const p = new Pipeline()
        const trades = p.table('trades', {
            block_number: uint64(),
            block_time: datetime(),
            pair: string(),
            volume: float64(),
        })

        trades.createView('volume_1h', {
            groupBy: ['pair'],
            select: (agg) => ({
                pair: agg.key.pair,
                totalVolume: agg.sum('volume'),
            }),
            slidingWindow: {
                interval: '1 hour',
                timeColumn: 'block_time',
            },
        })

        const db = p.build()

        // DOGE at t=0
        await db.ingest({
            data: {
                trades: [{block_number: 1, pair: 'DOGE', volume: 1000, block_time: 0}],
            },
            finalizedHead: {number: 1, hash: '0x1'},
            rollbackChain: [{number: 1, hash: '0x1'}],
        })

        // ETH at t=1hr+1s → DOGE expires completely
        const batch = await db.ingest({
            data: {
                trades: [{block_number: 2, pair: 'ETH', volume: 100, block_time: 3_601_000}],
            },
            finalizedHead: {number: 2, hash: '0x2'},
            rollbackChain: [{number: 2, hash: '0x2'}, {number: 1, hash: '0x1'}],
        })

        expect(batch).toBeTruthy()
        const records = batch!.tables.volume_1h

        const dogeDelete = records.find((r: any) => r.key.pair === 'DOGE')
        expect(dogeDelete).toBeDefined()
        expect(dogeDelete!.operation).toBe('delete')

        const ethInsert = records.find((r: any) => r.key.pair === 'ETH')
        expect(ethInsert).toBeDefined()
        expect(ethInsert!.operation).toBe('insert')
    })
})
