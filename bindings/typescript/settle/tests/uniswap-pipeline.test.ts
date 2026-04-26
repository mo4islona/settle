/**
 * Full Uniswap PnL pipeline using the builder API.
 *
 * swaps → swap_prices (reducer) → candles_5m (MV)
 *                                → wallet_pnl (chained reducer) → wallet_summary (MV)
 */

import { describe, expect, it } from 'vitest'
import { datetime, float64, interval, Pipeline, string, uint64 } from '../src/index'

const USDC = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
const WETH = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'

interface PriceState {
  ethUsd: number
}

interface PriceEmit {
  pool: string
  token: string
  blockTime: number
  priceUsd: number
  volumeUsd: number
  sender: string
  baseDelta: number
}

interface Position {
  balance: number
  costUsd: number
}

interface WalletState {
  positions: Record<string, Position>
}

interface WalletEmit {
  sender: string
  pool: string
  realizedPnl: number
  position: number
}

function buildPipeline() {
  const p = new Pipeline()

  const swaps = p.table(
    'swaps',
    {
      block_number: uint64(),
      block_time: datetime(),
      network: string(),
      pool: string(),
      token0: string(),
      token1: string(),
      sender: string(),
      amount0: float64(),
      amount1: float64(),
    },
    { virtual: true },
  )

  const swapPrices = swaps.createReducer<PriceState, PriceEmit>('swap_prices', {
    groupBy: 'network',
    initialState: { ethUsd: 0 },
    reduce(state, row) {
      if (row.amount0 === 0) return

      const t0 = row.token0.toLowerCase()
      const t1 = row.token1.toLowerCase()
      const ratio = Math.abs(row.amount1 / row.amount0)

      let priceUsd = 0
      let target = ''
      let volumeUsd = 0
      let baseDelta = 0
      let newEthUsd = state.ethUsd

      if (t1 === USDC) {
        priceUsd = ratio
        target = t0
        baseDelta = row.amount0
        volumeUsd = Math.abs(row.amount1)
        if (t0 === WETH) newEthUsd = priceUsd
      } else if (t0 === USDC) {
        priceUsd = ratio > 0 ? 1 / ratio : 0
        target = t1
        baseDelta = row.amount1
        volumeUsd = Math.abs(row.amount0)
        if (t1 === WETH) newEthUsd = priceUsd
      } else if (t1 === WETH) {
        priceUsd = ratio * state.ethUsd
        target = t0
        baseDelta = row.amount0
        volumeUsd = Math.abs(row.amount1) * state.ethUsd
      } else if (t0 === WETH) {
        priceUsd = ratio > 0 ? state.ethUsd / ratio : 0
        target = t1
        baseDelta = row.amount1
        volumeUsd = Math.abs(row.amount0) * state.ethUsd
      } else {
        return
      }

      if (priceUsd <= 0) return

      state.update({ ethUsd: newEthUsd })
      state.emit({
        pool: row.pool,
        token: target,
        blockTime: row.block_time,
        priceUsd,
        volumeUsd,
        sender: row.sender,
        baseDelta,
      })
    },
  })

  swapPrices.createView('candles_5m', {
    groupBy: ['pool', interval('blockTime', '5 minutes').as('window_start')],
    select: (agg) => ({
      pool: agg.key.pool,
      windowStart: agg.key.window_start,
      open: agg.first('priceUsd'),
      high: agg.max('priceUsd'),
      low: agg.min('priceUsd'),
      close: agg.last('priceUsd'),
      volume: agg.sum('volumeUsd'),
      tradeCount: agg.count(),
    }),
  })

  const walletPnl = swapPrices.createReducer<WalletState, WalletEmit>('wallet_pnl', {
    groupBy: 'sender',
    initialState: { positions: {} },
    reduce(state, row) {
      const pos: Position = state.positions[row.token] ?? { balance: 0, costUsd: 0 }
      let pnl = 0

      if (row.baseDelta > 0) {
        pos.balance += row.baseDelta
        pos.costUsd += row.baseDelta * row.priceUsd
      } else if (row.baseDelta < 0 && pos.balance > 0) {
        const sold = Math.abs(row.baseDelta)
        const avgCost = pos.costUsd / pos.balance
        pnl = sold * (row.priceUsd - avgCost)
        pos.balance -= sold
        pos.costUsd -= sold * avgCost
      }

      state.update({ positions: { ...state.positions, [row.token]: pos } })
      state.emit({ sender: row.sender, pool: row.pool, realizedPnl: pnl, position: pos.balance })
    },
  })

  walletPnl.createView('wallet_summary', {
    groupBy: ['sender', 'pool'],
    select: (agg) => ({
      sender: agg.key.sender,
      pool: agg.key.pool,
      totalPnl: agg.sum('realizedPnl'),
      currentPosition: agg.last('position'),
      tradeCount: agg.count(),
    }),
  })

  return p
}

describe('Uniswap PnL pipeline', () => {
  it('resolves ETH/USDC price and produces candles', async () => {
    const db = buildPipeline().build()

    // amount0 > 0 = sender buys token0, amount1 < 0 = sender pays token1
    const batch = await db.ingest({
      data: {
        swaps: [
          {
            block_number: 1,
            network: 'eth',
            pool: 'WETH/USDC',
            token0: WETH,
            token1: USDC,
            sender: '0xalice',
            amount0: 1,
            amount1: -2000,
            block_time: 60000,
          },
          {
            block_number: 1,
            network: 'eth',
            pool: 'WETH/USDC',
            token0: WETH,
            token1: USDC,
            sender: '0xbob',
            amount0: 2,
            amount1: -4100,
            block_time: 120000,
          },
        ],
      },
      finalizedHead: { number: 1, hash: '0x1' },
      rollbackChain: [{ number: 1, hash: '0x1' }],
    })

    expect(batch).toBeTruthy()
    expect(batch!.tables.swaps).toBeUndefined() // virtual

    const candles = batch!.tables.candles_5m
    expect(candles).toHaveLength(1)
    // ratio = |amount1/amount0|, price = ratio (t1 is USDC)
    expect(candles[0].values.open).toBe(2000)
    expect(candles[0].values.close).toBe(2050) // 4100/2
    expect(candles[0].values.high).toBe(2050)
    expect(candles[0].values.tradeCount).toBe(2)
  })

  it('computes wallet PnL with average cost method', async () => {
    const db = buildPipeline().build()

    // Block 1: alice buys 1 WETH @ 2000 (amount0>0 = buy token0)
    // Block 2: alice buys 1 more WETH @ 2200, then sells 1 WETH @ 2400
    const batch = await db.ingest({
      data: {
        swaps: [
          {
            block_number: 1,
            network: 'eth',
            pool: 'WETH/USDC',
            token0: WETH,
            token1: USDC,
            sender: '0xalice',
            amount0: 1,
            amount1: -2000,
            block_time: 60000,
          },
          {
            block_number: 2,
            network: 'eth',
            pool: 'WETH/USDC',
            token0: WETH,
            token1: USDC,
            sender: '0xalice',
            amount0: 1,
            amount1: -2200,
            block_time: 120000,
          },
          {
            block_number: 2,
            network: 'eth',
            pool: 'WETH/USDC',
            token0: WETH,
            token1: USDC,
            sender: '0xalice',
            amount0: -1,
            amount1: 2400,
            block_time: 180000,
          },
        ],
      },
      finalizedHead: { number: 2, hash: '0x2' },
      rollbackChain: [{ number: 2, hash: '0x2' }, { number: 1, hash: '0x1' }],
    })

    expect(batch).toBeTruthy()
    const wallet = batch!.tables.wallet_summary
    expect(wallet).toHaveLength(1)
    expect(wallet[0].values.tradeCount).toBe(3)
    expect(wallet[0].values.currentPosition).toBe(1) // bought 2, sold 1

    // avg cost = (2000 + 2200) / 2 = 2100, sold 1 @ 2400 → PnL = 300
    expect(wallet[0].values.totalPnl).toBeCloseTo(300, 0)
  })

  it('handles cross-price via WETH', async () => {
    const db = buildPipeline().build()
    const TOKEN_X = '0xtokenx'

    // Block 1: establish WETH price at 2000 USDC
    // Block 2: swap TOKEN_X / WETH → price resolved via ETH cross
    const batch = await db.ingest({
      data: {
        swaps: [
          {
            block_number: 1,
            network: 'eth',
            pool: 'WETH/USDC',
            token0: WETH,
            token1: USDC,
            sender: '0xalice',
            amount0: 1,
            amount1: -2000,
            block_time: 60000,
          },
          {
            block_number: 2,
            network: 'eth',
            pool: 'TOKENX/WETH',
            token0: TOKEN_X,
            token1: WETH,
            sender: '0xbob',
            amount0: 100,
            amount1: -1,
            block_time: 120000,
          },
        ],
      },
      finalizedHead: { number: 2, hash: '0x2' },
      rollbackChain: [{ number: 2, hash: '0x2' }, { number: 1, hash: '0x1' }],
    })

    expect(batch).toBeTruthy()

    // TOKEN_X price = ratio * ethUsd = (1/100) * 2000 = 20
    const candles = batch!.tables.candles_5m
    const tokenXCandle = candles.find((c: any) => c.key.pool === 'TOKENX/WETH')
    expect(tokenXCandle).toBeDefined()
    expect(tokenXCandle!.values.open).toBeCloseTo(20, 2)
  })

  it('supports rollback across the full pipeline', async () => {
    const db = buildPipeline().build()

    // Ingest blocks 1 and 2
    await db.ingest({
      data: {
        swaps: [
          {
            block_number: 1,
            network: 'eth',
            pool: 'WETH/USDC',
            token0: WETH,
            token1: USDC,
            sender: '0xalice',
            amount0: 1,
            amount1: -2000,
            block_time: 60000,
          },
          {
            block_number: 2,
            network: 'eth',
            pool: 'WETH/USDC',
            token0: WETH,
            token1: USDC,
            sender: '0xalice',
            amount0: 1,
            amount1: -2200,
            block_time: 120000,
          },
        ],
      },
      finalizedHead: { number: 1, hash: '0x1' },
      rollbackChain: [{ number: 2, hash: '0x2' }, { number: 1, hash: '0x1' }],
    })

    // Rollback block 2: ingest with rollbackChain that only includes block 1
    const batch = await db.ingest({
      data: {},
      finalizedHead: { number: 1, hash: '0x1' },
      rollbackChain: [{ number: 1, hash: '0x1' }],
    })

    expect(batch).toBeTruthy()
    const wallet = batch!.tables.wallet_summary
    expect(wallet).toHaveLength(1)
    expect(wallet[0].values.tradeCount).toBe(1)
    expect(wallet[0].values.currentPosition).toBe(1) // only block 1's buy
  })

  it('isolates wallet PnL per sender', async () => {
    const db = buildPipeline().build()

    const batch = await db.ingest({
      data: {
        swaps: [
          {
            block_number: 1,
            network: 'eth',
            pool: 'WETH/USDC',
            token0: WETH,
            token1: USDC,
            sender: '0xalice',
            amount0: 2,
            amount1: -4000,
            block_time: 60000,
          },
          {
            block_number: 1,
            network: 'eth',
            pool: 'WETH/USDC',
            token0: WETH,
            token1: USDC,
            sender: '0xbob',
            amount0: 1,
            amount1: -2000,
            block_time: 120000,
          },
        ],
      },
      finalizedHead: { number: 1, hash: '0x1' },
      rollbackChain: [{ number: 1, hash: '0x1' }],
    })

    expect(batch).toBeTruthy()
    const wallet = batch!.tables.wallet_summary
    expect(wallet).toHaveLength(2)

    const alice = wallet.find((r: any) => r.key.sender === '0xalice')
    const bob = wallet.find((r: any) => r.key.sender === '0xbob')
    expect(alice!.values.currentPosition).toBe(2)
    expect(bob!.values.currentPosition).toBe(1)
  })
})
