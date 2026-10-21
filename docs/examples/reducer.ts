import { Pipeline, float64, string, uint64 } from '@settle/stream'

// [!region pipeline]
const p = new Pipeline()

const trades = p.table(
  'trades',
  {
    block_number: uint64(),
    trader: string(),
    side: string(),
    amount: float64(),
    price: float64(),
  },
  { virtual: true },
)

type PnlState = { position: number; cost: number }
type PnlRow = { profit: number; position: number }

const pnl = trades.createReducer<PnlState, PnlRow>('pnl', {
  groupBy: 'trader',
  initialState: { position: 0, cost: 0 },
  reduce(state, row) {
    if (row.side === 'buy') {
      const position = state.position + row.amount
      const cost = state.cost + row.amount * row.price

      state.update({ position, cost })
      state.emit({ profit: 0, position })
      return
    }

    const averagePrice = state.cost / state.position
    const position = state.position - row.amount
    const cost = state.cost - row.amount * averagePrice

    state.update({ position, cost })
    state.emit({ profit: row.amount * (row.price - averagePrice), position })
  },
})

pnl.createView('trader_pnl', {
  groupBy: 'trader',
  select: (agg) => ({
    trader: agg.key.trader,
    profit: agg.sum('profit'),
    position: agg.last('position'),
    trades: agg.count(),
  }),
})
// [!endregion pipeline]

const db = p.build()

// [!region ingest]
const batch = await db.ingest({
  data: {
    trades: [
      { block_number: 1, trader: 'alice', side: 'buy', amount: 10, price: 2000 },
      { block_number: 2, trader: 'alice', side: 'buy', amount: 10, price: 2400 },
      { block_number: 3, trader: 'alice', side: 'sell', amount: 5, price: 2600 },
    ],
  },
  rollbackChain: [
    { number: 1, hash: '0x1a' },
    { number: 2, hash: '0x2a' },
    { number: 3, hash: '0x3a' },
  ],
  finalizedHead: { number: 0, hash: '0x00' },
})

for (const record of batch?.tables.trader_pnl ?? []) {
  console.log(record.operation, record.values)
}
// [!endregion ingest]

if (batch) db.ack(batch.sequence)
