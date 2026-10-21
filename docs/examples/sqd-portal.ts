import { commonAbis, evmEventDecoder, evmPortalStream } from '@subsquid/pipes/evm'
import { Pipeline, float64, string, uint64 } from '@settle/stream'
import { settleTarget } from './pipes-target.js'

const USDC = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'

// [!region pipeline]
const p = new Pipeline()

const flows = p.table(
  'usdc_flows',
  { block_number: uint64(), wallet: string(), amount: float64() },
  { virtual: true },
)

flows.createView('usdc_net_flow', {
  groupBy: 'wallet',
  select: (agg) => ({
    wallet: agg.key.wallet,
    net: agg.sum('amount'),
    transfers: agg.count(),
  }),
})

const db = p.build({ dataDir: './settle-data' })
// [!endregion pipeline]

// [!region stream]
const stream = evmPortalStream({
  id: 'usdc-net-flow',
  portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
  outputs: evmEventDecoder({
    range: { from: 21_000_000, to: 21_000_020 },
    contracts: [USDC],
    events: { transfers: commonAbis.erc20.events.Transfer },
  }),
})

await stream.pipeTo(
  settleTarget({
    db,
    toRows: ({ transfers }) => ({
      usdc_flows: transfers.flatMap(({ block, event }) => {
        const amount = Number(event.value) / 1e6
        return [
          { block_number: block.number, wallet: event.from, amount: -amount },
          { block_number: block.number, wallet: event.to, amount },
        ]
      }),
    }),
    apply: async (batch) => {
      const records = batch.tables.usdc_net_flow ?? []
      console.log(`batch ${batch.sequence}: ${records.length} changes, block ${batch.latestHead?.number}`)
    },
  }),
)
// [!endregion stream]
