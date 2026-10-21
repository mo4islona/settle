import { type ChangeBatch, Pipeline, type SettleCursor, string, uint64 } from '@settle/stream'

const RPC_URL = process.env.RPC_URL ?? 'https://ethereum-rpc.publicnode.com'
const START_BLOCK = process.env.START_BLOCK ? Number(process.env.START_BLOCK) : undefined
const MAX_BLOCKS = Number(process.env.MAX_BLOCKS ?? Infinity)

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

// [!region rpc]
type Block = {
  number: number
  hash: string
  parentHash: string
  miner: string
  gasUsed: number
}

async function rpc(method: string, params: unknown[]) {
  const response = await fetch(RPC_URL, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }),
  })

  const { result, error } = await response.json()
  if (error) throw new Error(error.message)

  return result
}

async function getBlock(tag: number | 'latest' | 'finalized'): Promise<Block> {
  const param = typeof tag === 'number' ? `0x${tag.toString(16)}` : tag
  const block = await rpc('eth_getBlockByNumber', [param, false])

  return {
    number: Number(block.number),
    hash: block.hash,
    parentHash: block.parentHash,
    miner: block.miner,
    gasUsed: Number(block.gasUsed),
  }
}
// [!endregion rpc]

// [!region pipeline]
const p = new Pipeline()

p.table(
  'blocks',
  { block_number: uint64(), fee_recipient: string(), gas_used: uint64() },
  { virtual: true },
).createView('fee_recipients', {
  groupBy: 'fee_recipient',
  select: (agg) => ({
    fee_recipient: agg.key.fee_recipient,
    blocks: agg.count(),
    gas: agg.sum('gas_used'),
  }),
})

const db = p.build({ dataDir: './settle-data' })
// [!endregion pipeline]

// [!region apply]
async function apply(batch: ChangeBatch | null) {
  if (!batch) return

  for (const record of batch.tables.fee_recipients ?? []) {
    console.log(record.operation, record.values)
  }

  db.ack(batch.sequence)
}
// [!endregion apply]

// [!region reorg]
async function handleReorg(from: number): Promise<SettleCursor> {
  const finalized = await getBlock('finalized')
  let depth = 32

  for (;;) {
    const lowest = Math.max(finalized.number, from - depth)

    const chain: SettleCursor[] = []
    for (let n = from; n >= lowest; n--) {
      const block = await getBlock(n)
      chain.push({ number: block.number, hash: block.hash })
    }

    // Without a common block, look further back, but not below finality.
    const found = db.resolveForkCursor(chain) !== null
    if (found || lowest === finalized.number) {
      const fork = db.handleFork(chain)
      await apply(fork.batch)
      return fork.cursor
    }

    depth *= 2
  }
}
// [!endregion reorg]

// [!region loop]
let last: SettleCursor | null = db.cursor
let next = last ? last.number + 1 : (START_BLOCK ?? (await getBlock('latest')).number)

for (let count = 0; count < MAX_BLOCKS; ) {
  const head = await getBlock('latest')
  if (next > head.number) {
    await sleep(1000)
    continue
  }

  const block = await getBlock(next)

  if (last && block.parentHash !== last.hash) {
    last = await handleReorg(last.number)
    console.log('fork point:', last.number)
    next = last.number + 1
    continue
  }

  const finalized = await getBlock('finalized')

  const batch = await db.ingest({
    data: {
      blocks: [{ block_number: block.number, fee_recipient: block.miner, gas_used: block.gasUsed }],
    },
    rollbackChain: [{ number: block.number, hash: block.hash }],
    finalizedHead: { number: finalized.number, hash: finalized.hash },
  })

  await apply(batch)

  last = { number: block.number, hash: block.hash }
  next = block.number + 1
  count++
}
// [!endregion loop]
