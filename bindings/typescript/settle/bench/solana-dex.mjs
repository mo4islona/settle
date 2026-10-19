// Solana DEX PnL through the napi binding: the Node side of
// benches/solana_dex.rs, with the same swaps, schema and target (a store
// that applies each change batch at once).
//
//   SDEX_WRITE=/path/sdex.bin cargo bench --bench solana_dex   # the swaps
//   pnpm run build:native && pnpm run build:ts
//   node bench/solana-dex.mjs /path/sdex.bin [cases]
//
// Cases: `<runtime>,<mode>` separated by spaces; runtimes `js` (JS callbacks,
// LANGUAGE EXTERNAL), `lua`, `er` (EventRules) and `floor` (the same logic
// in plain JS Maps, no Settle: what the JS work alone costs); modes
// `backfill` and `tip` as in the Rust bench. Each case runs in a process of
// its own. Env: SDEX_BATCH_SLOTS (100), SDEX_STORAGE (rocks | memory),
// SDEX_POSITIONS (pair | json: the positions of a wallet in one JSON map).

import { spawnSync } from 'node:child_process'
import { mkdirSync, mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { createRequire } from 'node:module'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { PerformanceObserver, performance } from 'node:perf_hooks'

const require = createRequire(import.meta.url)
const REPO = fileURLToPath(new URL('../../../../', import.meta.url))
const SQL = (name) => readFileSync(join(REPO, 'benches/solana_dex', name), 'utf8')

const SOL = 'So11111111111111111111111111111111111111112'
const SLOT_SECONDS = 0.4
const CONFIRMATION_DEPTH = 32
const TIP_SHARE = 0.2
const BATCH_SLOTS = Number(process.env.SDEX_BATCH_SLOTS ?? 100)
const STORAGE = process.env.SDEX_STORAGE ?? 'rocks'
const JSON_POSITIONS = process.env.SDEX_POSITIONS === 'json'
const ALL_CASES = [
  'js,backfill', 'lua,backfill', 'er,backfill', 'floor,backfill',
  'js,tip', 'lua,tip', 'er,tip',
]

// ─── The swaps ───────────────────────────────────────────────────────

/** Read the file of `Dataset::write` (benches/solana_dex/dataset.rs). */
function load(path) {
  const file = readFileSync(path)
  const buffer = file.buffer.slice(file.byteOffset, file.byteOffset + file.byteLength)
  const view = new DataView(buffer)
  const magic = new TextDecoder().decode(new Uint8Array(buffer, 0, 8))
  if (magic !== 'SDEX0001') throw new Error(`${path}: not a swaps file`)

  const align = (n) => Math.ceil(n / 8) * 8
  const stringBytes = view.getUint32(12, true)
  const strings = new TextDecoder().decode(new Uint8Array(buffer, 16, stringBytes)).split('\n')

  let at = align(16 + stringBytes)
  const blockCount = view.getUint32(at, true)
  const swapCount = view.getUint32(at + 4, true)
  at += 8
  const take = (Type, count) => {
    const array = new Type(buffer, at, count)
    at += array.byteLength
    return array
  }
  const slot = take(Float64Array, blockCount)
  const timestamp = take(Float64Array, blockCount)
  const solUsd = take(Float64Array, blockCount)
  const lengths = take(Uint32Array, blockCount)
  at = align(at)
  const base = take(Float64Array, swapCount)
  const quoteAmount = take(Float64Array, swapCount)
  const dex = take(Uint32Array, swapCount)
  const pool = take(Uint32Array, swapCount)
  const trader = take(Uint32Array, swapCount)
  const token = take(Uint32Array, swapCount)
  const quote = take(Uint32Array, swapCount)
  const buy = take(Uint8Array, swapCount)

  // The row objects that a decoder hands over, one array for each block.
  const blocks = []
  let i = 0
  for (let b = 0; b < blockCount; b++) {
    const rows = new Array(lengths[b])
    for (let r = 0; r < rows.length; r++, i++) {
      rows[r] = {
        block_number: slot[b],
        timestamp: timestamp[b],
        dex: strings[dex[i]],
        pool: strings[pool[i]],
        trader: strings[trader[i]],
        token: strings[token[i]],
        quote: strings[quote[i]],
        side: buy[i] ? 'buy' : 'sell',
        base_amount: base[i],
        quote_amount: quoteAmount[i],
        sol_usd: solUsd[b],
      }
    }
    blocks.push({ slot: slot[b], rows })
  }
  return { blocks, swaps: swapCount }
}

// ─── JS reducers: the logic of the Lua reducers ──────────────────────

function poolPrices(state, row) {
  let usd = row.quote_amount
  if (row.quote === SOL) usd *= row.sol_usd
  const price = usd / row.base_amount
  const change = state.price_usd > 0 ? price / state.price_usd - 1 : 0

  state.update({
    price_usd: price,
    volume_usd: state.volume_usd + usd,
    swaps: state.swaps + 1,
  })
  state.emit({
    token: row.token,
    dex: row.dex,
    timestamp: row.timestamp,
    price_usd: price,
    usd,
    price_change: change,
  })
}

function positions(state, row) {
  let usd = row.quote_amount
  if (row.quote === SOL) usd *= row.sol_usd
  const base = row.base_amount
  const price = usd / base
  let qty = state.qty
  let cost = state.cost_usd

  let realized = 0
  if (row.side === 'buy') {
    qty += base
    cost += usd
    state.update({
      qty,
      cost_usd: cost,
      realized_usd: state.realized_usd,
      buys: state.buys + 1,
      sells: state.sells,
    })
  } else {
    const avg = qty > 0 ? cost / qty : 0
    const matched = Math.min(base, qty)
    realized = matched * price - matched * avg
    qty -= matched
    cost -= matched * avg
    state.update({
      qty,
      cost_usd: cost,
      realized_usd: state.realized_usd + realized,
      buys: state.buys,
      sells: state.sells + 1,
    })
  }

  state.emit({
    realized,
    usd,
    price_usd: price,
    position: qty,
    cost_usd: cost,
    win: realized > 0 ? 1 : 0,
    loss: realized < 0 ? 1 : 0,
  })
}

/** lua/positions_json.sql: the positions of a wallet in one JSON map. */
function positionsJson(state, row) {
  let usd = row.quote_amount
  if (row.quote === SOL) usd *= row.sol_usd
  const base = row.base_amount
  const price = usd / base

  const book = state.book
  let p = book[row.token]
  if (!p) {
    p = { qty: 0, cost_usd: 0, realized_usd: 0, buys: 0, sells: 0 }
    book[row.token] = p
  }

  let realized = 0
  if (row.side === 'buy') {
    p.qty += base
    p.cost_usd += usd
    p.buys++
  } else {
    const avg = p.qty > 0 ? p.cost_usd / p.qty : 0
    const matched = Math.min(base, p.qty)
    realized = matched * price - matched * avg
    p.qty -= matched
    p.cost_usd -= matched * avg
    p.realized_usd += realized
    p.sells++
  }
  state.update({ book })

  state.emit({
    token: row.token,
    realized,
    usd,
    price_usd: price,
    position: p.qty,
    cost_usd: p.cost_usd,
    win: realized > 0 ? 1 : 0,
    loss: realized < 0 ? 1 : 0,
  })
}

/**
 * The two reducers and the five views in Maps, without rollback, change
 * records or storage: the least that the JS work of this pipeline costs.
 */
class Floor {
  pools = new Map()
  positions = new Map()
  candles = new Map()
  dexVolume = new Map()
  poolStats = new Map()
  wallets = new Map()
  open = new Map()

  ingest(rows) {
    for (const row of rows) {
      let usd = row.quote_amount
      if (row.quote === SOL) usd *= row.sol_usd
      const base = row.base_amount
      const price = usd / base

      let pool = this.pools.get(row.pool)
      if (!pool) this.pools.set(row.pool, (pool = { price_usd: 0, volume_usd: 0, swaps: 0 }))
      pool.price_usd = price
      pool.volume_usd += usd
      pool.swaps++

      const minute = Math.floor(row.timestamp / 60_000) * 60_000
      const candleKey = `${row.token}|${minute}`
      const candle = this.candles.get(candleKey)
      if (candle) {
        if (price > candle.high) candle.high = price
        if (price < candle.low) candle.low = price
        candle.close = price
        candle.volume_usd += usd
        candle.trades++
      } else {
        this.candles.set(candleKey, {
          open: price, high: price, low: price, close: price, volume_usd: usd, trades: 1,
        })
      }

      const hour = Math.floor(row.timestamp / 3_600_000) * 3_600_000
      const dexKey = `${row.dex}|${hour}`
      const dex = this.dexVolume.get(dexKey)
      if (dex) {
        dex.volume_usd += usd
        dex.swaps++
      } else {
        this.dexVolume.set(dexKey, { volume_usd: usd, swaps: 1 })
      }

      const stats = this.poolStats.get(row.pool)
      if (stats) {
        stats.price_usd = price
        stats.volume_usd += usd
        stats.swaps++
      } else {
        this.poolStats.set(row.pool, { price_usd: price, volume_usd: usd, swaps: 1 })
      }

      const positionKey = `${row.trader}|${row.token}`
      let position = this.positions.get(positionKey)
      if (!position) {
        position = { qty: 0, cost_usd: 0, realized_usd: 0, buys: 0, sells: 0 }
        this.positions.set(positionKey, position)
      }
      let realized = 0
      if (row.side === 'buy') {
        position.qty += base
        position.cost_usd += usd
        position.buys++
      } else {
        const avg = position.qty > 0 ? position.cost_usd / position.qty : 0
        const matched = Math.min(base, position.qty)
        realized = matched * price - matched * avg
        position.qty -= matched
        position.cost_usd -= matched * avg
        position.realized_usd += realized
        position.sells++
      }

      const wallet = this.wallets.get(row.trader)
      const win = realized > 0 ? 1 : 0
      const loss = realized < 0 ? 1 : 0
      if (wallet) {
        wallet.realized_pnl += realized
        wallet.volume_usd += usd
        wallet.trades++
        wallet.wins += win
        wallet.losses += loss
      } else {
        this.wallets.set(row.trader, {
          realized_pnl: realized, volume_usd: usd, trades: 1, wins: win, losses: loss,
        })
      }

      this.open.set(positionKey, { qty: position.qty, cost_usd: position.cost_usd, last_price: price })
    }
  }

  totals() {
    let realized = 0
    for (const w of this.wallets.values()) realized += w.realized_pnl
    let volume = 0
    for (const d of this.dexVolume.values()) volume += d.volume_usd
    let trades = 0
    for (const c of this.candles.values()) trades += c.trades
    return { realized, volume, trades }
  }
}

// ─── Target and measurements ─────────────────────────────────────────

/** Applies each batch at once; keeps the totals of benches/solana_dex.rs. */
class Target {
  records = 0
  realized = 0
  volume = 0
  trades = 0

  apply(batch) {
    for (const [table, records] of Object.entries(batch.tables)) {
      this.records += records.length
      const column = { wallet_pnl: 'realized_pnl', dex_volume_1h: 'volume_usd', token_candles_1m: 'trades' }[table]
      const total = { wallet_pnl: 'realized', dex_volume_1h: 'volume', token_candles_1m: 'trades' }[table]
      if (!column) continue
      for (const r of records) {
        const value = Number(r.values[column] ?? 0)
        if (r.operation === 'insert') this[total] += value
        else if (r.operation === 'update') this[total] += value - Number(r.prevValues?.[column] ?? 0)
        else this[total] -= value
      }
    }
  }
}

/** Where the time of the ingests went, from the perf tree of each batch. */
class Split {
  encode = 0
  decode = 0
  dag = 0
  raw = 0
  /** Reducer name → [its own time, the time of its views]. */
  reducers = {}
  parallel = false

  add(nodes) {
    for (const node of nodes) {
      if (node.name === 'encode') this.encode += node.durationMs
      else if (node.name === 'decode') this.decode += node.durationMs
      else if (node.name === 'ingest') {
        this.dag += node.durationMs
        this.walk(node.children, false)
      }
    }
  }

  // A reducer node holds its views as children; in a parallel branch its
  // time covers them too.
  walk(nodes, inParallel) {
    for (const node of nodes) {
      if (node.kind === 'raw_table') {
        this.raw += node.durationMs
      } else if (node.kind === 'reducer') {
        const views = node.children.reduce((sum, c) => sum + c.durationMs, 0)
        const own = inParallel ? node.durationMs - views : node.durationMs
        const total = (this.reducers[node.name] ??= [0, 0])
        total[0] += own
        total[1] += views
      } else if (node.kind === 'parallel') {
        this.parallel = true
        this.walk(node.children, true)
      } else if (node.kind === 'pipeline') {
        this.walk(node.children, inParallel)
      }
    }
  }
}

const cursor = (slot) => ({ number: slot, hash: `0x${slot.toString(16).padStart(16, '0')}` })

function open(runtime) {
  const dir = { js: 'external', lua: 'lua', er: 'er' }[runtime]
  const positionsFile = JSON_POSITIONS ? 'positions_json.sql' : 'positions.sql'
  const schema = [
    SQL('tables.sql'),
    SQL(`${dir}/pool_prices.sql`),
    SQL(`${dir}/${positionsFile}`),
    SQL('views.sql'),
  ].join('\n')

  let dataDir
  if (STORAGE === 'rocks') {
    mkdirSync(join(REPO, 'target/tmp'), { recursive: true })
    dataDir = mkdtempSync(join(REPO, 'target/tmp/sdex-node-'))
  }
  const db = Settle.open({ schema, dataDir })
  if (runtime === 'js') {
    db.registerReducerCallback('pool_prices', poolPrices)
    db.registerReducerCallback('positions', JSON_POSITIONS ? positionsJson : positions)
  }
  return { db, dataDir }
}

/** Batches of `size` blocks: the rows of all, and the last slot. */
function batches(blocks, size) {
  const out = []
  for (let i = 0; i < blocks.length; i += size) {
    const chunk = blocks.slice(i, i + size)
    out.push({ rows: chunk.flatMap((b) => b.rows), slots: chunk.map((b) => b.slot), end: i + chunk.length })
  }
  return out
}

const percentile = (sorted, p) => sorted[Math.round((sorted.length - 1) * p)] ?? 0

async function runCase(data, runtime, mode) {
  const { blocks } = data
  let gcMs = 0
  const gc = new PerformanceObserver((list) => {
    for (const entry of list.getEntries()) gcMs += entry.duration
  })

  const floor = runtime === 'floor' ? new Floor() : null
  const { db, dataDir } = floor ? {} : open(runtime)
  const target = new Target()
  const split = new Split()

  // One ingest, applied and acked: the time of the ingest and of the ack.
  const ingest = async (rows, slots, finalizedIndex) => {
    if (floor) {
      const started = performance.now()
      floor.ingest(rows)
      return [performance.now() - started, 0]
    }
    const finalized = blocks[finalizedIndex].slot
    const started = performance.now()
    const batch = await db.ingest({
      data: { swaps: rows },
      rollbackChain: slots.filter((s) => s > finalized).map(cursor),
      finalizedHead: cursor(finalized),
    })
    const ingestMs = performance.now() - started
    if (!batch) return [ingestMs, 0]
    split.add(batch.perf)
    target.apply(batch)
    const acked = performance.now()
    db.ack(batch.sequence)
    return [ingestMs, performance.now() - acked]
  }

  let timedFrom = 0
  if (mode === 'tip') {
    timedFrom = Math.floor((1 - TIP_SHARE) * blocks.length)
    for (const b of batches(blocks.slice(0, timedFrom), BATCH_SLOTS)) {
      await ingest(b.rows, b.slots, Math.max(b.end - 1 - CONFIRMATION_DEPTH, 0))
    }
    split.encode = split.decode = split.dag = split.raw = 0
    split.reducers = {}
  }

  const timed = blocks.slice(timedFrom)
  const work = mode === 'backfill'
    ? batches(timed, BATCH_SLOTS).map((b) => ({ ...b, finalized: b.end - 1 }))
    : timed.map((b, i) => ({
        rows: b.rows, slots: [b.slot], finalized: timedFrom + i - CONFIRMATION_DEPTH,
      }))
  const swaps = work.reduce((n, w) => n + w.rows.length, 0)

  const ingestMs = []
  let ackMs = 0
  const recordsBefore = target.records
  gc.observe({ entryTypes: ['gc'] })
  const rssBefore = process.memoryUsage().rss
  const cpuBefore = process.cpuUsage()
  const started = performance.now()

  for (const w of work) {
    const [i, a] = await ingest(w.rows, w.slots, w.finalized)
    ingestMs.push(i + a)
    ackMs += a
    // A real source waits for the network here, which lets the event loop run.
    await new Promise(setImmediate)
  }

  const wallMs = performance.now() - started
  const cpu = process.cpuUsage(cpuBefore)
  gc.disconnect()

  const quarter = Math.ceil(ingestMs.length / 4)
  const quarters = []
  for (let q = 0; q < ingestMs.length; q += quarter) {
    const ms = ingestMs.slice(q, q + quarter).reduce((a, b) => a + b, 0)
    const n = work.slice(q, q + quarter).reduce((a, w) => a + w.rows.length, 0)
    quarters.push(n / (ms / 1000))
  }

  const first = timed[0].slot
  const last = timed[timed.length - 1].slot
  const totals = floor ? floor.totals() : target
  if (dataDir) rmSync(dataDir, { recursive: true, force: true })

  return {
    label: `${runtime},${mode}${JSON_POSITIONS && runtime !== 'floor' ? ',json' : ''}`,
    swaps,
    slots: last - first + 1,
    wallMs,
    cpuMs: (cpu.user + cpu.system) / 1000,
    gcMs,
    ingestMs: ingestMs.sort((a, b) => a - b),
    totalIngestMs: ingestMs.reduce((a, b) => a + b, 0),
    ackMs,
    split,
    quarters,
    records: target.records - recordsBefore,
    rssBeforeMb: rssBefore / 2 ** 20,
    rssPeakMb: process.resourceUsage().maxRSS / 1024,
    check: { realized: totals.realized, volume: totals.volume, trades: totals.trades },
  }
}

function report(r) {
  const secs = r.wallMs / 1000
  const sorted = r.ingestMs
  const share = (ms) => `${((100 * ms) / r.totalIngestMs).toFixed(0)}%`
  const reducers = Object.entries(r.split.reducers).map(
    ([name, [own, views]]) => `${name} ${share(own)} + views ${share(views)}`,
  )
  const rest = r.totalIngestMs - r.split.encode - r.split.dag - r.split.decode - r.ackMs

  console.log(
    `${r.label.padEnd(20)} ${(r.swaps / secs).toFixed(0).padStart(9)} swaps/s  ` +
      `${((r.slots * SLOT_SECONDS) / secs).toFixed(1).padStart(5)}x real time  ${secs.toFixed(2).padStart(7)} s  ` +
      `cores ${(r.cpuMs / r.wallMs).toFixed(2).padStart(5)}  ` +
      `ingest p50 ${percentile(sorted, 0.5).toFixed(2).padStart(7)} p99 ${percentile(sorted, 0.99).toFixed(2).padStart(7)} ` +
      `max ${sorted[sorted.length - 1].toFixed(2).padStart(7)} ms  GC ${(r.gcMs / 1000).toFixed(2)} s  ` +
      `peak RSS ${r.rssPeakMb.toFixed(0)} MB (before ${r.rssBeforeMb.toFixed(0)})`,
  )
  if (!r.label.startsWith('floor')) {
    console.log(
      `${''.padEnd(20)} time: encode ${share(r.split.encode)}, DAG ${share(r.split.dag)} ` +
        `(raw ${share(r.split.raw)}; ${reducers.join('; ')}${r.split.parallel ? ', in parallel' : ''}), ` +
        `finality+batch ${share(rest)}, decode ${share(r.split.decode)}, commit ${share(r.ackMs)}`,
    )
  }
  console.log(
    `${''.padEnd(20)} swaps/s by quarter: ${r.quarters.map((q) => q.toFixed(0)).join(' / ')}   ` +
      `change records ${(r.records / secs).toFixed(0)}/s   ` +
      `check: realized ${r.check.realized.toExponential(6)} volume ${r.check.volume.toExponential(6)} ` +
      `trades ${r.check.trades}`,
  )
}

// ─── Main ────────────────────────────────────────────────────────────

const [path, ...rest] = process.argv.slice(2)
if (!path) {
  console.error('usage: node bench/solana-dex.mjs <swaps file> [runtime,mode ...]')
  process.exit(2)
}

let Settle
if (rest[0] === '--case') {
  ;({ Settle } = require('../dist/index.js'))
  const [runtime, mode] = rest[1].split(',')
  const data = load(path)
  const result = await runCase(data, runtime, mode)
  process.stdout.write(`\n${JSON.stringify(result)}\n`)
} else {
  // EventRules have no maps; the floor does not change.
  const defaults = JSON_POSITIONS ? ALL_CASES.filter((c) => /^(js|lua),/.test(c)) : ALL_CASES
  const cases = rest.length ? rest : defaults
  console.log(
    `node ${process.version}, storage ${STORAGE}, ${BATCH_SLOTS} slots in each backfill ingest, ` +
      `positions ${JSON_POSITIONS ? 'json' : 'pair'}\n`,
  )
  for (const c of cases) {
    const child = spawnSync(
      process.execPath,
      ['--max-old-space-size=32768', fileURLToPath(import.meta.url), path, '--case', c],
      { encoding: 'utf8', maxBuffer: 1 << 28, stdio: ['ignore', 'pipe', 'inherit'] },
    )
    const lines = (child.stdout ?? '').trim().split('\n')
    if (child.status !== 0) {
      console.log(`${c}: failed (exit ${child.status})`)
      continue
    }
    report(JSON.parse(lines[lines.length - 1]))
  }
}
