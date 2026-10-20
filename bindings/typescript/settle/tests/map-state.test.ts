/**
 * Map state fields in JS bodies, native and WASM: the batches of a body that
 * reads, sets and deletes Map entries across a fork match the batches of the
 * same body in Lua.
 */

import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { beforeAll, describe, expect, it } from 'vitest'
import {
  type ChangeBatch,
  type ISettle,
  type ReducerCtx,
  Settle,
  type StateMap,
} from '../src/index'
import { init, Settle as WebSettle } from '../src/web'
import { ingestAndAck } from './util'

beforeAll(async () => {
  await init(readFileSync(resolve(__dirname, '../src/wasm/settle_bg.wasm')))
})

const TABLES = `
  CREATE TABLE moves (
    block_number UInt64,
    user         String,
    token        String,
    amount       Float64
  );
`

const STATE = `
  STATE (
    book  Map(String, Json),
    moves Float64 DEFAULT 0,
    seen  Map(String, Float64)
  )
`

const VIEW = `
  CREATE MATERIALIZED VIEW holdings AS
  SELECT user, token, last(qty) AS qty, last(before) AS before, last(seen) AS seen,
         last(moves) AS moves
  FROM books
  GROUP BY user, token;
`

const LUA = `${TABLES}
  CREATE REDUCER books SOURCE moves GROUP BY user ${STATE}
  LANGUAGE lua PROCESS $$
    local p = state.book:get(row.token) or { qty = 0, n = 0 }
    local before = p.qty
    p.qty = p.qty + row.amount
    p.n = p.n + 1
    if p.qty == 0 then state.book:delete(row.token) else state.book:set(row.token, p) end
    local seen = (state.seen:get(row.token) or 0) + 1
    state.seen:set(row.token, seen)
    state.moves = state.moves + 1
    emit({ token = row.token, before = before, qty = p.qty, seen = seen, moves = state.moves })
  $$;
${VIEW}`

const EXTERNAL = `${TABLES}
  CREATE REDUCER books SOURCE moves GROUP BY user ${STATE}
  LANGUAGE EXTERNAL;
${VIEW}`

interface Position {
  qty: number
  n: number
}

interface Books {
  book: StateMap<Position>
  moves: number
  seen: StateMap<number>
}

/** The Lua body above. A changed position stays only after a `set`. */
function books(state: ReducerCtx<Books, unknown>, row: any) {
  const p = state.book.get(row.token) ?? { qty: 0, n: 0 }
  const before = p.qty
  p.qty += row.amount
  p.n += 1
  if (p.qty === 0) state.book.delete(row.token)
  else state.book.set(row.token, p)

  const seen = (state.seen.get(row.token) ?? 0) + 1
  state.seen.set(row.token, seen)
  state.update({ moves: state.moves + 1 })
  state.emit({ token: row.token, before, qty: p.qty, seen, moves: state.moves })
}

const move = (block_number: number, user: string, token: string, amount: number) => ({
  block_number,
  user,
  token,
  amount,
})

const at = (number: number, hash = `0x${number}`) => ({ number, hash })

/** A key as text, its fields in name order: the WASM batch has another field order. */
const keyText = (key: Record<string, unknown>) =>
  JSON.stringify(
    Object.keys(key)
      .sort()
      .map((name) => [name, key[name]]),
  )

/** The tables of a batch, records sorted by key, numbers as numbers. */
function tables(batch: ChangeBatch | null) {
  if (batch === null) return null
  const plain = (v: unknown) =>
    JSON.parse(JSON.stringify(v, (_, x) => (typeof x === 'bigint' ? Number(x) : x)))
  const out: Record<string, unknown[]> = {}
  for (const [name, records] of Object.entries(batch.tables)) {
    out[name] = records
      .map((r) => plain({ op: r.operation, key: r.key, values: r.values, prev: r.prevValues }))
      .sort((a, b) => keyText(a.key).localeCompare(keyText(b.key)))
  }
  return out
}

/**
 * The batches of three blocks, a fork back to block 1, two new blocks 2 and
 * 3, and a final block 4. Block 2 deletes `a/X`, so the new block 2 reads
 * it as it was after block 1.
 */
async function run(db: ISettle) {
  const out = []
  out.push(
    await ingestAndAck(db, {
      data: {
        moves: [
          move(1, 'a', 'X', 5),
          move(1, 'a', 'Y', 2),
          move(1, 'b', 'X', 1),
          move(2, 'a', 'X', -5),
          move(2, 'b', 'X', 2),
          move(3, 'a', 'X', 3),
          move(3, 'a', 'Y', 1),
        ],
      },
      finalizedHead: at(0),
      rollbackChain: [at(1), at(2), at(3)],
    }),
  )

  const fork = db.handleFork([at(1)])
  if (fork.batch) db.ack(fork.batch.sequence)
  out.push(fork.batch)

  out.push(
    await ingestAndAck(db, {
      data: {
        moves: [
          move(2, 'a', 'X', 1),
          move(2, 'b', 'Y', 4),
          move(3, 'a', 'X', -6),
          move(3, 'b', 'X', 1),
        ],
      },
      finalizedHead: at(1),
      rollbackChain: [at(1), at(2, '0x2b'), at(3, '0x3b')],
    }),
  )
  out.push(
    await ingestAndAck(db, {
      data: { moves: [move(4, 'a', 'X', 2)] },
      finalizedHead: at(4),
      rollbackChain: [at(4)],
    }),
  )
  return out.map(tables)
}

const holding = (batch: any, user: string, token: string) =>
  batch.holdings.find((r: any) => r.key.user === user && r.key.token === token).values

describe('Map state fields in JS bodies', () => {
  it('match the same body in Lua across a fork', async () => {
    const lua = await run(Settle.open({ schema: LUA }))

    const native = Settle.open({ schema: EXTERNAL })
    native.registerReducerCallback<Books>('books', books)
    expect(await run(native)).toEqual(lua)

    const web = WebSettle.open({ schema: EXTERNAL })
    web.registerReducerCallback<Books>('books', books)
    expect(await run(web)).toEqual(lua)

    // After the fork, a/X goes on from block 1 (5), not from the undone delete of block 2.
    expect(holding(lua[2], 'a', 'X')).toMatchObject({ qty: 0, before: 6, seen: 3 })
    expect(holding(lua[3], 'a', 'X')).toMatchObject({ qty: 2, before: 0, seen: 4, moves: 5 })
  })

  it('read back an entry in WASM as native does', async () => {
    const readBack = async (db: ISettle) => {
      const got: unknown[] = []
      db.registerReducerCallback<Books>('books', (state) => {
        const book = state.book as StateMap<unknown>
        book.set('big', { amount: 1n << 80n })
        book.set('proto', JSON.parse('{"__proto__": {"x": 1}}'))
        book.set('bytes', { raw: new Uint8Array([1, 2, 3]) })
        const throws = {
          get amount() {
            throw new Error('a getter threw')
          },
        }
        expect(() => book.set('big', throws)).toThrow()

        for (const key of ['big', 'proto', 'bytes']) got.push(book.get(key))
      })
      await db.ingest({ data: { moves: [move(1, 'a', 'X', 1)] }, finalizedHead: at(1) })
      return JSON.stringify(got)
    }

    const native = await readBack(Settle.open({ schema: EXTERNAL }))
    expect(native).toBe(
      '[{"amount":"1208925819614629174706176"},{"__proto__":{"x":1}},{"raw":[1,2,3]}]',
    )
    expect(await readBack(WebSettle.open({ schema: EXTERNAL }))).toBe(native)
  })

  for (const [name, open] of [
    ['native', () => Settle.open({ schema: EXTERNAL })],
    ['WASM', () => WebSettle.open({ schema: EXTERNAL })],
  ] as const) {
    it(`refuse a handle outside the rows of its group (${name})`, async () => {
      const db: ISettle = open()
      let kept: StateMap<number> | undefined
      db.registerReducerCallback<Books>('books', (state) => {
        kept?.get('X')
        kept = state.seen
      })

      await expect(
        db.ingest({
          data: { moves: [move(1, 'a', 'X', 1), move(1, 'b', 'X', 1)] },
          finalizedHead: at(1),
        }),
      ).rejects.toThrow("Map field 'seen' is used outside the rows of its group")
    })
  }
})
