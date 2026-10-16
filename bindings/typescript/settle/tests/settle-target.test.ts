/**
 * The `settleTarget` example loop: Settle ingests the next data while
 * `onChange` applies the batch before.
 */

import { setFlagsFromString } from 'node:v8'
import { runInNewContext } from 'node:vm'
import { describe, expect, it } from 'vitest'
import { type ChangeBatch, type SettleCursor, settleTarget } from '../examples/settle-target'

const SCHEMA = `
  CREATE TABLE events (
    block_number UInt64,
    user         String,
    amount       Float64
  );

  CREATE MATERIALIZED VIEW user_totals AS
  SELECT user, sum(amount) AS total
  FROM events
  GROUP BY user;
`

const hash = (n: number) => `0x${n.toString(16)}`

/** Block n with amount n, or no data, behind blocks 1..n. */
function item(n: number, withData = true) {
  const chain = Array.from({ length: n }, (_, i) => ({ number: i + 1, hash: hash(i + 1) }))
  const events = withData ? [{ block_number: n, user: 'alice', amount: n }] : []
  return {
    data: { events },
    ctx: {
      head: { finalized: { number: 0, hash: '0x0' } },
      state: { rollbackChain: chain },
    },
  }
}

/** A source of one block per item, logging each read. */
function source(blocks: number, log: string[]) {
  return function read(_cursor?: SettleCursor) {
    return (async function* () {
      for (let n = 1; n <= blocks; n++) {
        log.push(`read ${n}`)
        yield item(n)
      }
    })()
  }
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

function total(batch: ChangeBatch): number {
  return batch.tables.user_totals[0].values.total
}

describe('settleTarget', () => {
  it('applies every batch in order while the next one is ingested', async () => {
    const log: string[] = []
    const applied: ChangeBatch[] = []
    let running = 0

    const target = settleTarget({
      schema: SCHEMA,
      onChange: async ({ batch }) => {
        running += 1
        expect(running).toBe(1)
        log.push(`apply ${batch.latestHead?.number}`)
        await sleep(5)
        applied.push(batch)
        log.push(`applied ${batch.latestHead?.number}`)
        running -= 1
      },
    })
    await target.write({ read: source(6, log), logger: console })

    const sequences = applied.map((b) => b.sequence)
    expect(sequences).toEqual([...sequences].sort((a, b) => a - b))
    expect(applied.map((b) => b.latestHead?.number)).toEqual([1, 2, 3, 4, 5, 6])
    expect(total(applied[5])).toBe(21)

    // Block 2 is read and ingested while block 1 is applied.
    expect(log.indexOf('read 2')).toBeLessThan(log.indexOf('applied 1'))
  })

  it('stops at a failed onChange', async () => {
    const applied: number[] = []
    const target = settleTarget({
      schema: SCHEMA,
      onChange: async ({ batch }) => {
        await sleep(1)
        const block = batch.latestHead!.number
        if (block === 3) throw new Error('target is down')
        applied.push(block)
      },
    })

    await expect(target.write({ read: source(6, []), logger: console })).rejects.toThrow(
      'target is down',
    )
    expect(applied).toEqual([1, 2])
  })

  it('waits for room after an ingest that returns no batch', async () => {
    const applied: number[] = []
    const target = settleTarget({
      schema: SCHEMA,
      onChange: async ({ batch }) => {
        await sleep(5)
        applied.push(batch.latestHead!.number)
      },
    })

    // Each item with no data joins the unacked batch before it and takes
    // the second place in the queue.
    const read = (_cursor?: SettleCursor) =>
      (async function* () {
        for (let n = 1; n <= 3; n++) {
          yield item(n)
          yield item(n, false)
        }
      })()
    await target.write({ read, logger: console })

    expect(applied).toEqual([1, 2, 3])
  })

  it('stops at a failed onChange while it waits for the source', async () => {
    let release = () => {}
    const released = new Promise<void>((resolve) => {
      release = resolve
    })
    const read = (_cursor?: SettleCursor) =>
      (async function* () {
        yield item(1)
        await released
      })()

    const target = settleTarget({
      schema: SCHEMA,
      onChange: async () => {
        await sleep(1)
        throw new Error('target is down')
      },
    })
    const written = target.write({ read, logger: console }).then(
      () => 'done',
      (error: Error) => error.message,
    )
    const outcome = await Promise.race([written, sleep(500).then(() => 'still waiting')])
    release()

    expect(outcome).toBe('target is down')
  })

  it('handles a source error that comes after an apply failure', async () => {
    // The apply fails after `turns` microtask turns. Some of them land
    // between two reads, so the read after starts on an aborted signal, and
    // then the source fails.
    for (let turns = 0; turns < 20; turns++) {
      const read = (_cursor?: SettleCursor) =>
        (async function* () {
          yield item(1)
          yield item(1, false)
          await sleep(1)
          throw new Error('source is gone')
        })()

      const target = settleTarget({
        schema: SCHEMA,
        onChange: async () => {
          for (let i = 0; i < turns; i++) await null
          throw new Error('target is down')
        },
      })
      await expect(target.write({ read, logger: console })).rejects.toThrow('target is down')
    }
    // The source errors happen within this test.
    await sleep(10)
  })

  it('lets the apply in flight finish before it rethrows an error', async () => {
    const events: string[] = []
    const read = (_cursor?: SettleCursor) =>
      (async function* () {
        yield item(1)
        yield { data: item(2).data, ctx: { head: {}, state: item(2).ctx.state } }
      })()

    const target = settleTarget({
      schema: SCHEMA,
      onChange: async () => {
        await sleep(20)
        events.push('applied')
      },
    })
    await target.write({ read, logger: console }).catch(() => events.push('rejected'))

    expect(events).toEqual(['applied', 'rejected'])
  })

  it('keeps no source item alive after it is ingested', async () => {
    setFlagsFromString('--expose-gc')
    const gc = runInNewContext('gc') as () => void

    const items: WeakRef<object>[] = []
    const read = (_cursor?: SettleCursor) =>
      (async function* () {
        for (let n = 0; n < 500; n++) {
          const data = { payload: new Array(100).fill(n) }
          items.push(new WeakRef(data))
          yield {
            data,
            ctx: { head: { finalized: { number: 0, hash: '0x0' } }, state: { rollbackChain: [] } },
          }
        }
      })()

    const target = settleTarget({ schema: SCHEMA, transform: () => ({}), onChange: () => {} })
    await target.write({ read, logger: console })

    for (let i = 0; i < 3; i++) {
      await new Promise((resolve) => setImmediate(resolve))
      gc()
    }
    const alive = items.filter((ref) => ref.deref() !== undefined).length
    expect(alive).toBeLessThan(10)
    // The loop's closures stay reachable up to here.
    expect(target.write).toBeTypeOf('function')
  })

  it('applies a fork batch after the batches before it', async () => {
    const applied: ChangeBatch[] = []
    const target = settleTarget({
      schema: SCHEMA,
      onChange: async ({ batch }) => {
        await sleep(1)
        applied.push(batch)
      },
    })
    await target.write({ read: source(4, []), logger: console })

    const cursor = await target.fork([{ number: 2, hash: hash(2) }])
    expect(cursor).toEqual({ number: 2, hash: hash(2) })
    expect(applied).toHaveLength(5)
    expect(total(applied[4])).toBe(3)
  })
})
