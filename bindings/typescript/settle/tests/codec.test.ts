/**
 * Values that cross the native boundary in the binary layout of `codec.ts`:
 * ingest rows, reducer groups and results, change batches.
 */

import { describe, expect, it } from 'vitest'
import { Settle } from '../src/index'
import { ingestAndAck } from './util'

const NOTES_SCHEMA = `
  CREATE TABLE notes (block_number UInt64, user String, text String);
  CREATE REDUCER tagged SOURCE notes GROUP BY user STATE (last String DEFAULT '') LANGUAGE EXTERNAL;
  CREATE MATERIALIZED VIEW latest AS
  SELECT user, last(text) AS text, last(prev) AS prev
  FROM tagged
  GROUP BY user;
`

function at(block: number) {
  return {
    finalizedHead: { number: 0, hash: '0x0' },
    rollbackChain: [{ number: block, hash: `0x${block}` }],
  }
}

describe('Binary bridge', () => {
  it('keeps strings past ASCII in rows, states, emits and batches', async () => {
    const db = Settle.open({ schema: NOTES_SCHEMA })
    const seen: unknown[] = []
    db.registerReducerCallback<{ last: string }>('tagged', (state, row: any) => {
      seen.push([row.user, row.text, state.last])
      const prev = state.last
      state.update({ last: row.text })
      state.emit({ text: `${row.text}✓`, prev })
    })

    const batch = await ingestAndAck(db, {
      data: {
        notes: [
          { block_number: 1, user: 'ü', text: 'é' },
          { block_number: 1, user: 'plain', text: 'ascii' },
          { block_number: 1, user: 'ü', text: '😀 Ω' },
        ],
      },
      ...at(1),
    })

    expect(seen).toEqual([
      ['ü', 'é', ''],
      ['ü', '😀 Ω', 'é'],
      ['plain', 'ascii', ''],
    ])
    const latest = (user: string) => batch!.tables.latest.find((r) => r.key.user === user)!.values
    expect(latest('ü')).toEqual({ user: 'ü', text: '😀 Ω✓', prev: 'é' })
    expect(latest('plain')).toEqual({ user: 'plain', text: 'ascii✓', prev: '' })
    expect(batch!.tables.notes.map((r) => r.values.text)).toEqual(['é', 'ascii', '😀 Ω'])
  })

  it('passes a Null row field as undefined and a Null state field as null', async () => {
    const db = Settle.open({ schema: NOTES_SCHEMA })
    const seen: unknown[] = []
    db.registerReducerCallback<{ last: string | null }>('tagged', (state, row: any) => {
      seen.push([row.text, state.last])
      state.update({ last: null })
    })

    await ingestAndAck(db, { data: { notes: [{ block_number: 1, user: 'a' }] }, ...at(1) })
    await ingestAndAck(db, {
      data: { notes: [{ block_number: 2, user: 'a', text: 'x' }] },
      ...at(2),
    })

    expect(seen).toEqual([
      [undefined, ''],
      ['x', null],
    ])
  })

  it('keeps the state of a group whose reducer does not update it', async () => {
    const db = Settle.open({ schema: NOTES_SCHEMA })
    const seen: unknown[] = []
    db.registerReducerCallback<{ last: string }>('tagged', (state, row: any) => {
      seen.push(state.last)
      if (row.text === 'set') state.update({ last: 'kept' })
    })

    await ingestAndAck(db, {
      data: { notes: [{ block_number: 1, user: 'a', text: 'set' }] },
      ...at(1),
    })
    await ingestAndAck(db, {
      data: { notes: [{ block_number: 2, user: 'a', text: 'no' }] },
      ...at(2),
    })
    await ingestAndAck(db, {
      data: { notes: [{ block_number: 3, user: 'a', text: 'no' }] },
      ...at(3),
    })

    expect(seen).toEqual(['', 'kept', 'kept'])
  })

  it('rejects the ingest with the error that a reducer throws', async () => {
    const db = Settle.open({ schema: NOTES_SCHEMA })
    db.registerReducerCallback<{ last: string }>('tagged', (state, row: any) => {
      if (row.text === 'bad') throw new Error('reducer says no')
      state.update({ last: row.text })
      state.emit({ text: row.text, prev: state.last })
    })

    await expect(
      db.ingest({ data: { notes: [{ block_number: 1, user: 'a', text: 'bad' }] }, ...at(1) }),
    ).rejects.toThrow('reducer says no')

    // The failed ingest is undone: the same block goes in again.
    expect(db.isPoisoned).toBe(false)
    const batch = await ingestAndAck(db, {
      data: { notes: [{ block_number: 1, user: 'a', text: 'good' }] },
      ...at(1),
    })
    expect(batch!.tables.latest[0].values).toEqual({ user: 'a', text: 'good', prev: 'good' })
  })

  it('carries bytes, big integers and JSON on ingest as msgpack did', async () => {
    const db = Settle.open({
      schema: 'CREATE TABLE blobs (block_number UInt64, data BYTEA, big UInt64, meta String);',
    })
    const big = 2n ** 63n
    const batch = await ingestAndAck(db, {
      data: {
        blobs: [{ block_number: 1, data: new Uint8Array([1, 2, 255]), big, meta: { a: [1, 'x'] } }],
      },
      ...at(1),
    })

    const values = batch!.tables.blobs[0].values
    expect(values.data).toBeInstanceOf(Uint8Array)
    expect([...values.data]).toEqual([1, 2, 255])
    expect(values.big).toBe(big)
    expect(values.meta).toBe('{"a":[1,"x"]}')
    expect(batch!.tables.blobs[0].key).toEqual({ block_number: 1, _row_index: 0 })
  })

  it('rejects rows of a table that the schema lacks, unless there are none', async () => {
    const db = Settle.open({ schema: NOTES_SCHEMA })
    await expect(db.ingest({ data: { nope: [{ block_number: 1 }] }, ...at(1) })).rejects.toThrow(
      'unknown table: nope',
    )
    await expect(db.ingest({ data: { nope: [] }, ...at(1) })).resolves.toBeNull()
  })

  it('rejects a row without a block number', async () => {
    const db = Settle.open({ schema: NOTES_SCHEMA })
    await expect(
      db.ingest({ data: { notes: [{ user: 'a', text: 'x' }] }, ...at(1) }),
    ).rejects.toThrow("row in table 'notes' missing block_number")
  })

  it('decodes the compensating batch of a fork', async () => {
    const db = Settle.open({ schema: NOTES_SCHEMA })
    db.registerReducerCallback<{ last: string }>('tagged', (state, row: any) => {
      const prev = state.last
      state.update({ last: row.text })
      state.emit({ text: row.text, prev })
    })

    await ingestAndAck(db, {
      data: {
        notes: [
          { block_number: 1, user: 'a', text: 'one' },
          { block_number: 2, user: 'a', text: 'two' },
        ],
      },
      finalizedHead: { number: 0, hash: '0x0' },
      rollbackChain: [
        { number: 1, hash: '0x1' },
        { number: 2, hash: '0x2' },
      ],
    })

    const { cursor, batch } = db.handleFork([{ number: 1, hash: '0x1' }])
    expect(cursor).toEqual({ number: 1, hash: '0x1' })
    expect(batch).not.toBeNull()
    const view = batch!.tables.latest
    expect(view).toHaveLength(1)
    expect(view[0].operation).toBe('update')
    expect(view[0].values).toEqual({ user: 'a', text: 'one', prev: '' })
    expect(view[0].prevValues).toEqual({ user: 'a', text: 'two', prev: 'one' })
  })
})
