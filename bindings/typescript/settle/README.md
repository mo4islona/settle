# @settle/stream

Embedded rollback-aware computation engine for blockchain data. Routes raw events through **reducers** (TypeScript functions) and **materialized views**, producing change records (insert/update/delete) for downstream targets.

## Install

```bash
pnpm add @settle/stream
```

## Build from source

Requires **Rust** (stable) and **Node.js** >= 18.

```bash
pnpm install
pnpm run build        # release
pnpm run build:debug  # debug
```

## Quick start

```typescript
import { Pipeline, uint64, string, float64, interval } from '@settle/stream'

const p = new Pipeline()

const swaps = p.table('swaps', {
  block_number: uint64(),
  pool:         string(),
  amount:       float64(),
})

swaps
  .createReducer('totals', {
    groupBy: 'pool',
    initialState: { volume: 0 },
    reduce(state, row) {
      const volume = state.volume + row.amount
      return [{ volume }, { pool: row.pool, volume }]
    },
  })
  .createView('pool_volume', {
    groupBy: ['pool'],
    select: (agg) => ({
      pool:       agg.key.pool,
      totalVolume: agg.sum('volume'),
      tradeCount: agg.count(),
    }),
  })

const db = p.build()  // in-memory
// const db = p.build({ dataDir: './data' })  // persistent (RocksDB)

db.processBatch('swaps', 1000, [
  { pool: 'ETH/USDC', amount: 100 },
  { pool: 'ETH/USDC', amount: 200 },
])

const batch = db.flush()!
// batch.tables.pool_volume → [{ key: { pool: 'ETH/USDC' }, values: { totalVolume: 300, tradeCount: 2 } }]
```

## API

### Pipeline builder

```typescript
const p = new Pipeline()

// Define a table — column types are inferred into the row type
const table = p.table('name', {
  block_number: uint64(),
  user:         string(),
  amount:       float64(),
  timestamp:    datetime(),
  metadata:     json<MyType>(),  // json() is generic
})

// Create a reducer from a table (or another reducer)
const reducer = table.createReducer('reducer_name', {
  groupBy: 'user',                          // validated against row keys
  initialState: { total: 0 },              // state type inferred
  reduce(state, row) {                      // state & row fully typed
    return [
      { total: state.total + row.amount },  // new state
      { user: row.user, total: ... },       // emit (or null to skip)
    ]
  },
})

// Chain a reducer from another reducer's output
const chained = reducer.createReducer('downstream', { ... })

// Create a materialized view
reducer.createView('summary', {
  groupBy: ['user'],
  select: (agg) => ({
    user:   agg.key.user,
    total:  agg.sum('total'),         // validated against emit keys
    count:  agg.count(),
    first:  agg.first('total'),
    last:   agg.last('total'),
  }),
})

// Time-window grouping
reducer.createView('candles_5m', {
  groupBy: ['pool', interval('timestamp', '5 minutes').as('window_start')],
  select: (agg) => ({
    pool:        agg.key.pool,
    windowStart: agg.key.window_start,
    open:        agg.first('price'),
    high:        agg.max('price'),
    low:         agg.min('price'),
    close:       agg.last('price'),
    volume:      agg.sum('volume'),
  }),
})

// Build the database
const db = p.build()                          // in-memory
const db = p.build({ dataDir: './data' })     // RocksDB persistence
const db = p.build({ dataDir: ':memory:' })   // explicit in-memory (SQLite convention)
```

### Column types

| Function     | SQL type   | TypeScript type |
|-------------|------------|-----------------|
| `uint64()`  | `UInt64`   | `number`        |
| `int64()`   | `Int64`    | `number`        |
| `float64()` | `Float64`  | `number`        |
| `uint256()` | `Uint256`  | `bigint`        |
| `string()`  | `String`   | `string`        |
| `datetime()` | `DateTime` | `number`        |
| `boolean()` | `Boolean`  | `boolean`       |
| `bytes()`   | `Bytes`    | `Uint8Array`    |
| `base58()`  | `Base58`   | `string`        |
| `json<T>()` | `Json`     | `T` (default `any`) |

### Settle (low-level)

```typescript
import { Settle } from '@settle/stream'

const db = Settle.open({ schema: 'CREATE TABLE ...', dataDir: './data' })

db.processBatch('table', blockNumber, rows)
db.rollback(forkPoint)
db.finalize(blockNumber)
db.flush()        // → ChangeBatch | null
db.ingest(input)  // atomic: process + finalize + flush
```

### Aggregation functions

`sum`, `count`, `min`, `max`, `avg`, `first`, `last`

### Virtual tables

Tables declared with `{ virtual: true }` are processed by reducers but don't emit raw row changes:

```typescript
const orders = p.table('orders', { ... }, { virtual: true })
```

## Test

```bash
pnpm test
pnpm run lint
```
