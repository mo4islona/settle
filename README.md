# settle

Embedded rollback-aware computation engine for blockchain data. Processes raw rows through a DAG pipeline of reducers
and materialized views, emitting change batches (insert/update/delete) grouped by table.

## Architecture

```
Raw Tables ──► Reducers (Lua / External) ──► Materialized Views ──► Change Batches
                    │                               │
                    └──── state snapshots ──────────┘
                          (rollback-safe)
```

- **Raw Tables** — Append-only storage for incoming blockchain data. Supports `VIRTUAL` tables that skip change emission.
- **Reducers** — Stateful processors with `GROUP BY` routing. Lua scripts or external (host-language) callbacks. State
  is snapshotted per-block for rollback.
- **Materialized Views** — SQL-like aggregations (`sum`, `count`, `avg`, `min`, `max`, `first`, `last`, `ohlcv`) with
  automatic rollback support.
- **Change Batches** — Output grouped by table: `{ tables: { "table_name": [ChangeRecord, ...] } }`. Each record carries
  `operation` (insert/update/delete), `key`, `values`, and `prevValues`.

## Schema Definition

```sql
CREATE TABLE orders (
    block_number UInt64,
    trader       String,
    asset_id     String,
    usdc         UInt64,
    shares       UInt64,
    side         UInt64
);

CREATE REDUCER market_stats
SOURCE orders
GROUP BY asset_id
STATE (
    volume Float64 DEFAULT 0,
    trades UInt64  DEFAULT 0
)
LANGUAGE lua
PROCESS $$
    local vol = row.usdc / 1000000
    state.volume = state.volume + vol
    state.trades = state.trades + 1
    emit.asset_id = row.asset_id
    emit.volume = vol
$$;

CREATE MATERIALIZED VIEW token_summary AS
SELECT
    asset_id,
    sum(volume)  AS total_volume,
    count()      AS trade_count
FROM market_stats
GROUP BY asset_id;
```

### Map state

A state field can be a map from String keys to values, for example `book Map(String, JSON)`. A body changes one
entry at a time, so undo, finality and checkpoints cost as much as the changed entries, not the whole map. In Lua:
`state.book:get(key)` (a copy, or nil), `state.book:set(key, value)` (nil deletes) and `state.book:delete(key)`. In a
Rust closure: `state.map("book")`. JS reducers do not support Map fields yet. A reducer without `GROUP BY` has one
group, for example a price map with an entry for each token.

## Usage (Rust)

```rust
use settle::db::{Config, Settle};

let db = Settle::open(Config {
schema: schema_string,
data_dir: Some("/tmp/my-db".into()),
max_buffer_size: 10_000,
}) ?;

// Process rows
db.process_batch("orders", block_number, rows) ?;

// Finalize older blocks
db.finalize(block_number - 10);

// Get changes
if let Some(batch) = db.flush() {
for (table, records) in & batch.tables {
for record in records {
// record.operation, record.get("balance"), record.key("user"), record.prev("balance");
// record.values() iterates (name, value) in schema order
}
}
db.ack(batch.sequence);
}

// Rollback on chain reorg
db.rollback(fork_point);
```

## Usage (TypeScript)

```bash
npm install @settle/stream
```

```typescript
import {Settle} from '@settle/stream'

const db = Settle.open({schema: SCHEMA})

db.processBatch('orders', blockNumber, rows)
db.finalize(blockNumber - 10)

const batch = db.flush()
if (batch) {
    const summaries = batch.tables['token_summary'] ?? []
    for (const record of summaries) {
        console.log(record.operation, record.key, record.values)
    }
    db.ack(batch.sequence)
}
```

## Acks and durability

`ingest()` and `handle_fork()` return a change batch. Apply it to the target,
then call `ack(batch.sequence)`: only then does its state reach disk. With
RocksDB, `ack()` syncs the write-ahead log, so an acked batch also survives a
power loss; that is one fsync per `ack()` call. A reopen goes back to the last
acked batch, so the target must apply batches idempotently.

By default two batches may wait for their ack, so the next blocks can be
ingested while the target still applies the batch before. That is enough to
keep one target busy; `max_pending_batches` (`maxPendingBatches` in
TypeScript) changes it, and 1 makes `ingest()` wait for every ack:

- Apply every batch, in sequence order. A fork's compensating batch undoes
  the state after all the batches before it, acked or not.
- Acks are cumulative: `ack(n)` commits every batch up to `n`.
- A call that returns no batch while batches wait takes a place too: its
  writes commit with the newest batch. `unacked_batches()` counts these
  places.
- When `unacked_batches()` reaches `max_pending_batches`, `ingest()` and
  `handle_fork()` return `PendingAck`. Reducer registration and replay need
  every batch acked.
- After an error that poisons the instance, `ack()` still works. Ack the
  batches that the target applied, then drop and reopen: a reopen goes back
  to the last acked batch, and a later fork cannot undo the others. After a
  failed sync, `ack()` returns `SyncFailed` too: drop and reopen.

### Applied sequence

The process can stop after the target applied a batch and before its ack.
The reopen then goes back to the last acked batch, behind the target. The
same blocks sent again give the same batch, but if they fork away first,
the reopened instance cannot undo their changes in the target: it does not
know them. To close that gap, store `batch.sequence` in the target, in the
same transaction as the data of the batch, and pass it at open as
`applied_sequence` (`appliedSequence` in TypeScript; 0 before the first
batch). This turns on the journal:

- Each returned batch is on disk, in the journal, before the caller gets it.
  With RocksDB that write syncs the write-ahead log, and `ack()` does not
  sync: still one fsync per batch, but each batch is written twice.
- Open commits the journaled batches up to the applied sequence and drops the
  others, so Settle and the target hold the same state.
- Open fails when the target is behind the last acked batch, when it applied
  a batch that the journal does not hold, and when the journal holds batches
  but no applied sequence is given.
- After an error that poisons the instance, drop and reopen with the applied
  sequence: no ack is necessary.

## Storage Backends

- **Memory** — Default. No persistence, data lost on restart.
- **RocksDB** — Set `dataDir` for crash-safe persistence with WAL-based atomic commits.

### Schema changes

Stored rows and state are encoded by position, so a database remembers the
layout of each table, reducer and view: column names and types in order, a
reducer's source, `GROUP BY` and state fields, a view's source, select list,
`GROUP BY` and window. `open` refuses a schema that changes a stored layout.
Reducer logic, defaults, output aliases and new objects may change. A new
reducer or view does not see the finalized blocks from before it. To change a
layout, give the object a new name or rebuild the database.

`open` also refuses a database of another format version, and one without a
format version (written before format versions existed).

## Performance

Benchmarked on Apple M-series (`cargo bench`). Independent reducer branches execute in parallel via `rayon`.

| Benchmark                   | Description                                                          | Memory  | RocksDB |
|-----------------------------|----------------------------------------------------------------------|---------|---------|
| Raw table ingestion         | 200K rows, 2K blocks × 100 rows                                      | ~793K/s | ~774K/s |
| Raw + MV                    | 200K rows, 2K blocks × 100 rows, sum + count MV                      | ~325K/s | ~327K/s |
| Full pipeline — Lua         | 50K rows, 1K blocks × 50 rows, Lua reducer + MV, 100 groups          | ~171K/s | ~165K/s |
| Rollback                    | 75 blocks × 134 rows, undo all and emit compensating changes          | ~1.5M/s | ~1.4M/s |
| Ingest (atomic)             | 100K rows, 20 blocks × 5K rows, Raw + MV + finalize + flush          | ~756K/s | ~692K/s |
| Polymarket: market_stats    | 200K rows, 400 blocks × 500 rows, Lua reducer + MV, 10K tokens       | ~170K/s | ~170K/s |
| Polymarket: full pipeline   | 200K rows, 400 blocks × 500 rows, 2 reducers + 2 MVs, parallel       | ~148K/s | ~152K/s |
| Polymarket: 1M traders      | 500K rows, 500 blocks × 1K rows, 1M unique group keys                | ~125K/s | ~141K/s |

## Building

```bash
# Rust
cargo build --release
cargo test
cargo bench

# TypeScript bindings
cd bindings/typescript/settle
pnpm install
pnpm run build
```

## Key Dependencies

- `mlua` — Lua 5.4 runtime for reducer scripts
- `rocksdb` — Persistent storage backend
- `rustc-hash` — FxHash for hot-path hash maps
- `smallvec` — Stack-allocated group keys
- `rayon` — Parallel reducer branch execution
- `rmp-serde` — MessagePack serialization for N-API bridge

## License

MIT
