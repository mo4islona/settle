# settle

Embedded rollback-aware computation engine for blockchain data. Processes raw rows through a DAG pipeline of reducers
and materialized views, emitting change batches (insert/update/delete) grouped by table.

## Architecture

```
Raw Tables ──► Reducers (Lua / Event Rules) ──► Materialized Views ──► Change Batches
                    │                                  │
                    └──── state snapshots ─────────────┘
                          (rollback-safe)
```

- **Raw Tables** — Append-only storage for incoming blockchain data. Supports `VIRTUAL` tables that skip change emission.
- **Reducers** — Stateful processors with `GROUP BY` routing. Lua scripts or declarative event rules. State is
  snapshotted per-block for rollback.
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
// record.operation, record.key, record.values, record.prev_values
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

## Storage Backends

- **Memory** — Default. No persistence, data lost on restart.
- **RocksDB** — Set `dataDir` for crash-safe persistence with WAL-based atomic commits.

## Performance

Benchmarked on Apple M-series (`cargo bench`). Independent reducer branches execute in parallel via `rayon`.

| Benchmark                   | Description                                                          | Memory  | RocksDB |
|-----------------------------|----------------------------------------------------------------------|---------|---------|
| Raw table ingestion         | 200K rows, 2K blocks × 100 rows                                      | ~793K/s | ~774K/s |
| Raw + MV                    | 200K rows, 2K blocks × 100 rows, sum + count MV                      | ~325K/s | ~327K/s |
| Full pipeline — Lua         | 50K rows, 1K blocks × 50 rows, Lua reducer + MV, 100 groups          | ~171K/s | ~165K/s |
| Full pipeline — Event Rules | 100K rows, 2K blocks × 50 rows, declarative reducer + MV, 100 groups | ~137K/s | ~129K/s |
| Reducer only — Event Rules  | 200K rows, 2K blocks × 100 rows, no MV or storage                    | ~923K/s | —       |
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
