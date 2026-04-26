# Settle — Implementation Plan

Targets **Phase 1 (PoC)** scope.

---

## Completed Steps

### Step 0: Project Scaffolding — DONE

- [x] Rust project with `Cargo.toml` (serde, serde_json, thiserror, sqlparser, rmp-serde, mlua, napi)
- [x] Module structure: `types`, `schema/{parser,ast}`, `storage/{mod,memory}`, `engine/{dag,raw_table,reducer,mv,aggregation}`, `reducer_runtime/{event_rules,lua}`, `change`, `db`, `napi`, `json_conv`, `error`

### Step 1: Core Types — DONE

- [x] `Value` enum: UInt64, Int64, Float64, Uint256, String, DateTime, Boolean, Bytes, Base58
- [x] `ColumnType` enum, `Row = HashMap<String, Value>`, `BlockNumber = u64`
- [x] `ChangeRecord { table, operation, key, values, prev_values }`
- [x] `ChangeBatch { sequence, finalized_block, latest_block, records }`

### Step 2: Schema Parser — DONE

- [x] `CREATE TABLE` parsing via sqlparser (table name, columns, types)
- [x] `CREATE MATERIALIZED VIEW ... AS SELECT ... GROUP BY` (aggregations: sum, count, min, max, avg, first, last; time windowing: toStartOfInterval)
- [x] `CREATE REDUCER` custom syntax (SOURCE, GROUP BY, STATE, WHEN/THEN/SET/EMIT, ALWAYS EMIT, LANGUAGE lua)
- [x] Schema validation (source references, group-by columns, aggregation args)

### Step 3: Storage Layer — DONE

- [x] `StorageBackend` trait: raw rows, reducer state/snapshots, MV state, metadata
- [x] `MemoryBackend` (BTreeMap + Mutex, interior mutability)
- [x] MessagePack serialization via rmp-serde
- [x] `take_raw_rows_after` combined remove+return operation (Step 18)

### Step 4: Raw Table Engine — DONE

- [x] `RawTableEngine`: ingest rows, store via storage, rollback, emit Insert/Delete changes
- [x] Optimized rollback via `take_raw_rows_after` (single BTreeMap pass)

### Step 5: Aggregation Functions — DONE

- [x] `AggregationFunc` trait: add_block, remove_block, remove_blocks_after, finalize_up_to, current_value
- [x] 7 implementations: Sum, Count, Min, Max, Avg, First, Last
- [x] Per-block contributions via BTreeMap, batch rollback via `split_off` (Step 18)

### Step 6: Materialized View Engine — DONE

- [x] `MVEngine`: group key tracking, aggregation routing, change emission (Insert/Update/Delete)
- [x] `block_groups: BTreeMap<BlockNumber, HashSet<GroupKey>>` for O(log N) rollback (Step 18)
- [x] toStartOfInterval time windowing

### Step 7: Reducer Runtime — Event Rules — DONE

- [x] Expression evaluator (arithmetic, comparison, IF/CASE)
- [x] WHEN/THEN blocks, LET bindings, SET state mutations, EMIT columns, ALWAYS EMIT

### Step 8: Reducer Runtime — Lua — DONE

- [x] mlua integration with sandboxing (no os/io/debug/loadfile)
- [x] VM reuse across rows (pre-compiled function in Lua registry) (Step 17)
- [x] json module for complex state

### Step 9: Reducer Engine — DONE

- [x] `ReducerEngine`: group-by, state schema, runtime dispatch (Event Rules or Lua)
- [x] In-memory state cache + block snapshots (Step 16)
- [x] Snapshot rollback via BTreeMap, finalization persists to storage
- [x] `block_groups: BTreeMap` for O(log N) rollback (Step 18)

### Step 10: DAG Wiring — DONE

- [x] `SettleEngine`: topological sort, pipeline processing (raw → reducer → MV)
- [x] `process_batch`, `rollback`, `finalize` orchestration

### Step 11: Change Buffer — DONE

- [x] `ChangeBuffer`: accumulation, merging (Insert+Update→Insert, etc.), sequence numbers
- [x] Backpressure via configurable max buffer size

### Step 12: Public API — DONE

- [x] `Settle` struct: open, process_batch, rollback, finalize, flush, ack
- [x] `Config`: schema string, max_buffer_size, optional storage backend

### Step 13: ClickHouse Adapter — DONE (example only)

- [x] `example.ts` shows ClickHouse integration via ReplacingMergeTree + lightweight deletes
- [ ] No Rust-side adapter (downstream apply is user's responsibility via onData callback)

### Step 14: Pipes SDK Integration — DONE (initial, needs rework)

- [x] napi-rs bindings: `JsSettle` class with open, processBatch, rollback, finalize, flush, ack
- [x] TypeScript type definitions (index.d.ts, pipes.d.ts)
- [x] `settleTarget` Pipes SDK target (pipes.js): write loop, fork handler, finalization
- [x] E2E tests (e2e.test.ts): raw tables, reducers, MVs, forks, multiple tables
- [ ] Needs rework — see [PIPES_SDK_PLAN.md](./PIPES_SDK_PLAN.md)

### Step 15: Integration & Benchmarks — DONE

- [x] E2E test: full DEX pipeline (tests/e2e_integration.rs)
- [x] Benchmarks (benches/throughput.rs)

### Step 16: Batch State Snapshots — DONE

- [x] Reducer snapshots once per block (not per row), in-memory BTreeMap
- [x] Storage writes only on finalization

### Step 17: Lua VM Reuse — DONE

- [x] Pre-compiled Lua function in registry, reused across all process() calls

### Step 18: Rollback Optimization — DONE

- [x] BTreeMap + split_off across reducer, MV, aggregations
- [x] Combined take_raw_rows_after in storage

### Benchmark Results

Run: `cargo bench --bench throughput`

#### Memory vs RocksDB

| Benchmark | Memory | RocksDB | Change |
|-----------|-------:|--------:|------:|
| Raw ingestion (200K rows) | 825K rows/s | 814K rows/s | -1% |
| Raw + MV (200K rows) | 280K rows/s | 286K rows/s | +2% |
| Full pipeline — Event Rules (100K rows) | 124K rows/s | 123K rows/s | -1% |
| Full pipeline — Lua (50K rows) | 129K rows/s | 130K rows/s | ~0% |
| Ingest + persist (Raw + MV, 100K rows) | 403K rows/s | 380K rows/s | -6% |
| Rollback (75 blocks, 10K rows) | 7.2ms | 7.3ms | ~0% |
| 100K unique group keys | 296K rows/s | 298K rows/s | ~0% |
| Reducer-only Event Rules (isolated) | 1183K rows/s | — | — |

#### RFC Section 12.2 Targets

| Metric | Target | Result | Status |
|--------|--------|--------|--------|
| Raw row ingestion | >100K rows/s | ~825K rows/s | PASS (8.3x) |
| Reducer — Event Rules (isolated) | >200K rows/s | ~1183K rows/s | PASS (5.9x) |
| Full pipeline (raw + reducer + MV) | >50K rows/s | ~124K rows/s | PASS (2.5x) |
| Full pipeline — Lua | >30K rows/s | ~129K rows/s | PASS (4.3x) |
| Rollback (75 blocks, 10K rows) | <10ms | ~7ms | PASS (1.4x) |
| Ingest + persist (atomic) | >20K rows/s | ~403K rows/s | PASS (20x) |

#### Observations

- **Memory vs RocksDB gap is small (1-5%)** — the bottleneck is computation, not storage I/O. RocksDB writes are buffered in memtables, so the cost is mostly serialization.
- **MV overhead is 2.9x** — going from 825K (raw only) to 280K (raw + MV). Group key hashing, aggregation state management, and change emission are significant.
- **Reducer adds another 2.3x** — going from 280K to 124K. Expression evaluation, state snapshotting, and group-key lookup dominate.
- **Rollback passes** — ~7ms for 10K rows.
- **Row type only for storage** — Step 20 showed that using `Row` (Vec<Value>) in the pipeline added conversion overhead. Keeping `RowMap` (HashMap) throughout the pipeline and only using `Row` for storage serialization gave the best results.
- **Custom binary format** — Step 21 replaced MessagePack+serde with a tag+data format for row encoding. Raw ingestion +17%, rollback +15%, full pipeline +4-6%. Direct RowMap encoding also eliminates the RowMap→Row conversion in the ingest path.
- **Deferred merge** — Step 22 made buffer push append-only and deferred merge to flush time. Raw ingestion +11%, 100K unique group keys +13%. Full pipeline unchanged (dominated by reducer/MV computation, not buffer overhead).

---
## Remaining Work

### Step 19: Rollback Optimization (Phase 2) — DONE

Rollback improved from ~10ms to ~7ms (30% faster). Three changes:

- [x] **Consume raw rows by value** — `RawTableEngine::rollback()` now iterates `rolled_back` by value with `into_iter()`, moving rows instead of cloning them. Eliminates one `HashMap::clone()` per row.
- [x] **Consume MV group keys by value** — `MVEngine::rollback()` consumes `split_off` result by value, moving `GroupKey`s into the `touched_keys` set instead of cloning.
- [x] **Consume reducer group keys by value** — `ReducerEngine::rollback()` same pattern, moving `Vec<u8>` keys instead of cloning.

### Step 20: Row Type Optimization — DONE

Added `Row` struct (`Arc<ColumnRegistry>` + `Vec<Value>`) for compact storage serialization. Column names are interned as `u16` indices and shared via `Arc`. The pipeline (reducer, MV) uses `RowMap` (HashMap) directly — no conversion overhead.

- [x] **Intern column names** — `ColumnRegistry` maps column names ↔ `u16` indices for storage encoding.
- [x] **Storage format** — raw rows stored as `Vec<Vec<Value>>` (values only, no keys). Encoding/decoding uses the table's `ColumnRegistry`. Only `RawTableEngine::ingest` and `rollback` create `Row` objects.
- [x] **Pipeline uses RowMaps** — `ReducerRuntime`, `ReducerEngine`, `MVEngine`, and `SettleEngine` all operate on `RowMap` (HashMap). No RowMap↔Row conversions in the hot path.
- [x] **Public API** — `Settle::process_batch` accepts `Vec<RowMap>`. RowMaps flow through the pipeline unchanged.
- [ ] **Arena allocation for String values** — deferred to a future step.

Benchmark: Raw ingestion +15% (557K→638K), reducer-only +19% (993K→1180K). Full pipeline benchmarks unchanged (no regression). The key insight: Row is only beneficial for storage serialization (compact Vec<Value> encoding). Using it for pipeline processing added conversion overhead that negated the benefit.

### Step 21: Serialization Optimization — DONE

Replaced `rmp_serde` (MessagePack + serde) for raw row encoding with a custom binary format. Each value is encoded as a 1-byte type tag + raw data (LE integers, length-prefixed strings/bytes). No serde trait dispatch, no enum variant framing.

- [x] **Custom binary encoder/decoder** — `encode_value`/`decode_value` write values directly as `tag + data`. Format: `[num_rows: u32 LE, num_cols: u16 LE, values...]`. ~40% less encoding overhead per value vs msgpack+serde.
- [x] **Direct RowMap encoding** — `encode_rows_from_maps(&[RowMap], &ColumnRegistry)` encodes directly from HashMaps using column order. Eliminates the `RowMap → Row` conversion that `ingest()` previously needed for storage encoding.
- [x] **Faster decoding** — `decode_rows` reads values sequentially from bytes with known column count. No intermediate `Vec<Vec<Value>>` from serde deserialization.

Benchmark: Raw ingestion +17% (638K→744K), rollback +15% (8.8ms→7.5ms), full pipeline +4-6% across all benchmarks. MessagePack+serde still used for reducer state and group keys (not on hot path).

### Step 22: Change Buffer Merge Optimization — DONE

Made `ChangeBuffer::push()` append-only and deferred merge to `flush()` time. Also replaced the `hash_change_key` sort-based hash with a commutative wrapping_add hash (no allocation).

- [x] **Append-only push** — `push()` just extends the pending Vec. No hashing, no index lookup, no merge per record. O(1) amortized per record.
- [x] **Deferred merge at flush time** — `flush()` builds the merge index in a single pass over all pending records. Same merge semantics, but the HashMap is built once (not grown incrementally across thousands of push calls).
- [x] **Commutative hash** — `hash_change_key` uses `wrapping_add` of per-field hashes instead of sorting key fields into a Vec. Eliminates one allocation per record.

Benchmark: Raw ingestion +11% (744K→825K), 100K unique group keys +13% (262K→296K). Full pipeline benchmarks unchanged (buffer overhead is negligible vs reducer/MV computation).

### Step 23: Crash-Safe Finalization — DONE

Made finalization atomic: all reducer finalized state + engine metadata (latest_block, finalized_block, block_hashes) are committed in a single `StorageWriteBatch`. On crash, either all finalized state is written or none — no partial state.

- [x] **`StorageWriteBatch` type** — `BatchOp` enum (`SetReducerFinalized`, `PutMeta`) with `commit(&self, batch)` method on `StorageBackend` trait.
- [x] **MemoryBackend** — `commit()` acquires lock once, applies all ops atomically.
- [x] **RocksDbBackend** — `commit()` builds a `rocksdb::WriteBatch`, calls `db.write(batch)` — atomic via RocksDB WAL.
- [x] **ReducerEngine::finalize()** — now takes `&mut StorageWriteBatch`, collects writes instead of writing directly to storage.
- [x] **SettleEngine::finalize()** — passes batch through to each reducer.
- [x] **Settle::finalize() and ingest()** — create batch, collect reducer state + metadata, call `storage.commit(&batch)` once.
- [x] **Bug fix** — `Settle::finalize()` previously did not persist metadata (only `ingest()` did). Now both paths commit metadata atomically.

Crash safety model: raw rows are written eagerly per-block (single `put_raw_rows` = atomic via RocksDB WAL). Unfinalized state is replayed from raw rows on recovery (cheap, small window). Finalized state is now atomic — no partial writes possible.

Benchmark: No performance regression. All benchmarks unchanged (batch commit has same cost as individual writes — RocksDB buffers both in memtables).

### Future (Post-PoC)

- [x] Crash recovery — replay unfinalized blocks from raw rows on startup
- [ ] Replay rollback strategy (alternative to snapshot)
- [ ] Column-oriented storage for raw tables (faster scans, better compression)
