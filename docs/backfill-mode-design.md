# Backfill mode — durable checkpoint ≠ finality

Status: **implemented** (commit on `perf/finalize-serialization`, PR #23).
Non-breaking, off by default (`Config::backfill_checkpoint_interval = 1`).

## TL;DR

On historical backfill, persisting derived reducer/MV state to RocksDB on every
finalized block is 22–33% of wall time. This feature **defers that persistence**:
derived state is written every `N` blocks (a "checkpoint") instead of every
block. The in-memory aggregate is always current; only the *disk write* is
batched. A crash replays at most `N` blocks of raw rows from the last
checkpoint — extra work, never data loss.

Measured gain (RocksDB, finality at tip, 490K rows, `profile_backfill`):

| Batch | persist every block | deferred (interval=100) | gain |
|------:|--------------------:|------------------------:|-----:|
| 5 000 | 87K rows/s | 127K rows/s | **+45%** |
| 25 000 | 132K rows/s | 178K rows/s | **+35%** |

On `vs_postgres_simple_agg` (lighter pipeline) the deferral-safe pipelines
(EventRules, MV-only) show **−10…−22% engine time**; the external-reducer
pipeline is unchanged (deferral is gated off for it by design).

---

## 1. Background: the current durability model

Each `ingest()` builds one atomic `write_batch` and commits it (on `ack`, or
immediately for heartbeats):

```
        ingest(rows, finalized_head = F)
                 │
                 ▼
   ┌──────────────────────────────────────┐
   │ 1. raw rows of the block → write_batch│   source data
   │ 2. reducers/MVs update state IN MEMORY │   live aggregates
   │ 3. finalize(F): serialize derived      │   ◄── the 22–33% cost
   │      state of newly-final blocks       │       on backfill
   │      → write_batch  +  prune snapshots  │
   │ 4. meta (latest, finalized) → write_batch│
   └───────────────────┬──────────────────┘
                       │ storage.commit()
                       ▼
                  ┌──────────┐
                  │ RocksDB  │
                  └──────────┘
```

Recovery (`open()`):

```
  read F = META_FINALIZED_BLOCK,  L = META_LATEST_BLOCK
  reducers/MVs restore finalized state from disk
  replay_unfinalized(F+1 .. L)   ── re-feed raw rows through the pipeline
```

Crash-safety invariant: **disk is exactly one atomic batch behind memory.** On
a crash before `ack`, the whole batch is lost together; disk stays at a
consistent `(F, L)`; recovery replays `F+1..L`.

Two facts this relies on, both verified in the codebase:

- **Raw rows are never evicted** (`dag.rs`: raw-table finalize is a no-op), so
  the raw rows for every block ≥ 1 are on disk and replayable.
- `finalize` couples *persist* with *prune*, but `state_cache` (reducer) and
  `groups` (MV) keep the live value — they are not pruned.

---

## 2. The opportunity

On **historical backfill** the caller marks each block final at the batch tip
(`finalized_head == max block in batch`, no confirmation lag). So the pipeline
serializes + writes all changed group state on *every* block, even though:

- the data is historical (won't be rolled back), and
- the live aggregate is already correct in memory.

That per-block disk write is pure durability overhead we can amortize.

---

## 3. Design: two watermarks

Decouple finality from durability.

```
 blocks:  1   2   3  … 97  98  99 100  101 102 …
          ═══════════════════════════╗
          finalized (per the source)  ║  still arriving
                                      ║
   F (finality)  ─────────────────────╨─►  bounds rollback; advances with source
   D (durable)   ───────────────►          derived state actually on disk
                                  └── invariant: D ≤ min(F, latest)
```

- **F = `finalized_block`** (in memory): unchanged meaning — bounds rollback.
- **D = `durable_block`** (new): highest block whose derived state is on disk.

Today, and on the chain tip, `D == F` (we persist every finalize). Under
backfill deferral, `D` lags `F`: the derived state for `(D, F]` lives only in
memory and is rebuilt from raw rows on recovery.

### The one load-bearing decision (Option A)

**On disk, `META_FINALIZED_BLOCK` stores `D`, not `F`.**

This makes recovery correct with **zero changes to recovery code**: `open()`
already restores `finalized = META_FINALIZED_BLOCK` and replays
`finalized+1 .. latest`. With the field holding `D`, that becomes
`replay(D+1 .. latest)` automatically. Back-compat is exact: for non-deferred
operation `D == F`, so old databases read identically.

> The earlier naive design (persist `F` to disk, persist derived state only at
> `D`) was **rejected**: recovery would replay `F+1..L`, silently skipping the
> `(D, F]` blocks whose state was never written — permanent silent corruption.
> See §6.

---

## 4. How finalize works under deferral

`finalize(block, batch, persist)` — the `persist` flag is decided by `db.rs`.

```
 finalize(block, batch, persist):
   ── always ──────────────────────────────────────────
   merge per-block data into the cumulative aggregate (MV finalize_up_to)
   prune block_groups ≤ block        (rollback tracking; safe — never rolled back)

   ── if persist == false  (defer) ────────────────────
   record changed group keys into `pending_durable`
   RETAIN their block_snapshots       (so a later checkpoint reads the right value)
   return                              (no disk writes)

   ── if persist == true  (checkpoint) ────────────────
   to_persist = pending_durable ∪ this-batch's changed keys
   for each key: serialize current state → batch        (reducer: encode_values;
                                                          MV: serialize_mv_group)
   drain pending_durable, prune snapshots ≤ block
   advance durable_block = min(block, latest)            ◄── clamp (gappy chains)
```

`db.rs` decides `persist`:

```
 persist = NOT (deferral active) OR (interval elapsed)

 deferral active = backfill_checkpoint_interval > 1
                   AND engine.defer_allowed()              (gating, §5)
                   AND finalized_head ≥ latest_in_batch    (no-lag = backfill)

 interval elapsed = finalized_head − durable_block ≥ backfill_checkpoint_interval
```

Timeline, interval = 1000, 100 blocks/batch:

```
 batch1 [1..100]    persist=false  defer   D=0     pending={…}
 batch2 [101..200]  persist=false  defer   D=0     pending grows
 …
 batch10[901..1000] persist=true   CHECK   D=1000  pending flushed, snapshots pruned
 batch11[1001..1100]persist=false  defer   D=1000  pending grows
 …
                    └── disk touched once per 10 batches, not every batch
```

The no-op-finalize guard was updated so a *forced* checkpoint with
`durable < block` still runs even when finality didn't advance (otherwise a
checkpoint at the same `F` would be skipped).

---

## 5. Gating: where deferral is OFF

Deferral is disabled for the whole pipeline (`compute_defer_allowed()` at
construction) when it contains either:

| Pipeline feature | Defer? | Why |
|---|:---:|---|
| reducer (Lua/EventRules) + tumbling MV | ✅ on | running aggregate is replayable from raw rows |
| **sliding-window MV** | ❌ off | `block_times` meta + per-block agg data form one atomic unit; splitting their persist watermark corrupts window replay |
| **external reducer** (`LANGUAGE EXTERNAL`) | ❌ off | recovery replay *skips* external reducers (no host callback at `open()`), so deferred state could never be rebuilt |

Confirmed live in `vs_postgres_simple_agg`: the `settle_fn` (external) rows do
not speed up under `,backfill`; `settle_er` / `settle_mv` do.

---

## 6. Recovery & crash safety

```
 CRASH mid-backfill (D=0 on disk, memory had reached block 700)
        │  memory lost; disk holds:
        ▼
   ┌─────────────────────────────────┐
   │ raw rows:   blocks 1..700        │   (raw rows never evicted)
   │ derived:    as of D = 0  (empty) │
   │ META_FINALIZED_BLOCK = 0  (= D)  │   ◄── Option A: we stored D
   └─────────────────────────────────┘
        │  reopen()
        ▼
   replay_unfinalized(D+1 .. latest) = replay(1 .. 700)
        │  re-feed raw rows through reducer/MV
        ▼
   derived state fully rebuilt ✅   (cost: replay ≤ interval blocks)
```

Why the naive variant corrupts, and why Option A is safe:

```
  NAIVE (rejected):  disk META_FINALIZED = F(700), derived = D(0)
                     reopen → replay(F+1..L) = replay(701..)  ← skips 1..700
                     💀 aggregates for 1..700 lost, silently, forever

  OPTION A (shipped): disk META_FINALIZED = D(0)
                     reopen → replay(D+1..L) = replay(1..)    ← covers everything ✅
```

### Adversarial review (8 scenarios)

Before implementing, the design was attacked by 8 independent reviewers. The
naive "defer + advance finality" design was **NO-GO** — 4 silent-corruption
holes. The shipped v2 folds in every fix:

| # | Hole found | Fix in v2 |
|---|---|---|
| 1 | recovery anchored on F, skips `(D,F]` | Option A: persist D as `META_FINALIZED_BLOCK` |
| 6 | gappy chain `F > latest` → empty replay range strands data | clamp `D ≤ min(F, latest)` |
| 4 | sliding-window: `block_times` vs aggs split watermark | gate sliding-window pipelines off deferral |
| 5 | external reducer state unrebuildable (replay skips it) | gate external-reducer pipelines off deferral |
| 8 | MV group removed-then-re-added in one interval → deleted on disk | checkpoint reconcile: current membership wins (persist set ∩ present; delete set ∖ present) |
| 7 | forced checkpoint skipped by no-op guard | guard bypassed when `durable < block` |
| — | ack-retry / poison path | already correct: `ack` re-commits the stored batch, never re-runs finalize; `pending` blocks further ingest until commit |

### MV remove/re-add reconcile (hole #8)

```
  block D+1: group G appears        → G in pending_durable, on disk: absent
  block D+2: G's aggregate → empty  → G removed from memory, G in removed_groups
  block D+3: G re-appears           → G back in memory, in pending_durable
  CHECKPOINT: G is in BOTH pending_durable (persist) AND removed_groups (delete)

  rule: current in-memory membership wins
        persist = pending_durable ∩ present(groups)     → G persisted ✅
        delete  = removed_groups  ∖ present(groups)      → G NOT deleted ✅
```

---

## 7. Config & compatibility

```rust
Config::with_data_dir(schema, dir)
    .backfill_checkpoint_interval(1000)   // blocks; default 1 = persist every finalize
```

- **Default `1`** = exact pre-feature behavior (`D == F` always).
- **Non-breaking on disk**: no new meta key (`META_FINALIZED_BLOCK` is reused
  to mean "durable"); the Memory backend is unaffected.
- Has effect only on **no-lag** ingest (historical backfill) and only for
  **deferral-safe** pipelines (§5). On the chain tip (finality lags) it is a
  no-op regardless of the interval.

---

## 8. Tests

`src/backfill_tests.rs` (gated on the `rocksdb` feature), 7 crash/recovery
cases:

1. no-lag backfill, no checkpoint reached, drop+reopen → MV rebuilt from replay.
2. same for a Lua reducer + MV pipeline (reducer `pending_durable` path).
3. deferred output == always-persist output (deferral changes timing, not values).
4. checkpoint advances `durable` mid-backfill, then crash → tail replayed.
5. gappy chain `F > latest` across a checkpoint + crash → sub-F block survives
   (clamp).
6. deferral disabled for a sliding-window pipeline (durable tracks finality).
7. deferral disabled for an external-reducer pipeline.

Full suite: 318 lib + 49 integration green.

## 9. Benchmark

`benches/profile_backfill.rs` — RocksDB, finality at tip:

```
SETTLE_BATCH=5000  SETTLE_INTERVAL=1   ./profile_backfill   # persist every block
SETTLE_BATCH=5000  SETTLE_INTERVAL=100 ./profile_backfill   # deferred
```

`benches/vs_postgres_simple_agg.rs` adds a `,backfill` mode alongside `,fin`
(no-lag, persist every block) and tip, so the per-pipeline deferral effect is
visible against the Postgres baseline.

---

## Appendix: relevant code

| Concern | Location |
|---|---|
| watermarks, `finalize(persist)`, clamp, gating | `src/engine/dag.rs` |
| reducer defer (`pending_durable`, snapshot retention) | `src/engine/reducer.rs` `finalize` |
| MV defer + remove/re-add reconcile | `src/engine/mv.rs` `finalize` |
| persist `durable` as `META_FINALIZED_BLOCK`; persist decision | `src/db.rs` `append_meta_to_batch`, `ingest` |
| recovery (unchanged; anchors on the stored watermark) | `src/db.rs` `open` |
