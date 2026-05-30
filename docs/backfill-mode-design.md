# Backfill mode — durable checkpoint ≠ finality

## Problem

On historical backfill the caller marks each ingested block final immediately
(`finalized_head == latest block in batch`, no confirmation lag). Today every
`finalize()` serializes + commits all changed reducer/MV group state to RocksDB.
Measured cost of that persist on backfill (RocksDB, finality at tip, 490K rows):

| Batch | persist ON | persist OFF (probe) | gain |
|---|---:|---:|---:|
| 5 000 | 87K rows/s | 130K rows/s | +50% |
| 25 000 | 132K rows/s | 170K rows/s | +29% |

So derived-state persistence is 22–33% of backfill wall. We want to defer it.

## Current durability model (verified against code)

`ingest()`:
1. `process_batch_deferred` → raw rows into `write_batch`; reducer/MV in-memory
   state updated.
2. `engine.finalize(F)` → for newly-finalized blocks: serialize reducer
   (`set_reducer_finalized`) + MV (`put_mv_state`) state into `write_batch`;
   **prune** in-memory `block_snapshots`/`block_groups` ≤ F; advance
   `engine.finalized_block = F`.
3. `append_meta_to_batch` → `META_LATEST_BLOCK`, `META_FINALIZED_BLOCK`,
   `META_BLOCK_HASHES` into `write_batch`.
4. data path: stash `write_batch` as `pending`; caller applies ChangeBatch to
   target, calls `ack` → `storage.commit(write_batch)`. heartbeat path
   (no records): commit immediately.

Recovery (`open()`):
- read `F = META_FINALIZED_BLOCK`, `L = META_LATEST_BLOCK`.
- reducer/MV restore finalized state from disk (in their `new()`).
- `replay_unfinalized(F+1, L)` — re-feed raw rows F+1..L through reducers+MVs.

Crash-safety invariant: disk is exactly one atomic batch behind in-memory; on
crash before ack the whole batch is lost together, disk stays at a consistent
`(F_prev, L_prev)`, recovery replays `F_prev+1..L_prev`.

Two facts that make this work and that we rely on:
- **Raw rows are never evicted** (`dag.rs` raw-table finalize = "not implemented
  yet"), so raw rows for every block ≥ 1 are on disk and replayable.
- In `engine.finalize`, the persist and the prune are coupled; `state_cache`
  (reducer) and `groups` (MV) are NOT pruned — they keep the live value.

## Key simplifying insight

In **true backfill** the caller passes `finalized_head.number == max(block in
batch)` every ingest, so after each ingest `finalized == latest`:

- No unfinalized blocks exist. Therefore reducer `state_cache[key]` ==
  the finalized state as-of-`finalized` (no unfinalized contributions to strip).
- `block_snapshots` are immediately prunable (rollback can only target
  ≥ finalized = latest ⇒ no-op), exactly as today.
- MV finalized accumulators (`SumAgg.finalized` etc.) are already the as-of-F
  value after `finalize_up_to(F)`.

⇒ At a checkpoint we can persist **directly from live in-memory state**
(`state_cache` for reducer, current finalized aggs for MV) with no snapshot
gymnastics. The dangerous "lag" case (state_cache includes unfinalized) cannot
occur because we only defer when there is no lag.

## Design

Decouple two watermarks:

- **finality watermark `F`** (`finalized_block`, `META_FINALIZED_BLOCK`):
  unchanged meaning — bounds rollback, advances as caller reports finality.
- **durability checkpoint `D`** (`META_DURABLE_BLOCK`, new): highest block whose
  derived reducer/MV state is actually persisted to disk. Invariant **D ≤ F**.

### Eligibility (per ingest)

Deferral is enabled only when ALL hold:
1. The ingest is "no-lag": `finalized_head.number >= latest_block_in_batch`
   (caller commits immediately). With lag, behave exactly as today.
2. `D + checkpoint_interval > F` after this finalize (not yet time to checkpoint).
3. Config opt-in: `backfill_checkpoint_interval > 1` (default 1 = today's
   behavior, every finalize persists, D tracks F).

When deferral is NOT enabled, `finalize` persists as today and sets `D = F`.

### finalize(F) changes

```
should_persist = (F - last_durable >= checkpoint_interval) || !no_lag || forced
for each node: node.finalize(F, batch, should_persist)
  - always: in-memory merge (MV finalize_up_to) + prune ≤ F as today
  - if should_persist: serialize changed-since-D groups into batch
  - if !should_persist: accumulate changed group keys into a per-node
    "pending_durable" set (do NOT drain dirty / do NOT write to batch)
finalized_block = F
if should_persist:
  batch.put_meta(META_DURABLE_BLOCK, F); last_durable = F; clear pending_durable
```

Because deferral requires no-lag, "changed-since-D group state" == live
`state_cache[key]` (reducer) / live finalized aggs[key] (MV). So persisting at
checkpoint reads live memory; no retained snapshots needed beyond today's.

### Per-node "pending_durable" set

- **reducer**: `FxHashSet<Vec<u8>>` of group keys whose finalized state changed
  since last durable checkpoint. On checkpoint, for each: persist
  `encode_values(state_cache[key])`. (state_cache == finalized in no-lag.)
- **MV**: reuse the existing dirty mechanism but accumulate instead of drain —
  a `pending_durable: FxHashSet<GroupKey>`. On checkpoint, persist current
  finalized aggs for each (same `serialize_mv_group`). Also carry the existing
  `removed_groups` deletions to checkpoint time.

### Recovery change

`open()`: replay start = `D+1` (was `F+1`), where `D = META_DURABLE_BLOCK`
(default = F for DBs written before this feature, so back-compat is exact).
`replay_unfinalized(D+1, L)`. Raw rows D+1..L are on disk (never evicted).
Reducer/MV restore disk state as-of-D, replay rebuilds D+1..L.

### Mode transition (backfill → tip)

When an ingest arrives with lag (`finalized_head < latest_in_batch`) or finality
stops jumping, force a checkpoint at the current F BEFORE processing so D catches
up to F, then resume per-batch persist. This guarantees once we leave backfill,
D == F and behavior is identical to today.

Also force-checkpoint on clean shutdown is N/A (no Drop hook persists today); a
crash mid-backfill simply replays from the last D — correct, just more work.

## Crash scenarios (to be adversarially verified)

1. Crash mid-backfill before ack of batch K: disk at (F_{K-1}, D_last). Recovery
   restores as-of-D_last, replays D_last+1..L_{K-1}. Raw rows present. ✓
2. Crash right after a checkpoint commit: D advanced + derived persisted
   atomically (same write_batch). Recovery replays D+1..L. ✓
3. Rollback during backfill: finality monotonic, rollback bounded ≥ F ≥ D ⇒
   never touches persisted ≤ D state. ✓ (but verify handle_fork path + the
   `recovery_block` in-ingest rollback path don't assume D==F)
4. Gappy chains (Solana, latest may be < finalized): no-lag eligibility uses
   `finalized >= latest_in_batch`; verify this doesn't mis-trigger.
5. Sliding-window MV (does NOT call finalize_up_to, keeps per-block data):
   deferring its persist must still persist `block_times` meta correctly and not
   corrupt window expiry on replay.
6. Chained reducers (reducer sourcing another reducer): replay order from D+1.
7. External reducers needing host callback: replay skips them today; with D+1
   range, ensure no double-processing / no gap.
8. `process_batch` non-deferred (used in tests) — must be unaffected.
9. ack-failure / poison path: deferred derived state lives only in memory until
   checkpoint; a commit failure of a NON-checkpoint batch loses only raw rows +
   meta for that batch (recoverable via replay). A checkpoint-batch commit
   failure must preserve pending_durable so retry re-persists (don't clear
   pending_durable until commit success — tricky with current ack model).

## Open risk (the hard one)

Scenario 9 + the existing pending/ack split: `finalize` mutates in-memory
(`last_durable`, clears `pending_durable`, prunes) BEFORE `storage.commit`
happens (commit is on ack). If the checkpoint batch's commit fails:
- today: poison on heartbeat; on data path, `pending` retains the write_batch
  for retry — but `engine.finalize` already cleared `pending_durable` and
  advanced `last_durable` in memory. A retry of `ack` re-commits the SAME
  write_batch (which DOES contain the derived state), so disk lands correct.
  ✓ as long as we DON'T re-run finalize on retry (ack just re-commits the stored
  batch — verified: `ack` only does `storage.commit(p.write_batch)`).
- The danger is only if we advanced `last_durable` in memory but the batch that
  carried `META_DURABLE_BLOCK` never commits AND we then process more ingests
  thinking D advanced. But pending-ack BLOCKS further ingest until ack succeeds
  (guard_no_pending). So we can't proceed past an uncommitted checkpoint. ✓
  Must double-check the heartbeat (immediate-commit) checkpoint path: on failure
  it poisons ⇒ drop+reopen ⇒ recovery from on-disk D (old) ⇒ replays more. ✓

## Config surface

`Config.backfill_checkpoint_interval: u64` (blocks), default `1`
(= current behavior, feature off). Builder `.backfill_checkpoint_interval(n)`.
No on-disk format change (only a new optional meta key `durable_block`).
NON-breaking.

---

# v2 — Resolution (adversarial review folded in)

8-scenario adversarial review returned **NO-GO** on the v1 design: 4 critical
silent-corruption holes + 3 major gaps. Root cause: v1 advanced the *persisted*
finality watermark + pruned rollback data past D every batch, while persisting
derived state only at D, but recovery anchors baseline AND replay range on F.
The (D,F] window then existed on neither disk nor replay.

## Final contract (implemented)

**Option A — on-disk `finalized` IS the durable watermark.** We never persist a
finality watermark ahead of the derived state it implies. Concretely:

- New engine field `durable_block D` (in-memory `finalized_block F` may run
  ahead for rollback bounding). On-disk `META_FINALIZED_BLOCK` is written as
  **D, not F** (`append_meta_to_batch` change). So recovery's existing
  `set_finalized_block(disk) ; if latest>finalized replay(finalized+1,latest)`
  already does the right thing — **zero recovery-code change** (B1).
- `D = min(F, latest_block)` at every checkpoint — clamps to replayable raw data,
  fixing gappy chains where F>latest (B2).
- Deferral is **whole-pipeline gated**: `defer_allowed = no sliding MV AND no
  external reducer anywhere`. If false, persist every finalize (D==F, exactly
  today's behavior). Kills B4 (sliding) and B5 (external/external-chained) by
  construction.
- `finalize(block, batch, persist)`:
  - always: in-memory merge (MV `finalize_up_to`) + `block_groups` prune.
  - `persist=false`: reducer accumulates finalized keys into `pending_durable`
    AND **retains their snapshots** (so a later checkpoint reads the correct
    as-of-finalized value even if lag appears); MV accumulates dirty into
    `pending_durable`, leaves `removed_groups` to accumulate. No disk writes.
  - `persist=true`: persist `pending_durable ∪ this-batch keys`; reducer reads
    `find_snapshot_at_or_before(block)` (snapshots were retained), MV serializes
    live cumulative aggs; then prune. MV deletes `removed_groups` **only for keys
    not currently present** (membership-wins reconcile — B3).
- No-op guard updated: skip only when `block==finalized_block && has_finalized &&
  (!persist || durable>=block)` — so a forced checkpoint with `durable<block`
  still runs even when finality didn't advance (B7).
- Rollback leaves `durable` unchanged (monotonic floor). Under Option A on-disk
  finalized==durable==derived, so the fork/None=>0 divergence v1 feared cannot
  occur (B6).
- ack/atomicity unchanged and preserved: D is written in the SAME write_batch as
  its derived state; ack only re-commits the stored batch (B8).

## Config
`Config.backfill_checkpoint_interval: u64`, default `1` (feature OFF = exact
current behavior). Non-breaking: no new on-disk key (reuses META_FINALIZED_BLOCK
semantics), Memory backend unaffected.

## Test gates (must pass)
1. no-lag backfill, interval=100, ack several non-checkpoint batches, drop+reopen
   without a checkpoint → aggregates == from-scratch.
2. gappy F>latest across checkpoint + deferred sub-F block + crash → survives.
3. MV remove/re-add across interval + crash → group present, correct value.
4. (sliding & external pipelines: assert deferral disabled, behavior == today.)
