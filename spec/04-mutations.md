# 04 — Mutations

This document states the write rules `WP-n` and the transition catalog `TR-n`. The terms are in [03-data-model.md](03-data-model.md). The error classes are in [05-output-contract.md](05-output-contract.md).

Bands: 1–9 calls and input · 10–19 forks · 20–29 finality · 30–39 commit and ack · 40–49 checkpoints · 50–59 open · 60–69 reducer attach · 70–79 errors.

## The host loop

```
db ← open(config, applied_sequence?)                    # TR-4
attach external reducer code                            # TR-5, before the first ingest
loop:
    (rows, chain, finalized) ← next range from the source
    batch ← db.ingest(rows, chain, finalized)            # TR-1
    if batch ≠ ⊥:
        target.apply(batch)            # in sequence order, atomically, idempotently
        db.ack(batch.sequence)                           # TR-3, can lag by P-MAX-UNACKED places
on a fork signal from the source (previous blocks):
    wait until the target applied every returned batch
    (cursor, batch) ← db.fork(previous blocks)           # TR-2
    if batch ≠ ⊥: target.apply(batch); db.ack(batch.sequence)
    continue the source after cursor
```

## Calls and input

**WP-1 — One caller.** The host makes one call at a time, from one thread. Settle refuses a call that a reducer callback makes into the instance that runs it, with INVALID_OPERATION. The current code does not refuse it (GAP-16).

**WP-2 — Call atomicity.** Each call that can change state has its full effect or no effect. The only exception is a failure of WP-71, which leaves the instance poisoned.

**WP-3 — Gate order.** An ingest call checks these gates in order, before it changes anything:

1. Check the health. A poisoned instance fails with POISONED, a sync-failed instance with SYNC_FAILED.
2. Check the room in the ack queue (WP-32). A full queue fails with PENDING_ACK.
3. Check the finality order (WP-21).
4. Check the hash conflicts against finality (WP-12, WP-13).
5. Check the rows (WP-4, WP-5).

A fork call checks gates 1 and 2, then requires a non-empty chain (INVALID_INPUT). So a full queue gives PENDING_ACK also for an input that is not valid.

**WP-4 — Row checks.** Settle accepts the rows of an ingest call only when all these conditions are true:

- Each row carries a block number (DEF-1) of type UInt64, and it equals the block of the row.
- Each table with at least one row is a declared raw table.
- Each positional row has the column layout of its table.
- Each value has the declared type of its column, or converts to it exactly (ADR-15).

Otherwise the call fails with INVALID_INPUT. The current code checks no value type other than the block number (GAP-1). An entry for an undeclared table with no rows has no effect. The treatment of an undeclared column name in a row map is open (OQ-1).

**WP-5 — Block order.** The lowest block with rows in an ingest call MUST be above `L`. Here `L` is the latest block after the rollback of a hash conflict in the same call. Otherwise the call fails with INVALID_INPUT. This rule applies also to the rows of another table for an accepted block. So the host MUST give the rows of all tables of one block in one call.

**WP-6 — Row order.** Settle feeds the rows of one table and one block to the pipeline in input order, and the blocks in ascending order.

**WP-7 — Gaps.** Block numbers need not be contiguous. Settle accepts a block above `L` also when it lies below `F`.

**WP-8 — Row identity.** The record key of a raw row is its block number and its row index (DEF-61).

## Forks

**WP-10 — Fork evidence.** Only a hash conflict (DEF-12) is evidence of a fork. A block that the chain leaves out, and a cursor above `L`, are not evidence.

**WP-11 — Fork point.** Settle rolls back to the fork point of DEF-13.

**WP-12 — No common ancestor.** A call has no common ancestor when no fork point of DEF-13 exists. This applies to an ingest call with a hash conflict and to each fork call. Such a call fails with NO_COMMON_ANCESTOR and changes nothing (ADR-13). The current ingest path rolls back to block 0 when `F` is 0 (GAP-8).

**WP-13 — Below finality.** A call whose lowest hash conflict is at or below `F` fails with FORK_BELOW_FINALITY and changes nothing (ADR-2).

**WP-14 — Rollback effect.** After a rollback to `p`, the derived state of every reducer and view equals its derived state over `A↾p`. Settle restores the blocks at or below `p` that a sliding view expired after `p`. `L` becomes `p`, and Settle removes `H(b)` and the stored raw rows for every `b > p`.

**WP-15 — Fork call after the target.** The host MUST make a fork call only after the target applied every returned batch. Settle computes the compensation batch (DEF-65) from the state after all returned batches, acked or not (CN-5).

**WP-16 — Hash record.** After a successful call, `H(b)` (DEF-11) equals the hash (DEF-2) of each cursor (DEF-3) of the call, also for a block above `L`.

**WP-17 — Fork call result.** A fork call returns the fork point as a cursor and, if the rollback changed an output table, a compensation batch. The host continues the source after the fork point.

**WP-18 — In-line fork.** An ingest call with a hash conflict rolls back to the fork point and then processes its rows. Its rows MUST be above the fork point. The call returns one batch that merges the compensation and the records of the new rows.

## Finality

**WP-20 — Finality advance.** An ingest call with a finalized head `f ≥ F` sets `F` to `f`. `F` can exceed `L` (ADR-2).

**WP-21 — Finality order.** An ingest call with `f < F` fails with INVALID_INPUT and changes nothing.

**WP-22 — Effect of finality.** When `F` advances, the derived state of the blocks at or below `F` becomes final. Settle keeps no undo data for them (RS-10), and keeps only the hash anchors of RS-14 below `F`.

**WP-23 — Fork calls keep finality.** A fork call never changes `F`.

**WP-24 — Finality across a reopen.** After a reopen, `F` equals the finality watermark of the committed state (ADR-7).

## Commit and ack

**WP-30 — Write set.** Each call has one atomic write set. It holds the raw rows, the rollback deletes, the checkpoint, the watermarks, the hashes, the next sequence and the format stamp. RS-6 leaves some raw rows out. The write set enters the committed state only at its commit.

**WP-31 — Ack.** A call `ack(n)` has one of these effects:

- When the queue is empty or `n` is below the oldest unacked sequence, the call succeeds and changes nothing.
- When `n` is above the newest returned sequence, the call fails with WRONG_ACK_SEQUENCE and commits nothing.
- Otherwise Settle commits each unacked batch up to `n`, oldest first, each with its joined writes, and then syncs once.

**WP-32 — Queue room.** When the places in use reach P-MAX-UNACKED, an ingest call or a fork call fails with PENDING_ACK. The error carries the oldest unacked sequence and its age.

**WP-33 — Heartbeat writes.** When the queue is empty, the writes of a heartbeat call commit at once. Otherwise they join the newest unacked batch and take a place.

**WP-34 — Sync.** With the persistent backend and outside journal mode, an ack succeeds only after a sync that makes its commits durable against a power loss. In journal mode, Settle writes and syncs the journal entry of each batch before the call returns the batch, and an ack does not sync.

**WP-35 — Heartbeat sync.** Settle does not sync the commit of a heartbeat call on its own. A later sync covers it (ADR-1).

**WP-36 — Ack commit failure.** When the commit of a batch fails, the ack fails with STORAGE_FAILED. The batches before it stay committed, and that batch and every later one stay unacked. The host can call ack again.

**WP-37 — Sync failure.** When a sync fails, the call fails with SYNC_FAILED and the health becomes sync-failed. From then on every call, also an ack, fails with SYNC_FAILED (ADR-1).

**WP-38 — Sequence assignment.** A call that returns a batch gives it the sequence `s` and increments `s`. A call with no record, or whose records all cancel (RP-11), returns no batch and uses no sequence.

## Checkpoints

**WP-40 — Checkpoint decision.** Settle writes a checkpoint into the write set of each ingest call. It skips the checkpoint only when all these conditions are true:

- P-CHECKPOINT-INTERVAL is above 1.
- The pipeline is deferral-safe (DEF-84).
- The call is a backfill call (DEF-15).
- `D` is not ⊥.
- `min(F, L) − D` is below P-CHECKPOINT-INTERVAL (ADR-18).

**WP-41 — Deferral gate.** Settle computes the deferral-safe property at open and again after each reducer registration (ADR-2, ADR-17).

**WP-42 — Durable clamp.** A checkpoint sets `D` to `min(F, L)`. Before the first block with rows, no checkpoint occurs and `D` stays ⊥.

**WP-43 — Purge.** Each checkpoint purges raw rows by RS-2.

## Open

**WP-50 — Open order.** An open call does these steps in order:

1. Parse and validate the schema (INVALID_SCHEMA).
2. Choose the backend (WP-55).
3. Check the format of the stored data (WP-51).
4. Reconcile the journal (WP-52).
5. Check the format again, because the journal can hold the first layouts.
6. Restore the committed state, and replay the blocks above `D` (WP-53).
7. Restore the next sequence (WP-54).

**WP-51 — Format check.** The open fails with INCOMPATIBLE_STORAGE when any of these conditions is true:

- The stored format version is not P-FORMAT-VERSION.
- The database holds data but no format version.
- The stored layout of an object of the schema differs from its layout in the schema (DEF-34).

An open writes nothing to the store. The format version and the layouts go into the store with the first commit.

**WP-52 — Journal reconcile.** In journal mode, let `c` be the last committed sequence and `a` the applied sequence. The open fails with TARGET_MISMATCH when any of these conditions is true:

- The journal holds batches and the host gives no applied sequence.
- `a < c`: the target lost batches that it applied.
- The journal holds no entry for a sequence in `(c, a]`. This includes an `a` above the newest returned sequence.

Otherwise Settle commits the journal entries with a sequence in `(c, a]` in order, deletes all journal entries, and syncs, in one atomic write. A corrupt journal entry fails the open with STORAGE_FAILED.

**WP-53 — Restore and replay.** Settle restores `L`, `D`, `F` and `H` from the committed state, and the committed derived state of each object. It then replays the blocks above `D`, or from block 0 when `D` is ⊥, in reads of at most P-REPLAY-CHUNK blocks with rows per table. A replay emits no change records. Settle skips an external reducer that has no attached code, until TR-5 attaches it.

**WP-54 — Next sequence.** Settle restores `s` from the committed state, or sets it to 1 on a new database. Outside journal mode, a reopen can thus give a sequence again that a lost unacked batch used (RP-12).

**WP-55 — Backend choice.** An explicit backend in the configuration comes first. A data directory selects the persistent backend. Otherwise Settle uses the memory backend. A data directory in a build without the persistent backend fails with INVALID_CONFIG.

## Reducer attach

**WP-60 — Attach code.** A call that attaches code to a reducer succeeds only when all these conditions are true:

- The instance is healthy, and the ack queue is empty (PENDING_ACK otherwise).
- The reducer exists and has the external runtime kind.
- No code is attached to it yet.

Otherwise the call fails with INVALID_OPERATION. On success, Settle rolls the chain of the reducer back to `N` and replays it from `N + 1`. `N` is `D` while an open waits for code (RS-16), and `F` otherwise. The chain is the upstream reducers, the reducer, and the downstream reducers and views.

**WP-61 — Register a reducer.** A call that registers a new reducer at run time succeeds only when all these conditions are true:

- The instance is healthy, and the ack queue is empty.
- The name is new.
- The source is a raw table.
- The layout matches the stored layout of that name, if one exists.

A layout mismatch fails with INCOMPATIBLE_STORAGE, and a queue that is not empty with PENDING_ACK. Each other failure fails with INVALID_OPERATION. Today the output of such a reducer is not visible (GAP-20).

**WP-62 — External reducer without code.** An ingest call whose rows reach an external reducer with no attached code fails with INVALID_OPERATION. Settle undoes the call. While an open waits for code (RS-16), each ingest call fails with INVALID_OPERATION before it changes state. This includes a call whose rows reach no external reducer.

**WP-63 — Replay a reducer.** A call that replays a reducer needs a healthy instance and an empty queue. It replays the chain of WP-60.

**WP-64 — Attach atomicity.** An attach, registration or replay that fails before its replay leaves the state as it was before the call. A failure in the replay poisons the instance (WP-71), because the chain is then rolled back and in part replayed. A replay writes nothing to the store, so a reopen starts from the state before the call.

## Errors

**WP-70 — Undo of a failed call.** When a stage fails and the same call made no rollback before the failure, Settle undoes the call in memory and returns the error. The health stays healthy.

**WP-71 — Poison triggers.** Settle poisons the instance when any of these failures occurs:

- The commit of a heartbeat call fails while the queue is empty.
- A journal write or its sync fails. A failed sync makes the health sync-failed.
- A rollback fails part way.
- Processing fails after a rollback in the same call.
- The undo of WP-70 fails.
- The replay of an attach, a registration or a replay call fails (WP-64).

**WP-72 — Poison effect.** A poisoned instance fails each call with POISONED, except ack and the status reads. An ack still commits, so the host can ack the batches that the target applied, then drop the instance and reopen (ADR-1).

**WP-73 — No termination.** No input, schema text, reducer result or call sequence terminates the host process (FM-1).

## Transition catalog

### TR-1 — Ingest

*Input:* rows by table and block, a rollback chain, a finalized head `(f, h_f)`.

*Pre:* the gates of WP-3 pass. In detail:

- The health is healthy, and the places in use are below P-MAX-UNACKED.
- `f ≥ F`.
- No hash conflict at or below `F` (WP-13). If a conflict exists, a fork point `p` exists (WP-12).
- The rows pass WP-4, and the lowest block with rows is above `p` (or above `L` with no conflict).

*Post:*

- If a conflict exists, the state is first rolled back to `p` (WP-14, WP-18).
- `A` gains each input block. The derived state of each object equals its value over the new `A` (DEF-41, DEF-42).
- `L` is the highest block with rows, or `p` if no new block has rows. `F = f` (WP-20, WP-22). `H` records each supplied cursor (WP-16).
- A checkpoint is in the write set when WP-40 calls for it, and `D = min(F, L)` after it.
- If records exist after the merge of RP-11, the call returns a batch with sequence `s`, appends it to `Q`, and increments `s`. In journal mode, the journal holds the batch and is synced before the call returns.
- Otherwise the call is a heartbeat call (WP-33).

### TR-2 — Fork

*Input:* a non-empty rollback chain.

*Pre:* the instance is healthy and has room in the queue. A fork point `p` exists in `[F, L]` (WP-12).

*Post:*

- The state is rolled back to `p` (WP-14). `F` does not change (WP-23).
- `H` records each supplied cursor.
- The call returns the cursor `(p, H(p))` (WP-17), and a compensation batch (RP-9) if an output table changed. With no batch, the call is a heartbeat call.

### TR-3 — Ack

*Input:* a sequence `n` (DEF-66).

*Pre:* the health is not sync-failed.

*Post:* by WP-31, the committed state becomes the state after the newest acked batch, and the acked entries leave `Q` and `J`. By WP-36 and WP-37, a failure leaves the unacked suffix in `Q`.

### TR-4 — Open

*Input:* a configuration with a schema and an optional applied sequence.

*Pre:* the steps of WP-50 pass.

*Post:* `C` holds the reconciled committed state. In memory, `A`, `H`, `L`, `F`, `D` and the derived state equal the committed state, with the tail above `D` replayed. `Q` is empty, and `s` follows the last committed sequence. The health is healthy.

### TR-5 — Attach

*Input:* code for a reducer, a new reducer definition, or a reducer name to replay.

*Pre:* the conditions of WP-60, WP-61 or WP-63.

*Post:* the reducer runs the new code or exists. The derived state of its chain equals its value over `A↾L`, for every block above `F`.

### TR-6 — Crash

*Input:* none. The process stops, or the power fails.

*Post:* the state in memory is lost. The committed state keeps each commit that the tier of CN-10 makes durable. The next open applies TR-4.

### TR-7 — Poison

*Input:* a failure of WP-71.

*Post:* the health becomes poisoned, or sync-failed for a failed sync, with the reason of the failure. The state in memory can be ahead of the committed state and is no longer valid for new writes (WP-72).
