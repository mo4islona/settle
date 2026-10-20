# 07 — Invariants

Each invariant has a scope:

- `[state]` holds at every observation point.
- `[transition]` holds between two consecutive observation points.
- `[response]` holds for every result of a call.
- `[recovery]` holds across a crash and an open.

The test classes `CT-n` are in [13-conformance-tdd.md](13-conformance-tdd.md). `S` is the instance state of DEF-86.

Bands: 1–9 structural · 10–19 transition legality · 20–29 output semantics · 30–34 reports · 35–39 isolation · 40–49 durability and recovery.

## Structural

**INV-1 — Watermark order.** [state]
If `D ≠ ⊥`, then `D ≤ F` and `D ≤ L`. If `L = ⊥`, then `D = ⊥`.
*Why:* with a durable watermark above the data, an open skips blocks of its replay.
*Check:* CT-1 and CT-2 read the watermarks after every step and compare them with the model.

**INV-2 — Queue bound.** [state]
The places in use never exceed P-MAX-UNACKED.
*Why:* the queue bound is the only backpressure of Settle, and the unacked write sets live in memory.
*Check:* CT-1 reads the places after every step. CT-4 sends calls into a full queue.

**INV-3 — Consecutive sequences.** [state]
The unacked batches have consecutive sequences that end at `s − 1`. The first of them follows the last committed sequence.
*Why:* a gap or a repeat breaks the cumulative ack and the journal reconcile.
*Check:* CT-1 tracks every returned sequence. CT-2 checks the sequence after each reopen.

**INV-4 — One record per key.** [response]
A batch holds at most one record for each pair of table and key (RP-5).
*Why:* two records of one key make the result depend on the order of apply.
*Check:* the structural validator of CT-1 runs on every batch.

**INV-5 — Hash record.** [state]
For each block `b` in `(F, L]` with a supplied hash, `H(b)` equals the hash of the last successful call that supplied `b`.
*Why:* a stale hash hides a fork or makes a false one.
*Check:* CT-2 compares the stored hashes with the hashes in memory at each reopen. CT-1 compares them with the model.

**INV-6 — Group presence.** [state]
A view group is in `Out(A)` exactly when it has data (DEF-44).
*Why:* an empty group in the target is a row that no recomputation gives.
*Check:* CT-1 compares the target with the model, also for groups that a rollback or a window empties.

**INV-7 — Replay source.** [state]
For every block of `A` above `D`, the raw rows are in the committed state or in the write sets of the unacked batches.
*Why:* an open replays these rows. A purge above `D` makes the derived state lose blocks.
*Check:* CT-2 with a small retention margin. CT-7 checks the stored raw blocks against `D`.

**INV-8 — Emit columns.** [state]
A reducer has at most P-MAX-EMIT-COLUMNS emit columns. After an undo or a rollback, the emit columns that the undone blocks added are gone.
*Why:* a column from an undone block changes the layout of later emit rows.
*Check:* CT-4 sends a reducer over the limit. CT-1 checks the columns after each rollback.

**INV-9 — Derived state.** [state]
The derived state of each reducer and view equals DEF-41 and DEF-42 over `A` (DEF-10).
*Why:* this is the semantic base of every output property.
*Check:* CT-1 compares each object with an independent recomputation in the reference model.

## Transition legality

**INV-10 — Monotonic finality.** [transition]
Within one run, `F` never decreases. An open restores the `F` of the last durable commit (INV-47). A crash loses the finality advances of the unacked batches, as it loses their rows (CN-20).
*Why:* a finality that goes back lets a later fork undo final blocks.
*Check:* CT-1 checks `F` after each step. CT-2 compares the finalized cursor after each reopen with the one of the last durable commit.

**INV-11 — Final blocks stay.** [transition]
Let `m = min(F, L)` before a transition. After the transition, `A↾m` and the contributions of its blocks to the derived state are as before. A new block can still enter between `L` and `F` (WP-7, REQ-6), because it is above `m`. Across a crash and an open, the same holds with `m` of the last durable commit. A crash loses only the blocks of unacked batches (CN-20).
*Why:* the target relies on final data to stay.
*Check:* CT-1 sends forks below finality and expects FORK_BELOW_FINALITY with no change. It also sends blocks between `L` and `F` and checks that `A↾m` stays. CT-2 repeats it after a reopen.

**INV-12 — No effect on error.** [transition]
A call that fails, other than a poison trigger of WP-71, leaves every element of `S` as before the call. There is one exception. An ack that fails at the commit of a batch keeps the commits of the batches before it, and these batches leave the queue (WP-36).
*Why:* a host retries after an error and expects the instance to be intact. A retry of the ack commits the rest.
*Check:* CT-4 sends each error class and compares the state with a copy. CT-8 makes the store fail at each step. For a cumulative ack, it checks that exactly the batches before the failed commit are committed.

**INV-13 — Frame condition.** [transition]
`S` changes only by TR-1 to TR-7. A status read and the passage of time change nothing (RP-36).
*Why:* a hidden state change breaks determinism and replay.
*Check:* CT-1 calls each status read twice and compares the results and the state.

**INV-14 — Block order.** [transition]
After a successful ingest call, each new block with rows is above the latest block before it. The latest block here includes the rollback of the call.
*Why:* a block twice in `A` counts its rows twice.
*Check:* CT-4 sends blocks at and below `L`, also for a second table and for block 0.

**INV-15 — Commit order.** [transition]
A write set enters the committed state only in these cases:

- at the ack of its batch;
- at once, for a heartbeat call with an empty queue (WP-33);
- in the journal reconcile of an open (WP-52).

Batches commit in sequence order (CN-3).
*Why:* a commit before the ack breaks the recovery contract of REQ-12. A heartbeat call with an empty queue returns nothing for the target, so its commit cannot put Settle ahead of the target.
*Check:* CT-8 counts the commits of a store that records each commit. It also checks that a heartbeat call commits at once with an empty queue, and only with the next ack otherwise. CT-3 crashes between the commits of one cumulative ack.

**INV-16 — Sticky poison.** [transition]
After a poison, the health stays poisoned or sync-failed until the host drops the instance. A sync-failed instance fails every call.
*Why:* a write from a poisoned state can commit a checkpoint of lost data.
*Check:* CT-8 triggers each poison trigger and then sends every call.

**INV-17 — Validation before rollback.** [transition]
An ingest call with a hash conflict and an input that fails WP-4 or WP-5 does not roll back.
*Why:* a lost rollback leaves the target with forked data (a past defect class).
*Check:* CT-4 sends a conflict together with a bad row and compares the state with a copy.

**INV-18 — Explicit deletes.** [transition]
A delete record occurs only for a rollback or for a view group that a sliding window empties. Raw rows leave the store of Settle only by a rollback or by a purge.
*Why:* data that leaves by another path is silent loss.
*Check:* CT-1 classifies every delete record by its cause. CT-7 counts the stored raw blocks.

## Output semantics

**INV-20 — Target equivalence.** [response]
After each successful call, the target image (DEF-64) of all returned batches equals `Out(A)`, over the coverage of each object (CN-33). There is one exception. After a crash outside journal mode, the target can hold applied batches that Settle lost (RP-21). At each key that these batches changed, the equality holds again after the next record of that key. A key that gets no later record can stay different (FM-51).
*Why:* this is the main promise of Settle (G-1).
*Check:* CT-1 compares the target image with an independent recomputation after every step. CT-2 and CT-10 compare it after reopens and in domain scenarios.

**INV-21 — Record soundness.** [response] — known-violated (GAP-41)
Each record has the operation of RP-3 for `Out(A)` before its call, and no record is an empty change (RP-6).
*Why:* a target that checks the operation, for example an insert into a present key, fails.
*Check:* the structural validator of CT-1 applies each record to the model target and checks the operation.

**INV-22 — Previous values.** [response] — known-violated (GAP-10)
The previous values of each update record and each delete record equal the values of its key in `Out(A)` before the call (RP-4).
*Why:* a target that keeps a history or computes a difference uses them.
*Check:* the structural validator of CT-1 compares them with the model target.

**INV-23 — Virtual silence.** [response]
No record names a virtual table or a reducer.
*Why:* a virtual table exists to save the target writes.
*Check:* the structural validator of CT-1 checks the table of every record, also in compensation batches.

**INV-24 — Exact aggregates.** [response]
After any sequence of rollbacks and finality advances, each aggregate equals its value from scratch over `A`. A Float64 or saturating aggregate equals the two-level fold of DEF-42, bit for bit.
*Why:* min, max, first and last have no inverse, and a float sum depends on the order of addition (ADR-4).
*Check:* CT-1 folds `A` from scratch in two levels after each step and compares the bits. The values of the example in DEF-42 tell the two-level fold from a fold of the rows.

**INV-25 — Determinism.** [response]
Two instances with the same schema and the same calls give equal batches, modulo FV-1 to FV-5. Parallel and sequential stages give equal batches (CN-42).
*Why:* replay, reopen transparency and the target idempotence all need it.
*Check:* CT-1 runs one history twice. CT-12 compares parallel and sequential runs.

**INV-26 — Error soundness.** [response]
A call that fails returns no batch, and its error is in the closed set EC-1 to EC-14.
*Why:* a batch with an error, or an unknown error, leaves the host with no safe action.
*Check:* CT-4 and CT-5 check the class of each error in each binding.

**INV-27 — Type fidelity.** [response] — known-violated (GAP-1)
Each raw-table value in a record has the declared type of its column, or is Null. Each state value has the declared type of its field. Each aggregate result has the result type of DEF-43.
*Why:* a target with typed columns refuses a value of another type.
*Check:* the structural validator of CT-1 checks each value type. CT-4 sends values of each wrong type.

**INV-28 — No silent input loss.** [response] — known-violated (GAP-1, GAP-26)
Each input value reaches every aggregate that reads it, or the call fails.
*Why:* a skipped value gives a wrong sum with no error (G-7).
*Check:* CT-1 sends every value type into every numeric mode and compares the aggregates with the model.

**INV-29 — Provenance.** [response]
The values of each raw record (RP-2) equal the values of its input row. Each key value of a view record comes from an input row or a window start.
*Why:* Settle reports data. It does not invent data.
*Check:* the input ledger of CT-1 compares each raw record with the row that the simulator sent.

## Reports

**INV-30 — Status honesty.** [state]
At each observation point, the status reads of RP-30 to RP-35 equal the model state.
*Why:* the host continues the source from the latest cursor.
*Check:* CT-1 reads every status after every step. CT-5 does it in each binding.

**INV-31 — Batch heads.** [response]
The heads of each batch follow RP-7.
*Why:* a target that stores the head uses it to continue.
*Check:* the structural validator of CT-1.

**INV-32 — Sequence honesty.** [response]
Without a crash, the sequences of the returned batches are consecutive over ingest calls and fork calls (RP-10).
*Why:* the cumulative ack and the journal assume consecutive sequences.
*Check:* CT-1 tracks the sequences.

**INV-33 — No inert gauge.** [state] — known-violated (GAP-11)
Each gauge that a binding exposes reflects a real quantity of the instance.
*Why:* a gauge that never changes tells the host that nothing is wrong.
*Check:* CT-5 drives each gauge through its full range.

## Isolation

**INV-35 — Instance isolation.** [state]
Two instances with different data directories, or two memory instances, share no state (CN-44).
*Why:* one host can run several pipelines.
*Check:* CT-12 runs two instances in one process with mixed calls.

**INV-36 — Group isolation.** [transition] — known-violated (GAP-7)
A reducer body call reads and changes only the state of its own group. No value passes from one group to another, and no value outside the declared state fields survives a call.
*Why:* a value that leaks between groups is not rolled back and is lost at a reopen.
*Check:* CT-1 runs a body that writes an undeclared field, then reads it from another group.

## Durability and recovery

**INV-40 — Recovery equals commit.** [recovery]
After a crash and an open outside journal mode, the state equals the state after the last durable commit (CN-20).
*Why:* this is the restart promise of REQ-12.
*Check:* CT-2 at random points. CT-3 at each kill point.

**INV-41 — Journal recovery.** [recovery]
After an open in journal mode with an accepted applied sequence `a`, the state equals the state after batch `a` (CN-21).
*Why:* the journal exists to make Settle and the target agree.
*Check:* CT-2 in journal mode with a late target. CT-3 kills the process between the journal write and the return.

**INV-42 — Recovery idempotence.** [recovery]
A crash during an open, and a second open, give the state of one open (CN-22).
*Why:* a host retries a failed start.
*Check:* CT-3 kills the process at each step of an open.

**INV-43 — Reopen transparency.** [recovery]
A drop and an open at an observation point with an empty queue do not change any later batch (CN-23).
*Why:* a restart is not an input. The output does not depend on it.
*Check:* CT-2, the reopen-differential test.

**INV-44 — Deferral transparency.** [recovery]
The batches with any P-CHECKPOINT-INTERVAL equal the batches with an interval of 1, also across crashes and opens.
*Why:* deferral trades replay work for speed. The values stay the same.
*Check:* CT-2 runs one history with intervals 1 and N, with reopens.

**INV-45 — Layout safety.** [recovery]
No open reads stored data with a layout other than its stored layout. It fails with INCOMPATIBLE_STORAGE.
*Why:* a positional read with a wrong layout gives wrong values with no error (ADR-3).
*Check:* CT-11 changes each layout element and expects the refusal.

**INV-46 — Corruption is an error.** [recovery]
Stored data that does not decode gives STORAGE_FAILED. It never gives a default value or a skipped object. Settle keeps no checksums, so a corrupt value that still decodes is out of scope.
*Why:* a silent default is silent data loss.
*Check:* CT-8 corrupts each kind of stored value and opens.

**INV-47 — Finality survives a reopen.** [recovery]
After a crash and an open, `F` equals the finality watermark of the last durable commit.
*Why:* see INV-10.
*Check:* CT-1 and CT-2 compare the finalized cursor after each reopen with the one of the last durable commit, with deferral and with gappy chains.

**INV-48 — Maintenance transparency.** [recovery]
Purge, compaction and log rotation never change the state or a batch (CN-40).
*Why:* background work is not an input.
*Check:* CT-2 runs one history with retention margins 0, P-RAW-RETENTION and no purge, and compares the batches.

**INV-49 — Durability tiers.** [recovery]
After a crash of each kind, the store keeps each write that CN-10 marks as kept.
*Why:* the host acts on the tiers, for example when it deletes source data.
*Check:* CT-3 with a store that drops the writes after the last sync.

## Reading the catalog in tests

- An oracle run (CT-1) checks INV-1 to INV-9, INV-20 to INV-31 and INV-36 after every step.
- A reopen run (CT-2) checks INV-5, INV-10, INV-40, INV-43, INV-44, INV-47 and INV-48.
- A crash run (CT-3) checks INV-15, INV-40, INV-41, INV-42 and INV-49.
- An input-fault run (CT-4) checks INV-12, INV-14, INV-17, INV-26 and INV-27.
- A storage-fault run (CT-8) checks INV-12, INV-15, INV-16 and INV-46.
