# ADR-1 — The host protocol and crash safety

Status: Accepted (historical)

This record holds six related decisions about the calls of the host, the ack and the recovery after a crash. Each part gives its context, its decision and its consequences.

## 1. Atomic calls are the only write API

**Context.** Early versions of the API had separate calls to process rows, roll back, finalize and flush the change buffer. A host had to call them in the right order. A crash between two of these calls left the store in a state that no single call produced, for example rows without their metadata. The commit b41689f removed the unsafe `process_batch` from the public API, and fa63fa2 moved the commit to the ack. The Node binding notes that these calls were "not crash-safe individually".

**Decision.** The public API changes state only through atomic calls: open, ingest, fork, ack and the reducer attach calls. Each call computes one write set. Rollback and finality are inputs of an ingest call or a fork call, not calls of their own.

**Consequences.**

- A crash can only fall between two calls, so the recovery contract (CN-20) covers every crash point of the logic.
- A host cannot finalize without new rows. It sends a heartbeat call for that.
- The Rust crate still makes its engine module public, which leaves a way around this decision (GAP-39).
- Shapes: REQ-2, REQ-30, WP-2, WP-30.

## 2. Durability follows the ack

**Context.** Settle and the target are two stores. If Settle commits a batch before the target applies it, a crash can leave Settle ahead of the target. The target then misses the batch for good. If Settle commits only after the target applies the batch, a crash can leave the target ahead, and the batch comes again. The second failure is safe when the target applies batches idempotently. The commit fa63fa2 moved the commit to the ack.

**Decision.** An ingest call or a fork call keeps its write set in memory. The ack of the host commits it. A reopen goes back to the last acked batch, and the same blocks give the same batch again.

**Consequences.**

- The store of Settle is never ahead of the target.
- Outside journal mode, the target must apply batches idempotently (RP-21), and a sequence can repeat after a crash (RP-12).
- An applied but unacked batch whose blocks fork during a downtime stays in the target. Part 5 closes that gap as an option.
- Shapes: REQ-10, REQ-12, WP-30, WP-31, CN-2, CN-4, NG-5.

## 3. Two unacked batches by default, and cumulative acks

**Context.** With one batch in flight, Settle and the target never worked at the same time: ingest N, apply N, ack N. On a sum workload against Postgres, Settle → Postgres took 1.14 to 1.63 times the Postgres-only time. A depth of two batches brought it to 1.04 to 1.11 times, and depths 3 and 5 were within 2% of 2. A setting that each host must tune is bad for users (d64149a).

**Decision.** Up to P-MAX-UNACKED places can wait for an ack, 2 by default. An ack is cumulative. A call that returns no batch takes a place and joins the newest unacked batch, so heartbeat calls cannot grow memory without a bound. A full queue fails the call with PENDING_ACK.

**Consequences.**

- The total time of a pipelined run approaches the time of the slower side (PF-7).
- A fork compensation is computed from the state after all returned batches, so the target must apply every batch in order (RP-20, RP-22).
- Reducer attach and replay need an empty queue, because they read the store without the unacked writes.
- Shapes: REQ-11, WP-32, WP-33, INV-2, PF-2, DEF-67.

## 4. One sync per ack, and a failed sync is permanent

**Context.** A review found that an acked batch could be lost on a power loss, because the store did not sync its log (be3ead9). A sync costs about 60 µs on NVMe. When a sync fails, the operating system can drop the dirty pages, so a later sync can report success for writes that are gone.

**Decision.** An ack syncs the log once after its commits. A heartbeat commit does not sync on its own; a later sync covers it. After a failed sync, every later call fails with SYNC_FAILED, also an ack, and the host must drop and reopen.

**Consequences.**

- An acked batch survives a power loss (CN-10).
- The writes of a heartbeat call can be lost on a power loss; the next calls give them again (FM-56).
- A failed sync stops the instance, even if the disk recovers.
- Shapes: REQ-14, WP-34, WP-35, WP-37, FM-31.

## 5. Opt-in journal with the applied sequence of the target

**Context.** With part 2 alone, a crash after the target applied a batch and before its ack loses the batch from Settle. If those blocks fork during the downtime, the reopened Settle does not know them and cannot undo them in the target. A review rated this P1. Two-phase commit across Settle and the target was not possible with a generic target. A journal costs about 12 µs per tip call and 3% of backfill time, and it writes each batch twice (1a9f274).

**Decision.** When the host gives the applied sequence of the target at open, Settle journals and syncs each returned batch before the call returns it. The open commits the journal entries up to the applied sequence and drops the others. It refuses to open in these cases:

- The target is behind the last acked batch.
- The target applied a batch that the journal does not hold.
- The journal holds batches, and the host gives no applied sequence.

The journal is off by default.

**Consequences.**

- In journal mode, Settle and the target agree after every crash (REQ-13, INV-41).
- The target must store the sequence in the same transaction as the data (RP-23).
- Outside journal mode, the cheaper path stays, with its sequence reuse (RP-12) and the fork gap of FM-51.
- Shapes: REQ-13, WP-52, CN-21, RP-12, RP-23, FM-51, FM-52, NG-5.

## 6. Poison and recovery by drop and reopen

**Context.** Some failures leave the memory of Settle ahead of its store in a way that no undo can repair. Examples are a rollback that fails part way, a failure after a rollback in the same call, and a failed heartbeat commit or journal write. A retry from such a state can write a checkpoint of lost data. An ingest that failed after its fork rollback once lost the compensation (ae09d13).

**Decision.** On such a failure, Settle poisons the instance. Every later call fails with POISONED, except the ack and the status reads. The ack still commits, so the host acks the batches that the target applied, drops the instance and reopens. A failure that Settle can undo in memory does not poison.

**Consequences.**

- No write leaves a poisoned instance except the acks of batches that it already returned.
- The host needs a recovery procedure (RP-25).
- A failed attach is not covered yet, and leaves the chain in part replayed (GAP-31).
- Shapes: WP-70, WP-71, WP-72, INV-16, LIV-11.
