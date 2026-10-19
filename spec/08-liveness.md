# 08 — Liveness

Each property states a precondition, a bound, the observable witness (`OB-n`, [12-observability.md](12-observability.md)) and the test class (`CT-n`, [13-conformance-tdd.md](13-conformance-tdd.md)).

Bands: 1–9 progress · 10–19 recovery and release.

## 0. Environment

A liveness claim has meaning only in a defined environment:

- **Healthy host.** The host makes one call at a time. It applies and acks each returned batch within a finite time. It acks only sequences that it got.
- **Healthy store.** Each read, write and sync of the store succeeds within a finite time.
- **Adequate resources.** The process has the memory for the resident groups and the unacked write sets (RS-20), and the disk for the bound of RS-21.
- **Reducers that end.** Each reducer body call returns within a finite time. LIV-10 makes this a property of Settle.
- **Quiescent instance.** No call runs and the ack queue is empty.

## Progress

**LIV-1 — Calls return.**
*Pre:* a healthy store, adequate resources and reducers that end.
*Bound:* each call returns a result or an error. Its work is linear in the rows of the call, the depth of its rollback and the groups that it changes. Settle never waits for an internal event inside a call.
*Witness:* the call returns. OB-7 gives the time of each stage.
*Check:* CT-6 doubles each input size and expects a growth factor of at most P-SLO-INSTR-LINEAR.

**LIV-2 — No wait for an ack.**
*Pre:* none.
*Bound:* a call that finds a full queue fails at once with PENDING_ACK. Settle never blocks a call to wait for an ack.
*Witness:* OB-9, the error with the oldest sequence and its age.
*Check:* CT-4 fills the queue and times the refused call.

**LIV-3 — Bounded open.**
*Pre:* a healthy store and adequate resources.
*Bound:* the work of an open is linear in the tail above `D`, the stored view groups and the journal entries (PF-6). The tail is at most the finality lag at the crash plus P-CHECKPOINT-INTERVAL blocks. On the reference tail of SC-5, the open takes at most P-SLO-OPEN.
*Witness:* the open returns. OB-11 reports the phases of the open.
*Check:* CT-6 times the open of SC-5. CT-7 times an open after a long run.

**LIV-4 — Ack frees places.**
*Pre:* a successful ack of sequence `n`.
*Bound:* at the return of the ack, the places of the batches up to `n` and of their joined heartbeat calls are free.
*Witness:* OB-4.
*Check:* CT-1 reads the places after each ack.

**LIV-5 — The source makes progress.**
*Pre:* a healthy host, a healthy store, adequate resources, reducers that end and valid input.
*Bound:* each block of the source reaches `A` after a finite number of calls. No state of the instance refuses valid input forever.
*Witness:* OB-1 advances.
*Check:* CT-1 runs long random histories with a host that acks late.

**LIV-6 — Finality frees memory.**
*Pre:* an ingest call that moves `F`.
*Bound:* at the return of the call, Settle keeps no undo data for the blocks at or below the new `F`.
*Witness:* OB-10.
*Check:* CT-7 measures the memory of a long run with a fixed finality lag.

**LIV-7 — Purge converges.**
*Pre:* a checkpoint that sets `D`.
*Bound:* after the checkpoint, the store holds no raw rows of a block at or below `D − P-RAW-RETENTION − P-PURGE-STEP`.
*Witness:* OB-12, and a probe of the stored raw blocks.
*Check:* CT-7.

**LIV-8 — The stash empties.**
*Pre:* a sliding view and an ingest call that moves `F`.
*Bound:* at the return of the call, the expired-block stash holds no entry whose expiring block is at or below `F`. This applies in memory and in the next checkpoint.
*Witness:* OB-10.
*Check:* CT-7 with a sliding view.

**LIV-9 — The journal empties.**
*Pre:* journal mode.
*Bound:* the commit of a batch deletes its journal entries. An open deletes all journal entries (RS-15).
*Witness:* a probe of the journal entries in the store.
*Check:* CT-3 and CT-7.

## Recovery and release

**LIV-10 — Reducers end.** — known-violated (GAP-5)
*Pre:* none.
*Bound:* each reducer body call ends within P-REDUCER-STEPS steps and P-REDUCER-MEMORY of memory, or the call fails with REDUCER_FAILED.
*Witness:* OB-9, the REDUCER_FAILED error.
*Check:* CT-4 sends a body with an endless loop and a body that allocates without a bound.

**LIV-11 — Exit from poison.**
*Pre:* a poisoned or sync-failed instance and a healthy store.
*Bound:* a drop and one open give a healthy instance. No state of the store blocks the open forever.
*Witness:* OB-5.
*Check:* CT-8 triggers each poison trigger, then reopens and continues.

**LIV-12 — Lock release.** — known-violated (GAP-13)
*Pre:* the host closes an instance with a data directory.
*Bound:* an open of the same directory in the same process succeeds at once after the close.
*Witness:* the second open returns.
*Check:* CT-5 in each binding.

**LIV-13 — Attach ends.**
*Pre:* a healthy store and reducers that end.
*Bound:* the replay of an attach (WP-60) is linear in the tail above `F` of its chain.
*Witness:* the attach returns.
*Check:* CT-6 times an attach after a reopen with a long tail.

**LIV-14 — Alarm, not a silent retry.**
*Pre:* a store operation fails.
*Bound:* Settle returns STORAGE_FAILED or SYNC_FAILED in the same call. It never retries a failed store operation without an end.
*Witness:* OB-9.
*Check:* CT-8.

**LIV-15 — Ack-wait signal.**
*Pre:* calls fail with PENDING_ACK while the oldest unacked batch is older than P-ACK-WARN.
*Bound:* Settle emits the log event of OB-8 at each refused call.
*Witness:* OB-8.
*Check:* CT-4 with a host that never acks.
