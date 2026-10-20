# 06 — Consistency and durability

This document states the commit model, the durability tiers, the recovery contract and the format gate.

Bands: 1–9 commit model · 10–19 durability · 20–29 recovery · 30–39 format · 40–49 interference.

## Commit model

**CN-1 — One writer.** One instance at a time uses a database, and the host makes one call at a time (WP-1, NG-3). The calls of an instance thus have a total order.

**CN-2 — Atomic batches.** The committed state (DEF-71) is always the state after a prefix of the returned batches, each with its joined heartbeat writes. It also holds the heartbeat commits of an empty queue. No commit holds a part of a write set.

**CN-3 — Commit order.** Batches commit in sequence order. A later batch never commits before an earlier one.

**CN-4 — Memory ahead of disk.** The state in memory equals the committed state with the write sets of the unacked batches applied in order.

**CN-5 — Reads through unacked writes.** When a rollback needs stored data, Settle reads it through the write sets of the unacked batches. So a fork undoes the effect of unacked batches as well as acked ones.

## Durability

**CN-10 — Durability tiers.** With the persistent backend, a crash keeps these writes:

| Writes | Process crash | Power loss |
|---|---|---|
| An unacked batch, outside journal mode | Lost | Lost |
| An unacked batch, in journal mode | Kept: the journal entry is synced before the call returns (WP-34) | Kept |
| Heartbeat writes that joined an unacked batch, in journal mode | Kept | Can be lost |
| An acked batch, outside journal mode | Kept | Kept: the ack syncs (WP-34) |
| An acked batch, in journal mode | Kept | Kept: if the commit is lost, the open commits the journal entry again (WP-52) |
| A heartbeat commit with an empty queue | Kept | Can be lost until a later sync (WP-35) |

**CN-11 — Memory backend.** The memory backend keeps no state after the host drops the instance.

**CN-12 — Suffix loss.** A power loss removes only a suffix of the commits. The committed state after it is the state after a prefix of the commits (CN-2).

## Recovery

**CN-20 — Recovery contract.** After a crash and an open outside journal mode, each element of the state equals its value after the last durable commit. This holds for `A`, `H`, `L`, `F`, `D`, `s` and the derived state of every object, also the expired-block stash and the emit columns.

**CN-21 — Journal recovery.** After an open in journal mode with an applied sequence `a` that WP-52 accepts, the state equals the state after batch `a`.

**CN-22 — Recovery idempotence.** A crash during an open, and a second open, give the same state as one open.

**CN-23 — Reopen transparency.** Take an instance at an observation point with an empty queue. A drop and an open, followed by the same calls, give the same batches as the same calls with no reopen (INV-43).

**CN-24 — Replay equivalence.** The replay of a tail gives the same derived state as the live run of the same blocks.

**CN-25 — External chains at recovery.** Until code attaches to an external reducer, the derived state of its chain covers only the blocks at or below `D`. An ingest call that reaches the chain fails (WP-62). While the open waits for code (RS-16), every ingest call fails. The attach replays the chain (WP-60).

## Format

**CN-30 — Format gate.** An open refuses stored data that the schema would read with another layout (WP-51), and an open changes nothing in the store. The current code commits the journal before the second format check, so a failed open can change the store (GAP-19).

**CN-31 — No migration.** Settle does not convert stored data from one layout or format version to another (ADR-3).

**CN-32 — Allowed schema changes.** Between two opens, the schema can change reducer bodies, defaults, output names and runtime code, and it can add objects. The layout check also lets the virtual flag and the runtime kind change. The intent is to refuse these two changes too (GAP-42).

A change of a body, a default or external code MUST keep the results of the rows that Settle accepted earlier (ADR-11). A result is the state and the emit rows of a row. The schema author makes sure of this, and Settle does not check it. For example, a change from `n += x` to `n += 2 · x` breaks the rule. A change to `2 · x` only for the blocks above `L` of the last run keeps it. After a change that breaks the rule, the output of the reducer and of the objects after it is unspecified.

**CN-33 — Start of a new object.** An object that a schema adds covers only the blocks above `D` of the open where it first appears. Its output image is over that part of the chain. The other objects keep their full coverage.

**CN-34 — Removed objects.** The stored data of an object that the schema removes stays in the store. The intent is that it does not come back unseen when the object comes back (GAP-18, OQ-3).

## Interference

**CN-40 — Maintenance transparency.** Purge, compaction of the store and log rotation never change the state in memory or any batch. They also do not change the committed state that an open reads (INV-48).

**CN-41 — Clock independence.** No batch and no state depends on the wall clock (NG-6). Only the performance tree, the age in PENDING_ACK and the log events read it.

**CN-42 — Parallel stages.** Settle can process independent stages on more than one thread. The batches and the state are the same as with one thread (INV-25).

**CN-43 — Exclusive directory.** While an instance has a data directory open, a second open of that directory, in the same process or another, fails with STORAGE_FAILED. The current code relies on the lock of the persistent backend, and no test covers it.

**CN-44 — Instance isolation.** Two instances with different data directories, or two memory instances, share no state.
