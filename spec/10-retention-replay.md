# 10 — Retention and replay

This document states what Settle keeps, for how long, and how it rebuilds state from what it keeps.

Bands: 1–9 raw rows · 10–19 derived state · 20–29 space and memory.

## Raw rows

**RS-1 — Retention.** For each raw table, virtual tables included, the store keeps the raw rows of every block above `D − P-RAW-RETENTION`. It always keeps the raw rows of every block above `D` (INV-7).

**RS-2 — Purge.** Each checkpoint deletes the stored raw rows of the blocks at or below `D − P-RAW-RETENTION`, in steps of P-PURGE-STEP blocks (WP-43). The deletes are part of the atomic write set of the checkpoint. When P-RAW-RETENTION has its largest value, no purge occurs (ADR-3).

**RS-3 — Purge is invisible.** A purge emits no change record and changes no derived state (INV-48). The target keeps its copy of the purged rows.

**RS-4 — Rollback reach.** A rollback never goes below `F`, and `D ≤ F` (INV-1). So a rollback reads only raw rows above `D`, and RS-1 keeps them.

**RS-5 — No archive.** Settle offers no read of the raw rows. A purged row is gone from Settle (NG-8).

## Derived state

**RS-10 — Final state.** For each reducer group and each view group, Settle keeps the state at `F` in memory. A checkpoint writes the state at `F` of each group that changed since the last checkpoint.

**RS-11 — Unfinal data.** For each block above `F`, Settle keeps undo data. This is the state of each reducer group before each block that changed it, and the contribution of the block to each view group. The size of this data grows with `L − F` and with the groups that each block changes.

**RS-12 — Replay.** An open replays the blocks above `D` (DEF-87). It reads at most P-REPLAY-CHUNK blocks with rows per table at a time. So the raw rows of the tail are never in memory all at once. The work of a replay is linear in the tail (LIV-3). After the replay, the replayed blocks at or below `F` are final (ADR-7).

**RS-13 — Sliding windows.** For a sliding view, the store holds one record for each final block in the window. The expired-block stash keeps each block that left the window until the block that pushed it out is final (ADR-4). Then the stash entry and the window record of the expired block go away (LIV-8).

**RS-14 — Hash anchors.** Settle keeps `H(b)` for each block at or above `F` with a supplied hash. Below `F`, it keeps only the hashes of `D` and `L`.

**RS-15 — Journal size.** The journal holds the entries of at most P-MAX-UNACKED places. The commit of a batch deletes its entries, and an open deletes all entries (LIV-9).

**RS-16 — External chains.** An open does not replay an external reducer that has no code. If `D < min(F, L)` after the replay, the open waits for code: the reducers and views stay final at `D`, and `F` keeps its stored value. Each attach then replays its chain from `D + 1` (WP-60). The first ingest call after the last attach makes the replayed blocks final, and this ends the wait. With no such gap, an attach replays its chain from `F + 1` (ADR-17).

## Space and memory

**RS-20 — Resident memory.** The resident memory of an instance is the sum of these parts:

- the resident reducer groups and their states (RS-24);
- the resident view groups (RS-24);
- the unfinal data of RS-11;
- the write sets of the unacked batches;
- the key filters of the persistent store, P-KEY-FILTER for each stored group (RS-24).

The ceiling P-MEMORY-CEILING is the sum of the bounds of these parts (REQ-41, ADR-8). Each reducer and each view that is not sliding keeps about P-RESIDENT-GROUPS groups, and also the groups that RS-24 keeps in memory. A sliding view keeps the groups of its window. The memory backend holds the store in memory too, so an instance in memory has no ceiling.

**RS-21 — Disk bound.** The disk use of the persistent backend is at most `P-DISK-AMP × live + P-DISK-CONST`. The live data is the raw rows that RS-1 keeps, the final derived state, the window records, the stash and the journal. The bound needs compaction of the store, and a log of at most P-WAL-BOUND (ADR-3, HZ-9).

**RS-22 — Orphan data.** The stored data and layout of an object that the schema removed stay in the store (CN-34). They count against RS-21 with no end (GAP-18).

**RS-23 — Finality stall.** When `F` does not move, the unfinal data, the hashes and the raw rows above `F` grow linearly with `L − F`. This growth is inherent (HZ-4). The host controls it with the finality that it reports.

**RS-24 — Group eviction.** A reducer group or a view group leaves memory only when the committed store holds its state. So the group has no unfinal block, no final change that waits for a checkpoint, and the checkpoint that last wrote it is committed. After each ingest call, a reducer or a view that is not sliding and holds more than P-RESIDENT-GROUPS groups drops the groups that can leave. The groups that a checkpoint wrote longest ago go first, down to three quarters of P-RESIDENT-GROUPS.

A group that is not in memory loads its stored state on its next row. The key filters of the persistent store show that a group is new without a read of the store. An open loads no group of a view that is not sliding. An eviction changes no batch (INV-48).

## Interactions

- **With finality:** finality frees the unfinal data (RS-11, LIV-6) and the hashes below `F` (RS-14).
- **With forks:** a rollback removes the unfinal data above the fork point and restores the stash entries above it (WP-14). It reads raw rows only above `D` (RS-4).
- **With deferral:** `D` can lag `min(F, L)` by up to P-CHECKPOINT-INTERVAL blocks. The purge follows `D`, so the store keeps more raw rows, and the replay tail grows by the interval.
- **With a reopen:** the open sets the replay tail to the blocks above `D`. Only the blocks of the tail above `F` stay as unfinal data.
- **With liveness:** LIV-7, LIV-8 and LIV-9 bound how fast the space comes back.
- **With eviction:** the groups that the blocks above `F`, a deferred checkpoint or an unacked batch changed stay in memory (RS-24). So a finality stall, a long interval or a long ack queue can keep a node above P-RESIDENT-GROUPS.
