# 05 — Output contract

This document states what the host and the target get from Settle: the change batches, the fork results, the status reads and the error classes. Settle answers no query of the output tables (NG-1). The write rules that produce them are in [04-mutations.md](04-mutations.md).

Bands: 1–9 batch content · 10–19 sequence and order · 20–29 target duties · 30–39 status reads.

## Operations

| Operation | Result | Changes state? |
|---|---|---|
| Ingest call (TR-1) | A batch, or ⊥ for a heartbeat call, or an error | Yes |
| Fork call (TR-2) | The fork point cursor and an optional compensation batch, or an error | Yes |
| Ack (TR-3) | Success or an error | Yes |
| Latest cursor read | `(L, H(L))`, or ⊥ on an empty database | No |
| Finalized cursor read | `(F, H(F))` | No |
| Durable block read | `D` | No |
| Places read | The places in use, and whether any batch waits for an ack | No |
| Health read | Whether the instance is poisoned | No |
| Object reads | Whether a reducer exists and is external, and the columns of a table | No |

[14-interface-binding.md](14-interface-binding.md) maps each operation to its call in each binding.

## Batch content

**RP-1 — Tables.** A batch holds records only of output tables (DEF-31), and only of the output tables that have a record. No batch holds a record of a virtual table or of a reducer.

**RP-2 — Record shape.** The shape of a record depends on its table:

| Table | Key | Values |
|---|---|---|
| Raw table | (`block_number`, `_row_index`) (WP-8) | Every declared column, in order. An absent column is Null. |
| View | The key items, in select order | Every select item, in select order, with the key items. |

**RP-3 — Operations.** Let `T` be `Out(A)` before the call that returns a batch, and `T′` be `Out(A)` after it. By INV-20, `T` is also the target image before the batch, except at the keys of RP-21. Each record of the batch has one of these operations:

- An insert record names a key that is absent in `T` and present in `T′`.
- An update record names a key that is present in both, with values that differ.
- A delete record names a key that is present in `T` and absent in `T′`.

**RP-4 — Previous values.** An update record and a delete record carry the values of their key in `T`. An insert record carries no previous values. A view delete record also carries the previous values as its values. After an in-line fork, the current code gives a view delete record that merges the compensation update the values of that update (GAP-47). It gives no previous values to a raw delete record (GAP-10). It also gives none to an update record that merges a raw delete and a raw insert.

**RP-5 — One record per key.** A batch holds at most one record for each pair of table and key.

**RP-6 — No empty change.** A batch holds no record for a key whose values in `T` and `T′` are equal (DEF-5). The current code can emit an update whose values equal its previous values after an in-line fork (GAP-41).

**RP-7 — Heads.** The latest head of a batch is the cursor of `L` after the call. Its hash is empty when no hash for `L` was supplied. The finalized head is `(F, H(F))` when `H(F)` is known, and ⊥ otherwise.

**RP-8 — Performance tree.** A batch carries a tree of timings of the stages that produced it. Its content is informational and not part of the contract (FV-3).

**RP-9 — Compensation content.** A compensation batch holds a delete record for each raw row above the fork point of each non-virtual raw table. It also holds an update or a delete record for each view group that the rollback changed. A group that the rollback brings back from the expired-block stash gets an insert record.

## Sequence and order

**RP-10 — Sequence.** Without a crash, the batches of a database have the sequences 1, 2, 3 and so on (DEF-63), over ingest calls and fork calls together. After a reopen, the next batch has the sequence that follows the last committed batch, or the applied sequence in journal mode.

**RP-11 — Merge inside a call.** Settle merges all records of one key in one call into one record. The merge of an earlier record with a later record of the same key follows this table:

| Earlier record | Later record | Result |
|---|---|---|
| insert | update | An insert with the later values. |
| insert | delete | No record. |
| update | update | An update with the later values and the earlier previous values. |
| update | delete | A delete with the earlier previous values. |
| delete | insert | An update with the later values and the earlier previous values. |
| each other pair | | The later record. |

A call whose records all cancel returns no batch (WP-38).

**RP-12 — Sequence reuse outside journal mode.** After a crash outside journal mode, Settle can give a sequence that a lost unacked batch had to a different batch (ADR-1). Outside journal mode, the target MUST NOT use a sequence alone to identify a batch across a crash.

**RP-13 — Order inside a batch.** The order of the tables in a batch and the order of the records in a table are free variables (FV-1, FV-2). The keys of a batch are distinct (RP-5), so the order has no effect on the target image.

**RP-14 — Batch size.** Settle does not split a batch. The size of a batch grows with the rows of the call and with the depth of a rollback. The host bounds it by the size of its calls.

## Target duties

**RP-20 — Order and atomicity.** The target MUST apply every returned batch, in sequence order, and each batch atomically.

**RP-21 — Idempotent apply.** Outside journal mode, a reopen can return a batch that the target already applied. When the host sends the same blocks again, the batch is the same. So the target MUST apply a batch idempotently.

After such a reopen, the target can differ from `Out(A)` at each key that a lost batch changed. An example is a row that a lost compensation deleted. The next record of such a key gives the target the values of `Out(A)` again. That record can insert a key that is present in the target, and its previous values can differ from the target row (RP-3, RP-4).

**RP-22 — Compensation order.** The target MUST apply a compensation batch after every batch that Settle returned before it (WP-15).

**RP-23 — Journal duty.** In journal mode, the target MUST store the sequence of each batch in the same transaction as its data. The host MUST give the stored sequence at open, or 0 for a new target.

**RP-24 — Ack after apply.** The host MUST ack a batch only after the target applied it. An early ack lets Settle commit a state that the target does not have.

**RP-25 — Host recovery algorithm.** After a crash, a poison or a sync failure, the host recovers with this procedure:

```
# Journal mode
a ← target.stored_sequence()            # 0 for a new target
db ← open(config, applied_sequence = a)
if the open fails with TARGET_MISMATCH:
    stop and raise an alarm             # do not retry; the stores disagree
continue the source after db.latest_cursor()

# Outside journal mode
db ← open(config)
continue the source after db.latest_cursor()
apply each batch idempotently            # RP-21

# After POISONED
for each returned batch that the target applied, in order:
    db.ack(sequence)                    # the ack still commits (WP-72)
drop db, then open as above

# After SYNC_FAILED
drop db, then open as above             # no ack is possible (WP-37)

# After PENDING_ACK
apply and ack the oldest unacked batch, then repeat the call
```

## Status reads

**RP-30 — Freshness.** Each status read shows the state at the last observation point (DEF-90). The state in memory is ahead of the committed state by the unacked batches.

**RP-31 — Latest cursor.** The latest cursor read gives `L` and `H(L)`, with an empty hash when `H(L)` is unknown, or ⊥ on an empty database.

**RP-32 — Finalized cursor.** The finalized cursor read gives `F` and `H(F)`.

**RP-33 — Durable block.** The durable block read gives `D`, or 0 when `D` is ⊥.

**RP-34 — Places.** The places read gives the number of places in use (DEF-67). It is 0 exactly when no batch waits for an ack.

**RP-35 — Health.** The health read is true for a poisoned or a sync-failed instance.

**RP-36 — No effect.** A status read changes no state (INV-13).

## Error classes

The error classes are a closed set. Each binding maps each class to a concrete error in [14-interface-binding.md](14-interface-binding.md). An error never comes with a batch (INV-26).

| ID | Class | Trigger | Retry? | State change |
|---|---|---|---|---|
| EC-1 | PENDING_ACK | The ack queue is full (WP-32), or a reducer attach finds an unacked batch. | Yes, after an ack. | None. |
| EC-2 | WRONG_ACK_SEQUENCE | An ack names a sequence above the newest returned one. | No: a host defect. | None. |
| EC-3 | INVALID_INPUT | Bad input: a finality regression, a block at or below `L`, a bad block number or an undeclared table with rows. Also a row with the wrong layout, an empty fork chain or a value of the wrong type. | No, until the input changes. | None. |
| EC-4 | FORK_BELOW_FINALITY | A hash conflict at or below `F` (WP-13). | No: the source contradicts its finality. | None. |
| EC-5 | NO_COMMON_ANCESTOR | A fork with no supplied block that matches in `[F, L]` (WP-12). | Yes, with a longer chain. | None. |
| EC-6 | INVALID_SCHEMA | The schema text fails the grammar or the validation. | No, until the schema changes. | None. |
| EC-7 | INCOMPATIBLE_STORAGE | The format version or a layout does not match (WP-51, WP-61). | No: rename the object or use a new database. | None. |
| EC-8 | TARGET_MISMATCH | The journal and the applied sequence do not reconcile (WP-52). | No: an operator task. | None. |
| EC-9 | INVALID_OPERATION | A call that the current state does not allow. Examples: a rule of WP-60 or WP-61, an external reducer with no code (WP-62), a re-entrant call (WP-1). | No, until the state changes. | None. |
| EC-10 | REDUCER_FAILED | A reducer body fails, or a reducer emits more than P-MAX-EMIT-COLUMNS columns. | No, until the logic or the input changes. | None, or poison after a rollback in the same call (WP-71). |
| EC-11 | STORAGE_FAILED | A read or write of the store fails, or stored data does not decode. | An ack commit: yes. Otherwise drop and reopen. | For an ack commit: the batches before the failed batch stay committed (WP-36), and nothing else changes. Otherwise poison where WP-71 says so. |
| EC-12 | SYNC_FAILED | A sync failed in this instance (WP-37). | No: drop and reopen. | Sync-failed health. |
| EC-13 | POISONED | A call on a poisoned instance (WP-72). | No: ack, drop and reopen. | None. |
| EC-14 | INVALID_CONFIG | A configuration value that Settle cannot use: an unknown compression name, a negative count, or a data directory without the persistent backend. | No, until the configuration changes. | None. |
