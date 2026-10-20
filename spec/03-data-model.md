# 03 — Data model

This document defines the terms that the other documents use. It states no operational rule. The well-formedness rules of the state are the structural invariants INV-1 to INV-9 in [07-invariants.md](07-invariants.md).

Bands: 1–19 chain and input · 20–39 pipeline · 40–59 derived values · 60–79 output and delivery · 80–99 instance state.

## Chain and input

**DEF-1 — Block number.** A natural number in the range 0 to 2^64 − 1. Block 0 is an ordinary block. An empty database has no latest block, which is ⊥ and not 0.

**DEF-2 — Block hash.** An opaque text that the host supplies. Two hashes are equal only when their texts are equal byte for byte.

**DEF-3 — Cursor.** A pair (block number, block hash).

**DEF-4 — Value.** One element of a closed set of types:

| Type | Content |
|---|---|
| UInt64 | An unsigned 64-bit integer. |
| Int64 | A signed 64-bit integer. |
| Float64 | An IEEE 754 double. |
| Uint256 | An unsigned 256-bit integer. |
| String | A UTF-8 text. |
| DateTime | A signed 64-bit count of milliseconds since the Unix epoch. |
| Boolean | True or false. |
| Bytes | A byte sequence. |
| Base58 | A byte sequence that the schema marks as base58 data. |
| JSON | A JSON document. |
| Null | No value. Every column accepts Null. |

**DEF-5 — Value equality.** Two values are equal when they have the same type and the same content. Float64 values are equal when their bit patterns are equal, so a NaN equals the same NaN and −0.0 does not equal 0.0. Values of two different types are never equal.

**DEF-6 — Row.** A sequence of values in the column order of its table, or in the emit column order of its reducer.

**DEF-7 — Input block.** A block number `b` and, for each raw table `t`, a sequence of rows (DEF-6) `R(t, b)`. The row index of a row is its position in `R(t, b)`, from 0.

**DEF-8 — Rollback chain.** A set of cursors that the host supplies with an ingest call or a fork call. The order of the cursors has no meaning.

**DEF-9 — Finalized head.** The cursor that the host supplies with each ingest call. It is the claim of the source that this block and every block below it are final.

**DEF-10 — Accepted chain.** The map `A` from block number to input block, for every block that Settle accepted and did not roll back. `A↾b` is the part of `A` at or below block `b`.

**DEF-11 — Stored hash.** The partial map `H` from block number to hash. A successful call sets `H(b)` to the hash of each cursor that it supplied. A rollback to block `p` removes `H(b)` for every `b > p`. Below the finality watermark, Settle keeps only the anchors of RS-14.

**DEF-12 — Hash conflict.** A supplied cursor `(b, h)` with `b ≤ L`, `H(b)` defined and `h ≠ H(b)`. A block that the chain leaves out is not a conflict.

**DEF-13 — Fork point.** The block that a rollback keeps as the new head. It is the highest supplied block `p` with `F ≤ p ≤ L` whose supplied hash equals `H(p)`. For an ingest call, `p` also lies below the lowest hash conflict. The term "common ancestor" means the same block.

**DEF-14 — Gappy chain.** A chain whose block numbers are not contiguous, or whose finalized head lies above the latest block.

**DEF-15 — Backfill call.** An ingest call whose finalized head is at or above every block of the call. The code calls it a no-lag call.

## Pipeline

**DEF-20 — Schema.** A text in the schema language of IB-60. It declares the pipeline.

**DEF-21 — Pipeline.** A directed acyclic graph of objects. Raw tables have no source. Each reducer and each view reads exactly one raw table or one reducer. No object reads a view (NG-10).

**DEF-22 — Raw table.** A name and a sequence of typed columns. Each input row also carries its block number.

**DEF-23 — Virtual table.** A raw table whose rows feed the pipeline but which is not an output table.

**DEF-24 — Reducer.** A name, a source, a list of group-by columns, a sequence of state fields, a body, a runtime kind and a list of required modules. Each state field has a name, a type and a default, or is a Map field (DEF-26). A reducer can have no group-by column, and then it has one group.

**DEF-25 — Group and group key.** The group key of a source row is the tuple of its group-by values. Null is a valid element of a group key. A group is the set of rows of a source that share a group key. Today a String key can share a reducer group with a Null key (GAP-30).

**DEF-26 — Reducer state.** One value for each state field of each group. A new group starts with the defaults of the state fields. A Map field holds entries instead of one value: a value of its type for each String key. A new group has no entries, and no entry holds Null (ADR-12).

**DEF-27 — Emit row.** A row that a reducer body outputs for an input row. A body can emit zero or more rows for one input row. The emit columns of a reducer are the group-by columns, then the other names that the body emits, in the order of first use. Their number is at most P-MAX-EMIT-COLUMNS. When an emit row leaves a group-by column Null, the column takes the value of the input row.

**DEF-28 — Runtime kind.** The way that Settle runs a reducer body:

| Kind | Body | Call unit |
|---|---|---|
| Lua | A script in a sandbox. | One call for each input row. |
| External | Host code, for example a JS function. | One call for each block, with every group of the block. |
| Rust closure | A function that the host attaches. | One call for each input row. |

**DEF-29 — Module.** Shared Lua code that a reducer can require.

**DEF-30 — View.** A materialized view: a name, a source, a select list and an optional sliding window. The select list holds key items and aggregates. A key item is a plain column or a tumbling window start. The group key of a view is the tuple of the values of its key items, in select order.

**DEF-31 — Output table.** Each non-virtual raw table (DEF-22) and each view. A reducer is never an output table.

**DEF-32 — Tumbling window.** A key item `toStartOfInterval(c, I)`. For a row with timestamp `t` in milliseconds, its value is `trunc(t / I) × I`, where `trunc` rounds toward zero.

**DEF-33 — Sliding window.** A width `W` over a time column `c` of a view. The block timestamp `ts(b)` is the largest valid timestamp in the rows of block `b`. If the block has no valid timestamp, `ts(b)` is the time watermark of the blocks before it. The time watermark `τ` of a chain is the largest block timestamp in it, or 0 for an empty chain.

**DEF-34 — Layout.** The stored description that decides how the stored data of an object reads back:

| Object | Layout content |
|---|---|
| Raw table | The column names and types, in order. |
| Reducer | The source, the group-by columns, and the state field names and types, in order. |
| View | The source, and the select list: each key column, each aggregate as a function and a column, each window as a column and an interval. Also the group-by list and the sliding window: the width and the time column. |

The layout does not contain reducer bodies, defaults, output names, the virtual flag or the runtime kind.

**DEF-35 — Format version.** One number for the whole database, P-FORMAT-VERSION.

## Derived values

**DEF-40 — Chain order.** The rows of a raw table in `A`, sorted by block number and then by row index. The emit rows of a reducer, sorted by block number, then by input row index, then by emit order.

**DEF-41 — Reducer result.** For each group `g`, `state_g(A)` is the fold of the body over the rows of `g` in chain order, from the defaults. The emit rows of the reducer are the emit rows of these body calls, in chain order. Each body call sees only the state of its own group and the input row (INV-36). The body and the defaults are those of the current run. The fold covers the whole chain, so CN-32 limits the changes between two runs.

**DEF-42 — Aggregate.** A function over the rows of one view group. In a sliding view, only the rows of the blocks in the window count (DEF-45).

An aggregate folds in two levels. First, the values of each block fold in chain order into one block contribution, from the zero of the numeric mode. Then the block contributions fold in chain order, from the same zero. A Float64 sum and a saturating integer sum are not associative, so this order is part of the contract (ADR-4). For example, the blocks `[1e16]` and `[−1e16, 1]` give a Float64 sum of 0, and a fold of the rows gives 1.

| Function | Input values | Result | The aggregate has data when |
|---|---|---|---|
| `count()` | Each row. | The number of rows, as UInt64. | At least one row reached it. |
| `count(c)` | The non-Null values of `c`. | The number of these values, as UInt64. | At least one row reached it. Today a group with only Null values loses its data at finality (GAP-40). |
| `sum(c)` | The values of `c` in the numeric mode of `c` (DEF-43). | The sum in the arithmetic of the mode, in the two fold levels above. | At least one input value. |
| `min(c)`, `max(c)` | The same as `sum`. | The smallest or largest value. A NaN does not count. | At least one input value. |
| `avg(c)` | The same as `sum`. | The sum divided by the number of values, as Float64. In the BigInt mode, an integer division that gives a Uint256. | At least one input value. |
| `first(c)` | The non-Null values of `c`. | The first value in chain order. | At least one input value. |
| `last(c)` | The non-Null values of `c`. | The last value in chain order. | At least one input value. |

**DEF-43 — Numeric mode.** The arithmetic of `sum`, `min`, `max` and `avg` for a column. The declared type of the source column sets it. For a reducer source, the type of a state field or a group-by column sets it.

| Mode | Column types | Accumulator | Values that the mode reads | Result type |
|---|---|---|---|---|
| Int | UInt64, Int64, DateTime | A signed 128-bit integer with saturation. | UInt64, Int64, DateTime. | UInt64 when the result is in the UInt64 range, else Int64 when it fits, else Float64. |
| BigInt | Uint256 | An unsigned 256-bit integer with saturation. | Uint256, UInt64. | Uint256. |
| Float | Each other type, and each emit column that is not a state field or a group-by column. | An IEEE 754 double. | UInt64, Int64, Float64, DateTime. | Float64. |

By REQ-27, every value of a column is in its mode. The current code skips a value outside the mode without an error (GAP-1).

**DEF-44 — Group presence.** A view group is present in the output when at least one of its aggregates has data.

**DEF-45 — Window membership.** In a sliding view, a block `b` of `A` is in the window when `ts(b) ≥ τ(A) − W`. A block at the cutoff stays in the window.

**DEF-46 — Output image.** `Out(A)` gives, for each output table, the set of its rows over `A`:

- For a raw table `t`: one row for each pair `(b, i)` with `b` in `A` and `i` a row index of `R(t, b)`. The key is `(b, i)`, and the values are the columns of the row.
- For a view `v`: one row for each present group `g`. The key is `g`, and the values are the key items and the aggregate results, in select order.

`Out(A)` does not depend on purge, checkpoints, reopens or the history of forks. It depends only on `A`.

## Output and delivery

**DEF-60 — Change record.** A tuple (table, operation, key, values, previous values). The operation is insert, update or delete.

**DEF-61 — Record key.** For a raw table, the pair (`block_number`, `_row_index`). For a view, the group key.

**DEF-62 — Change batch.** A tuple (sequence, latest head, finalized head, records by table, performance tree). The short form is "batch".

**DEF-63 — Sequence.** A natural number from 1 that numbers the returned batches of a database.

**DEF-64 — Target image.** The state that a target reaches when it applies batches in sequence order, from empty. An insert puts the row of its key. An update replaces the values of its key. A delete removes its key.

**DEF-65 — Compensation batch.** A batch that a rollback produces. A fork call returns it alone. An ingest call with a hash conflict merges it with the records of its new rows.

**DEF-66 — Ack.** The call `ack(n)`. It tells Settle that the target applied every batch up to sequence `n`.

**DEF-67 — Unacked batch, ack queue and place.** A batch that Settle returned and the host did not ack yet is an unacked batch. The ack queue holds the unacked batches in sequence order. Each unacked batch takes one place, and each heartbeat call that joins it takes one more place.

**DEF-68 — Heartbeat call.** An ingest call or a fork call that succeeds and returns no batch. Its writes join the newest unacked batch, or commit at once when the queue is empty.

**DEF-69 — Journal.** A store, inside the database, of each returned and unacked batch and of the writes of the heartbeat calls that joined it. Journal mode is on when the host gives an applied sequence at open.

**DEF-70 — Applied sequence.** The sequence of the newest batch that the target applied. The target stores it in the same transaction as the data of the batch. Before the first batch it is 0.

**DEF-71 — Committed state.** The state that the store of Settle holds: the state after the last committed batch and the heartbeat writes committed with it.

## Instance state

**DEF-80 — Latest block.** `L` is the highest block of `A` with rows. After a rollback to `p`, `L` is `p`, until a later block with rows comes. On a database that never accepted rows, `L` is ⊥.

**DEF-81 — Finality watermark.** `F` is a block number, 0 at first. Ingest calls set it from the finalized head. It bounds every rollback.

**DEF-82 — Durable watermark.** `D` is the highest block whose derived state is in the committed state, or ⊥ when no derived state is committed.

**DEF-83 — Checkpoint.** The write of the derived state up to `F` into the writes of a call. A checkpoint sets `D` to `min(F, L)`.

**DEF-84 — Backfill deferral.** A mode where Settle skips the checkpoint of a backfill call until `F − D` reaches P-CHECKPOINT-INTERVAL. A pipeline is deferral-safe when it has no sliding view and no external reducer.

**DEF-85 — Health.** One of healthy, poisoned and sync-failed. A poisoned instance keeps the reason of its first failure.

**DEF-86 — Instance state.** The tuple `S = ⟨Π, A, H, L, F, D, Q, s, h, C, J⟩`:

| Element | Meaning |
|---|---|
| `Π` | The pipeline. |
| `A`, `H` | The accepted chain and the stored hashes. |
| `L`, `F`, `D` | The latest block, the finality watermark and the durable watermark. |
| `Q` | The ack queue: a sequence of entries (sequence, writes, number of joined heartbeat calls). |
| `s` | The next sequence. |
| `h` | The health. |
| `C` | The committed state. |
| `J` | The journal, in journal mode. |

The derived state of each reducer and view is a function of `A` (DEF-41, DEF-42). INV-1 to INV-9 state the well-formedness of `S`.

**DEF-87 — Replay.** At open, Settle sends the stored raw rows of the blocks above `D` through the pipeline, with no change records. A reducer attach replays its chain in the same way. The result follows CN-24.

**DEF-88 — Retention margin and purge.** The retention margin is P-RAW-RETENTION blocks. A purge deletes the stored raw rows of the blocks at or below `D − P-RAW-RETENTION`.

**DEF-89 — Expired-block stash.** For a sliding view, the data of each block that left the window, keyed by the block that pushed it out. Settle keeps an entry until that block is final.

**DEF-90 — Observation point.** A moment between two calls. Status reads and the committed state are defined only at observation points.

**DEF-91 — Store.** The backend that holds the committed state of an instance: the persistent backend of a data directory, or the memory backend.

## Transition summary

The semantics of each transition are in [04-mutations.md](04-mutations.md).

| ID | Transition | One line |
|---|---|---|
| TR-1 | Ingest | Validate, roll back on a hash conflict, process the new rows, move `F`, write a checkpoint when due, return a batch or join the queue. |
| TR-2 | Fork | Roll back to the fork point of the supplied chain and return a compensation batch. |
| TR-3 | Ack | Commit the unacked batches up to a sequence, oldest first, then sync. |
| TR-4 | Open | Check the format, reconcile the journal, restore the committed state and replay the tail. |
| TR-5 | Attach | Attach external reducer code, register a reducer at run time, or replay a reducer chain. |
| TR-6 | Crash | The process stops. The state in memory is lost; the committed state stays. |
| TR-7 | Poison | A failure that Settle cannot undo moves the instance to poisoned or sync-failed. |

## Terminology

| Term in the code | Term in this suite |
|---|---|
| `latest_block`, `latest_cursor` | Latest block `L` (DEF-80) |
| `finalized_block`, `finalized_cursor` | Finality watermark `F` (DEF-81) |
| `durable_block`; the stored meta key `finalized_block` | Durable watermark `D` (DEF-82) |
| `finalized_head` | Finalized head (DEF-9) |
| `rollback_chain`, `previous_blocks` | Rollback chain (DEF-8) |
| `handle_fork` | Fork call (TR-2) |
| `ingest`, `ingest_rows` | Ingest call (TR-1) |
| no-lag | Backfill call (DEF-15) |
| `ChangeBatch` | Change batch (DEF-62) |
| `ChangeRecord`, `ChangeOp` | Change record and its operation (DEF-60) |
| `prev_values`, `prevValues` | Previous values (DEF-60) |
| `_row_index` | Row index (DEF-7) |
| `PendingAck`, `unacked_batches`, `max_pending_batches` | Ack queue and places (DEF-67), P-MAX-UNACKED |
| heartbeat, joined writes | Heartbeat call (DEF-68) |
| `applied_sequence` | Applied sequence (DEF-70) |
| journal part | Journal entry (DEF-69) |
| `write_checkpoint`, `advance_finality` | Checkpoint (DEF-83), finality advance (WP-20) |
| `backfill_checkpoint_interval` | P-CHECKPOINT-INTERVAL |
| `defer_allowed` | Deferral-safe pipeline (DEF-84) |
| `raw_retention_blocks`, purge | Retention margin (DEF-88), P-RAW-RETENTION |
| `VIRTUAL` | Virtual table (DEF-23) |
| materialized view, MV | View (DEF-30) |
| `LANGUAGE EXTERNAL`, `register_reducer_callback` | External runtime kind (DEF-28), attach (TR-5) |
| `FnReducerRuntime` | Rust closure runtime kind (DEF-28) |
| emits | Emit rows (DEF-27) |
| `toStartOfInterval` | Tumbling window (DEF-32) |
| `WINDOW SLIDING`, watermark of a view | Sliding window and time watermark (DEF-33) |
| `expired` | Expired-block stash (DEF-89) |
| stamp, layout, `FORMAT_VERSION` | Layout (DEF-34), format version (DEF-35) |
| poisoned, `sync_failed` | Health (DEF-85) |
| `replay_unfinalized`, `replay_reducer_chain` | Replay (DEF-87) |
| `PerfNode` | Performance tree (DEF-62) |
| `StorageBackend`, `RocksDbBackend`, `MemoryBackend` | Store (DEF-91) |
