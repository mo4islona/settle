# 02 — Requirements

Each requirement states user-visible behavior. The acceptance status of each requirement is in the acceptance matrix of [13-conformance-tdd.md](13-conformance-tdd.md).

Bands: 1–9 core flow · 10–19 delivery and durability · 20–29 schema and computation · 30–39 bindings · 40–49 quality.

## Goals

Each design goal of [01-overview.md](01-overview.md) leads to these requirements:

| Goal | Requirements |
|---|---|
| G-1 | REQ-3, REQ-4, REQ-7 |
| G-2 | REQ-10, REQ-12, REQ-13, REQ-14 |
| G-3 | REQ-1, REQ-20 to REQ-24, REQ-35 |
| G-4 | REQ-11, REQ-16, REQ-40 |
| G-5 | REQ-20, REQ-22 |
| G-6 | REQ-31, REQ-32, REQ-33, REQ-34 |
| G-7 | REQ-25, REQ-26, REQ-27, REQ-42 |
| G-8 | REQ-41, REQ-43, REQ-44 |

## Core flow

**REQ-1 — Pipeline from one schema text.** [MUST]
Settle builds its whole pipeline from one schema text: raw tables, virtual tables, modules, reducers and materialized views. Settle refuses at open a schema that fails the grammar or the validation rules, and the error names the object at fault.
*Acceptance:* An open with each example schema of IB-60 succeeds. An open with an absent source, a reducer cycle or an unknown type fails with INVALID_SCHEMA.
*Trace:* ADR-14; DEF-20, DEF-21, IB-60, IB-61.

**REQ-2 — Atomic ingest.** [MUST]
The host gives Settle the rows of one or more blocks, a rollback chain and a finalized head in one ingest call. The call has its full effect or no effect, and it returns at most one change batch.
*Acceptance:* After an ingest that fails validation, every status read and every later batch are the same as if the call did not occur. A successful ingest returns zero or one batch.
*Trace:* ADR-1; TR-1, WP-2, INV-12.

**REQ-3 — Exact change batches.** [MUST]
Each change batch states the difference of the output tables before and after the call, as insert, update and delete records. Each update record and each delete record carries the previous values of its row.
*Acceptance:* A target that applies every batch in sequence order equals a recomputation of the output tables over the accepted chain. The previous values of each record equal the row of the target before the batch.
*Trace:* ADR-4; RP-1, RP-3, RP-4, INV-20, INV-21, INV-22.

**REQ-4 — Fork compensation.** [MUST]
When the source replaces recent blocks, Settle undoes their effect on the target with a compensation batch. The host can signal the fork in a fork call, or give the new chain with the next ingest call.
*Acceptance:* After a random history of ingests and forks above finality, the target equals a recomputation over the chain that remains (CT-1). The two fork paths give the same target.
*Trace:* ADR-2; TR-2, WP-10, WP-11, WP-14, INV-20.

**REQ-5 — Irreversible finality.** [MUST]
The host reports a finalized head with every ingest call. Within a run, blocks at or below the finality watermark never change. Across a reopen, the same holds for the finality watermark of the last durable commit. Settle refuses a fork whose common ancestor lies below the watermark.
*Acceptance:* A fork below finality fails with FORK_BELOW_FINALITY and changes nothing. After a reopen, the finalized cursor equals the finalized cursor of the last durable commit.
*Trace:* ADR-2, ADR-7; WP-13, WP-20, INV-10, INV-11, INV-47.

**REQ-6 — Gaps in the chain.** [MUST]
Settle accepts block numbers with gaps. The finalized head can lie above the latest block, for chains that skip block numbers.
*Acceptance:* An ingest of blocks 1, 5 and 9 with a finalized head at 20 succeeds. A later ingest of block 12 succeeds, and so does a later ingest of block 21.
*Trace:* ADR-2; WP-7, WP-20.

**REQ-7 — Virtual tables.** [MUST]
A virtual table feeds reducers and views, but Settle sends no record of it to the target.
*Acceptance:* No batch contains a record of a virtual table, also after a fork and after a reopen.
*Trace:* DEF-23; RP-1, INV-23.

**REQ-8 — One caller at a time.** [MUST — intent, currently violated by GAP-16]
The host uses an instance from one thread at a time. Settle refuses a call that a reducer callback makes into the instance that runs it.
*Acceptance:* A JS reducer callback that calls ack on its own instance gets INVALID_OPERATION, and the ingest that runs the callback is not affected.
*Trace:* WP-1, CN-1, FM-46.

## Delivery and durability

**REQ-10 — Ack-gated durability.** [MUST]
A returned batch reaches the committed state only when the host acks it. An ack is cumulative: an ack of sequence n commits every returned batch up to n.
*Acceptance:* A crash before the ack of batch n and a reopen give the state after the last acked batch. One ack of the newest sequence commits all unacked batches.
*Trace:* ADR-1; WP-30, WP-31, CN-2, INV-15.

**REQ-11 — Pipelined delivery.** [MUST]
Up to P-MAX-UNACKED places can wait for an ack, so Settle works on the next blocks while the target applies the previous batch. When the queue is full, an ingest fails with PENDING_ACK and changes nothing.
*Acceptance:* With P-MAX-UNACKED at 2, two ingests return batches without an ack, and the third fails with PENDING_ACK. After an ack, the third ingest succeeds.
*Trace:* ADR-1; WP-32, WP-33, INV-2.

**REQ-12 — Restart from the last acked batch.** [MUST]
After a crash and a reopen, Settle continues from the state after the last acked batch. When the host sends the same blocks again, Settle returns the same batches.
*Acceptance:* The reopen-differential test (CT-2) passes: an instance that reopens at random points gives the same target as an instance that never stops.
*Trace:* ADR-1; CN-20, INV-40, INV-43.

**REQ-13 — Journal reconcile with the target.** [MUST]
When the host gives the applied sequence of the target at open, Settle commits the batches that the target applied and drops the others. Settle refuses to open when it cannot reconcile with the target.
*Acceptance:* The target applies batch n, the process stops before the ack, and the host reopens with applied sequence n. A later fork of those blocks then undoes them in the target. An applied sequence below the last acked sequence fails with TARGET_MISMATCH.
*Trace:* ADR-1; WP-52, CN-21, INV-41.

**REQ-14 — Power-loss durability.** [MUST]
With the persistent backend, an acked batch survives a power loss. In journal mode, a returned batch survives a power loss before the host gets it.
*Acceptance:* A power-loss simulation after a successful ack keeps the batch (CT-3). After a failed sync, every later call fails with SYNC_FAILED.
*Trace:* ADR-1; WP-34, WP-37, CN-10, FM-31.

**REQ-15 — Memory mode.** [MUST]
Without a data directory, Settle keeps all state in memory and loses it when the host drops the instance.
*Acceptance:* An open without a data directory creates no file, in each binding.
*Trace:* CN-11, IB-23.

**REQ-16 — Backfill deferral.** [SHOULD]
In a backfill, the finalized head is at the tip of each batch. The operator can then let Settle write a checkpoint every P-CHECKPOINT-INTERVAL blocks in place of every block, with the same output.
*Acceptance:* The batches with an interval of N equal the batches with an interval of 1 (CT-2). A crash replays at most the blocks after the last checkpoint.
*Trace:* ADR-2; WP-40, WP-41, WP-42, INV-44, PF-8.

## Schema and computation

**REQ-20 — Aggregates.** [MUST]
A view supports sum, a count of rows, a count of values, min, max, avg, first and last. Each aggregate stays exact under a rollback, also the aggregates that have no inverse.
*Acceptance:* After random forks, each aggregate equals a recomputation over the chain that remains (CT-1). An integer sum stays exact within the accumulator range of DEF-43.
*Trace:* ADR-4; DEF-42, DEF-43, INV-24.

**REQ-21 — Tumbling windows.** [MUST]
A view can group rows by the start of a fixed time interval that a timestamp column gives.
*Acceptance:* Two rows with the same interval start land in one group. A row in the next interval lands in the next group.
*Trace:* DEF-32.

**REQ-22 — Sliding windows.** [MUST]
A view can aggregate the blocks whose timestamps lie within a width of the newest timestamp. The window stays exact under a rollback.
*Acceptance:* After random forks, the view equals a fresh view that receives only the blocks that remain (the sliding-window oracle of CT-1).
*Trace:* ADR-4; DEF-33, DEF-45, RS-13.

**REQ-23 — Reducers.** [MUST]
A reducer keeps a typed state for each group, with defaults, and emits rows for the next stage. A state field can be a Map, whose entries change one at a time (ADR-12). Settle runs reducer bodies in Lua, in host code and as Rust closures. It has no other body language (ADR-10).
*Acceptance:* One reducer logic in each runtime gives the same view output (CT-1, differential). A reducer with a Map field gives the same target after random forks and reopens as an instance that never stops. A reducer with a WHEN clause fails to open with INVALID_SCHEMA.
*Trace:* ADR-5, ADR-10, ADR-12; DEF-24, DEF-26, DEF-27, DEF-28.

**REQ-24 — Chained stages.** [MUST]
A reducer can read another reducer. Settle refuses a cycle and an absent source.
*Acceptance:* A schema with a reducer cycle fails with INVALID_SCHEMA. A chain of two reducers and a view gives the same target after a reopen and after a fork.
*Trace:* DEF-21, IB-61.

**REQ-25 — Safe schema changes.** [MUST]
Settle refuses at open a schema that changes the stored layout of an object. Reducer bodies, defaults, output names and new objects can change.
*Acceptance:* A swap of two column types fails with INCOMPATIBLE_STORAGE. A change of a reducer body opens and gives batches of the new logic.
*Trace:* ADR-3; CN-30, CN-32, INV-45.

**REQ-26 — Strict schema.** [MUST — intent, currently violated by GAP-2]
Settle refuses at open each schema construct that it does not execute. It does not ignore a construct silently.
*Acceptance:* A view with WHERE, HAVING, a join, ORDER BY, LIMIT or DISTINCT fails with INVALID_SCHEMA. Two objects with one name fail. A default that does not parse as its declared type fails.
*Trace:* ADR-14; IB-61.

**REQ-27 — Type fidelity.** [MUST — intent, currently violated by GAP-1]
Every value in an output table has the declared type of its column. Every input value reaches the aggregates that read it, or the call fails. Settle refuses a value that it cannot convert exactly.
*Acceptance:* A Float64 value with a fraction in a UInt64 column fails the ingest with INVALID_INPUT. A JS reducer that emits a UInt64 state field feeds the sum of a view.
*Trace:* ADR-15; INV-27, INV-28.

## Bindings

**REQ-30 — Rust API.** [MUST — intent, currently violated by GAP-39]
The Rust crate gives open, ingest, fork, ack, the reducer attach calls and the status reads of IB-1. Only these atomic calls change state.
*Acceptance:* The public API has no call that commits, rolls back or finalizes outside an atomic call.
*Trace:* ADR-1; IB-1, IB-2.

**REQ-31 — Node binding.** [MUST]
The Node package gives the semantics of the Rust API.
*Acceptance:* The same calls give the same batches in Node and in Rust (CT-5). The package installs and loads on each platform that IB-20 lists.
*Trace:* IB-20, IB-21, IB-22, GAP-25.

**REQ-32 — Browser binding.** [MUST — intent, currently violated by GAP-23, GAP-29]
The browser package runs the same pipelines in memory.
*Acceptance:* The browser entry loads from the packed package. The same calls give the same batches in the browser and in Node.
*Trace:* IB-30, IB-31, IB-32, IB-48.

**REQ-33 — Value fidelity at the JS boundary.** [MUST — intent, currently violated by GAP-9]
Integers up to 2^64 − 1 and 256-bit values cross the JS boundary without loss, in both directions. Block numbers cross it without loss up to P-JS-SAFE-INT.
*Acceptance:* A UInt64 value of 2^64 − 1 returns as a BigInt with the same value. A cursor at block 2^32 + 5 returns 2^32 + 5.
*Trace:* IB-40, IB-41, IB-42.

**REQ-34 — Typed errors.** [SHOULD — intent, currently violated by GAP-14]
Each binding lets the host tell the error classes EC-1 to EC-14 apart without a parse of the message text.
*Acceptance:* A JS test triggers each error class and reads its class from the error object.
*Trace:* IB-50, IB-51.

**REQ-35 — Pipeline builder.** [SHOULD]
The Rust and TS builders generate the schema text and attach the reducer code in one step.
*Acceptance:* A builder pipeline and the equal schema text give the same batches.
*Trace:* IB-27, IB-70.

## Quality

**REQ-40 — Throughput floors.** [MUST]
Settle meets the throughput floors of [11-performance.md](11-performance.md) for each reference scenario on the reference host.
*Acceptance:* Each SLO row of 11 holds in the benchmark run before a release.
*Trace:* ADR-9; SLI-1, SLI-8.

**REQ-41 — Bounded memory.** [MUST — intent, currently violated by GAP-6]
The resident memory of an instance stays below a ceiling that the host can compute from the configuration.
*Acceptance:* A run with P-W-GROUPS at its largest value stays below P-MEMORY-CEILING.
*Trace:* ADR-8; PF-1, RS-20.

**REQ-42 — No termination by input.** [MUST]
No schema text, row, chain, reducer result or call sequence terminates the host process. Settle returns an error.
*Acceptance:* The fuzzers for the schema text, the row codec and the call sequence find no termination within P-FUZZ-BUDGET (CT-9).
*Trace:* ADR-6; FM-1.

**REQ-43 — Bounded open time.** [MUST]
An open replays at most the blocks above the durable watermark, and its time is linear in that tail.
*Acceptance:* The open time of the reference tail is at most P-SLO-OPEN (SC-5).
*Trace:* ADR-3; LIV-3, SLI-5.

**REQ-44 — Bounded disk.** [MUST]
The disk use of the persistent backend stays within P-DISK-AMP times the live data plus P-DISK-CONST. Raw rows below the retention margin leave the store.
*Acceptance:* A long tip run (SC-2) keeps raw rows only for the blocks above `D − P-RAW-RETENTION − P-PURGE-STEP`.
*Trace:* ADR-3; RS-1, RS-2, RS-21.

**REQ-45 — Determinism.** [MUST]
The same calls on the same schema give the same batches, apart from the free variables FV-1 to FV-5.
*Acceptance:* Two instances that receive one call history give equal batches modulo the free variables (CT-1).
*Trace:* INV-25.

## Explicitly unspecified

Tests MUST NOT pin these behaviors:

- The order of the tables in a batch, and the order of the records inside one table (FV-1, FV-2).
- The content of the performance tree and its durations (FV-3).
- The text of error messages (FV-4) and the age value of PENDING_ACK (FV-5).
- The order in which Settle sends the tables of one ingest call through the pipeline. Each stage reads one source, so the order has no observable effect.
- The block at which a purge runs inside the margin, within the bound of RS-2.
- The disk layout, the key format and the file names of the persistent backend.
- The thread that runs each stage.
- The treatment of a column name in a row map that the table does not declare, until OQ-1 closes.
- The output of a reducer and of the objects after it, after a body change that breaks the rule of CN-32.

## Open questions

| ID | Question | Blocks | Owner |
|---|---|---|---|
| OQ-1 | Does Settle ignore a column name in a row map that the table does not declare (the current behavior), or refuse it? A typo then loses the column silently. | REQ-27 | Maintainer |
| OQ-3 | When an object comes back to the schema after a removal, does it start empty, load its old state, or does open refuse? | GAP-18 | Maintainer |
| OQ-4 | Does a Base58 value leave Settle as base58 text or as hex text? | GAP-28 | Maintainer |
| OQ-5 | Is the deferral gate for sliding windows permanent, or only a test gap? | ADR-2 | Maintainer |
| OQ-6 | Is an emit whose values are all Null an emit row (DEF-27)? Today it is no row, so a view that counts the rows of a reducer does not count that input row. | DEF-27 | Maintainer |

### Closed

| ID | Question | Answer |
|---|---|---|
| OQ-2 | Does the event-rules runtime stay? | No. ADR-10 removes it, on 2026-10-19. |
