# 01 — Overview

## What it is

Settle is a library that a host program embeds. The host reads blocks from a block source and gives each block's rows to Settle in an ingest call. Settle runs the rows through a pipeline that a schema text declares:

- **Raw tables** hold the input rows. A virtual table holds them too, but it sends no rows to the target.
- **Reducers** keep a state for each group of rows and emit rows for the next stage.
- **Materialized views** aggregate the rows of a raw table or a reducer by a group key, with an optional time window.

Each ingest call returns at most one change batch. The batch holds the inserts, updates and deletes that bring the output tables of the target to the new state. The host applies the batch to the target and then acks it. Only an acked batch reaches the disk of Settle.

The hot path is this loop: ingest a range of blocks, apply the returned batch, ack it. At the tip of the chain, a fork can replace recent blocks. Settle then returns a compensation batch that undoes the forked blocks in the target. Blocks at or below the finality watermark never change.

## Actors

| Actor | Role |
|---|---|
| Host | The program that embeds Settle. It reads the source, calls the API, applies each batch to the target and acks it. It is the only caller of Settle. |
| Source | The supplier of blocks, block hashes and finality, for example a data portal or a node. It never calls Settle (NG-2). The host passes its data on. |
| Target | The downstream store that holds the output tables, for example a database. It applies batches. In journal mode it also stores the applied sequence. |
| Schema author | The person who writes the schema text and the reducer bodies. |
| External runtime | Host code that runs the body of an external reducer. Settle calls it for each block. |
| Operator | The person or process that sets the configuration, owns the data directory and changes the schema. |
| Storage medium | The disk under the persistent backend. |

## Design goals

- **G-1 — Exact target under forks.** After the host applies every returned batch, the target equals a full recomputation over the current chain. See REQ-4, INV-20, INV-22.
- **G-2 — Crash safety in step with the ack.** A reopen continues from the last acked batch, or from the applied sequence of the target in journal mode. See REQ-12, REQ-13, CN-20, CN-21.
- **G-3 — Declarative pipelines.** One schema text declares tables, reducers and views. See REQ-1, REQ-20, REQ-23, REQ-24.
- **G-4 — Throughput that keeps one target busy.** Settle overlaps its work with the work of the target, in backfill and at the tip. See REQ-11, REQ-40, SLI-1, SLI-8.
- **G-5 — Exact aggregates that have no inverse.** Min, max, first, last and sliding windows stay exact under a rollback. See REQ-20, REQ-22, INV-24.
- **G-6 — One semantics on every binding.** Rust, Node and browser give the same batches for the same calls. See REQ-31, REQ-32, IB-40.
- **G-7 — Loud failure.** Settle does not lose data silently and does not read stored data with a wrong layout. See REQ-25, REQ-27, INV-28, INV-45.
- **G-8 — Bounded resources.** Memory, disk and open time have bounds that the configuration sets. See REQ-41, REQ-43, REQ-44.

## Non-goals

- **NG-1 — No queries.** Settle does not answer ad hoc reads of the output tables. The target serves the reads.
- **NG-2 — No connections.** Settle does not connect to the source or to the target. The host moves all data.
- **NG-3 — No second writer.** One host thread in one process uses a database at a time. See CN-1 and CN-43.
- **NG-4 — No layout migration.** A changed layout needs a new object name or a new database (ADR-3).
- **NG-5 — No exactly-once delivery without the target.** Outside journal mode, the target applies batches idempotently (ADR-1).
- **NG-6 — No wall clock.** Windows use the timestamps in the data. No output depends on the time of day (CN-41).
- **NG-7 — No rollback below finality.** A fork below the finality watermark is an error, never a deep rollback (ADR-2).
- **NG-8 — No archive.** Raw rows below the retention margin leave the store of Settle. The target keeps its own copy (ADR-3).
- **NG-9 — No browser persistence and no browser performance target.** The browser binding keeps all state in memory.
- **NG-10 — No views over views.** A view reads a raw table or a reducer, and a reducer reads a raw table or a reducer.

## Trust model

| Actor | Settle verifies | Settle trusts | The actor must never cause |
|---|---|---|---|
| Host | The shape of each argument, the block order, the finality order, the ack sequence and the hash conflicts against stored hashes. | That the hashes and the finality come from the source. That it applies batches in sequence order. | A termination of the process, corrupt stored state, or a rollback below finality. |
| Source | Nothing directly. The host passes its claims. | Its finality claims. | A rollback below finality. A conflict below finality is an error (WP-13). |
| Target | In journal mode, the applied sequence against the journal at open (WP-52). | That it applies each batch atomically and in order. | A reopen that silently disagrees with the target. |
| Schema author | The grammar, the references between objects and the stored layouts. | The logic of the reducer bodies. | A termination of the process, unbounded CPU time or unbounded memory. The bounds on CPU time and memory are open deviations (GAP-5). |
| External runtime | The shape of its output and the row index of each emit row. | The values that it returns. | A termination of the process. |
| Operator | The format version and the layouts of the stored data. | Exclusive use of the data directory. | A read of stored data with another layout (INV-45). |
| Storage medium | That each stored value decodes. | That a value that decodes is the value that Settle wrote. Settle keeps no checksums of its own. | A silent default in place of unreadable state (INV-46). |

## Lifecycle at a glance

Dataflow:

```
  source ──► host ──ingest(rows, rollback chain, finalized head)──► Settle
                                                                     │
          raw tables ──► reducers ──► materialized views             │
               │              (state per group)   │                  │
               └────── change records ────────────┘                  │
                                                                     ▼
  target ◄── host applies ◄────────────── change batch (sequence n) ─┘
     │
     └── host calls ack(n) ──► Settle commits the writes of batches ≤ n to disk
```

Block lifecycle: supplied → accepted (unfinalized) → finalized → durable → raw rows purged. An accepted block that is not final can also be rolled back.

Batch lifecycle: returned → applied by the target → acked → committed. A crash before the ack drops the batch from the state of Settle, but not from the target.

Process lifecycle: open (format check → journal reconcile → restore → replay) → healthy → poisoned (only after a failure) → dropped.
