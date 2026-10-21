# 15 — Parameters

**As of 2026-10-21.** This is a mutable status document. It holds the value of every `P-*` symbol of the suite. "Observed" is the value or the behavior of the code today. "Target" is the intended value. A ⚠ marks a proposed target that waits for its ADR.

## Protocol and configuration

| Parameter | Role | Observed | Target |
|---|---|---|---|
| P-MAX-UNACKED | The places of the ack queue (WP-32, REQ-11). | 2 by default, 1 at least | 2 by default (ADR-1) |
| P-CHECKPOINT-INTERVAL | The blocks between two checkpoints under deferral (WP-40). | 1 by default: no deferral | 1 by default (ADR-2) |
| P-RAW-RETENTION | The retention margin of raw rows (RS-1, RS-2). | 1,000 blocks | 1,000 blocks (ADR-3) |
| P-PURGE-STEP | The step of a purge (RS-2). | max(P-RAW-RETENTION / 10, 1) blocks | The same |
| P-RESIDENT-GROUPS | The groups that a reducer or a view that is not sliding keeps in memory before an eviction (RS-24). | 1,000,000 | ⚠ 1,000,000 (ADR-8) |
| P-KEY-FILTER | The key filter of the persistent store for each stored group (RS-20, RS-24). | 10 bits, about 1.25 bytes | ⚠ The same (ADR-8) |
| P-REPLAY-CHUNK | The blocks with rows for each table in one replay read (RS-12, WP-53). | 64 | 64 |
| P-WAL-BOUND | The largest size of the log of the persistent backend (RS-21). | 256 MiB | 256 MiB (ADR-3) |
| P-ACK-WARN | The age of the oldest unacked batch for the event of OB-8. | 10 s | 10 s |
| P-FORMAT-VERSION | The format version of the database (DEF-35, WP-51). | 1 | 1 until a release stores data (ADR-3) |
| P-BUFFER-SIZE | The inert buffer setting (IB-2). | 10,000, with no effect | Removed ⚠ (ADR-16) |

## Schema limits

| Parameter | Role | Observed | Target |
|---|---|---|---|
| P-MAX-COLUMNS | The columns of a table, and the state fields of a reducer (IB-67). | 65,535 | 65,535 |
| P-MAX-AGGREGATES | The aggregates of a view (IB-67). | 255 | 255 |
| P-MAX-EMIT-COLUMNS | The emit columns of a reducer (DEF-27, IB-67). | 1,024 | 1,024 |
| P-MAX-JSON-DEPTH | The depth of a JS object that the Node binding turns into JSON text (IB-40). | 100 | 100 |

## JS boundary

| Parameter | Role | Observed | Target |
|---|---|---|---|
| P-JS-SAFE-INT | The largest integer that a JS number holds without loss (REQ-33, IB-41). | Rows: 2^53 − 1. Cursors: 2^32 − 1 (GAP-9). | 2^53 − 1 for rows and cursors |

## Resource bounds

| Parameter | Role | Observed | Target |
|---|---|---|---|
| P-MEMORY-CEILING | The ceiling of resident memory (REQ-41, PF-1). | For each reducer and view that is not sliding: P-RESIDENT-GROUPS groups at P-SLO-RSS-PER-GROUP, plus the groups that RS-24 keeps. The key filters add P-KEY-FILTER for each stored group. None in memory mode. | ⚠ The same (ADR-8) |
| P-REDUCER-STEPS | The step budget of one reducer body call (LIV-10). | No bound (GAP-5) | ⚠ Proposed in ADR-8 |
| P-REDUCER-MEMORY | The memory budget of one reducer runtime (LIV-10). | No bound (GAP-5) | ⚠ Proposed in ADR-8 |
| P-DISK-AMP | The factor `A` of the disk bound (RS-21). | Write amplification 1.1 with compaction; 12 to 18 times the live data without it | ⚠ 2, to ratify in ADR-9 |
| P-DISK-CONST | The constant `C` of the disk bound (RS-21). | Not measured | ⚠ P-WAL-BOUND plus the memtables, to ratify in ADR-9 |
| P-MAINT-CPU | The share of process CPU for compaction (PF-5). | Below 1% | ⚠ 5%, to ratify in ADR-9 |

## Service level objectives

The baselines are in [11-performance.md](11-performance.md). The targets with ⚠ wait for ADR-9. The targets without ⚠ are the floors that the throughput bench prints.

| Parameter | Role | Observed | Target |
|---|---|---|---|
| P-SLO-RAW-RPS | SLI-1, raw ingestion. | 2,070K / 741K rows/s | More than 100K rows/s |
| P-SLO-LUA-RPS | SLI-1, full pipeline with Lua. | 473K / 285K rows/s | More than 30K rows/s |
| P-SLO-POLY-RPS | SLI-1, Polymarket full pipeline. | 216K / 198K rows/s | More than 150K rows/s |
| P-SLO-HIGHCARD-RPS | SLI-1, 1M traders. | 192K / 189K rows/s | More than 75K rows/s |
| P-SLO-DEFER-GAIN | SLI-1, backfill deferral against no deferral. | +35% to +45% | ⚠ At least +20%, to ratify in ADR-9 |
| P-SLO-TIP-LATENCY | SLI-2, one tip call and its ack. | 122 µs at a 10-minute window | ⚠ At most 250 µs at a 10-minute window, and no growth with the window (GAP-21) |
| P-SLO-ACK-SYNC | SLI-3. | About 60 µs on NVMe | ⚠ At most 200 µs (ADR-9) |
| P-SLO-ROLLBACK | SLI-4, 75 blocks with 10,050 rows. | 2.5 / 2.8 ms | Less than 10 ms |
| P-SLO-OPEN | SLI-5, the reference tail of SC-5. | 0.6 s | ⚠ At most 2 s (ADR-9) |
| P-SLO-RSS-PER-GROUP | SLI-6. | 358 bytes for each reducer group, 751 with a MAP field, 649 for each view group | ⚠ To ratify in ADR-9 |
| P-SLO-INSTR-LINEAR | SLI-7, the growth when the input doubles. | 2.0 to 2.1 | At most 2.1 |
| P-SLO-PG-RATIO | SLI-8, pipelined, aggregate workloads. | 0.71 to 0.95 | ⚠ At most 1.0 (ADR-9) |
| P-SLO-REORG-RATIO | SLI-9, aggregates with no inverse. | 0.08 to 0.14 | ⚠ At most 0.5 (ADR-9) |

## Merge gates

| Parameter | Role | Observed | Target |
|---|---|---|---|
| P-GATE-TEST-FAILURES | The failing tests that a gate accepts (MG-1, MG-2, MG-5, MG-6, MG-13). | 0 | 0 |
| P-GATE-PROPERTY-COV | The floor of the property coverage (MG-3). | 22 of 94 properties at C (23%). The checker prints the current value, and `tools/coverage-floor` holds the floor. | Never below the last stored value ⚠ (ADR-9) |
| P-GATE-DIFF-COV | The line coverage of changed lines (MG-4). | Not measured | ⚠ 80% (ADR-9) |
| P-GATE-TOTAL-COV | The line coverage of the whole crate (MG-4). | Not measured | ⚠ The first measured value, then only up (ADR-9) |
| P-GATE-INSTR-NOISE | The noise band of an instruction count (MG-7). | +1% soft limit in the bench | +1% |
| P-GATE-WALL-NOISE | The noise band of a wall-clock run (MG-8). | 10% to 20% between runs | ⚠ 20% with three runs (ADR-9) |
| P-GATE-SPEC-ERRORS | The error findings of the checker that MG-9 accepts. | 0 | 0 |
| P-GATE-LINT-WARNINGS | The linter warnings that MG-10 accepts. | No linter in CI | ⚠ 0 (ADR-9) |
| P-GATE-PR-BUDGET | The CI time of the per-PR gates. | Not measured | ⚠ 20 minutes (ADR-9) |
| P-QUARANTINE-DAYS | The expiry of a quarantined test (MG-12). | No policy | ⚠ 14 days (ADR-9) |
| P-ORACLE-SEEDS | The seeds and steps of a CT-1 or CT-2 run. | 300 seeds of 40 steps, 5,000 seeds of 8 steps, 1,000 seeds of 60 steps for the window oracle | The same per PR, ten times more nightly ⚠ (ADR-9) |
| P-FUZZ-BUDGET | The time of a nightly fuzz run for each target (CT-9, REQ-42). | No fuzzer | ⚠ 30 minutes (ADR-9) |

## Workload

| Parameter | Role | Observed | Target |
|---|---|---|---|
| P-W-ROWS-PER-BLOCK | Rows in each block (SC-1 to SC-8). | 50 to 500 in the benches | The same |
| P-W-BLOCKS-PER-CALL | Blocks in each ingest call. | 100 in backfill, 1 at the tip | The same |
| P-W-FINALITY-LAG | Blocks between `L` and `F` at the tip. | 64 | 64 |
| P-W-GROUPS | Distinct group keys. | 100, 10K, 100K, 1M | The same |
| P-W-REORG-EVERY | Blocks between two forks in SC-3. | Never, 50, 10 | The same |
| P-W-WINDOW | Widths of a sliding window in SC-6. | 10 minutes, 1 hour, 4 hours | The same |
| P-W-TABLES | Raw tables with rows. | 1 and 2 | The same |
