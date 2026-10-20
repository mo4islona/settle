# 11 — Performance

This document states the service level indicators, the objectives, the resource bounds, the workload model and the performance hazards. The baselines come from [BENCHMARKS.md](../BENCHMARKS.md), measured on 2026-10-19 on its reference host, unless a row gives another date. The targets with ⚠ wait for ADR-9.

## Service level indicators

| ID | Indicator | Measurement |
|---|---|---|
| SLI-1 | Ingest throughput | Input rows divided by the time from the first ingest call to the last ack of a run. |
| SLI-2 | Tip latency | The time of one ingest call and its ack, with one block in each call. The median and the 99th percentile. |
| SLI-3 | Ack sync cost | The time of an ack with a sync, minus the time of the same ack without a sync. |
| SLI-4 | Rollback time | The time of a fork call that rolls back `k` blocks with `r` rows. |
| SLI-5 | Open time | The time of an open with a tail of `n` blocks above `D` and `g` stored view groups. |
| SLI-6 | Memory per group | The peak resident memory of a run divided by its groups. |
| SLI-7 | Instructions per row | The instructions of the ingest calls of a case under a CPU simulator, divided by the rows. Also the growth factor when the input size doubles. |
| SLI-8 | Ratio to a Postgres indexer | The total time of a Settle → Postgres indexer divided by the total time of a Postgres-only indexer that makes the same final state. |
| SLI-9 | Reorg ratio | SLI-8 for the reorg workload, with aggregates that have no inverse and Postgres with fork bookkeeping. |

## Objectives

| SLI | Scenario | Target | Baseline (memory / persistent backend) |
|---|---|---|---|
| SLI-1 | SC-1, raw ingestion | P-SLO-RAW-RPS | 2,070K / 741K rows/s |
| SLI-1 | SC-1, full pipeline with Lua | P-SLO-LUA-RPS | 473K / 285K rows/s |
| SLI-1 | SC-4, Polymarket full pipeline | P-SLO-POLY-RPS | 216K / 198K rows/s (±17%) |
| SLI-1 | SC-4, 1M traders | P-SLO-HIGHCARD-RPS | 192K / 189K rows/s |
| SLI-1 | SC-1, backfill deferral | P-SLO-DEFER-GAIN | +45% at 5,000-row calls, +35% at 25,000-row calls (persistent, 2026-06) |
| SLI-2 | SC-6, sliding sum, windows of 10 min / 1 h / 4 h | P-SLO-TIP-LATENCY ⚠ | 122 / 208 / 1,103 µs per call and ack (persistent, 2026-10-16) |
| SLI-3 | SC-2 | P-SLO-ACK-SYNC ⚠ | about 60 µs per ack on NVMe (2026-10-16) |
| SLI-4 | SC-3, 75 blocks with 10,050 rows | P-SLO-ROLLBACK | 2.5 / 2.8 ms |
| SLI-5 | SC-5 | P-SLO-OPEN ⚠ | 0.6 s after a long tip run with the log bound (2026-10-17) |
| SLI-6 | SC-4 | P-SLO-RSS-PER-GROUP ⚠ | not measured (GAP-6) |
| SLI-7 | Scale cases | P-SLO-INSTR-LINEAR | a growth of 2.0 to 2.1 when the input doubles |
| SLI-7 | Each case | P-GATE-INSTR-NOISE | for example 311M instructions (15.6K per row) for a backfill sum view |
| SLI-8 | SC-7, pipelined | P-SLO-PG-RATIO ⚠ | 0.91–0.95 sum for each user, 0.71–0.72 stateful PnL, 1.22 raw rows in serial mode |
| SLI-9 | SC-3, min/max/last, a reorg never / every 50 / every 10 blocks | P-SLO-REORG-RATIO ⚠ | Settle 0.219 / 0.278 / 0.509 s; Postgres with block bookkeeping 1.581 / 2.493 / 6.093 s |

On raw rows with no aggregate, Settle cannot remove work from the target, so SLI-8 is above 1 by design. The SLO for SLI-8 applies to the aggregate workloads only.

## Resource bounds

**PF-1 — Memory ceiling.** The host can compute the ceiling P-MEMORY-CEILING of the resident memory (RS-20) from the configuration and the group count. Today no ceiling exists (GAP-6, ADR-8, HZ-3).

**PF-2 — Backpressure.** The ack queue is the backpressure of Settle (WP-32). P-MAX-UNACKED bounds the number of unacked write sets, but not their size (RP-14). The host bounds their size by the size of its calls (HZ-10).

**PF-3 — Linear work.** The work of a call grows at most linearly in its rows, its blocks, the groups that it changes and the tail of a replay. The patterns include a hot group over many blocks, a bulk expiry of a window, a long replay and many calls of one block (HZ-1).

**PF-4 — Window cost at the tip.** The cost of one block at the tip for a sliding view does not grow with the length of the window. Today it grows linearly with the window, because each expiry refolds the window (GAP-21, HZ-2).

**PF-5 — Maintenance budget.** The compaction of the store uses at most P-MAINT-CPU of the process CPU. It stays on, because without it the stale data grows to many times the live data (RS-21, ADR-3).

**PF-6 — Open work.** The work of an open is linear in the tail above `D` (LIV-3).

**PF-7 — Overlap with the target.** With P-MAX-UNACKED at 2 or more, Settle processes the next call while the target applies the previous batch. The total time of a pipelined run then approaches the time of the slower side.

**PF-8 — Checkpoint cost in backfill.** A checkpoint on each backfill call adds a premium of 1.2 to 1.9 times the tip time. Deferral removes most of it for deferral-safe pipelines, also with external reducers (REQ-16, HZ-7, ADR-17). The finality work stays.

## Workload model

| Parameter | Role |
|---|---|
| P-W-ROWS-PER-BLOCK | Rows in each block. |
| P-W-BLOCKS-PER-CALL | Blocks in each ingest call. |
| P-W-FINALITY-LAG | Blocks between `L` and `F` at the tip. |
| P-W-GROUPS | Distinct group keys. |
| P-W-REORG-EVERY | Blocks between two forks. |
| P-W-WINDOW | Width of a sliding window. |
| P-W-TABLES | Raw tables that receive rows. |

| ID | Scenario | Shape |
|---|---|---|
| SC-1 | Backfill | P-W-BLOCKS-PER-CALL blocks in each call, finality at the tip of each call, an ack after each call. |
| SC-2 | Tip | One block in each call, finality P-W-FINALITY-LAG behind, an ack after each call. |
| SC-3 | Reorg storm | SC-2 with a fork every P-W-REORG-EVERY blocks, with aggregates that have no inverse. |
| SC-4 | High cardinality | SC-1 with P-W-GROUPS at its large values. |
| SC-5 | Cold start | An open with a tail above `D` and stored view groups. |
| SC-6 | Sliding window at the tip | SC-2 with a sliding view at each value of P-W-WINDOW. |
| SC-7 | Settle → Postgres | SC-1 and SC-2 with a Postgres target, in serial and pipelined mode, against a Postgres-only indexer. |
| SC-8 | Soak | SC-2 for hours, with a fixed finality lag, then with a finality that does not move. |

## Hazards

| ID | Mechanism | Threatened property | Probe |
|---|---|---|---|
| HZ-1 | A per-row or per-block step that scans all unfinal blocks, a refold for each moved block, or a replay that reads the tail again. | PF-3, LIV-1 | Scale cases that double the input and assert the growth factor. |
| HZ-2 | A refold of the sliding window after each expiry. | PF-4 | SC-6 at three widths. |
| HZ-3 | Resident groups that grow with the cardinality. | PF-1, LIV-5 | SC-4 with a memory probe. |
| HZ-4 | A finality that stalls, so unfinal data, hashes and raw rows grow. | RS-23 | SC-8 with a finality that does not move. |
| HZ-5 | Rarely written parts of the store that keep old log files. | RS-21, LIV-3 | The log size in SC-8. |
| HZ-6 | Compaction off: open files and stale data grow. | RS-21 | Keep compaction on (ADR-3). |
| HZ-7 | A checkpoint on each backfill call. | SLI-1 | SC-1 with interval 1 against interval N. |
| HZ-8 | Marshal work at a runtime boundary that costs more than the body. | SLI-1 | The same reducer in each runtime. |
| HZ-9 | Many raw tables: each compaction of raw rows rewrites old history. | RS-21 | SC-1 with P-W-TABLES at 2. |
| HZ-10 | Very large calls that keep large write sets in memory. | PF-2 | SC-1 with large P-W-BLOCKS-PER-CALL and a memory probe. |
| HZ-11 | A measurement artifact: a loaded host, a temporary file system without fsync, an untagged database image or a bench without a state check. | Every SLO | The benchmark rules below. |

## Benchmark rules

These rules guard against HZ-11.

- Run each benchmark on the reference host, and check that the host is idle before the run.
- Build the two sides of a comparison in one directory, and run them one after the other in one session.
- Make each benchmark check the final state of its target.
- Keep the benchmark data on a disk with fsync, not on a temporary file system in memory.
- Pin the version of each external database image.
- Measure each reducer runtime with the same reducer logic (HZ-8).
- For an engine change, compare instruction counts first (SLI-7). Wall-clock runs move by 10% to 20% between runs.
- Find the saturation knee of the ack queue with P-MAX-UNACKED from 1 to 5. The baseline: 3 and 5 are within 2% of 2.
- Include an overload phase in which the host does not ack, and check the PENDING_ACK response.
