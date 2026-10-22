<!-- This file uses ASD-STE100 Simplified Technical English. Write new text to the same rules. -->

# Benchmarks

All numbers come from release builds with the default features (mimalloc).
The builds ran on 2026-10-22 (master `72ed7dd`) on an idle Linux server with
an AMD EPYC 9455 (48 cores). If the conditions of a section are different,
the section tells you.

| Bench | What it measures | Command |
|---|---|---|
| `vs_postgres_raw`, `vs_postgres_simple_agg`, `vs_postgres_stateful`, `vs_postgres_reorg` | The time of a Settle → Postgres indexer and of a Postgres-only indexer. Both indexers make the same Postgres state. | `cargo bench --bench <name>` (Docker is necessary) |
| `throughput` | Rows per second for typical pipelines, on Memory and on RocksDB | `cargo bench --bench throughput` |
| `solana_dex` | Swaps per second for a PnL pipeline over a synthetic Solana DEX chain, with three layouts of the state for each wallet | See [Map state](#map-state-solana_dex) |
| `resident_memory` | Bytes of memory for each group, and the cost of `max_resident_groups` | `cargo bench --bench resident_memory` (Linux) |
| `instructions` | Instructions and heap allocations for each ingest, under Valgrind. It shows engine changes that are smaller than the wall-clock noise. | See [Instruction counts](#instruction-counts) |
| `tip_rocks` | Ingest and ack at the chain tip on RocksDB, with and without the journal | `cargo bench --bench tip_rocks` |
| `profile_*` | One workload each, for a profiler. This file has no numbers for them. | `samply record cargo bench --bench <name>` |

## Settle vs Postgres

The benches compare two types of indexer. Both types put the same final state
into Postgres:

- **PG only** (`pg_only_*`, `postgres_only`): The indexer writes the raw rows
  to Postgres. Postgres also keeps the aggregates, with upserts.
- **Settle → PG** (`settle_*_then_pg`, `settle_then_postgres`): Settle keeps
  the aggregates. Postgres receives the raw rows and the aggregate rows that
  changed.

The conditions:

- Postgres 18 (the Debian image `postgres:18`) runs in Docker. It uses the
  default settings, but with `autovacuum_naptime=1` (see [Limits](#limits)).
- Settle keeps its state in RocksDB on the same disk.
- The input has 100K rows: 50 rows in each block, 100 blocks in each batch,
  20 batches.
- Each side writes a batch to Postgres in one transaction.
- All variants make the same Postgres state. The bench examines this state
  after each run.
- A PG-only time is the median of three runs. `PgBaseline` keeps these times
  in a cache. A Settle time comes from one run.
- The PG-only times ran on 2026-10-19, the Settle times on 2026-10-22. The
  Postgres side of the benches did not change between these days. The
  Postgres time of the pipelined Settle rows also did not change (0.75–0.78s
  for `simple_agg`, 2.0s for `stateful`).

### Variant names

| Tag | Meaning |
|---|---|
| `rocks` / `mem` | Settle keeps its state in RocksDB on disk (`rocks`) or in memory (`mem`). Only `rocks` is durable, as Postgres is. Thus the tables show only `rocks`. |
| `,pipe` | Pipelined mode. Two batches can wait for their ack at the same time (`max_pending_batches = 2`, the default). Settle ingests batch N+1 while a different thread writes batch N to Postgres. The total time is then approximately the time of the slower side. |
| serial (no `,pipe`) | Settle waits until Postgres writes a batch. Then it ingests the next batch. The total time is the Settle time plus the Postgres time. |
| tip (no mode tag) | Finality does not move. Settle keeps all blocks, because a rollback can occur. |
| `,fin` | Backfill. Each batch is final, and Settle writes a checkpoint of its state after each batch. |
| `,backfill` | The same as `,fin`, but Settle writes the checkpoint only one time in 1,000 blocks. |
| `fn` | A reducer in user code (`LANGUAGE EXTERNAL`). In this bench it is a Rust closure. In the TS binding it is a JS callback. |
| `lua` | A Lua reducer in the schema, with no host code. |
| `mv` | No reducer. A materialized view calculates `sum()` from the raw rows. |

### Summary

All rows in this table use pipelined mode (the default) and Settle on
RocksDB. A negative difference shows that Settle → PG is faster.

| Workload | Mode | PG only | Settle → PG | Difference |
|---|---|---:|---:|---:|
| Sum for each user (`simple_agg`) | backfill (`,backfill`) | 0.850s | 0.783–0.795s | −6% to −8% |
| | tip | 0.850s | 0.764–0.788s | −7% to −10% |
| PnL with 3 projections (`stateful`) | backfill (`,backfill`) | 2.978s | 2.166s | −27% |
| | tip | 2.978s | 2.117s | −29% |
| Raw rows, no aggregates (`raw`), serial | tip | 0.216s | 0.266s | +23% |

In pipelined mode, the total time is approximately the time that Postgres
works. Thus Settle → PG is faster only when Settle removes work from Postgres:

- Sum for each user: Each side writes 200K rows. But Settle sends one row for
  each key, with its new value, so Postgres does no `GROUP BY`. Postgres works
  for 0.75–0.78s, not for 0.85s.
- PnL: For each batch, the PG-only indexer reads back the state of the keys in
  the batch. In total, it reads 160K rows and writes 400K rows. With Settle,
  Postgres reads no rows and writes 330K rows.
- Raw rows: Settle cannot remove work from Postgres here. It only adds its own
  time.

In serial mode, the Settle time adds to the Postgres time. For the sum for
each user, Settle → PG is then 4% to 28% slower than PG only.

### Raw passthrough (`vs_postgres_raw`)

There is no aggregation. Settle keeps the raw rows in RocksDB (a stored
table). Then the rows go to Postgres, in serial mode.

| Variant | Total |
|---|---:|
| `postgres_only` (multi-row INSERT) | 0.216s |
| `settle_only` (no Postgres) | 0.051s |
| `settle_then_postgres` | 0.266s |

### Sum for each user (`vs_postgres_simple_agg`)

The workload has 10K users and calculates `SUM(value)` for each user. The raw
rows go to Postgres directly from the input. Thus Settle reads them as a
`VIRTUAL TABLE`.

There are two PG-only variants:

- `pg_only_batch`, 0.850s: one `unnest` + `GROUP BY` upsert for each batch.
- `pg_only_per_row`, 5.56s: one upsert for each row.

The values in parentheses are the difference from `pg_only_batch`. The last
column shows how long Postgres works in pipelined mode.

| Mode | Reducer | Serial | Pipelined | Postgres time, pipelined |
|---|---|---:|---:|---:|
| tip | `fn` | 0.901s (+6%) | 0.764s (−10%) | 0.745s |
| | `lua` | 0.963s (+13%) | 0.788s (−7%) | 0.767s |
| | `mv` | 0.883s (+4%) | 0.776s (−9%) | 0.760s |
| `,fin` | `fn` | 1.054s (+24%) | 0.810s (−5%) | 0.782s |
| | `lua` | 1.085s (+28%) | 0.806s (−5%) | 0.775s |
| | `mv` | 0.962s (+13%) | 0.796s (−6%) | 0.771s |
| `,backfill` | `fn` | 0.926s (+9%) | 0.795s (−6%) | 0.774s |
| | `lua` | 1.032s (+21%) | 0.791s (−7%) | 0.767s |
| | `mv` | 0.888s (+4%) | 0.783s (−8%) | 0.764s |

- The checkpoint after each batch (`,fin`) adds the most time to `fn` and
  `lua`. With `,backfill`, all three write fewer checkpoints. Before
  2026-10-21, an External reducer could not delay its checkpoint, and the
  `fn` row of `,backfill` was 1.040s (+22%).
- Each Settle row comes from one run. Between two runs, a serial total
  changes by approximately 5%.
- In each batch, no user occurs more than one time (5,000 rows, 5,000 keys).
  Thus Settle cannot merge rows, and Postgres receives the same number of rows
  from the two sides.

### Stateful PnL (`vs_postgres_stateful`)

The workload calculates the cost basis of each position with a moving
average. One `fn` reducer (`position`, GROUP BY user and token) sends its
output to 3 views:

- The position for each (user, token).
- The PnL for each (user, day).
- The total PnL for each user.

The PG-only indexer reads back the state of the keys in the batch
(`WHERE (user, token) IN unnest(...)`, and the same for days). It does the
moving-average calculation in Rust and writes the result with an upsert.
Settle reads `trades` as a `VIRTUAL TABLE`. In `,fin` and `,backfill`, the
finalized head is far above the data, as a source can report it in a
backfill.

| PG-only variant | Total | Rows written | Rows read |
|---|---:|---:|---:|
| `pg_raw_only` (trades only, no views) | 0.673s | 100,000 | 0 |
| `pg_only_smart` | 2.978s | 400,000 | 160,000 |
| `pg_only_smart[x4]` (4 batches in each transaction) | 2.141s | 300,000 | 60,000 |

Settle → PG uses `fn` and `rocks`. The values in parentheses are the
difference from `pg_only_smart`. In the last two columns, they are the
difference from `pg_only_smart[x4]`.

| Mode | Serial | Pipelined | Pipelined, `,q4` | Pipelined, `,x4` |
|---|---:|---:|---:|---:|
| tip | 2.489s (−16%) | 2.117s (−29%) | 1.893s (−12%) | 1.843s (−14%) |
| `,fin` | 2.883s (−3%) | 2.142s (−28%) | 1.952s (−9%) | 1.916s (−11%) |
| `,backfill` | 2.594s (−13%) | 2.166s (−27%) | 1.916s (−11%) | 1.847s (−14%) |
| Rows written | 330,000 | 330,000 | 285,000–295,000 | 280,000 |

- `,q4`: The Postgres thread takes up to 4 batches that wait already. It
  keeps the last record for each key and sends the ack for the newest batch.
  This ack also applies to all earlier batches. The thread does not wait for
  more batches.
- `,x4`: The same as `,q4`, but the Postgres thread waits for 4 batches. Thus
  it commits as frequently as `pg_only_smart[x4]`.
- In serial mode, Settle works 0.38s at the tip, 0.74s with `,fin` and 0.49s
  with `,backfill`. In pipelined mode, Postgres works for longer, so the
  three totals are almost the same.
- The `,backfill` row comes from a second run on the same day. In that run,
  the tip and `,fin` totals are at most 3% from the values above.
- The Postgres time follows the number of rows written. The 100K trades,
  which all variants write, take 0.67s. Each view row adds approximately 6µs.
- When both sides commit one time in 4 batches, PG only becomes 28% faster,
  but Settle only 11% to 13% faster. Thus the lead of Settle decreases from
  28–29% to 9–14%.

### Chain tip with reorgs (`vs_postgres_reorg`)

The input is 2,000 blocks of 50 rows over 200 assets, one block in each step.
After each R blocks, different rows replace the last 6 blocks. Postgres
commits each block. The undo of a reorg and its replacement blocks commit in
one transaction.

**Here, Settle writes nothing to Postgres.** The bench compares Settle alone
with a full Postgres indexer. Thus you cannot compare these numbers with the
workloads above.

`PG_FORKS=off,blocks,undo` (or `all`) selects the data that the Postgres
indexer keeps for the undo of a reorg:

- `off`: No data. An indexer at the chain tip cannot operate without this
  data. Thus `off` is only the lower limit.
- `blocks` (the rindexer method): One row for each block hash that is not
  final. The indexer deletes the rows below finality. It does the undo of its
  aggregates itself: reverse deltas for sum/count, and a new scan of the
  rows for min/max.
- `undo` (the Ponder method): A trigger copies each changed row into an undo
  table. A reorg restores the rows from this table, with no new scan.

A backfill does not keep this data. Thus the other workloads do not use it.

| Aggregate | Reorg | Settle | PG, off | PG + `blocks` | PG + `undo` |
|---|---|---:|---:|---:|---:|
| min/max/last | never | 0.219s | 1.145s | 1.581s | 6.654s |
| | every 50 blocks | 0.269s | 2.099s | 2.493s | 7.260s |
| | every 10 blocks | 0.490s | 5.590s | 6.093s | 11.087s |
| sum/count | never | 0.202s | 1.078s | 1.583s | 6.485s |
| | every 50 blocks | 0.245s | 1.364s | 1.848s | 7.131s |
| | every 10 blocks | 0.439s | 2.073s | 2.769s | 10.393s |

- Without reorgs, the undo log takes approximately 4× the time of the block
  hashes. It runs a trigger for each changed row and five statements for each
  block.
- For min/max, this difference decreases when reorgs occur frequently
  (4.2× → 1.8×). The cause is that a restore from the log does not scan the
  rows again. For sum/count, the reverse deltas stay fast (4.1× → 3.8×).
- Each row in the undo log causes two dead tuples: one from the checkpoint
  update and one from the delete at finality. With the default autovacuum nap
  of 60s, the undo tables grew without a limit during a run. Then the time for
  each block increased approximately ten times.
- The PG sum/count columns show the reverse-delta method. The bench also
  prints a second method, which groups the rows again after the reorg.

### JS reducers

This file has no JS numbers against Postgres. In these benches, `fn` is a
Rust closure in the position where the TS binding puts a JS callback. Thus
the runs do not include the napi call. Through the binding, an External JS
reducer takes 2.45 µs for each row at 100K groups, and a Rust closure takes
2.08 µs (see [History](#history), 2026-10-18). Thus the `fn` rows are the best
estimate for JS, with approximately 20% more Settle time.

### Limits

- **Local Docker**: Postgres uses the local disk with fsync on, on the same
  machine as Settle. On network storage, a commit takes more time. Then fewer,
  larger transactions are better for the two sides.
- **Faster than a chain**: The bench sends approximately 100 blocks per
  second. Thus Postgres runs with `autovacuum_naptime=1`. With the default nap
  of 60s, autovacuum does not start during a case.
- **Postgres build**: The bench uses the Debian image. The alpine (musl) build
  does `ON CONFLICT DO UPDATE` approximately 1.35× slower.
- **No pipeline mode and no cache of prepared statements**: On the two sides,
  each `tokio_postgres::execute` call is a separate round trip.
- **State check**: Each variant examines the final Postgres state. One time,
  a bench without this check showed a broken EventRules pipeline as the
  fastest.

### What these benches do not measure

Settle can do work that these benches do not measure:

1. **Streams to consumers**: Settle sends alerts and events when they occur.
   With Postgres, a consumer must poll.
2. **Rollback after a fork**: Settle does a rollback by block range. A
   Postgres indexer must keep block hashes and do reverse deltas or new scans.
   Or it must keep an undo log of all writes (see the reorg workload).
3. **No Postgres**: RocksDB is the single source of data, and you can query
   the views.
4. **Large state for each key**: For example, sliding windows or feature
   vectors. A Postgres indexer must load and save this state for each batch,
   and this takes most of the time. Settle keeps the state in memory.
5. **Fan-out**: One incremental state supplies data to many consumers.

## Map state (`solana_dex`)

`benches/solana_dex.rs` sends synthetic Solana swaps through three reducers
and five views. Settle keeps its state in RocksDB. The consumer applies each
change batch at once, so the numbers are the time of Settle alone, with no
Postgres. The chain makes approximately 600 swaps per second. Thus 60K swaps/s
is 100× real time.

- `prices` (one group): the USDC price of each token, from the swaps
  themselves, through a chain of pools. It keeps the last price of each token
  in a `Map` field. The other two reducers read their rows from it.
- `pool_prices` (GROUP BY pool): candles, volume for each DEX, pool stats.
- `positions`: average-cost PnL for each wallet and token, in one of three
  layouts (`SDEX_POSITIONS`):
  - `pair`: GROUP BY trader, token. Each group has scalar state.
  - `map`: GROUP BY trader. A `Map(String, …)` field keeps the positions of
    the wallet, with the token as the key.
  - `json`: GROUP BY trader. A `Json` field keeps the same positions. This
    was the only method before `Map` fields.

Modes: `backfill` sends 100 slots in each ingest, with finality at the last
block. `ckpt1000` is the same, but Settle writes its checkpoint only one time
in 1,000 blocks. `tip` sends one slot in each ingest, with finality 32 blocks
back.

With 4,000 slots (965K swaps, 182K wallets, 456K wallet-token pairs), in
swaps per second:

| Runtime | Mode | `pair` | `map` | `map` against `pair` |
|---|---|---:|---:|---:|
| Rust closure (`fn`) | backfill | 69.3K | 60.2K | −13% |
| | backfill, `ckpt1000` | 83.0K | 74.1K | −11% |
| | tip | 45.2K | 36.5K | −19% |
| Lua | backfill | 47.9K | 40.3K | −16% |
| | backfill, `ckpt1000` | 53.9K | 45.8K | −15% |
| | tip | 33.7K | 28.9K | −14% |

- At the tip, one slot takes 5.4–8.5 ms (p50). The chain makes a slot in
  400 ms.
- In a backfill with `fn`, the time is approximately 40% DAG, 34% finality
  and change batches, 27% RocksDB commit.
- Peak RSS with `fn`, backfill: `pair` 1,326 MB, `map` 1,530 MB.

The `json` layout becomes slower as the wallets get more tokens: each block
copies the full map of each wallet that it touches. Thus this table uses
1,000 slots (245K swaps):

| Runtime | Mode | `json` | `map` | `map` against `json` |
|---|---|---:|---:|---:|
| `fn` | backfill | 29.8K | 65.1K | 2.2× |
| | tip | 10.7K | 37.8K | 3.5× |
| Lua | backfill | 10.1K | 42.7K | 4.2× |
| | tip | 4.7K | 28.5K | 6.0× |

- In the backfill, `json` drops from the first quarter of the slots to the
  last: `fn` 58.4K → 20.6K, Lua 21.7K → 5.8K swaps/s. `map` stays in the
  range 58–78K (`fn`) and 39–49K (Lua).
- With 4,000 slots on 2026-10-19, `json` gave 12.7K (`fn`) and 3.1K (Lua)
  in the backfill. A Lua tip slot then took 290 ms (p50).

The Node bench (`bindings/typescript/settle/bench/solana-dex.mjs`) ran on
2026-10-20, before the changes of PR #58 and PR #60. With 4,000 slots and
`map`, an External JS reducer gave 43.1K (backfill) and 28.6K (tip) swaps/s,
and Lua through the binding 37.7K and 25.4K.

## Resident memory (`resident_memory`)

Each case puts N new groups into one node on RocksDB (blocks of 10K keys,
final at once), and then sends the same keys again. Bytes for each group
come from the growth of the peak RSS from 500K to 1M groups. With a cap,
`max_resident_groups` is 100,000. Run on 2026-10-22.

| Shape | Bytes for each group | Peak RSS at 1M groups | Peak RSS, cap 100K | First pass, cap 100K | Second pass, cap 100K |
|---|---:|---:|---:|---:|---:|
| Lua reducer, 2 scalar fields | 353 | 474 MB | 244 MB | +8% | ×1.8 |
| Lua reducer, a `Map` field with 1 key | 747 | 874 MB | 317 MB | +13% | ×2.5 |
| View (`sum`, `count`) | 638 | 774 MB | 282 MB | +22% | ×1.7 |
| Lua reducer → view (`last`, `count`) | 1,026 | 1,093 MB | 366 MB | +20% | ×1.8 |

- The last two columns compare the time with the time without a cap. In the
  first pass, all keys are new. In the second pass, almost each key is not
  in memory, so Settle reads it from RocksDB.
- The default cap is 1,000,000 groups for each reducer and each view.

## Throughput

Command: `cargo bench --bench throughput`. The values are rows per second,
the median of six runs (two sets of three, 20 minutes apart). The ± value is
half the range of the six runs.

| Pipeline | Rows | Memory | RocksDB | Target |
|---|---:|---:|---:|---|
| Raw ingestion | 200K | 2,163K ±3% | 753K ±4% | >100K |
| Raw + MV | 200K | 1,133K ±2% | 597K ±4% | >50K |
| Ingest + persist (Raw + MV) | 100K | 1,395K ±3% | 1,112K ±4% | >20K |
| Full pipeline — Lua | 50K | 457K ±2% | 285K ±4% | >30K |
| Full pipeline — Rust closure | 50K | 775K ±1% | 379K ±5% | >30K |
| Reducer only — Lua | 200K | 751K ±3% | — | >100K |
| Reducer only — Rust closure | 200K | 1,957K ±2% | — | >100K |
| 100K unique group keys (Lua) | 100K | 459K ±2% | 424K ±4% | — |
| Polymarket: market_stats only | 200K | 287K ±1% | 260K ±1% | >160K |
| Polymarket: insider_classifier only | 200K | 484K ±2% | 438K ±3% | >300K |
| Polymarket: full pipeline | 200K | 228K ±12% | 221K ±9% | >150K |
| Polymarket: 1M traders | 500K | 219K ±11% | 216K ±8% | >75K |

A rollback of 75 blocks (10,050 rows) takes 2.7 ms on Memory and 2.4 ms on
RocksDB. The target is less than 10 ms.

The Polymarket insider reducer keeps the positions of a trader in a `Json`
field, not in a `Map` field: it iterates over all positions with `pairs()`,
and a `Map` field has no iteration.

## Instruction counts

`benches/instructions.rs` counts the instructions and heap allocations of
`Settle::ingest` under Valgrind, with
[Gungraun](https://github.com/gungraun/gungraun). Callgrind counts the
instructions. DHAT counts the bytes and the blocks that the code allocates.

For one binary, the allocation counts are the same in each run (for Lua
cases, they change by a few blocks). The instruction counts change by less
than 0.4%. Thus one run of each build is sufficient to compare two builds.
Use this bench for engine changes that are smaller than the noise of the
wall-clock benches.

Requirements:

- Linux.
- `valgrind`, and a runner with the same version as the `gungraun`
  dev-dependency.
- A build without the default features, because DHAT cannot see allocations
  through mimalloc.

A run against a baseline fails with exit code 3 if instructions, bytes or
allocated blocks increase by more than 1%.

These commands install the runner, save a baseline on `master` and compare
the branch `my-work` with it:

```bash
cargo install gungraun-runner --version 0.20.0
F="--no-default-features --features lua,rayon"
git switch master  && cargo bench --bench instructions $F -- --save-baseline=base --parallel=auto
git switch my-work && cargo bench --bench instructions $F -- --baseline=base --parallel=auto
```

Each case writes its profiles to
`target/gungraun/settle/instructions/ingest/<mode>.<workload>/`:

- `callgrind_annotate --inclusive=yes callgrind.<mode>.<workload>.out` shows
  where the instructions go.
- `dhat.<mode>.<workload>.out` opens in Valgrind's `dh_view.html`.

The bench uses only Memory storage. RocksDB flushes and compacts on its own
threads, and the tools do not count these threads. For the same reason, Rayon
runs on the bench thread. The counts use glibc malloc, which uses more
instructions than mimalloc. Thus allocations have a larger effect here than
in production.

### Ingest

Each case ingests a workload in one of two modes. Before the count starts,
the setup ingests the same number of batches. Thus the counted batches start
from a steady state. The count includes `ack` and the drop of each
`ChangeBatch`, as a consumer does them.

| Workload | Pipeline |
|---|---|
| `mv_sum` | A `sum` MV on a virtual table |
| `mv_sum_stored` | The same, on a stored table. A stored table also emits a change record for each raw row. |
| `mv_minmax` | A `min`/`max`/`last` MV over 1,000 assets |
| `lua` | A Lua reducer → a `last` MV |
| `polymarket` | `tests/conformance/ct10_golden/polymarket.sql`: two Lua reducers and their MVs |

| Mode | Each ingest | Finality | Counted |
|---|---|---|---|
| `backfill` | 100 blocks × 50 rows | At the last block of the batch | 4 ingests, 20K rows |
| `tip` | 1 block × 50 rows | 64 blocks behind the tip | 200 ingests, 10K rows |

The `napi_*` cases count the Rust work of the napi binding for one ingest.
The binding decodes the rows from the arrays that `codec.ts` fills. Then it
calls `Settle::ingest_rows` and encodes the change batch for JS. The count
does not include the JS side. The other cases call `Settle::ingest` with rows
by name, as a Rust caller does.

| Case | Instructions | Per row | Bytes | Blocks | Blocks per row |
|---|---:|---:|---:|---:|---:|
| `backfill mv_sum` | 302M | 15.1K | 67.1 MB | 229K | 11.5 |
| `backfill mv_sum_stored` | 358M | 17.9K | 101.7 MB | 290K | 14.5 |
| `backfill mv_minmax` | 181M | 9.0K | 32.2 MB | 97K | 4.8 |
| `backfill lua` | 637M | 31.9K | 92.3 MB | 563K | 28.1 |
| `backfill polymarket` | 1,669M | 83.5K | 203.3 MB | 1,142K | 57.1 |
| `tip mv_sum` | 146M | 14.6K | 29.2 MB | 129K | 12.9 |
| `tip mv_sum_stored` | 171M | 17.1K | 42.2 MB | 161K | 16.1 |
| `tip mv_minmax` | 164M | 16.4K | 33.6 MB | 129K | 12.9 |
| `tip lua` | 319M | 31.9K | 41.7 MB | 297K | 29.7 |
| `tip polymarket` | 829M | 82.9K | 95.3 MB | 589K | 58.9 |
| `napi_backfill mv_sum` | 308M | 15.4K | 73.7 MB | 269K | 13.5 |
| `napi_backfill lua` | 644M | 32.2K | 99.0 MB | 603K | 30.2 |
| `napi_backfill polymarket` | 1,673M | 83.7K | 217.8 MB | 1,202K | 60.1 |
| `napi_tip mv_sum` | 153M | 15.3K | 33.9 MB | 158K | 15.8 |
| `napi_tip polymarket` | 835M | 83.5K | 104.7 MB | 629K | 62.9 |

Changes since the counts of 2026-10-19 (each step measured in one session):

- `polymarket`: +23% allocated blocks and +3.5% instructions from PR #47.
  Since that PR, a Lua reducer with STATE fields gets a new row table for
  each row, because the state can keep the row. The wall-clock time of the
  Polymarket `throughput` cases did not change.
- `mv_minmax` backfill: +4.5% instructions and +14% blocks from PR #60. A
  group that gets rows in more than one block of an ingest moves its blocks
  from the inline slot into a map at each ingest.
- MV cases: −1% to −3% instructions.

### Scale

Each case ingests one batch of N blocks with one row in each block. All rows
are in one group, and finality is at the last block of the batch. For a
linear engine, the count doubles when N doubles.

- `sliding_last` keeps a window that is wider than the batch, so no block
  expires.
- `sliding_jump` calculates `sum` in the same type of window. Then one block
  comes with a timestamp that makes all other blocks expire at the same time.

| Case | 2K blocks | 4K | 8K | When N doubles |
|---|---:|---:|---:|---:|
| `sum` | 33M | 68M | 140M | ×2.0–2.1 |
| `minmax` | 35M | 72M | 148M | ×2.0–2.1 |
| `sliding_last` | 44M | 94M | 193M | ×2.1 |
| `sliding_jump` | 43M | 89M | 182M | ×2.0–2.1 |

### Checkpoint and reopen

In the `checkpoint window_*` cases, a sliding window of one group holds N
blocks, one block per second. Thus each new block makes the oldest block
expire. Each ingest moves finality and writes a checkpoint of the full
window. Thus the count increases with the window size.

| Case | Instructions | Bytes | Blocks |
|---|---:|---:|---:|
| `checkpoint window_1k` (tip) | 27M | 2.4 MB | 18K |
| `checkpoint window_2k` (tip) | 44M | 2.5 MB | 18K |
| `checkpoint window_4k` (tip) | 77M | 2.5 MB | 18K |
| `checkpoint backfill_window_1k` | 24M | 1.6 MB | 13K |
| `checkpoint backfill_window_4k` | 60M | 1.6 MB | 13K |
| `reopen mv_sum_stored` | 260M | 47.1 MB | 260K |

In `reopen mv_sum_stored`, the derived state of the database is durable only
up to its first ingest. Thus `Settle::open` replays 700 blocks (35K rows), and
the case counts this replay. Since 2026-10-19, this count increased by 21%.
The PRs between #48 and #58 added 10%. PR #60 added 10%: a view loads its
groups on demand, with one read for each group.

## History

This table shows past optimizations and their measured effect. Each row
compares the builds before and after one change, on one machine. The machines
and the Postgres versions are different between rows. Thus compare values
only in the same row.

| Date | Change | Measured effect |
|---|---|---|
| 2026-03-10 | Lua state goes in function arguments and return values | Approximately 1.3 µs less for each row, with a state of 6 fields |
| 2026-03-28 | O(1) lookup of branches and MVs, JSON hash with no allocation, fewer clones of group keys | `throughput` on RocksDB +24% to +44% |
| 2026-03-30 | Lua emits only through `emit({...})` | Lua reducer only +50%. Lua full pipeline on RocksDB 3.4× faster. |
| 2026-05-29 | Finalize stops immediately when finality did not move | `profile_polymarket` (finality never moves) 51.05s → 5.62s |
| 2026-05-29 | Finalize writes only the MV groups that changed | `profile_polymarket_realistic` 31.78s → 6.60s |
| 2026-05-30 | FxHash for the dedup key of the change buffer | Before the change, SipHash was 6.9% of the flush profile |
| 2026-05-31 | mimalloc as the global allocator | Throughput +13% to +15% on simple_agg and Polymarket |
| 2026-10-15 | Fewer allocations in the MV engine and in finality | Instructions −9% to −20%, allocated blocks −19% to −43% |
| 2026-10-16 | Rows move into their block buckets, with no clone. Binary codecs for state. | `profile_polymarket_realistic` 76.5K → 111K rows/s. Before the change, the deep clone of rows was 12% inclusive. |
| 2026-10-16 | Incremental fold of aggregates | `scale sum` at 8K blocks 2,076M → 210M instructions. The count is now linear. Before, it increased ×3.5 when N doubled. |
| 2026-10-16 | Timestamp index for sliding windows. No new fold of the group for each expired block. | `sliding_last` at 8K 1,568M → 218M, `sliding_jump` at 8K 1,100M → 161M |
| 2026-10-16 | One MV emit for each ingest | `backfill mv_minmax` −52% instructions, −70% allocated blocks |
| 2026-10-16 | Changes go out in the order in which their keys first occur | Postgres time −3% to −5% in pipelined `simple_agg`. Up to +1.3% instructions. |
| 2026-10-16 | Replay reads 64 blocks of each table at a time | Peak RSS of a replay of 1.05M rows 547 MB → 448 MB |
| 2026-10-16 | Pipelined acks (`max_pending_batches`, default 2) | `simple_agg` total 1.14–1.63× → 1.04–1.11× of PG only (on Postgres 11) |
| 2026-10-18 | Rows by position from the input to the views | `msgpack_backfill mv_sum` −25.7%, `reopen` −47.4% instructions. `throughput` time −3% to −15%. |
| 2026-10-18 | Reducer state and emits by position | `backfill event_rules` −11.7%, `backfill polymarket` −14.3% instructions |
| 2026-10-18 | Binary bridge for the TS binding | External JS reducer through napi: 3.31 → 0.89 µs for each row at 100 groups, 9.22 → 4.20 at 100K groups. This is now approximately the time of EventRules. |
| 2026-10-18 | Reducer groups in a slab. Finality on the thread that processed the node. | At 100K groups, µs for each row: EventRules 3.27 → 2.10, Rust closure 3.67 → 2.08, External JS through napi 4.15 → 2.45 |
| 2026-10-18 | No state read for a new group when the database has no reducer state | RocksDB, 100K groups: +6% to +8% |
| 2026-10-19 | A Lua reducer converts the state of a group one time for each block, not for each row | Solana DEX, JSON layout, Lua backfill: 1,526 → 3,103 swaps/s |
| 2026-10-20 | `Map` state fields | Solana DEX, 1,000 slots: `map` is 2.2× (`fn` backfill) to 6.0× (Lua tip) faster than `json`. With 4,000 slots, `map` is 11% to 19% slower than `pair`. |
| 2026-10-21 | External reducers delay their checkpoint in a backfill too (ADR-17) | Solana DEX bench, 4,000 slots: Rust closures +13% and JS +14% at 1,000 blocks; +23% and +17% with 10 slots in each call at 100 blocks. `simple_agg` serial `fn,backfill` 1.040s → 0.942s |
| 2026-10-22 | Groups leave memory above `max_resident_groups`. One unfinal block stays inline in each group. | Solana DEX: peak RSS 3,030 → 1,320 MB, swaps/s +5% to +6%. `throughput` reducer only (Rust closure) −5%, rollback on Memory 2.3 → 2.7 ms. |
| 2026-10-22 | The checkpoint interval counts blocks of data, not blocks of finality (ADR-18) | `stateful` serial `,backfill`, finalized head far above the data: Settle time 0.836s → 0.486s, total 2.985s → 2.594s |
