# Benchmarks

Run: `cargo bench --bench throughput`

Hardware: Apple M2 Max

Last updated: 2026-03-30

## Results

### Memory Backend

| Benchmark | Rows | rows/s | Target | Status |
|-----------|------|--------|--------|--------|
| Raw ingestion | 200K | 735K | >100K | PASS |
| Raw + MV | 200K | 238K | >50K | PASS |
| Full pipeline — Event Rules | 100K | 90K | >50K | PASS |
| Full pipeline — Lua | 50K | 502K | >30K | PASS |
| Full pipeline — FnReducer | 50K | 196K | >30K | PASS |
| Reducer-only — Event Rules | 200K | 913K | >200K | PASS |
| Reducer-only — Lua | 200K | 821K | >100K | PASS |
| Reducer-only — FnReducer | 200K | 1219K | >100K | PASS |
| Polymarket: market_stats | 200K | 393K | >160K | PASS |
| Polymarket: insider_classifier | 200K | 285K | >300K | FAIL |
| Polymarket: full pipeline | 200K | 140K | >150K | FAIL |
| Polymarket: 1M traders | 500K | 113K | >75K | PASS |

### RocksDB Backend

| Benchmark | Rows | rows/s | Target | Status |
|-----------|------|--------|--------|--------|
| Raw ingestion | 200K | 681K | >100K | PASS |
| Raw + MV | 200K | 214K | >50K | PASS |
| Full pipeline — Event Rules | 100K | 77K | >50K | PASS |
| Full pipeline — Lua | 50K | 419K | >30K | PASS |
| Full pipeline — FnReducer | 50K | 141K | >30K | PASS |
| Rollback 75 blocks (10050 rows) | 10K | 1232K | <10ms | PASS |
| Ingest + persist (Raw + MV) | 100K | 542K | >20K | PASS |
| 100K unique group keys | 100K | 322K | baseline | PASS |
| Polymarket: market_stats | 200K | 383K | >160K | PASS |
| Polymarket: insider_classifier | 200K | 268K | >300K | FAIL |
| Polymarket: full pipeline | 200K | 136K | >150K | FAIL |
| Polymarket: 1M traders | 500K | 119K | >75K | PASS |

> ⚠️ Числа выше получены из других бенчей (вероятно `vs_postgres_stateful` или `profile_polymarket` с устаревшей конфигурацией). **Свежий `profile_polymarket` после rebuild с debug symbols даёт 9-15K rows/s** — см. секцию ниже. Не использовать таблицу выше как baseline для tier-by-tier сравнения; для этого — «Optimization Roadmap Baseline (2026-05-29)».

## Instruction counts (`instructions`)

`benches/instructions.rs` counts the instructions and heap allocations of
`Settle::ingest` under Valgrind with [Gungraun](https://github.com/gungraun/gungraun):
Callgrind for instructions, DHAT for bytes and blocks allocated. Between runs of
one binary, allocation counts repeat exactly (Lua cases to a few blocks) and
instruction counts move by less than 0.4%, so one run of each build compares two
builds. Use it for engine changes smaller than the ±10–20% noise of the
wall-clock benches.

It runs on Linux and needs `valgrind` and the runner of the same version as the
`gungraun` dev-dependency. DHAT cannot see allocations made through mimalloc, so
the bench builds without the default features. A run against a baseline fails
with exit code 3 when instructions, bytes or blocks allocated grow by more than 1%.

```bash
cargo install gungraun-runner --version 0.20.0
F="--no-default-features --features lua,rayon"
git switch master  && cargo bench --bench instructions $F -- --save-baseline=base --parallel=auto
git switch my-work && cargo bench --bench instructions $F -- --baseline=base --parallel=auto
```

Each case is a workload ingested in one of two modes. Setup ingests as many
batches again first, outside the count, so the counted batches see a steady
state. The count includes `ack` and the drop of each `ChangeBatch`, as a
consumer would do them.

| Workload | Pipeline |
|---|---|
| `mv_sum` | `sum` MV on a virtual table |
| `mv_sum_stored` | the same on a stored table, which also emits a change record per raw row |
| `mv_minmax` | `min`/`max`/`last` MV |
| `event_rules` | EventRules reducer → `last` MV |
| `polymarket` | `tests/polymarket/schema.sql`: two Lua reducers and their MVs |

| Mode | Ingest | Finality | Counted |
|---|---|---|---|
| `backfill` | 100 blocks × 50 rows | at the batch tip | 4 ingests, 20K rows |
| `tip` | 1 block × 50 rows | 64 blocks behind | 200 ingests, 10K rows |

Memory storage only: RocksDB flushes and compacts on its own threads, which
neither tool counts. Rayon runs on the bench thread for the same reason. The
counts use glibc malloc, which costs more instructions than mimalloc, so
allocation weighs more here than in production.

Numbers on 2026-10-15, after the MV allocation cuts. The first baseline of the
same day had 9–20% more instructions and 19–43% more allocated blocks.

| Case | Instructions | per row | Bytes | Blocks | per row |
|---|---:|---:|---:|---:|---:|
| `backfill mv_sum` | 452M | 22.6K | 87.0 MB | 415K | 20.7 |
| `backfill mv_sum_stored` | 600M | 30.0K | 140.6 MB | 636K | 31.8 |
| `backfill mv_minmax` | 459M | 23.0K | 79.8 MB | 400K | 20.0 |
| `backfill event_rules` | 789M | 39.5K | 126.4 MB | 824K | 41.2 |
| `backfill polymarket` | 2,456M | 122.8K | 255.8 MB | 1,661K | 83.1 |
| `tip mv_sum` | 230M | 23.0K | 39.7 MB | 221K | 22.1 |
| `tip mv_sum_stored` | 297M | 29.7K | 62.3 MB | 332K | 33.2 |
| `tip mv_minmax` | 291M | 29.1K | 48.6 MB | 261K | 26.1 |
| `tip event_rules` | 392M | 39.2K | 62.1 MB | 428K | 42.8 |
| `tip polymarket` | 1,221M | 122.1K | 121.6 MB | 849K | 84.9 |

The running fold of the aggregates (2026-10-16) moved these cases by −0.2% to
+0.4%, within the spread between two builds. One MV emit per ingest (the same
day) then cut them by 0.2–1.6%, and `backfill mv_minmax` by 52% of instructions
and 70% of allocated blocks: its 1,000 assets get about five rows per ingest, and
each row used to build a change record that the change buffer merged away.

### Scale cases

The `scaling` group ingests one batch of N blocks with one row each, all in one
group, with finality at the batch tip. A linear engine doubles the count when N
doubles. Before the running fold, `sum` and `min`/`max` read every unfinalized
block of the group on each block. `sliding_last` keeps a window wider than the
batch, so no block expires; before the timestamp index of the window, each
block scanned all the block times of the window. The last row of each case has
one MV emit per ingest, instead of one change record per block.

| Case | 2K blocks | 4K | 8K | Per doubling |
|---|---:|---:|---:|---:|
| `sum`, before the running fold | 169M | 571M | 2,076M | ×3.4–3.6 |
| `sum`, running fold | 52M | 105M | 210M | ×2.0 |
| `sum` | 30M | 58M | 121M | ×2.0 |
| `minmax`, before the running fold | 406M | 1,550M | 6,058M | ×3.8–3.9 |
| `minmax`, running fold | 36M | 74M | 151M | ×2.0 |
| `minmax` | 32M | 65M | 133M | ×2.0 |
| `sliding_last`, before the timestamp index | 137M | 445M | 1,568M | ×3.2–3.5 |
| `sliding_last`, timestamp index | 53M | 109M | 218M | ×2.0 |
| `sliding_last` | 30M | 63M | 127M | ×2.0 |

### Reopen

`recovery reopen` counts `Settle::open` of a database whose derived state is
durable only up to its first ingest: setup ingests the `backfill` inputs of
`mv_sum_stored` with persist deferral, so the open replays 700 blocks, 35K rows.
DHAT cannot show the peak heap of the replay here: the peak of the process falls
in setup.

Peak RSS of a reopen that replays 2,000 blocks, 1.05M rows (RocksDB, `sum` MV on
a stored table, a probe that is not in the repository), 2026-10-16:

| Replay reads | Peak RSS | Open |
|---|---:|---:|
| the whole tail at once | 547 MB | 0.71–0.82 s |
| 64 blocks per table at a time | 448 MB | 0.72–0.74 s |

Most of the rest is the state that the replay rebuilds: the replayed blocks stay
unfinalized until the next ingest brings the finality, so the MV holds per-block
data for the whole tail.

Each case leaves its profiles under `target/gungraun/settle/instructions/ingest/<mode>.<workload>/`.
`callgrind_annotate --inclusive=yes callgrind.<mode>.<workload>.out` shows where
the instructions go; `dhat.<mode>.<workload>.out` opens in Valgrind's `dh_view.html`.

## Optimization Roadmap Baseline (2026-05-29)

Точка отсчёта для tier-by-tier сравнения roadmap'а в `~/.claude/plans/optimized-coalescing-nova.md` (раздел «ИТОГОВЫЙ ROADMAP»).

### Зачем нужен этот baseline

Текущие OPTIMIZATION.md §0.7 оценки оказались спекулятивны (VA+П.2 в `stash@{0}` дали break-even вместо заявленных 2.3×). Flamegraph (samply) на двух конфигурациях `profile_polymarket` показал, что доминирует `MVEngine::finalize` (49-66% inclusive), а не bookkeeping ChangeRecord (1.5% self / 11% inclusive). Этот baseline — фундамент для проверки каждого tier'а на реальные multipliers.

### Конфигурация измерения

- **Schema**: `tests/polymarket/schema.sql` (множество MV, External reducer)
- **Workload**: 500K rows, 100K traders, ~10K assets, batch=500, 5K rows warmup
- **Backend**: Memory (RocksDB не используется в `profile_polymarket`)
- **Compiler**: `CARGO_PROFILE_RELEASE_DEBUG=true cargo build --release` (debug symbols для symbolication; добавляет ~5% overhead vs no-debug)
- **Hardware**: Apple Silicon, single user-thread main + rayon worker pool
- **Variance**: typically 1-2% (median of 2 runs ниже)

### Wall-clock baseline

| Bench (495K profile rows) | Run 1 | Run 2 | Median wall | Throughput |
|---|---:|---:|---:|---:|
| `profile_polymarket` (finalize_head=0 forever — текущий `ingest_one` pattern) | 50.76s | 51.34s | **51.05s** | **9.7K rows/s** |
| `profile_polymarket_realistic` (finalize_head=block−32, Ethereum-style finality lag) | 32.03s | 31.53s | **31.78s** | **15.6K rows/s** |
| Δ realistic / original | — | — | **−37.7%** | **+60%** |

Realistic быстрее **без единой строки кода** — текущий `engine.finalize(0)` на каждый ingest делает no-op work + serialize всех групп. Это 37% времени wasted.

### Flamegraph attribution (top-N by self time)

`profile_polymarket` original (54s run, samply 999Hz):

| % self | Function | Категория |
|---:|---|---|
| 17.64 | `__psynch_cvwait` | rayon idle |
| 8.60 | `BTreeMap::clone_subtree<u64, NumAccum>` | snapshot clone |
| 6.13 | `_xzm_free` | malloc/free |
| 5.03 | `_xzm_xzone_malloc_tiny` | malloc/free |
| 4.42 | `Value::clone` | clone |
| 4.29 | `BTreeMap::clone_subtree<u64, Value>` | snapshot clone |

`profile_polymarket_realistic` (33s run, samply 999Hz):

| % self | Function | Категория |
|---:|---|---|
| 25.39 | `__psynch_cvwait` | rayon idle ↑ |
| 7.04 | `_xzm_free` | malloc/free |
| 5.01 | `rmp::encode::uint::write_uint` | serialize ↑ |
| 3.01 | `_platform_memcmp` | mem ops |
| 2.45 | `BTreeMap<(String, Vec<u8>), Vec<u8>>::insert` | storage batch |
| 2.40 | `BTreeMap::clone_subtree<u64, NumAccum>` | snapshot ↓ |
| 2.26 | `rmp::encode::uint::write_u8` | serialize |
| 1.85 | `SumAgg::finalize_up_to` | aggregation |

### Categorized share (для tracking по tier'ам)

| Категория (matched в `/tmp/parse_samply_profile.py`) | Original self | Realistic self | Original incl | Realistic incl |
|---|---:|---:|---:|---:|
| `MVEngine::finalize` (inclusive only) | — | — | **65.6%** | **49.4%** |
| `rmp_serde::*` + `rmp::encode::*` (serialize) | ~6% | **15.0%** | 40.6% | 85.0% |
| `BTreeMap::clone_subtree` (snapshot) | ~13% | ~3.7% | — | — |
| `__psynch_cvwait` + rayon `wait_until_cold` (idle) | 17.6% / 23.5% | **25.4%** / 31.5% | — | — |
| `_xzm_*` malloc/free | ~12% | ~10% | — | — |
| Bookkeeping (compute_output + build_change_key + ChangeRecord HashMap) | 0.95% | 1.46% | 9.97% | 11.26% |
| Input (compute_group_key + agg feed + Row::get) | 2.39% | 4.77% | 106%* | 54.3% |

\* >100% inclusive из-за overcount (одна функция в нескольких stacks). Self time — единственный точный метрик.

### Целевые метрики для tier-by-tier tracking

После каждого tier'а замерять **median of 2 runs** на обоих конфигурациях и сравнивать с baseline:

1. **Wall (original config)** — должно падать к 31.8s после T1 (early-return в `engine.finalize`)
2. **Wall (realistic config)** — должно падать после T2 (dirty-tracking finalize), T3 (inline serialize)
3. **`MVEngine::finalize` inclusive %** на realistic — должно падать с 49.4% до ≤25% после T2, до ≤15% после T3
4. **`rmp_serde::*` self %** на realistic — должно падать с 15% после T3
5. **`__psynch_cvwait` self %** на realistic — должно падать до <5% после T4 (rayon fix)
6. **`BTreeMap::clone_subtree` self %** — должно исчезнуть из top-25 после T5

### Воспроизведение

```sh
# Build (with debug symbols for symbolication)
CARGO_PROFILE_RELEASE_DEBUG=true cargo build --release \
  --bench profile_polymarket --bench profile_polymarket_realistic

# Wall-clock measurement (median of 2)
BIN_ORIG=$(ls -t target/release/deps/profile_polymarket-* | grep -v '\.' | head -1)
BIN_REAL=$(ls -t target/release/deps/profile_polymarket_realistic-* | grep -v '\.' | head -1)
time "$BIN_ORIG"; time "$BIN_ORIG"
time "$BIN_REAL"; time "$BIN_REAL"

# Flamegraph (samply, macOS-friendly, no sudo)
samply record --save-only --no-open --unstable-presymbolicate \
  -o /tmp/profile_polymarket.json.gz -- "$BIN_ORIG"
samply record --save-only --no-open --unstable-presymbolicate \
  -o /tmp/profile_polymarket_realistic.json.gz -- "$BIN_REAL"

# Attribution analysis (top-N self + categorized %)
python3 /tmp/parse_samply_profile.py /tmp/profile_polymarket.json.gz
python3 /tmp/parse_samply_profile.py /tmp/profile_polymarket_realistic.json.gz

# Visual exploration in Firefox Profiler (optional)
samply load /tmp/profile_polymarket.json.gz
```

### Tier-by-tier результаты (заполнять по мере реализации)

| Tier | Дата | Original wall | Realistic wall | Original Δ | Realistic Δ | `MV::finalize` incl (realistic) | `rmp_serde` self (realistic) | `cvwait` self (realistic) | Заметки |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| **Baseline** | 2026-05-29 | **51.05s** | **31.78s** | — | — | **49.4%** | **15.0%** | **25.4%** | wasted finalize(0); см. roadmap |
| **T1** (`finalize` early-return) | 2026-05-29 | **5.62s** | **31.54s** | **−89% / 9.1×** | ≈0 (−0.7%) | 49.8% | 15.0% | 25.3% | original синтетический; realistic = real worst case теперь |
| **T2** (dirty-tracking) | 2026-05-29 | **5.37s** | **6.60s** | ≈0 | **−79% / 4.8×** | **исчез из top-15** | **0.86%** | 58.4% | rayon idle стал доминантой |
| T3 (inline serialize) | — | — | — | — | target −5..−10% | target ≤15% | target ≤7% | — | — |
| T4 (rayon fix) | — | — | — | — | target −30..−50% | — | — | target <5% | — |
| T5 (im-rc snapshots) | — | — | — | — | target −5..−10% | — | — | — | повторный flamegraph |

### T1 наблюдения (2026-05-29)

**Изменение:** `SettleEngine::finalize` (`src/engine/dag.rs:959`) теперь делает early-return когда `block == self.finalized_block && self.has_finalized`. Добавлен bool flag `has_finalized` (init false, set true в `set_finalized_block` для restore-path и в конце `finalize` после работы). 311 тестов библиотеки прошли.

**Wall-clock (median of 2 runs):**

| Bench | Baseline | T1 | Δ |
|---|---:|---:|---:|
| `profile_polymarket` (original) | 51.05s | **5.62s** | **−89% (9.1×)** |
| `profile_polymarket_realistic` | 31.78s | **31.54s** | −0.7% (noise) |

Original раньше 1000 раз вызывал `engine.finalize(0)` через `ingest_one` (`test_helpers.rs:55-63` всегда передаёт текущий `finalized_block()` = 0). После T1 — все эти calls short-circuit'ятся. Realistic не задело: `finalized_head=block−32` двигается каждый ingest → каждый `engine.finalize(N)` идёт с новым N → нет skip.

**Flamegraph attribution на T1-original (теперь синтетический):**

| % self | Function | Notes |
|---:|---|---|
| 58.67 | `__psynch_cvwait` | rayon workers idle (главный thread теперь очень короткий) |
| 4.27 | `_xzm_free` | malloc/free |
| 3.28 | `BTreeMap::Iter<u64, NumAccum>::next` | aggregate iteration |
| 2.55 | `Sip13::Hasher::write` | hash для group key |
| `MV::finalize` | — | **исчез из top-25** |
| `rmp_serde::*` | 0.01% | **серigure близко к нулю** |

Original теперь меряет «всё кроме finalize» — meaningless для tier-by-tier выбора. Дальше работаем только с realistic.

**Flamegraph attribution на T1-realistic (без изменений vs baseline):**

| % self | Baseline | T1 | Δ |
|---|---:|---:|---:|
| `MV::finalize` inclusive | 49.4% | 49.8% | +0.4pp (noise) |
| `rmp_serde::*` self | 15.0% | 15.0% | 0 |
| `__psynch_cvwait` self | 25.4% | 25.3% | 0 |
| `BTreeMap::clone_subtree<NumAccum>` self | 2.4% | 2.4% | 0 |
| Bookkeeping self | 1.5% | 1.4% | 0 |

Подтверждение что T1 не оптимизирует когда finality реально движется — на realistic engine.finalize выполняет real work каждый раз, и эту work надо снижать через T2 (dirty-tracking) и T3 (inline serialize).

**Production implication:** T1 даёт speedup пропорциональный доле heartbeat ingest'ов (без сдвига finalized_head). Если consumer всегда продвигает finality (каждый block) — speedup ≈ 0. Если часто heartbeat'ит (мониторинг, watermarks без новых finalized данных) — proportional.

**Bonus:** T1 fix'ит производственный bug — heartbeat ingest без новых данных раньше перезаписывал все group state на disk, теперь skip'ает.

### T2 наблюдения (2026-05-29)

**Изменение:** В `MVEngine` (`src/engine/mv.rs`) добавлено поле `dirty_groups: FxHashSet<GroupKey>`. Заполняется в `process_block` и `rollback` после `emit_changes` (через `self.dirty_groups.extend(touched_keys)`). В `finalize` теперь `mem::take(&mut self.dirty_groups)` → iterate ONLY dirty для `finalize_up_to` и `put_mv_state`. Untouched groups оставляются на диске as-is. 311 тестов библиотеки прошли без изменений.

**Wall-clock (median of 2 runs):**

| Bench | T1 | T2 | Δ |
|---|---:|---:|---:|
| `profile_polymarket` (original) | 5.62s | **5.37s** | ≈0 (noise; на original finalize не вызывается вообще после T1) |
| `profile_polymarket_realistic` | 31.54s | **6.60s** | **−79% (4.8×)** |

Гипотеза подтверждена: на Polymarket-like нагрузке per-batch tронуто ~500 уникальных группы (batch=500 rows), а total groups ~10K+ (assets × traders subset). Persist'ить все 10K вместо 500 = ~20× wasted work. После T2 цена finalize пропорциональна реальному изменению, не общему числу групп.

**Realistic теперь ~равен original** — обе версии бенча сходятся к одному уровню (5-7s), потому что для обоих finalize теперь делает только needed work.

**Flamegraph attribution на T2-realistic:**

| % self | Function | vs T1 baseline |
|---:|---|---|
| 58.42 | `__psynch_cvwait` | +33pp (доминанта; рабочие thread'ы простаивают) |
| 3.78 | `_xzm_free` | −3pp |
| 2.38 | `Sip13::Hasher::write` | +1pp |
| 1.97 | `_platform_memcmp` | ≈0 |
| 0.86 | `rmp::encode::*` (sum) | **−14pp** |
| `MV::finalize` | вне top-15 inclusive | **−49pp inclusive** |
| `BTreeMap::clone_subtree` | вне top-25 | −2pp |

**Категории self-time:**

| Категория | T1 | T2 | Δ |
|---|---:|---:|---:|
| serialize | 15.0% | **1.9%** | −13.1pp |
| input | 5.2% | 0.6% | −4.6pp |
| storage | 0.6% | 0.2% | −0.4pp |
| bookkeeping | 1.4% | 3.0% | +1.6pp (относительный рост, абсолютный baseline) |

**Следующий приоритет:** `__psynch_cvwait` 58.4% self + rayon `wait_until_cold` 70.9% inclusive показывают что workers сидят без работы. T3 (inline serialize) теперь даст ≤2% — почти ничего. **Следующий tier — T4 (investigate rayon parallelism)**, потенциально biggest win (+50-100%). Перепрыгнуть T3.

**Также:** main thread inclusive 29% (vs 68% на T1). Это значит **главный thread теперь делает ~30% wall time, остальное rayon overhead/wait**. Если parallelism исправить — main thread полностью насытится.

## History

### Measurements behind hot-path choices

Moved here from code comments, which keep only the reason.

| Choice | Measured effect |
|--------|-----------------|
| mimalloc as the global allocator | +13-15% throughput on simple_agg and polymarket |
| FxHash for the change-buffer dedup key | SipHash (`Sip13::write`) was ~6.9% of the flush profile |
| Ingest rows moved into per-block buckets, not cloned | The deep `RowMap` clone was 12% inclusive in the flamegraph |
| Lua state passed as function arguments and returns | ~1.3 µs/row saved for a 6-field state |

### Lua emit optimization (2026-03-30)

Removed `emit.field = val` syntax, only `emit({...})` call.
Eliminated per-call `pairs()` copy and emit table clearing.

| Benchmark | Before | After | Change |
|-----------|--------|-------|-------|
| Reducer-only — Lua [Memory] | 548K | 821K | **+50%** |
| Full pipeline — Lua [RocksDB] | 125K | 419K | **+235%** |
| Polymarket: market_stats [RocksDB] | 151K | 383K | **+154%** |

### Performance fixes batch (2026-03-28)

O(1) branch/MV lookup, JSON hashing without allocation, GroupKey clone reduction.

| Benchmark | Before | After | Change |
|-----------|--------|-------|-------|
| Raw + MV [RocksDB] | 196K | 283K | **+44%** |
| Full pipeline — Event Rules [RocksDB] | 86K | 117K | **+36%** |
| Full pipeline — FnReducer [RocksDB] | 145K | 187K | **+29%** |
| Full pipeline — Lua [RocksDB] | 126K | 156K | **+24%** |

### Initial baseline (2026-03-28)

First measurement after code review fixes.

| Benchmark | rows/s |
|-----------|--------|
| Raw ingestion [RocksDB] | 631K |
| Raw + MV [RocksDB] | 197K |
| Full pipeline — Event Rules [RocksDB] | 86K |
| Full pipeline — Lua [RocksDB] | 126K |
| Polymarket: full pipeline [RocksDB] | 139K |

---

## Settle vs Postgres

Run: `cargo bench --bench vs_postgres_raw|simple_agg|stateful` (Docker required)

Three workloads comparing Settle pipelines against a Postgres baseline that
receives the same input batches and ends with equivalent state. Hardware:
Apple M2 Max + Docker Desktop (PG 16 on overlay2). Last updated: 2026-05-19.

**Bench config** (`benches/common/mod.rs`): 100K rows, 50 rows/block, 100 blocks/batch (20 batches).

Both pipelines run with per-batch transactions, multi-row INSERT/UPSERT, correctness checks comparing PG state across all variants.

### Workload 1 — raw passthrough (`vs_postgres_raw`)

No aggregation. Pure throughput of pushing rows.

| Variant | Wall time | rows/s | Notes |
|---|---|---|---|
| `pg_only` | 0.42–0.53s | 190K–240K | multi-row INSERT |
| `settle_then_postgres` | 0.54–0.81s | 125K–185K | Settle ingest (RocksDB WAL) + INSERT to PG |

**Finding**: Settle overhead for forwarding data into Postgres is ~30–80% slowdown vs pg-only. Honest cost of "Settle as durable buffer in front of PG".

### Workload 2 — simple aggregation (per-user balance, `vs_postgres_simple_agg`)

10K users, `SUM(value)` per user. Both sides write the same 200K rows: 100K raw
rows with a multi-row INSERT and 100K balance upserts through `unnest`. Raw rows
go to PG straight from the input, so Settle reads them as a `VIRTUAL TABLE`.

Updated 2026-10-15 on an idle Linux x86-64 server, PG 16 in Docker; mean of two
runs. Before this update, Settle kept a stored copy of the raw rows and sent the
balances as a multi-row `VALUES` upsert, which cost it 15–17% of the total.

| Variant | Total | settle | pg | rows/s | Notes |
|---|---:|---:|---:|---:|---|
| `pg_only_per_row` | 9.3s | — | 9.3s | 11K | `INSERT ... ON CONFLICT DO UPDATE` per row |
| `pg_only_batch` | 0.736s | — | 0.736s | 136K | `unnest` + `GROUP BY` upsert |
| `settle_mv[mem]` | 0.851s | 0.139s | 0.713s | 118K | MV `sum()` on the raw table, no reducer |
| `settle_mv[rocks]` | 0.876s | 0.155s | 0.720s | 114K | |
| `settle_fn[mem]` | 0.933s | 0.220s | 0.713s | 107K | FnReducer → `last()` MV |
| `settle_fn[rocks]` | 0.989s | 0.253s | 0.736s | 101K | |
| `settle_er[mem]` | 0.995s | 0.271s | 0.725s | 100K | EventRules → `last()` MV |
| `settle_er[rocks]` | 1.007s | 0.270s | 0.737s | 99K | |
| `settle_mv[rocks,fin]` | 0.934s | 0.224s | 0.710s | 107K | finality at each batch tip |
| `settle_mv[rocks,backfill]` | 0.917s | 0.184s | 0.733s | 109K | `,fin` + persist deferral |

**Pipelined acks** (`,pipe` rows, 2026-10-16, same server, mean of two runs).
With two unacked batches (`max_pending_batches`, default 2), Settle ingests
batch N+1 while another thread applies batch N to PG and sends back its
sequence for `ack`. The serial rows set it to 1. In this run `pg_only_batch`
took 0.734s.

| Variant | Serial total | Pipelined total | vs `pg_only_batch` | PG busy |
|---|---:|---:|---:|---:|
| `settle_mv[mem]` | 0.853s | 0.774s | 1.16× → 1.05× | 0.713s |
| `settle_mv[rocks]` | 0.837s | 0.763s | 1.14× → 1.04× | 0.707s |
| `settle_fn[mem]` | 0.904s | 0.784s | 1.23× → 1.07× | 0.714s |
| `settle_fn[rocks]` | 0.943s | 0.796s | 1.28× → 1.08× | 0.731s |
| `settle_er[mem]` | 0.932s | 0.790s | 1.27× → 1.07× | 0.722s |
| `settle_er[rocks]` | 0.940s | 0.782s | 1.28× → 1.07× | 0.712s |
| `settle_fn[rocks,fin]` | 1.190s | 0.808s | 1.62× → 1.10× | 0.732s |
| `settle_er[rocks,fin]` | 1.196s | 0.808s | 1.63× → 1.10× | 0.730s |
| `settle_mv[rocks,fin]` | 0.935s | 0.780s | 1.27× → 1.06× | 0.722s |
| `settle_mv[rocks,backfill]` | 0.857s | 0.774s | 1.17× → 1.05× | 0.713s |

**Findings**:
- The PG part of each Settle variant equals `pg_only_batch`, so the whole gap is
  Settle's own time.
- With one batch in flight that time adds to the PG time: a consumer acks a
  batch before the next ingest, so Settle and PG never overlap. With two, PG is
  busy for as long as in `pg_only_batch` and hides Settle's time; what remains
  is the first batch, which PG cannot overlap, and the last ack. Settle's own
  time grows by up to 40% next to the PG thread, but stays hidden.
- 3 and 5 unacked batches measure the same as 2 (±2%, `mv` and `fn`, tip and
  `,fin`): with PG always busy, a longer queue has nothing to hide.
- Each batch touches each user once (5000 rows, 5000 keys), so Settle removes no
  PG rows here. Parity is the ceiling for this workload.
- The MV-only pipeline is the cheapest; a reducer costs about 2× its Settle time.
- `,fin` costs more Settle time than the tip mode, which never finalizes in this run.
- Per-row UPSERT is about 12× slower than the batched one.

### Workload 3 — stateful PnL with 3 projections (`vs_postgres_stateful`)

Moving-average cost basis. 1 reducer (`position`, GROUP BY user+token) feeds 3 MVs: per-(user,token) position, per-(user,day) PnL, per-user total PnL.

Settle uses `CREATE VIRTUAL TABLE` for `trades` (raw not persisted in Settle — see Engine TODOs).

PG-only uses the realistic scalable pattern: load **only the exact aggregate state for keys touched in this batch** (`WHERE (user, token) IN unnest(...)`, similarly for day), apply moving-avg math in Rust, UPSERT back.

2026-10-16, same server, mean of three runs. PG-only and the serial Settle
rows commit one transaction per batch of 5000 trades:

| Variant | Total | PG busy | pg_rows_w | pg_rows_r |
|---|---:|---:|---:|---:|
| `pg_raw_only` (trades only, no MV) | 0.756s | 0.756s | 100,000 | 0 |
| `pg_only_smart` | 3.022s | 3.022s | 400,000 | 160,000 |
| `pg_only_smart[x4]` (4 batches per transaction) | 2.211s | 2.211s | 300,000 | 60,000 |

Settle totals, and the factor against `pg_only_smart`:

| Variant | Settle time | Serial | `,pipe` | `,pipe,q4` | `,pipe,x4` |
|---|---:|---:|---:|---:|---:|
| `settle_fn[mem]` | 0.58–0.63s | 2.842s (1.06×) | 2.293s (1.32×) | 2.047s (1.48×) | 2.083s (1.45×) |
| `settle_fn[rocks]` | 0.59–0.65s | 2.900s (1.04×) | 2.345s (1.29×) | 2.061s (1.47×) | 2.110s (1.43×) |
| `settle_fn[mem,fin]` | 0.89–1.01s | 3.282s (0.92×) | 2.362s (1.28×) | 2.260s (1.34×) | 2.197s (1.38×) |
| `settle_fn[rocks,fin]` | 1.05–1.18s | 3.489s (0.87×) | 2.409s (1.25×) | 2.268s (1.33×) | 2.237s (1.35×) |
| PG busy | | | 2.11–2.17s | 1.85–2.02s | 1.80–1.85s |
| pg_rows_w | | 330,000 | 330,000 | 285,000–300,000 | 280,000 |

- **Serial** acks each batch before the next ingest (`max_pending_batches = 1`).
- **`,pipe`**: another thread applies each batch to PG in its own
  transaction; two batches may wait for their ack (the default).
- **`,pipe,q4`**: the PG thread takes up to 4 batches that already wait,
  keeps the last record per key and acks the newest (acks are cumulative).
  It does not wait for more batches, so it adds no delay while PG keeps up.
- **`,pipe,x4`**: the same, but the PG thread waits for 4 batches. It commits
  as often as `pg_only_smart[x4]`.

**Findings**:
- The table that was here before (Settle 2.7s of 5.1s, PG-only 1.6× faster) is
  out of date: Settle's own time is now 0.6s at the tip and up to 1.2s with
  `rocks,fin`.
- Serial, Settle's time adds to its PG time: 1.04–1.06× faster than PG-only at
  the tip, 0.87–0.92× with `,fin`.
- With `,pipe`, PG is busy for 2.1s of the 2.3–2.4s total and hides Settle's
  time: 1.25–1.32× faster than PG-only. The Settle rows write 330K rows to PG
  and read nothing; PG-only writes 400K rows and reads 160K.
- PG time follows the rows written: 0.75s for the 100K trades, which every
  variant writes, and about 6µs for each MV row. In a pipelined run, faster
  Settle code does not make the total shorter; fewer PG rows do.
- The merge removes 50K of the 230K MV rows: in this data each batch touches
  each user once and each day is new, so only positions and totals repeat.
  PG busy goes from 2.1s to 1.8s. The same 4-batch transactions with no merge
  measured 2.2s, so the gain comes from the rows, not from fewer commits.
- At the same transaction size PG-only gains as much: `pg_only_smart[x4]`
  (2.21s) against `settle_fn[...,pipe,x4]` (2.08–2.24s) is 0.99–1.06×.
- `,q4` merged less with `,fin` (7 transactions instead of 6, 300K rows): Settle
  is slower there, so fewer batches wait.

### Honest summary across all workloads

For each workload pair (PG vs best Settle [mem]):

| Workload | Settle/PG ratio | Settle wins? |
|---|---|---|
| Raw passthrough | 1.3–1.7× slower | No — Settle adds overhead, removes nothing |
| Simple aggregation | 1.04–1.11× slower (vs PG batch) with pipelined acks, 1.14–1.63× without | No — the same PG writes; parity at best |
| Stateful PnL (3 projections) | 1.25–1.32× faster with pipelined acks, 0.87–1.06× serial; 0.99–1.06× when both sides commit every 4 batches | Yes, by the PG work it removes: 0 reads, fewer rows written |

**Settle beats well-written Postgres only where it removes PG work.** In the stateful workload it reads nothing back and writes fewer rows; with the same work on both sides (raw passthrough, simple aggregation) it is at parity at best.

PG's batched SQL (multi-row INSERT, `unnest+GROUP BY ON CONFLICT`, `WHERE (a,b) IN unnest(...)`) extracts most of the wins that an in-memory streaming reducer could give. With pipelined acks Settle's CPU time hides behind the PG writes, so the total depends on the PG work each side does; serial, Settle's per-row reducer and MV overhead adds to it.

### Where Settle's value actually lies (not measured here)

This bench compares **"write to PG"** pipelines. Settle's design pays off when:

1. **Streaming downstream** — emit alerts / events at the moment they trigger. PG must poll or wait for changes.
2. **Fork-aware rollback** — Settle rolls back state by block range; PG would need `DELETE WHERE block >= X` + full replay of aggregates.
3. **Self-contained storage** — no PG in the picture; RocksDB as single source-of-truth + queryable MVs.
4. **Per-key state that's expensive to serialize** — JSON blobs, sliding windows, ML feature vectors. PG load/save of large state per batch dominates; Settle keeps it in RAM.
5. **Multi-consumer fan-out** — same incremental state feeds many downstream consumers, computed once.

For "compute aggregates and write to PG"-shaped workloads PG with idiomatic code is the right answer.

### Bench fairness caveats

- **Localhost Docker** — fsync is near-free on macOS overlay2. Per-batch transactions only saved ~1% here; on production storage (network FS, magnetic, SAN) the savings would be 5–15× larger and matter more.
- **No `synchronous_commit=off` tuning** — both sides on PG defaults.
- **No connection pooling / pipelining** — `tokio_postgres::execute` is sequential per statement. Real ETL would pipeline 7 statements per batch in one round-trip via `query_raw` + futures Stream. Local RTT is microseconds so impact is small here; over a network it would be 5–10× win.
- **No prepared-statement caching** — SQL parsed each `execute`. Modest cost for simple statements on localhost.
- **VIRTUAL TABLE for stateful workload** — Settle does not persist raw (matches "PG holds the raw, Settle holds aggregates"). See Engine TODOs.
- **EventRules `WHEN-EMIT` reading state** — initially measured EventRules 2× faster than FnReducer; turned out the emit silently produced `0` because of a Settle bug in WHEN-THEN-EMIT reading post-SET state. Workaround used: `WHEN-THEN-SET` + `ALWAYS EMIT`. Correctness check added after this incident — **without state validation a bench can measure a broken pipeline as "fast"**.
- **Single-shot runs** — no statistical sampling. ±10–20% noise between runs. Numbers above are representative single runs.

### Engine TODOs surfaced by this bench

- **Raw auto-purge after finalize** — Settle currently keeps raw forever even when block is well past fork window. For pipelines that forward raw elsewhere (PG, ClickHouse), this is pure duplication. `CREATE TABLE ... RETENTION FINALIZE` or auto-cleanup after N finalized blocks.
- **`SumAgg.cached_total`** — `SumAgg::current_value` is O(N_unfinalized_blocks). Maintain a running total alongside `blocks` BTreeMap, update incrementally on `add_block`, return O(1) in `current_value`. Major win for SumAgg/AvgAgg/CountAgg. Pre-finalize is the current workaround (drops N to 0) but only helps SumAgg, not LastAgg.
- **EventRules `WHEN-THEN-EMIT` reading state** — fix or document the requirement to use `ALWAYS EMIT` for state-derived fields. Currently silent wrong results.
- **MV batched `add_block`** — `MVEngine::process_block` calls `add_block(block, &[single_value])` per row. For high-fan-in groups (many rows per group per block), batching values per group would cut BTreeMap operations N→1. No effect for fan-in=1 workloads (we measured negative due to extra HashMap allocation), so this needs to be opt-in or conditional.
- **`MVEngine::process_block_rows`** taking `&[Row]` (column-indexed) instead of `&[RowMap]` (HashMap-keyed) — measured neutral-to-negative on narrow schemas (4-col), would help on wide schemas (50+ cols).
- **RocksDB tuning surface** — bench used defaults. Group commit, write buffer size, compression-off for benches would shave 20–30% off RocksDB variants.
- **Reducer trait** returns `Vec<RowMap>` — forces per-row HashMap+String allocations on the hot path. A sink-based API (`process(state, row, &mut EmitBuffer)`) could amortize allocations. Significant trait change.
