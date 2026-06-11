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

10K users, `SUM(value)` per user.

| Variant | Wall time | rows/s | pg_writes | Notes |
|---|---|---|---|---|
| `pg_only_per_row` | ~40s | ~2.5K | 100,100 | Naive `INSERT ... ON CONFLICT DO UPDATE` per row — kills network |
| `pg_only_batch` | 0.84s | 119K | 200 | `unnest+GROUP BY` UPSERT — idiomatic batch path |
| `settle_fn[mem]` | 1.98s | 50K | 200 | FnReducer + last() MV |
| `settle_er[mem]` | 2.01s | 50K | 200 | EventRules — ≈ FnReducer |
| `settle_mv[mem]` | 2.15s | 47K | 200 | MV `sum()` direct from raw, no reducer |
| `settle_fn[rocks]` | 2.99s | 33K | 200 | RocksDB backend, +50% vs mem |
| `settle_mv[mem,fin]` | 1.95s | 51K | 200 | Pre-finalized → SumAgg O(1) `current_value` |

**Findings**:
- **PG batch is unbeatable for small-state commutative aggregations**: indexed B-tree + HashAggregate in tight C loops.
- All 3 Settle variants (FnReducer / EventRules / MV-only) ≈ equal — main cost is `MVEngine` processing, not reducer dispatch.
- Per-row UPSERT in PG is **60× slower than batched** — never write it like this.
- `MV-only + pre-finalized` is best Settle variant — flushes blocks immediately so `SumAgg.current_value` stops being O(N_unfinalized_blocks).

### Workload 3 — stateful PnL with 3 projections (`vs_postgres_stateful`)

Moving-average cost basis. 1 reducer (`position`, GROUP BY user+token) feeds 3 MVs: per-(user,token) position, per-(user,day) PnL, per-user total PnL.

Settle uses `CREATE VIRTUAL TABLE` for `trades` (raw not persisted in Settle — see Engine TODOs).

| Variant | Total | settle | pg | rows/s | pg_writes | pg_rows_w | pg_reads | pg_rows_r |
|---|---|---|---|---|---|---|---|---|
| `pg_only_smart` | 3.16s | — | 3.16s | 32K | 80 | 400,000 | 60 | 160,000 |
| `settle_fn[mem]` | 5.11s | 2.71s | 2.40s | 20K | 66 | 330,000 | 0 | 0 |
| `settle_fn[rocks]` | 5.73s | 3.41s | 2.33s | 17K | 66 | 330,000 | 0 | 0 |
| `settle_fn[mem,fin]` | 5.11s | 2.72s | 2.39s | 20K | 66 | 330,000 | 0 | 0 |
| `settle_fn[rocks,fin]` | 5.88s | 3.45s | 2.43s | 17K | 66 | 330,000 | 0 | 0 |

PG-only uses the realistic scalable pattern: load **only the exact aggregate state for keys touched in this batch** (`WHERE (user, token) IN unnest(...)`, similarly for day), apply moving-avg math in Rust, UPSERT back.

**Findings**:
- **Settle architecturally avoids 160K reads** (0 reads vs PG's 160K) and 17% fewer writes.
- **PG wins on wall time by 1.6×** anyway: PG B-tree seek per row + state update is faster than Settle's per-row reducer + 3 MV pipeline dispatch.
- Settle's bottleneck is **CPU on reducer + 3 MVs** (~2.7s of 5.1s), not I/O.
- Adding more projections (1 → 3 MVs) made PG win **more**, not less: PG just runs 2 more cheap SELECTs+UPSERTs, Settle runs 2 more full MV pipelines.

### Honest summary across all workloads

For each workload pair (PG vs best Settle [mem]):

| Workload | Settle/PG ratio | Settle wins? |
|---|---|---|
| Raw passthrough | 1.3–1.7× slower | No — Settle adds overhead, removes nothing |
| Simple aggregation | 2.3× slower (vs PG batch) | No — PG `unnest+GROUP BY` is unbeatable for commutative ops |
| Stateful PnL (3 projections) | 1.6× slower | No on wall time, **yes on I/O** (0 reads, fewer writes) |

**Settle does not beat well-written Postgres on these workloads.** Period.

PG's batched SQL (multi-row INSERT, `unnest+GROUP BY ON CONFLICT`, `WHERE (a,b) IN unnest(...)`) extracts most of the wins that an in-memory streaming reducer could give. Settle's per-row reducer abstractions (FnReducer dispatch, HashMap-keyed emits, MV `compute_output` × `prev+current` per touched group per batch, BTreeMap-per-block agg state) add CPU overhead larger than the I/O savings on small-state workloads.

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
