//! Workload 3: Stateful PnL with multiple projections.
//!
//! One reducer keeps per-(user, token) state (qty, cost_basis) using
//! moving-average accounting; on each trade it emits `realized_delta` plus
//! current position state. Three MVs project the emission:
//!   - `user_token_position` — current quantity / cost_basis per (user, token)
//!   - `user_daily_pnl`      — realized PnL per (user, day) — SUM of deltas
//!   - `user_total_pnl`      — realized PnL per user — SUM of deltas
//!
//! For PG-only smart: 3 SELECTs (existing aggregate state per user) + 3
//! UPSERTs per batch — the realistic "carry-forward state" pattern.
//!
//! Run with: `cargo bench --bench vs_postgres_stateful` (Docker required)

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use settle::db::{Config, Settle};
use settle::reducer_runtime::fn_reducer::FnReducerRuntime;
use settle::test_helpers::{ingest_blocks, ingest_with_finalized};
use settle::types::{ChangeBatch, ChangeRecord, RowMap, Value};
use tokio_postgres::types::ToSql;

#[path = "common/mod.rs"]
mod common;

use common::{
    build_multi_insert, gen_trade, print_result_split, row_f64, row_i64, row_str, split_blocks,
    PgRuntime, BATCH_SIZE, BLOCKS_PER_BATCH, ROWS_PER_BLOCK,
};

// DISCLAIMER: `CREATE VIRTUAL TABLE` означает что Settle НЕ персистит raw
// rows в RocksDB — они только проходят через reducer. Это симуляция того
// как Settle должен работать с auto-purge raw после finalize (которого
// сейчас в движке нет — engine TODO). В реальной prod-ситуации Settle
// держал бы raw для unfinalized window (fork support) и дропал после
// finalize. Сейчас persistent CREATE TABLE дублировал бы raw с PG —
// нечестный overhead для бенча.
const SETTLE_SCHEMA: &str = r#"
CREATE VIRTUAL TABLE trades (
    seq          UInt64,
    block_number UInt64,
    user_addr    String,
    token        String,
    day          Int64,
    side         UInt64,
    amount       Float64,
    price        Float64
);

CREATE REDUCER position
SOURCE trades
GROUP BY user_addr, token
STATE (
    quantity   Float64 DEFAULT 0,
    cost_basis Float64 DEFAULT 0
)
LANGUAGE EXTERNAL;

CREATE MATERIALIZED VIEW user_token_position AS
SELECT
    user_addr,
    token,
    last(quantity)   AS quantity,
    last(cost_basis) AS cost_basis
FROM position
GROUP BY user_addr, token;

CREATE MATERIALIZED VIEW user_daily_pnl AS
SELECT
    user_addr,
    day,
    sum(realized_delta) AS daily_pnl
FROM position
GROUP BY user_addr, day;

CREATE MATERIALIZED VIEW user_total_pnl AS
SELECT
    user_addr,
    sum(realized_delta) AS total_pnl
FROM position
GROUP BY user_addr;
"#;

const PG_SCHEMA: &str = "
CREATE TABLE trades (
    seq          BIGINT PRIMARY KEY,
    block_number BIGINT NOT NULL,
    user_addr    TEXT NOT NULL,
    token        TEXT NOT NULL,
    day          BIGINT NOT NULL,
    side         BIGINT NOT NULL,
    amount       DOUBLE PRECISION NOT NULL,
    price        DOUBLE PRECISION NOT NULL
);
CREATE INDEX idx_trades_user ON trades(user_addr, seq);

CREATE TABLE user_token_position (
    user_addr  TEXT NOT NULL,
    token      TEXT NOT NULL,
    quantity   DOUBLE PRECISION NOT NULL,
    cost_basis DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (user_addr, token)
);

CREATE TABLE user_daily_pnl (
    user_addr TEXT NOT NULL,
    day       BIGINT NOT NULL,
    daily_pnl DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (user_addr, day)
);

CREATE TABLE user_total_pnl (
    user_addr TEXT PRIMARY KEY,
    total_pnl DOUBLE PRECISION NOT NULL
);
";

const TOTAL_ROWS: usize = 100_000;
const NUM_USERS: usize = 10_000;

// ─── FnReducer: per-(user, token) moving-avg cost basis ────────────────────

fn position_reducer() -> FnReducerRuntime {
    FnReducerRuntime::new(|state, row| {
        let user = row.get("user_addr").and_then(|v| v.as_str()).unwrap_or("");
        let token = row.get("token").and_then(|v| v.as_str()).unwrap_or("");
        let day = row.get("day").and_then(|v| v.as_i64()).unwrap_or(0);
        let side = row.get("side").and_then(|v| v.as_u64()).unwrap_or(0);
        let amount = row.get("amount").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let price = row.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0);

        let mut qty = state.get("quantity").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let mut cb = state.get("cost_basis").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let mut realized_delta = 0.0;

        if side == 0 {
            qty += amount;
            cb += amount * price;
        } else if qty > 0.0 {
            let avg_cost = cb / qty;
            let sold = amount.min(qty);
            realized_delta = sold * (price - avg_cost);
            qty -= sold;
            cb -= sold * avg_cost;
        }

        state.insert("quantity".into(), Value::Float64(qty));
        state.insert("cost_basis".into(), Value::Float64(cb));

        let mut emit = HashMap::new();
        emit.insert("user_addr".into(), Value::String(user.to_string()));
        emit.insert("token".into(), Value::String(token.to_string()));
        emit.insert("day".into(), Value::Int64(day));
        emit.insert("quantity".into(), Value::Float64(qty));
        emit.insert("cost_basis".into(), Value::Float64(cb));
        emit.insert("realized_delta".into(), Value::Float64(realized_delta));
        vec![emit]
    })
}

// ─── Settle setup ──────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
enum Storage {
    Memory,
    Rocks,
}

impl Storage {
    fn label(self) -> &'static str {
        match self {
            Storage::Memory => "mem",
            Storage::Rocks => "rocks",
        }
    }
}

fn make_cfg(schema: &str, storage: Storage) -> (Config, Option<tempfile::TempDir>) {
    match storage {
        Storage::Memory => (Config::new(schema), None),
        Storage::Rocks => {
            let dir = tempfile::tempdir().unwrap();
            let cfg = Config::with_data_dir(schema, dir.path().to_str().unwrap());
            (cfg, Some(dir))
        }
    }
}

fn open_settle_db(storage: Storage) -> anyhow::Result<(Settle, Option<tempfile::TempDir>)> {
    let (cfg, dir) = make_cfg(SETTLE_SCHEMA, storage);
    let mut db = Settle::open(cfg)?;
    db.register_reducer_callback("position", Box::new(position_reducer()))?;
    Ok((db, dir))
}

// ─── PG INSERT helpers ─────────────────────────────────────────────────────

fn pg_insert_trades(pg: &PgRuntime, rows: &[RowMap]) -> anyhow::Result<()> {
    let n = rows.len();
    if n == 0 {
        return Ok(());
    }
    let sql = build_multi_insert(
        "trades",
        &[
            "seq",
            "block_number",
            "user_addr",
            "token",
            "day",
            "side",
            "amount",
            "price",
        ],
        n,
    );
    let seq_buf: Vec<i64> = rows.iter().map(|r| row_i64(r, "seq")).collect();
    let block_buf: Vec<i64> = rows.iter().map(|r| row_i64(r, "block_number")).collect();
    let user_buf: Vec<String> = rows
        .iter()
        .map(|r| row_str(r, "user_addr").to_string())
        .collect();
    let token_buf: Vec<String> = rows
        .iter()
        .map(|r| row_str(r, "token").to_string())
        .collect();
    let day_buf: Vec<i64> = rows.iter().map(|r| row_i64(r, "day")).collect();
    let side_buf: Vec<i64> = rows.iter().map(|r| row_i64(r, "side")).collect();
    let amount_buf: Vec<f64> = rows.iter().map(|r| row_f64(r, "amount")).collect();
    let price_buf: Vec<f64> = rows.iter().map(|r| row_f64(r, "price")).collect();

    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(n * 8);
    for i in 0..n {
        params.push(&seq_buf[i]);
        params.push(&block_buf[i]);
        params.push(&user_buf[i]);
        params.push(&token_buf[i]);
        params.push(&day_buf[i]);
        params.push(&side_buf[i]);
        params.push(&amount_buf[i]);
        params.push(&price_buf[i]);
    }
    pg.execute(&sql, &params)?;
    Ok(())
}

// ─── PG-only smart: carry-forward state for all 3 aggregates ───────────────

fn pg_smart_step(pg: &PgRuntime, batch_rows: &[RowMap]) -> anyhow::Result<()> {
    if batch_rows.is_empty() {
        return Ok(());
    }

    // Collect EXACT keys touched in this batch (not all keys per user) —
    // загружаем только то что меняется. Масштабируемый паттерн.
    let mut pos_keys: HashSet<(String, String)> = HashSet::new();
    let mut daily_keys: HashSet<(String, i64)> = HashSet::new();
    let mut user_keys: HashSet<String> = HashSet::new();
    for r in batch_rows {
        let user = row_str(r, "user_addr").to_string();
        let token = row_str(r, "token").to_string();
        let day = row_i64(r, "day");
        pos_keys.insert((user.clone(), token));
        daily_keys.insert((user.clone(), day));
        user_keys.insert(user);
    }

    // Positions: SELECT WHERE (user, token) IN exact list (via unnest of two arrays).
    let positions = if pos_keys.is_empty() {
        HashMap::new()
    } else {
        let (u_arr, t_arr): (Vec<String>, Vec<String>) = pos_keys
            .iter()
            .map(|(u, t)| (u.clone(), t.clone()))
            .unzip();
        let rows = pg.query(
            "SELECT user_addr, token, quantity, cost_basis \
             FROM user_token_position \
             WHERE (user_addr, token) IN \
                (SELECT u, t FROM unnest($1::text[], $2::text[]) AS x(u, t))",
            &[&u_arr, &t_arr],
        )?;
        let mut m = HashMap::with_capacity(rows.len());
        for r in &rows {
            m.insert(
                (r.get(0), r.get(1)),
                (r.get::<_, f64>(2), r.get::<_, f64>(3)),
            );
        }
        m
    };
    let mut positions = positions;

    // Daily: SELECT WHERE (user, day) IN exact list.
    let daily = if daily_keys.is_empty() {
        HashMap::new()
    } else {
        let (u_arr, d_arr): (Vec<String>, Vec<i64>) = daily_keys
            .iter()
            .map(|(u, d)| (u.clone(), *d))
            .unzip();
        let rows = pg.query(
            "SELECT user_addr, day, daily_pnl FROM user_daily_pnl \
             WHERE (user_addr, day) IN \
                (SELECT u, d FROM unnest($1::text[], $2::bigint[]) AS x(u, d))",
            &[&u_arr, &d_arr],
        )?;
        let mut m = HashMap::with_capacity(rows.len());
        for r in &rows {
            m.insert((r.get(0), r.get(1)), r.get::<_, f64>(2));
        }
        m
    };
    let mut daily = daily;

    // Total: one row per user — just WHERE user_addr = ANY (correct cardinality).
    let users: Vec<String> = user_keys.iter().cloned().collect();
    let total_rows = pg.query(
        "SELECT user_addr, total_pnl FROM user_total_pnl WHERE user_addr = ANY($1)",
        &[&users],
    )?;
    let mut total: HashMap<String, f64> = HashMap::with_capacity(total_rows.len());
    for r in &total_rows {
        total.insert(r.get(0), r.get::<_, f64>(1));
    }

    // Apply new trades in order; track which keys changed so we only UPSERT those.
    let mut touched_pos: HashSet<(String, String)> = HashSet::new();
    let mut touched_daily: HashSet<(String, i64)> = HashSet::new();
    let mut touched_total: HashSet<String> = HashSet::new();

    for r in batch_rows {
        let user = row_str(r, "user_addr").to_string();
        let token = row_str(r, "token").to_string();
        let day = row_i64(r, "day");
        let side = row_i64(r, "side");
        let amount = row_f64(r, "amount");
        let price = row_f64(r, "price");

        let entry = positions
            .entry((user.clone(), token.clone()))
            .or_insert((0.0, 0.0));
        let (qty, cb) = entry;
        let mut realized_delta = 0.0;
        if side == 0 {
            *qty += amount;
            *cb += amount * price;
        } else if *qty > 0.0 {
            let avg_cost = *cb / *qty;
            let sold = amount.min(*qty);
            realized_delta = sold * (price - avg_cost);
            *qty -= sold;
            *cb -= sold * avg_cost;
        }
        touched_pos.insert((user.clone(), token.clone()));

        // Match Settle MV behavior: every touched (user, day) and user gets
        // a row, even when realized_delta == 0. Otherwise correctness check
        // sees row-count mismatch vs Settle's `sum() GROUP BY` which emits
        // for every group touched in the block.
        *daily.entry((user.clone(), day)).or_insert(0.0) += realized_delta;
        touched_daily.insert((user.clone(), day));
        *total.entry(user.clone()).or_insert(0.0) += realized_delta;
        touched_total.insert(user);
    }

    // UPSERT touched positions.
    if !touched_pos.is_empty() {
        let keys: Vec<&(String, String)> = touched_pos.iter().collect();
        let n = keys.len();
        let sql = format!(
            "{} ON CONFLICT (user_addr, token) DO UPDATE SET \
                quantity = EXCLUDED.quantity, \
                cost_basis = EXCLUDED.cost_basis",
            build_multi_insert(
                "user_token_position",
                &["user_addr", "token", "quantity", "cost_basis"],
                n,
            ),
        );
        let buf: Vec<(String, String, f64, f64)> = keys
            .iter()
            .map(|k| {
                let (q, c) = positions[*k];
                (k.0.clone(), k.1.clone(), q, c)
            })
            .collect();
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(n * 4);
        for e in &buf {
            params.push(&e.0);
            params.push(&e.1);
            params.push(&e.2);
            params.push(&e.3);
        }
        pg.execute(&sql, &params)?;
    }

    // UPSERT touched daily PnL.
    if !touched_daily.is_empty() {
        let keys: Vec<&(String, i64)> = touched_daily.iter().collect();
        let n = keys.len();
        let sql = format!(
            "{} ON CONFLICT (user_addr, day) DO UPDATE SET daily_pnl = EXCLUDED.daily_pnl",
            build_multi_insert("user_daily_pnl", &["user_addr", "day", "daily_pnl"], n),
        );
        let buf: Vec<(String, i64, f64)> = keys
            .iter()
            .map(|k| (k.0.clone(), k.1, daily[*k]))
            .collect();
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(n * 3);
        for e in &buf {
            params.push(&e.0);
            params.push(&e.1);
            params.push(&e.2);
        }
        pg.execute(&sql, &params)?;
    }

    // UPSERT touched total PnL.
    if !touched_total.is_empty() {
        let keys: Vec<&String> = touched_total.iter().collect();
        let n = keys.len();
        let sql = format!(
            "{} ON CONFLICT (user_addr) DO UPDATE SET total_pnl = EXCLUDED.total_pnl",
            build_multi_insert("user_total_pnl", &["user_addr", "total_pnl"], n),
        );
        let buf: Vec<(String, f64)> = keys.iter().map(|u| ((*u).clone(), total[*u])).collect();
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(n * 2);
        for e in &buf {
            params.push(&e.0);
            params.push(&e.1);
        }
        pg.execute(&sql, &params)?;
    }

    Ok(())
}

// ─── PG UPSERTs from Settle ChangeBatch records ────────────────────────────

fn pg_upsert_positions_from_changes(pg: &PgRuntime, records: &[ChangeRecord]) -> anyhow::Result<()> {
    let n = records.len();
    if n == 0 {
        return Ok(());
    }
    let sql = format!(
        "{} ON CONFLICT (user_addr, token) DO UPDATE SET \
            quantity = EXCLUDED.quantity, \
            cost_basis = EXCLUDED.cost_basis",
        build_multi_insert(
            "user_token_position",
            &["user_addr", "token", "quantity", "cost_basis"],
            n,
        ),
    );
    let user_buf: Vec<String> = records.iter().map(|r| key_str(r, "user_addr")).collect();
    let tok_buf: Vec<String> = records.iter().map(|r| key_str(r, "token")).collect();
    let qty_buf: Vec<f64> = records.iter().map(|r| val_f64(r, "quantity")).collect();
    let cb_buf: Vec<f64> = records.iter().map(|r| val_f64(r, "cost_basis")).collect();
    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(n * 4);
    for i in 0..n {
        params.push(&user_buf[i]);
        params.push(&tok_buf[i]);
        params.push(&qty_buf[i]);
        params.push(&cb_buf[i]);
    }
    pg.execute(&sql, &params)?;
    Ok(())
}

fn pg_upsert_daily_from_changes(pg: &PgRuntime, records: &[ChangeRecord]) -> anyhow::Result<()> {
    let n = records.len();
    if n == 0 {
        return Ok(());
    }
    let sql = format!(
        "{} ON CONFLICT (user_addr, day) DO UPDATE SET daily_pnl = EXCLUDED.daily_pnl",
        build_multi_insert("user_daily_pnl", &["user_addr", "day", "daily_pnl"], n),
    );
    let user_buf: Vec<String> = records.iter().map(|r| key_str(r, "user_addr")).collect();
    let day_buf: Vec<i64> = records.iter().map(|r| key_i64(r, "day")).collect();
    let pnl_buf: Vec<f64> = records.iter().map(|r| val_f64(r, "daily_pnl")).collect();
    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(n * 3);
    for i in 0..n {
        params.push(&user_buf[i]);
        params.push(&day_buf[i]);
        params.push(&pnl_buf[i]);
    }
    pg.execute(&sql, &params)?;
    Ok(())
}

fn pg_upsert_total_from_changes(pg: &PgRuntime, records: &[ChangeRecord]) -> anyhow::Result<()> {
    let n = records.len();
    if n == 0 {
        return Ok(());
    }
    let sql = format!(
        "{} ON CONFLICT (user_addr) DO UPDATE SET total_pnl = EXCLUDED.total_pnl",
        build_multi_insert("user_total_pnl", &["user_addr", "total_pnl"], n),
    );
    let user_buf: Vec<String> = records.iter().map(|r| key_str(r, "user_addr")).collect();
    let pnl_buf: Vec<f64> = records.iter().map(|r| val_f64(r, "total_pnl")).collect();
    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(n * 2);
    for i in 0..n {
        params.push(&user_buf[i]);
        params.push(&pnl_buf[i]);
    }
    pg.execute(&sql, &params)?;
    Ok(())
}

fn key_str(r: &ChangeRecord, col: &str) -> String {
    r.key
        .get(col)
        .or_else(|| r.values.get(col))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

fn key_i64(r: &ChangeRecord, col: &str) -> i64 {
    r.key
        .get(col)
        .or_else(|| r.values.get(col))
        .and_then(|v| v.as_i64())
        .unwrap_or(0)
}

fn val_f64(r: &ChangeRecord, col: &str) -> f64 {
    r.values.get(col).and_then(|v| v.as_f64()).unwrap_or(0.0)
}

// ─── Correctness check (3 tables) ──────────────────────────────────────────

#[derive(Debug, PartialEq, Clone, Copy)]
struct PgState {
    pos_count: i64,
    sum_qty: f64,
    sum_cb: f64,
    daily_count: i64,
    sum_daily: f64,
    total_count: i64,
    sum_total: f64,
}

fn read_pg_state(pg: &PgRuntime) -> anyhow::Result<PgState> {
    let pos = pg.rt.block_on(async {
        pg.client
            .query(
                "SELECT COUNT(*), COALESCE(SUM(quantity),0), COALESCE(SUM(cost_basis),0) \
                 FROM user_token_position",
                &[],
            )
            .await
    })?;
    let daily = pg.rt.block_on(async {
        pg.client
            .query(
                "SELECT COUNT(*), COALESCE(SUM(daily_pnl),0) FROM user_daily_pnl",
                &[],
            )
            .await
    })?;
    let total = pg.rt.block_on(async {
        pg.client
            .query(
                "SELECT COUNT(*), COALESCE(SUM(total_pnl),0) FROM user_total_pnl",
                &[],
            )
            .await
    })?;
    Ok(PgState {
        pos_count: pos[0].get(0),
        sum_qty: pos[0].get(1),
        sum_cb: pos[0].get(2),
        daily_count: daily[0].get(0),
        sum_daily: daily[0].get(1),
        total_count: total[0].get(0),
        sum_total: total[0].get(1),
    })
}

fn check_pg_state(
    pg: &PgRuntime,
    label: &str,
    baseline: &mut Option<PgState>,
) -> anyhow::Result<()> {
    let actual = read_pg_state(pg)?;
    let status = if let Some(exp) = baseline {
        let ok = exp.pos_count == actual.pos_count
            && (exp.sum_qty - actual.sum_qty).abs() < 0.1
            && (exp.sum_cb - actual.sum_cb).abs() < 0.1
            && exp.daily_count == actual.daily_count
            && (exp.sum_daily - actual.sum_daily).abs() < 0.1
            && exp.total_count == actual.total_count
            && (exp.sum_total - actual.sum_total).abs() < 0.1;
        if ok {
            "OK"
        } else {
            "*** MISMATCH ***"
        }
    } else {
        *baseline = Some(actual);
        "BASELINE"
    };
    eprintln!(
        "  [check] {label:<28}  pos={:>5}/{:>10.0}qty/{:>13.0}cb  daily={:>5}/{:>10.0}  total={:>5}/{:>10.0}  {}",
        actual.pos_count,
        actual.sum_qty,
        actual.sum_cb,
        actual.daily_count,
        actual.sum_daily,
        actual.total_count,
        actual.sum_total,
        status,
    );
    if status == "*** MISMATCH ***" {
        anyhow::bail!("{label}: state mismatch");
    }
    Ok(())
}

// ─── Runners ───────────────────────────────────────────────────────────────

fn attach_block_numbers(rows: &mut [RowMap], first_block: u64) {
    for (i, r) in rows.iter_mut().enumerate() {
        let block = first_block + (i / ROWS_PER_BLOCK) as u64;
        r.insert("block_number".to_string(), Value::UInt64(block));
    }
}

fn run_pg_only(pg: &PgRuntime, rows: &[RowMap]) -> anyhow::Result<()> {
    let mut block_no = 1u64;
    for chunk in rows.chunks(BATCH_SIZE) {
        let mut owned = chunk.to_vec();
        attach_block_numbers(&mut owned, block_no);
        // Per-batch transaction: 1 fsync for INSERT raw + 3 UPSERTs vs 4 fsyncs.
        pg.begin()?;
        pg_insert_trades(pg, &owned)?;
        pg_smart_step(pg, &owned)?;
        pg.commit()?;
        block_no += BLOCKS_PER_BATCH as u64;
    }
    Ok(())
}

const FINALIZED_AHEAD: u64 = 1_000_000_000;

fn run_settle_pg_timed(
    db: &mut Settle,
    pg: &PgRuntime,
    rows: &[RowMap],
    pre_finalized: bool,
) -> anyhow::Result<std::time::Duration> {
    let mut block_no = 1u64;
    let mut settle_total = std::time::Duration::ZERO;
    for chunk in rows.chunks(BATCH_SIZE) {
        let mut owned = chunk.to_vec();
        attach_block_numbers(&mut owned, block_no);

        let items: Vec<(String, u64, Vec<RowMap>)> = split_blocks(&owned, block_no)
            .into_iter()
            .map(|(b, c)| ("trades".to_string(), b, c.to_vec()))
            .collect();
        let t = Instant::now();
        let batch: ChangeBatch = if pre_finalized {
            ingest_with_finalized(db, items, FINALIZED_AHEAD)?
        } else {
            ingest_blocks(db, items)?
        }
        .expect("non-empty batch");
        settle_total += t.elapsed();

        pg.begin()?;
        pg_insert_trades(pg, &owned)?;
        pg_upsert_positions_from_changes(pg, batch.records_for("user_token_position"))?;
        pg_upsert_daily_from_changes(pg, batch.records_for("user_daily_pnl"))?;
        pg_upsert_total_from_changes(pg, batch.records_for("user_total_pnl"))?;
        pg.commit()?;
        block_no += BLOCKS_PER_BATCH as u64;
    }
    Ok(settle_total)
}

// ─── Driver ────────────────────────────────────────────────────────────────

fn main() -> anyhow::Result<()> {
    let rows: Vec<RowMap> = (0..TOTAL_ROWS).map(|i| gen_trade(i, NUM_USERS)).collect();

    eprintln!("workload: vs_postgres_stateful (per-(user,token) position + per-(user,day) PnL + per-user total)");
    eprintln!(
        "  config: {TOTAL_ROWS} rows, {NUM_USERS} users, 10 tokens, 20 days, \
         {ROWS_PER_BLOCK} rows/block, {BLOCKS_PER_BATCH} blocks/batch, {} batches",
        TOTAL_ROWS / BATCH_SIZE
    );

    let mut baseline: Option<PgState> = None;

    {
        let pg = PgRuntime::start()?;
        pg.batch_execute(PG_SCHEMA)?;
        pg.take_stats();
        let t = Instant::now();
        run_pg_only(&pg, &rows)?;
        let elapsed = t.elapsed();
        print_result_split(
            "pg_only_smart",
            TOTAL_ROWS,
            elapsed,
            std::time::Duration::ZERO,
            pg.take_stats(),
        );
        check_pg_state(&pg, "pg_only_smart", &mut baseline)?;
    }

    for pre_fin in [false, true] {
        let fin_tag = if pre_fin { ",fin" } else { "" };
        for storage in [Storage::Memory, Storage::Rocks] {
            let (mut db, _dir) = open_settle_db(storage)?;
            let pg = PgRuntime::start()?;
            pg.batch_execute(PG_SCHEMA)?;
            pg.take_stats();
            let t = Instant::now();
            let settle_t = run_settle_pg_timed(&mut db, &pg, &rows, pre_fin)?;
            let total = t.elapsed();
            let label = format!("settle_fn[{}{fin_tag}]_then_pg", storage.label());
            print_result_split(&label, TOTAL_ROWS, total, settle_t, pg.take_stats());
            check_pg_state(&pg, &label, &mut baseline)?;
        }
    }

    Ok(())
}
