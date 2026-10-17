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
use std::sync::mpsc;
use std::time::{Duration, Instant};

use settle::db::{Config, Settle};
use settle::reducer_runtime::fn_reducer::FnReducerRuntime;
use settle::test_helpers::{ingest_blocks, ingest_with_finalized, ingest_with_finalized_no_ack};
use settle::types::{ChangeBatch, ChangeRecord, RowMap, Value};
use tokio_postgres::types::ToSql;

#[path = "common/mod.rs"]
mod common;

use common::{
    build_multi_insert, gen_trade, print_result_split, row_f64, row_i64, row_str, split_blocks,
    PgBaseline, PgCase, PgRuntime, BATCH_SIZE, BLOCKS_PER_BATCH, ROWS_PER_BLOCK,
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
/// 8 columns × 5000 rows stays under the 65535 parameters of one statement.
const ROWS_PER_STATEMENT: usize = BATCH_SIZE;

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

        state.set("quantity", Value::Float64(qty));
        state.set("cost_basis", Value::Float64(cb));

        let mut emit = HashMap::new();
        emit.insert("user_addr".into(), Value::String(user.to_string()));
        emit.insert("token".into(), Value::String(token.to_string()));
        emit.insert("day".into(), Value::Int64(day));
        emit.insert("quantity".into(), Value::Float64(qty));
        emit.insert("cost_basis".into(), Value::Float64(cb));
        emit.insert("realized_delta".into(), Value::Float64(realized_delta));
        state.emit(emit)
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

fn make_cfg(schema: &str, storage: Storage, depth: usize) -> (Config, Option<tempfile::TempDir>) {
    let (cfg, dir) = match storage {
        Storage::Memory => (Config::new(schema), None),
        Storage::Rocks => {
            // In target/, on the disk: /tmp can be tmpfs, where fsync costs nothing.
            let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
            let cfg = Config::with_data_dir(schema, dir.path().to_str().unwrap());
            (cfg, Some(dir))
        }
    };
    (cfg.max_pending_batches(depth), dir)
}

fn open_settle_db(
    storage: Storage,
    depth: usize,
) -> anyhow::Result<(Settle, Option<tempfile::TempDir>)> {
    let (cfg, dir) = make_cfg(SETTLE_SCHEMA, storage, depth);
    let mut db = Settle::open(cfg)?;
    db.register_reducer_callback("position", Box::new(position_reducer()))?;
    Ok((db, dir))
}

// ─── PG INSERT helpers ─────────────────────────────────────────────────────

fn pg_insert_trades(pg: &PgRuntime, rows: &[RowMap]) -> anyhow::Result<()> {
    for part in rows.chunks(ROWS_PER_STATEMENT) {
        pg_insert_trades_statement(pg, part)?;
    }
    Ok(())
}

fn pg_insert_trades_statement(pg: &PgRuntime, rows: &[RowMap]) -> anyhow::Result<()> {
    let n = rows.len();
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
    let pos_keys: Vec<&(String, String)> = touched_pos.iter().collect();
    for keys in pos_keys.chunks(ROWS_PER_STATEMENT) {
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
    let daily_keys: Vec<&(String, i64)> = touched_daily.iter().collect();
    for keys in daily_keys.chunks(ROWS_PER_STATEMENT) {
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
    let total_keys: Vec<&String> = touched_total.iter().collect();
    for keys in total_keys.chunks(ROWS_PER_STATEMENT) {
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

fn pg_upsert_positions_from_changes(pg: &PgRuntime, records: &[&ChangeRecord]) -> anyhow::Result<()> {
    for part in records.chunks(ROWS_PER_STATEMENT) {
        pg_upsert_positions_statement(pg, part)?;
    }
    Ok(())
}

fn pg_upsert_positions_statement(pg: &PgRuntime, records: &[&ChangeRecord]) -> anyhow::Result<()> {
    let n = records.len();
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

fn pg_upsert_daily_from_changes(pg: &PgRuntime, records: &[&ChangeRecord]) -> anyhow::Result<()> {
    for part in records.chunks(ROWS_PER_STATEMENT) {
        pg_upsert_daily_statement(pg, part)?;
    }
    Ok(())
}

fn pg_upsert_daily_statement(pg: &PgRuntime, records: &[&ChangeRecord]) -> anyhow::Result<()> {
    let n = records.len();
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

fn pg_upsert_total_from_changes(pg: &PgRuntime, records: &[&ChangeRecord]) -> anyhow::Result<()> {
    for part in records.chunks(ROWS_PER_STATEMENT) {
        pg_upsert_total_statement(pg, part)?;
    }
    Ok(())
}

fn pg_upsert_total_statement(pg: &PgRuntime, records: &[&ChangeRecord]) -> anyhow::Result<()> {
    let n = records.len();
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
    r.key(col)
        .or_else(|| r.get(col))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

fn key_i64(r: &ChangeRecord, col: &str) -> i64 {
    r.key(col)
        .or_else(|| r.get(col))
        .and_then(|v| v.as_i64())
        .unwrap_or(0)
}

fn val_f64(r: &ChangeRecord, col: &str) -> f64 {
    r.get(col).and_then(|v| v.as_f64()).unwrap_or(0.0)
}

/// The records of `table` over `batches`, one per key: the last one. Each
/// record carries the whole row and this workload deletes nothing, so the
/// UPSERT of the last record gives the state after all the batches. One
/// batch has one record per key already.
fn last_per_key<'a>(
    batches: &[&'a ChangeBatch],
    table: &str,
    key_cols: &[&str],
) -> Vec<&'a ChangeRecord> {
    if let [batch] = batches {
        return batch.records_for(table).iter().collect();
    }

    let mut index: HashMap<Vec<Value>, usize> = HashMap::new();
    let mut merged: Vec<&ChangeRecord> = Vec::new();
    for record in batches.iter().flat_map(|b| b.records_for(table)) {
        let key: Vec<Value> = key_cols
            .iter()
            .map(|col| record.key(*col).cloned().unwrap_or(Value::Null))
            .collect();
        match index.get(&key) {
            Some(&i) => merged[i] = record,
            None => {
                index.insert(key, merged.len());
                merged.push(record);
            }
        }
    }
    merged
}

/// A batch of trades with the change batch Settle made from them.
type Applied = (Vec<RowMap>, ChangeBatch);

/// Apply one or more batches in one transaction.
fn pg_apply(pg: &PgRuntime, group: &[Applied]) -> anyhow::Result<()> {
    let batches: Vec<&ChangeBatch> = group.iter().map(|(_, batch)| batch).collect();
    let positions = last_per_key(&batches, "user_token_position", &["user_addr", "token"]);
    let daily = last_per_key(&batches, "user_daily_pnl", &["user_addr", "day"]);
    let total = last_per_key(&batches, "user_total_pnl", &["user_addr"]);

    pg.begin()?;
    for (trades, _) in group {
        pg_insert_trades(pg, trades)?;
    }
    pg_upsert_positions_from_changes(pg, &positions)?;
    pg_upsert_daily_from_changes(pg, &daily)?;
    pg_upsert_total_from_changes(pg, &total)?;
    pg.commit()?;
    Ok(())
}

// ─── Correctness check (3 tables) ──────────────────────────────────────────

#[derive(Debug, PartialEq, Clone, Copy, serde::Serialize, serde::Deserialize)]
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
    check_state(read_pg_state(pg)?, label, baseline)
}

fn check_state(actual: PgState, label: &str, baseline: &mut Option<PgState>) -> anyhow::Result<()> {
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

/// `batch_size` rows go into each transaction.
fn run_pg_only(pg: &PgRuntime, rows: &[RowMap], batch_size: usize) -> anyhow::Result<()> {
    let mut block_no = 1u64;
    for chunk in rows.chunks(batch_size) {
        let mut owned = chunk.to_vec();
        attach_block_numbers(&mut owned, block_no);
        // Per-batch transaction: 1 fsync for INSERT raw + 3 UPSERTs vs 4 fsyncs.
        pg.begin()?;
        pg_insert_trades(pg, &owned)?;
        pg_smart_step(pg, &owned)?;
        pg.commit()?;
        block_no += (chunk.len() / ROWS_PER_BLOCK) as u64;
    }
    Ok(())
}

fn run_pg_raw_only(pg: &PgRuntime, rows: &[RowMap]) -> anyhow::Result<()> {
    let mut block_no = 1u64;
    for chunk in rows.chunks(BATCH_SIZE) {
        let mut owned = chunk.to_vec();
        attach_block_numbers(&mut owned, block_no);
        pg.begin()?;
        pg_insert_trades(pg, &owned)?;
        pg.commit()?;
        block_no += BLOCKS_PER_BATCH as u64;
    }
    Ok(())
}

const FINALIZED_AHEAD: u64 = 1_000_000_000;

fn ingest_items(rows: &[RowMap], block_no: u64) -> Vec<(String, u64, Vec<RowMap>)> {
    split_blocks(rows, block_no)
        .into_iter()
        .map(|(b, c)| ("trades".to_string(), b, c.to_vec()))
        .collect()
}

fn run_settle_pg_timed(
    db: &mut Settle,
    pg: &PgRuntime,
    rows: &[RowMap],
    pre_finalized: bool,
) -> anyhow::Result<Duration> {
    let mut block_no = 1u64;
    let mut settle_total = Duration::ZERO;
    for chunk in rows.chunks(BATCH_SIZE) {
        let mut owned = chunk.to_vec();
        attach_block_numbers(&mut owned, block_no);

        let items = ingest_items(&owned, block_no);
        let t = Instant::now();
        let batch: ChangeBatch = if pre_finalized {
            ingest_with_finalized(db, items, FINALIZED_AHEAD)?
        } else {
            ingest_blocks(db, items)?
        }
        .expect("non-empty batch");
        settle_total += t.elapsed();

        pg_apply(pg, &[(owned, batch)])?;
        block_no += BLOCKS_PER_BATCH as u64;
    }
    Ok(settle_total)
}

/// How the PG applier takes batches from the queue into one transaction.
#[derive(Clone, Copy)]
enum Group {
    /// Up to `n` batches that wait in the queue; no wait for more.
    Queued(usize),
    /// `n` batches; the applier waits until they are ingested.
    Fixed(usize),
}

impl Group {
    fn size(self) -> usize {
        match self {
            Group::Queued(n) | Group::Fixed(n) => n,
        }
    }

    /// Room for the group PG applies and the next group Settle ingests.
    fn depth(self) -> usize {
        2 * self.size()
    }

    fn label(self) -> String {
        match self {
            Group::Queued(1) => ",pipe".to_string(),
            Group::Queued(n) => format!(",pipe,q{n}"),
            Group::Fixed(n) => format!(",pipe,x{n}"),
        }
    }
}

struct PipelinedRun {
    settle: Duration,
    pg_busy: Duration,
    transactions: usize,
}

/// `run_settle_pg_timed` with the PG side on another thread: Settle ingests
/// the next batches while PG applies the ones before. One ack commits a
/// whole group.
fn run_settle_pg_pipelined(
    db: &mut Settle,
    pg: &PgRuntime,
    rows: &[RowMap],
    pre_finalized: bool,
    group: Group,
) -> anyhow::Result<PipelinedRun> {
    let (batch_tx, batch_rx) = mpsc::channel::<Applied>();
    let (applied_tx, applied_rx) = mpsc::channel::<anyhow::Result<u64>>();

    std::thread::scope(|scope| {
        let applier = scope.spawn(move || {
            let mut busy = Duration::ZERO;
            let mut transactions = 0;
            while let Ok(first) = batch_rx.recv() {
                let mut taken = vec![first];
                while taken.len() < group.size() {
                    let next = match group {
                        Group::Queued(_) => batch_rx.try_recv().ok(),
                        Group::Fixed(_) => batch_rx.recv().ok(),
                    };
                    let Some(next) = next else { break };
                    taken.push(next);
                }

                let newest = taken.last().expect("one batch at least").1.sequence;
                let t = Instant::now();
                let applied = pg_apply(pg, &taken).map(|()| newest);
                busy += t.elapsed();
                transactions += 1;
                if applied_tx.send(applied).is_err() {
                    break;
                }
            }
            (busy, transactions)
        });

        let mut settle_total = Duration::ZERO;
        let mut ack = |db: &mut Settle, applied: anyhow::Result<u64>| -> anyhow::Result<()> {
            let sequence = applied?;
            let t = Instant::now();
            db.ack(sequence)?;
            settle_total += t.elapsed();
            Ok(())
        };

        let mut block_no = 1u64;
        let mut ingest_time = Duration::ZERO;
        for chunk in rows.chunks(BATCH_SIZE) {
            while let Ok(applied) = applied_rx.try_recv() {
                ack(db, applied)?;
            }
            while db.unacked_batches() >= group.depth() {
                ack(db, applied_rx.recv()?)?;
            }

            let mut owned = chunk.to_vec();
            attach_block_numbers(&mut owned, block_no);
            let items = ingest_items(&owned, block_no);
            let finalized = if pre_finalized { FINALIZED_AHEAD } else { db.finalized_block() };

            let t = Instant::now();
            let batch = ingest_with_finalized_no_ack(db, items, finalized)?.expect("non-empty batch");
            ingest_time += t.elapsed();

            batch_tx
                .send((owned, batch))
                .map_err(|_| anyhow::anyhow!("PG applier stopped"))?;
            block_no += BLOCKS_PER_BATCH as u64;
        }

        drop(batch_tx);
        while db.is_awaiting_ack() {
            ack(db, applied_rx.recv()?)?;
        }
        drop(ack);

        let (pg_busy, transactions) = applier.join().expect("applier thread");
        Ok(PipelinedRun {
            settle: settle_total + ingest_time,
            pg_busy,
            transactions,
        })
    })
}

// ─── Driver ────────────────────────────────────────────────────────────────

fn main() -> anyhow::Result<()> {
    let rows: Vec<RowMap> = (0..TOTAL_ROWS).map(|i| gen_trade(i, NUM_USERS)).collect();

    let config = format!(
        "{TOTAL_ROWS} rows, {NUM_USERS} users, 10 tokens, 20 days, \
         {ROWS_PER_BLOCK} rows/block, {BLOCKS_PER_BATCH} blocks/batch, {} batches",
        TOTAL_ROWS / BATCH_SIZE
    );
    eprintln!(
        "workload: vs_postgres_stateful (per-(user,token) position + per-(user,day) PnL + per-user total)"
    );
    eprintln!("  config: {config}");
    let mut pg_baseline = PgBaseline::open("vs_postgres_stateful", 1, &config)?;

    let mut baseline: Option<PgState> = None;

    // The floor: every variant writes these trades, with no MV in PG.
    let case = pg_baseline.case("pg_raw_only", || {
        let pg = PgRuntime::start()?;
        pg.batch_execute(PG_SCHEMA)?;
        pg.take_stats();
        let t = Instant::now();
        run_pg_raw_only(&pg, &rows)?;
        let elapsed = t.elapsed();
        Ok(PgCase {
            elapsed,
            stats: pg.take_stats(),
            output: (),
        })
    })?;
    print_result_split(
        "pg_raw_only",
        TOTAL_ROWS,
        case.elapsed,
        Duration::ZERO,
        case.stats,
    );

    // x4 commits as often as the Settle `x4` rows below.
    for batches_per_tx in [1, 4] {
        let label = match batches_per_tx {
            1 => "pg_only_smart".to_string(),
            n => format!("pg_only_smart[x{n}]"),
        };
        let case = pg_baseline.case(&label, || {
            let pg = PgRuntime::start()?;
            pg.batch_execute(PG_SCHEMA)?;
            pg.take_stats();
            let t = Instant::now();
            run_pg_only(&pg, &rows, batches_per_tx * BATCH_SIZE)?;
            let elapsed = t.elapsed();
            let stats = pg.take_stats();
            Ok(PgCase {
                elapsed,
                stats,
                output: read_pg_state(&pg)?,
            })
        })?;

        print_result_split(&label, TOTAL_ROWS, case.elapsed, Duration::ZERO, case.stats);
        check_state(case.output, &label, &mut baseline)?;
    }

    // None: ack each batch before the next ingest.
    let consumers = [
        None,
        Some(Group::Queued(1)),
        Some(Group::Queued(4)),
        Some(Group::Fixed(4)),
    ];
    for pre_fin in [false, true] {
        let fin_tag = if pre_fin { ",fin" } else { "" };
        for storage in [Storage::Memory, Storage::Rocks] {
            for consumer in consumers {
                let depth = consumer.map_or(1, Group::depth);
                let (mut db, _dir) = open_settle_db(storage, depth)?;
                let pg = PgRuntime::start()?;
                pg.batch_execute(PG_SCHEMA)?;
                pg.take_stats();

                let pipe = consumer.map_or(String::new(), Group::label);
                let label = format!("settle_fn[{}{fin_tag}{pipe}]_then_pg", storage.label());
                let t = Instant::now();
                let settle_t = match consumer {
                    None => run_settle_pg_timed(&mut db, &pg, &rows, pre_fin)?,
                    Some(group) => {
                        let run = run_settle_pg_pipelined(&mut db, &pg, &rows, pre_fin, group)?;
                        eprintln!(
                            "  [pipe] {label:<36}  pg busy={:>6.3}s  txs={}",
                            run.pg_busy.as_secs_f64(),
                            run.transactions,
                        );
                        run.settle
                    }
                };
                let total = t.elapsed();
                print_result_split(&label, TOTAL_ROWS, total, settle_t, pg.take_stats());
                check_pg_state(&pg, &label, &mut baseline)?;
            }
        }
    }

    Ok(())
}
