//! Benchmarks for Memory and RocksDB backends.
//!
//! Run with: cargo bench --bench throughput

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use delta_db::db::{Config, DeltaDb};
use delta_db::engine::reducer::ReducerEngine;
use delta_db::reducer_runtime::fn_reducer::FnReducerRuntime;
use delta_db::schema::parser::parse_schema;
use delta_db::storage::memory::MemoryBackend;
use delta_db::types::{ColumnRegistry, RowMap, Value};

const RAW_ONLY_SCHEMA: &str = r#"
    CREATE TABLE events (
        block_number UInt64,
        tx_hash      String,
        log_index    UInt64,
        from_addr    String,
        to_addr      String,
        value        Float64
    );
"#;

const RAW_WITH_MV_SCHEMA: &str = r#"
    CREATE TABLE events (
        block_number UInt64,
        from_addr    String,
        to_addr      String,
        value        Float64
    );

    CREATE MATERIALIZED VIEW volume_by_sender AS
    SELECT
        from_addr,
        sum(value) AS total_sent,
        count()    AS tx_count
    FROM events
    GROUP BY from_addr;
"#;

const REDUCER_EVENT_RULES_SCHEMA: &str = r#"
    CREATE TABLE trades (
        block_number UInt64,
        user         String,
        side         String,
        amount       Float64,
        price        Float64
    );

    CREATE REDUCER pnl
    SOURCE trades
    GROUP BY user
    STATE (
        quantity   Float64 DEFAULT 0,
        cost_basis Float64 DEFAULT 0
    )
        WHEN row.side = 'buy' THEN
            SET state.quantity = state.quantity + row.amount
            SET state.cost_basis = state.cost_basis + row.amount * row.price
            EMIT trade_pnl = 0
        WHEN row.side = 'sell' THEN
            LET avg_cost = state.cost_basis / state.quantity
            SET state.quantity = state.quantity - row.amount
            SET state.cost_basis = state.cost_basis - row.amount * avg_cost
            EMIT trade_pnl = row.amount * (row.price - avg_cost)
        ALWAYS EMIT
            state.quantity AS position_size
    END;

    CREATE MATERIALIZED VIEW position_summary AS
    SELECT
        user,
        sum(trade_pnl)       AS total_pnl,
        last(position_size)  AS current_position,
        count()              AS trade_count
    FROM pnl
    GROUP BY user;
"#;

const REDUCER_LUA_SCHEMA: &str = r#"
    CREATE TABLE trades (
        block_number UInt64,
        user         String,
        side         String,
        amount       Float64,
        price        Float64
    );

    CREATE REDUCER pnl
    SOURCE trades
    GROUP BY user
    STATE (
        quantity   Float64 DEFAULT 0,
        cost_basis Float64 DEFAULT 0
    )
    LANGUAGE lua
    PROCESS $$
        if row.side == "buy" then
            state.quantity = state.quantity + row.amount
            state.cost_basis = state.cost_basis + row.amount * row.price
            emit.trade_pnl = 0
        else
            local avg_cost = state.cost_basis / state.quantity
            emit.trade_pnl = row.amount * (row.price - avg_cost)
            state.quantity = state.quantity - row.amount
            state.cost_basis = state.cost_basis - row.amount * avg_cost
        end
        emit.position_size = state.quantity
    $$;

    CREATE MATERIALIZED VIEW position_summary AS
    SELECT
        user,
        sum(trade_pnl)       AS total_pnl,
        last(position_size)  AS current_position,
        count()              AS trade_count
    FROM pnl
    GROUP BY user;
"#;

const REDUCER_FN_SCHEMA: &str = r#"
    CREATE TABLE trades (
        block_number UInt64,
        user         String,
        side         String,
        amount       Float64,
        price        Float64
    );

    CREATE REDUCER pnl
    SOURCE trades
    GROUP BY user
    STATE (
        quantity   Float64 DEFAULT 0,
        cost_basis Float64 DEFAULT 0
    )
    LANGUAGE EXTERNAL;

    CREATE MATERIALIZED VIEW position_summary AS
    SELECT
        user,
        sum(trade_pnl)       AS total_pnl,
        last(position_size)  AS current_position,
        count()              AS trade_count
    FROM pnl
    GROUP BY user;
"#;

/// Build a PnL FnReducerRuntime — same logic as the Lua/EventRules versions.
fn pnl_fn_runtime() -> FnReducerRuntime {
    FnReducerRuntime::new(|state, row| {
        let side = row.get("side").and_then(|v| v.as_str()).unwrap_or("");
        let amount = row.get("amount").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let price = row.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0);

        let qty = state
            .get("quantity")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let cost = state
            .get("cost_basis")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);

        let mut emit = HashMap::new();

        if side == "buy" {
            state.insert("quantity".into(), Value::Float64(qty + amount));
            state.insert("cost_basis".into(), Value::Float64(cost + amount * price));
            emit.insert("trade_pnl".into(), Value::Float64(0.0));
        } else {
            let avg_cost = if qty > 0.0 { cost / qty } else { 0.0 };
            emit.insert(
                "trade_pnl".into(),
                Value::Float64(amount * (price - avg_cost)),
            );
            state.insert("quantity".into(), Value::Float64(qty - amount));
            state.insert(
                "cost_basis".into(),
                Value::Float64(cost - amount * avg_cost),
            );
        }

        let new_qty = state
            .get("quantity")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        emit.insert("position_size".into(), Value::Float64(new_qty));

        vec![emit]
    })
}

fn make_raw_row(i: usize) -> RowMap {
    HashMap::from([
        ("block_number".to_string(), Value::UInt64(i as u64 / 100)),
        ("tx_hash".to_string(), Value::String(format!("0x{i:064x}"))),
        ("log_index".to_string(), Value::UInt64(i as u64 % 100)),
        (
            "from_addr".to_string(),
            Value::String(format!("0xuser{}", i % 1000)),
        ),
        (
            "to_addr".to_string(),
            Value::String(format!("0xrecv{}", i % 500)),
        ),
        ("value".to_string(), Value::Float64(i as f64 * 0.001)),
    ])
}

fn make_raw_row_for_mv(i: usize) -> RowMap {
    HashMap::from([
        ("block_number".to_string(), Value::UInt64(i as u64 / 100)),
        (
            "from_addr".to_string(),
            Value::String(format!("0xuser{}", i % 1000)),
        ),
        (
            "to_addr".to_string(),
            Value::String(format!("0xrecv{}", i % 500)),
        ),
        ("value".to_string(), Value::Float64(i as f64 * 0.001)),
    ])
}

fn make_trade(user: &str, side: &str, amount: f64, price: f64) -> RowMap {
    HashMap::from([
        ("user".to_string(), Value::String(user.to_string())),
        ("side".to_string(), Value::String(side.to_string())),
        ("amount".to_string(), Value::Float64(amount)),
        ("price".to_string(), Value::Float64(price)),
    ])
}

// ─── Backend factories ─────────────────────────────────────────────

#[derive(Clone, Copy)]
enum Backend {
    Memory,
    RocksDb,
}

impl std::fmt::Display for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Backend::Memory => write!(f, "Memory"),
            Backend::RocksDb => write!(f, "RocksDB"),
        }
    }
}

fn make_config(schema: &str, backend: Backend) -> (Config, Option<tempfile::TempDir>) {
    match backend {
        Backend::Memory => (Config::new(schema), None),
        Backend::RocksDb => {
            let dir = tempfile::tempdir().unwrap();
            let cfg = Config::with_data_dir(schema, dir.path().to_str().unwrap());
            (cfg, Some(dir))
        }
    }
}

// ─── Bench results ─────────────────────────────────────────────────

struct BenchResult {
    name: String,
    #[allow(dead_code)]
    backend: String,
    total_rows: usize,
    elapsed_ms: f64,
    rows_per_sec: f64,
    pass: bool,
    target: String,
}

impl BenchResult {
    fn print(&self) {
        let status = if self.pass { "PASS" } else { "FAIL" };
        println!(
            "  [{status}] {:<45} {:>10.0} rows/s  ({} rows in {:.1}ms)  target: {}",
            self.name, self.rows_per_sec, self.total_rows, self.elapsed_ms, self.target
        );
    }
}

// ─── Benchmarks ────────────────────────────────────────────────────

fn bench_raw_ingestion(backend: Backend) -> BenchResult {
    let total_rows = 200_000;
    let batch_size = 100;
    let (cfg, _dir) = make_config(RAW_ONLY_SCHEMA, backend);
    let mut db = DeltaDb::open(cfg).unwrap();

    let rows: Vec<RowMap> = (0..total_rows).map(make_raw_row).collect();

    let start = Instant::now();
    for (block, chunk) in rows.chunks(batch_size).enumerate() {
        db.process_batch("events", block as u64, chunk.to_vec())
            .unwrap();
    }
    db.flush();
    let elapsed = start.elapsed();

    let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
    BenchResult {
        name: format!("Raw ingestion [{}]", backend),
        backend: backend.to_string(),
        total_rows,
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        rows_per_sec,
        pass: rows_per_sec > 100_000.0,
        target: ">100K rows/sec".to_string(),
    }
}

fn bench_raw_with_mv(backend: Backend) -> BenchResult {
    let total_rows = 200_000;
    let batch_size = 100;
    let (cfg, _dir) = make_config(RAW_WITH_MV_SCHEMA, backend);
    let mut db = DeltaDb::open(cfg).unwrap();

    let rows: Vec<RowMap> = (0..total_rows).map(make_raw_row_for_mv).collect();

    let start = Instant::now();
    for (block, chunk) in rows.chunks(batch_size).enumerate() {
        db.process_batch("events", block as u64, chunk.to_vec())
            .unwrap();
    }
    db.flush();
    let elapsed = start.elapsed();

    let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
    BenchResult {
        name: format!("Raw + MV [{}]", backend),
        backend: backend.to_string(),
        total_rows,
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        rows_per_sec,
        pass: rows_per_sec > 50_000.0,
        target: ">50K rows/sec".to_string(),
    }
}

fn bench_full_pipeline_event_rules(backend: Backend) -> BenchResult {
    let total_rows = 100_000;
    let batch_size = 50;
    let num_users = 100;
    let (cfg, _dir) = make_config(REDUCER_EVENT_RULES_SCHEMA, backend);
    let mut db = DeltaDb::open(cfg).unwrap();

    let rows: Vec<RowMap> = (0..total_rows)
        .map(|i| {
            let user = format!("user{}", i % num_users);
            let side = if i / num_users < 5 {
                "buy"
            } else if i % 3 == 0 {
                "sell"
            } else {
                "buy"
            };
            make_trade(
                &user,
                side,
                1.0 + (i as f64 * 0.01),
                2000.0 + (i as f64 * 0.1),
            )
        })
        .collect();

    let start = Instant::now();
    for (block, chunk) in rows.chunks(batch_size).enumerate() {
        db.process_batch("trades", block as u64, chunk.to_vec())
            .unwrap();
    }
    db.flush();
    let elapsed = start.elapsed();

    let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
    BenchResult {
        name: format!("Full pipeline — Event Rules [{}]", backend),
        backend: backend.to_string(),
        total_rows,
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        rows_per_sec,
        pass: rows_per_sec > 50_000.0,
        target: ">50K rows/sec".to_string(),
    }
}

fn bench_full_pipeline_lua(backend: Backend) -> BenchResult {
    let total_rows = 50_000;
    let batch_size = 50;
    let num_users = 100;
    let (cfg, _dir) = make_config(REDUCER_LUA_SCHEMA, backend);
    let mut db = DeltaDb::open(cfg).unwrap();

    let rows: Vec<RowMap> = (0..total_rows)
        .map(|i| {
            let user = format!("user{}", i % num_users);
            let side = if i / num_users < 5 {
                "buy"
            } else if i % 3 == 0 {
                "sell"
            } else {
                "buy"
            };
            make_trade(
                &user,
                side,
                1.0 + (i as f64 * 0.01),
                2000.0 + (i as f64 * 0.1),
            )
        })
        .collect();

    let start = Instant::now();
    for (block, chunk) in rows.chunks(batch_size).enumerate() {
        db.process_batch("trades", block as u64, chunk.to_vec())
            .unwrap();
    }
    db.flush();
    let elapsed = start.elapsed();

    let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
    BenchResult {
        name: format!("Full pipeline — Lua [{}]", backend),
        backend: backend.to_string(),
        total_rows,
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        rows_per_sec,
        pass: rows_per_sec > 30_000.0,
        target: ">30K rows/sec".to_string(),
    }
}

fn bench_full_pipeline_fn_reducer(backend: Backend) -> BenchResult {
    let total_rows = 50_000;
    let batch_size = 50;
    let num_users = 100;
    let (cfg, _dir) = make_config(REDUCER_FN_SCHEMA, backend);
    let mut db = DeltaDb::open(cfg).unwrap();

    // Inject the FnReducer runtime
    db.set_reducer_runtime("pnl", Box::new(pnl_fn_runtime()));

    let rows: Vec<RowMap> = (0..total_rows)
        .map(|i| {
            let user = format!("user{}", i % num_users);
            let side = if i / num_users < 5 {
                "buy"
            } else if i % 3 == 0 {
                "sell"
            } else {
                "buy"
            };
            make_trade(
                &user,
                side,
                1.0 + (i as f64 * 0.01),
                2000.0 + (i as f64 * 0.1),
            )
        })
        .collect();

    let start = Instant::now();
    for (block, chunk) in rows.chunks(batch_size).enumerate() {
        db.process_batch("trades", block as u64, chunk.to_vec())
            .unwrap();
    }
    db.flush();
    let elapsed = start.elapsed();

    let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
    BenchResult {
        name: format!("Full pipeline — FnReducer [{}]", backend),
        backend: backend.to_string(),
        total_rows,
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        rows_per_sec,
        pass: rows_per_sec > 30_000.0,
        target: ">30K rows/sec".to_string(),
    }
}

fn bench_reducer_event_rules_only() -> BenchResult {
    let total_rows = 200_000;
    let batch_size = 100;
    let num_users = 100;

    let schema = parse_schema(REDUCER_EVENT_RULES_SCHEMA).unwrap();
    let storage = Arc::new(MemoryBackend::new());
    let reducer_def = schema.reducers[0].clone();
    let source_table = &schema.tables[0];
    let source_names: Vec<String> = source_table
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect();
    let source_registry = ColumnRegistry::new(source_names);
    let mut engine = ReducerEngine::new(reducer_def, storage, &source_registry, &[]);

    let rows: Vec<RowMap> = (0..total_rows)
        .map(|i| {
            let user = format!("user{}", i % num_users);
            let side = if i / num_users < 5 {
                "buy"
            } else if i % 3 == 0 {
                "sell"
            } else {
                "buy"
            };
            make_trade(
                &user,
                side,
                1.0 + (i as f64 * 0.01),
                2000.0 + (i as f64 * 0.1),
            )
        })
        .collect();

    let start = Instant::now();
    for (block, chunk) in rows.chunks(batch_size).enumerate() {
        engine.process_block_maps(block as u64, chunk).unwrap();
    }
    let elapsed = start.elapsed();

    let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
    BenchResult {
        name: "Reducer-only — Event Rules [Memory]".to_string(),
        backend: "Memory".to_string(),
        total_rows,
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        rows_per_sec,
        pass: rows_per_sec > 200_000.0,
        target: ">200K rows/sec".to_string(),
    }
}

fn bench_reducer_lua_only() -> BenchResult {
    let total_rows = 200_000;
    let batch_size = 100;
    let num_users = 100;

    let schema = parse_schema(REDUCER_LUA_SCHEMA).unwrap();
    let storage = Arc::new(MemoryBackend::new());
    let reducer_def = schema.reducers[0].clone();
    let source_table = &schema.tables[0];
    let source_names: Vec<String> = source_table
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect();
    let source_registry = ColumnRegistry::new(source_names);
    let mut engine = ReducerEngine::new(reducer_def, storage, &source_registry, &[]);

    let rows: Vec<RowMap> = (0..total_rows)
        .map(|i| {
            let user = format!("user{}", i % num_users);
            let side = if i / num_users < 5 {
                "buy"
            } else if i % 3 == 0 {
                "sell"
            } else {
                "buy"
            };
            make_trade(
                &user,
                side,
                1.0 + (i as f64 * 0.01),
                2000.0 + (i as f64 * 0.1),
            )
        })
        .collect();

    let start = Instant::now();
    for (block, chunk) in rows.chunks(batch_size).enumerate() {
        engine.process_block_maps(block as u64, chunk).unwrap();
    }
    let elapsed = start.elapsed();

    let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
    BenchResult {
        name: "Reducer-only — Lua [Memory]".to_string(),
        backend: "Memory".to_string(),
        total_rows,
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        rows_per_sec,
        pass: rows_per_sec > 100_000.0,
        target: ">100K rows/sec".to_string(),
    }
}

fn bench_reducer_fn_only() -> BenchResult {
    let total_rows = 200_000;
    let batch_size = 100;
    let num_users = 100;

    let schema = parse_schema(REDUCER_FN_SCHEMA).unwrap();
    let storage = Arc::new(MemoryBackend::new());
    let reducer_def = schema.reducers[0].clone();
    let source_table = &schema.tables[0];
    let source_names: Vec<String> = source_table
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect();
    let source_registry = ColumnRegistry::new(source_names);
    let mut engine = ReducerEngine::with_runtime(
        reducer_def,
        storage,
        &source_registry,
        Box::new(pnl_fn_runtime()),
    );

    let rows: Vec<RowMap> = (0..total_rows)
        .map(|i| {
            let user = format!("user{}", i % num_users);
            let side = if i / num_users < 5 {
                "buy"
            } else if i % 3 == 0 {
                "sell"
            } else {
                "buy"
            };
            make_trade(
                &user,
                side,
                1.0 + (i as f64 * 0.01),
                2000.0 + (i as f64 * 0.1),
            )
        })
        .collect();

    let start = Instant::now();
    for (block, chunk) in rows.chunks(batch_size).enumerate() {
        engine.process_block_maps(block as u64, chunk).unwrap();
    }
    let elapsed = start.elapsed();

    let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
    BenchResult {
        name: "Reducer-only — FnReducer [Memory]".to_string(),
        backend: "Memory".to_string(),
        total_rows,
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        rows_per_sec,
        pass: rows_per_sec > 100_000.0,
        target: ">100K rows/sec".to_string(),
    }
}

fn bench_rollback(backend: Backend) -> BenchResult {
    let num_blocks = 75;
    let rows_per_block = 134; // ~10K total rows
    let num_users = 50;
    let (cfg, _dir) = make_config(REDUCER_EVENT_RULES_SCHEMA, backend);
    let mut db = DeltaDb::open(cfg).unwrap();

    let total_rows = num_blocks * rows_per_block;
    for block in 1..=num_blocks as u64 {
        let rows: Vec<RowMap> = (0..rows_per_block)
            .map(|i| {
                let idx = (block as usize - 1) * rows_per_block + i;
                let user = format!("user{}", idx % num_users);
                make_trade(&user, "buy", 1.0, 2000.0)
            })
            .collect();
        db.process_batch("trades", block, rows).unwrap();
    }
    db.flush();

    let start = Instant::now();
    db.rollback(0).unwrap();
    let _batch = db.flush();
    let elapsed = start.elapsed();

    let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
    BenchResult {
        name: format!("Rollback 75 blocks, {total_rows} rows [{}]", backend),
        backend: backend.to_string(),
        total_rows,
        elapsed_ms,
        rows_per_sec: total_rows as f64 / elapsed.as_secs_f64(),
        pass: elapsed_ms < 10.0,
        target: "<10ms".to_string(),
    }
}

fn bench_ingest(backend: Backend) -> BenchResult {
    let total_rows = 100_000;
    let batch_size = 5_000;
    let (cfg, _dir) = make_config(RAW_WITH_MV_SCHEMA, backend);
    let mut db = DeltaDb::open(cfg).unwrap();

    let rows: Vec<RowMap> = (0..total_rows).map(make_raw_row_for_mv).collect();

    // Group into per-block batches, each batch becomes one ingest() call
    let blocks: Vec<Vec<RowMap>> = rows.chunks(batch_size).map(|c| c.to_vec()).collect();

    let start = Instant::now();
    for (block_num, block_rows) in blocks.iter().enumerate() {
        let block = block_num as u64;
        let mut data = HashMap::new();
        // Add block_number to each row (ingest requires it)
        let rows_with_bn: Vec<RowMap> = block_rows
            .iter()
            .map(|r| {
                let mut r = r.clone();
                r.insert("block_number".to_string(), Value::UInt64(block));
                r
            })
            .collect();
        data.insert("events".to_string(), rows_with_bn);

        let batch = db
            .ingest(delta_db::db::IngestInput {
                data,
                rollback_chain: vec![delta_db::types::BlockCursor {
                    number: block,
                    hash: format!("0x{block:x}"),
                }],
                finalized_head: delta_db::types::BlockCursor {
                    number: if block > 0 { block - 1 } else { 0 },
                    hash: format!("0x{:x}", if block > 0 { block - 1 } else { 0 }),
                },
            })
            .unwrap();

        if let Some(b) = batch {
            db.ack(b.sequence);
        }
    }
    let elapsed = start.elapsed();

    let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
    BenchResult {
        name: format!("Ingest (Raw + MV + persist) [{}]", backend),
        backend: backend.to_string(),
        total_rows,
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        rows_per_sec,
        pass: rows_per_sec > 20_000.0,
        target: ">20K rows/sec".to_string(),
    }
}

fn bench_many_group_keys(backend: Backend) -> BenchResult {
    let num_keys = 100_000;
    let batch_size = 1000;
    let (cfg, _dir) = make_config(REDUCER_EVENT_RULES_SCHEMA, backend);
    let mut db = DeltaDb::open(cfg).unwrap();

    let start = Instant::now();
    for batch_idx in 0..(num_keys / batch_size) {
        let rows: Vec<RowMap> = (0..batch_size)
            .map(|i| {
                let user = format!("user{}", batch_idx * batch_size + i);
                make_trade(&user, "buy", 1.0, 2000.0)
            })
            .collect();
        db.process_batch("trades", batch_idx as u64, rows).unwrap();
    }
    let elapsed = start.elapsed();

    BenchResult {
        name: format!("{num_keys} unique group keys [{}]", backend),
        backend: backend.to_string(),
        total_rows: num_keys,
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        rows_per_sec: num_keys as f64 / elapsed.as_secs_f64(),
        pass: true,
        target: "baseline".to_string(),
    }
}

// ─── Polymarket schemas ───────────────────────────────────────────

const POLYMARKET_FULL_SCHEMA: &str = include_str!("../tests/polymarket/schema.sql");

const POLYMARKET_MARKET_STATS_ONLY: &str = r#"
CREATE VIRTUAL TABLE orders (
    block_number UInt64,
    timestamp    UInt64,
    trader       String,
    asset_id     String,
    usdc         UInt64,
    shares       UInt64,
    side         UInt64
);

CREATE REDUCER market_stats
SOURCE orders
GROUP BY asset_id
STATE (
    volume      Float64 DEFAULT 0,
    trades      UInt64  DEFAULT 0,
    sum_price   Float64 DEFAULT 0,
    sum_price_sq Float64 DEFAULT 0,
    first_seen  UInt64  DEFAULT 0,
    last_seen   UInt64  DEFAULT 0
)
LANGUAGE lua
PROCESS $$
    if row.shares == 0 then return end
    local price = row.usdc / row.shares
    local vol = row.usdc / 1000000
    state.volume = state.volume + vol
    state.trades = state.trades + 1
    state.sum_price = state.sum_price + price
    state.sum_price_sq = state.sum_price_sq + price * price
    if state.first_seen == 0 then state.first_seen = row.timestamp end
    state.last_seen = row.timestamp
    emit.asset_id = row.asset_id
    emit.volume = vol
    emit.price = price
    emit.price_sq = price * price
$$;

CREATE MATERIALIZED VIEW token_summary AS
SELECT
    asset_id,
    sum(volume)    AS total_volume,
    count()        AS trade_count,
    last(price)    AS last_price,
    sum(price)     AS sum_price,
    sum(price_sq)  AS sum_price_sq
FROM market_stats
GROUP BY asset_id;
"#;

const POLYMARKET_INSIDER_ONLY: &str = r#"
CREATE VIRTUAL TABLE orders (
    block_number UInt64,
    timestamp    UInt64,
    trader       String,
    asset_id     String,
    usdc         UInt64,
    shares       UInt64,
    side         UInt64
);

CREATE REDUCER insider_classifier
SOURCE orders
GROUP BY trader
STATE (
    status       String  DEFAULT 'unknown',
    window_start UInt64  DEFAULT 0,
    window_vol   UInt64  DEFAULT 0,
    window_trades UInt64 DEFAULT 0,
    positions    Json    DEFAULT '{}'
)
LANGUAGE lua
PROCESS $$
    if row.shares == 0 then return end
    local FIFTEEN_MIN = 900
    local VOLUME_THRESHOLD = 4000000000
    local MIN_PRICE_BPS = 9500
    local BPS_SCALE = 10000
    if row.side ~= 0 then return end
    if row.usdc * BPS_SCALE >= row.shares * MIN_PRICE_BPS then return end
    if state.status ~= "unknown" then
        if state.status == "insider" then
            local price = row.usdc / row.shares
            emit {
                trader = row.trader,
                asset_id = row.asset_id,
                volume = row.usdc / 1000000,
                price = price,
                price_sq = price * price,
                timestamp = row.timestamp,
                detected_at = row.timestamp
            }
        end
        return
    end
    if state.window_start == 0 then
        state.window_start = row.timestamp
    elseif row.timestamp - state.window_start > FIFTEEN_MIN then
        state.status = "clean"
        return
    end
    state.window_vol = state.window_vol + row.usdc
    state.window_trades = state.window_trades + 1
    local token = row.asset_id
    local price = row.usdc / row.shares
    local vol = row.usdc / 1000000
    local pos = state.positions[token]
    if not pos then
        pos = { volume = 0, trades = 0, sum_price = 0, sum_price_sq = 0,
                first_seen = row.timestamp, last_seen = row.timestamp }
    end
    pos.volume = pos.volume + vol
    pos.trades = pos.trades + 1
    pos.sum_price = pos.sum_price + price
    pos.sum_price_sq = pos.sum_price_sq + price * price
    if row.timestamp < pos.first_seen then pos.first_seen = row.timestamp end
    if row.timestamp > pos.last_seen then pos.last_seen = row.timestamp end
    state.positions[token] = pos
    if state.window_vol >= VOLUME_THRESHOLD then
        state.status = "insider"
        for tid, p in pairs(state.positions) do
            emit {
                trader = row.trader,
                asset_id = tid,
                volume = p.volume,
                price = p.sum_price / p.trades,
                price_sq = p.sum_price_sq / p.trades,
                timestamp = p.first_seen,
                detected_at = row.timestamp
            }
        end
    end
$$;

CREATE MATERIALIZED VIEW insider_positions AS
SELECT
    trader,
    asset_id,
    sum(volume)      AS total_volume,
    count()          AS trade_count,
    sum(price)       AS sum_price,
    sum(price_sq)    AS sum_price_sq,
    first(timestamp) AS first_seen,
    last(timestamp)  AS last_seen,
    first(detected_at) AS detected_at
FROM insider_classifier
GROUP BY trader, asset_id;
"#;

// ─── Polymarket data generation ───────────────────────────────────

/// Generate a realistic Polymarket order for benchmarking.
///
/// Distribution (per plan):
/// - 80% of orders from 10% of traders (power law)
/// - 60/40 BUY/SELL split
/// - 70% price above 0.95 (filtered by insider logic), 30% below
/// - ~10K unique asset_ids
fn make_polymarket_order(i: usize, num_traders: usize) -> RowMap {
    // Power law: 80% of orders from top 10% of traders
    let top_traders = num_traders / 10;
    let remaining = num_traders - top_traders;
    let trader_idx = if i % 5 < 4 {
        i % top_traders
    } else {
        top_traders + (i % remaining)
    };
    let trader = format!("0xtrader{trader_idx:06x}");

    // ~10K unique tokens
    let asset_id = format!("token_{:04}", i % 10_000);

    // 60/40 BUY/SELL
    let side: u64 = if i % 5 < 3 { 0 } else { 1 };

    // Price distribution: 70% above 0.95, 30% below
    let (usdc, shares) = if i % 10 < 7 {
        // High price: 0.96-0.99 (filtered out by insider logic)
        let price_bps = 9600 + (i % 400); // 0.96 to 0.9999
        let shares_val = 1_000_000_000u64;
        let usdc_val = shares_val * price_bps as u64 / 10_000;
        (usdc_val, shares_val)
    } else {
        // Low price: 0.30-0.90 (tracked by insider logic)
        let price_bps = 3000 + (i % 6000); // 0.30 to 0.8999
        let shares_val = 1_000_000_000u64;
        let usdc_val = shares_val * price_bps as u64 / 10_000;
        (usdc_val, shares_val)
    };

    // Timestamp: monotonically increasing, all within 15-min windows
    let timestamp = 1_000_000 + (i as u64 / 500); // increments every 500 orders

    HashMap::from([
        ("trader".to_string(), Value::String(trader)),
        ("asset_id".to_string(), Value::String(asset_id)),
        ("usdc".to_string(), Value::UInt64(usdc)),
        ("shares".to_string(), Value::UInt64(shares)),
        ("side".to_string(), Value::UInt64(side)),
        ("timestamp".to_string(), Value::UInt64(timestamp)),
    ])
}

// ─── Polymarket benchmarks ────────────────────────────────────────

/// market_stats reducer only — GROUP BY asset_id, ~10K unique tokens.
fn bench_polymarket_market_stats(backend: Backend) -> BenchResult {
    let total_rows = 200_000;
    let batch_size = 500;
    let (cfg, _dir) = make_config(POLYMARKET_MARKET_STATS_ONLY, backend);
    let mut db = DeltaDb::open(cfg).unwrap();

    let rows: Vec<RowMap> = (0..total_rows)
        .map(|i| make_polymarket_order(i, 10_000))
        .collect();

    let start = Instant::now();
    for (block, chunk) in rows.chunks(batch_size).enumerate() {
        db.process_batch("orders", block as u64, chunk.to_vec())
            .unwrap();
    }
    db.flush();
    let elapsed = start.elapsed();

    let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
    BenchResult {
        name: format!("Polymarket: market_stats only [{}]", backend),
        backend: backend.to_string(),
        total_rows,
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        rows_per_sec,
        pass: rows_per_sec > 160_000.0,
        target: ">160K rows/sec".to_string(),
    }
}

/// insider_classifier reducer only — GROUP BY trader, 100K unique traders.
fn bench_polymarket_insider_detect(backend: Backend) -> BenchResult {
    let total_rows = 200_000;
    let batch_size = 500;
    let num_traders = 100_000;
    let (cfg, _dir) = make_config(POLYMARKET_INSIDER_ONLY, backend);
    let mut db = DeltaDb::open(cfg).unwrap();

    let rows: Vec<RowMap> = (0..total_rows)
        .map(|i| make_polymarket_order(i, num_traders))
        .collect();

    let start = Instant::now();
    for (block, chunk) in rows.chunks(batch_size).enumerate() {
        db.process_batch("orders", block as u64, chunk.to_vec())
            .unwrap();
    }
    db.flush();
    let elapsed = start.elapsed();

    let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
    BenchResult {
        name: format!("Polymarket: insider_classifier only [{}]", backend),
        backend: backend.to_string(),
        total_rows,
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        rows_per_sec,
        pass: rows_per_sec > 300_000.0,
        target: ">300K rows/sec".to_string(),
    }
}

/// Full pipeline — both reducers + MVs, realistic data distribution.
fn bench_polymarket_full_pipeline(backend: Backend) -> BenchResult {
    let total_rows = 200_000;
    let batch_size = 500;
    let num_traders = 100_000;
    let (cfg, _dir) = make_config(POLYMARKET_FULL_SCHEMA, backend);
    let mut db = DeltaDb::open(cfg).unwrap();

    let rows: Vec<RowMap> = (0..total_rows)
        .map(|i| make_polymarket_order(i, num_traders))
        .collect();

    let start = Instant::now();
    for (block, chunk) in rows.chunks(batch_size).enumerate() {
        db.process_batch("orders", block as u64, chunk.to_vec())
            .unwrap();
    }
    db.flush();
    let elapsed = start.elapsed();

    let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
    BenchResult {
        name: format!("Polymarket: full pipeline [{}]", backend),
        backend: backend.to_string(),
        total_rows,
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        rows_per_sec,
        pass: rows_per_sec > 150_000.0,
        target: ">150K rows/sec".to_string(),
    }
}

/// High cardinality — 1M unique traders, measure throughput degradation.
fn bench_polymarket_high_cardinality(backend: Backend) -> BenchResult {
    let total_rows = 500_000;
    let batch_size = 1000;
    let num_traders = 1_000_000;
    let (cfg, _dir) = make_config(POLYMARKET_FULL_SCHEMA, backend);
    let mut db = DeltaDb::open(cfg).unwrap();

    let rows: Vec<RowMap> = (0..total_rows)
        .map(|i| make_polymarket_order(i, num_traders))
        .collect();

    let start = Instant::now();
    for (block, chunk) in rows.chunks(batch_size).enumerate() {
        db.process_batch("orders", block as u64, chunk.to_vec())
            .unwrap();
    }
    db.flush();
    let elapsed = start.elapsed();

    let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
    BenchResult {
        name: format!("Polymarket: 1M traders [{}]", backend),
        backend: backend.to_string(),
        total_rows,
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        rows_per_sec,
        pass: rows_per_sec > 75_000.0,
        target: ">75K rows/sec".to_string(),
    }
}

fn run_backend_benchmarks(backend: Backend) -> Vec<BenchResult> {
    let mut results = Vec::new();

    println!("  Core:");
    let r = bench_raw_ingestion(backend);
    r.print();
    results.push(r);
    let r = bench_raw_with_mv(backend);
    r.print();
    results.push(r);
    let r = bench_full_pipeline_event_rules(backend);
    r.print();
    results.push(r);
    let r = bench_full_pipeline_lua(backend);
    r.print();
    results.push(r);
    let r = bench_full_pipeline_fn_reducer(backend);
    r.print();
    results.push(r);
    let r = bench_rollback(backend);
    r.print();
    results.push(r);
    let r = bench_ingest(backend);
    r.print();
    results.push(r);
    let r = bench_many_group_keys(backend);
    r.print();
    results.push(r);

    println!("\n  Polymarket:");
    let r = bench_polymarket_market_stats(backend);
    r.print();
    results.push(r);
    let r = bench_polymarket_insider_detect(backend);
    r.print();
    results.push(r);
    let r = bench_polymarket_full_pipeline(backend);
    r.print();
    results.push(r);
    let r = bench_polymarket_high_cardinality(backend);
    r.print();
    results.push(r);

    results
}

fn main() {
    println!("=== Delta DB Benchmarks ===\n");

    let mut results: Vec<BenchResult> = Vec::new();

    println!("--- Memory ---");
    results.extend(run_backend_benchmarks(Backend::Memory));
    println!("\n  Reducer-only:");
    let r = bench_reducer_event_rules_only();
    r.print();
    results.push(r);
    let r = bench_reducer_lua_only();
    r.print();
    results.push(r);
    let r = bench_reducer_fn_only();
    r.print();
    results.push(r);

    println!("\n--- RocksDB ---");
    results.extend(run_backend_benchmarks(Backend::RocksDb));

    println!("\n=== Summary ===\n");

    let all_pass = results.iter().all(|r| r.pass);
    if all_pass {
        println!("All benchmarks PASSED.");
    } else {
        let failed: Vec<_> = results.iter().filter(|r| !r.pass).collect();
        println!("{} benchmark(s) FAILED:", failed.len());
        for r in &failed {
            println!(
                "  - {}: {:.0} rows/s (target: {})",
                r.name, r.rows_per_sec, r.target
            );
        }
    }
}
