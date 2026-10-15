//! Standalone Settle MV-only (no reducer) for profiling.
//! Run: samply record cargo bench --bench profile_settle_mv_only

use settle::db::{Config, Settle};
use settle::test_helpers::ingest_blocks;
use settle::types::RowMap;

#[path = "common/mod.rs"]
mod common;

use common::{gen_transfer, split_blocks, BATCH_SIZE, BLOCKS_PER_BATCH};

const SETTLE_SCHEMA_MV: &str = r#"
CREATE TABLE transfers (
    block_number UInt64,
    from_addr    String,
    to_addr      String,
    value        Float64
);

CREATE MATERIALIZED VIEW balances AS
SELECT
    from_addr,
    sum(value) AS balance
FROM transfers
GROUP BY from_addr;
"#;

const TOTAL_ROWS: usize = 500_000;
const NUM_USERS: usize = 10_000;

fn main() -> anyhow::Result<()> {
    let rows: Vec<RowMap> = (0..TOTAL_ROWS)
        .map(|i| gen_transfer(i, NUM_USERS))
        .collect();

    let dir = tempfile::tempdir()?;
    let cfg = Config::with_data_dir(SETTLE_SCHEMA_MV, dir.path().to_str().unwrap());
    let mut db = Settle::open(cfg)?;

    let t = std::time::Instant::now();
    let mut block_no = 1u64;
    for chunk in rows.chunks(BATCH_SIZE) {
        let items: Vec<(String, u64, Vec<RowMap>)> = split_blocks(chunk, block_no)
            .into_iter()
            .map(|(b, c)| ("transfers".to_string(), b, c.to_vec()))
            .collect();
        ingest_blocks(&mut db, items)?;
        block_no += BLOCKS_PER_BATCH as u64;
    }
    let elapsed = t.elapsed();
    eprintln!(
        "settle MV-only: {TOTAL_ROWS} rows in {:.3}s = {:.0} rows/s",
        elapsed.as_secs_f64(),
        TOTAL_ROWS as f64 / elapsed.as_secs_f64()
    );
    Ok(())
}
