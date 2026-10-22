//! Scenario benches: an app that people build, on a chain range recorded
//! from the Portal, on Postgres alone and on Settle with Postgres as its
//! target. Record a fixture with `benches/scenarios/fetch.py`, then:
//!
//!     SETTLE_FIXTURES=<dir> cargo bench --bench scenarios
//!
//! Docker is required. `SCENARIO` picks the scenario (erc20_balances or
//! uniswap_v3; default erc20_balances), `SIDES` (pg,settle), `PHASES`
//! (backfill,tip,restart), `RUNS` (5), `INTERVALS` (1: checkpoint intervals
//! of the Settle side), `TIP_BLOCKS` (1000), `BATCH_BYTES` (the pipes SDK
//! default, 10 MiB).

#[path = "common/mod.rs"]
mod common;
#[path = "scenarios/erc20_balances/mod.rs"]
mod erc20_balances;
#[path = "scenarios/fixture.rs"]
mod fixture;
#[path = "scenarios/harness.rs"]
mod harness;
#[path = "scenarios/metrics.rs"]
mod metrics;
#[path = "scenarios/uniswap_v3/mod.rs"]
mod uniswap_v3;

fn main() -> anyhow::Result<()> {
    let scenario = std::env::var("SCENARIO").unwrap_or_else(|_| "erc20_balances".to_string());
    match scenario.as_str() {
        "erc20_balances" => harness::main::<erc20_balances::Erc20Balances>(),
        "uniswap_v3" => harness::main::<uniswap_v3::UniswapV3>(),
        other => anyhow::bail!("unknown scenario {other}"),
    }
}
