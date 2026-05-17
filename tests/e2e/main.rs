//! Aggregator for true end-to-end tests against a real Postgres target
//! sink (spun up via testcontainers on every test). Requires a running
//! Docker daemon. For tests that verify delta-db's internal contracts
//! without a target sink, see `tests/integration/`.

mod common;

mod ack_durability;
mod fork;
