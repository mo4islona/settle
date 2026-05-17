//! Aggregator for integration tests against delta-db's RocksDB-backed
//! `Settle` API. No external dependencies — these tests verify delta-db's
//! internal contracts (buffer/sequence semantics, drop+reopen recovery,
//! dedup guard, fork detection, pending/ack state machine). For true
//! end-to-end coverage with a Postgres target sink see `tests/e2e/`.

mod common;

mod ack_durability;
mod buffering;
mod crash_recovery;
mod dedup;
mod fork;
