//! Postgres target-sink fixture for end-to-end tests.
//!
//! Each `start_pg()` call spins up a fresh `postgres:16-alpine` container via
//! `testcontainers` and creates the `token_summary` table that mirrors the
//! materialized view in the shared schema (`super::SCHEMA`). The harness
//! keeps the container alive for the lifetime of the returned `PgFixture`.
//!
//! Requires a running Docker daemon. If Docker is unavailable `start_pg()`
//! panics (via `.expect(...)`) — cargo surfaces this as a test failure
//! with the docker-error message in the panic payload, making the cause
//! obvious in CI logs. To skip these tests entirely, filter with
//! `cargo test --test integration`.

use settle::types::{ChangeBatch, ChangeOp, Value};
use testcontainers::{runners::AsyncRunner, ContainerAsync};
use testcontainers_modules::postgres::Postgres as PgImage;
use tokio_postgres::{Client, NoTls};

pub struct PgFixture {
    _container: ContainerAsync<PgImage>,
    pub client: Client,
}

pub async fn start_pg() -> PgFixture {
    let container = PgImage::default()
        .start()
        .await
        .expect("start postgres container (is Docker running?)");
    let host = container.get_host().await.expect("pg host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("pg port");
    let conn_str =
        format!("host={host} port={port} user=postgres password=postgres dbname=postgres");

    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect to postgres");
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("postgres connection error: {e}");
        }
    });

    client
        .batch_execute(
            "CREATE TABLE token_summary (
                asset_id TEXT PRIMARY KEY,
                total_volume BIGINT NOT NULL,
                trade_count BIGINT NOT NULL
            )",
        )
        .await
        .expect("create token_summary table");

    PgFixture {
        _container: container,
        client,
    }
}

/// Apply a single `ChangeBatch` to Postgres atomically: one transaction per
/// batch, idempotent upserts for Insert/Update, deletes for Delete. Either
/// the whole batch lands or nothing does — satisfies the caller-contract
/// documented in the durability spec.
pub async fn apply_batch(client: &mut Client, batch: &ChangeBatch) -> Result<(), tokio_postgres::Error> {
    let tx = client.transaction().await?;
    if let Some(records) = batch.tables.get("token_summary") {
        for rec in records {
            let asset_id = match rec.key.get("asset_id") {
                Some(Value::String(s)) => s.clone(),
                other => panic!("unexpected asset_id key: {other:?}"),
            };
            match rec.operation {
                ChangeOp::Insert | ChangeOp::Update => {
                    let total = num_u64(rec.values.get("total_volume")) as i64;
                    let count = num_u64(rec.values.get("trade_count")) as i64;
                    tx.execute(
                        "INSERT INTO token_summary (asset_id, total_volume, trade_count)
                         VALUES ($1, $2, $3)
                         ON CONFLICT (asset_id) DO UPDATE
                            SET total_volume = EXCLUDED.total_volume,
                                trade_count = EXCLUDED.trade_count",
                        &[&asset_id, &total, &count],
                    )
                    .await?;
                }
                ChangeOp::Delete => {
                    tx.execute(
                        "DELETE FROM token_summary WHERE asset_id = $1",
                        &[&asset_id],
                    )
                    .await?;
                }
            }
        }
    }
    tx.commit().await
}

pub async fn pg_row(client: &Client, asset: &str) -> Option<(i64, i64)> {
    client
        .query_opt(
            "SELECT total_volume, trade_count FROM token_summary WHERE asset_id = $1",
            &[&asset],
        )
        .await
        .expect("query pg")
        .map(|r| (r.get::<_, i64>(0), r.get::<_, i64>(1)))
}

fn num_u64(v: Option<&Value>) -> u64 {
    match v {
        Some(Value::UInt64(n)) => *n,
        Some(Value::Int64(n)) => *n as u64,
        Some(Value::Float64(f)) => *f as u64,
        other => panic!("expected numeric value, got {other:?}"),
    }
}
