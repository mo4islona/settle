//! E2E: pipelined acks against a real Postgres target. Settle ingests the
//! next blocks while a separate task applies earlier batches to Postgres
//! and sends back their sequences for `ack`. The order of the two sides is
//! up to the scheduler; the target must end the same in every order.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use settle::db::{Config, IngestInput, Settle};
use settle::test_helpers::cursor;
use settle::types::{BlockCursor, BlockNumber, ChangeBatch, Value};
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_postgres::Client;

use super::common::pg::{apply_batch, pg_row, start_pg};
use super::common::{order, SCHEMA};

const DEPTH: usize = 3;
const FORK_POINT: BlockNumber = 6;

#[derive(Clone)]
struct Block {
    number: BlockNumber,
    hash: String,
    /// The finalized head sent with this block. It trails the tip by five
    /// blocks, so the fork stays above it, and never goes back.
    finalized: BlockNumber,
    asset: &'static str,
    amount: u64,
}

/// Blocks 1..=10. Asset D is only in blocks above the fork point.
fn old_chain() -> Vec<Block> {
    (1..=10)
        .map(|n| Block {
            number: n,
            hash: cursor(n).hash,
            finalized: n.saturating_sub(5),
            asset: if n > FORK_POINT { "D" } else if n % 2 == 0 { "A" } else { "B" },
            amount: n,
        })
        .collect()
}

/// The old chain up to the fork point, then new blocks up to `last`.
/// Blocks 7..=9 have an asset each that no later block touches, so a lost
/// batch of theirs stays visible in the target.
fn new_chain(last: BlockNumber) -> Vec<Block> {
    let old_finalized = old_chain().last().unwrap().finalized;
    let new_blocks = (FORK_POINT + 1..=last).map(move |n| Block {
        number: n,
        hash: format!("0xfork{n}"),
        finalized: n.saturating_sub(5).max(old_finalized),
        asset: match n {
            7 => "E",
            8 => "F",
            9 => "G",
            _ if n % 2 == 0 => "A",
            _ => "B",
        },
        amount: n * 100,
    });
    old_chain()
        .into_iter()
        .take(FORK_POINT as usize)
        .chain(new_blocks)
        .collect()
}

fn input(block: &Block, chain: &[Block]) -> IngestInput {
    let finalized = block.finalized;
    let finalized_head = match chain.iter().find(|b| b.number == finalized) {
        Some(b) => BlockCursor {
            number: finalized,
            hash: b.hash.clone(),
        },
        None => cursor(finalized),
    };

    let mut row = order(block.asset, block.amount);
    row.insert("block_number".into(), Value::UInt64(block.number));
    IngestInput {
        data: HashMap::from([("orders".to_string(), vec![row])]),
        rollback_chain: vec![BlockCursor {
            number: block.number,
            hash: block.hash.clone(),
        }],
        finalized_head,
    }
}

fn chain_cursors(chain: &[Block]) -> Vec<BlockCursor> {
    chain
        .iter()
        .map(|b| BlockCursor {
            number: b.number,
            hash: b.hash.clone(),
        })
        .collect()
}

/// Asset → (total volume, trade count) after `chain`.
fn expected(chain: &[Block]) -> BTreeMap<&'static str, (i64, i64)> {
    let mut rows = BTreeMap::new();
    for block in chain {
        let (total, count) = rows.entry(block.asset).or_insert((0, 0));
        *total += block.amount as i64;
        *count += 1;
    }
    rows
}

async fn assert_target(client: &Client, chain: &[Block]) {
    let expected = expected(chain);
    for asset in ["A", "B", "D", "E", "F", "G"] {
        let want = expected.get(asset).copied();
        assert_eq!(pg_row(client, asset).await, want, "asset {asset}");
    }
}

fn open(dir: &Path) -> Settle {
    let config = Config::with_data_dir(SCHEMA, dir.to_str().unwrap()).max_pending_batches(DEPTH);
    Settle::open(config).unwrap()
}

/// Settle on this thread, the Postgres apply on a task.
struct Pipeline {
    db: Settle,
    batches: mpsc::UnboundedSender<ChangeBatch>,
    applied: mpsc::UnboundedReceiver<u64>,
    applier: JoinHandle<Client>,
    stopped: Arc<AtomicBool>,
}

impl Pipeline {
    fn start(db: Settle, client: Client) -> Self {
        let (batches, mut to_apply) = mpsc::unbounded_channel::<ChangeBatch>();
        let (applied_tx, applied) = mpsc::unbounded_channel();
        let stopped = Arc::new(AtomicBool::new(false));
        let stop = stopped.clone();
        let applier = tokio::spawn(async move {
            let mut client = client;
            while let Some(batch) = to_apply.recv().await {
                if stop.load(Ordering::SeqCst) {
                    break;
                }
                apply_batch(&mut client, &batch).await.expect("apply");
                let _ = applied_tx.send(batch.sequence);
            }
            client
        });
        Self {
            db,
            batches,
            applied,
            applier,
            stopped,
        }
    }

    /// Ack what the target applied; wait while `DEPTH` batches are unacked.
    async fn make_room(&mut self) {
        while let Ok(sequence) = self.applied.try_recv() {
            self.db.ack(sequence).unwrap();
        }
        while self.db.unacked_batches() >= DEPTH {
            let sequence = self.applied.recv().await.unwrap();
            self.db.ack(sequence).unwrap();
        }
    }

    async fn ingest(&mut self, input: IngestInput) {
        self.make_room().await;
        if let Some(batch) = self.db.ingest(input).unwrap() {
            self.batches.send(batch).unwrap();
        }
    }

    async fn fork(&mut self, chain: Vec<BlockCursor>) -> BlockNumber {
        self.make_room().await;
        let fork = self.db.handle_fork(chain).unwrap();
        if let Some(batch) = fork.batch {
            self.batches.send(batch).unwrap();
        }
        fork.cursor.number
    }

    async fn drain(mut self) -> Client {
        while self.db.is_awaiting_ack() {
            let sequence = self.applied.recv().await.unwrap();
            self.db.ack(sequence).unwrap();
        }
        drop(self.batches);
        self.applier.await.unwrap()
    }

    /// The process dies: the target stops wherever it is, so of the unacked
    /// batches some are applied and the others are lost.
    async fn crash(self) -> Client {
        self.stopped.store(true, Ordering::SeqCst);
        drop(self.db);
        drop(self.batches);
        self.applier.await.unwrap()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn pipelined_apply_matches_the_chain() {
    let pg = start_pg().await;
    let tmp = TempDir::new().unwrap();
    let mut pipeline = Pipeline::start(open(tmp.path()), pg.client);

    let chain = old_chain();
    for block in &chain {
        pipeline.ingest(input(block, &chain)).await;
    }

    let client = pipeline.drain().await;
    assert_target(&client, &chain).await;
}

/// The fork arrives while batches of the old chain wait for their ack, and
/// the target may not have applied them yet.
#[tokio::test(flavor = "multi_thread")]
async fn fork_rolls_back_unacked_batches_in_target() {
    let pg = start_pg().await;
    let tmp = TempDir::new().unwrap();
    let mut pipeline = Pipeline::start(open(tmp.path()), pg.client);

    let old = old_chain();
    for block in &old {
        pipeline.ingest(input(block, &old)).await;
    }

    let new = new_chain(12);
    let fork_point = pipeline.fork(chain_cursors(&new)).await;
    assert_eq!(fork_point, FORK_POINT);
    for block in &new[FORK_POINT as usize..] {
        pipeline.ingest(input(block, &new)).await;
    }

    let client = pipeline.drain().await;
    assert_target(&client, &new).await;
}

/// A crash loses the unacked batches, applied or not. The reopened instance
/// handles the fork again from its acked state and ingests the rest of the
/// new chain.
#[tokio::test(flavor = "multi_thread")]
async fn crash_with_applied_unacked_batches_then_resume() {
    let pg = start_pg().await;
    let tmp = TempDir::new().unwrap();
    let mut pipeline = Pipeline::start(open(tmp.path()), pg.client);

    let old = old_chain();
    for block in &old {
        pipeline.ingest(input(block, &old)).await;
    }
    let new = new_chain(12);
    pipeline.fork(chain_cursors(&new)).await;
    for block in &new[FORK_POINT as usize..9] {
        pipeline.ingest(input(block, &new)).await;
    }
    let client = pipeline.crash().await;

    let mut pipeline = Pipeline::start(open(tmp.path()), client);
    let resume_after = pipeline.fork(chain_cursors(&new)).await;
    for block in new.iter().filter(|b| b.number > resume_after) {
        pipeline.ingest(input(block, &new)).await;
    }

    let client = pipeline.drain().await;
    assert_target(&client, &new).await;
}
