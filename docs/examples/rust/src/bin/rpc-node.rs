use std::error::Error;
use std::time::Duration;

use serde_json::{Value as Json, json};
use settle::db::{IngestInput, Settle};
use settle::types::{BlockCursor, ChangeBatch, RowMap, Value};
use settle::{BuildOptions, Pipeline, ViewOptions, string, uint64};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

// [!region rpc]
struct Block {
    number: u64,
    hash: String,
    parent_hash: String,
    miner: String,
    gas_used: u64,
}

struct Node {
    url: String,
}

impl Node {
    fn rpc(&self, method: &str, params: Json) -> Result<Json> {
        let body = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
        let response: Json = ureq::post(&self.url).send_json(body)?.into_json()?;

        if let Some(error) = response.get("error") {
            return Err(error.to_string().into());
        }
        Ok(response["result"].clone())
    }

    /// `tag` is a block number in hex, `latest` or `finalized`.
    fn block(&self, tag: &str) -> Result<Block> {
        let block = self.rpc("eth_getBlockByNumber", json!([tag, false]))?;
        let text = |field: &str| block[field].as_str().unwrap_or_default().to_string();
        let number = |field: &str| u64::from_str_radix(text(field).trim_start_matches("0x"), 16);

        Ok(Block {
            number: number("number")?,
            hash: text("hash"),
            parent_hash: text("parentHash"),
            miner: text("miner"),
            gas_used: number("gasUsed")?,
        })
    }
}

fn tag(number: u64) -> String {
    format!("0x{number:x}")
}
// [!endregion rpc]

// [!region apply]
fn apply(db: &mut Settle, batch: Option<ChangeBatch>) -> Result<()> {
    let Some(batch) = batch else { return Ok(()) };

    for record in batch.tables.get("fee_recipients").into_iter().flatten() {
        let fields: Vec<String> = record.values().map(|(n, v)| format!("{n}: {v}")).collect();
        let operation = format!("{:?}", record.operation).to_lowercase();
        println!("{operation} {{ {} }}", fields.join(", "));
    }

    db.ack(batch.sequence)?;
    Ok(())
}
// [!endregion apply]

// [!region reorg]
fn handle_reorg(node: &Node, db: &mut Settle, from: u64) -> Result<BlockCursor> {
    let finalized = node.block("finalized")?;
    let mut depth = 32;

    loop {
        let lowest = finalized.number.max(from.saturating_sub(depth));

        let mut chain = Vec::new();
        for n in (lowest..=from).rev() {
            let block = node.block(&tag(n))?;
            chain.push(BlockCursor { number: block.number, hash: block.hash });
        }

        // Without a common block, look further back, but not below finality.
        let pairs: Vec<(u64, &str)> = chain.iter().map(|c| (c.number, c.hash.as_str())).collect();
        let found = db.resolve_fork_cursor(&pairs).is_some();

        if found || lowest == finalized.number {
            let fork = db.handle_fork(chain)?;
            apply(db, fork.batch)?;
            return Ok(fork.cursor);
        }

        depth *= 2;
    }
}
// [!endregion reorg]

fn main() -> Result<()> {
    let node = Node {
        url: std::env::var("RPC_URL").unwrap_or("https://ethereum-rpc.publicnode.com".into()),
    };
    let start_block: Option<u64> = std::env::var("START_BLOCK").ok().map(|s| s.parse()).transpose()?;
    let max_blocks: u64 = std::env::var("MAX_BLOCKS").map_or(Ok(u64::MAX), |s| s.parse())?;

    // [!region pipeline]
    let mut p = Pipeline::new();

    p.virtual_table(
        "blocks",
        [("block_number", uint64()), ("fee_recipient", string()), ("gas_used", uint64())],
    )
    .create_view(
        "fee_recipients",
        ViewOptions {
            group_by: vec!["fee_recipient".into()],
            sliding_window: None,
            select: Box::new(|agg| {
                vec![
                    ("fee_recipient".into(), agg.key("fee_recipient").into()),
                    ("blocks".into(), agg.count().into()),
                    ("gas".into(), agg.sum("gas_used").into()),
                ]
            }),
        },
    );

    let mut db = p.build(BuildOptions::new().data_dir("./settle-data"))?;
    // [!endregion pipeline]

    // [!region loop]
    let mut last = db.latest_cursor();
    let mut next = match &last {
        Some(cursor) => cursor.number + 1,
        None => match start_block {
            Some(block) => block,
            None => node.block("latest")?.number,
        },
    };

    let mut count = 0;
    while count < max_blocks {
        let head = node.block("latest")?;
        if next > head.number {
            std::thread::sleep(Duration::from_secs(1));
            continue;
        }

        let block = node.block(&tag(next))?;

        if let Some(previous) = &last {
            if block.parent_hash != previous.hash {
                let fork_point = handle_reorg(&node, &mut db, previous.number)?;
                println!("fork point: {}", fork_point.number);
                next = fork_point.number + 1;
                last = Some(fork_point);
                continue;
            }
        }

        let finalized = node.block("finalized")?;
        let row = RowMap::from([
            ("block_number".into(), Value::UInt64(block.number)),
            ("fee_recipient".into(), Value::String(block.miner.clone())),
            ("gas_used".into(), Value::UInt64(block.gas_used)),
        ]);

        let batch = db.ingest(IngestInput {
            data: [("blocks".to_string(), vec![row])].into(),
            rollback_chain: vec![BlockCursor { number: block.number, hash: block.hash.clone() }],
            finalized_head: BlockCursor { number: finalized.number, hash: finalized.hash },
        })?;

        apply(&mut db, batch)?;

        last = Some(BlockCursor { number: block.number, hash: block.hash });
        next = block.number + 1;
        count += 1;
    }
    // [!endregion loop]

    Ok(())
}
