//! A fixture recorded by `fetch.py`: the blocks of a chain range and the
//! decoded rows of each, as the Portal sent them.

#![allow(dead_code)]

use std::ops::Range;
use std::path::{Path, PathBuf};

use serde::Deserialize;

const MAGIC: &[u8; 8] = b"SETFIX01";
const BLOCK_HEADER: usize = 8 + 8 + 32 + 4 + 4;

#[derive(Debug, Deserialize)]
pub struct Head {
    pub number: u64,
    pub hash: String,
}

#[derive(Debug, Deserialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
}

#[derive(Debug, Deserialize)]
pub struct Manifest {
    pub scenario: String,
    pub dataset: String,
    pub from_block: u64,
    pub to_block: u64,
    pub columns: Vec<Column>,
    pub blocks: usize,
    pub rows: usize,
    pub json_bytes: u64,
    pub rows_sha256: String,
    pub recorded_at: String,
    pub finalized_head: Head,
    /// (unix ms, head, finalized head), polled while recording.
    pub finality_samples: Vec<(u64, u64, u64)>,
}

pub struct Block {
    pub number: u64,
    pub timestamp: u64,
    pub hash: String,
    /// The size of the block's JSON line in the Portal response.
    pub json_bytes: u64,
    pub rows: Range<usize>,
}

pub struct Fixture {
    pub manifest: Manifest,
    pub blocks: Vec<Block>,
    rows: Vec<u8>,
    row_width: usize,
}

fn type_width(ty: &str) -> usize {
    match ty {
        "u8" => 1,
        "addr" => 20,
        "u256" | "i256" => 32,
        other => panic!("unknown fixture column type {other}"),
    }
}

impl Fixture {
    /// `$SETTLE_FIXTURES/<scenario>`.
    pub fn dir(scenario: &str) -> anyhow::Result<PathBuf> {
        let root = std::env::var("SETTLE_FIXTURES").map_err(|_| {
            anyhow::anyhow!(
                "SETTLE_FIXTURES is not set; record one with benches/scenarios/fetch.py"
            )
        })?;
        Ok(Path::new(&root).join(scenario))
    }

    pub fn load(scenario: &str) -> anyhow::Result<Self> {
        let dir = Self::dir(scenario)?;
        let manifest: Manifest =
            serde_json::from_slice(&std::fs::read(dir.join("manifest.json"))?)?;
        anyhow::ensure!(
            manifest.scenario == scenario,
            "{dir:?} holds {}",
            manifest.scenario
        );

        let raw = std::fs::read(dir.join("rows.bin"))?;
        anyhow::ensure!(raw.starts_with(MAGIC), "rows.bin: not a fixture file");
        let row_width: usize = manifest.columns.iter().map(|c| type_width(&c.ty)).sum();

        let mut blocks = Vec::with_capacity(manifest.blocks);
        let mut rows = Vec::with_capacity(manifest.rows * row_width);
        let mut at = MAGIC.len();
        while at < raw.len() {
            let header = &raw[at..at + BLOCK_HEADER];
            let u64_at = |i: usize| u64::from_le_bytes(header[i..i + 8].try_into().unwrap());
            let u32_at = |i: usize| u32::from_le_bytes(header[i..i + 4].try_into().unwrap());
            let count = u32_at(52) as usize;
            at += BLOCK_HEADER;

            let first = rows.len() / row_width;
            rows.extend_from_slice(&raw[at..at + count * row_width]);
            at += count * row_width;
            blocks.push(Block {
                number: u64_at(0),
                timestamp: u64_at(8),
                hash: format!("0x{}", hex(&header[16..48])),
                json_bytes: u32_at(48) as u64,
                rows: first..first + count,
            });
        }

        anyhow::ensure!(
            blocks.len() == manifest.blocks && rows.len() == manifest.rows * row_width,
            "rows.bin does not match manifest.json"
        );
        Ok(Self {
            manifest,
            blocks,
            rows,
            row_width,
        })
    }

    pub fn row_count(&self) -> usize {
        self.rows.len() / self.row_width
    }

    pub fn row(&self, i: usize) -> &[u8] {
        &self.rows[i * self.row_width..(i + 1) * self.row_width]
    }

    /// The byte range of `name` in a row.
    pub fn column(&self, name: &str) -> Range<usize> {
        let mut offset = 0;
        for column in &self.manifest.columns {
            let width = type_width(&column.ty);
            if column.name == name {
                return offset..offset + width;
            }
            offset += width;
        }
        panic!("the fixture has no column {name}")
    }

    /// The blocks cut into the batches that the pipes SDK hands to an app
    /// when the app is slower than the Portal: a batch ends at the first
    /// block that brings its JSON to `max_bytes`.
    pub fn batches(&self, blocks: Range<usize>, max_bytes: u64) -> Vec<Range<usize>> {
        let mut batches = Vec::new();
        let mut start = blocks.start;
        let mut bytes = 0;
        for i in blocks.clone() {
            bytes += self.blocks[i].json_bytes;
            if bytes >= max_bytes {
                batches.push(start..i + 1);
                start = i + 1;
                bytes = 0;
            }
        }
        if start < blocks.end {
            batches.push(start..blocks.end);
        }
        batches
    }

    /// The index of the highest block at or below `number`.
    pub fn block_at_or_below(&self, number: u64) -> usize {
        self.blocks
            .partition_point(|b| b.number <= number)
            .saturating_sub(1)
    }
}

/// The finalized head of a replayed chain, from the lag that the Portal
/// showed while recording. The recorded pattern repeats when the replay is
/// longer than the recording.
pub struct Finality {
    /// (head offset from the first sample, lag) in head order.
    points: Vec<(u64, u64)>,
    period: u64,
}

impl Finality {
    pub fn new(samples: &[(u64, u64, u64)]) -> Self {
        let mut samples: Vec<(u64, u64)> = samples.iter().map(|&(_, h, f)| (h, f)).collect();
        samples.sort_unstable();
        assert!(!samples.is_empty(), "the fixture has no finality samples");

        let first_head = samples[0].0;
        let points = samples
            .iter()
            .map(|&(h, f)| (h - first_head, h - f))
            .collect();
        let period = samples.last().unwrap().0 - first_head;
        Self { points, period }
    }

    /// The finalized head when the replay that started at head `start`
    /// reaches head `head`.
    pub fn at(&self, start: u64, head: u64) -> u64 {
        let pos = if self.period == 0 {
            0
        } else {
            (head - start) % self.period
        };
        let k = self.points.partition_point(|&(offset, _)| offset <= pos) - 1;
        let (offset, lag) = self.points[k];
        head.saturating_sub(lag + (pos - offset))
    }

    pub fn lag_range(&self) -> (u64, u64) {
        let lags = self.points.iter().map(|&(_, lag)| lag);
        (lags.clone().min().unwrap(), lags.max().unwrap())
    }
}

pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 15) as usize] as char);
    }
    out
}
