//! The input simulator: random call histories from a seed. It plays a
//! source chain that forks, and a host that applies and acks late, crashes
//! and reopens. It reads the model state to pick meaningful calls, never
//! the state of Settle.

use std::collections::{BTreeMap, HashMap};

use settle::types::{RowMap, Value};

use super::model::{Cursor, State};
use super::rng::Rng;
use super::schema::{Schema, Type};

#[derive(Clone, Debug)]
pub enum Step {
    Ingest {
        rows: BTreeMap<String, Vec<RowMap>>,
        chain: Vec<Cursor>,
        fin: Cursor,
        #[allow(dead_code)]
        what: &'static str,
    },
    Fork {
        chain: Vec<Cursor>,
        #[allow(dead_code)]
        what: &'static str,
    },
    /// The target applies the oldest returned batches.
    Apply(usize),
    Ack(u64),
    /// The process dies with its unacked batches, and the host reopens.
    Crash,
}

/// What the host knows about its own side.
pub struct Host {
    pub unapplied: usize,
    pub applied_unacked: Vec<u64>,
    pub newest: Option<u64>,
    pub places: usize,
    pub max_unacked: usize,
    /// After a reopen Settle reports `D` as its finality (GAP-3) until the
    /// next ingest. A call that finality alone rejects would pass there.
    pub finality_lags: bool,
}

pub struct Sim<'s> {
    schema: &'s Schema,
    rng: Rng,
    /// The fork generation of the source chain.
    epoch: u64,
    /// The hash that the source reports for each height now.
    canon: BTreeMap<u64, String>,
    /// The rows of each block the source made, by height and hash: a block
    /// sent again after a crash has the same rows.
    content: HashMap<(u64, String), BTreeMap<String, Vec<RowMap>>>,
}

impl<'s> Sim<'s> {
    pub fn new(schema: &'s Schema, seed: u64) -> Self {
        Sim {
            schema,
            rng: Rng::new(seed),
            epoch: 0,
            canon: BTreeMap::new(),
            content: HashMap::new(),
        }
    }

    pub fn next(&mut self, st: &State, host: &Host) -> Step {
        let pending = host.unapplied > 0 || !host.applied_unacked.is_empty();
        let full = host.places >= host.max_unacked;
        if pending && (self.rng.chance(30) || (full && self.rng.chance(70))) {
            return self.host_step(host);
        }

        let roll = self.rng.below(100);
        if roll < 8 {
            return Step::Crash;
        }
        if roll < 20
            && let Some(step) = self.fork(st)
        {
            return step;
        }
        if roll < 25
            && let Some(step) = self.invalid(st, host)
        {
            return step;
        }
        if roll < 40 {
            return self.ingest(st, false);
        }
        self.ingest(st, true)
    }

    fn host_step(&mut self, host: &Host) -> Step {
        let apply = host.unapplied > 0 && (host.applied_unacked.is_empty() || self.rng.chance(55));
        if apply {
            return Step::Apply(1 + self.rng.below(host.unapplied as u64) as usize);
        }
        if self.rng.chance(8) {
            let newest = host.newest.unwrap_or(0);
            return Step::Ack(newest + 1 + self.rng.below(3));
        }
        Step::Ack(*self.rng.pick(&host.applied_unacked))
    }

    fn hash(&self, block: u64) -> String {
        format!("0x{block:x}.{}", self.epoch)
    }

    /// The hash of a height on the source chain; a new height gets one.
    fn source_hash(&mut self, block: u64) -> String {
        let hash = self.hash(block);
        self.canon.entry(block).or_insert(hash).clone()
    }

    /// The source replaces every block above `p`.
    fn fork_source(&mut self, p: u64) {
        self.canon.retain(|b, _| *b <= p);
        self.epoch += 1;
    }

    /// The recent chain a host sends: the source hashes of the heights in
    /// `[F, L]` that Settle stored, the newest few and the lowest.
    fn recent_chain(&mut self, st: &State) -> Vec<Cursor> {
        let heights = unfinal_heights(st);
        let newest = heights.iter().rev().take(8);
        let mut chosen: Vec<u64> = newest.chain(heights.first()).copied().collect();
        chosen.sort_unstable();
        chosen.dedup();
        chosen
            .into_iter()
            .map(|b| (b, self.source_hash(b)))
            .collect()
    }

    /// The latest block once the chain's fork, if any, is rolled back: the
    /// rows of the call must be above it (WP-5).
    fn base(st: &State, chain: &[Cursor]) -> Option<u64> {
        let l = st.l?;
        let conflict = chain
            .iter()
            .filter(|(b, h)| *b <= l && st.h.get(b).is_some_and(|s| s != h))
            .map(|(b, _)| *b)
            .min();
        let Some(c) = conflict else {
            return Some(l);
        };
        chain
            .iter()
            .filter(|(b, h)| *b < c && *b >= st.f && st.h.get(b) == Some(h))
            .map(|(b, _)| *b)
            .max()
    }

    /// The heights at or above `F` and below `L` that a fork can keep.
    fn fork_points(st: &State) -> Vec<u64> {
        let mut heights = unfinal_heights(st);
        heights.retain(|b| Some(*b) != st.l);
        heights
    }

    fn fork(&mut self, st: &State) -> Option<Step> {
        let points = Self::fork_points(st);
        if points.is_empty() {
            return None;
        }
        let p = *self.rng.pick(&points);
        self.fork_source(p);

        if self.rng.chance(50) {
            let chain = self.recent_chain(st);
            return Some(Step::Fork {
                chain,
                what: "fork",
            });
        }
        Some(self.ingest(st, true))
    }

    /// A call that the spec rejects, and that changes nothing.
    fn invalid(&mut self, st: &State, host: &Host) -> Option<Step> {
        let l = st.l?;
        let chain = self.recent_chain(st);
        let fin = (st.f, self.source_hash(st.f));

        match self.rng.below(5) {
            0 if st.f > 0 && !host.finality_lags => {
                let lower = st.f - 1 - self.rng.below(st.f.min(3));
                let fin = (lower, self.source_hash(lower));
                Some(Step::Ingest {
                    rows: BTreeMap::new(),
                    chain,
                    fin,
                    what: "lower finality",
                })
            }
            1 => {
                let base = Self::base(st, &chain)?;
                let table = self.schema.tables[0].name.clone();
                let rows = BTreeMap::from([(table.clone(), self.rows_for(base, &table))]);
                Some(Step::Ingest {
                    rows,
                    chain,
                    fin,
                    what: "old block",
                })
            }
            2 => {
                let mut row = RowMap::new();
                row.insert("block_number".into(), Value::UInt64(l + 1));
                let rows = BTreeMap::from([("nope".into(), vec![row])]);
                Some(Step::Ingest {
                    rows,
                    chain,
                    fin,
                    what: "unknown table",
                })
            }
            3 => {
                let chain = unfinal_heights(st)
                    .into_iter()
                    .map(|b| (b, format!("0xbad{b:x}")))
                    .collect();
                Some(Step::Fork {
                    chain,
                    what: "no ancestor",
                })
            }
            4 if st.f > 0 && st.f <= l && !host.finality_lags => {
                let mut chain = chain;
                chain.push((st.f, format!("0xbad{:x}", st.f)));
                Some(Step::Ingest {
                    rows: BTreeMap::new(),
                    chain,
                    fin,
                    what: "fork below finality",
                })
            }
            _ => None,
        }
    }

    /// A data call (`data`) or a heartbeat. Its chain shows a fork of the
    /// source that Settle has not seen, so the call can roll back in-line.
    fn ingest(&mut self, st: &State, data: bool) -> Step {
        let mut chain = self.recent_chain(st);
        let base = Self::base(st, &chain);
        let mut rows: BTreeMap<String, Vec<RowMap>> = BTreeMap::new();
        let mut tip = base;

        if data {
            let count = if self.rng.chance(15) {
                4 + self.rng.below(5)
            } else {
                1 + self.rng.below(3)
            };

            // First the blocks that the host sent before and the source still
            // has, as a host resends after a crash; then new ones.
            let resent: Vec<u64> = self
                .canon
                .iter()
                .filter(|(b, h)| {
                    base.is_none_or(|base| **b > base)
                        && self.content.contains_key(&(**b, (*h).clone()))
                })
                .map(|(b, _)| *b)
                .take(count as usize)
                .collect();
            let mut next = match (resent.last(), base) {
                (Some(last), _) => last + 1 + self.rng.below(2),
                (None, Some(base)) => base + 1 + self.rng.below(2),
                (None, None) => self.rng.below(2),
            };
            let mut blocks = resent;
            while blocks.len() < count as usize {
                blocks.push(next);
                next += 1 + self.rng.below(2);
            }

            for block in blocks {
                let hash = self.source_hash(block);
                let content = match self.content.get(&(block, hash.clone())) {
                    Some(content) => content.clone(),
                    None => {
                        let content = self.block_rows(block);
                        self.content.insert((block, hash.clone()), content.clone());
                        content
                    }
                };
                for (table, table_rows) in content {
                    rows.entry(table).or_default().extend(table_rows);
                }
                chain.push((block, hash));
                tip = Some(block);
            }
            self.maybe_poison(&mut rows);
        }

        let tip = tip.unwrap_or(0);
        // Mostly a lag behind the tip, which forks need; sometimes a backfill
        // call, and sometimes finality above the data, as on a gappy chain.
        let reported = match self.rng.below(100) {
            0..35 => st.f,
            35..60 => tip,
            60..90 => tip.saturating_sub(1 + self.rng.below(4)),
            _ => tip + 1 + self.rng.below(8),
        };
        let fin = reported.max(st.f);
        let fin = (fin, self.source_hash(fin));
        let what = if data { "data" } else { "heartbeat" };
        Step::Ingest {
            rows,
            chain,
            fin,
            what,
        }
    }

    /// The rows of a new block: one or two per table, some tables none.
    fn block_rows(&mut self, block: u64) -> BTreeMap<String, Vec<RowMap>> {
        let mut content = BTreeMap::new();
        let names: Vec<String> = self.schema.tables.iter().map(|t| t.name.clone()).collect();
        for (i, name) in names.iter().enumerate() {
            // The first table gets rows in every block, so each call has data.
            if i > 0 && self.rng.chance(40) {
                continue;
            }
            let rows = self.rows_for(block, name);
            content.insert(name.clone(), rows);
        }
        content
    }

    fn rows_for(&mut self, block: u64, table: &str) -> Vec<RowMap> {
        let columns = self
            .schema
            .table(table)
            .expect("a declared table")
            .columns
            .clone();
        (0..1 + self.rng.below(2))
            .map(|_| {
                columns
                    .iter()
                    .map(|(column, ty)| (column.clone(), self.value(block, column, *ty)))
                    .collect()
            })
            .collect()
    }

    fn value(&mut self, block: u64, column: &str, ty: Type) -> Value {
        if column == "block_number" {
            return Value::UInt64(block);
        }
        if self.schema.nulls && self.rng.chance(5) {
            return Value::Null;
        }
        match ty {
            Type::UInt64 => Value::UInt64(self.rng.below(10)),
            Type::Int64 => Value::Int64(self.rng.below(11) as i64 - 5),
            // No NaN: min and max take it as a value (GAP-48).
            // 1e16 + 1 rounds, so the order of the fold shows in the bits (DEF-42).
            Type::Float64 if self.rng.chance(15) => {
                Value::Float64(*self.rng.pick(&[1e16, -1e16, 1.0]))
            }
            Type::Float64 => Value::Float64(self.rng.below(17) as f64 / 4.0 - 2.0),
            Type::String => Value::String(["A", "B", "C"][self.rng.below(3) as usize].to_string()),
            // Ten minutes a block, so a window of an hour holds six.
            Type::DateTime => Value::DateTime(block as i64 * 600_000),
        }
    }

    /// A row that the body rejects, in some data calls.
    fn maybe_poison(&mut self, rows: &mut BTreeMap<String, Vec<RowMap>>) {
        let Some(poison) = self.schema.poison else {
            return;
        };
        if !self.rng.chance(20) {
            return;
        }
        let table = &self.schema.tables[0].name;
        let Some(table_rows) = rows.get_mut(table) else {
            return;
        };
        let at = self.rng.below(table_rows.len() as u64) as usize;
        let mut row = table_rows[at].clone();
        row.insert("amount".into(), Value::UInt64(poison));
        table_rows.insert(at, row);
    }
}

/// The heights in `[F, L]` with a stored hash. Finality can run above `L`.
fn unfinal_heights(st: &State) -> Vec<u64> {
    match st.l {
        Some(l) if st.f <= l => st.h.range(st.f..=l).map(|(b, _)| *b).collect(),
        _ => Vec::new(),
    }
}
