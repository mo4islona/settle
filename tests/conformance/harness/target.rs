//! The model target: a downstream store that applies batches in order and
//! idempotently (RP-20, RP-21), as DEF-64 defines its image.

use std::collections::HashSet;

use settle::types::Value;

use super::model::{Batch, Image, Key, Op};

#[derive(Clone, Default)]
pub struct Target {
    pub image: Image,
    /// The sequence of the last applied batch, stored with the data (RP-23).
    pub applied: u64,
}

impl Target {
    pub fn apply(&mut self, batch: &Batch) {
        for record in &batch.records {
            let table = self.image.entry(record.table.clone()).or_default();
            match record.op {
                Op::Insert | Op::Update => {
                    table.insert(record.key.clone(), record.values.clone());
                }
                Op::Delete => {
                    table.remove(&record.key);
                }
            }
        }
        self.applied = batch.seq;
    }

    pub fn get(&self, table: &str, key: &Key) -> Option<&Vec<Value>> {
        self.image.get(table)?.get(key)
    }
}

/// The keys that can differ from the output image after a crash outside
/// journal mode (RP-21): the keys of applied batches that the crash lost.
#[derive(Clone, Default)]
pub struct Stale(pub HashSet<(String, Key)>);

impl Stale {
    pub fn add(&mut self, batch: &Batch) {
        for record in &batch.records {
            self.0.insert((record.table.clone(), record.key.clone()));
        }
    }

    /// A later record gives the target the values of the output image again.
    pub fn refresh(&mut self, batch: &Batch) {
        for record in &batch.records {
            self.0.remove(&(record.table.clone(), record.key.clone()));
        }
    }

    pub fn contains(&self, table: &str, key: &Key) -> bool {
        self.0.contains(&(table.to_string(), key.clone()))
    }
}
