//! The input ledger (HC-10): every row that the simulator sent, for the
//! provenance checks of INV-29.

use std::collections::{HashMap, HashSet};

use settle::types::{RowMap, Value};

use super::model::{Batch, Op};
use super::schema::{Item, Schema};
use super::validate::{Kind, Violation};

#[derive(Default)]
pub struct Ledger {
    /// (table, block, row index) → each version of the row that was sent.
    rows: HashMap<(String, u64, u64), Vec<Vec<Value>>>,
    /// column → every value that a row sent in it.
    values: HashMap<String, HashSet<Value>>,
}

impl Ledger {
    pub fn record(&mut self, schema: &Schema, table: &str, rows: &[RowMap]) {
        let Some(declared) = schema.table(table) else {
            return;
        };

        let mut index: HashMap<u64, u64> = HashMap::new();
        for row in rows {
            let Some(Value::UInt64(block)) = row.get("block_number") else {
                continue;
            };
            let i = index.entry(*block).or_default();
            let values: Vec<Value> = declared
                .columns
                .iter()
                .map(|(c, _)| row.get(c).cloned().unwrap_or(Value::Null))
                .collect();
            self.rows
                .entry((table.to_string(), *block, *i))
                .or_default()
                .push(values);
            *i += 1;

            for (column, value) in row {
                self.values
                    .entry(column.clone())
                    .or_default()
                    .insert(value.clone());
            }
        }
    }

    /// A raw record holds a sent row, and a view key item a sent value.
    pub fn check(&self, schema: &Schema, batch: &Batch) -> Vec<Violation> {
        let mut found = Vec::new();

        for record in batch.records.iter().filter(|r| r.op != Op::Delete) {
            let violation = |detail| Violation {
                kind: Kind::Provenance,
                table: record.table.clone(),
                key: record.key.clone(),
                detail,
            };

            if schema.table(&record.table).is_some() {
                let [Value::UInt64(block), Value::UInt64(i)] = record.key[..] else {
                    found.push(violation(format!("raw key {:?}", record.key)));
                    continue;
                };
                let sent = self.rows.get(&(record.table.clone(), block, i));
                if !sent.is_some_and(|versions| versions.contains(&record.values)) {
                    found.push(violation(format!("{:?} was never sent", record.values)));
                }
                continue;
            }

            let Some(view) = schema.view(&record.table) else {
                continue;
            };
            for (item, value) in view.items.iter().zip(&record.values) {
                let Item::Key(column) = item else {
                    continue;
                };
                let sent = self
                    .values
                    .get(column)
                    .is_some_and(|values| values.contains(value));
                if !value.is_null() && !sent {
                    found.push(violation(format!("{column} = {value:?} was never sent")));
                }
            }
        }

        found
    }
}
