//! The structural validators of `spec/13-conformance-tdd.md` (HC-9). They
//! need no model: each batch is checked against the target image before it.

use std::collections::HashSet;

use settle::types::Value;

use super::agg::Mode;
use super::model::{Batch, Key, Op};
use super::schema::{Func, Item, Schema, Type};
use super::target::{Stale, Target};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// INV-32: the sequence follows the previous one.
    Sequence,
    /// INV-23: a record of a virtual table or a reducer.
    NotOutput,
    /// INV-4: two records of one key.
    DuplicateKey,
    /// INV-21: an insert of a key that the target holds.
    InsertOfPresent,
    /// INV-21: an update or a delete of a key that the target does not hold.
    ChangeOfAbsent,
    /// RP-6: an update to the values that the key holds.
    EmptyChange,
    /// INV-22: an update or a delete without previous values.
    MissingPrev,
    /// INV-22: previous values that differ from the target row.
    WrongPrev,
    /// RP-4: an insert with previous values.
    PrevOnInsert,
    /// RP-4: a delete whose values are not the deleted row.
    DeleteValues,
    /// INV-27: a value outside the type of its column.
    Type,
    /// INV-31: the finalized head goes back.
    FinalizedHead,
    /// INV-29: a value that no input row holds.
    Provenance,
}

impl Kind {
    pub fn rule(self) -> &'static str {
        match self {
            Kind::Sequence => "INV-32",
            Kind::NotOutput => "INV-23",
            Kind::DuplicateKey => "INV-4",
            Kind::InsertOfPresent | Kind::ChangeOfAbsent => "INV-21",
            Kind::EmptyChange => "RP-6",
            Kind::MissingPrev | Kind::WrongPrev => "INV-22",
            Kind::PrevOnInsert | Kind::DeleteValues => "RP-4",
            Kind::Type => "INV-27",
            Kind::FinalizedHead => "INV-31",
            Kind::Provenance => "INV-29",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Violation {
    pub kind: Kind,
    pub table: String,
    pub key: Key,
    #[allow(dead_code)]
    pub detail: String,
}

/// What the validators remember between the batches of one instance.
#[derive(Default)]
pub struct Validator {
    last_seq: Option<u64>,
    last_finalized: Option<u64>,
}

impl Validator {
    /// A reopen can reuse the sequences of lost batches (RP-12).
    pub fn reopen(&mut self) {
        *self = Validator::default();
    }

    pub fn check(
        &mut self,
        schema: &Schema,
        batch: &Batch,
        target: &Target,
        stale: &Stale,
    ) -> Vec<Violation> {
        let mut found = Vec::new();
        let mut flag = |kind, table: &str, key: &Key, detail: String| {
            found.push(Violation {
                kind,
                table: table.to_string(),
                key: key.clone(),
                detail,
            });
        };

        if let Some(last) = self.last_seq
            && batch.seq != last + 1
        {
            flag(
                Kind::Sequence,
                "",
                &vec![],
                format!("{} after {last}", batch.seq),
            );
        }
        self.last_seq = Some(batch.seq);

        if let Some(finalized) = &batch.finalized {
            if self.last_finalized.is_some_and(|last| finalized.0 < last) {
                flag(
                    Kind::FinalizedHead,
                    "",
                    &vec![],
                    format!("{} after {:?}", finalized.0, self.last_finalized),
                );
            }
            self.last_finalized = Some(finalized.0);
        }

        let mut seen = HashSet::new();
        for record in &batch.records {
            let (table, key) = (record.table.as_str(), &record.key);
            if !schema.is_output(table) {
                flag(Kind::NotOutput, table, key, String::new());
                continue;
            }
            if !seen.insert((table, key)) {
                flag(Kind::DuplicateKey, table, key, String::new());
            }
            for (column, value, ty) in typed_values(schema, table, &record.values) {
                if !ty.admits(value) {
                    flag(Kind::Type, table, key, format!("{column} = {value:?}"));
                }
            }

            if record.op == Op::Insert && record.prev.is_some() {
                flag(Kind::PrevOnInsert, table, key, format!("{:?}", record.prev));
            }
            if record.op != Op::Insert && record.prev.is_none() {
                flag(Kind::MissingPrev, table, key, format!("{:?}", record.op));
            }

            // RP-21: after a crash the target can hold anything at a stale key.
            if stale.contains(table, key) {
                continue;
            }
            let held = target.get(table, key);
            match (record.op, held) {
                (Op::Insert, Some(row)) => {
                    flag(Kind::InsertOfPresent, table, key, format!("holds {row:?}"))
                }
                (Op::Update | Op::Delete, None) => {
                    flag(Kind::ChangeOfAbsent, table, key, format!("{:?}", record.op))
                }
                (Op::Update, Some(row)) if *row == record.values => {
                    flag(Kind::EmptyChange, table, key, format!("{row:?}"))
                }
                (Op::Delete, Some(row)) if *row != record.values => flag(
                    Kind::DeleteValues,
                    table,
                    key,
                    format!("{:?} for {row:?}", record.values),
                ),
                _ => {}
            }
            if let (Some(prev), Some(row)) = (&record.prev, held)
                && prev != row
            {
                flag(Kind::WrongPrev, table, key, format!("{prev:?} for {row:?}"));
            }
        }

        found
    }
}

/// Each value of a record with the type that INV-27 gives its column, for
/// the columns whose type the schema fixes.
fn typed_values<'a>(
    schema: &'a Schema,
    table: &str,
    values: &'a [Value],
) -> Vec<(String, &'a Value, Type)> {
    if let Some(table) = schema.table(table) {
        return table
            .columns
            .iter()
            .zip(values)
            .map(|((c, ty), v)| (c.clone(), v, *ty))
            .collect();
    }

    let view = schema.view(table).expect("an output table");
    let mut typed = Vec::new();
    for (item, value) in view.items.iter().zip(values) {
        let column_type = |c: &str| schema.column_type(&view.source, c);
        let ty = match item {
            Item::Key(c) => column_type(c),
            Item::Tumbling { .. } => Some(Type::DateTime),
            Item::Agg {
                func: Func::Count, ..
            } => Some(Type::UInt64),
            Item::Agg {
                func: Func::Avg, ..
            } => Some(Type::Float64),
            Item::Agg {
                func: Func::First | Func::Last,
                column,
                ..
            } => column.as_deref().and_then(column_type),
            Item::Agg {
                func: Func::Sum | Func::Min | Func::Max,
                column,
                ..
            } => {
                let mode = Mode::of(column.as_deref().and_then(column_type));
                // DEF-43: the Int mode gives UInt64, Int64 or Float64.
                if mode == Mode::Int && matches!(value, Value::Int64(_) | Value::Float64(_)) {
                    None
                } else {
                    Some(if mode == Mode::Int {
                        Type::UInt64
                    } else {
                        Type::Float64
                    })
                }
            }
        };
        if let Some(ty) = ty {
            typed.push((item.name().to_string(), value, ty));
        }
    }
    typed
}
