//! The reference model of `spec/13-conformance-tdd.md` (HC-4): a pure state
//! machine over the instance state of DEF-86. `out` recomputes the output
//! image from the accepted chain alone; the model never reads Settle.

use std::collections::{BTreeMap, HashMap, VecDeque};

use settle::types::{RowMap, Value};

use super::agg::{self, Mode};
use super::schema::{Item, Schema};

pub type Key = Vec<Value>;
/// Table → key → the values of the record columns (RP-2).
pub type Image = HashMap<String, HashMap<Key, Vec<Value>>>;
pub type Cursor = (u64, String);
/// DEF-10: block → table → rows in input order.
pub type Chain = BTreeMap<u64, BTreeMap<String, Vec<RowMap>>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Record {
    pub table: String,
    pub op: Op,
    pub key: Key,
    pub values: Vec<Value>,
    pub prev: Option<Vec<Value>>,
}

#[derive(Clone, Debug)]
pub struct Batch {
    pub seq: u64,
    pub latest: Option<Cursor>,
    pub finalized: Option<Cursor>,
    pub records: Vec<Record>,
}

/// The error classes of IB-50 that a Rust caller can tell apart. EC-3, EC-4,
/// EC-5, EC-8 and EC-9 share one variant (GAP-14).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    PendingAck,
    WrongAckSequence,
    Poisoned,
    SyncFailed,
    InvalidSchema,
    IncompatibleStorage,
    ReducerFailed,
    StorageFailed,
    InvalidOperation,
}

#[derive(Debug)]
pub struct Rejection {
    pub class: Class,
    /// The spec class, for the report.
    #[allow(dead_code)]
    pub why: &'static str,
}

fn reject<T>(class: Class, why: &'static str) -> Result<T, Rejection> {
    Err(Rejection { class, why })
}

/// The part of DEF-86 that a commit makes durable.
#[derive(Clone, Debug, Default)]
pub struct State {
    pub a: Chain,
    pub h: BTreeMap<u64, String>,
    pub l: Option<u64>,
    pub f: u64,
    pub d: Option<u64>,
    /// The next sequence.
    pub s: u64,
}

struct Entry {
    seq: u64,
    joined: usize,
    /// What an ack commits, and what the journal holds: the state after the
    /// call and the calls that joined it.
    after: State,
}

pub struct Config {
    pub max_unacked: usize,
    pub interval: u64,
    pub journal: bool,
}

/// The status reads of RP-30 to RP-35, as the Rust API gives them.
#[derive(Clone, Debug, PartialEq)]
pub struct Status {
    pub latest_block: u64,
    pub latest_cursor: Option<Cursor>,
    pub finalized_block: u64,
    pub finalized_cursor: Option<Cursor>,
    /// 0 for ⊥.
    pub durable_block: u64,
    pub unacked: usize,
    pub poisoned: bool,
}

pub struct Model<'s> {
    schema: &'s Schema,
    cfg: Config,
    pub st: State,
    committed: State,
    queue: VecDeque<Entry>,
    poisoned: bool,
    /// How the last call without a batch changed the queue, for `adopt`.
    joined: Option<Joined>,
    /// The fork point of the last call, if it rolled back.
    pub rolled_back: Option<u64>,
    /// The chain after the rollback of the last call, if it was an ingest
    /// that rolled back: an in-line fork.
    rolled: Option<Chain>,
}

struct Joined {
    committed: State,
    newest: Option<(usize, State)>,
}

impl<'s> Model<'s> {
    pub fn new(schema: &'s Schema, cfg: Config) -> Self {
        let st = State {
            s: 1,
            ..State::default()
        };
        Model {
            schema,
            cfg,
            committed: st.clone(),
            st,
            queue: VecDeque::new(),
            poisoned: false,
            joined: None,
            rolled_back: None,
            rolled: None,
        }
    }

    pub fn status(&self) -> Status {
        let st = &self.st;
        Status {
            latest_block: st.l.unwrap_or(0),
            latest_cursor: st.l.map(|l| (l, st.h.get(&l).cloned().unwrap_or_default())),
            finalized_block: st.f,
            finalized_cursor: st.h.get(&st.f).map(|h| (st.f, h.clone())),
            durable_block: st.d.unwrap_or(0),
            unacked: self.places(),
            poisoned: self.poisoned,
        }
    }

    /// DEF-67: each unacked batch and each call that joined it.
    pub fn places(&self) -> usize {
        self.queue.iter().map(|e| 1 + e.joined).sum()
    }

    /// The sequence of the last committed batch, 0 for none.
    pub fn committed_seq(&self) -> u64 {
        self.committed.s - 1
    }

    pub fn out(&self) -> Image {
        out(self.schema, &self.st.a).expect("the accepted chain folds without error")
    }

    fn gate(&self) -> Result<(), Rejection> {
        if self.poisoned {
            return reject(Class::Poisoned, "POISONED");
        }
        if self.places() >= self.cfg.max_unacked {
            return reject(Class::PendingAck, "PENDING_ACK");
        }
        Ok(())
    }

    /// TR-1.
    pub fn ingest(
        &mut self,
        rows: &BTreeMap<String, Vec<RowMap>>,
        chain: &[Cursor],
        fin: &Cursor,
    ) -> Result<Option<Batch>, Rejection> {
        self.rolled_back = None;
        self.rolled = None;
        self.gate()?;
        let st = &self.st;

        if fin.0 < st.f {
            return reject(Class::InvalidOperation, "INVALID_INPUT: finality goes back");
        }

        let supplied: Vec<&Cursor> = chain.iter().chain(std::iter::once(fin)).collect();
        let conflict = st.l.and_then(|l| {
            supplied
                .iter()
                .filter(|(b, h)| *b <= l && st.h.get(b).is_some_and(|stored| stored != h))
                .map(|(b, _)| *b)
                .min()
        });

        let mut fork_point = None;
        if let Some(c) = conflict {
            if c <= st.f {
                return reject(Class::InvalidOperation, "FORK_BELOW_FINALITY");
            }
            let l = st.l.expect("a conflict needs a latest block");
            let p = supplied
                .iter()
                .filter(|(b, h)| *b < c && st.f <= *b && *b <= l && st.h.get(b) == Some(h))
                .map(|(b, _)| *b)
                .max();
            let Some(p) = p else {
                return reject(Class::InvalidOperation, "NO_COMMON_ANCESTOR");
            };
            fork_point = Some(p);
        }
        let base = fork_point.or(st.l);

        for (table, table_rows) in rows {
            if table_rows.is_empty() {
                continue;
            }
            if self.schema.table(table).is_none() {
                return reject(Class::InvalidOperation, "INVALID_INPUT: unknown table");
            }
        }
        let blocks = || rows.values().flatten().map(block_of);
        if let (Some(min), Some(base)) = (blocks().min(), base)
            && min <= base
        {
            return reject(
                Class::InvalidOperation,
                "INVALID_INPUT: block at or below the latest",
            );
        }

        let before = self.out();
        let mut next = self.st.clone();
        if let Some(p) = fork_point {
            rollback(&mut next, p);
        }
        let rolled = fork_point.map(|_| next.a.clone());
        for (table, table_rows) in rows {
            for row in table_rows {
                let block = next.a.entry(block_of(row)).or_default();
                block.entry(table.clone()).or_default().push(row.clone());
            }
        }
        next.l = blocks().max().or(base);
        next.f = fin.0;
        for (b, h) in supplied {
            next.h.insert(*b, h.clone());
        }

        let after = match out(self.schema, &next.a) {
            Ok(after) => after,
            Err(_) => {
                // WP-71: the rollback of the same call cannot be undone.
                if fork_point.is_some() {
                    self.poisoned = true;
                }
                return reject(Class::ReducerFailed, "REDUCER_FAILED");
            }
        };

        self.checkpoint(&mut next);
        prune_hashes(&mut next);
        self.st = next;
        self.rolled_back = fork_point;
        self.rolled = rolled;
        Ok(self.emit(diff(&before, &after)))
    }

    /// Whether the last call was an in-line fork: an ingest that rolled back.
    pub fn in_line_fork(&self) -> bool {
        self.rolled.is_some()
    }

    /// GAP-47: the values of `key` after the rollback of the last ingest. A
    /// view delete that merges the compensation update of that rollback
    /// carries them.
    pub fn after_rollback(&self, table: &str, key: &Key) -> Option<Vec<Value>> {
        let chain = self.rolled.as_ref()?;
        let image = out(self.schema, chain).expect("a rollback folds without error");
        image.get(table)?.get(key).cloned()
    }

    /// WP-40 to WP-42. The call is a backfill call when the finalized head is
    /// at or above the latest block after it (DEF-15).
    fn checkpoint(&self, st: &mut State) {
        let backfill = st.f >= st.l.unwrap_or(0);
        let defer = self.cfg.interval > 1 && self.schema.deferral_safe() && backfill;
        let due =
            st.d.is_none_or(|d| st.f.saturating_sub(d) >= self.cfg.interval);
        if defer && !due {
            return;
        }
        if let Some(l) = st.l
            && st.d.is_none_or(|d| d < st.f)
        {
            st.d = Some(st.f.min(l));
        }
    }

    /// TR-2.
    pub fn fork(&mut self, chain: &[Cursor]) -> Result<(Cursor, Option<Batch>), Rejection> {
        self.rolled_back = None;
        self.rolled = None;
        self.gate()?;
        if chain.is_empty() {
            return reject(Class::InvalidOperation, "INVALID_INPUT: empty chain");
        }

        let st = &self.st;
        let Some(l) = st.l else {
            return reject(Class::InvalidOperation, "NO_COMMON_ANCESTOR");
        };
        let p = chain
            .iter()
            .filter(|(b, h)| st.f <= *b && *b <= l && st.h.get(b) == Some(h))
            .map(|(b, _)| *b)
            .max();
        let Some(p) = p else {
            return reject(Class::InvalidOperation, "NO_COMMON_ANCESTOR");
        };

        let before = self.out();
        let mut next = self.st.clone();
        rollback(&mut next, p);
        for (b, h) in chain {
            next.h.insert(*b, h.clone());
        }
        let cursor = (p, next.h[&p].clone());
        let after = out(self.schema, &next.a).expect("a rollback folds without error");
        self.st = next;
        self.rolled_back = Some(p);
        Ok((cursor, self.emit(diff(&before, &after))))
    }

    fn emit(&mut self, records: Vec<Record>) -> Option<Batch> {
        // WP-33: a call without a batch commits at once, or joins the newest batch.
        if records.is_empty() {
            self.joined = Some(Joined {
                committed: self.committed.clone(),
                newest: self.queue.back().map(|e| (e.joined, e.after.clone())),
            });
            match self.queue.back_mut() {
                None => self.committed = self.st.clone(),
                Some(newest) => {
                    newest.joined += 1;
                    newest.after = self.st.clone();
                }
            }
            return None;
        }

        self.joined = None;
        let st = &self.st;
        let batch = Batch {
            seq: st.s,
            latest: st.l.map(|l| (l, st.h.get(&l).cloned().unwrap_or_default())),
            finalized: st.h.get(&st.f).map(|h| (st.f, h.clone())),
            records,
        };
        self.st.s += 1;
        self.queue.push_back(Entry {
            seq: batch.seq,
            joined: 0,
            after: self.st.clone(),
        });
        Some(batch)
    }

    /// GAP-41: Settle returned a batch of empty changes where the spec
    /// returns none. Take the last call as one that returned it, so that the
    /// sequences and the queue stay aligned.
    pub fn adopt(&mut self, records: Vec<Record>) -> Batch {
        let joined = self.joined.take().expect("the last call returned no batch");
        self.committed = joined.committed;
        if let (Some(newest), Some((count, after))) = (self.queue.back_mut(), joined.newest) {
            newest.joined = count;
            newest.after = after;
        }
        self.emit(records).expect("a batch has records")
    }

    /// TR-3.
    pub fn ack(&mut self, n: u64) -> Result<(), Rejection> {
        let Some(newest) = self.queue.back() else {
            return Ok(());
        };
        if n > newest.seq {
            return reject(Class::WrongAckSequence, "WRONG_ACK_SEQUENCE");
        }
        while self.queue.front().is_some_and(|e| e.seq <= n) {
            self.committed = self.queue.pop_front().unwrap().after;
        }
        Ok(())
    }

    /// TR-6 then TR-4. In journal mode, `applied` is the sequence that the
    /// target stored (RP-23), and the open commits the journal up to it.
    pub fn crash_and_open(&mut self, applied: Option<u64>) {
        if self.cfg.journal {
            let applied = applied.expect("journal mode gives the applied sequence");
            assert!(
                applied >= self.committed_seq(),
                "the target is behind the store"
            );
            if applied > self.committed_seq() {
                let entry = self.queue.iter().find(|e| e.seq == applied);
                self.committed = entry
                    .expect("the journal holds every returned batch")
                    .after
                    .clone();
            }
        }
        self.queue.clear();
        self.st = self.committed.clone();
        self.poisoned = false;
    }
}

fn block_of(row: &RowMap) -> u64 {
    match row.get("block_number") {
        Some(Value::UInt64(b)) => *b,
        other => panic!("the harness sends a UInt64 block number, got {other:?}"),
    }
}

/// WP-14.
fn rollback(st: &mut State, p: u64) {
    st.a.retain(|b, _| *b <= p);
    st.h.retain(|b, _| *b <= p);
    st.l = Some(p);
}

/// RS-14: at or above `F` every supplied hash, below it only `D` and `L`.
fn prune_hashes(st: &mut State) {
    let (f, d, l) = (st.f, st.d, st.l);
    st.h.retain(|b, _| *b >= f || Some(*b) == d || Some(*b) == l);
}

/// RP-3, RP-4 and RP-6: the records that turn `before` into `after`.
pub fn diff(before: &Image, after: &Image) -> Vec<Record> {
    let empty = HashMap::new();
    let mut records = Vec::new();
    let tables: std::collections::BTreeSet<&String> = before.keys().chain(after.keys()).collect();

    for table in tables {
        let old = before.get(table).unwrap_or(&empty);
        let new = after.get(table).unwrap_or(&empty);

        for (key, values) in new {
            let record = |op, prev: Option<&Vec<Value>>| Record {
                table: table.clone(),
                op,
                key: key.clone(),
                values: values.clone(),
                prev: prev.cloned(),
            };
            match old.get(key) {
                None => records.push(record(Op::Insert, None)),
                Some(prev) if prev != values => records.push(record(Op::Update, Some(prev))),
                Some(_) => {}
            }
        }
        for (key, values) in old {
            if !new.contains_key(key) {
                records.push(Record {
                    table: table.clone(),
                    op: Op::Delete,
                    key: key.clone(),
                    values: values.clone(),
                    prev: Some(values.clone()),
                });
            }
        }
    }
    records
}

/// DEF-46: the output image of the accepted chain. Err is a body error.
pub fn out(schema: &Schema, a: &Chain) -> Result<Image, String> {
    let mut image = Image::new();

    for table in schema.tables.iter().filter(|t| !t.is_virtual) {
        let rows = image.entry(table.name.clone()).or_default();
        for (block, tables) in a {
            for (i, row) in tables.get(&table.name).into_iter().flatten().enumerate() {
                let key = vec![Value::UInt64(*block), Value::UInt64(i as u64)];
                let values = table
                    .columns
                    .iter()
                    .map(|(c, _)| row.get(c).cloned().unwrap_or(Value::Null))
                    .collect();
                rows.insert(key, values);
            }
        }
    }

    // DEF-40 and DEF-41: each body folds over its group in chain order.
    let mut emits: HashMap<&str, Vec<(u64, RowMap)>> = HashMap::new();
    for reducer in &schema.reducers {
        let input = source_rows(a, &emits, &reducer.source);
        let mut groups: HashMap<Key, _> = HashMap::new();
        let mut rows = Vec::new();

        for (block, row) in input {
            let key: Key = reducer
                .group_by
                .iter()
                .map(|c| row.get(c).cloned().unwrap_or(Value::Null))
                .collect();
            let state = groups.entry(key).or_insert_with(|| reducer.defaults());
            for mut emit in (reducer.body)(state, &row)? {
                // DEF-27: a group-by column that the emit leaves Null comes from the row.
                for column in &reducer.group_by {
                    if emit.get(column).is_none_or(Value::is_null) {
                        emit.insert(
                            column.clone(),
                            row.get(column).cloned().unwrap_or(Value::Null),
                        );
                    }
                }
                rows.push((block, emit));
            }
        }
        emits.insert(&reducer.name, rows);
    }

    for view in &schema.views {
        let mut rows = source_rows(a, &emits, &view.source);
        if let Some(sliding) = &view.sliding {
            rows = window_rows(rows, &sliding.column, sliding.seconds);
        }

        // DEF-30: the group key is the tuple of the key items.
        let mut groups: HashMap<Key, Vec<(u64, RowMap)>> = HashMap::new();
        for (block, row) in rows {
            let key = view
                .items
                .iter()
                .filter_map(|item| match item {
                    Item::Key(c) => Some(row.get(c).cloned().unwrap_or(Value::Null)),
                    Item::Tumbling {
                        column, seconds, ..
                    } => Some(agg::window_start(
                        row.get(column).unwrap_or(&Value::Null),
                        *seconds,
                    )),
                    Item::Agg { .. } => None,
                })
                .collect();
            groups.entry(key).or_default().push((block, row));
        }

        let table = image.entry(view.name.clone()).or_default();
        for (key, rows) in groups {
            let mut keys = key.iter();
            let mut present = false;
            let values = view
                .items
                .iter()
                .map(|item| match item {
                    Item::Key(_) | Item::Tumbling { .. } => keys.next().unwrap().clone(),
                    Item::Agg { func, column, .. } => {
                        let mode = Mode::of(
                            column
                                .as_deref()
                                .and_then(|c| schema.column_type(&view.source, c)),
                        );
                        let blocks = by_block(&rows, column.as_deref());
                        let (value, has_data) = agg::aggregate(*func, mode, &blocks);
                        present |= has_data;
                        value
                    }
                })
                .collect();
            // DEF-44: a group is present when one of its aggregates has data.
            if present {
                table.insert(key, values);
            }
        }
    }

    Ok(image)
}

/// The rows of a source in chain order (DEF-40), each with its block.
fn source_rows(
    a: &Chain,
    emits: &HashMap<&str, Vec<(u64, RowMap)>>,
    source: &str,
) -> Vec<(u64, RowMap)> {
    if let Some(rows) = emits.get(source) {
        return rows.clone();
    }
    a.iter()
        .flat_map(|(block, tables)| {
            tables
                .get(source)
                .into_iter()
                .flatten()
                .map(|row| (*block, row.clone()))
        })
        .collect()
}

/// DEF-45: only the rows of the blocks in the window.
fn window_rows(rows: Vec<(u64, RowMap)>, column: &str, seconds: u64) -> Vec<(u64, RowMap)> {
    let mut blocks: Vec<(u64, Vec<RowMap>)> = Vec::new();
    for (block, row) in rows {
        match blocks.last_mut() {
            Some((b, rows)) if *b == block => rows.push(row),
            _ => blocks.push((block, vec![row])),
        }
    }

    let stamps: Vec<Vec<Option<i64>>> = blocks
        .iter()
        .map(|(_, rows)| {
            rows.iter()
                .map(|r| r.get(column).and_then(agg::millis))
                .collect()
        })
        .collect();
    let keep = agg::in_window(&stamps, seconds);

    blocks
        .into_iter()
        .zip(keep)
        .filter(|(_, keep)| *keep)
        .flat_map(|((block, rows), _)| rows.into_iter().map(move |row| (block, row)))
        .collect()
}

/// The values of `column` per block, in chain order; `count()` counts rows.
fn by_block(rows: &[(u64, RowMap)], column: Option<&str>) -> Vec<Vec<Value>> {
    let mut blocks: Vec<(u64, Vec<Value>)> = Vec::new();
    for (block, row) in rows {
        let value = match column {
            Some(c) => row.get(c).cloned().unwrap_or(Value::Null),
            None => Value::UInt64(1),
        };
        match blocks.last_mut() {
            Some((b, values)) if b == block => values.push(value),
            _ => blocks.push((*block, vec![value])),
        }
    }
    blocks.into_iter().map(|(_, values)| values).collect()
}
