//! Runs one history on Settle and on the reference model, and compares
//! them after every call: the result, the batch, the status reads and the
//! target image (CT-1).

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use settle::db::{Config, IngestInput, Settle};
use settle::error::Error;
use settle::storage::StorageBackend;
use settle::storage::memory::MemoryBackend;
use settle::types::{BlockCursor, ChangeBatch, ChangeOp, Value};

use super::gaps::{Gap, Hits};
use super::ledger::Ledger;
use super::model::{self, Batch, Class, Cursor, Image, Model, Op, Record, Rejection, Status};
use super::schema::Schema;
use super::sim::{Host, Sim, Step};
use super::target::{Stale, Target};
use super::validate::{Kind, Validator};

#[derive(Clone, Copy, Debug)]
pub struct RunConfig {
    pub interval: u64,
    pub depth: usize,
    pub journal: bool,
}

impl RunConfig {
    /// Each seed runs one of the combinations of checkpoint interval, queue
    /// depth and journal mode.
    pub fn of_seed(seed: u64) -> RunConfig {
        RunConfig {
            interval: [1, 3][(seed % 2) as usize],
            depth: [1, 2, 4][(seed / 2 % 3) as usize],
            journal: seed / 6 % 2 == 1,
        }
    }
}

/// The known gaps that histories hit, and the cases they reached.
#[derive(Default, Debug)]
pub struct Report {
    pub hits: Hits,
    pub seen: BTreeMap<String, u64>,
}

impl Report {
    pub fn merge(&mut self, other: &Report) {
        self.hits.merge(&other.hits);
        for (case, n) in &other.seen {
            *self.seen.entry(case.clone()).or_default() += n;
        }
    }
}

/// Runs `steps` calls of the history of `seed`. Panics with the seed and
/// the call log at the first difference.
pub fn run(schema: &Schema, seed: u64, steps: usize) -> Report {
    let mut run = Run::new(schema, seed);
    for n in 0..steps {
        run.step(n);
    }
    Report {
        hits: run.hits,
        seen: run.seen,
    }
}

struct Run<'s> {
    schema: &'s Schema,
    ddl: String,
    seed: u64,
    cfg: RunConfig,
    storage: Arc<MemoryBackend>,
    /// None only while a crash replaces the instance.
    db: Option<Settle>,
    model: Model<'s>,
    sim: Sim<'s>,
    /// What the target applied.
    target: Target,
    /// The target with every returned batch applied, the image before the next batch.
    projected: Target,
    unapplied: VecDeque<Batch>,
    applied_unacked: Vec<Batch>,
    stale: Stale,
    validator: Validator,
    ledger: Ledger,
    hits: Hits,
    seen: BTreeMap<String, u64>,
    /// The view deletes of the current batch that merge a compensation update (GAP-47).
    merged: HashSet<(String, model::Key)>,
    /// From an open to the next ingest, Settle reports `D` as its finality (GAP-3).
    finality_lags: bool,
    log: Vec<String>,
}

impl<'s> Run<'s> {
    fn new(schema: &'s Schema, seed: u64) -> Self {
        let cfg = RunConfig::of_seed(seed);
        let ddl = schema.ddl();
        let storage = Arc::new(MemoryBackend::new());
        let db = open(&ddl, &storage, &cfg, 0);
        let model_cfg = model::Config {
            max_unacked: cfg.depth,
            interval: cfg.interval,
            journal: cfg.journal,
        };

        Run {
            schema,
            ddl,
            seed,
            cfg,
            storage,
            db: Some(db),
            model: Model::new(schema, model_cfg),
            sim: Sim::new(schema, seed),
            target: Target::default(),
            projected: Target::default(),
            unapplied: VecDeque::new(),
            applied_unacked: Vec::new(),
            stale: Stale::default(),
            validator: Validator::default(),
            ledger: Ledger::default(),
            hits: Hits::default(),
            seen: BTreeMap::new(),
            merged: HashSet::new(),
            finality_lags: false,
            log: Vec::new(),
        }
    }

    #[track_caller]
    fn fail(&self, what: String) -> ! {
        panic!(
            "CT-1 {} seed {} {:?}: {what}\nreplay with SETTLE_SEED={}\n\n{}",
            self.schema.name,
            self.seed,
            self.cfg,
            self.seed,
            self.log.join("\n")
        );
    }

    fn db(&mut self) -> &mut Settle {
        self.db.as_mut().expect("an open instance")
    }

    fn host(&self) -> Host {
        Host {
            unapplied: self.unapplied.len(),
            applied_unacked: self.applied_unacked.iter().map(|b| b.seq).collect(),
            newest: self
                .unapplied
                .back()
                .or(self.applied_unacked.last())
                .map(|b| b.seq),
            places: self.model.places(),
            max_unacked: self.cfg.depth,
            finality_lags: self.finality_lags,
        }
    }

    fn saw(&mut self, case: impl Into<String>) {
        *self.seen.entry(case.into()).or_default() += 1;
    }

    fn step(&mut self, n: usize) {
        let step = self.sim.next(&self.model.st, &self.host());
        self.log.push(format!("{n}: {step:?}"));

        match step {
            Step::Ingest {
                rows, chain, fin, ..
            } => {
                let before = self.model.out();
                let expected = self.model.ingest(&rows, &chain, &fin);
                let input = IngestInput {
                    data: rows.clone().into_iter().collect(),
                    rollback_chain: chain.iter().map(cursor).collect(),
                    finalized_head: cursor(&fin),
                };
                for (table, table_rows) in &rows {
                    self.ledger.record(self.schema, table, table_rows);
                }
                let actual = self.db().ingest(input);
                if self.call(&before, expected, actual) {
                    self.finality_lags = false;
                    if self.model.in_line_fork() {
                        self.saw("in-line fork");
                    }
                }
            }
            Step::Fork { chain, .. } => {
                let before = self.model.out();
                let expected = self.model.fork(&chain);
                let actual = self.db().handle_fork(chain.iter().map(cursor).collect());

                let (expected, expected_cursor) = match expected {
                    Ok((cursor, batch)) => (Ok(batch), Some(cursor)),
                    Err(e) => (Err(e), None),
                };
                let (actual, actual_cursor) = match actual {
                    Ok(result) => (
                        Ok(result.batch),
                        Some((result.cursor.number, result.cursor.hash)),
                    ),
                    Err(e) => (Err(e), None),
                };
                if expected_cursor != actual_cursor {
                    self.fail(format!(
                        "fork point {actual_cursor:?}, the model {expected_cursor:?}"
                    ));
                }
                if self.call(&before, expected, actual) {
                    self.saw("fork call");
                }
            }
            Step::Apply(count) => {
                for _ in 0..count {
                    let batch = self.unapplied.pop_front().expect("a returned batch");
                    self.target.apply(&batch);
                    self.stale.refresh(&batch);
                    self.applied_unacked.push(batch);
                }
            }
            Step::Ack(seq) => {
                let expected = self.model.ack(seq);
                let actual = self.db().ack(seq);
                self.same_result(&expected, &actual);
                if let Err(e) = &expected {
                    self.saw(format!("rejected {:?}", e.class));
                }
                let committed = self.model.committed_seq();
                self.applied_unacked.retain(|b| b.seq > committed);
            }
            Step::Crash => self.crash(),
        }

        self.compare_status();
        self.compare_image();

        let st = &self.model.st;
        let above = st.l.is_some_and(|l| st.f > l);
        let deferred = st.l.is_some_and(|l| st.d.is_some_and(|d| d < st.f.min(l)));
        if above {
            self.saw("finality above latest");
        }
        if deferred {
            self.saw("deferred checkpoint");
        }
    }

    fn crash(&mut self) {
        // RP-21: the target keeps what it applied, the store loses it.
        if !self.cfg.journal {
            for batch in &self.applied_unacked {
                self.stale.add(batch);
            }
        }
        self.saw("crash");
        if !self.applied_unacked.is_empty() {
            self.saw(if self.cfg.journal {
                "open commits the journal"
            } else {
                "crash loses applied batches"
            });
        }

        let applied = self.target.applied;
        self.db = None;
        self.db = Some(open(&self.ddl, &self.storage, &self.cfg, applied));
        self.model
            .crash_and_open(self.cfg.journal.then_some(applied));

        self.unapplied.clear();
        self.applied_unacked.clear();
        self.projected = self.target.clone();
        self.validator.reopen();
        self.finality_lags = true;
    }

    /// Compares one ingest or fork call; true when it succeeded.
    fn call(
        &mut self,
        before: &Image,
        expected: Result<Option<Batch>, Rejection>,
        actual: Result<Option<ChangeBatch>, Error>,
    ) -> bool {
        self.same_result(&expected, &actual);
        if let Err(e) = &expected {
            self.saw(format!("rejected {:?}", e.class));
        }
        let (Ok(expected), Ok(actual)) = (expected, actual) else {
            return false;
        };
        self.saw(if actual.is_some() {
            "call with a batch"
        } else {
            "call without a batch"
        });

        let actual =
            actual.map(|batch| normalize(self.schema, &batch).unwrap_or_else(|e| self.fail(e)));
        let expected = match (expected, &actual) {
            (None, None) => return true,
            (Some(expected), Some(_)) => expected,
            // GAP-41: a batch where every record is an empty change.
            (None, Some(actual)) => {
                let empty = actual.records.iter().all(|r| self.empty_change(before, r));
                if !(empty && self.model.in_line_fork()) {
                    self.fail(format!("a batch where the model has none: {actual:#?}"));
                }
                self.hits.hit(Gap::Gap41);
                self.model.adopt(actual.records.clone())
            }
            (Some(expected), None) => self.fail(format!("no batch, the model has {expected:#?}")),
        };
        let actual = actual.expect("a batch");

        self.compare_batch(before, &expected, &actual);
        self.validate(&actual);
        self.projected.apply(&actual);
        self.unapplied.push_back(actual);
        true
    }

    fn same_result<T, U>(&self, expected: &Result<T, Rejection>, actual: &Result<U, Error>) {
        match (expected, actual) {
            (Ok(_), Ok(_)) => {}
            (Err(e), Err(a)) if e.class == class(a) => {}
            (Err(e), Err(a)) => self.fail(format!("error {a}, the model {e:?}")),
            (Err(e), Ok(_)) => self.fail(format!("success, the model rejects: {e:?}")),
            (Ok(_), Err(a)) => self.fail(format!("error {a}, the model succeeds")),
        }
    }

    /// An update to the values that the key held before the call (RP-6).
    fn empty_change(&self, before: &Image, record: &Record) -> bool {
        let held = before.get(&record.table).and_then(|t| t.get(&record.key));
        record.op == Op::Update && held == Some(&record.values)
    }

    /// GAP-10: a raw record without previous values. A raw delete comes only
    /// from a rollback, and such an update only from the delete and insert of
    /// one key in an in-line fork.
    fn gap10(&self, table: &str, op: Op) -> bool {
        let raw = self.schema.table(table).is_some();
        let shows = match op {
            Op::Delete => self.model.rolled_back.is_some(),
            Op::Update => self.model.in_line_fork(),
            Op::Insert => false,
        };
        raw && shows
    }

    fn compare_batch(&mut self, before: &Image, expected: &Batch, actual: &Batch) {
        self.merged.clear();
        if (expected.seq, &expected.latest) != (actual.seq, &actual.latest) {
            self.fail(format!(
                "sequence {} and latest head {:?}, the model {} and {:?}",
                actual.seq, actual.latest, expected.seq, expected.latest
            ));
        }
        if expected.finalized != actual.finalized {
            if !self.gap3_head(&actual.finalized) {
                self.fail(format!(
                    "finalized head {:?}, the model {:?}",
                    actual.finalized, expected.finalized
                ));
            }
            self.hits.hit(Gap::Gap3);
        }

        let by_key = |batch: &Batch| -> HashMap<(String, model::Key), Record> {
            batch
                .records
                .iter()
                .map(|r| ((r.table.clone(), r.key.clone()), r.clone()))
                .collect()
        };
        let mut expected = by_key(expected);
        let mut diffs = Vec::new();

        for record in &actual.records {
            let Some(model) = expected.remove(&(record.table.clone(), record.key.clone())) else {
                if self.model.in_line_fork() && self.empty_change(before, record) {
                    self.hits.hit(Gap::Gap41);
                } else {
                    diffs.push(format!("extra {record:?}"));
                }
                continue;
            };

            let same_prev = model.prev == record.prev;
            let gap10 = !same_prev && record.prev.is_none() && self.gap10(&record.table, record.op);
            if gap10 {
                self.hits.hit(Gap::Gap10);
            }
            let gap47 = record.op == Op::Delete
                && model.op == Op::Delete
                && model.values != record.values
                && self.schema.view(&record.table).is_some()
                && self.model.after_rollback(&record.table, &record.key)
                    == Some(record.values.clone());
            if gap47 {
                self.hits.hit(Gap::Gap47);
                self.merged
                    .insert((record.table.clone(), record.key.clone()));
            }
            let same_values = model.values == record.values || gap47;
            if model.op != record.op || !same_values || !(same_prev || gap10) {
                diffs.push(format!("{record:?}\n  the model {model:?}"));
            }
        }
        diffs.extend(expected.values().map(|r| format!("missing {r:?}")));

        if !diffs.is_empty() {
            diffs.sort();
            self.fail(format!(
                "batch {} differs:\n{}",
                actual.seq,
                diffs.join("\n")
            ));
        }
    }

    /// The validators and the ledger on a returned batch, before it is applied.
    fn validate(&mut self, batch: &Batch) {
        let mut found = self
            .validator
            .check(self.schema, batch, &self.projected, &self.stale);
        found.extend(self.ledger.check(self.schema, batch));

        let ops: HashMap<_, _> = batch
            .records
            .iter()
            .map(|r| ((r.table.clone(), r.key.clone()), r.op))
            .collect();
        let mut unexplained = Vec::new();
        for violation in found {
            let key = (violation.table.clone(), violation.key.clone());
            let op = ops.get(&key).copied().unwrap_or(Op::Insert);
            let gap = match violation.kind {
                Kind::MissingPrev if self.gap10(&violation.table, op) => Some(Gap::Gap10),
                Kind::EmptyChange if self.model.in_line_fork() => Some(Gap::Gap41),
                Kind::DeleteValues if self.merged.contains(&key) => Some(Gap::Gap47),
                _ => None,
            };
            match gap {
                Some(gap) => self.hits.hit(gap),
                None => unexplained.push(format!("{} {violation:?}", violation.kind.rule())),
            }
        }
        if !unexplained.is_empty() {
            self.fail(format!(
                "validators on batch {}:\n{}",
                batch.seq,
                unexplained.join("\n")
            ));
        }
    }

    /// After an open Settle reports `(D, H(D))` as its finalized head (GAP-3).
    fn gap3_head(&self, head: &Option<Cursor>) -> bool {
        let st = &self.model.st;
        let durable = st.d.unwrap_or(0);
        let expected = st.h.get(&durable).map(|h| (durable, h.clone()));
        self.finality_lags && durable < st.f && *head == expected
    }

    /// The status reads, twice (INV-13, INV-30).
    fn compare_status(&mut self) {
        let mut expected = self.model.status();
        let db = self.db.as_ref().expect("an open instance");
        let actual = status(db);
        if actual != status(db) {
            self.fail("a status read changed the status".into());
        }

        // A poisoned instance holds state that it cannot commit (WP-71).
        if expected.poisoned || actual.poisoned {
            if expected.poisoned != actual.poisoned {
                self.fail(format!(
                    "poisoned {}, the model {}",
                    actual.poisoned, expected.poisoned
                ));
            }
            return;
        }

        let lags = (actual.finalized_block, &actual.finalized_cursor)
            != (expected.finalized_block, &expected.finalized_cursor);
        if lags
            && self.gap3_head(&actual.finalized_cursor)
            && actual.finalized_block == expected.durable_block
        {
            self.hits.hit(Gap::Gap3);
            expected.finalized_block = actual.finalized_block;
            expected.finalized_cursor = actual.finalized_cursor.clone();
        }
        if actual != expected {
            self.fail(format!("status {actual:#?}\nthe model {expected:#?}"));
        }
    }

    /// INV-20: the target, with every returned batch, holds the output image,
    /// except at the keys that a crash left stale.
    fn compare_image(&mut self) {
        let expected = self.model.out();
        let empty = HashMap::new();
        let mut diffs = Vec::new();

        for table in expected.keys().chain(self.projected.image.keys()) {
            let ours = self.projected.image.get(table).unwrap_or(&empty);
            let theirs = expected.get(table).unwrap_or(&empty);
            for key in ours.keys().chain(theirs.keys()) {
                if self.stale.contains(table, key) {
                    continue;
                }
                let (held, want) = (ours.get(key), theirs.get(key));
                if held != want {
                    diffs.push(format!("{table} {key:?}: target {held:?}, out(A) {want:?}"));
                }
            }
        }

        if !diffs.is_empty() {
            diffs.sort();
            diffs.dedup();
            self.fail(format!(
                "the target differs from out(A):\n{}",
                diffs.join("\n")
            ));
        }
    }
}

fn open(ddl: &str, storage: &Arc<MemoryBackend>, cfg: &RunConfig, applied: u64) -> Settle {
    let storage: Arc<dyn StorageBackend> = storage.clone();
    let mut config = Config::new(ddl)
        .storage(storage)
        .max_pending_batches(cfg.depth)
        .backfill_checkpoint_interval(cfg.interval)
        .raw_retention_blocks(0)
        .collect_perf(false);
    if cfg.journal {
        config = config.applied_sequence(applied);
    }
    match Settle::open(config) {
        Ok(db) => db,
        Err(e) => panic!("open: {e}\n{ddl}"),
    }
}

fn cursor((number, hash): &Cursor) -> BlockCursor {
    BlockCursor {
        number: *number,
        hash: hash.clone(),
    }
}

fn class(e: &Error) -> Class {
    match e {
        Error::Schema(_) => Class::InvalidSchema,
        Error::IncompatibleStorage(_) => Class::IncompatibleStorage,
        Error::Reducer(_) => Class::ReducerFailed,
        Error::Storage(_)
        | Error::Rollback(_)
        | Error::Serialization(_)
        | Error::Deserialization(_) => Class::StorageFailed,
        Error::InvalidOperation(_) => Class::InvalidOperation,
        Error::PendingAck { .. } => Class::PendingAck,
        Error::WrongAckSequence { .. } => Class::WrongAckSequence,
        Error::Poisoned(_) => Class::Poisoned,
        Error::SyncFailed(_) => Class::SyncFailed,
    }
}

fn status(db: &Settle) -> Status {
    let cursor = |c: Option<BlockCursor>| c.map(|c| (c.number, c.hash));
    Status {
        latest_block: db.latest_block(),
        latest_cursor: cursor(db.latest_cursor()),
        finalized_block: db.finalized_block(),
        finalized_cursor: cursor(db.finalized_cursor()),
        durable_block: db.durable_block(),
        unacked: db.unacked_batches(),
        poisoned: db.is_poisoned(),
    }
}

/// A Settle batch in the form of the model, with the column layout checked (RP-2).
fn normalize(schema: &Schema, batch: &ChangeBatch) -> Result<Batch, String> {
    let mut records = Vec::new();
    let tables: BTreeMap<_, _> = batch.tables.iter().collect();

    for (table, table_records) in tables {
        let columns = schema.is_output(table).then(|| schema.columns(table));
        for record in table_records {
            let names: Vec<String> = record.values().map(|(name, _)| name.to_string()).collect();
            if columns.as_ref().is_some_and(|columns| *columns != names) {
                return Err(format!(
                    "RP-2: {table} has columns {names:?}, the schema {columns:?}"
                ));
            }

            let values =
                |it: &mut dyn Iterator<Item = (&str, &Value)>| it.map(|(_, v)| v.clone()).collect();
            records.push(Record {
                table: table.clone(),
                op: match record.operation {
                    ChangeOp::Insert => Op::Insert,
                    ChangeOp::Update => Op::Update,
                    ChangeOp::Delete => Op::Delete,
                },
                key: values(&mut record.key_values()),
                values: values(&mut record.values()),
                prev: record.prev_values().map(|mut prev| values(&mut prev)),
            });
        }
    }

    let head = |c: &Option<BlockCursor>| c.as_ref().map(|c| (c.number, c.hash.clone()));
    Ok(Batch {
        seq: batch.sequence,
        latest: head(&batch.latest_head),
        finalized: head(&batch.finalized_head),
        records,
    })
}
