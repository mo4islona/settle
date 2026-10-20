//! CT-1: random histories against the reference model, with the structural
//! validators on every batch.

use crate::harness::driver::{self, Report};
use crate::harness::gaps::Gap;
use crate::harness::rng::seeds;
use crate::harness::schema::Schema;
use crate::schemas;

const SEEDS: u64 = 300;
const STEPS: usize = 40;

fn check(schema: Schema, seeds: std::ops::Range<u64>, steps: usize) -> Report {
    let mut report = Report::default();
    for seed in seeds {
        report.merge(&driver::run(&schema, seed, steps));
    }
    report
}

/// A sample of the histories of the schemas that reach every case.
fn sample() -> Report {
    let mut report = check(schemas::sum(), 0..200, STEPS);
    report.merge(&check(schemas::reducer(), 0..200, STEPS));
    report.merge(&check(schemas::sliding(), 0..200, STEPS));
    report
}

#[test]
fn ct01_sum() {
    check(schemas::sum(), seeds(SEEDS), STEPS);
}

#[test]
fn ct01_sum_short() {
    check(schemas::sum(), seeds(5000), 8);
}

#[test]
fn ct01_two_tables() {
    check(schemas::two_tables(), seeds(SEEDS), STEPS);
}

#[test]
fn ct01_tumbling() {
    check(schemas::tumbling(), seeds(SEEDS), STEPS);
}

#[test]
fn ct01_sliding() {
    check(schemas::sliding(), seeds(SEEDS), STEPS);
}

#[test]
fn ct01_sliding_no_expiry() {
    check(schemas::sliding_no_expiry(), seeds(SEEDS), STEPS);
}

#[test]
fn ct01_reducer() {
    check(schemas::reducer(), seeds(SEEDS), STEPS);
}

#[test]
fn ct01_map() {
    check(schemas::map(), seeds(SEEDS), STEPS);
}

/// Each tolerated gap still shows. When a fix makes one vanish, this test
/// fails: remove the gap from `gaps.rs` and move its rows in the spec.
#[test]
fn known_gaps_still_reproduce() {
    let hits = sample().hits;
    let missing: Vec<Gap> = Gap::ALL
        .into_iter()
        .filter(|g| !hits.0.contains_key(g))
        .collect();
    assert!(
        missing.is_empty(),
        "no longer seen: {missing:?}, hits {hits:?}"
    );
}

/// The simulator still reaches each case that CT-1 claims to check.
#[test]
fn histories_reach_every_case() {
    let seen = sample().seen;
    let cases = [
        "call with a batch",
        "call without a batch",
        "in-line fork",
        "fork call",
        "crash",
        "crash loses applied batches",
        "open commits the journal",
        "finality above latest",
        "deferred checkpoint",
        "rejected PendingAck",
        "rejected InvalidOperation",
        "rejected WrongAckSequence",
        "rejected ReducerFailed",
        "rejected Poisoned",
    ];
    let missing: Vec<&str> = cases
        .into_iter()
        .filter(|c| !seen.contains_key(*c))
        .collect();
    assert!(missing.is_empty(), "not reached: {missing:?}\n{seen:#?}");
    eprintln!("{seen:#?}");
}
