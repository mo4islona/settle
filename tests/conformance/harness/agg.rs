//! Aggregates and windows of the model, written from the spec (DEF-32,
//! DEF-33, DEF-42 to DEF-45), not from the engine.

use settle::types::Value;

use super::schema::{Func, Type};

/// DEF-43: the arithmetic of `sum`, `min`, `max` and `avg` for a column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// i128, saturating.
    Int,
    Float,
}

impl Mode {
    pub fn of(ty: Option<Type>) -> Mode {
        match ty {
            Some(Type::UInt64 | Type::Int64 | Type::DateTime) => Mode::Int,
            _ => Mode::Float,
        }
    }
}

#[derive(Clone, Copy)]
enum Num {
    Int(i128),
    Float(f64),
}

/// The value as the mode reads it; None for a value outside the mode, and Null.
fn read(mode: Mode, value: &Value) -> Option<Num> {
    match (mode, value) {
        (Mode::Int, Value::UInt64(v)) => Some(Num::Int(*v as i128)),
        (Mode::Int, Value::Int64(v) | Value::DateTime(v)) => Some(Num::Int(*v as i128)),
        (Mode::Float, Value::UInt64(v)) => Some(Num::Float(*v as f64)),
        (Mode::Float, Value::Int64(v) | Value::DateTime(v)) => Some(Num::Float(*v as f64)),
        (Mode::Float, Value::Float64(v)) => Some(Num::Float(*v)),
        _ => None,
    }
}

/// The result type of the Int mode: UInt64 when it fits, else Int64, else Float64.
fn int_value(v: i128) -> Value {
    if let Ok(v) = u64::try_from(v) {
        Value::UInt64(v)
    } else if let Ok(v) = i64::try_from(v) {
        Value::Int64(v)
    } else {
        Value::Float64(v as f64)
    }
}

fn add(a: Num, b: Num) -> Num {
    match (a, b) {
        (Num::Int(a), Num::Int(b)) => Num::Int(a.saturating_add(b)),
        (Num::Float(a), Num::Float(b)) => Num::Float(a + b),
        _ => unreachable!("one mode per aggregate"),
    }
}

fn zero(mode: Mode) -> Num {
    match mode {
        Mode::Int => Num::Int(0),
        Mode::Float => Num::Float(0.0),
    }
}

fn to_value(n: Num) -> Value {
    match n {
        Num::Int(v) => int_value(v),
        Num::Float(v) => Value::Float64(v),
    }
}

/// One aggregate over the rows of one group. `blocks` holds the values of
/// the column for each block, in chain order; for `count()` each row gives
/// a non-Null value. Returns the result and whether the aggregate has data.
pub fn aggregate(func: Func, mode: Mode, blocks: &[Vec<Value>]) -> (Value, bool) {
    let rows = blocks.iter().map(Vec::len).sum::<usize>();
    let values = || blocks.iter().flatten().filter(|v| !v.is_null());

    match func {
        Func::Count => (Value::UInt64(values().count() as u64), rows > 0),
        Func::Sum => {
            let (sum, n) = two_level_sum(mode, blocks);
            (to_value(sum), n > 0)
        }
        Func::Avg => {
            let (sum, n) = two_level_sum(mode, blocks);
            if n == 0 {
                return (Value::Null, false);
            }
            let sum = match sum {
                Num::Int(v) => v as f64,
                Num::Float(v) => v,
            };
            (Value::Float64(sum / n as f64), true)
        }
        Func::Min | Func::Max => {
            // DEF-42: a NaN does not count, also for whether there is data.
            let numbers = values()
                .filter_map(|v| read(mode, v))
                .filter(|n| !matches!(n, Num::Float(f) if f.is_nan()));
            let best = numbers.reduce(|a, b| match (a, b) {
                (Num::Int(a), Num::Int(b)) => Num::Int(if func == Func::Min {
                    a.min(b)
                } else {
                    a.max(b)
                }),
                (Num::Float(a), Num::Float(b)) => Num::Float(if func == Func::Min {
                    a.min(b)
                } else {
                    a.max(b)
                }),
                _ => unreachable!("one mode per aggregate"),
            });
            match best {
                Some(n) => (to_value(n), true),
                None => (Value::Null, false),
            }
        }
        Func::First => match values().next() {
            Some(v) => (v.clone(), true),
            None => (Value::Null, false),
        },
        Func::Last => match values().last() {
            Some(v) => (v.clone(), true),
            None => (Value::Null, false),
        },
    }
}

/// DEF-42: the values of each block fold into one contribution from zero,
/// then the contributions fold from zero. Returns the sum and the number of
/// values that the mode read.
fn two_level_sum(mode: Mode, blocks: &[Vec<Value>]) -> (Num, usize) {
    let mut total = zero(mode);
    let mut n = 0;
    for block in blocks {
        let mut contribution = zero(mode);
        for value in block.iter().filter_map(|v| read(mode, v)) {
            contribution = add(contribution, value);
            n += 1;
        }
        total = add(total, contribution);
    }
    (total, n)
}

/// A timestamp as milliseconds, for the windows of DEF-32 and DEF-33.
pub fn millis(value: &Value) -> Option<i64> {
    match value {
        Value::DateTime(v) | Value::Int64(v) => Some(*v),
        Value::UInt64(v) => Some(*v as i64),
        _ => None,
    }
}

/// DEF-32: `trunc(t / I) × I`.
pub fn window_start(value: &Value, seconds: u64) -> Value {
    let interval = seconds as i64 * 1000;
    let t = millis(value).unwrap_or(0);
    Value::DateTime(t / interval * interval)
}

/// DEF-33 and DEF-45: which blocks are in a sliding window. `blocks` holds
/// the timestamps of the rows of each block, in chain order.
pub fn in_window(blocks: &[Vec<Option<i64>>], seconds: u64) -> Vec<bool> {
    let mut watermark = 0i64;
    let stamps: Vec<i64> = blocks
        .iter()
        .map(|rows| {
            let ts = rows.iter().flatten().copied().max().unwrap_or(watermark);
            watermark = watermark.max(ts);
            ts
        })
        .collect();

    let cutoff = watermark.saturating_sub((seconds as i64).saturating_mul(1000));
    stamps.iter().map(|&ts| ts >= cutoff).collect()
}

#[test]
fn min_and_max_skip_nan() {
    let nan = || Value::Float64(f64::NAN);
    let only_nan = [vec![nan()], vec![nan(), nan()]];
    let mixed = [
        vec![nan(), Value::Float64(2.0)],
        vec![Value::Float64(-1.0), nan()],
    ];

    for func in [Func::Min, Func::Max] {
        assert_eq!(
            aggregate(func, Mode::Float, &only_nan),
            (Value::Null, false),
            "{func:?}"
        );
    }
    assert_eq!(
        aggregate(Func::Min, Mode::Float, &mixed),
        (Value::Float64(-1.0), true)
    );
    assert_eq!(
        aggregate(Func::Max, Mode::Float, &mixed),
        (Value::Float64(2.0), true)
    );
}
