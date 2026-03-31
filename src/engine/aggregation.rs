use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::types::{BlockNumber, Value};

// ---------------------------------------------------------------------------
// Numeric accumulator — mode determined by column type at construction
// ---------------------------------------------------------------------------

use crate::types::ColumnType;
use ethnum::U256;

/// Whether an aggregation column uses integer, float, or big-integer arithmetic.
/// Determined once at construction from the source column's declared type.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum NumMode {
    Int,
    Float,
    BigInt,
}

impl NumMode {
    fn from_column_type(ct: &ColumnType) -> Self {
        match ct {
            ColumnType::UInt64 | ColumnType::Int64 | ColumnType::DateTime => NumMode::Int,
            ColumnType::Uint256 => NumMode::BigInt,
            _ => NumMode::Float,
        }
    }
}

/// Accumulates numeric values in i128, f64, or U256 depending on column type.
/// The variant is fixed at construction — no mixed-type branches.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum NumAccum {
    Int(i128),
    Float(f64),
    BigInt(#[serde(with = "u256_serde")] U256),
}

mod u256_serde {
    use ethnum::U256;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &U256, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(&v.to_be_bytes())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<U256, D::Error> {
        use serde::de::Error;
        let bytes: &[u8] = <&[u8]>::deserialize(d)?;
        if bytes.len() != 32 {
            return Err(D::Error::invalid_length(bytes.len(), &"32 bytes for U256"));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(bytes);
        Ok(U256::from_be_bytes(arr))
    }
}

impl NumAccum {
    fn zero(mode: NumMode) -> Self {
        match mode {
            NumMode::Int => NumAccum::Int(0),
            NumMode::Float => NumAccum::Float(0.0),
            NumMode::BigInt => NumAccum::BigInt(U256::ZERO),
        }
    }

    /// Extract a numeric value, coercing to the accumulator's mode.
    fn from_value(v: &Value, mode: NumMode) -> Option<Self> {
        match mode {
            NumMode::Int => match v {
                Value::UInt64(n) => Some(NumAccum::Int(*n as i128)),
                Value::Int64(n) => Some(NumAccum::Int(*n as i128)),
                Value::DateTime(n) => Some(NumAccum::Int(*n as i128)),
                _ => None,
            },
            NumMode::Float => v.as_f64().map(NumAccum::Float),
            NumMode::BigInt => match v {
                Value::Uint256(bytes) => Some(NumAccum::BigInt(U256::from_be_bytes(*bytes))),
                Value::UInt64(n) => Some(NumAccum::BigInt(U256::from(*n))),
                _ => None,
            },
        }
    }

    fn add(self, other: NumAccum) -> NumAccum {
        match (self, other) {
            (NumAccum::Int(a), NumAccum::Int(b)) => NumAccum::Int(a.saturating_add(b)),
            (NumAccum::Float(a), NumAccum::Float(b)) => NumAccum::Float(a + b),
            (NumAccum::BigInt(a), NumAccum::BigInt(b)) => NumAccum::BigInt(a.saturating_add(b)),
            _ => unreachable!("mode is fixed at construction"),
        }
    }

    fn to_f64(self) -> f64 {
        match self {
            NumAccum::Int(v) => v as f64,
            NumAccum::Float(v) => v,
            NumAccum::BigInt(v) => v.as_f64(),
        }
    }

    fn to_value(self) -> Value {
        match self {
            NumAccum::Int(v) => {
                if v >= 0 && v <= u64::MAX as i128 {
                    Value::UInt64(v as u64)
                } else if v >= i64::MIN as i128 && v <= i64::MAX as i128 {
                    Value::Int64(v as i64)
                } else {
                    Value::Float64(v as f64)
                }
            }
            NumAccum::Float(v) => Value::Float64(v),
            NumAccum::BigInt(v) => Value::Uint256(v.to_be_bytes()),
        }
    }

    fn min(self, other: NumAccum) -> NumAccum {
        match (self, other) {
            (NumAccum::Int(a), NumAccum::Int(b)) => NumAccum::Int(a.min(b)),
            (NumAccum::Float(a), NumAccum::Float(b)) => NumAccum::Float(a.min(b)),
            (NumAccum::BigInt(a), NumAccum::BigInt(b)) => NumAccum::BigInt(a.min(b)),
            _ => unreachable!("mode is fixed at construction"),
        }
    }

    fn max(self, other: NumAccum) -> NumAccum {
        match (self, other) {
            (NumAccum::Int(a), NumAccum::Int(b)) => NumAccum::Int(a.max(b)),
            (NumAccum::Float(a), NumAccum::Float(b)) => NumAccum::Float(a.max(b)),
            (NumAccum::BigInt(a), NumAccum::BigInt(b)) => NumAccum::BigInt(a.max(b)),
            _ => unreachable!("mode is fixed at construction"),
        }
    }
}

/// Trait for rollback-aware aggregation functions.
///
/// Each function tracks per-block contributions separately from finalized state,
/// enabling surgical rollback to any block boundary.
pub trait AggregationFunc: Send + Sync {
    /// Feed values from a single block into the aggregation.
    fn add_block(&mut self, block: BlockNumber, values: &[Value]);

    /// Remove a block's contributions (rollback).
    fn remove_block(&mut self, block: BlockNumber);

    /// Remove all blocks after fork_point in one operation (batch rollback).
    /// Default implementation calls remove_block for each, but implementations
    /// can use split_off for O(log N) performance.
    fn remove_blocks_after(&mut self, fork_point: BlockNumber);

    /// Finalize all blocks up to and including the given block.
    /// Merges their contributions into the finalized state and discards per-block data.
    fn finalize_up_to(&mut self, block: BlockNumber);

    /// Compute the current aggregated value (finalized + all unfinalized blocks).
    fn current_value(&self) -> Value;

    /// Returns true if the aggregation has any data (finalized or unfinalized).
    fn has_data(&self) -> bool;

    /// Serialize to bytes for persistence.
    fn to_bytes(&self) -> Vec<u8>;

    /// Serialize only the finalized portion (no unfinalized block data).
    /// Used for crash-safe MV persistence — unfinalized blocks are replayed on recovery.
    fn to_finalized_bytes(&self) -> Vec<u8>;

    /// Deserialize from bytes.
    fn from_bytes(bytes: &[u8]) -> Self
    where
        Self: Sized;

    /// Return the block numbers that have unfinalized contributions.
    /// Used for rebuilding block_groups from persisted sliding window state.
    fn block_numbers(&self) -> Vec<BlockNumber>;
}

// ---------------------------------------------------------------------------
// Sum
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SumAgg {
    mode: NumMode,
    finalized: NumAccum,
    blocks: BTreeMap<BlockNumber, NumAccum>,
    has_finalized: bool,
}

impl SumAgg {
    pub fn new(column_type: &ColumnType) -> Self {
        let mode = NumMode::from_column_type(column_type);
        Self {
            mode,
            finalized: NumAccum::zero(mode),
            blocks: BTreeMap::new(),
            has_finalized: false,
        }
    }
}

impl AggregationFunc for SumAgg {
    fn add_block(&mut self, block: BlockNumber, values: &[Value]) {
        let mut has_values = false;
        let partial = values
            .iter()
            .filter_map(|v| {
                let n = NumAccum::from_value(v, self.mode);
                if n.is_some() {
                    has_values = true;
                }
                n
            })
            .fold(NumAccum::zero(self.mode), NumAccum::add);
        if has_values {
            let entry = self
                .blocks
                .entry(block)
                .or_insert(NumAccum::zero(self.mode));
            *entry = entry.add(partial);
        }
    }

    fn remove_block(&mut self, block: BlockNumber) {
        self.blocks.remove(&block);
    }

    fn remove_blocks_after(&mut self, fork_point: BlockNumber) {
        let _ = self.blocks.split_off(&(fork_point + 1));
    }

    fn finalize_up_to(&mut self, block: BlockNumber) {
        let to_finalize: Vec<_> = self.blocks.range(..=block).map(|(&b, &v)| (b, v)).collect();
        if !to_finalize.is_empty() {
            self.has_finalized = true;
        }
        for (b, v) in to_finalize {
            self.finalized = self.finalized.add(v);
            self.blocks.remove(&b);
        }
    }

    fn current_value(&self) -> Value {
        let total = self
            .blocks
            .values()
            .copied()
            .fold(self.finalized, NumAccum::add);
        total.to_value()
    }

    fn has_data(&self) -> bool {
        self.has_finalized || !self.blocks.is_empty()
    }

    fn to_bytes(&self) -> Vec<u8> {
        rmp_serde::to_vec(self).unwrap()
    }

    fn to_finalized_bytes(&self) -> Vec<u8> {
        let mut copy = self.clone();
        copy.blocks.clear();
        rmp_serde::to_vec(&copy).unwrap()
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        rmp_serde::from_slice(bytes).unwrap()
    }

    fn block_numbers(&self) -> Vec<BlockNumber> {
        self.blocks.keys().copied().collect()
    }
}

// ---------------------------------------------------------------------------
// Count
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountAgg {
    finalized: u64,
    blocks: BTreeMap<BlockNumber, u64>,
}

impl CountAgg {
    pub fn new() -> Self {
        Self {
            finalized: 0,
            blocks: BTreeMap::new(),
        }
    }
}

impl AggregationFunc for CountAgg {
    fn add_block(&mut self, block: BlockNumber, values: &[Value]) {
        let count = values.iter().filter(|v| !v.is_null()).count() as u64;
        *self.blocks.entry(block).or_insert(0) += count;
    }

    fn remove_block(&mut self, block: BlockNumber) {
        self.blocks.remove(&block);
    }

    fn remove_blocks_after(&mut self, fork_point: BlockNumber) {
        let _ = self.blocks.split_off(&(fork_point + 1));
    }

    fn finalize_up_to(&mut self, block: BlockNumber) {
        let to_finalize: Vec<_> = self.blocks.range(..=block).map(|(&b, &v)| (b, v)).collect();
        for (b, v) in to_finalize {
            self.finalized += v;
            self.blocks.remove(&b);
        }
    }

    fn current_value(&self) -> Value {
        let total = self.finalized + self.blocks.values().sum::<u64>();
        Value::UInt64(total)
    }

    fn has_data(&self) -> bool {
        self.finalized > 0 || !self.blocks.is_empty()
    }

    fn to_bytes(&self) -> Vec<u8> {
        rmp_serde::to_vec(self).unwrap()
    }

    fn to_finalized_bytes(&self) -> Vec<u8> {
        let mut copy = self.clone();
        copy.blocks.clear();
        rmp_serde::to_vec(&copy).unwrap()
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        rmp_serde::from_slice(bytes).unwrap()
    }

    fn block_numbers(&self) -> Vec<BlockNumber> {
        self.blocks.keys().copied().collect()
    }
}

// ---------------------------------------------------------------------------
// Min
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MinAgg {
    mode: NumMode,
    finalized: Option<NumAccum>,
    blocks: BTreeMap<BlockNumber, NumAccum>,
}

impl MinAgg {
    pub fn new(column_type: &ColumnType) -> Self {
        Self {
            mode: NumMode::from_column_type(column_type),
            finalized: None,
            blocks: BTreeMap::new(),
        }
    }
}

impl AggregationFunc for MinAgg {
    fn add_block(&mut self, block: BlockNumber, values: &[Value]) {
        let block_min = values
            .iter()
            .filter_map(|v| NumAccum::from_value(v, self.mode))
            .reduce(NumAccum::min);
        if let Some(min_val) = block_min {
            let entry = self.blocks.entry(block).or_insert(min_val);
            *entry = (*entry).min(min_val);
        }
    }

    fn remove_block(&mut self, block: BlockNumber) {
        self.blocks.remove(&block);
    }

    fn remove_blocks_after(&mut self, fork_point: BlockNumber) {
        let _ = self.blocks.split_off(&(fork_point + 1));
    }

    fn finalize_up_to(&mut self, block: BlockNumber) {
        let to_finalize: Vec<_> = self.blocks.range(..=block).map(|(&b, &v)| (b, v)).collect();
        for (b, v) in to_finalize {
            self.finalized = Some(match self.finalized {
                Some(f) => f.min(v),
                None => v,
            });
            self.blocks.remove(&b);
        }
    }

    fn current_value(&self) -> Value {
        let unfinalized_min = self.blocks.values().copied().reduce(NumAccum::min);
        let result = match (self.finalized, unfinalized_min) {
            (Some(f), Some(u)) => Some(f.min(u)),
            (Some(f), None) => Some(f),
            (None, Some(u)) => Some(u),
            (None, None) => None,
        };
        match result {
            Some(v) => v.to_value(),
            None => Value::Null,
        }
    }

    fn has_data(&self) -> bool {
        self.finalized.is_some() || !self.blocks.is_empty()
    }

    fn to_bytes(&self) -> Vec<u8> {
        rmp_serde::to_vec(self).unwrap()
    }

    fn to_finalized_bytes(&self) -> Vec<u8> {
        let mut copy = self.clone();
        copy.blocks.clear();
        rmp_serde::to_vec(&copy).unwrap()
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        rmp_serde::from_slice(bytes).unwrap()
    }

    fn block_numbers(&self) -> Vec<BlockNumber> {
        self.blocks.keys().copied().collect()
    }
}

// ---------------------------------------------------------------------------
// Max
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaxAgg {
    mode: NumMode,
    finalized: Option<NumAccum>,
    blocks: BTreeMap<BlockNumber, NumAccum>,
}

impl MaxAgg {
    pub fn new(column_type: &ColumnType) -> Self {
        Self {
            mode: NumMode::from_column_type(column_type),
            finalized: None,
            blocks: BTreeMap::new(),
        }
    }
}

impl AggregationFunc for MaxAgg {
    fn add_block(&mut self, block: BlockNumber, values: &[Value]) {
        let block_max = values
            .iter()
            .filter_map(|v| NumAccum::from_value(v, self.mode))
            .reduce(NumAccum::max);
        if let Some(max_val) = block_max {
            let entry = self.blocks.entry(block).or_insert(max_val);
            *entry = (*entry).max(max_val);
        }
    }

    fn remove_block(&mut self, block: BlockNumber) {
        self.blocks.remove(&block);
    }

    fn remove_blocks_after(&mut self, fork_point: BlockNumber) {
        let _ = self.blocks.split_off(&(fork_point + 1));
    }

    fn finalize_up_to(&mut self, block: BlockNumber) {
        let to_finalize: Vec<_> = self.blocks.range(..=block).map(|(&b, &v)| (b, v)).collect();
        for (b, v) in to_finalize {
            self.finalized = Some(match self.finalized {
                Some(f) => f.max(v),
                None => v,
            });
            self.blocks.remove(&b);
        }
    }

    fn current_value(&self) -> Value {
        let unfinalized_max = self.blocks.values().copied().reduce(NumAccum::max);
        let result = match (self.finalized, unfinalized_max) {
            (Some(f), Some(u)) => Some(f.max(u)),
            (Some(f), None) => Some(f),
            (None, Some(u)) => Some(u),
            (None, None) => None,
        };
        match result {
            Some(v) => v.to_value(),
            None => Value::Null,
        }
    }

    fn has_data(&self) -> bool {
        self.finalized.is_some() || !self.blocks.is_empty()
    }

    fn to_bytes(&self) -> Vec<u8> {
        rmp_serde::to_vec(self).unwrap()
    }

    fn to_finalized_bytes(&self) -> Vec<u8> {
        let mut copy = self.clone();
        copy.blocks.clear();
        rmp_serde::to_vec(&copy).unwrap()
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        rmp_serde::from_slice(bytes).unwrap()
    }

    fn block_numbers(&self) -> Vec<BlockNumber> {
        self.blocks.keys().copied().collect()
    }
}

// ---------------------------------------------------------------------------
// Avg — stored as (sum, count) internally
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvgAgg {
    mode: NumMode,
    finalized_sum: NumAccum,
    finalized_count: u64,
    blocks: BTreeMap<BlockNumber, (NumAccum, u64)>,
}

impl AvgAgg {
    pub fn new(column_type: &ColumnType) -> Self {
        let mode = NumMode::from_column_type(column_type);
        Self {
            mode,
            finalized_sum: NumAccum::zero(mode),
            finalized_count: 0,
            blocks: BTreeMap::new(),
        }
    }
}

impl AggregationFunc for AvgAgg {
    fn add_block(&mut self, block: BlockNumber, values: &[Value]) {
        let mut sum = NumAccum::zero(self.mode);
        let mut count = 0u64;
        for v in values {
            if let Some(n) = NumAccum::from_value(v, self.mode) {
                sum = sum.add(n);
                count += 1;
            }
        }
        if count > 0 {
            let entry = self
                .blocks
                .entry(block)
                .or_insert((NumAccum::zero(self.mode), 0));
            entry.0 = entry.0.add(sum);
            entry.1 += count;
        }
    }

    fn remove_block(&mut self, block: BlockNumber) {
        self.blocks.remove(&block);
    }

    fn remove_blocks_after(&mut self, fork_point: BlockNumber) {
        let _ = self.blocks.split_off(&(fork_point + 1));
    }

    fn finalize_up_to(&mut self, block: BlockNumber) {
        let to_finalize: Vec<_> = self.blocks.range(..=block).map(|(&b, &v)| (b, v)).collect();
        for (b, (s, c)) in to_finalize {
            self.finalized_sum = self.finalized_sum.add(s);
            self.finalized_count += c;
            self.blocks.remove(&b);
        }
    }

    fn current_value(&self) -> Value {
        let total_sum = self
            .blocks
            .values()
            .map(|(s, _)| *s)
            .fold(self.finalized_sum, NumAccum::add);
        let total_count: u64 =
            self.finalized_count + self.blocks.values().map(|(_, c)| c).sum::<u64>();
        if total_count == 0 {
            Value::Null
        } else if let NumAccum::BigInt(sum) = total_sum {
            // Integer division for Uint256 — no f64 precision loss
            Value::Uint256((sum / U256::from(total_count)).to_be_bytes())
        } else {
            Value::Float64(total_sum.to_f64() / total_count as f64)
        }
    }

    fn has_data(&self) -> bool {
        self.finalized_count > 0 || !self.blocks.is_empty()
    }

    fn to_bytes(&self) -> Vec<u8> {
        rmp_serde::to_vec(self).unwrap()
    }

    fn to_finalized_bytes(&self) -> Vec<u8> {
        let mut copy = self.clone();
        copy.blocks.clear();
        rmp_serde::to_vec(&copy).unwrap()
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        rmp_serde::from_slice(bytes).unwrap()
    }

    fn block_numbers(&self) -> Vec<BlockNumber> {
        self.blocks.keys().copied().collect()
    }
}

// ---------------------------------------------------------------------------
// First — finalized value is immutable once set; per-block first candidates
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirstAgg {
    finalized: Option<Value>,
    /// block -> first value seen in that block
    blocks: BTreeMap<BlockNumber, Value>,
}

impl FirstAgg {
    pub fn new() -> Self {
        Self {
            finalized: None,
            blocks: BTreeMap::new(),
        }
    }
}

impl AggregationFunc for FirstAgg {
    fn add_block(&mut self, block: BlockNumber, values: &[Value]) {
        if self.blocks.contains_key(&block) {
            return; // keep the first value for this block
        }
        if let Some(first) = values.iter().find(|v| !v.is_null()) {
            self.blocks.insert(block, first.clone());
        }
    }

    fn remove_block(&mut self, block: BlockNumber) {
        self.blocks.remove(&block);
    }

    fn remove_blocks_after(&mut self, fork_point: BlockNumber) {
        let _ = self.blocks.split_off(&(fork_point + 1));
    }

    fn finalize_up_to(&mut self, block: BlockNumber) {
        // Finalized first = earliest block's first value
        if self.finalized.is_none() {
            let earliest = self.blocks.range(..=block).next().map(|(_, v)| v.clone());
            if let Some(v) = earliest {
                self.finalized = Some(v);
            }
        }
        let to_remove: Vec<_> = self.blocks.range(..=block).map(|(&b, _)| b).collect();
        for b in to_remove {
            self.blocks.remove(&b);
        }
    }

    fn current_value(&self) -> Value {
        // Finalized takes priority (it's the earliest value ever)
        if let Some(v) = &self.finalized {
            return v.clone();
        }
        // Otherwise pick the earliest unfinalized block's first value
        self.blocks.values().next().cloned().unwrap_or(Value::Null)
    }

    fn has_data(&self) -> bool {
        self.finalized.is_some() || !self.blocks.is_empty()
    }

    fn to_bytes(&self) -> Vec<u8> {
        rmp_serde::to_vec(self).unwrap()
    }

    fn to_finalized_bytes(&self) -> Vec<u8> {
        let mut copy = self.clone();
        copy.blocks.clear();
        rmp_serde::to_vec(&copy).unwrap()
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        rmp_serde::from_slice(bytes).unwrap()
    }

    fn block_numbers(&self) -> Vec<BlockNumber> {
        self.blocks.keys().copied().collect()
    }
}

// ---------------------------------------------------------------------------
// Last — per-block last values; pick latest remaining
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastAgg {
    finalized: Option<Value>,
    /// block -> last value seen in that block
    blocks: BTreeMap<BlockNumber, Value>,
}

impl LastAgg {
    pub fn new() -> Self {
        Self {
            finalized: None,
            blocks: BTreeMap::new(),
        }
    }
}

impl AggregationFunc for LastAgg {
    fn add_block(&mut self, block: BlockNumber, values: &[Value]) {
        // Always update to the last non-null value for this block
        if let Some(last) = values.iter().rev().find(|v| !v.is_null()) {
            self.blocks.insert(block, last.clone());
        }
    }

    fn remove_block(&mut self, block: BlockNumber) {
        self.blocks.remove(&block);
    }

    fn remove_blocks_after(&mut self, fork_point: BlockNumber) {
        let _ = self.blocks.split_off(&(fork_point + 1));
    }

    fn finalize_up_to(&mut self, block: BlockNumber) {
        // Finalized last = latest value from blocks being finalized
        let latest = self
            .blocks
            .range(..=block)
            .next_back()
            .map(|(_, v)| v.clone());
        if let Some(v) = latest {
            self.finalized = Some(v);
        }
        let to_remove: Vec<_> = self.blocks.range(..=block).map(|(&b, _)| b).collect();
        for b in to_remove {
            self.blocks.remove(&b);
        }
    }

    fn current_value(&self) -> Value {
        // Latest unfinalized block takes priority, else finalized
        if let Some((_, v)) = self.blocks.iter().next_back() {
            return v.clone();
        }
        self.finalized.clone().unwrap_or(Value::Null)
    }

    fn has_data(&self) -> bool {
        self.finalized.is_some() || !self.blocks.is_empty()
    }

    fn to_bytes(&self) -> Vec<u8> {
        rmp_serde::to_vec(self).unwrap()
    }

    fn to_finalized_bytes(&self) -> Vec<u8> {
        let mut copy = self.clone();
        copy.blocks.clear();
        rmp_serde::to_vec(&copy).unwrap()
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        rmp_serde::from_slice(bytes).unwrap()
    }

    fn block_numbers(&self) -> Vec<BlockNumber> {
        self.blocks.keys().copied().collect()
    }
}

// ---------------------------------------------------------------------------
// toStartOfInterval — pure function
// ---------------------------------------------------------------------------

/// Truncate a DateTime (milliseconds) to the start of the given interval.
pub fn to_start_of_interval(datetime_ms: i64, interval_seconds: u64) -> i64 {
    let interval_ms = interval_seconds as i64 * 1000;
    if interval_ms == 0 {
        return datetime_ms;
    }
    (datetime_ms / interval_ms) * interval_ms
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

use crate::schema::ast::AggFunc;

/// Create a boxed aggregation function from the schema's AggFunc enum and source column type.
pub fn create_agg(func: &AggFunc, column_type: &ColumnType) -> Box<dyn AggregationFunc> {
    match func {
        AggFunc::Sum => Box::new(SumAgg::new(column_type)),
        AggFunc::Count => Box::new(CountAgg::new()),
        AggFunc::Min => Box::new(MinAgg::new(column_type)),
        AggFunc::Max => Box::new(MaxAgg::new(column_type)),
        AggFunc::Avg => Box::new(AvgAgg::new(column_type)),
        AggFunc::First => Box::new(FirstAgg::new()),
        AggFunc::Last => Box::new(LastAgg::new()),
    }
}

/// Restore a boxed aggregation function from persisted bytes.
pub fn restore_agg(func: &AggFunc, bytes: &[u8]) -> Box<dyn AggregationFunc> {
    match func {
        AggFunc::Sum => Box::new(SumAgg::from_bytes(bytes)),
        AggFunc::Count => Box::new(CountAgg::from_bytes(bytes)),
        AggFunc::Min => Box::new(MinAgg::from_bytes(bytes)),
        AggFunc::Max => Box::new(MaxAgg::from_bytes(bytes)),
        AggFunc::Avg => Box::new(AvgAgg::from_bytes(bytes)),
        AggFunc::First => Box::new(FirstAgg::from_bytes(bytes)),
        AggFunc::Last => Box::new(LastAgg::from_bytes(bytes)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Sum ---

    #[test]
    fn sum_basic() {
        let mut agg = SumAgg::new(&ColumnType::Float64);
        agg.add_block(100, &[Value::Float64(10.0), Value::Float64(20.0)]);
        agg.add_block(101, &[Value::Float64(5.0)]);
        assert_eq!(agg.current_value(), Value::Float64(35.0));
    }

    #[test]
    fn sum_rollback() {
        let mut agg = SumAgg::new(&ColumnType::Float64);
        agg.add_block(100, &[Value::Float64(10.0)]);
        agg.add_block(101, &[Value::Float64(20.0)]);
        agg.add_block(102, &[Value::Float64(30.0)]);

        agg.remove_block(102);
        assert_eq!(agg.current_value(), Value::Float64(30.0));

        agg.remove_block(101);
        assert_eq!(agg.current_value(), Value::Float64(10.0));
    }

    #[test]
    fn sum_finalize() {
        let mut agg = SumAgg::new(&ColumnType::Float64);
        agg.add_block(100, &[Value::Float64(10.0)]);
        agg.add_block(101, &[Value::Float64(20.0)]);
        agg.add_block(102, &[Value::Float64(30.0)]);

        agg.finalize_up_to(101);
        // Finalized: 10+20=30, unfinalized: 30
        assert_eq!(agg.current_value(), Value::Float64(60.0));

        // Rollback block 102
        agg.remove_block(102);
        assert_eq!(agg.current_value(), Value::Float64(30.0));
    }

    #[test]
    fn sum_with_integers() {
        let mut agg = SumAgg::new(&ColumnType::UInt64);
        agg.add_block(100, &[Value::UInt64(10), Value::UInt64(5)]);
        assert_eq!(agg.current_value(), Value::UInt64(15));
    }

    #[test]
    fn sum_ignores_non_numeric() {
        let mut agg = SumAgg::new(&ColumnType::Float64);
        agg.add_block(
            100,
            &[
                Value::Float64(10.0),
                Value::String("hello".into()),
                Value::Null,
            ],
        );
        assert_eq!(agg.current_value(), Value::Float64(10.0));
    }

    // --- Count ---

    #[test]
    fn count_basic() {
        let mut agg = CountAgg::new();
        agg.add_block(100, &[Value::Float64(1.0), Value::Float64(2.0)]);
        agg.add_block(101, &[Value::Float64(3.0)]);
        assert_eq!(agg.current_value(), Value::UInt64(3));
    }

    #[test]
    fn count_skips_null() {
        let mut agg = CountAgg::new();
        agg.add_block(
            100,
            &[Value::Float64(1.0), Value::Null, Value::Float64(2.0)],
        );
        assert_eq!(agg.current_value(), Value::UInt64(2));
    }

    #[test]
    fn count_rollback() {
        let mut agg = CountAgg::new();
        agg.add_block(100, &[Value::Float64(1.0)]);
        agg.add_block(101, &[Value::Float64(2.0), Value::Float64(3.0)]);

        agg.remove_block(101);
        assert_eq!(agg.current_value(), Value::UInt64(1));
    }

    #[test]
    fn count_finalize_then_rollback() {
        let mut agg = CountAgg::new();
        agg.add_block(100, &[Value::Float64(1.0)]);
        agg.add_block(101, &[Value::Float64(2.0)]);
        agg.add_block(102, &[Value::Float64(3.0)]);

        agg.finalize_up_to(101);
        assert_eq!(agg.current_value(), Value::UInt64(3));

        agg.remove_block(102);
        assert_eq!(agg.current_value(), Value::UInt64(2));
    }

    // --- Min ---

    #[test]
    fn min_basic() {
        let mut agg = MinAgg::new(&ColumnType::Float64);
        agg.add_block(100, &[Value::Float64(30.0), Value::Float64(10.0)]);
        agg.add_block(101, &[Value::Float64(20.0)]);
        assert_eq!(agg.current_value(), Value::Float64(10.0));
    }

    #[test]
    fn min_rollback_recomputes() {
        let mut agg = MinAgg::new(&ColumnType::Float64);
        agg.add_block(100, &[Value::Float64(20.0)]);
        agg.add_block(101, &[Value::Float64(5.0)]); // this is the global min
        agg.add_block(102, &[Value::Float64(15.0)]);

        agg.remove_block(101);
        // Min should now be 15.0 (recomputed from remaining blocks 100, 102)
        assert_eq!(agg.current_value(), Value::Float64(15.0));
    }

    #[test]
    fn min_finalize_preserves() {
        let mut agg = MinAgg::new(&ColumnType::Float64);
        agg.add_block(100, &[Value::Float64(10.0)]);
        agg.add_block(101, &[Value::Float64(5.0)]);
        agg.add_block(102, &[Value::Float64(20.0)]);

        agg.finalize_up_to(101);
        // Finalized min: 5.0
        assert_eq!(agg.current_value(), Value::Float64(5.0));

        agg.remove_block(102);
        // Still 5.0 (finalized)
        assert_eq!(agg.current_value(), Value::Float64(5.0));
    }

    #[test]
    fn min_empty() {
        let agg = MinAgg::new(&ColumnType::Float64);
        assert_eq!(agg.current_value(), Value::Null);
        assert!(!agg.has_data());
    }

    // --- Max ---

    #[test]
    fn max_basic() {
        let mut agg = MaxAgg::new(&ColumnType::Float64);
        agg.add_block(100, &[Value::Float64(10.0), Value::Float64(30.0)]);
        agg.add_block(101, &[Value::Float64(20.0)]);
        assert_eq!(agg.current_value(), Value::Float64(30.0));
    }

    #[test]
    fn max_rollback_recomputes() {
        let mut agg = MaxAgg::new(&ColumnType::Float64);
        agg.add_block(100, &[Value::Float64(10.0)]);
        agg.add_block(101, &[Value::Float64(50.0)]); // global max
        agg.add_block(102, &[Value::Float64(30.0)]);

        agg.remove_block(101);
        assert_eq!(agg.current_value(), Value::Float64(30.0));
    }

    #[test]
    fn max_finalize_preserves() {
        let mut agg = MaxAgg::new(&ColumnType::Float64);
        agg.add_block(100, &[Value::Float64(50.0)]);
        agg.add_block(101, &[Value::Float64(10.0)]);
        agg.add_block(102, &[Value::Float64(30.0)]);

        agg.finalize_up_to(101);
        // Finalized max: 50.0
        agg.remove_block(102);
        assert_eq!(agg.current_value(), Value::Float64(50.0));
    }

    // --- Avg ---

    #[test]
    fn avg_basic() {
        let mut agg = AvgAgg::new(&ColumnType::Float64);
        agg.add_block(100, &[Value::Float64(10.0), Value::Float64(20.0)]);
        agg.add_block(101, &[Value::Float64(30.0)]);
        // avg(10, 20, 30) = 20
        assert_eq!(agg.current_value(), Value::Float64(20.0));
    }

    #[test]
    fn avg_rollback() {
        let mut agg = AvgAgg::new(&ColumnType::Float64);
        agg.add_block(100, &[Value::Float64(10.0)]);
        agg.add_block(101, &[Value::Float64(30.0)]);
        // avg(10, 30) = 20
        assert_eq!(agg.current_value(), Value::Float64(20.0));

        agg.remove_block(101);
        // avg(10) = 10
        assert_eq!(agg.current_value(), Value::Float64(10.0));
    }

    #[test]
    fn avg_finalize_then_rollback() {
        let mut agg = AvgAgg::new(&ColumnType::Float64);
        agg.add_block(100, &[Value::Float64(10.0)]);
        agg.add_block(101, &[Value::Float64(20.0)]);
        agg.add_block(102, &[Value::Float64(60.0)]);

        agg.finalize_up_to(101);
        // finalized: sum=30, count=2. unfinalized: block 102 sum=60, count=1
        // total avg = 90/3 = 30
        assert_eq!(agg.current_value(), Value::Float64(30.0));

        agg.remove_block(102);
        // avg = 30/2 = 15
        assert_eq!(agg.current_value(), Value::Float64(15.0));
    }

    #[test]
    fn avg_empty() {
        let agg = AvgAgg::new(&ColumnType::Float64);
        assert_eq!(agg.current_value(), Value::Null);
    }

    // --- First ---

    #[test]
    fn first_basic() {
        let mut agg = FirstAgg::new();
        agg.add_block(100, &[Value::Float64(10.0)]);
        agg.add_block(101, &[Value::Float64(20.0)]);
        assert_eq!(agg.current_value(), Value::Float64(10.0));
    }

    #[test]
    fn first_rollback_earliest() {
        let mut agg = FirstAgg::new();
        agg.add_block(100, &[Value::Float64(10.0)]);
        agg.add_block(101, &[Value::Float64(20.0)]);

        agg.remove_block(100);
        assert_eq!(agg.current_value(), Value::Float64(20.0));
    }

    #[test]
    fn first_finalized_immutable() {
        let mut agg = FirstAgg::new();
        agg.add_block(100, &[Value::Float64(10.0)]);
        agg.add_block(101, &[Value::Float64(20.0)]);
        agg.add_block(102, &[Value::Float64(30.0)]);

        agg.finalize_up_to(101);
        // Finalized first: 10.0
        assert_eq!(agg.current_value(), Value::Float64(10.0));

        agg.remove_block(102);
        // Still 10.0 — finalized is immutable
        assert_eq!(agg.current_value(), Value::Float64(10.0));
    }

    #[test]
    fn first_skips_null() {
        let mut agg = FirstAgg::new();
        agg.add_block(100, &[Value::Null, Value::Float64(10.0)]);
        assert_eq!(agg.current_value(), Value::Float64(10.0));
    }

    // --- Last ---

    #[test]
    fn last_basic() {
        let mut agg = LastAgg::new();
        agg.add_block(100, &[Value::Float64(10.0)]);
        agg.add_block(101, &[Value::Float64(20.0)]);
        assert_eq!(agg.current_value(), Value::Float64(20.0));
    }

    #[test]
    fn last_rollback_falls_back() {
        let mut agg = LastAgg::new();
        agg.add_block(100, &[Value::Float64(10.0)]);
        agg.add_block(101, &[Value::Float64(20.0)]);

        agg.remove_block(101);
        assert_eq!(agg.current_value(), Value::Float64(10.0));
    }

    #[test]
    fn last_finalize_then_rollback_to_finalized() {
        let mut agg = LastAgg::new();
        agg.add_block(100, &[Value::Float64(10.0)]);
        agg.add_block(101, &[Value::Float64(20.0)]);
        agg.add_block(102, &[Value::Float64(30.0)]);

        agg.finalize_up_to(101);
        // Finalized last: 20.0, unfinalized latest: 30.0
        assert_eq!(agg.current_value(), Value::Float64(30.0));

        agg.remove_block(102);
        // Falls back to finalized: 20.0
        assert_eq!(agg.current_value(), Value::Float64(20.0));
    }

    #[test]
    fn last_multiple_values_in_block() {
        let mut agg = LastAgg::new();
        agg.add_block(
            100,
            &[
                Value::Float64(10.0),
                Value::Float64(20.0),
                Value::Float64(30.0),
            ],
        );
        assert_eq!(agg.current_value(), Value::Float64(30.0));
    }

    #[test]
    fn last_skips_trailing_null() {
        let mut agg = LastAgg::new();
        agg.add_block(100, &[Value::Float64(10.0), Value::Null]);
        assert_eq!(agg.current_value(), Value::Float64(10.0));
    }

    // --- toStartOfInterval ---

    #[test]
    fn interval_5min() {
        // 5 minutes = 300 seconds
        let ts = 1_700_000_123_456i64; // some timestamp in ms
        let result = to_start_of_interval(ts, 300);
        assert_eq!(result % (300 * 1000), 0);
        assert!(result <= ts);
        assert!(ts - result < 300 * 1000);
    }

    #[test]
    fn interval_1hour() {
        let ts = 1_700_000_123_456i64;
        let result = to_start_of_interval(ts, 3600);
        assert_eq!(result % (3600 * 1000), 0);
        assert!(result <= ts);
    }

    #[test]
    fn interval_exact_boundary() {
        let ts = 300_000i64; // exactly 5 minutes in ms
        assert_eq!(to_start_of_interval(ts, 300), 300_000);
    }

    // --- Serialization roundtrip ---

    #[test]
    fn sum_serde_roundtrip() {
        let mut agg = SumAgg::new(&ColumnType::Float64);
        agg.add_block(100, &[Value::Float64(10.0)]);
        agg.add_block(101, &[Value::Float64(20.0)]);
        agg.finalize_up_to(100);

        let bytes = agg.to_bytes();
        let restored = SumAgg::from_bytes(&bytes);
        assert_eq!(restored.current_value(), agg.current_value());
    }

    #[test]
    fn first_serde_roundtrip() {
        let mut agg = FirstAgg::new();
        agg.add_block(100, &[Value::String("hello".into())]);
        agg.finalize_up_to(100);

        let bytes = agg.to_bytes();
        let restored = FirstAgg::from_bytes(&bytes);
        assert_eq!(restored.current_value(), Value::String("hello".into()));
    }

    #[test]
    fn last_serde_roundtrip() {
        let mut agg = LastAgg::new();
        agg.add_block(100, &[Value::UInt64(42)]);
        agg.add_block(101, &[Value::UInt64(99)]);

        let bytes = agg.to_bytes();
        let restored = LastAgg::from_bytes(&bytes);
        assert_eq!(restored.current_value(), Value::UInt64(99));
    }

    // --- OHLCV candle scenario from RFC Section 10 ---

    #[test]
    fn ohlcv_candle_rollback() {
        // Simulate: 50 trades across blocks 1000-1003. Block 1003 gets rolled back.
        let mut open = FirstAgg::new();
        let mut high = MaxAgg::new(&ColumnType::Float64);
        let mut low = MinAgg::new(&ColumnType::Float64);
        let mut close = LastAgg::new();
        let mut volume = SumAgg::new(&ColumnType::Float64);
        let mut count = CountAgg::new();

        // Block 1000: price=100, amount=1
        let prices_1000 = &[Value::Float64(100.0)];
        let amounts_1000 = &[Value::Float64(1.0)];
        open.add_block(1000, prices_1000);
        high.add_block(1000, prices_1000);
        low.add_block(1000, prices_1000);
        close.add_block(1000, prices_1000);
        volume.add_block(1000, amounts_1000);
        count.add_block(1000, prices_1000);

        // Block 1001: price=110, amount=2
        let prices_1001 = &[Value::Float64(110.0)];
        let amounts_1001 = &[Value::Float64(2.0)];
        open.add_block(1001, prices_1001);
        high.add_block(1001, prices_1001);
        low.add_block(1001, prices_1001);
        close.add_block(1001, prices_1001);
        volume.add_block(1001, amounts_1001);
        count.add_block(1001, prices_1001);

        // Block 1002: price=90, amount=3
        let prices_1002 = &[Value::Float64(90.0)];
        let amounts_1002 = &[Value::Float64(3.0)];
        open.add_block(1002, prices_1002);
        high.add_block(1002, prices_1002);
        low.add_block(1002, prices_1002);
        close.add_block(1002, prices_1002);
        volume.add_block(1002, amounts_1002);
        count.add_block(1002, prices_1002);

        // Block 1003: price=200, amount=10 (will be rolled back)
        let prices_1003 = &[Value::Float64(200.0)];
        let amounts_1003 = &[Value::Float64(10.0)];
        open.add_block(1003, prices_1003);
        high.add_block(1003, prices_1003);
        low.add_block(1003, prices_1003);
        close.add_block(1003, prices_1003);
        volume.add_block(1003, amounts_1003);
        count.add_block(1003, prices_1003);

        // Before rollback
        assert_eq!(high.current_value(), Value::Float64(200.0));
        assert_eq!(close.current_value(), Value::Float64(200.0));
        assert_eq!(volume.current_value(), Value::Float64(16.0));
        assert_eq!(count.current_value(), Value::UInt64(4));

        // Rollback block 1003
        open.remove_block(1003);
        high.remove_block(1003);
        low.remove_block(1003);
        close.remove_block(1003);
        volume.remove_block(1003);
        count.remove_block(1003);

        // After rollback
        assert_eq!(open.current_value(), Value::Float64(100.0)); // unchanged
        assert_eq!(high.current_value(), Value::Float64(110.0)); // recomputed
        assert_eq!(low.current_value(), Value::Float64(90.0)); // recomputed
        assert_eq!(close.current_value(), Value::Float64(90.0)); // falls back to block 1002
        assert_eq!(volume.current_value(), Value::Float64(6.0)); // subtracted
        assert_eq!(count.current_value(), Value::UInt64(3)); // subtracted
    }

    /// Issue #15: SumAgg must report has_data=true even when finalized sum is exactly 0.0.
    #[test]
    fn sum_has_data_after_zero_sum_finalization() {
        let mut agg = SumAgg::new(&ColumnType::Float64);
        assert!(!agg.has_data(), "empty agg should have no data");

        agg.add_block(100, &[Value::Float64(10.0)]);
        agg.add_block(101, &[Value::Float64(-10.0)]);
        assert!(agg.has_data(), "agg with pending blocks should have data");

        agg.finalize_up_to(101);
        assert_eq!(agg.current_value(), Value::Float64(0.0));
        assert!(
            agg.has_data(),
            "agg with finalized zero sum should still report has_data=true"
        );
    }

    /// Integer aggregations must preserve precision for values > 2^53.
    #[test]
    fn sum_preserves_large_integer_precision() {
        let mut agg = SumAgg::new(&ColumnType::UInt64);
        let large: u64 = (1u64 << 53) + 1; // 9007199254740993 — loses precision as f64
        agg.add_block(100, &[Value::UInt64(large)]);
        agg.add_block(101, &[Value::UInt64(large)]);
        let expected = large * 2;
        assert_eq!(agg.current_value(), Value::UInt64(expected));
    }

    #[test]
    fn min_preserves_large_integer_precision() {
        let mut agg = MinAgg::new(&ColumnType::UInt64);
        let a: u64 = (1u64 << 53) + 1;
        let b: u64 = (1u64 << 53) + 2;
        agg.add_block(100, &[Value::UInt64(b)]);
        agg.add_block(101, &[Value::UInt64(a)]);
        // a and b are indistinguishable as f64 — this test would fail with f64 accumulation
        assert_eq!(agg.current_value(), Value::UInt64(a));
    }

    #[test]
    fn max_preserves_large_integer_precision() {
        let mut agg = MaxAgg::new(&ColumnType::UInt64);
        let a: u64 = (1u64 << 53) + 1;
        let b: u64 = (1u64 << 53) + 2;
        agg.add_block(100, &[Value::UInt64(a)]);
        agg.add_block(101, &[Value::UInt64(b)]);
        assert_eq!(agg.current_value(), Value::UInt64(b));
    }

    #[test]
    fn sum_float_column_returns_float() {
        let mut agg = SumAgg::new(&ColumnType::Float64);
        agg.add_block(100, &[Value::Float64(10.5)]);
        assert_eq!(agg.current_value(), Value::Float64(10.5));
    }

    // --- Uint256 aggregation tests ---

    fn u256_val(lo: u128) -> Value {
        Value::Uint256(ethnum::U256::from(lo).to_be_bytes())
    }

    fn u256_from_val(v: &Value) -> ethnum::U256 {
        match v {
            Value::Uint256(b) => ethnum::U256::from_be_bytes(*b),
            _ => panic!("expected Uint256, got {v:?}"),
        }
    }

    #[test]
    fn uint256_sum() {
        let mut agg = SumAgg::new(&ColumnType::Uint256);
        // Values larger than u64::MAX
        let big: u128 = (u64::MAX as u128) + 1;
        agg.add_block(100, &[u256_val(big)]);
        agg.add_block(101, &[u256_val(big)]);
        assert_eq!(
            u256_from_val(&agg.current_value()),
            ethnum::U256::from(big * 2)
        );
    }

    #[test]
    fn uint256_min_max() {
        let mut min_agg = MinAgg::new(&ColumnType::Uint256);
        let mut max_agg = MaxAgg::new(&ColumnType::Uint256);
        let a: u128 = 100;
        let b: u128 = 200;
        min_agg.add_block(100, &[u256_val(a), u256_val(b)]);
        max_agg.add_block(100, &[u256_val(a), u256_val(b)]);
        assert_eq!(
            u256_from_val(&min_agg.current_value()),
            ethnum::U256::from(a)
        );
        assert_eq!(
            u256_from_val(&max_agg.current_value()),
            ethnum::U256::from(b)
        );
    }

    #[test]
    fn uint256_avg() {
        let mut agg = AvgAgg::new(&ColumnType::Uint256);
        agg.add_block(100, &[u256_val(100), u256_val(200)]);
        // AVG of Uint256 returns Uint256 (integer division)
        assert_eq!(
            u256_from_val(&agg.current_value()),
            ethnum::U256::from(150u64)
        );
    }

    #[test]
    fn uint256_avg_large_values_no_precision_loss() {
        let mut agg = AvgAgg::new(&ColumnType::Uint256);
        // 1 ETH = 10^18 wei — values > 2^53 that lose precision as f64
        let one_eth: u128 = 1_000_000_000_000_000_000;
        let two_eth: u128 = 2 * one_eth;
        agg.add_block(100, &[u256_val(one_eth), u256_val(two_eth)]);
        // AVG = 1.5 ETH = 1_500_000_000_000_000_000 (exact in U256)
        let expected = ethnum::U256::from(one_eth + one_eth / 2);
        assert_eq!(u256_from_val(&agg.current_value()), expected);
    }

    #[test]
    fn uint256_rollback() {
        let mut agg = SumAgg::new(&ColumnType::Uint256);
        agg.add_block(100, &[u256_val(100)]);
        agg.add_block(101, &[u256_val(200)]);
        assert_eq!(
            u256_from_val(&agg.current_value()),
            ethnum::U256::from(300u64)
        );
        agg.remove_block(101);
        assert_eq!(
            u256_from_val(&agg.current_value()),
            ethnum::U256::from(100u64)
        );
    }
}
