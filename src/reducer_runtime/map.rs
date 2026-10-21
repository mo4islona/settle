//! MAP state fields: a map from String keys to values in the state of a
//! group. A change of one entry costs O(1), however many entries the map
//! holds: a journal of the changed entries takes a block back, not a copy of
//! the map, and a checkpoint writes only the entries that changed.

use std::collections::VecDeque;
use std::sync::Arc;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::storage::{self, StorageWriteBatch};
use crate::types::{BlockNumber, ColumnRegistry, Value};

/// The MAP fields of one group. An entry never holds Null: a set to Null
/// removes it.
pub struct StateMaps {
    /// The names of the MAP fields of the reducer, in schema order.
    names: Arc<ColumnRegistry>,
    /// The entries of each MAP field, in the order of `names`.
    entries: Vec<FxHashMap<Arc<str>, Value>>,
    /// The block of the changes that come next.
    block: BlockNumber,
    /// The changes of the blocks not final yet, oldest first.
    undo: Journal,
    /// The keys that a final block changed since the last checkpoint.
    unsaved: Unsaved,
}

/// One change of an entry.
struct Change {
    block: BlockNumber,
    map: u16,
    key: Arc<str>,
    /// The value that the change replaced; None if the entry was absent.
    old: Option<Value>,
}

/// Changes in block order. One change stays inline: most groups change one
/// entry between two finalizations, and an idle group holds no buffer.
#[derive(Default)]
enum Journal {
    #[default]
    Empty,
    One(Change),
    Many(VecDeque<Change>),
}

impl Journal {
    fn push(&mut self, change: Change) {
        match self {
            Self::Many(changes) => changes.push_back(change),
            Self::Empty => *self = Self::One(change),
            Self::One(_) => {
                let Self::One(first) = std::mem::take(self) else {
                    unreachable!()
                };
                let mut changes = VecDeque::with_capacity(4);
                changes.extend([first, change]);
                *self = Self::Many(changes);
            }
        }
    }

    /// Remove the newest change if its block is above `block`.
    fn pop_above(&mut self, block: BlockNumber) -> Option<Change> {
        let popped = match self {
            Self::Empty => None,
            Self::One(change) if change.block <= block => None,
            Self::One(_) => match std::mem::take(self) {
                Self::One(change) => Some(change),
                _ => unreachable!(),
            },
            Self::Many(changes) => match changes.back() {
                Some(change) if change.block > block => changes.pop_back(),
                _ => None,
            },
        };
        self.drop_empty();
        popped
    }

    /// Remove the changes up to `block`, and hand each to `f`, oldest first.
    fn drain_up_to(&mut self, block: BlockNumber, mut f: impl FnMut(Change)) {
        match self {
            Self::Empty => {}
            Self::One(change) if change.block > block => {}
            Self::One(_) => {
                if let Self::One(change) = std::mem::take(self) {
                    f(change);
                }
            }
            Self::Many(changes) => {
                let finalized = changes.partition_point(|c| c.block <= block);
                changes.drain(..finalized).for_each(f);
            }
        }
        self.drop_empty();
    }

    fn iter(&self) -> impl Iterator<Item = &Change> {
        let (one, many) = match self {
            Self::Empty => (None, None),
            Self::One(change) => (Some(change), None),
            Self::Many(changes) => (None, Some(changes.iter())),
        };
        one.into_iter().chain(many.into_iter().flatten())
    }

    /// For each of `keys` that a block after `finalized` changed, the value
    /// that the first of those changes replaced: its value as of finality.
    /// The other changes stay out, however many they are.
    fn replaced_after(
        &self,
        finalized: BlockNumber,
        keys: &Unsaved,
    ) -> FxHashMap<(u16, &str), Option<&Value>> {
        let mut replaced = FxHashMap::default();
        for change in self.iter().filter(|c| c.block > finalized) {
            if keys.contains(change.map, &change.key) {
                replaced
                    .entry((change.map, &*change.key))
                    .or_insert(change.old.as_ref());
            }
        }
        replaced
    }

    fn drop_empty(&mut self) {
        if let Self::Many(changes) = self
            && changes.is_empty()
        {
            *self = Self::Empty;
        }
    }
}

/// Keys of MAP fields, as `(map, key)`. One key stays inline, as in
/// `Journal`.
#[derive(Default)]
enum Unsaved {
    #[default]
    Empty,
    One(u16, Arc<str>),
    /// The keys of each MAP field.
    Many(Vec<FxHashSet<Arc<str>>>),
}

impl Unsaved {
    /// Add `key` of MAP field `map`, one of `maps` fields.
    fn insert(&mut self, map: u16, key: Arc<str>, maps: usize) {
        match self {
            Self::Many(keys) => {
                keys[map as usize].insert(key);
            }
            Self::One(m, k) if *m == map && **k == *key => {}
            Self::Empty => *self = Self::One(map, key),
            Self::One(..) => {
                let Self::One(first_map, first) = std::mem::take(self) else {
                    unreachable!()
                };
                let mut keys = vec![FxHashSet::default(); maps];
                keys[first_map as usize].insert(first);
                keys[map as usize].insert(key);
                *self = Self::Many(keys);
            }
        }
    }

    fn contains(&self, map: u16, key: &str) -> bool {
        match self {
            Self::Empty => false,
            Self::One(m, k) => *m == map && **k == *key,
            Self::Many(keys) => keys[map as usize].contains(key),
        }
    }

    /// Hand each key to `f`, with its MAP field.
    fn for_each(self, mut f: impl FnMut(u16, Arc<str>)) {
        match self {
            Self::Empty => {}
            Self::One(map, key) => f(map, key),
            Self::Many(keys) => {
                for (map, keys) in keys.into_iter().enumerate() {
                    keys.into_iter().for_each(|key| f(map as u16, key));
                }
            }
        }
    }
}

impl StateMaps {
    /// Empty maps for the MAP fields `names`.
    pub fn new(names: Arc<ColumnRegistry>) -> Self {
        let count = names.len();
        Self {
            names,
            entries: (0..count).map(|_| FxHashMap::default()).collect(),
            block: 0,
            undo: Journal::Empty,
            unsaved: Unsaved::Empty,
        }
    }

    /// The names of the MAP fields, in the order of their indices.
    pub fn names(&self) -> &ColumnRegistry {
        &self.names
    }

    /// The MAP field `name`.
    pub fn map(&mut self, name: &str) -> Option<MapMut<'_>> {
        let map = self.names.get_id(name)?;
        self.map_at(map as usize)
    }

    /// The MAP field at index `map` of `names`.
    pub fn map_at(&mut self, map: usize) -> Option<MapMut<'_>> {
        (map < self.entries.len()).then_some(MapMut { maps: self, map })
    }

    /// The value of `key` in MAP field `map`.
    pub fn get(&self, map: usize, key: &str) -> Option<&Value> {
        self.entries[map].get(key)
    }

    /// Set `key` in MAP field `map`; Null removes it.
    pub fn set(&mut self, map: usize, key: &str, value: Value) {
        if value.is_null() {
            self.remove(map, key);
            return;
        }

        let key: Arc<str> = key.into();
        let old = self.entries[map].insert(key.clone(), value);
        self.log(map, key, old);
    }

    /// Remove `key` from MAP field `map`. False if it was absent.
    pub fn remove(&mut self, map: usize, key: &str) -> bool {
        let Some((key, old)) = self.entries[map].remove_entry(key) else {
            return false;
        };
        self.log(map, key, Some(old));
        true
    }

    /// Start the changes of `block`.
    pub(crate) fn begin(&mut self, block: BlockNumber) {
        self.block = block;
    }

    /// An entry of the stored state, at load.
    pub(crate) fn insert_stored(&mut self, map: usize, key: &str, value: Value) {
        self.entries[map].insert(key.into(), value);
    }

    fn log(&mut self, map: usize, key: Arc<str>, old: Option<Value>) {
        self.undo.push(Change {
            block: self.block,
            map: map as u16,
            key,
            old,
        });
    }

    /// Undo the changes of the blocks after `fork_point`, newest first.
    pub(crate) fn rollback(&mut self, fork_point: BlockNumber) {
        while let Some(change) = self.undo.pop_above(fork_point) {
            let entries = &mut self.entries[change.map as usize];
            match change.old {
                Some(old) => entries.insert(change.key, old),
                None => entries.remove(&*change.key),
            };
        }
    }

    /// The blocks up to `block` are final: their changes can no longer be
    /// undone, and their keys wait for the next checkpoint.
    pub(crate) fn finalize(&mut self, block: BlockNumber) {
        let maps = self.entries.len();
        let unsaved = &mut self.unsaved;
        self.undo
            .drain_up_to(block, |change| unsaved.insert(change.map, change.key, maps));
    }

    /// Write each unsaved entry as of `finalized` into `batch`: its value,
    /// or a delete if it was absent then.
    pub(crate) fn write_checkpoint(
        &mut self,
        finalized: BlockNumber,
        reducer: &str,
        group_key: &[u8],
        batch: &mut StorageWriteBatch,
    ) {
        let replaced = self.undo.replaced_after(finalized, &self.unsaved);
        let entries = &self.entries;

        std::mem::take(&mut self.unsaved).for_each(|map, key| {
            let value = match replaced.get(&(map, &*key)) {
                Some(old) => *old,
                None => entries[map as usize].get(&*key),
            };
            match value {
                Some(value) => {
                    let bytes = storage::encode_value_bytes(value);
                    batch.put_reducer_map_entry(reducer, group_key, map, &key, bytes);
                }
                None => batch.delete_reducer_map_entry(reducer, group_key, map, &key),
            }
        });
    }
}

/// A MAP field of the group that a reducer call processes.
pub struct MapMut<'a> {
    maps: &'a mut StateMaps,
    map: usize,
}

impl MapMut<'_> {
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.maps.get(self.map, key)
    }

    /// Set `key`; Null removes it.
    pub fn set(&mut self, key: &str, value: Value) {
        self.maps.set(self.map, key, value);
    }

    /// Remove `key`. False if it was absent.
    pub fn remove(&mut self, key: &str) -> bool {
        self.maps.remove(self.map, key)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::storage::BatchOp;

    fn book() -> StateMaps {
        StateMaps::new(Arc::new(ColumnRegistry::new(vec!["book".to_string()])))
    }

    fn entries(maps: &StateMaps) -> Vec<(String, Value)> {
        let mut out: Vec<_> = maps.entries[0]
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    fn written(batch: &StorageWriteBatch) -> Vec<(String, Option<Value>)> {
        let mut out: Vec<_> = batch
            .ops
            .iter()
            .map(|op| match op {
                BatchOp::PutReducerMapEntry { key, value, .. } => (
                    key.clone(),
                    Some(storage::decode_value_bytes(value).unwrap()),
                ),
                BatchOp::DeleteReducerMapEntry { key, .. } => (key.clone(), None),
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// The entries that `batch` writes, with their MAP field; None deletes.
    fn written_to(batch: &StorageWriteBatch) -> Vec<(usize, String, Option<Value>)> {
        let decode = |value: &[u8]| storage::decode_value_bytes(value).unwrap();
        let entry = |op: &BatchOp| match op {
            BatchOp::PutReducerMapEntry {
                map, key, value, ..
            } => (*map as usize, key.clone(), Some(decode(value))),
            BatchOp::DeleteReducerMapEntry { map, key, .. } => (*map as usize, key.clone(), None),
            other => panic!("unexpected {other:?}"),
        };
        batch.ops.iter().map(entry).collect()
    }

    /// A rollback restores the entries that the undone blocks set, added and
    /// removed, also when one block changed a key twice.
    #[test]
    fn rollback_restores_set_added_and_removed_entries() {
        let mut maps = book();
        maps.begin(1);
        maps.set(0, "a", Value::UInt64(1));
        maps.set(0, "b", Value::UInt64(2));

        maps.begin(2);
        maps.set(0, "a", Value::UInt64(10));
        maps.set(0, "a", Value::UInt64(11));
        maps.remove(0, "b");
        maps.set(0, "c", Value::UInt64(3));
        maps.set(0, "d", Value::Null);
        assert_eq!(
            entries(&maps),
            [
                ("a".into(), Value::UInt64(11)),
                ("c".into(), Value::UInt64(3))
            ]
        );

        maps.rollback(1);
        assert_eq!(
            entries(&maps),
            [
                ("a".into(), Value::UInt64(1)),
                ("b".into(), Value::UInt64(2))
            ]
        );
        maps.rollback(0);
        assert!(entries(&maps).is_empty());
    }

    /// A checkpoint writes only the keys that final blocks changed, each as
    /// of finality: a later change does not leak in, and a key absent at
    /// finality is deleted.
    #[test]
    fn checkpoint_writes_changed_keys_as_of_finality() {
        let mut maps = book();
        maps.begin(1);
        maps.set(0, "kept", Value::UInt64(1));
        maps.set(0, "gone", Value::UInt64(2));
        maps.set(0, "later", Value::UInt64(3));
        maps.finalize(1);
        let mut batch = StorageWriteBatch::new();
        maps.write_checkpoint(1, "r", b"g", &mut batch);
        assert_eq!(written(&batch).len(), 3);

        maps.begin(2);
        maps.remove(0, "gone");
        maps.set(0, "later", Value::UInt64(30));
        maps.set(0, "new", Value::UInt64(4));
        maps.begin(3);
        maps.set(0, "later", Value::UInt64(300));
        maps.remove(0, "new");
        maps.finalize(2);

        let mut batch = StorageWriteBatch::new();
        maps.write_checkpoint(2, "r", b"g", &mut batch);
        assert_eq!(
            written(&batch),
            [
                ("gone".into(), None),
                ("later".into(), Some(Value::UInt64(30))),
                ("new".into(), Some(Value::UInt64(4))),
            ]
        );

        let mut batch = StorageWriteBatch::new();
        maps.write_checkpoint(2, "r", b"g", &mut batch);
        assert!(batch.ops.is_empty());
    }

    /// Maps with nothing to undo or to write hold no buffer for that: most
    /// groups are idle, and an idle group must stay small.
    #[test]
    fn idle_maps_hold_no_buffer() {
        let mut maps = book();
        maps.begin(1);
        for key in ["a", "b", "c", "d", "e"] {
            maps.set(0, key, Value::UInt64(1));
        }
        maps.finalize(1);
        maps.write_checkpoint(1, "r", b"g", &mut StorageWriteBatch::new());

        assert!(matches!(maps.undo, Journal::Empty));
        assert!(matches!(maps.unsaved, Unsaved::Empty));
    }

    /// A checkpoint looks up only the keys that it writes, not every key
    /// that an unfinal block changed.
    #[test]
    fn checkpoint_skips_unfinal_keys_it_does_not_write() {
        let mut maps = book();
        maps.begin(1);
        maps.set(0, "saved", Value::UInt64(1));
        maps.begin(2);
        for key in 0..1000 {
            maps.set(0, &format!("k{key}"), Value::UInt64(key));
        }
        maps.set(0, "saved", Value::UInt64(2));
        maps.finalize(1);

        let replaced = maps.undo.replaced_after(1, &maps.unsaved);
        assert_eq!(replaced.len(), 1);
        assert_eq!(replaced[&(0, "saved")], Some(&Value::UInt64(1)));
    }

    /// Random changes, rollbacks, finalizations and checkpoints agree with
    /// copies of the maps: the entries now, and in storage the entries as of
    /// the finality of the last checkpoint.
    #[test]
    fn maps_agree_with_copies() {
        type Copy = [BTreeMap<String, u64>; 2];

        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut next = |n: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % n
        };

        for _ in 0..500 {
            let names = ColumnRegistry::new(vec!["a".to_string(), "b".to_string()]);
            let mut maps = StateMaps::new(Arc::new(names));
            let mut now: Copy = Default::default();
            // The entries before each block that is not final.
            let mut before: BTreeMap<BlockNumber, Copy> = BTreeMap::new();
            let mut stored: Copy = Default::default();
            let mut finalized = 0;
            let mut block = 1;

            for _ in 0..40 {
                match next(6) {
                    0..=2 => {
                        before.entry(block).or_insert_with(|| now.clone());
                        maps.begin(block);
                        let map = next(2) as usize;
                        let key = format!("k{}", next(3));
                        if next(3) == 0 {
                            maps.remove(map, &key);
                            now[map].remove(&key);
                        } else {
                            let value = 1 + next(100);
                            maps.set(map, &key, Value::UInt64(value));
                            now[map].insert(key, value);
                        }
                    }
                    3 => block += 1,
                    4 => {
                        let fork = finalized + next(block - finalized + 1);
                        maps.rollback(fork);
                        if let Some(copy) = before.range(fork + 1..).next().map(|(_, c)| c) {
                            now = copy.clone();
                        }
                        before.split_off(&(fork + 1));
                        block = block.max(fork + 1);
                    }
                    _ => {
                        finalized += next(block - finalized + 1);
                        maps.finalize(finalized);
                        before = before.split_off(&(finalized + 1));
                        block = block.max(finalized + 1);
                        if next(2) == 0 {
                            continue;
                        }

                        let mut batch = StorageWriteBatch::new();
                        maps.write_checkpoint(finalized, "r", b"g", &mut batch);
                        for (map, key, value) in written_to(&batch) {
                            match value {
                                Some(value) => stored[map].insert(key, value.as_u64().unwrap()),
                                None => stored[map].remove(&key),
                            };
                        }
                        let at_finality = before.values().next().unwrap_or(&now);
                        assert_eq!(&stored, at_finality);
                    }
                }

                for (map, copy) in now.iter().enumerate() {
                    let entries: BTreeMap<String, u64> = maps.entries[map]
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.as_u64().unwrap()))
                        .collect();
                    assert_eq!(&entries, copy);
                }
            }
        }
    }
}
