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
    undo: VecDeque<Change>,
    /// For each MAP field, the keys that a final block changed since the
    /// last checkpoint.
    unsaved: Vec<FxHashSet<Arc<str>>>,
}

/// One change of an entry.
struct Change {
    block: BlockNumber,
    map: u16,
    key: Arc<str>,
    /// The value that the change replaced; None if the entry was absent.
    old: Option<Value>,
}

impl StateMaps {
    /// Empty maps for the MAP fields `names`.
    pub fn new(names: Arc<ColumnRegistry>) -> Self {
        let count = names.len();
        Self {
            names,
            entries: (0..count).map(|_| FxHashMap::default()).collect(),
            block: 0,
            undo: VecDeque::new(),
            unsaved: (0..count).map(|_| FxHashSet::default()).collect(),
        }
    }

    /// The names of the MAP fields, in the order of their indices.
    pub fn names(&self) -> &ColumnRegistry {
        &self.names
    }

    /// The MAP field `name`.
    pub fn map(&mut self, name: &str) -> Option<MapMut<'_>> {
        let map = self.names.get_id(name)? as usize;
        Some(MapMut { maps: self, map })
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
        self.undo.push_back(Change {
            block: self.block,
            map: map as u16,
            key,
            old,
        });
    }

    /// Undo the changes of the blocks after `fork_point`, newest first.
    pub(crate) fn rollback(&mut self, fork_point: BlockNumber) {
        let kept = self.undo.partition_point(|c| c.block <= fork_point);
        while self.undo.len() > kept {
            let change = self.undo.pop_back().expect("a change above the kept ones");
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
        let finalized = self.undo.partition_point(|c| c.block <= block);
        for change in self.undo.drain(..finalized) {
            self.unsaved[change.map as usize].insert(change.key);
        }
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
        let Self {
            entries,
            undo,
            unsaved,
            ..
        } = self;

        // An entry that a block after finality changed had, as of finality,
        // the value that the first of those changes replaced.
        let above = undo.partition_point(|c| c.block <= finalized);
        let mut replaced: FxHashMap<(u16, &str), Option<&Value>> = FxHashMap::default();
        for change in undo.range(above..) {
            if unsaved[change.map as usize].contains(&*change.key) {
                replaced
                    .entry((change.map, &*change.key))
                    .or_insert(change.old.as_ref());
            }
        }

        for (map, keys) in unsaved.iter_mut().enumerate() {
            let map = map as u16;
            for key in keys.drain() {
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
            }
        }
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
}
