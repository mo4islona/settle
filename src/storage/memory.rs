use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::error::Result;
use crate::types::BlockNumber;

use super::journal::JournalPart;
use super::{BatchOp, StorageBackend, StorageWriteBatch};

/// In-memory storage backend using BTreeMap. Suitable for testing and small datasets.
pub struct MemoryBackend {
    inner: Mutex<MemoryInner>,
}

struct MemoryInner {
    /// raw:{table}:{block} -> encoded row bytes
    raw: BTreeMap<(String, BlockNumber), Vec<u8>>,
    /// reducer:{name}:{group_key_hex}:{block} -> state bytes
    reducer_states: BTreeMap<(String, Vec<u8>, BlockNumber), Vec<u8>>,
    /// reducer_finalized:{name}:{group_key_hex} -> state bytes
    reducer_finalized: BTreeMap<(String, Vec<u8>), Vec<u8>>,
    /// mv:{view}:{group_key_hex} -> state bytes
    mv_states: BTreeMap<(String, Vec<u8>), Vec<u8>>,
    /// meta:{key} -> value bytes
    meta: BTreeMap<String, Vec<u8>>,
    block_hashes: BTreeMap<BlockNumber, String>,
    /// (sequence, part) -> journal entry
    journal: BTreeMap<(u64, u64), Vec<u8>>,
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(MemoryInner {
                raw: BTreeMap::new(),
                reducer_states: BTreeMap::new(),
                reducer_finalized: BTreeMap::new(),
                mv_states: BTreeMap::new(),
                meta: BTreeMap::new(),
                block_hashes: BTreeMap::new(),
                journal: BTreeMap::new(),
            }),
        }
    }
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageBackend for MemoryBackend {
    // --- Raw table rows (encoded bytes) ---

    fn put_raw_rows(&self, table: &str, block: BlockNumber, data: &[u8]) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let key = (table.to_string(), block);
        inner.raw.insert(key, data.to_vec());
        Ok(())
    }

    fn get_raw_rows(
        &self,
        table: &str,
        from_block: BlockNumber,
        to_block: BlockNumber,
    ) -> Result<Vec<(BlockNumber, Vec<u8>)>> {
        self.get_raw_rows_limited(table, from_block, to_block, usize::MAX)
    }

    fn get_raw_rows_limited(
        &self,
        table: &str,
        from_block: BlockNumber,
        to_block: BlockNumber,
        max_blocks: usize,
    ) -> Result<Vec<(BlockNumber, Vec<u8>)>> {
        let inner = self.inner.lock().unwrap();
        let start = (table.to_string(), from_block);
        let end = (table.to_string(), to_block);
        let mut result = Vec::new();
        for (key, data) in inner.raw.range(start..=end).take(max_blocks) {
            if key.0 == table {
                result.push((key.1, data.clone()));
            }
        }
        Ok(result)
    }

    fn delete_raw_rows_after(&self, table: &str, after_block: BlockNumber) -> Result<()> {
        // Guard: after_block + 1 would overflow u64::MAX to 0, scanning the entire map.
        // MAX is a valid no-op: nothing can exist after the maximum block.
        if after_block == BlockNumber::MAX {
            return Ok(());
        }
        let mut inner = self.inner.lock().unwrap();
        let keys_to_remove: Vec<_> = inner
            .raw
            .range((table.to_string(), after_block + 1)..)
            .take_while(|(k, _)| k.0 == table)
            .map(|(k, _)| k.clone())
            .collect();
        for key in keys_to_remove {
            inner.raw.remove(&key);
        }
        Ok(())
    }

    fn take_raw_rows_after(
        &self,
        table: &str,
        after_block: BlockNumber,
    ) -> Result<Vec<(BlockNumber, Vec<u8>)>> {
        // Guard: after_block + 1 would overflow u64::MAX to 0, scanning the entire map.
        // MAX is a valid no-op: nothing can exist after the maximum block.
        if after_block == BlockNumber::MAX {
            return Ok(Vec::new());
        }
        let mut inner = self.inner.lock().unwrap();
        let keys_to_remove: Vec<_> = inner
            .raw
            .range((table.to_string(), after_block + 1)..)
            .take_while(|(k, _)| k.0 == table)
            .map(|(k, _)| k.clone())
            .collect();
        let mut result = Vec::with_capacity(keys_to_remove.len());
        for key in keys_to_remove {
            if let Some(data) = inner.raw.remove(&key) {
                result.push((key.1, data));
            }
        }
        Ok(result)
    }

    // --- Reducer state snapshots ---

    fn put_reducer_state(
        &self,
        reducer: &str,
        group_key: &[u8],
        block: BlockNumber,
        state: &[u8],
    ) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let key = (reducer.to_string(), group_key.to_vec(), block);
        inner.reducer_states.insert(key, state.to_vec());
        Ok(())
    }

    fn get_reducer_state(
        &self,
        reducer: &str,
        group_key: &[u8],
        block: BlockNumber,
    ) -> Result<Option<Vec<u8>>> {
        let inner = self.inner.lock().unwrap();
        let key = (reducer.to_string(), group_key.to_vec(), block);
        Ok(inner.reducer_states.get(&key).cloned())
    }

    fn get_reducer_state_at_or_before(
        &self,
        reducer: &str,
        group_key: &[u8],
        block: BlockNumber,
    ) -> Result<Option<(BlockNumber, Vec<u8>)>> {
        let inner = self.inner.lock().unwrap();
        let search_key = (reducer.to_string(), group_key.to_vec(), block);
        if let Some((k, v)) = inner.reducer_states.range(..=search_key).next_back() {
            if k.0 == reducer && k.1 == group_key {
                return Ok(Some((k.2, v.clone())));
            }
        }
        Ok(None)
    }

    fn delete_reducer_states_after(
        &self,
        reducer: &str,
        group_key: &[u8],
        after_block: BlockNumber,
    ) -> Result<()> {
        // Guard: after_block + 1 would overflow u64::MAX to 0, scanning the entire map.
        // MAX is a valid no-op: nothing can exist after the maximum block.
        if after_block == BlockNumber::MAX {
            return Ok(());
        }
        let mut inner = self.inner.lock().unwrap();
        let start = (reducer.to_string(), group_key.to_vec(), after_block + 1);
        let keys_to_remove: Vec<_> = inner
            .reducer_states
            .range(start..)
            .take_while(|(k, _)| k.0 == reducer && k.1 == group_key)
            .map(|(k, _)| k.clone())
            .collect();
        for key in keys_to_remove {
            inner.reducer_states.remove(&key);
        }
        Ok(())
    }

    // --- Reducer finalized state ---

    fn get_reducer_finalized(&self, reducer: &str, group_key: &[u8]) -> Result<Option<Vec<u8>>> {
        let inner = self.inner.lock().unwrap();
        let key = (reducer.to_string(), group_key.to_vec());
        Ok(inner.reducer_finalized.get(&key).cloned())
    }

    fn set_reducer_finalized(&self, reducer: &str, group_key: &[u8], state: &[u8]) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let key = (reducer.to_string(), group_key.to_vec());
        inner.reducer_finalized.insert(key, state.to_vec());
        Ok(())
    }

    fn has_reducer_finalized(&self, reducer: &str) -> Result<bool> {
        let inner = self.inner.lock().unwrap();
        let first = inner
            .reducer_finalized
            .range((reducer.to_string(), Vec::new())..)
            .next();
        Ok(first.is_some_and(|((r, _), _)| r == reducer))
    }

    fn delete_reducer_states_up_to(
        &self,
        reducer: &str,
        group_key: &[u8],
        up_to_block: BlockNumber,
    ) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let start = (reducer.to_string(), group_key.to_vec(), 0u64);
        let end = (reducer.to_string(), group_key.to_vec(), up_to_block);
        let keys_to_remove: Vec<_> = inner
            .reducer_states
            .range(start..=end)
            .map(|(k, _)| k.clone())
            .collect();
        for key in keys_to_remove {
            inner.reducer_states.remove(&key);
        }
        Ok(())
    }

    // --- MV state ---

    fn put_mv_state(&self, view: &str, group_key: &[u8], state: &[u8]) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let key = (view.to_string(), group_key.to_vec());
        inner.mv_states.insert(key, state.to_vec());
        Ok(())
    }

    fn get_mv_state(&self, view: &str, group_key: &[u8]) -> Result<Option<Vec<u8>>> {
        let inner = self.inner.lock().unwrap();
        let key = (view.to_string(), group_key.to_vec());
        Ok(inner.mv_states.get(&key).cloned())
    }

    fn delete_mv_state(&self, view: &str, group_key: &[u8]) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let key = (view.to_string(), group_key.to_vec());
        inner.mv_states.remove(&key);
        Ok(())
    }

    fn list_mv_group_keys(&self, view: &str) -> Result<Vec<Vec<u8>>> {
        let inner = self.inner.lock().unwrap();
        let keys: Vec<Vec<u8>> = inner
            .mv_states
            .keys()
            .filter(|(v, _)| v == view)
            .map(|(_, gk)| gk.clone())
            .collect();
        Ok(keys)
    }

    // --- Metadata ---

    fn put_meta(&self, key: &str, value: &[u8]) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.meta.insert(key.to_string(), value.to_vec());
        Ok(())
    }

    fn get_meta(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.meta.get(key).cloned())
    }

    fn get_block_hashes(&self) -> Result<Vec<(BlockNumber, String)>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .block_hashes
            .iter()
            .map(|(&block, hash)| (block, hash.clone()))
            .collect())
    }

    // --- Atomic batch commit ---

    fn commit(&self, batch: &StorageWriteBatch) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        for op in &batch.ops {
            match op {
                BatchOp::PutRawRows { table, block, data } => {
                    inner.raw.insert((table.clone(), *block), data.clone());
                }
                BatchOp::SetReducerFinalized {
                    reducer,
                    group_key,
                    state,
                } => {
                    let key = (reducer.clone(), group_key.clone());
                    inner.reducer_finalized.insert(key, state.clone());
                }
                BatchOp::PutMvState {
                    view,
                    group_key,
                    state,
                } => {
                    let key = (view.clone(), group_key.clone());
                    inner.mv_states.insert(key, state.clone());
                }
                BatchOp::PutMeta { key, value } => {
                    inner.meta.insert(key.clone(), value.clone());
                }
                BatchOp::DeleteMvState { view, group_key } => {
                    inner.mv_states.remove(&(view.clone(), group_key.clone()));
                }
                BatchOp::DeleteRawRowsAfter { table, after_block } => {
                    if *after_block < BlockNumber::MAX {
                        let keys: Vec<_> = inner
                            .raw
                            .range((table.clone(), *after_block + 1)..)
                            .take_while(|((t, _), _)| t == table)
                            .map(|(k, _)| k.clone())
                            .collect();
                        for k in keys {
                            inner.raw.remove(&k);
                        }
                    }
                }
                BatchOp::DeleteRawRows {
                    table,
                    from_block,
                    to_block,
                } if from_block <= to_block => {
                    let keys: Vec<_> = inner
                        .raw
                        .range((table.clone(), *from_block)..=(table.clone(), *to_block))
                        .map(|(k, _)| k.clone())
                        .collect();
                    for k in keys {
                        inner.raw.remove(&k);
                    }
                }
                BatchOp::DeleteRawRows { .. } => {}
                BatchOp::PutBlockHash { block, hash } => {
                    inner.block_hashes.insert(*block, hash.clone());
                }
                BatchOp::DeleteBlockHash { block } => {
                    inner.block_hashes.remove(block);
                }
                BatchOp::PutJournal {
                    sequence,
                    part,
                    entry,
                } => {
                    inner.journal.insert((*sequence, *part), entry.clone());
                }
                BatchOp::DeleteJournal { sequence, part } => {
                    inner.journal.remove(&(*sequence, *part));
                }
            }
        }
        Ok(())
    }

    fn journal(&self) -> Result<Vec<JournalPart>> {
        let inner = self.inner.lock().unwrap();
        let parts = inner
            .journal
            .iter()
            .map(|(&(sequence, part), entry)| JournalPart {
                sequence,
                part,
                entry: entry.clone(),
            });
        Ok(parts.collect())
    }

    // --- Bulk operations ---

    fn list_reducer_group_keys(&self, reducer: &str) -> Result<Vec<Vec<u8>>> {
        let inner = self.inner.lock().unwrap();
        let mut seen = std::collections::HashSet::new();
        let mut keys = Vec::new();
        for (r, gk, _) in inner.reducer_states.keys() {
            if r == reducer && seen.insert(gk.clone()) {
                keys.push(gk.clone());
            }
        }
        for (r, gk) in inner.reducer_finalized.keys() {
            if r == reducer && seen.insert(gk.clone()) {
                keys.push(gk.clone());
            }
        }
        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{decode_rows, decode_state, encode_group_key, encode_rows, encode_state};
    use crate::types::{ColumnRegistry, Row, Value};
    use std::sync::Arc;

    fn test_registry() -> Arc<ColumnRegistry> {
        Arc::new(ColumnRegistry::new(vec![
            "user".to_string(),
            "amount".to_string(),
        ]))
    }

    fn make_row(registry: &Arc<ColumnRegistry>, user: &str, amount: f64) -> Row {
        let mut row = Row::new(registry.clone());
        row.set("user", Value::String(user.to_string()));
        row.set("amount", Value::Float64(amount));
        row
    }

    #[test]
    fn raw_rows_store_and_retrieve() {
        let backend = MemoryBackend::new();
        let reg = test_registry();
        let rows1 = vec![make_row(&reg, "alice", 10.0)];
        let rows2 = vec![make_row(&reg, "bob", 20.0)];

        backend
            .put_raw_rows("swaps", 100, &encode_rows(&rows1))
            .unwrap();
        backend
            .put_raw_rows("swaps", 101, &encode_rows(&rows2))
            .unwrap();

        let result = backend.get_raw_rows("swaps", 100, 101).unwrap();
        assert_eq!(result.len(), 2);
        let decoded0 = decode_rows(&result[0].1, &reg).unwrap();
        let decoded1 = decode_rows(&result[1].1, &reg).unwrap();
        assert_eq!(result[0].0, 100);
        assert_eq!(decoded0.len(), 1);
        assert_eq!(result[1].0, 101);
        assert_eq!(decoded1[0].get("user"), Some(&Value::String("bob".into())));
    }

    #[test]
    fn raw_rows_range_query() {
        let backend = MemoryBackend::new();
        let reg = Arc::new(ColumnRegistry::new(vec!["block".to_string()]));
        for block in 100..110 {
            let mut row = Row::new(reg.clone());
            row.set("block", Value::UInt64(block));
            backend
                .put_raw_rows("t", block, &encode_rows(&[row]))
                .unwrap();
        }

        let result = backend.get_raw_rows("t", 103, 106).unwrap();
        assert_eq!(result.len(), 4);
        assert_eq!(result[0].0, 103);
        assert_eq!(result[3].0, 106);
    }

    #[test]
    fn raw_rows_delete_after() {
        let backend = MemoryBackend::new();
        let reg = Arc::new(ColumnRegistry::new(vec!["b".to_string()]));
        for block in 100..105 {
            let mut row = Row::new(reg.clone());
            row.set("b", Value::UInt64(block));
            backend
                .put_raw_rows("t", block, &encode_rows(&[row]))
                .unwrap();
        }

        backend.delete_raw_rows_after("t", 102).unwrap();

        let result = backend.get_raw_rows("t", 100, 110).unwrap();
        assert_eq!(result.len(), 3); // 100, 101, 102
        assert_eq!(result.last().unwrap().0, 102);
    }

    #[test]
    fn raw_rows_isolate_tables() {
        let backend = MemoryBackend::new();
        let reg = Arc::new(ColumnRegistry::new(vec!["t".to_string()]));
        let mut row_a = Row::new(reg.clone());
        row_a.set("t", Value::String("a".into()));
        let mut row_b = Row::new(reg.clone());
        row_b.set("t", Value::String("b".into()));

        backend
            .put_raw_rows("a", 100, &encode_rows(&[row_a]))
            .unwrap();
        backend
            .put_raw_rows("b", 100, &encode_rows(&[row_b]))
            .unwrap();

        backend.delete_raw_rows_after("a", 99).unwrap();

        assert_eq!(backend.get_raw_rows("a", 100, 100).unwrap().len(), 0);
        assert_eq!(backend.get_raw_rows("b", 100, 100).unwrap().len(), 1);
    }

    #[test]
    fn reducer_state_snapshots() {
        let backend = MemoryBackend::new();
        let gk = encode_group_key(&[Value::String("alice".into()), Value::String("ETH".into())]);

        let state1 = encode_state(&[("qty".to_string(), Value::Float64(10.0))].into());
        let state2 = encode_state(&[("qty".to_string(), Value::Float64(15.0))].into());

        backend
            .put_reducer_state("pnl", &gk, 1000, &state1)
            .unwrap();
        backend
            .put_reducer_state("pnl", &gk, 1001, &state2)
            .unwrap();

        let loaded = backend
            .get_reducer_state("pnl", &gk, 1000)
            .unwrap()
            .unwrap();
        let decoded = decode_state(&loaded);
        assert_eq!(decoded.get("qty"), Some(&Value::Float64(10.0)));

        let (blk, data) = backend
            .get_reducer_state_at_or_before("pnl", &gk, 1005)
            .unwrap()
            .unwrap();
        assert_eq!(blk, 1001);
        let decoded = decode_state(&data);
        assert_eq!(decoded.get("qty"), Some(&Value::Float64(15.0)));

        let (blk, _) = backend
            .get_reducer_state_at_or_before("pnl", &gk, 1000)
            .unwrap()
            .unwrap();
        assert_eq!(blk, 1000);

        assert!(
            backend
                .get_reducer_state_at_or_before("pnl", &gk, 999)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn reducer_state_delete_after() {
        let backend = MemoryBackend::new();
        let gk = encode_group_key(&[Value::String("alice".into())]);

        for block in 1000..1005 {
            let state = encode_state(&[("qty".to_string(), Value::Float64(block as f64))].into());
            backend.put_reducer_state("r", &gk, block, &state).unwrap();
        }

        backend.delete_reducer_states_after("r", &gk, 1002).unwrap();

        assert!(backend.get_reducer_state("r", &gk, 1002).unwrap().is_some());
        assert!(backend.get_reducer_state("r", &gk, 1003).unwrap().is_none());
        assert!(backend.get_reducer_state("r", &gk, 1004).unwrap().is_none());
    }

    #[test]
    fn reducer_state_delete_up_to() {
        let backend = MemoryBackend::new();
        let gk = encode_group_key(&[Value::String("alice".into())]);

        for block in 1000..1005 {
            let state = encode_state(&[("qty".to_string(), Value::Float64(block as f64))].into());
            backend.put_reducer_state("r", &gk, block, &state).unwrap();
        }

        backend.delete_reducer_states_up_to("r", &gk, 1002).unwrap();

        assert!(backend.get_reducer_state("r", &gk, 1000).unwrap().is_none());
        assert!(backend.get_reducer_state("r", &gk, 1002).unwrap().is_none());
        assert!(backend.get_reducer_state("r", &gk, 1003).unwrap().is_some());
    }

    #[test]
    fn reducer_finalized_state() {
        let backend = MemoryBackend::new();
        let gk = encode_group_key(&[Value::String("alice".into())]);

        assert!(backend.get_reducer_finalized("r", &gk).unwrap().is_none());

        let state = encode_state(&[("qty".to_string(), Value::Float64(15.0))].into());
        backend.set_reducer_finalized("r", &gk, &state).unwrap();

        let loaded = backend.get_reducer_finalized("r", &gk).unwrap().unwrap();
        let decoded = decode_state(&loaded);
        assert_eq!(decoded.get("qty"), Some(&Value::Float64(15.0)));
    }

    #[test]
    fn mv_state_crud() {
        let backend = MemoryBackend::new();
        let gk = encode_group_key(&[Value::String("ETH/USDC".into()), Value::UInt64(1200)]);
        let state = b"some_accumulator_state";

        assert!(backend.get_mv_state("candles_5m", &gk).unwrap().is_none());

        backend.put_mv_state("candles_5m", &gk, state).unwrap();
        let loaded = backend.get_mv_state("candles_5m", &gk).unwrap().unwrap();
        assert_eq!(loaded, state);

        let keys = backend.list_mv_group_keys("candles_5m").unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], gk);

        backend.delete_mv_state("candles_5m", &gk).unwrap();
        assert!(backend.get_mv_state("candles_5m", &gk).unwrap().is_none());
        assert_eq!(backend.list_mv_group_keys("candles_5m").unwrap().len(), 0);
    }

    #[test]
    fn metadata_operations() {
        let backend = MemoryBackend::new();

        assert!(backend.get_meta("cursor").unwrap().is_none());

        backend.put_meta("cursor", b"12345").unwrap();
        let v = backend.get_meta("cursor").unwrap().unwrap();
        assert_eq!(v, b"12345");

        backend.put_meta("cursor", b"67890").unwrap();
        let v = backend.get_meta("cursor").unwrap().unwrap();
        assert_eq!(v, b"67890");
    }

    #[test]
    fn list_reducer_group_keys() {
        let backend = MemoryBackend::new();
        let gk1 = encode_group_key(&[Value::String("alice".into())]);
        let gk2 = encode_group_key(&[Value::String("bob".into())]);

        backend.put_reducer_state("r", &gk1, 100, b"s1").unwrap();
        backend.put_reducer_state("r", &gk1, 101, b"s2").unwrap();
        backend.put_reducer_state("r", &gk2, 100, b"s3").unwrap();
        backend.set_reducer_finalized("r", &gk1, b"f1").unwrap();

        let keys = backend.list_reducer_group_keys("r").unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&gk1));
        assert!(keys.contains(&gk2));

        let keys = backend.list_reducer_group_keys("other").unwrap();
        assert!(keys.is_empty());
    }

    /// Issue #6: delete_raw_rows_after with u64::MAX must not overflow and delete everything.
    #[test]
    fn delete_raw_rows_after_u64_max_is_noop() {
        let backend = MemoryBackend::new();
        let reg = Arc::new(ColumnRegistry::new(vec!["b".to_string()]));
        for block in 100..105 {
            let mut row = Row::new(reg.clone());
            row.set("b", Value::UInt64(block));
            backend
                .put_raw_rows("t", block, &encode_rows(&[row]))
                .unwrap();
        }

        // u64::MAX means "nothing is after this block", should be a no-op
        backend
            .delete_raw_rows_after("t", BlockNumber::MAX)
            .unwrap();

        let result = backend.get_raw_rows("t", 100, 110).unwrap();
        assert_eq!(result.len(), 5, "All rows should be preserved");
    }

    /// Issue #6: take_raw_rows_after with u64::MAX must not overflow and take everything.
    #[test]
    fn take_raw_rows_after_u64_max_is_noop() {
        let backend = MemoryBackend::new();
        let reg = Arc::new(ColumnRegistry::new(vec!["b".to_string()]));
        for block in 100..105 {
            let mut row = Row::new(reg.clone());
            row.set("b", Value::UInt64(block));
            backend
                .put_raw_rows("t", block, &encode_rows(&[row]))
                .unwrap();
        }

        let taken = backend.take_raw_rows_after("t", BlockNumber::MAX).unwrap();
        assert!(taken.is_empty(), "Nothing should be taken");

        let result = backend.get_raw_rows("t", 100, 110).unwrap();
        assert_eq!(result.len(), 5, "All rows should be preserved");
    }

    /// Issue #6: delete_reducer_states_after with u64::MAX must not overflow.
    #[test]
    fn delete_reducer_states_after_u64_max_is_noop() {
        let backend = MemoryBackend::new();
        let gk = encode_group_key(&[Value::String("alice".into())]);

        for block in 1000..1005 {
            let state = encode_state(&[("qty".to_string(), Value::Float64(block as f64))].into());
            backend.put_reducer_state("r", &gk, block, &state).unwrap();
        }

        backend
            .delete_reducer_states_after("r", &gk, BlockNumber::MAX)
            .unwrap();

        for block in 1000..1005 {
            assert!(
                backend
                    .get_reducer_state("r", &gk, block)
                    .unwrap()
                    .is_some(),
                "Block {block} state should be preserved"
            );
        }
    }

    #[test]
    fn reducer_state_isolates_group_keys() {
        let backend = MemoryBackend::new();
        let gk1 = encode_group_key(&[Value::String("alice".into())]);
        let gk2 = encode_group_key(&[Value::String("bob".into())]);

        backend
            .put_reducer_state("r", &gk1, 100, b"alice_state")
            .unwrap();
        backend
            .put_reducer_state("r", &gk2, 100, b"bob_state")
            .unwrap();

        backend.delete_reducer_states_after("r", &gk1, 99).unwrap();

        assert!(backend.get_reducer_state("r", &gk1, 100).unwrap().is_none());
        assert_eq!(
            backend.get_reducer_state("r", &gk2, 100).unwrap().unwrap(),
            b"bob_state"
        );
    }
}
