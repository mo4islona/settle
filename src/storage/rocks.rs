use std::collections::HashSet;
use std::path::Path;

use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, DBCompressionType, Direction,
    IteratorMode, Options, ReadOptions, WriteBatch,
};

use crate::error::{Error, Result};
use crate::types::BlockNumber;

use super::{BatchOp, StorageBackend, StorageWriteBatch};

const CF_RAW: &str = "raw";
const CF_REDUCER_SNAP: &str = "reducer_snap";
const CF_REDUCER_FIN: &str = "reducer_fin";
const CF_MV: &str = "mv";
const CF_META: &str = "meta";

fn to_err(e: rocksdb::Error) -> Error {
    Error::Storage(e.to_string())
}

// ---------------------------------------------------------------------------
// Key encoding
//
// All keys use big-endian integers so that RocksDB's default bytewise
// comparator gives the correct sort order.
//
// CF_RAW:           {table}\0{block_be8}
// CF_REDUCER_SNAP:  {reducer}\0{gk_len_be2}{group_key}{block_be8}
// CF_REDUCER_FIN:   {reducer}\0{group_key}
// CF_MV:            {view}\0{group_key}
// CF_META:          {key}
// ---------------------------------------------------------------------------

fn raw_key(table: &str, block: BlockNumber) -> Vec<u8> {
    let mut k = Vec::with_capacity(table.len() + 1 + 8);
    k.extend_from_slice(table.as_bytes());
    k.push(0);
    k.extend_from_slice(&block.to_be_bytes());
    k
}

fn raw_table_prefix(table: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(table.len() + 1);
    k.extend_from_slice(table.as_bytes());
    k.push(0);
    k
}

fn snap_prefix(reducer: &str, gk: &[u8]) -> Vec<u8> {
    let gk_len = gk.len() as u16;
    let mut k = Vec::with_capacity(reducer.len() + 1 + 2 + gk.len());
    k.extend_from_slice(reducer.as_bytes());
    k.push(0);
    k.extend_from_slice(&gk_len.to_be_bytes());
    k.extend_from_slice(gk);
    k
}

fn snap_key(reducer: &str, gk: &[u8], block: BlockNumber) -> Vec<u8> {
    let mut k = snap_prefix(reducer, gk);
    k.extend_from_slice(&block.to_be_bytes());
    k
}

fn name_prefix(name: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(name.len() + 1);
    k.extend_from_slice(name.as_bytes());
    k.push(0);
    k
}

fn fin_key(reducer: &str, gk: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(reducer.len() + 1 + gk.len());
    k.extend_from_slice(reducer.as_bytes());
    k.push(0);
    k.extend_from_slice(gk);
    k
}

fn kv_key(ns: &str, gk: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(ns.len() + 1 + gk.len());
    k.extend_from_slice(ns.as_bytes());
    k.push(0);
    k.extend_from_slice(gk);
    k
}

/// Compute an exclusive upper bound for prefix iteration.
/// Increments the last non-0xff byte.
fn upper_bound(prefix: &[u8]) -> Vec<u8> {
    let mut ub = prefix.to_vec();
    while let Some(last) = ub.last_mut() {
        if *last < 0xff {
            *last += 1;
            return ub;
        }
        ub.pop();
    }
    ub
}

// ---------------------------------------------------------------------------
// Backend
// ---------------------------------------------------------------------------

/// RocksDB tuning options exposed via Config.
#[derive(Debug, Default)]
pub struct RocksDbConfig {
    /// Compression: "none", "snappy" (default), "zstd", "lz4".
    pub compression: Option<String>,
    /// Disable automatic background compactions.
    pub disable_compaction: bool,
    /// Block cache size in bytes. None = RocksDB default (~8MB per CF).
    /// 0 = disable block cache entirely.
    pub cache_size: Option<usize>,
}

/// RocksDB-backed persistent storage for Delta DB.
pub struct RocksDbBackend {
    db: DB,
}

impl RocksDbBackend {
    /// Open (or create) a RocksDB database at the given path.
    pub fn open(path: impl AsRef<Path>, config: &RocksDbConfig) -> Result<Self> {
        let comp_type = match config.compression.as_deref() {
            Some("none") => DBCompressionType::None,
            Some("zstd") => DBCompressionType::Zstd,
            Some("lz4") => DBCompressionType::Lz4,
            Some("snappy") | None => DBCompressionType::Snappy,
            Some(other) => {
                return Err(Error::Storage(format!(
                    "invalid RocksDB compression '{other}' (allowed: none, snappy, zstd, lz4)"
                )));
            }
        };

        // Shared block cache across all column families
        let shared_cache = config.cache_size.map(|size| {
            if size > 0 {
                Some(Cache::new_lru_cache(size))
            } else {
                None
            }
        });

        let make_opts = || {
            let mut opts = Options::default();
            opts.set_compression_type(comp_type);
            if config.disable_compaction {
                opts.set_disable_auto_compactions(true);
            }
            match &shared_cache {
                Some(Some(cache)) => {
                    let mut block_opts = BlockBasedOptions::default();
                    block_opts.set_block_cache(cache);
                    opts.set_block_based_table_factory(&block_opts);
                }
                Some(None) => {
                    let mut block_opts = BlockBasedOptions::default();
                    block_opts.disable_cache();
                    opts.set_block_based_table_factory(&block_opts);
                }
                None => {}
            }
            opts
        };

        let mut db_opts = make_opts();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let cfs = [CF_RAW, CF_REDUCER_SNAP, CF_REDUCER_FIN, CF_MV, CF_META]
            .into_iter()
            .map(|name| ColumnFamilyDescriptor::new(name, make_opts()))
            .collect::<Vec<_>>();

        let db = DB::open_cf_descriptors(&db_opts, path, cfs).map_err(to_err)?;
        Ok(Self { db })
    }

    /// Destroy the database at the given path (deletes all data).
    pub fn destroy(path: impl AsRef<Path>) -> Result<()> {
        DB::destroy(&Options::default(), path).map_err(to_err)
    }
}

impl StorageBackend for RocksDbBackend {
    // --- Raw table rows ---

    fn put_raw_rows(&self, table: &str, block: BlockNumber, data: &[u8]) -> Result<()> {
        let cf = self.db.cf_handle(CF_RAW).expect("raw CF");
        self.db
            .put_cf(cf, raw_key(table, block), data)
            .map_err(to_err)
    }

    fn get_raw_rows(
        &self,
        table: &str,
        from_block: BlockNumber,
        to_block: BlockNumber,
    ) -> Result<Vec<(BlockNumber, Vec<u8>)>> {
        let cf = self.db.cf_handle(CF_RAW).expect("raw CF");
        let prefix = raw_table_prefix(table);
        let start = raw_key(table, from_block);
        let end_excl = if to_block < BlockNumber::MAX {
            raw_key(table, to_block + 1)
        } else {
            upper_bound(&prefix)
        };

        let mut opts = ReadOptions::default();
        opts.set_iterate_upper_bound(end_excl);

        let mut result = Vec::new();
        let iter =
            self.db
                .iterator_cf_opt(cf, opts, IteratorMode::From(&start, Direction::Forward));
        for item in iter {
            let (k, v) = item.map_err(to_err)?;
            if !k.starts_with(&prefix) {
                break;
            }
            let block = BlockNumber::from_be_bytes(
                k.get(prefix.len()..prefix.len() + 8)
                    .and_then(|s| s.try_into().ok())
                    .ok_or_else(|| {
                        Error::Storage("corrupt key: missing block number suffix".into())
                    })?,
            );
            result.push((block, v.to_vec()));
        }
        Ok(result)
    }

    fn delete_raw_rows_after(&self, table: &str, after_block: BlockNumber) -> Result<()> {
        if after_block == BlockNumber::MAX {
            return Ok(());
        }
        let cf = self.db.cf_handle(CF_RAW).expect("raw CF");
        let start = raw_key(table, after_block + 1);
        let ub = upper_bound(&raw_table_prefix(table));

        let mut opts = ReadOptions::default();
        opts.set_iterate_upper_bound(ub);

        let keys: Vec<Box<[u8]>> = self
            .db
            .iterator_cf_opt(cf, opts, IteratorMode::From(&start, Direction::Forward))
            .map(|item| item.map(|(k, _)| k).map_err(to_err))
            .collect::<Result<Vec<_>>>()?;

        if !keys.is_empty() {
            let mut batch = WriteBatch::default();
            for k in &keys {
                batch.delete_cf(cf, k);
            }
            self.db.write(batch).map_err(to_err)?;
        }
        Ok(())
    }

    fn take_raw_rows_after(
        &self,
        table: &str,
        after_block: BlockNumber,
    ) -> Result<Vec<(BlockNumber, Vec<u8>)>> {
        if after_block == BlockNumber::MAX {
            return Ok(Vec::new());
        }
        let cf = self.db.cf_handle(CF_RAW).expect("raw CF");
        let prefix = raw_table_prefix(table);
        let start = raw_key(table, after_block + 1);
        let ub = upper_bound(&prefix);

        let mut opts = ReadOptions::default();
        opts.set_iterate_upper_bound(ub);

        let mut result = Vec::new();
        let mut batch = WriteBatch::default();

        let iter =
            self.db
                .iterator_cf_opt(cf, opts, IteratorMode::From(&start, Direction::Forward));
        for item in iter {
            let (k, v) = item.map_err(to_err)?;
            let block = BlockNumber::from_be_bytes(
                k.get(prefix.len()..prefix.len() + 8)
                    .and_then(|s| s.try_into().ok())
                    .ok_or_else(|| {
                        Error::Storage("corrupt key: missing block number suffix".into())
                    })?,
            );
            result.push((block, v.to_vec()));
            batch.delete_cf(cf, k.as_ref());
        }

        // Build batch during iteration to avoid intermediate keys_to_delete Vec.
        if !result.is_empty() {
            self.db.write(batch).map_err(to_err)?;
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
        let cf = self.db.cf_handle(CF_REDUCER_SNAP).expect("reducer_snap CF");
        self.db
            .put_cf(cf, snap_key(reducer, group_key, block), state)
            .map_err(to_err)
    }

    fn get_reducer_state(
        &self,
        reducer: &str,
        group_key: &[u8],
        block: BlockNumber,
    ) -> Result<Option<Vec<u8>>> {
        let cf = self.db.cf_handle(CF_REDUCER_SNAP).expect("reducer_snap CF");
        self.db
            .get_cf(cf, snap_key(reducer, group_key, block))
            .map_err(to_err)
    }

    fn get_reducer_state_at_or_before(
        &self,
        reducer: &str,
        group_key: &[u8],
        block: BlockNumber,
    ) -> Result<Option<(BlockNumber, Vec<u8>)>> {
        let cf = self.db.cf_handle(CF_REDUCER_SNAP).expect("reducer_snap CF");
        let prefix = snap_prefix(reducer, group_key);
        let search = snap_key(reducer, group_key, block);

        let mut opts = ReadOptions::default();
        opts.set_iterate_lower_bound(prefix.clone());
        opts.set_iterate_upper_bound(upper_bound(&prefix));

        let iter =
            self.db
                .iterator_cf_opt(cf, opts, IteratorMode::From(&search, Direction::Reverse));
        for item in iter {
            let (k, v) = item.map_err(to_err)?;
            if !k.starts_with(&prefix) {
                break;
            }
            let blk = BlockNumber::from_be_bytes(
                k.get(prefix.len()..prefix.len() + 8)
                    .and_then(|s| s.try_into().ok())
                    .ok_or_else(|| {
                        Error::Storage("corrupt key: missing block number suffix".into())
                    })?,
            );
            return Ok(Some((blk, v.to_vec())));
        }
        Ok(None)
    }

    fn delete_reducer_states_after(
        &self,
        reducer: &str,
        group_key: &[u8],
        after_block: BlockNumber,
    ) -> Result<()> {
        if after_block == BlockNumber::MAX {
            return Ok(());
        }
        let cf = self.db.cf_handle(CF_REDUCER_SNAP).expect("reducer_snap CF");
        let start = snap_key(reducer, group_key, after_block + 1);
        let ub = upper_bound(&snap_prefix(reducer, group_key));

        let mut opts = ReadOptions::default();
        opts.set_iterate_upper_bound(ub);

        let keys: Vec<Box<[u8]>> = self
            .db
            .iterator_cf_opt(cf, opts, IteratorMode::From(&start, Direction::Forward))
            .map(|item| item.map(|(k, _)| k).map_err(to_err))
            .collect::<Result<Vec<_>>>()?;

        if !keys.is_empty() {
            let mut batch = WriteBatch::default();
            for k in &keys {
                batch.delete_cf(cf, k);
            }
            self.db.write(batch).map_err(to_err)?;
        }
        Ok(())
    }

    // --- Reducer finalized state ---

    fn get_reducer_finalized(&self, reducer: &str, group_key: &[u8]) -> Result<Option<Vec<u8>>> {
        let cf = self.db.cf_handle(CF_REDUCER_FIN).expect("reducer_fin CF");
        self.db
            .get_cf(cf, fin_key(reducer, group_key))
            .map_err(to_err)
    }

    fn set_reducer_finalized(&self, reducer: &str, group_key: &[u8], state: &[u8]) -> Result<()> {
        let cf = self.db.cf_handle(CF_REDUCER_FIN).expect("reducer_fin CF");
        self.db
            .put_cf(cf, fin_key(reducer, group_key), state)
            .map_err(to_err)
    }

    fn delete_reducer_states_up_to(
        &self,
        reducer: &str,
        group_key: &[u8],
        up_to_block: BlockNumber,
    ) -> Result<()> {
        let cf = self.db.cf_handle(CF_REDUCER_SNAP).expect("reducer_snap CF");
        let prefix = snap_prefix(reducer, group_key);
        let start = snap_key(reducer, group_key, 0);
        let end_excl = if up_to_block < BlockNumber::MAX {
            snap_key(reducer, group_key, up_to_block + 1)
        } else {
            upper_bound(&prefix)
        };

        let mut opts = ReadOptions::default();
        opts.set_iterate_upper_bound(end_excl);

        let keys: Vec<Box<[u8]>> = self
            .db
            .iterator_cf_opt(cf, opts, IteratorMode::From(&start, Direction::Forward))
            .map(|item| item.map(|(k, _)| k).map_err(to_err))
            .collect::<Result<Vec<_>>>()?;

        if !keys.is_empty() {
            let mut batch = WriteBatch::default();
            for k in &keys {
                batch.delete_cf(cf, k);
            }
            self.db.write(batch).map_err(to_err)?;
        }
        Ok(())
    }

    // --- MV state ---

    fn put_mv_state(&self, view: &str, group_key: &[u8], state: &[u8]) -> Result<()> {
        let cf = self.db.cf_handle(CF_MV).expect("mv CF");
        self.db
            .put_cf(cf, kv_key(view, group_key), state)
            .map_err(to_err)
    }

    fn get_mv_state(&self, view: &str, group_key: &[u8]) -> Result<Option<Vec<u8>>> {
        let cf = self.db.cf_handle(CF_MV).expect("mv CF");
        self.db.get_cf(cf, kv_key(view, group_key)).map_err(to_err)
    }

    fn delete_mv_state(&self, view: &str, group_key: &[u8]) -> Result<()> {
        let cf = self.db.cf_handle(CF_MV).expect("mv CF");
        self.db
            .delete_cf(cf, kv_key(view, group_key))
            .map_err(to_err)
    }

    fn list_mv_group_keys(&self, view: &str) -> Result<Vec<Vec<u8>>> {
        let cf = self.db.cf_handle(CF_MV).expect("mv CF");
        let prefix = name_prefix(view);
        let ub = upper_bound(&prefix);

        let mut opts = ReadOptions::default();
        opts.set_iterate_upper_bound(ub);

        let mut keys = Vec::new();
        let iter =
            self.db
                .iterator_cf_opt(cf, opts, IteratorMode::From(&prefix, Direction::Forward));
        for item in iter {
            let (k, _) = item.map_err(to_err)?;
            if !k.starts_with(&prefix) {
                break;
            }
            keys.push(k[prefix.len()..].to_vec());
        }
        Ok(keys)
    }

    // --- Metadata ---

    fn put_meta(&self, key: &str, value: &[u8]) -> Result<()> {
        let cf = self.db.cf_handle(CF_META).expect("meta CF");
        self.db.put_cf(cf, key.as_bytes(), value).map_err(to_err)
    }

    fn get_meta(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let cf = self.db.cf_handle(CF_META).expect("meta CF");
        self.db.get_cf(cf, key.as_bytes()).map_err(to_err)
    }

    // --- Atomic batch commit ---

    fn commit(&self, batch: &StorageWriteBatch) -> Result<()> {
        let mut wb = WriteBatch::default();
        for op in &batch.ops {
            match op {
                BatchOp::PutRawRows { table, block, data } => {
                    let cf = self.db.cf_handle(CF_RAW).expect("raw CF");
                    wb.put_cf(cf, raw_key(table, *block), data);
                }
                BatchOp::SetReducerFinalized {
                    reducer,
                    group_key,
                    state,
                } => {
                    let cf = self.db.cf_handle(CF_REDUCER_FIN).expect("reducer_fin CF");
                    wb.put_cf(cf, fin_key(reducer, group_key), state);
                }
                BatchOp::PutMvState {
                    view,
                    group_key,
                    state,
                } => {
                    let cf = self.db.cf_handle(CF_MV).expect("mv CF");
                    wb.put_cf(cf, kv_key(view, group_key), state);
                }
                BatchOp::PutMeta { key, value } => {
                    let cf = self.db.cf_handle(CF_META).expect("meta CF");
                    wb.put_cf(cf, key.as_bytes(), value);
                }
                BatchOp::DeleteMvState { view, group_key } => {
                    let cf = self.db.cf_handle(CF_MV).expect("mv CF");
                    wb.delete_cf(cf, kv_key(view, group_key));
                }
                BatchOp::DeleteRawRowsAfter { table, after_block } => {
                    if *after_block < BlockNumber::MAX {
                        let cf = self.db.cf_handle(CF_RAW).expect("raw CF");
                        let start = raw_key(table, *after_block + 1);
                        let ub = upper_bound(&raw_table_prefix(table));
                        let mut opts = ReadOptions::default();
                        opts.set_iterate_upper_bound(ub);
                        for item in self.db.iterator_cf_opt(
                            cf,
                            opts,
                            IteratorMode::From(&start, Direction::Forward),
                        ) {
                            let (k, _) = item.map_err(to_err)?;
                            wb.delete_cf(cf, &k);
                        }
                    }
                }
            }
        }
        self.db.write(wb).map_err(to_err)
    }

    // --- Bulk operations ---

    fn list_reducer_group_keys(&self, reducer: &str) -> Result<Vec<Vec<u8>>> {
        let prefix = name_prefix(reducer);
        let ub = upper_bound(&prefix);
        let mut seen = HashSet::new();
        let mut keys = Vec::new();

        // Scan reducer_snap CF: keys are {reducer}\0{gk_len_be2}{gk}{block_be8}
        {
            let cf = self.db.cf_handle(CF_REDUCER_SNAP).expect("reducer_snap CF");
            let mut opts = ReadOptions::default();
            opts.set_iterate_upper_bound(ub.clone());

            let iter =
                self.db
                    .iterator_cf_opt(cf, opts, IteratorMode::From(&prefix, Direction::Forward));
            for item in iter {
                let (k, _) = item.map_err(to_err)?;
                if !k.starts_with(&prefix) {
                    break;
                }
                let rest = &k[prefix.len()..];
                // need at least 2 (gk_len) + 8 (block) bytes
                if rest.len() < 10 {
                    continue;
                }
                let gk_len = u16::from_be_bytes([rest[0], rest[1]]) as usize;
                if rest.len() < 2 + gk_len + 8 {
                    continue;
                }
                let gk = rest[2..2 + gk_len].to_vec();
                if seen.insert(gk.clone()) {
                    keys.push(gk);
                }
            }
        }

        // Scan reducer_fin CF: keys are {reducer}\0{gk}
        {
            let cf = self.db.cf_handle(CF_REDUCER_FIN).expect("reducer_fin CF");
            let mut opts = ReadOptions::default();
            opts.set_iterate_upper_bound(ub);

            let iter =
                self.db
                    .iterator_cf_opt(cf, opts, IteratorMode::From(&prefix, Direction::Forward));
            for item in iter {
                let (k, _) = item.map_err(to_err)?;
                if !k.starts_with(&prefix) {
                    break;
                }
                let gk = k[prefix.len()..].to_vec();
                if seen.insert(gk.clone()) {
                    keys.push(gk);
                }
            }
        }

        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{decode_state, encode_group_key, encode_state};
    use crate::types::{RowMap, Value};

    fn test_backend() -> (RocksDbBackend, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let backend = RocksDbBackend::open(dir.path(), &RocksDbConfig::default()).unwrap();
        (backend, dir)
    }

    fn make_state(pairs: &[(&str, Value)]) -> RowMap {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    // --- Raw rows ---

    #[test]
    fn raw_rows_store_and_retrieve() {
        let (b, _dir) = test_backend();

        b.put_raw_rows("swaps", 100, b"data_block_100").unwrap();
        b.put_raw_rows("swaps", 101, b"data_block_101").unwrap();

        let result = b.get_raw_rows("swaps", 100, 101).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, 100);
        assert_eq!(result[0].1, b"data_block_100");
        assert_eq!(result[1].0, 101);
        assert_eq!(result[1].1, b"data_block_101");
    }

    #[test]
    fn raw_rows_range_query() {
        let (b, _dir) = test_backend();
        for block in 100..110u64 {
            b.put_raw_rows("t", block, format!("b{block}").as_bytes())
                .unwrap();
        }

        let result = b.get_raw_rows("t", 103, 106).unwrap();
        assert_eq!(result.len(), 4);
        assert_eq!(result[0].0, 103);
        assert_eq!(result[3].0, 106);
    }

    #[test]
    fn raw_rows_delete_after() {
        let (b, _dir) = test_backend();
        for block in 100..105u64 {
            b.put_raw_rows("t", block, format!("b{block}").as_bytes())
                .unwrap();
        }

        b.delete_raw_rows_after("t", 102).unwrap();

        let result = b.get_raw_rows("t", 100, 110).unwrap();
        assert_eq!(result.len(), 3); // 100, 101, 102
        assert_eq!(result.last().unwrap().0, 102);
    }

    #[test]
    fn raw_rows_take_after() {
        let (b, _dir) = test_backend();
        for block in 100..105u64 {
            b.put_raw_rows("t", block, format!("b{block}").as_bytes())
                .unwrap();
        }

        let taken = b.take_raw_rows_after("t", 102).unwrap();
        assert_eq!(taken.len(), 2); // 103, 104
        assert_eq!(taken[0].0, 103);
        assert_eq!(taken[1].0, 104);

        // Verify they're gone
        let remaining = b.get_raw_rows("t", 100, 110).unwrap();
        assert_eq!(remaining.len(), 3); // 100, 101, 102
    }

    #[test]
    fn raw_rows_isolate_tables() {
        let (b, _dir) = test_backend();
        b.put_raw_rows("a", 100, b"data_a").unwrap();
        b.put_raw_rows("b", 100, b"data_b").unwrap();

        b.delete_raw_rows_after("a", 99).unwrap();

        assert_eq!(b.get_raw_rows("a", 100, 100).unwrap().len(), 0);
        assert_eq!(b.get_raw_rows("b", 100, 100).unwrap().len(), 1);
    }

    // --- Reducer state snapshots ---

    #[test]
    fn reducer_state_snapshots() {
        let (b, _dir) = test_backend();
        let gk = encode_group_key(&[Value::String("alice".into()), Value::String("ETH".into())]);

        let state1 = encode_state(&make_state(&[("qty", Value::Float64(10.0))]));
        let state2 = encode_state(&make_state(&[("qty", Value::Float64(15.0))]));

        b.put_reducer_state("pnl", &gk, 1000, &state1).unwrap();
        b.put_reducer_state("pnl", &gk, 1001, &state2).unwrap();

        // Exact lookup
        let loaded = b.get_reducer_state("pnl", &gk, 1000).unwrap().unwrap();
        let decoded = decode_state(&loaded);
        assert_eq!(decoded.get("qty"), Some(&Value::Float64(10.0)));

        // At-or-before
        let (blk, data) = b
            .get_reducer_state_at_or_before("pnl", &gk, 1005)
            .unwrap()
            .unwrap();
        assert_eq!(blk, 1001);
        let decoded = decode_state(&data);
        assert_eq!(decoded.get("qty"), Some(&Value::Float64(15.0)));

        // At-or-before exact
        let (blk, _) = b
            .get_reducer_state_at_or_before("pnl", &gk, 1000)
            .unwrap()
            .unwrap();
        assert_eq!(blk, 1000);

        // At-or-before nothing
        assert!(
            b.get_reducer_state_at_or_before("pnl", &gk, 999)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn reducer_state_delete_after() {
        let (b, _dir) = test_backend();
        let gk = encode_group_key(&[Value::String("alice".into())]);

        for block in 1000..1005 {
            let state = encode_state(&make_state(&[("qty", Value::Float64(block as f64))]));
            b.put_reducer_state("r", &gk, block, &state).unwrap();
        }

        b.delete_reducer_states_after("r", &gk, 1002).unwrap();

        assert!(b.get_reducer_state("r", &gk, 1002).unwrap().is_some());
        assert!(b.get_reducer_state("r", &gk, 1003).unwrap().is_none());
        assert!(b.get_reducer_state("r", &gk, 1004).unwrap().is_none());
    }

    #[test]
    fn reducer_state_delete_up_to() {
        let (b, _dir) = test_backend();
        let gk = encode_group_key(&[Value::String("alice".into())]);

        for block in 1000..1005 {
            let state = encode_state(&make_state(&[("qty", Value::Float64(block as f64))]));
            b.put_reducer_state("r", &gk, block, &state).unwrap();
        }

        b.delete_reducer_states_up_to("r", &gk, 1002).unwrap();

        assert!(b.get_reducer_state("r", &gk, 1000).unwrap().is_none());
        assert!(b.get_reducer_state("r", &gk, 1002).unwrap().is_none());
        assert!(b.get_reducer_state("r", &gk, 1003).unwrap().is_some());
    }

    #[test]
    fn reducer_state_isolates_group_keys() {
        let (b, _dir) = test_backend();
        let gk1 = encode_group_key(&[Value::String("alice".into())]);
        let gk2 = encode_group_key(&[Value::String("bob".into())]);

        b.put_reducer_state("r", &gk1, 100, b"alice_state").unwrap();
        b.put_reducer_state("r", &gk2, 100, b"bob_state").unwrap();

        b.delete_reducer_states_after("r", &gk1, 99).unwrap();

        assert!(b.get_reducer_state("r", &gk1, 100).unwrap().is_none());
        assert_eq!(
            b.get_reducer_state("r", &gk2, 100).unwrap().unwrap(),
            b"bob_state"
        );
    }

    // --- Reducer finalized state ---

    #[test]
    fn reducer_finalized_state() {
        let (b, _dir) = test_backend();
        let gk = encode_group_key(&[Value::String("alice".into())]);

        assert!(b.get_reducer_finalized("r", &gk).unwrap().is_none());

        let state = encode_state(&make_state(&[("qty", Value::Float64(15.0))]));
        b.set_reducer_finalized("r", &gk, &state).unwrap();

        let loaded = b.get_reducer_finalized("r", &gk).unwrap().unwrap();
        let decoded = decode_state(&loaded);
        assert_eq!(decoded.get("qty"), Some(&Value::Float64(15.0)));
    }

    // --- MV state ---

    #[test]
    fn mv_state_crud() {
        let (b, _dir) = test_backend();
        let gk = encode_group_key(&[Value::String("ETH/USDC".into()), Value::UInt64(1200)]);
        let state = b"some_accumulator_state";

        assert!(b.get_mv_state("candles_5m", &gk).unwrap().is_none());

        b.put_mv_state("candles_5m", &gk, state).unwrap();
        let loaded = b.get_mv_state("candles_5m", &gk).unwrap().unwrap();
        assert_eq!(loaded, state);

        let keys = b.list_mv_group_keys("candles_5m").unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], gk);

        b.delete_mv_state("candles_5m", &gk).unwrap();
        assert!(b.get_mv_state("candles_5m", &gk).unwrap().is_none());
        assert_eq!(b.list_mv_group_keys("candles_5m").unwrap().len(), 0);
    }

    // --- Metadata ---

    #[test]
    fn metadata_operations() {
        let (b, _dir) = test_backend();

        assert!(b.get_meta("cursor").unwrap().is_none());

        b.put_meta("cursor", b"12345").unwrap();
        assert_eq!(b.get_meta("cursor").unwrap().unwrap(), b"12345");

        b.put_meta("cursor", b"67890").unwrap();
        assert_eq!(b.get_meta("cursor").unwrap().unwrap(), b"67890");
    }

    // --- Bulk operations ---

    #[test]
    fn list_reducer_group_keys() {
        let (b, _dir) = test_backend();
        let gk1 = encode_group_key(&[Value::String("alice".into())]);
        let gk2 = encode_group_key(&[Value::String("bob".into())]);

        b.put_reducer_state("r", &gk1, 100, b"s1").unwrap();
        b.put_reducer_state("r", &gk1, 101, b"s2").unwrap();
        b.put_reducer_state("r", &gk2, 100, b"s3").unwrap();
        b.set_reducer_finalized("r", &gk1, b"f1").unwrap();

        let keys = b.list_reducer_group_keys("r").unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&gk1));
        assert!(keys.contains(&gk2));

        // Different reducer is isolated
        assert!(b.list_reducer_group_keys("other").unwrap().is_empty());
    }

    // --- Persistence ---

    #[test]
    fn data_persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();

        // Write data
        {
            let b = RocksDbBackend::open(path, &RocksDbConfig::default()).unwrap();
            b.put_raw_rows("t", 100, b"test_row_data").unwrap();
            b.put_meta("cursor", b"100").unwrap();
        }

        // Reopen and verify
        {
            let b = RocksDbBackend::open(path, &RocksDbConfig::default()).unwrap();
            let rows = b.get_raw_rows("t", 100, 100).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1, b"test_row_data");
            assert_eq!(b.get_meta("cursor").unwrap().unwrap(), b"100");
        }
    }

    // --- Config options ---

    #[test]
    fn open_with_compression_options() {
        for compression in &["none", "snappy", "zstd", "lz4"] {
            let dir = tempfile::tempdir().unwrap();
            let config = RocksDbConfig {
                compression: Some(compression.to_string()),
                ..Default::default()
            };
            let b = RocksDbBackend::open(dir.path(), &config).unwrap();
            b.put_raw_rows("t", 1, b"data").unwrap();
            assert_eq!(b.get_raw_rows("t", 1, 1).unwrap().len(), 1);
        }
    }

    #[test]
    fn open_with_invalid_compression_errors() {
        let dir = tempfile::tempdir().unwrap();
        let config = RocksDbConfig {
            compression: Some("brotli".to_string()),
            ..Default::default()
        };
        assert!(RocksDbBackend::open(dir.path(), &config).is_err());
    }

    #[test]
    fn open_with_cache_options() {
        // Explicit cache size
        let dir = tempfile::tempdir().unwrap();
        let config = RocksDbConfig {
            cache_size: Some(4 * 1024 * 1024),
            ..Default::default()
        };
        let b = RocksDbBackend::open(dir.path(), &config).unwrap();
        b.put_raw_rows("t", 1, b"data").unwrap();
        assert_eq!(b.get_raw_rows("t", 1, 1).unwrap().len(), 1);

        // Cache disabled
        let dir2 = tempfile::tempdir().unwrap();
        let config2 = RocksDbConfig {
            cache_size: Some(0),
            ..Default::default()
        };
        let b2 = RocksDbBackend::open(dir2.path(), &config2).unwrap();
        b2.put_raw_rows("t", 1, b"data").unwrap();
        assert_eq!(b2.get_raw_rows("t", 1, 1).unwrap().len(), 1);
    }

    #[test]
    fn open_with_compaction_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let config = RocksDbConfig {
            disable_compaction: true,
            ..Default::default()
        };
        let b = RocksDbBackend::open(dir.path(), &config).unwrap();
        b.put_raw_rows("t", 1, b"data").unwrap();
        assert_eq!(b.get_raw_rows("t", 1, 1).unwrap().len(), 1);
    }
}
