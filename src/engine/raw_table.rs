use std::collections::HashMap;
use std::sync::Arc;

use crate::error::Result;
use crate::schema::ast::TableDef;
use crate::storage::{self, StorageBackend, StorageWriteBatch};
use crate::types::{BlockNumber, ColumnRegistry, ChangeOp, ChangeRecord, Row, RowMap, Value};

/// Manages ingestion, storage, and rollback for a single raw table.
pub struct RawTableEngine {
    def: TableDef,
    storage: Arc<dyn StorageBackend>,
    registry: Arc<ColumnRegistry>,
}

impl RawTableEngine {
    pub fn new(def: TableDef, storage: Arc<dyn StorageBackend>) -> Self {
        let names: Vec<String> = def.columns.iter().map(|c| c.name.clone()).collect();
        let registry = Arc::new(ColumnRegistry::new(names));
        Self {
            def,
            storage,
            registry,
        }
    }

    pub fn name(&self) -> &str {
        &self.def.name
    }

    pub fn def(&self) -> &TableDef {
        &self.def
    }

    pub fn registry(&self) -> &Arc<ColumnRegistry> {
        &self.registry
    }

    /// Ingest a batch of rows for a given block number.
    /// Encodes directly from RowMaps using the column registry (no intermediate Row objects).
    /// Returns change records (one Insert per row).
    pub fn ingest(&self, block: BlockNumber, row_maps: &[RowMap]) -> Result<Vec<ChangeRecord>> {
        if row_maps.is_empty() {
            return Ok(Vec::new());
        }

        // Encode directly from RowMaps — no Row conversion needed
        let encoded = storage::encode_rows_from_maps(row_maps, &self.registry);
        self.storage.put_raw_rows(&self.def.name, block, &encoded)?;

        let changes = row_maps
            .iter()
            .enumerate()
            .map(|(idx, values)| {
                let mut key = HashMap::new();
                key.insert("block_number".to_string(), Value::UInt64(block));
                key.insert("_row_index".to_string(), Value::UInt64(idx as u64));

                ChangeRecord {
                    table: self.def.name.clone(),
                    operation: ChangeOp::Insert,
                    key,
                    values: values.clone(),
                    prev_values: None,
                }
            })
            .collect();

        Ok(changes)
    }

    /// Ingest rows without creating change records (for virtual tables).
    /// Stores the rows for replay but skips the expensive change record allocation.
    pub fn ingest_no_changes(&self, block: BlockNumber, row_maps: &[RowMap]) -> Result<()> {
        if row_maps.is_empty() {
            return Ok(());
        }
        let encoded = storage::encode_rows_from_maps(row_maps, &self.registry);
        self.storage.put_raw_rows(&self.def.name, block, &encoded)?;
        Ok(())
    }

    /// Ingest rows, deferring the storage write to a WriteBatch.
    /// Returns change records (one Insert per row) or none for virtual tables.
    pub fn ingest_to_batch(
        &self,
        block: BlockNumber,
        row_maps: &[RowMap],
        batch: &mut StorageWriteBatch,
        virtual_table: bool,
    ) -> Result<Vec<ChangeRecord>> {
        if row_maps.is_empty() {
            return Ok(Vec::new());
        }

        let encoded = storage::encode_rows_from_maps(row_maps, &self.registry);
        batch.put_raw_rows(&self.def.name, block, encoded);

        if virtual_table {
            return Ok(Vec::new());
        }

        let changes = row_maps
            .iter()
            .enumerate()
            .map(|(idx, values)| {
                let mut key = HashMap::new();
                key.insert("block_number".to_string(), Value::UInt64(block));
                key.insert("_row_index".to_string(), Value::UInt64(idx as u64));

                ChangeRecord {
                    table: self.def.name.clone(),
                    operation: ChangeOp::Insert,
                    key,
                    values: values.clone(),
                    prev_values: None,
                }
            })
            .collect();

        Ok(changes)
    }

    /// Roll back all rows where block_number > fork_point.
    /// Returns compensating Delete change records for the rolled-back rows.
    /// Uses `rollback_to_batch` internally and commits atomically.
    pub fn rollback(&self, fork_point: BlockNumber) -> Result<Vec<ChangeRecord>> {
        let mut batch = StorageWriteBatch::new();
        let changes = self.rollback_to_batch(fork_point, &mut batch)?;
        self.storage.commit(&batch)?;
        Ok(changes)
    }

    /// Roll back all rows where block_number > fork_point, deferring the
    /// storage deletion to the provided write batch for atomic commit.
    /// Returns compensating Delete change records for the rolled-back rows.
    pub fn rollback_to_batch(
        &self,
        fork_point: BlockNumber,
        batch: &mut StorageWriteBatch,
    ) -> Result<Vec<ChangeRecord>> {
        if fork_point == BlockNumber::MAX {
            return Ok(Vec::new());
        }

        // Read rows that will be rolled back (they're still in storage)
        let rolled_back =
            self.storage
                .get_raw_rows(&self.def.name, fork_point + 1, BlockNumber::MAX)?;

        // Defer the deletion to the write batch
        batch.delete_raw_rows_after(&self.def.name, fork_point);

        let mut changes = Vec::new();
        for (block, data) in rolled_back {
            let rows = storage::decode_rows(&data, &self.registry)?;
            for (idx, row) in rows.into_iter().enumerate() {
                let mut key = HashMap::new();
                key.insert("block_number".to_string(), Value::UInt64(block));
                key.insert("_row_index".to_string(), Value::UInt64(idx as u64));

                changes.push(ChangeRecord {
                    table: self.def.name.clone(),
                    operation: ChangeOp::Delete,
                    key,
                    values: row.to_map(),
                    prev_values: None,
                });
            }
        }

        Ok(changes)
    }

    /// Get all rows for a block range (inclusive). Used for reducer replay.
    pub fn get_rows(
        &self,
        from_block: BlockNumber,
        to_block: BlockNumber,
    ) -> Result<Vec<(BlockNumber, Vec<Row>)>> {
        let raw = self
            .storage
            .get_raw_rows(&self.def.name, from_block, to_block)?;
        raw.into_iter()
            .map(|(block, data)| {
                let rows = storage::decode_rows(&data, &self.registry)?;
                Ok((block, rows))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ast::ColumnDef;
    use crate::storage::memory::MemoryBackend;
    use crate::types::ColumnType;

    fn test_table_def() -> TableDef {
        TableDef {
            name: "trades".to_string(),
            columns: vec![
                ColumnDef {
                    name: "block_number".to_string(),
                    column_type: ColumnType::UInt64,
                },
                ColumnDef {
                    name: "user".to_string(),
                    column_type: ColumnType::String,
                },
                ColumnDef {
                    name: "amount".to_string(),
                    column_type: ColumnType::Float64,
                },
            ],
            virtual_table: false,
        }
    }

    fn make_row_map(user: &str, amount: f64) -> RowMap {
        HashMap::from([
            ("user".to_string(), Value::String(user.to_string())),
            ("amount".to_string(), Value::Float64(amount)),
        ])
    }

    #[test]
    fn ingest_produces_insert_changes() {
        let storage = Arc::new(MemoryBackend::new());
        let engine = RawTableEngine::new(test_table_def(), storage);

        let rows = vec![make_row_map("alice", 10.0), make_row_map("bob", 20.0)];
        let changes = engine.ingest(1000, &rows).unwrap();

        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].operation, ChangeOp::Insert);
        assert_eq!(changes[0].table, "trades");
        assert_eq!(
            changes[0].key.get("block_number"),
            Some(&Value::UInt64(1000))
        );
        assert_eq!(changes[0].key.get("_row_index"), Some(&Value::UInt64(0)));
        assert_eq!(
            changes[0].values.get("user"),
            Some(&Value::String("alice".into()))
        );

        assert_eq!(changes[1].key.get("_row_index"), Some(&Value::UInt64(1)));
        assert_eq!(
            changes[1].values.get("user"),
            Some(&Value::String("bob".into()))
        );
    }

    #[test]
    fn ingest_empty_batch_is_noop() {
        let storage = Arc::new(MemoryBackend::new());
        let engine = RawTableEngine::new(test_table_def(), storage);

        let changes = engine.ingest(1000, &[]).unwrap();
        assert!(changes.is_empty());
    }

    #[test]
    fn ingest_stores_rows_retrievable() {
        let storage = Arc::new(MemoryBackend::new());
        let engine = RawTableEngine::new(test_table_def(), storage);

        engine.ingest(1000, &[make_row_map("alice", 10.0)]).unwrap();
        engine.ingest(1001, &[make_row_map("bob", 20.0)]).unwrap();

        let rows = engine.get_rows(1000, 1001).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, 1000);
        assert_eq!(rows[1].0, 1001);
    }

    #[test]
    fn rollback_deletes_rows_and_emits_changes() {
        let storage = Arc::new(MemoryBackend::new());
        let engine = RawTableEngine::new(test_table_def(), storage);

        engine.ingest(1000, &[make_row_map("alice", 10.0)]).unwrap();
        engine.ingest(1001, &[make_row_map("bob", 20.0)]).unwrap();
        engine
            .ingest(
                1002,
                &[make_row_map("carol", 30.0), make_row_map("dave", 40.0)],
            )
            .unwrap();

        // Rollback to block 1000 (delete 1001 and 1002)
        let changes = engine.rollback(1000).unwrap();

        // Should get 3 Delete changes (1 from block 1001 + 2 from block 1002)
        assert_eq!(changes.len(), 3);
        for d in &changes {
            assert_eq!(d.operation, ChangeOp::Delete);
            assert_eq!(d.table, "trades");
        }

        // Verify bob's row is in the changes
        assert!(
            changes
                .iter()
                .any(|d| d.values.get("user") == Some(&Value::String("bob".into())))
        );

        // Verify storage only has block 1000
        let remaining = engine.get_rows(1000, 1010).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].0, 1000);
    }

    #[test]
    fn rollback_to_latest_is_noop() {
        let storage = Arc::new(MemoryBackend::new());
        let engine = RawTableEngine::new(test_table_def(), storage);

        engine.ingest(1000, &[make_row_map("alice", 10.0)]).unwrap();

        let changes = engine.rollback(1000).unwrap();
        assert!(changes.is_empty());

        let remaining = engine.get_rows(1000, 1000).unwrap();
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn full_cycle_ingest_rollback_reingest() {
        let storage = Arc::new(MemoryBackend::new());
        let engine = RawTableEngine::new(test_table_def(), storage);

        // Ingest 3 blocks
        engine.ingest(1000, &[make_row_map("alice", 10.0)]).unwrap();
        engine.ingest(1001, &[make_row_map("bob", 20.0)]).unwrap();
        engine.ingest(1002, &[make_row_map("carol", 30.0)]).unwrap();

        // Rollback block 1002
        let rollback_changes = engine.rollback(1001).unwrap();
        assert_eq!(rollback_changes.len(), 1);
        assert_eq!(
            rollback_changes[0].values.get("user"),
            Some(&Value::String("carol".into()))
        );

        // Re-ingest block 1002 with different data (reorg)
        let new_changes = engine.ingest(1002, &[make_row_map("eve", 50.0)]).unwrap();
        assert_eq!(new_changes.len(), 1);
        assert_eq!(
            new_changes[0].values.get("user"),
            Some(&Value::String("eve".into()))
        );

        // Verify final state
        let all_rows = engine.get_rows(1000, 1010).unwrap();
        assert_eq!(all_rows.len(), 3);
        assert_eq!(all_rows[2].0, 1002);
        assert_eq!(
            all_rows[2].1[0].get("user"),
            Some(&Value::String("eve".into()))
        );
    }
}
