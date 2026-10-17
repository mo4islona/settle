use std::sync::Arc;

use smallvec::smallvec;

use crate::error::Result;
use crate::schema::ast::TableDef;
use crate::storage::{self, PendingWrites, StorageBackend, StorageWriteBatch};
use crate::types::{
    BlockNumber, ChangeOp, ChangeRecord, ChangeSchema, ColumnRegistry, Row, RowMap, Value,
};

/// Manages ingestion, storage, and rollback for a single raw table.
pub struct RawTableEngine {
    def: TableDef,
    storage: Arc<dyn StorageBackend>,
    registry: Arc<ColumnRegistry>,
    change_schema: Arc<ChangeSchema>,
}

impl RawTableEngine {
    pub fn new(def: TableDef, storage: Arc<dyn StorageBackend>) -> Self {
        let names: Vec<String> = def.columns.iter().map(|c| c.name.clone()).collect();
        let change_schema = Arc::new(ChangeSchema::raw_table(&def.name, &names));
        let registry = Arc::new(ColumnRegistry::new(names));
        Self {
            def,
            storage,
            registry,
            change_schema,
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

        Ok(self.insert_changes(block, row_maps))
    }

    /// One Insert per row, with the columns in schema order.
    fn insert_changes(&self, block: BlockNumber, row_maps: &[RowMap]) -> Vec<ChangeRecord> {
        let columns = self.registry.names();

        row_maps
            .iter()
            .enumerate()
            .map(|(idx, row)| {
                let values = columns
                    .iter()
                    .map(|name| row.get(name).cloned().unwrap_or(Value::Null))
                    .collect();

                ChangeRecord::new(
                    ChangeOp::Insert,
                    self.change_schema.clone(),
                    smallvec![Value::UInt64(block), Value::UInt64(idx as u64)],
                    values,
                    None,
                )
            })
            .collect()
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

        Ok(self.insert_changes(block, row_maps))
    }

    /// Roll back all rows where block_number > fork_point.
    /// Returns compensating Delete change records for the rolled-back rows.
    /// Uses `rollback_to_batch` internally and commits atomically.
    pub fn rollback(&self, fork_point: BlockNumber) -> Result<Vec<ChangeRecord>> {
        let mut batch = StorageWriteBatch::new();
        let changes = self.rollback_to_batch(fork_point, &mut batch, &PendingWrites::default())?;
        self.storage.commit(&batch)?;
        Ok(changes)
    }

    /// Roll back all rows where block_number > fork_point, deferring the
    /// storage deletion to the provided write batch for atomic commit.
    /// Returns compensating Delete change records for the rolled-back rows,
    /// also for the rows that `pending` has not committed yet.
    pub fn rollback_to_batch(
        &self,
        fork_point: BlockNumber,
        batch: &mut StorageWriteBatch,
        pending: &PendingWrites,
    ) -> Result<Vec<ChangeRecord>> {
        if fork_point == BlockNumber::MAX {
            return Ok(Vec::new());
        }

        let stored =
            self.storage
                .get_raw_rows(&self.def.name, fork_point + 1, BlockNumber::MAX)?;
        let rolled_back = pending.raw_rows_after(&self.def.name, fork_point, stored);

        // Defer the deletion to the write batch
        batch.delete_raw_rows_after(&self.def.name, fork_point);

        let mut changes = Vec::new();
        for (block, data) in rolled_back {
            let rows = storage::decode_rows(&data, &self.registry)?;
            for (idx, row) in rows.into_iter().enumerate() {
                changes.push(ChangeRecord::new(
                    ChangeOp::Delete,
                    self.change_schema.clone(),
                    smallvec![Value::UInt64(block), Value::UInt64(idx as u64)],
                    row.into_values().into(),
                    None,
                ));
            }
        }

        Ok(changes)
    }

    /// The rows of the first `max_blocks` blocks with data in a block range
    /// (inclusive). Used for replay.
    pub fn get_rows(
        &self,
        from_block: BlockNumber,
        to_block: BlockNumber,
        max_blocks: usize,
    ) -> Result<Vec<(BlockNumber, Vec<Row>)>> {
        let raw =
            self.storage
                .get_raw_rows_limited(&self.def.name, from_block, to_block, max_blocks)?;
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
    use std::collections::HashMap;

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
        assert_eq!(changes[0].table(), "trades");
        assert_eq!(changes[0].key("block_number"), Some(&Value::UInt64(1000)));
        assert_eq!(changes[0].key("_row_index"), Some(&Value::UInt64(0)));
        assert_eq!(changes[0].get("user"), Some(&Value::String("alice".into())));

        assert_eq!(changes[1].key("_row_index"), Some(&Value::UInt64(1)));
        assert_eq!(changes[1].get("user"), Some(&Value::String("bob".into())));
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

        let rows = engine.get_rows(1000, 1001, usize::MAX).unwrap();
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
            assert_eq!(d.table(), "trades");
        }

        // Verify bob's row is in the changes
        assert!(
            changes
                .iter()
                .any(|d| d.get("user") == Some(&Value::String("bob".into())))
        );

        // Verify storage only has block 1000
        let remaining = engine.get_rows(1000, 1010, usize::MAX).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].0, 1000);
    }

    /// An Insert and the Delete that undoes it carry the same columns: those
    /// of the schema, in its order, with Null where the row has none.
    #[test]
    fn insert_and_its_rollback_delete_carry_the_schema_columns() {
        let storage = Arc::new(MemoryBackend::new());
        let engine = RawTableEngine::new(test_table_def(), storage);

        let row = HashMap::from([
            ("user".to_string(), Value::String("alice".into())),
            ("extra".to_string(), Value::UInt64(7)),
        ]);
        engine.ingest(1000, &[make_row_map("bob", 1.0)]).unwrap();
        let insert = engine.ingest(1001, &[row]).unwrap().remove(0);
        let delete = engine.rollback(1000).unwrap().remove(0);

        let columns = |r: &ChangeRecord| {
            r.values()
                .map(|(name, value)| (name.to_string(), value.clone()))
                .collect::<Vec<_>>()
        };
        let expected = vec![
            ("block_number".to_string(), Value::Null),
            ("user".to_string(), Value::String("alice".into())),
            ("amount".to_string(), Value::Null),
        ];
        assert_eq!(columns(&insert), expected);
        assert_eq!(columns(&delete), expected);
        assert_eq!(insert.get("extra"), None);
        assert_eq!(delete.key_map(), insert.key_map());
    }

    #[test]
    fn rollback_to_latest_is_noop() {
        let storage = Arc::new(MemoryBackend::new());
        let engine = RawTableEngine::new(test_table_def(), storage);

        engine.ingest(1000, &[make_row_map("alice", 10.0)]).unwrap();

        let changes = engine.rollback(1000).unwrap();
        assert!(changes.is_empty());

        let remaining = engine.get_rows(1000, 1000, usize::MAX).unwrap();
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
            rollback_changes[0].get("user"),
            Some(&Value::String("carol".into()))
        );

        // Re-ingest block 1002 with different data (reorg)
        let new_changes = engine.ingest(1002, &[make_row_map("eve", 50.0)]).unwrap();
        assert_eq!(new_changes.len(), 1);
        assert_eq!(
            new_changes[0].get("user"),
            Some(&Value::String("eve".into()))
        );

        // Verify final state
        let all_rows = engine.get_rows(1000, 1010, usize::MAX).unwrap();
        assert_eq!(all_rows.len(), 3);
        assert_eq!(all_rows[2].0, 1002);
        assert_eq!(
            all_rows[2].1[0].get("user"),
            Some(&Value::String("eve".into()))
        );
    }
}
