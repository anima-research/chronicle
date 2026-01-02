//! NAPI bindings for Node.js/TypeScript.

use crate::{
    error::StoreError, CompactionSummary, Record, RecordId, Sequence, StateOperation,
    StateRegistration, StateStrategy, Store, StoreConfig,
};
use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::sync::Arc;

/// JavaScript-friendly Store wrapper.
#[napi]
pub struct JsStore {
    inner: Option<Arc<Store>>,
}

/// Record returned to JavaScript.
#[napi(object)]
pub struct JsRecord {
    pub id: String,
    pub sequence: i64,
    pub record_type: String,
    pub payload: Buffer,
    pub timestamp: i64,
    pub caused_by: Vec<String>,
    pub linked_to: Vec<String>,
}

impl From<Record> for JsRecord {
    fn from(r: Record) -> Self {
        JsRecord {
            id: r.id.0.to_string(),
            sequence: r.sequence.0 as i64,
            record_type: r.record_type,
            payload: Buffer::from(r.payload),
            timestamp: r.timestamp.0,
            caused_by: r.caused_by.iter().map(|id| id.0.to_string()).collect(),
            linked_to: r.linked_to.iter().map(|id| id.0.to_string()).collect(),
        }
    }
}

/// Branch info returned to JavaScript.
#[napi(object)]
pub struct JsBranch {
    pub id: String,
    pub name: String,
    pub head: i64,
    pub parent_id: Option<String>,
    pub branch_point: Option<i64>,
    pub created: i64,
}

/// Store statistics.
#[napi(object)]
pub struct JsStoreStats {
    pub record_count: i64,
    pub blob_count: i64,
    pub branch_count: i64,
    pub state_slot_count: i64,
    pub total_size_bytes: i64,
    pub blob_size_bytes: i64,
}

/// Configuration for creating a store.
#[napi(object)]
pub struct JsStoreConfig {
    pub path: String,
    pub blob_cache_size: Option<i64>,
}

/// State registration options.
#[napi(object)]
pub struct JsStateRegistration {
    pub id: String,
    pub strategy: String, // "snapshot" | "append_log"
    pub delta_snapshot_every: Option<i64>,
    pub full_snapshot_every: Option<i64>,
    pub initial_value: Option<Buffer>,
}

/// GC options for branches.
#[napi(object)]
pub struct JsBranchGcOptions {
    pub delete_orphaned: Option<bool>,
    pub delete_empty: Option<bool>,
    pub delete_stale_older_than: Option<i64>,
    pub name_patterns: Option<Vec<String>>,
    pub force: Option<bool>,
    pub reparent_to: Option<String>,
}

/// GC result.
#[napi(object)]
pub struct JsBranchGcResult {
    pub deleted: Vec<String>,
    pub skipped: Vec<String>,
    pub reparented: i64,
}

/// Compaction summary.
#[napi(object)]
pub struct JsCompactionSummary {
    pub total_operations: i64,
    pub compactable_operations: i64,
    pub total_bytes: i64,
    pub compactable_bytes: i64,
    pub states_needing_compaction: i64,
}

/// Options for appending a record with links.
#[napi(object)]
pub struct JsRecordOptions {
    pub caused_by: Option<Vec<String>>,
    pub linked_to: Option<Vec<String>>,
}

/// Query filter for records.
#[napi(object)]
pub struct JsQueryFilter {
    pub types: Option<Vec<String>>,
    pub from_sequence: Option<i64>,
    pub to_sequence: Option<i64>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

/// State info returned to JavaScript.
#[napi(object)]
pub struct JsStateInfo {
    pub id: String,
    pub strategy: String,
    pub item_count: Option<i64>,
    pub ops_since_snapshot: i64,
}

impl From<CompactionSummary> for JsCompactionSummary {
    fn from(s: CompactionSummary) -> Self {
        JsCompactionSummary {
            total_operations: s.total_operations as i64,
            compactable_operations: s.compactable_operations as i64,
            total_bytes: s.total_bytes as i64,
            compactable_bytes: s.compactable_bytes as i64,
            states_needing_compaction: s.states_needing_compaction as i64,
        }
    }
}

fn to_napi_error(e: StoreError) -> napi::Error {
    napi::Error::from_reason(e.to_string())
}

#[napi]
impl JsStore {
    /// Get the inner store, returning an error if closed.
    fn get_store(&self) -> Result<&Arc<Store>> {
        self.inner
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("Store has been closed"))
    }

    /// Create a new store at the given path.
    #[napi(factory)]
    pub fn create(config: JsStoreConfig) -> Result<JsStore> {
        let store_config = StoreConfig {
            path: config.path.into(),
            blob_cache_size: config.blob_cache_size.map(|s| s as usize).unwrap_or(1000),
            create_if_missing: false,
        };
        let store = Store::create(store_config).map_err(to_napi_error)?;
        Ok(JsStore {
            inner: Some(Arc::new(store)),
        })
    }

    /// Open an existing store.
    #[napi(factory)]
    pub fn open(config: JsStoreConfig) -> Result<JsStore> {
        let store_config = StoreConfig {
            path: config.path.into(),
            blob_cache_size: config.blob_cache_size.map(|s| s as usize).unwrap_or(1000),
            create_if_missing: false,
        };
        let store = Store::open(store_config).map_err(to_napi_error)?;
        Ok(JsStore {
            inner: Some(Arc::new(store)),
        })
    }

    /// Open or create a store.
    #[napi(factory)]
    pub fn open_or_create(config: JsStoreConfig) -> Result<JsStore> {
        let store_config = StoreConfig {
            path: config.path.into(),
            blob_cache_size: config.blob_cache_size.map(|s| s as usize).unwrap_or(1000),
            create_if_missing: true,
        };
        let store = Store::open_or_create(store_config).map_err(to_napi_error)?;
        Ok(JsStore {
            inner: Some(Arc::new(store)),
        })
    }

    /// Close the store, releasing the lock and any resources.
    /// After closing, all operations on this store will fail.
    #[napi]
    pub fn close(&mut self) -> Result<()> {
        if let Some(store) = self.inner.take() {
            // Sync before closing
            store.sync().map_err(to_napi_error)?;
            // Drop the Arc - if this is the last reference, the store will be dropped
            drop(store);
        }
        Ok(())
    }

    /// Check if the store has been closed.
    #[napi]
    pub fn is_closed(&self) -> bool {
        self.inner.is_none()
    }

    // --- Records ---

    /// Append a record to the store.
    #[napi]
    pub fn append(&self, record_type: String, payload: Buffer) -> Result<JsRecord> {
        let store = self.get_store()?;
        let input = crate::RecordInput::raw(&record_type, payload.to_vec());
        let record = store.append(input).map_err(to_napi_error)?;
        Ok(record.into())
    }

    /// Append a JSON record.
    #[napi]
    pub fn append_json(&self, record_type: String, data: serde_json::Value) -> Result<JsRecord> {
        let store = self.get_store()?;
        let input =
            crate::RecordInput::json(&record_type, &data).map_err(|e| to_napi_error(e.into()))?;
        let record = store.append(input).map_err(to_napi_error)?;
        Ok(record.into())
    }

    /// Get a record by ID.
    #[napi]
    pub fn get_record(&self, id: String) -> Result<Option<JsRecord>> {
        let store = self.get_store()?;
        let id: u64 = id
            .parse()
            .map_err(|_| napi::Error::from_reason("Invalid record ID"))?;
        let record = store.get_record(RecordId(id)).map_err(to_napi_error)?;
        Ok(record.map(Into::into))
    }

    /// Get record IDs by type.
    #[napi]
    pub fn get_record_ids_by_type(&self, record_type: String) -> Result<Vec<String>> {
        let store = self.get_store()?;
        Ok(store
            .get_records_by_type(&record_type)
            .into_iter()
            .map(|id| id.0.to_string())
            .collect())
    }

    // --- Blobs ---

    /// Store a blob and return its hash.
    #[napi]
    pub fn store_blob(&self, content: Buffer, content_type: String) -> Result<String> {
        let store = self.get_store()?;
        let hash = store
            .store_blob(&content, &content_type)
            .map_err(to_napi_error)?;
        Ok(hash.to_string())
    }

    /// Get a blob by hash.
    #[napi]
    pub fn get_blob(&self, hash: String) -> Result<Option<Buffer>> {
        let store = self.get_store()?;
        let hash = crate::Hash::from_hex(&hash)
            .map_err(|_| napi::Error::from_reason("Invalid hash"))?;
        let blob = store.get_blob(&hash).map_err(to_napi_error)?;
        Ok(blob.map(|b| Buffer::from(b.content)))
    }

    // --- Branches ---

    /// Create a new branch.
    #[napi]
    pub fn create_branch(&self, name: String, from: Option<String>) -> Result<JsBranch> {
        let store = self.get_store()?;
        let branch = store
            .create_branch(&name, from.as_deref())
            .map_err(to_napi_error)?;
        Ok(JsBranch {
            id: branch.id.0.to_string(),
            name: branch.name,
            head: branch.head.0 as i64,
            parent_id: branch.parent.map(|p| p.0.to_string()),
            branch_point: branch.branch_point.map(|s| s.0 as i64),
            created: branch.created.0,
        })
    }

    /// Create a new branch without copying state from parent.
    #[napi]
    pub fn create_empty_branch(&self, name: String, from: Option<String>) -> Result<JsBranch> {
        let store = self.get_store()?;
        let branch = store
            .create_empty_branch(&name, from.as_deref())
            .map_err(to_napi_error)?;
        Ok(JsBranch {
            id: branch.id.0.to_string(),
            name: branch.name,
            head: branch.head.0 as i64,
            parent_id: branch.parent.map(|p| p.0.to_string()),
            branch_point: branch.branch_point.map(|s| s.0 as i64),
            created: branch.created.0,
        })
    }

    /// Switch to a branch.
    #[napi]
    pub fn switch_branch(&self, name: String) -> Result<JsBranch> {
        let store = self.get_store()?;
        let branch = store.switch_branch(&name).map_err(to_napi_error)?;
        Ok(JsBranch {
            id: branch.id.0.to_string(),
            name: branch.name,
            head: branch.head.0 as i64,
            parent_id: branch.parent.map(|p| p.0.to_string()),
            branch_point: branch.branch_point.map(|s| s.0 as i64),
            created: branch.created.0,
        })
    }

    /// Get the current branch.
    #[napi]
    pub fn current_branch(&self) -> Result<JsBranch> {
        let store = self.get_store()?;
        let branch = store.current_branch();
        Ok(JsBranch {
            id: branch.id.0.to_string(),
            name: branch.name,
            head: branch.head.0 as i64,
            parent_id: branch.parent.map(|p| p.0.to_string()),
            branch_point: branch.branch_point.map(|s| s.0 as i64),
            created: branch.created.0,
        })
    }

    /// List all branches.
    #[napi]
    pub fn list_branches(&self) -> Result<Vec<JsBranch>> {
        let store = self.get_store()?;
        Ok(store
            .list_branches()
            .into_iter()
            .map(|b| JsBranch {
                id: b.id.0.to_string(),
                name: b.name,
                head: b.head.0 as i64,
                parent_id: b.parent.map(|p| p.0.to_string()),
                branch_point: b.branch_point.map(|s| s.0 as i64),
                created: b.created.0,
            })
            .collect())
    }

    /// Delete a branch.
    #[napi]
    pub fn delete_branch(&self, name: String) -> Result<()> {
        let store = self.get_store()?;
        store.delete_branch(&name).map_err(to_napi_error)
    }

    // --- State Management ---

    /// Register a new state.
    #[napi]
    pub fn register_state(&self, registration: JsStateRegistration) -> Result<()> {
        let store = self.get_store()?;
        let strategy = match registration.strategy.as_str() {
            "snapshot" => StateStrategy::Snapshot,
            "append_log" => StateStrategy::AppendLog {
                delta_snapshot_every: registration.delta_snapshot_every.unwrap_or(100) as u64,
                full_snapshot_every: registration.full_snapshot_every.unwrap_or(10) as u64,
            },
            _ => return Err(napi::Error::from_reason("Invalid strategy")),
        };

        let reg = StateRegistration {
            id: registration.id,
            strategy,
            initial_value: registration.initial_value.map(|b| b.to_vec()),
        };

        store.register_state(reg).map_err(to_napi_error)
    }

    /// Get state value.
    #[napi]
    pub fn get_state(&self, state_id: String) -> Result<Option<Buffer>> {
        let store = self.get_store()?;
        let state = store.get_state(&state_id).map_err(to_napi_error)?;
        Ok(state.map(Buffer::from))
    }

    /// Get state as JSON.
    #[napi]
    pub fn get_state_json(&self, state_id: String) -> Result<Option<serde_json::Value>> {
        let store = self.get_store()?;
        let state = store.get_state(&state_id).map_err(to_napi_error)?;
        match state {
            Some(bytes) => {
                let value: serde_json::Value = serde_json::from_slice(&bytes)
                    .map_err(|e| napi::Error::from_reason(e.to_string()))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Set state value (for Snapshot strategy).
    #[napi]
    pub fn set_state(&self, state_id: String, value: Buffer) -> Result<JsRecord> {
        let store = self.get_store()?;
        let record = store
            .update_state(&state_id, StateOperation::Set(value.to_vec()))
            .map_err(to_napi_error)?;
        Ok(record.into())
    }

    /// Set state as JSON.
    #[napi]
    pub fn set_state_json(&self, state_id: String, value: serde_json::Value) -> Result<JsRecord> {
        let store = self.get_store()?;
        let bytes =
            serde_json::to_vec(&value).map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let record = store
            .update_state(&state_id, StateOperation::Set(bytes))
            .map_err(to_napi_error)?;
        Ok(record.into())
    }

    /// Append to an AppendLog state.
    #[napi]
    pub fn append_to_state(&self, state_id: String, item: Buffer) -> Result<JsRecord> {
        let store = self.get_store()?;
        let record = store
            .update_state(&state_id, StateOperation::Append(item.to_vec()))
            .map_err(to_napi_error)?;
        Ok(record.into())
    }

    /// Append JSON to an AppendLog state.
    #[napi]
    pub fn append_to_state_json(
        &self,
        state_id: String,
        item: serde_json::Value,
    ) -> Result<JsRecord> {
        let store = self.get_store()?;
        let bytes =
            serde_json::to_vec(&item).map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let record = store
            .update_state(&state_id, StateOperation::Append(bytes))
            .map_err(to_napi_error)?;
        Ok(record.into())
    }

    /// Edit an item in an AppendLog state.
    #[napi]
    pub fn edit_state_item(
        &self,
        state_id: String,
        index: i64,
        new_value: Buffer,
    ) -> Result<JsRecord> {
        let store = self.get_store()?;
        let record = store
            .update_state(
                &state_id,
                StateOperation::Edit {
                    index: index as usize,
                    new_value: new_value.to_vec(),
                },
            )
            .map_err(to_napi_error)?;
        Ok(record.into())
    }

    /// Redact items from an AppendLog state.
    #[napi]
    pub fn redact_state_items(
        &self,
        state_id: String,
        start: i64,
        end: i64,
    ) -> Result<JsRecord> {
        let store = self.get_store()?;
        let record = store
            .update_state(
                &state_id,
                StateOperation::Redact {
                    start: start as usize,
                    end: end as usize,
                },
            )
            .map_err(to_napi_error)?;
        Ok(record.into())
    }

    /// Get the length of an AppendLog state.
    #[napi]
    pub fn get_state_len(&self, state_id: String) -> Result<Option<i64>> {
        let store = self.get_store()?;
        let len = store
            .get_state_len(&state_id)
            .map_err(to_napi_error)?;
        Ok(len.map(|l| l as i64))
    }

    /// Get a slice of an AppendLog state.
    #[napi]
    pub fn get_state_slice(
        &self,
        state_id: String,
        offset: i64,
        limit: i64,
    ) -> Result<Option<Buffer>> {
        let store = self.get_store()?;
        let slice = store
            .get_state_slice(&state_id, offset as usize, limit as usize)
            .map_err(to_napi_error)?;
        Ok(slice.map(Buffer::from))
    }

    /// Get the last N items from an AppendLog state.
    #[napi]
    pub fn get_state_tail(&self, state_id: String, count: i64) -> Result<Option<Buffer>> {
        let store = self.get_store()?;
        let tail = store
            .get_state_tail(&state_id, count as usize)
            .map_err(to_napi_error)?;
        Ok(tail.map(Buffer::from))
    }

    // --- Compaction ---

    /// Compact a state by creating a full snapshot.
    #[napi]
    pub fn compact_state(&self, state_id: String) -> Result<Option<JsRecord>> {
        let store = self.get_store()?;
        let record = store
            .compact_state(&state_id)
            .map_err(to_napi_error)?;
        Ok(record.map(Into::into))
    }

    /// Compact all states.
    #[napi]
    pub fn compact_all_states(&self) -> Result<i64> {
        let store = self.get_store()?;
        let count = store.compact_all_states().map_err(to_napi_error)?;
        Ok(count as i64)
    }

    /// Get compaction summary.
    #[napi]
    pub fn get_compaction_summary(&self) -> Result<JsCompactionSummary> {
        let store = self.get_store()?;
        let summary = store
            .get_compaction_summary()
            .map_err(to_napi_error)?;
        Ok(summary.into())
    }

    // --- Stats ---

    /// Get store statistics.
    #[napi]
    pub fn stats(&self) -> Result<JsStoreStats> {
        let store = self.get_store()?;
        let stats = store.stats().map_err(to_napi_error)?;
        Ok(JsStoreStats {
            record_count: stats.record_count as i64,
            blob_count: stats.blob_count as i64,
            branch_count: stats.branch_count as i64,
            state_slot_count: stats.state_slot_count as i64,
            total_size_bytes: stats.total_size_bytes as i64,
            blob_size_bytes: stats.blob_size_bytes as i64,
        })
    }

    // --- New UI/Explorer Methods ---

    /// Append a record with caused_by/linked_to links.
    #[napi]
    pub fn append_with_links(
        &self,
        record_type: String,
        payload: Buffer,
        options: JsRecordOptions,
    ) -> Result<JsRecord> {
        let store = self.get_store()?;
        let caused_by: Vec<RecordId> = options
            .caused_by
            .unwrap_or_default()
            .iter()
            .filter_map(|s| s.parse::<u64>().ok().map(RecordId))
            .collect();
        let linked_to: Vec<RecordId> = options
            .linked_to
            .unwrap_or_default()
            .iter()
            .filter_map(|s| s.parse::<u64>().ok().map(RecordId))
            .collect();

        let input = crate::RecordInput::raw(&record_type, payload.to_vec())
            .with_caused_by(caused_by)
            .with_linked_to(linked_to);
        let record = store.append(input).map_err(to_napi_error)?;
        Ok(record.into())
    }

    /// Append a JSON record with caused_by/linked_to links.
    #[napi]
    pub fn append_json_with_links(
        &self,
        record_type: String,
        data: serde_json::Value,
        options: JsRecordOptions,
    ) -> Result<JsRecord> {
        let store = self.get_store()?;
        let caused_by: Vec<RecordId> = options
            .caused_by
            .unwrap_or_default()
            .iter()
            .filter_map(|s| s.parse::<u64>().ok().map(RecordId))
            .collect();
        let linked_to: Vec<RecordId> = options
            .linked_to
            .unwrap_or_default()
            .iter()
            .filter_map(|s| s.parse::<u64>().ok().map(RecordId))
            .collect();

        let input = crate::RecordInput::json(&record_type, &data)
            .map_err(|e| to_napi_error(e.into()))?
            .with_caused_by(caused_by)
            .with_linked_to(linked_to);
        let record = store.append(input).map_err(to_napi_error)?;
        Ok(record.into())
    }

    /// Query records with filters.
    #[napi]
    pub fn query(&self, filter: JsQueryFilter) -> Result<Vec<JsRecord>> {
        let store = self.get_store()?;
        let from_seq = filter.from_sequence.map(|s| s as u64).unwrap_or(0);
        let to_seq = filter.to_sequence.map(|s| s as u64);
        let limit = filter.limit.map(|l| l as usize);
        let offset = filter.offset.map(|o| o as usize).unwrap_or(0);
        let types = filter.types;

        let mut records = Vec::new();
        let mut skipped = 0;

        for result in store.iter_from(Sequence(from_seq)) {
            let (_, record) = result.map_err(to_napi_error)?;

            // Check sequence bound
            if let Some(to) = to_seq {
                if record.sequence.0 > to {
                    break;
                }
            }

            // Check type filter
            if let Some(ref type_filter) = types {
                if !type_filter.contains(&record.record_type) {
                    continue;
                }
            }

            // Handle offset
            if skipped < offset {
                skipped += 1;
                continue;
            }

            records.push(record.into());

            // Handle limit
            if let Some(max) = limit {
                if records.len() >= max {
                    break;
                }
            }
        }

        Ok(records)
    }

    /// List all registered states with their info.
    #[napi]
    pub fn list_states(&self) -> Result<Vec<JsStateInfo>> {
        let store = self.get_store()?;
        let branch_id = store.current_branch().id;
        Ok(store
            .state
            .state_ids()
            .into_iter()
            .map(|id| {
                let strategy = store
                    .state
                    .get_strategy(&id)
                    .map(|s| match s {
                        StateStrategy::Snapshot => "snapshot".to_string(),
                        StateStrategy::AppendLog { .. } => "append_log".to_string(),
                        StateStrategy::Delta { .. } => "delta".to_string(),
                        StateStrategy::Struct { .. } => "struct".to_string(),
                    })
                    .unwrap_or_else(|| "unknown".to_string());

                let (item_count, ops_since_snapshot) = store
                    .state
                    .get_head(branch_id, &id)
                    .map(|h| (Some(h.item_count as i64), h.ops_since_delta_snapshot as i64))
                    .unwrap_or((None, 0));

                JsStateInfo {
                    id,
                    strategy,
                    item_count,
                    ops_since_snapshot,
                }
            })
            .collect())
    }

    /// Get the current sequence number (head of current branch).
    #[napi]
    pub fn current_sequence(&self) -> Result<i64> {
        let store = self.get_store()?;
        Ok(store.current_branch().head.0 as i64)
    }

    /// Get state value at a specific sequence (historical access).
    #[napi]
    pub fn get_state_at(&self, state_id: String, at_sequence: i64) -> Result<Option<Buffer>> {
        let store = self.get_store()?;
        let state = store
            .get_state_at(&state_id, Sequence(at_sequence as u64))
            .map_err(to_napi_error)?;
        Ok(state.map(Buffer::from))
    }

    /// Get state as JSON at a specific sequence (historical access).
    #[napi]
    pub fn get_state_json_at(
        &self,
        state_id: String,
        at_sequence: i64,
    ) -> Result<Option<serde_json::Value>> {
        let store = self.get_store()?;
        let state = store
            .get_state_at(&state_id, Sequence(at_sequence as u64))
            .map_err(to_napi_error)?;
        match state {
            Some(bytes) => {
                let value: serde_json::Value = serde_json::from_slice(&bytes)
                    .map_err(|e| napi::Error::from_reason(e.to_string()))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Get records that were caused by a given record.
    #[napi]
    pub fn get_effects(&self, record_id: String) -> Result<Vec<String>> {
        let store = self.get_store()?;
        let id: u64 = record_id
            .parse()
            .map_err(|_| napi::Error::from_reason("Invalid record ID"))?;
        let effects = store.index.get_caused_by(RecordId(id));
        Ok(effects.iter().map(|id| id.0.to_string()).collect())
    }

    /// Get records that link to a given record.
    #[napi]
    pub fn get_links_to(&self, record_id: String) -> Result<Vec<String>> {
        let store = self.get_store()?;
        let id: u64 = record_id
            .parse()
            .map_err(|_| napi::Error::from_reason("Invalid record ID"))?;
        let links = store.index.get_linked_to(RecordId(id));
        Ok(links.iter().map(|id| id.0.to_string()).collect())
    }

    /// Sync all pending writes to disk.
    #[napi]
    pub fn sync(&self) -> Result<()> {
        let store = self.get_store()?;
        store.sync().map_err(to_napi_error)
    }
}
