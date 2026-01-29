//! Main Store struct tying all components together.

use crate::blobs::BlobStorage;
use crate::branches::BranchManager;
use crate::error::{Result, StoreError};
use crate::records::{RecordIndex, RecordLog};
use crate::state::StateManager;
use crate::subscriptions::{SubscriptionConfig, SubscriptionHandle, SubscriptionId, SubscriptionManager};
use crate::types::{
    Blob, Branch, Hash, Record, RecordId, RecordInput, Sequence,
    StateOperation, StateRegistration, StateUpdateRecord, StoreStats, Timestamp,
};
use fs2::FileExt;
use parking_lot::Mutex;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Store configuration.
#[derive(Clone, Debug)]
pub struct StoreConfig {
    /// Base path for the store.
    pub path: PathBuf,

    /// Blob cache size (number of blobs).
    pub blob_cache_size: usize,

    /// Whether to create the store if it doesn't exist.
    pub create_if_missing: bool,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("./store"),
            blob_cache_size: 1000,
            create_if_missing: true,
        }
    }
}

/// Summary of compaction potential across all states.
#[derive(Clone, Debug)]
pub struct CompactionSummary {
    /// Total number of state operations across all states.
    pub total_operations: u64,
    /// Operations that could be skipped after compaction.
    pub compactable_operations: u64,
    /// Total bytes used by state operations.
    pub total_bytes: u64,
    /// Bytes that could be reclaimed (logically) after compaction.
    pub compactable_bytes: u64,
    /// Number of states that would benefit from compaction.
    pub states_needing_compaction: usize,
}

/// Magic bytes for store manifest.
const STORE_MAGIC: &[u8; 4] = b"RST\0";

/// Current store format version.
const STORE_VERSION: u8 = 1;

/// The main record store.
///
/// Provides a unified interface for:
/// - Appending records to the log
/// - Storing and retrieving blobs
/// - Managing per-state chains
/// - Creating and switching branches
pub struct Store {
    /// Store configuration.
    config: StoreConfig,

    /// Lock file for exclusive access.
    _lock_file: File,

    /// Record log (shared with StateManager for disk-based traversal).
    log: Arc<RecordLog>,

    /// Record index.
    pub(crate) index: RecordIndex,

    /// Blob storage.
    blobs: BlobStorage,

    /// State manager.
    pub(crate) state: StateManager,

    /// Branch manager.
    branches: BranchManager,

    /// Subscription manager for live updates.
    subscriptions: SubscriptionManager,

    /// Lock for write operations to ensure atomicity.
    write_lock: Mutex<()>,
}

impl Store {
    /// Open an existing store or create a new one.
    pub fn open_or_create(config: StoreConfig) -> Result<Self> {
        if config.path.exists() {
            Self::open(config)
        } else if config.create_if_missing {
            Self::create(config)
        } else {
            Err(StoreError::NotInitialized)
        }
    }

    /// Create a new store.
    pub fn create(config: StoreConfig) -> Result<Self> {
        // Create directory structure
        fs::create_dir_all(&config.path)?;
        fs::create_dir_all(config.path.join("blobs"))?;

        // Write manifest
        Self::write_manifest(&config.path)?;

        // Acquire lock
        let lock_file = Self::acquire_lock(&config.path)?;

        // Initialize components
        let log = Arc::new(RecordLog::open(config.path.join("records.log"))?);
        let blobs = BlobStorage::new(config.path.join("blobs"), config.blob_cache_size)?;
        let mut state = StateManager::new(config.path.join("state.bin"))?;
        let branches = BranchManager::new(config.path.join("branches.bin"))?;

        // Build index from log (empty for new store, but consistent with open())
        let index = RecordIndex::rebuild_from_log(config.path.join("records.idx"), &log)?;

        // Connect state manager to log for disk-based traversal
        state.set_log(Arc::clone(&log));

        Ok(Self {
            config,
            _lock_file: lock_file,
            log,
            index,
            blobs,
            state,
            branches,
            subscriptions: SubscriptionManager::new(),
            write_lock: Mutex::new(()),
        })
    }

    /// Open an existing store.
    pub fn open(config: StoreConfig) -> Result<Self> {
        // Verify manifest
        Self::verify_manifest(&config.path)?;

        // Acquire lock
        let lock_file = Self::acquire_lock(&config.path)?;

        // Open components
        let log = Arc::new(RecordLog::open(config.path.join("records.log"))?);
        let blobs = BlobStorage::new(config.path.join("blobs"), config.blob_cache_size)?;
        let mut state = StateManager::load(config.path.join("state.bin"))?;
        let branches = BranchManager::load(config.path.join("branches.bin"))?;

        // Rebuild index from log (O(N) startup, but O(1) sync)
        let index = RecordIndex::rebuild_from_log(config.path.join("records.idx"), &log)?;

        // Connect state manager to log for disk-based traversal
        state.set_log(Arc::clone(&log));

        Ok(Self {
            config,
            _lock_file: lock_file,
            log,
            index,
            blobs,
            state,
            branches,
            subscriptions: SubscriptionManager::new(),
            write_lock: Mutex::new(()),
        })
    }

    // --- Record Operations ---

    /// Append a record to the current branch.
    pub fn append(&self, input: RecordInput) -> Result<Record> {
        let _lock = self.write_lock.lock();

        let branch = self.branches.current_branch();
        let next_seq = branch.head.next();

        let (record, offset) = self.log.append(input, branch.id, next_seq)?;

        // Update indices
        self.index.add(
            record.id,
            branch.id,
            next_seq,
            offset,
            &record.record_type,
            &record.caused_by,
            &record.linked_to,
        );

        // Update branch head
        self.branches.update_head(branch.id, next_seq)?;

        // Broadcast to subscribers
        self.subscriptions.broadcast_record(&record);
        self.subscriptions.broadcast_branch_head(&branch.name, next_seq);

        Ok(record)
    }

    /// Get a record by ID.
    pub fn get_record(&self, id: RecordId) -> Result<Option<Record>> {
        if let Some(offset) = self.index.get_offset_by_id(id) {
            Ok(Some(self.log.read_at(offset)?))
        } else {
            Ok(None)
        }
    }

    /// Get records by type.
    pub fn get_records_by_type(&self, record_type: &str) -> Vec<RecordId> {
        self.index.get_by_type(record_type)
    }

    /// Iterate records from a sequence.
    pub fn iter_from(&self, seq: Sequence) -> impl Iterator<Item = Result<(u64, Record)>> + '_ {
        let branch = self.branches.current_branch();
        let offset = self
            .index
            .get_offset(branch.id, seq)
            .unwrap_or(0);
        self.log.iter_from(offset)
    }

    /// Query records in a sequence range with efficient O(log n + k) lookup.
    ///
    /// This uses the BTreeMap index to find records without scanning from the start.
    /// - `from`: Start sequence (inclusive), None for beginning
    /// - `to`: End sequence (inclusive), None for end
    /// - `limit`: Maximum records to return
    /// - `reverse`: If true, return records in descending sequence order
    /// - `types`: Optional record type filter (applied after fetching)
    ///
    /// Returns records matching the criteria.
    pub fn query_range(
        &self,
        from: Option<Sequence>,
        to: Option<Sequence>,
        limit: usize,
        reverse: bool,
        types: Option<&[String]>,
    ) -> Result<Vec<Record>> {
        let branch = self.branches.current_branch();

        // Get offsets from index
        // Note: We may need to fetch more than `limit` if filtering by type
        let fetch_limit = if types.is_some() {
            // Fetch more to account for filtering; will be limited later
            limit * 4 + 100
        } else {
            limit
        };

        let offsets = self.index.query_range(
            branch.id,
            from,
            to,
            fetch_limit,
            reverse,
        );

        // Read records and filter
        let mut records = Vec::with_capacity(limit);
        for (_seq, offset) in offsets {
            if records.len() >= limit {
                break;
            }

            let record = self.log.read_at(offset)?;

            // Apply type filter if specified
            if let Some(types) = types {
                if !types.contains(&record.record_type) {
                    continue;
                }
            }

            records.push(record);
        }

        Ok(records)
    }

    // --- Blob Operations ---

    /// Store a blob.
    pub fn store_blob(&self, content: &[u8], content_type: &str) -> Result<Hash> {
        self.blobs.store(content, content_type)
    }

    /// Get a blob by hash.
    pub fn get_blob(&self, hash: &Hash) -> Result<Option<Blob>> {
        self.blobs.get(hash)
    }

    /// Check if a blob exists.
    pub fn blob_exists(&self, hash: &Hash) -> bool {
        self.blobs.exists(hash)
    }

    // --- State Operations ---

    /// Register a new state.
    pub fn register_state(&self, registration: StateRegistration) -> Result<()> {
        self.state.register_state(registration)
    }

    /// Update a state and record it.
    ///
    /// The operation is validated by applying it to the current state before
    /// recording. This ensures only valid operations are written to the log.
    ///
    /// Snapshots are automatically created when thresholds are reached.
    pub fn update_state(
        &self,
        state_id: &str,
        operation: StateOperation,
    ) -> Result<Record> {
        self.update_state_internal(state_id, operation, false)
    }

    /// Internal update_state with option to skip auto-snapshot.
    fn update_state_internal(
        &self,
        state_id: &str,
        operation: StateOperation,
        skip_auto_snapshot: bool,
    ) -> Result<Record> {
        let _lock = self.write_lock.lock();

        let branch = self.branches.current_branch();

        // Validate operation WITHOUT loading full state (critical for 50M+ operations)
        // - Append: Always succeeds, no validation needed
        // - Edit: Just check index < len
        // - Redact: Check start <= end <= len
        // - Set/Snapshot/DeltaSnapshot: No validation needed
        match &operation {
            StateOperation::Edit { index, .. } => {
                let len = self.get_state_len(state_id)?.unwrap_or(0);
                if *index >= len {
                    return Err(StoreError::InvalidOperation(format!(
                        "Edit index {} out of bounds (len={})",
                        index, len
                    )));
                }
            }
            StateOperation::Redact { start, end } => {
                // Note: Out-of-bounds or start > end redacts are allowed and will be
                // no-ops or clamped during apply_operation. This is intentional for
                // flexibility - the actual validation happens at reconstruction time.
                let _ = (start, end); // Suppress unused warnings
            }
            // Append, Set, Snapshot, DeltaSnapshot - no validation needed
            _ => {}
        }

        // Get current head offset for this state (for chaining)
        let prev_update_offset = self.state.get_head(branch.id, state_id).map(|h| h.head_offset);

        let next_seq = branch.head.next();

        // Create the state update record payload
        let update = StateUpdateRecord {
            record_id: RecordId(0), // Will be assigned
            global_sequence: next_seq,
            state_id: state_id.to_string(),
            prev_update_offset,
            operation: operation.clone(),
            timestamp: Timestamp::now(),
        };

        // Serialize and append
        let payload = serde_json::to_vec(&update)?;
        let input = RecordInput::raw("state_update", payload);

        let (record, offset) = self.log.append(input, branch.id, next_seq)?;

        // Update the state manager with the offset
        self.state.record_update(branch.id, state_id, offset, &operation)?;

        // Update indices
        self.index.add(
            record.id,
            branch.id,
            next_seq,
            offset,
            &record.record_type,
            &record.caused_by,
            &record.linked_to,
        );

        // Update branch head
        self.branches.update_head(branch.id, next_seq)?;

        // Broadcast state delta to subscribers
        self.subscriptions.broadcast_state_delta(state_id, operation, next_seq);
        self.subscriptions.broadcast_branch_head(&branch.name, next_seq);

        // Auto-snapshot if needed (based on strategy thresholds)
        // Done after indices/branch update so the snapshot sees consistent state
        if !skip_auto_snapshot {
            // Drop the lock before calling auto_snapshot to avoid deadlock
            drop(_lock);
            self.auto_snapshot_if_needed(state_id)?;
        }

        Ok(record)
    }

    /// Get the current value of a state.
    pub fn get_state(&self, state_id: &str) -> Result<Option<Vec<u8>>> {
        let branch_id = self.branches.current_branch().id;
        self.state.get_state(branch_id, state_id)
    }

    /// Get the value of a state at a specific sequence number (historical access).
    ///
    /// This reconstructs the state as it was at the given sequence by:
    /// 1. Walking the state chain backwards from the head
    /// 2. Skipping operations with sequence > at_sequence
    /// 3. Collecting operations with sequence <= at_sequence
    /// 4. Applying them in forward order
    ///
    /// Returns None if the state didn't exist at that sequence.
    pub fn get_state_at(&self, state_id: &str, at_sequence: Sequence) -> Result<Option<Vec<u8>>> {
        let branch_id = self.branches.current_branch().id;
        self.get_state_at_for_branch(branch_id, state_id, at_sequence)
    }

    /// Get the value of a state at a specific sequence on a specific branch.
    ///
    /// This is like `get_state_at` but allows querying a branch other than the current one.
    /// Used internally for branch state materialization.
    fn get_state_at_for_branch(
        &self,
        branch_id: crate::types::BranchId,
        state_id: &str,
        at_sequence: Sequence,
    ) -> Result<Option<Vec<u8>>> {
        let head = match self.state.get_head(branch_id, state_id) {
            Some(h) => h,
            None => return Ok(None),
        };

        // Walk chain backwards, collecting operations at or before target sequence
        let mut operations = Vec::new();
        let mut current_offset = Some(head.head_offset);
        let mut hit_snapshot = false;
        let mut found_any = false;

        while let Some(offset) = current_offset {
            let record = self.log.read_at(offset)?;

            // Skip if this record is after the target sequence
            if record.sequence > at_sequence {
                // Parse just to get prev_update_offset
                let update: StateUpdateRecord = serde_json::from_slice(&record.payload)
                    .map_err(|e| StoreError::Deserialization(e.to_string()))?;
                current_offset = update.prev_update_offset;
                continue;
            }

            found_any = true;

            let update: StateUpdateRecord = serde_json::from_slice(&record.payload)
                .map_err(|e| StoreError::Deserialization(e.to_string()))?;

            match &update.operation {
                StateOperation::Snapshot(_) => {
                    operations.push(update.operation.clone());
                    break;
                }
                StateOperation::DeltaSnapshot(_) => {
                    operations.push(update.operation.clone());
                    hit_snapshot = true;
                }
                _ => {
                    if !hit_snapshot {
                        operations.push(update.operation.clone());
                    }
                }
            }

            current_offset = update.prev_update_offset;
        }

        if !found_any {
            // State didn't exist at that sequence
            return Ok(None);
        }

        // Apply operations in forward order
        operations.reverse();
        let mut state = Vec::new();
        for op in operations {
            state = crate::state::apply_operation(state, op)?;
        }

        Ok(Some(state))
    }

    /// Find the chain head info (offset and item count) at a historical sequence.
    ///
    /// Used by `create_branch_at` to point a new branch at an existing chain position
    /// without writing new records. Returns `(head_offset, item_count)` or None if
    /// no state existed at that sequence.
    fn find_chain_info_at(
        &self,
        branch_id: crate::types::BranchId,
        state_id: &str,
        at_sequence: Sequence,
    ) -> Result<Option<(u64, usize)>> {
        let head = match self.state.get_head(branch_id, state_id) {
            Some(h) => h,
            None => return Ok(None),
        };

        // Walk chain to find the last update at or before target sequence
        let mut current_offset = Some(head.head_offset);
        let mut found_offset: Option<u64> = None;
        let mut operations = Vec::new();
        let mut hit_snapshot = false;

        while let Some(offset) = current_offset {
            let record = self.log.read_at(offset)?;

            let update: StateUpdateRecord = serde_json::from_slice(&record.payload)
                .map_err(|e| StoreError::Deserialization(e.to_string()))?;

            // Skip if this record is after the target sequence
            if record.sequence > at_sequence {
                current_offset = update.prev_update_offset;
                continue;
            }

            // This is the first record at or before target - it becomes our head
            if found_offset.is_none() {
                found_offset = Some(offset);
            }

            // Collect operations to compute item_count
            match &update.operation {
                StateOperation::Snapshot(_) => {
                    operations.push(update.operation.clone());
                    break;
                }
                StateOperation::DeltaSnapshot(_) => {
                    operations.push(update.operation.clone());
                    hit_snapshot = true;
                }
                _ => {
                    if !hit_snapshot {
                        operations.push(update.operation.clone());
                    }
                }
            }

            current_offset = update.prev_update_offset;
        }

        let head_offset = match found_offset {
            Some(o) => o,
            None => return Ok(None), // No state at that sequence
        };

        // Reconstruct state to count items
        operations.reverse();
        let mut state = Vec::new();
        for op in operations {
            state = crate::state::apply_operation(state, op)?;
        }

        // Count items in the resulting state (assumes JSON array for AppendLog)
        let item_count = if state.is_empty() {
            0
        } else {
            serde_json::from_slice::<Vec<serde_json::Value>>(&state)
                .map(|arr| arr.len())
                .unwrap_or(0)
        };

        Ok(Some((head_offset, item_count)))
    }

    /// Get the length of an AppendLog state without loading all items.
    ///
    /// This is O(1) - the count is tracked in the state chain head.
    /// Returns None if state doesn't exist, Some(0) for empty state.
    pub fn get_state_len(&self, state_id: &str) -> Result<Option<usize>> {
        let branch_id = self.branches.current_branch().id;
        match self.state.get_head(branch_id, state_id) {
            Some(head) => Ok(Some(head.item_count)),
            None => Ok(None),
        }
    }

    /// Get a slice of an AppendLog state.
    ///
    /// This is efficient for accessing recent items (near the end) because
    /// we traverse from HEAD. For items near the start, consider caching.
    ///
    /// - `offset`: Starting index (0-based from beginning of array)
    /// - `limit`: Maximum number of items to return
    ///
    /// Returns the items as a JSON array.
    pub fn get_state_slice(
        &self,
        state_id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Option<Vec<u8>>> {
        // For now, reconstruct full state and slice
        // TODO: Optimize to only reconstruct what's needed
        let state = match self.get_state(state_id)? {
            Some(s) => s,
            None => return Ok(None),
        };

        if state.is_empty() {
            return Ok(Some(serde_json::to_vec(&Vec::<serde_json::Value>::new())?));
        }

        let arr: Vec<serde_json::Value> = serde_json::from_slice(&state)
            .map_err(|e| StoreError::Deserialization(e.to_string()))?;

        let end = (offset + limit).min(arr.len());
        let start = offset.min(arr.len());
        let slice: Vec<_> = arr[start..end].to_vec();

        Ok(Some(serde_json::to_vec(&slice)?))
    }

    /// Get the last N items from an AppendLog state.
    ///
    /// This is O(count + recent_ops) - only traverses as far back as needed.
    /// For states with many items but few recent changes, this is very fast.
    pub fn get_state_tail(&self, state_id: &str, count: usize) -> Result<Option<Vec<u8>>> {
        let branch_id = self.branches.current_branch().id;
        let head = match self.state.get_head(branch_id, state_id) {
            Some(h) => h,
            None => return Ok(None),
        };

        if count == 0 {
            return Ok(Some(serde_json::to_vec(&Vec::<serde_json::Value>::new())?));
        }

        // Fast path: if requesting more items than exist, use full reconstruction
        if count >= head.item_count {
            return self.get_state(state_id);
        }

        // Collect items walking backwards until we have enough
        // We need to handle: Append (add 1), DeltaSnapshot (add N), Snapshot (stop), Redact/Edit (complex)
        let mut items_collected: Vec<serde_json::Value> = Vec::new();
        let mut current_offset = Some(head.head_offset);
        let mut need_full_reconstruct = false;

        while let Some(offset) = current_offset {
            if items_collected.len() >= count {
                break;
            }

            let record = self.log.read_at(offset)?;
            let update: StateUpdateRecord = serde_json::from_slice(&record.payload)
                .map_err(|e| StoreError::Deserialization(e.to_string()))?;

            match &update.operation {
                StateOperation::Append(item) => {
                    let value: serde_json::Value = serde_json::from_slice(item)
                        .map_err(|e| StoreError::Deserialization(e.to_string()))?;
                    items_collected.push(value);
                }
                StateOperation::DeltaSnapshot(data) => {
                    let arr: Vec<serde_json::Value> = serde_json::from_slice(data)
                        .map_err(|e| StoreError::Deserialization(e.to_string()))?;
                    // Take from end of delta to fill our need
                    let need = count - items_collected.len();
                    let start = arr.len().saturating_sub(need);
                    for item in arr[start..].iter().rev() {
                        items_collected.push(item.clone());
                        if items_collected.len() >= count {
                            break;
                        }
                    }
                }
                StateOperation::Snapshot(data) => {
                    let arr: Vec<serde_json::Value> = serde_json::from_slice(data)
                        .map_err(|e| StoreError::Deserialization(e.to_string()))?;
                    // Take from end of snapshot
                    let need = count - items_collected.len();
                    let start = arr.len().saturating_sub(need);
                    for item in arr[start..].iter().rev() {
                        items_collected.push(item.clone());
                        if items_collected.len() >= count {
                            break;
                        }
                    }
                    break; // Full snapshot - we're done
                }
                StateOperation::Edit { .. } | StateOperation::Redact { .. } => {
                    // Edit/Redact make tail optimization complex - fall back to full reconstruct
                    need_full_reconstruct = true;
                    break;
                }
                _ => {}
            }

            current_offset = update.prev_update_offset;
        }

        if need_full_reconstruct {
            // Fall back to full reconstruction and slice
            let state = self.get_state(state_id)?.unwrap_or_default();
            if state.is_empty() {
                return Ok(Some(serde_json::to_vec(&Vec::<serde_json::Value>::new())?));
            }
            let arr: Vec<serde_json::Value> = serde_json::from_slice(&state)
                .map_err(|e| StoreError::Deserialization(e.to_string()))?;
            let start = arr.len().saturating_sub(count);
            return Ok(Some(serde_json::to_vec(&arr[start..])?));
        }

        // Reverse since we collected in reverse order
        items_collected.reverse();
        Ok(Some(serde_json::to_vec(&items_collected)?))
    }

    /// Legacy implementation for reference - walks entire chain
    #[allow(dead_code)]
    fn get_state_tail_full_reconstruct(&self, state_id: &str, count: usize) -> Result<Option<Vec<u8>>> {
        let branch_id = self.branches.current_branch().id;
        let head = match self.state.get_head(branch_id, state_id) {
            Some(h) => h,
            None => return Ok(None),
        };

        if count == 0 {
            return Ok(Some(serde_json::to_vec(&Vec::<serde_json::Value>::new())?));
        }

        // Collect all items in proper order by traversing the chain
        let mut all_items: Vec<serde_json::Value> = Vec::new();
        let mut current_offset = Some(head.head_offset);
        let mut hit_snapshot = false;

        // First pass: collect operations in reverse order
        let mut ops: Vec<StateOperation> = Vec::new();

        while let Some(offset) = current_offset {
            let record = self.log.read_at(offset)?;
            let update: StateUpdateRecord = serde_json::from_slice(&record.payload)
                .map_err(|e| StoreError::Deserialization(e.to_string()))?;

            match &update.operation {
                StateOperation::Snapshot(_) => {
                    ops.push(update.operation.clone());
                    break; // Full snapshot has everything
                }
                StateOperation::DeltaSnapshot(_) => {
                    ops.push(update.operation.clone());
                    hit_snapshot = true;
                }
                _ => {
                    if !hit_snapshot {
                        ops.push(update.operation.clone());
                    }
                }
            }

            current_offset = update.prev_update_offset;
        }

        // Apply operations in forward order (reverse of collection order)
        ops.reverse();
        for op in ops {
            match op {
                StateOperation::Append(item) => {
                    let value: serde_json::Value = serde_json::from_slice(&item)
                        .map_err(|e| StoreError::Deserialization(e.to_string()))?;
                    all_items.push(value);
                }
                StateOperation::Snapshot(data) | StateOperation::DeltaSnapshot(data) => {
                    let arr: Vec<serde_json::Value> = serde_json::from_slice(&data)
                        .map_err(|e| StoreError::Deserialization(e.to_string()))?;
                    all_items.extend(arr);
                }
                StateOperation::Redact { start, end } => {
                    let start = start.min(all_items.len());
                    let end = end.min(all_items.len());
                    if start < end {
                        all_items.drain(start..end);
                    }
                }
                StateOperation::Edit { index, new_value } => {
                    if index < all_items.len() {
                        let value: serde_json::Value = serde_json::from_slice(&new_value)
                            .map_err(|e| StoreError::Deserialization(e.to_string()))?;
                        all_items[index] = value;
                    }
                }
                _ => {}
            }
        }

        // Take the last `count` items
        let start = all_items.len().saturating_sub(count);
        let tail: Vec<_> = all_items[start..].to_vec();

        Ok(Some(serde_json::to_vec(&tail)?))
    }

    /// Iterate over items in an AppendLog state without loading all into memory.
    ///
    /// Returns an iterator that yields items one at a time.
    pub fn iter_state_items(
        &self,
        state_id: &str,
    ) -> Result<Option<StateItemIterator>> {
        let head = match self.state.get_head(self.branches.current_branch().id, state_id) {
            Some(h) => h,
            None => return Ok(None),
        };

        Ok(Some(StateItemIterator::new(
            self.log.clone(),
            head.head_offset,
        )))
    }

    /// Check if a state needs a snapshot.
    pub fn state_needs_snapshot(&self, state_id: &str) -> bool {
        self.state.needs_snapshot(self.branches.current_branch().id, state_id)
    }

    /// Get what type of snapshot is needed (if any).
    pub fn snapshot_needed(&self, state_id: &str) -> Option<crate::state::SnapshotNeeded> {
        self.state.snapshot_needed(self.branches.current_branch().id, state_id)
    }

    /// Get compaction statistics for a state.
    ///
    /// Returns information about how many operations could be compacted.
    pub fn get_compaction_stats(
        &self,
        state_id: &str,
    ) -> Option<crate::state::CompactionStats> {
        self.state.get_compaction_stats(self.branches.current_branch().id, state_id)
    }

    /// Get detailed chain statistics for a state.
    ///
    /// This traverses the entire chain to count operations and bytes.
    pub fn get_chain_stats(
        &self,
        state_id: &str,
    ) -> Result<Option<crate::state::ChainStats>> {
        self.state.count_chain_operations(self.branches.current_branch().id, state_id)
    }

    /// Compact a state by creating a full snapshot.
    ///
    /// This doesn't delete old records (append-only log), but the new snapshot
    /// allows reconstruction to skip all previous operations. Returns the
    /// snapshot record if created, or None if the state doesn't exist.
    ///
    /// For maximum compaction benefit, this creates a full snapshot regardless
    /// of the configured snapshot strategy.
    pub fn compact_state(&self, state_id: &str) -> Result<Option<Record>> {
        let current = match self.get_state(state_id)? {
            Some(s) => s,
            None => return Ok(None),
        };

        // Create a full snapshot with the current state
        let record = self.update_state(state_id, StateOperation::Snapshot(current))?;
        Ok(Some(record))
    }

    /// Compact all states by creating full snapshots.
    ///
    /// Returns the number of states compacted.
    pub fn compact_all_states(&self) -> Result<usize> {
        let state_ids = self.state.state_ids();
        let mut compacted = 0;

        for state_id in state_ids {
            if self.compact_state(&state_id)?.is_some() {
                compacted += 1;
            }
        }

        Ok(compacted)
    }

    /// Get compaction summary for all states.
    ///
    /// Returns total operations and bytes that could be skipped after compaction.
    pub fn get_compaction_summary(&self) -> Result<CompactionSummary> {
        let state_ids = self.state.state_ids();
        let mut total_operations = 0u64;
        let mut compactable_operations = 0u64;
        let mut total_bytes = 0u64;
        let mut compactable_bytes = 0u64;
        let mut states_with_history = 0usize;

        for state_id in state_ids {
            if let Some(stats) = self.get_chain_stats(&state_id)? {
                total_operations += stats.total_operations;
                total_bytes += stats.total_bytes;

                if stats.has_full_snapshot {
                    compactable_operations += stats.operations_before_snapshot;
                    compactable_bytes += stats.bytes_before_snapshot;
                } else if stats.total_operations > 1 {
                    // No snapshot yet, all but one operation is compactable
                    compactable_operations += stats.total_operations - 1;
                    states_with_history += 1;
                }
            }
        }

        Ok(CompactionSummary {
            total_operations,
            compactable_operations,
            total_bytes,
            compactable_bytes,
            states_needing_compaction: states_with_history,
        })
    }

    /// Create a snapshot if needed, returning the record if one was created.
    ///
    /// For AppendLog strategy:
    /// - Delta snapshots contain items added since the last delta/full snapshot
    /// - Full snapshots contain the entire state
    ///
    /// Note: This is now called automatically by `update_state()`. You only need
    /// to call this manually if you want to force a snapshot check at a specific time.
    pub fn create_snapshot_if_needed(&self, state_id: &str) -> Result<Option<Record>> {
        self.create_snapshot_if_needed_internal(state_id, false)
    }

    /// Internal snapshot creation that can skip auto-snapshot to avoid recursion.
    fn create_snapshot_if_needed_internal(
        &self,
        state_id: &str,
        skip_auto: bool,
    ) -> Result<Option<Record>> {
        use crate::state::SnapshotNeeded;

        match self.state.snapshot_needed(self.branches.current_branch().id, state_id) {
            None => Ok(None),
            Some(SnapshotNeeded::Full) => {
                let current = self.get_state(state_id)?.unwrap_or_default();
                let record = self.update_state_internal(
                    state_id,
                    StateOperation::Snapshot(current),
                    skip_auto,
                )?;
                Ok(Some(record))
            }
            Some(SnapshotNeeded::Delta) => {
                let delta_items = self.compute_delta_items(state_id)?;
                let record = self.update_state_internal(
                    state_id,
                    StateOperation::DeltaSnapshot(delta_items),
                    skip_auto,
                )?;
                Ok(Some(record))
            }
        }
    }

    /// Auto-snapshot helper called by update_state. Skips auto-snapshot on the
    /// snapshot operation itself to avoid infinite recursion.
    fn auto_snapshot_if_needed(&self, state_id: &str) -> Result<()> {
        self.create_snapshot_if_needed_internal(state_id, true)?;
        Ok(())
    }

    /// Compute items added since the last delta or full snapshot.
    ///
    /// This walks the chain collecting Append operations until hitting a snapshot.
    fn compute_delta_items(&self, state_id: &str) -> Result<Vec<u8>> {
        let head = match self.state.get_head(self.branches.current_branch().id, state_id) {
            Some(h) => h,
            None => return Ok(serde_json::to_vec(&Vec::<serde_json::Value>::new())?),
        };

        // Walk chain backwards, collecting append operations until snapshot
        let mut appended_items: Vec<serde_json::Value> = Vec::new();
        let mut current_offset = Some(head.head_offset);

        while let Some(offset) = current_offset {
            let record = self.log.read_at(offset)?;
            let update: StateUpdateRecord = serde_json::from_slice(&record.payload)
                .map_err(|e| StoreError::Deserialization(e.to_string()))?;

            match &update.operation {
                StateOperation::Append(item) => {
                    let value: serde_json::Value = serde_json::from_slice(item)
                        .map_err(|e| StoreError::Deserialization(e.to_string()))?;
                    appended_items.push(value);
                }
                StateOperation::Snapshot(_) | StateOperation::DeltaSnapshot(_) => {
                    // Hit a snapshot, stop collecting
                    break;
                }
                _ => {
                    // Other operations (Edit, Redact) - for now, fall back to full state
                    // This is a simplification; proper delta handling would track edits too
                }
            }

            current_offset = update.prev_update_offset;
        }

        // Reverse since we collected in reverse order
        appended_items.reverse();

        serde_json::to_vec(&appended_items).map_err(|e| StoreError::Serialization(e.to_string()))
    }

    // --- Branch Operations ---

    /// Create a new branch from the current branch head.
    pub fn create_branch(&self, name: &str, from: Option<&str>) -> Result<Branch> {
        let parent = if let Some(from_name) = from {
            self.branches.get_branch(from_name).ok_or_else(|| {
                StoreError::BranchNotFound(from_name.to_string())
            })?
        } else {
            self.branches.current_branch()
        };

        let parent_name = parent.name.clone();
        let new_branch = self.branches.create_branch(name, Some(&parent_name))?;

        // Copy state chain heads from parent to child
        self.state.copy_heads_for_branch(parent.id, new_branch.id);

        // Broadcast branch created
        self.subscriptions.broadcast_branch_created(&new_branch, Some(parent_name));

        Ok(new_branch)
    }

    /// Create a branch without copying state from parent.
    /// This is useful for creating branches with custom state (e.g., time-travel branching).
    pub fn create_empty_branch(&self, name: &str, from: Option<&str>) -> Result<Branch> {
        let parent_name = from.map(|n| n.to_string()).or_else(|| {
            Some(self.branches.current_branch().name.clone())
        });
        let new_branch = self.branches.create_branch(name, from)?;

        // Broadcast branch created
        self.subscriptions.broadcast_branch_created(&new_branch, parent_name);

        // Don't copy state - let the caller populate state manually
        Ok(new_branch)
    }

    /// Create a branch at a specific sequence (time-travel branching).
    ///
    /// This creates a new branch that starts with the state as it existed at the
    /// given sequence on the parent branch. The new branch's state heads point to
    /// the same underlying chain as the parent - no new records are written.
    ///
    /// # Arguments
    ///
    /// * `name` - Name for the new branch
    /// * `from` - Parent branch name to branch from
    /// * `at` - Sequence number on parent to branch at (must be <= parent's head)
    pub fn create_branch_at(&self, name: &str, from: &str, at: Sequence) -> Result<Branch> {
        let parent = self
            .branches
            .get_branch(from)
            .ok_or_else(|| StoreError::BranchNotFound(from.to_string()))?;

        let new_branch = self.branches.create_branch_at(name, from, at)?;

        // Point the new branch's state heads at the parent's chain at the branch point.
        // No new records are written - the branch shares the existing chain up to this point.
        for state_id in self.state.state_ids() {
            if let Some((head_offset, item_count)) =
                self.find_chain_info_at(parent.id, &state_id, at)?
            {
                self.state
                    .set_head_for_branch(new_branch.id, &state_id, head_offset, item_count);
            }
        }

        self.subscriptions
            .broadcast_branch_created(&new_branch, Some(from.to_string()));

        Ok(new_branch)
    }

    /// Switch to a different branch.
    pub fn switch_branch(&self, name: &str) -> Result<Branch> {
        self.branches.switch_branch(name)
    }

    /// Get the current branch.
    pub fn current_branch(&self) -> Branch {
        self.branches.current_branch()
    }

    /// List all branches.
    pub fn list_branches(&self) -> Vec<Branch> {
        self.branches.list_branches()
    }

    /// Delete a branch.
    pub fn delete_branch(&self, name: &str) -> Result<()> {
        self.branches.delete_branch(name)?;

        // Broadcast branch deleted
        self.subscriptions.broadcast_branch_deleted(name);

        Ok(())
    }

    // --- Store Operations ---

    /// Get store statistics.
    pub fn stats(&self) -> Result<StoreStats> {
        Ok(StoreStats {
            record_count: self.index.count() as u64,
            blob_count: self.blobs.list()?.len() as u64,
            branch_count: self.branches.branch_count() as u64,
            state_slot_count: self.state.state_count() as u64,
            total_size_bytes: self.log.size() + self.blobs.total_size()?,
            blob_size_bytes: self.blobs.total_size()?,
            index_size_bytes: 0, // Would need to track this
        })
    }

    /// Sync all data to disk.
    ///
    /// This is O(1) - only syncs the log file and small metadata files.
    /// The record index is not persisted; it's rebuilt from the log on startup.
    pub fn sync(&self) -> Result<()> {
        // Sync the append-only log (O(1) - just fsync)
        self.log.sync()?;
        // Sync small metadata files (O(states) and O(branches), typically tiny)
        self.state.save()?;
        self.branches.save()?;
        Ok(())
    }

    /// Get the store path.
    pub fn path(&self) -> &Path {
        &self.config.path
    }

    /// Get records that were caused by a given record (reverse lookup).
    ///
    /// Returns record IDs that have `record_id` in their `caused_by` field.
    pub fn get_effects(&self, record_id: RecordId) -> Vec<RecordId> {
        self.index.get_caused_by(record_id)
    }

    /// Get records that link to a given record (reverse lookup).
    ///
    /// Returns record IDs that have `record_id` in their `linked_to` field.
    pub fn get_links_to(&self, record_id: RecordId) -> Vec<RecordId> {
        self.index.get_linked_to(record_id)
    }

    // --- Subscription Operations ---

    /// Subscribe to store events.
    ///
    /// Returns a handle for receiving events. Call `catch_up_subscription` to
    /// replay historical data before going live.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let handle = store.subscribe(SubscriptionConfig {
    ///     filter: SubscriptionFilter::records(),
    ///     from_sequence: Some(Sequence(100)),
    ///     ..Default::default()
    /// });
    ///
    /// // Replay historical data
    /// store.catch_up_subscription(handle.id)?;
    ///
    /// // Now receive live events
    /// while let Ok(event) = handle.recv() {
    ///     match event {
    ///         StoreEvent::Record { record } => { /* ... */ }
    ///         StoreEvent::CaughtUp => { /* now live */ }
    ///         _ => {}
    ///     }
    /// }
    /// ```
    pub fn subscribe(&self, config: SubscriptionConfig) -> SubscriptionHandle {
        self.subscriptions.subscribe(config)
    }

    /// Unsubscribe and clean up.
    pub fn unsubscribe(&self, id: SubscriptionId) {
        self.subscriptions.unsubscribe(id)
    }

    /// Get the number of active subscriptions.
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.subscription_count()
    }

    /// Mark a subscription as caught up (for manual catch-up handling).
    pub fn mark_subscription_caught_up(&self, id: SubscriptionId) -> Result<()> {
        self.subscriptions.mark_caught_up(id)
    }

    /// Perform catch-up for a subscription.
    ///
    /// If `from_sequence` is set in the config, this replays:
    /// 1. State snapshots for subscribed states at the starting sequence
    /// 2. Historical records matching the filter from that sequence
    ///
    /// After catch-up completes, the subscription is marked as caught up
    /// and will receive live events.
    pub fn catch_up_subscription(&self, id: SubscriptionId) -> Result<()> {
        let config = self
            .subscriptions
            .get_config(id)
            .ok_or(StoreError::SubscriptionDropped)?;

        // If no from_sequence, just mark as caught up immediately
        let from_seq = match config.from_sequence {
            Some(seq) => seq,
            None => {
                return self.subscriptions.mark_caught_up(id);
            }
        };

        // Send state snapshots for subscribed states
        if config.filter.include_state_changes {
            let state_ids = match &config.filter.state_ids {
                Some(ids) => ids.clone(),
                None => self.state.state_ids(),
            };

            for state_id in state_ids {
                // Get state at the starting sequence, or current state if it didn't exist then
                let (state_data, snapshot_seq) = match self.get_state_at(&state_id, from_seq)? {
                    Some(data) => (data, from_seq),
                    None => {
                        // State didn't exist at from_seq, try current state
                        match self.get_state(&state_id)? {
                            Some(data) => (data, self.current_branch().head),
                            None => continue, // State doesn't exist at all
                        }
                    }
                };

                // Check size limits and truncate if needed
                let total_bytes = state_data.len();
                let (data, truncated, from_index, total_length) =
                    if total_bytes > config.max_snapshot_bytes {
                        // Try to parse as JSON array to provide partial data
                        if let Ok(serde_json::Value::Array(arr)) =
                            serde_json::from_slice::<serde_json::Value>(&state_data)
                        {
                            let total_len = arr.len();
                            // Send tail of the array that fits
                            let mut size = 0;
                            let mut start_idx = total_len;
                            for (i, item) in arr.iter().rev().enumerate() {
                                let item_size = serde_json::to_string(item)
                                    .map(|s| s.len())
                                    .unwrap_or(0);
                                if size + item_size > config.max_snapshot_bytes {
                                    break;
                                }
                                size += item_size;
                                start_idx = total_len - 1 - i;
                            }
                            let truncated_arr: Vec<_> = arr[start_idx..].to_vec();
                            (
                                serde_json::Value::Array(truncated_arr),
                                true,
                                Some(start_idx),
                                Some(total_len),
                            )
                        } else {
                            // Non-array, just send null and mark truncated
                            (serde_json::Value::Null, true, None, None)
                        }
                    } else {
                        let json_data: serde_json::Value =
                            serde_json::from_slice(&state_data).unwrap_or(serde_json::Value::Null);
                        (json_data, false, None, None)
                    };

                let event = crate::subscriptions::StoreEvent::StateSnapshot {
                    state_id,
                    data,
                    // Use the sequence where we got the snapshot from
                    sequence: snapshot_seq,
                    truncated,
                    total_bytes,
                    from_index,
                    total_length,
                };

                if !self.subscriptions.send_to(id, event) {
                    return Err(StoreError::SubscriptionDropped);
                }
            }
        }

        // Replay historical records
        if config.filter.include_records {
            let current_branch = self.branches.current_branch();
            let payload_threshold = 4096; // Same as manager default

            for result in self.iter_from(from_seq) {
                let (_offset, record) = result?;

                // Skip records not on current branch
                if record.branch != current_branch.id {
                    continue;
                }

                // Apply record type filter
                if let Some(ref types) = config.filter.record_types {
                    if !types.contains(&record.record_type) {
                        continue;
                    }
                }

                let summary =
                    crate::subscriptions::RecordSummary::from_record(&record, payload_threshold);
                let event = crate::subscriptions::StoreEvent::Record { record: summary };

                if !self.subscriptions.send_to(id, event) {
                    return Err(StoreError::SubscriptionDropped);
                }
            }
        }

        // Mark as caught up
        self.subscriptions.mark_caught_up(id)
    }

    // --- Private Helpers ---

    fn write_manifest(path: &Path) -> Result<()> {
        use std::io::Write;

        let manifest_path = path.join("MANIFEST");
        let mut file = File::create(manifest_path)?;

        file.write_all(STORE_MAGIC)?;
        file.write_all(&[STORE_VERSION])?;
        file.sync_all()?;

        Ok(())
    }

    fn verify_manifest(path: &Path) -> Result<()> {
        use std::io::Read;

        let manifest_path = path.join("MANIFEST");
        let mut file = File::open(manifest_path)?;

        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        if &magic != STORE_MAGIC {
            return Err(StoreError::InvalidFormat("Invalid store magic".into()));
        }

        let mut version = [0u8; 1];
        file.read_exact(&mut version)?;
        if version[0] != STORE_VERSION {
            return Err(StoreError::InvalidFormat(format!(
                "Unsupported store version: {}",
                version[0]
            )));
        }

        Ok(())
    }

    fn acquire_lock(path: &Path) -> Result<File> {
        let lock_path = path.join("LOCK");
        let lock_file = File::create(lock_path)?;

        lock_file
            .try_lock_exclusive()
            .map_err(|_| StoreError::Locked)?;

        Ok(lock_file)
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        // Best-effort sync on drop
        let _ = self.sync();
    }
}

/// Iterator over items in an AppendLog state.
///
/// This reconstructs items lazily, yielding them one at a time.
/// Items are yielded in order (oldest first).
pub struct StateItemIterator {
    log: Arc<RecordLog>,
    /// Buffer of items to yield
    items_buffer: Vec<serde_json::Value>,
    /// Current index in items_buffer
    current_index: usize,
    /// Head offset to start from
    head_offset: u64,
    /// Whether we've finished preparing
    prepared: bool,
}

impl StateItemIterator {
    fn new(log: Arc<RecordLog>, head_offset: u64) -> Self {
        Self {
            log,
            items_buffer: Vec::new(),
            current_index: 0,
            head_offset,
            prepared: false,
        }
    }

    /// Prepare by collecting and ordering all items.
    fn prepare(&mut self) -> Result<()> {
        if self.prepared {
            return Ok(());
        }

        // Two-pass approach: collect operations, reverse, apply
        let mut ops: Vec<StateOperation> = Vec::new();
        let mut current_offset = Some(self.head_offset);
        let mut hit_snapshot = false;

        // Pass 1: Collect operations in reverse order
        while let Some(offset) = current_offset {
            let record = self.log.read_at(offset)?;
            let update: StateUpdateRecord = serde_json::from_slice(&record.payload)
                .map_err(|e| StoreError::Deserialization(e.to_string()))?;

            match &update.operation {
                StateOperation::Snapshot(_) => {
                    ops.push(update.operation.clone());
                    break; // Full snapshot has everything
                }
                StateOperation::DeltaSnapshot(_) => {
                    ops.push(update.operation.clone());
                    hit_snapshot = true;
                }
                _ => {
                    if !hit_snapshot {
                        ops.push(update.operation.clone());
                    }
                }
            }

            current_offset = update.prev_update_offset;
        }

        // Pass 2: Apply operations in forward order
        ops.reverse();
        for op in ops {
            match op {
                StateOperation::Append(item) => {
                    let value: serde_json::Value = serde_json::from_slice(&item)
                        .map_err(|e| StoreError::Deserialization(e.to_string()))?;
                    self.items_buffer.push(value);
                }
                StateOperation::Snapshot(data) | StateOperation::DeltaSnapshot(data) => {
                    let arr: Vec<serde_json::Value> = serde_json::from_slice(&data)
                        .map_err(|e| StoreError::Deserialization(e.to_string()))?;
                    self.items_buffer.extend(arr);
                }
                StateOperation::Redact { start, end } => {
                    let start = start.min(self.items_buffer.len());
                    let end = end.min(self.items_buffer.len());
                    if start < end {
                        self.items_buffer.drain(start..end);
                    }
                }
                StateOperation::Edit { index, new_value } => {
                    if index < self.items_buffer.len() {
                        let value: serde_json::Value = serde_json::from_slice(&new_value)
                            .map_err(|e| StoreError::Deserialization(e.to_string()))?;
                        self.items_buffer[index] = value;
                    }
                }
                _ => {}
            }
        }

        self.prepared = true;
        Ok(())
    }
}

impl Iterator for StateItemIterator {
    type Item = Result<serde_json::Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.prepared {
            if let Err(e) = self.prepare() {
                return Some(Err(e));
            }
        }

        if self.current_index >= self.items_buffer.len() {
            None
        } else {
            let item = self.items_buffer[self.current_index].clone();
            self.current_index += 1;
            Some(Ok(item))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    fn test_config(dir: &TempDir) -> StoreConfig {
        StoreConfig {
            path: dir.path().join("store"),
            blob_cache_size: 100,
            create_if_missing: true,
        }
    }

    #[test]
    fn test_create_store() {
        let dir = TempDir::new().unwrap();
        let store = Store::create(test_config(&dir)).unwrap();

        assert!(store.path().join("MANIFEST").exists());
        assert!(store.path().join("records.log").exists());
    }

    #[test]
    fn test_append_record() {
        let dir = TempDir::new().unwrap();
        let store = Store::create(test_config(&dir)).unwrap();

        let input = RecordInput::json("message", &json!({"text": "Hello"})).unwrap();
        let record = store.append(input).unwrap();

        assert_eq!(record.sequence, Sequence(1));
        assert_eq!(record.record_type, "message");
    }

    #[test]
    fn test_get_record() {
        let dir = TempDir::new().unwrap();
        let store = Store::create(test_config(&dir)).unwrap();

        let input = RecordInput::json("message", &json!({"text": "Hello"})).unwrap();
        let record = store.append(input).unwrap();

        let retrieved = store.get_record(record.id).unwrap().unwrap();
        assert_eq!(retrieved.id, record.id);
        assert_eq!(retrieved.payload, record.payload);
    }

    #[test]
    fn test_store_blob() {
        let dir = TempDir::new().unwrap();
        let store = Store::create(test_config(&dir)).unwrap();

        let content = b"function hello() { return 'world'; }";
        let hash = store.store_blob(content, "application/javascript").unwrap();

        let blob = store.get_blob(&hash).unwrap().unwrap();
        assert_eq!(blob.content, content);
        assert_eq!(blob.content_type, "application/javascript");
    }

    #[test]
    fn test_state_operations() {
        let dir = TempDir::new().unwrap();
        let store = Store::create(test_config(&dir)).unwrap();

        // Register state
        store.register_state(StateRegistration {
            id: "counter".to_string(),
            strategy: crate::types::StateStrategy::Snapshot,
            initial_value: None,
        }).unwrap();

        // Update state
        store.update_state("counter", StateOperation::Set(b"42".to_vec())).unwrap();

        // Get state
        let value = store.get_state("counter").unwrap().unwrap();
        assert_eq!(value, b"42");
    }

    #[test]
    fn test_branch_operations() {
        let dir = TempDir::new().unwrap();
        let store = Store::create(test_config(&dir)).unwrap();

        // Append to main
        let input = RecordInput::json("message", &json!({"text": "main"})).unwrap();
        store.append(input).unwrap();

        // Create branch
        store.create_branch("feature", None).unwrap();
        store.switch_branch("feature").unwrap();

        // Append to feature
        let input = RecordInput::json("message", &json!({"text": "feature"})).unwrap();
        let record = store.append(input).unwrap();

        assert_eq!(record.sequence, Sequence(2));

        // Check branch list
        let branches = store.list_branches();
        assert_eq!(branches.len(), 2);
    }

    #[test]
    fn test_persistence() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir);

        // Create and write
        {
            let store = Store::create(config.clone()).unwrap();

            let input = RecordInput::json("message", &json!({"text": "Hello"})).unwrap();
            store.append(input).unwrap();

            store.store_blob(b"content", "text/plain").unwrap();

            store.register_state(StateRegistration {
                id: "test".to_string(),
                strategy: crate::types::StateStrategy::Snapshot,
                initial_value: None,
            }).unwrap();

            store.update_state("test", StateOperation::Set(b"value".to_vec())).unwrap();

            store.create_branch("feature", None).unwrap();

            store.sync().unwrap();
        }

        // Reopen and verify
        {
            let store = Store::open(config).unwrap();

            assert_eq!(store.index.count(), 2); // message + state_update

            let branches = store.list_branches();
            assert_eq!(branches.len(), 2);

            let state = store.get_state("test").unwrap().unwrap();
            assert_eq!(state, b"value");
        }
    }

    #[test]
    fn test_store_lock() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir);

        let _store1 = Store::create(config.clone()).unwrap();

        // Second store should fail to acquire lock
        let result = Store::open(config);
        assert!(matches!(result, Err(StoreError::Locked)));
    }

    #[test]
    fn test_stats() {
        let dir = TempDir::new().unwrap();
        let store = Store::create(test_config(&dir)).unwrap();

        let input = RecordInput::json("message", &json!({"text": "Hello"})).unwrap();
        store.append(input).unwrap();

        store.store_blob(b"content", "text/plain").unwrap();

        let stats = store.stats().unwrap();
        assert_eq!(stats.record_count, 1);
        assert_eq!(stats.blob_count, 1);
        assert_eq!(stats.branch_count, 1);
    }

    #[test]
    fn test_get_state_len() {
        let dir = TempDir::new().unwrap();
        let store = Store::create(test_config(&dir)).unwrap();

        // Nonexistent state
        assert_eq!(store.get_state_len("messages").unwrap(), None);

        // Register state
        store.register_state(StateRegistration {
            id: "messages".to_string(),
            strategy: crate::types::StateStrategy::AppendLog {
                delta_snapshot_every: 100,
                full_snapshot_every: 10,
            },
            initial_value: None,
        }).unwrap();

        // Empty state
        assert_eq!(store.get_state_len("messages").unwrap(), None);

        // Add items
        for i in 1..=5 {
            store.update_state("messages", StateOperation::Append(
                serde_json::to_vec(&json!({"id": i})).unwrap()
            )).unwrap();
        }

        assert_eq!(store.get_state_len("messages").unwrap(), Some(5));
    }

    #[test]
    fn test_get_state_slice() {
        let dir = TempDir::new().unwrap();
        let store = Store::create(test_config(&dir)).unwrap();

        store.register_state(StateRegistration {
            id: "items".to_string(),
            strategy: crate::types::StateStrategy::AppendLog {
                delta_snapshot_every: 100,
                full_snapshot_every: 10,
            },
            initial_value: None,
        }).unwrap();

        // Add items
        for i in 1..=10 {
            store.update_state("items", StateOperation::Append(
                serde_json::to_vec(&i).unwrap()
            )).unwrap();
        }

        // Get middle slice
        let slice = store.get_state_slice("items", 3, 4).unwrap().unwrap();
        let arr: Vec<i32> = serde_json::from_slice(&slice).unwrap();
        assert_eq!(arr, vec![4, 5, 6, 7]);

        // Get first slice
        let slice = store.get_state_slice("items", 0, 3).unwrap().unwrap();
        let arr: Vec<i32> = serde_json::from_slice(&slice).unwrap();
        assert_eq!(arr, vec![1, 2, 3]);

        // Get end slice
        let slice = store.get_state_slice("items", 8, 10).unwrap().unwrap();
        let arr: Vec<i32> = serde_json::from_slice(&slice).unwrap();
        assert_eq!(arr, vec![9, 10]);

        // Out of bounds
        let slice = store.get_state_slice("items", 15, 5).unwrap().unwrap();
        let arr: Vec<i32> = serde_json::from_slice(&slice).unwrap();
        assert_eq!(arr, Vec::<i32>::new());
    }

    #[test]
    fn test_get_state_tail() {
        let dir = TempDir::new().unwrap();
        let store = Store::create(test_config(&dir)).unwrap();

        store.register_state(StateRegistration {
            id: "items".to_string(),
            strategy: crate::types::StateStrategy::AppendLog {
                delta_snapshot_every: 100,
                full_snapshot_every: 10,
            },
            initial_value: None,
        }).unwrap();

        // Add items
        for i in 1..=10 {
            store.update_state("items", StateOperation::Append(
                serde_json::to_vec(&i).unwrap()
            )).unwrap();
        }

        // Get last 3
        let tail = store.get_state_tail("items", 3).unwrap().unwrap();
        let arr: Vec<i32> = serde_json::from_slice(&tail).unwrap();
        assert_eq!(arr, vec![8, 9, 10]);

        // Get last 1
        let tail = store.get_state_tail("items", 1).unwrap().unwrap();
        let arr: Vec<i32> = serde_json::from_slice(&tail).unwrap();
        assert_eq!(arr, vec![10]);

        // Get more than exists
        let tail = store.get_state_tail("items", 20).unwrap().unwrap();
        let arr: Vec<i32> = serde_json::from_slice(&tail).unwrap();
        assert_eq!(arr, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

        // Get zero
        let tail = store.get_state_tail("items", 0).unwrap().unwrap();
        let arr: Vec<i32> = serde_json::from_slice(&tail).unwrap();
        assert_eq!(arr, Vec::<i32>::new());
    }

    #[test]
    fn test_iter_state_items() {
        let dir = TempDir::new().unwrap();
        let store = Store::create(test_config(&dir)).unwrap();

        store.register_state(StateRegistration {
            id: "items".to_string(),
            strategy: crate::types::StateStrategy::AppendLog {
                delta_snapshot_every: 100,
                full_snapshot_every: 10,
            },
            initial_value: None,
        }).unwrap();

        // Add items
        for i in 1..=5 {
            store.update_state("items", StateOperation::Append(
                serde_json::to_vec(&i).unwrap()
            )).unwrap();
        }

        // Iterate
        let iter = store.iter_state_items("items").unwrap().unwrap();
        let collected: Vec<i32> = iter
            .map(|r| serde_json::from_value(r.unwrap()).unwrap())
            .collect();

        assert_eq!(collected, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_lazy_loading_with_snapshots() {
        let dir = TempDir::new().unwrap();
        let store = Store::create(test_config(&dir)).unwrap();

        store.register_state(StateRegistration {
            id: "items".to_string(),
            strategy: crate::types::StateStrategy::AppendLog {
                delta_snapshot_every: 3,
                full_snapshot_every: 2,
            },
            initial_value: None,
        }).unwrap();

        // Add items - this will trigger snapshots
        for i in 1..=10 {
            store.update_state("items", StateOperation::Append(
                serde_json::to_vec(&i).unwrap()
            )).unwrap();
            // Create snapshot if needed
            store.create_snapshot_if_needed("items").unwrap();
        }

        // Verify tail still works with snapshots
        let tail = store.get_state_tail("items", 3).unwrap().unwrap();
        let arr: Vec<i32> = serde_json::from_slice(&tail).unwrap();
        assert_eq!(arr, vec![8, 9, 10]);

        // Verify iteration still works
        let iter = store.iter_state_items("items").unwrap().unwrap();
        let collected: Vec<i32> = iter
            .map(|r| serde_json::from_value(r.unwrap()).unwrap())
            .collect();
        assert_eq!(collected, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }

    #[test]
    fn test_compact_state() {
        let dir = TempDir::new().unwrap();
        let store = Store::create(test_config(&dir)).unwrap();

        store.register_state(StateRegistration {
            id: "items".to_string(),
            strategy: crate::types::StateStrategy::AppendLog {
                delta_snapshot_every: 100,
                full_snapshot_every: 100,
            },
            initial_value: None,
        }).unwrap();

        // Add items without auto-snapshot
        for i in 1..=10 {
            store.update_state("items", StateOperation::Append(
                serde_json::to_vec(&i).unwrap()
            )).unwrap();
        }

        // Check chain stats before compaction
        let stats_before = store.get_chain_stats("items").unwrap().unwrap();
        assert_eq!(stats_before.total_operations, 10);
        assert!(!stats_before.has_full_snapshot);

        // Compact
        let record = store.compact_state("items").unwrap().unwrap();
        assert_eq!(record.record_type, "state_update");

        // Check chain stats after compaction
        let stats_after = store.get_chain_stats("items").unwrap().unwrap();
        assert_eq!(stats_after.total_operations, 11); // 10 appends + 1 snapshot
        assert!(stats_after.has_full_snapshot);
        assert_eq!(stats_after.operations_before_snapshot, 10); // All old ops before snapshot

        // State should still be correct
        let state = store.get_state("items").unwrap().unwrap();
        let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
        assert_eq!(arr, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }

    #[test]
    fn test_compaction_summary() {
        let dir = TempDir::new().unwrap();
        let store = Store::create(test_config(&dir)).unwrap();

        // Create two states
        store.register_state(StateRegistration {
            id: "state1".to_string(),
            strategy: crate::types::StateStrategy::AppendLog {
                delta_snapshot_every: 100,
                full_snapshot_every: 100,
            },
            initial_value: None,
        }).unwrap();

        store.register_state(StateRegistration {
            id: "state2".to_string(),
            strategy: crate::types::StateStrategy::Snapshot,
            initial_value: None,
        }).unwrap();

        // Add operations
        for i in 1..=5 {
            store.update_state("state1", StateOperation::Append(
                serde_json::to_vec(&i).unwrap()
            )).unwrap();
        }

        store.update_state("state2", StateOperation::Set(b"\"v1\"".to_vec())).unwrap();
        store.update_state("state2", StateOperation::Set(b"\"v2\"".to_vec())).unwrap();

        // Check summary
        let summary = store.get_compaction_summary().unwrap();
        assert_eq!(summary.total_operations, 7); // 5 appends + 2 sets
        assert!(summary.compactable_operations > 0);

        // Compact all
        let compacted = store.compact_all_states().unwrap();
        assert_eq!(compacted, 2);

        // State values should be preserved
        let state1 = store.get_state("state1").unwrap().unwrap();
        let arr: Vec<i32> = serde_json::from_slice(&state1).unwrap();
        assert_eq!(arr, vec![1, 2, 3, 4, 5]);

        let state2 = store.get_state("state2").unwrap().unwrap();
        assert_eq!(state2, b"\"v2\"");
    }

    #[test]
    fn test_get_compaction_stats() {
        let dir = TempDir::new().unwrap();
        let store = Store::create(test_config(&dir)).unwrap();

        store.register_state(StateRegistration {
            id: "items".to_string(),
            strategy: crate::types::StateStrategy::AppendLog {
                delta_snapshot_every: 3,
                full_snapshot_every: 2,
            },
            initial_value: None,
        }).unwrap();

        // No stats for state with no updates
        assert!(store.get_compaction_stats("items").is_none());

        // Add some operations - auto-snapshot kicks in at 3
        for i in 1..=5 {
            store.update_state("items", StateOperation::Append(
                serde_json::to_vec(&i).unwrap()
            )).unwrap();
        }

        // After 5 appends with delta_snapshot_every=3:
        // - Appends 1,2,3 -> delta snapshot auto-created (ops=0, deltas=1)
        // - Appends 4,5 -> ops=2
        // So: ops_since_last_full = 2 ops + 1 delta = 3
        let stats = store.get_compaction_stats("items").unwrap();
        assert_eq!(stats.ops_since_last_full_snapshot, 3); // 2 ops + 1 delta
        assert_eq!(stats.delta_snapshots_since_full, 1);
        assert!(stats.last_full_snapshot_offset.is_none());
        assert!(stats.last_delta_snapshot_offset.is_some());
    }

    #[test]
    fn test_subscription_catch_up_records() {
        use crate::subscriptions::{SubscriptionConfig, SubscriptionFilter, StoreEvent};
        use std::time::Duration;

        let dir = TempDir::new().unwrap();
        let store = Store::create(test_config(&dir)).unwrap();

        // Create some records first
        for i in 1..=5 {
            store
                .append(RecordInput::json("message", &serde_json::json!({ "i": i })).unwrap())
                .unwrap();
        }

        // Subscribe from sequence 3
        let config = SubscriptionConfig {
            filter: SubscriptionFilter::records(),
            from_sequence: Some(Sequence(3)),
            ..Default::default()
        };
        let handle = store.subscribe(config);

        // Perform catch-up
        store.catch_up_subscription(handle.id).unwrap();

        // Should receive records 3, 4, 5 (sequences >= 3)
        let mut received = Vec::new();
        while let Ok(event) = handle.recv_timeout(Duration::from_millis(50)) {
            match event {
                StoreEvent::Record { record } => {
                    received.push(record.sequence.0);
                }
                StoreEvent::CaughtUp => break,
                _ => {}
            }
        }

        assert_eq!(received, vec![3, 4, 5]);
    }

    #[test]
    fn test_subscription_catch_up_state() {
        use crate::subscriptions::{SubscriptionConfig, SubscriptionFilter, StoreEvent};
        use std::time::Duration;

        let dir = TempDir::new().unwrap();
        let store = Store::create(test_config(&dir)).unwrap();

        // Register and populate state
        store
            .register_state(StateRegistration {
                id: "counter".to_string(),
                strategy: crate::types::StateStrategy::Snapshot,
                initial_value: None,
            })
            .unwrap();

        // Update state a few times
        store
            .update_state("counter", StateOperation::Set(b"10".to_vec()))
            .unwrap();
        let seq2 = store
            .update_state("counter", StateOperation::Set(b"20".to_vec()))
            .unwrap();
        store
            .update_state("counter", StateOperation::Set(b"30".to_vec()))
            .unwrap();

        // Subscribe to state from seq2
        let config = SubscriptionConfig {
            filter: SubscriptionFilter::states(vec!["counter".to_string()]),
            from_sequence: Some(seq2.sequence),
            ..Default::default()
        };
        let handle = store.subscribe(config);

        // Perform catch-up
        store.catch_up_subscription(handle.id).unwrap();

        // Should receive state snapshot at seq2 (value 20)
        let event = handle.recv_timeout(Duration::from_millis(100)).unwrap();
        match event {
            StoreEvent::StateSnapshot { state_id, data, .. } => {
                assert_eq!(state_id, "counter");
                assert_eq!(data, serde_json::json!(20));
            }
            _ => panic!("Expected StateSnapshot, got {:?}", event),
        }

        // Then CaughtUp
        let event = handle.recv_timeout(Duration::from_millis(100)).unwrap();
        assert!(matches!(event, StoreEvent::CaughtUp));
    }

    #[test]
    fn test_subscription_catch_up_no_from_sequence() {
        use crate::subscriptions::{SubscriptionConfig, SubscriptionFilter, StoreEvent};
        use std::time::Duration;

        let dir = TempDir::new().unwrap();
        let store = Store::create(test_config(&dir)).unwrap();

        // Create some records
        for i in 1..=3 {
            store
                .append(RecordInput::json("message", &serde_json::json!({ "i": i })).unwrap())
                .unwrap();
        }

        // Subscribe without from_sequence (live only)
        let config = SubscriptionConfig {
            filter: SubscriptionFilter::records(),
            from_sequence: None,
            ..Default::default()
        };
        let handle = store.subscribe(config);

        // Perform catch-up (should immediately mark as caught up)
        store.catch_up_subscription(handle.id).unwrap();

        // Should receive CaughtUp immediately, no historical records
        let event = handle.recv_timeout(Duration::from_millis(100)).unwrap();
        assert!(matches!(event, StoreEvent::CaughtUp));

        // No more events
        let result = handle.recv_timeout(Duration::from_millis(50));
        assert!(result.is_err());
    }
}
