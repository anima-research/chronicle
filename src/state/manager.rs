//! State manager with disk-based chain traversal and LRU cache.
//!
//! Chain traversal uses file offsets stored in each update record,
//! eliminating the need to keep all updates in memory. An LRU cache
//! stores recently reconstructed states for fast repeated access.

use crate::error::{Result, StoreError};
use crate::records::RecordLog;
use crate::state::operations::apply_operation;
use crate::types::{BranchId, StateOperation, StateRegistration, StateStrategy, StateUpdateRecord};
use lru::LruCache;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Magic bytes for state index file.
const STATE_INDEX_MAGIC: &[u8; 4] = b"STI\0";

/// Current state index format version.
const STATE_INDEX_VERSION: u8 = 2; // Bumped for new format

/// Default cache size (number of states).
const DEFAULT_CACHE_SIZE: usize = 1000;

/// What type of snapshot is needed for a state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotNeeded {
    /// A delta snapshot (stores items since last delta/full snapshot).
    Delta,
    /// A full snapshot (stores entire state).
    Full,
}

/// Statistics about compaction potential for a state.
#[derive(Clone, Debug)]
pub struct CompactionStats {
    /// Operations since the last full snapshot.
    pub ops_since_last_full_snapshot: u64,
    /// Offset of the last full snapshot (if any).
    pub last_full_snapshot_offset: Option<u64>,
    /// Offset of the last delta snapshot (if any).
    pub last_delta_snapshot_offset: Option<u64>,
    /// Number of delta snapshots since the last full snapshot.
    pub delta_snapshots_since_full: u64,
}

/// Detailed chain statistics for compaction analysis.
#[derive(Clone, Debug)]
pub struct ChainStats {
    /// Total number of operations in the chain.
    pub total_operations: u64,
    /// Operations that are before the most recent full snapshot.
    pub operations_before_snapshot: u64,
    /// Total bytes used by all operations.
    pub total_bytes: u64,
    /// Bytes used by operations before the most recent full snapshot.
    pub bytes_before_snapshot: u64,
    /// Whether the chain has at least one full snapshot.
    pub has_full_snapshot: bool,
}

/// Tracks the chain head for a single state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StateChainHead {
    /// File offset of the most recent update record.
    pub head_offset: u64,

    /// Number of operations since last delta snapshot (for AppendLog).
    pub ops_since_delta_snapshot: u64,

    /// Number of delta snapshots since last full snapshot.
    pub delta_snapshots_since_full: u64,

    /// File offset of most recent delta snapshot.
    pub last_delta_snapshot_offset: Option<u64>,

    /// File offset of most recent full snapshot.
    pub last_full_snapshot_offset: Option<u64>,

    /// Whether there are non-Append operations (Edit, Redact) since the last snapshot.
    /// When true, a full snapshot should be created instead of a delta snapshot,
    /// because delta snapshots can only track Append operations.
    #[serde(default)]
    pub has_non_append_since_snapshot: bool,

    /// Current number of items in the state (for O(1) length queries).
    /// Updated on each Append (+1), Redact (-(end-start)), Snapshot (=snapshot.len).
    #[serde(default)]
    pub item_count: usize,
}

/// In-memory state index (small - just heads and strategies).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StateIndex {
    /// Chain heads per (branch_id, state_id).
    pub heads: HashMap<(BranchId, String), StateChainHead>,

    /// Registered state strategies.
    pub strategies: HashMap<String, StateStrategy>,
}

/// Cached state value.
#[derive(Clone)]
struct CachedState {
    value: Vec<u8>,
    head_offset: u64, // To detect staleness
}

/// State manager handles per-state chains and reconstruction.
///
/// Uses disk-based chain traversal (no in-memory update storage)
/// with an LRU cache for frequently accessed states.
pub struct StateManager {
    /// Path to state index file.
    path: PathBuf,

    /// In-memory index (just heads + strategies, very small).
    index: RwLock<StateIndex>,

    /// LRU cache for reconstructed states.
    cache: RwLock<LruCache<String, CachedState>>,

    /// Reference to record log for disk-based chain traversal.
    log: Option<Arc<RecordLog>>,
}

impl StateManager {
    /// Create a new state manager.
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        Self::with_cache_size(path, DEFAULT_CACHE_SIZE)
    }

    /// Create a new state manager with custom cache size.
    pub fn with_cache_size(path: impl AsRef<Path>, cache_size: usize) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let cache_size = NonZeroUsize::new(cache_size.max(1)).unwrap();

        Ok(Self {
            path,
            index: RwLock::new(StateIndex::default()),
            cache: RwLock::new(LruCache::new(cache_size)),
            log: None,
        })
    }

    /// Set the record log reference for disk-based traversal.
    pub fn set_log(&mut self, log: Arc<RecordLog>) {
        self.log = Some(log);
    }

    /// Load state manager from file.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        Self::load_with_cache_size(path, DEFAULT_CACHE_SIZE)
    }

    /// Load state manager from file with custom cache size.
    pub fn load_with_cache_size(path: impl AsRef<Path>, cache_size: usize) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let cache_size = NonZeroUsize::new(cache_size.max(1)).unwrap();

        let manager = Self {
            path: path.clone(),
            index: RwLock::new(StateIndex::default()),
            cache: RwLock::new(LruCache::new(cache_size)),
            log: None,
        };

        if path.exists() {
            manager.load_from_file()?;
        }

        Ok(manager)
    }

    /// Register a new state with its strategy.
    pub fn register_state(&self, registration: StateRegistration) -> Result<()> {
        let mut index = self.index.write();

        if index.strategies.contains_key(&registration.id) {
            return Err(StoreError::StateExists(registration.id));
        }

        index
            .strategies
            .insert(registration.id.clone(), registration.strategy);

        Ok(())
    }

    /// Record a state update (called when a state update record is appended).
    ///
    /// This only updates the chain head metadata - the actual update is in the log.
    pub fn record_update(
        &self,
        branch_id: BranchId,
        state_id: &str,
        offset: u64,
        operation: &StateOperation,
    ) -> Result<()> {
        let mut index = self.index.write();

        let key = (branch_id, state_id.to_string());
        let head = index.heads.entry(key.clone()).or_insert_with(|| {
            StateChainHead {
                head_offset: offset,
                ops_since_delta_snapshot: 0,
                delta_snapshots_since_full: 0,
                last_delta_snapshot_offset: None,
                last_full_snapshot_offset: None,
                has_non_append_since_snapshot: false,
                item_count: 0,
            }
        });

        head.head_offset = offset;

        match operation {
            StateOperation::Snapshot(data) => {
                // Full snapshot resets everything
                head.ops_since_delta_snapshot = 0;
                head.delta_snapshots_since_full = 0;
                head.last_full_snapshot_offset = Some(offset);
                head.last_delta_snapshot_offset = None; // Full snapshot supersedes deltas
                head.has_non_append_since_snapshot = false; // Reset the flag
                // Update item count from snapshot
                if let Ok(arr) = serde_json::from_slice::<Vec<serde_json::Value>>(data) {
                    head.item_count = arr.len();
                }
            }
            StateOperation::DeltaSnapshot(_) => {
                // Delta snapshot resets op counter, increments delta counter
                // Note: Delta doesn't change item_count - it consolidates existing items
                head.ops_since_delta_snapshot = 0;
                head.delta_snapshots_since_full += 1;
                head.last_delta_snapshot_offset = Some(offset);
            }
            StateOperation::Append(_) => {
                head.ops_since_delta_snapshot += 1;
                head.item_count += 1;
            }
            StateOperation::Redact { start, end } => {
                head.ops_since_delta_snapshot += 1;
                head.has_non_append_since_snapshot = true;
                // Redact removes items (clamped to valid range)
                let remove_count = end.saturating_sub(*start).min(head.item_count);
                head.item_count = head.item_count.saturating_sub(remove_count);
            }
            StateOperation::Edit { .. } => {
                head.ops_since_delta_snapshot += 1;
                head.has_non_append_since_snapshot = true;
                // Edit doesn't change count
            }
            StateOperation::Set(_) => {
                head.ops_since_delta_snapshot += 1;
                // Set replaces entire state - can't track count without parsing
            }
            StateOperation::Delta { .. } | StateOperation::Field { .. } => {
                head.ops_since_delta_snapshot += 1;
                // Delta/Field operations for Struct type - don't change count
            }
        }

        // Invalidate cache for this state (need to invalidate for all branches)
        // Use a cache key that includes branch
        let cache_key = format!("{}:{}", branch_id.0, state_id);
        self.cache.write().pop(&cache_key);

        Ok(())
    }

    /// Get the current value of a state.
    ///
    /// Uses LRU cache for fast repeated access. On cache miss,
    /// reconstructs state by traversing the chain from disk.
    pub fn get_state(&self, branch_id: BranchId, state_id: &str) -> Result<Option<Vec<u8>>> {
        let index = self.index.read();

        let key = (branch_id, state_id.to_string());
        let head = match index.heads.get(&key) {
            Some(h) => h.clone(),
            None => return Ok(None),
        };
        drop(index);

        let cache_key = format!("{}:{}", branch_id.0, state_id);

        // Check cache
        {
            let mut cache = self.cache.write();
            if let Some(cached) = cache.get(&cache_key) {
                if cached.head_offset == head.head_offset {
                    return Ok(Some(cached.value.clone()));
                }
                // Stale cache entry, will reconstruct
            }
        }

        // Cache miss - reconstruct from disk
        let log = self
            .log
            .as_ref()
            .ok_or_else(|| StoreError::NotInitialized)?;

        let value = self.reconstruct_from_disk(log, head.head_offset)?;

        // Cache the result
        {
            let mut cache = self.cache.write();
            cache.put(
                cache_key,
                CachedState {
                    value: value.clone(),
                    head_offset: head.head_offset,
                },
            );
        }

        Ok(Some(value))
    }

    /// Reconstruct state by traversing chain from disk.
    ///
    /// For AppendLog with incremental snapshots:
    /// - Full Snapshot: Stop traversal completely, use as base
    /// - DeltaSnapshot: Consolidates ops before it; continue to find more deltas/full snapshot
    /// - Regular ops: Collect only if before any snapshot
    ///
    /// Reconstruction: base_snapshot + delta_snapshots + recent_ops
    fn reconstruct_from_disk(&self, log: &RecordLog, head_offset: u64) -> Result<Vec<u8>> {
        let mut operations = Vec::new();
        let mut current_offset = Some(head_offset);
        let mut hit_snapshot = false; // Once we hit any snapshot, stop collecting regular ops

        // Follow chain backwards, collecting operations
        while let Some(offset) = current_offset {
            let record = log.read_at(offset)?;

            // Parse the state update from the record payload
            let update: StateUpdateRecord = serde_json::from_slice(&record.payload)
                .map_err(|e| StoreError::Deserialization(e.to_string()))?;

            match &update.operation {
                StateOperation::Snapshot(_) => {
                    // Full snapshot - add it and stop completely
                    operations.push(update.operation.clone());
                    break;
                }
                StateOperation::DeltaSnapshot(_) => {
                    // Delta snapshot - add it and continue looking for more snapshots
                    // but stop collecting regular operations
                    operations.push(update.operation.clone());
                    hit_snapshot = true;
                }
                _ => {
                    // Regular operation - only collect if we haven't hit a snapshot yet
                    if !hit_snapshot {
                        operations.push(update.operation.clone());
                    }
                }
            }

            current_offset = update.prev_update_offset;
        }

        // Apply operations in forward order (reverse of collection order)
        operations.reverse();

        let mut state = Vec::new();
        for op in operations {
            state = apply_operation(state, op)?;
        }

        Ok(state)
    }

    /// Get the chain head for a state.
    pub fn get_head(&self, branch_id: BranchId, state_id: &str) -> Option<StateChainHead> {
        let key = (branch_id, state_id.to_string());
        self.index.read().heads.get(&key).cloned()
    }

    /// Get the strategy for a state.
    pub fn get_strategy(&self, state_id: &str) -> Option<StateStrategy> {
        self.index.read().strategies.get(state_id).cloned()
    }

    /// Check what kind of snapshot a state needs (if any).
    ///
    /// Returns `None` if no snapshot is needed, or the type of snapshot needed.
    pub fn snapshot_needed(&self, branch_id: BranchId, state_id: &str) -> Option<SnapshotNeeded> {
        let index = self.index.read();

        let key = (branch_id, state_id.to_string());
        let head = match index.heads.get(&key) {
            Some(h) => h,
            None => return None,
        };

        let strategy = match index.strategies.get(state_id) {
            Some(s) => s,
            None => return None,
        };

        match strategy {
            StateStrategy::Snapshot => None, // Set strategy always stores full value
            StateStrategy::Delta { snapshot_every } => {
                if head.ops_since_delta_snapshot >= *snapshot_every {
                    Some(SnapshotNeeded::Full)
                } else {
                    None
                }
            }
            StateStrategy::AppendLog {
                delta_snapshot_every,
                full_snapshot_every,
            } => {
                // Check if full snapshot is needed first
                if head.delta_snapshots_since_full >= *full_snapshot_every {
                    Some(SnapshotNeeded::Full)
                } else if head.ops_since_delta_snapshot >= *delta_snapshot_every {
                    // If there are non-Append operations (Edit, Redact) since the last snapshot,
                    // we need a full snapshot instead of a delta, because delta snapshots
                    // only track Append operations.
                    if head.has_non_append_since_snapshot {
                        Some(SnapshotNeeded::Full)
                    } else {
                        Some(SnapshotNeeded::Delta)
                    }
                } else {
                    None
                }
            }
            StateStrategy::Struct { .. } => {
                // For struct, snapshot when ops exceed threshold
                if head.ops_since_delta_snapshot >= 100 {
                    Some(SnapshotNeeded::Full)
                } else {
                    None
                }
            }
        }
    }

    /// Backwards-compatible check if any snapshot is needed.
    pub fn needs_snapshot(&self, branch_id: BranchId, state_id: &str) -> bool {
        self.snapshot_needed(branch_id, state_id).is_some()
    }

    /// Copy state chain heads from one branch to another (for branching).
    pub fn copy_heads_for_branch(&self, from_branch: BranchId, to_branch: BranchId) {
        let mut index = self.index.write();

        // Find all heads for the source branch
        let heads_to_copy: Vec<_> = index.heads.iter()
            .filter(|((branch_id, _), _)| *branch_id == from_branch)
            .map(|((_, state_id), head)| (state_id.clone(), head.clone()))
            .collect();

        // Copy them to the new branch
        for (state_id, head) in heads_to_copy {
            index.heads.insert((to_branch, state_id), head);
        }
    }

    /// Set a state chain head for a branch at a specific offset.
    ///
    /// Used by `create_branch_at` to point a new branch's state head at an
    /// existing position in the parent's chain, without writing new records.
    pub fn set_head_for_branch(
        &self,
        branch_id: BranchId,
        state_id: &str,
        head_offset: u64,
        item_count: usize,
    ) {
        let mut index = self.index.write();

        let head = StateChainHead {
            head_offset,
            // Fresh snapshot accounting - the branch starts its own tracking
            ops_since_delta_snapshot: 0,
            delta_snapshots_since_full: 0,
            last_delta_snapshot_offset: None,
            last_full_snapshot_offset: None,
            has_non_append_since_snapshot: false,
            item_count,
        };

        index.heads.insert((branch_id, state_id.to_string()), head);
    }

    /// Get all registered state IDs.
    pub fn state_ids(&self) -> Vec<String> {
        self.index.read().strategies.keys().cloned().collect()
    }

    /// Get count of registered states.
    pub fn state_count(&self) -> usize {
        self.index.read().strategies.len()
    }

    /// Clear the cache (useful for testing or memory pressure).
    pub fn clear_cache(&self) {
        self.cache.write().clear();
    }

    /// Get cache statistics.
    pub fn cache_len(&self) -> usize {
        self.cache.read().len()
    }

    /// Get compaction statistics for a state.
    ///
    /// Returns the number of operations that would be eliminated by compaction,
    /// and the offset of the oldest operation that's still needed.
    pub fn get_compaction_stats(&self, branch_id: BranchId, state_id: &str) -> Option<CompactionStats> {
        let index = self.index.read();
        let key = (branch_id, state_id.to_string());
        let head = index.heads.get(&key)?;

        Some(CompactionStats {
            ops_since_last_full_snapshot: head.ops_since_delta_snapshot
                + head.delta_snapshots_since_full,
            last_full_snapshot_offset: head.last_full_snapshot_offset,
            last_delta_snapshot_offset: head.last_delta_snapshot_offset,
            delta_snapshots_since_full: head.delta_snapshots_since_full,
        })
    }

    /// Count operations in the chain for a state.
    ///
    /// This traverses the chain to count how many records would be eliminated
    /// by creating a full snapshot at the current head.
    pub fn count_chain_operations(&self, branch_id: BranchId, state_id: &str) -> Result<Option<ChainStats>> {
        let index = self.index.read();
        let key = (branch_id, state_id.to_string());
        let head = match index.heads.get(&key) {
            Some(h) => h.clone(),
            None => return Ok(None),
        };
        drop(index);

        let log = self
            .log
            .as_ref()
            .ok_or_else(|| StoreError::NotInitialized)?;

        let mut total_ops = 0u64;
        let mut ops_before_snapshot = 0u64;
        let mut total_bytes = 0u64;
        let mut bytes_before_snapshot = 0u64;
        let mut found_full_snapshot = false;
        let mut current_offset = Some(head.head_offset);

        while let Some(offset) = current_offset {
            let record = log.read_at(offset)?;
            let record_size = record.payload.len() as u64;

            let update: StateUpdateRecord = serde_json::from_slice(&record.payload)
                .map_err(|e| StoreError::Deserialization(e.to_string()))?;

            total_ops += 1;
            total_bytes += record_size;

            if found_full_snapshot {
                ops_before_snapshot += 1;
                bytes_before_snapshot += record_size;
            }

            if matches!(update.operation, StateOperation::Snapshot(_)) {
                found_full_snapshot = true;
            }

            current_offset = update.prev_update_offset;
        }

        Ok(Some(ChainStats {
            total_operations: total_ops,
            operations_before_snapshot: ops_before_snapshot,
            total_bytes,
            bytes_before_snapshot,
            has_full_snapshot: found_full_snapshot,
        }))
    }

    /// Save state index to file.
    pub fn save(&self) -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)?;

        // Write magic
        file.write_all(STATE_INDEX_MAGIC)?;

        // Write version
        file.write_all(&[STATE_INDEX_VERSION])?;

        // Serialize index with MessagePack
        let index = self.index.read();
        let encoded =
            rmp_serde::to_vec(&*index).map_err(|e| StoreError::Serialization(e.to_string()))?;

        // Write length and data
        file.write_all(&(encoded.len() as u64).to_le_bytes())?;
        file.write_all(&encoded)?;

        file.sync_all()?;
        Ok(())
    }

    /// Load state index from file.
    fn load_from_file(&self) -> Result<()> {
        let mut file = File::open(&self.path)?;

        // Read magic
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        if &magic != STATE_INDEX_MAGIC {
            return Err(StoreError::InvalidFormat(
                "Invalid state index magic".into(),
            ));
        }

        // Read version
        let mut version = [0u8; 1];
        file.read_exact(&mut version)?;
        if version[0] != STATE_INDEX_VERSION {
            return Err(StoreError::InvalidFormat(format!(
                "Unsupported state index version: {}",
                version[0]
            )));
        }

        // Read index
        let mut len_bytes = [0u8; 8];
        file.read_exact(&mut len_bytes)?;
        let len = u64::from_le_bytes(len_bytes) as usize;

        let mut encoded = vec![0u8; len];
        file.read_exact(&mut encoded)?;

        let index: StateIndex = rmp_serde::from_slice(&encoded)
            .map_err(|e| StoreError::Deserialization(e.to_string()))?;

        *self.index.write() = index;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{BranchId, RecordInput, Sequence, Timestamp};
    use tempfile::TempDir;

    const TEST_BRANCH: BranchId = BranchId(1);

    fn setup_test() -> (TempDir, Arc<RecordLog>, StateManager) {
        let dir = TempDir::new().unwrap();
        let log = Arc::new(RecordLog::open(dir.path().join("records.log")).unwrap());
        let mut manager = StateManager::new(dir.path().join("state.bin")).unwrap();
        manager.set_log(Arc::clone(&log));
        (dir, log, manager)
    }

    fn append_state_update(
        log: &RecordLog,
        state_id: &str,
        prev_offset: Option<u64>,
        operation: StateOperation,
        seq: u64,
    ) -> u64 {
        let update = StateUpdateRecord {
            record_id: crate::types::RecordId(0), // Will be assigned
            global_sequence: Sequence(seq),
            state_id: state_id.to_string(),
            prev_update_offset: prev_offset,
            operation,
            timestamp: Timestamp::now(),
        };

        let payload = serde_json::to_vec(&update).unwrap();
        let input = RecordInput::raw("state_update", payload);
        let (_, offset) = log.append(input, BranchId(1), Sequence(seq)).unwrap();
        offset
    }

    #[test]
    fn test_register_and_update() {
        let (_dir, log, manager) = setup_test();

        manager
            .register_state(StateRegistration {
                id: "counter".to_string(),
                strategy: StateStrategy::Snapshot,
                initial_value: None,
            })
            .unwrap();

        // Append state update to log
        let offset = append_state_update(
            &log,
            "counter",
            None,
            StateOperation::Set(b"42".to_vec()),
            1,
        );

        // Record in manager
        manager
            .record_update(TEST_BRANCH, "counter", offset, &StateOperation::Set(b"42".to_vec()))
            .unwrap();

        // Get state
        let state = manager.get_state(TEST_BRANCH, "counter").unwrap().unwrap();
        assert_eq!(state, b"42");
    }

    #[test]
    fn test_chain_reconstruction() {
        let (_dir, log, manager) = setup_test();

        manager
            .register_state(StateRegistration {
                id: "items".to_string(),
                strategy: StateStrategy::AppendLog {
                    delta_snapshot_every: 10,
                    full_snapshot_every: 5,
                },
                initial_value: None,
            })
            .unwrap();

        // Build chain
        let offset1 = append_state_update(
            &log,
            "items",
            None,
            StateOperation::Append(b"1".to_vec()),
            1,
        );
        manager
            .record_update(TEST_BRANCH, "items", offset1, &StateOperation::Append(b"1".to_vec()))
            .unwrap();

        let offset2 = append_state_update(
            &log,
            "items",
            Some(offset1),
            StateOperation::Append(b"2".to_vec()),
            2,
        );
        manager
            .record_update(TEST_BRANCH, "items", offset2, &StateOperation::Append(b"2".to_vec()))
            .unwrap();

        let offset3 = append_state_update(
            &log,
            "items",
            Some(offset2),
            StateOperation::Append(b"3".to_vec()),
            3,
        );
        manager
            .record_update(TEST_BRANCH, "items", offset3, &StateOperation::Append(b"3".to_vec()))
            .unwrap();

        // Reconstruct state
        let state = manager.get_state(TEST_BRANCH, "items").unwrap().unwrap();
        let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
        assert_eq!(arr, vec![1, 2, 3]);
    }

    #[test]
    fn test_snapshot_breaks_chain() {
        let (_dir, log, manager) = setup_test();

        manager
            .register_state(StateRegistration {
                id: "items".to_string(),
                strategy: StateStrategy::AppendLog {
                    delta_snapshot_every: 10,
                    full_snapshot_every: 5,
                },
                initial_value: None,
            })
            .unwrap();

        // Build chain
        let offset1 = append_state_update(
            &log,
            "items",
            None,
            StateOperation::Append(b"1".to_vec()),
            1,
        );
        manager
            .record_update(TEST_BRANCH, "items", offset1, &StateOperation::Append(b"1".to_vec()))
            .unwrap();

        let offset2 = append_state_update(
            &log,
            "items",
            Some(offset1),
            StateOperation::Append(b"2".to_vec()),
            2,
        );
        manager
            .record_update(TEST_BRANCH, "items", offset2, &StateOperation::Append(b"2".to_vec()))
            .unwrap();

        // Snapshot
        let snapshot_data = serde_json::to_vec(&serde_json::json!([1, 2])).unwrap();
        let snapshot_op = StateOperation::Snapshot(snapshot_data.clone());
        let offset3 = append_state_update(&log, "items", Some(offset2), snapshot_op.clone(), 3);
        manager
            .record_update(TEST_BRANCH, "items", offset3, &snapshot_op)
            .unwrap();

        // More appends after snapshot
        let offset4 = append_state_update(
            &log,
            "items",
            Some(offset3),
            StateOperation::Append(b"3".to_vec()),
            4,
        );
        manager
            .record_update(TEST_BRANCH, "items", offset4, &StateOperation::Append(b"3".to_vec()))
            .unwrap();

        // Reconstruction should start from snapshot
        let state = manager.get_state(TEST_BRANCH, "items").unwrap().unwrap();
        let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
        assert_eq!(arr, vec![1, 2, 3]);

        // Check ops_since_delta_snapshot
        let head = manager.get_head(TEST_BRANCH, "items").unwrap();
        assert_eq!(head.ops_since_delta_snapshot, 1);
    }

    #[test]
    fn test_cache_hit() {
        let (_dir, log, manager) = setup_test();

        manager
            .register_state(StateRegistration {
                id: "data".to_string(),
                strategy: StateStrategy::Snapshot,
                initial_value: None,
            })
            .unwrap();

        let offset = append_state_update(
            &log,
            "data",
            None,
            StateOperation::Set(b"\"cached\"".to_vec()),
            1,
        );
        manager
            .record_update(TEST_BRANCH, "data", offset, &StateOperation::Set(b"\"cached\"".to_vec()))
            .unwrap();

        // First call - cache miss
        assert_eq!(manager.cache_len(), 0);
        let _ = manager.get_state(TEST_BRANCH, "data").unwrap();
        assert_eq!(manager.cache_len(), 1);

        // Second call - cache hit (no additional disk reads)
        let state = manager.get_state(TEST_BRANCH, "data").unwrap().unwrap();
        assert_eq!(state, b"\"cached\"");
        assert_eq!(manager.cache_len(), 1);
    }

    #[test]
    fn test_cache_invalidation() {
        let (_dir, log, manager) = setup_test();

        manager
            .register_state(StateRegistration {
                id: "data".to_string(),
                strategy: StateStrategy::Snapshot,
                initial_value: None,
            })
            .unwrap();

        let offset1 = append_state_update(
            &log,
            "data",
            None,
            StateOperation::Set(b"\"v1\"".to_vec()),
            1,
        );
        manager
            .record_update(TEST_BRANCH, "data", offset1, &StateOperation::Set(b"\"v1\"".to_vec()))
            .unwrap();

        // Populate cache
        let _ = manager.get_state(TEST_BRANCH, "data").unwrap();
        assert_eq!(manager.cache_len(), 1);

        // Update invalidates cache
        let offset2 = append_state_update(
            &log,
            "data",
            Some(offset1),
            StateOperation::Set(b"\"v2\"".to_vec()),
            2,
        );
        manager
            .record_update(TEST_BRANCH, "data", offset2, &StateOperation::Set(b"\"v2\"".to_vec()))
            .unwrap();
        assert_eq!(manager.cache_len(), 0);

        // Next read gets new value
        let state = manager.get_state(TEST_BRANCH, "data").unwrap().unwrap();
        assert_eq!(state, b"\"v2\"");
    }

    #[test]
    fn test_nonexistent_state() {
        let (_dir, _log, manager) = setup_test();

        let state = manager.get_state(TEST_BRANCH, "nonexistent").unwrap();
        assert!(state.is_none());
    }

    #[test]
    fn test_persistence() {
        let dir = TempDir::new().unwrap();
        let state_path = dir.path().join("state.bin");
        let log_path = dir.path().join("records.log");

        // Create and save
        {
            let log = Arc::new(RecordLog::open(&log_path).unwrap());
            let mut manager = StateManager::new(&state_path).unwrap();
            manager.set_log(Arc::clone(&log));

            manager
                .register_state(StateRegistration {
                    id: "test".to_string(),
                    strategy: StateStrategy::Snapshot,
                    initial_value: None,
                })
                .unwrap();

            let offset = append_state_update(
                &log,
                "test",
                None,
                StateOperation::Set(b"\"persisted\"".to_vec()),
                1,
            );
            manager
                .record_update(TEST_BRANCH, "test", offset, &StateOperation::Set(b"\"persisted\"".to_vec()))
                .unwrap();

            manager.save().unwrap();
        }

        // Load and verify
        {
            let log = Arc::new(RecordLog::open(&log_path).unwrap());
            let mut manager = StateManager::load(&state_path).unwrap();
            manager.set_log(Arc::clone(&log));

            let state = manager.get_state(TEST_BRANCH, "test").unwrap().unwrap();
            assert_eq!(state, b"\"persisted\"");
        }
    }

    #[test]
    fn test_delta_snapshot_reconstruction() {
        let (_dir, log, manager) = setup_test();

        manager
            .register_state(StateRegistration {
                id: "items".to_string(),
                strategy: StateStrategy::AppendLog {
                    delta_snapshot_every: 3,
                    full_snapshot_every: 3,
                },
                initial_value: None,
            })
            .unwrap();

        // Build chain with appends
        let offset1 = append_state_update(
            &log,
            "items",
            None,
            StateOperation::Append(b"1".to_vec()),
            1,
        );
        manager
            .record_update(TEST_BRANCH, "items", offset1, &StateOperation::Append(b"1".to_vec()))
            .unwrap();

        let offset2 = append_state_update(
            &log,
            "items",
            Some(offset1),
            StateOperation::Append(b"2".to_vec()),
            2,
        );
        manager
            .record_update(TEST_BRANCH, "items", offset2, &StateOperation::Append(b"2".to_vec()))
            .unwrap();

        // Delta snapshot
        let delta_data = serde_json::to_vec(&serde_json::json!([1, 2])).unwrap();
        let delta_op = StateOperation::DeltaSnapshot(delta_data.clone());
        let offset3 = append_state_update(&log, "items", Some(offset2), delta_op.clone(), 3);
        manager.record_update(TEST_BRANCH, "items", offset3, &delta_op).unwrap();

        // More appends after delta
        let offset4 = append_state_update(
            &log,
            "items",
            Some(offset3),
            StateOperation::Append(b"3".to_vec()),
            4,
        );
        manager
            .record_update(TEST_BRANCH, "items", offset4, &StateOperation::Append(b"3".to_vec()))
            .unwrap();

        let offset5 = append_state_update(
            &log,
            "items",
            Some(offset4),
            StateOperation::Append(b"4".to_vec()),
            5,
        );
        manager
            .record_update(TEST_BRANCH, "items", offset5, &StateOperation::Append(b"4".to_vec()))
            .unwrap();

        // Reconstruct - should use delta snapshot as base
        let state = manager.get_state(TEST_BRANCH, "items").unwrap().unwrap();
        let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
        assert_eq!(arr, vec![1, 2, 3, 4]);

        // Check tracking
        let head = manager.get_head(TEST_BRANCH, "items").unwrap();
        assert_eq!(head.ops_since_delta_snapshot, 2); // Two appends after delta
        assert_eq!(head.delta_snapshots_since_full, 1); // One delta snapshot
    }

    #[test]
    fn test_multiple_delta_snapshots_reconstruction() {
        let (_dir, log, manager) = setup_test();

        manager
            .register_state(StateRegistration {
                id: "items".to_string(),
                strategy: StateStrategy::AppendLog {
                    delta_snapshot_every: 2,
                    full_snapshot_every: 5,
                },
                initial_value: None,
            })
            .unwrap();

        // First delta snapshot: [1, 2]
        let delta1 = serde_json::to_vec(&serde_json::json!([1, 2])).unwrap();
        let op1 = StateOperation::DeltaSnapshot(delta1);
        let offset1 = append_state_update(&log, "items", None, op1.clone(), 1);
        manager.record_update(TEST_BRANCH, "items", offset1, &op1).unwrap();

        // Second delta snapshot: [3, 4]
        let delta2 = serde_json::to_vec(&serde_json::json!([3, 4])).unwrap();
        let op2 = StateOperation::DeltaSnapshot(delta2);
        let offset2 = append_state_update(&log, "items", Some(offset1), op2.clone(), 2);
        manager.record_update(TEST_BRANCH, "items", offset2, &op2).unwrap();

        // Append after deltas
        let offset3 = append_state_update(
            &log,
            "items",
            Some(offset2),
            StateOperation::Append(b"5".to_vec()),
            3,
        );
        manager
            .record_update(TEST_BRANCH, "items", offset3, &StateOperation::Append(b"5".to_vec()))
            .unwrap();

        // Reconstruct - should traverse both delta snapshots
        let state = manager.get_state(TEST_BRANCH, "items").unwrap().unwrap();
        let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
        assert_eq!(arr, vec![1, 2, 3, 4, 5]);

        // Check tracking
        let head = manager.get_head(TEST_BRANCH, "items").unwrap();
        assert_eq!(head.delta_snapshots_since_full, 2);
    }

    #[test]
    fn test_full_snapshot_resets_deltas() {
        let (_dir, log, manager) = setup_test();

        manager
            .register_state(StateRegistration {
                id: "items".to_string(),
                strategy: StateStrategy::AppendLog {
                    delta_snapshot_every: 2,
                    full_snapshot_every: 2,
                },
                initial_value: None,
            })
            .unwrap();

        // Delta snapshot
        let delta1 = serde_json::to_vec(&serde_json::json!([1, 2])).unwrap();
        let op1 = StateOperation::DeltaSnapshot(delta1);
        let offset1 = append_state_update(&log, "items", None, op1.clone(), 1);
        manager.record_update(TEST_BRANCH, "items", offset1, &op1).unwrap();

        assert_eq!(
            manager.get_head(TEST_BRANCH, "items").unwrap().delta_snapshots_since_full,
            1
        );

        // Full snapshot - should reset delta counter
        let full = serde_json::to_vec(&serde_json::json!([1, 2])).unwrap();
        let op2 = StateOperation::Snapshot(full);
        let offset2 = append_state_update(&log, "items", Some(offset1), op2.clone(), 2);
        manager.record_update(TEST_BRANCH, "items", offset2, &op2).unwrap();

        let head = manager.get_head(TEST_BRANCH, "items").unwrap();
        assert_eq!(head.delta_snapshots_since_full, 0);
        assert!(head.last_full_snapshot_offset.is_some());
    }

    #[test]
    fn test_snapshot_needed_logic() {
        let (_dir, _log, manager) = setup_test();

        manager
            .register_state(StateRegistration {
                id: "items".to_string(),
                strategy: StateStrategy::AppendLog {
                    delta_snapshot_every: 3,
                    full_snapshot_every: 2,
                },
                initial_value: None,
            })
            .unwrap();

        // No head yet, no snapshot needed
        assert!(manager.snapshot_needed(TEST_BRANCH, "items").is_none());
    }
}
