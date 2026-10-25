//! State manager with disk-based chain traversal and LRU cache.
//!
//! Chain traversal uses file offsets stored in each update record,
//! eliminating the need to keep all updates in memory. An LRU cache
//! stores recently reconstructed states for fast repeated access.

use super::cache::{SharedCache, StateBytes, StateItems};
use super::{FieldIndexKind, FieldIndexManager};
use crate::atomic_file::Checkpoint;
use crate::error::{Result, StoreError};
use crate::records::RecordLog;
use crate::types::{
    BranchId, StateOperation, StateRegistration, StateStrategy, StateUpdateRecord, TreeOp,
    TreeState,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::Read;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
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
///
/// **Wire-format constraint:** heads are persisted via `rmp_serde::to_vec`
/// (compact mode), which encodes structs as *positional arrays* — field order
/// IS the on-disk format, and there are no field names to disambiguate.
/// Append new fields at the END only, with `#[serde(default)]` (serde fills
/// defaults for missing trailing elements); inserting or reordering fields
/// silently misparses every existing store's index. See
/// `test_state_chain_head_trailing_fields_default` for the compat guarantee.
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

    /// Whether there are non-Append operations (Edit, Redact, Set) since the last snapshot.
    /// When true, a full snapshot should be created instead of a delta snapshot,
    /// because delta snapshots can only track Append operations.
    #[serde(default)]
    pub has_non_append_since_snapshot: bool,

    /// Current number of items in the state (for O(1) length queries via
    /// `get_state_len`). Updated on each Append (+1), Redact (minus the items
    /// of `[start, end)` that exist: both ends clamped to the count), and a
    /// full Snapshot or a Set of an array (=its length). Exact for an
    /// AppendLog written with its own operations (Appends, Edits, Redacts,
    /// and Sets and Snapshots of arrays), except a count an older build
    /// stored before Redacts were clamped: loading keeps a stored count as it
    /// is, so that one can stay low until a full Snapshot or array Set counts
    /// the items again. For Tree states it is an estimate between full
    /// snapshots (an overwrite counts as an insert, and a remove as a removal
    /// whether or not the path existed) and is corrected from the snapshot
    /// bytes at each full.
    /// Snapshot spacing does NOT read this field — it reads
    /// `item_count_at_last_full`, which is always stamped from parsed
    /// snapshot bytes and therefore exact.
    #[serde(default)]
    pub item_count: usize,

    /// Item count captured at the most recent full snapshot, stamped from the
    /// parsed snapshot bytes (exact — not the running estimate above).
    /// Size-aware full-snapshot spacing compares ops-since-full against this
    /// so that growing states snapshot on doubling (amortized-linear disk)
    /// instead of on a fixed interval (quadratic disk, see issue #11).
    /// Defaults to 0 on legacy indexes, leaving the configured-interval floor
    /// in charge (the old cadence, to within one op at the boundary) until the
    /// first full snapshot stamps a baseline.
    #[serde(default)]
    pub item_count_at_last_full: usize,
}

/// Chain heads for every `(branch, state)` pair, grouped by branch.
///
/// Grouping by branch is what keeps branch operations proportional to the
/// branch involved rather than to the whole store: forking copies one
/// branch's map (or just the selected entries of it), dropping a branch
/// removes one entry, and neither scans the heads of any other branch. The
/// per-branch map is ordered so that a state-id prefix selects a contiguous
/// range.
///
/// **On-disk format:** unchanged. This serializes as the flat
/// `(branch_id, state_id) -> head` map that `state.bin` has always held
/// (the format a `HashMap<(BranchId, String), StateChainHead>` produces), and
/// deserializes from it — see `test_state_index_wire_format_is_flat_map`.
#[derive(Clone, Debug, Default)]
pub struct BranchHeads {
    by_branch: HashMap<BranchId, BTreeMap<String, StateChainHead>>,
}

impl BranchHeads {
    /// Head of `state_id` on `branch`, if it has one.
    pub fn get(&self, branch: BranchId, state_id: &str) -> Option<&StateChainHead> {
        self.by_branch.get(&branch)?.get(state_id)
    }

    /// Mutable head of `state_id` on `branch`, if it has one.
    pub fn get_mut(&mut self, branch: BranchId, state_id: &str) -> Option<&mut StateChainHead> {
        self.by_branch.get_mut(&branch)?.get_mut(state_id)
    }

    /// Insert or replace the head for `(branch, state_id)`.
    pub fn insert(
        &mut self,
        (branch, state_id): (BranchId, String),
        head: StateChainHead,
    ) -> Option<StateChainHead> {
        self.by_branch.entry(branch).or_default().insert(state_id, head)
    }

    /// All heads of one branch, ordered by state id.
    pub fn branch(&self, branch: BranchId) -> Option<&BTreeMap<String, StateChainHead>> {
        self.by_branch.get(&branch)
    }

    /// Remove every head of `branch`, returning them.
    pub fn remove_branch(&mut self, branch: BranchId) -> Option<BTreeMap<String, StateChainHead>> {
        self.by_branch.remove(&branch)
    }

    /// Every head in the index.
    pub fn iter(&self) -> impl Iterator<Item = ((BranchId, &String), &StateChainHead)> {
        self.by_branch.iter().flat_map(|(branch, heads)| {
            heads.iter().map(move |(state_id, head)| ((*branch, state_id), head))
        })
    }

    /// Total number of heads across all branches.
    pub fn len(&self) -> usize {
        self.by_branch.values().map(|heads| heads.len()).sum()
    }

    /// Whether the index holds no heads at all.
    pub fn is_empty(&self) -> bool {
        self.by_branch.values().all(|heads| heads.is_empty())
    }
}

impl Serialize for BranchHeads {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        // Entry-by-entry this makes the same serializer calls as
        // `HashMap<(BranchId, String), StateChainHead>`. The entry count is
        // passed explicitly: the nested iterator has no exact size hint, and
        // MessagePack needs the map length up front.
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.len()))?;
        for (key, head) in self.iter() {
            map.serialize_entry(&key, head)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for BranchHeads {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct HeadsVisitor;

        impl<'de> serde::de::Visitor<'de> for HeadsVisitor {
            type Value = BranchHeads;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a map of (branch_id, state_id) to chain head")
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<BranchHeads, A::Error> {
                let mut heads = BranchHeads::default();
                while let Some((key, head)) =
                    map.next_entry::<(BranchId, String), StateChainHead>()?
                {
                    heads.insert(key, head);
                }
                Ok(heads)
            }
        }

        deserializer.deserialize_map(HeadsVisitor)
    }
}

/// Which of a parent branch's states a new branch inherits.
///
/// A state is selected if its id equals one of `state_ids` or starts with one
/// of `state_prefixes`. An empty selection selects nothing. Ids that name no
/// state, or a state with no value on the parent, are ignored: there is
/// nothing to inherit and the state reads as empty on the child either way.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StateSelection {
    /// Exact state ids to inherit.
    pub state_ids: Vec<String>,
    /// State-id prefixes to inherit.
    pub state_prefixes: Vec<String>,
}

impl StateSelection {
    /// Whether `state_id` is covered by this selection.
    pub fn matches(&self, state_id: &str) -> bool {
        self.state_ids.iter().any(|id| id == state_id)
            || self.state_prefixes.iter().any(|p| state_id.starts_with(p.as_str()))
    }
}

/// In-memory state index (small - just heads and strategies).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StateIndex {
    /// Chain heads per (branch_id, state_id).
    pub heads: BranchHeads,

    /// Registered state strategies.
    pub strategies: HashMap<String, StateStrategy>,
}

/// State manager handles per-state chains and reconstruction.
///
/// Uses disk-based chain traversal (no in-memory update storage)
/// with an LRU cache for frequently accessed states.
pub struct StateManager {
    /// Path to state index file.
    path: PathBuf,

    /// Last successfully published checkpoint, never seeded just by loading.
    checkpoint: Checkpoint,

    /// In-memory index (just heads + strategies, very small).
    index: RwLock<StateIndex>,

    /// LRU cache for reconstructed states (serialized bytes).
    ///
    /// All three caches are keyed per `(branch, state)` and validated by
    /// `head_offset`, and all three share a value between branches whose
    /// head for the state is at the same offset — see `state::cache`.
    cache: RwLock<SharedCache<StateBytes>>,

    /// LRU cache for per-item byte spans of array-shaped states.
    /// Populated lazily by `get_state_items`, then kept current across
    /// Append/Edit/Redact writes by deriving the next value from the cached
    /// one (`record_update_with`) instead of being dropped and re-read.
    items_cache: RwLock<SharedCache<StateItems>>,

    /// LRU cache for decoded Tree states. Populated lazily by
    /// `get_tree_state`; same key scheme and head_offset validity as above.
    tree_cache: RwLock<SharedCache<TreeState>>,

    /// Reference to record log for disk-based chain traversal.
    log: Option<Arc<RecordLog>>,

    /// Secondary indexes on JSON fields of state-slot items, incrementally
    /// maintained from every operation this manager records. Persisted to
    /// its own file (`state-indexes.bin`) alongside `state.bin` — see
    /// `save`/`load_from_file`.
    field_indexes: RwLock<FieldIndexManager>,

    /// Path to the persisted field-index file, derived once at construction
    /// as a sibling of `path` (`state.bin` -> `state-indexes.bin`).
    field_index_path: PathBuf,

    /// Branches that were deleted since the last `purge_retired_branches`.
    /// Their chain heads stay in `index` (and so in the next `state.bin`)
    /// until the store has durably recorded the deletion — see
    /// `retire_branch`.
    retired_branches: parking_lot::Mutex<Vec<BranchId>>,

    /// Number of times a state value was rebuilt from the log
    /// (`reconstruct_from_disk`). Everything served from a cache — this
    /// branch's own entry or one shared with another branch — leaves it
    /// untouched, which makes it the observable for "this read did not
    /// re-materialize".
    reconstructions: AtomicU64,
}

/// Derive the field-index file path as a sibling of the state-index path.
fn field_index_path_for(state_index_path: &Path) -> PathBuf {
    match state_index_path.parent() {
        Some(parent) => parent.join("state-indexes.bin"),
        None => PathBuf::from("state-indexes.bin"),
    }
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
        let field_index_path = field_index_path_for(&path);

        Ok(Self {
            path,
            checkpoint: Checkpoint::default(),
            index: RwLock::new(StateIndex::default()),
            cache: RwLock::new(SharedCache::new(cache_size)),
            items_cache: RwLock::new(SharedCache::new(cache_size)),
            tree_cache: RwLock::new(SharedCache::new(cache_size)),
            log: None,
            field_indexes: RwLock::new(FieldIndexManager::new()),
            field_index_path,
            retired_branches: parking_lot::Mutex::new(Vec::new()),
            reconstructions: AtomicU64::new(0),
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
        let field_index_path = field_index_path_for(&path);

        let manager = Self {
            path: path.clone(),
            checkpoint: Checkpoint::default(),
            index: RwLock::new(StateIndex::default()),
            cache: RwLock::new(SharedCache::new(cache_size)),
            items_cache: RwLock::new(SharedCache::new(cache_size)),
            tree_cache: RwLock::new(SharedCache::new(cache_size)),
            log: None,
            field_indexes: RwLock::new(FieldIndexManager::new()),
            field_index_path,
            retired_branches: parking_lot::Mutex::new(Vec::new()),
            reconstructions: AtomicU64::new(0),
        };

        if path.exists() {
            manager.load_from_file()?;
        }

        Ok(manager)
    }

    /// Validate that every persisted chain-head offset points inside the valid
    /// region of the log (`< valid_len`).
    ///
    /// `state.bin` is only flushed on `sync()`/`Drop`, so after a torn tail is
    /// truncated on log open, a persisted `head_offset` (or snapshot offset)
    /// could point into bytes that no longer exist — dereferencing it later
    /// would read garbage or a wrong record. Any offset at or past `valid_len`
    /// is unrecoverable metadata corruption and fails the open loudly rather
    /// than silently.
    pub fn validate_offsets(&self, valid_len: u64) -> Result<()> {
        let index = self.index.read();
        for ((branch, state_id), head) in index.heads.iter() {
            let offsets = [
                Some(head.head_offset),
                head.last_delta_snapshot_offset,
                head.last_full_snapshot_offset,
            ];
            for offset in offsets.into_iter().flatten() {
                if offset >= valid_len {
                    return Err(StoreError::Corruption(format!(
                        "state '{}' on branch {:?} has chain offset {} at/past the valid log \
                         length {}; state metadata is newer than the durable log (torn tail or \
                         corrupt state.bin)",
                        state_id, branch, offset, valid_len
                    )));
                }
            }
        }
        Ok(())
    }

    /// Last state update included in this metadata checkpoint.
    ///
    /// Store serializes mutations and checkpoints under its write lock. Heads
    /// are saved together, and branch copies only refer to earlier records, so
    /// their maximum offset also covers every preceding state update. Branch
    /// deletion retains heads, so it cannot move this watermark backwards.
    /// None means no update has been checkpointed, including offset zero.
    pub(crate) fn checkpoint_offset(&self) -> Option<u64> {
        self.index.read().heads.iter().map(|(_, h)| h.head_offset).max()
    }

    /// Highest branch identity referenced by state metadata, including copied
    /// heads that may have reached disk before their branch was published.
    pub(crate) fn max_branch_id(&self) -> Option<BranchId> {
        self.index.read().heads.iter().map(|((branch, _), _)| branch).max()
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

    /// Update the strategy parameters of an already-registered state.
    ///
    /// Registrations persist in `state.bin`, so a consumer that re-registers
    /// on every boot (context-manager does) can never retune snapshot
    /// cadence on an existing store — `register_state` errors with
    /// `StateExists` and the persisted strategy wins forever (2026-08-01:
    /// Mythos's messages state was stuck on `full_snapshot_every: 10`, a
    /// full copy of the entire history every ~500 appends — 57% of the last
    /// GB of its log). This is the upsert leg.
    ///
    /// Same strategy KIND only: swapping the kind out from under a live
    /// chain (AppendLog -> Snapshot etc.) would change reconstruction
    /// semantics for records already on disk. Cadence fields are free to
    /// change; they only steer FUTURE snapshot scheduling.
    pub fn update_state_strategy(&self, id: &str, strategy: StateStrategy) -> Result<()> {
        let mut index = self.index.write();
        let existing = index
            .strategies
            .get(id)
            .ok_or_else(|| StoreError::StateNotRegistered(id.to_string()))?;
        if std::mem::discriminant(existing) != std::mem::discriminant(&strategy) {
            return Err(StoreError::InvalidOperation(format!(
                "update_state_strategy('{}'): cannot change strategy kind ({:?} -> {:?})",
                id, existing, strategy
            )));
        }
        index.strategies.insert(id.to_string(), strategy);
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
        self.record_update_with(branch_id, state_id, offset, operation, false)
    }

    /// `record_update`, with the caller's assurance about the value.
    ///
    /// `content_preserving` says that `operation` is a snapshot (full or
    /// delta) of exactly the value the state already had at its previous
    /// head — true for the snapshots the store writes itself (threshold
    /// snapshots, `compact_state`, `tree_snapshot`) when no other write
    /// reached the state between reading the value and recording the
    /// snapshot. Such a record moves the chain head without changing what
    /// the state materializes to, so the cached values are kept and re-filed
    /// under the new head instead of being dropped. It must be `false` for a
    /// snapshot whose payload came from anywhere else.
    pub(crate) fn record_update_with(
        &self,
        branch_id: BranchId,
        state_id: &str,
        offset: u64,
        operation: &StateOperation,
        content_preserving: bool,
    ) -> Result<()> {
        let mut index = self.index.write();

        let branch_heads = index.heads.by_branch.entry(branch_id).or_default();
        // Where the chain stood before this record (`None`: first record).
        let prev_offset = branch_heads.get(state_id).map(|h| h.head_offset);
        if !branch_heads.contains_key(state_id) {
            branch_heads.insert(state_id.to_string(), StateChainHead {
                head_offset: offset,
                ops_since_delta_snapshot: 0,
                delta_snapshots_since_full: 0,
                last_delta_snapshot_offset: None,
                last_full_snapshot_offset: None,
                has_non_append_since_snapshot: false,
                item_count: 0,
                item_count_at_last_full: 0,
            });
        }
        let head = branch_heads
            .get_mut(state_id)
            .expect("head inserted above");

        head.head_offset = offset;

        match operation {
            StateOperation::Snapshot(data) => {
                // Full snapshot resets everything
                head.ops_since_delta_snapshot = 0;
                head.delta_snapshots_since_full = 0;
                head.last_full_snapshot_offset = Some(offset);
                head.last_delta_snapshot_offset = None; // Full snapshot supersedes deltas
                head.has_non_append_since_snapshot = false; // Reset the flag
                // Update item count from snapshot: array-shaped (AppendLog) states
                // count elements, map-shaped (Tree) states count entries.
                head.item_count = match serde_json::from_slice::<serde_json::Value>(data) {
                    Ok(serde_json::Value::Array(arr)) => arr.len(),
                    Ok(serde_json::Value::Object(map)) => map.len(),
                    _ => 0,
                };
                head.item_count_at_last_full = head.item_count;
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
            StateOperation::AppendMany(items) => {
                // One operation for the cadence, however many items, as a
                // TreeBatch counts. Append-only, so a delta snapshot may
                // still consolidate it (`compute_delta_items` takes its
                // items).
                head.ops_since_delta_snapshot += 1;
                head.item_count += items.len();
            }
            StateOperation::Redact { start, end } => {
                head.ops_since_delta_snapshot += 1;
                head.has_non_append_since_snapshot = true;
                // Redact removes the items of [start, end) that exist: both
                // ends are clamped to the length, as the materialized value
                // clamps them, so a range running past the end removes only
                // the items before it.
                let start = (*start).min(head.item_count);
                let end = (*end).min(head.item_count);
                head.item_count -= end.saturating_sub(start);
            }
            StateOperation::Edit { .. } => {
                head.ops_since_delta_snapshot += 1;
                head.has_non_append_since_snapshot = true;
                // Edit doesn't change count
            }
            StateOperation::Set(data) => {
                head.ops_since_delta_snapshot += 1;
                // Set replaces the entire state. Append-log delta snapshots only
                // consolidate Append operations, so allowing a delta after Set
                // would walk past the replacement and resurrect older appends.
                // Force the next threshold snapshot to contain the full state.
                head.has_non_append_since_snapshot = true;
                if let Ok(arr) = serde_json::from_slice::<Vec<serde_json::Value>>(data) {
                    head.item_count = arr.len();
                }
            }
            StateOperation::TreeSet { .. } => {
                head.ops_since_delta_snapshot += 1;
                // An estimate: overwrites count as inserts (newness is
                // unknown here without the tree contents). Corrected from the
                // parsed snapshot bytes at each full snapshot. Consumed by
                // `get_state_len` only; snapshot spacing reads the exact
                // `item_count_at_last_full` instead.
                head.item_count += 1;
            }
            StateOperation::TreeRemove { .. } => {
                head.ops_since_delta_snapshot += 1;
                head.item_count = head.item_count.saturating_sub(1);
            }
            StateOperation::TreeBatch { ops } => {
                head.ops_since_delta_snapshot += 1;
                for op in ops {
                    match op {
                        TreeOp::Set { .. } => head.item_count += 1,
                        TreeOp::Remove { .. } => {
                            head.item_count = head.item_count.saturating_sub(1)
                        }
                    }
                }
            }
            StateOperation::TreeDeltaSnapshot(_) => {
                head.ops_since_delta_snapshot = 0;
                head.delta_snapshots_since_full += 1;
                head.last_delta_snapshot_offset = Some(offset);
            }
            StateOperation::Delta { .. } | StateOperation::Field { .. } => {
                head.ops_since_delta_snapshot += 1;
                // Delta/Field operations for Struct type - don't change count
            }
        }

        // Bring this branch's cached values for the state in line with the
        // new head. Entries are per branch: nothing another branch holds is
        // touched, including a value this branch was sharing with it.
        let cache_key = format!("{}:{}", branch_id.0, state_id);
        let preserved_from = if content_preserving { prev_offset } else { None };

        // Serialized bytes: kept across a content-preserving snapshot,
        // otherwise dropped (the next `get_state` rebuilds them).
        {
            let mut cache = self.cache.write();
            match preserved_from {
                Some(prev) => cache.rekey(&cache_key, prev, offset),
                None => cache.pop(&cache_key),
            }
        }

        // Per-item form: derive the new value from the one cached at the
        // previous head (this branch's own, or one another branch holds at
        // that offset) rather than dropping it — otherwise every append
        // would cost the next slice read a full re-materialization. Only
        // when a value is already cached: a write never materializes.
        {
            let mut items_cache = self.items_cache.write();
            let next = prev_offset
                .and_then(|prev| items_cache.take(&cache_key, prev))
                .and_then(|items| Self::advance_items(items, operation, content_preserving));
            if let Some(items) = next {
                items_cache.put(cache_key.clone(), offset, items);
            }
        }

        // Decoded trees are not dropped on write (a stale entry is detected
        // by head_offset on the next read); a content-preserving snapshot
        // keeps the decoded tree current.
        if let Some(prev) = preserved_from {
            self.tree_cache.write().rekey(&cache_key, prev, offset);
        }

        // Incrementally maintain the field indexes registered for this state
        // ON THIS BRANCH. Gated on `has_indexes_on` so a slot with nothing
        // registered on the branch being written pays no parse cost at all
        // on the hot append path — including when other branches do index it.
        //
        // Parse failures poison (drop) every field index this branch holds
        // for this state_id rather than being silently skipped: skipping
        // would leave `by_ordinal` one short forever, permanently
        // misaligning every later ordinal against the real slot content with
        // no signal to the caller. A poisoned index is simply gone —
        // `register_field_index` rebuilds it from scratch — which is far
        // safer than confidently returning wrong ordinals. Indexes other
        // branches hold for the same state_id are never touched by this
        // write; see `FieldIndexManager`'s "Branch scoping" docs.
        if self.field_indexes.read().has_indexes_on(state_id, branch_id) {
            match operation {
                StateOperation::Append(item) => {
                    match serde_json::from_slice::<serde_json::Value>(item) {
                        Ok(value) => self
                            .field_indexes
                            .write()
                            .on_append(state_id, branch_id, offset, &value),
                        Err(_) => self.field_indexes.write().poison_state_on(state_id, branch_id),
                    }
                }
                StateOperation::AppendMany(items) => {
                    // Each item at its own ordinal, in order, as its own
                    // Append would be. Every item is parsed before any is
                    // indexed: one that doesn't parse poisons the indexes,
                    // as it would on its own.
                    let values: std::result::Result<Vec<serde_json::Value>, _> = items
                        .iter()
                        .map(|item| serde_json::from_slice::<serde_json::Value>(item))
                        .collect();
                    let mut field_indexes = self.field_indexes.write();
                    match values {
                        Ok(values) => {
                            for value in &values {
                                field_indexes.on_append(state_id, branch_id, offset, value);
                            }
                        }
                        Err(_) => field_indexes.poison_state_on(state_id, branch_id),
                    }
                }
                StateOperation::DeltaSnapshot(_) => {
                    // A DeltaSnapshot consolidates Append operations ALREADY
                    // recorded (and already indexed via their own Append
                    // hooks above) into one record — it introduces no new
                    // items and changes no existing ordinal. Exactly
                    // mirrors `head.item_count` a few lines up in this same
                    // function, which is deliberately left untouched by
                    // DeltaSnapshot for the same reason. Treating this as a
                    // fresh batch of appends (the bug this comment replaces)
                    // double-indexed every item covered by the delta.
                    //
                    // It DOES move the chain's head record, though — advance
                    // the index's tracked head_offset to match (content
                    // unchanged), or a later `prune_stale` (on the next
                    // load) would wrongly discard an otherwise still-correct
                    // index purely because a DeltaSnapshot happened since it
                    // was last touched.
                    self.field_indexes
                        .write()
                        .touch_head_offset(state_id, branch_id, offset);
                }
                StateOperation::Edit { index, new_value } => {
                    match serde_json::from_slice::<serde_json::Value>(new_value) {
                        Ok(value) => self.field_indexes.write().on_edit(
                            state_id,
                            branch_id,
                            offset,
                            *index as u32,
                            &value,
                        ),
                        Err(_) => self.field_indexes.write().poison_state_on(state_id, branch_id),
                    }
                }
                StateOperation::Redact { start, end } => {
                    self.field_indexes.write().on_redact(
                        state_id,
                        branch_id,
                        offset,
                        *start as u32,
                        *end as u32,
                    );
                }
                StateOperation::Set(data)
                | StateOperation::Snapshot(data)
                | StateOperation::Delta { new_value: data, .. } => {
                    // `Delta` (the Delta-strategy whole-state replace — NOT
                    // `DeltaSnapshot`, handled above) replaces the entire
                    // materialized value exactly like `Set`/`Snapshot` does
                    // (see `apply_operation`'s `Delta` arm: `Ok(new_value)`,
                    // same as `Set`). Left unhandled, a registered index
                    // would keep confidently answering queries against the
                    // value BEFORE the Delta — stale-but-confident, worse
                    // than an honest `None`. Same full-rebuild path as
                    // Set/Snapshot; same poison-on-parse-failure fallback.
                    match serde_json::from_slice::<Vec<serde_json::Value>>(data) {
                        Ok(items) => self.field_indexes.write().on_full_replace(
                            state_id,
                            branch_id,
                            offset,
                            items.iter(),
                        ),
                        Err(_) => self.field_indexes.write().poison_state_on(state_id, branch_id),
                    }
                }
                // Tree ops (path->entry maps) and Struct Field ops are not
                // ordinal arrays — field indexing (built on `by_ordinal`)
                // doesn't apply to them.
                StateOperation::TreeSet { .. }
                | StateOperation::TreeRemove { .. }
                | StateOperation::TreeBatch { .. }
                | StateOperation::TreeDeltaSnapshot(_)
                | StateOperation::Field { .. } => {}
            }
        }

        Ok(())
    }

    /// Get the current value of a state.
    ///
    /// Uses LRU cache for fast repeated access. On cache miss,
    /// reconstructs state by traversing the chain from disk — unless another
    /// branch already holds the value for the same chain head, in which case
    /// that value is shared.
    pub fn get_state(&self, branch_id: BranchId, state_id: &str) -> Result<Option<Vec<u8>>> {
        Ok(self
            .get_state_shared(branch_id, state_id)?
            .map(|state| state.data.clone()))
    }

    /// `get_state` without the final copy: the cached bytes themselves.
    fn get_state_shared(
        &self,
        branch_id: BranchId,
        state_id: &str,
    ) -> Result<Option<Arc<StateBytes>>> {
        let index = self.index.read();

        let head = match index.heads.get(branch_id, state_id) {
            Some(h) => h.clone(),
            None => return Ok(None),
        };
        drop(index);

        let cache_key = format!("{}:{}", branch_id.0, state_id);

        // Check cache: this branch's entry, or a value another branch holds
        // at the same head offset.
        if let Some(cached) = self.cache.write().get(&cache_key, head.head_offset) {
            return Ok(Some(cached));
        }

        // The per-item form may be current when the bytes are not (it is
        // kept across writes, the bytes are dropped). Joining canonical
        // items yields exactly the bytes a reconstruction would serialize,
        // without touching the log. Not cached: the items already hold the
        // data, and a second full copy per branch is what this avoids.
        if let Some(items) = self.items_cache.read().peek(&cache_key, head.head_offset) {
            if items.is_canonical() {
                return Ok(Some(Arc::new(StateBytes {
                    data: Self::join_items(&items, 0, items.len()),
                    canonical: true,
                })));
            }
        }

        // Cache miss - reconstruct from disk
        let log = self
            .log
            .as_ref()
            .ok_or_else(|| StoreError::NotInitialized)?;

        let (data, canonical) = self.reconstruct_from_disk(log, head.head_offset)?;
        let value = Arc::new(StateBytes { data, canonical });

        // Cache the result
        self.cache
            .write()
            .put(cache_key, head.head_offset, Arc::clone(&value));

        Ok(Some(value))
    }

    /// Items `start..end` re-joined as a JSON array (item bytes verbatim).
    pub(crate) fn join_items(items: &StateItems, start: usize, end: usize) -> Vec<u8> {
        let total: usize = items.range(start, end).map(|i| i.len() + 1).sum();
        let mut out = Vec::with_capacity(total + 2);
        out.push(b'[');
        for (i, item) in items.range(start, end).enumerate() {
            if i > 0 {
                out.push(b',');
            }
            out.extend_from_slice(item);
        }
        out.push(b']');
        out
    }

    /// The value the per-item cache holds for the state after `operation`,
    /// derived from `items`, the value it held before. `None` when the new
    /// value cannot be derived (the entry is then dropped and the next read
    /// re-materializes, as every write used to cause).
    ///
    /// Derivation must produce exactly what re-materializing would.
    /// Materialization decodes every item and serializes it again, so the
    /// base is first brought into that form (`StateItems::into_canonical` —
    /// a no-op for a value that already is, and otherwise computed once and
    /// shared by every branch deriving from the same inherited value), and
    /// an appended or edited item is stored re-serialized.
    fn advance_items(
        items: Arc<StateItems>,
        operation: &StateOperation,
        content_preserving: bool,
    ) -> Option<Arc<StateItems>> {
        fn reserialize(item: &[u8]) -> Option<Arc<[u8]>> {
            let value: serde_json::Value = serde_json::from_slice(item).ok()?;
            serde_json::to_vec(&value).ok().map(Arc::from)
        }

        match operation {
            StateOperation::Snapshot(_) | StateOperation::DeltaSnapshot(_)
                if content_preserving =>
            {
                Some(items)
            }
            StateOperation::Append(item) => {
                let item = reserialize(item)?;
                let mut items = items.into_canonical()?;
                Arc::make_mut(&mut items).push(item);
                Some(items)
            }
            StateOperation::AppendMany(batch) => {
                let batch: Vec<Arc<[u8]>> = batch
                    .iter()
                    .map(|item| reserialize(item))
                    .collect::<Option<_>>()?;
                let mut items = items.into_canonical()?;
                let target = Arc::make_mut(&mut items);
                for item in batch {
                    target.push(item);
                }
                Some(items)
            }
            StateOperation::Edit { index, new_value } => {
                if *index >= items.len() {
                    return None;
                }
                let item = reserialize(new_value)?;
                let mut items = items.into_canonical()?;
                Arc::make_mut(&mut items).set(*index, item);
                Some(items)
            }
            StateOperation::Redact { start, end } => {
                let mut items = items.into_canonical()?;
                Arc::make_mut(&mut items).remove_range(*start, *end);
                Some(items)
            }
            _ => None,
        }
    }

    /// Get the current value of an array-shaped (AppendLog) state as
    /// per-item raw JSON byte spans.
    ///
    /// The full state is materialized at most once; the split result is
    /// cached and shared out as `Arc`, so item and slice reads are
    /// O(returned data) instead of O(state size). Validity is governed by
    /// the same head_offset discipline as `get_state`. A write to the state
    /// on this branch moves the head; the cached items follow it when the
    /// new value can be derived from them (Append, AppendMany, Edit, Redact,
    /// and the store's own snapshots), and are otherwise dropped so the next
    /// read re-materializes. A branch whose head is at an offset another
    /// branch has cached shares that branch's items. No caller-side
    /// invalidation exists.
    ///
    /// Returns an error if the state is not a JSON array (e.g. a Snapshot
    /// strategy state holding an object) — matching the behavior of the
    /// previous slice implementation.
    pub fn get_state_items(
        &self,
        branch_id: BranchId,
        state_id: &str,
    ) -> Result<Option<Arc<StateItems>>> {
        let index = self.index.read();
        let head = match index.heads.get(branch_id, state_id) {
            Some(h) => h.clone(),
            None => return Ok(None),
        };
        drop(index);

        let cache_key = format!("{}:{}", branch_id.0, state_id);

        // Check items cache
        if let Some(cached) = self.items_cache.write().get(&cache_key, head.head_offset) {
            return Ok(Some(cached));
        }

        // Materialize the full state once (served from the byte cache when
        // warm), then split into item spans without building a Value tree.
        let state = match self.get_state_shared(branch_id, state_id)? {
            Some(s) => s,
            None => return Ok(None),
        };

        let items = if state.data.is_empty() {
            StateItems::from_items(std::iter::empty(), state.canonical)
        } else {
            let raw: Vec<&serde_json::value::RawValue> = serde_json::from_slice(&state.data)
                .map_err(|e| StoreError::Deserialization(e.to_string()))?;
            StateItems::from_items(raw.iter().map(|r| r.get().as_bytes()), state.canonical)
        };
        let items = Arc::new(items);

        self.items_cache
            .write()
            .put(cache_key, head.head_offset, Arc::clone(&items));

        Ok(Some(items))
    }

    /// Point lookup of a single AppendLog item.
    ///
    /// Fast path: by far the most common point lookup is the just-appended
    /// LAST item — the context-manager's write-through fetches the canonical
    /// serde form immediately after every append, i.e. immediately after the
    /// write invalidated both caches. Before this method existed, that
    /// lookup fell into `get_state_items` and re-materialized the entire
    /// state on every single append (2026-08-01 Mythos: each materialization
    /// re-parsed a 114 MB snapshot chain — 10-30s CPU stalls at every turn
    /// boundary). When the head record is an `Append` (or an `AppendMany`,
    /// whose last item is the state's last) and the caller asks for the
    /// final index, the item is served from the head record alone: O(one
    /// record read) regardless of state size, no cache churn.
    ///
    /// Byte fidelity: an Append record stores exactly the bytes that
    /// materialization re-emits for that item — both sides serialize the
    /// same `serde_json::Value`, and Value objects are BTreeMap-backed so
    /// key order is canonical. Asserted by the fast/slow equivalence test.
    /// That holds for a writer that serializes its items with serde_json,
    /// as `append_to_state_json_with_identity` does; bytes written in
    /// another spelling come back as written.
    ///
    /// Fallbacks preserve the previous semantics: a warm items cache is
    /// used when valid (same head_offset discipline as `get_state_items`),
    /// and any other head or non-final index takes the full materialization
    /// path.
    pub fn get_state_item(
        &self,
        branch_id: BranchId,
        state_id: &str,
        index: usize,
    ) -> Result<Option<Vec<u8>>> {
        let head = {
            let idx = self.index.read();
            match idx.heads.get(branch_id, state_id) {
                Some(h) => h.clone(),
                None => return Ok(None),
            }
        };

        // Warm cache wins: O(1) and avoids even the head-record read.
        let cache_key = format!("{}:{}", branch_id.0, state_id);
        if let Some(cached) = self.items_cache.write().get(&cache_key, head.head_offset) {
            return Ok(cached.get(index).map(|item| item.to_vec()));
        }

        // Head-record fast path: last item + Append head.
        if head.item_count > 0 && index == head.item_count - 1 {
            let log = self
                .log
                .as_ref()
                .ok_or_else(|| StoreError::NotInitialized)?;
            let record = log.read_at(head.head_offset)?;
            let update = StateUpdateRecord::decode(&record)?;
            match update.operation {
                StateOperation::Append(item) => return Ok(Some(item)),
                // A batch at the head ends with the last item, as the last
                // of its items appended one at a time would.
                StateOperation::AppendMany(mut items) if !items.is_empty() => {
                    return Ok(items.pop());
                }
                // Non-Append head (Edit / Redact / snapshot / Set): the last
                // item isn't recoverable from the head record alone.
                _ => {}
            }
        }

        // Slow path: full materialization (result cached for later reads).
        match self.get_state_items(branch_id, state_id)? {
            Some(items) => Ok(items.get(index).map(|item| item.to_vec())),
            None => Ok(None),
        }
    }

    /// Get the current value of a Tree state as its decoded path→entry map.
    ///
    /// The map is parsed at most once per head_offset; the decoded form is
    /// cached and shared out as `Arc`. This removes the per-call full-map
    /// parse: point reads (`tree_get`) become O(log n), prefix lists
    /// O(log n + matches) via `BTreeMap::range`, and unfiltered lists remain
    /// O(n) by necessity. Note the memory trade: this is a third LRU (up to
    /// `cache_size` decoded trees, bounded by entry count, not bytes) holding
    /// decoded forms of states whose serialized bytes may also sit in the
    /// byte cache — acceptable while trees stay bounded (they are also
    /// eviction-managed by consumers), but worth revisiting if a byte-aware
    /// cache budget ever lands. Validity follows the same
    /// head_offset discipline as `get_state` — any write to the state (on this
    /// branch) produces a new head_offset and the next read re-parses. No
    /// caller-side invalidation exists.
    pub fn get_tree_state(
        &self,
        branch_id: BranchId,
        state_id: &str,
    ) -> Result<Option<Arc<TreeState>>> {
        let index = self.index.read();
        let head = match index.heads.get(branch_id, state_id) {
            Some(h) => h.clone(),
            None => return Ok(None),
        };
        drop(index);

        let cache_key = format!("{}:{}", branch_id.0, state_id);

        // Check tree cache (own entry, or one another branch holds at the
        // same head offset)
        if let Some(cached) = self.tree_cache.write().get(&cache_key, head.head_offset) {
            return Ok(Some(cached));
        }

        // Materialize the state bytes once (served from the byte cache when
        // warm), then decode the map once per head_offset.
        let state = match self.get_state_shared(branch_id, state_id)? {
            Some(s) if !s.data.is_empty() => s,
            _ => return Ok(None),
        };

        let tree: TreeState = serde_json::from_slice(&state.data)
            .map_err(|e| StoreError::Deserialization(e.to_string()))?;
        let tree = Arc::new(tree);

        self.tree_cache
            .write()
            .put(cache_key, head.head_offset, Arc::clone(&tree));

        Ok(Some(tree))
    }

    /// Reconstruct state by traversing chain from disk.
    ///
    /// For AppendLog with incremental snapshots:
    /// - Full Snapshot: Stop traversal completely, use as base
    /// - DeltaSnapshot: Consolidates ops before it; continue to find more deltas/full snapshot
    /// - Regular ops: Collect only if before any snapshot
    ///
    /// Reconstruction: base_snapshot + delta_snapshots + recent_ops
    fn reconstruct_from_disk(&self, log: &RecordLog, head_offset: u64) -> Result<(Vec<u8>, bool)> {
        self.reconstructions.fetch_add(1, Ordering::Relaxed);
        let mut operations = Vec::new();
        let mut current_offset = Some(head_offset);
        let mut hit_snapshot = false; // Once we hit any snapshot, stop collecting regular ops

        // Follow chain backwards, collecting operations
        while let Some(offset) = current_offset {
            let record = log.read_at(offset)?;

            // Parse the state update from the record payload
            let update = StateUpdateRecord::decode(&record)?;

            match &update.operation {
                StateOperation::Snapshot(_) | StateOperation::Set(_) => {
                    // Full-state terminal: both Snapshot and Set replace the
                    // entire state in materialize_operations, so anything older
                    // is superseded. Add it and stop. Walking past a Set was the
                    // bug that made cold reconstruction of Snapshot-strategy
                    // states O(full chain) — those states are written via Set
                    // and never get a periodic Snapshot, so the walk ran to
                    // sequence 0. Mirrors get_state_tail's Set handling.
                    operations.push(update.operation.clone());
                    break;
                }
                StateOperation::DeltaSnapshot(_) | StateOperation::TreeDeltaSnapshot(_) => {
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

        // Apply operations in forward order (reverse of collection order),
        // decoding once and encoding once — per-op application made cold
        // reconstruction O(tail × N).
        operations.reverse();
        crate::state::materialize_operations_tagged(operations)
    }

    /// Get the chain head for a state.
    pub fn get_head(&self, branch_id: BranchId, state_id: &str) -> Option<StateChainHead> {
        self.index.read().heads.get(branch_id, state_id).cloned()
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

        let head = match index.heads.get(branch_id, state_id) {
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
                // Check if full snapshot is needed first.
                // Size-aware spacing: a full snapshot embeds the whole state, so
                // fixed-interval fulls cost O(N²/interval) disk on growing states
                // (issue #11). Grow the interval with the state: snapshot when the
                // ops covered since the last full reach the state's size at that
                // full (doubling), with the configured interval as the floor so
                // small states keep the configured cadence.
                let ops_covered_since_full = head.delta_snapshots_since_full
                    * delta_snapshot_every
                    + head.ops_since_delta_snapshot;
                let full_interval = delta_snapshot_every
                    .saturating_mul(*full_snapshot_every)
                    .max(head.item_count_at_last_full as u64);
                if ops_covered_since_full >= full_interval {
                    Some(SnapshotNeeded::Full)
                } else if head.ops_since_delta_snapshot >= *delta_snapshot_every {
                    // Delta snapshots only track Append operations, so after a
                    // non-Append op (Edit, Redact, Set) no delta may be taken
                    // until a full resets the chain — a delta would walk past
                    // the mutation and resurrect older values. We do NOT force
                    // an early full for it (historically that meant a full
                    // snapshot of the whole state every delta interval while
                    // edits kept occurring — O(N²/D) disk): the raw tail just
                    // grows until the size-aware full above fires, and
                    // single-pass materialization keeps reading it O(N).
                    if head.has_non_append_since_snapshot {
                        None
                    } else {
                        Some(SnapshotNeeded::Delta)
                    }
                } else {
                    None
                }
            }
            StateStrategy::Tree {
                delta_snapshot_every,
                full_snapshot_every,
            } => {
                // Same size-aware full spacing as AppendLog (issue #11). The
                // baseline is `item_count_at_last_full`, stamped exactly from
                // the parsed snapshot bytes — the per-op tree estimate is not
                // consulted here.
                let ops_covered_since_full = head.delta_snapshots_since_full
                    * delta_snapshot_every
                    + head.ops_since_delta_snapshot;
                let full_interval = delta_snapshot_every
                    .saturating_mul(*full_snapshot_every)
                    .max(head.item_count_at_last_full as u64);
                if ops_covered_since_full >= full_interval {
                    Some(SnapshotNeeded::Full)
                } else if head.ops_since_delta_snapshot >= *delta_snapshot_every {
                    Some(SnapshotNeeded::Delta)
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
    ///
    /// Heads are copied verbatim, snapshot accounting included, so the new
    /// branch schedules snapshots exactly as its parent would have. With
    /// `selection` only the selected states are copied; the others have no
    /// head on the new branch and read there as registered-but-empty.
    ///
    /// Cost is proportional to the heads copied (plus a `log n` lookup per
    /// exact id or prefix), never to the number of branches or to the heads
    /// other branches hold. Returns the number of heads copied.
    pub fn copy_heads_for_branch(
        &self,
        from_branch: BranchId,
        to_branch: BranchId,
        selection: Option<&StateSelection>,
    ) -> usize {
        let mut index = self.index.write();

        let copied: BTreeMap<String, StateChainHead> = match index.heads.branch(from_branch) {
            None => return 0,
            Some(parent) => match selection {
                None => parent.clone(),
                // Collecting into a map dedups a state named twice (by id
                // and by prefix, or by overlapping prefixes).
                Some(selection) => Self::select_heads(parent, selection)
                    .map(|(state_id, head)| (state_id.clone(), head.clone()))
                    .collect(),
            },
        };

        let count = copied.len();
        if count > 0 {
            for (state_id, head) in &copied {
                self.share_cached(to_branch, state_id, head.head_offset);
            }
            index
                .heads
                .by_branch
                .entry(to_branch)
                .or_default()
                .extend(copied);
        }
        count
    }

    /// Give `branch_id` a reference to whatever is already cached for the
    /// chain head `head_offset` it has just been handed.
    ///
    /// Without this a new branch would only find its parent's cached value
    /// if it happened to read before the parent's next write moved the
    /// parent's entry on. Holding its own reference pins the value the
    /// branch actually inherited. Limited to the per-item and decoded-tree
    /// forms, which share structure with their successors (pinning one costs
    /// a few pointers); the serialized bytes are a flat copy per version and
    /// are shared only on demand, by a read at the same offset.
    fn share_cached(&self, branch_id: BranchId, state_id: &str, head_offset: u64) {
        let cache_key = format!("{}:{}", branch_id.0, state_id);
        self.items_cache.write().adopt(cache_key.clone(), head_offset);
        self.tree_cache.write().adopt(cache_key, head_offset);
    }

    /// The entries of one branch's head map that `selection` covers. A state
    /// covered more than once is yielded more than once; callers dedup.
    fn select_heads<'a>(
        heads: &'a BTreeMap<String, StateChainHead>,
        selection: &'a StateSelection,
    ) -> impl Iterator<Item = (&'a String, &'a StateChainHead)> + 'a {
        let exact = selection
            .state_ids
            .iter()
            .filter_map(move |id| heads.get_key_value(id.as_str()));
        let prefixed = selection.state_prefixes.iter().flat_map(move |prefix| {
            heads
                .range::<str, _>((
                    std::ops::Bound::Included(prefix.as_str()),
                    std::ops::Bound::Unbounded,
                ))
                .take_while(move |(id, _)| id.starts_with(prefix.as_str()))
        });
        exact.chain(prefixed)
    }

    /// The heads of `branch_id` covered by `selection` (all of them when
    /// `selection` is `None`), each state once, ordered by state id.
    pub fn selected_heads(
        &self,
        branch_id: BranchId,
        selection: Option<&StateSelection>,
    ) -> Vec<(String, StateChainHead)> {
        let index = self.index.read();
        let heads = match index.heads.branch(branch_id) {
            Some(h) => h,
            None => return Vec::new(),
        };
        match selection {
            None => heads.iter().map(|(id, h)| (id.clone(), h.clone())).collect(),
            Some(selection) => Self::select_heads(heads, selection)
                .map(|(id, h)| (id.clone(), h.clone()))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        }
    }

    /// Install a complete chain head for a branch.
    ///
    /// Used by `create_branch_at` to point a new branch's state head at an
    /// existing position in the parent's chain, without writing new records.
    /// The caller supplies the whole head — including the snapshot accounting
    /// the chain had at that position — because the accounting decides which
    /// kind of snapshot the branch may take next: starting a branch with
    /// "no non-append ops since the last snapshot" while its inherited chain
    /// holds an uncovered Edit/Redact/Set lets an append-only delta snapshot
    /// walk past that mutation and lose it.
    pub fn insert_head_for_branch(&self, branch_id: BranchId, state_id: &str, head: StateChainHead) {
        let mut index = self.index.write();
        self.share_cached(branch_id, state_id, head.head_offset);
        index.heads.insert((branch_id, state_id.to_string()), head);
    }

    /// Register (or refresh) a secondary index on a JSON field of every item
    /// currently in `state_id` (materialized on `branch_id`), extracted via
    /// the JSON-pointer `field_path`. Idempotent: a no-op if an index for
    /// this `(state_id, field_path)` is already registered on `branch_id`,
    /// fresh (the slot's current `head_offset`), and of the same `kind` —
    /// see `FieldIndexManager::register`. Each branch holds its own index:
    /// registering here neither replaces nor disturbs one another branch
    /// holds for the same field (see `FieldIndexManager`'s module docs).
    ///
    /// When another branch already holds this index at the very `head_offset`
    /// this branch's slot is at — a branch that has not written to a slot it
    /// inherited, registering what its parent registered — that index is
    /// copied instead of rebuilt: no materialization, no JSON decode.
    ///
    /// The freshness check runs BEFORE this materializes/parses the slot's
    /// JSON — `head_offset` alone (already cheap: an index lookup, not a
    /// chain walk) is enough to answer "already fresh", so the common
    /// already-registered case (callers, e.g. context-manager's
    /// `MessageStore`, that call this unconditionally on every boot) never
    /// pays for decoding a large slot just to throw the result away. Only a
    /// genuine rebuild reaches `get_state`/`serde_json::from_slice` below.
    ///
    /// Callers must hold `Store::write_lock` across this call (as every
    /// other state-mutating path already does) so it can't race a
    /// concurrent `record_update` and double-count the in-flight item.
    pub fn register_field_index(
        &self,
        branch_id: BranchId,
        state_id: &str,
        field_path: &str,
        kind: FieldIndexKind,
    ) -> Result<()> {
        let head_offset = self.get_head(branch_id, state_id).map(|h| h.head_offset);

        if self
            .field_indexes
            .read()
            .is_fresh(state_id, field_path, kind, branch_id, head_offset)
        {
            return Ok(());
        }

        if self.field_indexes.write().register_from_sibling(
            state_id,
            field_path,
            kind,
            branch_id,
            head_offset,
        )? {
            return Ok(());
        }

        // Decode from the per-item cache when it is current (no log walk,
        // no second copy of the slot); otherwise materialize.
        let cached_items = head_offset.and_then(|offset| {
            let cache_key = format!("{}:{}", branch_id.0, state_id);
            self.items_cache.write().get(&cache_key, offset)
        });
        let items: Vec<serde_json::Value> = match cached_items {
            Some(cached) => cached
                .iter()
                .map(serde_json::from_slice)
                .collect::<std::result::Result<_, _>>()
                .map_err(|e| StoreError::Deserialization(e.to_string()))?,
            None => match self.get_state_shared(branch_id, state_id)? {
                Some(state) if !state.data.is_empty() => serde_json::from_slice(&state.data)
                    .map_err(|e| StoreError::Deserialization(e.to_string()))?,
                _ => Vec::new(),
            },
        };

        self.field_indexes.write().register(
            state_id,
            field_path,
            kind,
            branch_id,
            head_offset,
            items.iter(),
        )
    }

    /// Query ordinals of a registered `Number` field index within
    /// `[gte, lte]` (either bound optional), scoped to `branch_id`. Returns
    /// `None` if no such index is currently registered FOR `branch_id`
    /// (never registered there, wrong kind, or poisoned by a parse failure;
    /// an index another branch holds is never served — a pure read after
    /// `switch_branch` with no intervening write must never see another
    /// branch's ordinals) — distinct from `Some(vec![])`, an
    /// index that exists, matches this branch, and has no matches.
    #[allow(clippy::too_many_arguments)]
    pub fn query_field_index_range(
        &self,
        branch_id: BranchId,
        state_id: &str,
        field_path: &str,
        gte: Option<f64>,
        lte: Option<f64>,
        limit: Option<usize>,
        offset: Option<usize>,
        reverse: bool,
    ) -> Option<Vec<u32>> {
        self.field_indexes
            .read()
            .query_range(state_id, field_path, branch_id, gte, lte, limit, offset, reverse)
    }

    /// Query ordinals of a registered `String` field index equal to `value`,
    /// scoped to `branch_id`. Returns `None` if no such index is currently
    /// registered for `branch_id` — see `query_field_index_range`'s doc for
    /// the `None` vs `Some(vec![])` distinction and the branch check.
    pub fn query_field_index_eq(
        &self,
        branch_id: BranchId,
        state_id: &str,
        field_path: &str,
        value: &str,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Option<Vec<u32>> {
        self.field_indexes
            .read()
            .query_eq(state_id, field_path, branch_id, value, limit, offset)
    }

    /// Distinct values and ordinal counts for a registered `String` field
    /// index, scoped to `branch_id`. Returns `None` if no such index is
    /// currently registered for `branch_id`.
    pub fn field_index_value_counts(
        &self,
        branch_id: BranchId,
        state_id: &str,
        field_path: &str,
    ) -> Option<Vec<(String, u32)>> {
        self.field_indexes.read().value_counts(state_id, field_path, branch_id)
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
        self.items_cache.write().clear();
        self.tree_cache.write().clear();
    }

    /// How many times a state value has been rebuilt from the log since this
    /// manager was created. A read served from a cache does not count.
    pub fn reconstruction_count(&self) -> u64 {
        self.reconstructions.load(Ordering::Relaxed)
    }

    /// Get cache statistics.
    pub fn cache_len(&self) -> usize {
        self.cache.read().len()
    }

    /// Release what is held for a branch that has just been deleted.
    ///
    /// Derived data goes at once: the branch's cache entries (values other
    /// branches still reference stay alive for them) and its field indexes.
    ///
    /// Its chain heads are only MARKED here and removed by
    /// `purge_retired_branches`, which the store calls after it has saved
    /// the branch list. `state.bin` and `branches.bin` are separate files
    /// written one after the other; if the heads left `state.bin` first, a
    /// crash between the two writes would bring the branch back (still
    /// listed in `branches.bin`) with every state empty. Keeping them until
    /// the deletion itself is durable means a crash can only bring the
    /// branch back whole, or leave unreferenced heads behind — never a
    /// branch stripped of its state.
    pub fn retire_branch(&self, branch_id: BranchId) {
        let prefix = format!("{}:", branch_id.0);
        self.cache.write().pop_prefix(&prefix);
        self.items_cache.write().pop_prefix(&prefix);
        self.tree_cache.write().pop_prefix(&prefix);
        self.field_indexes.write().drop_branch(branch_id);
        self.retired_branches.lock().push(branch_id);
    }

    /// Remove the chain heads of every branch retired so far. Call only
    /// once the deletion of those branches has been durably saved. Returns
    /// the number of heads removed. The next `save` writes a `state.bin`
    /// without them.
    pub fn purge_retired_branches(&self) -> usize {
        let retired: Vec<BranchId> = std::mem::take(&mut *self.retired_branches.lock());
        if retired.is_empty() {
            return 0;
        }
        let mut index = self.index.write();
        retired
            .into_iter()
            .filter_map(|branch_id| index.heads.remove_branch(branch_id))
            .map(|heads| heads.len())
            .sum()
    }

    /// Total number of chain heads held, across all branches.
    pub fn head_count(&self) -> usize {
        self.index.read().heads.len()
    }

    /// Get compaction statistics for a state.
    ///
    /// Returns the number of operations that would be eliminated by compaction,
    /// and the offset of the oldest operation that's still needed.
    pub fn get_compaction_stats(&self, branch_id: BranchId, state_id: &str) -> Option<CompactionStats> {
        let index = self.index.read();
        let head = index.heads.get(branch_id, state_id)?;
        // Each delta snapshot covers delta_snapshot_every ops; a strategy
        // without delta snapshots never increments delta_snapshots_since_full,
        // so the multiplier is moot there.
        let delta_every = match index.strategies.get(state_id) {
            Some(StateStrategy::AppendLog { delta_snapshot_every, .. })
            | Some(StateStrategy::Tree { delta_snapshot_every, .. }) => *delta_snapshot_every,
            _ => 1,
        };

        Some(CompactionStats {
            ops_since_last_full_snapshot: head.delta_snapshots_since_full * delta_every
                + head.ops_since_delta_snapshot,
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
        let head = match index.heads.get(branch_id, state_id) {
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

            let update = StateUpdateRecord::decode(&record)?;

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
        let index = self.index.read();
        let encoded = rmp_serde::to_vec(&*index)
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        self.checkpoint.save(
            &self.path,
            &[
                STATE_INDEX_MAGIC,
                &[STATE_INDEX_VERSION],
                &(encoded.len() as u64).to_le_bytes(),
                &encoded,
            ],
        )?;
        drop(index);

        // Field indexes live in their own file (`state-indexes.bin`), kept
        // separate from `state.bin`'s format on purpose: `state.bin` missing
        // or version-mismatched is a hard-fail (chain-head offsets are load-
        // bearing), while a missing/stale field-index file is recoverable
        // (derived data — `load` returns `None` for it, never an error).
        //
        // Written AFTER `state.bin` is already saved above, and its
        // own failure is deliberately swallowed (logged, not propagated):
        // `state.bin` is the real durable data this function exists to
        // protect, and a disk-full/permission failure writing the derived
        // index must not make `Store::sync()` report failure for data that
        // in fact saved fine — callers reading `sync()`'s `Result` as "did
        // my durable data make it to disk" would otherwise get a false
        // negative. The next successful `save()` (or a fresh
        // `register_field_index`) recovers it; nothing is lost except the
        // index itself, which is always rebuildable.
        if let Err(e) = self.field_indexes.read().save(&self.field_index_path) {
            tracing::warn!(
                error = %e,
                path = %self.field_index_path.display(),
                "failed to persist state field indexes; state.bin (the durable data) synced \
                 successfully regardless — the field index will be stale until the next \
                 successful save or a fresh register_field_index call"
            );
        }

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

        // Field indexes: missing/unparseable/version-mismatched is NOT
        // fatal (unlike the state index above) — `FieldIndexManager::load`
        // already returns `None` for all of those cases, and an absent
        // field-index file (e.g. a store written before this feature
        // existed) just starts with nothing registered.
        //
        // A file that DOES parse can still be stale: `save()` writes
        // `state.bin` first and deliberately swallows a later field-index
        // save failure, so a crash in that window — or a structurally valid
        // but older `state-indexes.bin` restored from a backup — parses
        // fine while no longer matching the chain `self.index` above was
        // just reconstructed from. `load()` alone can't catch that (it has
        // no view of the real chain); `prune_stale` closes the gap here,
        // dropping any index whose stored head_offset doesn't match what
        // the just-loaded `StateIndex` reports right now for that
        // `(state_id, branch_id)` — the promise that "a stale index starts
        // empty" only holds if this runs before the loaded manager is
        // trusted.
        if let Some(mut field_indexes) = FieldIndexManager::load(&self.field_index_path)? {
            let current_index = self.index.read();
            field_indexes.prune_stale(|state_id, branch_id| {
                current_index
                    .heads
                    .get(branch_id, state_id)
                    .map(|h| h.head_offset)
            });
            drop(current_index);
            *self.field_indexes.write() = field_indexes;
        }

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

    /// A batch moves the chain head by one operation and all of its items,
    /// stays append-only for the snapshot schedule, and materializes as its
    /// items in order after what came before.
    #[test]
    fn test_append_many_head_accounting() {
        let (_dir, log, manager) = setup_test();
        manager
            .register_state(StateRegistration {
                id: "items".to_string(),
                strategy: StateStrategy::AppendLog {
                    delta_snapshot_every: 2,
                    full_snapshot_every: 100,
                },
                initial_value: None,
            })
            .unwrap();

        let first = StateOperation::Append(b"1".to_vec());
        let offset1 = append_state_update(&log, "items", None, first.clone(), 1);
        manager.record_update(TEST_BRANCH, "items", offset1, &first).unwrap();

        let batch = StateOperation::AppendMany(vec![b"2".to_vec(), b"3".to_vec(), b"4".to_vec()]);
        let offset2 = append_state_update(&log, "items", Some(offset1), batch.clone(), 2);
        manager.record_update(TEST_BRANCH, "items", offset2, &batch).unwrap();

        let head = manager.get_head(TEST_BRANCH, "items").unwrap();
        assert_eq!(head.head_offset, offset2);
        assert_eq!(head.item_count, 4);
        assert_eq!(head.ops_since_delta_snapshot, 2);
        assert!(!head.has_non_append_since_snapshot);
        assert_eq!(manager.snapshot_needed(TEST_BRANCH, "items"), Some(SnapshotNeeded::Delta));
        assert_eq!(manager.get_state(TEST_BRANCH, "items").unwrap().unwrap(), b"[1,2,3,4]");
        assert_eq!(manager.get_state_item(TEST_BRANCH, "items", 3).unwrap().unwrap(), b"4");
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
    fn test_get_state_items_split_and_staleness() {
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

        // Nonexistent state -> None
        assert!(manager.get_state_items(TEST_BRANCH, "items").unwrap().is_none());

        let item1 = serde_json::to_vec(&serde_json::json!({"id": "a", "n": 1})).unwrap();
        let offset1 =
            append_state_update(&log, "items", None, StateOperation::Append(item1.clone()), 1);
        manager
            .record_update(TEST_BRANCH, "items", offset1, &StateOperation::Append(item1))
            .unwrap();

        let items = manager.get_state_items(TEST_BRANCH, "items").unwrap().unwrap();
        assert_eq!(items.len(), 1);
        let parsed: serde_json::Value = serde_json::from_slice(&items[0]).unwrap();
        assert_eq!(parsed["id"], "a");

        // Append again — cached items must be invalidated by head_offset change
        let item2 = serde_json::to_vec(&serde_json::json!({"id": "b", "n": 2})).unwrap();
        let offset2 = append_state_update(
            &log,
            "items",
            Some(offset1),
            StateOperation::Append(item2.clone()),
            2,
        );
        manager
            .record_update(TEST_BRANCH, "items", offset2, &StateOperation::Append(item2))
            .unwrap();

        let items = manager.get_state_items(TEST_BRANCH, "items").unwrap().unwrap();
        assert_eq!(items.len(), 2);
        let parsed: serde_json::Value = serde_json::from_slice(&items[1]).unwrap();
        assert_eq!(parsed["id"], "b");

        // Item bytes must reparse to exactly what the full state contains
        let full = manager.get_state(TEST_BRANCH, "items").unwrap().unwrap();
        let full_arr: Vec<serde_json::Value> = serde_json::from_slice(&full).unwrap();
        for (i, item) in items.iter().enumerate() {
            let v: serde_json::Value = serde_json::from_slice(item).unwrap();
            assert_eq!(v, full_arr[i]);
        }
    }

    #[test]
    fn test_get_state_items_redact_invalidation() {
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

        let mut prev = None;
        for i in 0..4u64 {
            let item = serde_json::to_vec(&serde_json::json!(i)).unwrap();
            let offset = append_state_update(
                &log,
                "items",
                prev,
                StateOperation::Append(item.clone()),
                i + 1,
            );
            manager
                .record_update(TEST_BRANCH, "items", offset, &StateOperation::Append(item))
                .unwrap();
            prev = Some(offset);
        }

        // Warm the items cache
        let items = manager.get_state_items(TEST_BRANCH, "items").unwrap().unwrap();
        assert_eq!(items.len(), 4);

        // Redact [1,3) — cache must not serve the stale 4-item split
        let redact = StateOperation::Redact { start: 1, end: 3 };
        let offset = append_state_update(&log, "items", prev, redact.clone(), 5);
        manager.record_update(TEST_BRANCH, "items", offset, &redact).unwrap();

        let items = manager.get_state_items(TEST_BRANCH, "items").unwrap().unwrap();
        assert_eq!(items.len(), 2);
        let v0: serde_json::Value = serde_json::from_slice(&items[0]).unwrap();
        let v1: serde_json::Value = serde_json::from_slice(&items[1]).unwrap();
        assert_eq!(v0, serde_json::json!(0));
        assert_eq!(v1, serde_json::json!(3));
    }

    #[test]
    fn test_get_state_items_non_array_errors() {
        let (_dir, log, manager) = setup_test();

        manager
            .register_state(StateRegistration {
                id: "obj".to_string(),
                strategy: StateStrategy::Snapshot,
                initial_value: None,
            })
            .unwrap();

        let set = StateOperation::Set(b"{\"not\":\"an array\"}".to_vec());
        let offset = append_state_update(&log, "obj", None, set.clone(), 1);
        manager.record_update(TEST_BRANCH, "obj", offset, &set).unwrap();

        assert!(manager.get_state_items(TEST_BRANCH, "obj").is_err());
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

    /// Table-driven check of the size-aware full-snapshot spacing (issue #11):
    /// feed crafted heads, assert Full/Delta/None per arm.
    #[test]
    fn test_snapshot_needed_spacing_table() {
        fn head(
            osds: u64,
            dsf: u64,
            baseline: usize,
            non_append: bool,
        ) -> StateChainHead {
            StateChainHead {
                head_offset: 1,
                ops_since_delta_snapshot: osds,
                delta_snapshots_since_full: dsf,
                last_delta_snapshot_offset: None,
                last_full_snapshot_offset: None,
                has_non_append_since_snapshot: non_append,
                item_count: baseline,
                item_count_at_last_full: baseline,
            }
        }

        // (strategy-id, head, expected) — AppendLog D=100, F=20 → floor K=2000.
        let append_log = StateStrategy::AppendLog {
            delta_snapshot_every: 100,
            full_snapshot_every: 20,
        };
        let tree = StateStrategy::Tree {
            delta_snapshot_every: 100,
            full_snapshot_every: 20,
        };
        // Saturation guard: D·F would overflow u64 (2^33 · 2^33 = 2^66);
        // saturating_mul must yield "never" rather than a tiny wrapped
        // interval that fulls on every op.
        let huge = StateStrategy::AppendLog {
            delta_snapshot_every: 1 << 33,
            full_snapshot_every: 1 << 33,
        };

        let cases: Vec<(&str, &StateStrategy, StateChainHead, Option<SnapshotNeeded>)> = vec![
            // Legacy index (baseline 0): floor K governs — full at ops_covered = 2000.
            ("legacy_full", &append_log, head(100, 19, 0, false), Some(SnapshotNeeded::Full)),
            // One op short of the floor and short of a delta boundary: nothing.
            ("legacy_below", &append_log, head(99, 19, 0, false), None),
            // Mid-interval delta boundary: delta.
            ("delta_due", &append_log, head(100, 5, 0, false), Some(SnapshotNeeded::Delta)),
            // Small state (baseline < K): configured cadence unchanged.
            ("small_state_full", &append_log, head(100, 19, 500, false), Some(SnapshotNeeded::Full)),
            // Grown state (baseline 5000 > K): full deferred until doubled...
            ("doubling_full", &append_log, head(100, 49, 5000, false), Some(SnapshotNeeded::Full)),
            // ...and a delta (not a full) where the old fixed interval would have fired.
            ("doubling_defers_fixed_interval", &append_log, head(100, 19, 5000, false), Some(SnapshotNeeded::Delta)),
            // Non-append ops block delta snapshots (a delta would resurrect
            // pre-edit values) but do NOT force an early full — the raw tail
            // rides until the size-aware full fires.
            ("non_append_blocks_delta", &append_log, head(100, 5, 0, true), None),
            // ...and the size-aware full still fires on schedule with the flag set.
            ("non_append_full_on_schedule", &append_log, head(2000, 0, 0, true), Some(SnapshotNeeded::Full)),
            // Tree arm mirrors AppendLog.
            ("tree_doubling_full", &tree, head(100, 49, 5000, false), Some(SnapshotNeeded::Full)),
            ("tree_doubling_defers", &tree, head(100, 19, 5000, false), Some(SnapshotNeeded::Delta)),
            ("tree_below", &tree, head(50, 10, 5000, false), None),
            // Overflow-prone config: no spurious full from a wrapped interval.
            ("saturating_interval", &huge, head(1000, 0, 0, false), None),
        ];

        for (name, strategy, h, expected) in cases {
            let (_dir, _log, manager) = setup_test();
            manager
                .register_state(StateRegistration {
                    id: name.to_string(),
                    strategy: (*strategy).clone(),
                    initial_value: None,
                })
                .unwrap();
            manager
                .index
                .write()
                .heads
                .insert((TEST_BRANCH, name.to_string()), h);
            assert_eq!(
                manager.snapshot_needed(TEST_BRANCH, name),
                expected,
                "case {}",
                name
            );
        }
    }

    /// Wire-format compat: heads are persisted positionally (rmp_serde compact
    /// mode), so fields appended with #[serde(default)] must deserialize to
    /// their defaults when reading an index written before they existed.
    /// Guards the append-only constraint documented on StateChainHead.
    #[test]
    fn test_state_chain_head_trailing_fields_default() {
        /// The pre-#11 layout (through `item_count`), same field order.
        #[derive(Serialize)]
        struct HeadV2 {
            head_offset: u64,
            ops_since_delta_snapshot: u64,
            delta_snapshots_since_full: u64,
            last_delta_snapshot_offset: Option<u64>,
            last_full_snapshot_offset: Option<u64>,
            has_non_append_since_snapshot: bool,
            item_count: usize,
        }
        /// The layout before `item_count` existed.
        #[derive(Serialize)]
        struct HeadV1 {
            head_offset: u64,
            ops_since_delta_snapshot: u64,
            delta_snapshots_since_full: u64,
            last_delta_snapshot_offset: Option<u64>,
            last_full_snapshot_offset: Option<u64>,
        }

        let v2 = HeadV2 {
            head_offset: 42,
            ops_since_delta_snapshot: 7,
            delta_snapshots_since_full: 3,
            last_delta_snapshot_offset: Some(40),
            last_full_snapshot_offset: Some(10),
            has_non_append_since_snapshot: true,
            item_count: 123,
        };
        let bytes = rmp_serde::to_vec(&v2).unwrap();
        let head: StateChainHead = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(head.head_offset, 42);
        assert_eq!(head.item_count, 123);
        assert_eq!(head.item_count_at_last_full, 0, "new field must default");

        let v1 = HeadV1 {
            head_offset: 42,
            ops_since_delta_snapshot: 7,
            delta_snapshots_since_full: 3,
            last_delta_snapshot_offset: None,
            last_full_snapshot_offset: None,
        };
        let bytes = rmp_serde::to_vec(&v1).unwrap();
        let head: StateChainHead = rmp_serde::from_slice(&bytes).unwrap();
        assert!(!head.has_non_append_since_snapshot);
        assert_eq!(head.item_count, 0);
        assert_eq!(head.item_count_at_last_full, 0);
    }
    /// `state.bin` compatibility: `BranchHeads` must read the flat
    /// `(branch_id, state_id) -> head` map every existing store holds, and
    /// write a map the previous in-memory layout reads back unchanged.
    #[test]
    fn test_state_index_wire_format_is_flat_map() {
        /// `StateIndex` exactly as it was declared through 0.4.0.
        #[derive(Serialize, Deserialize)]
        struct LegacyStateIndex {
            heads: HashMap<(BranchId, String), StateChainHead>,
            strategies: HashMap<String, StateStrategy>,
        }

        fn head(n: u64) -> StateChainHead {
            StateChainHead {
                head_offset: n,
                ops_since_delta_snapshot: n + 1,
                delta_snapshots_since_full: n + 2,
                last_delta_snapshot_offset: Some(n + 3),
                last_full_snapshot_offset: if n % 2 == 0 { Some(n + 4) } else { None },
                has_non_append_since_snapshot: n % 2 == 1,
                item_count: (n + 5) as usize,
                item_count_at_last_full: (n + 6) as usize,
            }
        }

        let mut legacy = LegacyStateIndex {
            heads: HashMap::new(),
            strategies: HashMap::new(),
        };
        let mut n = 0;
        for branch in [1u64, 2, 7, 300] {
            for state in ["messages", "agent/a/tree", "", "ünïcode"] {
                legacy.heads.insert((BranchId(branch), state.to_string()), head(n));
                n += 1;
            }
        }
        legacy.strategies.insert("messages".to_string(), StateStrategy::Snapshot);
        legacy.strategies.insert(
            "agent/a/tree".to_string(),
            StateStrategy::Tree { delta_snapshot_every: 5, full_snapshot_every: 3 },
        );

        // Old bytes -> new layout.
        let legacy_bytes = rmp_serde::to_vec(&legacy).unwrap();
        let index: StateIndex = rmp_serde::from_slice(&legacy_bytes).unwrap();
        assert_eq!(index.heads.len(), legacy.heads.len());
        for ((branch, state), expected) in &legacy.heads {
            let got = index.heads.get(*branch, state).expect("head survived");
            assert_eq!(format!("{got:?}"), format!("{expected:?}"));
        }
        assert_eq!(index.strategies.len(), 2);

        // New bytes -> old layout.
        let new_bytes = rmp_serde::to_vec(&index).unwrap();
        assert_eq!(
            new_bytes.len(),
            legacy_bytes.len(),
            "same encoding, entry order aside"
        );
        let back: LegacyStateIndex = rmp_serde::from_slice(&new_bytes).unwrap();
        assert_eq!(back.heads.len(), legacy.heads.len());
        for (key, expected) in &legacy.heads {
            assert_eq!(format!("{:?}", back.heads[key]), format!("{expected:?}"));
        }
        assert_eq!(back.strategies.len(), 2);

        // An empty index round-trips too (a store with no state writes yet).
        let empty = rmp_serde::to_vec(&StateIndex::default()).unwrap();
        let legacy_empty = rmp_serde::to_vec(&LegacyStateIndex {
            heads: HashMap::new(),
            strategies: HashMap::new(),
        })
        .unwrap();
        assert_eq!(empty, legacy_empty);
        assert!(rmp_serde::from_slice::<StateIndex>(&legacy_empty).unwrap().heads.is_empty());
    }

    #[test]
    fn test_copy_heads_selection() {
        let (_dir, _log, manager) = setup_test();
        let parent = BranchId(1);
        let other = BranchId(9);
        let head = |n: u64| StateChainHead {
            head_offset: n,
            ops_since_delta_snapshot: 3,
            delta_snapshots_since_full: 2,
            last_delta_snapshot_offset: Some(n),
            last_full_snapshot_offset: Some(1),
            has_non_append_since_snapshot: true,
            item_count: 10,
            item_count_at_last_full: 4,
        };
        for (i, id) in ["a/1", "a/2", "a0", "ab", "b/1", "shared"].iter().enumerate() {
            manager.insert_head_for_branch(parent, id, head(100 + i as u64));
            manager.insert_head_for_branch(other, id, head(900 + i as u64));
        }

        let ids = |branch| -> Vec<String> {
            manager.selected_heads(branch, None).into_iter().map(|(id, _)| id).collect()
        };

        // Everything.
        assert_eq!(manager.copy_heads_for_branch(parent, BranchId(2), None), 6);
        assert_eq!(ids(BranchId(2)), ids(parent));

        // Exact ids and prefixes, overlapping, plus names that match nothing.
        let selection = StateSelection {
            state_ids: vec!["shared".into(), "a/1".into(), "missing".into()],
            state_prefixes: vec!["a/".into(), "a/1".into(), "zzz".into()],
        };
        assert_eq!(manager.copy_heads_for_branch(parent, BranchId(3), Some(&selection)), 3);
        assert_eq!(ids(BranchId(3)), vec!["a/1", "a/2", "shared"]);
        // Verbatim, accounting included, and taken from the parent — not
        // from another branch holding the same state ids.
        for (id, got) in manager.selected_heads(BranchId(3), None) {
            let expected = manager.get_head(parent, &id).unwrap();
            assert_eq!(format!("{got:?}"), format!("{expected:?}"));
        }
        assert_eq!(
            manager
                .selected_heads(parent, Some(&selection))
                .into_iter()
                .map(|(id, _)| id)
                .collect::<Vec<_>>(),
            vec!["a/1", "a/2", "shared"]
        );

        // Empty selection inherits nothing; so does an unknown parent.
        assert_eq!(
            manager.copy_heads_for_branch(parent, BranchId(4), Some(&StateSelection::default())),
            0
        );
        assert!(ids(BranchId(4)).is_empty());
        assert_eq!(manager.copy_heads_for_branch(BranchId(77), BranchId(5), None), 0);

        // The empty prefix selects every state.
        let all = StateSelection { state_ids: vec![], state_prefixes: vec![String::new()] };
        assert_eq!(manager.copy_heads_for_branch(parent, BranchId(6), Some(&all)), 6);
    }
}
