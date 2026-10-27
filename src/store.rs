//! Main Store struct tying all components together.

use crate::blobs::BlobStorage;
use crate::branches::BranchManager;
use crate::error::{Result, StoreError};
use crate::records::{RecordIndex, RecordLog, RecoveryReport};
use crate::state::StateManager;
use crate::subscriptions::{SubscriptionConfig, SubscriptionHandle, SubscriptionId, SubscriptionManager};
use crate::types::{
    Blob, Branch, Hash, Record, RecordId, RecordInput, Sequence,
    StateOperation, StateRegistration, StateUpdateRecord, StoreStats, Timestamp,
    TreeChange, TreeEntry, TreeOp, TreeState,
};
use fs2::FileExt;
use parking_lot::Mutex;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
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

/// Admission state protected by the store's write mutex.
#[derive(Default)]
struct WriteState {
    /// Metadata-only mutations cannot be replayed from records.log. If their
    /// checkpoint failed, finish it before allowing another mutation to use
    /// the in-memory definition. Reads may still inspect that tentative state.
    checkpoint_required: bool,

    /// A branch creation or deletion that no checkpoint has persisted yet.
    /// Subscribers hear of it once one does, as its caller does. At most one
    /// is ever pending, since a mutation first finishes a failed checkpoint.
    pending_branch_event: Option<BranchEvent>,
}

/// A branch change to announce once a checkpoint persists it.
enum BranchEvent {
    Created { branch: Branch, parent: Option<String> },
    Deleted { name: String },
}

struct ChainInfoAt {
    head_offset: u64,
    item_count: usize,
    non_append_since_snapshot: bool,
}

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
    write_lock: Mutex<WriteState>,

    /// When false, `update_state` skips the auto-snapshot pass. Bulk loaders
    /// (e.g. importers) turn this off to avoid the O(N²) snapshot series and
    /// take a single compacting snapshot at the end via `compact_state`.
    auto_snapshot_enabled: AtomicBool,

    /// What `open` had to recover, for [`Store::recovery`].
    recovery: Option<RecoveryReport>,
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
            write_lock: Mutex::new(WriteState::default()),
            auto_snapshot_enabled: AtomicBool::new(true),
            recovery: None,
        })
    }

    /// Open an existing store.
    pub fn open(config: StoreConfig) -> Result<Self> {
        // Verify manifest
        Self::verify_manifest(&config.path)?;

        // Acquire lock
        let lock_file = Self::acquire_lock(&config.path)?;

        // With the lock held, no save of this store is in progress, so its
        // own staging files here were left by interrupted saves. The sweep's
        // doc says what else it can reach.
        crate::atomic_file::remove_staging_files(&config.path);

        // Open components
        let log = Arc::new(RecordLog::open(config.path.join("records.log"))?);
        let blobs = BlobStorage::new(config.path.join("blobs"), config.blob_cache_size)?;
        let mut state = StateManager::load(config.path.join("state.bin"))?;
        let branches = BranchManager::load(config.path.join("branches.bin"))?;

        // Reconcile branch heads against what the durable log actually holds.
        // `branches.bin` is only flushed on sync/Drop, so after an unclean
        // shutdown it can be stale-low (log has newer records -> next append
        // would reuse a durable sequence) or point past a truncated torn tail.
        // Both are corrected here from the sequences the log scan collected.
        let reconciliation = branches.reconcile_heads(log.max_sequences());
        for fix in &reconciliation.head_fixes {
            tracing::warn!(
                branch = %fix.name,
                old_head = fix.old_head.0,
                new_head = fix.new_head.0,
                "reconciled branch head to match durable log (stale branches.bin or torn tail)"
            );
        }
        for orphan in &reconciliation.orphaned_branch_ids {
            tracing::warn!(
                branch_id = orphan.0,
                "durable log holds records for a BranchId absent from branches.bin \
                 (orphaned by a crash before metadata flush); its records are unreachable \
                 and next_id was clamped so the id is never reissued"
            );
        }

        // A crash between the state and branch checkpoint publications can
        // leave copied heads for an unpublished branch with no log records.
        // Reserve those identities too, so a new empty branch cannot inherit
        // the abandoned branch's state through an accidentally reused ID.
        if let Some(id) = state.max_branch_id() {
            branches.reserve_id_after(id)?;
        }

        // Fail loudly if state metadata points past the (possibly truncated)
        // valid log — its offsets would otherwise dereference to garbage.
        state.validate_offsets(log.size())?;

        // Surface any torn-tail recovery the log performed on open.
        if let Some(rec) = log.recovery() {
            tracing::warn!(
                truncated_at = rec.truncated_at,
                dropped_bytes = rec.dropped_bytes,
                valid_records = rec.valid_records,
                "record log recovered a torn tail on open"
            );
        }

        // Connect the state manager for disk-based chain traversal.
        state.set_log(Arc::clone(&log));

        // Recover the state-update tail while rebuilding the record index.
        // The checkpoint covers all updates through its latest head, including
        // updates on other branches. Keep this boundary fixed during replay.
        let checkpoint = state.checkpoint_offset();
        let mut records = 0u64;
        // Updates replayed per state that has no registration in state.bin.
        let mut unregistered = std::collections::BTreeMap::<String, u64>::new();
        let mut undecodable = 0u64;
        let mut first_undecodable = None;
        let mut on_missing_branches = 0u64;
        let index = RecordIndex::rebuild_from_log_with(
            config.path.join("records.idx"),
            &log,
            |offset, record| {
                records += 1;
                if record.record_type != "state_update"
                    || checkpoint.is_some_and(|saved| offset <= saved)
                {
                    return Ok(());
                }
                // Earlier releases let any append use this type, so a record
                // may not be a state update at all, whatever branch it names.
                // It stays in the log as an ordinary record.
                let Ok(update) = StateUpdateRecord::decode(record) else {
                    undecodable += 1;
                    first_undecodable.get_or_insert(offset);
                    return Ok(());
                };

                // Earlier releases saved a branch only at sync, so a crash can
                // leave updates on a branch that branches.bin doesn't hold.
                // Nothing can read that branch, so they aren't replayed;
                // recovery() counts them.
                if branches.get_branch_by_id(record.branch).is_none() {
                    on_missing_branches += 1;
                    return Ok(());
                }
                // A state can have updates and no registration: a consumer
                // may update a state it never registered, and an earlier
                // release that crashed after registering a state and before
                // its next sync left the registration out of state.bin.
                // Replay doesn't need the strategy, so the chain is kept and
                // checkpointed like any other, and register_state supplies a
                // strategy whenever it's called.
                if state.get_strategy(&update.state_id).is_none() {
                    *unregistered.entry(update.state_id.clone()).or_default() += 1;
                }
                let previous = state.get_head(record.branch, &update.state_id)
                    .map(|head| head.head_offset);
                if update.prev_update_offset != previous {
                    return Err(StoreError::Corruption(format!(
                        "cannot replay state '{}' at log offset {}: previous offset {:?} \
                         does not match checkpoint/replayed head {:?}",
                        update.state_id, offset, update.prev_update_offset, previous
                    )));
                }
                state.record_update(record.branch, &update.state_id, offset, &update.operation)
            },
        )?;

        for (state_id, updates) in &unregistered {
            tracing::warn!(
                state = %state_id,
                updates,
                "replayed updates for a state with no registration in state.bin: one written \
                 without a registration, or whose registration an earlier release lost"
            );
        }
        if let Some(first_offset) = first_undecodable {
            tracing::warn!(
                records = undecodable,
                first_offset,
                "skipped records typed state_update that don't decode as state updates; \
                 they stay in the log as ordinary records"
            );
        }
        if on_missing_branches > 0 {
            tracing::warn!(
                updates = on_missing_branches,
                "did not replay state updates on branches that branches.bin doesn't hold; \
                 nothing can read those branches"
            );
        }
        let unregistered_states: Vec<_> = unregistered.into_iter().collect();
        let recovery = match log.recovery() {
            Some(torn) => Some(RecoveryReport {
                unregistered_states,
                undecodable_state_updates: undecodable,
                updates_on_missing_branches: on_missing_branches,
                ..torn.clone()
            }),
            None if !unregistered_states.is_empty()
                || undecodable > 0
                || on_missing_branches > 0 =>
            {
                Some(RecoveryReport {
                    truncated_at: log.size(),
                    dropped_bytes: 0,
                    valid_records: records,
                    unregistered_states,
                    undecodable_state_updates: undecodable,
                    updates_on_missing_branches: on_missing_branches,
                })
            }
            None => None,
        };

        Ok(Self {
            config,
            _lock_file: lock_file,
            log,
            index,
            blobs,
            state,
            branches,
            subscriptions: SubscriptionManager::new(),
            write_lock: Mutex::new(WriteState::default()),
            auto_snapshot_enabled: AtomicBool::new(true),
            recovery,
        })
    }

    /// What the most recent open had to recover, if anything: a torn tail
    /// record it truncated (crash during append), updates it replayed for a
    /// state whose registration isn't in `state.bin`, records typed
    /// `state_update` it skipped because they don't decode as state updates,
    /// and updates it didn't replay because their branch isn't in
    /// `branches.bin`.
    ///
    /// `None` when none of these happened. A report doesn't by itself mean a
    /// tail was torn: `dropped_bytes` is 0 when nothing was. Nor does a state
    /// with no registration mean damage: a state written without one is named
    /// whenever its updates are replayed. Callers that need to know their
    /// store lost un-fsynced writes can consult this instead of scraping the
    /// `tracing` warnings out of logs.
    pub fn recovery(&self) -> Option<RecoveryReport> {
        self.recovery.clone()
    }

    /// Toggle the auto-snapshot pass inside `update_state`.
    ///
    /// While disabled, snapshot thresholds are still tracked but no snapshot
    /// record is written — reconstruction will walk the full chain on every
    /// read until a snapshot is installed. Intended for bulk loaders. Typical
    /// flow:
    ///
    ///   1. `set_auto_snapshot(false)`
    ///   2. perform bulk writes
    ///   3. `compact_state(state_id)` to install a single final snapshot
    ///   4. `set_auto_snapshot(true)` if further writes are expected
    ///
    /// Default is `true`. The flag is a plain bool with `Relaxed` ordering:
    /// it's only ever read by `update_state` itself; nothing else is
    /// synchronised through it.
    pub fn set_auto_snapshot(&self, enabled: bool) {
        self.auto_snapshot_enabled.store(enabled, Ordering::Relaxed);
    }

    /// Whether the auto-snapshot pass is currently enabled.
    pub fn auto_snapshot_enabled(&self) -> bool {
        self.auto_snapshot_enabled.load(Ordering::Relaxed)
    }

    // --- Record Operations ---

    /// Append a record to the current branch.
    /// The internal `state_update` type is reserved for `update_state`.
    pub fn append(&self, input: RecordInput) -> Result<Record> {
        if input.record_type == "state_update" {
            return Err(StoreError::InvalidOperation(
                "record type 'state_update' is reserved; use update_state instead".into(),
            ));
        }
        let mut write = self.write_lock.lock();
        self.retry_metadata_checkpoint(&mut write)?;

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

    /// Get the IDs of a type's records across all branches, in ascending ID
    /// order, which is log order.
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

    /// Register a new state and persist its strategy before returning.
    /// The log cannot reconstruct a lost registration after a crash.
    /// If the checkpoint fails, the error is returned but the registration
    /// stays in memory, and the next successful checkpoint publishes it:
    /// `sync()`, or the retry that every later mutation makes first. Calling
    /// this again retries that checkpoint first and, once it succeeds, returns
    /// `StateExists`, since the registration was kept.
    pub fn register_state(&self, registration: StateRegistration) -> Result<()> {
        let mut write = self.write_lock.lock();
        self.retry_metadata_checkpoint(&mut write)?;
        self.state.register_state(registration)?;
        write.checkpoint_required = true;
        self.sync_locked(&mut write)
    }

    /// Update the strategy parameters of an already-registered state (same
    /// kind only — see `StateManager::update_state_strategy`). Lets boot-time
    /// re-registration retune snapshot cadence on existing stores instead of
    /// being permanently pinned to the values from first registration.
    /// Checkpoints before returning. If the checkpoint fails, the error is
    /// returned but the new strategy stays in memory, and the next successful
    /// checkpoint publishes it: `sync()`, or the retry that every later
    /// mutation makes first.
    pub fn update_state_strategy(
        &self,
        state_id: &str,
        strategy: crate::types::StateStrategy,
    ) -> Result<()> {
        let mut write = self.write_lock.lock();
        self.retry_metadata_checkpoint(&mut write)?;
        self.state.update_state_strategy(state_id, strategy)?;
        write.checkpoint_required = true;
        self.sync_locked(&mut write)
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
        self.update_state_with_builder(state_id, skip_auto_snapshot, move |_, _| Ok(operation))
    }

    /// Append a JSON object to an AppendLog state, injecting the assigned
    /// record id and sequence into the payload before serialising. This lets
    /// callers write items that carry their own chronicle identity in one op,
    /// instead of the older append-then-edit pattern (which forced every
    /// threshold-triggered snapshot to be Full because the Edit flipped
    /// `has_non_append_since_snapshot`).
    ///
    /// `item` must be a JSON object. `id_field` receives the assigned record
    /// id as a string (matching the napi `JsRecord.id` shape); `sequence_field`
    /// receives the assigned sequence as a number. Existing values at those
    /// keys are overwritten.
    pub fn append_to_state_json_with_identity(
        &self,
        state_id: &str,
        item: serde_json::Value,
        id_field: &str,
        sequence_field: &str,
    ) -> Result<Record> {
        self.update_state_with_builder(state_id, false, move |id, seq| {
            let mut item = item;
            let map = match &mut item {
                serde_json::Value::Object(map) => map,
                _ => {
                    return Err(StoreError::InvalidOperation(
                        "payload must be a JSON object".into(),
                    ));
                }
            };
            map.insert(
                id_field.to_string(),
                serde_json::Value::String(id.0.to_string()),
            );
            map.insert(
                sequence_field.to_string(),
                serde_json::Value::Number(serde_json::Number::from(seq.0)),
            );
            let bytes = serde_json::to_vec(&item)?;
            Ok(StateOperation::Append(bytes))
        })
    }

    /// Core update path. Builds the operation via a closure that receives the
    /// about-to-be-assigned `(record_id, sequence)`; callers that don't need
    /// those values pass a closure that ignores them. Holds `write_lock` for
    /// the whole operation so the peeked id/seq match what `log.append`
    /// actually assigns. Validation runs after the builder, so the builder
    /// can also surface `InvalidOperation` (e.g. for non-object payloads in
    /// `append_to_state_json_with_identity`).
    fn update_state_with_builder<F>(
        &self,
        state_id: &str,
        skip_auto_snapshot: bool,
        operation_builder: F,
    ) -> Result<Record>
    where
        F: FnOnce(RecordId, Sequence) -> Result<StateOperation>,
    {
        let mut write = self.write_lock.lock();
        self.retry_metadata_checkpoint(&mut write)?;

        let branch = self.branches.current_branch();
        let prev_update_offset = self.state.get_head(branch.id, state_id).map(|h| h.head_offset);
        let next_seq = branch.head.next();
        // `peek_next_id` is only valid while `write_lock` is held — nothing
        // else can `log.append` between here and the call below.
        let next_id = self.log.peek_next_id();

        let operation = operation_builder(next_id, next_seq)?;

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
            StateOperation::TreeSet { .. }
            | StateOperation::TreeRemove { .. }
            | StateOperation::TreeBatch { .. } => {
                if !matches!(
                    self.state.get_strategy(state_id),
                    Some(crate::types::StateStrategy::Tree { .. })
                ) {
                    return Err(StoreError::InvalidOperation(format!(
                        "Tree operations require Tree strategy, state '{}' uses {:?}",
                        state_id,
                        self.state.get_strategy(state_id)
                    )));
                }
            }
            // Append, Set, Snapshot, DeltaSnapshot, TreeDeltaSnapshot - no validation needed
            _ => {}
        }

        // Create the state update record payload
        let update = StateUpdateRecord {
            record_id: RecordId(0), // Will be assigned
            global_sequence: next_seq,
            state_id: state_id.to_string(),
            prev_update_offset,
            operation: operation.clone(),
            timestamp: Timestamp::now(),
        };

        // Serialize and append. MessagePack with a stamped encoding tag —
        // StateUpdateRecord::decode gates on it, and pre-existing JSON
        // records (encoding Raw/Json) keep decoding via the fallback arm.
        let (payload, encoding) = update.encode()?;
        let mut input = RecordInput::raw("state_update", payload);
        input.encoding = encoding;

        let (record, offset) = self.log.append(input, branch.id, next_seq)?;
        // Tripwire: peek + append must agree because `write_lock` is held
        // across both. If this ever fires, the payload on disk would
        // claim id X while the record envelope says Y — silent corruption.
        // Kept on in release builds for that reason.
        assert_eq!(
            record.id, next_id,
            "record id assignment raced under write_lock",
        );

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
        if !skip_auto_snapshot && self.auto_snapshot_enabled.load(Ordering::Relaxed) {
            // Drop the lock before calling auto_snapshot to avoid deadlock
            drop(write);
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

        let mut walk = crate::state::ChainWalk::new(&self.log, head.head_offset, Some(at_sequence));
        let mut operations = (&mut walk).collect::<Result<Vec<_>>>()?;
        if walk.first_offset().is_none() {
            // State didn't exist at that sequence
            return Ok(None);
        }

        // Apply operations in forward order (single materialization pass)
        operations.reverse();
        let state = crate::state::materialize_operations(operations)?;

        Ok(Some(state))
    }

    fn find_chain_info_at(
        &self,
        branch_id: crate::types::BranchId,
        state_id: &str,
        at_sequence: Sequence,
    ) -> Result<Option<ChainInfoAt>> {
        let head = match self.state.get_head(branch_id, state_id) {
            Some(h) => h,
            None => return Ok(None),
        };

        let mut walk = crate::state::ChainWalk::new(&self.log, head.head_offset, Some(at_sequence));
        let mut operations = (&mut walk).collect::<Result<Vec<_>>>()?;
        let head_offset = match walk.first_offset() {
            Some(o) => o,
            None => return Ok(None), // No state at that sequence
        };
        let non_append_since_snapshot =
            operations.iter().any(crate::state::is_non_append_change) || walk.changed_behind_delta();

        // Reconstruct state to count items (single materialization pass)
        operations.reverse();
        let state = crate::state::materialize_operations(operations)?;

        // Count items in the resulting state using strategy-aware parsing
        let item_count = if state.is_empty() {
            0
        } else {
            match self.state.get_strategy(state_id) {
                Some(crate::types::StateStrategy::Tree { .. }) => {
                    serde_json::from_slice::<TreeState>(&state)
                        .map(|t| t.len())
                        .unwrap_or(0)
                }
                _ => {
                    serde_json::from_slice::<Vec<serde_json::Value>>(&state)
                        .map(|arr| arr.len())
                        .unwrap_or(0)
                }
            }
        };

        Ok(Some(ChainInfoAt { head_offset, item_count, non_append_since_snapshot }))
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
    /// Served from the state manager's per-item cache: the full state is
    /// materialized and split at most once per head_offset, after which
    /// slices are O(returned data). The result is the items re-joined as a
    /// JSON array (item bytes verbatim).
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
        let branch_id = self.branches.current_branch().id;
        let items = match self.state.get_state_items(branch_id, state_id)? {
            Some(i) => i,
            None => return Ok(None),
        };

        let start = offset.min(items.len());
        let end = offset.saturating_add(limit).min(items.len());

        let slice = &items[start..end];
        let total: usize = slice.iter().map(|i| i.len()).sum();
        let mut out = Vec::with_capacity(total + slice.len() + 2);
        out.push(b'[');
        for (i, item) in slice.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            out.extend_from_slice(item);
        }
        out.push(b']');
        Ok(Some(out))
    }

    /// Get a single item of an AppendLog state by index, as raw JSON bytes.
    ///
    /// O(item size) once the per-item cache is warm (see `get_state_slice`).
    /// Returns `None` if the state doesn't exist or the index is out of range.
    pub fn get_state_item(&self, state_id: &str, index: usize) -> Result<Option<Vec<u8>>> {
        let branch_id = self.branches.current_branch().id;
        // Delegates to the manager, which serves the just-appended last item
        // from the head record in O(1) instead of re-materializing the whole
        // state (see StateManager::get_state_item).
        self.state.get_state_item(branch_id, state_id, index)
    }

    /// Get the last N items from an AppendLog state.
    ///
    /// Reads back from the head only as far as it needs, decoding each
    /// snapshot or Set it takes items from. Where it meets a Redact, an Edit
    /// or a Delta, or `count` reaches the stored item count, it reads the
    /// whole value instead. Returns at most `count` items.
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
        let mut need_full_reconstruct = count >= head.item_count;

        // Collect items walking backwards until we have enough
        let mut items_collected: Vec<serde_json::Value> = Vec::new();
        let mut walk = crate::state::ChainWalk::new(&self.log, head.head_offset, None);
        let take_last = |items_collected: &mut Vec<serde_json::Value>, data: &[u8]| -> Result<()> {
            let arr: Vec<serde_json::Value> = serde_json::from_slice(data)
                .map_err(|e| StoreError::Deserialization(e.to_string()))?;
            let need = count - items_collected.len();
            let start = arr.len().saturating_sub(need);
            items_collected.extend(arr[start..].iter().rev().cloned());
            Ok(())
        };

        while !need_full_reconstruct && items_collected.len() < count {
            let op = match walk.next() {
                Some(op) => op?,
                None => break,
            };
            match &op {
                StateOperation::Append(item) => {
                    let value: serde_json::Value = serde_json::from_slice(item)
                        .map_err(|e| StoreError::Deserialization(e.to_string()))?;
                    items_collected.push(value);
                }
                StateOperation::DeltaSnapshot(data) => take_last(&mut items_collected, data)?,
                StateOperation::Snapshot(data) | StateOperation::Set(data) => {
                    take_last(&mut items_collected, data)?;
                    break; // Full state - we're done
                }
                op if crate::state::is_non_append_change(op) => {
                    need_full_reconstruct = true;
                    break;
                }
                _ => {}
            }
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

        let full = || -> Result<Option<Record>> {
            let current = self.get_state(state_id)?.unwrap_or_default();
            let record = self.update_state_internal(
                state_id,
                StateOperation::Snapshot(current),
                skip_auto,
            )?;
            Ok(Some(record))
        };

        match self.state.snapshot_needed(self.branches.current_branch().id, state_id) {
            None => Ok(None),
            Some(SnapshotNeeded::Full) => full(),
            Some(SnapshotNeeded::Delta) => {
                // Check if this is a Tree strategy (uses TreeDeltaSnapshot) or AppendLog (uses DeltaSnapshot)
                let is_tree = matches!(
                    self.state.get_strategy(state_id),
                    Some(crate::types::StateStrategy::Tree { .. })
                );
                if is_tree {
                    let delta_ops = self.compute_tree_delta_ops(state_id)?;
                    let record = self.update_state_internal(
                        state_id,
                        StateOperation::TreeDeltaSnapshot(delta_ops),
                        skip_auto,
                    )?;
                    Ok(Some(record))
                } else {
                    match self.compute_delta_items(state_id)? {
                        Some(delta_items) => {
                            let record = self.update_state_internal(
                                state_id,
                                StateOperation::DeltaSnapshot(delta_items),
                                skip_auto,
                            )?;
                            Ok(Some(record))
                        }
                        None => full(),
                    }
                }
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
    /// This walks the chain collecting Append operations until hitting a snapshot,
    /// and returns None if it meets a Redact, an Edit, a Set or a Delta first.
    fn compute_delta_items(&self, state_id: &str) -> Result<Option<Vec<u8>>> {
        let head = match self.state.get_head(self.branches.current_branch().id, state_id) {
            Some(h) => h,
            None => return Ok(Some(serde_json::to_vec(&Vec::<serde_json::Value>::new())?)),
        };

        // Walk chain backwards, collecting append operations until snapshot
        let mut appended_items: Vec<serde_json::Value> = Vec::new();
        let mut current_offset = Some(head.head_offset);

        while let Some(offset) = current_offset {
            let record = self.log.read_at(offset)?;
            let update = StateUpdateRecord::decode(&record)?;

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
                op if crate::state::is_non_append_change(op) => return Ok(None),
                _ => {}
            }

            current_offset = update.prev_update_offset;
        }

        // Reverse since we collected in reverse order
        appended_items.reverse();

        serde_json::to_vec(&appended_items)
            .map(Some)
            .map_err(|e| StoreError::Serialization(e.to_string()))
    }

    /// Compute tree operations since the last delta or full snapshot.
    ///
    /// Walks the chain collecting TreeSet/TreeRemove/TreeBatch operations until hitting a snapshot.
    /// Returns Vec<TreeOp> directly (no serialization).
    fn compute_tree_delta_ops(&self, state_id: &str) -> Result<Vec<TreeOp>> {
        let head = match self.state.get_head(self.branches.current_branch().id, state_id) {
            Some(h) => h,
            None => return Ok(Vec::new()),
        };

        // Collect ops in reverse (newest first) — deduplicate to last-write-wins per path.
        // Since we walk from newest to oldest, the first op we see for a path is the final state.
        let mut seen = std::collections::HashSet::new();
        let mut ops: Vec<TreeOp> = Vec::new();
        let mut current_offset = Some(head.head_offset);

        while let Some(offset) = current_offset {
            let record = self.log.read_at(offset)?;
            let update = StateUpdateRecord::decode(&record)?;

            let mut push_if_unseen = |op: TreeOp| {
                let path = match &op {
                    TreeOp::Set { path, .. } | TreeOp::Remove { path } => path.clone(),
                };
                if seen.insert(path) {
                    ops.push(op);
                }
            };

            match &update.operation {
                StateOperation::TreeSet { path, entry } => {
                    push_if_unseen(TreeOp::Set {
                        path: path.clone(),
                        entry: entry.clone(),
                    });
                }
                StateOperation::TreeRemove { path } => {
                    push_if_unseen(TreeOp::Remove {
                        path: path.clone(),
                    });
                }
                StateOperation::TreeBatch { ops: batch_ops } => {
                    // Process batch ops in reverse so later ops in same batch win
                    for op in batch_ops.iter().rev() {
                        push_if_unseen(op.clone());
                    }
                }
                StateOperation::Snapshot(_) | StateOperation::TreeDeltaSnapshot(_) => {
                    break;
                }
                _ => {}
            }

            current_offset = update.prev_update_offset;
        }

        // Reverse since we collected in reverse order
        ops.reverse();

        Ok(ops)
    }

    // --- State Field Indexes ---

    /// Register (or refresh) a secondary index on a JSON field (by
    /// JSON-pointer path) of every item in a state slot, on the current
    /// branch. Idempotent — a no-op if the index is already registered and
    /// fresh (same branch, same slot head) for the slot's current content.
    /// See `StateManager::register_field_index`.
    ///
    /// Takes `write_lock` for the whole call, like every other
    /// state-mutating path (`update_state_with_builder` et al.) — without
    /// it, a concurrent `record_update` from an in-flight write could land
    /// between this call's materialization read and its registration,
    /// either double-counting that item (materialized once, then indexed
    /// again by the concurrent `on_append`) or vanishing from the index
    /// entirely, depending on interleaving.
    ///
    /// Unlike state registration, this doesn't checkpoint before returning:
    /// an index is derived from its state, so a crash before the next
    /// checkpoint loses only the index. A query then returns `None`, which is
    /// the cue to register it again.
    pub fn register_state_field_index(
        &self,
        state_id: &str,
        field_path: &str,
        kind: crate::state::FieldIndexKind,
    ) -> Result<()> {
        let mut write = self.write_lock.lock();
        self.retry_metadata_checkpoint(&mut write)?;
        let branch_id = self.branches.current_branch().id;
        self.state
            .register_field_index(branch_id, state_id, field_path, kind)
    }

    /// Query ordinals of a registered `Number` field index within
    /// `[gte, lte]` (either bound optional), scoped to the CURRENT branch.
    /// Returns matching ordinals only — fetch content separately via
    /// `get_state_item`/`get_state_slice`.
    ///
    /// Returns `None` if no such index is currently registered for the
    /// current branch (never registered, wrong kind, poisoned by a
    /// cross-branch write or a parse failure, or — the case a pure
    /// `switch_branch` with no follow-up write hits — registered against a
    /// *different* branch than the one you're on now) — distinct from
    /// `Some(vec![])`, an index that exists, matches this branch, and has
    /// no matches in range. Callers that get `None` for an index they
    /// expect should re-`register_state_field_index` rather than treating
    /// the empty result as "no data".
    #[allow(clippy::too_many_arguments)]
    pub fn query_state_index_range(
        &self,
        state_id: &str,
        field_path: &str,
        gte: Option<f64>,
        lte: Option<f64>,
        limit: Option<usize>,
        offset: Option<usize>,
        reverse: bool,
    ) -> Option<Vec<u32>> {
        let branch_id = self.branches.current_branch().id;
        self.state
            .query_field_index_range(branch_id, state_id, field_path, gte, lte, limit, offset, reverse)
    }

    /// Query ordinals of a registered `String` field index equal to `value`,
    /// scoped to the CURRENT branch. Returns `None` if no such index is
    /// currently registered for the current branch — see
    /// `query_state_index_range`'s doc for the `None` vs `Some(vec![])`
    /// distinction and the branch check.
    pub fn query_state_index_eq(
        &self,
        state_id: &str,
        field_path: &str,
        value: &str,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Option<Vec<u32>> {
        let branch_id = self.branches.current_branch().id;
        self.state
            .query_field_index_eq(branch_id, state_id, field_path, value, limit, offset)
    }

    /// Distinct values and ordinal counts for a registered `String` field
    /// index, scoped to the CURRENT branch. Returns `None` if no such index
    /// is currently registered for the current branch.
    pub fn get_state_index_value_counts(
        &self,
        state_id: &str,
        field_path: &str,
    ) -> Option<Vec<(String, u32)>> {
        let branch_id = self.branches.current_branch().id;
        self.state.field_index_value_counts(branch_id, state_id, field_path)
    }

    // --- Tree Operations ---

    /// Validate a tree path: must be non-empty, relative, no null bytes, no `.` or `..` components.
    fn validate_tree_path(path: &str) -> Result<()> {
        if path.is_empty()
            || path.starts_with('/')
            || path.contains('\0')
            || path.split('/').any(|c| c == ".." || c == ".")
        {
            return Err(StoreError::InvalidOperation(format!(
                "Invalid tree path: '{}'",
                path
            )));
        }
        Ok(())
    }

    /// Set a single file in a tree state.
    pub fn tree_set(&self, state_id: &str, path: &str, entry: &TreeEntry) -> Result<Record> {
        Self::validate_tree_path(path)?;
        self.update_state(
            state_id,
            StateOperation::TreeSet {
                path: path.to_string(),
                entry: entry.clone(),
            },
        )
    }

    /// Remove a single file from a tree state.
    pub fn tree_remove(&self, state_id: &str, path: &str) -> Result<Record> {
        Self::validate_tree_path(path)?;
        self.update_state(
            state_id,
            StateOperation::TreeRemove {
                path: path.to_string(),
            },
        )
    }

    /// Apply a batch of tree operations atomically.
    pub fn tree_batch(&self, state_id: &str, ops: &[TreeOp]) -> Result<Record> {
        for op in ops {
            match op {
                TreeOp::Set { path, .. } | TreeOp::Remove { path } => {
                    Self::validate_tree_path(path)?;
                }
            }
        }
        self.update_state(state_id, StateOperation::TreeBatch { ops: ops.to_vec() })
    }

    /// Get a single file entry from a tree state.
    pub fn tree_get(&self, state_id: &str, path: &str) -> Result<Option<TreeEntry>> {
        let tree = match self
            .state
            .get_tree_state(self.branches.current_branch().id, state_id)?
        {
            Some(t) => t,
            None => return Ok(None),
        };
        Ok(tree.get(path).cloned())
    }

    /// List files in a tree state, optionally filtered by path prefix.
    pub fn tree_list(&self, state_id: &str, prefix: Option<&str>) -> Result<Vec<(String, TreeEntry)>> {
        let tree = match self
            .state
            .get_tree_state(self.branches.current_branch().id, state_id)?
        {
            Some(t) => t,
            None => return Ok(Vec::new()),
        };

        match prefix {
            Some(p) => {
                let start = p.to_string();
                Ok(tree
                    .range(start..)
                    .take_while(|(k, _)| k.starts_with(p))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect())
            }
            None => Ok(tree.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
        }
    }

    /// Compute the diff between a tree state at two sequence points.
    ///
    /// Returns a list of changes (Added, Modified, Removed).
    pub fn tree_diff(
        &self,
        state_id: &str,
        from_seq: Sequence,
        to_seq: Sequence,
    ) -> Result<Vec<TreeChange>> {
        let from_state = self.get_state_at(state_id, from_seq)?;
        let to_state = self.get_state_at(state_id, to_seq)?;

        let from_tree: TreeState = match from_state {
            Some(s) if !s.is_empty() => serde_json::from_slice(&s)
                .map_err(|e| StoreError::Deserialization(e.to_string()))?,
            _ => TreeState::new(),
        };
        let to_tree: TreeState = match to_state {
            Some(s) if !s.is_empty() => serde_json::from_slice(&s)
                .map_err(|e| StoreError::Deserialization(e.to_string()))?,
            _ => TreeState::new(),
        };

        let mut changes = Vec::new();

        // Merge-join over sorted maps
        let mut from_iter = from_tree.iter().peekable();
        let mut to_iter = to_tree.iter().peekable();

        loop {
            match (from_iter.peek(), to_iter.peek()) {
                (None, None) => break,
                (Some((path, entry)), None) => {
                    changes.push(TreeChange::Removed {
                        path: (*path).clone(),
                        entry: (*entry).clone(),
                    });
                    from_iter.next();
                }
                (None, Some((path, entry))) => {
                    changes.push(TreeChange::Added {
                        path: (*path).clone(),
                        entry: (*entry).clone(),
                    });
                    to_iter.next();
                }
                (Some((from_path, from_entry)), Some((to_path, to_entry))) => {
                    match from_path.cmp(to_path) {
                        std::cmp::Ordering::Less => {
                            changes.push(TreeChange::Removed {
                                path: (*from_path).clone(),
                                entry: (*from_entry).clone(),
                            });
                            from_iter.next();
                        }
                        std::cmp::Ordering::Greater => {
                            changes.push(TreeChange::Added {
                                path: (*to_path).clone(),
                                entry: (*to_entry).clone(),
                            });
                            to_iter.next();
                        }
                        std::cmp::Ordering::Equal => {
                            if from_entry != to_entry {
                                changes.push(TreeChange::Modified {
                                    path: (*from_path).clone(),
                                    old: (*from_entry).clone(),
                                    new: (*to_entry).clone(),
                                });
                            }
                            from_iter.next();
                            to_iter.next();
                        }
                    }
                }
            }
        }

        Ok(changes)
    }

    /// Force a full snapshot of a tree state (compaction).
    pub fn tree_snapshot(&self, state_id: &str) -> Result<Option<Record>> {
        let current = match self.get_state(state_id)? {
            Some(s) => s,
            None => return Ok(None),
        };
        let record = self.update_state_internal(
            state_id,
            StateOperation::Snapshot(current),
            true, // skip auto-snapshot
        )?;
        Ok(Some(record))
    }

    // --- Branch Operations ---

    /// Create a new branch from the current branch head and checkpoint it.
    /// If the checkpoint fails, the error is returned but the branch stays in
    /// memory, and the next successful checkpoint publishes it and tells
    /// subscribers: `sync()`, or the retry that every later mutation makes
    /// first. Calling this again retries that checkpoint first and, once it
    /// succeeds, returns `BranchExists`.
    pub fn create_branch(&self, name: &str, from: Option<&str>) -> Result<Branch> {
        let mut write = self.write_lock.lock();
        self.retry_metadata_checkpoint(&mut write)?;
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

        // Persist copied state heads before publishing the branch identity.
        write.checkpoint_required = true;
        write.pending_branch_event = Some(BranchEvent::Created {
            branch: new_branch.clone(),
            parent: Some(parent_name),
        });
        self.sync_locked(&mut write)?;

        Ok(new_branch)
    }

    /// Create and checkpoint a branch without copying state from parent.
    /// This is useful for creating branches with custom state (e.g., time-travel branching).
    /// A failed checkpoint leaves the branch in memory, as for `create_branch`.
    pub fn create_empty_branch(&self, name: &str, from: Option<&str>) -> Result<Branch> {
        let mut write = self.write_lock.lock();
        self.retry_metadata_checkpoint(&mut write)?;
        let new_branch = self.branches.create_branch(name, from)?;
        // The branch manager creates it under `from`, or under main when that
        // is None, so announce the parent it recorded.
        let parent_name = new_branch
            .parent
            .and_then(|id| self.branches.get_branch_by_id(id))
            .map(|parent| parent.name);

        write.checkpoint_required = true;
        write.pending_branch_event = Some(BranchEvent::Created {
            branch: new_branch.clone(),
            parent: parent_name,
        });
        self.sync_locked(&mut write)?;

        // Don't copy state - let the caller populate state manually
        Ok(new_branch)
    }

    /// Create and checkpoint a branch at a specific sequence (time-travel branching).
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
    ///
    /// A failed checkpoint leaves the branch in memory, as for `create_branch`.
    pub fn create_branch_at(&self, name: &str, from: &str, at: Sequence) -> Result<Branch> {
        let mut write = self.write_lock.lock();
        self.retry_metadata_checkpoint(&mut write)?;
        let parent = self
            .branches
            .get_branch(from)
            .ok_or_else(|| StoreError::BranchNotFound(from.to_string()))?;

        // Prepare all fallible chain reads before allocating a branch. A
        // corrupt historical chain must not leave a partly constructed branch
        // for a subsequent mutation (or Drop) to checkpoint.
        if self.branches.get_branch(name).is_some() {
            return Err(StoreError::BranchExists(name.to_string()));
        }
        if at > parent.head {
            return Err(StoreError::InvalidSequence(at, parent.head));
        }
        let mut heads = Vec::new();
        for state_id in self.state.state_ids() {
            if let Some(info) = self.find_chain_info_at(parent.id, &state_id, at)? {
                heads.push((state_id, info));
            }
        }
        let new_branch = self.branches.create_branch_at(name, from, at)?;
        for (state_id, info) in heads {
            self.state.set_head_for_branch(
                new_branch.id,
                &state_id,
                info.head_offset,
                info.item_count,
                info.non_append_since_snapshot,
            );
        }

        write.checkpoint_required = true;
        write.pending_branch_event = Some(BranchEvent::Created {
            branch: new_branch.clone(),
            parent: Some(from.to_string()),
        });
        self.sync_locked(&mut write)?;

        Ok(new_branch)
    }

    /// Switch to a different branch and checkpoint the current-branch pointer.
    /// If the checkpoint fails, the error is returned but the switch stays in
    /// effect, and the next successful checkpoint publishes it: `sync()`, or
    /// the retry that every later mutation makes first, so later writes go
    /// to the new branch.
    pub fn switch_branch(&self, name: &str) -> Result<Branch> {
        let mut write = self.write_lock.lock();
        self.retry_metadata_checkpoint(&mut write)?;
        let branch = self.branches.switch_branch(name)?;
        write.checkpoint_required = true;
        self.sync_locked(&mut write)?;
        Ok(branch)
    }

    /// Get the current branch.
    pub fn current_branch(&self) -> Branch {
        self.branches.current_branch()
    }

    /// List all branches.
    pub fn list_branches(&self) -> Vec<Branch> {
        self.branches.list_branches()
    }

    /// Delete a branch and checkpoint the deletion.
    /// If the checkpoint fails, the error is returned but the branch stays
    /// deleted in memory, and the next successful checkpoint publishes the
    /// deletion and tells subscribers: `sync()`, or the retry that every
    /// later mutation makes first. Calling this again retries that checkpoint
    /// first and, once it succeeds, returns `BranchNotFound`.
    pub fn delete_branch(&self, name: &str) -> Result<()> {
        let mut write = self.write_lock.lock();
        self.retry_metadata_checkpoint(&mut write)?;
        self.branches.delete_branch(name)?;
        write.checkpoint_required = true;
        write.pending_branch_event = Some(BranchEvent::Deleted {
            name: name.to_string(),
        });
        self.sync_locked(&mut write)?;

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
    /// Flushes the record log and publishes the metadata checkpoints
    /// (`state.bin`, `state-indexes.bin` and `branches.bin`). A failure to
    /// save `state-indexes.bin` is only logged: field indexes are derived, and
    /// `register_state_field_index` rebuilds a missing or stale one.
    ///
    /// Once an fsync of the record log has failed, this call's own or a
    /// periodic one, this returns the failure every time until the store is
    /// reopened, as [`StoreError::DurabilityLost`], and publishes nothing.
    /// Linux reports a write-back error to one fsync, and a later one can
    /// succeed although the records written before the failure never reached
    /// the disk, so a checkpoint published after it could name records the
    /// disk doesn't hold. Registration, strategy changes, and branch
    /// creation, deletion and switching fail the same way, and so does any
    /// write while one of them waits for its checkpoint. Reopening the store
    /// is the only way to resume durable writes. It replays the log from the
    /// last checkpoint, and can't tell whether records the operating system
    /// still caches reached the disk: if they didn't while later records did,
    /// an open after the cache drops them refuses the store as mid-log
    /// corruption.
    ///
    /// On open, complete state updates in the log past the last checkpoint
    /// are replayed, except those on a branch that `branches.bin` doesn't
    /// hold, which [`Store::recovery`] counts. Other records are only indexed
    /// again, as on every open.
    ///
    /// State registration, strategy changes, and branch creation, deletion
    /// and switching checkpoint before returning, because the record log
    /// cannot reconstruct them. If that checkpoint fails, the change stays in
    /// memory, and every later mutation retries the checkpoint before
    /// proceeding, so a successful write cannot depend on an unpersisted
    /// registration or branch. This method retries it too; metadata is not
    /// rolled back. A failed sync with no such change pending holds back no
    /// later append or state update.
    ///
    /// On Unix a save also syncs the containing directory, where the system
    /// can. Where this process can't open that directory to sync it, the
    /// checkpoint's newest save may not survive a power loss, and the store
    /// logs a warning the first time. On Windows it doesn't wait for the
    /// rename to reach the disk, so after an OS crash or power loss the store
    /// may open from an earlier checkpoint. The record index is rebuilt on
    /// startup; checkpoint cost depends on state heads, branch metadata, and
    /// any registered field indexes.
    pub fn sync(&self) -> Result<()> {
        let mut write = self.write_lock.lock();
        self.sync_locked(&mut write)
    }

    /// Finish a failed metadata checkpoint before admitting another mutation.
    fn retry_metadata_checkpoint(&self, write: &mut WriteState) -> Result<()> {
        if write.checkpoint_required {
            self.sync_locked(write)?;
        }
        Ok(())
    }

    /// Call only while holding write_lock, so a state head cannot reach disk
    /// ahead of its log record and branch/state checkpoints cannot interleave.
    /// A branch change waiting on a checkpoint is announced once one succeeds.
    fn sync_locked(&self, write: &mut WriteState) -> Result<()> {
        // Sync the append-only log (O(1) - just fsync)
        self.log.sync()?;
        // Sync small metadata files (O(states) and O(branches), typically tiny)
        self.state.save()?;
        self.branches.save()?;
        write.checkpoint_required = false;
        match write.pending_branch_event.take() {
            Some(BranchEvent::Created { branch, parent }) => {
                self.subscriptions.broadcast_branch_created(&branch, parent)
            }
            Some(BranchEvent::Deleted { name }) => self.subscriptions.broadcast_branch_deleted(&name),
            None => {}
        }
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

    // LOCK is opened without truncating it, and nothing is written to it. It
    // must be a regular file in the store's directory, or absent (then it's
    // created). A symlink is refused: its target can be replaced by a rename,
    // leaving the lock on the old file while the next open locks the new one,
    // so two processes could hold the store. A FIFO would hang the open.
    fn acquire_lock(path: &Path) -> Result<File> {
        let lock_path = path.join("LOCK");
        match fs::symlink_metadata(&lock_path) {
            Ok(meta) if !meta.file_type().is_file() => {
                let kind = if meta.file_type().is_symlink() {
                    "a symbolic link"
                } else if meta.is_dir() {
                    "a directory"
                } else {
                    "not a regular file"
                };
                return Err(StoreError::InvalidFormat(format!(
                    "{} is {kind}, and the store takes its lock only on a regular LOCK file of \
                     its own. Stop anything that uses this store, then move LOCK aside or remove \
                     it: the next open creates a regular one",
                    lock_path.display()
                )));
            }
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
            _ => {}
        }
        let lock_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;

        lock_file.try_lock_exclusive().map_err(lock_error)?;

        Ok(lock_file)
    }
}

/// What a failed `try_lock_exclusive` on LOCK means. fs2 reports another
/// holder of the lock (another process, or another open in this one) as its
/// contention error (EWOULDBLOCK, or ERROR_LOCK_VIOLATION on Windows), and
/// only that is `Locked`. Any other failure is the I/O error it is, so it
/// isn't reported as the store being held.
fn lock_error(error: std::io::Error) -> StoreError {
    if error.raw_os_error() == fs2::lock_contended_error().raw_os_error() {
        StoreError::Locked
    } else {
        StoreError::Io(error)
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        // Best-effort sync on drop. Nothing else would report its failure.
        if let Err(error) = self.sync() {
            tracing::warn!(
                %error,
                path = %self.config.path.display(),
                "final sync on drop failed: the next open replays state updates from the log \
                 past the last checkpoint, and may lose any registration or branch change \
                 whose checkpoint failed"
            );
        }
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
        let mut ops = crate::state::ChainWalk::new(&self.log, self.head_offset, None)
            .collect::<Result<Vec<_>>>()?;

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
                StateOperation::Set(data) | StateOperation::Delta { new_value: data, .. } => {
                    let arr: Vec<serde_json::Value> = serde_json::from_slice(&data)
                        .map_err(|e| StoreError::Deserialization(e.to_string()))?;
                    self.items_buffer = arr;
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
    fn a_head_whose_flag_missed_a_change_takes_a_full_snapshot_not_a_delta() {
        let n = |v: i64| serde_json::to_vec(&json!(v)).unwrap();
        let cases: Vec<(&str, Vec<StateOperation>, serde_json::Value)> = vec![
            ("Redact", vec![StateOperation::Redact { start: 1, end: 100 }], json!([0, 2, 3])),
            (
                "Edit",
                vec![StateOperation::Edit { index: 0, new_value: serde_json::to_vec(&json!("e")).unwrap() }],
                json!(["e", 1, 2, 3]),
            ),
            (
                "Set",
                vec![StateOperation::Append(n(7)), StateOperation::Set(serde_json::to_vec(&json!([9])).unwrap())],
                json!([9, 2, 3]),
            ),
            (
                "Delta",
                vec![
                    StateOperation::Append(n(7)),
                    StateOperation::Delta {
                        old_hash: crate::types::Hash([0; 32]),
                        new_value: serde_json::to_vec(&json!([9])).unwrap(),
                    },
                ],
                json!([9, 2, 3]),
            ),
        ];
        for (name, change, expected) in cases {
            let dir = TempDir::new().unwrap();
            let store = Store::create(test_config(&dir)).unwrap();
            store
                .register_state(StateRegistration {
                    id: "s".to_string(),
                    strategy: crate::types::StateStrategy::AppendLog {
                        delta_snapshot_every: 2,
                        full_snapshot_every: 100,
                    },
                    initial_value: None,
                })
                .unwrap();
            for v in [0, 1] {
                store.update_state("s", StateOperation::Append(n(v))).unwrap();
            }
            for op in change {
                store.update_state("s", op).unwrap();
            }
            let child = store.create_branch_at("child", "main", store.current_branch().head).unwrap();
            let head = store.state.get_head(child.id, "s").unwrap();
            assert!(head.has_non_append_since_snapshot, "{name}: setup: the branch inherits the flag");
            store.state.set_head_for_branch(child.id, "s", head.head_offset, head.item_count, false);
            store.switch_branch("child").unwrap();
            for v in [2, 3] {
                store.update_state("s", StateOperation::Append(n(v))).unwrap();
            }
            let value: serde_json::Value =
                serde_json::from_slice(&store.get_state("s").unwrap().unwrap()).unwrap();
            assert_eq!(value, expected, "{name}");
            let head = store.state.get_head(child.id, "s").unwrap();
            assert!(head.last_full_snapshot_offset.is_some(), "{name}: the snapshot that came due was a full one");
            assert!(!head.has_non_append_since_snapshot, "{name}");
            assert_eq!(
                store.get_state_len("s").unwrap(),
                Some(expected.as_array().unwrap().len()),
                "{name}"
            );
        }
    }

    #[test]
    fn a_head_an_earlier_release_left_across_a_change_keeps_its_reading_and_takes_deltas() {
        let n = |v: i64| serde_json::to_vec(&json!(v)).unwrap();
        let read = |store: &Store| -> serde_json::Value { serde_json::from_slice(&store.get_state("s").unwrap().unwrap()).unwrap() };
        let reads_alike = |store: &Store, expected: &serde_json::Value, what: &str| {
            let len = expected.as_array().unwrap().len();
            assert_eq!(&read(store), expected, "{what}: get_state");
            let slice: serde_json::Value = serde_json::from_slice(&store.get_state_slice("s", 0, len).unwrap().unwrap()).unwrap();
            assert_eq!(&slice, expected, "{what}: get_state_slice");
            let items: Vec<serde_json::Value> = store.iter_state_items("s").unwrap().unwrap().map(|i| i.unwrap()).collect();
            assert_eq!(&json!(items), expected, "{what}: iter_state_items");
            for k in 1..=len {
                let tail: serde_json::Value = serde_json::from_slice(&store.get_state_tail("s", k).unwrap().unwrap()).unwrap();
                assert_eq!(tail, json!(expected.as_array().unwrap()[len - k..]), "{what}: get_state_tail({k})");
            }
        };
        let dir = TempDir::new().unwrap();
        let store = Store::create(test_config(&dir)).unwrap();
        store
            .register_state(StateRegistration {
                id: "s".to_string(),
                strategy: crate::types::StateStrategy::AppendLog { delta_snapshot_every: 2, full_snapshot_every: 100 },
                initial_value: None,
            })
            .unwrap();
        for v in [0, 1] {
            store.update_state("s", StateOperation::Append(n(v))).unwrap();
        }
        store.update_state("s", StateOperation::Redact { start: 1, end: 100 }).unwrap();
        store.set_auto_snapshot(false);
        for v in [2, 3] {
            store.update_state("s", StateOperation::Append(n(v))).unwrap();
        }
        store.update_state("s", StateOperation::DeltaSnapshot(serde_json::to_vec(&json!([2, 3])).unwrap())).unwrap();
        let main = store.current_branch().id;
        let head = store.state.get_head(main, "s").unwrap();
        store.state.set_head_for_branch(main, "s", head.head_offset, head.item_count, false);
        store.sync().unwrap();
        drop(store);
        let store = Store::open(StoreConfig { create_if_missing: false, ..test_config(&dir) }).unwrap();
        let head = store.state.get_head(main, "s").unwrap();
        assert!(!head.has_non_append_since_snapshot, "setup: the head holds false, as an earlier release saved it");
        let old = json!([0, 1, 2, 3]);
        reads_alike(&store, &old, "at the old head");
        assert_eq!(store.get_state_len("s").unwrap(), Some(3), "its count, short of the reading");
        store.set_auto_snapshot(false);
        for v in [4, 5] {
            store.update_state("s", StateOperation::Append(n(v))).unwrap();
        }
        assert_eq!(store.snapshot_needed("s"), Some(crate::state::SnapshotNeeded::Delta), "a delta comes due on top of the old one");
        let deltas = store.get_compaction_stats("s").unwrap().delta_snapshots_since_full;
        store.create_snapshot_if_needed("s").unwrap();
        let stats = store.get_compaction_stats("s").unwrap();
        assert_eq!((stats.delta_snapshots_since_full, stats.last_full_snapshot_offset), (deltas + 1, None), "it was taken as a delta");
        drop(store);
        let store = Store::open(StoreConfig { create_if_missing: false, ..test_config(&dir) }).unwrap();
        reads_alike(&store, &json!([0, 1, 2, 3, 4, 5]), "after the new delta and a reopen");
        assert_eq!(store.get_state_len("s").unwrap(), Some(5));
    }

    /// Opens a store this process closed a moment ago. A child that a
    /// concurrent test spawns shares this process's open files until it
    /// execs, the store's LOCK among them, so the lock can still be held
    /// for that moment.
    fn reopen(config: StoreConfig) -> Store {
        try_reopen(config).unwrap()
    }

    fn try_reopen(config: StoreConfig) -> Result<Store> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            match Store::open(config.clone()) {
                Err(StoreError::Locked) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(5))
                }
                result => return result,
            }
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
    fn sync_waits_for_active_writer() {
        use std::sync::mpsc;
        use std::time::Duration;
        let dir = TempDir::new().unwrap();
        let store = Arc::new(Store::create(test_config(&dir)).unwrap());
        store.sync().unwrap();
        let writer = store.write_lock.lock();
        let (started_tx, started_rx) = mpsc::channel();
        let (finished_tx, finished_rx) = mpsc::channel();
        let sync_store = Arc::clone(&store);
        let thread = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            finished_tx.send(sync_store.sync()).unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let while_locked = finished_rx.recv_timeout(Duration::from_millis(250));
        drop(writer);
        assert!(
            matches!(while_locked, Err(mpsc::RecvTimeoutError::Timeout)),
            "sync passed an active writer: {while_locked:?}"
        );
        finished_rx.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
        thread.join().unwrap();
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
            let store = reopen(config);

            assert_eq!(store.index.count(), 2); // message + state_update

            let branches = store.list_branches();
            assert_eq!(branches.len(), 2);

            let state = store.get_state("test").unwrap().unwrap();
            assert_eq!(state, b"value");
        }
    }

    #[test]
    fn only_another_holders_lock_is_reported_as_locked() {
        assert!(matches!(
            lock_error(fs2::lock_contended_error()),
            StoreError::Locked
        ));
        // Any other error comes back as itself, its OS code included.
        let unsupported = std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "no locks on this filesystem",
        );
        match lock_error(unsupported) {
            StoreError::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::Unsupported),
            other => panic!("{other:?}"),
        }
        #[cfg(unix)]
        for code in [libc::ENOLCK, libc::EOPNOTSUPP] {
            match lock_error(std::io::Error::from_raw_os_error(code)) {
                StoreError::Io(e) => assert_eq!(e.raw_os_error(), Some(code)),
                other => panic!("{code}: {other:?}"),
            }
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

        // Overflow-safe: offset + limit past usize::MAX must not panic
        let slice = store.get_state_slice("items", 5, usize::MAX).unwrap().unwrap();
        let arr: Vec<i32> = serde_json::from_slice(&slice).unwrap();
        assert_eq!(arr, vec![6, 7, 8, 9, 10]);
    }

    #[test]
    fn test_get_state_item() {
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

        // Missing state -> None
        assert!(store.get_state_item("items", 0).unwrap().is_none());

        for i in 1..=5 {
            store.update_state("items", StateOperation::Append(
                serde_json::to_vec(&json!({"id": i})).unwrap()
            )).unwrap();
        }

        // Point lookups (warm the item cache, then read repeatedly)
        for _ in 0..3 {
            let item = store.get_state_item("items", 2).unwrap().unwrap();
            let v: serde_json::Value = serde_json::from_slice(&item).unwrap();
            assert_eq!(v["id"], 3);
        }

        // Out of range -> None
        assert!(store.get_state_item("items", 5).unwrap().is_none());

        // A write after cached reads must be visible (head_offset invalidation)
        store.update_state("items", StateOperation::Append(
            serde_json::to_vec(&json!({"id": 6})).unwrap()
        )).unwrap();
        let item = store.get_state_item("items", 5).unwrap().unwrap();
        let v: serde_json::Value = serde_json::from_slice(&item).unwrap();
        assert_eq!(v["id"], 6);

        // An edit must be visible too
        store.update_state("items", StateOperation::Edit {
            index: 0,
            new_value: serde_json::to_vec(&json!({"id": 100})).unwrap(),
        }).unwrap();
        let item = store.get_state_item("items", 0).unwrap().unwrap();
        let v: serde_json::Value = serde_json::from_slice(&item).unwrap();
        assert_eq!(v["id"], 100);
    }

    #[test]
    fn test_get_state_item_branch_isolation() {
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

        store.update_state("items", StateOperation::Append(
            serde_json::to_vec(&json!("base")).unwrap()
        )).unwrap();

        // Warm cache on the original branch
        let item = store.get_state_item("items", 0).unwrap().unwrap();
        assert_eq!(serde_json::from_slice::<serde_json::Value>(&item).unwrap(), json!("base"));

        // Fork, append on the fork — reads must reflect the fork, then the
        // original again after switching back (no cross-branch bleed).
        store.create_branch("fork", None).unwrap();
        store.switch_branch("fork").unwrap();
        store.update_state("items", StateOperation::Append(
            serde_json::to_vec(&json!("forked")).unwrap()
        )).unwrap();
        assert_eq!(store.get_state_len("items").unwrap(), Some(2));
        let item = store.get_state_item("items", 1).unwrap().unwrap();
        assert_eq!(serde_json::from_slice::<serde_json::Value>(&item).unwrap(), json!("forked"));

        store.switch_branch("main").unwrap();
        assert_eq!(store.get_state_len("items").unwrap(), Some(1));
        assert!(store.get_state_item("items", 1).unwrap().is_none());
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
    fn test_snapshot_strategy_reconstructs_from_newest_set() {
        // Regression: Snapshot-strategy states are written via Set and never
        // get a periodic Snapshot record (snapshot_needed -> None for the
        // Snapshot strategy). Cold reconstruction must treat the newest Set as
        // a full-state terminal and stop the backward walk there; walking to
        // sequence 0 made boot O(full chain) for these states (Sol, 2026-07-31).
        // This exercises reconstruct_from_disk on a long all-Set chain and
        // asserts the reopened value is the last Set.
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir);
        {
            let store = Store::create(config.clone()).unwrap();
            store.register_state(StateRegistration {
                id: "bookkeeping".to_string(),
                strategy: crate::types::StateStrategy::Snapshot,
                initial_value: None,
            }).unwrap();
            for i in 0..200 {
                store.update_state("bookkeeping", StateOperation::Set(
                    serde_json::to_vec(&json!({ "n": i })).unwrap()
                )).unwrap();
            }
            store.sync().unwrap();
        }
        // Reopen: the in-memory cache is gone, so get_state hits
        // reconstruct_from_disk over the full Set chain.
        {
            let store = reopen(config);
            let raw = store.get_state("bookkeeping").unwrap().unwrap();
            let parsed: serde_json::Value = serde_json::from_slice(&raw).unwrap();
            assert_eq!(parsed, json!({ "n": 199 }));
        }
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
        // So: ops_since_last_full = 1 delta * 3 ops covered + 2 raw ops = 5
        let stats = store.get_compaction_stats("items").unwrap();
        assert_eq!(stats.ops_since_last_full_snapshot, 5); // 1 delta covering 3 + 2 raw
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

    // R1-1: a failed periodic fsync must not leave a record that its state's
    // head never adopted, or the next update names the same predecessor and
    // replay refuses the store. The child keeps writing after one failed
    // fsync, and again while every fsync fails, and exits without Drop, so
    // the reopen has only the log and the registration's checkpoint to go on.
    #[test]
    fn failed_periodic_fsync_keeps_heads_and_log_in_step() {
        for failures in ["one", "every"] {
            let dir = TempDir::new().unwrap();
            let config = test_config(&dir);
            run_fsync_failure_child(&config, "update", failures);

            let store = Store::open(config).unwrap();
            let items: Vec<u64> =
                serde_json::from_slice(&store.get_state("items").unwrap().unwrap()).unwrap();
            assert_eq!(items, (0..250).collect::<Vec<u64>>(), "{failures}");
        }
    }

    // The same failure on the generic path: each record keeps its own
    // sequence, where a record left behind by a failed append used to share
    // its sequence with the next one.
    #[test]
    fn failed_periodic_fsync_leaves_no_generic_sequence_twice() {
        for failures in ["one", "every"] {
            let dir = TempDir::new().unwrap();
            let config = test_config(&dir);
            run_fsync_failure_child(&config, "append", failures);

            let store = Store::open(config).unwrap();
            let mut sequences: Vec<u64> = store
                .get_records_by_type("message")
                .into_iter()
                .map(|id| store.get_record(id).unwrap().unwrap().sequence.0)
                .collect();
            sequences.sort_unstable();
            assert_eq!(sequences, (1..=250).collect::<Vec<u64>>(), "{failures}");
        }
    }

    // After one failed fsync of the log, nothing publishes a checkpoint until
    // the store is reopened: neither sync(), a metadata mutation nor Drop,
    // and each of them reports DurabilityLost. Writes go on until a metadata
    // mutation waits for its checkpoint, and then report it too.
    #[test]
    fn a_failed_log_fsync_publishes_nothing_until_reopen() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir);
        let registration = |id: &str| StateRegistration {
            id: id.into(),
            strategy: crate::types::StateStrategy::AppendLog {
                delta_snapshot_every: 1000,
                full_snapshot_every: 1000,
            },
            initial_value: None,
        };
        let store = Store::create(config.clone()).unwrap();
        store.register_state(registration("items")).unwrap();
        store.set_auto_snapshot(false);
        let checkpoint = std::fs::read(config.path.join("state.bin")).unwrap();

        // The 100th update's periodic fsync fails, and the retry succeeds.
        *store.log.fsync_failures.lock() = 1;
        for n in 0..150u64 {
            store
                .update_state("items", StateOperation::Append(n.to_string().into_bytes()))
                .unwrap();
        }
        for _ in 0..2 {
            let result = store.sync();
            assert_eq!(
                std::fs::read(config.path.join("state.bin")).unwrap(),
                checkpoint,
                "sync() published state.bin"
            );
            let error = result.unwrap_err();
            assert!(matches!(error, StoreError::DurabilityLost(_)), "{error}");
            assert!(error.to_string().contains("until it is reopened"), "{error}");
        }
        fn lost<T>(result: Result<T>) -> bool {
            matches!(result, Err(StoreError::DurabilityLost(_)))
        }
        assert!(lost(store.register_state(registration("other"))));
        // Every call that retries the registration's pending checkpoint first
        // meets the same failure.
        assert!(
            lost(store.update_state("items", StateOperation::Append(b"150".to_vec()))),
            "a write waits for the registration's checkpoint"
        );
        assert!(lost(store.append(RecordInput::raw("event", b"blocked".to_vec()))));
        assert!(lost(store.update_state_strategy(
            "items",
            crate::types::StateStrategy::AppendLog {
                delta_snapshot_every: 50,
                full_snapshot_every: 20,
            },
        )));
        assert!(lost(store.register_state_field_index(
            "items",
            "/n",
            crate::state::FieldIndexKind::Number
        )));
        assert!(lost(store.create_branch("blocked", None)));
        assert!(lost(store.create_empty_branch("blocked-empty", None)));
        assert!(lost(store.create_branch_at("blocked-at", "main", Sequence(0))));
        assert!(lost(store.switch_branch("main")));
        assert!(lost(store.delete_branch("missing")));
        assert!(lost(store.sync()));
        drop(store);
        assert_eq!(
            std::fs::read(config.path.join("state.bin")).unwrap(),
            checkpoint,
            "nothing published state.bin"
        );

        let store = reopen(config);
        let items: Vec<u64> =
            serde_json::from_slice(&store.get_state("items").unwrap().unwrap()).unwrap();
        assert_eq!(items, (0..150).collect::<Vec<u64>>());
        store.register_state(registration("other")).unwrap();
        store.sync().unwrap();
    }

    // What a checkpoint published past records the disk lost would meet: a
    // log whose torn tail open truncates, behind a head in state.bin.
    #[test]
    fn a_checkpoint_past_the_logs_valid_end_refuses_open() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir);
        let store = Store::create(config.clone()).unwrap();
        store
            .register_state(StateRegistration {
                id: "items".into(),
                strategy: crate::types::StateStrategy::Snapshot,
                initial_value: None,
            })
            .unwrap();
        for n in 0..3u64 {
            store
                .update_state("items", StateOperation::Set(n.to_string().into_bytes()))
                .unwrap();
        }
        store.sync().unwrap();
        drop(store);
        let log = config.path.join("records.log");
        let len = std::fs::metadata(&log).unwrap().len();
        let file = std::fs::OpenOptions::new().write(true).open(&log).unwrap();
        file.set_len(len - 5).unwrap();
        drop(file);

        let error = match try_reopen(config) {
            Ok(_) => panic!("expected open to refuse a checkpoint past the log's valid end"),
            Err(error) => error,
        };
        assert!(matches!(error, StoreError::Corruption(_)), "{error:?}");
        assert!(error.to_string().contains("newer than the durable log"), "{error}");
    }

    fn run_fsync_failure_child(config: &StoreConfig, writes: &str, failures: &str) {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "store::tests::failed_periodic_fsync_child"])
            .env("CHRONICLE_FSYNC_FAILURE_STORE", &config.path)
            .env("CHRONICLE_FSYNC_FAILURE_WRITES", writes)
            .env("CHRONICLE_FSYNC_FAILURES", failures)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(73),
            "child did not finish writing:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn failed_periodic_fsync_child() {
        let Some(path) = std::env::var_os("CHRONICLE_FSYNC_FAILURE_STORE") else {
            return;
        };
        let writes = std::env::var("CHRONICLE_FSYNC_FAILURE_WRITES").unwrap();
        let failures = std::env::var("CHRONICLE_FSYNC_FAILURES").unwrap();
        let store = Store::create(StoreConfig {
            path: path.into(),
            ..Default::default()
        })
        .unwrap();
        store
            .register_state(StateRegistration {
                id: "items".into(),
                strategy: crate::types::StateStrategy::AppendLog {
                    delta_snapshot_every: 1000,
                    full_snapshot_every: 1000,
                },
                initial_value: None,
            })
            .unwrap();
        store.set_auto_snapshot(false);

        // From here the next fsync fails, the log's periodic one, or every
        // fsync does.
        *store.log.fsync_failures.lock() = if failures == "one" { 1 } else { u64::MAX };
        for n in 0..250u64 {
            // Like the writer in the review's reproduction, carry on past errors.
            let payload = n.to_string().into_bytes();
            let _ = if writes == "update" {
                store
                    .update_state("items", StateOperation::Append(payload))
                    .map(drop)
            } else {
                store.append(RecordInput::raw("message", payload)).map(drop)
            };
            if n == 150 {
                assert!(store.sync().is_err(), "sync reports the failed fsync");
                assert!(store.sync().is_err(), "and goes on reporting it");
            }
        }
        // Exit without destructors, as a crash would.
        std::process::exit(73);
    }
}
