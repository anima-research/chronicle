//! Main Store struct tying all components together.

use crate::blobs::BlobStorage;
use crate::branches::BranchManager;
use crate::error::{Result, StoreError};
use crate::records::{RecordIndex, RecordLog, RecoveryReport};
use crate::state::{StateManager, StateSelection};
use crate::subscriptions::{SubscriptionConfig, SubscriptionHandle, SubscriptionId, SubscriptionManager};
use crate::types::{
    Blob, Branch, BranchId, Hash, Record, RecordId, RecordInput, Sequence,
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

/// What a batched append to an append-log state recorded
/// (`Store::append_many_to_state` and its forms, and
/// `Store::append_many_to_state_json_with_identity`).
#[derive(Clone, Debug)]
pub struct AppendedBatch {
    /// The one record that holds the whole batch, at one sequence.
    pub record: Record,
    /// The ordinal the batch's first item took: the state's length on the
    /// branch, as `get_state_len` counts it, just before the batch, read
    /// under the same lock as the write. Item `i` of the batch is at
    /// `first_ordinal + i`, so a caller has every item's ordinal without a
    /// read.
    pub first_ordinal: usize,
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

/// Store format version of a store every release reads: the MANIFEST is the
/// magic and this byte. A new store starts at it, and stays at it until it
/// first holds something a build without a format feature can't read.
const STORE_VERSION: u8 = 1;

/// Store format version of a store whose MANIFEST also names format
/// features: after the magic and this byte, a little-endian `u64` of feature
/// bits. Builds that know only version 1 refuse such a store at open
/// ("Unsupported store version: 2"), before they read any of it, instead of
/// meeting a record they can't decode part way through the log. This build
/// refuses a feature bit it doesn't know the same way.
const STORE_VERSION_FEATURES: u8 = 2;

/// Format feature: the log holds `StateOperation::AppendMany` records. Set
/// in the MANIFEST before the first one is written.
const FEATURE_APPEND_MANY: u64 = 1 << 0;

/// Every format feature this build reads.
const KNOWN_FEATURES: u64 = FEATURE_APPEND_MANY;

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

    /// The format features the MANIFEST on disk names (0 for a version 1
    /// manifest). A write that needs one it lacks rewrites the MANIFEST
    /// first; see `Store::require_features`.
    features: u64,
}

/// A branch change to announce once a checkpoint persists it.
enum BranchEvent {
    Created { branch: Branch, parent: Option<String> },
    Deleted { name: String },
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
    ///
    /// A directory that holds any of a store's files (`MANIFEST`,
    /// `records.log`, `state.bin`, `branches.bin`, a symbolic link counting
    /// even when its target is missing) is opened, so one whose `MANIFEST`
    /// is missing or unusable is refused, with open's account of what it
    /// found, whatever `create_if_missing` says. Otherwise it is created into
    /// (when `create_if_missing` is set) only if it is missing or holds
    /// nothing but what a new store can sit beside: `lost+found` (a fresh
    /// volume's root), Finder's `.DS_Store`, the `LOCK` and `blobs/` a create
    /// leaves when it stops before its manifest, and a checkpoint's staging
    /// file, as a create leaves when it stops during its manifest save. A
    /// directory holding anything else, such as a mistaken path to a project,
    /// isn't taken for a place to put a store.
    pub fn open_or_create(config: StoreConfig) -> Result<Self> {
        if Self::present_store_file(&config.path)?.is_some() {
            return Self::open(config);
        }
        if let Some(other) = Self::foreign_entry(&config.path) {
            return Err(StoreError::InvalidFormat(format!(
                "{} has no MANIFEST and holds {other}, so it isn't a Chronicle store or an empty place for one",
                config.path.display()
            )));
        }
        if config.create_if_missing {
            Self::create(config)
        } else {
            Err(StoreError::NotInitialized)
        }
    }

    fn foreign_entry(path: &Path) -> Option<String> {
        let allowed = |name: &str| {
            ["lost+found", ".DS_Store", "LOCK", "blobs"].contains(&name)
                || name.starts_with(crate::atomic_file::STAGING_PREFIX)
        };
        fs::read_dir(path).ok()?.flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .find(|name| !allowed(name))
    }

    /// Create a new store.
    ///
    /// Refuses a directory that already holds a store: creating over one
    /// used to keep its records but publish empty state and branch indexes
    /// over its own at the first sync (#34). Use [`Store::open`] or
    /// [`Store::open_or_create`] for a store that may exist.
    ///
    /// It never writes a MANIFEST over a store's own, so it can't drop the
    /// format features a version 2 MANIFEST names. A create that did, even
    /// one that then failed on the lock, let a build that predates those
    /// features open a store it can't read: open names a feature again only
    /// for the records past the checkpoint.
    pub fn create(config: StoreConfig) -> Result<Self> {
        // Create directory structure
        fs::create_dir_all(&config.path)?;

        // The lock comes first, so a create aimed at a store another process
        // has open writes nothing there before it fails (#34).
        let lock_file = Self::acquire_lock(&config.path)?;
        if let Some(found) = Self::present_store_file(&config.path)? {
            return Err(StoreError::InvalidOperation(if found == "MANIFEST" {
                format!(
                    "create: {} already holds a Chronicle store; open it with open or open_or_create",
                    config.path.display()
                )
            } else {
                format!(
                    "create: {} holds {found} but no MANIFEST: a damaged Chronicle store, which create won't write over",
                    config.path.display()
                )
            }));
        }

        // With the lock held, staging files here were left by interrupted
        // saves, a create's own manifest save among them (as open sweeps them).
        crate::atomic_file::remove_staging_files(&config.path);
        fs::create_dir_all(config.path.join("blobs"))?;
        Self::write_manifest(&config.path)?;

        // Initialize components
        let log = Arc::new(RecordLog::open(config.path.join("records.log"))?);
        let blobs = BlobStorage::new(config.path.join("blobs"), config.blob_cache_size)?;
        // A blobs/ directory can outlive the rest of an earlier store (create
        // refuses only the store's data files), and hold staging files its
        // interrupted blob stores left. The lock is held, as at open.
        blobs.remove_staging_files();
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
        let mut features = Self::verify_manifest(&config.path)?;

        // With the lock held, no save of this store is in progress, so its
        // own staging files here were left by interrupted saves. The sweep's
        // doc says what else it can reach.
        crate::atomic_file::remove_staging_files(&config.path);

        // Open components
        let log = Arc::new(RecordLog::open(config.path.join("records.log"))?);
        let blobs = BlobStorage::new(config.path.join("blobs"), config.blob_cache_size)?;
        blobs.remove_staging_files();
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
        // Format features the replayed tail's operations need.
        let mut replayed_features = 0u64;
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
                replayed_features |= Self::features_of(&update.operation);

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
        // The MANIFEST names a feature before the first record that needs it
        // is written, so this finds nothing missing unless that rewrite was
        // lost, as a power loss can lose a rename that wasn't synced. The
        // tail past the checkpoint is all this sees; it marks the store
        // again so an older build still refuses it. A mark lost once the
        // batch is behind the checkpoint isn't found here, which is why
        // create never writes over an existing MANIFEST.
        let missing = replayed_features & !features;
        if missing != 0 {
            tracing::warn!(
                features = missing,
                "the log holds records whose format features the MANIFEST doesn't name; \
                 naming them now"
            );
            features |= missing;
            Self::write_manifest_features(&config.path, features)?;
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
            write_lock: Mutex::new(WriteState {
                features,
                ..WriteState::default()
            }),
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

    // --- Branch targeting ---
    //
    // Every branch-dependent operation exists in two forms. `foo(..)` targets
    // the store-wide cursor (`switch_branch` / `current_branch`) and is a
    // one-line delegation to `foo_on(cursor, ..)`. `foo_on(branch_id, ..)`
    // targets an explicitly chosen branch and never reads or moves the
    // cursor, so any number of callers can work on different branches of the
    // same open store at once — the basis of branch-bound handles ("views").
    //
    // One `_on` call resolves to ONE branch from start to finish: the write,
    // the auto-snapshot that may follow it, delta computation, compaction,
    // tree operations, field-index maintenance and every read inside it all
    // receive the same `branch_id`. Nothing inside re-reads the cursor.
    //
    // A `branch_id` whose branch has been deleted is an error
    // (`BranchNotFound`), or `None`/`false` for the few `_on` queries whose
    // return type has no error channel.

    /// Look up a branch by id, failing if it does not (or no longer) exist.
    fn branch_by_id(&self, branch_id: BranchId) -> Result<Branch> {
        self.branches.get_branch_by_id(branch_id).ok_or_else(|| {
            StoreError::BranchNotFound(format!("branch id {} (deleted?)", branch_id.0))
        })
    }

    /// Whether a branch with this id currently exists.
    pub fn branch_exists(&self, branch_id: BranchId) -> bool {
        self.branches.get_branch_by_id(branch_id).is_some()
    }

    /// A branch by id, or `None` if it does not (or no longer) exist.
    pub fn get_branch_by_id(&self, branch_id: BranchId) -> Option<Branch> {
        self.branches.get_branch_by_id(branch_id)
    }

    /// A branch by name.
    pub fn get_branch(&self, name: &str) -> Option<Branch> {
        self.branches.get_branch(name)
    }

    /// Append a record to the current branch.
    /// The internal `state_update` type is reserved for `update_state`.
    pub fn append(&self, input: RecordInput) -> Result<Record> {
        self.append_on(self.branches.current_id(), input)
    }

    /// Append a record to the given branch.
    /// The internal `state_update` type is reserved for `update_state`.
    pub fn append_on(&self, branch_id: BranchId, input: RecordInput) -> Result<Record> {
        if input.record_type == "state_update" {
            return Err(StoreError::InvalidOperation(
                "record type 'state_update' is reserved; use update_state instead".into(),
            ));
        }
        let mut write = self.write_lock.lock();
        self.retry_metadata_checkpoint(&mut write)?;

        let branch = self.branch_by_id(branch_id)?;
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

    /// Get several records by ID in one call: for each ID, in the order
    /// given, what `get_record` returns for it, `None` for an ID with no
    /// record. IDs may repeat. Store-wide, like `get_record`. The IDs are
    /// looked up together and the records read under one hold of the log
    /// file, rather than one each.
    pub fn get_records(&self, ids: &[RecordId]) -> Result<Vec<Option<Record>>> {
        let offsets = self.index.get_offsets_by_id(ids);
        let found: Vec<u64> = offsets.iter().flatten().copied().collect();
        let mut records = self.log.read_many_at(&found)?.into_iter();
        Ok(offsets
            .iter()
            .map(|offset| offset.and_then(|_| records.next()))
            .collect())
    }

    /// Get the IDs of a type's records across all branches, in ascending ID
    /// order, which is log order.
    pub fn get_records_by_type(&self, record_type: &str) -> Vec<RecordId> {
        self.index.get_by_type(record_type)
    }

    /// Iterate records from a sequence.
    pub fn iter_from(&self, seq: Sequence) -> impl Iterator<Item = Result<(u64, Record)>> + '_ {
        self.iter_from_on(self.branches.current_id(), seq)
    }

    /// Iterate the log from the given branch's record at `seq` (from the
    /// start of the log if that branch has no record at `seq`).
    pub fn iter_from_on(
        &self,
        branch_id: BranchId,
        seq: Sequence,
    ) -> impl Iterator<Item = Result<(u64, Record)>> + '_ {
        let offset = self
            .index
            .get_offset(branch_id, seq)
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
        self.query_range_on(self.branches.current_id(), from, to, limit, reverse, types)
    }

    /// `query_range` against the given branch. Like `query_range`, this
    /// returns the records written ON that branch; records a branch inherited
    /// from its parent belong to the parent.
    pub fn query_range_on(
        &self,
        branch_id: BranchId,
        from: Option<Sequence>,
        to: Option<Sequence>,
        limit: usize,
        reverse: bool,
        types: Option<&[String]>,
    ) -> Result<Vec<Record>> {
        let branch = self.branch_by_id(branch_id)?;

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

    /// Store a blob, as [`BlobStorage::store`] does.
    pub fn store_blob(&self, content: &[u8], content_type: &str) -> Result<Hash> {
        self.blobs.store(content, content_type)
    }

    /// Get a blob by hash, as [`BlobStorage::get`] does.
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
    /// The operation is checked against what the index holds, not applied,
    /// before it's recorded: an Edit's index against the state's length, and a
    /// Tree operation against its strategy. Other operations are recorded as
    /// given, and one that can't be applied, such as an Append of bytes that
    /// aren't JSON, shows when the state is next materialized.
    ///
    /// An `AppendMany` with no items is refused. The first `AppendMany` a
    /// store records also rewrites its MANIFEST, before the record is
    /// written, to format version 2 with the batch feature named, so a build
    /// that predates the operation refuses the store when it opens it. A
    /// store never asked to record one keeps version 1, which every release
    /// opens. If that rewrite fails, the error is returned and the batch isn't
    /// recorded; when only the directory sync after its rename failed, the
    /// MANIFEST is already at version 2. If the record then fails to be
    /// written, the error is returned and the store stays at version 2 with
    /// no batch.
    ///
    /// Snapshots are created automatically when the state's thresholds are
    /// reached, by the write that reaches them, once its record is written
    /// (unless `set_auto_snapshot(false)`). A recorded write never returns
    /// an error from that snapshot (issue #39). If the snapshot fails, on a
    /// full disk, say, or because it materializes an item that doesn't
    /// parse, the failure is logged as a `tracing` warning and the write
    /// returns its record. The snapshot stays due, so the next write to the
    /// state on that branch tries it again. A state whose snapshot keeps
    /// failing, such as one holding an Append that isn't JSON, logs a
    /// warning at each write that finds a snapshot due, and takes none
    /// until a `Set` replaces its value.
    pub fn update_state(
        &self,
        state_id: &str,
        operation: StateOperation,
    ) -> Result<Record> {
        self.update_state_on(self.branches.current_id(), state_id, operation)
    }

    /// `update_state` against the given branch.
    pub fn update_state_on(
        &self,
        branch_id: BranchId,
        state_id: &str,
        operation: StateOperation,
    ) -> Result<Record> {
        self.update_state_internal(branch_id, state_id, operation, false, None)
    }

    /// Internal update_state with option to skip auto-snapshot.
    ///
    /// `snapshot_of` is for the snapshots the store writes itself: the chain
    /// head offset the snapshot's content was read at. See
    /// `update_state_with_builder`.
    fn update_state_internal(
        &self,
        branch_id: BranchId,
        state_id: &str,
        operation: StateOperation,
        skip_auto_snapshot: bool,
        snapshot_of: Option<u64>,
    ) -> Result<Record> {
        self.update_state_with_builder(
            branch_id,
            state_id,
            skip_auto_snapshot,
            snapshot_of,
            move |_, _| Ok(operation),
        )
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
    ///
    /// An `item` that isn't a JSON object is refused with `InvalidOperation`
    /// and writes nothing. Once the item is recorded, the call returns its
    /// record: if the snapshot the write takes after it fails, that is
    /// logged and the next write tries it again, as for `update_state`
    /// (issue #39).
    pub fn append_to_state_json_with_identity(
        &self,
        state_id: &str,
        item: serde_json::Value,
        id_field: &str,
        sequence_field: &str,
    ) -> Result<Record> {
        self.append_to_state_json_with_identity_on(
            self.branches.current_id(),
            state_id,
            item,
            id_field,
            sequence_field,
        )
    }

    /// `append_to_state_json_with_identity` against the given branch.
    pub fn append_to_state_json_with_identity_on(
        &self,
        branch_id: BranchId,
        state_id: &str,
        item: serde_json::Value,
        id_field: &str,
        sequence_field: &str,
    ) -> Result<Record> {
        self.update_state_with_builder(branch_id, state_id, false, None, move |id, seq| {
            let bytes = stamp_identity(item, id_field, sequence_field, id, seq)?;
            Ok(StateOperation::Append(bytes))
        })
    }

    /// Append several items to an AppendLog state as one record, at one
    /// sequence (a `StateOperation::AppendMany`), and say which ordinals
    /// they took. See `AppendedBatch::first_ordinal`.
    ///
    /// Reads see the batch as they would see the same items appended one at
    /// a time, except that a historical read or a fork cut by sequence keeps
    /// or drops it whole, as it has one sequence. It counts as one operation
    /// toward the state's snapshot cadence, and the first batch a store
    /// records marks its MANIFEST so that releases which predate batches
    /// refuse the store (see
    /// `update_state`). Each item must be one JSON value, as for an
    /// `Append`. An empty batch is refused with `InvalidOperation` and
    /// writes nothing. Once the batch is recorded, the call returns it: if
    /// the snapshot the write takes after it fails, that is logged and the
    /// next write tries it again, as for `update_state` (issue #39).
    pub fn append_many_to_state(
        &self,
        state_id: &str,
        items: Vec<Vec<u8>>,
    ) -> Result<AppendedBatch> {
        self.append_many_to_state_on(self.branches.current_id(), state_id, items)
    }

    /// `append_many_to_state` against the given branch.
    pub fn append_many_to_state_on(
        &self,
        branch_id: BranchId,
        state_id: &str,
        items: Vec<Vec<u8>>,
    ) -> Result<AppendedBatch> {
        self.append_batch_with_builder(branch_id, state_id, move |_, _| Ok(items))
    }

    /// `append_many_to_state` with each item given as a JSON value,
    /// serialized as `serde_json` writes it.
    pub fn append_many_to_state_json(
        &self,
        state_id: &str,
        items: &[serde_json::Value],
    ) -> Result<AppendedBatch> {
        self.append_many_to_state_json_on(self.branches.current_id(), state_id, items)
    }

    /// `append_many_to_state_json` against the given branch.
    pub fn append_many_to_state_json_on(
        &self,
        branch_id: BranchId,
        state_id: &str,
        items: &[serde_json::Value],
    ) -> Result<AppendedBatch> {
        let items = items
            .iter()
            .map(serde_json::to_vec)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        self.append_many_to_state_on(branch_id, state_id, items)
    }

    /// Append several JSON objects to an AppendLog state as one record, at
    /// one sequence, writing that record's id and sequence into each of them
    /// first: the batched form of `append_to_state_json_with_identity`, as
    /// `append_many_to_state_json` is of a plain JSON append.
    ///
    /// Each item is stamped exactly as `append_to_state_json_with_identity`
    /// stamps its one item: `id_field` receives the record id as a string,
    /// `sequence_field` the sequence as a number, existing values at those
    /// keys are overwritten, and the object is serialized as `serde_json`
    /// writes it. The batch is one record, so every item of it carries the
    /// same id and sequence, those of the record that holds it. A reader
    /// that cuts by an item's sequence (`create_branch_at` at that
    /// sequence keeps the item, at the one before drops it) keeps or drops
    /// the whole batch with it.
    ///
    /// Otherwise it is `append_many_to_state_json`: reads see the items as
    /// the same stamped objects appended one at a time, the batch counts as
    /// one operation toward the snapshot cadence, the returned
    /// `AppendedBatch` says which ordinals they took, and the first batch a
    /// store records marks its MANIFEST so that releases which predate
    /// batches refuse the store (see `update_state`). A store never asked to
    /// record a batch, by this call or another, keeps the format every
    /// release opens.
    ///
    /// Refused with `InvalidOperation`, writing nothing, when `items` is
    /// empty or any item isn't a JSON object. Once the batch is recorded,
    /// the call returns it: if the snapshot the write takes after it fails,
    /// that is logged and the next write tries it again, as for
    /// `update_state` (issue #39).
    pub fn append_many_to_state_json_with_identity(
        &self,
        state_id: &str,
        items: &[serde_json::Value],
        id_field: &str,
        sequence_field: &str,
    ) -> Result<AppendedBatch> {
        self.append_many_to_state_json_with_identity_on(
            self.branches.current_id(),
            state_id,
            items,
            id_field,
            sequence_field,
        )
    }

    /// `append_many_to_state_json_with_identity` against the given branch.
    pub fn append_many_to_state_json_with_identity_on(
        &self,
        branch_id: BranchId,
        state_id: &str,
        items: &[serde_json::Value],
        id_field: &str,
        sequence_field: &str,
    ) -> Result<AppendedBatch> {
        self.append_batch_with_builder(branch_id, state_id, |id, seq| {
            if let Some(at) = items.iter().position(|item| !item.is_object()) {
                return Err(StoreError::InvalidOperation(format!(
                    "payload must be a JSON object: item {} of the batch is not",
                    at
                )));
            }
            items
                .iter()
                .map(|item| stamp_identity(item.clone(), id_field, sequence_field, id, seq))
                .collect()
        })
    }

    /// The batched appends' common path: records the items `items_builder`
    /// gives, which may name the `(record_id, sequence)` the batch is about
    /// to take, as one `AppendMany` through `update_state_with_builder`, and
    /// reads the first ordinal under the same lock.
    fn append_batch_with_builder<F>(
        &self,
        branch_id: BranchId,
        state_id: &str,
        items_builder: F,
    ) -> Result<AppendedBatch>
    where
        F: FnOnce(RecordId, Sequence) -> Result<Vec<Vec<u8>>>,
    {
        let mut first_ordinal = 0;
        let record = self.update_state_with_builder(branch_id, state_id, false, None, |id, seq| {
            let items = items_builder(id, seq)?;
            // The builder runs under `write_lock`, so no other write to this
            // state can land between this count and the batch.
            first_ordinal = self
                .state
                .get_head(branch_id, state_id)
                .map(|h| h.item_count)
                .unwrap_or(0);
            Ok(StateOperation::AppendMany(items))
        })?;
        Ok(AppendedBatch {
            record,
            first_ordinal,
        })
    }

    /// Core update path. Builds the operation via a closure that receives the
    /// about-to-be-assigned `(record_id, sequence)`; callers that don't need
    /// those values pass a closure that ignores them. Holds `write_lock` for
    /// the whole operation so the peeked id/seq match what `log.append`
    /// actually assigns. Validation runs after the builder, so the builder
    /// can also surface `InvalidOperation` (e.g. for non-object payloads in
    /// `append_to_state_json_with_identity`).
    ///
    /// `snapshot_of` is `Some(offset)` when the operation is a snapshot whose
    /// content the caller read from this state while its chain head was at
    /// `offset`. If the head is still there once the write lock is held, the
    /// snapshot provably restates the current value, and the state manager
    /// keeps its cached values across it instead of dropping them. If
    /// another write got in between, the snapshot is recorded exactly as
    /// before and the caches are dropped as for any other write.
    fn update_state_with_builder<F>(
        &self,
        branch_id: BranchId,
        state_id: &str,
        skip_auto_snapshot: bool,
        snapshot_of: Option<u64>,
        operation_builder: F,
    ) -> Result<Record>
    where
        F: FnOnce(RecordId, Sequence) -> Result<StateOperation>,
    {
        let mut write = self.write_lock.lock();
        self.retry_metadata_checkpoint(&mut write)?;

        let branch = self.branch_by_id(branch_id)?;
        let prev_update_offset = self.state.get_head(branch.id, state_id).map(|h| h.head_offset);
        let next_seq = branch.head.next();
        // `peek_next_id` is only valid while `write_lock` is held — nothing
        // else can `log.append` between here and the call below.
        let next_id = self.log.peek_next_id();

        let operation = operation_builder(next_id, next_seq)?;

        if operation.contains_empty_batch() {
            return Err(StoreError::InvalidOperation(
                "AppendMany needs at least one item: an empty batch would be a record that \
                 appends nothing"
                    .into(),
            ));
        }

        // Validate operation WITHOUT loading full state (critical for 50M+ operations)
        // - Append, AppendMany: Always succeed, no validation needed (beyond
        //   the batch holding an item, above)
        // - Edit: Just check index < len
        // - Redact: No validation: when it's applied, a range past the end is
        //   clamped to the length and a reversed one removes nothing
        // - Set/Snapshot/DeltaSnapshot: No validation needed
        match &operation {
            StateOperation::Edit { index, .. } => {
                let len = self
                    .state
                    .get_head(branch.id, state_id)
                    .map(|h| h.item_count)
                    .unwrap_or(0);
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
                // flexibility.
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
            // Append, AppendMany, Set, Snapshot, DeltaSnapshot,
            // TreeDeltaSnapshot - no validation needed
            _ => {}
        }

        // A record a build without one of the format features can't read
        // goes into the log only once the MANIFEST names that feature, so
        // such a build refuses the store at open instead of part way
        // through it. If the MANIFEST can't be rewritten, nothing is written.
        self.require_features(&mut write, Self::features_of(&operation))?;

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
        let content_preserving = snapshot_of.is_some() && snapshot_of == prev_update_offset;
        self.state.record_update_with(
            branch.id,
            state_id,
            offset,
            &operation,
            content_preserving,
        )?;

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
        //
        // The write is recorded by now, so the snapshot's failure isn't
        // returned (issue #39): a caller that got an error would take the
        // write for one that didn't happen, and one that retried a write that
        // isn't idempotent would make it twice. It is logged instead. The
        // snapshot stays due, since only a snapshot record resets the state's
        // count of operations since the last one, so the next write to the
        // state on this branch checks the thresholds again and tries it.
        if !skip_auto_snapshot && self.auto_snapshot_enabled.load(Ordering::Relaxed) {
            // Drop the lock before calling auto_snapshot to avoid deadlock
            drop(write);
            // Same branch as the write above — never the cursor, which may
            // point elsewhere (or have moved) by now.
            if let Err(error) = self.auto_snapshot_if_needed(branch.id, state_id) {
                tracing::warn!(
                    %error,
                    state_id,
                    branch = %branch.name,
                    sequence = next_seq.0,
                    "the snapshot after a recorded state write failed: the write stands and \
                     returns its record, and the next write to the state tries the snapshot again"
                );
            }
        }

        Ok(record)
    }

    /// Get the current value of a state.
    pub fn get_state(&self, state_id: &str) -> Result<Option<Vec<u8>>> {
        self.get_state_on(self.branches.current_id(), state_id)
    }

    /// `get_state` against the given branch.
    pub fn get_state_on(&self, branch_id: BranchId, state_id: &str) -> Result<Option<Vec<u8>>> {
        self.branch_by_id(branch_id)?;
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
        self.get_state_at_on(self.branches.current_id(), state_id, at_sequence)
    }

    /// Get the value of a state at a specific sequence on a specific branch.
    ///
    /// This is like `get_state_at` but allows querying a branch other than the current one.
    pub fn get_state_at_on(
        &self,
        branch_id: BranchId,
        state_id: &str,
        at_sequence: Sequence,
    ) -> Result<Option<Vec<u8>>> {
        self.branch_by_id(branch_id)?;
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

    /// Rebuild the chain head a state had on `branch_id` at a historical
    /// sequence: the offset of the last update at or before `at_sequence`,
    /// plus the item count and snapshot accounting the chain carried there.
    ///
    /// Used by `create_branch_at` to point a new branch at an existing chain
    /// position without writing new records. Returns `None` if the state had
    /// no update at or before that sequence.
    ///
    /// The accounting is derived from the walk itself and mirrors what
    /// `StateManager::record_update` accumulates going forward, so a branch
    /// created at a historical point schedules its next snapshot exactly as
    /// the parent did when it stood there (and exactly as an ordinary
    /// `create_branch` taken at that moment would have). A full `Snapshot`
    /// resets every accounting field, so walking back to the newest one (or
    /// to the start of the chain) is enough to reproduce them all:
    ///
    /// - `ops_since_delta_snapshot`: ops newer than the newest snapshot of
    ///   either kind;
    /// - `delta_snapshots_since_full` / `last_delta_snapshot_offset`: delta
    ///   snapshots newer than the newest full one;
    /// - `has_non_append_since_snapshot`: any Redact/Edit/Set/Delta newer
    ///   than the newest full snapshot. This is the field that guards correctness —
    ///   while it is set no append-only delta snapshot may be taken;
    /// - `last_full_snapshot_offset` / `item_count_at_last_full`: that newest
    ///   full snapshot and the number of items it holds.
    ///
    /// A `Set` ends the walk for the VALUE (it replaces the state) but not
    /// for the accounting (`record_update` counts it as an ordinary
    /// non-append op), so for strategies that schedule snapshots the walk
    /// continues past it to the previous full snapshot. `Snapshot`-strategy
    /// states are written by `Set` alone and never schedule a snapshot, so
    /// for them the walk stops at the first `Set` as before — their
    /// accounting fields are reporting-only and cover just the ops seen.
    ///
    /// `known_full` lets the walk stop WITHOUT reading the full snapshot it
    /// ends at: `(offset, item_count)` of a full snapshot the caller knows
    /// lies on this chain at or before `at_sequence`, and how many items it
    /// holds (the parent head's `last_full_snapshot_offset` and
    /// `item_count_at_last_full`). A full snapshot record carries the whole
    /// state, so reading it is what makes a historical fork cost in
    /// proportion to the state's size; with it known, the walk reads only
    /// the records written since. Applies to array-shaped states, whose
    /// item count follows from lengths; if the count cannot be decided that
    /// way the walk is redone in full.
    fn find_chain_head_at(
        &self,
        branch_id: crate::types::BranchId,
        state_id: &str,
        at_sequence: Sequence,
        known_full: Option<(u64, usize)>,
    ) -> Result<Option<crate::state::StateChainHead>> {
        let head = match self.state.get_head(branch_id, state_id) {
            Some(h) => h,
            None => return Ok(None),
        };

        let strategy = self.state.get_strategy(state_id);
        let schedules_snapshots = !matches!(
            strategy,
            None | Some(crate::types::StateStrategy::Snapshot)
        );

        // Walk chain to find the last update at or before target sequence
        let mut current_offset = Some(head.head_offset);
        let mut found_offset: Option<u64> = None;

        // Value reconstruction (same collection rule as reconstruct_from_disk),
        // used only to count items.
        let mut operations = Vec::new();
        let mut hit_snapshot = false;
        let mut value_complete = false;
        // Length of the array the collected operations apply on top of, when
        // the value's base is a known full snapshot that was not read.
        let mut known_base_len: Option<usize> = None;

        // Snapshot accounting, gathered newest-to-oldest.
        let mut ops_since_delta_snapshot = 0u64;
        let mut seen_snapshot = false;
        let mut delta_snapshots_since_full = 0u64;
        let mut last_delta_snapshot_offset: Option<u64> = None;
        let mut last_full_snapshot: Option<(u64, usize)> = None;
        let mut has_non_append_since_snapshot = false;

        while let Some(offset) = current_offset {
            if let Some((full_offset, full_len)) = known_full {
                if offset == full_offset {
                    // A full snapshot at or before the target, of known
                    // length: everything the walk needs from it.
                    if found_offset.is_none() {
                        found_offset = Some(offset);
                    }
                    last_full_snapshot = Some((offset, full_len));
                    if !value_complete {
                        known_base_len = Some(full_len);
                    }
                    break;
                }
            }

            let record = self.log.read_at(offset)?;

            let update = StateUpdateRecord::decode(&record)?;

            // Skip if this record is after the target sequence
            if record.sequence > at_sequence {
                current_offset = update.prev_update_offset;
                continue;
            }

            // This is the first record at or before target - it becomes our head
            if found_offset.is_none() {
                found_offset = Some(offset);
            }

            let is_full_snapshot = matches!(update.operation, StateOperation::Snapshot(_));

            // Accounting.
            match &update.operation {
                StateOperation::Snapshot(data) => {
                    last_full_snapshot =
                        Some((offset, crate::state::snapshot_item_count(data)));
                }
                StateOperation::DeltaSnapshot(_) | StateOperation::TreeDeltaSnapshot(_) => {
                    if last_delta_snapshot_offset.is_none() {
                        last_delta_snapshot_offset = Some(offset);
                    }
                    delta_snapshots_since_full += 1;
                    seen_snapshot = true;
                }
                other => {
                    if !seen_snapshot {
                        ops_since_delta_snapshot += 1;
                    }
                    if crate::state::is_non_append_change(other) {
                        has_non_append_since_snapshot = true;
                    }
                }
            }

            // Collect operations to compute item_count
            if !value_complete {
                match &update.operation {
                    StateOperation::Snapshot(_) | StateOperation::Set(_) => {
                        // Full-state terminal (Set replaces state exactly like
                        // Snapshot in materialize_operations); nothing older
                        // contributes to the value.
                        operations.push(update.operation.clone());
                        value_complete = true;
                    }
                    StateOperation::DeltaSnapshot(_) | StateOperation::TreeDeltaSnapshot(_) => {
                        operations.push(update.operation.clone());
                        hit_snapshot = true;
                    }
                    _ => {
                        if !hit_snapshot {
                            operations.push(update.operation.clone());
                        }
                    }
                }
            }

            if is_full_snapshot || (value_complete && !schedules_snapshots) {
                break;
            }

            current_offset = update.prev_update_offset;
        }

        let head_offset = match found_offset {
            Some(o) => o,
            None => return Ok(None), // No state at that sequence
        };

        operations.reverse();
        let is_tree = matches!(strategy, Some(crate::types::StateStrategy::Tree { .. }));

        // Count items in the resulting state. Array-shaped states are counted
        // from lengths alone; anything that cannot be decided that way (tree
        // states, non-array values, ops materialization would reject) takes
        // the materializing path, which also surfaces any chain error.
        let counted = if is_tree {
            None
        } else {
            match known_base_len {
                Some(base) => crate::state::array_len_from(base, &operations),
                None => crate::state::array_len_after(&operations),
            }
        };
        let item_count = match counted {
            Some(n) => n,
            None if known_full.is_some() => {
                // Not decidable from lengths: redo the walk reading the
                // snapshot, so the value can be materialized.
                return self.find_chain_head_at(branch_id, state_id, at_sequence, None);
            }
            None => {
                let state = crate::state::materialize_operations(operations)?;
                if state.is_empty() {
                    0
                } else if is_tree {
                    serde_json::from_slice::<TreeState>(&state)
                        .map(|t| t.len())
                        .unwrap_or(0)
                } else {
                    serde_json::from_slice::<Vec<serde_json::Value>>(&state)
                        .map(|arr| arr.len())
                        .unwrap_or(0)
                }
            }
        };

        Ok(Some(crate::state::StateChainHead {
            head_offset,
            ops_since_delta_snapshot,
            delta_snapshots_since_full,
            last_delta_snapshot_offset,
            last_full_snapshot_offset: last_full_snapshot.map(|(offset, _)| offset),
            has_non_append_since_snapshot,
            item_count,
            item_count_at_last_full: last_full_snapshot.map(|(_, n)| n).unwrap_or(0),
        }))
    }

    /// Get the length of an AppendLog state without loading all items.
    ///
    /// This is O(1) - the count is tracked in the state chain head.
    /// Returns None if state doesn't exist, Some(0) for empty state.
    pub fn get_state_len(&self, state_id: &str) -> Result<Option<usize>> {
        self.get_state_len_on(self.branches.current_id(), state_id)
    }

    /// `get_state_len` against the given branch.
    pub fn get_state_len_on(&self, branch_id: BranchId, state_id: &str) -> Result<Option<usize>> {
        self.branch_by_id(branch_id)?;
        match self.state.get_head(branch_id, state_id) {
            Some(head) => Ok(Some(head.item_count)),
            None => Ok(None),
        }
    }

    /// The log offset of a state's newest chain record on the current
    /// branch, or `None` if the state has no value there. See
    /// [`Store::state_head_offset_on`].
    pub fn state_head_offset(&self, state_id: &str) -> Result<Option<u64>> {
        self.state_head_offset_on(self.branches.current_id(), state_id)
    }

    /// `state_head_offset` against the given branch.
    ///
    /// The head offset changes on every write to that state on that branch
    /// — append, edit, redact, set, and the snapshots the store adds after
    /// them — and on nothing else, so it is an exact, O(1) revision key for
    /// a cached copy of the value. Two branches that share an unchanged
    /// inherited value report the same offset. Unlike an item count it
    /// cannot be fooled by a count-preserving edit; unlike the store's
    /// sequence it does not move when other states are written.
    pub fn state_head_offset_on(&self, branch_id: BranchId, state_id: &str) -> Result<Option<u64>> {
        self.branch_by_id(branch_id)?;
        Ok(self.state.get_head(branch_id, state_id).map(|head| head.head_offset))
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
        self.get_state_slice_on(self.branches.current_id(), state_id, offset, limit)
    }

    /// `get_state_slice` against the given branch.
    pub fn get_state_slice_on(
        &self,
        branch_id: BranchId,
        state_id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Option<Vec<u8>>> {
        self.branch_by_id(branch_id)?;
        let items = match self.state.get_state_items(branch_id, state_id)? {
            Some(i) => i,
            None => return Ok(None),
        };

        let start = offset.min(items.len());
        let end = offset.saturating_add(limit).min(items.len());

        Ok(Some(StateManager::join_items(&items, start, end)))
    }

    /// The items of an AppendLog state on the given branch, each as its raw
    /// JSON bytes, without copying them: the returned value is the cached
    /// one, shared with every other reader and with any other branch whose
    /// chain head for this state is at the same log offset.
    pub fn state_items_on(
        &self,
        branch_id: BranchId,
        state_id: &str,
    ) -> Result<Option<Arc<crate::state::StateItems>>> {
        self.branch_by_id(branch_id)?;
        self.state.get_state_items(branch_id, state_id)
    }

    /// Get a single item of an AppendLog state by index, as raw JSON bytes.
    ///
    /// O(item size) once the per-item cache is warm (see `get_state_slice`).
    /// Returns `None` if the state doesn't exist, the index is out of range,
    /// or the state's value isn't an array (where `get_state_slice` errors).
    pub fn get_state_item(&self, state_id: &str, index: usize) -> Result<Option<Vec<u8>>> {
        self.get_state_item_on(self.branches.current_id(), state_id, index)
    }

    /// `get_state_item` against the given branch.
    pub fn get_state_item_on(
        &self,
        branch_id: BranchId,
        state_id: &str,
        index: usize,
    ) -> Result<Option<Vec<u8>>> {
        self.branch_by_id(branch_id)?;
        // Delegates to the manager, which serves the just-appended last item
        // from the head record in O(1) instead of re-materializing the whole
        // state (see StateManager::get_state_item).
        self.state.get_state_item(branch_id, state_id, index)
    }

    /// Several items of an AppendLog state by ordinal, as raw JSON bytes, in
    /// one call: for each ordinal, in the order given, the item
    /// `get_state_item` returns for it, `None` where the ordinal is out of
    /// range, the state has no value, or its value isn't an array. Ordinals
    /// may repeat and come in any order. All of them are answered from one
    /// version of the state, read at most once, and an empty request reads
    /// nothing (see `StateManager::get_state_items_by_ordinal`).
    pub fn get_state_items_by_ordinal(
        &self,
        state_id: &str,
        ordinals: &[usize],
    ) -> Result<Vec<Option<Vec<u8>>>> {
        self.get_state_items_by_ordinal_on(self.branches.current_id(), state_id, ordinals)
    }

    /// `get_state_items_by_ordinal` against the given branch.
    pub fn get_state_items_by_ordinal_on(
        &self,
        branch_id: BranchId,
        state_id: &str,
        ordinals: &[usize],
    ) -> Result<Vec<Option<Vec<u8>>>> {
        self.branch_by_id(branch_id)?;
        self.state
            .get_state_items_by_ordinal(branch_id, state_id, ordinals)
    }

    /// Get the last N items from an AppendLog state.
    ///
    /// Reads back from the head only as far as it needs, decoding each
    /// snapshot or Set it takes items from. Where it meets a Redact, an Edit
    /// or a Delta, or `count` reaches the stored item count, it reads the
    /// whole value instead. Returns at most `count` items.
    pub fn get_state_tail(&self, state_id: &str, count: usize) -> Result<Option<Vec<u8>>> {
        self.get_state_tail_on(self.branches.current_id(), state_id, count)
    }

    /// `get_state_tail` against the given branch.
    pub fn get_state_tail_on(
        &self,
        branch_id: BranchId,
        state_id: &str,
        count: usize,
    ) -> Result<Option<Vec<u8>>> {
        self.branch_by_id(branch_id)?;
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
                StateOperation::AppendMany(items) => {
                    // Newest first, as the walk collects; only as many as
                    // the tail still needs.
                    for item in items.iter().rev() {
                        if items_collected.len() >= count {
                            break;
                        }
                        let value: serde_json::Value = serde_json::from_slice(item)
                            .map_err(|e| StoreError::Deserialization(e.to_string()))?;
                        items_collected.push(value);
                    }
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
            let state = self.get_state_on(branch_id, state_id)?.unwrap_or_default();
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
        self.iter_state_items_on(self.branches.current_id(), state_id)
    }

    /// `iter_state_items` against the given branch.
    pub fn iter_state_items_on(
        &self,
        branch_id: BranchId,
        state_id: &str,
    ) -> Result<Option<StateItemIterator>> {
        self.branch_by_id(branch_id)?;
        let head = match self.state.get_head(branch_id, state_id) {
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
        self.snapshot_needed(state_id).is_some()
    }

    /// Get what type of snapshot is needed (if any).
    pub fn snapshot_needed(&self, state_id: &str) -> Option<crate::state::SnapshotNeeded> {
        self.snapshot_needed_on(self.branches.current_id(), state_id)
    }

    /// `snapshot_needed` against the given branch (`None` if it was deleted).
    pub fn snapshot_needed_on(
        &self,
        branch_id: BranchId,
        state_id: &str,
    ) -> Option<crate::state::SnapshotNeeded> {
        if !self.branch_exists(branch_id) {
            return None;
        }
        self.state.snapshot_needed(branch_id, state_id)
    }

    /// Get compaction statistics for a state.
    ///
    /// Returns information about how many operations could be compacted.
    pub fn get_compaction_stats(
        &self,
        state_id: &str,
    ) -> Option<crate::state::CompactionStats> {
        self.get_compaction_stats_on(self.branches.current_id(), state_id)
    }

    /// `get_compaction_stats` against the given branch (`None` if it was deleted).
    pub fn get_compaction_stats_on(
        &self,
        branch_id: BranchId,
        state_id: &str,
    ) -> Option<crate::state::CompactionStats> {
        if !self.branch_exists(branch_id) {
            return None;
        }
        self.state.get_compaction_stats(branch_id, state_id)
    }

    /// Get detailed chain statistics for a state.
    ///
    /// This traverses the entire chain to count operations and bytes.
    pub fn get_chain_stats(
        &self,
        state_id: &str,
    ) -> Result<Option<crate::state::ChainStats>> {
        self.get_chain_stats_on(self.branches.current_id(), state_id)
    }

    /// `get_chain_stats` against the given branch.
    pub fn get_chain_stats_on(
        &self,
        branch_id: BranchId,
        state_id: &str,
    ) -> Result<Option<crate::state::ChainStats>> {
        self.branch_by_id(branch_id)?;
        self.state.count_chain_operations(branch_id, state_id)
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
        self.compact_state_on(self.branches.current_id(), state_id)
    }

    /// `compact_state` against the given branch.
    pub fn compact_state_on(&self, branch_id: BranchId, state_id: &str) -> Result<Option<Record>> {
        let read_at = self.state.get_head(branch_id, state_id).map(|h| h.head_offset);
        let current = match self.get_state_on(branch_id, state_id)? {
            Some(s) => s,
            None => return Ok(None),
        };

        // Create a full snapshot with the current state
        let record = self.update_state_internal(
            branch_id,
            state_id,
            StateOperation::Snapshot(current),
            false,
            read_at,
        )?;
        Ok(Some(record))
    }

    /// Compact all states by creating full snapshots.
    ///
    /// Returns the number of states compacted.
    pub fn compact_all_states(&self) -> Result<usize> {
        self.compact_all_states_on(self.branches.current_id())
    }

    /// `compact_all_states` against the given branch.
    pub fn compact_all_states_on(&self, branch_id: BranchId) -> Result<usize> {
        let state_ids = self.state.state_ids();
        let mut compacted = 0;

        for state_id in state_ids {
            if self.compact_state_on(branch_id, &state_id)?.is_some() {
                compacted += 1;
            }
        }

        Ok(compacted)
    }

    /// Get compaction summary for all states.
    ///
    /// Returns total operations and bytes that could be skipped after compaction.
    pub fn get_compaction_summary(&self) -> Result<CompactionSummary> {
        self.get_compaction_summary_on(self.branches.current_id())
    }

    /// `get_compaction_summary` against the given branch.
    pub fn get_compaction_summary_on(&self, branch_id: BranchId) -> Result<CompactionSummary> {
        let state_ids = self.state.state_ids();
        let mut total_operations = 0u64;
        let mut compactable_operations = 0u64;
        let mut total_bytes = 0u64;
        let mut compactable_bytes = 0u64;
        let mut states_with_history = 0usize;

        for state_id in state_ids {
            if let Some(stats) = self.get_chain_stats_on(branch_id, &state_id)? {
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
        self.create_snapshot_if_needed_on(self.branches.current_id(), state_id)
    }

    /// `create_snapshot_if_needed` against the given branch.
    pub fn create_snapshot_if_needed_on(
        &self,
        branch_id: BranchId,
        state_id: &str,
    ) -> Result<Option<Record>> {
        self.create_snapshot_if_needed_internal(branch_id, state_id, false)
    }

    /// Internal snapshot creation that can skip auto-snapshot to avoid recursion.
    fn create_snapshot_if_needed_internal(
        &self,
        branch_id: BranchId,
        state_id: &str,
        skip_auto: bool,
    ) -> Result<Option<Record>> {
        use crate::state::SnapshotNeeded;

        self.branch_by_id(branch_id)?;
        // The head the snapshot's content is read at (see `snapshot_of`).
        let read_at = self.state.get_head(branch_id, state_id).map(|h| h.head_offset);
        let full = || -> Result<Option<Record>> {
            let current = self.get_state_on(branch_id, state_id)?.unwrap_or_default();
            let record = self.update_state_internal(
                branch_id,
                state_id,
                StateOperation::Snapshot(current),
                skip_auto,
                read_at,
            )?;
            Ok(Some(record))
        };

        match self.state.snapshot_needed(branch_id, state_id) {
            None => Ok(None),
            Some(SnapshotNeeded::Full) => full(),
            Some(SnapshotNeeded::Delta) => {
                // Check if this is a Tree strategy (uses TreeDeltaSnapshot) or AppendLog (uses DeltaSnapshot)
                let is_tree = matches!(
                    self.state.get_strategy(state_id),
                    Some(crate::types::StateStrategy::Tree { .. })
                );
                if is_tree {
                    let delta_ops = self.compute_tree_delta_ops(branch_id, state_id)?;
                    let record = self.update_state_internal(
                        branch_id,
                        state_id,
                        StateOperation::TreeDeltaSnapshot(delta_ops),
                        skip_auto,
                        read_at,
                    )?;
                    Ok(Some(record))
                } else {
                    match self.compute_delta_items(branch_id, state_id)? {
                        Some(delta_items) => {
                            let record = self.update_state_internal(
                                branch_id,
                                state_id,
                                StateOperation::DeltaSnapshot(delta_items),
                                skip_auto,
                                read_at,
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
    fn auto_snapshot_if_needed(&self, branch_id: BranchId, state_id: &str) -> Result<()> {
        self.create_snapshot_if_needed_internal(branch_id, state_id, true)?;
        Ok(())
    }

    /// Compute items added since the last delta or full snapshot.
    ///
    /// This walks the chain collecting the items of Append and AppendMany
    /// operations until hitting a full or delta snapshot, and returns None if
    /// it meets a Redact, an Edit, a Set or a Delta first. A delta snapshot
    /// replaces every operation it covers on later reads, so an append-only
    /// operation this walk skipped would lose its items.
    fn compute_delta_items(&self, branch_id: BranchId, state_id: &str) -> Result<Option<Vec<u8>>> {
        let head = match self.state.get_head(branch_id, state_id) {
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
                StateOperation::AppendMany(items) => {
                    // Newest first, like the walk: the reverse below puts
                    // the batch back in its own order.
                    for item in items.iter().rev() {
                        let value: serde_json::Value = serde_json::from_slice(item)
                            .map_err(|e| StoreError::Deserialization(e.to_string()))?;
                        appended_items.push(value);
                    }
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
    fn compute_tree_delta_ops(&self, branch_id: BranchId, state_id: &str) -> Result<Vec<TreeOp>> {
        let head = match self.state.get_head(branch_id, state_id) {
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
        self.register_state_field_index_on(self.branches.current_id(), state_id, field_path, kind)
    }

    /// `register_state_field_index` against the given branch.
    pub fn register_state_field_index_on(
        &self,
        branch_id: BranchId,
        state_id: &str,
        field_path: &str,
        kind: crate::state::FieldIndexKind,
    ) -> Result<()> {
        let mut write = self.write_lock.lock();
        self.retry_metadata_checkpoint(&mut write)?;
        self.branch_by_id(branch_id)?;
        self.state
            .register_field_index(branch_id, state_id, field_path, kind)
    }

    /// Query ordinals of a registered `Number` field index within
    /// `[gte, lte]` (either bound optional), scoped to the CURRENT branch.
    /// Returns matching ordinals only — fetch content separately via
    /// `get_state_item`/`get_state_slice`.
    ///
    /// Returns `None` if no such index is currently registered for the
    /// current branch (never registered there, wrong kind, poisoned by a
    /// parse failure, or — the case a pure `switch_branch` hits —
    /// registered only on a *different* branch than the one you're on now;
    /// indexes are per branch and a new branch starts with none) — distinct from
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
        self.query_state_index_range_on(
            self.branches.current_id(),
            state_id,
            field_path,
            gte,
            lte,
            limit,
            offset,
            reverse,
        )
    }

    /// `query_state_index_range` against the given branch (`None` if it was
    /// deleted, as for any branch without a registered index).
    #[allow(clippy::too_many_arguments)]
    pub fn query_state_index_range_on(
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
        if !self.branch_exists(branch_id) {
            return None;
        }
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
        self.query_state_index_eq_on(
            self.branches.current_id(),
            state_id,
            field_path,
            value,
            limit,
            offset,
        )
    }

    /// `query_state_index_eq` against the given branch.
    pub fn query_state_index_eq_on(
        &self,
        branch_id: BranchId,
        state_id: &str,
        field_path: &str,
        value: &str,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Option<Vec<u32>> {
        if !self.branch_exists(branch_id) {
            return None;
        }
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
        self.get_state_index_value_counts_on(self.branches.current_id(), state_id, field_path)
    }

    /// `get_state_index_value_counts` against the given branch.
    pub fn get_state_index_value_counts_on(
        &self,
        branch_id: BranchId,
        state_id: &str,
        field_path: &str,
    ) -> Option<Vec<(String, u32)>> {
        if !self.branch_exists(branch_id) {
            return None;
        }
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
        self.tree_set_on(self.branches.current_id(), state_id, path, entry)
    }

    /// `tree_set` against the given branch.
    pub fn tree_set_on(
        &self,
        branch_id: BranchId,
        state_id: &str,
        path: &str,
        entry: &TreeEntry,
    ) -> Result<Record> {
        Self::validate_tree_path(path)?;
        self.update_state_on(
            branch_id,
            state_id,
            StateOperation::TreeSet {
                path: path.to_string(),
                entry: entry.clone(),
            },
        )
    }

    /// Remove a single file from a tree state.
    pub fn tree_remove(&self, state_id: &str, path: &str) -> Result<Record> {
        self.tree_remove_on(self.branches.current_id(), state_id, path)
    }

    /// `tree_remove` against the given branch.
    pub fn tree_remove_on(&self, branch_id: BranchId, state_id: &str, path: &str) -> Result<Record> {
        Self::validate_tree_path(path)?;
        self.update_state_on(
            branch_id,
            state_id,
            StateOperation::TreeRemove {
                path: path.to_string(),
            },
        )
    }

    /// Apply a batch of tree operations atomically.
    pub fn tree_batch(&self, state_id: &str, ops: &[TreeOp]) -> Result<Record> {
        self.tree_batch_on(self.branches.current_id(), state_id, ops)
    }

    /// `tree_batch` against the given branch.
    pub fn tree_batch_on(&self, branch_id: BranchId, state_id: &str, ops: &[TreeOp]) -> Result<Record> {
        for op in ops {
            match op {
                TreeOp::Set { path, .. } | TreeOp::Remove { path } => {
                    Self::validate_tree_path(path)?;
                }
            }
        }
        self.update_state_on(branch_id, state_id, StateOperation::TreeBatch { ops: ops.to_vec() })
    }

    /// Get a single file entry from a tree state.
    pub fn tree_get(&self, state_id: &str, path: &str) -> Result<Option<TreeEntry>> {
        self.tree_get_on(self.branches.current_id(), state_id, path)
    }

    /// `tree_get` against the given branch.
    pub fn tree_get_on(
        &self,
        branch_id: BranchId,
        state_id: &str,
        path: &str,
    ) -> Result<Option<TreeEntry>> {
        self.branch_by_id(branch_id)?;
        let tree = match self.state.get_tree_state(branch_id, state_id)? {
            Some(t) => t,
            None => return Ok(None),
        };
        Ok(tree.get(path).cloned())
    }

    /// List files in a tree state, optionally filtered by path prefix.
    pub fn tree_list(&self, state_id: &str, prefix: Option<&str>) -> Result<Vec<(String, TreeEntry)>> {
        self.tree_list_on(self.branches.current_id(), state_id, prefix)
    }

    /// `tree_list` against the given branch.
    pub fn tree_list_on(
        &self,
        branch_id: BranchId,
        state_id: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<(String, TreeEntry)>> {
        self.branch_by_id(branch_id)?;
        let tree = match self.state.get_tree_state(branch_id, state_id)? {
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
        self.tree_diff_on(self.branches.current_id(), state_id, from_seq, to_seq)
    }

    /// `tree_diff` against the given branch.
    pub fn tree_diff_on(
        &self,
        branch_id: BranchId,
        state_id: &str,
        from_seq: Sequence,
        to_seq: Sequence,
    ) -> Result<Vec<TreeChange>> {
        let from_state = self.get_state_at_on(branch_id, state_id, from_seq)?;
        let to_state = self.get_state_at_on(branch_id, state_id, to_seq)?;

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
        self.tree_snapshot_on(self.branches.current_id(), state_id)
    }

    /// `tree_snapshot` against the given branch.
    pub fn tree_snapshot_on(&self, branch_id: BranchId, state_id: &str) -> Result<Option<Record>> {
        let read_at = self.state.get_head(branch_id, state_id).map(|h| h.head_offset);
        let current = match self.get_state_on(branch_id, state_id)? {
            Some(s) => s,
            None => return Ok(None),
        };
        let record = self.update_state_internal(
            branch_id,
            state_id,
            StateOperation::Snapshot(current),
            true, // skip auto-snapshot
            read_at,
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
        self.create_branch_inheriting(name, from, None)
    }

    /// Create a new branch from a branch head, inheriting only the states
    /// `selection` covers (see [`StateSelection`]).
    ///
    /// Selected states are inherited exactly as `create_branch` inherits them
    /// (the parent's chain head is copied verbatim, snapshot accounting
    /// included). Every other state has no value on the new branch: it reads
    /// there as a registered-but-empty state does on a `create_empty_branch`
    /// branch, and a write starts a fresh chain.
    ///
    /// Copying the heads costs in proportion to the number of states
    /// inherited, not to the number of states the parent holds or to the
    /// number of branches; the checkpoint that ends the call still saves every
    /// branch's heads.
    pub fn create_branch_with_states(
        &self,
        name: &str,
        from: Option<&str>,
        selection: &StateSelection,
    ) -> Result<Branch> {
        self.create_branch_inheriting(name, from, Some(selection))
    }

    fn create_branch_inheriting(
        &self,
        name: &str,
        from: Option<&str>,
        selection: Option<&StateSelection>,
    ) -> Result<Branch> {
        // Held so the branch point and the copied chain heads describe the
        // same instant: no write can land on the parent between the two.
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
        self.state
            .copy_heads_for_branch(parent.id, new_branch.id, selection);

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
        self.create_branch_at_inheriting(name, from, at, None)
    }

    /// Create a branch at a specific sequence, inheriting only the states
    /// `selection` covers. The historical counterpart of
    /// [`Store::create_branch_with_states`]; unselected states have no value
    /// on the new branch.
    pub fn create_branch_at_with_states(
        &self,
        name: &str,
        from: &str,
        at: Sequence,
        selection: &StateSelection,
    ) -> Result<Branch> {
        self.create_branch_at_inheriting(name, from, at, Some(selection))
    }

    fn create_branch_at_inheriting(
        &self,
        name: &str,
        from: &str,
        at: Sequence,
        selection: Option<&StateSelection>,
    ) -> Result<Branch> {
        let mut write = self.write_lock.lock();
        self.retry_metadata_checkpoint(&mut write)?;

        let parent = self
            .branches
            .get_branch(from)
            .ok_or_else(|| StoreError::BranchNotFound(from.to_string()))?;

        // Checked up front (in the order the branch manager reports them) so
        // a doomed call fails before any chain is walked, and every fallible
        // chain read comes before the branch is allocated: a corrupt
        // historical chain must not leave a partly constructed branch for a
        // subsequent mutation (or Drop) to checkpoint.
        if self.branches.get_branch(name).is_some() {
            return Err(StoreError::BranchExists(name.to_string()));
        }
        if at > parent.head {
            return Err(StoreError::InvalidSequence(at, parent.head));
        }

        // The log is append-only, so when `at` names a record written on the
        // parent itself, "at or before `at` on this chain" is the same as
        // "at or before that record's offset": everything older in the chain
        // (the parent's earlier records and whatever it inherited when it was
        // created) was written before it, everything newer after it. A state
        // whose head already sits at or before that offset has not changed
        // since `at`, and its current head IS its historical head — taken
        // verbatim, with no chain walk and no record read. `None` when `at`
        // is not one of the parent's own sequences (at or before its own
        // branch point); every state then takes the walk. Tree states always
        // take it: their live `item_count` is an upper-bound estimate, and a
        // historical branch has always been given the exact entry count.
        let at_offset = self.index.get_offset(parent.id, at);

        // Rebuild the heads before creating the branch, so a chain error
        // cannot leave a half-populated branch behind.
        let mut heads = Vec::new();
        for (state_id, parent_head) in self.state.selected_heads(parent.id, selection) {
            let strategy = self.state.get_strategy(&state_id);
            let is_tree = matches!(strategy, Some(crate::types::StateStrategy::Tree { .. }));
            let head = match at_offset {
                Some(offset) if !is_tree && parent_head.head_offset <= offset => {
                    Some(parent_head)
                }
                _ => {
                    // The parent's last full snapshot, if it predates the
                    // branch point and its length is on record, bounds the
                    // walk without being read. `item_count_at_last_full` is
                    // 0 on heads written before that field existed (and on
                    // an empty snapshot, which costs nothing to read), so 0
                    // means "not on record".
                    let known_full = match (
                        at_offset,
                        parent_head.last_full_snapshot_offset,
                        &strategy,
                    ) {
                        (
                            Some(at_offset),
                            Some(full_offset),
                            Some(crate::types::StateStrategy::AppendLog { .. }),
                        ) if full_offset <= at_offset && parent_head.item_count_at_last_full > 0 => {
                            Some((full_offset, parent_head.item_count_at_last_full))
                        }
                        _ => None,
                    };
                    self.find_chain_head_at(parent.id, &state_id, at, known_full)?
                }
            };
            if let Some(head) = head {
                heads.push((state_id, head));
            }
        }

        let new_branch = self.branches.create_branch_at(name, from, at)?;

        // Point the new branch's state heads at the parent's chain at the branch point.
        // No new records are written - the branch shares the existing chain up to this point.
        for (state_id, head) in heads {
            self.state
                .insert_head_for_branch(new_branch.id, &state_id, head);
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
    ///
    /// Besides removing the branch, this releases what the store held for
    /// it: its cached state values and field indexes immediately, and its
    /// chain heads once the deletion has been saved (this call's checkpoint,
    /// or the next `sync`), so neither memory nor `state.bin` keeps growing
    /// with branches that no longer exist. Records the branch wrote stay in
    /// the append-only log.
    ///
    /// If the checkpoint fails, the error is returned but the branch stays
    /// deleted in memory, and the next successful checkpoint publishes the
    /// deletion and tells subscribers: `sync()`, or the retry that every
    /// later mutation makes first. Calling this again retries that checkpoint
    /// first and, once it succeeds, returns `BranchNotFound`.
    pub fn delete_branch(&self, name: &str) -> Result<()> {
        // Serialized with writes, so a write that has already resolved this
        // branch finishes before the branch disappears under it.
        let mut write = self.write_lock.lock();
        self.retry_metadata_checkpoint(&mut write)?;

        let branch_id = self.branches.get_branch(name).map(|b| b.id);
        self.branches.delete_branch(name)?;
        if let Some(branch_id) = branch_id {
            self.state.retire_branch(branch_id);
        }

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

    /// Make the MANIFEST name `required` format features before a record
    /// that needs them is written. A no-op once it names them all, so it
    /// writes the MANIFEST at most once per feature over the store's life.
    /// Call only while holding write_lock.
    fn require_features(&self, write: &mut WriteState, required: u64) -> Result<()> {
        if required & !write.features == 0 {
            return Ok(());
        }
        let features = write.features | required;
        Self::write_manifest_features(&self.config.path, features).map_err(|error| match error {
            StoreError::Io(error) => StoreError::Io(std::io::Error::new(
                error.kind(),
                format!(
                    "the batch was not recorded, because of an error marking {} for batched appends: {error}",
                    self.config.path.join("MANIFEST").display()
                ),
            )),
            other => other,
        })?;
        write.features = features;
        Ok(())
    }

    /// The format features a store must name to hold `operation`.
    fn features_of(operation: &StateOperation) -> u64 {
        if operation.contains_append_many() {
            FEATURE_APPEND_MANY
        } else {
            0
        }
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
        // Deleted branches are now gone from branches.bin, so their chain
        // heads can go too (see `StateManager::retire_branch` for why not
        // before). They drop out of state.bin at the next sync.
        self.state.purge_retired_branches();
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

    /// How many times a state value has been rebuilt from the log (walked
    /// back to its last full snapshot and re-materialized) since this store
    /// was opened. Reads served from the state caches — including a value
    /// another branch materialized at the same chain head — do not count.
    /// A diagnostic counter: use it to confirm that a read was (or was not)
    /// served from memory instead of timing it.
    pub fn state_reconstruction_count(&self) -> u64 {
        self.state.reconstruction_count()
    }

    /// Drop every cached materialized state value. The next read of each
    /// state rebuilds it from the log. For tests and memory pressure; never
    /// needed for correctness.
    pub fn clear_state_caches(&self) {
        self.state.clear_cache()
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
        self.catch_up_subscription_on(self.branches.current_id(), id)
    }

    /// `catch_up_subscription`, replaying the given branch's states and
    /// records instead of the current branch's.
    pub fn catch_up_subscription_on(&self, branch_id: BranchId, id: SubscriptionId) -> Result<()> {
        let current_branch = self.branch_by_id(branch_id)?;
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
                let (state_data, snapshot_seq) = match self.get_state_at_on(branch_id, &state_id, from_seq)? {
                    Some(data) => (data, from_seq),
                    None => {
                        // State didn't exist at from_seq, try current state
                        match self.get_state_on(branch_id, &state_id)? {
                            Some(data) => (data, current_branch.head),
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
            let payload_threshold = 4096; // Same as manager default

            for result in self.iter_from_on(branch_id, from_seq) {
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

    /// The files whose presence means a directory already holds a store.
    const STORE_FILES: [&'static str; 4] = ["MANIFEST", "records.log", "state.bin", "branches.bin"];

    fn present_store_file(path: &Path) -> std::io::Result<Option<&'static str>> {
        for file in Self::STORE_FILES {
            match fs::symlink_metadata(path.join(file)) {
                Ok(_) => return Ok(Some(file)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(std::io::Error::new(
                        error.kind(),
                        format!("cannot tell whether {} holds a store's {file}: {error}", path.display()),
                    ))
                }
            }
        }
        Ok(None)
    }

    // Published whole, through a staging file renamed over it, so a crash
    // mid-write can't leave a MANIFEST that create refuses and open can't read.
    // Only create calls it, under the lock and once it has found no MANIFEST,
    // so a version 2 MANIFEST is never written back to version 1.
    fn write_manifest(path: &Path) -> Result<()> {
        crate::atomic_file::Checkpoint::default().save(&path.join("MANIFEST"), &[STORE_MAGIC, &[STORE_VERSION]])
    }

    /// Rewrite the MANIFEST as format version 2 naming `features`. It goes
    /// through a new file beside it, synced and renamed over it, so a crash
    /// leaves the old MANIFEST or the new one, never a torn one; on Unix the
    /// directory is synced too, so the new one is on disk before this
    /// returns. A version 1 store is never written back: features are only
    /// ever added.
    fn write_manifest_features(path: &Path, features: u64) -> Result<()> {
        crate::atomic_file::Checkpoint::default().save(
            &path.join("MANIFEST"),
            &[STORE_MAGIC, &[STORE_VERSION_FEATURES], &features.to_le_bytes()],
        )
    }

    /// Check the MANIFEST and return the format features it names (none
    /// for version 1).
    fn verify_manifest(path: &Path) -> Result<u64> {
        use std::io::Read;

        let manifest_path = path.join("MANIFEST");
        let mut file = File::open(&manifest_path).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => match Self::present_store_file(path) {
                Err(error) => error.into(),
                Ok(Some("MANIFEST")) => StoreError::InvalidFormat(format!(
                    "{} is a symbolic link (to {}) that leads to nothing, so the store can't be opened until what it leads to is back (on a volume that isn't mounted, say)",
                    manifest_path.display(),
                    fs::read_link(&manifest_path)
                        .map(|target| target.display().to_string())
                        .unwrap_or_else(|_| "a path that can't be read".into())
                )),
                Ok(Some(found)) => StoreError::InvalidFormat(format!(
                    "{} has no MANIFEST but holds {found}: a damaged Chronicle store, which neither open nor create will take as it is",
                    path.display()
                )),
                Ok(None) => StoreError::InvalidFormat(format!(
                    "{} has no MANIFEST, so it isn't a Chronicle store",
                    path.display()
                )),
            },
            _ => e.into(),
        })?;

        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        if &magic != STORE_MAGIC {
            return Err(StoreError::InvalidFormat("Invalid store magic".into()));
        }

        let mut version = [0u8; 1];
        file.read_exact(&mut version)?;
        match version[0] {
            STORE_VERSION => Ok(0),
            STORE_VERSION_FEATURES => {
                let mut bits = [0u8; 8];
                file.read_exact(&mut bits).map_err(|e| {
                    StoreError::InvalidFormat(format!(
                        "Store manifest version 2 is too short to hold its format features: {e}"
                    ))
                })?;
                let features = u64::from_le_bytes(bits);
                let unknown = features & !KNOWN_FEATURES;
                if unknown != 0 {
                    return Err(StoreError::InvalidFormat(format!(
                        "Store uses format features this build can't read (bits {unknown:#x}); \
                         it was written by a newer chronicle"
                    )));
                }
                Ok(features)
            }
            other => Err(StoreError::InvalidFormat(format!(
                "Unsupported store version: {other}"
            ))),
        }
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

/// The item an identity append stores: `item`, which must be a JSON object,
/// with the record id it is about to be written under at `id_field` (as a
/// string, matching the napi `JsRecord.id` shape) and that record's sequence
/// at `sequence_field` (as a number), serialized as `serde_json` writes it.
/// Existing values at those keys are overwritten. The single and the batched
/// identity appends both stamp through this, so an item reads the same
/// whichever wrote it, given the same record.
fn stamp_identity(
    mut item: serde_json::Value,
    id_field: &str,
    sequence_field: &str,
    id: RecordId,
    seq: Sequence,
) -> Result<Vec<u8>> {
    let map = item.as_object_mut().ok_or_else(|| {
        StoreError::InvalidOperation("payload must be a JSON object".into())
    })?;
    map.insert(
        id_field.to_string(),
        serde_json::Value::String(id.0.to_string()),
    );
    map.insert(
        sequence_field.to_string(),
        serde_json::Value::Number(serde_json::Number::from(seq.0)),
    );
    Ok(serde_json::to_vec(&item)?)
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
                StateOperation::AppendMany(items) => {
                    for item in items {
                        let value: serde_json::Value = serde_json::from_slice(&item)
                            .map_err(|e| StoreError::Deserialization(e.to_string()))?;
                        self.items_buffer.push(value);
                    }
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

    /// The head an earlier release's `set_head_for_branch` installed for a
    /// branch made at a past point: the chain position and the count, with
    /// fresh snapshot accounting and no change on record.
    fn earlier_release_head(head: &crate::state::StateChainHead) -> crate::state::StateChainHead {
        crate::state::StateChainHead {
            head_offset: head.head_offset,
            ops_since_delta_snapshot: 0,
            delta_snapshots_since_full: 0,
            last_delta_snapshot_offset: None,
            last_full_snapshot_offset: None,
            has_non_append_since_snapshot: false,
            item_count: head.item_count,
            item_count_at_last_full: head.item_count,
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
            store.state.insert_head_for_branch(child.id, "s", earlier_release_head(&head));
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
        store.state.insert_head_for_branch(main, "s", earlier_release_head(&head));
        store.sync().unwrap();
        drop(store);
        let store = reopen(test_config(&dir));
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
        let store = reopen(test_config(&dir));
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

    // Open reads each record twice: the log's validating scan, then the
    // index rebuild. The rebuild's iterator used to read every record a
    // second time to find where it ended, so open read each one three times.
    #[test]
    fn open_reads_each_record_once_to_validate_and_once_to_index() {
        use crate::records::RECORD_DECODES;
        let dir = TempDir::new().unwrap();
        {
            let store = Store::create(test_config(&dir)).unwrap();
            store
                .register_state(StateRegistration {
                    id: "log".to_string(),
                    strategy: crate::types::StateStrategy::AppendLog {
                        delta_snapshot_every: 10,
                        full_snapshot_every: 4,
                    },
                    initial_value: None,
                })
                .unwrap();
            for i in 0..40 {
                let input = RecordInput::json("event", &json!({ "n": i })).unwrap();
                store.append(input).unwrap();
                store
                    .update_state("log", StateOperation::Append(format!("{i}").into_bytes()))
                    .unwrap();
            }
        }
        let records = reopen(test_config(&dir)).log.iter().count() as u64;
        assert!(
            records > 80,
            "40 records, 40 appends and their snapshots: {records}"
        );

        RECORD_DECODES.with(|decodes| decodes.set(0));
        let store = reopen(test_config(&dir));
        assert_eq!(RECORD_DECODES.with(|decodes| decodes.get()), 2 * records);
        assert_eq!(store.get_state_len("log").unwrap(), Some(40));
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

    /// A delta snapshot holds the items appended since the snapshot before
    /// it, and its chain still links back through the appends it covers. A
    /// tail longer than the newest delta must skip those appends and take
    /// its older items from the delta before, as reconstruction does, not
    /// collect the covered appends a second time. Batches and single
    /// appends alike.
    #[test]
    fn test_get_state_tail_past_a_delta_snapshot() {
        let dir = TempDir::new().unwrap();
        let store = Store::create(test_config(&dir)).unwrap();
        store.register_state(StateRegistration {
            id: "items".to_string(),
            strategy: crate::types::StateStrategy::AppendLog {
                delta_snapshot_every: 3,
                full_snapshot_every: 100,
            },
            initial_value: None,
        }).unwrap();
        store.register_state(StateRegistration {
            id: "batched".to_string(),
            strategy: crate::types::StateStrategy::AppendLog {
                delta_snapshot_every: 2,
                full_snapshot_every: 100,
            },
            initial_value: None,
        }).unwrap();

        // Deltas after 3, 6 and 9; 10 and 11 are newer than the last.
        for i in 1..=11 {
            store.update_state("items", StateOperation::Append(
                serde_json::to_vec(&i).unwrap()
            )).unwrap();
        }
        // Batches [1,2] [3]: delta; [4,5] [6,7,8]: delta; [9,10,11].
        for batch in [&[1, 2][..], &[3], &[4, 5], &[6, 7, 8], &[9, 10, 11]] {
            store.update_state("batched", StateOperation::AppendMany(
                batch.iter().map(|i| serde_json::to_vec(i).unwrap()).collect()
            )).unwrap();
        }
        assert_eq!(store.get_compaction_stats("items").unwrap().delta_snapshots_since_full, 3);
        assert_eq!(store.get_compaction_stats("batched").unwrap().delta_snapshots_since_full, 2);

        for id in ["items", "batched"] {
            for count in 1..=11 {
                let tail = store.get_state_tail(id, count).unwrap().unwrap();
                let arr: Vec<i32> = serde_json::from_slice(&tail).unwrap();
                assert_eq!(arr, ((12 - count as i32)..=11).collect::<Vec<_>>(), "{id}: tail of {count}");
            }
        }
    }

    /// The items a delta snapshot is built from: every item of every batch
    /// since the last snapshot, in order, between the single appends.
    #[test]
    fn test_compute_delta_items_takes_every_item_of_a_batch() {
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
        let main = store.current_branch().id;

        store.update_state("items", StateOperation::Append(b"0".to_vec())).unwrap();
        store.compact_state("items").unwrap();
        store.update_state("items", StateOperation::Append(b"1".to_vec())).unwrap();
        store.update_state("items", StateOperation::AppendMany(
            vec![b"2".to_vec(), b"{\"b\":3, \"a\":3}".to_vec(), b"4".to_vec()]
        )).unwrap();
        store.update_state("items", StateOperation::Append(b"5".to_vec())).unwrap();
        store.update_state("items", StateOperation::AppendMany(vec![b"6".to_vec()])).unwrap();

        let delta = store
            .compute_delta_items(main, "items")
            .unwrap()
            .expect("no Redact, Edit, Set or Delta since the snapshot");
        assert_eq!(delta, br#"[1,2,{"a":3,"b":3},4,5,6]"#);
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
    /// A head rebuilt by `create_branch_at` must equal the head the parent
    /// carried when it stood at that sequence — every accounting field, not
    /// just the offset. Recorded live after each write and compared against a
    /// historical branch created afterwards at the same sequence.
    #[test]
    fn test_create_branch_at_reproduces_parent_head() {
        use crate::types::{StateRegistration, StateStrategy};

        let dir = TempDir::new().unwrap();
        let store = Store::create(test_config(&dir)).unwrap();
        store
            .register_state(StateRegistration {
                id: "log".to_string(),
                // Floor of 3 * 4 = 12 ops between fulls: the script below
                // crosses delta and full boundaries several times.
                strategy: StateStrategy::AppendLog {
                    delta_snapshot_every: 3,
                    full_snapshot_every: 4,
                },
                initial_value: None,
            })
            .unwrap();

        let mut script: Vec<StateOperation> = Vec::new();
        for i in 0..10 {
            script.push(StateOperation::Append(format!("{i}").into_bytes()));
        }
        script.push(StateOperation::Edit { index: 2, new_value: b"\"edited\"".to_vec() });
        for i in 10..14 {
            script.push(StateOperation::Append(format!("{i}").into_bytes()));
        }
        script.push(StateOperation::Redact { start: 0, end: 2 });
        for i in 14..30 {
            script.push(StateOperation::Append(format!("{i}").into_bytes()));
        }
        script.push(StateOperation::Set(b"[100,101,102]".to_vec()));
        for i in 30..36 {
            script.push(StateOperation::Append(format!("{i}").into_bytes()));
        }
        script.push(StateOperation::Edit { index: 1, new_value: b"\"again\"".to_vec() });
        for i in 36..60 {
            script.push(StateOperation::Append(format!("{i}").into_bytes()));
        }

        let main = store.current_branch().id;
        let mut history = Vec::new();
        let mut kinds_seen = (false, false, false); // (delta, full, flag set)
        for op in script {
            store.update_state("log", op).unwrap();
            let head = store.state.get_head(main, "log").unwrap();
            kinds_seen.0 |= head.last_delta_snapshot_offset.is_some();
            kinds_seen.1 |= head.last_full_snapshot_offset.is_some();
            kinds_seen.2 |= head.has_non_append_since_snapshot;
            history.push((store.current_branch().head, head));
        }
        assert_eq!(
            kinds_seen,
            (true, true, true),
            "script must exercise delta snapshots, full snapshots and the non-append flag"
        );

        for (i, (seq, expected)) in history.iter().enumerate() {
            let name = format!("at-{i}");
            let branch = store.create_branch_at(&name, "main", *seq).unwrap();
            let got = store.state.get_head(branch.id, "log").unwrap();
            assert_eq!(
                format!("{got:?}"),
                format!("{expected:?}"),
                "historical head at sequence {} differs from the parent's head then",
                seq.0
            );
        }

        // Branching at the head is the same as an ordinary fork.
        let seq = store.current_branch().head;
        let at_head = store.create_branch_at("at-head", "main", seq).unwrap();
        let forked = store.create_branch("forked", None).unwrap();
        assert_eq!(
            format!("{:?}", store.state.get_head(at_head.id, "log").unwrap()),
            format!("{:?}", store.state.get_head(forked.id, "log").unwrap()),
        );
    }

    /// Same property for Tree states. `item_count` is excluded: the live head
    /// carries the documented upper-bound estimate (overwrites count as
    /// inserts) while the historical head counts the real entries.
    #[test]
    fn test_create_branch_at_reproduces_tree_accounting() {
        use crate::types::{StateRegistration, StateStrategy};

        let dir = TempDir::new().unwrap();
        let store = Store::create(test_config(&dir)).unwrap();
        store
            .register_state(StateRegistration {
                id: "tree".to_string(),
                strategy: StateStrategy::Tree {
                    delta_snapshot_every: 3,
                    full_snapshot_every: 3,
                },
                initial_value: None,
            })
            .unwrap();

        let entry = |n: u64| TreeEntry { blob_hash: format!("h{n}"), size: n, mode: 0o644 };
        let main = store.current_branch().id;
        let mut history = Vec::new();
        for i in 0..25u64 {
            if i % 7 == 6 {
                store.tree_remove("tree", &format!("f{}", i - 1)).unwrap();
            } else {
                // Every third write overwrites an existing path.
                let path = if i % 3 == 2 { format!("f{}", i - 1) } else { format!("f{i}") };
                store.tree_set("tree", &path, &entry(i)).unwrap();
            }
            history.push((store.current_branch().head, store.state.get_head(main, "tree").unwrap()));
        }
        assert!(history.iter().any(|(_, h)| h.last_full_snapshot_offset.is_some()));
        assert!(history.iter().any(|(_, h)| h.delta_snapshots_since_full > 0));

        for (i, (seq, expected)) in history.iter().enumerate() {
            let branch = store.create_branch_at(&format!("at-{i}"), "main", *seq).unwrap();
            let mut got = store.state.get_head(branch.id, "tree").unwrap();
            let actual_len = store
                .get_state_at_on(main, "tree", *seq)
                .unwrap()
                .map(|bytes| serde_json::from_slice::<TreeState>(&bytes).unwrap().len())
                .unwrap();
            assert_eq!(got.item_count, actual_len, "historical tree count is exact");
            got.item_count = expected.item_count;
            assert_eq!(format!("{got:?}"), format!("{expected:?}"), "sequence {}", seq.0);
        }
    }

    fn copy_dir(from: &Path, to: &Path) {
        fs::create_dir_all(to).unwrap();
        for entry in fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let target = to.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_dir(&entry.path(), &target);
            } else {
                fs::copy(entry.path(), target).unwrap();
            }
        }
    }

    fn register_log(store: &Store, id: &str) {
        store
            .register_state(crate::types::StateRegistration {
                id: id.to_string(),
                strategy: crate::types::StateStrategy::AppendLog {
                    delta_snapshot_every: 4,
                    full_snapshot_every: 3,
                },
                initial_value: None,
            })
            .unwrap();
    }

    /// Deleting a branch releases its chain heads, caches and field indexes,
    /// so a store that forks and deletes many short-lived branches does not
    /// accumulate them in memory or in state.bin.
    #[test]
    fn test_delete_branch_releases_its_state() {
        let dir = TempDir::new().unwrap();
        let store = Store::create(test_config(&dir)).unwrap();
        for i in 0..6 {
            let id = format!("s{i}");
            register_log(&store, &id);
            for n in 0..5 {
                store
                    .update_state(&id, StateOperation::Append(format!("{{\"n\":{n}}}").into_bytes()))
                    .unwrap();
            }
        }
        let baseline_heads = store.state.head_count();
        assert_eq!(baseline_heads, 6);
        store.sync().unwrap();
        let state_bin = store.path().join("state.bin");
        let indexes_bin = store.path().join("state-indexes.bin");
        let baseline_size = fs::metadata(&state_bin).unwrap().len();
        let baseline_indexes = fs::metadata(&indexes_bin).unwrap().len();

        let mut ids = Vec::new();
        for round in 0..20 {
            let name = format!("child-{round}");
            let child = store.create_branch(&name, None).unwrap().id;
            store
                .update_state_on(child, "s0", StateOperation::Append(b"{\"n\":99}".to_vec()))
                .unwrap();
            store
                .register_state_field_index_on(child, "s0", "/n", crate::state::FieldIndexKind::Number)
                .unwrap();
            assert!(store.state_items_on(child, "s1").unwrap().is_some());
            ids.push((name, child));
        }
        assert_eq!(store.state.head_count(), baseline_heads * 21);
        store.sync().unwrap();
        assert!(fs::metadata(&state_bin).unwrap().len() > baseline_size * 10);
        assert!(fs::metadata(&indexes_bin).unwrap().len() > baseline_indexes + 20 * 20);

        for (name, child) in &ids {
            store.delete_branch(name).unwrap();
            assert_eq!(
                store.query_state_index_range_on(*child, "s0", "/n", None, None, None, None, false),
                None
            );
        }
        // A deletion checkpoints before it returns: that checkpoint wrote
        // state.bin with the heads still in it, then branches.bin without the
        // branch, and only then let the heads go...
        assert_eq!(store.state.head_count(), baseline_heads);
        // ...the field indexes (derived data) were dropped at once, so the
        // same checkpoint already wrote a file without them...
        assert_eq!(fs::metadata(&indexes_bin).unwrap().len(), baseline_indexes);
        // ...and state.bin, which the last deletion's checkpoint wrote with
        // that branch's heads still in it, is back to its size at the
        // following sync.
        assert!(fs::metadata(&state_bin).unwrap().len() > baseline_size);
        store.sync().unwrap();
        assert_eq!(fs::metadata(&state_bin).unwrap().len(), baseline_size);

        // The parent is intact, in memory and after a reopen.
        let expected = store.get_state("s0").unwrap().unwrap();
        assert_eq!(store.get_state_len("s0").unwrap(), Some(5));
        drop(store);
        let store = Store::open(test_config(&dir)).unwrap();
        assert_eq!(store.get_state("s0").unwrap().unwrap(), expected);
        assert_eq!(store.state.head_count(), baseline_heads);
        assert_eq!(store.list_branches().len(), 1);
    }

    /// state.bin and branches.bin are written one after the other. A crash
    /// between the two writes of the checkpoint a delete makes must bring
    /// the branch back whole — never listed but stripped of its state.
    #[test]
    fn test_crash_between_metadata_writes_after_delete_keeps_branch_whole() {
        let dir = TempDir::new().unwrap();
        let store = Store::create(test_config(&dir)).unwrap();
        register_log(&store, "log");
        for n in 0..7 {
            store
                .update_state("log", StateOperation::Append(format!("{n}").into_bytes()))
                .unwrap();
        }
        let child = store.create_branch("child", None).unwrap().id;
        for n in 100..103 {
            store
                .update_state_on(child, "log", StateOperation::Append(format!("{n}").into_bytes()))
                .unwrap();
        }
        store.sync().unwrap();
        let child_value = store.get_state_on(child, "log").unwrap().unwrap();

        // The deletion as `delete_branch` makes it, up to its checkpoint's
        // second file: the branch is gone in memory, its heads are retired,
        // the log and state.bin are written, branches.bin is not yet. Freeze
        // the files as a crash at this point would leave them.
        {
            let mut write = store.write_lock.lock();
            store.branches.delete_branch("child").unwrap();
            store.state.retire_branch(child);
            write.checkpoint_required = true;
            store.log.sync().unwrap();
            store.state.save().unwrap();
        }
        let crashed = dir.path().join("crashed");
        copy_dir(store.path(), &crashed);

        let recovered = Store::open(StoreConfig {
            path: crashed,
            blob_cache_size: 100,
            create_if_missing: false,
        })
        .unwrap();
        let back = recovered.get_branch("child").expect("branches.bin still lists the branch");
        assert_eq!(back.id, child);
        assert_eq!(
            recovered.get_state_on(back.id, "log").unwrap().unwrap(),
            child_value,
            "a branch that survives the crash must still have its state"
        );

        // Without the crash the checkpoint completes and the branch is gone
        // for good; its heads leave state.bin at the sync after that.
        store.sync().unwrap();
        assert_eq!(store.state.head_count(), 1);
        drop(store);
        let store = Store::open(test_config(&dir)).unwrap();
        assert!(store.get_branch("child").is_none());
        assert_eq!(store.get_state_len("log").unwrap(), Some(7));
    }

    /// A parent head written before `item_count_at_last_full` existed has 0
    /// there. The historical fork must not take that for the length of the
    /// parent's last full snapshot.
    #[test]
    fn test_create_branch_at_with_legacy_parent_head_counts_items() {
        let dir = TempDir::new().unwrap();
        let store = Store::create(test_config(&dir)).unwrap();
        register_log(&store, "log");
        for n in 0..9 {
            store
                .update_state("log", StateOperation::Append(format!("{n}").into_bytes()))
                .unwrap();
        }
        store.compact_state("log").unwrap();
        for n in 9..12 {
            store
                .update_state("log", StateOperation::Append(format!("{n}").into_bytes()))
                .unwrap();
        }
        let at = store.current_branch().head;
        store
            .update_state("log", StateOperation::Append(b"12".to_vec()))
            .unwrap();

        let main = store.current_branch().id;
        let mut legacy = store.state.get_head(main, "log").unwrap();
        assert_eq!(legacy.item_count_at_last_full, 9);
        assert!(legacy.last_full_snapshot_offset.is_some());

        // With the length on record the snapshot is not needed...
        let modern = store.create_branch_at("modern", "main", at).unwrap();
        assert_eq!(store.get_state_len_on(modern.id, "log").unwrap(), Some(12));

        // ...and without it the walk reads the snapshot and still counts right.
        legacy.item_count_at_last_full = 0;
        store.state.insert_head_for_branch(main, "log", legacy);
        let from_legacy = store.create_branch_at("from-legacy", "main", at).unwrap();
        assert_eq!(store.get_state_len_on(from_legacy.id, "log").unwrap(), Some(12));
        assert_eq!(
            store.state.get_head(from_legacy.id, "log").unwrap().item_count_at_last_full,
            9
        );
        for branch in [modern.id, from_legacy.id] {
            let items: Vec<u64> =
                serde_json::from_slice(&store.get_state_on(branch, "log").unwrap().unwrap()).unwrap();
            assert_eq!(items, (0..12).collect::<Vec<u64>>());
        }
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
