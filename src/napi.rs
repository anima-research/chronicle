//! NAPI bindings for Node.js/TypeScript.

use crate::{
    error::StoreError,
    state::StateSelection,
    subscriptions::{
        DropReason, RecordSummary, StoreEvent, SubscriptionConfig, SubscriptionFilter,
        SubscriptionHandle, SubscriptionId,
    },
    types::{TreeChange, TreeEntry, TreeOp},
    Branch, BranchId, CompactionSummary, FieldIndexKind, Record, RecordId, Sequence, StateOperation,
    StateRegistration, StateStrategy, Store, StoreConfig,
};
use napi::bindgen_prelude::*;
use napi_derive::napi;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::Arc;

/// JavaScript-friendly Store wrapper.
///
/// There are two kinds of handle onto one open store:
///
/// - the **root** handle, returned by `create` / `open` / `openOrCreate`.
///   Its branch-dependent methods follow the store-wide current branch
///   (`switchBranch` / `currentBranch`), exactly as before views existed;
/// - **views**, returned by `view(branchName)`. A view shares the root's
///   underlying store — same log, same caches, same lock file — but every
///   branch-dependent method on it targets the one branch it is pinned to. A
///   view never reads or moves the current branch, so any number of views
///   (and the root) can be used side by side on different branches.
///
/// Lifetime: the root owns the store. `close()` on the root closes the store
/// for every view at once (they throw "Store has been closed" from then on)
/// and releases the lock file immediately, however many views are still
/// referenced. `close()` on a view only detaches that view.
#[napi]
pub struct JsStore {
    /// The open store, shared by the root handle and every view made from
    /// it. The root's `close()` empties the slot, which is what closes the
    /// store for all of them and lets the `Store` (and its lock file) drop
    /// even while views are still alive.
    shared: Arc<RwLock<Option<Arc<Store>>>>,
    /// The branch this handle is pinned to; `None` on the root handle.
    pinned: Option<PinnedBranch>,
    /// Set once `close()` has been called on a view.
    detached: bool,
    /// Active subscription handles, stored by ID.
    subscription_handles: Mutex<HashMap<u64, SubscriptionHandle>>,
}

/// The branch a view is bound to. Bound by id: ids are never reused, so a
/// view cannot be silently re-pointed at a different branch that later takes
/// the same name. The name is kept for `pinnedBranch()` and error messages
/// (branches cannot be renamed).
struct PinnedBranch {
    id: BranchId,
    name: String,
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

/// What `appendManyToStateJson` or `appendManyToStateJsonWithIdentity`
/// recorded.
#[napi(object)]
pub struct JsAppendedBatch {
    /// The one record that holds the whole batch, at one sequence.
    pub record: JsRecord,
    /// The index the batch's first item took: the state's length on the
    /// handle's branch, as `getStateLen` counts it, just before the batch.
    /// Item `i` of the batch is at `firstOrdinal + i`.
    pub first_ordinal: i64,
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

impl From<Branch> for JsBranch {
    fn from(branch: Branch) -> Self {
        JsBranch {
            id: branch.id.0.to_string(),
            name: branch.name,
            head: branch.head.0 as i64,
            parent_id: branch.parent.map(|p| p.0.to_string()),
            branch_point: branch.branch_point.map(|s| s.0 as i64),
            created: branch.created.0,
        }
    }
}

/// Which of the parent's states a selective fork inherits
/// (`createBranchWithStates` / `createBranchAtWithStates`).
///
/// A state is inherited if its id equals one of `stateIds` or starts with one
/// of `statePrefixes` (a plain string prefix of the id). Omitting both — or
/// passing two empty arrays — inherits nothing. Ids and prefixes that match
/// no state are ignored.
#[napi(object)]
pub struct JsStateFilter {
    pub state_ids: Option<Vec<String>>,
    pub state_prefixes: Option<Vec<String>>,
}

impl From<JsStateFilter> for StateSelection {
    fn from(filter: JsStateFilter) -> Self {
        StateSelection {
            state_ids: filter.state_ids.unwrap_or_default(),
            state_prefixes: filter.state_prefixes.unwrap_or_default(),
        }
    }
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

/// What the most recent `open` had to recover, surfaced to JavaScript:
/// a torn/partial tail record it truncated, updates it replayed for a state
/// with no registration, or records typed `state_update` it skipped.
#[napi(object)]
pub struct JsRecoveryReport {
    /// Byte offset the log was truncated back to (end of the last valid
    /// record), or the log's length when nothing was torn.
    pub truncated_at: i64,
    /// Number of trailing bytes discarded (up to `sync_interval` un-fsynced
    /// records). 0 when nothing was torn.
    pub dropped_bytes: i64,
    /// Number of valid records that survived recovery.
    pub valid_records: i64,
    /// Each state whose updates were replayed although the store holds no
    /// registration for it: one written without a registration, or whose
    /// registration an earlier release lost in a crash.
    pub unregistered_states: Vec<JsUnregisteredState>,
    /// Records typed `state_update` that don't decode as state updates, which
    /// open skipped. They stay in the log as ordinary records.
    pub undecodable_state_updates: i64,
    /// Records typed `state_update` on a branch that the store's branch
    /// checkpoint doesn't hold, which open didn't replay, since nothing can
    /// read that branch: an earlier release saved a branch only at sync.
    pub updates_on_missing_branches: i64,
}

/// A state whose updates `open` replayed without a registration.
#[napi(object)]
pub struct JsUnregisteredState {
    pub id: String,
    /// How many of its updates were replayed.
    pub updates: i64,
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
    /// If true, return records in descending sequence order (newest first).
    pub reverse: Option<bool>,
}

/// Options for a numeric range query against a registered state field index
/// (see `registerStateFieldIndex`). Either bound may be omitted for an
/// open-ended range.
#[napi(object)]
pub struct JsIndexRangeQuery {
    pub gte: Option<f64>,
    pub lte: Option<f64>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    /// If true, return ordinals in descending value order.
    pub reverse: Option<bool>,
}

/// Options for an equality query against a registered state field index.
#[napi(object)]
pub struct JsIndexEqQuery {
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

/// A distinct string field-index value and how many ordinals carry it.
#[napi(object)]
pub struct JsValueCount {
    pub value: String,
    pub count: u32,
}

/// State info returned to JavaScript.
#[napi(object)]
pub struct JsStateInfo {
    pub id: String,
    pub strategy: String,
    pub item_count: Option<i64>,
    pub ops_since_snapshot: i64,
}

// --- Subscription types ---

/// Configuration for creating a subscription.
#[napi(object)]
pub struct JsSubscriptionConfig {
    /// Max buffered events before dropping subscriber (default: 1000).
    pub buffer_size: Option<i64>,
    /// Max bytes for state snapshots (default: 10MB).
    pub max_snapshot_bytes: Option<i64>,
    /// Starting sequence for catch-up (None = live only).
    pub from_sequence: Option<i64>,
    /// Filter criteria.
    pub filter: Option<JsSubscriptionFilter>,
}

/// Filter criteria for subscriptions.
#[napi(object)]
pub struct JsSubscriptionFilter {
    /// Filter by record types (None = all).
    pub record_types: Option<Vec<String>>,
    /// Filter by branch name.
    pub branch: Option<String>,
    /// Subscribe to specific state IDs.
    pub state_ids: Option<Vec<String>>,
    /// Include record events.
    pub include_records: Option<bool>,
    /// Include state change events.
    pub include_state_changes: Option<bool>,
    /// Include branch events.
    pub include_branch_events: Option<bool>,
}

/// A store event.
#[napi(object)]
pub struct JsStoreEvent {
    /// Event type: "record", "state_snapshot", "state_delta", "branch_head",
    /// "branch_created", "branch_deleted", "caught_up", "dropped"
    pub event_type: String,
    /// JSON-serialized event data.
    ///
    /// A "state_delta" carries `state_id`, `sequence` and the recorded
    /// `operation`, keyed by its variant name. An item's bytes are an array
    /// of integers: `{"Append": [..]}` for one item, and
    /// `{"AppendMany": [[..], [..], ..]}` for a batch from
    /// `appendManyToStateJson` or `appendManyToStateJsonWithIdentity`, which
    /// is one event for the whole batch at its one sequence. A subscriber
    /// that follows an append log by its deltas needs both.
    pub data: String,
}

impl From<StoreEvent> for JsStoreEvent {
    fn from(event: StoreEvent) -> Self {
        let event_type = match &event {
            StoreEvent::Record { .. } => "record",
            StoreEvent::StateSnapshot { .. } => "state_snapshot",
            StoreEvent::StateDelta { .. } => "state_delta",
            StoreEvent::BranchHead { .. } => "branch_head",
            StoreEvent::BranchCreated { .. } => "branch_created",
            StoreEvent::BranchDeleted { .. } => "branch_deleted",
            StoreEvent::CaughtUp => "caught_up",
            StoreEvent::Dropped { .. } => "dropped",
        };
        let data = serde_json::to_string(&event).unwrap_or_else(|_| "{}".to_string());
        JsStoreEvent {
            event_type: event_type.to_string(),
            data,
        }
    }
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

// --- Tree types ---

/// A tree entry (file in a tree state).
#[napi(object)]
pub struct JsTreeEntry {
    pub blob_hash: String,
    pub size: i64,
    pub mode: i32,
}

/// A tree operation for batch updates.
#[napi(object)]
pub struct JsTreeOp {
    /// "set" or "remove"
    pub op: String,
    pub path: String,
    pub entry: Option<JsTreeEntry>,
}

/// A change between two tree states.
///
/// Field population by change_type:
/// Field population by change_type:
/// - "added": `old_entry` = None, `new_entry` = the new entry
/// - "modified": `old_entry` = previous entry, `new_entry` = current entry
/// - "removed": `old_entry` = the removed entry, `new_entry` = None
#[napi(object)]
pub struct JsTreeChange {
    /// "added", "modified", or "removed"
    pub change_type: String,
    pub path: String,
    /// Previous entry (None for "added")
    pub old_entry: Option<JsTreeEntry>,
    /// Current entry (None for "removed")
    pub new_entry: Option<JsTreeEntry>,
}

/// A tree list entry (path + entry).
#[napi(object)]
pub struct JsTreeListEntry {
    pub path: String,
    pub blob_hash: String,
    pub size: i64,
    pub mode: i32,
}

fn to_napi_error(e: StoreError) -> napi::Error {
    napi::Error::from_reason(e.to_string())
}

/// Shared JsStateRegistration -> StateStrategy conversion for
/// register_state and update_state_strategy.
fn js_registration_strategy(registration: &JsStateRegistration) -> Result<StateStrategy> {
    Ok(match registration.strategy.as_str() {
        "snapshot" => StateStrategy::Snapshot,
        "append_log" => StateStrategy::AppendLog {
            delta_snapshot_every: registration.delta_snapshot_every.unwrap_or(100) as u64,
            full_snapshot_every: registration.full_snapshot_every.unwrap_or(10) as u64,
        },
        "tree" => StateStrategy::Tree {
            delta_snapshot_every: registration.delta_snapshot_every.unwrap_or(50) as u64,
            full_snapshot_every: registration.full_snapshot_every.unwrap_or(10) as u64,
        },
        _ => return Err(napi::Error::from_reason("Invalid strategy")),
    })
}

#[napi]
impl JsStore {
    /// Get the inner store, returning an error if closed.
    fn get_store(&self) -> Result<Arc<Store>> {
        if self.detached {
            return Err(napi::Error::from_reason("View has been closed"));
        }
        self.shared
            .read()
            .clone()
            .ok_or_else(|| napi::Error::from_reason("Store has been closed"))
    }

    /// The store plus the branch this handle's branch-dependent methods act
    /// on: the pinned branch for a view, the store's current branch for the
    /// root. Fails if a view's branch has been deleted.
    fn target(&self) -> Result<(Arc<Store>, BranchId)> {
        let store = self.get_store()?;
        let branch = match &self.pinned {
            Some(pinned) => {
                if !store.branch_exists(pinned.id) {
                    return Err(napi::Error::from_reason(format!(
                        "Branch not found: '{}' (the branch this view is pinned to has been deleted)",
                        pinned.name
                    )));
                }
                pinned.id
            }
            None => store.current_branch().id,
        };
        Ok((store, branch))
    }

    /// Wrap a freshly opened store in a root handle.
    fn root(store: Store) -> JsStore {
        JsStore {
            shared: Arc::new(RwLock::new(Some(Arc::new(store)))),
            pinned: None,
            detached: false,
            subscription_handles: Mutex::new(HashMap::new()),
        }
    }

    /// Create a new store at the given path. Refuses a path that already holds
    /// a store (its MANIFEST, records.log, state.bin or branches.bin); use
    /// `openOrCreate` or `open` for a store that may exist.
    #[napi(factory)]
    pub fn create(config: JsStoreConfig) -> Result<JsStore> {
        let store_config = StoreConfig {
            path: config.path.into(),
            blob_cache_size: config.blob_cache_size.map(|s| s as usize).unwrap_or(1000),
            create_if_missing: false,
        };
        let store = Store::create(store_config).map_err(to_napi_error)?;
        Ok(Self::root(store))
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
        Ok(Self::root(store))
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
        Ok(Self::root(store))
    }

    /// Close the store, releasing the lock and any resources.
    /// After closing, all operations on this store will fail.
    ///
    /// On the root handle this closes the store itself: it is synced, every
    /// view made from this handle stops working ("Store has been closed"),
    /// and the lock file is released before `close()` returns — live views
    /// do not keep the store or its lock alive. If the record log's fsync
    /// has failed, this throws that error after closing the store, so it
    /// can be reopened.
    ///
    /// On a view this only detaches the view: its subscriptions are removed
    /// and its methods throw "View has been closed". The store stays open,
    /// nothing is synced, and the root and other views are unaffected. A
    /// view never needs to be closed; dropping it is enough.
    #[napi]
    pub fn close(&mut self) -> Result<()> {
        if self.pinned.is_some() {
            if !self.detached {
                let handles: Vec<u64> = self.subscription_handles.lock().drain().map(|(id, _)| id).collect();
                if let Some(store) = self.shared.read().as_ref() {
                    for id in handles {
                        store.unsubscribe(SubscriptionId(id));
                    }
                }
                self.detached = true;
            }
            return Ok(());
        }

        let store = self.shared.write().take();
        if let Some(store) = store {
            // Sync before closing
            store.sync().map_err(to_napi_error)?;
            // Drop the Arc. The slot was the only long-lived reference (views
            // hold the slot, not the store), so the store is dropped here.
            drop(store);
        }
        Ok(())
    }

    /// Check if the store has been closed.
    ///
    /// On a view: true once the view itself was closed OR the root closed
    /// the store.
    #[napi]
    pub fn is_closed(&self) -> bool {
        self.detached || self.shared.read().is_none()
    }

    /// Open a view: a handle onto this same store whose branch-dependent
    /// methods all act on `branchName`, whatever the current branch is and
    /// wherever it moves afterwards.
    ///
    /// On a view:
    /// - records, states, trees, queries, field indexes, compaction,
    ///   `listStates()` item counts, `currentSequence()` and subscription
    ///   catch-up use the pinned branch; `currentBranch()` returns it;
    /// - `switchBranch` throws (the root's cursor is the only cursor);
    /// - `createBranch(name)` / `createBranchWithStates(name, undefined, ..)`
    ///   / `createEmptyBranch(name)` with `from` omitted use the pinned
    ///   branch as the parent;
    /// - everything else (blobs, `registerState`, `getRecord`, `sync`,
    ///   `stats`, `listBranches`, `setAutoSnapshot`, ...) is store-wide and
    ///   behaves as on the root handle.
    ///
    /// Creating a view is O(1): it allocates nothing per state and reads
    /// nothing from the log. Views share the store's caches, so a state the
    /// view's branch inherited unchanged is served from the value already
    /// materialized for the parent. All handles of one store must be used
    /// from the thread that opened it, like the root handle.
    ///
    /// If the pinned branch is deleted, every branch-dependent method of the
    /// view throws "Branch not found". See `close()` for lifetime.
    #[napi]
    pub fn view(&self, branch_name: String) -> Result<JsStore> {
        let store = self.get_store()?;
        let branch = store
            .get_branch(&branch_name)
            .ok_or_else(|| to_napi_error(StoreError::BranchNotFound(branch_name.clone())))?;
        Ok(JsStore {
            shared: Arc::clone(&self.shared),
            pinned: Some(PinnedBranch {
                id: branch.id,
                name: branch.name,
            }),
            detached: false,
            subscription_handles: Mutex::new(HashMap::new()),
        })
    }

    /// Name of the branch this handle is pinned to, or `null` on the root
    /// handle (which follows the current branch).
    #[napi]
    pub fn pinned_branch(&self) -> Option<String> {
        self.pinned.as_ref().map(|p| p.name.clone())
    }

    /// What the most recent open had to recover, if anything.
    ///
    /// Returns a report when `open` truncated a torn/partial tail record
    /// (crash during append), replayed updates for a state with no
    /// registration, skipped records typed `state_update` that don't decode
    /// as state updates, or passed over updates on a branch the store never
    /// saved, and `null` when none of these happened. A report doesn't by
    /// itself mean a tail was torn: `droppedBytes` is 0 when nothing was. Nor
    /// does a state with no registration mean damage: a state written without
    /// one is named whenever its updates are replayed. Lets JS callers detect
    /// lost un-fsynced writes without scraping logs.
    #[napi]
    pub fn recovery(&self) -> Result<Option<JsRecoveryReport>> {
        let store = self.get_store()?;
        Ok(store.recovery().map(|r| JsRecoveryReport {
            truncated_at: r.truncated_at as i64,
            dropped_bytes: r.dropped_bytes as i64,
            valid_records: r.valid_records as i64,
            unregistered_states: r
                .unregistered_states
                .into_iter()
                .map(|(id, updates)| JsUnregisteredState {
                    id,
                    updates: updates as i64,
                })
                .collect(),
            undecodable_state_updates: r.undecodable_state_updates as i64,
            updates_on_missing_branches: r.updates_on_missing_branches as i64,
        }))
    }

    // --- Records ---

    /// Append a record to the store.
    /// The internal `state_update` type is reserved for the state-update helpers.
    #[napi]
    pub fn append(&self, record_type: String, payload: Buffer) -> Result<JsRecord> {
        let (store, branch) = self.target()?;
        let input = crate::RecordInput::raw(&record_type, payload.to_vec());
        let record = store.append_on(branch, input).map_err(to_napi_error)?;
        Ok(record.into())
    }

    /// Append a JSON record.
    /// The internal `state_update` type is reserved for the state-update helpers.
    #[napi]
    pub fn append_json(&self, record_type: String, data: serde_json::Value) -> Result<JsRecord> {
        let (store, branch) = self.target()?;
        let input =
            crate::RecordInput::json(&record_type, &data).map_err(|e| to_napi_error(e.into()))?;
        let record = store.append_on(branch, input).map_err(to_napi_error)?;
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

    /// Get several records by ID in one call. The result has one entry per
    /// ID, in the order given: the record, as `getRecord` returns it, or
    /// `null` for an ID with no record. IDs may repeat. Throws, reading
    /// nothing, if an ID isn't a record ID.
    #[napi]
    pub fn get_records(&self, ids: Vec<String>) -> Result<Vec<Option<JsRecord>>> {
        let store = self.get_store()?;
        let ids = ids
            .iter()
            .map(|id| {
                id.parse::<u64>()
                    .map(RecordId)
                    .map_err(|_| napi::Error::from_reason(format!("Invalid record ID: {:?}", id)))
            })
            .collect::<Result<Vec<_>>>()?;
        let records = store.get_records(&ids).map_err(to_napi_error)?;
        Ok(records
            .into_iter()
            .map(|record| record.map(Into::into))
            .collect())
    }

    /// Decode a `state_update` record's payload to a JSON string, regardless
    /// of its on-disk encoding (legacy JSON under a Raw/Json tag, or the
    /// current MessagePack). JS consumers must use this instead of
    /// `JSON.parse(record.payload)` — after the 2026-08 encoding migration
    /// the raw payload is no longer guaranteed to be JSON. The operation's
    /// value bytes are surfaced as JSON integer arrays (the legacy shape),
    /// so existing shape-expectations keep holding. Returns `null` when no
    /// record has this id or its type isn't `state_update`, and throws for a
    /// record typed `state_update` that doesn't decode as a state update.
    #[napi]
    pub fn get_state_update_json(&self, id: String) -> Result<Option<String>> {
        let store = self.get_store()?;
        let id: u64 = id
            .parse()
            .map_err(|_| napi::Error::from_reason("Invalid record ID"))?;
        let record = match store.get_record(RecordId(id)).map_err(to_napi_error)? {
            Some(r) => r,
            None => return Ok(None),
        };
        if record.record_type != "state_update" {
            return Ok(None);
        }
        let update =
            crate::types::StateUpdateRecord::decode(&record).map_err(to_napi_error)?;
        let json = serde_json::to_string(&update)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(Some(json))
    }

    /// Get the IDs of a type's records across all branches, in ascending ID
    /// order, which is log order: the last is the newest. The IDs are decimal
    /// strings, so compare them as numbers. The records a torn tail drops at
    /// open are gone, and their IDs are issued again to new records, possibly
    /// of another type: a last ID taken before the crash can name a different
    /// record afterwards, and a count taken then can skip new ones.
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

    /// Store a blob and return its hash. Bytes already stored are left as they
    /// are, unless their file is cut short, damaged or holds other bytes, which
    /// is replaced; bytes read or stored since the store opened may be taken as
    /// stored from memory. A symbolic link at the blob's name is read through,
    /// and one that leads nowhere is replaced. Throws when something other than
    /// a regular file, such as a directory, is at the blob's name or a link's
    /// target, when its file can't be read or is in a blob format version newer
    /// than this chronicle reads, when writing or syncing the blob fails, and
    /// for a content type longer than 65,535 bytes. A sync that fails after the
    /// blob is in place still throws, and storing the blob again before the
    /// store closes tries the sync again.
    #[napi]
    pub fn store_blob(&self, content: Buffer, content_type: String) -> Result<String> {
        let store = self.get_store()?;
        let hash = store
            .store_blob(&content, &content_type)
            .map_err(to_napi_error)?;
        Ok(hash.to_string())
    }

    /// Get a blob by hash. A blob read or stored since the store opened may be
    /// answered from memory. Otherwise it throws when the blob's file is cut
    /// short or damaged, which storing the same bytes again replaces, or when
    /// something other than a regular file is at its name or a link's target.
    /// It also throws when the file can't be opened or read, such as for its
    /// permissions, when a link at its name loops, and when the file is in a
    /// blob format version newer than this chronicle reads, which storing
    /// doesn't replace. A symbolic link that leads nowhere reads as no blob.
    #[napi]
    pub fn get_blob(&self, hash: String) -> Result<Option<Buffer>> {
        let store = self.get_store()?;
        let hash = crate::Hash::from_hex(&hash)
            .map_err(|_| napi::Error::from_reason("Invalid hash"))?;
        let blob = store.get_blob(&hash).map_err(to_napi_error)?;
        Ok(blob.map(|b| Buffer::from(b.content)))
    }

    // --- Branches ---

    /// The parent a fork uses when `from` is omitted: the pinned branch on a
    /// view (which must still exist), `None` on the root so the store falls
    /// back to its own default.
    fn default_parent(&self, from: Option<String>) -> Result<Option<String>> {
        match (&from, &self.pinned) {
            (None, Some(pinned)) => {
                self.target()?;
                Ok(Some(pinned.name.clone()))
            }
            _ => Ok(from),
        }
    }

    /// Create a new branch, and checkpoint it before returning.
    /// If the checkpoint fails, this throws but the branch stays in memory;
    /// the next successful checkpoint publishes it and tells subscribers:
    /// `sync()`, or the retry that every later mutation makes first. Calling
    /// this again retries that checkpoint first and, once it succeeds, throws,
    /// since the branch exists.
    ///
    /// With `from` omitted the parent is the current branch — on a view, the
    /// branch the view is pinned to.
    #[napi]
    pub fn create_branch(&self, name: String, from: Option<String>) -> Result<JsBranch> {
        let store = self.get_store()?;
        let from = self.default_parent(from)?;
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

    /// Create a new branch that inherits only the states `filter` selects
    /// (exact `stateIds` and/or `statePrefixes`; see `JsStateFilter`).
    ///
    /// Selected states are inherited exactly as `createBranch` inherits
    /// them. Every other state has no value on the new branch — it reads
    /// there as a registered state does on a `createEmptyBranch` branch
    /// (`getState` → null, `getStateLen` → null) — and a write to it starts a
    /// fresh chain that shares nothing with the parent's.
    ///
    /// Copying the heads costs in proportion to the number of states
    /// inherited, not to how many states the parent has, how many branches
    /// exist, or how much history the store holds; the checkpoint that ends
    /// the call still saves every branch's heads. With `from` omitted the
    /// parent is the current branch (on a view, the pinned branch).
    #[napi]
    pub fn create_branch_with_states(
        &self,
        name: String,
        from: Option<String>,
        filter: JsStateFilter,
    ) -> Result<JsBranch> {
        let store = self.get_store()?;
        let from = self.default_parent(from)?;
        let branch = store
            .create_branch_with_states(&name, from.as_deref(), &filter.into())
            .map_err(to_napi_error)?;
        Ok(branch.into())
    }

    /// Create a new branch at a specific sequence point of `from`, inheriting
    /// only the states `filter` selects. The historical counterpart of
    /// `createBranchWithStates`: selected states read as they did on `from`
    /// at `atSequence`, the others have no value.
    #[napi]
    pub fn create_branch_at_with_states(
        &self,
        name: String,
        from: String,
        at_sequence: i64,
        filter: JsStateFilter,
    ) -> Result<JsBranch> {
        let store = self.get_store()?;
        let branch = store
            .create_branch_at_with_states(
                &name,
                &from,
                Sequence(at_sequence as u64),
                &filter.into(),
            )
            .map_err(to_napi_error)?;
        Ok(branch.into())
    }

    /// Create a new branch at a specific sequence point.
    /// This enables time-travel branching - the new branch will have state as of that sequence.
    /// A failed checkpoint leaves the branch in memory, as for `createBranch`.
    #[napi]
    pub fn create_branch_at(&self, name: String, from: String, at_sequence: i64) -> Result<JsBranch> {
        let store = self.get_store()?;
        let branch = store
            .create_branch_at(&name, &from, Sequence(at_sequence as u64))
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
    /// A failed checkpoint leaves the branch in memory, as for `createBranch`.
    #[napi]
    pub fn create_empty_branch(&self, name: String, from: Option<String>) -> Result<JsBranch> {
        let store = self.get_store()?;
        let from = self.default_parent(from)?;
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

    /// Switch to a branch, and checkpoint the switch before returning.
    /// If the checkpoint fails, this throws but the switch stays in effect;
    /// the next successful checkpoint publishes it: `sync()`, or the retry
    /// that every later mutation makes first, so later writes go to the new
    /// branch.
    ///
    ///
    /// Root handle only. A view is pinned to its branch for life and throws:
    /// moving the store-wide current branch from a view would silently
    /// redirect the root handle.
    #[napi]
    pub fn switch_branch(&self, name: String) -> Result<JsBranch> {
        let store = self.get_store()?;
        if let Some(pinned) = &self.pinned {
            return Err(napi::Error::from_reason(format!(
                "switchBranch is not available on a view (pinned to branch '{}'); \
                 call view('{}') on the store for a handle on that branch instead",
                pinned.name, name
            )));
        }
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

    /// Get the current branch. On a view: the branch it is pinned to.
    #[napi]
    pub fn current_branch(&self) -> Result<JsBranch> {
        let (store, branch) = self.target()?;
        let branch = store
            .get_branch_by_id(branch)
            .ok_or_else(|| napi::Error::from_reason("Branch not found"))?;
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

    /// Delete a branch, and checkpoint the deletion before returning.
    /// If the checkpoint fails, this throws but the branch stays deleted in
    /// memory; the next successful checkpoint publishes the deletion and
    /// tells subscribers: `sync()`, or the retry that every later mutation
    /// makes first. Calling this again retries that checkpoint first and,
    /// once it succeeds, throws, since the branch is gone.
    ///
    ///
    /// The main branch and the current branch cannot be deleted; nor can a
    /// view delete the branch it is itself pinned to. A branch that OTHER
    /// views are pinned to can be deleted — those views then throw
    /// "Branch not found" from every branch-dependent method.
    #[napi]
    pub fn delete_branch(&self, name: String) -> Result<()> {
        let store = self.get_store()?;
        if let Some(pinned) = &self.pinned {
            if pinned.name == name && store.branch_exists(pinned.id) {
                return Err(napi::Error::from_reason(format!(
                    "Branch not found: Cannot delete the branch this view is pinned to ('{}')",
                    name
                )));
            }
        }
        store.delete_branch(&name).map_err(to_napi_error)
    }

    // --- State Management ---

    /// Register a new state, and checkpoint it before returning. If the
    /// checkpoint fails, this throws but the registration stays in memory;
    /// the next successful checkpoint publishes it: `sync()`, or the retry
    /// that every later mutation makes first. Calling this again retries that
    /// checkpoint first and, once it succeeds, throws, since the state is
    /// registered.
    #[napi]
    pub fn register_state(&self, registration: JsStateRegistration) -> Result<()> {
        let store = self.get_store()?;
        let strategy = js_registration_strategy(&registration)?;

        let reg = StateRegistration {
            id: registration.id,
            strategy,
            initial_value: registration.initial_value.map(|b| b.to_vec()),
        };

        store.register_state(reg).map_err(to_napi_error)
    }

    /// Update the strategy parameters of an already-registered state.
    /// Same strategy kind only; cadence fields (deltaSnapshotEvery /
    /// fullSnapshotEvery) are free to change and steer future snapshot
    /// scheduling. Use after registerState throws "State already exists"
    /// so boot-time registration constants actually apply to existing
    /// stores instead of being pinned to first-registration values.
    /// `initialValue` is ignored (the state already has a chain).
    /// Checkpoints before returning. If the checkpoint fails, this throws but
    /// the new strategy stays in memory; the next successful checkpoint
    /// publishes it: `sync()`, or the retry that every later mutation makes
    /// first.
    #[napi]
    pub fn update_state_strategy(&self, registration: JsStateRegistration) -> Result<()> {
        let store = self.get_store()?;
        let strategy = js_registration_strategy(&registration)?;
        store
            .update_state_strategy(&registration.id, strategy)
            .map_err(to_napi_error)
    }

    /// Get state value.
    #[napi]
    pub fn get_state(&self, state_id: String) -> Result<Option<Buffer>> {
        let (store, branch) = self.target()?;
        let state = store.get_state_on(branch, &state_id).map_err(to_napi_error)?;
        Ok(state.map(Buffer::from))
    }

    /// Get state as JSON.
    #[napi]
    pub fn get_state_json(&self, state_id: String) -> Result<Option<serde_json::Value>> {
        let (store, branch) = self.target()?;
        let state = store.get_state_on(branch, &state_id).map_err(to_napi_error)?;
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
        let (store, branch) = self.target()?;
        let record = store
            .update_state_on(branch, &state_id, StateOperation::Set(value.to_vec()))
            .map_err(to_napi_error)?;
        Ok(record.into())
    }

    /// Set state as JSON.
    #[napi]
    pub fn set_state_json(&self, state_id: String, value: serde_json::Value) -> Result<JsRecord> {
        let (store, branch) = self.target()?;
        let bytes =
            serde_json::to_vec(&value).map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let record = store
            .update_state_on(branch, &state_id, StateOperation::Set(bytes))
            .map_err(to_napi_error)?;
        Ok(record.into())
    }

    /// Append to an AppendLog state.
    #[napi]
    pub fn append_to_state(&self, state_id: String, item: Buffer) -> Result<JsRecord> {
        let (store, branch) = self.target()?;
        let record = store
            .update_state_on(branch, &state_id, StateOperation::Append(item.to_vec()))
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
        let (store, branch) = self.target()?;
        let bytes =
            serde_json::to_vec(&item).map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let record = store
            .update_state_on(branch, &state_id, StateOperation::Append(bytes))
            .map_err(to_napi_error)?;
        Ok(record.into())
    }

    /// Append JSON to an AppendLog state, injecting the assigned record id
    /// and sequence into the payload in a single atomic op.
    ///
    /// `item` must be a JSON object. `idField` receives the assigned record
    /// id (as a string, matching `JsRecord.id`); `sequenceField` receives the
    /// assigned sequence (as a number). Use this in place of
    /// `appendToStateJson` + `editStateItem` when the caller wants each
    /// stored item to carry its own chronicle identity — it avoids the
    /// follow-up Edit that would otherwise promote every snapshot to Full.
    ///
    /// Throws, writing nothing, for an `item` that isn't a JSON object.
    /// Once the item is recorded the call returns its record. A write that
    /// reaches the state's snapshot cadence takes the snapshot after its
    /// record, and if that snapshot fails the call still returns: the store
    /// logs a `tracing` warning, and the next write to the state tries the
    /// snapshot again (issue #39).
    #[napi]
    pub fn append_to_state_json_with_identity(
        &self,
        state_id: String,
        item: serde_json::Value,
        id_field: String,
        sequence_field: String,
    ) -> Result<JsRecord> {
        let (store, branch) = self.target()?;
        let record = store
            .append_to_state_json_with_identity_on(branch, &state_id, item, &id_field, &sequence_field)
            .map_err(to_napi_error)?;
        Ok(record.into())
    }

    /// Append several JSON items to an AppendLog state as one record, at one
    /// sequence, and return that record with the index the first item took
    /// (`firstOrdinal`); item `i` is at `firstOrdinal + i`.
    ///
    /// Reads see the items exactly as if each had been appended with
    /// `appendToStateJson`, in order. The batch counts as one operation
    /// toward the state's snapshot cadence, and subscribers get one
    /// "state_delta" event for it (see `JsStoreEvent.data`). Throws, writing
    /// nothing, for an empty `items`. Once the batch is recorded the call
    /// returns it. A write that reaches the snapshot cadence takes the
    /// snapshot after its record, and if that snapshot fails the call still
    /// returns: the store logs a `tracing` warning, and the next write to
    /// the state tries the snapshot again (issue #39).
    ///
    /// The first batch a store records changes its format: the store's
    /// `MANIFEST` goes to version 2, and releases from before batched
    /// appends refuse to open it.
    #[napi]
    pub fn append_many_to_state_json(
        &self,
        state_id: String,
        items: Vec<serde_json::Value>,
    ) -> Result<JsAppendedBatch> {
        let (store, branch) = self.target()?;
        let batch = store
            .append_many_to_state_json_on(branch, &state_id, &items)
            .map_err(to_napi_error)?;
        Ok(JsAppendedBatch {
            record: batch.record.into(),
            first_ordinal: batch.first_ordinal as i64,
        })
    }

    /// Append several JSON objects to an AppendLog state as one record, at
    /// one sequence, writing that record's id and sequence into each of them
    /// first, and return the record with the index the first item took
    /// (`firstOrdinal`); item `i` is at `firstOrdinal + i`. It is the batched
    /// form of `appendToStateJsonWithIdentity`, as `appendManyToStateJson`
    /// is of `appendToStateJson`.
    ///
    /// Each item is stamped as `appendToStateJsonWithIdentity` stamps its
    /// one item: `idField` receives the record id (as a string, matching
    /// `JsRecord.id`), `sequenceField` the sequence (as a number), and
    /// existing values at those keys are overwritten. The batch is one
    /// record, so every item of it carries the same id and sequence, those
    /// of `record`. A branch cut by an item's sequence (`createBranchAt` at
    /// it keeps the item, at the one before drops it) keeps or drops the
    /// whole batch with it.
    ///
    /// Otherwise it is `appendManyToStateJson`: reads see the stamped items
    /// as if each had been appended alone, the batch counts as one operation
    /// toward the snapshot cadence, and subscribers get one "state_delta"
    /// event for it. Throws, writing nothing, for an empty `items` or an
    /// item that isn't a JSON object. Once the batch is recorded the call
    /// returns it. A write that reaches the snapshot cadence takes the
    /// snapshot after its record, and if that snapshot fails the call still
    /// returns: the store logs a `tracing` warning, and the next write to
    /// the state tries the snapshot again (issue #39).
    ///
    /// The first batch a store records, by this call or
    /// `appendManyToStateJson`, changes its format: the store's `MANIFEST`
    /// goes to version 2, and releases from before batched appends refuse to
    /// open it. A store that never records a batch keeps the format they
    /// open.
    #[napi]
    pub fn append_many_to_state_json_with_identity(
        &self,
        state_id: String,
        items: Vec<serde_json::Value>,
        id_field: String,
        sequence_field: String,
    ) -> Result<JsAppendedBatch> {
        let (store, branch) = self.target()?;
        let batch = store
            .append_many_to_state_json_with_identity_on(
                branch,
                &state_id,
                &items,
                &id_field,
                &sequence_field,
            )
            .map_err(to_napi_error)?;
        Ok(JsAppendedBatch {
            record: batch.record.into(),
            first_ordinal: batch.first_ordinal as i64,
        })
    }

    /// Edit an item in an AppendLog state.
    #[napi]
    pub fn edit_state_item(
        &self,
        state_id: String,
        index: i64,
        new_value: Buffer,
    ) -> Result<JsRecord> {
        let (store, branch) = self.target()?;
        let record = store
            .update_state_on(
                branch,
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
        let (store, branch) = self.target()?;
        let record = store
            .update_state_on(
                branch,
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
        let (store, branch) = self.target()?;
        let len = store
            .get_state_len_on(branch, &state_id)
            .map_err(to_napi_error)?;
        Ok(len.map(|l| l as i64))
    }

    /// Revision key for a state's value on this handle's branch: the log
    /// offset of its newest chain record, or `null` if the state has no
    /// value there. It changes on every write to that state on that branch
    /// (append, edit, redact, set, and the store's own snapshots) and on
    /// nothing else, so a cached copy of the value is current exactly while
    /// this number is unchanged — including across several handles on the
    /// same branch, which a per-handle counter cannot see. O(1).
    #[napi]
    pub fn state_head_offset(&self, state_id: String) -> Result<Option<i64>> {
        let (store, branch) = self.target()?;
        let offset = store
            .state_head_offset_on(branch, &state_id)
            .map_err(to_napi_error)?;
        Ok(offset.map(|o| o as i64))
    }

    /// Get a slice of an AppendLog state.
    #[napi]
    pub fn get_state_slice(
        &self,
        state_id: String,
        offset: i64,
        limit: i64,
    ) -> Result<Option<Buffer>> {
        let (store, branch) = self.target()?;
        let slice = store
            .get_state_slice_on(branch, &state_id, offset as usize, limit as usize)
            .map_err(to_napi_error)?;
        Ok(slice.map(Buffer::from))
    }

    /// Get a single item of an AppendLog state by index, as JSON.
    ///
    /// O(item size) once the per-item cache is warm — unlike `getStateJson`,
    /// which materializes and converts the entire state on every call. Use
    /// this for point lookups in large states. Returns `null` if the state
    /// doesn't exist, the index is negative or out of range, or the state's
    /// value isn't an array.
    #[napi]
    pub fn get_state_item_json(
        &self,
        state_id: String,
        index: i64,
    ) -> Result<Option<serde_json::Value>> {
        if index < 0 {
            return Ok(None);
        }
        let (store, branch) = self.target()?;
        let item = store
            .get_state_item_on(branch, &state_id, index as usize)
            .map_err(to_napi_error)?;
        match item {
            Some(bytes) => {
                let value: serde_json::Value = serde_json::from_slice(&bytes)
                    .map_err(|e| napi::Error::from_reason(e.to_string()))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Get several items of an AppendLog state by index, as JSON, in one
    /// call. The result has one entry per index in `ordinals`, in the order
    /// given: what `getStateItemJson` returns for that index, so `null` if
    /// it is negative or out of range, the state doesn't exist, or the
    /// state's value isn't an array. Indices may repeat and come in any
    /// order.
    ///
    /// Every entry comes from the same version of the state, and a cold
    /// state is materialized at most once for the whole call, not once per
    /// index. A request with no index that is zero or more (an empty one,
    /// or one of negative indices only) reads nothing.
    #[napi]
    pub fn get_state_items_json(
        &self,
        state_id: String,
        ordinals: Vec<i64>,
    ) -> Result<Vec<Option<serde_json::Value>>> {
        // A negative index answers null without a read, as getStateItemJson's
        // does; only the others go to the store.
        let asked: Vec<Option<usize>> = ordinals.iter().map(|&i| usize::try_from(i).ok()).collect();
        let wanted: Vec<usize> = asked.iter().flatten().copied().collect();
        if wanted.is_empty() {
            return Ok(vec![None; ordinals.len()]);
        }
        let (store, branch) = self.target()?;
        let mut items = store
            .get_state_items_by_ordinal_on(branch, &state_id, &wanted)
            .map_err(to_napi_error)?
            .into_iter();
        asked
            .into_iter()
            .map(|ordinal| {
                // One answer per index the store was asked for, in order.
                let item = match ordinal {
                    Some(_) => items.next().flatten(),
                    None => None,
                };
                item.map(|bytes| {
                    serde_json::from_slice(&bytes)
                        .map_err(|e| napi::Error::from_reason(e.to_string()))
                })
                .transpose()
            })
            .collect()
    }

    /// Get the last N items from an AppendLog state.
    #[napi]
    pub fn get_state_tail(&self, state_id: String, count: i64) -> Result<Option<Buffer>> {
        let (store, branch) = self.target()?;
        let tail = store
            .get_state_tail_on(branch, &state_id, count as usize)
            .map_err(to_napi_error)?;
        Ok(tail.map(Buffer::from))
    }

    // --- Compaction ---

    /// Toggle auto-snapshotting inside `update_state`.
    ///
    /// While disabled, snapshot thresholds are tracked but no snapshot record
    /// is written. Use this around bulk imports to avoid the O(N²) snapshot
    /// series, and call `compactState` afterwards to write a single final
    /// snapshot so reconstruction stays fast. Default is `true`.
    #[napi]
    pub fn set_auto_snapshot(&self, enabled: bool) -> Result<()> {
        let store = self.get_store()?;
        store.set_auto_snapshot(enabled);
        Ok(())
    }

    /// Whether auto-snapshotting is currently enabled.
    #[napi]
    pub fn auto_snapshot_enabled(&self) -> Result<bool> {
        let store = self.get_store()?;
        Ok(store.auto_snapshot_enabled())
    }

    /// Compact a state by creating a full snapshot.
    #[napi]
    pub fn compact_state(&self, state_id: String) -> Result<Option<JsRecord>> {
        let (store, branch) = self.target()?;
        let record = store
            .compact_state_on(branch, &state_id)
            .map_err(to_napi_error)?;
        Ok(record.map(Into::into))
    }

    /// Compact all states.
    #[napi]
    pub fn compact_all_states(&self) -> Result<i64> {
        let (store, branch) = self.target()?;
        let count = store.compact_all_states_on(branch).map_err(to_napi_error)?;
        Ok(count as i64)
    }

    /// Get compaction summary.
    #[napi]
    pub fn get_compaction_summary(&self) -> Result<JsCompactionSummary> {
        let (store, branch) = self.target()?;
        let summary = store
            .get_compaction_summary_on(branch)
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
    /// The internal `state_update` type is reserved for the state-update helpers.
    #[napi]
    pub fn append_with_links(
        &self,
        record_type: String,
        payload: Buffer,
        options: JsRecordOptions,
    ) -> Result<JsRecord> {
        let (store, branch) = self.target()?;
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
        let record = store.append_on(branch, input).map_err(to_napi_error)?;
        Ok(record.into())
    }

    /// Append a JSON record with caused_by/linked_to links.
    /// The internal `state_update` type is reserved for the state-update helpers.
    #[napi]
    pub fn append_json_with_links(
        &self,
        record_type: String,
        data: serde_json::Value,
        options: JsRecordOptions,
    ) -> Result<JsRecord> {
        let (store, branch) = self.target()?;
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
        let record = store.append_on(branch, input).map_err(to_napi_error)?;
        Ok(record.into())
    }

    /// Query records with filters.
    ///
    /// Uses efficient BTreeMap-based range queries for O(log n + k) performance.
    /// - `from_sequence`: Start sequence (inclusive)
    /// - `to_sequence`: End sequence (inclusive)
    /// - `limit`: Maximum records to return
    /// - `offset`: Skip first N records (note: offset is less efficient than sequence-based pagination)
    /// - `reverse`: Return records in descending sequence order (newest first)
    /// - `types`: Filter by record types
    #[napi]
    pub fn query(&self, filter: JsQueryFilter) -> Result<Vec<JsRecord>> {
        let (store, branch) = self.target()?;
        let from_seq = filter.from_sequence.map(|s| Sequence(s as u64));
        let to_seq = filter.to_sequence.map(|s| Sequence(s as u64));
        // Default limit to 10,000 to avoid capacity overflow panics
        let limit = filter.limit.map(|l| l as usize).unwrap_or(10_000);
        let offset = filter.offset.map(|o| o as usize).unwrap_or(0);
        let reverse = filter.reverse.unwrap_or(false);
        let types = filter.types;

        // For offset-based pagination, we need to fetch offset + limit records
        // This is less efficient than sequence-based pagination, but maintains backward compatibility
        let fetch_limit = if offset > 0 {
            offset.saturating_add(limit)
        } else {
            limit
        };

        let records = store
            .query_range_on(
                branch,
                from_seq,
                to_seq,
                fetch_limit,
                reverse,
                types.as_ref().map(|v| v.as_slice()),
            )
            .map_err(to_napi_error)?;

        // Apply offset (skip first N)
        let result: Vec<JsRecord> = records
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|r| r.into())
            .collect();

        Ok(result)
    }

    /// List all registered states with their info.
    #[napi]
    pub fn list_states(&self) -> Result<Vec<JsStateInfo>> {
        let (store, branch_id) = self.target()?;
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
                        StateStrategy::Tree { .. } => "tree".to_string(),
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
        let (store, branch) = self.target()?;
        let branch = store
            .get_branch_by_id(branch)
            .ok_or_else(|| napi::Error::from_reason("Branch not found"))?;
        Ok(branch.head.0 as i64)
    }

    /// Get state value at a specific sequence (historical access).
    #[napi]
    pub fn get_state_at(&self, state_id: String, at_sequence: i64) -> Result<Option<Buffer>> {
        let (store, branch) = self.target()?;
        let state = store
            .get_state_at_on(branch, &state_id, Sequence(at_sequence as u64))
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
        let (store, branch) = self.target()?;
        let state = store
            .get_state_at_on(branch, &state_id, Sequence(at_sequence as u64))
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

    /// Flush pending record/state contents and publish metadata checkpoints.
    /// A failure to save field indexes is only logged, since they are derived.
    /// Once an fsync of the record log has failed, this call's own or a
    /// periodic one, this throws that failure every time until the store is
    /// reopened, and publishes nothing: records written before it may not
    /// have reached the disk, and a later fsync can't show that they did.
    /// Its message begins `Durability lost:`. Registering a state, changing
    /// a strategy, and creating, deleting or switching a branch throw it too,
    /// and so does any write while one of them waits for its checkpoint.
    /// Reopening the store is the only way to resume durable writes.
    /// Reopening can't tell whether records the operating system still caches
    /// reached the disk: if they didn't while later records did, an open
    /// after the cache drops them refuses the store as mid-log corruption.
    /// Reopen replays complete state updates past the last checkpoint; other
    /// records are only indexed again.
    /// On Unix a save also syncs the containing directory, where the system
    /// can. On Windows it doesn't wait for the rename to reach the disk, so
    /// after an OS crash or power loss the store may open from an earlier
    /// checkpoint.
    /// State registration, strategy changes and branch changes checkpoint
    /// before returning. After one of those checkpoints fails, further
    /// mutations retry it before proceeding, and the change stays applied in
    /// memory. A failed sync with no such change pending holds back no later
    /// append or state update.
    #[napi]
    pub fn sync(&self) -> Result<()> {
        let store = self.get_store()?;
        store.sync().map_err(to_napi_error)
    }

    // --- Subscription Methods ---

    /// Subscribe to store events.
    ///
    /// Returns a subscription ID. Use `poll_subscription` to receive events
    /// and `catch_up_subscription` to replay historical data.
    #[napi]
    pub fn subscribe(&self, config: Option<JsSubscriptionConfig>) -> Result<String> {
        let store = self.get_store()?;

        // Convert JS config to Rust config
        let rust_config = match config {
            Some(cfg) => {
                let filter = cfg.filter.map(|f| SubscriptionFilter {
                    record_types: f.record_types,
                    branch: f.branch,
                    state_ids: f.state_ids,
                    include_records: f.include_records.unwrap_or(false),
                    include_state_changes: f.include_state_changes.unwrap_or(false),
                    include_branch_events: f.include_branch_events.unwrap_or(false),
                });

                SubscriptionConfig {
                    buffer_size: cfg.buffer_size.map(|s| s as usize).unwrap_or(1000),
                    max_snapshot_bytes: cfg
                        .max_snapshot_bytes
                        .map(|s| s as usize)
                        .unwrap_or(10 * 1024 * 1024),
                    from_sequence: cfg.from_sequence.map(|s| Sequence(s as u64)),
                    filter: filter.unwrap_or_default(),
                }
            }
            None => SubscriptionConfig::default(),
        };

        let handle = store.subscribe(rust_config);
        let id = handle.id.0;

        // Store the handle for later polling
        self.subscription_handles.lock().insert(id, handle);

        Ok(id.to_string())
    }

    /// Unsubscribe and clean up a subscription.
    #[napi]
    pub fn unsubscribe(&self, subscription_id: String) -> Result<()> {
        let store = self.get_store()?;
        let id: u64 = subscription_id
            .parse()
            .map_err(|_| napi::Error::from_reason("Invalid subscription ID"))?;

        // Remove handle from our map
        self.subscription_handles.lock().remove(&id);

        // Unsubscribe from store
        store.unsubscribe(SubscriptionId(id));
        Ok(())
    }

    /// Perform catch-up for a subscription (replay historical data).
    ///
    /// This replays state snapshots and historical records based on the
    /// subscription's `from_sequence` setting.
    #[napi]
    pub fn catch_up_subscription(&self, subscription_id: String) -> Result<()> {
        let (store, branch) = self.target()?;
        let id: u64 = subscription_id
            .parse()
            .map_err(|_| napi::Error::from_reason("Invalid subscription ID"))?;

        store
            .catch_up_subscription_on(branch, SubscriptionId(id))
            .map_err(to_napi_error)
    }

    /// Poll for the next event from a subscription (non-blocking).
    ///
    /// Returns null if no events are available.
    #[napi]
    pub fn poll_subscription(&self, subscription_id: String) -> Result<Option<JsStoreEvent>> {
        let id: u64 = subscription_id
            .parse()
            .map_err(|_| napi::Error::from_reason("Invalid subscription ID"))?;

        let handles = self.subscription_handles.lock();
        let handle = handles
            .get(&id)
            .ok_or_else(|| napi::Error::from_reason("Subscription not found"))?;

        match handle.try_recv() {
            Ok(event) => Ok(Some(event.into())),
            Err(crossbeam_channel::TryRecvError::Empty) => Ok(None),
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                Err(napi::Error::from_reason("Subscription disconnected"))
            }
        }
    }

    /// Poll for the next event with a timeout (milliseconds).
    ///
    /// Returns null if no event received within the timeout.
    #[napi]
    pub fn poll_subscription_timeout(
        &self,
        subscription_id: String,
        timeout_ms: i64,
    ) -> Result<Option<JsStoreEvent>> {
        let id: u64 = subscription_id
            .parse()
            .map_err(|_| napi::Error::from_reason("Invalid subscription ID"))?;

        let handles = self.subscription_handles.lock();
        let handle = handles
            .get(&id)
            .ok_or_else(|| napi::Error::from_reason("Subscription not found"))?;

        let timeout = std::time::Duration::from_millis(timeout_ms as u64);
        match handle.recv_timeout(timeout) {
            Ok(event) => Ok(Some(event.into())),
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => Ok(None),
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                Err(napi::Error::from_reason("Subscription disconnected"))
            }
        }
    }

    /// Get the number of active subscriptions.
    #[napi]
    pub fn subscription_count(&self) -> Result<i64> {
        let store = self.get_store()?;
        Ok(store.subscription_count() as i64)
    }

    // --- Tree Operations ---

    /// Set a single file in a tree state.
    #[napi]
    pub fn tree_set(
        &self,
        state_id: String,
        path: String,
        entry: JsTreeEntry,
    ) -> Result<JsRecord> {
        let (store, branch) = self.target()?;
        let tree_entry = TreeEntry {
            blob_hash: entry.blob_hash,
            size: entry.size as u64,
            mode: entry.mode as u32,
        };
        let record = store
            .tree_set_on(branch, &state_id, &path, &tree_entry)
            .map_err(to_napi_error)?;
        Ok(record.into())
    }

    /// Remove a single file from a tree state.
    #[napi]
    pub fn tree_remove(&self, state_id: String, path: String) -> Result<JsRecord> {
        let (store, branch) = self.target()?;
        let record = store.tree_remove_on(branch, &state_id, &path).map_err(to_napi_error)?;
        Ok(record.into())
    }

    /// Apply a batch of tree operations atomically.
    #[napi]
    pub fn tree_batch(&self, state_id: String, ops: Vec<JsTreeOp>) -> Result<JsRecord> {
        let (store, branch) = self.target()?;
        let tree_ops: Vec<TreeOp> = ops
            .into_iter()
            .map(|op| match op.op.as_str() {
                "set" => {
                    let entry = op
                        .entry
                        .ok_or_else(|| napi::Error::from_reason("TreeOp 'set' requires entry"))?;
                    Ok(TreeOp::Set {
                        path: op.path,
                        entry: TreeEntry {
                            blob_hash: entry.blob_hash,
                            size: entry.size as u64,
                            mode: entry.mode as u32,
                        },
                    })
                }
                "remove" => Ok(TreeOp::Remove { path: op.path }),
                other => Err(napi::Error::from_reason(format!(
                    "Invalid tree op: '{}', expected 'set' or 'remove'",
                    other
                ))),
            })
            .collect::<Result<Vec<_>>>()?;

        let record = store
            .tree_batch_on(branch, &state_id, &tree_ops)
            .map_err(to_napi_error)?;
        Ok(record.into())
    }

    /// Get a single file entry from a tree state.
    #[napi]
    pub fn tree_get(&self, state_id: String, path: String) -> Result<Option<JsTreeEntry>> {
        let (store, branch) = self.target()?;
        let entry = store
            .tree_get_on(branch, &state_id, &path)
            .map_err(to_napi_error)?;
        Ok(entry.map(|e| JsTreeEntry {
            blob_hash: e.blob_hash,
            size: e.size as i64,
            mode: e.mode as i32,
        }))
    }

    /// List files in a tree state, optionally filtered by path prefix.
    #[napi]
    pub fn tree_list(
        &self,
        state_id: String,
        prefix: Option<String>,
    ) -> Result<Vec<JsTreeListEntry>> {
        let (store, branch) = self.target()?;
        let entries = store
            .tree_list_on(branch, &state_id, prefix.as_deref())
            .map_err(to_napi_error)?;
        Ok(entries
            .into_iter()
            .map(|(path, entry)| JsTreeListEntry {
                path,
                blob_hash: entry.blob_hash,
                size: entry.size as i64,
                mode: entry.mode as i32,
            })
            .collect())
    }

    /// Compute diff between a tree state at two sequence points.
    #[napi]
    pub fn tree_diff(
        &self,
        state_id: String,
        from_sequence: i64,
        to_sequence: i64,
    ) -> Result<Vec<JsTreeChange>> {
        let (store, branch) = self.target()?;
        let changes = store
            .tree_diff_on(
                branch,
                &state_id,
                Sequence(from_sequence as u64),
                Sequence(to_sequence as u64),
            )
            .map_err(to_napi_error)?;

        Ok(changes
            .into_iter()
            .map(|change| match change {
                TreeChange::Added { path, entry } => JsTreeChange {
                    change_type: "added".to_string(),
                    path,
                    old_entry: None,
                    new_entry: Some(JsTreeEntry {
                        blob_hash: entry.blob_hash,
                        size: entry.size as i64,
                        mode: entry.mode as i32,
                    }),
                },
                TreeChange::Modified { path, old, new } => JsTreeChange {
                    change_type: "modified".to_string(),
                    path,
                    old_entry: Some(JsTreeEntry {
                        blob_hash: old.blob_hash,
                        size: old.size as i64,
                        mode: old.mode as i32,
                    }),
                    new_entry: Some(JsTreeEntry {
                        blob_hash: new.blob_hash,
                        size: new.size as i64,
                        mode: new.mode as i32,
                    }),
                },
                TreeChange::Removed { path, entry } => JsTreeChange {
                    change_type: "removed".to_string(),
                    path,
                    old_entry: Some(JsTreeEntry {
                        blob_hash: entry.blob_hash,
                        size: entry.size as i64,
                        mode: entry.mode as i32,
                    }),
                    new_entry: None,
                },
            })
            .collect())
    }

    /// Force a full snapshot of a tree state (compaction).
    #[napi]
    pub fn tree_snapshot(&self, state_id: String) -> Result<Option<JsRecord>> {
        let (store, branch) = self.target()?;
        let record = store.tree_snapshot_on(branch, &state_id).map_err(to_napi_error)?;
        Ok(record.map(|r| r.into()))
    }

    // --- State Field Indexes ---

    /// Register (or refresh) a secondary index on a JSON field of every item
    /// in a state slot (state slots are JSON arrays, e.g. context-manager's
    /// `messages` slot). `field` is a JSON-pointer path (e.g. `/timestamp` or
    /// `/metadata/external/channelId`). `kind` is `"number"` or `"string"`.
    ///
    /// Idempotent — a no-op if an index for this `(stateId, field)` is
    /// already registered, fresh (same branch, same slot head), and of the
    /// same kind. Incrementally maintained thereafter as the slot mutates
    /// (append, edit, redact, set/snapshot) on the branch it was registered
    /// on, and persisted alongside the store.
    ///
    /// Indexes are per branch (the current branch; on a view, the pinned
    /// branch). Writes on other branches to the same `stateId` neither
    /// update nor drop this index, and a branch that has not registered the
    /// index has none: queries there return `null` until it registers its
    /// own. Registering on a branch whose slot is unchanged since it forked
    /// copies the parent's index instead of rebuilding it; either way each
    /// branch's index costs memory proportional to the slot's length.
    ///
    /// Unlike `registerState`, this doesn't checkpoint before returning: an
    /// index is derived from its state, so a crash before the next `sync()`
    /// loses only the index. Queries then return `null`, the cue to call
    /// this again.
    #[napi]
    pub fn register_state_field_index(
        &self,
        state_id: String,
        field: String,
        kind: String,
    ) -> Result<()> {
        let (store, branch) = self.target()?;
        let kind = match kind.as_str() {
            "number" => FieldIndexKind::Number,
            "string" => FieldIndexKind::String,
            _ => {
                return Err(napi::Error::from_reason(format!(
                    "Invalid field index kind '{}': expected \"number\" or \"string\"",
                    kind
                )))
            }
        };
        store
            .register_state_field_index_on(branch, &state_id, &field, kind)
            .map_err(to_napi_error)
    }

    /// Query ordinals of a registered numeric field index within
    /// `[opts.gte, opts.lte]` (either bound optional). Returns matching
    /// ordinals (array indices), not content — fetch items separately via
    /// `getStateItemJson`/`getStateSlice`.
    ///
    /// Returns `null` if no such `"number"` index is currently registered
    /// on this branch (never registered there, registered as `"string"`
    /// instead, or dropped by a parse failure) — distinct from `[]`, an index
    /// that exists but has no matches in range. Treat `null` as "call
    /// `registerStateFieldIndex` again", not as "empty range".
    #[napi]
    pub fn query_state_index_range(
        &self,
        state_id: String,
        field: String,
        opts: JsIndexRangeQuery,
    ) -> Result<Option<Vec<u32>>> {
        let (store, branch) = self.target()?;
        Ok(store.query_state_index_range_on(
            branch,
            &state_id,
            &field,
            opts.gte,
            opts.lte,
            opts.limit.map(|l| l as usize),
            opts.offset.map(|o| o as usize),
            opts.reverse.unwrap_or(false),
        ))
    }

    /// Query ordinals of a registered string field index equal to `value`.
    /// Returns matching ordinals only.
    ///
    /// Returns `null` if no such `"string"` index is currently registered —
    /// see `queryStateIndexRange`'s doc for the `null` vs `[]` distinction.
    #[napi]
    pub fn query_state_index_eq(
        &self,
        state_id: String,
        field: String,
        value: String,
        opts: JsIndexEqQuery,
    ) -> Result<Option<Vec<u32>>> {
        let (store, branch) = self.target()?;
        Ok(store.query_state_index_eq_on(
            branch,
            &state_id,
            &field,
            &value,
            opts.limit.map(|l| l as usize),
            opts.offset.map(|o| o as usize),
        ))
    }

    /// Distinct values and ordinal counts for a registered string field
    /// index — O(index size), no content decoding.
    ///
    /// Returns `null` if no such `"string"` index is currently registered.
    #[napi]
    pub fn get_state_index_value_counts(
        &self,
        state_id: String,
        field: String,
    ) -> Result<Option<Vec<JsValueCount>>> {
        let (store, branch) = self.target()?;
        Ok(store
            .get_state_index_value_counts_on(branch, &state_id, &field)
            .map(|counts| {
                counts
                    .into_iter()
                    .map(|(value, count)| JsValueCount { value, count })
                    .collect()
            }))
    }
}
