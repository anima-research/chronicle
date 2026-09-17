//! Secondary indexes on a JSON field extracted from every item of a state
//! slot (state slots are JSON arrays managed by [`super::StateManager`], e.g.
//! context-manager's `messages` slot).
//!
//! Each registered `(state_id, field_path)` pair maintains:
//! - a dense `by_ordinal` vector so Redact's ordinal shift and Edit's
//!   old-value removal never need to re-read old payload bytes, and
//! - a reverse index (`BTreeMap` for numeric range queries, `HashMap` for
//!   string equality/group-by) mapping extracted value -> ordinals.
//!
//! Queries return matching **ordinals** (array indices) only — callers fetch
//! content separately via `getStateItemJson`/`getStateSlice`. This makes
//! "ordinals with timestamp in [A,B]" or "ordinals where channelId == X"
//! O(log n + k) instead of decoding every item in the slot.
//!
//! `field_path` is a JSON-pointer string (RFC 6901), e.g. `/timestamp` or
//! `/metadata/external/channelId`, resolved via [`serde_json::Value::pointer`].
//!
//! **Branch scoping:** the map is keyed by `(state_id, field_path)` alone —
//! only one branch's view of a slot can have a live index at a time. Each
//! [`FieldIndex`] remembers the `BranchId` it was built against; any
//! mutating op for the same `state_id` arriving on a *different* branch
//! poisons (drops) that index instead of silently mixing the two branches'
//! ordinals into one structure. This is deliberately conservative: ordinary
//! branch-and-switch usage (this store's headline feature) must never leave
//! a query returning ordinals that belong to a different branch's array.
//! Querying a poisoned/never-registered index returns `None` (see the
//! `query_*`/`value_counts` docs) so callers can tell "no index" from "index
//! says zero matches" and re-register for the branch they're on.
//!
//! **Freshness:** `register` treats an existing index as fresh only if its
//! `kind`, `branch_id`, AND the slot's `head_offset` at last maintenance all
//! match — not just item count. Count-only freshness would miss the case
//! where a count-preserving Edit landed after `state.bin` was durably saved
//! but before `state-indexes.bin` was (the two are separate, non-atomic
//! files); comparing `head_offset` catches that and any other value drift.

use crate::error::{Result, StoreError};
use crate::types::BranchId;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

/// Magic bytes for the persisted state field-index file.
const FIELD_INDEX_MAGIC: &[u8; 4] = b"SFI\0";

/// Current field-index file format version. Bumped from 1 -> 2 when
/// `branch_id`/`head_offset` were added to the persisted shape (never
/// released, so no real back-compat burden — bumped anyway so a leftover
/// v1 file fails the version check and loads as `None` rather than
/// misparsing).
const FIELD_INDEX_VERSION: u8 = 2;

/// The declared type of a registered field index.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldIndexKind {
    /// Values are extracted via `Value::as_f64` and indexed for range
    /// queries.
    ///
    /// TODO: `f64` keys lose precision for integer IDs above 2^53 (e.g.
    /// Discord/Snowflake-style string-of-digits IDs parsed as numbers) —
    /// two distinct large IDs can collide onto the same reverse-index key.
    /// Recommend `String` kind for anything ID-shaped rather than genuinely
    /// numeric (timestamps, counts) until this gets a dedicated integer path.
    Number,
    /// Values are extracted as JSON strings and indexed for equality /
    /// group-by queries.
    String,
}

/// An extracted, indexed field value.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum IndexedValue {
    Number(f64),
    Str(String),
}

/// `f64` newtype with a total order (via `total_cmp`), so it can key a
/// `BTreeMap`. JSON numbers are never NaN, but `total_cmp` gives a defined
/// (if unremarkable) order even if one sneaks in.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
struct OrderedF64(f64);

impl Eq for OrderedF64 {}

impl PartialOrd for OrderedF64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// Insert `ordinal` into a sorted ordinal list, keeping it sorted. Ordinals
/// mostly arrive in increasing order (append-heavy workloads), but an Edit
/// can re-insert an ordinal below the bucket's max, so a plain `push` would
/// leave the list unsorted — binary-search insertion keeps queries returning
/// ordinals in ascending order within a bucket without a separate sort pass.
fn insert_ordinal_sorted(list: &mut Vec<u32>, ordinal: u32) {
    match list.binary_search(&ordinal) {
        Ok(_) => {} // already present; shouldn't happen, but idempotent
        Err(pos) => list.insert(pos, ordinal),
    }
}

/// Remove `ordinal` from a sorted ordinal list.
fn remove_ordinal(list: &mut Vec<u32>, ordinal: u32) {
    if let Ok(pos) = list.binary_search(&ordinal) {
        list.remove(pos);
    }
}

/// One registered field index: dense per-ordinal values plus a reverse
/// index for queries, scoped to the branch it was built against.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct FieldIndex {
    kind: FieldIndexKind,
    /// The branch this index's content reflects. A mutating op for the same
    /// `state_id` on any other branch poisons (drops) this index rather than
    /// applying — see the module docs' "Branch scoping" note.
    branch_id: BranchId,
    /// Offset of the state-chain head record this index was last maintained
    /// against (`None` only for a freshly-registered index over an empty,
    /// never-written slot). Used by `register`'s freshness check instead of
    /// item count alone — see the module docs' "Freshness" note.
    head_offset: Option<u64>,
    /// `by_ordinal[i]` is the extracted value for item `i`, or `None` if the
    /// field was absent / not extractable (type mismatch) at that item.
    by_ordinal: Vec<Option<IndexedValue>>,
    /// Reverse index for `Number` fields: value -> sorted ordinals.
    number_rev: BTreeMap<OrderedF64, Vec<u32>>,
    /// Reverse index for `String` fields: value -> sorted ordinals.
    string_rev: HashMap<String, Vec<u32>>,
}

impl FieldIndex {
    fn new(kind: FieldIndexKind, branch_id: BranchId, head_offset: Option<u64>) -> Self {
        Self {
            kind,
            branch_id,
            head_offset,
            by_ordinal: Vec::new(),
            number_rev: BTreeMap::new(),
            string_rev: HashMap::new(),
        }
    }

    fn insert_into_reverse(&mut self, ordinal: u32, value: &Option<IndexedValue>) {
        match value {
            Some(IndexedValue::Number(n)) => {
                insert_ordinal_sorted(self.number_rev.entry(OrderedF64(*n)).or_default(), ordinal);
            }
            Some(IndexedValue::Str(s)) => {
                insert_ordinal_sorted(self.string_rev.entry(s.clone()).or_default(), ordinal);
            }
            None => {}
        }
    }

    fn remove_from_reverse(&mut self, ordinal: u32, value: &Option<IndexedValue>) {
        match value {
            Some(IndexedValue::Number(n)) => {
                let key = OrderedF64(*n);
                if let Some(list) = self.number_rev.get_mut(&key) {
                    remove_ordinal(list, ordinal);
                    if list.is_empty() {
                        self.number_rev.remove(&key);
                    }
                }
            }
            Some(IndexedValue::Str(s)) => {
                let mut drop_key = false;
                if let Some(list) = self.string_rev.get_mut(s) {
                    remove_ordinal(list, ordinal);
                    drop_key = list.is_empty();
                }
                if drop_key {
                    self.string_rev.remove(s);
                }
            }
            None => {}
        }
    }

    /// Append one value at the end (new ordinal = current length).
    fn push(&mut self, value: Option<IndexedValue>) {
        let ordinal = self.by_ordinal.len() as u32;
        self.insert_into_reverse(ordinal, &value);
        self.by_ordinal.push(value);
    }

    /// Rebuild the whole index (and reverse index) fresh from `items`.
    fn rebuild<'a>(&mut self, field_path: &str, items: impl Iterator<Item = &'a serde_json::Value>) {
        self.by_ordinal.clear();
        self.number_rev.clear();
        self.string_rev.clear();
        for item in items {
            let value = extract_indexed_value(item, field_path, self.kind);
            self.push(value);
        }
    }
}

/// Fold `-0.0` to `0.0`. `total_cmp` (what orders `OrderedF64`, below)
/// distinguishes the two — unlike ordinary `==` and `<`/`>`, which treat
/// them equal — so without this, an item indexed at `-0.0` would sort just
/// below `0.0` in the reverse index and an inclusive `[0, 0]` range query
/// would silently miss it. Applied both when indexing a value and when
/// evaluating a query's bounds, so either side landing on `-0.0` still
/// matches the other.
fn normalize_zero(v: f64) -> f64 {
    if v == 0.0 {
        0.0
    } else {
        v
    }
}

fn extract_indexed_value(
    item: &serde_json::Value,
    field_path: &str,
    kind: FieldIndexKind,
) -> Option<IndexedValue> {
    let value = item.pointer(field_path)?;
    match kind {
        FieldIndexKind::Number => value
            .as_f64()
            .map(|n| IndexedValue::Number(normalize_zero(n))),
        FieldIndexKind::String => match value {
            serde_json::Value::String(s) => Some(IndexedValue::Str(s.clone())),
            _ => None,
        },
    }
}

fn validate_field_path(field_path: &str) -> Result<()> {
    // RFC 6901: the empty string, or a string starting with "/".
    if field_path.is_empty() || field_path.starts_with('/') {
        Ok(())
    } else {
        Err(StoreError::InvalidOperation(format!(
            "field index path '{}' is not a valid JSON pointer (must be empty or start with '/')",
            field_path
        )))
    }
}

/// Owns every registered field index across every state slot, keyed by
/// `(state_id, field_path)`. See the module docs for the branch-scoping and
/// freshness rules this type enforces.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FieldIndexManager {
    indexes: HashMap<(String, String), FieldIndex>,
}

impl FieldIndexManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether any field is registered for `state_id` — a cheap gate so
    /// `record_update`'s per-op hooks can skip parsing item bytes entirely
    /// when nothing is indexed for the mutated slot.
    pub fn has_indexes_for(&self, state_id: &str) -> bool {
        self.indexes.keys().any(|(sid, _)| sid == state_id)
    }

    fn matching_keys(&self, state_id: &str) -> Vec<(String, String)> {
        self.indexes
            .keys()
            .filter(|(sid, _)| sid == state_id)
            .cloned()
            .collect()
    }

    /// `true` if `key` is registered and was built against `branch_id`.
    fn branch_matches(&self, key: &(String, String), branch_id: BranchId) -> bool {
        self.indexes.get(key).map(|i| i.branch_id) == Some(branch_id)
    }

    /// Whether a `register` call with these exact `(kind, branch_id,
    /// head_offset)` would be a no-op — i.e. an index for `(state_id,
    /// field_path)` is already registered and matches all three.
    ///
    /// Exposed separately from `register` (which also takes this same
    /// no-op path internally) so a caller that would otherwise have to
    /// materialize and fully JSON-parse the whole slot just to build the
    /// `current_items` iterator can check freshness FIRST and skip that
    /// work entirely on the already-fresh path — `head_offset` alone is
    /// enough to answer the question. Materializing a large slot isn't
    /// free (~110ms measured for 20k items vs ~12us for a query), and
    /// registration is commonly called unconditionally on every boot
    /// (context-manager's `MessageStore` does), so an idempotent
    /// already-registered call staying cheap is load-bearing, not a nicety.
    pub fn is_fresh(
        &self,
        state_id: &str,
        field_path: &str,
        kind: FieldIndexKind,
        branch_id: BranchId,
        head_offset: Option<u64>,
    ) -> bool {
        match self.indexes.get(&(state_id.to_string(), field_path.to_string())) {
            Some(existing) => {
                existing.kind == kind
                    && existing.branch_id == branch_id
                    && existing.head_offset == head_offset
            }
            None => false,
        }
    }

    /// Register a field index, building it fresh from `current_items` if
    /// it isn't already registered and fresh for `branch_id` — same `kind`,
    /// same `branch_id`, AND the same `head_offset` as last maintained
    /// (idempotent no-op only in that exact case; see module docs).
    ///
    /// `current_items` is only consumed on the non-fresh (rebuild) path —
    /// see `is_fresh`'s doc if you can check freshness before your caller
    /// even materializes the iterator's source; `StateManager::register_field_index`
    /// does exactly that.
    pub fn register<'a>(
        &mut self,
        state_id: &str,
        field_path: &str,
        kind: FieldIndexKind,
        branch_id: BranchId,
        head_offset: Option<u64>,
        current_items: impl Iterator<Item = &'a serde_json::Value>,
    ) -> Result<()> {
        validate_field_path(field_path)?;
        if self.is_fresh(state_id, field_path, kind, branch_id, head_offset) {
            return Ok(()); // already fresh
        }

        let key = (state_id.to_string(), field_path.to_string());
        let mut index = FieldIndex::new(kind, branch_id, head_offset);
        index.rebuild(field_path, current_items);
        self.indexes.insert(key, index);
        Ok(())
    }

    /// Drop every field index registered for `state_id` — used when an item
    /// in the slot can't be parsed as JSON at all (so `by_ordinal` alignment
    /// can't be maintained for ANY field on that slot going forward) or when
    /// an incremental op reveals internal desync (e.g. an Edit ordinal past
    /// the index's recorded length). Fails closed: a poisoned index is gone,
    /// not silently wrong — the next `register` rebuilds it from scratch.
    pub fn poison_state(&mut self, state_id: &str) {
        for key in self.matching_keys(state_id) {
            self.indexes.remove(&key);
        }
    }

    /// Advance the tracked `head_offset` for every index registered for
    /// `state_id` on `branch_id` to `offset`, WITHOUT touching `by_ordinal`
    /// or either reverse index. For an op that moves the chain's head
    /// record but changes no indexed content — currently only
    /// `DeltaSnapshot`, which consolidates Appends already reflected in the
    /// index via their own `on_append` calls. Without this, `prune_stale`
    /// (which compares a loaded index's stored `head_offset` against the
    /// real chain's current one) would treat an otherwise still-correct
    /// index as stale — and discard it — purely because a DeltaSnapshot
    /// happened since it was last touched. A branch mismatch still poisons,
    /// same as every other op.
    pub fn touch_head_offset(&mut self, state_id: &str, branch_id: BranchId, offset: u64) {
        for key in self.matching_keys(state_id) {
            if !self.branch_matches(&key, branch_id) {
                self.indexes.remove(&key);
                continue;
            }
            if let Some(index) = self.indexes.get_mut(&key) {
                index.head_offset = Some(offset);
            }
        }
    }

    /// Incrementally index one appended item, at `offset` (the log offset of
    /// the record this Append was written as) on `branch_id`. Any index for
    /// `state_id` registered on a different branch is poisoned instead of
    /// updated.
    pub fn on_append(&mut self, state_id: &str, branch_id: BranchId, offset: u64, item: &serde_json::Value) {
        for key in self.matching_keys(state_id) {
            if !self.branch_matches(&key, branch_id) {
                self.indexes.remove(&key);
                continue;
            }
            let field_path = key.1.clone();
            let index = self.indexes.get_mut(&key).expect("just matched");
            let value = extract_indexed_value(item, &field_path, index.kind);
            index.push(value);
            index.head_offset = Some(offset);
        }
    }

    /// Incrementally index an Edit at `ordinal` (0-based), at `offset` on
    /// `branch_id`: removes the old value from the reverse index and
    /// inserts the new one, overwriting `by_ordinal[ordinal]`. An
    /// out-of-range ordinal (relative to this index's own `by_ordinal`,
    /// which should always track the slot's length exactly) means the index
    /// has desynced from the slot for some other reason — poisoned rather
    /// than silently left short, same as a branch mismatch.
    pub fn on_edit(
        &mut self,
        state_id: &str,
        branch_id: BranchId,
        offset: u64,
        ordinal: u32,
        new_value: &serde_json::Value,
    ) {
        for key in self.matching_keys(state_id) {
            if !self.branch_matches(&key, branch_id) {
                self.indexes.remove(&key);
                continue;
            }
            let i = ordinal as usize;
            let len = self.indexes.get(&key).expect("just matched").by_ordinal.len();
            if i >= len {
                self.indexes.remove(&key);
                continue;
            }
            let field_path = key.1.clone();
            let index = self.indexes.get_mut(&key).expect("just matched");
            let old_value = index.by_ordinal[i].clone();
            index.remove_from_reverse(ordinal, &old_value);
            let extracted = extract_indexed_value(new_value, &field_path, index.kind);
            index.insert_into_reverse(ordinal, &extracted);
            index.by_ordinal[i] = extracted;
            index.head_offset = Some(offset);
        }
    }

    /// Incrementally index a Redact of `[start, end)`, at `offset` on
    /// `branch_id`: drops those ordinals' entries (both `by_ordinal` and the
    /// reverse index), then shifts every remaining ordinal `>= end` down by
    /// `end - start`, in `by_ordinal` (via `Vec::drain`, which shifts
    /// naturally) AND in every reverse-index ordinal list (which must be
    /// walked and decremented explicitly).
    ///
    /// TODO: the reverse-index shift below is O(total entries in this
    /// index), not O(redacted range) — a large index with a small redact
    /// still pays a full walk of every bucket. Fine at the sizes this has
    /// been exercised at; would want a smarter structure (e.g. a Fenwick/BIT
    /// of shifts, or lazily-applied offsets) if redact-heavy workloads on
    /// multi-million-item indexes show up.
    pub fn on_redact(&mut self, state_id: &str, branch_id: BranchId, offset: u64, start: u32, end: u32) {
        for key in self.matching_keys(state_id) {
            if !self.branch_matches(&key, branch_id) {
                self.indexes.remove(&key);
                continue;
            }
            let index = self.indexes.get_mut(&key).expect("just matched");
            let len = index.by_ordinal.len();
            let start_c = (start as usize).min(len);
            let end_c = (end as usize).min(len);
            if start_c >= end_c {
                index.head_offset = Some(offset);
                continue;
            }
            let shift = (end_c - start_c) as u32;

            let removed: Vec<Option<IndexedValue>> =
                index.by_ordinal.drain(start_c..end_c).collect();
            for (i, value) in removed.into_iter().enumerate() {
                let ordinal = start_c as u32 + i as u32;
                index.remove_from_reverse(ordinal, &value);
            }

            for list in index.number_rev.values_mut() {
                for o in list.iter_mut() {
                    if *o >= end_c as u32 {
                        *o -= shift;
                    }
                }
            }
            for list in index.string_rev.values_mut() {
                for o in list.iter_mut() {
                    if *o >= end_c as u32 {
                        *o -= shift;
                    }
                }
            }
            index.head_offset = Some(offset);
        }
    }

    /// Full rebuild of every field registered for `state_id` (that matches
    /// `branch_id` — others are poisoned), from `items` (the slot's new
    /// complete content) at `offset`. Used for `Set`/`Snapshot` ops
    /// (compaction's snapshot included — ordinals and values are unchanged
    /// by a compacting snapshot, so this is a correctness-preserving
    /// rebuild, not a semantic change).
    pub fn on_full_replace<'a>(
        &mut self,
        state_id: &str,
        branch_id: BranchId,
        offset: u64,
        items: impl Iterator<Item = &'a serde_json::Value>,
    ) {
        let keys = self.matching_keys(state_id);
        if keys.is_empty() {
            return;
        }
        let items: Vec<&serde_json::Value> = items.collect();
        for key in keys {
            if !self.branch_matches(&key, branch_id) {
                self.indexes.remove(&key);
                continue;
            }
            let field_path = key.1.clone();
            let index = self.indexes.get_mut(&key).expect("just matched");
            index.rebuild(&field_path, items.iter().copied());
            index.head_offset = Some(offset);
        }
    }

    /// Ordinals with a `Number` field in `[gte, lte]` (either bound
    /// optional), in ascending (or, if `reverse`, descending) value order.
    /// Returns `None` if no such `Number` index is currently registered for
    /// `branch_id` (never registered, wrong kind, poisoned, OR registered
    /// against a *different* branch — a pure read after `switch_branch`
    /// with no intervening write must never serve another branch's
    /// ordinals) — distinct from `Some(vec![])`, which means the index
    /// exists, matches this branch, and genuinely has no matches in range.
    #[allow(clippy::too_many_arguments)]
    pub fn query_range(
        &self,
        state_id: &str,
        field_path: &str,
        branch_id: BranchId,
        gte: Option<f64>,
        lte: Option<f64>,
        limit: Option<usize>,
        offset: Option<usize>,
        reverse: bool,
    ) -> Option<Vec<u32>> {
        let index = self.indexes.get(&(state_id.to_string(), field_path.to_string()))?;
        if index.kind != FieldIndexKind::Number || index.branch_id != branch_id {
            return None;
        }

        let lo = OrderedF64(normalize_zero(gte.unwrap_or(f64::NEG_INFINITY)));
        let hi = OrderedF64(normalize_zero(lte.unwrap_or(f64::INFINITY)));
        if lo > hi {
            return Some(Vec::new());
        }

        let mut result: Vec<u32> = Vec::new();
        if reverse {
            for (_, ords) in index.number_rev.range(lo..=hi).rev() {
                result.extend(ords.iter().rev().copied());
            }
        } else {
            for (_, ords) in index.number_rev.range(lo..=hi) {
                result.extend(ords.iter().copied());
            }
        }

        Some(apply_offset_limit(result, offset, limit))
    }

    /// Ordinals with a `String` field equal to `value`. Returns `None` if no
    /// such `String` index is currently registered for `branch_id` — see
    /// `query_range`'s doc for the `None` vs `Some(vec![])` distinction and
    /// why `branch_id` is checked here too.
    pub fn query_eq(
        &self,
        state_id: &str,
        field_path: &str,
        branch_id: BranchId,
        value: &str,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Option<Vec<u32>> {
        let index = self.indexes.get(&(state_id.to_string(), field_path.to_string()))?;
        if index.kind != FieldIndexKind::String || index.branch_id != branch_id {
            return None;
        }

        let result = index.string_rev.get(value).cloned().unwrap_or_default();
        Some(apply_offset_limit(result, offset, limit))
    }

    /// Distinct values and their ordinal counts for a registered `String`
    /// field index, sorted by value. O(index size) — no content decoding.
    /// Returns `None` if no such `String` index is currently registered for
    /// `branch_id` (see `query_range`'s doc for why branch is checked here
    /// too).
    pub fn value_counts(
        &self,
        state_id: &str,
        field_path: &str,
        branch_id: BranchId,
    ) -> Option<Vec<(String, u32)>> {
        let index = self.indexes.get(&(state_id.to_string(), field_path.to_string()))?;
        if index.kind != FieldIndexKind::String || index.branch_id != branch_id {
            return None;
        }

        let mut counts: Vec<(String, u32)> = index
            .string_rev
            .iter()
            .map(|(value, ords)| (value.clone(), ords.len() as u32))
            .collect();
        counts.sort_by(|a, b| a.0.cmp(&b.0));
        Some(counts)
    }

    /// Persist to `path` as `SFI\0` + version byte + `rmp_serde` body,
    /// mirroring `state.bin`'s save format.
    ///
    /// TODO: `register`'s `current_items` materializes the whole slot into a
    /// `Vec<&Value>` before indexing it. Fine at the sizes exercised so far;
    /// a streaming/chunked build would be worth doing before this is used on
    /// multi-GB slots.
    pub fn save(&self, path: &Path) -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        file.write_all(FIELD_INDEX_MAGIC)?;
        file.write_all(&[FIELD_INDEX_VERSION])?;

        let encoded =
            rmp_serde::to_vec(self).map_err(|e| StoreError::Serialization(e.to_string()))?;
        file.write_all(&(encoded.len() as u64).to_le_bytes())?;
        file.write_all(&encoded)?;

        file.sync_all()?;
        Ok(())
    }

    /// Load from `path`. Unlike `state.bin`, a missing file, a version
    /// mismatch, or any parse failure is NOT an error — it returns `None`
    /// and the caller starts with an empty `FieldIndexManager` (indexes are
    /// derived data; the source of truth is the state chain itself, so
    /// losing a stale/corrupt index file is safe, unlike losing `state.bin`
    /// which would strand chain-head offsets).
    pub fn load(path: &Path) -> Result<Option<Self>> {
        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(_) => return Ok(None),
        };

        let mut magic = [0u8; 4];
        if file.read_exact(&mut magic).is_err() || &magic != FIELD_INDEX_MAGIC {
            return Ok(None);
        }

        let mut version = [0u8; 1];
        if file.read_exact(&mut version).is_err() || version[0] != FIELD_INDEX_VERSION {
            return Ok(None);
        }

        let mut len_bytes = [0u8; 8];
        if file.read_exact(&mut len_bytes).is_err() {
            return Ok(None);
        }
        let len = u64::from_le_bytes(len_bytes);

        // Validate the persisted length against the file's actual remaining
        // bytes BEFORE allocating anything based on it. A corrupt, torn, or
        // adversarially-crafted file (valid magic+version, garbage length —
        // e.g. `u64::MAX`) must fail exactly like any other corrupt-file
        // case here (`Ok(None)`), not panic. `vec![0u8; len]` on an
        // unvalidated `len` is a capacity-overflow / OOM panic waiting to
        // happen, and this file is read during `Store::open` — a panic here
        // would take down the whole store open, not just this derived,
        // supposed-to-be-safely-discardable index.
        let file_len = match file.metadata() {
            Ok(m) => m.len(),
            Err(_) => return Ok(None),
        };
        // Bytes already consumed: magic (4) + version (1) + len field (8).
        let already_read: u64 = 4 + 1 + 8;
        let remaining = file_len.saturating_sub(already_read);
        if len > remaining {
            return Ok(None);
        }
        // `len <= remaining <= file_len`, so this cast and allocation are
        // bounded by the real file size on disk, not attacker-controlled.
        let len = len as usize;

        let mut encoded = vec![0u8; len];
        if file.read_exact(&mut encoded).is_err() {
            return Ok(None);
        }

        match rmp_serde::from_slice::<Self>(&encoded) {
            Ok(manager) => Ok(Some(manager)),
            Err(_) => Ok(None),
        }
    }

    /// Drop every index whose stored `(branch_id, head_offset)` doesn't
    /// exactly match what `current_head_offset(state_id, branch_id)`
    /// reports right now. `load()` only rejects a file that fails to parse
    /// — it has no way to know whether the *content* it successfully parsed
    /// is still current, since `state.bin` and `state-indexes.bin` are
    /// separate, non-atomically-written files (`save()` writes `state.bin`
    /// first and swallows a later field-index save failure). A crash in
    /// that window, or a structurally-valid-but-older `state-indexes.bin`
    /// restored from a backup, parses fine but is stale relative to the
    /// real chain — this is the check that must run before a freshly loaded
    /// manager is trusted, closing the gap `load()` alone leaves open.
    ///
    /// Call this once right after `load()` succeeds, passing a closure that
    /// reads the just-reconstructed `StateIndex`'s current head_offset for
    /// `(state_id, branch_id)` (`None` for a slot with no head yet).
    pub fn prune_stale<F>(&mut self, mut current_head_offset: F)
    where
        F: FnMut(&str, BranchId) -> Option<u64>,
    {
        let stale_keys: Vec<(String, String)> = self
            .indexes
            .iter()
            .filter(|((state_id, _), index)| {
                current_head_offset(state_id, index.branch_id) != index.head_offset
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in stale_keys {
            self.indexes.remove(&key);
        }
    }
}

fn apply_offset_limit(values: Vec<u32>, offset: Option<usize>, limit: Option<usize>) -> Vec<u32> {
    let start = offset.unwrap_or(0).min(values.len());
    let end = match limit {
        Some(l) => start.saturating_add(l).min(values.len()),
        None => values.len(),
    };
    values[start..end].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    const MAIN: BranchId = BranchId(1);
    const SIDE: BranchId = BranchId(2);

    fn items(values: &[serde_json::Value]) -> Vec<serde_json::Value> {
        values.to_vec()
    }

    #[test]
    fn test_register_and_query_range() {
        let mut mgr = FieldIndexManager::new();
        let data = items(&[
            json!({"timestamp": 10, "text": "a"}),
            json!({"timestamp": 20, "text": "b"}),
            json!({"timestamp": 30, "text": "c"}),
        ]);
        mgr.register("messages", "/timestamp", FieldIndexKind::Number, MAIN, Some(300), data.iter())
            .unwrap();

        let result = mgr
            .query_range("messages", "/timestamp", MAIN, Some(15.0), Some(30.0), None, None, false)
            .unwrap();
        assert_eq!(result, vec![1, 2]);

        let result_rev = mgr
            .query_range("messages", "/timestamp", MAIN, Some(15.0), Some(30.0), None, None, true)
            .unwrap();
        assert_eq!(result_rev, vec![2, 1]);
    }

    #[test]
    fn test_query_unregistered_returns_none() {
        let mgr = FieldIndexManager::new();
        assert_eq!(mgr.query_range("s", "/v", MAIN, None, None, None, None, false), None);
        assert_eq!(mgr.query_eq("s", "/v", MAIN, "x", None, None), None);
        assert_eq!(mgr.value_counts("s", "/v", MAIN), None);
    }

    #[test]
    fn test_query_wrong_kind_returns_none() {
        let mut mgr = FieldIndexManager::new();
        let data = items(&[json!({"v": "a"})]);
        mgr.register("s", "/v", FieldIndexKind::String, MAIN, Some(10), data.iter())
            .unwrap();

        // Registered as String; a Number-shaped query must say "no such
        // index" rather than silently reading nothing.
        assert_eq!(mgr.query_range("s", "/v", MAIN, None, None, None, None, false), None);
        // The right kind returns Some(...), even if empty.
        assert!(mgr.query_eq("s", "/v", MAIN, "nope", None, None).is_some());
    }

    #[test]
    fn test_query_wrong_branch_returns_none() {
        // Regression for the read-after-switch bug: a pure query on a
        // branch other than the one the index was built against must say
        // "no such index" — never serve the other branch's ordinals, and
        // never require a write to have happened on the querying branch
        // first (the old write-time-only poison missed exactly this case).
        let mut mgr = FieldIndexManager::new();
        let data = items(&[json!({"v": 1}), json!({"v": 2}), json!({"v": 3})]);
        mgr.register("messages", "/v", FieldIndexKind::Number, MAIN, Some(30), data.iter())
            .unwrap();

        // Registered on MAIN: querying MAIN works.
        assert_eq!(
            mgr.query_range("messages", "/v", MAIN, None, None, None, None, false).unwrap(),
            vec![0, 1, 2]
        );
        assert_eq!(
            mgr.value_counts("messages", "/v", MAIN),
            None,
            "wrong kind (Number index queried as String) already returns None regardless of branch"
        );

        // Querying SIDE — a pure read, no write ever happened on SIDE —
        // must return None, not MAIN's ordinals.
        assert_eq!(
            mgr.query_range("messages", "/v", SIDE, None, None, None, None, false),
            None,
            "querying a different branch than the index was built on must never serve its ordinals"
        );
    }

    #[test]
    fn test_register_idempotent_on_matching_head_offset() {
        let mut mgr = FieldIndexManager::new();
        let data = items(&[json!({"v": 1}), json!({"v": 2})]);
        mgr.register("s", "/v", FieldIndexKind::Number, MAIN, Some(100), data.iter())
            .unwrap();

        // Re-register with the same branch + head_offset: no-op.
        mgr.register("s", "/v", FieldIndexKind::Number, MAIN, Some(100), data.iter())
            .unwrap();
        let result = mgr.query_range("s", "/v", MAIN, None, None, None, None, false).unwrap();
        assert_eq!(result, vec![0, 1]);

        // Different head_offset (even same length): rebuilds.
        let data2 = items(&[json!({"v": 5}), json!({"v": 6})]);
        mgr.register("s", "/v", FieldIndexKind::Number, MAIN, Some(200), data2.iter())
            .unwrap();
        let result = mgr.query_range("s", "/v", MAIN, Some(6.0), None, None, None, false).unwrap();
        assert_eq!(result, vec![1]);
    }

    #[test]
    fn test_on_append() {
        let mut mgr = FieldIndexManager::new();
        mgr.register("s", "/channelId", FieldIndexKind::String, MAIN, None, std::iter::empty())
            .unwrap();

        mgr.on_append("s", MAIN, 10, &json!({"channelId": "c1"}));
        mgr.on_append("s", MAIN, 20, &json!({"channelId": "c2"}));
        mgr.on_append("s", MAIN, 30, &json!({"channelId": "c1"}));

        let mut result = mgr.query_eq("s", "/channelId", MAIN, "c1", None, None).unwrap();
        result.sort();
        assert_eq!(result, vec![0, 2]);
        assert_eq!(mgr.query_eq("s", "/channelId", MAIN, "c2", None, None).unwrap(), vec![1]);
    }

    #[test]
    fn test_delta_snapshot_is_a_field_index_noop() {
        // Regression for the double-index bug: a DeltaSnapshot consolidates
        // items ALREADY indexed via prior Appends — the field index must
        // never see it as new content. FieldIndexManager itself has no
        // `on_delta_snapshot` method at all (StateManager::record_update
        // simply does not call into the field index for that operation) —
        // this test documents that contract by asserting a bare `on_append`
        // sequence (the only path that legitimately grows the index) stays
        // exactly 1:1 with the number of Appends, with no hidden
        // amplification hook to trip.
        let mut mgr = FieldIndexManager::new();
        mgr.register("s", "/v", FieldIndexKind::Number, MAIN, None, std::iter::empty())
            .unwrap();
        for i in 0..10 {
            mgr.on_append("s", MAIN, i, &json!({"v": i}));
        }
        let all = mgr.query_range("s", "/v", MAIN, None, None, None, None, false).unwrap();
        assert_eq!(all.len(), 10, "10 appends must yield exactly 10 indexed ordinals");
    }

    #[test]
    fn test_on_edit_updates_old_and_new() {
        let mut mgr = FieldIndexManager::new();
        let data = items(&[json!({"v": "a"}), json!({"v": "b"}), json!({"v": "a"})]);
        mgr.register("s", "/v", FieldIndexKind::String, MAIN, Some(1), data.iter())
            .unwrap();

        assert_eq!(mgr.query_eq("s", "/v", MAIN, "a", None, None).unwrap(), vec![0, 2]);

        // Edit ordinal 0 from "a" to "c".
        mgr.on_edit("s", MAIN, 2, 0, &json!({"v": "c"}));

        // Old value "a" no longer includes ordinal 0.
        assert_eq!(mgr.query_eq("s", "/v", MAIN, "a", None, None).unwrap(), vec![2]);
        // New value "c" includes ordinal 0.
        assert_eq!(mgr.query_eq("s", "/v", MAIN, "c", None, None).unwrap(), vec![0]);
        // "b" unaffected.
        assert_eq!(mgr.query_eq("s", "/v", MAIN, "b", None, None).unwrap(), vec![1]);
    }

    #[test]
    fn test_on_edit_out_of_range_poisons() {
        let mut mgr = FieldIndexManager::new();
        let data = items(&[json!({"v": "a"})]);
        mgr.register("s", "/v", FieldIndexKind::String, MAIN, Some(1), data.iter())
            .unwrap();

        mgr.on_edit("s", MAIN, 2, 5, &json!({"v": "z"})); // ordinal 5 doesn't exist
        assert_eq!(
            mgr.query_eq("s", "/v", MAIN, "a", None, None),
            None,
            "desynced index must be poisoned, not left stale"
        );
    }

    #[test]
    fn test_on_redact_shifts_ordinals() {
        let mut mgr = FieldIndexManager::new();
        let data = items(&[
            json!({"v": "a"}),
            json!({"v": "b"}),
            json!({"v": "c"}),
            json!({"v": "d"}),
            json!({"v": "e"}),
        ]);
        mgr.register("s", "/v", FieldIndexKind::String, MAIN, Some(1), data.iter())
            .unwrap();

        // Redact [1, 3) removes "b" and "c"; "d" (was 3) -> 1, "e" (was 4) -> 2.
        mgr.on_redact("s", MAIN, 2, 1, 3);

        assert_eq!(mgr.query_eq("s", "/v", MAIN, "b", None, None).unwrap(), Vec::<u32>::new());
        assert_eq!(mgr.query_eq("s", "/v", MAIN, "c", None, None).unwrap(), Vec::<u32>::new());
        assert_eq!(mgr.query_eq("s", "/v", MAIN, "a", None, None).unwrap(), vec![0]);
        assert_eq!(mgr.query_eq("s", "/v", MAIN, "d", None, None).unwrap(), vec![1]);
        assert_eq!(mgr.query_eq("s", "/v", MAIN, "e", None, None).unwrap(), vec![2]);
    }

    #[test]
    fn test_on_redact_shifts_number_reverse_index() {
        let mut mgr = FieldIndexManager::new();
        let data = items(&[
            json!({"v": 100}),
            json!({"v": 200}),
            json!({"v": 300}),
            json!({"v": 400}),
        ]);
        mgr.register("s", "/v", FieldIndexKind::Number, MAIN, Some(1), data.iter())
            .unwrap();

        mgr.on_redact("s", MAIN, 2, 0, 2);

        // Only "300" (now ordinal 0) and "400" (now ordinal 1) remain.
        assert_eq!(
            mgr.query_range("s", "/v", MAIN, None, None, None, None, false).unwrap(),
            vec![0, 1]
        );
        assert_eq!(
            mgr.query_range("s", "/v", MAIN, Some(300.0), Some(300.0), None, None, false).unwrap(),
            vec![0]
        );
        assert_eq!(
            mgr.query_range("s", "/v", MAIN, Some(400.0), Some(400.0), None, None, false).unwrap(),
            vec![1]
        );
    }

    #[test]
    fn test_on_full_replace_rebuilds() {
        let mut mgr = FieldIndexManager::new();
        let data = items(&[json!({"v": 1}), json!({"v": 2})]);
        mgr.register("s", "/v", FieldIndexKind::Number, MAIN, Some(1), data.iter())
            .unwrap();

        let new_data = items(&[json!({"v": 10}), json!({"v": 20}), json!({"v": 30})]);
        mgr.on_full_replace("s", MAIN, 2, new_data.iter());

        let result = mgr.query_range("s", "/v", MAIN, Some(15.0), None, None, None, false).unwrap();
        assert_eq!(result, vec![1, 2]);
        assert_eq!(
            mgr.query_range("s", "/v", MAIN, None, None, None, None, false).unwrap().len(),
            3
        );
    }

    #[test]
    fn test_value_counts() {
        let mut mgr = FieldIndexManager::new();
        let data = items(&[
            json!({"v": "x"}),
            json!({"v": "y"}),
            json!({"v": "x"}),
            json!({"v": "x"}),
        ]);
        mgr.register("s", "/v", FieldIndexKind::String, MAIN, Some(1), data.iter())
            .unwrap();

        let counts = mgr.value_counts("s", "/v", MAIN).unwrap();
        assert_eq!(counts, vec![("x".to_string(), 3), ("y".to_string(), 1)]);
    }

    #[test]
    fn test_cross_branch_write_poisons_index() {
        // Regression for the branch-corruption bug: an index registered on
        // MAIN must be poisoned (not silently updated) by an op on SIDE for
        // the same state_id.
        let mut mgr = FieldIndexManager::new();
        let data = items(&[json!({"v": 1}), json!({"v": 2}), json!({"v": 3})]);
        mgr.register("messages", "/v", FieldIndexKind::Number, MAIN, Some(30), data.iter())
            .unwrap();
        assert!(mgr.query_range("messages", "/v", MAIN, None, None, None, None, false).is_some());

        // A write on a different branch for the same state_id.
        mgr.on_append("messages", SIDE, 40, &json!({"v": 4}));

        // MAIN's index is gone, not silently extended with SIDE's item.
        assert_eq!(
            mgr.query_range("messages", "/v", MAIN, None, None, None, None, false),
            None,
            "cross-branch write must poison, never contaminate, the other branch's index"
        );
    }

    #[test]
    fn test_parse_failure_poisons_via_manager_level_helper() {
        // FieldIndexManager itself never parses raw bytes (that happens in
        // StateManager::record_update, which calls `poison_state` on parse
        // failure) — this test exercises `poison_state` directly as the
        // mechanism that call site relies on.
        let mut mgr = FieldIndexManager::new();
        let data = items(&[json!({"v": 1})]);
        mgr.register("s", "/v", FieldIndexKind::Number, MAIN, Some(1), data.iter())
            .unwrap();
        assert!(mgr.query_range("s", "/v", MAIN, None, None, None, None, false).is_some());

        mgr.poison_state("s");
        assert_eq!(mgr.query_range("s", "/v", MAIN, None, None, None, None, false), None);
    }

    #[test]
    fn test_persistence_round_trip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state-indexes.bin");

        let mut mgr = FieldIndexManager::new();
        let data = items(&[
            json!({"timestamp": 1, "channel": "a"}),
            json!({"timestamp": 2, "channel": "b"}),
            json!({"timestamp": 3, "channel": "a"}),
        ]);
        mgr.register("messages", "/timestamp", FieldIndexKind::Number, MAIN, Some(300), data.iter())
            .unwrap();
        mgr.register("messages", "/channel", FieldIndexKind::String, MAIN, Some(300), data.iter())
            .unwrap();
        mgr.save(&path).unwrap();

        let loaded = FieldIndexManager::load(&path).unwrap().unwrap();
        assert_eq!(
            loaded
                .query_range("messages", "/timestamp", MAIN, Some(2.0), None, None, None, false)
                .unwrap(),
            vec![1, 2]
        );
        assert_eq!(
            loaded.query_eq("messages", "/channel", MAIN, "a", None, None).unwrap(),
            vec![0, 2]
        );
    }

    #[test]
    fn test_load_missing_file_returns_none() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("does-not-exist.bin");
        assert!(FieldIndexManager::load(&path).unwrap().is_none());
    }

    #[test]
    fn test_load_stale_version_returns_none() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state-indexes.bin");
        let mut file = File::create(&path).unwrap();
        file.write_all(FIELD_INDEX_MAGIC).unwrap();
        file.write_all(&[255u8]).unwrap(); // bogus version
        file.write_all(&0u64.to_le_bytes()).unwrap();
        drop(file);

        assert!(FieldIndexManager::load(&path).unwrap().is_none());
    }

    #[test]
    fn test_load_garbage_returns_none() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("garbage.bin");
        std::fs::write(&path, b"not a chronicle field index file at all").unwrap();
        assert!(FieldIndexManager::load(&path).unwrap().is_none());
    }

    #[test]
    fn test_load_oversized_length_field_does_not_panic() {
        // Regression: valid magic + valid version + a length field that
        // claims far more bytes than the file actually has (here:
        // u64::MAX, the adversarial/corrupt extreme) must fail exactly
        // like any other corrupt-file case (`Ok(None)`) — not panic via an
        // unvalidated `vec![0u8; len]` capacity overflow. This file is read
        // during `Store::open`, so a panic here would take down the whole
        // store open over a derived, supposed-to-be-safely-discardable
        // index file.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state-indexes.bin");
        let mut file = File::create(&path).unwrap();
        file.write_all(FIELD_INDEX_MAGIC).unwrap();
        file.write_all(&[FIELD_INDEX_VERSION]).unwrap();
        file.write_all(&u64::MAX.to_le_bytes()).unwrap(); // wildly oversized length
        drop(file);

        assert!(FieldIndexManager::load(&path).unwrap().is_none());
    }

    #[test]
    fn test_load_length_field_exceeding_remaining_bytes_does_not_panic() {
        // Less extreme than u64::MAX: a length that's merely larger than
        // what's actually left in the file (e.g. a torn/truncated write)
        // must also be rejected before allocating, not panic or read past
        // EOF into whatever `read_exact` happens to do.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state-indexes.bin");
        let mut file = File::create(&path).unwrap();
        file.write_all(FIELD_INDEX_MAGIC).unwrap();
        file.write_all(&[FIELD_INDEX_VERSION]).unwrap();
        file.write_all(&100u64.to_le_bytes()).unwrap(); // claims 100 bytes follow
        file.write_all(b"only ten!!").unwrap(); // but only 10 are actually there
        drop(file);

        assert!(FieldIndexManager::load(&path).unwrap().is_none());
    }

    #[test]
    fn test_negative_zero_matches_positive_zero_in_range_query() {
        // Regression: `total_cmp` (which orders the reverse-index BTreeMap)
        // distinguishes -0.0 from 0.0, unlike ordinary numeric equality —
        // without normalization, an item indexed at -0.0 would sort just
        // below 0.0 and an inclusive [0, 0] query would silently miss it.
        let mut mgr = FieldIndexManager::new();
        let data = items(&[json!({"v": -0.0}), json!({"v": 0.0}), json!({"v": 1.0})]);
        mgr.register("s", "/v", FieldIndexKind::Number, MAIN, Some(1), data.iter())
            .unwrap();

        let zero_range = mgr
            .query_range("s", "/v", MAIN, Some(0.0), Some(0.0), None, None, false)
            .unwrap();
        assert_eq!(
            zero_range,
            vec![0, 1],
            "both the -0.0 and 0.0 items must match an inclusive [0, 0] query"
        );

        // A query bound that's itself -0.0 must behave identically.
        let neg_zero_bound = mgr
            .query_range("s", "/v", MAIN, Some(-0.0), Some(-0.0), None, None, false)
            .unwrap();
        assert_eq!(neg_zero_bound, vec![0, 1]);
    }

    #[test]
    fn test_prune_stale_drops_mismatched_head_offset() {
        // Regression for the load-time-freshness bug: a structurally-valid
        // (parses fine) but stale persisted index — its stored head_offset
        // no longer matches what the real chain reports for that
        // (state_id, branch_id) — must be dropped, not served.
        let mut mgr = FieldIndexManager::new();
        let data = items(&[json!({"v": 1})]);
        mgr.register("messages", "/v", FieldIndexKind::Number, MAIN, Some(100), data.iter())
            .unwrap();
        // A second, still-fresh index on a different state_id must survive
        // pruning untouched — prune_stale only removes what's actually stale.
        mgr.register("other", "/v", FieldIndexKind::Number, MAIN, Some(50), data.iter())
            .unwrap();

        // Simulate the real chain having advanced past what this index
        // reflects: "messages" is now at head_offset 200, not 100; "other"
        // is unchanged at 50.
        mgr.prune_stale(|state_id, _branch| match state_id {
            "messages" => Some(200),
            "other" => Some(50),
            _ => None,
        });

        assert_eq!(
            mgr.query_range("messages", "/v", MAIN, None, None, None, None, false),
            None,
            "stale index (head_offset mismatch) must be dropped, not served as current"
        );
        assert!(
            mgr.query_range("other", "/v", MAIN, None, None, None, None, false).is_some(),
            "an index that IS still fresh must survive prune_stale"
        );
    }

    #[test]
    fn test_prune_stale_keeps_matching_empty_head() {
        // An index registered against an empty slot (head_offset = None)
        // stays fresh as long as the slot is STILL empty (current head is
        // also None) — None == None must count as fresh, not stale.
        let mut mgr = FieldIndexManager::new();
        mgr.register("s", "/v", FieldIndexKind::Number, MAIN, None, std::iter::empty())
            .unwrap();

        mgr.prune_stale(|_, _| None);
        assert!(mgr.query_range("s", "/v", MAIN, None, None, None, None, false).is_some());

        // But once the slot has actually been written to (current head is
        // now Some), the None-headed registration is stale.
        mgr.prune_stale(|_, _| Some(1));
        assert_eq!(mgr.query_range("s", "/v", MAIN, None, None, None, None, false), None);
    }

    #[test]
    fn test_limit_offset() {
        let mut mgr = FieldIndexManager::new();
        let data = items(&(0..10).map(|i| json!({"v": i})).collect::<Vec<_>>());
        mgr.register("s", "/v", FieldIndexKind::Number, MAIN, Some(1), data.iter())
            .unwrap();

        let page = mgr
            .query_range("s", "/v", MAIN, None, None, Some(3), Some(2), false)
            .unwrap();
        assert_eq!(page, vec![2, 3, 4]);
    }

    #[test]
    fn test_no_indexes_registered_is_noop() {
        let mut mgr = FieldIndexManager::new();
        // Nothing registered for "s" — these must not panic.
        mgr.on_append("s", MAIN, 1, &json!({"v": 1}));
        mgr.on_edit("s", MAIN, 2, 0, &json!({"v": 2}));
        mgr.on_redact("s", MAIN, 3, 0, 1);
        assert_eq!(mgr.query_range("s", "/v", MAIN, None, None, None, None, false), None);
    }
}
