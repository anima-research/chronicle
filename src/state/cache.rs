//! Materialized-state caches shared across branches.
//!
//! A state's materialized value is a pure function of its chain head: the
//! record at `head_offset` and everything behind it never change. Two
//! branches whose head for a state sits at the same log offset therefore
//! hold the same value — which is exactly the situation right after a fork,
//! and for as long as neither side writes to that state.
//!
//! [`SharedCache`] keeps the per-`(branch, state)` LRU the state manager
//! always had (one entry per key, replaced when the key's head moves, so a
//! superseded value is released as soon as nobody else holds it) and adds an
//! index of the live values by `head_offset`. A branch that misses on its own
//! key but finds a live value at its head offset takes a reference to it
//! instead of walking the log again: no re-materialization and no copy.
//!
//! [`StateItems`] is the per-item form of an array-shaped state. It is a
//! persistent vector (fixed-size chunks of reference-counted items), so a
//! branch that appends to, edits or redacts an inherited array derives its
//! new value from the cached one by copying a few pointers, and the two
//! versions keep sharing every item and every untouched chunk.

use lru::LruCache;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock, Weak};

struct Entry<T> {
    value: Arc<T>,
    head_offset: u64, // To detect staleness
}

/// An LRU of materialized values keyed by `(branch, state)`, with sharing of
/// equal values (same `head_offset`) across keys.
pub(super) struct SharedCache<T> {
    entries: LruCache<String, Entry<T>>,
    /// Live values by the chain head offset they materialize. Weak: an entry
    /// here never keeps a value alive, it only lets another key find a value
    /// some LRU entry (or reader) still holds.
    by_offset: HashMap<u64, Weak<T>>,
}

impl<T> SharedCache<T> {
    pub(super) fn new(capacity: NonZeroUsize) -> Self {
        Self {
            entries: LruCache::new(capacity),
            by_offset: HashMap::new(),
        }
    }

    /// The value for `key` at `head_offset`: `key`'s own entry if it is at
    /// that offset, otherwise a value another key holds at the same offset
    /// (which `key` then references too). `None` means nothing live
    /// materializes that offset.
    pub(super) fn get(&mut self, key: &str, head_offset: u64) -> Option<Arc<T>> {
        if let Some(entry) = self.entries.get(key) {
            if entry.head_offset == head_offset {
                return Some(Arc::clone(&entry.value));
            }
            // Stale entry; replaced below if another key has the value.
        }
        let value = self.live_at(head_offset)?;
        self.entries.put(
            key.to_string(),
            Entry {
                value: Arc::clone(&value),
                head_offset,
            },
        );
        Some(value)
    }

    /// A live value at `head_offset`, if any key holds one.
    pub(super) fn live_at(&mut self, head_offset: u64) -> Option<Arc<T>> {
        match self.by_offset.get(&head_offset).map(Weak::upgrade) {
            Some(Some(value)) => Some(value),
            Some(None) => {
                self.by_offset.remove(&head_offset);
                None
            }
            None => None,
        }
    }

    /// Whether `key` has an entry at `head_offset`, or could take one from
    /// another key, without touching LRU order or creating an entry.
    pub(super) fn peek(&self, key: &str, head_offset: u64) -> Option<Arc<T>> {
        if let Some(entry) = self.entries.peek(key) {
            if entry.head_offset == head_offset {
                return Some(Arc::clone(&entry.value));
            }
        }
        self.by_offset.get(&head_offset).and_then(Weak::upgrade)
    }

    /// Store `value` as `key`'s entry at `head_offset`.
    pub(super) fn put(&mut self, key: String, head_offset: u64, value: Arc<T>) {
        self.by_offset.insert(head_offset, Arc::downgrade(&value));
        self.entries.put(key, Entry { value, head_offset });
        self.prune();
    }

    /// Make `key` reference the live value at `head_offset`, if there is one.
    /// Used when a branch is given a head another branch already has cached.
    pub(super) fn adopt(&mut self, key: String, head_offset: u64) -> bool {
        match self.live_at(head_offset) {
            Some(value) => {
                self.entries.put(key, Entry { value, head_offset });
                true
            }
            None => false,
        }
    }

    /// Remove `key`'s entry.
    pub(super) fn pop(&mut self, key: &str) {
        self.entries.pop(key);
    }

    /// Remove and return the value `key` should derive its next value from
    /// when its head moves on from `head_offset`: its own entry if that is
    /// where it was, otherwise a value another key holds there. `key`'s own
    /// entry is removed either way (it is about to be superseded), which
    /// also leaves the returned `Arc` the only strong reference whenever no
    /// other key or reader holds the value — `Arc::make_mut` then updates it
    /// without cloning.
    pub(super) fn take(&mut self, key: &str, head_offset: u64) -> Option<Arc<T>> {
        match self.entries.pop(key) {
            Some(entry) if entry.head_offset == head_offset => Some(entry.value),
            _ => self.live_at(head_offset),
        }
    }

    /// `key`'s head moved from `from_offset` to `to_offset` without the
    /// value changing: keep the cached value, now filed at the new offset.
    /// If neither `key` nor anyone else has the value, `key` ends up with no
    /// entry.
    pub(super) fn rekey(&mut self, key: &str, from_offset: u64, to_offset: u64) {
        if let Some(value) = self.take(key, from_offset) {
            self.put(key.to_string(), to_offset, value);
        }
    }

    /// Remove every entry whose key starts with `prefix`.
    pub(super) fn pop_prefix(&mut self, prefix: &str) {
        let keys: Vec<String> = self
            .entries
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            self.entries.pop(&key);
        }
    }

    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn clear(&mut self) {
        self.entries.clear();
        self.by_offset.clear();
    }

    /// Forget offsets whose value is gone. Amortized: runs when the offset
    /// index has grown to twice the number of entries that could be alive.
    fn prune(&mut self) {
        if self.by_offset.len() > 2 * self.entries.len() + 16 {
            self.by_offset.retain(|_, value| value.strong_count() > 0);
        }
    }
}

/// Materialized bytes of a state.
pub(super) struct StateBytes {
    pub(super) data: Vec<u8>,
    /// Whether `data` is known to be this store's own serialization of the
    /// decoded value (what `serde_json` emits for the parsed items), as
    /// opposed to bytes a caller supplied in a `Set`/`Snapshot` that are
    /// returned verbatim. The next value of an array may only be derived
    /// from canonical items: re-materializing after a write re-serializes
    /// every item, so deriving from items in any other spelling would differ
    /// from it byte for byte (same JSON, different bytes).
    pub(super) canonical: bool,
}

/// Number of items per chunk of a [`StateItems`]. Deriving a new version
/// copies at most one chunk of pointers per touched position plus one pointer
/// per chunk, so this bounds the per-write cost (about 16 KiB) while keeping
/// the chunk list short (one pointer per 1024 items).
const CHUNK: usize = 1024;

/// The items of an array-shaped (AppendLog) state, each as its raw JSON
/// bytes.
///
/// Returned by [`StateManager::get_state_items`](super::StateManager::get_state_items)
/// behind an `Arc`. Indexing and iteration yield `&[u8]`.
///
/// Internally a persistent vector: items are reference-counted and grouped
/// in chunks that are themselves reference-counted, so the version of an
/// array after an append/edit/redact shares all untouched items and chunks
/// with the version before it — and with every other branch holding either.
#[derive(Clone, Debug, Default)]
pub struct StateItems {
    /// Every chunk but the last holds exactly `CHUNK` items; the last holds
    /// `1..=CHUNK`. No empty chunks.
    chunks: Vec<Arc<Vec<Arc<[u8]>>>>,
    len: usize,
    /// See [`StateBytes::canonical`].
    canonical: bool,
    /// The canonical form of a value that is not known to be canonical,
    /// computed on first need (see `into_canonical`) and then shared by
    /// everyone holding this value. Always empty on a canonical value.
    canonical_form: OnceLock<Option<Arc<StateItems>>>,
}

impl StateItems {
    pub(super) fn from_items<'a>(items: impl Iterator<Item = &'a [u8]>, canonical: bool) -> Self {
        let mut out = StateItems {
            chunks: Vec::new(),
            len: 0,
            canonical,
            canonical_form: OnceLock::new(),
        };
        for item in items {
            out.push(Arc::from(item));
        }
        out
    }

    /// Number of items.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether there are no items.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Raw JSON bytes of the item at `index`.
    pub fn get(&self, index: usize) -> Option<&[u8]> {
        if index >= self.len {
            return None;
        }
        Some(&self.chunks[index / CHUNK][index % CHUNK])
    }

    /// All items in order.
    pub fn iter(&self) -> impl Iterator<Item = &[u8]> + '_ {
        self.chunks
            .iter()
            .flat_map(|chunk| chunk.iter().map(|item| &**item))
    }

    /// The items in `start..end` (clamped to the length), in order.
    pub fn range(&self, start: usize, end: usize) -> impl Iterator<Item = &[u8]> + '_ {
        let end = end.min(self.len);
        let start = start.min(end);
        (start..end).map(move |i| &*self.chunks[i / CHUNK][i % CHUNK])
    }

    pub(super) fn is_canonical(&self) -> bool {
        self.canonical
    }

    /// This value with every item in canonical form — the form a
    /// re-materialization after the next write would give each item — as the
    /// base to derive that next value from. `None` if an item is not valid
    /// JSON (materialization would fail too).
    ///
    /// A value already known canonical is returned as is. Otherwise each
    /// item is decoded and re-serialized once, and the result is remembered
    /// on this value, so every branch deriving from the same inherited value
    /// shares one canonical base instead of building its own. Items (and
    /// whole chunks) whose bytes were canonical all along are reused, not
    /// copied: for a value that merely could not be PROVEN canonical — a
    /// snapshot this store wrote of its own serialization, read back
    /// verbatim — the canonical form costs a pass over the items and no
    /// additional memory.
    pub(super) fn into_canonical(self: Arc<Self>) -> Option<Arc<StateItems>> {
        if self.canonical {
            return Some(self);
        }
        self.canonical_form
            .get_or_init(|| self.canonicalize().map(Arc::new))
            .clone()
    }

    fn canonicalize(&self) -> Option<StateItems> {
        let mut chunks = Vec::with_capacity(self.chunks.len());
        for chunk in &self.chunks {
            let mut unchanged = true;
            let mut items = Vec::with_capacity(chunk.len());
            for item in chunk.iter() {
                let value: serde_json::Value = serde_json::from_slice(item).ok()?;
                let bytes = serde_json::to_vec(&value).ok()?;
                if bytes.as_slice() == &**item {
                    items.push(Arc::clone(item));
                } else {
                    unchanged = false;
                    items.push(Arc::from(bytes));
                }
            }
            chunks.push(if unchanged {
                Arc::clone(chunk)
            } else {
                Arc::new(items)
            });
        }
        Some(StateItems {
            chunks,
            len: self.len,
            canonical: true,
            canonical_form: OnceLock::new(),
        })
    }

    fn pointers(&self, start: usize) -> impl Iterator<Item = &Arc<[u8]>> + '_ {
        (start..self.len).map(move |i| &self.chunks[i / CHUNK][i % CHUNK])
    }

    pub(super) fn push(&mut self, item: Arc<[u8]>) {
        match self.chunks.last_mut() {
            Some(last) if last.len() < CHUNK => Arc::make_mut(last).push(item),
            _ => {
                let mut chunk = Vec::with_capacity(16);
                chunk.push(item);
                self.chunks.push(Arc::new(chunk));
            }
        }
        self.len += 1;
    }

    /// Replace the item at `index`. Returns `false` if out of range.
    pub(super) fn set(&mut self, index: usize, item: Arc<[u8]>) -> bool {
        if index >= self.len {
            return false;
        }
        Arc::make_mut(&mut self.chunks[index / CHUNK])[index % CHUNK] = item;
        true
    }

    /// Remove `start..end`, clamped to the length exactly as a `Redact`
    /// operation is when a state is materialized.
    pub(super) fn remove_range(&mut self, start: usize, end: usize) {
        let start = start.min(self.len);
        let end = end.min(self.len);
        if start >= end {
            return;
        }
        // Chunks wholly before `start` are kept (and stay shared); the rest
        // is re-chunked from the surviving item pointers.
        let keep_chunks = start / CHUNK;
        let kept = keep_chunks * CHUNK;
        let tail: Vec<Arc<[u8]>> = self
            .pointers(kept)
            .enumerate()
            .filter(|(i, _)| {
                let index = kept + i;
                index < start || index >= end
            })
            .map(|(_, item)| Arc::clone(item))
            .collect();
        self.chunks.truncate(keep_chunks);
        self.len = kept;
        for item in tail {
            self.push(item);
        }
    }
}

impl std::ops::Index<usize> for StateItems {
    type Output = [u8];

    fn index(&self, index: usize) -> &[u8] {
        self.get(index).expect("StateItems index out of bounds")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cap(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
    }

    fn item(n: usize) -> Arc<[u8]> {
        Arc::from(format!("{n}").as_bytes())
    }

    fn contents(items: &StateItems) -> Vec<String> {
        items.iter().map(|b| String::from_utf8(b.to_vec()).unwrap()).collect()
    }

    #[test]
    fn shared_cache_shares_values_at_the_same_offset() {
        let mut cache: SharedCache<Vec<u8>> = SharedCache::new(cap(8));
        let value = Arc::new(vec![1u8, 2, 3]);
        cache.put("1:s".to_string(), 100, Arc::clone(&value));

        // Own key, right offset.
        assert!(Arc::ptr_eq(&cache.get("1:s", 100).unwrap(), &value));
        // Own key, moved head: stale.
        assert!(cache.get("1:s", 101).is_none());
        // Another key at the same offset shares the very same allocation.
        let other = cache.get("2:s", 100).unwrap();
        assert!(Arc::ptr_eq(&other, &value));
        assert_eq!(cache.len(), 2);
        // Another key at a different offset does not.
        assert!(cache.get("3:s", 999).is_none());
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn shared_cache_releases_a_value_when_its_last_holder_goes() {
        let mut cache: SharedCache<Vec<u8>> = SharedCache::new(cap(8));
        cache.put("1:s".to_string(), 100, Arc::new(vec![1]));
        assert!(cache.get("2:s", 100).is_some());

        // One holder moves on: the value is still held by the other.
        cache.pop("1:s");
        assert!(cache.peek("9:s", 100).is_some());
        // Last holder goes: nothing keeps the value alive, nobody finds it.
        cache.pop("2:s");
        assert!(cache.peek("9:s", 100).is_none());
        assert!(cache.get("9:s", 100).is_none());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn shared_cache_eviction_keeps_shared_values_reachable() {
        let mut cache: SharedCache<Vec<u8>> = SharedCache::new(cap(2));
        cache.put("1:s".to_string(), 100, Arc::new(vec![1]));
        assert!(cache.get("2:s", 100).is_some());
        // Evicts "1:s" (least recently used); "2:s" still holds the value.
        cache.put("3:t".to_string(), 200, Arc::new(vec![2]));
        assert_eq!(cache.len(), 2);
        assert!(cache.get("1:s", 100).is_some(), "re-shared from the surviving holder");
    }

    #[test]
    fn shared_cache_take_and_rekey() {
        let mut cache: SharedCache<Vec<u8>> = SharedCache::new(cap(8));
        cache.put("1:s".to_string(), 100, Arc::new(vec![1]));

        // Sole holder: taking yields the only strong reference, so
        // `Arc::make_mut` updates the value without cloning it.
        let taken = cache.take("1:s", 100).unwrap();
        assert_eq!(Arc::strong_count(&taken), 1);
        assert_eq!(cache.len(), 0);

        // With a second holder the taken Arc is shared, and the other
        // holder's entry is untouched.
        cache.put("1:s".to_string(), 100, taken);
        assert!(cache.get("2:s", 100).is_some());
        let taken = cache.take("1:s", 100).unwrap();
        assert_eq!(Arc::strong_count(&taken), 2);
        assert!(cache.get("2:s", 100).is_some());
        drop(taken);

        // A key with no entry of its own derives from another key's value.
        assert!(cache.take("3:s", 100).is_some());
        // A stale own entry is dropped, and yields nothing by itself.
        cache.put("4:s".to_string(), 50, Arc::new(vec![9]));
        assert!(cache.take("4:s", 77).is_none());
        assert!(cache.peek("4:s", 50).is_none());

        // rekey: same value, new offset, old offset no longer served to this key.
        cache.rekey("2:s", 100, 150);
        assert!(cache.get("2:s", 150).is_some());
        assert!(cache.get("5:s", 150).is_some());
        cache.rekey("6:s", 12345, 12346);
        assert!(cache.peek("6:s", 12346).is_none());

        cache.pop_prefix("2:");
        cache.pop_prefix("5:");
        assert!(cache.peek("7:s", 150).is_none());
    }

    #[test]
    fn shared_cache_offset_index_does_not_grow_without_bound() {
        let mut cache: SharedCache<Vec<u8>> = SharedCache::new(cap(4));
        for offset in 0..10_000u64 {
            cache.put("1:s".to_string(), offset, Arc::new(vec![0]));
        }
        assert!(cache.by_offset.len() <= 2 * 4 + 17);
    }

    /// Every operation on `StateItems` must agree with the same operation on
    /// a plain `Vec`, across chunk boundaries, and must leave earlier
    /// versions untouched.
    #[test]
    fn state_items_match_a_plain_vec_and_keep_old_versions_intact() {
        let mut model: Vec<String> = Vec::new();
        let mut items = StateItems::from_items(std::iter::empty(), true);
        let mut versions: Vec<(StateItems, Vec<String>)> = Vec::new();

        // A deterministic pseudo-random script (xorshift) of pushes, sets
        // and removals, long enough to span several chunks.
        let mut seed = 0x9E3779B97F4A7C15u64;
        let mut next = move |bound: usize| -> usize {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed % bound.max(1) as u64) as usize
        };

        for step in 0..9000 {
            match next(100) {
                0..=84 => {
                    items.push(item(step));
                    model.push(format!("{step}"));
                }
                85..=94 if !model.is_empty() => {
                    let i = next(model.len());
                    assert!(items.set(i, item(step + 1_000_000)));
                    model[i] = format!("{}", step + 1_000_000);
                }
                95..=99 => {
                    let start = next(model.len() + 3);
                    let end = start + next(6);
                    items.remove_range(start, end);
                    let s = start.min(model.len());
                    let e = end.min(model.len());
                    if s < e {
                        model.drain(s..e);
                    }
                }
                _ => {}
            }
            assert_eq!(items.len(), model.len());
            if step % 500 == 0 {
                versions.push((items.clone(), model.clone()));
            }
        }
        assert!(model.len() > 3 * CHUNK, "script must span several chunks");
        assert_eq!(contents(&items), model);
        for (i, expected) in model.iter().enumerate().step_by(97) {
            assert_eq!(&items[i], expected.as_bytes());
        }
        assert_eq!(items.get(model.len()), None);
        assert!(!items.set(model.len(), item(0)));
        assert_eq!(
            items.range(CHUNK - 2, CHUNK + 3).map(|b| b.to_vec()).collect::<Vec<_>>(),
            model[CHUNK - 2..CHUNK + 3].iter().map(|s| s.as_bytes().to_vec()).collect::<Vec<_>>()
        );
        assert_eq!(items.range(model.len() - 1, usize::MAX).count(), 1);
        assert_eq!(items.range(usize::MAX, 3).count(), 0);

        // Snapshots taken along the way were not disturbed by later writes.
        for (version, expected) in &versions {
            assert_eq!(&contents(version), expected);
        }
        // Invariant: all chunks full except the last, none empty.
        let (last, full) = items.chunks.split_last().unwrap();
        assert!(full.iter().all(|c| c.len() == CHUNK));
        assert!(!last.is_empty() && last.len() <= CHUNK);
    }

    #[test]
    fn state_items_versions_share_chunks() {
        let mut base = StateItems::from_items(std::iter::empty(), true);
        for i in 0..(CHUNK * 4 + 10) {
            base.push(item(i));
        }
        let mut child = base.clone();
        child.push(item(999_999));
        child.set(CHUNK + 5, item(888_888));

        // Chunks 0, 2 and 3 are the same allocations; 1 (edited) and 4
        // (appended to) were copied — as pointers, the items are shared.
        for i in [0, 2, 3] {
            assert!(Arc::ptr_eq(&base.chunks[i], &child.chunks[i]));
        }
        for i in [1, 4] {
            assert!(!Arc::ptr_eq(&base.chunks[i], &child.chunks[i]));
        }
        assert!(Arc::ptr_eq(&base.chunks[1][0], &child.chunks[1][0]));
        assert_eq!(base.len() + 1, child.len());
        assert_eq!(&base[CHUNK + 5], format!("{}", CHUNK + 5).as_bytes());
        assert_eq!(&child[CHUNK + 5], b"888888");

        // A redact keeps the chunks before it shared.
        let mut redacted = base.clone();
        redacted.remove_range(CHUNK * 2 + 1, CHUNK * 2 + 4);
        assert!(Arc::ptr_eq(&base.chunks[0], &redacted.chunks[0]));
        assert!(Arc::ptr_eq(&base.chunks[1], &redacted.chunks[1]));
        assert_eq!(redacted.len(), base.len() - 3);
    }

    #[test]
    fn canonical_form_is_computed_once_shared_and_reuses_canonical_items() {
        // Items 0 and 2 are already in serde_json's own form; 1 and 3 are not.
        let raw: [&[u8]; 4] = [b"{\"a\":1,\"b\":2}", b"{\"b\": 2, \"a\": 1}", b"\"x\"", b"1e2"];
        let mut many: Vec<&[u8]> = Vec::new();
        for i in 0..(CHUNK * 2 + 4) {
            // First two chunks entirely canonical, the last one mixed.
            many.push(if i < CHUNK * 2 { raw[(i % 2) * 2] } else { raw[i % 4] });
        }
        let value = Arc::new(StateItems::from_items(many.iter().copied(), false));
        assert!(!value.is_canonical());

        let first = Arc::clone(&value).into_canonical().unwrap();
        let second = Arc::clone(&value).into_canonical().unwrap();
        assert!(Arc::ptr_eq(&first, &second), "one canonical base for everyone deriving from this value");
        assert!(first.is_canonical());
        assert_eq!(first.len(), value.len());

        // Wholly canonical chunks are the same chunks; canonical items in a
        // mixed chunk are the same items; the rest are re-spelled.
        assert!(Arc::ptr_eq(&value.chunks[0], &first.chunks[0]));
        assert!(Arc::ptr_eq(&value.chunks[1], &first.chunks[1]));
        assert!(!Arc::ptr_eq(&value.chunks[2], &first.chunks[2]));
        assert!(Arc::ptr_eq(&value.chunks[2][0], &first.chunks[2][0]));
        assert_eq!(&first[CHUNK * 2 + 1], b"{\"a\":1,\"b\":2}");
        assert_eq!(&first[CHUNK * 2 + 3], b"100.0");
        // The original is untouched.
        assert_eq!(&value[CHUNK * 2 + 1], raw[1]);

        // A canonical value is its own canonical form.
        let again = Arc::clone(&first).into_canonical().unwrap();
        assert!(Arc::ptr_eq(&again, &first));

        // An item that is not JSON has no canonical form.
        let broken = Arc::new(StateItems::from_items([b"{".as_slice()].into_iter(), false));
        assert!(broken.into_canonical().is_none());
    }
}
