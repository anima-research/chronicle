//! Fixes from the seventh review of the batched calls (chronicle#62).
//!
//! - A batched append counts a state's items before it takes its ordinals,
//!   when the session hasn't counted them: the item count a head holds can
//!   be off, as one a release before 0.5.0 stored after a Redact past the
//!   end, or one a caller's `DeltaSnapshot` left. The head-record fast paths
//!   of the batched calls serve only a count the session has checked.

use chronicle::{
    BranchId, StateIndex, StateOperation, StateRegistration, StateStrategy, Store, StoreConfig,
    StoreError,
};
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

fn config(path: &Path) -> StoreConfig {
    StoreConfig {
        path: path.to_path_buf(),
        blob_cache_size: 16,
        create_if_missing: true,
    }
}

fn register_log(store: &Store, id: &str) {
    store
        .register_state(StateRegistration {
            id: id.to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 1000,
                full_snapshot_every: 1000,
            },
            initial_value: None,
        })
        .unwrap();
}

fn append(store: &Store, id: &str, value: Value) {
    store
        .update_state(id, StateOperation::Append(serde_json::to_vec(&value).unwrap()))
        .unwrap();
}

fn value(store: &Store, id: &str) -> Value {
    serde_json::from_slice(&store.get_state(id).unwrap().unwrap()).unwrap()
}

fn text(bytes: Option<Vec<u8>>) -> Option<String> {
    bytes.map(|b| String::from_utf8(b).unwrap())
}

fn texts(items: Vec<Option<Vec<u8>>>) -> Vec<Option<String>> {
    items.into_iter().map(text).collect()
}

/// Opens the store at `path` with auto-snapshots off, so nothing but the
/// test's own writes reaches the log.
fn open(path: &Path) -> Store {
    let store = Store::open(config(path)).unwrap();
    store.set_auto_snapshot(false);
    store
}

/// Rewrites `state.bin` so the head of `state` on `branch` holds `count`, as
/// a release that counted it another way would have stored it.
fn store_count(path: &Path, branch: BranchId, state: &str, count: usize) {
    let file = path.join("state.bin");
    let bytes = fs::read(&file).unwrap();
    assert_eq!(&bytes[0..4], b"STI\0");
    let version = bytes[4];
    let len = u64::from_le_bytes(bytes[5..13].try_into().unwrap()) as usize;
    let mut index: StateIndex = rmp_serde::from_slice(&bytes[13..13 + len]).unwrap();
    index.heads.get_mut(branch, state).unwrap().item_count = count;
    let encoded = rmp_serde::to_vec(&index).unwrap();
    let mut out = Vec::new();
    out.extend_from_slice(b"STI\0");
    out.push(version);
    out.extend_from_slice(&(encoded.len() as u64).to_le_bytes());
    out.extend_from_slice(&encoded);
    fs::write(&file, out).unwrap();
}

/// A store as a release before 0.5.0 left it: six appends, then a Redact of
/// `[3, 9)`. The value is `[0, 1, 2]`; such a release stored the count
/// `6 - min(9 - 3, 6) = 0`.
fn legacy_store(dir: &TempDir) -> PathBuf {
    let path = dir.path().join("store");
    let store = Store::create(config(&path)).unwrap();
    store.set_auto_snapshot(false);
    register_log(&store, "s");
    for i in 0..6 {
        append(&store, "s", json!(i));
    }
    store
        .update_state("s", StateOperation::Redact { start: 3, end: 9 })
        .unwrap();
    assert_eq!(store.get_state_len("s").unwrap(), Some(3), "this build clamps");
    let branch = store.current_branch().id;
    store.sync().unwrap();
    drop(store);
    store_count(&path, branch, "s", 0);
    path
}

#[test]
fn a_batch_counts_a_legacy_states_items_before_it_takes_its_ordinals() {
    let dir = TempDir::new().unwrap();
    let path = legacy_store(&dir);

    let store = open(&path);
    assert_eq!(value(&store, "s"), json!([0, 1, 2]));
    assert_eq!(store.get_state_len("s").unwrap(), Some(0), "the stored count, as loaded");

    let batch = store
        .append_many_to_state_json("s", &[json!("a"), json!("b")])
        .unwrap();
    assert_eq!(batch.first_ordinal, 3, "the batch's first item is the state's fourth");
    assert_eq!(store.get_state_len("s").unwrap(), Some(5));
    assert_eq!(value(&store, "s"), json!([0, 1, 2, "a", "b"]));
    store.sync().unwrap();
    drop(store);

    // Cold caches: the count the batch stored is the state's.
    let store = open(&path);
    assert_eq!(store.get_state_len("s").unwrap(), Some(5));
    assert_eq!(text(store.get_state_item("s", 1).unwrap()).as_deref(), Some("1"));
    drop(store);
    let store = open(&path);
    assert_eq!(
        texts(store.get_state_items_by_ordinal("s", &[1, 4]).unwrap()),
        [Some("1".to_string()), Some("\"b\"".to_string())]
    );
    store
        .update_state("s", StateOperation::Edit { index: 4, new_value: b"\"e\"".to_vec() })
        .unwrap();
    assert_eq!(value(&store, "s"), json!([0, 1, 2, "a", "e"]));
}

#[test]
fn a_batch_after_a_callers_delta_snapshot_counts_the_items_it_restates() {
    // The store counts a DeltaSnapshot as restating items already counted,
    // which is what its own are. A caller's can be anything.
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    store.set_auto_snapshot(false);
    register_log(&store, "log");
    store
        .update_state(
            "log",
            StateOperation::DeltaSnapshot(serde_json::to_vec(&json!([1, 2])).unwrap()),
        )
        .unwrap();
    assert_eq!(store.get_state_len("log").unwrap(), Some(0));

    let batch = store.append_many_to_state_json("log", &[json!(3), json!(4)]).unwrap();
    assert_eq!(batch.first_ordinal, 2);
    assert_eq!(store.get_state_len("log").unwrap(), Some(4));
    store.clear_state_caches();
    assert_eq!(text(store.get_state_item("log", 1).unwrap()).as_deref(), Some("2"));
    store.clear_state_caches();
    assert_eq!(text(store.get_state_item("log", 3).unwrap()).as_deref(), Some("4"));
    store.clear_state_caches();
    assert_eq!(
        texts(store.get_state_items_by_ordinal("log", &[1]).unwrap()),
        [Some("2".to_string())]
    );
}

#[test]
fn the_count_is_read_once_per_state_in_a_session() {
    let dir = TempDir::new().unwrap();
    let path = legacy_store(&dir);
    let store = open(&path);

    let before = store.state_reconstruction_count();
    assert!(matches!(
        store.append_many_to_state_json("s", &[]),
        Err(StoreError::InvalidOperation(_))
    ));
    assert_eq!(store.state_reconstruction_count(), before, "an empty batch counts nothing");
    store.append_many_to_state_json("s", &[json!("a")]).unwrap();
    assert_eq!(store.state_reconstruction_count(), before + 1, "the count read the value once");
    let second = store.append_many_to_state_json("s", &[json!("b")]).unwrap();
    assert_eq!(second.first_ordinal, 4);
    assert_eq!(
        texts(store.get_state_items_by_ordinal("s", &[0, 4]).unwrap()),
        [Some("0".to_string()), Some("\"b\"".to_string())]
    );
    assert_eq!(
        store.state_reconstruction_count(),
        before + 1,
        "the second batch took the count it kept, and the items it read stay cached"
    );
}

#[test]
fn a_batch_to_a_state_whose_items_cant_be_read_takes_the_stored_count() {
    // An Append of bytes that aren't JSON is recorded as given, and every
    // materialization fails on it. There is nothing to count; the batch takes
    // the stored count, as it would without the check, and doesn't try the
    // count again at the next batch.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    {
        let store = Store::create(config(&path)).unwrap();
        store.set_auto_snapshot(false);
        register_log(&store, "s");
        store
            .update_state("s", StateOperation::Append(b"not json".to_vec()))
            .unwrap();
        store.sync().unwrap();
    }
    let store = open(&path);
    let before = store.state_reconstruction_count();
    let first = store.append_many_to_state_json("s", &[json!(1), json!(2)]).unwrap();
    assert_eq!(first.first_ordinal, 1);
    let second = store.append_many_to_state_json("s", &[json!(3)]).unwrap();
    assert_eq!(second.first_ordinal, 3);
    assert_eq!(store.get_state_len("s").unwrap(), Some(4));
    assert_eq!(store.state_reconstruction_count(), before + 1, "one attempt, not one per batch");
    assert!(matches!(
        store.get_state_item("s", 3),
        Err(StoreError::Deserialization(_))
    ));
}

#[test]
fn the_batched_head_record_fast_paths_serve_only_a_checked_count() {
    // A batch at the head whose stored count is one short: the head record's
    // last item is item 2, not item 1, which the count would make last.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let branch = {
        let store = Store::create(config(&path)).unwrap();
        store.set_auto_snapshot(false);
        register_log(&store, "s");
        store
            .append_many_to_state_json("s", &[json!("a"), json!("b"), json!("c")])
            .unwrap();
        store.sync().unwrap();
        store.current_branch().id
    };
    store_count(&path, branch, "s", 2);

    let store = open(&path);
    assert_eq!(text(store.get_state_item("s", 1).unwrap()).as_deref(), Some("\"b\""));
    drop(store);
    let store = open(&path);
    assert_eq!(
        texts(store.get_state_items_by_ordinal("s", &[1, 1]).unwrap()),
        [Some("\"b\"".to_string()), Some("\"b\"".to_string())]
    );

    // A batch counts the items again, and the head-record path then serves
    // the batch's last item from the record alone.
    let batch = store.append_many_to_state_json("s", &[json!("d")]).unwrap();
    assert_eq!(batch.first_ordinal, 3);
    store.clear_state_caches();
    let before = store.state_reconstruction_count();
    assert_eq!(text(store.get_state_item("s", 3).unwrap()).as_deref(), Some("\"d\""));
    assert_eq!(
        texts(store.get_state_items_by_ordinal("s", &[3]).unwrap()),
        [Some("\"d\"".to_string())]
    );
    assert_eq!(store.state_reconstruction_count(), before, "served from the head record");
}
