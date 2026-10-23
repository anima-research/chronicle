//! A Redact whose range runs past the end of an append-log state removes only
//! the items that exist, as the materialized value does, and the stored item
//! count (what `get_state_len` reports and Edit's bounds check reads) must
//! agree with it, in memory and after a reopen.

use chronicle::{StateOperation, StateRegistration, StateStrategy, Store, StoreConfig};
use serde_json::{json, Value};
use tempfile::TempDir;

fn config(dir: &TempDir, create: bool) -> StoreConfig {
    StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: create,
    }
}

fn store_with_items(dir: &TempDir, n: i64) -> Store {
    let store = Store::create(config(dir, true)).unwrap();
    store.set_auto_snapshot(false);
    store
        .register_state(StateRegistration {
            id: "s".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 1000,
                full_snapshot_every: 1000,
            },
            initial_value: None,
        })
        .unwrap();
    for i in 0..n {
        store
            .update_state(
                "s",
                StateOperation::Append(serde_json::to_vec(&json!(i)).unwrap()),
            )
            .unwrap();
    }
    store
}

fn value(store: &Store) -> Vec<Value> {
    serde_json::from_slice(&store.get_state("s").unwrap().unwrap()).unwrap()
}

/// The count after the Redact, read in memory and again after a reopen, must
/// equal the value's length, and an Edit of the last item must be accepted.
fn assert_count_matches(dir: TempDir, store: Store, expected: Vec<Value>) {
    assert_eq!(value(&store), expected);
    assert_eq!(store.get_state_len("s").unwrap(), Some(expected.len()));
    if let Some(last) = expected.len().checked_sub(1) {
        store
            .update_state(
                "s",
                StateOperation::Edit {
                    index: last,
                    new_value: serde_json::to_vec(&json!("edited")).unwrap(),
                },
            )
            .unwrap();
    }
    store.sync().unwrap();
    drop(store);
    let store = Store::open(config(&dir, false)).unwrap();
    assert_eq!(store.get_state_len("s").unwrap(), Some(expected.len()));
}

#[test]
fn redact_past_the_end_counts_only_the_items_it_removes() {
    let dir = TempDir::new().unwrap();
    let store = store_with_items(&dir, 5);
    store
        .update_state("s", StateOperation::Redact { start: 4, end: 100 })
        .unwrap();
    assert_count_matches(dir, store, vec![json!(0), json!(1), json!(2), json!(3)]);
}

#[test]
fn redact_wholly_past_the_end_removes_nothing() {
    let dir = TempDir::new().unwrap();
    let store = store_with_items(&dir, 5);
    store
        .update_state("s", StateOperation::Redact { start: 50, end: 60 })
        .unwrap();
    assert_count_matches(
        dir,
        store,
        vec![json!(0), json!(1), json!(2), json!(3), json!(4)],
    );
}

#[test]
fn redact_inside_the_state_counts_as_before() {
    let dir = TempDir::new().unwrap();
    let store = store_with_items(&dir, 5);
    store
        .update_state("s", StateOperation::Redact { start: 1, end: 3 })
        .unwrap();
    assert_count_matches(dir, store, vec![json!(0), json!(3), json!(4)]);
}

#[test]
fn a_reversed_range_removes_nothing() {
    let dir = TempDir::new().unwrap();
    let store = store_with_items(&dir, 5);
    store
        .update_state("s", StateOperation::Redact { start: 3, end: 1 })
        .unwrap();
    assert_count_matches(
        dir,
        store,
        vec![json!(0), json!(1), json!(2), json!(3), json!(4)],
    );
}

#[test]
fn an_item_appended_after_redacting_past_the_end_reads_at_its_own_index() {
    let dir = TempDir::new().unwrap();
    let store = store_with_items(&dir, 5);
    store
        .update_state("s", StateOperation::Redact { start: 4, end: 100 })
        .unwrap();
    store
        .update_state(
            "s",
            StateOperation::Append(serde_json::to_vec(&json!("new")).unwrap()),
        )
        .unwrap();
    let item = |i: usize| -> Value {
        serde_json::from_slice(&store.get_state_item("s", i).unwrap().unwrap()).unwrap()
    };
    assert_eq!(item(0), json!(0));
    assert_eq!(item(4), json!("new"));
    assert_eq!(store.get_state_len("s").unwrap(), Some(5));
}
