//! A Redact's range reaches a field index as it reaches the value: both
//! clamp it to the items there are, so a range ending at or past 2^32
//! removes from the index exactly what it removes from the value.
#![cfg(target_pointer_width = "64")]

use chronicle::{FieldIndexKind, StateOperation, StateRegistration, StateStrategy, Store, StoreConfig};
use serde_json::{json, Value};
use tempfile::TempDir;

fn op(store: &Store, operation: StateOperation) {
    store.update_state("s", operation).unwrap();
}

fn value(store: &Store) -> Vec<Value> {
    serde_json::from_slice(&store.get_state("s").unwrap().unwrap()).unwrap()
}

#[test]
fn a_redact_past_u32_leaves_a_field_index_as_the_value() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: true,
    })
    .unwrap();
    store.set_auto_snapshot(false);
    store
        .register_state(StateRegistration {
            id: "s".to_string(),
            strategy: StateStrategy::AppendLog { delta_snapshot_every: 1000, full_snapshot_every: 1000 },
            initial_value: None,
        })
        .unwrap();
    for id in ["a", "b", "c", "d", "e"] {
        op(&store, StateOperation::Append(serde_json::to_vec(&json!({ "id": id })).unwrap()));
    }
    store.register_state_field_index("s", "/id", FieldIndexKind::String).unwrap();
    let ordinals = |id: &str| store.query_state_index_eq("s", "/id", id, None, None).unwrap();
    // Wholly past the end: nothing is removed, from the value or the index.
    op(&store, StateOperation::Redact { start: 1 << 32, end: (1 << 32) + 1 });
    assert_eq!(value(&store).len(), 5);
    assert_eq!((ordinals("a"), ordinals("b")), (vec![0], vec![1]));
    // From inside to past 2^32: the value loses "d" and "e", and so does the index.
    op(&store, StateOperation::Redact { start: 3, end: (1 << 32) + 1 });
    assert_eq!(value(&store).len(), 3);
    assert_eq!((ordinals("c"), ordinals("d"), ordinals("e")), (vec![2], vec![], vec![]));
}
