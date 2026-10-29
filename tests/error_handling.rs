//! Error handling and edge case tests.

use chronicle::{
    RecordId, StateOperation, StateRegistration, StateStrategy, Store, StoreConfig, StoreError,
};
use tempfile::TempDir;

fn test_store(dir: &TempDir) -> Store {
    Store::create(StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: true,
    })
    .unwrap()
}

// --- State Errors ---

#[test]
fn test_get_unregistered_state() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    // Should return None, not error
    let result = store.get_state("nonexistent").unwrap();
    assert!(result.is_none());
}

#[test]
fn test_register_duplicate_state() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store
        .register_state(StateRegistration {
            id: "test".to_string(),
            strategy: StateStrategy::Snapshot,
            initial_value: None,
        })
        .unwrap();

    let result = store.register_state(StateRegistration {
        id: "test".to_string(),
        strategy: StateStrategy::Snapshot,
        initial_value: None,
    });

    assert!(matches!(result, Err(StoreError::StateExists(_))));
}

#[test]
fn test_edit_out_of_bounds() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store
        .register_state(StateRegistration {
            id: "items".to_string(),
            strategy: StateStrategy::AppendLog { delta_snapshot_every: 10, full_snapshot_every: 5 },
            initial_value: None,
        })
        .unwrap();

    // Add one item
    store
        .update_state("items", StateOperation::Append(b"1".to_vec()))
        .unwrap();

    // Try to edit at invalid index - should fail at write time
    let result = store.update_state(
        "items",
        StateOperation::Edit {
            index: 5,
            new_value: b"999".to_vec(),
        },
    );

    assert!(matches!(result, Err(StoreError::InvalidOperation(_))));

    // State should still be valid
    let state = store.get_state("items").unwrap().unwrap();
    let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
    assert_eq!(arr, vec![1]);
}

// --- Branch Errors ---

#[test]
fn test_create_duplicate_branch() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store.create_branch("feature", None).unwrap();

    let result = store.create_branch("feature", None);
    assert!(matches!(result, Err(StoreError::BranchExists(_))));
}

#[test]
fn test_switch_to_nonexistent_branch() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    let result = store.switch_branch("nonexistent");
    assert!(matches!(result, Err(StoreError::BranchNotFound(_))));
}

#[test]
fn test_delete_main_branch() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    let result = store.delete_branch("main");
    assert!(result.is_err());
}

#[test]
fn test_create_branch_from_nonexistent() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    let result = store.create_branch("new", Some("nonexistent"));
    assert!(matches!(result, Err(StoreError::BranchNotFound(_))));
}

// --- Record Errors ---

#[test]
fn test_get_nonexistent_record() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    let result = store.get_record(RecordId(999)).unwrap();
    assert!(result.is_none());
}

// --- Blob Errors ---

#[test]
fn test_get_nonexistent_blob() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    let hash = chronicle::Hash::from_bytes(b"nonexistent");
    let result = store.get_blob(&hash).unwrap();
    assert!(result.is_none());
}

// --- Store Errors ---

#[test]
fn test_open_nonexistent_store() {
    let dir = TempDir::new().unwrap();

    let result = Store::open(StoreConfig {
        path: dir.path().join("nonexistent"),
        blob_cache_size: 100,
        create_if_missing: false,
    });

    assert!(result.is_err());
}

// --- JSON Parsing Errors ---

#[test]
fn test_append_to_non_array_state() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store
        .register_state(StateRegistration {
            id: "obj".to_string(),
            strategy: StateStrategy::Snapshot,
            initial_value: None,
        })
        .unwrap();

    // Set as object, not array
    store
        .update_state("obj", StateOperation::Set(b"{\"key\": \"value\"}".to_vec()))
        .unwrap();

    // Appending to non-array succeeds at write time (lazy validation for performance)
    // but fails on reconstruction/read
    store.update_state("obj", StateOperation::Append(b"1".to_vec())).unwrap();

    // Reading the state should fail during reconstruction
    let result = store.get_state("obj");
    assert!(matches!(result, Err(StoreError::Deserialization(_))));
}

#[test]
fn test_invalid_json_in_append() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store
        .register_state(StateRegistration {
            id: "items".to_string(),
            strategy: StateStrategy::AppendLog { delta_snapshot_every: 10, full_snapshot_every: 5 },
            initial_value: None,
        })
        .unwrap();

    // Invalid JSON succeeds at write time (lazy validation for performance)
    // but fails on reconstruction/read
    store.update_state("items", StateOperation::Append(b"not valid json".to_vec())).unwrap();

    // Reading the state should fail during reconstruction
    let result = store.get_state("items");
    assert!(matches!(result, Err(StoreError::Deserialization(_))));
}

// --- Boundary Conditions ---

#[test]
fn test_empty_blob() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    let hash = store.store_blob(b"", "application/octet-stream").unwrap();
    let blob = store.get_blob(&hash).unwrap().unwrap();

    assert!(blob.content.is_empty());
}

#[test]
fn test_empty_record_payload() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    use chronicle::RecordInput;

    let record = store
        .append(RecordInput::raw("empty", vec![]))
        .unwrap();

    let retrieved = store.get_record(record.id).unwrap().unwrap();
    assert!(retrieved.payload.is_empty());
}

#[test]
fn test_unicode_in_state_id() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store
        .register_state(StateRegistration {
            id: "状態_🎉_данные".to_string(),
            strategy: StateStrategy::Snapshot,
            initial_value: None,
        })
        .unwrap();

    store
        .update_state(
            "状態_🎉_данные",
            StateOperation::Set(b"\"value\"".to_vec()),
        )
        .unwrap();

    let state = store.get_state("状態_🎉_данные").unwrap().unwrap();
    assert_eq!(state, b"\"value\"");
}

#[test]
fn test_unicode_in_branch_name() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store.create_branch("功能分支_🌿", None).unwrap();

    let branch = store.switch_branch("功能分支_🌿").unwrap();
    assert_eq!(branch.name, "功能分支_🌿");
}
