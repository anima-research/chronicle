use chronicle::{Hash, StateOperation, StateRegistration, StateStrategy, Store, StoreConfig};
use serde_json::{json, Value};
use tempfile::TempDir;

fn store(dir: &TempDir) -> Store {
    let store = Store::create(StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: true,
    })
    .unwrap();
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
    for i in 0..5 {
        store
            .update_state(
                "s",
                StateOperation::Append(serde_json::to_vec(&json!(i)).unwrap()),
            )
            .unwrap();
    }
    store
}

fn delta(value: &[u8]) -> StateOperation {
    StateOperation::Delta {
        old_hash: Hash::from_bytes(b""),
        new_value: value.to_vec(),
    }
}

fn items(store: &Store) -> Vec<Value> {
    serde_json::from_slice(&store.get_state("s").unwrap().unwrap()).unwrap()
}

#[test]
fn a_delta_replacing_the_value_sets_the_length_on_the_branch_and_on_forks_after_it() {
    let dir = TempDir::new().unwrap();
    let store = store(&dir);
    store
        .update_state("s", delta(&serde_json::to_vec(&json!(["a", "b"])).unwrap()))
        .unwrap();
    assert_eq!(items(&store), vec![json!("a"), json!("b")]);
    assert_eq!(store.get_state_len("s").unwrap(), Some(2));
    let edit_past_the_end = StateOperation::Edit {
        index: 2,
        new_value: serde_json::to_vec(&json!("z")).unwrap(),
    };
    assert!(store.update_state("s", edit_past_the_end).is_err());

    let after_delta = store.current_branch().head;
    store
        .create_branch_at("at-head", "main", after_delta)
        .unwrap();
    store
        .update_state(
            "s",
            StateOperation::Append(serde_json::to_vec(&json!("x")).unwrap()),
        )
        .unwrap();
    store
        .create_branch_at("in-the-past", "main", after_delta)
        .unwrap();
    store.switch_branch("in-the-past").unwrap();
    assert_eq!(store.get_state_len("s").unwrap(), Some(2));
    store
        .update_state(
            "s",
            StateOperation::Append(serde_json::to_vec(&json!("y")).unwrap()),
        )
        .unwrap();
    assert_eq!(
        store.get_state_item("s", 2).unwrap(),
        Some(serde_json::to_vec(&json!("y")).unwrap())
    );
    assert_eq!(store.get_state_len("s").unwrap(), Some(3));

    store.switch_branch("at-head").unwrap();
    assert_eq!(store.get_state_len("s").unwrap(), Some(2));
    store
        .update_state(
            "s",
            StateOperation::Append(serde_json::to_vec(&json!("c")).unwrap()),
        )
        .unwrap();
    assert_eq!(
        store.get_state_item("s", 2).unwrap(),
        Some(serde_json::to_vec(&json!("c")).unwrap())
    );
    assert_eq!(store.get_state_item("s", 5).unwrap(), None);
    assert_eq!(items(&store), vec![json!("a"), json!("b"), json!("c")]);
    assert_eq!(store.get_state_len("s").unwrap(), Some(3));
}

#[test]
fn a_replace_with_no_bytes_leaves_a_length_of_zero() {
    for operation in [StateOperation::Set(Vec::new()), delta(b"")] {
        let dir = TempDir::new().unwrap();
        let store = store(&dir);
        store.update_state("s", operation).unwrap();
        let tail: Vec<Value> =
            serde_json::from_slice(&store.get_state_tail("s", 1).unwrap().unwrap()).unwrap();
        assert!(tail.is_empty());
        store
            .update_state(
                "s",
                StateOperation::Append(serde_json::to_vec(&json!("x")).unwrap()),
            )
            .unwrap();
        assert_eq!(items(&store), vec![json!("x")]);
        assert_eq!(store.get_state_len("s").unwrap(), Some(1));
    }
}

#[test]
fn a_set_of_an_array_sets_the_length() {
    let dir = TempDir::new().unwrap();
    let store = store(&dir);
    store
        .update_state(
            "s",
            StateOperation::Set(serde_json::to_vec(&json!(["p", "q", "r"])).unwrap()),
        )
        .unwrap();
    assert_eq!(store.get_state_len("s").unwrap(), Some(3));
}
