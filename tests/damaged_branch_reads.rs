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

fn store(dir: &TempDir) -> Store {
    let store = Store::create(config(dir, true)).unwrap();
    store
        .register_state(StateRegistration {
            id: "s".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 2,
                full_snapshot_every: 100,
            },
            initial_value: None,
        })
        .unwrap();
    store
}

fn bytes(v: Value) -> Vec<u8> {
    serde_json::to_vec(&v).unwrap()
}

fn op(store: &Store, operation: StateOperation) {
    store.update_state("s", operation).unwrap();
}

fn append(store: &Store, v: Value) {
    op(store, StateOperation::Append(bytes(v)));
}

fn value(store: &Store) -> Vec<Value> {
    serde_json::from_slice(&store.get_state("s").unwrap().unwrap()).unwrap()
}

fn damaged_branch(dir: &TempDir, change: &[StateOperation], old_delta: Value) -> Store {
    let store = store(dir);
    append(&store, json!(0));
    append(&store, json!(1));
    assert_eq!(store.get_compaction_stats("s").unwrap().delta_snapshots_since_full, 1, "setup: main's delta");
    for operation in change {
        op(&store, operation.clone());
    }
    store.create_branch_at("child", "main", store.current_branch().head).unwrap();
    store.switch_branch("child").unwrap();
    store.set_auto_snapshot(false);
    append(&store, json!(2));
    op(&store, StateOperation::DeltaSnapshot(bytes(old_delta)));
    append(&store, json!(3));
    store.set_auto_snapshot(true);
    store
}

fn reopened(store: Store, dir: &TempDir) -> Store {
    drop(store);
    let store = Store::open(config(dir, false)).unwrap();
    store.switch_branch("child").unwrap();
    store
}

fn cases() -> Vec<(&'static str, Vec<StateOperation>, Value, Vec<Value>)> {
    vec![
        ("Redact", vec![StateOperation::Redact { start: 1, end: 100 }], json!([2]), vec![json!(0), json!(2), json!(3)]),
        (
            "Edit",
            vec![StateOperation::Edit { index: 1, new_value: bytes(json!("edited")) }],
            json!([2]),
            vec![json!(0), json!("edited"), json!(2), json!(3)],
        ),
        (
            "Set",
            vec![StateOperation::Append(bytes(json!(7))), StateOperation::Set(bytes(json!([9])))],
            json!([7, 2]),
            vec![json!(9), json!(2), json!(3)],
        ),
        (
            "Delta",
            vec![
                StateOperation::Append(bytes(json!(7))),
                StateOperation::Delta { old_hash: chronicle::Hash([0; 32]), new_value: bytes(json!([9])) },
            ],
            json!([7, 2]),
            vec![json!(9), json!(2), json!(3)],
        ),
    ]
}

#[test]
fn a_damaged_branch_reads_its_true_value() {
    for (name, change, old_delta, expected) in cases() {
        let dir = TempDir::new().unwrap();
        let store = reopened(damaged_branch(&dir, &change, old_delta), &dir);
        assert_eq!(value(&store), expected, "{name}: get_state");
        let slice: Vec<Value> =
            serde_json::from_slice(&store.get_state_slice("s", 0, expected.len()).unwrap().unwrap()).unwrap();
        assert_eq!(slice, expected, "{name}: get_state_slice");
        let items: Vec<Value> = store.iter_state_items("s").unwrap().unwrap().map(|item| item.unwrap()).collect();
        assert_eq!(items, expected, "{name}: iter_state_items");
        for n in 1..=expected.len() {
            let tail: Vec<Value> = serde_json::from_slice(&store.get_state_tail("s", n).unwrap().unwrap()).unwrap();
            assert_eq!(tail, expected[expected.len() - n..], "{name}: get_state_tail({n})");
        }
        if name != "Delta" {
            assert_eq!(store.get_state_len("s").unwrap(), Some(expected.len()), "{name}: get_state_len");
        }
    }
}

#[test]
fn a_damaged_branch_reads_its_true_value_at_a_past_sequence() {
    for (name, change, old_delta, mut expected) in cases() {
        let dir = TempDir::new().unwrap();
        let store = damaged_branch(&dir, &change, old_delta);
        let before_3 = chronicle::Sequence(store.current_branch().head.0 - 1);
        let store = reopened(store, &dir);
        expected.pop();
        let at: Vec<Value> = serde_json::from_slice(&store.get_state_at("s", before_3).unwrap().unwrap()).unwrap();
        assert_eq!(at, expected, "{name}");
    }
}

#[test]
fn a_branch_made_from_a_damaged_branch_starts_from_its_true_value() {
    for (name, change, old_delta, mut expected) in cases() {
        let dir = TempDir::new().unwrap();
        let store = reopened(damaged_branch(&dir, &change, old_delta), &dir);
        store.create_branch_at("grandchild", "child", store.current_branch().head).unwrap();
        store.switch_branch("grandchild").unwrap();
        assert_eq!(value(&store), expected, "{name}: its value at the point");
        assert_eq!(store.get_state_len("s").unwrap(), Some(expected.len()), "{name}: its count at the point");
        store.set_auto_snapshot(false);
        append(&store, json!(4));
        append(&store, json!(5));
        assert_eq!(store.snapshot_needed("s"), None, "{name}: no delta is due across the change");
        expected.extend([json!(4), json!(5)]);
        assert_eq!(value(&store), expected, "{name}");
    }
}

#[test]
fn a_full_snapshot_of_a_damaged_branch_keeps_its_true_value() {
    for (name, change, old_delta, expected) in cases() {
        let dir = TempDir::new().unwrap();
        let store = reopened(damaged_branch(&dir, &change, old_delta), &dir);
        store.compact_state("s").unwrap();
        let store = reopened(store, &dir);
        assert_eq!(value(&store), expected, "{name}");
        assert_eq!(store.get_state_len("s").unwrap(), Some(expected.len()), "{name}: the full snapshot's count");
    }
}

#[test]
fn a_delta_with_only_appends_behind_it_stands_for_them() {
    let dir = TempDir::new().unwrap();
    let store = store(&dir);
    op(&store, StateOperation::DeltaSnapshot(bytes(json!([0, 1]))));
    append(&store, json!(2));
    append(&store, json!(3));
    assert_eq!(store.get_compaction_stats("s").unwrap().delta_snapshots_since_full, 2, "setup: the store's own delta");
    append(&store, json!(4));
    let store = {
        drop(store);
        Store::open(config(&dir, false)).unwrap()
    };
    assert_eq!(value(&store), vec![json!(0), json!(1), json!(2), json!(3), json!(4)]);
    let tail: Vec<Value> = serde_json::from_slice(&store.get_state_tail("s", 3).unwrap().unwrap()).unwrap();
    assert_eq!(tail, vec![json!(2), json!(3), json!(4)], "a tail no longer than asked, whatever the stored count");
}

#[test]
fn a_field_index_an_earlier_release_saved_is_built_again() {
    let dir = TempDir::new().unwrap();
    let store = store(&dir);
    for id in ["a", "b"] {
        append(&store, json!({ "id": id }));
    }
    store.register_state_field_index("s", "/id", chronicle::FieldIndexKind::String).unwrap();
    store.sync().unwrap();
    drop(store);
    let store = Store::open(config(&dir, false)).unwrap();
    assert_eq!(store.query_state_index_eq("s", "/id", "b", None, None), Some(vec![1]), "setup: this release's file is taken up");
    drop(store);
    let path = dir.path().join("store").join("state-indexes.bin");
    let mut file = std::fs::read(&path).unwrap();
    assert_eq!(&file[..4], b"SFI\0", "setup: the index file");
    file[4] = 2;
    std::fs::write(&path, file).unwrap();
    let store = Store::open(config(&dir, false)).unwrap();
    assert_eq!(store.query_state_index_eq("s", "/id", "b", None, None), None, "not taken up");
    store.register_state_field_index("s", "/id", chronicle::FieldIndexKind::String).unwrap();
    assert_eq!(store.query_state_index_eq("s", "/id", "b", None, None), Some(vec![1]));
}

#[test]
fn a_tail_reaching_a_delta_reads_the_value_it_set() {
    let dir = TempDir::new().unwrap();
    let store = store(&dir);
    append(&store, json!(0));
    append(&store, json!(1));
    op(&store, StateOperation::Delta { old_hash: chronicle::Hash([0; 32]), new_value: bytes(json!([9, 8])) });
    append(&store, json!(2));
    append(&store, json!(3));
    assert_eq!(value(&store), vec![json!(9), json!(8), json!(2), json!(3)], "setup");
    let tail: Vec<Value> = serde_json::from_slice(&store.get_state_tail("s", 3).unwrap().unwrap()).unwrap();
    assert_eq!(tail, vec![json!(8), json!(2), json!(3)]);
}

#[test]
fn a_tail_takes_each_item_once() {
    let dir = TempDir::new().unwrap();
    let store = store(&dir);
    for v in 0..7 {
        append(&store, json!(v));
    }
    assert_eq!(store.get_compaction_stats("s").unwrap().delta_snapshots_since_full, 3, "setup: three deltas");
    let all = value(&store);
    assert_eq!(all, (0..7).map(|v| json!(v)).collect::<Vec<_>>(), "setup");
    for n in 1..7 {
        let tail: Vec<Value> = serde_json::from_slice(&store.get_state_tail("s", n).unwrap().unwrap()).unwrap();
        assert_eq!(tail, all[7 - n..], "tail({n})");
    }
}
