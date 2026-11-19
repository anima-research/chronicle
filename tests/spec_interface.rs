use chronicle::{RecordInput, Store, StoreConfig};
use serde_json::json;
use tempfile::TempDir;

#[test]
fn r1_store_with_store_config_and_record_input_is_the_crates_entry() {
    let dir = TempDir::new().unwrap();
    let config = StoreConfig {
        path: dir.path().join("store"),
        ..Default::default()
    };
    let store = Store::create(config).unwrap();
    let record = store.append(RecordInput::json("note", &json!({ "n": 1 })).unwrap()).unwrap();
    assert_eq!(store.get_record(record.id).unwrap().unwrap().payload, br#"{"n":1}"#);
}

#[test]
#[allow(unused_imports)]
fn r2_the_crate_makes_its_modules_public() {
    use chronicle::{blobs, branches, error, records, state, store, subscriptions, types, wal};
}
