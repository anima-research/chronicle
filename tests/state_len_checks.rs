use chronicle::{
    BranchId, StateIndex, StateOperation, StateRegistration, StateStrategy, Store, StoreConfig,
    StoreError, TreeEntry,
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

fn open(path: &Path) -> Store {
    let store = Store::open(config(path)).unwrap();
    store.set_auto_snapshot(false);
    store
}

fn create(path: &Path) -> Store {
    let store = Store::create(config(path)).unwrap();
    store.set_auto_snapshot(false);
    store
}

fn register(store: &Store, id: &str, strategy: StateStrategy) {
    store
        .register_state(StateRegistration {
            id: id.to_string(),
            strategy,
            initial_value: None,
        })
        .unwrap();
}

fn append_log() -> StateStrategy {
    StateStrategy::AppendLog {
        delta_snapshot_every: 1000,
        full_snapshot_every: 1000,
    }
}

fn append(store: &Store, id: &str, value: Value) {
    store
        .update_state(id, StateOperation::Append(serde_json::to_vec(&value).unwrap()))
        .unwrap();
}

fn items(store: &Store, id: &str) -> Value {
    serde_json::from_slice(&store.get_state(id).unwrap().unwrap()).unwrap()
}

fn reads(store: &Store) -> u64 {
    store.state_reconstruction_count()
}

fn state_bin(path: &Path) -> (StateIndex, Vec<u8>) {
    let bytes = fs::read(path.join("state.bin")).unwrap();
    assert_eq!(&bytes[0..5], b"STI\0\x02");
    let len = u64::from_le_bytes(bytes[5..13].try_into().unwrap()) as usize;
    (rmp_serde::from_slice(&bytes[13..13 + len]).unwrap(), bytes[13 + len..].to_vec())
}

fn write_state_bin(path: &Path, index: &StateIndex, checks: &[u8]) {
    let encoded = rmp_serde::to_vec(index).unwrap();
    let mut out = b"STI\0\x02".to_vec();
    out.extend_from_slice(&(encoded.len() as u64).to_le_bytes());
    out.extend_from_slice(&encoded);
    out.extend_from_slice(checks);
    fs::write(path.join("state.bin"), out).unwrap();
}

fn rewrite_state_bin(path: &Path, edit: impl FnOnce(&mut StateIndex)) {
    let (mut index, checks) = state_bin(path);
    edit(&mut index);
    write_state_bin(path, &index, &checks);
}

fn store_count(path: &Path, branch: BranchId, state: &str, count: usize) {
    rewrite_state_bin(path, |index| index.heads.get_mut(branch, state).unwrap().item_count = count);
}

fn as_a_release_without_checks_left_it(path: &Path) {
    let (index, _) = state_bin(path);
    write_state_bin(path, &index, &[]);
}

fn legacy_store(dir: &TempDir) -> (PathBuf, BranchId) {
    let path = dir.path().join("store");
    let store = create(&path);
    register(&store, "s", append_log());
    for i in 0..6 {
        append(&store, "s", json!(i));
    }
    store
        .update_state("s", StateOperation::Redact { start: 3, end: 9 })
        .unwrap();
    assert_eq!(items(&store, "s"), json!([0, 1, 2]), "setup: the value");
    let branch = store.current_branch().id;
    store.sync().unwrap();
    drop(store);
    store_count(&path, branch, "s", 0);
    as_a_release_without_checks_left_it(&path);
    (path, branch)
}

fn three_items(dir: &TempDir) -> (PathBuf, BranchId) {
    let path = dir.path().join("store");
    let store = create(&path);
    register(&store, "s", append_log());
    for i in 0..3 {
        append(&store, "s", json!(i));
    }
    let branch = store.current_branch().id;
    store.sync().unwrap();
    (path, branch)
}

#[test]
fn a_length_read_counts_a_legacy_heads_items_once() {
    let dir = TempDir::new().unwrap();
    let (path, _) = legacy_store(&dir);
    let store = open(&path);
    let before = reads(&store);
    assert_eq!(store.get_state_len("s").unwrap(), Some(3), "the items the state holds");
    assert_eq!(reads(&store), before + 1, "the count read the value once");
    store.clear_state_caches();
    assert_eq!(store.get_state_len("s").unwrap(), Some(3));
    assert_eq!(reads(&store), before + 1, "the second length read read nothing");
    assert_eq!(items(&store, "s"), json!([0, 1, 2]));
}

#[test]
fn a_counted_legacy_head_is_read_for_no_count_after_a_sync_and_a_reopen() {
    let dir = TempDir::new().unwrap();
    let (path, _) = legacy_store(&dir);
    let store = open(&path);
    assert_eq!(store.get_state_len("s").unwrap(), Some(3));
    store.sync().unwrap();
    drop(store);

    let store = open(&path);
    let before = reads(&store);
    assert_eq!(store.get_state_len("s").unwrap(), Some(3));
    let batch = store.append_many_to_state_json("s", &[json!("a")]).unwrap();
    assert_eq!(batch.first_ordinal, 3);
    assert_eq!(reads(&store), before, "the length read and the batch read nothing");
    assert_eq!(items(&store, "s"), json!([0, 1, 2, "a"]));
}

#[test]
fn a_length_read_after_a_callers_delta_snapshot_counts_the_items_it_restates() {
    let dir = TempDir::new().unwrap();
    let store = create(&dir.path().join("store"));
    register(&store, "log", append_log());
    store
        .update_state("log", StateOperation::DeltaSnapshot(serde_json::to_vec(&json!([1, 2])).unwrap()))
        .unwrap();
    assert_eq!(store.get_state_len("log").unwrap(), Some(2));
    append(&store, "log", json!(3));
    assert_eq!(store.get_state_len("log").unwrap(), Some(3));
    assert_eq!(items(&store, "log"), json!([1, 2, 3]));
}

#[test]
fn a_store_this_build_wrote_rebuilds_nothing_for_a_length_a_tail_or_an_append_after_a_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let store = Store::create(config(&path)).unwrap();
    register(
        &store,
        "messages",
        StateStrategy::AppendLog {
            delta_snapshot_every: 10,
            full_snapshot_every: 3,
        },
    );
    for i in 0..150 {
        append(&store, "messages", json!(format!("message_{i}")));
    }
    store.sync().unwrap();
    drop(store);

    let store = open(&path);
    let before = reads(&store);
    assert_eq!(store.get_state_len("messages").unwrap(), Some(150));
    let tail: Vec<String> = serde_json::from_slice(&store.get_state_tail("messages", 10).unwrap().unwrap()).unwrap();
    assert_eq!(tail.last().map(String::as_str), Some("message_149"));
    append(&store, "messages", json!("final"));
    assert_eq!(store.get_state_len("messages").unwrap(), Some(151));
    assert_eq!(reads(&store), before, "a length, a tail or an append rebuilt the state");
}

#[test]
fn the_appends_an_open_replays_keep_a_saved_check() {
    let dir = TempDir::new().unwrap();
    let (path, _) = legacy_store(&dir);
    let store = open(&path);
    assert_eq!(store.get_state_len("s").unwrap(), Some(3));
    store.sync().unwrap();
    let saved = fs::read(path.join("state.bin")).unwrap();
    assert!(!state_bin(&path).1.is_empty(), "setup: state.bin holds the checks");
    append(&store, "s", json!("a"));
    append(&store, "s", json!("b"));
    drop(store);
    fs::write(path.join("state.bin"), saved).unwrap();

    let store = open(&path);
    let before = reads(&store);
    assert_eq!(store.get_state_len("s").unwrap(), Some(5));
    assert_eq!(reads(&store), before, "the replayed appends kept the check");
    assert_eq!(items(&store, "s"), json!([0, 1, 2, "a", "b"]));
}

#[test]
fn a_saved_check_is_dropped_where_state_bin_holds_another_count() {
    let dir = TempDir::new().unwrap();
    let (path, branch) = three_items(&dir);
    store_count(&path, branch, "s", 0);

    let store = open(&path);
    let before = reads(&store);
    assert_eq!(store.get_state_len("s").unwrap(), Some(3), "the items, not the count in state.bin");
    assert_eq!(reads(&store), before + 1, "the head was counted");
}

#[test]
fn a_saved_check_is_dropped_where_its_head_has_moved() {
    let dir = TempDir::new().unwrap();
    let (path, branch) = legacy_store(&dir);
    let store = open(&path);
    assert_eq!(store.get_state_len("s").unwrap(), Some(3));
    store.sync().unwrap();
    let (_, older) = state_bin(&path);
    append(&store, "s", json!("x"));
    store.sync().unwrap();
    drop(store);
    let (mut index, _) = state_bin(&path);
    index.heads.get_mut(branch, "s").unwrap().item_count = 3;
    write_state_bin(&path, &index, &older);

    let store = open(&path);
    assert_eq!(store.get_state_len("s").unwrap(), Some(4), "the items, not the older check's count");
    assert_eq!(items(&store, "s"), json!([0, 1, 2, "x"]));
}

#[test]
fn checks_that_dont_read_leave_every_head_to_be_counted() {
    type Damage = fn(Vec<u8>) -> Vec<u8>;
    let damages: [(&str, Damage); 6] = [
        ("absent, as a release that reads the index alone saves state.bin", |_| Vec::new()),
        ("cut in their header", |checks| checks[..7].to_vec()),
        ("cut in their body", |checks| checks[..checks.len() - 1].to_vec()),
        ("of another version", |mut checks| {
            checks[4] = checks[4].wrapping_add(1);
            checks
        }),
        ("a body that doesn't decode", |checks| {
            let mut damaged = checks[..13].to_vec();
            damaged.extend(std::iter::repeat(0xc1).take(checks.len() - 13));
            damaged
        }),
        ("followed by more bytes", |mut checks| {
            checks.push(0);
            checks
        }),
    ];
    for (name, damage) in damages {
        let dir = TempDir::new().unwrap();
        let (path, _) = three_items(&dir);
        let (index, checks) = state_bin(&path);
        assert_eq!(&checks[..4], b"SCC\0", "{name}: setup, the sync saved the checks");
        write_state_bin(&path, &index, &damage(checks));

        let store = open(&path);
        let before = reads(&store);
        assert_eq!(store.get_state_len("s").unwrap(), Some(3), "{name}");
        assert_eq!(reads(&store), before + 1, "{name}: the head was counted");
    }
}

#[test]
fn a_branch_made_at_the_head_takes_the_check_and_one_given_a_stale_head_counts_it() {
    let dir = TempDir::new().unwrap();
    let (path, _) = legacy_store(&dir);
    let store = open(&path);

    let at = store.current_branch().head;
    store.create_branch_at("at-the-head", "main", at).unwrap();
    store.switch_branch("at-the-head").unwrap();
    assert_eq!(store.get_state_len("s").unwrap(), Some(3), "a branch given the stale head counts it");

    store.switch_branch("main").unwrap();
    assert_eq!(store.get_state_len("s").unwrap(), Some(3));
    store.create_branch("child", Some("main")).unwrap();
    store.switch_branch("child").unwrap();
    store.clear_state_caches();
    let before = reads(&store);
    assert_eq!(store.get_state_len("s").unwrap(), Some(3));
    assert_eq!(reads(&store), before, "the child took its parent's check");

    store.switch_branch("main").unwrap();
    let at = store.current_branch().head;
    store.create_branch_at("checked-at-the-head", "main", at).unwrap();
    store.switch_branch("checked-at-the-head").unwrap();
    store.clear_state_caches();
    let before = reads(&store);
    assert_eq!(store.get_state_len("s").unwrap(), Some(3));
    assert_eq!(reads(&store), before, "a branch made at a checked head took its parent's check");
}

#[test]
fn a_length_read_of_a_state_that_isnt_an_append_log_gives_the_stored_count_and_reads_nothing() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let branch = {
        let store = create(&path);
        register(&store, "tree", StateStrategy::Tree { delta_snapshot_every: 1000, full_snapshot_every: 1000 });
        register(&store, "whole", StateStrategy::Snapshot);
        let entry = TreeEntry { blob_hash: "0".repeat(64), size: 1, mode: 0o644 };
        for path in ["a", "b", "a"] {
            store
                .update_state("tree", StateOperation::TreeSet { path: path.to_string(), entry: entry.clone() })
                .unwrap();
        }
        store
            .update_state("whole", StateOperation::Set(serde_json::to_vec(&json!([1, 2])).unwrap()))
            .unwrap();
        append(&store, "unregistered", json!(1));
        append(&store, "unregistered", json!(2));
        store.sync().unwrap();
        store.current_branch().id
    };
    rewrite_state_bin(&path, |index| {
        index.heads.get_mut(branch, "whole").unwrap().item_count = 5;
        index.heads.get_mut(branch, "unregistered").unwrap().item_count = 7;
    });

    let store = open(&path);
    let before = reads(&store);
    assert_eq!(store.get_state_len("tree").unwrap(), Some(3), "the tree's estimate: an overwrite counts as an insert");
    assert_eq!(store.get_state_len("whole").unwrap(), Some(5));
    assert_eq!(store.get_state_len("unregistered").unwrap(), Some(7));
    assert_eq!(reads(&store), before, "no length read read a value");
}

#[test]
fn an_append_log_whose_value_isnt_an_array_gives_the_stored_count_and_isnt_read_again_after_a_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    {
        let store = create(&path);
        register(&store, "s", append_log());
        store.update_state("s", StateOperation::Append(b"not json".to_vec())).unwrap();
        store.sync().unwrap();
    }
    as_a_release_without_checks_left_it(&path);

    let store = open(&path);
    let before = reads(&store);
    assert_eq!(store.get_state_len("s").unwrap(), Some(1), "the stored count");
    assert_eq!(store.get_state_len("s").unwrap(), Some(1));
    assert_eq!(reads(&store), before + 1, "one attempt to count");
    store.sync().unwrap();
    drop(store);

    let store = open(&path);
    let before = reads(&store);
    assert_eq!(store.get_state_len("s").unwrap(), Some(1));
    assert_eq!(reads(&store), before, "the saved check says there is nothing to count");
    assert!(
        matches!(store.get_state_items_by_ordinal("s", &[0]), Err(StoreError::Deserialization(_))),
        "the saved check isn't exact, so the head record doesn't serve the last item"
    );
}

#[test]
fn a_length_read_whose_count_meets_a_damaged_record_throws_and_changes_nothing() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    {
        let store = create(&path);
        register(&store, "s", append_log());
        append(&store, "s", json!("first"));
        append(&store, "s", json!("damaged-in-the-chain"));
        append(&store, "s", json!("last"));
        store.sync().unwrap();
    }
    as_a_release_without_checks_left_it(&path);
    let store = open(&path);
    let records = store.stats().unwrap().record_count;
    damage(&path.join("records.log"), b"damaged-in-the-chain");

    for attempt in ["first", "second"] {
        match store.get_state_len("s") {
            Err(StoreError::ChecksumMismatch { .. }) => {}
            other => panic!("{attempt} length read: expected the checksum error, got {other:?}"),
        }
    }
    assert_eq!(store.stats().unwrap().record_count, records, "nothing written");
}

#[test]
fn with_no_write_between_the_length_is_the_next_items_ordinal() {
    let dir = TempDir::new().unwrap();
    let (path, _) = legacy_store(&dir);
    let store = open(&path);
    assert_eq!(store.get_state_len("s").unwrap(), Some(3));
    append(&store, "s", json!("x"));
    assert_eq!(store.get_state_item("s", 3).unwrap().as_deref(), Some(b"\"x\"".as_slice()));
    assert_eq!(store.get_state_len("s").unwrap(), Some(4), "the length after one append includes it");
    let batch = store.append_many_to_state_json("s", &[json!("y")]).unwrap();
    assert_eq!(batch.first_ordinal, 4);
    assert_eq!(store.get_state_len("s").unwrap(), Some(5));
}

fn damage(file: &Path, marker: &[u8]) {
    use std::io::{Seek, SeekFrom, Write};
    let bytes = fs::read(file).unwrap();
    let at = bytes
        .windows(marker.len())
        .position(|window| window == marker)
        .expect("the marker is in the file");
    let mut handle = fs::OpenOptions::new().write(true).open(file).unwrap();
    handle.seek(SeekFrom::Start(at as u64)).unwrap();
    handle.write_all(&[bytes[at] ^ 0x20]).unwrap();
}

#[test]
fn a_saved_exact_check_serves_a_batchs_last_item_from_its_record_after_a_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    {
        let store = create(&path);
        register(&store, "s", append_log());
        store.append_many_to_state_json("s", &[json!(0), json!(1), json!(2)]).unwrap();
        store.sync().unwrap();
    }
    let store = open(&path);
    store.clear_state_caches();
    let before = reads(&store);
    let last = store.get_state_items_by_ordinal("s", &[2]).unwrap();
    assert_eq!(last, vec![Some(serde_json::to_vec(&json!(2)).unwrap())]);
    assert_eq!(reads(&store), before, "the saved check is exact, so the batch's last item came from its record");
}

#[test]
fn an_item_read_at_a_stale_counts_last_index_counts_the_list_first() {
    let dir = TempDir::new().unwrap();
    let (path, branch) = three_items(&dir);
    store_count(&path, branch, "s", 1);
    as_a_release_without_checks_left_it(&path);
    let store = open(&path);
    assert_eq!(
        store.get_state_item("s", 0).unwrap(),
        Some(serde_json::to_vec(&json!(0)).unwrap()),
        "the item at index 0, not the head record's"
    );
    let before = reads(&store);
    assert_eq!(store.get_state_len("s").unwrap(), Some(3));
    assert_eq!(reads(&store), before, "the item read left the count checked");
}

#[test]
fn an_item_read_whose_count_meets_a_damaged_record_throws() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    {
        let store = create(&path);
        register(&store, "s", append_log());
        append(&store, "s", json!("first"));
        append(&store, "s", json!("damaged-in-the-chain"));
        append(&store, "s", json!("last"));
        store.sync().unwrap();
    }
    as_a_release_without_checks_left_it(&path);
    let store = open(&path);
    let records = store.stats().unwrap().record_count;
    damage(&path.join("records.log"), b"damaged-in-the-chain");

    match store.get_state_item("s", 2) {
        Err(StoreError::ChecksumMismatch { .. }) => {}
        other => panic!("an item read whose count meets a damaged record: expected the checksum error, got {other:?}"),
    }
    assert_eq!(store.stats().unwrap().record_count, records, "nothing written");
}
