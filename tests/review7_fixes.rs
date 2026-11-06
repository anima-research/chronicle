//! Fixes from the seventh review of the batched calls (chronicle#62).
//!
//! - A batched append counts a state's items before it takes its ordinals,
//!   when the session hasn't counted them: the item count a head holds can
//!   be off, as one a release before 0.5.0 stored after a Redact past the
//!   end, or one a caller's `DeltaSnapshot` left. The head-record fast paths
//!   of the batched calls serve only a count the session has checked.
//! - The point lookups report malformed state bytes as the error they are,
//!   and answer `None` only for a value that is one JSON value but not an
//!   array.
//! - The batched identity append refuses one key for both the id and the
//!   sequence.

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

#[test]
fn a_point_lookup_reports_malformed_state_bytes() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    for (n, bytes) in [&b"{"[..], b"{\"a\": ", b"123abc", b"[1, 2", b"tru"].into_iter().enumerate() {
        let id = format!("bad{n}");
        store
            .register_state(StateRegistration {
                id: id.clone(),
                strategy: StateStrategy::Snapshot,
                initial_value: None,
            })
            .unwrap();
        store.update_state(&id, StateOperation::Set(bytes.to_vec())).unwrap();
        let shown = String::from_utf8_lossy(bytes);
        store.clear_state_caches();
        assert!(
            matches!(store.get_state_item(&id, 0), Err(StoreError::Deserialization(_))),
            "{shown}: {:?}",
            store.get_state_item(&id, 0)
        );
        store.clear_state_caches();
        assert!(
            matches!(
                store.get_state_items_by_ordinal(&id, &[0, 1]),
                Err(StoreError::Deserialization(_))
            ),
            "{shown}"
        );
    }

    // One JSON value that isn't an array still has no item at any index.
    for (n, value) in [json!({"a": 1}), json!("text"), json!(7), json!(null)].into_iter().enumerate() {
        let id = format!("other{n}");
        store
            .register_state(StateRegistration {
                id: id.clone(),
                strategy: StateStrategy::Snapshot,
                initial_value: None,
            })
            .unwrap();
        store
            .update_state(&id, StateOperation::Set(serde_json::to_vec(&value).unwrap()))
            .unwrap();
        store.clear_state_caches();
        assert_eq!(store.get_state_item(&id, 0).unwrap(), None, "{value}");
        assert_eq!(store.get_state_items_by_ordinal(&id, &[0]).unwrap(), [None], "{value}");
    }
}

#[test]
fn an_identity_batch_with_one_key_for_id_and_sequence_is_refused_and_writes_nothing() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    register_log(&store, "log");
    let manifest = fs::read(dir.path().join("MANIFEST")).unwrap();
    let head = store.current_branch().head;

    match store.append_many_to_state_json_with_identity("log", &[json!({"x": 1})], "identity", "identity") {
        Err(StoreError::InvalidOperation(message)) => {
            assert!(message.contains("identity"), "{message}")
        }
        other => panic!("expected InvalidOperation, got {:?}", other.map(|b| b.record.id)),
    }
    assert_eq!(store.current_branch().head, head, "no record");
    assert_eq!(store.get_state_len("log").unwrap(), None);
    assert_eq!(fs::read(dir.path().join("MANIFEST")).unwrap(), manifest, "no format mark");

    // Distinct keys still go through.
    let batch = store
        .append_many_to_state_json_with_identity("log", &[json!({"x": 1})], "id", "seq")
        .unwrap();
    assert_eq!(batch.first_ordinal, 0);
}

/// Open syncs the directory of a store whose MANIFEST names format features.
/// Where the directory can't be opened to sync it, as one this process may
/// write and search but not list, the store still opens, as a save there
/// goes without the sync.
#[cfg(unix)]
#[test]
fn a_marked_store_in_a_directory_that_cant_be_listed_still_opens() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    {
        let store = Store::create(config(&path)).unwrap();
        register_log(&store, "s");
        store.append_many_to_state_json("s", &[json!(1)]).unwrap();
        store.sync().unwrap();
    }
    fs::set_permissions(&path, fs::Permissions::from_mode(0o300)).unwrap();
    let listable = fs::read_dir(&path).is_ok();
    let opened = Store::open(config(&path)).map(|store| value(&store, "s"));
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    if listable {
        // Running as root, where the mode doesn't stop a listing.
        return;
    }
    assert_eq!(opened.unwrap(), json!([1]));
}

/// A MANIFEST that is a symbolic link into another directory is marked
/// there: the mark's rename is made in the directory holding the file the
/// link leads to, and that is the directory open syncs for a marked store.
/// The sync itself can't be seen from here; the store opens and reads its
/// batch, and the link is still a link.
#[cfg(unix)]
#[test]
fn a_marked_store_whose_manifest_links_into_another_directory_opens() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let elsewhere = dir.path().join("elsewhere");
    fs::create_dir(&elsewhere).unwrap();
    drop(Store::create(config(&path)).unwrap());
    fs::rename(path.join("MANIFEST"), elsewhere.join("MANIFEST")).unwrap();
    std::os::unix::fs::symlink(elsewhere.join("MANIFEST"), path.join("MANIFEST")).unwrap();
    {
        let store = Store::open(config(&path)).unwrap();
        register_log(&store, "s");
        store.append_many_to_state_json("s", &[json!(1)]).unwrap();
        store.sync().unwrap();
    }
    assert!(fs::symlink_metadata(path.join("MANIFEST")).unwrap().file_type().is_symlink());
    assert_eq!(fs::read(elsewhere.join("MANIFEST")).unwrap()[4], 2, "marked where the link leads");

    let store = Store::open(config(&path)).unwrap();
    assert_eq!(value(&store, "s"), json!([1]));
}

/// Flips one byte of the first copy of `marker` in `file`, through a handle
/// of its own, as damage from outside the store would.
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

/// A batch whose count reads a record of the state's chain that fails its
/// checksum is refused with that error and writes nothing. Open checks
/// every record's checksum, so the damage is made after it. A value that
/// reads and isn't an array is another matter: the batch takes the stored
/// count (above).
#[test]
fn a_batch_whose_count_meets_a_damaged_record_is_refused() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    {
        let store = Store::create(config(&path)).unwrap();
        store.set_auto_snapshot(false);
        register_log(&store, "s");
        append(&store, "s", json!("first"));
        append(&store, "s", json!("damaged-in-the-chain"));
        append(&store, "s", json!("last"));
        store.sync().unwrap();
    }
    let store = open(&path);
    let head = store.current_branch().head;
    let records = store.stats().unwrap().record_count;
    damage(&path.join("records.log"), b"damaged-in-the-chain");

    match store.append_many_to_state_json("s", &[json!(1)]) {
        Err(StoreError::ChecksumMismatch { .. }) => {}
        other => panic!(
            "expected the checksum error, got {:?}",
            other.map(|batch| batch.first_ordinal)
        ),
    }
    assert_eq!(store.current_branch().head, head, "no record");
    assert_eq!(store.stats().unwrap().record_count, records);
    assert_eq!(store.get_state_len("s").unwrap(), Some(3), "the stored count, unchanged");
}
