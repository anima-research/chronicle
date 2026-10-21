//! `get_records_by_type` (`getRecordIdsByType` in Node) lists a type's record
//! IDs in log order, which is ascending ID order, and context-manager relies on
//! it: the last ID is the newest, and IDs past a count it has seen are new.
//! These tests hold that order across what could break it: interleaved types,
//! a reopen, appends and state updates from many threads, the state-update
//! append path, and a torn tail whose dropped ID is issued again.

use chronicle::types::{StateOperation, StateRegistration, StateStrategy};
use chronicle::{RecordId, RecordInput, Store, StoreConfig};
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

fn config(path: &Path) -> StoreConfig {
    StoreConfig {
        path: path.to_path_buf(),
        blob_cache_size: 100,
        create_if_missing: true,
    }
}

fn append(store: &Store, record_type: &str, i: u64) -> RecordId {
    store
        .append(RecordInput::raw(
            record_type,
            format!("{record_type} {i}").into_bytes(),
        ))
        .unwrap()
        .id
}

/// The type's list is strictly ascending, and holds `expected` IDs.
fn assert_ascending(store: &Store, record_type: &str, expected: usize) {
    let ids: Vec<u64> = store
        .get_records_by_type(record_type)
        .into_iter()
        .map(|id| id.0)
        .collect();
    assert_eq!(ids.len(), expected, "{record_type}: {ids:?}");
    assert!(
        ids.windows(2).all(|w| w[0] < w[1]),
        "{record_type} IDs out of ascending order: {ids:?}"
    );
}

#[test]
fn type_lists_ascend_with_interleaved_types_and_across_a_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    {
        let store = Store::create(config(&path)).unwrap();
        for i in 0..12 {
            append(&store, if i % 3 == 0 { "layout" } else { "message" }, i);
        }
        assert_ascending(&store, "layout", 4);
        assert_ascending(&store, "message", 8);
        store.sync().unwrap();
    }
    // Reopened, the index is rebuilt from the log; appends carry on from there.
    let store = Store::open_or_create(config(&path)).unwrap();
    assert_ascending(&store, "layout", 4);
    assert_ascending(&store, "message", 8);
    for i in 12..18 {
        append(&store, if i % 2 == 0 { "layout" } else { "message" }, i);
    }
    assert_ascending(&store, "layout", 7);
    assert_ascending(&store, "message", 11);
}

#[test]
fn type_lists_ascend_after_a_torn_tail_issues_its_id_again() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let newest_layout = {
        let store = Store::create(config(&path)).unwrap();
        for i in 0..10 {
            append(&store, if i % 2 == 0 { "layout" } else { "message" }, i);
        }
        store.sync().unwrap();
        // Two more layouts; the second is torn below, as a crash mid-append
        // would leave it.
        append(&store, "layout", 10);
        append(&store, "layout", 11)
    };
    let log_path = path.join("records.log");
    let len = std::fs::metadata(&log_path).unwrap().len();
    let file = OpenOptions::new().write(true).open(&log_path).unwrap();
    file.set_len(len - 5).unwrap();
    file.sync_all().unwrap();
    drop(file);

    let store = Store::open_or_create(config(&path)).unwrap();
    assert!(store.recovery().is_some(), "the torn tail was recovered");
    assert!(
        !store.get_records_by_type("layout").contains(&newest_layout),
        "the torn record is gone"
    );
    assert_ascending(&store, "layout", 6);
    // The torn record's ID is issued again, here to a record of another type,
    // and each list still ascends.
    assert_eq!(
        append(&store, "message", 12),
        newest_layout,
        "the torn record's ID is issued again"
    );
    append(&store, "layout", 13);
    assert_ascending(&store, "layout", 7);
    assert_ascending(&store, "message", 6);
    store.sync().unwrap();
    drop(store);
    // Reopened, the records written past the truncation point are read back
    // in order.
    let store = Store::open_or_create(config(&path)).unwrap();
    assert_ascending(&store, "layout", 7);
    assert_ascending(&store, "message", 6);
}

#[test]
fn concurrent_appends_keep_each_type_list_ascending() {
    // The order rests on the store's write lock, held across the log append
    // and the index add at both append sites (`append` and `update_state`).
    // A race test: it catches a missing lock only when two threads switch in
    // the gap between those two steps, so it's a smoke test for the lock.
    let dir = TempDir::new().unwrap();
    let store = Arc::new(Store::create(config(&dir.path().join("store"))).unwrap());
    let threads: Vec<_> = (0..4u64)
        .map(|t| {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                for i in 0..12u64 {
                    append(
                        &store,
                        if (t + i) % 2 == 0 { "layout" } else { "message" },
                        t * 100 + i,
                    );
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_ascending(&store, "layout", 24);
    assert_ascending(&store, "message", 24);
}

#[test]
fn concurrent_state_updates_keep_the_state_update_list_ascending() {
    // `update_state` is the other append site; it too holds the write lock
    // across the log append and the index add. A smoke test for that lock,
    // as the one above is for `append`'s.
    let dir = TempDir::new().unwrap();
    let store = Arc::new(Store::create(config(&dir.path().join("store"))).unwrap());
    for t in 0..4u64 {
        store
            .register_state(StateRegistration {
                id: format!("log-{t}"),
                strategy: StateStrategy::AppendLog {
                    delta_snapshot_every: 100,
                    full_snapshot_every: 100,
                },
                initial_value: None,
            })
            .unwrap();
    }
    let threads: Vec<_> = (0..4u64)
        .map(|t| {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                for i in 0..12u64 {
                    store
                        .update_state(
                            &format!("log-{t}"),
                            StateOperation::Append(format!("{{\"i\":{i}}}").into_bytes()),
                        )
                        .unwrap();
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_ascending(&store, "state_update", 48);
}

#[test]
fn state_update_list_ascends_before_and_after_a_reopen() {
    // State updates reach the type index through their own append path.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    {
        let store = Store::create(config(&path)).unwrap();
        store
            .register_state(StateRegistration {
                id: "log".to_string(),
                strategy: StateStrategy::AppendLog {
                    delta_snapshot_every: 100,
                    full_snapshot_every: 100,
                },
                initial_value: None,
            })
            .unwrap();
        for i in 0..8u64 {
            store
                .update_state(
                    "log",
                    StateOperation::Append(format!("{{\"i\":{i}}}").into_bytes()),
                )
                .unwrap();
            append(&store, "message", i);
        }
        // One state_update record per update: registration writes none.
        assert_ascending(&store, "state_update", 8);
        store.sync().unwrap();
    }
    let store = Store::open_or_create(config(&path)).unwrap();
    assert_ascending(&store, "state_update", 8);
    assert_ascending(&store, "message", 8);
}
