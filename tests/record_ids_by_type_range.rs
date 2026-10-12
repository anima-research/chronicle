//! `get_records_by_type_range` answers from the type's ID list by binary
//! search, which is only right while that list is in ascending ID order. These
//! tests hold it against a plain filter of `get_records_by_type` across what
//! can reorder or reissue IDs: interleaved types, a reopen, and a torn tail
//! whose truncated IDs are handed out again.

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

/// Every range answer equals the filter of the whole list it stands for.
fn assert_ranges_match_the_list(store: &Store, record_type: &str) {
    let all: Vec<u64> = store
        .get_records_by_type(record_type)
        .into_iter()
        .map(|id| id.0)
        .collect();
    assert!(
        all.windows(2).all(|pair| pair[0] < pair[1]),
        "{record_type} IDs are in ascending order: {all:?}"
    );
    let max = all.last().copied().unwrap_or(0) + 2;
    for after in std::iter::once(None).chain((0..=max).map(Some)) {
        for before in std::iter::once(None).chain((0..=max).map(Some)) {
            let within: Vec<u64> = all
                .iter()
                .copied()
                .filter(|id| after.map_or(true, |a| *id > a) && before.map_or(true, |b| *id < b))
                .collect();
            for limit in [None, Some(0), Some(1), Some(3)] {
                for reverse in [false, true] {
                    let mut expected = within.clone();
                    if reverse {
                        expected.reverse();
                    }
                    if let Some(limit) = limit {
                        expected.truncate(limit);
                    }
                    let got: Vec<u64> = store
                        .get_records_by_type_range(
                            record_type,
                            after.map(RecordId),
                            before.map(RecordId),
                            limit,
                            reverse,
                        )
                        .into_iter()
                        .map(|id| id.0)
                        .collect();
                    assert_eq!(
                        got, expected,
                        "{record_type}: after {after:?}, before {before:?}, limit {limit:?}, reverse {reverse}"
                    );
                }
            }
        }
    }
}

#[test]
fn ranges_match_the_type_list_with_interleaved_types_and_across_a_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    {
        let store = Store::create(config(&path)).unwrap();
        for i in 0..12 {
            append(&store, if i % 3 == 0 { "layout" } else { "message" }, i);
        }
        assert_ranges_match_the_list(&store, "layout");
        assert_ranges_match_the_list(&store, "message");
        store.sync().unwrap();
    }
    // Reopened, the index is rebuilt from the log; appends carry on from there.
    let store = Store::open_or_create(config(&path)).unwrap();
    for i in 12..18 {
        append(&store, if i % 2 == 0 { "layout" } else { "message" }, i);
    }
    assert_ranges_match_the_list(&store, "layout");
    assert_ranges_match_the_list(&store, "message");
}

#[test]
fn ranges_match_the_type_list_after_a_torn_tail_reissues_ids() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let (newest_layout, durable_layout) = {
        let store = Store::create(config(&path)).unwrap();
        let mut durable = RecordId(0);
        for i in 0..10 {
            let id = append(&store, if i % 2 == 0 { "layout" } else { "message" }, i);
            if i % 2 == 0 {
                durable = id;
            }
        }
        store.sync().unwrap();
        // Two more layouts; the second is torn below, as a crash mid-append
        // would leave it.
        append(&store, "layout", 10);
        let newest = append(&store, "layout", 11);
        (newest, durable)
    };
    // A crash mid-append: the final record's bytes are cut short.
    let log_path = path.join("records.log");
    let len = std::fs::metadata(&log_path).unwrap().len();
    let file = OpenOptions::new().write(true).open(&log_path).unwrap();
    file.set_len(len - 5).unwrap();
    file.sync_all().unwrap();
    drop(file);

    let store = Store::open_or_create(config(&path)).unwrap();
    assert!(store.recovery().is_some(), "the torn tail was recovered");
    let layouts = store.get_records_by_type("layout");
    assert!(!layouts.contains(&newest_layout), "the torn record is gone");
    assert!(
        layouts.contains(&durable_layout),
        "every synced record survives"
    );
    // The torn record's ID is handed out again, to a record of another type.
    assert_eq!(
        append(&store, "message", 12),
        newest_layout,
        "the torn record's ID is reissued"
    );
    append(&store, "layout", 13);
    assert_ranges_match_the_list(&store, "layout");
    assert_ranges_match_the_list(&store, "message");
    // A bound that is a record of another type, or no record, still bounds.
    let after_reissued =
        store.get_records_by_type_range("layout", Some(newest_layout), None, None, false);
    assert_eq!(
        after_reissued.len(),
        1,
        "only the layout appended after the reissued ID"
    );
}

#[test]
fn concurrent_appends_keep_each_type_list_in_order() {
    // The order rests on the store's write lock, held across the log append
    // and the index add at both append sites (`append` and `update_state`).
    // This test races `append`: appends from many threads must still leave
    // each type's list ascending, and its ranges right.
    let dir = TempDir::new().unwrap();
    let store = Arc::new(Store::create(config(&dir.path().join("store"))).unwrap());
    let threads: Vec<_> = (0..4u64)
        .map(|t| {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                for i in 0..12u64 {
                    append(
                        &store,
                        if (t + i) % 2 == 0 {
                            "layout"
                        } else {
                            "message"
                        },
                        t * 100 + i,
                    );
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(
        store.get_records_by_type("layout").len() + store.get_records_by_type("message").len(),
        48
    );
    assert_ranges_match_the_list(&store, "layout");
    assert_ranges_match_the_list(&store, "message");
}

#[test]
fn ranges_cover_state_updates_before_and_after_a_reopen() {
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
        assert!(store.get_records_by_type("state_update").len() >= 8);
        assert_ranges_match_the_list(&store, "state_update");
        store.sync().unwrap();
    }
    let store = Store::open_or_create(config(&path)).unwrap();
    assert_ranges_match_the_list(&store, "state_update");
    assert_ranges_match_the_list(&store, "message");
}

#[test]
fn a_cursor_kept_across_a_torn_tail_skips_the_record_that_reuses_its_id() {
    // IDs lost to a torn tail are issued again. A reader that kept a cursor
    // at a lost ID, for the same type, passes over the record that reuses it:
    // the hazard the changelog names for cursors kept across a crash.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let lost = {
        let store = Store::create(config(&path)).unwrap();
        for i in 0..4 {
            append(&store, "layout", i);
        }
        store.sync().unwrap();
        append(&store, "layout", 4)
    };
    // A reader paging layouts saw the last one and kept it as its cursor.
    let cursor = lost;
    let log_path = path.join("records.log");
    let len = std::fs::metadata(&log_path).unwrap().len();
    let file = OpenOptions::new().write(true).open(&log_path).unwrap();
    file.set_len(len - 5).unwrap();
    file.sync_all().unwrap();
    drop(file);

    let store = Store::open_or_create(config(&path)).unwrap();
    assert!(store.recovery().is_some(), "the torn tail was recovered");
    let reissued = append(&store, "layout", 5);
    assert_eq!(
        reissued, lost,
        "the lost ID is issued again, to the same type"
    );
    let next = append(&store, "layout", 6);
    assert_eq!(
        store.get_records_by_type_range("layout", Some(cursor), None, None, false),
        vec![next],
        "the record that reused the cursor's ID is passed over"
    );
    assert_ranges_match_the_list(&store, "layout");
}
