//! Crash-recovery tests: a torn (partial) tail record in the append-only log
//! must not brick the store on the next open.
//!
//! Covers finding 6.1 in UNTESTED-ROUTES-TEST-PLAN.md: previously a partial
//! final record made `Store::open` fail entirely, taking the whole agent
//! (history, checkpoints, lessons pointers) offline after any unclean kill.

use chronicle::{RecordInput, Sequence, Store, StoreConfig, StoreError};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use tempfile::TempDir;

fn config(path: &Path) -> StoreConfig {
    StoreConfig {
        path: path.to_path_buf(),
        blob_cache_size: 100,
        create_if_missing: true,
    }
}

/// Append N raw records to a fresh store, then drop it (releasing the lock).
fn create_store_with_records(store_path: &Path, n: u64) {
    let store = Store::create(config(store_path)).unwrap();
    for i in 1..=n {
        store
            .append(RecordInput::raw(
                "message",
                format!("payload {}", i).into_bytes(),
            ))
            .unwrap();
    }
    store.sync().unwrap();
    drop(store);
}

#[test]
fn torn_tail_record_is_recovered_on_open() {
    let dir = TempDir::new().unwrap();
    let store_path = dir.path().join("store");
    const N: u64 = 10;

    create_store_with_records(&store_path, N);

    // Simulate a torn write: chop a few bytes off the end of the record log,
    // as a power loss / OOM-kill mid-append would.
    let log_path = store_path.join("records.log");
    let len = std::fs::metadata(&log_path).unwrap().len();
    let file = OpenOptions::new().write(true).open(&log_path).unwrap();
    file.set_len(len - 5).unwrap();
    file.sync_all().unwrap();
    drop(file);

    // Reopen: must succeed (previously failed with a checksum/IO error).
    let store = Store::open_or_create(config(&store_path)).unwrap();

    // The first N-1 records survive intact.
    let records = store
        .query_range(None, None, 1000, false, None)
        .unwrap();
    assert_eq!(records.len(), (N - 1) as usize);
    for (i, record) in records.iter().enumerate() {
        assert_eq!(record.sequence, Sequence((i + 1) as u64));
        assert_eq!(record.payload, format!("payload {}", i + 1).into_bytes());
    }

    // The log file was truncated back to the last valid record boundary.
    let new_len = std::fs::metadata(&log_path).unwrap().len();
    assert!(new_len < len - 5, "log file should have been truncated");

    // The store keeps working: new appends land and survive a clean reopen.
    let appended = store
        .append(RecordInput::raw("message", b"after recovery".to_vec()))
        .unwrap();
    assert!(appended.sequence > Sequence(N - 1));
    store.sync().unwrap();
    drop(store);

    let store = Store::open_or_create(config(&store_path)).unwrap();
    let records = store
        .query_range(None, None, 1000, false, None)
        .unwrap();
    assert_eq!(records.len(), N as usize); // N-1 survivors + 1 new
    assert_eq!(records.last().unwrap().payload, b"after recovery".to_vec());
}

#[test]
fn torn_tail_recovery_is_repeatable_across_crashes() {
    let dir = TempDir::new().unwrap();
    let store_path = dir.path().join("store");
    create_store_with_records(&store_path, 5);

    let log_path = store_path.join("records.log");

    // Two successive crash/tear/reopen cycles.
    for expected_after in [4usize, 3usize] {
        let len = std::fs::metadata(&log_path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&log_path).unwrap();
        file.set_len(len - 3).unwrap();
        drop(file);

        let store = Store::open_or_create(config(&store_path)).unwrap();
        let records = store
            .query_range(None, None, 1000, false, None)
            .unwrap();
        assert_eq!(records.len(), expected_after);
        drop(store);
    }
}

#[test]
fn midlog_corruption_still_fails_open() {
    let dir = TempDir::new().unwrap();
    let store_path = dir.path().join("store");
    create_store_with_records(&store_path, 5);

    // Corrupt a byte inside the SECOND record's payload region (framing
    // intact, checksum broken) — valid records follow, so this is mid-log
    // corruption and must NOT be silently dropped.
    let log_path = store_path.join("records.log");
    // Record layout prefix: magic(4)+ver(1)+flags(1)+id(8)+seq(8)+branch(8)
    //   +ts(8)+type_len(2)+type(7 for "message")+enc(1)+payload_len(4) = 52,
    // then payload ("payload 1" = 9 bytes) + caused_by(2) + linked_to(2)
    // + checksum(4) => record 1 spans [0, 69); record 2's payload starts at
    // 69 + 52 = 121. Rather than hardcoding, flip a byte a few bytes into
    // record 2's payload by scanning for its payload text.
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&log_path)
        .unwrap();
    let mut contents = Vec::new();
    file.read_to_end(&mut contents).unwrap();
    let needle = b"payload 2";
    let pos = contents
        .windows(needle.len())
        .position(|w| w == needle)
        .expect("record 2 payload not found in log") as u64;
    file.seek(SeekFrom::Start(pos)).unwrap();
    file.write_all(b"POISONED!").unwrap();
    file.sync_all().unwrap();
    drop(file);

    let err = match Store::open_or_create(config(&store_path)) {
        Ok(_) => panic!("expected open to fail on mid-log corruption"),
        Err(e) => e,
    };
    assert!(
        matches!(err, StoreError::Corruption(_)),
        "expected Corruption error for mid-log damage, got: {:?}",
        err
    );

    // And the log must not have been truncated behind the operator's back.
    let needle_still_there = {
        let mut f = OpenOptions::new().read(true).open(&log_path).unwrap();
        let mut c = Vec::new();
        f.read_to_end(&mut c).unwrap();
        c.windows(9).any(|w| w == b"payload 5")
    };
    assert!(needle_still_there, "mid-log corruption must not truncate the log");
}
