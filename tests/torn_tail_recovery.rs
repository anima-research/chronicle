//! Crash-recovery tests: a torn (partial) tail record in the append-only log
//! must not brick the store on the next open.
//!
//! Covers finding 6.1 in UNTESTED-ROUTES-TEST-PLAN.md: previously a partial
//! final record made `Store::open` fail entirely, taking the whole agent
//! (history, checkpoints, lessons pointers) offline after any unclean kill.

use chronicle::{BranchId, RecordInput, Sequence, Store, StoreConfig, StoreError};
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
    // After branch-head reconciliation the head is clamped to the last
    // surviving record (N-1), so the next append is exactly N — no hole, no
    // reused sequence.
    let appended = store
        .append(RecordInput::raw("message", b"after recovery".to_vec()))
        .unwrap();
    assert_eq!(appended.sequence, Sequence(N));
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

/// Regression for the duplicate-sequence bug: a stale `branches.bin` (persisted
/// head older than the durable log) must NOT let the next append reuse a
/// sequence already occupied by a durable record. Reconciliation raises the
/// stale head to what the log actually holds.
#[test]
fn stale_branches_bin_does_not_reissue_durable_sequence() {
    let dir = TempDir::new().unwrap();
    let store_path = dir.path().join("store");
    let branches_path = store_path.join("branches.bin");

    // 1. Five records, synced: branches.bin now records head = 5, log holds 5.
    let store = Store::create(config(&store_path)).unwrap();
    for i in 1..=5u64 {
        store
            .append(RecordInput::raw("message", format!("first {}", i).into_bytes()))
            .unwrap();
    }
    store.sync().unwrap();

    // Snapshot the stale branches.bin (head = 5).
    let stale_branches = std::fs::read(&branches_path).unwrap();

    // 2. Five MORE records, synced: log + branches.bin now both at 10.
    for i in 6..=10u64 {
        store
            .append(RecordInput::raw("message", format!("second {}", i).into_bytes()))
            .unwrap();
    }
    store.sync().unwrap();
    drop(store);

    // 3. Simulate a crash where the log's 5 newer records reached disk but the
    //    branches.bin metadata flush did not: roll branches.bin back to head=5
    //    while leaving all 10 durable records in the log.
    std::fs::write(&branches_path, &stale_branches).unwrap();

    // 4. Reopen. Reconciliation must raise the head from the stale 5 to the
    //    log's actual 10 — so the next append is 11, not a duplicate 6.
    let store = Store::open_or_create(config(&store_path)).unwrap();

    // All ten durable records are still visible (nothing was dropped).
    let records = store.query_range(None, None, 1000, false, None).unwrap();
    assert_eq!(records.len(), 10, "all durable records must remain visible");

    let appended = store
        .append(RecordInput::raw("message", b"post-crash".to_vec()))
        .unwrap();
    assert_eq!(
        appended.sequence,
        Sequence(11),
        "append after a stale branches.bin must NOT reuse a durable sequence"
    );

    // No two records share a sequence on the branch.
    store.sync().unwrap();
    drop(store);
    let store = Store::open_or_create(config(&store_path)).unwrap();
    let records = store.query_range(None, None, 1000, false, None).unwrap();
    let mut seqs: Vec<u64> = records.iter().map(|r| r.sequence.0).collect();
    let before = seqs.len();
    seqs.sort_unstable();
    seqs.dedup();
    assert_eq!(before, seqs.len(), "no duplicate sequences may exist");
    assert_eq!(seqs, (1..=11).collect::<Vec<_>>(), "sequences 1..=11, contiguous");
}

/// Regression for the *branch-identity* variant of the duplicate-sequence bug:
/// a stale `branches.bin` also rolls back `next_id`, so a branch that was
/// created and written to — but lost before its metadata flushed — leaves
/// durable records under a BranchId that a naive `create_branch` would happily
/// re-issue. The reborn branch would then adopt the dead branch's records as
/// ghosts and reissue their sequences. Reconciliation must clamp `next_id`
/// above every BranchId the log holds.
#[test]
fn stale_next_id_does_not_reissue_branch_id_over_durable_records() {
    let dir = TempDir::new().unwrap();
    let store_path = dir.path().join("store");
    let branches_path = store_path.join("branches.bin");

    // 1. Three records on main, synced. Snapshot branches.bin at this baseline:
    //    main head = 3, next_id = 2, and NO "feature" branch yet.
    let store = Store::create(config(&store_path)).unwrap();
    for i in 1..=3u64 {
        store
            .append(RecordInput::raw("message", format!("main {}", i).into_bytes()))
            .unwrap();
    }
    store.sync().unwrap();
    let baseline_branches = std::fs::read(&branches_path).unwrap();

    // 2. Create "feature" (gets BranchId(2)), switch to it, append 3 records.
    //    These reach the durable log under BranchId(2), sequences 4..=6.
    let feature = store.create_branch("feature", None).unwrap();
    assert_eq!(feature.id, BranchId(2));
    store.switch_branch("feature").unwrap();
    for i in 1..=3u64 {
        store
            .append(RecordInput::raw("message", format!("feature {}", i).into_bytes()))
            .unwrap();
    }
    store.sync().unwrap();
    drop(store);

    // 3. Simulate a crash where feature's records reached the log but the
    //    metadata flush that would have recorded the branch (and bumped
    //    next_id) did not: roll branches.bin back to the baseline. The log
    //    still holds BranchId(2)'s three durable records.
    std::fs::write(&branches_path, &baseline_branches).unwrap();

    // 4. Reopen. Reconciliation must clamp next_id above BranchId(2) and flag it
    //    as orphaned.
    let store = Store::open_or_create(config(&store_path)).unwrap();
    let recovery = store.recovery();
    assert!(recovery.is_none(), "log tail was intact; only metadata was rolled back");

    // 5. The next branch MUST NOT reuse the dead BranchId(2) — it gets BranchId(3).
    let reborn = store.create_branch("other", None).unwrap();
    assert_ne!(
        reborn.id,
        BranchId(2),
        "must not reissue the dead branch's id and adopt its durable records"
    );
    assert_eq!(reborn.id, BranchId(3), "next_id clamped above every BranchId in the log");

    // 6. "other" must be ghost-free: it does NOT see feature's durable records.
    store.switch_branch("other").unwrap();
    let ghosts = store.query_range(None, None, 1000, false, None).unwrap();
    assert!(
        ghosts.is_empty(),
        "a freshly created branch must not adopt another branch's durable records"
    );

    // 7. Appending on "other" lands on BranchId(3) at a fresh sequence — it does
    //    not reissue a sequence already occupied by feature's durable records.
    let appended = store
        .append(RecordInput::raw("message", b"on-other".to_vec()))
        .unwrap();
    assert_eq!(appended.branch, BranchId(3));
    assert_eq!(
        appended.sequence,
        Sequence(4),
        "fresh branch appends on its own id; no durable sequence is reissued"
    );

    // 8. main still holds exactly its three original records.
    store.switch_branch("main").unwrap();
    let main_records = store.query_range(None, None, 1000, false, None).unwrap();
    assert_eq!(main_records.len(), 3, "main's durable records are untouched");
}

/// A torn `payload_len` field (e.g. 0xFFFFFFFF from a tear) must be rejected on
/// the recovery scan WITHOUT attempting a multi-gigabyte allocation. This
/// reproduces the OOM-during-recovery crash-loop the length guard prevents.
#[test]
fn torn_payload_len_is_rejected_without_huge_alloc() {
    let dir = TempDir::new().unwrap();
    let store_path = dir.path().join("store");
    create_store_with_records(&store_path, 3);

    // Corrupt the LAST record's payload_len field to 0xFFFFFFFF and truncate
    // the tail, making it a torn-tail candidate whose length field, if trusted,
    // would request a ~4 GiB allocation during the open scan.
    let log_path = store_path.join("records.log");
    let mut file = OpenOptions::new().read(true).write(true).open(&log_path).unwrap();
    let mut contents = Vec::new();
    file.read_to_end(&mut contents).unwrap();

    // Find the last record's payload text ("payload 3") and overwrite the
    // 4-byte payload_len that immediately precedes it with 0xFFFFFFFF.
    let needle = b"payload 3";
    let payload_pos = contents
        .windows(needle.len())
        .rposition(|w| w == needle)
        .expect("last record payload not found") as u64;
    let len_field_pos = payload_pos - 4; // payload_len sits right before payload
    file.seek(SeekFrom::Start(len_field_pos)).unwrap();
    file.write_all(&0xFFFF_FFFFu32.to_le_bytes()).unwrap();
    // Drop the actual payload bytes so this is unambiguously a torn tail.
    file.set_len(payload_pos).unwrap();
    file.sync_all().unwrap();
    drop(file);

    // Open must succeed by recovering the two intact records — and must NOT
    // try to allocate 4 GiB (the length guard rejects the field, classifying
    // the record as a torn tail). If it OOMs, this test crashes instead.
    let store = Store::open_or_create(config(&store_path)).unwrap();
    let records = store.query_range(None, None, 1000, false, None).unwrap();
    assert_eq!(records.len(), 2, "two intact records recovered, torn tail dropped");
}
