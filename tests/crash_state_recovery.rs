//! Process-crash recovery: the child exits without running Store::drop.
//! These tests cover process death, not storage-device/power-loss behavior.

use chronicle::{
    BranchId, FieldIndexKind, Hash, PayloadEncoding, RecordId, RecordInput, RecordLog, Sequence,
    StateOperation, StateRegistration, StateStrategy, StateUpdateRecord, Store, StoreConfig,
    StoreError, Timestamp, TreeEntry,
};
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn config(path: &Path) -> StoreConfig {
    StoreConfig {
        path: path.to_owned(),
        ..Default::default()
    }
}

/// Opens a store in the test's own process. A child that a concurrent test
/// spawns shares this process's open files until it execs, the LOCK of any
/// store open here among them, so a store this process closed a moment ago
/// can still be locked for that moment.
fn open_store(path: &Path) -> chronicle::Result<Store> {
    retry_locked(|| Store::open(config(path)))
}

fn retry_locked(open: impl Fn() -> chronicle::Result<Store>) -> chronicle::Result<Store> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match open() {
            Err(StoreError::Locked) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5))
            }
            result => return result,
        }
    }
}

fn register(store: &Store, id: &str, strategy: StateStrategy) {
    store
        .register_state(StateRegistration {
            id: id.into(),
            strategy,
            initial_value: None,
        })
        .unwrap();
}

fn log_strategy() -> StateStrategy {
    StateStrategy::AppendLog {
        delta_snapshot_every: 2,
        full_snapshot_every: 2,
    }
}

fn append(store: &Store, id: &str, value: Value) {
    store
        .update_state(
            id,
            StateOperation::Append(serde_json::to_vec(&value).unwrap()),
        )
        .unwrap();
}

fn append_many(store: &Store, id: &str, values: &[Value]) {
    store
        .update_state(
            id,
            StateOperation::AppendMany(values.iter().map(|v| serde_json::to_vec(v).unwrap()).collect()),
        )
        .unwrap();
}

fn set(store: &Store, id: &str, value: Value) {
    store
        .update_state(id, StateOperation::Set(serde_json::to_vec(&value).unwrap()))
        .unwrap();
}

fn value(store: &Store, id: &str) -> Value {
    store
        .get_state(id)
        .unwrap()
        .map(|bytes| serde_json::from_slice(&bytes).unwrap())
        .unwrap_or(Value::Null)
}

fn observe(store: &Store) -> Value {
    let states: Vec<_> = ["items", "files", "settings"]
        .iter()
        .map(|id| {
            json!({
                "id": id,
                "value": value(store, id),
                "len": store.get_state_len(id).unwrap(),
                "snapshot_needed": format!("{:?}", store.snapshot_needed(id)),
                "compaction": format!("{:?}", store.get_compaction_stats(id)),
            })
        })
        .collect();
    json!({
        "states": states,
        "head": store.current_branch().head.0,
        "records": store.stats().unwrap().record_count,
        "field_matches": store.query_state_index_range("items", "/n", Some(5.0), None, None, None, false),
    })
}

fn crash(path: &Path, scenario: &str) {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_writer", "--nocapture"])
        .env("CHRONICLE_CRASH_PATH", path)
        .env("CHRONICLE_CRASH_SCENARIO", scenario)
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(73),
        "child did not reach crash point:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn crash_writer() {
    let Some(path) = std::env::var_os("CHRONICLE_CRASH_PATH") else {
        return;
    };
    let path = Path::new(&path);
    let scenario = std::env::var("CHRONICLE_CRASH_SCENARIO").unwrap();
    if let Some(point) = scenario.strip_prefix("checkpoint/") {
        crash_from_checkpoint(path, point);
    }
    if scenario == "reopen" {
        let store = Store::open(config(path)).unwrap();
        assert_eq!(store.get_state_len("items").unwrap(), Some(10));
        // Crash again after replay, before the recovered heads are saved.
        std::process::exit(73);
    }
    if scenario == "reopen-mixed" {
        let store = Store::open(config(path)).unwrap();
        let expected: Value =
            serde_json::from_slice(&std::fs::read(path.join("expected.json")).unwrap()).unwrap();
        assert_eq!(observe(&store), expected);
        std::process::exit(73);
    }
    let store = Store::create(config(path)).unwrap();
    if scenario == "unregistered" {
        // A consumer that writes a state it never registers, as
        // context-manager's image-strip watermark is written: one update
        // synced, and one the process dies before syncing.
        set(&store, "watermark", json!({"sequence": 1}));
        store.sync().unwrap();
        set(&store, "watermark", json!({"sequence": 2}));
        std::process::exit(73);
    }
    if scenario == "lost-registration" {
        // An earlier release saved a registration only at sync. Model its
        // crash: "keep" was registered and synced, then "items" was
        // registered and updated, and the process died before the next sync.
        // The parent puts back the state.bin saved before "items" existed.
        store.set_auto_snapshot(false);
        register(&store, "keep", log_strategy());
        append(&store, "keep", json!("synced"));
        store.sync().unwrap();
        std::fs::copy(path.join("state.bin"), path.join("state.before")).unwrap();
        register(&store, "items", log_strategy());
        for n in 0..10 {
            append(&store, "items", json!(n));
        }
        // A later update to "keep", so any checkpoint after replay holds a
        // head past every update of "items".
        append(&store, "keep", json!("tail"));
        std::process::exit(73);
    }
    register(&store, "items", log_strategy());

    match scenario.as_str() {
        "registration" => {
            store
                .update_state_strategy(
                    "items",
                    StateStrategy::AppendLog {
                        delta_snapshot_every: 17,
                        full_snapshot_every: 11,
                    },
                )
                .unwrap();
        }
        "fresh" | "tail" | "torn" => {
            store.set_auto_snapshot(false);
            for n in 0..10 {
                append(&store, "items", json!(n));
                if scenario != "fresh" && n == 4 {
                    store.sync().unwrap();
                }
            }
            assert_eq!(store.get_state_len("items").unwrap(), Some(10));
            if scenario == "tail" {
                store
                    .append(RecordInput::raw("message", b"ordinary log tail".to_vec()))
                    .unwrap();
            }
        }
        "mixed" => {
            register(
                &store,
                "files",
                StateStrategy::Tree {
                    delta_snapshot_every: 2,
                    full_snapshot_every: 2,
                },
            );
            register(&store, "settings", StateStrategy::Snapshot);
            append(&store, "items", json!({"n": 0}));
            store
                .register_state_field_index("items", "/n", FieldIndexKind::Number)
                .unwrap();
            store.sync().unwrap();
            for n in 1..15 {
                append(&store, "items", json!({"n": n}));
                store
                    .tree_set(
                        "files",
                        &format!("file-{n}"),
                        &TreeEntry {
                            blob_hash: format!("hash-{n}"),
                            size: n,
                            mode: 0o644,
                        },
                    )
                    .unwrap();
                store
                    .update_state(
                        "settings",
                        StateOperation::Set(serde_json::to_vec(&json!({"n": n})).unwrap()),
                    )
                    .unwrap();
            }
            store
                .update_state(
                    "items",
                    StateOperation::Edit {
                        index: 3,
                        new_value: br#"{"n":99}"#.to_vec(),
                    },
                )
                .unwrap();
            store
                .update_state("items", StateOperation::Redact { start: 1, end: 3 })
                .unwrap();
            store.tree_remove("files", "file-4").unwrap();
            store.append(RecordInput::raw("message", vec![])).unwrap();
            std::fs::write(
                path.join("expected.json"),
                serde_json::to_vec(&observe(&store)).unwrap(),
            )
            .unwrap();
        }
        "batch" => {
            // Batches past the checkpoint, crossing delta and full
            // snapshots, beside single appends, an edit and a redact, with a
            // field index kept on the slot.
            append(&store, "items", json!({"n": 0}));
            store
                .register_state_field_index("items", "/n", FieldIndexKind::Number)
                .unwrap();
            store.sync().unwrap();
            for n in 1..8 {
                append_many(
                    &store,
                    "items",
                    &[json!({"n": n * 10}), json!({"n": n * 10 + 1}), json!({"n": n * 10 + 2})],
                );
                append(&store, "items", json!({"n": n * 10 + 3}));
            }
            store
                .update_state(
                    "items",
                    StateOperation::Edit {
                        index: 2,
                        new_value: br#"{"n":5}"#.to_vec(),
                    },
                )
                .unwrap();
            store
                .update_state("items", StateOperation::Redact { start: 4, end: 6 })
                .unwrap();
            append_many(&store, "items", &[json!({"n": 7}), json!({"n": 8})]);
            std::fs::write(
                path.join("expected.json"),
                serde_json::to_vec(&observe(&store)).unwrap(),
            )
            .unwrap();
        }
        "identity-batch" => {
            // Identity batches and single identity appends past the
            // checkpoint, crossing delta and full snapshots, with the
            // record each one took written down beside the store.
            store
                .append_to_state_json_with_identity("items", json!({"n": 0}), "by", "at")
                .unwrap();
            store
                .register_state_field_index("items", "/n", FieldIndexKind::Number)
                .unwrap();
            store.sync().unwrap();
            let mut taken = Vec::new();
            for n in 1..7u64 {
                let items = [json!({"n": n * 10}), json!({"n": n * 10 + 1, "by": "stale"})];
                let batch = store
                    .append_many_to_state_json_with_identity("items", &items, "by", "at")
                    .unwrap();
                taken.push(json!([batch.record.id.0.to_string(), batch.record.sequence.0, items.len()]));
                let single = store
                    .append_to_state_json_with_identity("items", json!({"n": n * 10 + 2}), "by", "at")
                    .unwrap();
                taken.push(json!([single.id.0.to_string(), single.sequence.0, 1]));
            }
            std::fs::write(
                path.join("expected.json"),
                serde_json::to_vec(&observe(&store)).unwrap(),
            )
            .unwrap();
            std::fs::write(path.join("taken.json"), serde_json::to_vec(&taken).unwrap()).unwrap();
        }
        "batch-torn" => {
            store.set_auto_snapshot(false);
            for n in 0..5 {
                append(&store, "items", json!(n));
            }
            store.sync().unwrap();
            append_many(&store, "items", &[json!(5), json!(6), json!(7)]);
            append_many(&store, "items", &[json!(8), json!(9)]);
            assert_eq!(store.get_state_len("items").unwrap(), Some(10));
        }
        "delete" => {
            store.set_auto_snapshot(false);
            append(&store, "items", json!(0));
            store.create_branch("gone", None).unwrap();
            store.switch_branch("gone").unwrap();
            for n in 1..5 {
                append(&store, "items", json!(n));
            }
            store.switch_branch("main").unwrap();
            store.delete_branch("gone").unwrap();
            append(&store, "items", json!(9));
        }
        mode => {
            store.set_auto_snapshot(false);
            append(&store, "items", json!(0));
            append(&store, "items", json!(1));
            match mode.split('-').next().unwrap() {
                "branch" => {
                    store.create_branch("feature", None).unwrap();
                }
                "at" => {
                    store
                        .create_branch_at("feature", "main", Sequence(1))
                        .unwrap();
                }
                "empty" => {
                    store.create_empty_branch("feature", None).unwrap();
                }
                other => panic!("unknown scenario {other}"),
            }
            if mode.ends_with("tail") {
                store.switch_branch("feature").unwrap();
                for n in 2..5 {
                    append(&store, "items", json!(n));
                }
            }
        }
    }

    // No destructors: unlike dropping the Store, this cannot flush metadata.
    std::process::exit(73);
}

#[test]
fn registration_and_strategy_change_survive_without_close() {
    let dir = TempDir::new().unwrap();
    crash(dir.path(), "registration");
    let store = open_store(dir.path()).unwrap();
    assert_eq!(store.stats().unwrap().state_slot_count, 1);
    assert!(matches!(
        store.register_state(StateRegistration {
            id: "items".into(),
            strategy: log_strategy(),
            initial_value: None,
        }),
        Err(StoreError::StateExists(_))
    ));
    for n in 0..16 {
        append(&store, "items", json!(n));
    }
    assert_eq!(
        store.stats().unwrap().record_count,
        16,
        "retuned cadence survived"
    );
    append(&store, "items", json!(16));
    assert_eq!(
        store.stats().unwrap().record_count,
        18,
        "delta at retuned threshold"
    );
}

#[test]
fn new_store_and_incremental_tail_replay_without_duplicates() {
    for scenario in ["fresh", "tail"] {
        let dir = TempDir::new().unwrap();
        crash(dir.path(), scenario);
        crash(dir.path(), "reopen");
        for cycle in 0..2 {
            let store = open_store(dir.path()).unwrap();
            assert_eq!(
                value(&store, "items"),
                json!([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
            );
            assert_eq!(store.get_state_len("items").unwrap(), Some(10));
            assert_eq!(
                store.stats().unwrap().record_count,
                if scenario == "tail" { 11 } else { 10 }
            );
            assert!(store.recovery().is_none(), "an intact tail loses no bytes");
            if cycle == 1 {
                store.set_auto_snapshot(false);
                append(&store, "items", json!(10));
                assert_eq!(store.get_state_len("items").unwrap(), Some(11));
                assert_eq!(
                    store.current_branch().head.0,
                    if scenario == "tail" { 12 } else { 11 }
                );
            }
        }
    }
}

#[test]
fn replay_restores_mixed_operations_snapshots_and_field_index() {
    let dir = TempDir::new().unwrap();
    crash(dir.path(), "mixed");
    // The child replays and exits before anything checkpoints, so the first
    // open here replays the same tail again. The second follows Drop's
    // checkpoint.
    crash(dir.path(), "reopen-mixed");
    let expected: Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("expected.json")).unwrap()).unwrap();
    for _ in 0..2 {
        let store = open_store(dir.path()).unwrap();
        assert_eq!(observe(&store), expected);
        assert_eq!(value(&store, "items").as_array().unwrap().len(), 13);
        assert_eq!(
            store.get_state_at("items", Sequence(1)).unwrap(),
            Some(br#"[{"n":0}]"#.to_vec())
        );
    }
}

/// Batches written past the checkpoint replay whole, with the snapshot
/// accounting and field index they had, on every open until a checkpoint
/// covers them. The child's MANIFEST rewrite is undone here first, as a
/// power loss can undo an unsynced rename: replaying a batch marks the
/// MANIFEST again.
#[test]
fn replay_restores_batches_and_marks_the_manifest_again() {
    let dir = TempDir::new().unwrap();
    crash(dir.path(), "batch");
    let manifest = dir.path().join("MANIFEST");
    // Version 2, naming the AppendMany feature (bit 0) and no other.
    let marked = [b"RST\0\x02".as_slice(), &1u64.to_le_bytes()[..]].concat();
    assert_eq!(std::fs::read(&manifest).unwrap(), marked);
    std::fs::write(&manifest, b"RST\0\x01").unwrap();

    let expected: Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("expected.json")).unwrap()).unwrap();
    for _ in 0..2 {
        let store = open_store(dir.path()).unwrap();
        assert_eq!(observe(&store), expected);
        assert_eq!(value(&store, "items").as_array().unwrap().len(), 1 + 7 * 4 - 2 + 2);
        assert_eq!(std::fs::read(&manifest).unwrap(), marked, "marked again, with the feature named");
    }
}

/// Identity batches written past the checkpoint replay with the stamps
/// they were written with: every item still names the record that holds
/// it, and a batch's items all name its one record. The first open replays
/// them from the log; the second reads them from the checkpoint the first
/// one's close saved.
#[test]
fn replay_keeps_the_stamps_of_identity_batches() {
    let dir = TempDir::new().unwrap();
    crash(dir.path(), "identity-batch");
    let expected: Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("expected.json")).unwrap()).unwrap();
    let taken: Vec<(String, u64, usize)> =
        serde_json::from_slice(&std::fs::read(dir.path().join("taken.json")).unwrap()).unwrap();
    for open in 0..2 {
        let store = open_store(dir.path()).unwrap();
        assert_eq!(observe(&store), expected, "open {open}");
        let items = value(&store, "items");
        let items = items.as_array().unwrap();
        assert_eq!(items.len(), 1 + 6 * 3, "open {open}");
        let mut ordinal = 1;
        for (id, sequence, count) in &taken {
            for item in &items[ordinal..ordinal + count] {
                assert_eq!(item["by"], json!(id), "open {open}: ordinal {ordinal}");
                assert_eq!(item["at"], json!(sequence), "open {open}: ordinal {ordinal}");
            }
            let record = store
                .get_record(RecordId(id.parse().unwrap()))
                .unwrap()
                .unwrap();
            assert_eq!(record.sequence, Sequence(*sequence));
            ordinal += count;
        }
        assert_eq!(
            std::fs::read(dir.path().join("MANIFEST")).unwrap(),
            [b"RST\0\x02".as_slice(), &1u64.to_le_bytes()[..]].concat(),
            "version 2, the AppendMany flag and no other"
        );
    }
}

/// A batch is one record, so a torn tail drops it whole: none of its items
/// survive, and the batch before it survives whole.
#[test]
fn a_torn_batch_is_dropped_whole() {
    let dir = TempDir::new().unwrap();
    crash(dir.path(), "batch-torn");
    let log_path = dir.path().join("records.log");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&log_path)
        .unwrap();
    file.set_len(file.metadata().unwrap().len() - 3).unwrap();
    drop(file);
    let store = open_store(dir.path()).unwrap();
    assert_eq!(value(&store, "items"), json!([0, 1, 2, 3, 4, 5, 6, 7]));
    assert_eq!(store.get_state_len("items").unwrap(), Some(8));
    assert!(store.recovery().is_some());
    store.set_auto_snapshot(false);
    append_many(&store, "items", &[json!(98), json!(99)]);
    // Five appends and the surviving batch took sequences 1 to 6.
    assert_eq!(store.current_branch().head, Sequence(7));
    assert_eq!(
        value(&store, "items"),
        json!([0, 1, 2, 3, 4, 5, 6, 7, 98, 99])
    );
}

#[test]
fn all_branch_creation_paths_survive_with_inherited_heads_and_tail() {
    for (kind, base) in [
        ("branch", json!([0, 1])),
        ("at", json!([0])),
        ("empty", Value::Null),
    ] {
        for suffix in ["created", "tail"] {
            let dir = TempDir::new().unwrap();
            crash(dir.path(), &format!("{kind}-{suffix}"));
            let store = open_store(dir.path()).unwrap();
            assert_eq!(store.list_branches().len(), 2, "{kind}-{suffix}");
            assert_eq!(
                store.current_branch().name,
                if suffix == "tail" { "feature" } else { "main" }
            );
            store.switch_branch("feature").unwrap();
            let mut expected = base.as_array().cloned().unwrap_or_default();
            if suffix == "tail" {
                expected.extend([json!(2), json!(3), json!(4)]);
            }
            let expected = if expected.is_empty() {
                Value::Null
            } else {
                json!(expected)
            };
            assert_eq!(value(&store, "items"), expected, "{kind}-{suffix}");
            store.switch_branch("main").unwrap();
            assert_eq!(
                value(&store, "items"),
                json!([0, 1]),
                "child must not advance parent"
            );
        }
    }
}

#[test]
fn deleted_branch_is_not_resurrected_by_replay() {
    let dir = TempDir::new().unwrap();
    crash(dir.path(), "delete");
    let store = open_store(dir.path()).unwrap();
    assert_eq!(store.list_branches().len(), 1);
    assert_eq!(value(&store, "items"), json!([0, 9]));
    assert_eq!(store.get_state_len("items").unwrap(), Some(2));
    let branch = store.create_empty_branch("gone", None).unwrap();
    assert!(branch.id.0 > 2, "deletion does not reuse identity");
    store.switch_branch("gone").unwrap();
    assert_eq!(value(&store, "items"), Value::Null);
}

#[test]
fn torn_tail_replays_only_complete_updates() {
    let dir = TempDir::new().unwrap();
    crash(dir.path(), "torn");
    let log_path = dir.path().join("records.log");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&log_path)
        .unwrap();
    file.set_len(file.metadata().unwrap().len() - 3).unwrap();
    drop(file);
    let store = open_store(dir.path()).unwrap();
    assert_eq!(value(&store, "items"), json!([0, 1, 2, 3, 4, 5, 6, 7, 8]));
    assert_eq!(store.get_state_len("items").unwrap(), Some(9));
    assert!(store.recovery().is_some());
    store.set_auto_snapshot(false);
    append(&store, "items", json!(99));
    assert_eq!(store.current_branch().head, Sequence(10));
    assert_eq!(
        value(&store, "items"),
        json!([0, 1, 2, 3, 4, 5, 6, 7, 8, 99])
    );
}

#[test]
fn legacy_json_tail_replays_from_offset_zero() {
    let dir = TempDir::new().unwrap();
    {
        let store = Store::create(config(dir.path())).unwrap();
        register(&store, "items", log_strategy());
    }
    let log = RecordLog::open(dir.path().join("records.log")).unwrap();
    let update = StateUpdateRecord {
        record_id: RecordId(0),
        global_sequence: Sequence(1),
        state_id: "items".into(),
        prev_update_offset: None,
        operation: StateOperation::Append(b"42".to_vec()),
        timestamp: Timestamp(0),
    };
    let mut input = RecordInput::raw("state_update", serde_json::to_vec(&update).unwrap());
    input.encoding = PayloadEncoding::Raw;
    log.append(input, BranchId(1), Sequence(1)).unwrap();
    log.sync().unwrap();
    drop(log);
    let store = open_store(dir.path()).unwrap();
    assert_eq!(value(&store, "items"), json!([42]));
    assert_eq!(store.get_state_len("items").unwrap(), Some(1));
}

#[test]
fn lost_registration_is_replayed_and_stays_writable() {
    let dir = TempDir::new().unwrap();
    crash(dir.path(), "lost-registration");
    std::fs::rename(
        dir.path().join("state.before"),
        dir.path().join("state.bin"),
    )
    .unwrap();
    let items: Vec<Value> = (0..10).map(|n| json!(n)).collect();

    let store = open_store(dir.path()).unwrap();
    assert_eq!(value(&store, "items"), json!(items));
    assert_eq!(value(&store, "keep"), json!(["synced", "tail"]));
    let report = store.recovery().unwrap();
    assert_eq!(report.unregistered_states, vec![("items".to_string(), 10)]);
    assert_eq!(report.undecodable_state_updates, 0);
    assert_eq!(report.dropped_bytes, 0);
    assert_eq!(store.stats().unwrap().state_slot_count, 1);
    // It takes updates before its registration comes back, as any state does.
    append(&store, "items", json!(10));
    // Drop checkpoints every head, so the next open replays nothing: the
    // chain must be saved by then.
    drop(store);

    let store = open_store(dir.path()).unwrap();
    assert!(store.recovery().is_none());
    let mut items = items;
    items.push(json!(10));
    assert_eq!(value(&store, "items"), json!(items));
    // The consumer's own registration gives it back its strategy.
    register(&store, "items", log_strategy());
    append(&store, "items", json!(11));
    drop(store);

    let store = open_store(dir.path()).unwrap();
    items.push(json!(11));
    assert_eq!(value(&store, "items"), json!(items));
    assert_eq!(store.get_state_len("items").unwrap(), Some(12));
    assert_eq!(store.stats().unwrap().state_slot_count, 2);
}

// A state written without a registration, as 0.4.x allowed and as
// context-manager writes its image-strip watermark: its updates are kept
// across a sync and a crash, it stays writable, and register_state can still
// give it a strategy later.
#[test]
fn a_state_written_without_registration_is_kept_and_stays_writable() {
    let dir = TempDir::new().unwrap();
    crash(dir.path(), "unregistered");

    let store = open_store(dir.path()).unwrap();
    assert_eq!(value(&store, "watermark"), json!({"sequence": 2}));
    let report = store.recovery().unwrap();
    assert_eq!(
        report.unregistered_states,
        vec![("watermark".to_string(), 1)]
    );
    assert_eq!(report.dropped_bytes, 0);
    assert_eq!(store.stats().unwrap().state_slot_count, 0);
    set(&store, "watermark", json!({"sequence": 3}));
    drop(store);

    let store = open_store(dir.path()).unwrap();
    assert!(store.recovery().is_none());
    assert_eq!(value(&store, "watermark"), json!({"sequence": 3}));
    register(&store, "watermark", StateStrategy::Snapshot);
    set(&store, "watermark", json!({"sequence": 4}));
    drop(store);

    let store = open_store(dir.path()).unwrap();
    assert_eq!(value(&store, "watermark"), json!({"sequence": 4}));
    assert_eq!(store.stats().unwrap().state_slot_count, 1);
}

#[test]
fn record_typed_state_update_that_is_not_one_is_skipped_and_counted() {
    let dir = TempDir::new().unwrap();
    {
        let store = Store::create(config(dir.path())).unwrap();
        register(&store, "items", log_strategy());
        store.set_auto_snapshot(false);
        append(&store, "items", json!(1));
    }
    // What an earlier release's generic append could write under this name.
    let log = RecordLog::open(dir.path().join("records.log")).unwrap();
    log.append(
        RecordInput::raw("state_update", b"custom payload".to_vec()),
        BranchId(1),
        Sequence(2),
    )
    .unwrap();
    log.sync().unwrap();
    drop(log);

    // It stays past the checkpoint until a state update moves it.
    for _ in 0..2 {
        let store = open_store(dir.path()).unwrap();
        let report = store.recovery().unwrap();
        assert_eq!(report.undecodable_state_updates, 1);
        assert!(report.unregistered_states.is_empty());
        assert_eq!(report.dropped_bytes, 0);
        assert_eq!(report.valid_records, 2);
        assert_eq!(value(&store, "items"), json!([1]));
        assert_eq!(store.stats().unwrap().record_count, 2);
    }
    let store = open_store(dir.path()).unwrap();
    store.set_auto_snapshot(false);
    append(&store, "items", json!(2));
    drop(store);
    let store = open_store(dir.path()).unwrap();
    assert!(store.recovery().is_none());
    assert_eq!(value(&store, "items"), json!([1, 2]));
}

fn append_raw_update(log: &RecordLog, branch: BranchId, sequence: u64, prev: Option<u64>) -> u64 {
    let update = StateUpdateRecord {
        record_id: RecordId(0),
        global_sequence: Sequence(sequence),
        state_id: "items".into(),
        prev_update_offset: prev,
        operation: StateOperation::Append(sequence.to_string().into_bytes()),
        timestamp: Timestamp(0),
    };
    let (payload, encoding) = update.encode().unwrap();
    let mut input = RecordInput::raw("state_update", payload);
    input.encoding = encoding;
    let (_, offset) = log.append(input, branch, Sequence(sequence)).unwrap();
    log.sync().unwrap();
    offset
}

#[test]
fn update_whose_predecessor_is_not_its_head_refuses_open() {
    let dir = TempDir::new().unwrap();
    {
        let store = Store::create(config(dir.path())).unwrap();
        register(&store, "items", log_strategy());
        store.set_auto_snapshot(false);
        // The log's first record, at offset 0, then one after it.
        append(&store, "items", json!(0));
        append(&store, "items", json!(1));
    }
    // A second update after the first one: the fork a writer leaves when
    // its head missed an update it had written.
    let log = RecordLog::open(dir.path().join("records.log")).unwrap();
    let offset = append_raw_update(&log, BranchId(1), 3, Some(0));
    drop(log);
    let before = std::fs::read(dir.path().join("records.log")).unwrap();

    let err = match open_store(dir.path()) {
        Ok(_) => panic!("a broken chain opened"),
        Err(e) => e,
    };
    let at = format!("log offset {offset}");
    assert!(
        matches!(&err, StoreError::Corruption(msg) if msg.contains(&at)),
        "{err}"
    );
    assert_eq!(
        std::fs::read(dir.path().join("records.log")).unwrap(),
        before
    );
}

#[test]
fn updates_on_a_branch_missing_from_branches_bin_are_not_replayed() {
    let dir = TempDir::new().unwrap();
    {
        let store = Store::create(config(dir.path())).unwrap();
        register(&store, "items", log_strategy());
        store.set_auto_snapshot(false);
        append(&store, "items", json!(0));
    }
    // An earlier release saved branches.bin only at sync, so a crash can
    // leave updates on a branch it never saved. The first names its parent's
    // head as predecessor, which a branch without a head can't match.
    let log = RecordLog::open(dir.path().join("records.log")).unwrap();
    append_raw_update(&log, BranchId(7), 2, Some(0));
    drop(log);

    // It stays past the checkpoint, and counted, until a state update moves
    // the checkpoint past it.
    for _ in 0..2 {
        let store = open_store(dir.path()).unwrap();
        assert_eq!(store.list_branches().len(), 1);
        assert_eq!(value(&store, "items"), json!([0]));
        let report = store.recovery().unwrap();
        assert_eq!(report.updates_on_missing_branches, 1);
        assert_eq!(report.dropped_bytes, 0);
        assert!(report.unregistered_states.is_empty());
    }
    let store = open_store(dir.path()).unwrap();
    store.set_auto_snapshot(false);
    append(&store, "items", json!(3));
    drop(store);
    let store = open_store(dir.path()).unwrap();
    assert!(store.recovery().is_none());
    assert_eq!(value(&store, "items"), json!([0, 3]));
}

#[test]
fn an_undecodable_record_on_a_missing_branch_is_counted_as_undecodable() {
    let dir = TempDir::new().unwrap();
    {
        let store = Store::create(config(dir.path())).unwrap();
        register(&store, "items", log_strategy());
        store.set_auto_snapshot(false);
        append(&store, "items", json!(1));
    }
    // An earlier release's generic append under this name, on a branch
    // branches.bin doesn't hold. It isn't a state update, whatever branch it
    // names.
    let log = RecordLog::open(dir.path().join("records.log")).unwrap();
    log.append(
        RecordInput::raw("state_update", b"custom payload".to_vec()),
        BranchId(7),
        Sequence(2),
    )
    .unwrap();
    log.sync().unwrap();
    drop(log);

    let store = open_store(dir.path()).unwrap();
    let report = store.recovery().unwrap();
    assert_eq!(report.undecodable_state_updates, 1);
    assert_eq!(report.updates_on_missing_branches, 0);
    assert_eq!(value(&store, "items"), json!([1]));
}

#[test]
fn partially_published_branch_cannot_leak_heads_into_a_new_empty_branch() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    register(&store, "items", log_strategy());
    append(&store, "items", json!(42));
    store.sync().unwrap();
    let previous_branches = std::fs::read(dir.path().join("branches.bin")).unwrap();
    let abandoned = store.create_branch("unpublished", None).unwrap();
    drop(store);

    // Model interruption after state.bin publication but before branches.bin.
    // The child has copied state heads, but no log record reserves its ID.
    std::fs::write(dir.path().join("branches.bin"), previous_branches).unwrap();
    let store = open_store(dir.path()).unwrap();
    assert_eq!(store.list_branches().len(), 1);
    let new = store.create_empty_branch("new", None).unwrap();
    assert!(new.id > abandoned.id);
    store.switch_branch("new").unwrap();
    assert_eq!(value(&store, "items"), Value::Null);
    append(&store, "items", json!(99));
    assert_eq!(value(&store, "items"), json!([99]));
}

#[test]
fn generic_appends_cannot_forge_internal_state_updates() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    register(&store, "items", log_strategy());
    let update = StateUpdateRecord {
        record_id: RecordId(0),
        global_sequence: Sequence(1),
        state_id: "items".into(),
        prev_update_offset: None,
        operation: StateOperation::Append(b"42".to_vec()),
        timestamp: Timestamp(0),
    };
    for payload in [b"{}".to_vec(), serde_json::to_vec(&update).unwrap()] {
        let result = store.append(RecordInput::raw("state_update", payload));
        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));
        assert_eq!(store.stats().unwrap().record_count, 0);
        assert_eq!(store.current_branch().head, Sequence(0));
        assert_eq!(
            std::fs::metadata(dir.path().join("records.log"))
                .unwrap()
                .len(),
            0
        );
    }
    store.sync().unwrap();
    drop(store);
    let store = open_store(dir.path()).unwrap();
    assert_eq!(store.stats().unwrap().record_count, 0);
    assert_eq!(value(&store, "items"), Value::Null);
}

const TABLE: &str = "mcpl/coalescing";
const JOURNAL: &str = "mcpl/coalescing-journal";

fn journal_items() -> [Value; 2] {
    [
        json!({"subject": "z", "record": null, "seq": 401}),
        json!({"subject": "a", "record": {"subject": "a"}, "seq": 402}),
    ]
}

fn compacted_table() -> Value {
    json!({"version": 1, "subjects": [{"subject": "a"}], "receipts": [], "journal": 402})
}

fn chunk(id: usize, marked: bool) -> Value {
    let mut row = json!({"id": id, "compressed": marked});
    if marked {
        row["summaryId"] = json!("s");
    }
    row
}

const MAIN_BLOB: &[u8] = &[b'm'; 9];
const OLD_BLOB: &[u8] = &[b'o'; 20];
const ADOPTED_BLOB: &[u8] = &[b'a'; 300];

fn entry(content: &[u8], mode: u32) -> TreeEntry {
    TreeEntry {
        blob_hash: Hash::from_bytes(content).to_hex(),
        size: content.len() as u64,
        mode,
    }
}

fn old_entry() -> TreeEntry {
    entry(OLD_BLOB, 0o600)
}

fn adopted_entry() -> TreeEntry {
    entry(ADOPTED_BLOB, 0o755)
}

#[cfg(unix)]
fn interrupt_next_append(path: &Path) {
    let limit = (std::fs::metadata(path.join("records.log")).unwrap().len() + 1) as libc::rlim_t;
    let rlimit = libc::rlimit {
        rlim_cur: limit,
        rlim_max: limit,
    };
    unsafe {
        libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
        assert_eq!(libc::setrlimit(libc::RLIMIT_FSIZE, &rlimit), 0);
    }
}

fn refused_by_the_size_limit(error: StoreError) {
    match error {
        #[cfg(unix)]
        StoreError::Io(cause) if cause.raw_os_error() == Some(libc::EFBIG) => {}
        other => panic!("expected the file-size limit's EFBIG, got {other}"),
    }
}

const TYPED_RECORDS: [(&str, &str); 5] = [
    ("round", "prefix"),
    ("journal", "received"),
    ("round", "accepted one"),
    ("journal", "applied"),
    ("round", "accepted two"),
];

fn crash_from_checkpoint(path: &Path, point: &str) -> ! {
    let store = open_store(path).unwrap();
    store.set_auto_snapshot(false);
    match point {
        "restore-source" => {
            assert_eq!(store.current_branch().name, "child");
            store.switch_branch("source").unwrap();
            store.sync().unwrap();
        }
        "watermark" | "cleared" => {
            for item in journal_items() {
                append(&store, JOURNAL, item);
            }
            set(&store, TABLE, compacted_table());
            if point == "cleared" {
                set(&store, JOURNAL, json!([]));
            }
        }
        point if point.starts_with("mark-") => {
            let count: usize = point[5..].parse().unwrap();
            append(&store, "summaries", json!({"id": "s", "sources": [0, 2]}));
            for index in [0, 2].into_iter().take(count) {
                store
                    .update_state(
                        "chunks",
                        StateOperation::Edit {
                            index,
                            new_value: serde_json::to_vec(&chunk(index, true)).unwrap(),
                        },
                    )
                    .unwrap();
            }
        }
        point if point.starts_with("adopt-") => {
            let (op, boundary) = point[6..].split_once('-').unwrap();
            assert_eq!(store.current_branch().name, "workspace");
            assert_eq!(store.tree_get("files", "tool").unwrap(), Some(old_entry()));
            if boundary != "before" {
                #[cfg(unix)]
                if boundary == "failed" {
                    interrupt_next_append(path);
                }
                let result = match op {
                    "set" => store.tree_set("files", "tool", &adopted_entry()),
                    "remove" => store.tree_remove("files", "tool"),
                    other => panic!("unknown adoption {other}"),
                };
                if boundary == "failed" {
                    refused_by_the_size_limit(result.unwrap_err());
                } else {
                    result.unwrap();
                }
                let got = store.tree_get("files", "tool").unwrap();
                if op == "set" {
                    assert_eq!(
                        got,
                        Some(if boundary == "failed" {
                            old_entry()
                        } else {
                            adopted_entry()
                        })
                    );
                } else {
                    assert_eq!(
                        got,
                        if boundary == "failed" {
                            Some(old_entry())
                        } else {
                            None
                        }
                    );
                }
            }
        }
        "redact" | "redact-refused" => {
            let mut rows = value(&store, "messages");
            #[cfg(unix)]
            if point == "redact-refused" {
                interrupt_next_append(path);
            }
            let result =
                store.update_state("messages", StateOperation::Redact { start: 1, end: 4 });
            if point == "redact-refused" {
                refused_by_the_size_limit(result.unwrap_err());
            } else {
                result.unwrap();
                rows.as_array_mut().unwrap().drain(1..4);
            }
            assert_eq!(value(&store, "messages"), rows);
        }
        "typed-tail" => {
            let mut ids = Vec::new();
            for &(kind, text) in &TYPED_RECORDS[1..4] {
                ids.push(
                    store
                        .append(RecordInput::raw(kind, text.as_bytes().to_vec()))
                        .unwrap()
                        .id,
                );
            }
            let branch = store.current_branch();
            std::fs::write(
                path.join("tail-ids.json"),
                serde_json::to_vec(&json!({
                    "ids": ids,
                    "branch": branch.id.0,
                    "next": branch.head.next().0,
                }))
                .unwrap(),
            )
            .unwrap();
        }
        "torn-layouts" => {
            let ids: Vec<u64> = ["compile-intact", "compile-torn"]
                .iter()
                .map(|compile| {
                    store
                        .append(RecordInput::json("layout", &json!({ "compileId": compile })).unwrap())
                        .unwrap()
                        .id
                        .0
                })
                .collect();
            std::fs::write(path.join("tail-ids.json"), serde_json::to_vec(&ids).unwrap()).unwrap();
        }
        other => panic!("unknown checkpoint crash point {other}"),
    }
    std::process::exit(73);
}

#[test]
fn a_switch_back_to_source_survives_a_barrier_without_drop() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    let source = store.create_branch("source", None).unwrap().id;
    store.switch_branch("source").unwrap();
    let child = store.create_branch("child", None).unwrap().id;
    store.switch_branch("child").unwrap();
    drop(store);

    let store = open_store(dir.path()).unwrap();
    assert_eq!(store.current_branch().id, child);
    store.switch_branch("source").unwrap();
    drop(store);
    let store = open_store(dir.path()).unwrap();
    assert_eq!(store.current_branch().id, source);

    store.switch_branch("child").unwrap();
    drop(store);
    crash(dir.path(), "checkpoint/restore-source");
    let store = open_store(dir.path()).unwrap();
    assert_eq!(
        (
            store.current_branch().id,
            store.get_branch("child").map(|b| b.id)
        ),
        (source, Some(child))
    );
}

#[test]
fn interrupted_watermark_and_marking_sequences_recover_only_the_completed_prefix() {
    for point in ["watermark", "cleared"] {
        let dir = TempDir::new().unwrap();
        let store = Store::create(config(dir.path())).unwrap();
        register(&store, TABLE, StateStrategy::Snapshot);
        register(&store, JOURNAL, log_strategy());
        set(
            &store,
            TABLE,
            json!({"version": 1, "subjects": [{"subject": "z"}], "receipts": [], "journal": 400}),
        );
        drop(store);
        crash(dir.path(), &format!("checkpoint/{point}"));

        let store = open_store(dir.path()).unwrap();
        let journal = value(&store, JOURNAL);
        if point == "watermark" {
            assert_eq!(journal, json!(journal_items()));
        }
        assert_eq!(
            (value(&store, TABLE), journal),
            (
                compacted_table(),
                if point == "watermark" {
                    json!(journal_items())
                } else {
                    json!([])
                }
            ),
            "{point}"
        );
    }

    for marks in 0..=2 {
        let dir = TempDir::new().unwrap();
        let store = Store::create(config(dir.path())).unwrap();
        store.set_auto_snapshot(false);
        register(&store, "summaries", log_strategy());
        register(&store, "chunks", log_strategy());
        for i in 0..3 {
            append(&store, "chunks", chunk(i, false));
        }
        drop(store);
        crash(dir.path(), &format!("checkpoint/mark-{marks}"));

        let store = open_store(dir.path()).unwrap();
        assert_eq!(
            (value(&store, "summaries"), value(&store, "chunks")),
            (
                json!([{"id": "s", "sources": [0, 2]}]),
                json!([chunk(0, marks >= 1), chunk(1, false), chunk(2, marks == 2)]),
            ),
            "after {marks} marks"
        );
    }
}

#[test]
fn an_interrupted_adoption_recovers_the_tree_outcome_without_completion_evidence() {
    for op in ["set", "remove"] {
        for boundary in ["before", "failed", "after"] {
            if boundary == "failed" && !cfg!(unix) {
                continue;
            }
            let dir = TempDir::new().unwrap();
            let store = Store::create(config(dir.path())).unwrap();
            store.set_auto_snapshot(false);
            register(
                &store,
                "files",
                StateStrategy::Tree {
                    delta_snapshot_every: 2,
                    full_snapshot_every: 2,
                },
            );
            for content in [MAIN_BLOB, OLD_BLOB, ADOPTED_BLOB] {
                let hash = store.store_blob(content, "application/octet-stream").unwrap();
                assert_eq!(hash.to_hex(), entry(content, 0).blob_hash);
            }
            let main = store.current_branch().id;
            let other = entry(MAIN_BLOB, 0o644);
            store.tree_set("files", "tool", &other).unwrap();
            let workspace = store.create_branch("workspace", None).unwrap().id;
            store.switch_branch("workspace").unwrap();
            store.tree_set("files", "tool", &old_entry()).unwrap();
            let intent = store
                .append(
                    RecordInput::json("agreement", &json!({"pending": op, "path": "tool"}))
                        .unwrap(),
                )
                .unwrap();
            drop(store);
            crash(dir.path(), &format!("checkpoint/adopt-{op}-{boundary}"));

            let landed = boundary == "after";
            let expected = match (op, landed) {
                (_, false) => Some(old_entry()),
                ("set", true) => Some(adopted_entry()),
                ("remove", true) => None,
                _ => unreachable!(),
            };
            for _ in 0..2 {
                let store = open_store(dir.path()).unwrap();
                let got = store.tree_get_on(workspace, "files", "tool").unwrap();
                assert_eq!(
                    (
                        store.get_records_by_type("agreement"),
                        got.as_ref().map(|e| &e.blob_hash),
                        store.tree_get_on(main, "files", "tool").unwrap()
                    ),
                    (
                        vec![intent.id],
                        expected.as_ref().map(|e| &e.blob_hash),
                        Some(other.clone())
                    ),
                    "{op}-{boundary}"
                );
                let head = store.get_branch_by_id(workspace).unwrap().head;
                let expected_head = if landed {
                    intent.sequence.next()
                } else {
                    intent.sequence
                };
                if op == "set" {
                    assert_eq!((got, head), (expected.clone(), expected_head), "{boundary}");
                } else {
                    assert_eq!((got, head), (expected.clone(), expected_head), "{boundary}");
                }
            }
            if op == "remove" && boundary == "before" {
                let store = open_store(dir.path()).unwrap();
                let log = dir.path().join("records.log");
                let intact = std::fs::read(&log).unwrap();
                std::fs::write(&log, []).unwrap();
                let read = store.tree_get_on(workspace, "files", "tool");
                std::fs::write(&log, intact).unwrap();
                assert!(match read {
                    Err(_) => true,
                    Ok(entry) => entry == Some(old_entry()),
                });
            }
        }
    }
}

#[test]
fn a_torn_shard_redaction_never_recovers_part_of_the_selected_group() {
    let rows = [
        json!({"id": "before"}),
        json!({"bodyGroupId": "g", "shard": 0}),
        json!({"bodyGroupId": "g", "shard": 1}),
        json!({"bodyGroupId": "g", "shard": 2}),
        json!({"id": "after"}),
    ];
    for point in ["intact", "torn", "refused"] {
        if point == "refused" && !cfg!(unix) {
            continue;
        }
        let dir = TempDir::new().unwrap();
        let store = Store::create(config(dir.path())).unwrap();
        store.set_auto_snapshot(false);
        register(&store, "messages", log_strategy());
        for row in &rows {
            append(&store, "messages", row.clone());
        }
        let before = store.get_records_by_type("state_update").len();
        drop(store);
        crash(
            dir.path(),
            if point == "refused" {
                "checkpoint/redact-refused"
            } else {
                "checkpoint/redact"
            },
        );
        if point == "torn" {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(dir.path().join("records.log"))
                .unwrap();
            file.set_len(file.metadata().unwrap().len() - 1).unwrap();
        }
        let store = open_store(dir.path()).unwrap();
        let expected = if point == "intact" {
            json!([rows[0], rows[4]])
        } else {
            json!(rows)
        };
        assert_eq!(
            (
                value(&store, "messages"),
                store.get_records_by_type("state_update").len()
            ),
            (expected, before + usize::from(point == "intact")),
            "{point}"
        );
    }
}

#[test]
fn a_torn_layout_after_writer_death_leaves_the_one_before_it_and_its_id_is_issued_again() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    let prefix: Vec<_> = (0..4)
        .map(|i| {
            let kind = if i % 2 == 0 { "layout" } else { "message" };
            store
                .append(RecordInput::raw(kind, format!("{kind} {i}").into_bytes()))
                .unwrap()
        })
        .collect();
    drop(store);
    crash(dir.path(), "checkpoint/torn-layouts");
    let ids: Vec<u64> =
        serde_json::from_slice(&std::fs::read(dir.path().join("tail-ids.json")).unwrap()).unwrap();
    let (intact, torn) = (RecordId(ids[0]), RecordId(ids[1]));
    let log = dir.path().join("records.log");
    let len = std::fs::metadata(&log).unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&log)
        .unwrap()
        .set_len(len - 1)
        .unwrap();

    let store = open_store(dir.path()).unwrap();
    let kept = store.get_record(intact).unwrap().unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&kept.payload).unwrap(),
        json!({ "compileId": "compile-intact" })
    );
    assert!(store.get_record(torn).unwrap().is_none());
    let mut layouts: Vec<_> = prefix
        .iter()
        .filter(|record| record.record_type == "layout")
        .map(|record| record.id)
        .collect();
    layouts.push(intact);
    assert_eq!(store.get_records_by_type("layout"), layouts);
    assert_eq!(store.current_branch().head, kept.sequence);
    let next = store
        .append(RecordInput::raw("message", b"after the crash".to_vec()))
        .unwrap();
    assert_eq!((next.id, next.sequence), (torn, kept.sequence.next()));
    assert_eq!(store.get_records_by_type("layout"), layouts);
    let messages: Vec<_> = prefix
        .iter()
        .filter(|record| record.record_type == "message")
        .map(|record| record.id)
        .chain([next.id])
        .collect();
    assert_eq!(store.get_records_by_type("message"), messages);
}

#[test]
fn open_or_create_reindexes_a_complete_typed_tail_once_after_writer_death() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    let (kind, text) = TYPED_RECORDS[0];
    let prefix = store
        .append(RecordInput::raw(kind, text.as_bytes().to_vec()))
        .unwrap();
    drop(store);
    crash(dir.path(), "checkpoint/typed-tail");
    let written: Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("tail-ids.json")).unwrap()).unwrap();
    let mut ids = vec![prefix.id];
    ids.extend(serde_json::from_value::<Vec<RecordId>>(written["ids"].clone()).unwrap());
    let (kind, text) = TYPED_RECORDS[4];
    let log = RecordLog::open(dir.path().join("records.log")).unwrap();
    let (last, _) = log
        .append(
            RecordInput::raw(kind, text.as_bytes().to_vec()),
            BranchId(written["branch"].as_u64().unwrap()),
            Sequence(written["next"].as_u64().unwrap()),
        )
        .unwrap();
    drop(log);
    ids.push(last.id);
    assert_eq!(ids.len(), TYPED_RECORDS.len());

    for _ in 0..2 {
        let store = retry_locked(|| Store::open_or_create(config(dir.path()))).unwrap();
        for kind in ["round", "journal"] {
            let expected: Vec<_> = TYPED_RECORDS
                .iter()
                .zip(&ids)
                .filter(|((ty, _), _)| *ty == kind)
                .map(|(_, id)| *id)
                .collect();
            assert_eq!(store.get_records_by_type(kind), expected, "{kind}");
        }
        for (id, (kind, text)) in ids.iter().zip(TYPED_RECORDS) {
            let record = store.get_record(*id).unwrap().unwrap();
            assert_eq!(
                (
                    record.id,
                    record.record_type.as_str(),
                    record.payload.as_slice()
                ),
                (*id, kind, text.as_bytes())
            );
        }
    }
}
