//! Process-crash recovery: the child exits without running Store::drop.
//! These tests cover process death, not storage-device/power-loss behavior.

use chronicle::{
    BranchId, FieldIndexKind, PayloadEncoding, RecordId, RecordInput, RecordLog, Sequence,
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
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match Store::open(config(path)) {
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
    let at = format!("at log offset {offset}: previous offset Some(0) does not match");
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
