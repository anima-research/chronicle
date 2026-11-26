//! A failed metadata checkpoint must not admit dependent writes. The child
//! repairs the injected I/O failure, writes successfully, then exits without
//! destructors so reopen cannot rely on a final Store::drop checkpoint.

use chronicle::{
    Branch, FieldIndexKind, RecordInput, Sequence, StateOperation, StateRegistration,
    StateSelection, StateStrategy, Store, StoreConfig, StoreError, StoreEvent,
    SubscriptionConfig, SubscriptionFilter, SubscriptionHandle,
};
use serde_json::{json, Value};
use std::fs;
use std::path::Path;
use std::process::Command;
use tempfile::TempDir;

fn config(path: &Path) -> StoreConfig {
    StoreConfig {
        path: path.to_owned(),
        ..Default::default()
    }
}

fn registration(id: &str) -> StateRegistration {
    StateRegistration {
        id: id.into(),
        strategy: StateStrategy::AppendLog {
            delta_snapshot_every: 100,
            full_snapshot_every: 100,
        },
        initial_value: None,
    }
}

fn append(store: &Store, n: i32) -> chronicle::Result<chronicle::Record> {
    store.update_state("items", StateOperation::Append(n.to_string().into_bytes()))
}

fn assert_io<T>(result: chronicle::Result<T>) {
    match result {
        Err(StoreError::Io(_)) => {}
        Err(other) => panic!("expected pending-checkpoint I/O error, got {other}"),
        Ok(_) => panic!("mutation succeeded despite pending checkpoint failure"),
    }
}

// Replacing a metadata pathname with a directory forces atomic publication
// to fail on every platform, without relying on root/permission behavior.
fn obstruct(path: &Path) -> Option<Vec<u8>> {
    let previous = if path.exists() {
        Some(fs::read(path).unwrap())
    } else {
        None
    };
    if previous.is_some() {
        fs::remove_file(path).unwrap();
    }
    fs::create_dir(path).unwrap();
    previous
}

fn repair(path: &Path, previous: Option<Vec<u8>>) {
    fs::remove_dir(path).unwrap();
    if let Some(bytes) = previous {
        fs::write(path, bytes).unwrap();
    }
}

fn check_write_admission(store: &Store) {
    let before_len = fs::metadata(store.path().join("records.log"))
        .unwrap()
        .len();
    let before_count = store.stats().unwrap().record_count;
    let before_states = store.stats().unwrap().state_slot_count;
    let before_branches = store.list_branches().len();
    let before_head = store.current_branch().head;
    assert_io(store.append(RecordInput::raw("event", b"blocked".to_vec())));
    assert_io(append(store, 99));
    assert_io(store.register_state(registration("other")));
    assert_io(store.update_state_strategy(
        "items",
        StateStrategy::AppendLog {
            delta_snapshot_every: 50,
            full_snapshot_every: 20,
        },
    ));
    assert_io(store.register_state_field_index("items", "/n", FieldIndexKind::Number));
    assert_io(store.create_branch("blocked", None));
    assert_io(store.create_empty_branch("blocked-empty", None));
    assert_io(store.create_branch_at("blocked-at", "main", Sequence(0)));
    assert_io(store.switch_branch("main"));
    // The retry comes first: without it, this fails with BranchNotFound.
    assert_io(store.switch_branch("missing"));
    assert_io(store.delete_branch("missing"));
    assert_io(store.sync());
    assert_eq!(
        fs::metadata(store.path().join("records.log"))
            .unwrap()
            .len(),
        before_len
    );
    assert_eq!(store.stats().unwrap().record_count, before_count);
    assert_eq!(store.stats().unwrap().state_slot_count, before_states);
    assert_eq!(store.list_branches().len(), before_branches);
    assert_eq!(store.current_branch().head, before_head);
    assert!(store
        .query_state_index_range("items", "/n", None, None, None, None, false)
        .is_none());
}

fn branch_events(store: &Store) -> SubscriptionHandle {
    let handle = store.subscribe(SubscriptionConfig {
        filter: SubscriptionFilter {
            include_branch_events: true,
            ..Default::default()
        },
        ..Default::default()
    });
    store.catch_up_subscription(handle.id).unwrap();
    assert!(matches!(handle.try_recv(), Ok(StoreEvent::CaughtUp)));
    handle
}

/// The branch creations and deletions a subscriber has heard of since the
/// last call.
fn announced(events: &SubscriptionHandle) -> Vec<String> {
    events
        .receiver
        .try_iter()
        .filter_map(|event| match event {
            StoreEvent::BranchCreated { branch } => Some(format!("created {}", branch.name)),
            StoreEvent::BranchDeleted { name } => Some(format!("deleted {name}")),
            _ => None,
        })
        .collect()
}

// An empty branch created with no parent named is created under main, and its
// announcement names that parent, whichever branch is current.
#[test]
fn an_empty_branch_is_announced_with_the_parent_it_was_created_under() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    store.create_branch("feature", None).unwrap();
    store.switch_branch("feature").unwrap();
    let events = branch_events(&store);

    let branch = store.create_empty_branch("empty", None).unwrap();
    let main = store
        .list_branches()
        .into_iter()
        .find(|b| b.name == "main")
        .unwrap();
    assert_eq!(branch.parent, Some(main.id));
    let parents: Vec<Option<String>> = events
        .receiver
        .try_iter()
        .filter_map(|event| match event {
            StoreEvent::BranchCreated { branch } => Some(branch.parent),
            _ => None,
        })
        .collect();
    assert_eq!(parents, vec![Some("main".to_string())]);
}

fn create_branch(store: &Store, kind: &str) -> chronicle::Result<Branch> {
    let items = StateSelection {
        state_ids: vec!["items".into()],
        state_prefixes: Vec::new(),
    };
    match kind.trim_end_matches("-abandoned") {
        "branch" => store.create_branch("feature", None),
        "selected" => store.create_branch_with_states("feature", None, &items),
        "empty" => store.create_empty_branch("feature", None),
        "at" => store.create_branch_at("feature", "main", Sequence(1)),
        "at-selected" => store.create_branch_at_with_states("feature", "main", Sequence(1), &items),
        other => panic!("unknown branch kind {other}"),
    }
}

/// Open the store again. A child process that a concurrent test spawns holds
/// this process's open files, the store's LOCK among them, until it execs.
fn reopen(path: &Path) -> Store {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match Store::open(config(path)) {
            Err(StoreError::Locked) if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(5))
            }
            result => return result.unwrap(),
        }
    }
}

fn names(store: &Store) -> Vec<String> {
    let mut names: Vec<String> = store.list_branches().into_iter().map(|b| b.name).collect();
    names.sort();
    names
}

#[test]
fn failed_checkpoint_child() {
    let Some(path) = std::env::var_os("CHRONICLE_CHECKPOINT_FAILURE_PATH") else {
        return;
    };
    let path = Path::new(&path);
    let scenario = std::env::var("CHRONICLE_CHECKPOINT_FAILURE_SCENARIO").unwrap();
    let store = Store::create(config(path)).unwrap();
    store.set_auto_snapshot(false);
    if scenario == "registration" {
        let state_path = path.join("state.bin");
        let previous = obstruct(&state_path);
        assert_io(store.register_state(registration("items")));
        assert_io(store.register_state(registration("items")));
        check_write_admission(&store);
        assert_eq!(store.stats().unwrap().state_slot_count, 0);
        repair(&state_path, previous);
        store.register_state(registration("items")).unwrap();
    } else if scenario == "registration-in-state-bin" {
        let branches_path = path.join("branches.bin");
        let previous = obstruct(&branches_path);
        assert_io(store.register_state(registration("items")));
        assert_eq!(store.stats().unwrap().state_slot_count, 0);
        repair(&branches_path, previous);
        std::process::exit(73);
    } else if scenario.starts_with("delete") {
        store.register_state(registration("items")).unwrap();
        append(&store, 0).unwrap();
        let events = branch_events(&store);
        store.create_branch("feature", None).unwrap();
        assert_eq!(announced(&events), ["created feature"]);
        let branches_path = path.join("branches.bin");
        let previous = obstruct(&branches_path);
        assert_io(store.delete_branch("feature"));
        assert_eq!(names(&store), ["feature", "main"]);
        check_write_admission(&store);
        assert!(announced(&events).is_empty());
        repair(&branches_path, previous);
        assert!(matches!(
            store.append_many_to_state_json("items", &[]),
            Err(StoreError::InvalidOperation(_))
        ));
        assert!(matches!(
            store.append_many_to_state_json_with_identity("items", &[], "id", "seq"),
            Err(StoreError::InvalidOperation(_))
        ));
        store.sync().unwrap();
        assert!(announced(&events).is_empty(), "an undone deletion is never announced");
        if scenario == "delete" {
            store.delete_branch("feature").unwrap();
            assert_eq!(announced(&events), ["deleted feature"]);
            store.sync().unwrap();
            assert!(announced(&events).is_empty(), "announced once");
        }
    } else {
        store.register_state(registration("items")).unwrap();
        append(&store, 0).unwrap();
        append(&store, 1).unwrap();
        store.sync().unwrap();
        let probe = store.create_empty_branch("probe", None).unwrap();
        store.delete_branch("probe").unwrap();
        let events = branch_events(&store);
        let branches_path = path.join("branches.bin");
        let previous = obstruct(&branches_path);
        assert_io(create_branch(&store, &scenario));
        assert_eq!(names(&store), ["main"]);
        check_write_admission(&store);
        assert!(announced(&events).is_empty());
        repair(&branches_path, previous);
        if !scenario.ends_with("-abandoned") {
            let branch = create_branch(&store, &scenario).unwrap();
            assert_eq!(announced(&events), ["created feature"]);
            assert!(
                branch.id.0 > probe.id.0 + 1,
                "the failed call's ID stays used: {:?} after {:?}",
                branch.id,
                probe.id
            );
            store.switch_branch("feature").unwrap();
            assert!(announced(&events).is_empty(), "announced once");
        }
    }
    append(&store, 42).unwrap();
    std::process::exit(73);
}

fn run_crash(path: &Path, scenario: &str) {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "failed_checkpoint_child", "--nocapture"])
        .env("CHRONICLE_CHECKPOINT_FAILURE_PATH", path)
        .env("CHRONICLE_CHECKPOINT_FAILURE_SCENARIO", scenario)
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(73),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn failed_registration_blocks_writes_until_checkpoint_retry_succeeds() {
    let dir = TempDir::new().unwrap();
    run_crash(dir.path(), "registration");
    let store = Store::open(config(dir.path())).unwrap();
    assert_eq!(store.stats().unwrap().state_slot_count, 1);
    assert_eq!(store.get_state_len("items").unwrap(), Some(1));
    assert_eq!(
        serde_json::from_slice::<Value>(&store.get_state("items").unwrap().unwrap()).unwrap(),
        json!([42])
    );
}

#[test]
fn a_registration_whose_branch_list_save_failed_can_be_on_disk_at_reopen() {
    let dir = TempDir::new().unwrap();
    run_crash(dir.path(), "registration-in-state-bin");
    let store = Store::open(config(dir.path())).unwrap();
    assert_eq!(store.stats().unwrap().state_slot_count, 1);
    assert!(matches!(
        store.register_state(registration("items")),
        Err(StoreError::StateExists(_))
    ));
    append(&store, 7).unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&store.get_state("items").unwrap().unwrap()).unwrap(),
        json!([7])
    );
}

#[test]
fn failed_branch_publication_blocks_writes_until_checkpoint_retry_succeeds() {
    for (kind, expected) in [
        ("branch", json!([0, 1, 42])),
        ("selected", json!([0, 1, 42])),
        ("at", json!([0, 42])),
        ("at-selected", json!([0, 42])),
        ("empty", json!([42])),
    ] {
        let dir = TempDir::new().unwrap();
        run_crash(dir.path(), kind);
        let store = Store::open(config(dir.path())).unwrap();
        assert_eq!(store.current_branch().name, "feature");
        assert_eq!(names(&store), ["feature", "main"], "{kind}");
        assert_eq!(
            serde_json::from_slice::<Value>(&store.get_state("items").unwrap().unwrap()).unwrap(),
            expected,
            "{kind}"
        );
        store.switch_branch("main").unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&store.get_state("items").unwrap().unwrap()).unwrap(),
            json!([0, 1])
        );
    }
}

#[test]
fn an_abandoned_branch_creation_leaves_no_branch_at_reopen() {
    for kind in ["branch-abandoned", "empty-abandoned"] {
        let dir = TempDir::new().unwrap();
        run_crash(dir.path(), kind);
        let store = Store::open(config(dir.path())).unwrap();
        assert_eq!(names(&store), ["main"], "{kind}");
        let items = || {
            serde_json::from_slice::<Value>(&store.get_state("items").unwrap().unwrap()).unwrap()
        };
        assert_eq!(items(), json!([0, 1, 42]), "{kind}");
        store.create_branch("later", None).unwrap();
        store.switch_branch("later").unwrap();
        assert_eq!(items(), json!([0, 1, 42]), "{kind}");
    }
}

#[test]
fn a_deletion_made_again_after_an_undone_one_is_announced_once_and_holds_at_reopen() {
    let dir = TempDir::new().unwrap();
    run_crash(dir.path(), "delete");
    let store = Store::open(config(dir.path())).unwrap();
    assert_eq!(names(&store), ["main"]);
    assert_eq!(
        serde_json::from_slice::<Value>(&store.get_state("items").unwrap().unwrap()).unwrap(),
        json!([0, 42])
    );
}

#[test]
fn an_abandoned_branch_deletion_leaves_the_branch_whole_at_reopen() {
    let dir = TempDir::new().unwrap();
    run_crash(dir.path(), "delete-abandoned");
    let store = Store::open(config(dir.path())).unwrap();
    assert_eq!(names(&store), ["feature", "main"]);
    let feature = store.get_branch("feature").unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&store.get_state_on(feature.id, "items").unwrap().unwrap())
            .unwrap(),
        json!([0])
    );
}

#[test]
fn a_switch_whose_branch_list_save_fails_leaves_the_source_selected() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    store.register_state(registration("items")).unwrap();
    let target = store.create_branch("target", None).unwrap();
    append(&store, 1).unwrap();
    let branches_path = dir.path().join("branches.bin");
    let previous = obstruct(&branches_path);
    assert_io(store.switch_branch("target"));
    assert_eq!(store.current_branch().name, "main");
    repair(&branches_path, previous);
    append(&store, 2).unwrap();
    let items = |branch| {
        serde_json::from_slice::<Value>(&store.get_state_on(branch, "items").unwrap().unwrap_or_default())
            .unwrap_or(Value::Null)
    };
    let main = store.get_branch("main").unwrap().id;
    assert_eq!(items(main), json!([1, 2]));
    assert_eq!(items(target.id), Value::Null);
    store.switch_branch("target").unwrap();
    assert_eq!(store.current_branch().name, "target");
    drop(store);
    let store = reopen(dir.path());
    assert_eq!(store.current_branch().name, "target");
}

#[test]
fn a_deletion_whose_checkpoint_fails_leaves_the_branch_with_what_the_store_held() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    store.set_auto_snapshot(true);
    store.register_state(registration("items")).unwrap();
    store
        .register_state(StateRegistration {
            id: "rows".into(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 2,
                full_snapshot_every: 100,
            },
            initial_value: None,
        })
        .unwrap();
    let feature = store.create_branch("feature", None).unwrap();
    let object = |n: i32| StateOperation::Append(serde_json::to_vec(&json!({ "n": n })).unwrap());
    store.update_state_on(feature.id, "items", object(1)).unwrap();
    store.update_state_on(feature.id, "items", object(2)).unwrap();
    store
        .register_state_field_index_on(feature.id, "items", "/n", FieldIndexKind::Number)
        .unwrap();
    store
        .update_state_on(feature.id, "rows", StateOperation::Append(b"not json".to_vec()))
        .unwrap();
    store
        .update_state_on(feature.id, "rows", StateOperation::Append(b"1".to_vec()))
        .unwrap();
    let failures = || {
        store
            .snapshot_failures()
            .into_iter()
            .map(|failure| (failure.branch, failure.state_id))
            .collect::<Vec<_>>()
    };
    assert_eq!(failures(), [("feature".to_string(), "rows".to_string())]);
    let read = || serde_json::from_slice::<Value>(&store.get_state_on(feature.id, "items").unwrap().unwrap()).unwrap();
    let query = || store.query_state_index_range_on(feature.id, "items", "/n", None, None, None, None, false);
    assert_eq!(read(), json!([{ "n": 1 }, { "n": 2 }]));
    assert_eq!(query(), Some(vec![0, 1]));
    let events = branch_events(&store);

    let branches_path = dir.path().join("branches.bin");
    let previous = obstruct(&branches_path);
    assert_io(store.delete_branch("feature"));
    assert_eq!(names(&store), ["feature", "main"]);
    assert_eq!(read(), json!([{ "n": 1 }, { "n": 2 }]));
    assert_eq!(query(), Some(vec![0, 1]));
    assert_eq!(failures(), [("feature".to_string(), "rows".to_string())]);
    assert!(announced(&events).is_empty());
    assert_io(store.update_state_on(feature.id, "items", object(3)));
    repair(&branches_path, previous);

    store.update_state_on(feature.id, "items", object(3)).unwrap();
    assert_eq!(read(), json!([{ "n": 1 }, { "n": 2 }, { "n": 3 }]));
    assert!(announced(&events).is_empty());
    store.delete_branch("feature").unwrap();
    assert_eq!(announced(&events), ["deleted feature"]);
    assert_eq!(failures(), []);
    store.sync().unwrap();
    assert!(announced(&events).is_empty(), "announced once");
}

#[test]
fn failed_historical_chain_read_does_not_allocate_a_partial_branch() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    store.register_state(registration("items")).unwrap();
    // The append fast path accepts these bytes, but this malformed payload
    // cannot be materialized while preparing the historical branch's count.
    store
        .update_state("items", StateOperation::Append(b"not-json".to_vec()))
        .unwrap();
    // The parent must have moved past `at`: a state whose head is at or
    // before the branch point is inherited verbatim, with no chain read.
    append(&store, 1).unwrap();
    let err = store
        .create_branch_at("bad", "main", Sequence(1))
        .unwrap_err();
    assert!(matches!(err, StoreError::Deserialization(_)), "{err}");
    assert_eq!(store.list_branches().len(), 1);
    let fresh = store.create_empty_branch("fresh", None).unwrap();
    assert_eq!(store.get_state_on(fresh.id, "items").unwrap(), None);
}

#[test]
fn updates_after_a_successful_checkpoint_do_not_checkpoint_again() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    store.set_auto_snapshot(false);
    let state_path = dir.path().join("state.bin");
    let published = || same_file::Handle::from_path(&state_path).unwrap();

    // After the registration's own checkpoint.
    store.register_state(registration("items")).unwrap();
    let registered = published();
    append(&store, 0).unwrap();
    append(&store, 1).unwrap();
    assert_eq!(published(), registered, "a state update checkpointed");

    // After a failed checkpoint that a later sync() finished.
    let previous = obstruct(&state_path);
    assert_io(store.register_state(registration("other")));
    repair(&state_path, previous);
    store.sync().unwrap();
    let retried = published();
    append(&store, 2).unwrap();
    append(&store, 3).unwrap();
    assert_eq!(published(), retried, "a state update checkpointed");
}

/// Collects what the test's tracing subscriber writes.
#[derive(Clone, Default)]
struct Logged(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for Logged {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn a_failed_final_sync_is_logged() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    let state_path = dir.path().join("state.bin");
    let previous = obstruct(&state_path);
    assert_io(store.register_state(registration("items")));

    let logged = Logged::default();
    let writer = logged.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .finish();
    tracing::subscriber::with_default(subscriber, || drop(store));
    repair(&state_path, previous);

    let text = String::from_utf8(logged.0.lock().unwrap().clone()).unwrap();
    let store_path = dir.path().display().to_string();
    assert!(
        text.lines().any(|line| line.contains("WARN")
            && line.contains(&store_path)
            && line.contains("can hold part of a change whose call failed")),
        "{text}"
    );
}
