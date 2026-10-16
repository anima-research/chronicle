//! A failed metadata checkpoint must not admit dependent writes. The child
//! repairs the injected I/O failure, writes successfully, then exits without
//! destructors so reopen cannot rely on a final Store::drop checkpoint.

use chronicle::{
    Branch, FieldIndexKind, RecordInput, Sequence, StateOperation, StateRegistration,
    StateStrategy, Store, StoreConfig, StoreError, StoreEvent, SubscriptionConfig,
    SubscriptionFilter, SubscriptionHandle,
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
    assert_eq!(store.stats().unwrap().state_slot_count, 1);
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
    match kind {
        "branch" => store.create_branch("feature", None),
        "empty" => store.create_empty_branch("feature", None),
        "at" => store.create_branch_at("feature", "main", Sequence(1)),
        other => panic!("unknown branch kind {other}"),
    }
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
        repair(&state_path, previous);
        // Retry finishes the pending checkpoint before reporting the retained
        // registration. Subsequent writes may now safely use its definition.
        assert!(matches!(
            store.register_state(registration("items")),
            Err(StoreError::StateExists(_))
        ));
    } else if scenario == "delete" {
        store.register_state(registration("items")).unwrap();
        append(&store, 0).unwrap();
        let events = branch_events(&store);
        store.create_branch("feature", None).unwrap();
        assert_eq!(announced(&events), ["created feature"]);
        let branches_path = path.join("branches.bin");
        let previous = obstruct(&branches_path);
        assert_io(store.delete_branch("feature"));
        check_write_admission(&store);
        // Subscribers hear of the deletion once a checkpoint persists it.
        assert!(announced(&events).is_empty());
        repair(&branches_path, previous);
        store.sync().unwrap();
        assert_eq!(announced(&events), ["deleted feature"]);
        store.sync().unwrap();
        assert!(announced(&events).is_empty(), "announced once");
    } else {
        store.register_state(registration("items")).unwrap();
        append(&store, 0).unwrap();
        append(&store, 1).unwrap();
        store.sync().unwrap();
        let events = branch_events(&store);
        let branches_path = path.join("branches.bin");
        let previous = obstruct(&branches_path);
        assert_io(create_branch(&store, &scenario));
        check_write_admission(&store);
        // Subscribers hear of the branch once a checkpoint persists it: here,
        // the retry that the second creation makes before it finds the name
        // taken.
        assert!(announced(&events).is_empty());
        repair(&branches_path, previous);
        assert!(matches!(
            create_branch(&store, &scenario),
            Err(StoreError::BranchExists(_))
        ));
        assert_eq!(announced(&events), ["created feature"]);
        store.switch_branch("feature").unwrap();
        assert!(announced(&events).is_empty(), "announced once");
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
fn failed_branch_publication_blocks_writes_until_checkpoint_retry_succeeds() {
    for (kind, expected) in [
        ("branch", json!([0, 1, 42])),
        ("at", json!([0, 42])),
        ("empty", json!([42])),
    ] {
        let dir = TempDir::new().unwrap();
        run_crash(dir.path(), kind);
        let store = Store::open(config(dir.path())).unwrap();
        assert_eq!(store.current_branch().name, "feature");
        assert_eq!(store.list_branches().len(), 2);
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
fn failed_branch_deletion_is_announced_once_its_checkpoint_succeeds() {
    let dir = TempDir::new().unwrap();
    run_crash(dir.path(), "delete");
    let store = Store::open(config(dir.path())).unwrap();
    assert_eq!(store.list_branches().len(), 1);
    assert_eq!(
        serde_json::from_slice::<Value>(&store.get_state("items").unwrap().unwrap()).unwrap(),
        json!([0, 42])
    );
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
    let err = store
        .create_branch_at("bad", "main", Sequence(1))
        .unwrap_err();
    assert!(matches!(err, StoreError::Deserialization(_)), "{err}");
    assert_eq!(store.list_branches().len(), 1);
    let fresh = store.create_empty_branch("fresh", None).unwrap();
    assert_eq!(fresh.id.0, 2);
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
    assert!(text.contains("final sync on drop failed"), "{text}");
}
