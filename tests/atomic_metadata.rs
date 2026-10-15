//! Every metadata saver publishes a new file, leaving an open previous
//! checkpoint intact. These tests fail against truncate-in-place saves.

use chronicle::{
    FieldIndexKind, StateOperation, StateRegistration, StateStrategy, Store, StoreConfig,
    StoreError,
};
use same_file::Handle;
use std::fs::{self, File};
use std::io::Read;
use std::path::Path;
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn config(path: &Path) -> StoreConfig {
    StoreConfig {
        path: path.to_owned(),
        ..Default::default()
    }
}

/// Opens a store this process closed a moment ago. A child that a
/// concurrent test spawns shares this process's open files until it execs,
/// the store's LOCK among them, so the lock can still be held for that
/// moment.
fn reopen(path: &Path) -> Store {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match Store::open(config(path)) {
            Err(StoreError::Locked) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5))
            }
            result => return result.unwrap(),
        }
    }
}

fn append(store: &Store, timestamp: u64) {
    store
        .update_state(
            "messages",
            StateOperation::Append(format!("{{\"timestamp\":{timestamp}}}").into_bytes()),
        )
        .unwrap();
}

fn register(store: &Store) {
    store
        .register_state(StateRegistration {
            id: "messages".into(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 100,
                full_snapshot_every: 100,
            },
            initial_value: None,
        })
        .unwrap();
}

fn assert_published_without_truncating(name: &str) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let store = Store::create(config(&path)).unwrap();
    register(&store);
    append(&store, 10);
    store
        .register_state_field_index("messages", "/timestamp", FieldIndexKind::Number)
        .unwrap();
    store.sync().unwrap();

    let checkpoint = path.join(name);
    let previous_bytes = fs::read(&checkpoint).unwrap();
    let mut previous_file = File::open(&checkpoint).unwrap();
    append(&store, 20);
    store.create_branch("feature", None).unwrap();
    store.sync().unwrap();

    let mut still_previous = Vec::new();
    previous_file.read_to_end(&mut still_previous).unwrap();
    assert_eq!(
        still_previous, previous_bytes,
        "{name} was overwritten in place"
    );
    assert_ne!(
        fs::read(&checkpoint).unwrap(),
        previous_bytes,
        "{name} did not advance"
    );
    drop(previous_file);
    drop(store);

    let reopened = reopen(&path);
    assert_eq!(reopened.get_state_len("messages").unwrap(), Some(2));
    assert!(reopened.list_branches().iter().any(|b| b.name == "feature"));
    assert_eq!(
        reopened.query_state_index_range(
            "messages",
            "/timestamp",
            Some(10.0),
            Some(20.0),
            None,
            None,
            false
        ),
        Some(vec![0, 1]),
    );
}

#[test]
fn sync_only_republishes_metadata_that_changed() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let store = Store::create(config(&path)).unwrap();
    register(&store);
    append(&store, 10);
    store
        .register_state_field_index("messages", "/timestamp", FieldIndexKind::Number)
        .unwrap();
    store.sync().unwrap();
    let names = ["state.bin", "state-indexes.bin", "branches.bin"];
    let identities = || names.map(|name| Handle::from_path(path.join(name)).unwrap());
    let first = identities();

    store.sync().unwrap();
    assert_eq!(
        identities(),
        first,
        "unchanged sync must not republish metadata"
    );

    store
        .append(chronicle::RecordInput::raw("message", b"raw".to_vec()))
        .unwrap();
    store.sync().unwrap();
    let raw = identities();
    assert_eq!(raw[0], first[0], "raw append leaves state metadata alone");
    assert_eq!(raw[1], first[1], "raw append leaves field indexes alone");
    assert_ne!(raw[2], first[2], "the new branch head must persist");

    append(&store, 20);
    store.sync().unwrap();
    let state = identities();
    for i in 0..names.len() {
        assert_ne!(state[i], raw[i], "{} changed", names[i]);
    }
    store.sync().unwrap();
    assert_eq!(identities(), state);

    drop(store);
    let reopened = reopen(&path);
    // Loading bytes is not a durability acknowledgement from this writer.
    reopened.sync().unwrap();
    let loaded = identities();
    for i in 0..names.len() {
        assert_ne!(loaded[i], state[i], "{} first save after load", names[i]);
    }
    reopened.sync().unwrap();
    assert_eq!(identities(), loaded);
    assert_eq!(reopened.get_state_len("messages").unwrap(), Some(2));
}

#[test]
fn state_checkpoint_is_replaced() {
    assert_published_without_truncating("state.bin");
}

#[test]
fn branch_checkpoint_is_replaced() {
    assert_published_without_truncating("branches.bin");
}

#[test]
fn field_index_checkpoint_is_replaced() {
    assert_published_without_truncating("state-indexes.bin");
}

// #19: a sync cut off while writing `state.bin` must leave the store opening
// at the checkpoint before it. The child's writes fail for real, at the file
// size limit, partway through the new checkpoint. On `main`, which truncated
// `state.bin` and rewrote it in place, the reopen fails reading it.
#[cfg(unix)]
#[test]
fn interrupted_sync_reopens_at_the_previous_checkpoint() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let store = Store::create(config(&path)).unwrap();
    register(&store);
    append(&store, 10);
    store.sync().unwrap();
    drop(store);
    let previous = fs::read(path.join("state.bin")).unwrap();

    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "interrupted_sync_child"])
        .env("CHRONICLE_INTERRUPTED_SYNC_STORE", &path)
        // Half the previous checkpoint, so the next one is cut partway.
        .env(
            "CHRONICLE_INTERRUPTED_SYNC_LIMIT",
            (previous.len() / 2).to_string(),
        )
        .output()
        .unwrap();
    // 73 comes only from the end of the child test, so a filter that ran no
    // test, which exits 0, can't pass for an interrupted sync.
    assert_eq!(
        child.status.code(),
        Some(73),
        "child failed: {}{}",
        String::from_utf8_lossy(&child.stdout),
        String::from_utf8_lossy(&child.stderr)
    );

    assert_eq!(fs::read(path.join("state.bin")).unwrap(), previous);
    let reopened = reopen(&path);
    assert_eq!(reopened.get_state_len("messages").unwrap(), Some(1));
}

#[cfg(unix)]
#[test]
fn interrupted_sync_child() {
    let Some(path) = std::env::var_os("CHRONICLE_INTERRUPTED_SYNC_STORE") else {
        return;
    };
    let limit: u64 = std::env::var("CHRONICLE_INTERRUPTED_SYNC_LIMIT")
        .unwrap()
        .parse()
        .unwrap();
    let store = Store::open(config(Path::new(&path))).unwrap();
    append(&store, 20);
    // From here a write past `limit` bytes of any file fails with EFBIG, which
    // SIGXFSZ would otherwise turn into the process's death. The appended
    // record is already in the log, so only the checkpoints meet the limit.
    let limit = limit as libc::rlim_t;
    let rlimit = libc::rlimit {
        rlim_cur: limit,
        rlim_max: limit,
    };
    // SAFETY: plain libc calls with a valid, initialized rlimit.
    unsafe {
        libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
        assert_eq!(libc::setrlimit(libc::RLIMIT_FSIZE, &rlimit), 0);
    }
    match store.sync() {
        Err(chronicle::StoreError::Io(error)) => {
            assert_eq!(error.raw_os_error(), Some(libc::EFBIG), "{error}")
        }
        other => panic!("expected the checkpoint write to fail with EFBIG, got {other:?}"),
    }
    // Without destructors, so Drop's sync can't touch the store again.
    std::process::exit(73);
}

#[test]
fn open_removes_staging_files_left_by_interrupted_saves() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    drop(Store::create(config(&path)).unwrap());
    // What a save killed before its rename leaves beside the checkpoint.
    let stage = path.join(".chronicle-checkpoint-a1B2c3");
    fs::write(&stage, b"unfinished checkpoint").unwrap();
    // Saves only ever stage regular files, so a link with the name stays.
    #[cfg(unix)]
    let link = {
        let link = path.join(".chronicle-checkpoint-link");
        std::os::unix::fs::symlink("state.bin", &link).unwrap();
        link
    };

    let _store = reopen(&path);
    assert!(!stage.exists());
    #[cfg(unix)]
    assert!(fs::symlink_metadata(&link)
        .unwrap()
        .file_type()
        .is_symlink());
}

// The sweep waits for the store lock: an open that finds the store locked
// fails before it touches a staging file, which may be a save's in progress.
#[test]
fn open_of_a_locked_store_leaves_its_staging_files() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let _store = Store::create(config(&path)).unwrap();
    let stage = path.join(".chronicle-checkpoint-a1B2c3");
    fs::write(&stage, b"a save in progress").unwrap();

    assert!(matches!(
        Store::open(config(&path)),
        Err(chronicle::StoreError::Locked)
    ));
    assert!(stage.exists());
}
