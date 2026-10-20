//! `Store::create` on a directory that already holds a store (#34). It used to
//! truncate and rewrite MANIFEST before taking the lock, then build empty
//! state and branch managers whose first sync published over the store's own,
//! so a create where an open was meant kept the records and lost every
//! registration and branch, with no error. Now create takes the lock first and
//! refuses a directory that holds a store; open_or_create creates into an
//! existing empty directory; and opening a directory with no MANIFEST says so.

use chronicle::{StateOperation, StateRegistration, StateStrategy, Store, StoreConfig, StoreError};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use tempfile::TempDir;

fn config(path: &Path) -> StoreConfig {
    StoreConfig {
        path: path.to_owned(),
        ..Default::default()
    }
}

/// A store with one registered state holding one record, and one branch
/// beyond main, synced and closed.
fn populated(path: &Path) {
    let store = Store::create(config(path)).unwrap();
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
    store
        .update_state("messages", StateOperation::Append(b"{\"n\":1}".to_vec()))
        .unwrap();
    store.create_branch("feature", None).unwrap();
    store.sync().unwrap();
}

#[test]
fn create_over_an_existing_store_refuses_and_keeps_it() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    populated(&path);

    match Store::create(config(&path)) {
        Err(StoreError::InvalidOperation(msg)) => assert!(msg.contains("already holds"), "{msg}"),
        Err(e) => panic!("expected InvalidOperation, got {e}"),
        Ok(_) => panic!("create over an existing store succeeded"),
    }

    let store = Store::open(config(&path)).unwrap();
    assert!(store.get_state("messages").unwrap().is_some(), "the registration survives");
    assert!(
        store.list_branches().iter().any(|b| b.name == "feature"),
        "the branch survives"
    );
}

#[test]
fn create_aimed_at_an_open_store_writes_nothing_there() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let held = Store::create(config(&path)).unwrap();

    // A read-only MANIFEST makes any write to it fail loudly: a create that
    // wrote the manifest before taking the lock reports an IO error here, one
    // that takes the lock first reports Locked and never reaches the file.
    let manifest = path.join("MANIFEST");
    let before = fs::read(&manifest).unwrap();
    fs::set_permissions(&manifest, fs::Permissions::from_mode(0o444)).unwrap();
    let result = Store::create(config(&path));
    fs::set_permissions(&manifest, fs::Permissions::from_mode(0o644)).unwrap();

    assert!(matches!(result, Err(StoreError::Locked)), "got {:?}", result.err().map(|e| e.to_string()));
    assert_eq!(fs::read(&manifest).unwrap(), before);
    drop(held);
}

#[test]
fn open_or_create_creates_into_an_existing_empty_directory() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("volume");
    fs::create_dir(&path).unwrap();

    let store = Store::open_or_create(config(&path)).unwrap();
    drop(store);
    assert!(path.join("MANIFEST").exists());
    Store::open(config(&path)).unwrap();
}

#[test]
fn open_or_create_still_refuses_an_empty_directory_without_create_if_missing() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("volume");
    fs::create_dir(&path).unwrap();

    let result = Store::open_or_create(StoreConfig { create_if_missing: false, ..config(&path) });
    assert!(matches!(result, Err(StoreError::NotInitialized)), "got {:?}", result.err().map(|e| e.to_string()));
}

#[test]
fn opening_a_directory_without_a_manifest_names_it() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("not-a-store");
    fs::create_dir(&path).unwrap();
    fs::write(path.join("notes.txt"), b"someone else's").unwrap();

    match Store::open(config(&path)) {
        Err(StoreError::InvalidFormat(msg)) => {
            assert!(msg.contains("MANIFEST") && msg.contains(&*path.to_string_lossy()), "{msg}")
        }
        Err(e) => panic!("expected InvalidFormat naming the MANIFEST and path, got {e}"),
        Ok(_) => panic!("opened a directory that isn't a store"),
    }
}
