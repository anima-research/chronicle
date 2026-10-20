//! `Store::create` on a directory that already holds a store (#34). It used to
//! truncate and rewrite MANIFEST before taking the lock, then build empty
//! state and branch managers whose first sync published over the store's own,
//! so a create where an open was meant kept the records and lost every
//! registration and branch, with no error. Now create takes the lock first and
//! refuses a directory that holds a store or a store's data files;
//! open_or_create decides by MANIFEST, so it creates into an empty directory
//! or a fresh volume's root; and opening a directory with no MANIFEST says
//! whether it's a damaged store or no store at all.

use chronicle::{StateOperation, StateRegistration, StateStrategy, Store, StoreConfig, StoreError};
use std::fs;
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

fn invalid_operation(result: chronicle::Result<Store>) -> String {
    match result {
        Err(StoreError::InvalidOperation(msg)) => msg,
        Err(e) => panic!("expected InvalidOperation, got {e}"),
        Ok(_) => panic!("succeeded where it should refuse"),
    }
}

#[test]
fn create_over_an_existing_store_refuses_and_keeps_it() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    populated(&path);

    let msg = invalid_operation(Store::create(config(&path)));
    assert!(msg.contains("already holds") && msg.contains(&*path.to_string_lossy()), "{msg}");

    let store = Store::open(config(&path)).unwrap();
    assert!(store.get_state("messages").unwrap().is_some(), "the registration survives");
    assert!(
        store.list_branches().iter().any(|b| b.name == "feature"),
        "the branch survives"
    );
}

#[test]
fn create_refuses_each_data_file_of_a_store_without_its_manifest() {
    for file in ["records.log", "state.bin", "branches.bin"] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("damaged");
        fs::create_dir(&path).unwrap();
        fs::write(path.join(file), b"x").unwrap();

        let msg = invalid_operation(Store::create(config(&path)));
        assert!(
            msg.contains(file) && msg.contains("damaged") && msg.contains(&*path.to_string_lossy()),
            "{file}: {msg}"
        );
        // open_or_create sends it to create (no MANIFEST), which refuses it too.
        invalid_operation(Store::open_or_create(config(&path)));
        assert_eq!(fs::read(path.join(file)).unwrap(), b"x", "{file} is untouched");
        assert!(!path.join("MANIFEST").exists(), "{file}: no MANIFEST was written");
    }
}

/// A create aimed at a store another process has open writes nothing there.
/// A read-only MANIFEST makes a write fail loudly for an ordinary user, and a
/// manifest backdated a day catches a rewrite of identical bytes where the
/// mode doesn't stop it (as root). Unix only: it sets file modes.
#[cfg(unix)]
#[test]
fn create_aimed_at_an_open_store_writes_nothing_there() {
    use std::os::unix::fs::PermissionsExt;
    use std::time::{Duration, SystemTime};

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let held = Store::create(config(&path)).unwrap();

    let manifest = path.join("MANIFEST");
    let before = fs::read(&manifest).unwrap();
    let a_day_ago = SystemTime::now() - Duration::from_secs(86_400);
    fs::File::options().write(true).open(&manifest).unwrap().set_modified(a_day_ago).unwrap();
    fs::set_permissions(&manifest, fs::Permissions::from_mode(0o444)).unwrap();
    let result = Store::create(config(&path));
    fs::set_permissions(&manifest, fs::Permissions::from_mode(0o644)).unwrap();

    assert!(matches!(result, Err(StoreError::Locked)), "got {:?}", result.err().map(|e| e.to_string()));
    assert_eq!(fs::read(&manifest).unwrap(), before);
    assert_eq!(fs::metadata(&manifest).unwrap().modified().unwrap(), a_day_ago, "MANIFEST was rewritten");
    drop(held);
}

#[test]
fn open_or_create_creates_into_an_existing_empty_directory() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("volume");
    fs::create_dir(&path).unwrap();

    drop(Store::open_or_create(config(&path)).unwrap());
    assert!(path.join("MANIFEST").exists());
    Store::open(config(&path)).unwrap();
}

#[test]
fn open_or_create_creates_at_a_fresh_volumes_root() {
    // mke2fs leaves lost+found at the root of a new ext4 volume.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("volume");
    fs::create_dir_all(path.join("lost+found")).unwrap();

    drop(Store::open_or_create(config(&path)).unwrap());
    assert!(path.join("MANIFEST").exists());
    assert!(path.join("lost+found").is_dir(), "what was there stays");
}

#[test]
fn open_or_create_finishes_a_create_that_stopped_before_its_manifest() {
    // What a create interrupted after taking the lock leaves.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    fs::create_dir_all(path.join("blobs")).unwrap();
    fs::write(path.join("LOCK"), b"").unwrap();

    drop(Store::open_or_create(config(&path)).unwrap());
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
fn opening_a_directory_without_a_manifest_says_what_it_is() {
    let dir = TempDir::new().unwrap();
    let not_a_store = dir.path().join("not-a-store");
    fs::create_dir(&not_a_store).unwrap();
    fs::write(not_a_store.join("notes.txt"), b"someone else's").unwrap();
    let damaged = dir.path().join("damaged");
    fs::create_dir(&damaged).unwrap();
    fs::write(damaged.join("records.log"), b"x").unwrap();

    for (path, says) in [(&not_a_store, "isn't a Chronicle store"), (&damaged, "damaged Chronicle store")] {
        match Store::open(config(path)) {
            Err(StoreError::InvalidFormat(msg)) => {
                assert!(msg.contains("MANIFEST") && msg.contains(says) && msg.contains(&*path.to_string_lossy()), "{msg}")
            }
            Err(e) => panic!("expected InvalidFormat, got {e}"),
            Ok(_) => panic!("opened a directory with no MANIFEST"),
        }
    }
}
