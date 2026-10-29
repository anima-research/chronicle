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
fn open_or_create_refuses_a_directory_holding_other_files_and_writes_nothing() {
    // A mistaken path to a project directory: main refused it with an IO
    // error, and it mustn't become a place to put a store.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("project");
    fs::create_dir(&path).unwrap();
    fs::write(path.join("Cargo.toml"), b"[package]").unwrap();

    match Store::open_or_create(config(&path)) {
        Err(StoreError::InvalidFormat(msg)) => assert!(msg.contains("Cargo.toml") && msg.contains("no MANIFEST"), "{msg}"),
        Err(e) => panic!("expected InvalidFormat naming the file, got {e}"),
        Ok(_) => panic!("created a store in a directory of other files"),
    }
    assert!(!path.join("MANIFEST").exists() && !path.join("LOCK").exists(), "nothing was written");
}

#[test]
fn open_or_create_creates_beside_a_finder_ds_store() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("volume");
    fs::create_dir(&path).unwrap();
    fs::write(path.join(".DS_Store"), b"").unwrap();
    drop(Store::open_or_create(config(&path)).unwrap());
    assert!(path.join("MANIFEST").exists());
}

#[test]
fn open_or_create_finishes_a_create_that_stopped_inside_its_manifest_save() {
    // The manifest is saved through a staging file renamed over MANIFEST; a
    // create that stops between the two leaves that file beside LOCK and
    // blobs/ (Basil-1007's case on #46).
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    fs::create_dir_all(path.join("blobs")).unwrap();
    fs::write(path.join("LOCK"), b"").unwrap();
    let staged = path.join(".chronicle-checkpoint-AbC123");
    fs::write(&staged, b"CHR").unwrap();

    drop(Store::open_or_create(config(&path)).unwrap());
    Store::open(config(&path)).unwrap();
    assert!(!staged.exists(), "the interrupted save's staging file is swept");
}

#[test]
fn open_or_create_without_create_if_missing_refuses_a_directory_with_no_manifest() {
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

#[cfg(unix)]
#[test]
fn a_store_file_left_as_a_link_to_nothing_is_refused_and_nothing_is_written_through_it() {
    for file in ["MANIFEST", "records.log", "state.bin", "branches.bin"] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("store");
        let volume = dir.path().join("unmounted");
        fs::create_dir(&path).unwrap();
        fs::create_dir(&volume).unwrap();
        std::os::unix::fs::symlink(volume.join(file), path.join(file)).unwrap();

        let msg = invalid_operation(Store::create(config(&path)));
        assert!(msg.contains(&*path.to_string_lossy()), "{file}: {msg}");
        match Store::open_or_create(config(&path)) {
            Err(StoreError::InvalidOperation(_)) | Err(StoreError::InvalidFormat(_)) => {}
            Err(e) => panic!("{file}: expected a refusal, got {e}"),
            Ok(_) => panic!("{file}: open_or_create took a directory holding a link to a missing {file}"),
        }
        assert_eq!(fs::read_dir(&volume).unwrap().count(), 0, "{file}: nothing was written through the link");
        assert!(
            file == "MANIFEST" || fs::symlink_metadata(path.join("MANIFEST")).is_err(),
            "{file}: no MANIFEST was written"
        );
    }
}

#[cfg(unix)]
#[test]
fn opening_a_store_whose_manifest_links_to_nothing_names_the_link() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    populated(&path);
    let volume = dir.path().join("unmounted");
    fs::create_dir(&volume).unwrap();
    fs::remove_file(path.join("MANIFEST")).unwrap();
    std::os::unix::fs::symlink(volume.join("MANIFEST"), path.join("MANIFEST")).unwrap();

    for result in [Store::open(config(&path)), Store::open_or_create(config(&path))] {
        match result {
            Err(StoreError::InvalidFormat(msg)) => assert!(
                msg.contains("symbolic link") && msg.contains(&*volume.join("MANIFEST").to_string_lossy()),
                "{msg}"
            ),
            Err(e) => panic!("expected InvalidFormat naming the link, got {e}"),
            Ok(_) => panic!("opened a store whose MANIFEST links to nothing"),
        }
    }
    assert_eq!(fs::read_dir(&volume).unwrap().count(), 0, "nothing was written through the link");
}
