//! `Store::create` on a directory that already holds a store (#34). It used to
//! truncate and rewrite MANIFEST before taking the lock, then build empty
//! state and branch managers whose first sync published over the store's own,
//! so a create where an open was meant kept the records and lost every
//! registration and branch, with no error. Now create takes the lock first and
//! refuses a directory that holds a store or a store's data files;
//! open_or_create opens a store, refuses a damaged one as open does, and
//! creates only where a new store can sit beside what's there; and opening a
//! directory with no MANIFEST says whether it's a damaged store or no store at
//! all.

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

fn invalid_format(result: chronicle::Result<Store>) -> String {
    match result {
        Err(StoreError::InvalidFormat(msg)) => msg,
        Err(e) => panic!("expected InvalidFormat, got {e}"),
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
    for file in ["records.log", "state.bin", "state-indexes.bin", "branches.bin"] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("damaged");
        fs::create_dir(&path).unwrap();
        fs::write(path.join(file), b"x").unwrap();

        let msg = invalid_operation(Store::create(config(&path)));
        assert!(
            msg.contains(file) && msg.contains("a damaged Chronicle store") && msg.contains(&*path.to_string_lossy()),
            "{file}: {msg}"
        );
        // open_or_create says what open says, whichever its flag.
        for create_if_missing in [true, false] {
            let msg = invalid_format(Store::open_or_create(StoreConfig { create_if_missing, ..config(&path) }));
            assert!(msg.contains(file) && msg.contains("a damaged Chronicle store"), "{file}: {msg}");
        }
        assert_eq!(fs::read(path.join(file)).unwrap(), b"x", "{file} is untouched");
        assert!(!path.join("MANIFEST").exists(), "{file}: no MANIFEST was written");
    }
}

#[test]
fn a_store_that_lost_only_its_manifest_is_damaged_from_every_entry_point() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    populated(&path);
    fs::remove_file(path.join("MANIFEST")).unwrap();
    let records = fs::read(path.join("records.log")).unwrap();

    for create_if_missing in [true, false] {
        let msg = invalid_format(Store::open_or_create(StoreConfig { create_if_missing, ..config(&path) }));
        assert!(msg.contains("damaged") && msg.contains(&*path.to_string_lossy()), "{msg}");
    }
    assert!(invalid_format(Store::open(config(&path))).contains("damaged"));
    assert!(invalid_operation(Store::create(config(&path))).contains("damaged"));
    assert!(!path.join("MANIFEST").exists(), "no MANIFEST was written");
    assert_eq!(fs::read(path.join("records.log")).unwrap(), records);
}

#[test]
fn a_damaged_store_beside_other_files_is_still_called_damaged() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("damaged");
    fs::create_dir(&path).unwrap();
    fs::write(path.join("records.log"), b"x").unwrap();
    fs::write(path.join("notes.txt"), b"someone else's").unwrap();

    for create_if_missing in [true, false] {
        let msg = invalid_format(Store::open_or_create(StoreConfig { create_if_missing, ..config(&path) }));
        assert!(msg.contains("records.log") && msg.contains("a damaged Chronicle store"), "{msg}");
    }
}

#[test]
fn blobs_with_data_in_them_and_no_manifest_are_a_damaged_store() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("damaged");
    fs::create_dir_all(path.join("blobs/ab")).unwrap();
    fs::write(path.join("blobs/ab/cdef"), b"data").unwrap();

    assert!(invalid_operation(Store::create(config(&path))).contains("blobs"));
    for create_if_missing in [true, false] {
        let msg = invalid_format(Store::open_or_create(StoreConfig { create_if_missing, ..config(&path) }));
        assert!(msg.contains("blobs") && msg.contains("a damaged Chronicle store"), "{msg}");
    }
    assert_eq!(fs::read(path.join("blobs/ab/cdef")).unwrap(), b"data");
    assert!(!path.join("MANIFEST").exists(), "no MANIFEST was written");
}

#[test]
fn create_refuses_a_directory_holding_only_a_manifest() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    populated(&path);
    let manifest = fs::read(path.join("MANIFEST")).unwrap();
    let only = dir.path().join("only");
    fs::create_dir(&only).unwrap();
    fs::write(only.join("MANIFEST"), &manifest).unwrap();

    assert!(invalid_operation(Store::create(config(&only))).contains("already holds"));
    assert!(!only.join("records.log").exists(), "nothing was built beside it");
}

/// The refusal mustn't send anyone to entry points that fail on the same file.
#[test]
fn create_over_a_manifest_that_does_not_verify_says_so() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    drop(Store::create(config(&path)).unwrap());
    fs::write(path.join("MANIFEST"), b"CH").unwrap();

    let msg = invalid_operation(Store::create(config(&path)));
    assert!(msg.contains("MANIFEST") && msg.contains("doesn't verify"), "{msg}");
    assert!(!msg.contains("open it"), "{msg}");
    assert_eq!(fs::read(path.join("MANIFEST")).unwrap(), b"CH", "left as it was");
}

/// Runs on every platform, Windows's mandatory locks among them. A rewrite of
/// MANIFEST would put back the same bytes, so its modification time is what
/// catches one: backdated, and read back since a filesystem may round it.
#[test]
fn create_aimed_at_an_open_store_is_locked_and_leaves_its_manifest() {
    use std::time::{Duration, SystemTime};

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let held = Store::create(config(&path)).unwrap();
    let manifest = path.join("MANIFEST");
    let before = fs::read(&manifest).unwrap();
    let a_day_ago = SystemTime::now() - Duration::from_secs(86_400);
    fs::File::options().write(true).open(&manifest).unwrap().set_modified(a_day_ago).unwrap();
    let stored = fs::metadata(&manifest).unwrap().modified().unwrap();

    let result = Store::create(config(&path));
    assert!(matches!(result, Err(StoreError::Locked)), "got {:?}", result.err().map(|e| e.to_string()));
    assert_eq!(fs::read(&manifest).unwrap(), before);
    assert_eq!(fs::metadata(&manifest).unwrap().modified().unwrap(), stored, "MANIFEST was rewritten");
    drop(held);
}

/// The same with MANIFEST read-only, which a create that writes before it takes
/// the lock would meet at once on some writers. A second handle in this process
/// meets the lock as another process's would. The modification time is what
/// catches a rewrite here too, since today's writer renames over MANIFEST, which
/// its mode doesn't stop. Unix only: it sets file modes.
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
    // Read back: a filesystem may round what it was asked to store.
    let stored = fs::metadata(&manifest).unwrap().modified().unwrap();
    fs::set_permissions(&manifest, fs::Permissions::from_mode(0o444)).unwrap();
    let result = Store::create(config(&path));
    fs::set_permissions(&manifest, fs::Permissions::from_mode(0o644)).unwrap();

    assert!(matches!(result, Err(StoreError::Locked)), "got {:?}", result.err().map(|e| e.to_string()));
    assert_eq!(fs::read(&manifest).unwrap(), before);
    assert_eq!(fs::metadata(&manifest).unwrap().modified().unwrap(), stored, "MANIFEST was rewritten");
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
fn open_or_create_without_create_if_missing_refuses_other_files_as_not_a_store() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("project");
    fs::create_dir(&path).unwrap();
    fs::write(path.join("Cargo.toml"), b"[package]").unwrap();

    let msg = invalid_format(Store::open_or_create(StoreConfig { create_if_missing: false, ..config(&path) }));
    assert!(msg.contains("Cargo.toml"), "{msg}");
}

#[test]
fn open_or_create_still_creates_a_directory_that_is_missing() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("a/new/store");

    drop(Store::open_or_create(config(&path)).unwrap());
    Store::open(config(&path)).unwrap();
}

/// Allowed names count only as the kind of entry a new store can sit beside.
#[test]
fn open_or_create_refuses_an_allowed_name_of_the_wrong_kind() {
    let dir = TempDir::new().unwrap();
    let file_store = dir.path().join("file-entry");
    fs::create_dir(&file_store).unwrap();
    fs::write(file_store.join("blobs"), b"a file").unwrap();
    let file_found = dir.path().join("file-found");
    fs::create_dir(&file_found).unwrap();
    fs::write(file_found.join("lost+found"), b"a file").unwrap();
    let dir_ds_store = dir.path().join("dir-ds-store");
    fs::create_dir_all(dir_ds_store.join(".DS_Store")).unwrap();

    let dir_staging = dir.path().join("dir-staging");
    fs::create_dir_all(dir_staging.join(".chronicle-checkpoint-AbC123")).unwrap();
    fs::write(dir_staging.join("LOCK"), b"").unwrap();

    for (path, name) in [(&file_store, "blobs"), (&file_found, "lost+found"), (&dir_ds_store, ".DS_Store")] {
        let msg = invalid_format(Store::open_or_create(config(path)));
        assert!(msg.contains(name), "{msg}");
        assert!(!path.join("MANIFEST").exists() && !path.join("LOCK").exists(), "{name}: nothing was written");
    }
    // A directory named like a staging file, beside the LOCK a stopped create
    // would leave: the sweep wouldn't take it, so it isn't one.
    let msg = invalid_format(Store::open_or_create(config(&dir_staging)));
    assert!(msg.contains(".chronicle-checkpoint-AbC123"), "{msg}");
    assert!(!dir_staging.join("MANIFEST").exists(), "nothing was written");
}

/// A file named like a staging file is one only where a create could have left
/// it: a create takes LOCK before it stages anything.
#[test]
fn open_or_create_leaves_a_staging_lookalike_with_no_lock_beside_it() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("notes");
    fs::create_dir(&path).unwrap();
    let notes = path.join(".chronicle-checkpoint-notes");
    fs::write(&notes, b"mine").unwrap();

    let msg = invalid_format(Store::open_or_create(config(&path)));
    assert!(msg.contains(".chronicle-checkpoint-notes"), "{msg}");
    assert_eq!(fs::read(&notes).unwrap(), b"mine");
    assert!(!path.join("MANIFEST").exists() && !path.join("LOCK").exists(), "nothing was written");
}

/// A directory it can't list isn't an empty one: it may hold anything. Unix
/// only: it sets directory modes. As root the listing succeeds and finds the
/// file, so the check is what both ways share: a refusal, and nothing written.
#[cfg(unix)]
#[test]
fn open_or_create_refuses_a_directory_it_cannot_list_and_writes_nothing() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("write-only");
    fs::create_dir(&path).unwrap();
    fs::write(path.join("Cargo.toml"), b"[package]").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o333)).unwrap();
    let result = Store::open_or_create(config(&path));
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();

    match result {
        Err(e) => assert!(e.to_string().contains(&*path.to_string_lossy()), "{e}"),
        Ok(_) => panic!("created a store in a directory it can't list"),
    }
    assert!(!path.join("MANIFEST").exists() && !path.join("LOCK").exists(), "nothing was written");
}

/// A MANIFEST link with nothing behind it isn't an absent MANIFEST: a create
/// would write through it, into wherever it leads. Unix only: it makes a link.
#[cfg(unix)]
#[test]
fn open_or_create_refuses_a_dangling_manifest_link_and_writes_nothing() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    fs::create_dir(&path).unwrap();
    let elsewhere = dir.path().join("elsewhere");
    std::os::unix::fs::symlink(&elsewhere, path.join("MANIFEST")).unwrap();

    let msg = invalid_format(Store::open_or_create(config(&path)));
    assert!(msg.contains("nothing behind it"), "{msg}");
    assert!(!elsewhere.exists() && !path.join("LOCK").exists(), "nothing was written");
    let msg = invalid_operation(Store::create(config(&path)));
    assert!(msg.contains("MANIFEST") && msg.contains("nothing behind it"), "{msg}");
    assert!(!elsewhere.exists(), "create didn't write through it either");
}

/// The same for a data file's link, and create names it.
#[cfg(unix)]
#[test]
fn create_refuses_a_dangling_data_file_link_and_writes_nothing_through_it() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    fs::create_dir(&path).unwrap();
    let elsewhere = dir.path().join("elsewhere");
    std::os::unix::fs::symlink(&elsewhere, path.join("state.bin")).unwrap();

    assert!(invalid_operation(Store::create(config(&path))).contains("state.bin"));
    assert!(!elsewhere.exists());
}

/// A `blobs/` that can't be listed may hold anything, as a directory that can't
/// be listed may. Unix only: it sets modes. As root the listing succeeds and
/// finds the data, so the check is what both share: a refusal.
#[cfg(unix)]
#[test]
fn a_blobs_directory_that_cannot_be_listed_is_refused() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    fs::create_dir_all(path.join("blobs/ab")).unwrap();
    fs::write(path.join("blobs/ab/cdef"), b"data").unwrap();
    fs::set_permissions(path.join("blobs"), fs::Permissions::from_mode(0o333)).unwrap();
    let opened = Store::open_or_create(config(&path)).err().map(|e| e.to_string());
    let created = Store::create(config(&path)).err().map(|e| e.to_string());
    fs::set_permissions(path.join("blobs"), fs::Permissions::from_mode(0o755)).unwrap();

    let opened = opened.expect("open_or_create refused");
    let created = created.expect("create refused");
    assert!(opened.contains("blobs") && created.contains("blobs"), "{opened} / {created}");
    assert!(!path.join("MANIFEST").exists(), "no MANIFEST was written");
}

/// What the volume or Finder leaves in an otherwise empty `blobs/` isn't data.
#[test]
fn a_stopped_create_whose_blobs_holds_only_a_volumes_or_finders_entry_is_finished() {
    let dir = TempDir::new().unwrap();
    let finder = dir.path().join("finder");
    fs::create_dir_all(finder.join("blobs")).unwrap();
    fs::write(finder.join("blobs/.DS_Store"), b"").unwrap();
    let volume = dir.path().join("volume");
    fs::create_dir_all(volume.join("blobs/lost+found")).unwrap();

    for path in [&finder, &volume] {
        drop(Store::open_or_create(config(path)).unwrap());
        Store::open(config(path)).unwrap();
    }
    drop(Store::create(config(&dir.path().join("fresh"))).unwrap());
}

/// A `blobs` link says nothing about this store: it may lead to a shared
/// directory. `create` takes it, as it always has. Unix only: it makes a link.
#[cfg(unix)]
#[test]
fn create_takes_a_blobs_link_to_a_directory_with_data_in_it() {
    let dir = TempDir::new().unwrap();
    let share = dir.path().join("share");
    fs::create_dir_all(share.join("ab")).unwrap();
    fs::write(share.join("ab/cdef"), b"data").unwrap();
    let path = dir.path().join("store");
    fs::create_dir(&path).unwrap();
    std::os::unix::fs::symlink(&share, path.join("blobs")).unwrap();

    drop(Store::create(config(&path)).unwrap());
    assert_eq!(fs::read(share.join("ab/cdef")).unwrap(), b"data");
}

/// A store it can't look inside isn't an uninitialized one. Unix only: it sets
/// modes. As root the look succeeds and the store opens.
#[cfg(unix)]
#[test]
fn open_or_create_without_create_if_missing_does_not_call_an_unsearchable_store_uninitialized() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    populated(&path);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o666)).unwrap();
    let result = Store::open_or_create(StoreConfig { create_if_missing: false, ..config(&path) });
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();

    assert!(!matches!(result, Err(StoreError::NotInitialized)));
}

/// A file named like a staging file is one only where a create could have left
/// it, for create as for open_or_create. This holds for that call: once the
/// store exists it has a LOCK, and each open sweeps by the store's own prefix.
#[test]
fn create_leaves_a_staging_lookalike_with_no_lock_beside_it() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("notes");
    fs::create_dir(&path).unwrap();
    let notes = path.join(".chronicle-checkpoint-notes");
    fs::write(&notes, b"mine").unwrap();

    drop(Store::create(config(&path)).unwrap());
    assert_eq!(fs::read(&notes).unwrap(), b"mine");
}

/// `open` on a path that isn't there says so, as an IO error, rather than calling
/// it a directory that isn't a store.
#[test]
fn open_on_a_missing_path_says_it_does_not_exist() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("typo/store");

    match Store::open(config(&path)) {
        Err(StoreError::Io(e)) => {
            assert_eq!(e.kind(), std::io::ErrorKind::NotFound);
            assert!(e.to_string().contains(&*path.to_string_lossy()), "{e}");
        }
        Err(e) => panic!("expected an IO NotFound, got {e}"),
        Ok(_) => panic!("opened a path that isn't there"),
    }
}

/// `open` says what `open_or_create` says of a `blobs/` it can't list. Unix only:
/// it sets modes. As root the listing succeeds, so the refusal is damaged instead.
#[cfg(unix)]
#[test]
fn open_reports_a_look_that_failed_rather_than_no_store() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    fs::create_dir_all(path.join("blobs/ab")).unwrap();
    fs::set_permissions(path.join("blobs"), fs::Permissions::from_mode(0o333)).unwrap();
    let listable = fs::read_dir(path.join("blobs")).is_ok();
    let opened = Store::open(config(&path)).err().map(|e| e.to_string());
    fs::set_permissions(path.join("blobs"), fs::Permissions::from_mode(0o755)).unwrap();

    let opened = opened.expect("open refused");
    assert!(opened.contains("blobs") && !opened.contains("isn't a Chronicle store"), "{opened}");
    assert!(listable || opened.contains("looking in"), "the failed look, not a verdict: {opened}");
}

/// open_or_create doesn't take a `blobs` link, wherever it leads: a store would
/// write into what it can't vouch for. `create`, asked for by name, does (above).
/// Unix only: it makes links.
#[cfg(unix)]
#[test]
fn open_or_create_refuses_a_blobs_link_and_writes_nothing_through_it() {
    use std::os::unix::fs::symlink;

    let dir = TempDir::new().unwrap();
    let share = dir.path().join("share");
    fs::create_dir_all(&share).unwrap();
    fs::write(share.join("notes.txt"), b"mine").unwrap();
    let file = dir.path().join("a-file");
    fs::write(&file, b"x").unwrap();

    for (name, target) in [("to-a-directory", share.clone()), ("to-a-file", file), ("dangling", dir.path().join("gone"))] {
        let path = dir.path().join(name);
        fs::create_dir(&path).unwrap();
        symlink(&target, path.join("blobs")).unwrap();
        let msg = invalid_format(Store::open_or_create(config(&path)));
        assert!(msg.contains("holds blobs"), "{name}: {msg}");
        assert!(!path.join("MANIFEST").exists() && !path.join("LOCK").exists(), "{name}: nothing was written");
    }
    assert_eq!(fs::read_dir(&share).unwrap().count(), 1, "the link's target is as it was");
}

/// A failure reading MANIFEST names where. Unix only: the path is a file, whose
/// `MANIFEST` fails as not a directory.
#[cfg(unix)]
#[test]
fn open_names_the_path_when_reading_its_manifest_fails() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("a-file");
    fs::write(&path, b"x").unwrap();

    match Store::open(config(&path)) {
        Err(StoreError::Io(e)) => assert!(e.to_string().contains(&*path.to_string_lossy()), "{e}"),
        Err(e) => panic!("expected an IO error, got {e}"),
        Ok(_) => panic!("opened a file"),
    }
}

/// `LOCK` is the lock's to judge, and it says what to do about one that isn't a
/// regular file.
#[test]
fn open_or_create_leaves_a_lock_of_the_wrong_kind_to_the_lock() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    fs::create_dir_all(path.join("LOCK")).unwrap();

    let msg = invalid_format(Store::open_or_create(config(&path)));
    assert!(msg.contains("takes its lock only on a regular LOCK file"), "{msg}");
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
    // Before any open, which sweeps too.
    assert!(!staged.exists(), "the interrupted save's staging file is swept");
    Store::open(config(&path)).unwrap();
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
