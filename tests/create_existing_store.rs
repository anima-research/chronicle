//! `Store::create` on a directory that already holds a store (#34). It used to
//! truncate and rewrite MANIFEST before taking the lock, then build empty
//! state and branch managers whose first sync published over the store's own,
//! so a create where an open was meant kept the records and lost every
//! registration and branch, with no error. Now create takes the lock first and
//! refuses a directory that holds a store or a store's data files; and
//! opening a directory with no MANIFEST says whether it's a damaged store or
//! no store at all.

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
        assert_eq!(fs::read(path.join(file)).unwrap(), b"x", "{file} is untouched");
        assert!(!path.join("MANIFEST").exists(), "{file}: no MANIFEST was written");
    }
}

#[test]
fn open_or_create_refuses_a_damaged_store_whatever_create_if_missing_says() {
    for file in ["records.log", "state.bin", "branches.bin"] {
        for create_if_missing in [true, false] {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("damaged");
            fs::create_dir(&path).unwrap();
            fs::write(path.join(file), b"x").unwrap();

            match Store::open_or_create(StoreConfig { create_if_missing, ..config(&path) }) {
                Err(StoreError::InvalidFormat(msg)) => assert!(
                    msg.contains(file) && msg.contains("damaged") && msg.contains(&*path.to_string_lossy()),
                    "{file}, create_if_missing {create_if_missing}: {msg}"
                ),
                Err(e) => panic!("{file}, create_if_missing {create_if_missing}: expected InvalidFormat, got {e}"),
                Ok(_) => panic!("{file}, create_if_missing {create_if_missing}: took a damaged store"),
            }
            let mut left: Vec<_> = fs::read_dir(&path).unwrap().map(|e| e.unwrap().file_name()).collect();
            left.sort();
            assert_eq!(left, [file], "{file}, create_if_missing {create_if_missing}: nothing was written there");
            assert_eq!(fs::read(path.join(file)).unwrap(), b"x");
        }
    }
}

/// A create aimed at a store another process has open writes nothing there.
/// A read-only MANIFEST makes a write fail loudly for an ordinary user, and a
/// manifest whose time is set far in the past catches a rewrite of identical
/// bytes where the mode doesn't stop it (as root). The time is a whole
/// second, so a filesystem that keeps coarser times (HFS+ and FAT keep 1 s
/// and 2 s) holds it as set. Unix only: it sets file modes.
#[cfg(unix)]
#[test]
fn create_aimed_at_an_open_store_writes_nothing_there() {
    use std::os::unix::fs::PermissionsExt;
    use std::time::{Duration, UNIX_EPOCH};

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let held = Store::create(config(&path)).unwrap();

    let manifest = path.join("MANIFEST");
    let before = fs::read(&manifest).unwrap();
    let long_ago = UNIX_EPOCH + Duration::from_secs(1_000_000_000);
    fs::File::options().write(true).open(&manifest).unwrap().set_modified(long_ago).unwrap();
    fs::set_permissions(&manifest, fs::Permissions::from_mode(0o444)).unwrap();
    let result = Store::create(config(&path));
    fs::set_permissions(&manifest, fs::Permissions::from_mode(0o644)).unwrap();

    assert!(matches!(result, Err(StoreError::Locked)), "got {:?}", result.err().map(|e| e.to_string()));
    assert_eq!(fs::read(&manifest).unwrap(), before);
    assert_eq!(fs::metadata(&manifest).unwrap().modified().unwrap(), long_ago, "MANIFEST was rewritten");
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
    // What a new volume's root holds before anyone writes there: mke2fs
    // leaves lost+found on ext4, macOS adds .fseventsd, .Spotlight-V100,
    // .Trashes and .TemporaryItems to a volume it mounts, and Windows adds
    // System Volume Information and $RECYCLE.BIN.
    for entry in [
        "lost+found",
        ".fseventsd",
        ".Spotlight-V100",
        ".Trashes",
        ".TemporaryItems",
        "System Volume Information",
        "$RECYCLE.BIN",
    ] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("volume");
        fs::create_dir_all(path.join(entry)).unwrap();

        drop(Store::open_or_create(config(&path)).unwrap_or_else(|error| panic!("{entry}: {error}")));
        assert!(path.join("MANIFEST").exists(), "{entry}");
        assert!(path.join(entry).is_dir(), "{entry}: what was there stays");
    }
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
    assert!(!staged.exists(), "the create swept the interrupted save's staging file");
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

        for create_if_missing in [true, false] {
            match Store::open_or_create(StoreConfig { create_if_missing, ..config(&path) }) {
                Err(StoreError::InvalidFormat(msg)) => assert!(
                    msg.contains(file) && msg.contains(if file == "MANIFEST" { "leads to nothing" } else { "damaged" }),
                    "{file}, {create_if_missing}: {msg}"
                ),
                Err(e) => panic!("{file}, {create_if_missing}: expected InvalidFormat, got {e}"),
                Ok(_) => panic!("{file}, {create_if_missing}: took a directory holding a link to a missing {file}"),
            }
        }
        let left: Vec<_> = fs::read_dir(&path).unwrap().map(|e| e.unwrap().file_name()).collect();
        assert_eq!(left, [file], "{file}: open_or_create wrote nothing there");

        let msg = invalid_operation(Store::create(config(&path)));
        assert!(msg.contains(&*path.to_string_lossy()), "{file}: {msg}");
        assert!(
            file != "MANIFEST"
                || (msg.contains("leads to nothing")
                    && !msg.contains("already holds")
                    && msg.contains("Store::open_or_create")
                    && msg.contains("openOrCreate")),
            "{file}: {msg}"
        );
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

#[cfg(unix)]
#[test]
fn open_or_create_on_a_path_that_is_a_file_names_it() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("not-a-directory");
    fs::write(&path, b"someone's file").unwrap();

    for create_if_missing in [true, false] {
        match Store::open_or_create(StoreConfig { create_if_missing, ..config(&path) }) {
            Err(StoreError::Io(error)) => assert!(
                error.kind() == std::io::ErrorKind::NotADirectory
                    && error.to_string().contains(&*path.to_string_lossy())
                    && error.to_string().contains("MANIFEST"),
                "{create_if_missing}: {error}"
            ),
            Err(e) => panic!("{create_if_missing}: expected an I/O error naming the path, got {e}"),
            Ok(_) => panic!("{create_if_missing}: opened a store at a file"),
        }
    }
    assert_eq!(fs::read(&path).unwrap(), b"someone's file");
}

/// A directory this process may write and search but not list: the store
/// files' names can be looked up, and come back absent, but whether it holds
/// anything else can't be told, so open_or_create neither creates a store
/// there nor says there is none. Unix only: it sets a directory's mode.
#[cfg(unix)]
#[test]
fn open_or_create_on_a_directory_it_cant_list_returns_the_listing_error() {
    use std::os::unix::fs::PermissionsExt;

    for create_if_missing in [true, false] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("project");
        fs::create_dir(&path).unwrap();
        fs::write(path.join("notes.txt"), b"someone's notes").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o300)).unwrap();
        let listable = fs::read_dir(&path).is_ok();
        let result = Store::open_or_create(StoreConfig { create_if_missing, ..config(&path) })
            .map(drop);
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        if listable {
            // Running as root, where the mode doesn't stop a listing.
            eprintln!("skipped: the directory could be listed despite its mode (running as root?)");
            return;
        }

        match result {
            Err(StoreError::Io(error)) => assert!(
                error.kind() == std::io::ErrorKind::PermissionDenied
                    && error.to_string().contains(&*path.to_string_lossy()),
                "{create_if_missing}: {error}"
            ),
            Err(e) => panic!("{create_if_missing}: expected the listing's I/O error, got {e}"),
            Ok(()) => panic!("{create_if_missing}: created a store in a directory it can't list"),
        }
        let left: Vec<_> = fs::read_dir(&path).unwrap().map(|e| e.unwrap().file_name()).collect();
        assert_eq!(left, ["notes.txt"], "{create_if_missing}: nothing was written there");
    }
}

#[test]
fn create_on_a_manifest_that_isnt_a_stores_says_what_is_wrong_and_keeps_it() {
    for (name, bytes) in [
        ("empty", &b""[..]),
        ("foreign", &b"hello world"[..]),
        ("cut short", &b"RST"[..]),
        ("unknown version", &b"RST\0\x09"[..]),
        ("features cut short", &b"RST\0\x02\x01"[..]),
        ("unknown features", &b"RST\0\x02\0\0\0\0\0\0\0\x80"[..]),
    ] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("store");
        fs::create_dir(&path).unwrap();
        let manifest = path.join("MANIFEST");
        fs::write(&manifest, bytes).unwrap();

        let msg = invalid_operation(Store::create(config(&path)));
        assert!(
            msg.contains(&*manifest.to_string_lossy()) && msg.contains("won't write over it"),
            "{name}: {msg}"
        );
        assert!(
            !msg.contains("already holds") && !msg.contains("open_or_create") && !msg.contains("openOrCreate"),
            "{name}: it doesn't send the caller to calls that fail on this MANIFEST: {msg}"
        );
        // Only a file too short to hold a header is one to remove: another
        // file's bytes may be someone's.
        assert_eq!(
            msg.contains("remove the MANIFEST"),
            bytes.len() < 5,
            "{name}: {msg}"
        );
        assert_eq!(fs::read(&manifest).unwrap(), bytes, "{name}: MANIFEST is untouched");
    }
}

/// A MANIFEST that isn't a regular file is refused without being opened:
/// opening a FIFO for reading waits for a writer.
#[cfg(unix)]
#[test]
fn create_on_a_manifest_that_isnt_a_regular_file_refuses_without_hanging() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    fs::create_dir(&path).unwrap();
    let manifest = path.join("MANIFEST");
    let c_path = std::ffi::CString::new(manifest.to_str().unwrap()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);

    let (done, result) = std::sync::mpsc::channel();
    let store_config = config(&path);
    std::thread::spawn(move || {
        let _ = done.send(Store::create(store_config).map(drop));
    });
    match result.recv_timeout(std::time::Duration::from_secs(10)) {
        Ok(Err(StoreError::InvalidOperation(msg))) => assert!(
            msg.contains(&*manifest.to_string_lossy())
                && msg.contains("isn't a regular file")
                && msg.contains("won't write over it"),
            "{msg}"
        ),
        Ok(other) => panic!("{other:?}"),
        Err(_) => panic!("create on a FIFO MANIFEST didn't return within 10 s"),
    }
}

/// `open` and `open_or_create` refuse a MANIFEST that isn't a regular file
/// without opening it, as create does: opening a FIFO for reading waits for a
/// writer, and 0.6.0's open waited there.
#[cfg(unix)]
#[test]
fn open_on_a_manifest_that_isnt_a_regular_file_refuses_without_hanging() {
    for call in ["open", "open_or_create"] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("store");
        fs::create_dir(&path).unwrap();
        let manifest = path.join("MANIFEST");
        let c_path = std::ffi::CString::new(manifest.to_str().unwrap()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);

        let (done, result) = std::sync::mpsc::channel();
        let store_config = config(&path);
        let which = call.to_string();
        std::thread::spawn(move || {
            let opened = if which == "open" { Store::open(store_config) } else { Store::open_or_create(store_config) };
            let _ = done.send(opened.map(drop));
        });
        match result.recv_timeout(std::time::Duration::from_secs(10)) {
            Ok(Err(StoreError::InvalidFormat(msg))) => assert!(
                msg.contains(&*manifest.to_string_lossy()) && msg.contains("isn't a regular file"),
                "{call}: {msg}"
            ),
            Ok(other) => panic!("{call}: {other:?}"),
            Err(_) => panic!("{call} on a FIFO MANIFEST didn't return within 10 s"),
        }
        assert!(!path.join("records.log").exists(), "{call} wrote nothing");
        std::mem::forget(dir); // a thread that hung on main may still hold it
    }
}

/// A directory named MANIFEST is refused the same way, on every platform.
#[test]
fn open_on_a_manifest_that_is_a_directory_says_so() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    fs::create_dir_all(path.join("MANIFEST")).unwrap();
    for (call, opened) in [("open", Store::open(config(&path))), ("open_or_create", Store::open_or_create(config(&path)))] {
        match opened {
            Err(StoreError::InvalidFormat(msg)) => assert!(msg.contains("isn't a regular file"), "{call}: {msg}"),
            Err(other) => panic!("{call}: expected InvalidFormat, got {other:?}"),
            Ok(_) => panic!("{call} opened a store on a MANIFEST directory"),
        }
    }
    assert!(path.join("MANIFEST").is_dir());
}

#[test]
fn create_names_open_or_create_in_both_apis() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    populated(&path);

    let msg = invalid_operation(Store::create(config(&path)));
    assert!(
        msg.contains("Store::open_or_create") && msg.contains("openOrCreate"),
        "{msg}"
    );
}

/// A MANIFEST that ends before its header does (empty, as an interrupted
/// create from a release before 0.6 could leave it, or cut short) is
/// refused by open and open_or_create as a format error naming it, and
/// create's refusal says how to start over.
#[test]
fn a_manifest_cut_short_of_its_header_is_named_and_create_says_how_to_start_over() {
    for bytes in [&b""[..], b"RST", b"RST\0"] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("store");
        fs::create_dir(&path).unwrap();
        let manifest = path.join("MANIFEST");
        fs::write(&manifest, bytes).unwrap();

        for (call, result) in [("open", Store::open(config(&path))), ("open_or_create", Store::open_or_create(config(&path)))] {
            match result {
                Err(StoreError::InvalidFormat(msg)) => {
                    assert!(msg.contains(&*manifest.to_string_lossy()), "{bytes:?}, {call}: {msg}")
                }
                Err(e) => panic!("{bytes:?}, {call}: expected InvalidFormat naming the MANIFEST, got {e}"),
                Ok(_) => panic!("{bytes:?}, {call}: opened a store with no MANIFEST header"),
            }
        }
        let msg = invalid_operation(Store::create(config(&path)));
        assert!(
            msg.contains("ends before a MANIFEST's header does")
                && msg.contains("remove the MANIFEST and create again")
                && msg.contains("records.log"),
            "{bytes:?}: {msg}"
        );
        assert_eq!(fs::read(&manifest).unwrap(), bytes, "{bytes:?}: MANIFEST is untouched");
    }
}
