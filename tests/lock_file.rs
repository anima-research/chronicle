//! The store's LOCK file: open and create lock it without truncating it,
//! create a missing one, refuse one that isn't a regular file, and still
//! refuse a second holder of the lock.

use chronicle::{Store, StoreConfig, StoreError};
use std::fs;
use tempfile::TempDir;

// A store at `dir/store`, created when it's missing.
fn config(dir: &TempDir) -> StoreConfig {
    StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: true,
    }
}

#[test]
fn opening_a_store_leaves_its_lock_files_bytes_alone() {
    let dir = TempDir::new().unwrap();
    drop(Store::create(config(&dir)).unwrap());
    let lock = dir.path().join("store").join("LOCK");
    fs::write(&lock, b"left by a tool").unwrap();

    let store = Store::open(config(&dir)).unwrap();
    assert!(matches!(Store::open(config(&dir)), Err(StoreError::Locked)));
    // Read once the store is closed: on Windows the lock is mandatory.
    drop(store);

    assert_eq!(fs::read(&lock).unwrap(), b"left by a tool");
}

#[test]
fn creating_a_store_leaves_an_existing_lock_files_bytes_alone() {
    let dir = TempDir::new().unwrap();
    let lock = dir.path().join("store").join("LOCK");
    fs::create_dir_all(lock.parent().unwrap()).unwrap();
    fs::write(&lock, b"left by a tool").unwrap();

    drop(Store::create(config(&dir)).unwrap());

    assert_eq!(fs::read(&lock).unwrap(), b"left by a tool");
}

#[test]
fn a_missing_lock_file_is_created_and_locked() {
    let dir = TempDir::new().unwrap();
    drop(Store::create(config(&dir)).unwrap());
    let lock = dir.path().join("store").join("LOCK");
    fs::remove_file(&lock).unwrap();

    let store = Store::open(config(&dir)).unwrap();

    assert!(fs::symlink_metadata(&lock).unwrap().file_type().is_file());
    assert!(matches!(Store::open(config(&dir)), Err(StoreError::Locked)));
    drop(store);
}

#[test]
fn a_lock_that_is_a_directory_is_refused() {
    let dir = TempDir::new().unwrap();
    drop(Store::create(config(&dir)).unwrap());
    let lock = dir.path().join("store").join("LOCK");
    fs::remove_file(&lock).unwrap();
    fs::create_dir(&lock).unwrap();

    match Store::open(config(&dir)) {
        Err(StoreError::InvalidFormat(message)) => {
            assert!(message.contains("is a directory"), "{message}")
        }
        other => panic!("{:?}", other.map(|_| "a store")),
    }
    assert!(matches!(
        Store::create(config(&dir)),
        Err(StoreError::InvalidFormat(_))
    ));
}

#[cfg(unix)]
#[test]
fn a_lock_that_is_a_symlink_is_refused_and_its_target_left_alone() {
    let dir = TempDir::new().unwrap();
    drop(Store::create(config(&dir)).unwrap());
    let lock = dir.path().join("store").join("LOCK");
    let target = dir.path().join("elsewhere");
    fs::write(&target, b"fourteen bytes").unwrap();
    fs::remove_file(&lock).unwrap();
    std::os::unix::fs::symlink(&target, &lock).unwrap();

    match Store::open(config(&dir)) {
        Err(StoreError::InvalidFormat(message)) => {
            assert!(message.contains("is a symbolic link"), "{message}")
        }
        other => panic!("{:?}", other.map(|_| "a store")),
    }
    assert!(matches!(
        Store::create(config(&dir)),
        Err(StoreError::InvalidFormat(_))
    ));

    assert_eq!(fs::read(&target).unwrap(), b"fourteen bytes");
    assert!(fs::symlink_metadata(&lock)
        .unwrap()
        .file_type()
        .is_symlink());
}

#[cfg(unix)]
#[test]
fn a_lock_that_is_a_dangling_symlink_is_refused_and_creates_nothing() {
    let dir = TempDir::new().unwrap();
    drop(Store::create(config(&dir)).unwrap());
    let lock = dir.path().join("store").join("LOCK");
    // The target's directory exists, so following the link would create it.
    let target = dir.path().join("elsewhere");
    fs::remove_file(&lock).unwrap();
    std::os::unix::fs::symlink(&target, &lock).unwrap();

    let opened = Store::open(config(&dir));
    let created = Store::create(config(&dir));

    assert!(!target.exists(), "nothing created at the target");
    assert!(matches!(opened, Err(StoreError::InvalidFormat(_))));
    assert!(matches!(created, Err(StoreError::InvalidFormat(_))));
}

#[cfg(unix)]
#[test]
fn a_lock_that_is_a_fifo_is_refused_without_hanging() {
    let dir = TempDir::new().unwrap();
    drop(Store::create(config(&dir)).unwrap());
    let lock = dir.path().join("store").join("LOCK");
    fs::remove_file(&lock).unwrap();
    let c_path = std::ffi::CString::new(lock.to_str().unwrap()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);

    // Opening a FIFO for writing blocks until a reader comes, so open and
    // create each run on a thread, and a refusal has to arrive within the wait.
    for (route, run) in [
        (
            "open",
            Store::open as fn(StoreConfig) -> chronicle::Result<Store>,
        ),
        ("create", Store::create),
    ] {
        let (done, result) = std::sync::mpsc::channel();
        let store_config = config(&dir);
        std::thread::spawn(move || {
            let _ = done.send(run(store_config).map(|_| ()));
        });
        match result.recv_timeout(std::time::Duration::from_secs(10)) {
            Ok(Err(StoreError::InvalidFormat(message))) => {
                assert!(message.contains("not a regular file"), "{route}: {message}")
            }
            Ok(other) => panic!("{route}: {other:?}"),
            Err(_) => panic!("{route} of a FIFO LOCK didn't return within 10 s"),
        }
    }
}
