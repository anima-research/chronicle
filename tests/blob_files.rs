//! What a store's open does with the blob staging files that interrupted
//! stores left, and the content-type limit for content a store already holds,
//! through the store. tests/blob_storage.rs covers the blob write itself:
//! damaged copies, links, stores that stop partway and a failed rename.

use chronicle::{Hash, Store, StoreConfig, StoreError};
use std::fs;
use std::path::{Path, PathBuf};
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

/// Where a blob's file is: `blobs/<first byte in hex>/<hash in hex>`.
fn blob_file(store: &Path, hash: &Hash) -> PathBuf {
    let hex = hash.to_hex();
    store.join("blobs").join(&hex[..2]).join(hex)
}

/// How many staging files are in the store's `blobs/`.
fn staged(store: &Path) -> usize {
    fs::read_dir(store.join("blobs"))
        .unwrap()
        .filter(|entry| {
            let name = entry.as_ref().unwrap().file_name();
            name.to_string_lossy().starts_with(".chronicle-blob-")
        })
        .count()
}

#[test]
fn open_removes_blob_staging_files_left_by_interrupted_stores() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let stage = path.join("blobs").join(".chronicle-blob-a1B2c3");
    let hash = {
        let store = Store::create(config(&path)).unwrap();
        let hash = store.store_blob(b"kept", "text/plain").unwrap();
        let before = store.stats().unwrap();
        assert_eq!(before.blob_count, 1, "the stored blob is counted");
        assert!(before.blob_size_bytes >= b"kept".len() as u64, "and its bytes: {before:?}");
        fs::write(&stage, b"unfinished blob").unwrap();
        let during = store.stats().unwrap();
        assert_eq!(
            (during.blob_count, during.blob_size_bytes),
            (before.blob_count, before.blob_size_bytes),
            "not counted while it is there"
        );
        hash
    };
    assert!(stage.exists());
    drop(reopen(&path));
    assert!(!stage.exists());
    let store = reopen(&path);
    assert_eq!(store.get_blob(&hash).unwrap().unwrap().content, b"kept");
}

// An open that finds the store locked fails before it touches a staging
// file, which may be a store in progress.
#[test]
fn open_of_a_locked_store_leaves_its_blob_staging_files() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let _store = Store::create(config(&path)).unwrap();
    let stage = path.join("blobs").join(".chronicle-blob-a1B2c3");
    fs::write(&stage, b"a store in progress").unwrap();

    assert!(matches!(
        Store::open(config(&path)),
        Err(StoreError::Locked)
    ));
    assert!(stage.exists());
}

// A content type longer than 65,535 bytes is refused for content the store
// already holds, whether it has the blob cached or reads its file, as it is
// for new content, and the file stays as it was. A store of content already
// there used to return its hash whatever the content type.
#[test]
fn a_content_type_too_long_is_refused_whether_or_not_the_content_is_stored() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let too_long = "x".repeat(65_536);
    let hash = {
        let store = Store::create(config(&path)).unwrap();
        let hash = store.store_blob(b"stored", "text/plain").unwrap();
        for content in [&b"stored"[..], b"new"] {
            assert!(matches!(
                store.store_blob(content, &too_long),
                Err(StoreError::InvalidOperation(_))
            ));
        }
        hash
    };
    let file = blob_file(&path, &hash);
    let whole = fs::read(&file).unwrap();

    let store = reopen(&path);
    assert!(matches!(
        store.store_blob(b"stored", &too_long),
        Err(StoreError::InvalidOperation(_))
    ));
    assert_eq!(fs::read(&file).unwrap(), whole);
    assert_eq!(
        store.get_blob(&hash).unwrap().unwrap().content_type,
        "text/plain"
    );
    assert!(!store.blob_exists(&Hash::from_bytes(b"new")));
    assert_eq!(staged(&path), 0);
}
