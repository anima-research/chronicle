//! A blob's file is written under a staging name in `blobs/` and renamed into
//! its shard, so a crash leaves it whole or absent. These go through the
//! store: a torn file that an in-place write left is replaced by the next
//! store of its content after a reopen, opening the store removes the
//! staging files that interrupted stores left, but only with the lock held,
//! and a store whose rename fails removes its own. A content type too long
//! for its field is refused whatever the store holds.

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
fn a_torn_blob_is_replaced_by_storing_its_content_after_a_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let content: Vec<u8> = (0..50_000u32).map(|i| (i % 249) as u8).collect();
    let hash = {
        let store = Store::create(config(&path)).unwrap();
        store.store_blob(&content, "image/png").unwrap()
    };
    let file = blob_file(&path, &hash);
    let whole = fs::read(&file).unwrap();
    // What a crash in an in-place write leaves: the file cut short.
    fs::write(&file, &whole[..whole.len() / 3]).unwrap();

    let store = reopen(&path);
    assert!(store.blob_exists(&hash), "a file is there, whole or not");
    assert!(store.get_blob(&hash).is_err());
    assert_eq!(store.store_blob(&content, "image/png").unwrap(), hash);
    assert_eq!(fs::read(&file).unwrap(), whole);
    drop(store);

    let store = reopen(&path);
    let blob = store.get_blob(&hash).unwrap().unwrap();
    assert_eq!(blob.content, content);
    assert_eq!(blob.content_type, "image/png");
    let stats = store.stats().unwrap();
    assert_eq!(stats.blob_count, 1);
    assert_eq!(stats.blob_size_bytes, whole.len() as u64);
}

#[test]
fn open_removes_blob_staging_files_left_by_interrupted_stores() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let hash = {
        let store = Store::create(config(&path)).unwrap();
        store.store_blob(b"kept", "text/plain").unwrap()
    };
    // What a store killed before its rename leaves beside the shards.
    let stage = path.join("blobs").join(".chronicle-blob-a1B2c3");
    fs::write(&stage, b"unfinished blob").unwrap();
    {
        let store = reopen(&path);
        // Not counted while it is there.
        assert_eq!(store.stats().unwrap().blob_count, 1);
    }
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

// A store whose rename fails, here because a directory is in the way at the
// blob's path, returns the error, removes its staging file and leaves the
// directory. Once the path is clear, the next store writes the blob.
#[test]
fn a_store_whose_rename_fails_removes_its_staging_file() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let store = Store::create(config(&path)).unwrap();
    let hash = Hash::from_bytes(b"blocked");
    let file = blob_file(&path, &hash);
    fs::create_dir_all(file.join("in the way")).unwrap();

    let error = store.store_blob(b"blocked", "text/plain").unwrap_err();
    assert!(
        error
            .to_string()
            .contains("cannot rename the new blob file over"),
        "{error}"
    );
    assert_eq!(staged(&path), 0);
    assert!(file.join("in the way").is_dir());

    fs::remove_dir_all(&file).unwrap();
    assert_eq!(store.store_blob(b"blocked", "text/plain").unwrap(), hash);
    assert_eq!(staged(&path), 0);
    drop(store);
    let blob = reopen(&path).get_blob(&hash).unwrap().unwrap();
    assert_eq!(blob.content, b"blocked");
}
