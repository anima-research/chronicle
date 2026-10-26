use chronicle::{Hash, Store, StoreConfig, StoreError};
use same_file::Handle;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tempfile::TempDir;

const CONTENT: &[u8] = b"the bytes a workspace file holds";
const CONTENT_TYPE: &str = "text/plain";

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

fn files_under_blobs(store: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut directories = vec![store.join("blobs")];
    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(&directory).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                directories.push(entry.path());
            } else {
                files.push(entry.path());
            }
        }
    }
    files.sort();
    files
}

fn the_one_blob_file(store: &Path) -> PathBuf {
    let files = files_under_blobs(store);
    assert_eq!(files.len(), 1, "{files:?}");
    files.into_iter().next().unwrap()
}

fn cut_short(file: Vec<u8>) -> Vec<u8> {
    file[..file.len() / 2].to_vec()
}

fn last_content_byte_flipped(mut file: Vec<u8>) -> Vec<u8> {
    let checksum_starts = file.len() - 4;
    file[checksum_starts - 1] ^= 0xff;
    file
}

fn magic_replaced(mut file: Vec<u8>) -> Vec<u8> {
    file[..4].copy_from_slice(b"NOPE");
    file
}

fn content_length_past_the_end(mut file: Vec<u8>) -> Vec<u8> {
    let content_type_length = u16::from_le_bytes([file[5], file[6]]) as usize;
    let content_length_starts = 7 + content_type_length;
    file[content_length_starts..content_length_starts + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    file
}

#[test]
fn storing_the_content_again_replaces_a_copy_that_cannot_be_read_back() {
    let damages: [(&str, fn(Vec<u8>) -> Vec<u8>); 4] = [
        ("cut short", cut_short),
        ("a flipped content byte", last_content_byte_flipped),
        ("another magic", magic_replaced),
        ("a content length past the end", content_length_past_the_end),
    ];
    for (damage, damaged) in damages {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("store");
        let store = Store::create(config(&path)).unwrap();
        let hash = store.store_blob(CONTENT, CONTENT_TYPE).unwrap();
        drop(store);
        let file = the_one_blob_file(&path);
        fs::write(&file, damaged(fs::read(&file).unwrap())).unwrap();

        let store = reopen(&path);
        assert!(
            store.get_blob(&hash).is_err(),
            "{damage}: the damaged copy read back"
        );
        assert_eq!(
            store.store_blob(CONTENT, CONTENT_TYPE).unwrap(),
            hash,
            "{damage}"
        );
        drop(store);

        let blob = reopen(&path).get_blob(&hash).unwrap();
        assert_eq!(blob.unwrap().content, CONTENT, "{damage}");
    }
}

#[test]
fn storing_the_content_again_replaces_a_file_holding_another_blob() {
    let other = TempDir::new().unwrap();
    let other_path = other.path().join("store");
    let other_store = Store::create(config(&other_path)).unwrap();
    other_store
        .store_blob(b"some other bytes", CONTENT_TYPE)
        .unwrap();
    drop(other_store);
    let other_file = fs::read(the_one_blob_file(&other_path)).unwrap();

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let store = Store::create(config(&path)).unwrap();
    let hash = store.store_blob(CONTENT, CONTENT_TYPE).unwrap();
    drop(store);
    fs::write(the_one_blob_file(&path), other_file).unwrap();

    let store = reopen(&path);
    assert!(store.get_blob(&hash).is_err());
    store.store_blob(CONTENT, CONTENT_TYPE).unwrap();
    drop(store);

    let blob = reopen(&path).get_blob(&hash).unwrap();
    assert_eq!(blob.unwrap().content, CONTENT);
}

#[test]
fn storing_the_content_again_leaves_a_readable_copy_as_it_is() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let store = Store::create(config(&path)).unwrap();
    store.store_blob(CONTENT, CONTENT_TYPE).unwrap();
    drop(store);
    let file = the_one_blob_file(&path);
    let stored = Handle::from_path(&file).unwrap();
    let bytes = fs::read(&file).unwrap();

    let store = reopen(&path);
    store
        .store_blob(CONTENT, "application/octet-stream")
        .unwrap();

    assert_eq!(Handle::from_path(&file).unwrap(), stored);
    assert_eq!(fs::read(&file).unwrap(), bytes);
    assert_eq!(files_under_blobs(&path), vec![file]);
}

#[test]
fn a_store_puts_back_a_file_removed_while_its_content_was_cached() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let store = Store::create(config(&path)).unwrap();
    let hash = store.store_blob(CONTENT, CONTENT_TYPE).unwrap();
    fs::remove_file(the_one_blob_file(&path)).unwrap();

    store.store_blob(CONTENT, CONTENT_TYPE).unwrap();
    drop(store);

    let blob = reopen(&path).get_blob(&hash).unwrap();
    assert_eq!(blob.unwrap().content, CONTENT);
}

#[test]
fn a_content_type_too_long_to_record_is_refused_and_nothing_is_stored() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let store = Store::create(config(&path)).unwrap();
    let longest = "t".repeat(usize::from(u16::MAX));
    let too_long = format!("{longest}t");

    match store.store_blob(CONTENT, &too_long) {
        Err(StoreError::InvalidOperation(message)) => {
            assert!(message.contains("65535"), "{message}")
        }
        other => panic!("expected the content type to be refused, got {other:?}"),
    }
    assert!(store
        .get_blob(&Hash::from_bytes(CONTENT))
        .unwrap()
        .is_none());
    assert_eq!(files_under_blobs(&path), Vec::<PathBuf>::new());

    let hash = store.store_blob(CONTENT, &longest).unwrap();
    drop(store);
    let blob = reopen(&path).get_blob(&hash).unwrap().unwrap();
    assert_eq!(
        (blob.content.as_slice(), blob.content_type),
        (CONTENT, longest)
    );
}

#[cfg(unix)]
const SIZE_LIMIT: u64 = 4096;

#[cfg(unix)]
fn larger_than_the_limit() -> Vec<u8> {
    vec![7; 16 * SIZE_LIMIT as usize]
}

#[cfg(unix)]
fn store_past_the_size_limit_in_a_child(path: &Path, ignore_sigxfsz: bool) -> std::process::Output {
    std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "store_past_the_size_limit_child"])
        .env("CHRONICLE_BLOB_LIMIT_STORE", path)
        .env(
            "CHRONICLE_BLOB_LIMIT_IGNORE_SIGXFSZ",
            if ignore_sigxfsz { "1" } else { "0" },
        )
        .output()
        .unwrap()
}

#[cfg(unix)]
#[test]
fn a_store_killed_partway_leaves_the_blob_missing_and_the_next_open_clears_what_it_wrote() {
    use std::os::unix::process::ExitStatusExt;
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let store = Store::create(config(&path)).unwrap();
    let kept = store.store_blob(CONTENT, CONTENT_TYPE).unwrap();
    drop(store);
    let kept_file = the_one_blob_file(&path);

    let child = store_past_the_size_limit_in_a_child(&path, false);
    assert_eq!(
        child.status.signal(),
        Some(libc::SIGXFSZ),
        "child: {:?} {}{}",
        child.status,
        String::from_utf8_lossy(&child.stdout),
        String::from_utf8_lossy(&child.stderr)
    );

    let store = reopen(&path);
    assert_eq!(files_under_blobs(&path), vec![kept_file]);
    let lost = Hash::from_bytes(&larger_than_the_limit());
    assert!(store.get_blob(&lost).unwrap().is_none());
    assert_eq!(store.get_blob(&kept).unwrap().unwrap().content, CONTENT);
}

#[cfg(unix)]
#[test]
fn a_store_that_fails_partway_leaves_nothing_behind() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    drop(Store::create(config(&path)).unwrap());

    let child = store_past_the_size_limit_in_a_child(&path, true);
    assert_eq!(
        child.status.code(),
        Some(73),
        "child failed: {}{}",
        String::from_utf8_lossy(&child.stdout),
        String::from_utf8_lossy(&child.stderr)
    );

    assert_eq!(files_under_blobs(&path), Vec::<PathBuf>::new());
    let lost = Hash::from_bytes(&larger_than_the_limit());
    assert!(reopen(&path).get_blob(&lost).unwrap().is_none());
}

#[cfg(unix)]
#[test]
fn store_past_the_size_limit_child() {
    let Some(path) = std::env::var_os("CHRONICLE_BLOB_LIMIT_STORE") else {
        return;
    };
    let ignore_sigxfsz = std::env::var("CHRONICLE_BLOB_LIMIT_IGNORE_SIGXFSZ").unwrap() == "1";
    let store = Store::open(config(Path::new(&path))).unwrap();
    let limit = SIZE_LIMIT as libc::rlim_t;
    let rlimit = libc::rlimit {
        rlim_cur: limit,
        rlim_max: limit,
    };
    // SAFETY: plain libc calls with a valid, initialized rlimit.
    unsafe {
        if ignore_sigxfsz {
            libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
        }
        assert_eq!(libc::setrlimit(libc::RLIMIT_FSIZE, &rlimit), 0);
    }
    match store.store_blob(&larger_than_the_limit(), CONTENT_TYPE) {
        Err(StoreError::Io(error)) => {
            assert_eq!(error.raw_os_error(), Some(libc::EFBIG), "{error}")
        }
        other => panic!("expected the blob write to fail with EFBIG, got {other:?}"),
    }
    std::process::exit(73);
}
