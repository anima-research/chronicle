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

fn cut_inside_its_header(file: Vec<u8>) -> Vec<u8> {
    file[..6].to_vec()
}

fn version_zero(mut file: Vec<u8>) -> Vec<u8> {
    file[4] = 0;
    file
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

fn content_type_length_cut_to_16_bits(file: Vec<u8>) -> Vec<u8> {
    let content_type_length = u16::from_le_bytes([file[5], file[6]]) as usize;
    let mut cut = file[..5].to_vec();
    cut.extend_from_slice(&((u16::MAX as usize + 1) as u16).to_le_bytes());
    cut.extend(vec![b't'; u16::MAX as usize + 1]);
    cut.extend_from_slice(&file[7 + content_type_length..]);
    cut
}

#[test]
fn storing_the_content_again_replaces_a_copy_that_cannot_be_read_back() {
    let damages: [(&str, fn(Vec<u8>) -> Vec<u8>, bool); 7] = [
        ("cut short", cut_short, true),
        ("a format version of 0", version_zero, true),
        ("cut inside its header", cut_inside_its_header, true),
        ("a flipped content byte", last_content_byte_flipped, false),
        ("another magic", magic_replaced, false),
        (
            "a content length past the end",
            content_length_past_the_end,
            true,
        ),
        (
            "a content type of 65,536 bytes written with its length cut to 16 bits",
            content_type_length_cut_to_16_bits,
            true,
        ),
    ];
    for (damage, damaged, names_the_file) in damages {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("store");
        let store = Store::create(config(&path)).unwrap();
        let hash = store.store_blob(CONTENT, CONTENT_TYPE).unwrap();
        drop(store);
        let file = the_one_blob_file(&path);
        fs::write(&file, damaged(fs::read(&file).unwrap())).unwrap();

        let store = reopen(&path);
        match store.get_blob(&hash) {
            Err(StoreError::InvalidFormat(message)) if names_the_file => assert!(
                message.contains(&file.display().to_string()),
                "{damage}: {message}"
            ),
            Err(_) if !names_the_file => {}
            other => panic!("{damage}: expected an error naming the file, got {other:?}"),
        }
        assert_eq!(
            store.store_blob(CONTENT, CONTENT_TYPE).unwrap(),
            hash,
            "{damage}"
        );
        assert_eq!(files_under_blobs(&path), vec![file.clone()], "{damage}");
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
    assert_eq!(files_under_blobs(&path).len(), 1);
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
    let too_long_in_two_byte_characters = "é".repeat(usize::from(u16::MAX) / 2 + 1);
    let refused = |result: chronicle::Result<Hash>| match result {
        Err(StoreError::InvalidOperation(message)) => {
            assert!(message.contains("65535"), "{message}")
        }
        other => panic!("expected the content type to be refused, got {other:?}"),
    };

    refused(store.store_blob(CONTENT, &too_long_in_two_byte_characters));
    assert!(store
        .get_blob(&Hash::from_bytes(CONTENT))
        .unwrap()
        .is_none());
    assert_eq!(files_under_blobs(&path), Vec::<PathBuf>::new());

    let hash = store.store_blob(CONTENT, &longest).unwrap();
    refused(store.store_blob(CONTENT, &too_long_in_two_byte_characters));
    drop(store);
    let blob = reopen(&path).get_blob(&hash).unwrap().unwrap();
    assert_eq!(
        (blob.content.as_slice(), blob.content_type),
        (CONTENT, longest)
    );
}

#[test]
fn a_store_leaves_a_directory_at_the_blobs_name_and_says_so() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let store = Store::create(config(&path)).unwrap();
    let hash = Hash::from_bytes(CONTENT);
    let at_its_name = path
        .join("blobs")
        .join(hash.shard_prefix())
        .join(hash.to_hex());
    fs::create_dir_all(&at_its_name).unwrap();

    match store.store_blob(CONTENT, CONTENT_TYPE) {
        Err(StoreError::Io(error)) => assert!(
            error.to_string().contains("isn't a regular file"),
            "{error}"
        ),
        other => panic!("expected the store to refuse, got {other:?}"),
    }
    assert!(at_its_name.is_dir());
    assert!(store.get_blob(&hash).is_err());
}

#[test]
fn a_store_of_cached_bytes_refuses_a_directory_at_the_blobs_name() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let store = Store::create(config(&path)).unwrap();
    store.store_blob(CONTENT, CONTENT_TYPE).unwrap();
    let file = the_one_blob_file(&path);
    fs::remove_file(&file).unwrap();
    fs::create_dir(&file).unwrap();

    match store.store_blob(CONTENT, CONTENT_TYPE) {
        Err(StoreError::Io(error)) => assert!(
            error.to_string().contains("isn't a regular file"),
            "{error}"
        ),
        other => panic!("expected the store to refuse, got {other:?}"),
    }
    assert!(file.is_dir());
}

#[cfg(unix)]
#[test]
fn a_store_replaces_a_link_to_nothing_at_the_blobs_name() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let store = Store::create(config(&path)).unwrap();
    let hash = Hash::from_bytes(CONTENT);
    let shard = path.join("blobs").join(hash.shard_prefix());
    fs::create_dir_all(&shard).unwrap();
    let at_its_name = shard.join(hash.to_hex());
    std::os::unix::fs::symlink(dir.path().join("nothing here"), &at_its_name).unwrap();

    assert_eq!(store.store_blob(CONTENT, CONTENT_TYPE).unwrap(), hash);
    drop(store);

    assert!(fs::symlink_metadata(&at_its_name).unwrap().is_file());
    assert!(!dir.path().join("nothing here").exists());
    let blob = reopen(&path).get_blob(&hash).unwrap();
    assert_eq!(blob.unwrap().content, CONTENT);
}

#[cfg(unix)]
#[test]
fn a_store_reads_through_a_link_to_a_good_blob_file_and_leaves_it() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let store = Store::create(config(&path)).unwrap();
    let hash = store.store_blob(CONTENT, CONTENT_TYPE).unwrap();
    drop(store);
    let file = the_one_blob_file(&path);
    let elsewhere = dir.path().join("elsewhere");
    fs::rename(&file, &elsewhere).unwrap();
    std::os::unix::fs::symlink(&elsewhere, &file).unwrap();

    let target_bytes = fs::read(&elsewhere).unwrap();

    let store = reopen(&path);
    assert_eq!(store.store_blob(CONTENT, CONTENT_TYPE).unwrap(), hash);
    drop(store);

    assert!(fs::symlink_metadata(&file)
        .unwrap()
        .file_type()
        .is_symlink());
    assert_eq!(fs::read(&elsewhere).unwrap(), target_bytes);
    let blob = reopen(&path).get_blob(&hash).unwrap();
    assert_eq!(blob.unwrap().content, CONTENT);
}

#[cfg(unix)]
#[test]
fn a_store_replaces_a_link_to_a_damaged_file_and_leaves_its_target() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let store = Store::create(config(&path)).unwrap();
    let hash = store.store_blob(CONTENT, CONTENT_TYPE).unwrap();
    drop(store);
    let file = the_one_blob_file(&path);
    let elsewhere = dir.path().join("elsewhere");
    let damaged = cut_short(fs::read(&file).unwrap());
    fs::write(&elsewhere, &damaged).unwrap();
    fs::remove_file(&file).unwrap();
    std::os::unix::fs::symlink(&elsewhere, &file).unwrap();

    let store = reopen(&path);
    assert_eq!(store.store_blob(CONTENT, CONTENT_TYPE).unwrap(), hash);
    drop(store);

    assert!(fs::symlink_metadata(&file).unwrap().is_file());
    assert_eq!(fs::read(&elsewhere).unwrap(), damaged);
    let blob = reopen(&path).get_blob(&hash).unwrap();
    assert_eq!(blob.unwrap().content, CONTENT);
}

#[cfg(unix)]
#[test]
fn a_store_refuses_a_link_that_loops_and_leaves_it() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let store = Store::create(config(&path)).unwrap();
    let hash = Hash::from_bytes(CONTENT);
    let shard = path.join("blobs").join(hash.shard_prefix());
    fs::create_dir_all(&shard).unwrap();
    let at_its_name = shard.join(hash.to_hex());
    std::os::unix::fs::symlink(&at_its_name, &at_its_name).unwrap();

    assert!(matches!(
        store.store_blob(CONTENT, CONTENT_TYPE),
        Err(StoreError::Io(_))
    ));
    assert!(matches!(store.get_blob(&hash), Err(StoreError::Io(_))));
    assert_eq!(
        fs::read_link(&at_its_name).unwrap(),
        at_its_name,
        "the link is left as it was"
    );
}

#[test]
fn a_store_leaves_a_blob_file_in_a_newer_format_and_says_so() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let store = Store::create(config(&path)).unwrap();
    let hash = store.store_blob(CONTENT, CONTENT_TYPE).unwrap();
    drop(store);
    let file = the_one_blob_file(&path);
    let mut newer = fs::read(&file).unwrap();
    newer[4] = 3;
    fs::write(&file, &newer).unwrap();

    let store = reopen(&path);
    match store.store_blob(CONTENT, CONTENT_TYPE) {
        Err(StoreError::InvalidFormat(message)) => {
            assert!(message.contains("Unsupported blob version 3"), "{message}");
            assert!(message.contains(&*file.to_string_lossy()), "{message}");
        }
        other => panic!("expected the store to refuse, got {other:?}"),
    }
    assert!(matches!(
        store.get_blob(&hash),
        Err(StoreError::InvalidFormat(_))
    ));
    drop(store);
    assert_eq!(fs::read(&file).unwrap(), newer);
}

#[cfg(unix)]
#[test]
fn a_blob_directory_the_account_may_not_read_still_takes_stores() {
    use std::os::unix::fs::PermissionsExt;
    // SAFETY: geteuid has no preconditions.
    if unsafe { libc::geteuid() } == 0 {
        return;
    }
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    drop(Store::create(config(&path)).unwrap());
    let blobs = path.join("blobs");
    fs::set_permissions(&blobs, fs::Permissions::from_mode(0o300)).unwrap();

    let store = reopen(&path);
    let stored = store.store_blob(CONTENT, CONTENT_TYPE);
    drop(store);
    let read = reopen(&path).get_blob(&Hash::from_bytes(CONTENT));
    fs::set_permissions(&blobs, fs::Permissions::from_mode(0o755)).unwrap();

    let hash = stored.unwrap();
    assert_eq!(read.unwrap().unwrap().content, CONTENT);
    assert_eq!(hash, Hash::from_bytes(CONTENT));
}

#[cfg(unix)]
#[test]
fn a_store_returns_at_once_when_a_fifo_is_at_the_blobs_name() {
    use std::os::unix::ffi::OsStrExt;
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let store = Store::create(config(&path)).unwrap();
    let hash = Hash::from_bytes(CONTENT);
    let shard = path.join("blobs").join(hash.shard_prefix());
    fs::create_dir_all(&shard).unwrap();
    let at_its_name = shard.join(hash.to_hex());
    let fifo = std::ffi::CString::new(at_its_name.as_os_str().as_bytes()).unwrap();
    // SAFETY: a valid, NUL-terminated path.
    assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);

    match store.store_blob(CONTENT, CONTENT_TYPE) {
        Err(StoreError::Io(error)) => assert!(
            error.to_string().contains("isn't a regular file"),
            "{error}"
        ),
        other => panic!("expected the store to refuse, got {other:?}"),
    }
    use std::os::unix::fs::FileTypeExt;
    assert!(fs::symlink_metadata(&at_its_name)
        .unwrap()
        .file_type()
        .is_fifo());
}

#[cfg(unix)]
#[test]
fn a_store_leaves_a_blob_file_it_may_not_read_and_says_so() {
    use std::os::unix::fs::PermissionsExt;
    // SAFETY: geteuid has no preconditions.
    if unsafe { libc::geteuid() } == 0 {
        return;
    }
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let store = Store::create(config(&path)).unwrap();
    store.store_blob(CONTENT, CONTENT_TYPE).unwrap();
    drop(store);
    let file = the_one_blob_file(&path);
    let stored = Handle::from_path(&file).unwrap();
    let bytes = fs::read(&file).unwrap();
    fs::set_permissions(&file, fs::Permissions::from_mode(0o000)).unwrap();

    let refused = reopen(&path).store_blob(CONTENT, CONTENT_TYPE);
    fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap();

    match refused {
        Err(StoreError::Io(error)) => {
            assert_eq!(
                error.kind(),
                std::io::ErrorKind::PermissionDenied,
                "{error}"
            )
        }
        other => panic!("expected the store to refuse, got {other:?}"),
    }
    assert_eq!(Handle::from_path(&file).unwrap(), stored);
    assert_eq!(fs::read(&file).unwrap(), bytes);
}

#[cfg(unix)]
#[test]
fn a_store_whose_rename_fails_leaves_no_file_behind() {
    use std::os::unix::fs::PermissionsExt;
    // SAFETY: geteuid has no preconditions.
    if unsafe { libc::geteuid() } == 0 {
        return;
    }
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let store = Store::create(config(&path)).unwrap();
    let hash = Hash::from_bytes(CONTENT);
    let shard = path.join("blobs").join(hash.shard_prefix());
    fs::create_dir_all(&shard).unwrap();
    fs::set_permissions(&shard, fs::Permissions::from_mode(0o555)).unwrap();

    let stored = store.store_blob(CONTENT, CONTENT_TYPE);
    fs::set_permissions(&shard, fs::Permissions::from_mode(0o755)).unwrap();

    match stored {
        Err(StoreError::Io(error)) => assert!(
            error.to_string().contains("cannot rename the new blob"),
            "{error}"
        ),
        other => panic!("expected the rename to fail, got {other:?}"),
    }
    assert_eq!(files_under_blobs(&path), Vec::<PathBuf>::new());
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
fn a_store_killed_partway_leaves_the_blob_missing_and_the_next_open_clears_its_staging_file() {
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

    let lost = Hash::from_bytes(&larger_than_the_limit());
    let left = files_under_blobs(&path);
    // On macOS, SIGXFSZ can arrive after the failed write has returned, so the
    // store may already have removed its staging file when the child dies.
    let staging_left = if cfg!(target_os = "macos") {
        0..=1
    } else {
        1..=1
    };
    assert!(staging_left.contains(&(left.len() - 1)), "{left:?}");
    assert!(left.contains(&kept_file), "{left:?}");
    assert!(
        left.iter()
            .all(|file| file.file_name().unwrap().to_str() != Some(&*lost.to_hex())),
        "{left:?}"
    );

    let store = reopen(&path);
    assert_eq!(files_under_blobs(&path), vec![kept_file]);
    assert!(store.get_blob(&lost).unwrap().is_none());
    assert_eq!(store.get_blob(&kept).unwrap().unwrap().content, CONTENT);
}

#[cfg(unix)]
#[test]
fn a_store_that_fails_partway_leaves_no_file_behind() {
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
fn a_store_that_stops_partway_leaves_the_file_it_was_replacing() {
    use std::os::unix::process::ExitStatusExt;
    for killed in [false, true] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("store");
        drop(Store::create(config(&path)).unwrap());
        let lost = Hash::from_bytes(&larger_than_the_limit());
        let shard = path.join("blobs").join(lost.shard_prefix());
        fs::create_dir_all(&shard).unwrap();
        let damaged = shard.join(lost.to_hex());
        fs::write(&damaged, b"a damaged blob").unwrap();

        let child = store_past_the_size_limit_in_a_child(&path, !killed);
        let ended = if killed {
            child.status.signal() == Some(libc::SIGXFSZ)
        } else {
            child.status.code() == Some(73)
        };
        assert!(
            ended,
            "killed {killed}: child: {:?} {}{}",
            child.status,
            String::from_utf8_lossy(&child.stdout),
            String::from_utf8_lossy(&child.stderr)
        );

        assert_eq!(
            fs::read(&damaged).unwrap(),
            b"a damaged blob",
            "killed {killed}"
        );
        let before_the_sweep = files_under_blobs(&path);
        if killed {
            // On macOS, SIGXFSZ can arrive after the failed write has returned,
            // so the store may already have removed its staging file.
            let staging_left = if cfg!(target_os = "macos") {
                0..=1
            } else {
                1..=1
            };
            assert!(
                staging_left.contains(&(before_the_sweep.len() - 1)),
                "{before_the_sweep:?}"
            );
            assert!(before_the_sweep.contains(&damaged));
        } else {
            assert_eq!(before_the_sweep, vec![damaged.clone()]);
        }
        drop(reopen(&path));
        assert_eq!(
            files_under_blobs(&path),
            vec![damaged.clone()],
            "killed {killed}"
        );
        assert_eq!(
            fs::read(&damaged).unwrap(),
            b"a damaged blob",
            "killed {killed}"
        );
    }
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
