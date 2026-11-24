use chronicle::{RecordInput, StateOperation, StateRegistration, StateStrategy, Store, StoreConfig, StoreError};
use std::fs;
use std::path::Path;
use tempfile::TempDir;

fn config(path: &Path) -> StoreConfig {
    StoreConfig {
        path: path.to_owned(),
        ..Default::default()
    }
}

fn register(store: &Store) {
    store
        .register_state(StateRegistration {
            id: "messages".into(),
            strategy: StateStrategy::Snapshot,
            initial_value: None,
        })
        .unwrap();
}

fn closed_store(path: &Path, fill: impl FnOnce(&Store)) {
    let store = Store::create(config(path)).unwrap();
    fill(&store);
    store.sync().unwrap();
    drop(store);
}

fn with_state_history(store: &Store) {
    register(store);
    for n in 0..3u8 {
        store.update_state("messages", StateOperation::Set(vec![b'0' + n])).unwrap();
    }
}

fn corruption(result: chronicle::Result<Store>) -> String {
    match result {
        Err(StoreError::Corruption(message)) => message,
        Err(error) => panic!("expected Corruption, got {error:?}"),
        Ok(_) => panic!("the open succeeded"),
    }
}

fn assert_names_the_missing_log(message: &str, store: &Path, used: &str) {
    assert!(message.contains(&*store.to_string_lossy()), "{message}");
    assert!(message.contains("records.log is missing"), "{message}");
    assert!(message.contains(&format!("holds {used},")), "{message}");
    assert!(message.contains("created no records.log"), "{message}");
    assert!(message.contains("Restore the records.log"), "{message}");
    assert!(!message.contains("torn tail"), "{message}");
}

#[test]
fn a_used_store_without_its_log_is_refused_and_no_log_is_created() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    closed_store(&path, with_state_history);
    fs::remove_file(path.join("records.log")).unwrap();
    fs::remove_dir_all(path.join("blobs")).unwrap();

    for _ in 0..2 {
        assert_names_the_missing_log(&corruption(Store::open(config(&path))), &path, "state.bin and branches.bin");
        assert!(fs::symlink_metadata(path.join("records.log")).is_err(), "open created a records.log");
    }
    assert_names_the_missing_log(&corruption(Store::open_or_create(config(&path))), &path, "state.bin and branches.bin");
    assert!(fs::symlink_metadata(path.join("records.log")).is_err(), "open_or_create created a records.log");
    assert!(fs::symlink_metadata(path.join("blobs")).is_err(), "a refused open created blobs/");
}

#[test]
fn a_store_with_records_and_no_registered_state_is_refused_without_its_log() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    closed_store(&path, |store| {
        for n in 0..3u8 {
            store.append(RecordInput::raw("message", vec![b'0' + n])).unwrap();
        }
    });
    fs::remove_file(path.join("records.log")).unwrap();

    assert_names_the_missing_log(&corruption(Store::open(config(&path))), &path, "state.bin and branches.bin");
    assert!(fs::symlink_metadata(path.join("records.log")).is_err());
}

#[test]
fn a_store_with_a_state_and_a_branch_but_no_records_is_refused_without_its_log() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    closed_store(&path, |store| {
        register(store);
        store.create_branch("feature", None).unwrap();
    });
    fs::remove_file(path.join("records.log")).unwrap();

    assert_names_the_missing_log(&corruption(Store::open(config(&path))), &path, "state.bin and branches.bin");
    assert!(fs::symlink_metadata(path.join("records.log")).is_err());
}

#[test]
fn state_bin_or_branches_bin_alone_shows_a_used_store_without_its_log() {
    for (kept, gone) in [("state.bin", "branches.bin"), ("branches.bin", "state.bin")] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("store");
        closed_store(&path, with_state_history);
        fs::remove_file(path.join("records.log")).unwrap();
        fs::remove_file(path.join(gone)).unwrap();

        assert_names_the_missing_log(&corruption(Store::open(config(&path))), &path, kept);
        assert!(fs::symlink_metadata(path.join("records.log")).is_err(), "{kept}");
    }
}

fn only_the_manifest(path: &Path) {
    closed_store(path, |_| {});
    for entry in fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name() != "MANIFEST" {
            let kept = entry.path();
            if kept.is_dir() {
                fs::remove_dir_all(kept).unwrap();
            } else {
                fs::remove_file(kept).unwrap();
            }
        }
    }
}

#[test]
fn a_directory_with_only_a_manifest_opens_and_gets_its_log() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    only_the_manifest(&path);

    let store = Store::open_or_create(config(&path)).unwrap();
    store.append(RecordInput::raw("message", b"first".to_vec())).unwrap();
    drop(store);
    assert!(fs::metadata(path.join("records.log")).unwrap().len() > 0);
    Store::open(config(&path)).unwrap();
}

#[cfg(unix)]
#[test]
fn a_log_that_links_to_nothing_in_a_used_store_is_refused_and_nothing_is_written_through_it() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let volume = dir.path().join("unmounted");
    fs::create_dir(&volume).unwrap();
    closed_store(&path, with_state_history);
    let link = path.join("records.log");
    fs::remove_file(&link).unwrap();
    std::os::unix::fs::symlink(volume.join("records.log"), &link).unwrap();

    let message = corruption(Store::open(config(&path)));
    assert!(message.contains(&*link.to_string_lossy()), "{message}");
    assert!(message.contains("leads to nothing"), "{message}");
    assert!(message.contains("holds state.bin and branches.bin,"), "{message}");
    assert!(message.contains("Make the link lead to the store's records.log"), "{message}");
    assert!(!message.contains("remove the link"), "{message}");
    assert!(fs::symlink_metadata(&link).unwrap().file_type().is_symlink());
    assert_eq!(fs::read_dir(&volume).unwrap().count(), 0, "the open wrote through the link");
}

#[cfg(unix)]
#[test]
fn a_log_that_links_to_nothing_beside_only_a_manifest_is_refused_and_nothing_is_written_through_it() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let volume = dir.path().join("unmounted");
    fs::create_dir(&volume).unwrap();
    only_the_manifest(&path);
    let link = path.join("records.log");
    std::os::unix::fs::symlink(volume.join("records.log"), &link).unwrap();

    for open in [Store::open, Store::open_or_create] {
        let message = corruption(open(config(&path)));
        assert!(message.contains(&*link.to_string_lossy()), "{message}");
        assert!(message.contains("leads to nothing"), "{message}");
        assert!(!message.contains("state.bin") && !message.contains("branches.bin"), "{message}");
        assert!(message.contains("remove the link, and the next open creates one here"), "{message}");
        assert_eq!(fs::read_dir(&volume).unwrap().count(), 0, "the open wrote through the link");
    }
}

#[cfg(unix)]
#[test]
fn a_state_or_branch_file_whose_link_cannot_be_followed_is_refused_and_names_the_link() {
    for file in ["state.bin", "branches.bin"] {
        for leads_to_itself in [false, true] {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("store");
            let volume = dir.path().join("unmounted");
            fs::create_dir(&volume).unwrap();
            closed_store(&path, with_state_history);
            let link = path.join(file);
            fs::remove_file(&link).unwrap();
            if leads_to_itself {
                std::os::unix::fs::symlink(file, &link).unwrap();
            } else {
                std::os::unix::fs::symlink(volume.join(file), &link).unwrap();
            }

            let message = corruption(Store::open(config(&path)));
            assert!(message.contains(&*link.to_string_lossy()), "{file}: {message}");
            let says = if leads_to_itself { "whose target the open can't reach" } else { "leads to nothing" };
            assert!(message.contains(says), "{file}: {message}");
            assert!(message.contains(&format!("Make the link lead to the store's {file}")), "{file}: {message}");
            assert_eq!(fs::read_dir(&volume).unwrap().count(), 0, "{file}: the open wrote through the link");
        }
    }
}

#[test]
fn an_empty_log_under_a_state_head_is_named_empty_and_nothing_is_saved_or_created() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    closed_store(&path, with_state_history);
    fs::write(path.join("records.log"), b"").unwrap();
    fs::remove_dir_all(path.join("blobs")).unwrap();
    let branches = fs::read(path.join("branches.bin")).unwrap();
    let state = fs::read(path.join("state.bin")).unwrap();

    assert_names_the_empty_log(&corruption(Store::open(config(&path))), &path);
    assert_eq!(fs::read(path.join("branches.bin")).unwrap(), branches, "the refused open changed branches.bin");
    assert_eq!(fs::read(path.join("state.bin")).unwrap(), state, "the refused open changed state.bin");
    assert!(fs::symlink_metadata(path.join("blobs")).is_err(), "the refused open created blobs/");
}

fn assert_names_the_empty_log(message: &str, store: &Path) {
    assert!(message.contains(&*store.join("records.log").to_string_lossy()), "{message}");
    let heads = format!("is empty, though {} holds heads", store.join("state.bin").to_string_lossy());
    assert!(message.contains(&heads), "{message}");
    assert!(
        message.contains("An open by an earlier release of Chronicle creates such an empty file"),
        "{message}"
    );
    assert!(message.contains("finds the log's first record incomplete"), "{message}");
    assert!(message.contains("Restore the store's records.log"), "{message}");
    assert!(!message.contains("torn tail"), "{message}");
}

#[test]
fn a_store_whose_own_recovery_emptied_its_log_opens_again() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    closed_store(&path, |store| {
        store.append(RecordInput::raw("message", b"only".to_vec())).unwrap();
        store.create_branch("feature", None).unwrap();
    });
    let log = path.join("records.log");
    fs::OpenOptions::new().write(true).open(&log).unwrap().set_len(5).unwrap();

    drop(Store::open(config(&path)).unwrap());
    assert_eq!(fs::metadata(&log).unwrap().len(), 0);
    drop(Store::open(config(&path)).unwrap());
}

#[test]
fn a_log_whose_only_record_is_torn_keeps_the_torn_tail_words() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    closed_store(&path, |store| {
        register(store);
        store.update_state("messages", StateOperation::Set(b"only".to_vec())).unwrap();
    });
    let log = path.join("records.log");
    let file = fs::OpenOptions::new().write(true).open(&log).unwrap();
    file.set_len(5).unwrap();
    drop(file);

    let message = corruption(Store::open(config(&path)));
    assert!(
        message.contains("state metadata is newer than the durable log (torn tail or corrupt state.bin)"),
        "{message}"
    );
    assert_eq!(fs::metadata(&log).unwrap().len(), 0);

    assert_names_the_empty_log(&corruption(Store::open(config(&path))), &path);
}
