//! Integration tests for tree state strategy through the Store API.
//!
//! These tests exercise the full codepath: tree_set/remove/batch → chain storage →
//! auto-snapshotting → reconstruction → tree_get/list/diff, including persistence
//! across store reopen and branch isolation.

use chronicle::{
    StateRegistration, StateStrategy, Store, StoreConfig, TreeEntry, TreeOp,
};
use tempfile::TempDir;

fn test_store(dir: &TempDir) -> Store {
    Store::create(StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: true,
    })
    .unwrap()
}

fn open_store(dir: &TempDir) -> Store {
    Store::open(StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: false,
    })
    .unwrap()
}

fn entry(hash: &str, size: u64) -> TreeEntry {
    TreeEntry {
        blob_hash: hash.to_string(),
        size,
        mode: 0o644,
    }
}

fn register_tree(store: &Store, id: &str) {
    store
        .register_state(StateRegistration {
            id: id.to_string(),
            strategy: StateStrategy::Tree {
                delta_snapshot_every: 5,
                full_snapshot_every: 3,
            },
            initial_value: None,
        })
        .unwrap();
}

// --- Basic CRUD ---

#[test]
fn test_tree_set_and_get() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);
    register_tree(&store, "files");

    store.tree_set("files", "src/main.rs", &entry("abc123", 100)).unwrap();
    store.tree_set("files", "README.md", &entry("def456", 50)).unwrap();

    let e = store.tree_get("files", "src/main.rs").unwrap().unwrap();
    assert_eq!(e.blob_hash, "abc123");
    assert_eq!(e.size, 100);

    let e = store.tree_get("files", "README.md").unwrap().unwrap();
    assert_eq!(e.blob_hash, "def456");

    assert!(store.tree_get("files", "nonexistent").unwrap().is_none());
}

#[test]
fn test_tree_remove() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);
    register_tree(&store, "files");

    store.tree_set("files", "a.txt", &entry("aaa", 10)).unwrap();
    store.tree_set("files", "b.txt", &entry("bbb", 20)).unwrap();
    store.tree_remove("files", "a.txt").unwrap();

    assert!(store.tree_get("files", "a.txt").unwrap().is_none());
    assert!(store.tree_get("files", "b.txt").unwrap().is_some());
}

#[test]
fn test_tree_batch() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);
    register_tree(&store, "files");

    store.tree_set("files", "old.txt", &entry("old", 5)).unwrap();

    store
        .tree_batch(
            "files",
            &[
                TreeOp::Set {
                    path: "new1.txt".to_string(),
                    entry: entry("n1", 10),
                },
                TreeOp::Set {
                    path: "new2.txt".to_string(),
                    entry: entry("n2", 20),
                },
                TreeOp::Remove {
                    path: "old.txt".to_string(),
                },
            ],
        )
        .unwrap();

    assert!(store.tree_get("files", "old.txt").unwrap().is_none());
    assert!(store.tree_get("files", "new1.txt").unwrap().is_some());
    assert!(store.tree_get("files", "new2.txt").unwrap().is_some());
}

// --- List with prefix ---

#[test]
fn test_tree_list_and_prefix() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);
    register_tree(&store, "files");

    store.tree_set("files", "src/main.rs", &entry("a", 1)).unwrap();
    store.tree_set("files", "src/lib.rs", &entry("b", 2)).unwrap();
    store.tree_set("files", "tests/test.rs", &entry("c", 3)).unwrap();
    store.tree_set("files", "README.md", &entry("d", 4)).unwrap();

    // List all
    let all = store.tree_list("files", None).unwrap();
    assert_eq!(all.len(), 4);

    // List with prefix
    let src = store.tree_list("files", Some("src/")).unwrap();
    assert_eq!(src.len(), 2);
    assert!(src.iter().all(|(k, _)| k.starts_with("src/")));

    let tests = store.tree_list("files", Some("tests/")).unwrap();
    assert_eq!(tests.len(), 1);

    // Prefix with no matches
    let none = store.tree_list("files", Some("nope/")).unwrap();
    assert!(none.is_empty());
}

// --- Diff ---

#[test]
fn test_tree_diff() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);
    register_tree(&store, "files");

    store.tree_set("files", "a.txt", &entry("aaa", 10)).unwrap();
    let seq1 = store.current_branch().head;

    store.tree_set("files", "b.txt", &entry("bbb", 20)).unwrap();
    store.tree_set("files", "a.txt", &entry("aaa_v2", 15)).unwrap();
    store.tree_remove("files", "a.txt").unwrap();
    let seq2 = store.current_branch().head;

    let changes = store.tree_diff("files", seq1, seq2).unwrap();
    assert_eq!(changes.len(), 2); // a.txt removed, b.txt added

    // Verify change types
    let added: Vec<_> = changes
        .iter()
        .filter(|c| matches!(c, chronicle::TreeChange::Added { .. }))
        .collect();
    let removed: Vec<_> = changes
        .iter()
        .filter(|c| matches!(c, chronicle::TreeChange::Removed { .. }))
        .collect();
    assert_eq!(added.len(), 1);
    assert_eq!(removed.len(), 1);
}

// --- Auto-snapshotting ---

#[test]
fn test_tree_auto_snapshot_and_correctness() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    // delta_snapshot_every=5, full_snapshot_every=3
    // So after 5 ops → delta snapshot, after 15 ops (3 deltas) → full snapshot
    register_tree(&store, "files");

    // Write 20 files to cross multiple snapshot thresholds
    for i in 0..20 {
        store
            .tree_set("files", &format!("file_{:03}.txt", i), &entry(&format!("hash_{}", i), i as u64 * 10))
            .unwrap();
    }

    // Verify all files are present after snapshotting
    let all = store.tree_list("files", None).unwrap();
    assert_eq!(all.len(), 20);

    for i in 0..20 {
        let e = store.tree_get("files", &format!("file_{:03}.txt", i)).unwrap().unwrap();
        assert_eq!(e.blob_hash, format!("hash_{}", i));
    }
}

// --- Persistence across reopen ---

#[test]
fn test_tree_persistence_across_reopen() {
    let dir = TempDir::new().unwrap();

    // Write some data
    {
        let store = test_store(&dir);
        register_tree(&store, "workspace");

        store.tree_set("workspace", "src/main.rs", &entry("main_hash", 500)).unwrap();
        store.tree_set("workspace", "src/lib.rs", &entry("lib_hash", 300)).unwrap();
        store.tree_set("workspace", "Cargo.toml", &entry("cargo_hash", 100)).unwrap();

        store.sync().unwrap();
    }

    // Reopen and verify — state is already registered from reconstruction
    {
        let store = open_store(&dir);

        let all = store.tree_list("workspace", None).unwrap();
        assert_eq!(all.len(), 3);

        let e = store.tree_get("workspace", "src/main.rs").unwrap().unwrap();
        assert_eq!(e.blob_hash, "main_hash");
        assert_eq!(e.size, 500);
    }
}

// --- Branch isolation ---

#[test]
fn test_tree_branch_isolation() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);
    register_tree(&store, "files");

    // Write on main branch
    store.tree_set("files", "shared.txt", &entry("main_version", 10)).unwrap();

    // Create branch and modify
    store.create_branch("feature", Some("main")).unwrap();
    store.switch_branch("feature").unwrap();

    store.tree_set("files", "shared.txt", &entry("feature_version", 20)).unwrap();
    store.tree_set("files", "feature_only.txt", &entry("feat", 30)).unwrap();

    // Verify feature branch state
    let e = store.tree_get("files", "shared.txt").unwrap().unwrap();
    assert_eq!(e.blob_hash, "feature_version");
    assert!(store.tree_get("files", "feature_only.txt").unwrap().is_some());

    // Switch back to main — should see original state
    store.switch_branch("main").unwrap();

    let e = store.tree_get("files", "shared.txt").unwrap().unwrap();
    assert_eq!(e.blob_hash, "main_version");
    assert!(store.tree_get("files", "feature_only.txt").unwrap().is_none());
}

// --- Strategy validation ---

#[test]
fn test_tree_ops_rejected_on_non_tree_state() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    // Register an append_log state
    store
        .register_state(StateRegistration {
            id: "messages".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 10,
                full_snapshot_every: 5,
            },
            initial_value: None,
        })
        .unwrap();

    // Tree operations should fail on append_log state
    let result = store.tree_set("messages", "file.txt", &entry("hash", 10));
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("Tree"), "Error should mention Tree strategy: {}", err);
}

// --- Diff after modifications ---

#[test]
fn test_tree_diff_modifications() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);
    register_tree(&store, "files");

    store.tree_set("files", "a.txt", &entry("v1", 10)).unwrap();
    store.tree_set("files", "b.txt", &entry("v1", 20)).unwrap();
    let seq1 = store.current_branch().head;

    // Modify a, leave b unchanged
    store.tree_set("files", "a.txt", &entry("v2", 15)).unwrap();
    let seq2 = store.current_branch().head;

    let changes = store.tree_diff("files", seq1, seq2).unwrap();
    assert_eq!(changes.len(), 1);

    match &changes[0] {
        chronicle::TreeChange::Modified { path, old, new } => {
            assert_eq!(path, "a.txt");
            assert_eq!(old.blob_hash, "v1");
            assert_eq!(new.blob_hash, "v2");
        }
        other => panic!("Expected Modified, got {:?}", other),
    }
}
