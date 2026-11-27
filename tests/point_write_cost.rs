#![cfg(target_os = "linux")]

use chronicle::{StateOperation, StateRegistration, StateStrategy, Store, StoreConfig, TreeEntry};
use serde_json::json;
use std::fs;
use tempfile::TempDir;

fn create_store(dir: &TempDir) -> Store {
    Store::create(StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: true,
    })
    .unwrap()
}

fn bytes_written<T>(op: impl FnOnce() -> T) -> (T, u64) {
    let written = || -> u64 {
        let io = fs::read_to_string("/proc/thread-self/io").expect("/proc/thread-self/io");
        io.lines()
            .find_map(|line| line.strip_prefix("wchar:"))
            .expect("a wchar line")
            .trim()
            .parse()
            .expect("a byte count")
    };
    let before = written();
    let result = op();
    let after = written();
    (result, after - before)
}

#[test]
fn ordinary_point_replacements_and_evictions_stay_small_as_the_collection_grows() {
    const TARGET: &str = "agent-00000000";
    const SNAPSHOT_EVERY: u64 = 10_000;
    const TREE_BUDGET: u64 = 4 * 1024;
    let hash = |i: usize| chronicle::Hash::from_bytes(&i.to_le_bytes()).to_hex();
    let entry = |i| TreeEntry {
        blob_hash: hash(i),
        size: 1024,
        mode: 0o644,
    };

    for n in [32, 4096] {
        let dir = TempDir::new().unwrap();
        let store = create_store(&dir);
        store
            .register_state(StateRegistration {
                id: "tree".into(),
                strategy: StateStrategy::Tree {
                    delta_snapshot_every: SNAPSHOT_EVERY,
                    full_snapshot_every: 1,
                },
                initial_value: None,
            })
            .unwrap();
        for i in 0..n {
            store
                .tree_set("tree", &format!("agent-{i:08}"), &entry(i))
                .unwrap();
        }
        assert_eq!(store.tree_list("tree", None).unwrap().len(), n);
        assert_eq!(store.tree_get("tree", TARGET).unwrap(), Some(entry(0)));

        let replacement = entry(10_000);
        let (set, set_written) = bytes_written(|| store.tree_set("tree", TARGET, &replacement));
        set.unwrap();
        let after_set = store.stats().unwrap();
        assert!(set_written > 0 && set_written <= TREE_BUDGET, "set in {n}-key tree wrote {set_written} bytes");
        assert_eq!(after_set.state_slot_count, 1);
        assert_eq!(store.tree_get("tree", TARGET).unwrap(), Some(replacement));

        let (removed, remove_written) = bytes_written(|| store.tree_remove("tree", TARGET));
        removed.unwrap();
        let after_remove = store.stats().unwrap();
        assert!(remove_written > 0 && remove_written <= TREE_BUDGET, "remove in {n}-key tree wrote {remove_written} bytes");
        assert_eq!(after_remove.state_slot_count, 1);
        assert_eq!(store.tree_get("tree", TARGET).unwrap(), None);
        assert_eq!(store.tree_list("tree", None).unwrap().len(), n - 1);

        store
            .register_state(StateRegistration {
                id: "log".into(),
                strategy: StateStrategy::AppendLog {
                    delta_snapshot_every: SNAPSHOT_EVERY,
                    full_snapshot_every: 1,
                },
                initial_value: None,
            })
            .unwrap();
        for i in 0..n {
            let item = serde_json::to_vec(&json!({"body": hash(i)})).unwrap();
            store
                .update_state("log", StateOperation::Append(item))
                .unwrap();
        }
        assert_eq!(store.get_state_len("log").unwrap(), Some(n));
        let edited = serde_json::to_vec(&json!({"body": hash(10_000)})).unwrap();
        let (edit, edit_written) = bytes_written(|| {
            store.update_state(
                "log",
                StateOperation::Edit {
                    index: 0,
                    new_value: edited.clone(),
                },
            )
        });
        edit.unwrap();
        assert!(
            edit_written > 0 && edit_written <= 4 * edited.len() as u64 + 1024,
            "edit in {n}-item log wrote {edit_written} bytes"
        );
        assert_eq!(store.get_state_item("log", 0).unwrap(), Some(edited));
        assert_eq!(store.get_state_len("log").unwrap(), Some(n));
    }
}
