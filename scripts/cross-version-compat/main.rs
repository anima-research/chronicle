//! Cross-version store-compatibility harness (issue #15, acceptance test 1).
//!
//! Compiled twice by `scripts/cross-version-compat.sh`: once against
//! chronicle v0.2.6 and once against the working tree. The same store
//! directory is then passed through the phases:
//!
//!   create      (0.2.6)  — create store, register states, write past
//!                          delta + full snapshot boundaries
//!   extend      (tree)   — open, verify, retune cadence, write past at
//!                          least one more full boundary
//!   verify-old  (0.2.6)  — THE acceptance gate: the unmodified 0.2.6
//!                          build must open the store the new build wrote,
//!                          materialize every state, time-travel and
//!                          branch over the mixed-provenance chain, and
//!                          keep writing
//!   verify-new  (tree)   — reopen after the 0.2.6 rollback wrote more
//!
//! The `new-api` cargo feature gates calls that only exist on the working
//! tree (`update_state_strategy`); the script enables it for the
//! working-tree build only.

use chronicle::{
    Sequence, StateOperation, StateRegistration, StateStrategy, Store, StoreConfig, TreeEntry,
};
use serde_json::json;
use std::path::{Path, PathBuf};

/// Full snapshot every DELTA_EVERY * FULL_EVERY = 6 appends, so every
/// phase crosses several delta and full boundaries.
const DELTA_EVERY: u64 = 3;
const FULL_EVERY: u64 = 2;

fn open_store(dir: &Path, create: bool) -> Store {
    let config = StoreConfig {
        path: dir.to_path_buf(),
        blob_cache_size: 100,
        create_if_missing: create,
    };
    let store = if create {
        Store::create(config).expect("create store")
    } else {
        Store::open(config).expect("open store")
    };
    store
}

fn msg(i: usize) -> Vec<u8> {
    serde_json::to_vec(&json!({"i": i, "content": format!("message {i}")})).unwrap()
}

/// Append messages [from, to); returns the sequence of the last append.
fn append_messages(store: &Store, from: usize, to: usize) -> u64 {
    let mut last = 0;
    for i in from..to {
        let record = store
            .update_state("messages", StateOperation::Append(msg(i)))
            .expect("append message");
        last = record.sequence.0;
    }
    last
}

fn set_config(store: &Store, generation: u64) {
    let payload = serde_json::to_vec(&json!({"generation": generation})).unwrap();
    store
        .update_state("config", StateOperation::Set(payload))
        .expect("set config");
}

fn tree_put(store: &Store, path: &str, content: &str) {
    let hash = store
        .store_blob(content.as_bytes(), "text/plain")
        .expect("store blob");
    store
        .tree_set(
            "files",
            path,
            &TreeEntry {
                blob_hash: hash.to_hex(),
                size: content.len() as u64,
                mode: 0o644,
            },
        )
        .expect("tree set");
}

fn tree_file_content(i: usize) -> String {
    format!("contents of file {i}")
}

/// Materialize every state and check it against the expected shape.
fn verify(store: &Store, expected_msgs: usize, expected_tree: usize, expected_generation: u64) {
    let state = store
        .get_state("messages")
        .expect("get messages")
        .expect("messages exist");
    let items: Vec<serde_json::Value> = serde_json::from_slice(&state).expect("messages are JSON");
    assert_eq!(items.len(), expected_msgs, "messages length");
    for (i, item) in items.iter().enumerate() {
        assert_eq!(item["i"], i as u64, "message {i} identity");
    }
    assert_eq!(
        store.get_state_len("messages").expect("get_state_len"),
        Some(expected_msgs),
        "messages item count"
    );

    let config: serde_json::Value = serde_json::from_slice(
        &store
            .get_state("config")
            .expect("get config")
            .expect("config exists"),
    )
    .expect("config is JSON");
    assert_eq!(config["generation"], expected_generation, "config generation");

    let listing = store.tree_list("files", None).expect("tree list");
    assert_eq!(listing.len(), expected_tree, "tree entry count");
    for i in 0..expected_tree {
        let path = format!("dir/file-{i}.txt");
        let entry = store
            .tree_get("files", &path)
            .expect("tree get")
            .unwrap_or_else(|| panic!("tree entry {path} missing"));
        assert_eq!(
            entry.size,
            tree_file_content(i).len() as u64,
            "tree entry {path} size"
        );
    }
}

/// Messages state as of the phase-A checkpoint, via time travel.
fn verify_checkpoint(store: &Store, checkpoint_seq: u64, expected_msgs: usize) {
    let state = store
        .get_state_at("messages", Sequence(checkpoint_seq))
        .expect("get_state_at")
        .expect("messages exist at checkpoint");
    let items: Vec<serde_json::Value> = serde_json::from_slice(&state).unwrap();
    assert_eq!(items.len(), expected_msgs, "messages at checkpoint");
}

fn checkpoint_path(store_dir: &Path) -> PathBuf {
    store_dir.with_extension("checkpoint.json")
}

fn read_checkpoint(store_dir: &Path) -> u64 {
    let raw = std::fs::read(checkpoint_path(store_dir)).expect("read checkpoint");
    let v: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    v["seq"].as_u64().expect("checkpoint seq")
}

fn main() {
    let mut args = std::env::args().skip(1);
    let phase = args.next().expect("usage: <phase> <store-dir>");
    let store_dir = PathBuf::from(args.next().expect("usage: <phase> <store-dir>"));

    match phase.as_str() {
        // Phase A (0.2.6): create and populate past snapshot boundaries.
        "create" => {
            let store = open_store(&store_dir, true);
            store
                .register_state(StateRegistration {
                    id: "messages".to_string(),
                    strategy: StateStrategy::AppendLog {
                        delta_snapshot_every: DELTA_EVERY,
                        full_snapshot_every: FULL_EVERY,
                    },
                    initial_value: None,
                })
                .expect("register messages");
            store
                .register_state(StateRegistration {
                    id: "config".to_string(),
                    strategy: StateStrategy::Snapshot,
                    initial_value: None,
                })
                .expect("register config");
            store
                .register_state(StateRegistration {
                    id: "files".to_string(),
                    strategy: StateStrategy::Tree {
                        delta_snapshot_every: DELTA_EVERY,
                        full_snapshot_every: FULL_EVERY,
                    },
                    initial_value: None,
                })
                .expect("register files");

            set_config(&store, 1);
            let checkpoint_seq = append_messages(&store, 0, 20);
            set_config(&store, 2);
            for i in 0..8 {
                tree_put(&store, &format!("dir/file-{i}.txt"), &tree_file_content(i));
            }

            std::fs::write(
                checkpoint_path(&store_dir),
                serde_json::to_vec(&json!({"seq": checkpoint_seq})).unwrap(),
            )
            .expect("write checkpoint");

            verify(&store, 20, 8, 2);
            println!("phase create: OK (checkpoint seq {checkpoint_seq})");
        }

        // Phase B (working tree): open, verify, retune, write more.
        "extend" => {
            let store = open_store(&store_dir, false);
            verify(&store, 20, 8, 2);
            verify_checkpoint(&store, read_checkpoint(&store_dir), 20);

            // Exercise the cherry-picked update_state_strategy leg: retune
            // cadence on the existing store (same strategy kind).
            #[cfg(feature = "new-api")]
            store
                .update_state_strategy(
                    "messages",
                    StateStrategy::AppendLog {
                        delta_snapshot_every: DELTA_EVERY + 1,
                        full_snapshot_every: FULL_EVERY,
                    },
                )
                .expect("update_state_strategy");

            append_messages(&store, 20, 40);
            set_config(&store, 3);
            for i in 8..12 {
                tree_put(&store, &format!("dir/file-{i}.txt"), &tree_file_content(i));
            }

            verify(&store, 40, 12, 3);
            println!("phase extend: OK");
        }

        // Phase C (0.2.6): the acceptance gate — open a store the new
        // build wrote, materialize, time-travel, branch, keep writing.
        "verify-old" => {
            let store = open_store(&store_dir, false);
            verify(&store, 40, 12, 3);

            let checkpoint_seq = read_checkpoint(&store_dir);
            verify_checkpoint(&store, checkpoint_seq, 20);

            // Branch at the checkpoint over the mixed-provenance chain.
            let main_branch = store.current_branch().name.clone();
            store
                .create_branch_at("rollback-check", &main_branch, Sequence(checkpoint_seq))
                .expect("create_branch_at");
            store
                .switch_branch("rollback-check")
                .expect("switch to branch");
            let branched = store
                .get_state("messages")
                .expect("get on branch")
                .expect("messages on branch");
            let items: Vec<serde_json::Value> = serde_json::from_slice(&branched).unwrap();
            assert_eq!(items.len(), 20, "messages on checkpoint branch");
            store.switch_branch(&main_branch).expect("switch back");

            // A rolled-back 0.2.6 must be able to keep operating.
            append_messages(&store, 40, 45);
            verify(&store, 45, 12, 3);
            println!("phase verify-old: OK");
        }

        // Phase D (working tree): reopen after the 0.2.6 rollback wrote.
        "verify-new" => {
            let store = open_store(&store_dir, false);
            verify(&store, 45, 12, 3);
            verify_checkpoint(&store, read_checkpoint(&store_dir), 20);
            println!("phase verify-new: OK");
        }

        other => panic!("unknown phase {other}"),
    }
}
