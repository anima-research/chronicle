//! Disk-growth scaling gates.
//!
//! Deterministic CI gates for the superlinear-scaling campaign: total on-disk
//! bytes must grow (near-)linearly in operation count. Each test runs the same
//! workload at N and 4N ops and asserts the byte ratio stays well below the
//! quadratic signature (~16x); the ratio for linear growth is ~4x plus
//! snapshot-cadence overhead. Tables are printed for slope inspection with
//! `cargo test --test disk_scaling -- --nocapture`.

use chronicle::{
    RecordInput, StateOperation, StateRegistration, StateStrategy, Store, StoreConfig, TreeEntry,
};
use serde_json::json;
use std::fs;
use std::path::Path;
use tempfile::TempDir;

fn create_store(dir: &TempDir) -> Store {
    Store::create(StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: true,
    })
    .unwrap()
}

fn bytes_written<T>(op: impl FnOnce() -> T) -> (T, Option<u64>) {
    let written = || -> Option<u64> {
        let io = fs::read_to_string("/proc/thread-self/io").ok()?;
        io.lines()
            .find_map(|line| line.strip_prefix("wchar:"))?
            .trim()
            .parse()
            .ok()
    };
    let before = written();
    let result = op();
    let after = written();
    (result, before.zip(after).map(|(before, after)| after - before))
}

fn dir_bytes(path: &Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                total += dir_bytes(&p);
            } else if let Ok(md) = entry.metadata() {
                total += md.len();
            }
        }
    }
    total
}

/// Run `workload(store, n)` at each size; return total store bytes per size.
fn measure<F: Fn(&Store, usize)>(sizes: &[usize], workload: F) -> Vec<u64> {
    sizes
        .iter()
        .map(|&n| {
            let dir = TempDir::new().unwrap();
            let store = create_store(&dir);
            workload(&store, n);
            store.sync().unwrap();
            let bytes = dir_bytes(&dir.path().join("store"));
            drop(store);
            bytes
        })
        .collect()
}

fn assert_linear(name: &str, sizes: &[usize], bytes: &[u64], max_ratio_per_4x: f64) {
    println!("--- {} ---", name);
    for (n, b) in sizes.iter().zip(bytes) {
        println!("  n={:<8} bytes={:<12} bytes/op={:.0}", n, b, *b as f64 / *n as f64);
    }
    for w in bytes.windows(2) {
        let ratio = w[1] as f64 / w[0] as f64;
        println!("  4x ops -> {:.2}x bytes", ratio);
        assert!(
            ratio < max_ratio_per_4x,
            "{}: disk grew {:.2}x for 4x ops (limit {:.1}x) — superlinear disk growth",
            name,
            ratio,
            max_ratio_per_4x
        );
    }
}

const SIZES: [usize; 3] = [500, 2_000, 8_000];

/// Sizes for the growing-state tests. Deeper than SIZES so the 4× window
/// sits in the snapshot policy's steady state (the first full snapshot fires
/// at ~2k ops at framework-default cadence; windows straddling it measure the
/// warm-up transient, not the asymptote).
const GROWTH_SIZES: [usize; 2] = [8_000, 32_000];

/// Ratio limit for the growing-state tests. Size-aware (doubling) full
/// snapshots write ≈2× the append bytes in total (S + 2S + 4S + … ≤ 2N), so
/// with delta snapshots (≈1× appends) the steady-state total is ≈4× appends
/// and the per-4×-ops ratio converges to ~4.1–4.4 with phase-of-doubling
/// wobble. The old fixed-interval policy measures ≥7.5× at these same sizes —
/// 5.5 separates the two cleanly with margin both ways.
const GROWTH_LIMIT: f64 = 5.5;

/// Plain record appends: bytes/op must be constant.
#[test]
fn record_append_disk_linear() {
    let bytes = measure(&SIZES, |store, n| {
        for i in 0..n {
            let input = RecordInput::json("event", &json!({"seq": i, "body": "x".repeat(64)}))
                .unwrap();
            store.append(input).unwrap();
        }
    });
    assert_linear("record_append", &SIZES, &bytes, 5.0);
}

/// AppendLog via raw Append ops at framework-default snapshot cadence.
#[test]
fn append_log_disk_linear() {
    let bytes = measure(&GROWTH_SIZES, |store, n| {
        store
            .register_state(StateRegistration {
                id: "log".to_string(),
                strategy: StateStrategy::AppendLog {
                    delta_snapshot_every: 100,
                    full_snapshot_every: 20,
                },
                initial_value: None,
            })
            .unwrap();
        for i in 0..n {
            store
                .update_state(
                    "log",
                    StateOperation::Append(
                        serde_json::to_vec(&json!({"seq": i, "body": "x".repeat(64)})).unwrap(),
                    ),
                )
                .unwrap();
        }
    });
    assert_linear("append_log(100x20)", &GROWTH_SIZES, &bytes, GROWTH_LIMIT);
}

/// The AF inference/process-log write path (append_to_state_json_with_identity).
#[test]
fn json_append_with_identity_disk_linear() {
    let bytes = measure(&GROWTH_SIZES, |store, n| {
        store
            .register_state(StateRegistration {
                id: "log".to_string(),
                strategy: StateStrategy::AppendLog {
                    delta_snapshot_every: 100,
                    full_snapshot_every: 20,
                },
                initial_value: None,
            })
            .unwrap();
        for i in 0..n {
            store
                .append_to_state_json_with_identity(
                    "log",
                    json!({"kind": "entry", "seq": i, "body": "x".repeat(64)}),
                    "id",
                    "sequence",
                )
                .unwrap();
        }
    });
    assert_linear("json_append_with_identity", &GROWTH_SIZES, &bytes, GROWTH_LIMIT);
}

/// Tree states (the AF turn-checkpoints layout): unique-path inserts.
#[test]
fn tree_disk_linear() {
    let bytes = measure(&GROWTH_SIZES, |store, n| {
        store
            .register_state(StateRegistration {
                id: "tree".to_string(),
                strategy: StateStrategy::Tree {
                    delta_snapshot_every: 100,
                    full_snapshot_every: 20,
                },
                initial_value: None,
            })
            .unwrap();
        for i in 0..n {
            store
                .tree_set(
                    "tree",
                    &format!("agents/a{}", i),
                    &TreeEntry {
                        blob_hash: format!("{:064x}", i),
                        size: 1024,
                        mode: 0o644,
                    },
                )
                .unwrap();
        }
    });
    assert_linear("tree(100x20)", &GROWTH_SIZES, &bytes, GROWTH_LIMIT);
}

/// Appends with an Edit sprinkled every 50 ops — the autobio summary-merge
/// pattern. Edits set has_non_append_since_snapshot; disk must nevertheless
/// stay linear (historically this forced a FULL snapshot of the whole state
/// at every delta boundary — O(N²/D), the worst quadratic in the family).
#[test]
fn edit_sprinkled_append_log_disk_linear() {
    let bytes = measure(&GROWTH_SIZES, |store, n| {
        store
            .register_state(StateRegistration {
                id: "log".to_string(),
                strategy: StateStrategy::AppendLog {
                    delta_snapshot_every: 100,
                    full_snapshot_every: 20,
                },
                initial_value: None,
            })
            .unwrap();
        for i in 0..n {
            store
                .update_state(
                    "log",
                    StateOperation::Append(
                        serde_json::to_vec(&json!({"seq": i, "body": "x".repeat(64)})).unwrap(),
                    ),
                )
                .unwrap();
            if i % 50 == 49 {
                store
                    .update_state(
                        "log",
                        StateOperation::Edit {
                            index: i / 2,
                            new_value: serde_json::to_vec(&json!({"seq": i / 2, "body": "edited"}))
                                .unwrap(),
                        },
                    )
                    .unwrap();
            }
        }
    });
    assert_linear("edit_sprinkled_append_log", &GROWTH_SIZES, &bytes, GROWTH_LIMIT);
}

/// Tree overwrite churn: repeatedly rewriting the SAME small set of paths.
/// Disk must scale with op count, not op count x tree size.
#[test]
fn tree_overwrite_churn_disk_linear() {
    let bytes = measure(&SIZES, |store, n| {
        store
            .register_state(StateRegistration {
                id: "tree".to_string(),
                strategy: StateStrategy::Tree {
                    delta_snapshot_every: 100,
                    full_snapshot_every: 20,
                },
                initial_value: None,
            })
            .unwrap();
        for i in 0..n {
            store
                .tree_set(
                    "tree",
                    &format!("agents/a{}", i % 10),
                    &TreeEntry {
                        blob_hash: format!("{:064x}", i),
                        size: 1024,
                        mode: 0o644,
                    },
                )
                .unwrap();
        }
    });
    assert_linear("tree_overwrite_churn", &SIZES, &bytes, 5.0);
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
        if let Some(bytes) = set_written {
            assert!(bytes <= TREE_BUDGET, "set in {n}-key tree wrote {bytes} bytes");
        }
        assert_eq!(after_set.state_slot_count, 1);
        assert_eq!(store.tree_get("tree", TARGET).unwrap(), Some(replacement));

        let (removed, remove_written) = bytes_written(|| store.tree_remove("tree", TARGET));
        removed.unwrap();
        let after_remove = store.stats().unwrap();
        if let Some(bytes) = remove_written {
            assert!(bytes <= TREE_BUDGET, "remove in {n}-key tree wrote {bytes} bytes");
        }
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
        if let Some(bytes) = edit_written {
            assert!(
                bytes <= 4 * edited.len() as u64 + 1024,
                "edit in {n}-item log wrote {bytes} bytes"
            );
        }
        assert_eq!(store.get_state_item("log", 0).unwrap(), Some(edited));
        assert_eq!(store.get_state_len("log").unwrap(), Some(n));
    }
}
