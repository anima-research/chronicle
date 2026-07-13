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
#[ignore = "known quadratic: fixed-interval full snapshots embed whole state, O(N^2/K) disk — see anima-research/chronicle#11; un-ignore when snapshot spacing is size-aware"]
fn append_log_disk_linear() {
    let bytes = measure(&SIZES, |store, n| {
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
    assert_linear("append_log(100x20)", &SIZES, &bytes, 5.0);
}

/// The AF inference/process-log write path (append_to_state_json_with_identity).
#[test]
#[ignore = "known quadratic: same snapshot-cadence term as append_log_disk_linear — see anima-research/chronicle#11"]
fn json_append_with_identity_disk_linear() {
    let bytes = measure(&SIZES, |store, n| {
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
    assert_linear("json_append_with_identity", &SIZES, &bytes, 5.0);
}

/// Tree states (the AF turn-checkpoints layout): unique-path inserts.
#[test]
#[ignore = "known quadratic: same snapshot-cadence term as append_log_disk_linear, worst case (growing key set) — see anima-research/chronicle#11"]
fn tree_disk_linear() {
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
    assert_linear("tree(100x20)", &SIZES, &bytes, 5.0);
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

/// The known-quadratic control: whole-array Set per "append" (the pre-fix AF
/// pattern). This test asserts the pattern IS detected as superlinear by our
/// measurement — guarding the gate itself against going numb.
#[test]
fn set_rewrite_control_is_superlinear() {
    let sizes = [100usize, 400, 1600];
    let bytes = measure(&sizes, |store, n| {
        store
            .register_state(StateRegistration {
                id: "state".to_string(),
                strategy: StateStrategy::Snapshot,
                initial_value: None,
            })
            .unwrap();
        let mut entries: Vec<serde_json::Value> = Vec::new();
        for i in 0..n {
            entries.push(json!({"seq": i, "body": "x".repeat(64)}));
            store
                .update_state(
                    "state",
                    StateOperation::Set(serde_json::to_vec(&entries).unwrap()),
                )
                .unwrap();
        }
    });
    println!("--- set_rewrite_control ---");
    for (n, b) in sizes.iter().zip(&bytes) {
        println!("  n={:<8} bytes={:<12}", n, b);
    }
    let ratio = bytes[2] as f64 / bytes[1] as f64;
    assert!(
        ratio > 10.0,
        "control failed: Set-rewrite pattern only grew {:.2}x for 4x ops — \
         the disk measurement would not catch a quadratic regression",
        ratio
    );
}
