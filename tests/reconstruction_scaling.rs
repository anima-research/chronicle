//! Cold-reconstruction scaling gates.
//!
//! Reconstruction (store reopen, cache miss, branch switch) must be
//! (near-)linear in state size. Historically it applied ops one at a time,
//! each op re-parsing and re-serializing the full state — O(tail × N) — which
//! every warm-cache bench missed. These gates measure the genuinely cold path:
//! build a store, drop it, reopen, time the first `get_state`.
//!
//! Sizes sit just below doubling-policy full-snapshot points (worst phase:
//! longest possible tail since the last full), so the quadratic signature is
//! maximal: per-4×-N cold-read ratio ~16× when reconstruction is per-op,
//! ~4× when it materializes once. Bound 8.0 separates them.
//!
//! Wall-clock in CI is noisy; the bound is deliberately generous and the
//! workloads are sized for seconds, not minutes. Tables print with
//! `-- --nocapture`.

use chronicle::{
    StateOperation, StateRegistration, StateStrategy, Store, StoreConfig, TreeEntry,
};
use serde_json::json;
use std::time::Instant;
use tempfile::TempDir;

fn config(dir: &TempDir) -> StoreConfig {
    StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: true,
    }
}

/// Build a store with `build`, drop it, reopen, and time the first read.
fn cold_read_ms<F: Fn(&Store)>(dir: &TempDir, state_id: &str, build: F) -> f64 {
    {
        let store = Store::create(config(dir)).unwrap();
        build(&store);
        store.sync().unwrap();
    }
    let store = Store::open(config(dir)).unwrap();
    let t = Instant::now();
    let state = store.get_state(state_id).unwrap();
    let ms = t.elapsed().as_secs_f64() * 1000.0;
    assert!(state.is_some(), "cold read returned no state");
    ms
}

fn assert_cold_linear(name: &str, sizes: &[usize], times_ms: &[f64], max_ratio_per_4x: f64) {
    println!("--- {} ---", name);
    for (n, t) in sizes.iter().zip(times_ms) {
        println!("  n={:<8} cold read {:>10.1} ms", n, t);
    }
    for w in times_ms.windows(2) {
        let ratio = w[1] / w[0].max(0.1);
        println!("  4x n -> {:.2}x cold-read time", ratio);
        assert!(
            ratio < max_ratio_per_4x,
            "{}: cold read grew {:.2}x for 4x state size (limit {:.1}x) — superlinear reconstruction",
            name,
            ratio,
            max_ratio_per_4x
        );
    }
}

/// Worst-phase sizes: just below the doubling policy's full-snapshot points
/// (fulls at ~2k/4k/8k/16k ops), so the unsnapshotted tail is maximal.
const COLD_SIZES: [usize; 2] = [3_900, 15_900];
const COLD_LIMIT: f64 = 8.0;

fn register_log(store: &Store) {
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
}

/// Pure appends at framework-default cadence: cold read must be ~linear even
/// with the doubling policy's long delta-snapshot chains.
#[test]
#[ignore = "known quadratic: per-op reconstruction makes cold reads O(tail x N); un-ignore with single-pass materialization"]
fn cold_read_append_log_linear() {
    let times: Vec<f64> = COLD_SIZES
        .iter()
        .map(|&n| {
            let dir = TempDir::new().unwrap();
            cold_read_ms(&dir, "log", |store| {
                register_log(store);
                for i in 0..n {
                    store
                        .update_state(
                            "log",
                            StateOperation::Append(
                                serde_json::to_vec(&json!({"seq": i, "body": "x".repeat(64)}))
                                    .unwrap(),
                            ),
                        )
                        .unwrap();
                }
            })
        })
        .collect();
    assert_cold_linear("cold_append_log", &COLD_SIZES, &times, COLD_LIMIT);
}

/// Appends with an edit sprinkled every 50 ops (the summary-merge pattern):
/// edits block delta snapshots, so the raw tail grows — cold read must stay
/// ~linear regardless.
#[test]
fn cold_read_edit_sprinkled_linear() {
    let times: Vec<f64> = COLD_SIZES
        .iter()
        .map(|&n| {
            let dir = TempDir::new().unwrap();
            cold_read_ms(&dir, "log", |store| {
                register_log(store);
                for i in 0..n {
                    store
                        .update_state(
                            "log",
                            StateOperation::Append(
                                serde_json::to_vec(&json!({"seq": i, "body": "x".repeat(64)}))
                                    .unwrap(),
                            ),
                        )
                        .unwrap();
                    if i % 50 == 49 {
                        store
                            .update_state(
                                "log",
                                StateOperation::Edit {
                                    index: i / 2,
                                    new_value: serde_json::to_vec(
                                        &json!({"seq": i / 2, "body": "edited"}),
                                    )
                                    .unwrap(),
                                },
                            )
                            .unwrap();
                    }
                }
            })
        })
        .collect();
    assert_cold_linear("cold_edit_sprinkled", &COLD_SIZES, &times, COLD_LIMIT);
}

/// Tree states: unique-path inserts at default cadence. Cold reconstruction
/// applies TreeSet chains — must be ~linear in entry count.
#[test]
fn cold_read_tree_linear() {
    let sizes = [1_900usize, 7_900];
    let times: Vec<f64> = sizes
        .iter()
        .map(|&n| {
            let dir = TempDir::new().unwrap();
            cold_read_ms(&dir, "tree", |store| {
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
            })
        })
        .collect();
    assert_cold_linear("cold_tree", &sizes, &times, COLD_LIMIT);
}
