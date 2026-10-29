//! Cold-reconstruction scaling gates.
//!
//! Reconstruction (store reopen, cache miss, branch switch) must be
//! (near-)linear in state size. Historically it applied ops one at a time,
//! each op re-parsing and re-serializing the full state — O(tail × N) — which
//! every warm-cache bench missed. These gates measure a rebuild from the log:
//! build a store, drop every cached state value (`clear_state_caches`), and
//! time the next `get_state`, which `state_reconstruction_count` shows rebuilt
//! the value. Opening a store and switching branch aren't timed. The OS page
//! cache stays warm. That is acceptable for THIS gate because the quadratic it
//! guards is CPU-bound (per-op parse + reserialize):
//! it measured 11.6× per 4× N through a warm page cache before the fix. A
//! disk-latency regression would need an I/O-cold harness; this is not one.
//!
//! Phase discipline: cold-read cost depends on state size N AND on the tail
//! of ops since the last full snapshot — and under the size-aware (doubling)
//! full-snapshot policy a fixed op count lands at an arbitrary phase of the
//! doubling cycle, different per workload. (Review caught this file's first
//! version with edit-sprinkled tails of 1,978 ops at n=3,900 but 529 at
//! n=15,900 — anti-correlated with the property under test; the deleted
//! quadratic scored BETTER than the fix on that gate.) So the gates assume
//! no phase: each build steps until it OBSERVES a full snapshot fire (the
//! `ops_since_last_full_snapshot` counter resetting), then grows the tail to
//! half the state size at that full. Size and tail are then both proportional
//! to the observed full size across gate sizes, and the assertion bound
//! scales off the measured item ratio instead of assuming 4×. The tables
//! print items/tail per point so the phase is auditable, not assumed.
//!
//! Timing discipline: each point is the min of 3 rebuilds, each timed by the
//! thread's CPU time, so time spent waiting to run on a busy machine isn't
//! counted, and the gates serialize on a mutex so they never contend with each
//! other. Tables print with `-- --nocapture`. Unix only: the thread's CPU time
//! comes from `clock_gettime`.
#![cfg(unix)]

use chronicle::{
    StateOperation, StateRegistration, StateStrategy, Store, StoreConfig, TreeEntry,
};
use serde_json::json;
use std::sync::Mutex;
use tempfile::TempDir;

/// Serializes the gates: timed reads must not contend.
static GATE: Mutex<()> = Mutex::new(());

fn config(dir: &TempDir) -> StoreConfig {
    StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: true,
    }
}

struct SizedStore {
    store: Store,
    _dir: TempDir,
    /// Items in the state at measurement time.
    items: usize,
    /// Ops since the last full snapshot at measurement time.
    tail: u64,
}

/// Build a store at a KNOWN worst-ish phase: run `step` n times, keep
/// stepping until a full snapshot is observed to fire, then keep stepping
/// until the tail since that full is half the state size at the full. Both
/// the state size and the tail end up proportional to the size of the
/// observed full, whatever op count the doubling policy fired it at.
fn build_at_known_phase<F: FnMut(&Store, usize)>(
    state_id: &str,
    n: usize,
    register: impl Fn(&Store),
    mut step: F,
) -> SizedStore {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(&dir)).unwrap();
    register(&store);
    let tail = |store: &Store| {
        store
            .get_compaction_stats(state_id)
            .map(|s| s.ops_since_last_full_snapshot)
            .unwrap_or(0)
    };
    let mut items = 0;
    for _ in 0..n {
        step(&store, items);
        items += 1;
    }
    // Step until a full fires: the tail counter resets when it does.
    let mut prev = tail(&store);
    loop {
        step(&store, items);
        items += 1;
        let t = tail(&store);
        if t < prev {
            break;
        }
        prev = t;
    }
    // The full just fired covers ~items entries; the next full is at least
    // that many ops away (size-aware spacing), so a half-size tail cannot
    // trigger one mid-growth.
    let target = (items / 2) as u64;
    while tail(&store) < target {
        step(&store, items);
        items += 1;
    }
    let tail = tail(&store);
    store.sync().unwrap();
    SizedStore { store, _dir: dir, items, tail }
}

fn timed_ms<T>(f: impl FnOnce() -> T) -> (T, f64) {
    fn cpu_ms() -> f64 {
        let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
        let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
        assert_eq!(rc, 0, "clock_gettime(CLOCK_THREAD_CPUTIME_ID)");
        ts.tv_sec as f64 * 1000.0 + ts.tv_nsec as f64 / 1e6
    }
    let start = cpu_ms();
    let value = f();
    (value, cpu_ms() - start)
}

fn min_rebuild_ms(store: &Store, state_id: &str) -> f64 {
    (0..3)
        .map(|_| {
            store.clear_state_caches();
            let rebuilds = store.state_reconstruction_count();
            let (state, ms) = timed_ms(|| store.get_state(state_id).unwrap());
            assert!(state.is_some(), "the read returned no state");
            assert_eq!(
                store.state_reconstruction_count(),
                rebuilds + 1,
                "the read rebuilt the state"
            );
            ms
        })
        .fold(f64::INFINITY, f64::min)
}

/// Assert per-window cold-read growth stays below 2× the measured item-count
/// growth. Linear reconstruction tracks the item ratio (~4× per window with
/// cadence wobble); the per-op quadratic tracks its square (~16×): tail and
/// size both grow with items by construction, so cost ∝ items × items.
fn assert_cold_linear(name: &str, points: &[(SizedStore, f64)]) {
    println!("--- {} ---", name);
    for (s, t) in points {
        println!(
            "  items={:<8} tail={:<7} min cold read {:>9.1} ms",
            s.items, s.tail, t
        );
    }
    for w in points.windows(2) {
        let item_ratio = w[1].0.items as f64 / w[0].0.items as f64;
        let limit = item_ratio * 2.0;
        let ratio = w[1].1 / w[0].1.max(0.5);
        println!(
            "  {:.2}x items -> {:.2}x cold-read time (limit {:.1}x)",
            item_ratio, ratio, limit
        );
        assert!(
            ratio < limit,
            "{}: cold read grew {:.2}x for {:.2}x items (limit {:.1}x) — superlinear reconstruction",
            name,
            ratio,
            item_ratio,
            limit
        );
    }
}

/// Base step counts: the observed full then lands at the next doubling point
/// (~3k/12k items), giving a ~4× item ratio between the gate sizes.
const COLD_SIZES: [usize; 2] = [750, 6_000];

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

fn append_entry(store: &Store, i: usize) {
    store
        .update_state(
            "log",
            StateOperation::Append(
                serde_json::to_vec(&json!({"seq": i, "body": "x".repeat(64)})).unwrap(),
            ),
        )
        .unwrap();
}

/// Pure appends at framework-default cadence: cold read must be ~linear even
/// with the doubling policy's long delta-snapshot chains.
#[test]
fn cold_read_append_log_linear() {
    let _serial = GATE.lock().unwrap_or_else(|e| e.into_inner());
    let points: Vec<(SizedStore, f64)> = COLD_SIZES
        .iter()
        .map(|&n| {
            let s = build_at_known_phase("log", n, register_log, append_entry);
            let t = min_rebuild_ms(&s.store, "log");
            (s, t)
        })
        .collect();
    assert_cold_linear("cold_append_log", &points);
}

/// Appends with an edit sprinkled every 50 steps (the summary-merge pattern):
/// edits block delta snapshots, so the raw tail grows — cold read must stay
/// ~linear regardless.
#[test]
fn cold_read_edit_sprinkled_linear() {
    let _serial = GATE.lock().unwrap_or_else(|e| e.into_inner());
    let step = |store: &Store, i: usize| {
        append_entry(store, i);
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
    };
    let points: Vec<(SizedStore, f64)> = COLD_SIZES
        .iter()
        .map(|&n| {
            let s = build_at_known_phase("log", n, register_log, step);
            let t = min_rebuild_ms(&s.store, "log");
            (s, t)
        })
        .collect();
    assert_cold_linear("cold_edit_sprinkled", &points);
}

/// Tree states: unique-path inserts at default cadence. Cold reconstruction
/// applies TreeSet chains — must be ~linear in entry count.
#[test]
fn cold_read_tree_linear() {
    let _serial = GATE.lock().unwrap_or_else(|e| e.into_inner());
    let register = |store: &Store| {
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
    };
    let step = |store: &Store, i: usize| {
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
    };
    let points: Vec<(SizedStore, f64)> = COLD_SIZES
        .iter()
        .map(|&n| {
            let s = build_at_known_phase("tree", n, register, step);
            let t = min_rebuild_ms(&s.store, "tree");
            (s, t)
        })
        .collect();
    assert_cold_linear("cold_tree", &points);
}
