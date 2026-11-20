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
    FieldIndexKind, StateOperation, StateRegistration, StateStrategy, Store,
    StoreConfig, TreeEntry,
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

struct ProbeStore {
    store: Store,
    _dir: TempDir,
    messages: usize,
    branches: usize,
    body_bytes: usize,
    head: u64,
}

fn indexed_messages(messages: usize, branches: usize) -> ProbeStore {
    indexed_messages_of(messages, branches, 64)
}

fn message_id(i: usize) -> String {
    format!("m{i:06}")
}

fn indexed_messages_of(messages: usize, branches: usize, body_bytes: usize) -> ProbeStore {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(&dir)).unwrap();
    let mut parent = store.current_branch().name;
    for i in 2..branches {
        parent = store
            .create_empty_branch(&format!("other-{i}"), Some(&parent))
            .unwrap()
            .name;
    }
    store
        .create_empty_branch("active", Some(&parent))
        .unwrap();
    store.switch_branch("active").unwrap();
    store
        .register_state(StateRegistration {
            id: "messages".into(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: messages as u64 + 1,
                full_snapshot_every: 20,
            },
            initial_value: None,
        })
        .unwrap();
    let mut previous_head = None;
    for i in 0..messages {
        let last = i == messages - 1;
        if last {
            previous_head = store.state_head_offset("messages").unwrap();
        }
        let bulky = last || i == messages / 2 || i == messages / 2 + 1;
        let body = "x".repeat(if bulky { body_bytes.max(messages * 64) } else { body_bytes });
        let message = json!({"channel": if last { "rare" } else { "common" },
                             "id": message_id(i), "timestamp": i, "body": body});
        store
            .update_state(
                "messages",
                StateOperation::Append(serde_json::to_vec(&message).unwrap()),
            )
            .unwrap();
    }
    let head = store.state_head_offset("messages").unwrap().unwrap();
    assert_ne!(
        Some(head),
        previous_head,
        "last append should advance the freshness token"
    );
    store
        .register_state_field_index("messages", "/channel", FieldIndexKind::String)
        .unwrap();
    store
        .register_state_field_index("messages", "/timestamp", FieldIndexKind::Number)
        .unwrap();
    store
        .register_state_field_index("messages", "/id", FieldIndexKind::String)
        .unwrap();
    store.sync().unwrap();
    ProbeStore {
        store,
        _dir: dir,
        messages,
        branches,
        body_bytes,
        head,
    }
}

fn probe_growth<T>(
    name: &str,
    small: &ProbeStore,
    large: &ProbeStore,
    read: impl Fn(&ProbeStore) -> T,
    check: impl Fn(&ProbeStore, T),
) -> f64 {
    let measure = |p: &ProbeStore| {
        let min_ms = (0..3)
            .map(|_| {
                let mut total = 0.0;
                for _ in 0..128 {
                    p.store.clear_state_caches();
                    let rebuilds = p.store.state_reconstruction_count();
                    let (result, ms) = timed_ms(|| read(p));
                    total += ms;
                    check(p, result);
                    assert_eq!(
                        p.store.state_reconstruction_count(),
                        rebuilds,
                        "{name} rebuilt the slot"
                    );
                }
                total
            })
            .fold(f64::INFINITY, f64::min);
        println!(
            "  {name}: {} messages of {} bytes, {} branches: {min_ms:.3} ms/128 calls",
            p.messages, p.body_bytes, p.branches
        );
        min_ms
    };
    let base = measure(small);
    measure(large) / (base + 1.0)
}

#[test]
fn cold_metadata_and_narrow_index_probes_do_not_scale_with_message_content() {
    let _serial = GATE.lock().unwrap_or_else(|e| e.into_inner());
    let small = indexed_messages(128, 2);
    let deep = indexed_messages(8192, 2);
    const FLAT: f64 = 3.0;
    let growth = probe_growth(
        "stateHeadOffset",
        &small,
        &deep,
        |p| p.store.state_head_offset("messages").unwrap(),
        |p, got| assert_eq!(got, Some(p.head)),
    );
    assert!(
        growth < FLAT,
        "stateHeadOffset cost grew {growth:.2}x with history"
    );

    let growth = probe_growth(
        "getStateLen",
        &small,
        &deep,
        |p| p.store.get_state_len("messages").unwrap(),
        |p, got| assert_eq!(got, Some(p.messages)),
    );
    assert!(
        growth < FLAT,
        "getStateLen cost grew {growth:.2}x with history"
    );

    let heavy = indexed_messages_of(small.messages, small.branches, 64 * 1024);
    let counts = |p: &ProbeStore| p.store.get_state_index_value_counts("messages", "/channel");
    let check_counts = |p: &ProbeStore, got: Option<Vec<(String, u32)>>| {
        let mut counts = got.expect("registered channel index");
        counts.sort_unstable();
        assert_eq!(
            counts,
            [
                (String::from("common"), p.messages as u32 - 1),
                (String::from("rare"), 1)
            ]
        );
    };
    let content = probe_growth("getStateIndexValueCounts", &small, &heavy, counts, check_counts);
    assert!(
        content < FLAT,
        "getStateIndexValueCounts cost grew {content:.2}x with message content"
    );
    let index = probe_growth("getStateIndexValueCounts", &small, &deep, counts, check_counts);
    assert!(
        index < FLAT,
        "getStateIndexValueCounts cost grew {index:.2}x with 64x the messages under the same two values"
    );

    let id_keys = |p: &ProbeStore| -> Vec<(String, Vec<u32>)> {
        let n = p.messages;
        let hits = [0, n / 3, 2 * n / 3, n - 1].map(|i| (message_id(i), vec![i as u32]));
        let misses = [n, n + 1, 3 * n, 999_999].map(|i| (message_id(i), vec![]));
        hits.into_iter().chain(misses).collect()
    };
    let growth = probe_growth(
        "queryStateIndexEq by id",
        &small,
        &deep,
        |p| {
            id_keys(p)
                .into_iter()
                .map(|(key, _)| p.store.query_state_index_eq("messages", "/id", &key, None, None))
                .collect::<Vec<_>>()
        },
        |p, got| {
            let expected: Vec<_> = id_keys(p).into_iter().map(|(_, ordinals)| Some(ordinals)).collect();
            assert_eq!(got, expected);
        },
    );
    assert!(
        growth < FLAT,
        "queryStateIndexEq cost grew {growth:.2}x with distinct keys"
    );

    let growth = probe_growth(
        "queryStateIndexEq",
        &small,
        &deep,
        |p| {
            p.store
                .query_state_index_eq("messages", "/channel", "rare", None, None)
        },
        |p, got| assert_eq!(got, Some(vec![p.messages as u32 - 1])),
    );
    assert!(
        growth < FLAT,
        "queryStateIndexEq cost grew {growth:.2}x with nonmatches"
    );

    let growth = probe_growth(
        "queryStateIndexRange",
        &small,
        &deep,
        |p| {
            let mid = (p.messages / 2) as f64;
            p.store.query_state_index_range(
                "messages",
                "/timestamp",
                Some(mid),
                Some(mid + 1.0),
                None,
                None,
                false,
            )
        },
        |p, got| {
            let mid = (p.messages / 2) as u32;
            assert_eq!(got, Some(vec![mid, mid + 1]))
        },
    );
    let log_ratio = (deep.messages as f64).log2() / (small.messages as f64).log2();
    assert!(
        growth < FLAT * log_ratio,
        "queryStateIndexRange cost grew {growth:.2}x with history"
    );
}
