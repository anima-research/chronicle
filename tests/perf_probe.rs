//! Targeted wall-clock probes for anomalies surfaced by benches/performance.rs.
//! Not part of the normal test run — execute with:
//!   cargo test --release --test perf_probe -- --ignored --nocapture

use chronicle::{
    RecordInput, StateRegistration, StateStrategy, Store, StoreConfig,
};
use serde_json::json;
use std::time::Instant;
use tempfile::TempDir;

fn create_store(dir: &TempDir) -> Store {
    Store::create(StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 1000,
        create_if_missing: true,
    })
    .unwrap()
}

fn register_log(store: &Store, delta: u64, full: u64) {
    store
        .register_state(StateRegistration {
            id: "log".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: delta,
                full_snapshot_every: full,
            },
            initial_value: None,
        })
        .unwrap();
}

fn append_json(store: &Store, i: usize) {
    store
        .append_to_state_json_with_identity(
            "log",
            json!({"kind": "entry", "seq": i, "body": "x".repeat(64)}),
            "id",
            "sequence",
        )
        .unwrap();
}

/// Is the json-append slowdown at large accumulated size caused by snapshot
/// cadence, or by the append path itself?
#[test]
#[ignore = "manual probe"]
fn probe_json_append_snapshot_isolation() {
    const TIMED: usize = 500;
    for (label, delta, full) in [
        ("default(100x20)", 100u64, 20u64),
        ("snapshots-off", 1_000_000_000u64, 1_000_000_000u64),
    ] {
        println!("--- cadence: {} ---", label);
        for accumulated in [1_000usize, 10_000, 50_000] {
            let dir = TempDir::new().unwrap();
            let store = create_store(&dir);
            register_log(&store, delta, full);
            for i in 0..accumulated {
                append_json(&store, i);
            }
            let t = Instant::now();
            for i in 0..TIMED {
                append_json(&store, accumulated + i);
            }
            let per_op = t.elapsed().as_micros() as f64 / TIMED as f64;
            println!("  accumulated={:<8} {:>10.1} us/append", accumulated, per_op);
        }
    }
}

/// Is the plain-record-append slowdown at 50k store size real, and where does
/// it set in?
#[test]
#[ignore = "manual probe"]
fn probe_record_append_store_size() {
    const TIMED: usize = 1_000;
    for store_size in [0usize, 10_000, 50_000, 100_000, 200_000] {
        let dir = TempDir::new().unwrap();
        let store = create_store(&dir);
        for i in 0..store_size {
            let input = RecordInput::json("filler", &json!({"seq": i})).unwrap();
            store.append(input).unwrap();
        }
        let t = Instant::now();
        for _ in 0..TIMED {
            let input = RecordInput::json("event", &json!({"data": "test"})).unwrap();
            store.append(input).unwrap();
        }
        let per_op = t.elapsed().as_micros() as f64 / TIMED as f64;
        println!("store_size={:<8} {:>10.1} us/append", store_size, per_op);
    }
}
