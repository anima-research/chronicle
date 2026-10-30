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

/// Diagnostic: tally state-op record types and sizes for the append_log gate
/// workload, to see exactly when snapshots fire under the current policy.
#[test]
#[ignore = "manual probe"]
fn probe_snapshot_tally() {
    use chronicle::StateOperation as SO;
    let dir = TempDir::new().unwrap();
    let store = create_store(&dir);
    register_log(&store, 100, 20);
    for i in 0..2000usize {
        append_json(&store, i);
    }
    store.sync().unwrap();
    let recs: Vec<_> = store
        .iter_from(chronicle::Sequence(0))
        .filter_map(|r| r.ok().map(|(_, rec)| rec))
        .collect();
    let mut tally: std::collections::BTreeMap<&str, (usize, usize)> = Default::default();
    let mut snapshot_seqs: Vec<(u64, usize)> = Vec::new();
    for r in &recs {
        let upd: chronicle::StateUpdateRecord = match serde_json::from_slice(&r.payload) {
            Ok(u) => u,
            Err(_) => continue,
        };
        let (name, bytes) = match &upd.operation {
            SO::Append(d) => ("Append", d.len()),
            SO::Snapshot(d) => ("Snapshot(full)", d.len()),
            SO::DeltaSnapshot(d) => ("DeltaSnapshot", d.len()),
            other => ("other", serde_json::to_vec(other).map(|v| v.len()).unwrap_or(0)),
        };
        if name == "Snapshot(full)" {
            snapshot_seqs.push((r.sequence.0, bytes));
        }
        let e = tally.entry(name).or_default();
        e.0 += 1;
        e.1 += bytes;
    }
    for (name, (count, bytes)) in &tally {
        println!("{:<16} count={:<6} bytes={}", name, count, bytes);
    }
    println!("full snapshots at (seq, bytes): {:?}", snapshot_seqs);
}

/// How long `Store::open` takes over a large log, and how much of it one pass
/// of the record iterator is. Open reads every record twice: the validating
/// scan, then the index rebuild through the iterator. Each point is the min of
/// 3 opens through a warm page cache, so it measures the reads' CPU and
/// syscall cost, not the disk.
#[test]
#[ignore = "manual probe"]
fn probe_open_time_over_a_large_log() {
    for records in [50_000usize, 200_000] {
        let dir = TempDir::new().unwrap();
        {
            let store = create_store(&dir);
            for i in 0..records {
                let input =
                    RecordInput::json("event", &json!({"seq": i, "body": "x".repeat(64)})).unwrap();
                store.append(input).unwrap();
            }
            store.sync().unwrap();
        }
        let config = || StoreConfig {
            path: dir.path().join("store"),
            blob_cache_size: 1000,
            create_if_missing: false,
        };
        let mut open_ms = f64::INFINITY;
        let mut iterate_ms = f64::INFINITY;
        for _ in 0..3 {
            let t = Instant::now();
            let store = Store::open(config()).unwrap();
            open_ms = open_ms.min(t.elapsed().as_secs_f64() * 1000.0);
            let t = Instant::now();
            let seen = store.iter_from(chronicle::Sequence(0)).count();
            iterate_ms = iterate_ms.min(t.elapsed().as_secs_f64() * 1000.0);
            assert_eq!(seen, records);
        }
        println!(
            "records={:<8} open {:>9.1} ms   one iterator pass {:>9.1} ms",
            records, open_ms, iterate_ms
        );
    }
}
