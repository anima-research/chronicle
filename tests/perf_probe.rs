//! Not part of the normal test run — execute with:
//!   cargo test --release --test perf_probe -- --ignored --nocapture

use chronicle::{RecordInput, Store, StoreConfig};
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
