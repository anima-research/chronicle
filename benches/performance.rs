//! Performance benchmarks for the chronicle store.
//!
//! Groups parameterized by size run at geometric points so per-op cost can be
//! compared across a decade of N — flat per-op cost across sizes is the pass
//! condition (see the superlinear-scaling campaign). Deterministic disk-growth
//! assertions live in `tests/disk_scaling.rs`; these benches measure wall-clock.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use chronicle::{
    RecordInput, StateOperation, StateRegistration, StateStrategy, Store, StoreConfig, TreeEntry,
};
use serde_json::json;
use tempfile::TempDir;

fn create_store(dir: &TempDir) -> Store {
    Store::create(StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 1000,
        create_if_missing: true,
    })
    .unwrap()
}

fn register_append_log(store: &Store, id: &str) {
    store
        .register_state(StateRegistration {
            id: id.to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 100,
                full_snapshot_every: 20,
            },
            initial_value: None,
        })
        .unwrap();
}

/// Benchmark state reconstruction with varying chain depths
fn bench_state_reconstruction(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_reconstruction");

    for chain_depth in [10, 100, 500, 1000] {
        group.bench_with_input(
            BenchmarkId::new("chain_depth", chain_depth),
            &chain_depth,
            |b, &depth| {
                let dir = TempDir::new().unwrap();
                let store = create_store(&dir);

                store
                    .register_state(StateRegistration {
                        id: "items".to_string(),
                        strategy: StateStrategy::AppendLog {
                            delta_snapshot_every: 10000, // No snapshots during bench
                            full_snapshot_every: 1000,
                        },
                        initial_value: None,
                    })
                    .unwrap();

                // Build chain
                for i in 0..depth {
                    store
                        .update_state(
                            "items",
                            StateOperation::Append(format!("{}", i).into_bytes()),
                        )
                        .unwrap();
                }

                b.iter(|| {
                    black_box(store.get_state("items").unwrap());
                });
            },
        );
    }

    group.finish();
}

/// Benchmark state reconstruction with snapshots
fn bench_state_with_snapshots(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_with_snapshots");

    // Fixed total operations, varying snapshot frequency
    let total_ops = 1000;

    for snapshot_every in [10, 50, 100, 500] {
        group.bench_with_input(
            BenchmarkId::new("snapshot_every", snapshot_every),
            &snapshot_every,
            |b, &snap_freq| {
                let dir = TempDir::new().unwrap();
                let store = create_store(&dir);

                store
                    .register_state(StateRegistration {
                        id: "items".to_string(),
                        strategy: StateStrategy::AppendLog {
                            delta_snapshot_every: snap_freq,
                            full_snapshot_every: 10,
                        },
                        initial_value: None,
                    })
                    .unwrap();

                for i in 0..total_ops {
                    store
                        .update_state(
                            "items",
                            StateOperation::Append(format!("{}", i).into_bytes()),
                        )
                        .unwrap();
                }

                b.iter(|| {
                    black_box(store.get_state("items").unwrap());
                });
            },
        );
    }

    group.finish();
}

/// Benchmark record append at varying pre-existing store sizes.
/// Per-op cost must be flat in store size (append-only log).
fn bench_record_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_append");

    for store_size in [0usize, 10_000, 50_000] {
        group.bench_with_input(
            BenchmarkId::new("store_size", store_size),
            &store_size,
            |b, &size| {
                let dir = TempDir::new().unwrap();
                let store = create_store(&dir);
                for i in 0..size {
                    let input = RecordInput::json("filler", &json!({"seq": i})).unwrap();
                    store.append(input).unwrap();
                }

                b.iter(|| {
                    let input = RecordInput::json("event", &json!({"data": "test"})).unwrap();
                    black_box(store.append(input).unwrap());
                });
            },
        );
    }

    group.finish();
}

/// Benchmark blob operations
fn bench_blob_store(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let store = create_store(&dir);

    let content: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();

    c.bench_function("blob_store_10kb", |b| {
        b.iter(|| {
            black_box(store.store_blob(&content, "application/octet-stream").unwrap());
        });
    });
}

/// Benchmark with historical records (untraversed)
fn bench_with_history(c: &mut Criterion) {
    let mut group = c.benchmark_group("with_history");

    for history_size in [100, 1000, 10000] {
        group.bench_with_input(
            BenchmarkId::new("history_records", history_size),
            &history_size,
            |b, &size| {
                let dir = TempDir::new().unwrap();
                let store = create_store(&dir);

                // Create historical records (untraversed)
                for i in 0..size {
                    let input = RecordInput::json("history", &json!({"seq": i})).unwrap();
                    store.append(input).unwrap();
                }

                // Register state AFTER history
                store
                    .register_state(StateRegistration {
                        id: "current".to_string(),
                        strategy: StateStrategy::AppendLog {
                            delta_snapshot_every: 50,
                            full_snapshot_every: 10,
                        },
                        initial_value: None,
                    })
                    .unwrap();

                for i in 0..100 {
                    store
                        .update_state(
                            "current",
                            StateOperation::Append(format!("{}", i).into_bytes()),
                        )
                        .unwrap();
                }

                // Benchmark reading current state (should not traverse history)
                b.iter(|| {
                    black_box(store.get_state("current").unwrap());
                });
            },
        );
    }

    group.finish();
}

/// Point reads on AppendLog states at varying log lengths.
/// Regression bench for the per-item cache (cde8ecb): O(item), flat in log length.
fn bench_point_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("point_reads");

    for log_len in [1_000usize, 10_000, 50_000] {
        let dir = TempDir::new().unwrap();
        let store = create_store(&dir);
        register_append_log(&store, "log");
        for i in 0..log_len {
            store
                .update_state(
                    "log",
                    StateOperation::Append(
                        serde_json::to_vec(&json!({"seq": i, "body": "x".repeat(64)})).unwrap(),
                    ),
                )
                .unwrap();
        }

        group.bench_with_input(
            BenchmarkId::new("get_state_item_mid", log_len),
            &log_len,
            |b, &len| {
                b.iter(|| {
                    black_box(store.get_state_item("log", len / 2).unwrap());
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("get_state_tail_20", log_len),
            &log_len,
            |b, _| {
                b.iter(|| {
                    black_box(store.get_state_tail("log", 20).unwrap());
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("get_state_len", log_len),
            &log_len,
            |b, _| {
                b.iter(|| {
                    black_box(store.get_state_len("log").unwrap());
                });
            },
        );
    }

    group.finish();
}

/// append_to_state_json_with_identity at varying accumulated log sizes.
/// The AF inference/process logs and the conhost subagent archive ride on this;
/// per-append cost must be flat in accumulated length.
fn bench_json_append_with_identity(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_append_with_identity");

    for log_len in [100usize, 1_000, 10_000] {
        group.bench_with_input(
            BenchmarkId::new("accumulated", log_len),
            &log_len,
            |b, &len| {
                let dir = TempDir::new().unwrap();
                let store = create_store(&dir);
                register_append_log(&store, "log");
                for i in 0..len {
                    store
                        .append_to_state_json_with_identity(
                            "log",
                            json!({"kind": "warmup", "seq": i}),
                            "id",
                            "sequence",
                        )
                        .unwrap();
                }

                b.iter(|| {
                    black_box(
                        store
                            .append_to_state_json_with_identity(
                                "log",
                                json!({"kind": "bench", "payload": "y".repeat(128)}),
                                "id",
                                "sequence",
                            )
                            .unwrap(),
                    );
                });
            },
        );
    }

    group.finish();
}

/// Tree state operations at varying tree sizes.
/// The AF turn-checkpoints tree rides on tree_set/tree_get/tree_remove;
/// per-op cost should stay flat (or logarithmic) in entry count.
fn bench_tree_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("tree_ops");

    fn entry(i: usize) -> TreeEntry {
        TreeEntry {
            blob_hash: format!("{:064x}", i),
            size: 1024,
            mode: 0o644,
        }
    }

    for tree_size in [100usize, 1_000, 10_000] {
        let dir = TempDir::new().unwrap();
        let store = create_store(&dir);
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
        for i in 0..tree_size {
            store.tree_set("tree", &format!("agents/a{}", i), &entry(i)).unwrap();
        }

        group.bench_with_input(
            BenchmarkId::new("tree_set_overwrite", tree_size),
            &tree_size,
            |b, &size| {
                b.iter(|| {
                    black_box(
                        store
                            .tree_set("tree", &format!("agents/a{}", size / 2), &entry(0))
                            .unwrap(),
                    );
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("tree_get", tree_size),
            &tree_size,
            |b, &size| {
                b.iter(|| {
                    black_box(store.tree_get("tree", &format!("agents/a{}", size / 2)).unwrap());
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_state_reconstruction,
    bench_state_with_snapshots,
    bench_record_append,
    bench_blob_store,
    bench_with_history,
    bench_point_reads,
    bench_json_append_with_identity,
    bench_tree_ops,
);

criterion_main!(benches);
