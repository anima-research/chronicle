#![cfg(unix)]

use chronicle::{
    FieldIndexKind, StateOperation, StateRegistration, StateStrategy, Store, StoreConfig,
};
use serde_json::json;
use tempfile::TempDir;

fn config(dir: &TempDir) -> StoreConfig {
    StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: true,
    }
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
