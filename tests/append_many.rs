//! `StateOperation::AppendMany`: several items appended to an append-log
//! state as one record, at one sequence, counted as one operation.
//!
//! Every read must see a batch exactly as it would see the same items
//! appended one at a time: values, lengths, tails, slices, point lookups,
//! iteration, delta snapshots, field-index answers, historical reads and
//! forks, before and after a reopen. What differs on purpose is the number
//! of records and the snapshot cadence, which counts a batch as one
//! operation, as it counts a `TreeBatch`.
//!
//! The first batch written to a store also marks its MANIFEST, so a build
//! that predates the variant refuses the whole store at open instead of
//! failing part way through it.

use chronicle::{
    FieldIndexKind, Sequence, StateOperation, StateRegistration, StateStrategy,
    StateUpdateRecord, Store, StoreConfig, StoreError,
};
use serde_json::{json, Value};
use std::path::Path;
use tempfile::TempDir;

fn config(path: &Path) -> StoreConfig {
    StoreConfig {
        path: path.to_path_buf(),
        blob_cache_size: 16,
        create_if_missing: true,
    }
}

fn register(store: &Store, id: &str, delta_snapshot_every: u64, full_snapshot_every: u64) {
    store
        .register_state(StateRegistration {
            id: id.to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every,
                full_snapshot_every,
            },
            initial_value: None,
        })
        .unwrap();
}

fn batch(items: &[Value]) -> StateOperation {
    StateOperation::AppendMany(items.iter().map(|v| serde_json::to_vec(v).unwrap()).collect())
}

fn value(store: &Store, id: &str) -> Value {
    store
        .get_state(id)
        .unwrap()
        .map(|bytes| serde_json::from_slice(&bytes).unwrap())
        .unwrap_or(Value::Null)
}

fn text(bytes: Option<Vec<u8>>) -> Option<String> {
    bytes.map(|b| String::from_utf8(b).unwrap())
}

/// Item payloads for the differential workload. Several are not in
/// serde_json's own spelling (unsorted keys, whitespace, exponents,
/// escapes), so a read that returned stored bytes where a rebuild returns
/// re-serialized ones would show.
const RAW_ITEMS: [&[u8]; 8] = [
    b"{\"n\":1,\"tag\":\"a\"}",
    b"{ \"tag\" : \"b\", \"n\" : 2.50 }",
    b"{\"z\":[1, 2],\"n\":1e1,\"tag\":\"c\"}",
    b"{\"tag\":\"\\u0061\"}",
    b"{\"n\":-0.0,\"tag\":\"b\"}",
    b"7",
    b"\"plain\"",
    b"{\"n\":30,\"tag\":null}",
];

/// Every read of an append-log state the store offers, on the current
/// branch, as one comparable value. Byte-exact where the store promises its
/// own spelling (whole value, tail, slice, iteration); point lookups compare
/// as JSON values, since the head-record fast path returns the bytes a
/// writer stored, in whatever spelling it used, and two chains that take
/// their snapshots at different times reach it at different moments.
fn observe(store: &Store, id: &str) -> Value {
    let len = store.get_state_len(id).unwrap();
    let n = len.unwrap_or(0);
    let tails: Vec<Option<String>> = [1usize, 2, 5, n.saturating_sub(1), n]
        .iter()
        .map(|&k| text(store.get_state_tail(id, k).unwrap()))
        .collect();
    let items: Vec<Value> = (n.saturating_sub(7)..n)
        .map(|i| serde_json::from_slice(&store.get_state_item(id, i).unwrap().unwrap()).unwrap())
        .collect();
    let iterated: Option<Vec<Value>> = store
        .iter_state_items(id)
        .unwrap()
        .map(|it| it.map(|item| item.unwrap()).collect());
    let tags: Vec<Option<Vec<u32>>> = ["a", "b", "c"]
        .iter()
        .map(|tag| store.query_state_index_eq(id, "/tag", tag, None, None))
        .collect();
    json!({
        "state": text(store.get_state(id).unwrap()),
        "len": len,
        "tails": tails,
        "slice": text(store.get_state_slice(id, 1, 4).unwrap()),
        "items": items,
        "iterated": iterated,
        "tags": tags,
        "n_range": store.query_state_index_range(id, "/n", Some(0.0), Some(20.0), None, None, false),
        "n_all": store.query_state_index_range(id, "/n", None, None, None, None, true),
    })
}

/// The same random workload written to two states of one store: "single"
/// gets each batch as one `Append` per item, "batched" as one
/// `AppendMany`. Edits, redacts, compactions and cache drops are applied to
/// both alike. Every read must agree after every step, against a model,
/// and again after a reopen.
#[test]
fn a_batch_reads_exactly_as_its_items_appended_one_at_a_time() {
    for seed in [3u64, 17, 101, 4242] {
        let dir = TempDir::new().unwrap();
        let mut rng = seed.wrapping_mul(0x9E3779B97F4A7C15) | 1;
        let mut rnd = move |bound: usize| -> usize {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            (rng % bound.max(1) as u64) as usize
        };
        let mut model: Vec<Value> = Vec::new();
        // The batched state's value as of each batch it recorded.
        let mut history: Vec<(Sequence, Vec<Value>)> = Vec::new();
        {
            let store = Store::create(config(dir.path())).unwrap();
            // Small cadences, so both chains cross many delta and full
            // snapshots, at different points.
            register(&store, "single", 3, 3);
            register(&store, "batched", 3, 3);
            for id in ["single", "batched"] {
                store.register_state_field_index(id, "/tag", FieldIndexKind::String).unwrap();
                store.register_state_field_index(id, "/n", FieldIndexKind::Number).unwrap();
            }

            for step in 0..160 {
                match rnd(100) {
                    0..=59 => {
                        let raws: Vec<&[u8]> =
                            (0..1 + rnd(6)).map(|_| RAW_ITEMS[rnd(RAW_ITEMS.len())]).collect();
                        for raw in &raws {
                            store.update_state("single", StateOperation::Append(raw.to_vec())).unwrap();
                            model.push(serde_json::from_slice(raw).unwrap());
                        }
                        let record = store
                            .update_state(
                                "batched",
                                StateOperation::AppendMany(raws.iter().map(|r| r.to_vec()).collect()),
                            )
                            .unwrap();
                        history.push((record.sequence, model.clone()));
                    }
                    60..=69 if !model.is_empty() => {
                        let index = rnd(model.len());
                        let raw = RAW_ITEMS[rnd(RAW_ITEMS.len())];
                        for id in ["single", "batched"] {
                            store
                                .update_state(id, StateOperation::Edit { index, new_value: raw.to_vec() })
                                .unwrap();
                        }
                        model[index] = serde_json::from_slice(raw).unwrap();
                    }
                    70..=74 => {
                        let start = rnd(model.len() + 1);
                        let end = (start + rnd(3)).min(model.len());
                        for id in ["single", "batched"] {
                            store.update_state(id, StateOperation::Redact { start, end }).unwrap();
                        }
                        model.drain(start..end);
                    }
                    75..=79 => store.clear_state_caches(),
                    80..=83 => {
                        store.compact_state("single").unwrap();
                        store.compact_state("batched").unwrap();
                    }
                    _ => {}
                }

                let single = observe(&store, "single");
                let batched = observe(&store, "batched");
                assert_eq!(batched, single, "seed {seed} step {step}: batched and single reads differ");
                if !model.is_empty() {
                    assert_eq!(value(&store, "batched"), json!(model), "seed {seed} step {step}: model");
                }
            }
            assert!(history.len() > 20, "seed {seed}: the workload must write batches");

            // Historical reads and forks at a batch's own sequence see the
            // value as of that batch, whole.
            for (at, expected) in history.iter().step_by(5) {
                assert_eq!(
                    store.get_state_at("batched", *at).unwrap().map(|b| serde_json::from_slice::<Value>(&b).unwrap()),
                    Some(json!(expected)),
                    "seed {seed}: get_state_at {at:?}"
                );
            }
            let (at, expected) = history[history.len() / 2].clone();
            let fork = store.create_branch_at("fork", "main", at).unwrap();
            assert_eq!(
                store.get_state_len_on(fork.id, "batched").unwrap(),
                Some(expected.len()),
                "seed {seed}: a fork at a batch counts its items"
            );
            store
                .update_state_on(fork.id, "batched", batch(&[json!({"n": 5, "tag": "fork"})]))
                .unwrap();
            let mut forked = expected.clone();
            forked.push(json!({"n": 5, "tag": "fork"}));
            store.clear_state_caches();
            assert_eq!(
                store
                    .get_state_on(fork.id, "batched")
                    .unwrap()
                    .map(|b| serde_json::from_slice::<Value>(&b).unwrap()),
                Some(json!(forked))
            );
            store.sync().unwrap();
        }

        // The same from a cold reopen, where field indexes come back from
        // their own file and every value from the log.
        let store = Store::open(config(dir.path())).unwrap();
        assert_eq!(observe(&store, "batched"), observe(&store, "single"), "seed {seed}: after reopen");
        if !model.is_empty() {
            assert_eq!(value(&store, "batched"), json!(model), "seed {seed}: model after reopen");
        }
    }
}

/// The delta snapshot a batch leads to holds every item of the batch, in
/// order, together with the single appends around it. Collecting only
/// `Append` records would cover the batch's operation count and drop its
/// items, so a cold read after the delta would lose them.
#[test]
fn a_delta_snapshot_keeps_every_item_of_a_batch() {
    let dir = TempDir::new().unwrap();
    {
        let store = Store::create(config(dir.path())).unwrap();
        register(&store, "log", 3, 100);
        store
            .update_state("log", StateOperation::Append(serde_json::to_vec(&json!("a")).unwrap()))
            .unwrap();
        store
            .update_state("log", batch(&[json!("b"), json!("c"), json!("d")]))
            .unwrap();
        let before = store.get_records_by_type("state_update").len();
        // The third operation since the start: the store takes a delta
        // snapshot right after it.
        store
            .update_state("log", StateOperation::Append(serde_json::to_vec(&json!("e")).unwrap()))
            .unwrap();
        let ids = store.get_records_by_type("state_update");
        assert_eq!(ids.len(), before + 2, "the append and the delta snapshot after it");
        let snapshot = StateUpdateRecord::decode(&store.get_record(*ids.last().unwrap()).unwrap().unwrap()).unwrap();
        match snapshot.operation {
            StateOperation::DeltaSnapshot(items) => assert_eq!(
                serde_json::from_slice::<Value>(&items).unwrap(),
                json!(["a", "b", "c", "d", "e"])
            ),
            other => panic!("expected a delta snapshot at the head, got {other:?}"),
        }

        // A batch after the delta, then a second delta covering it.
        store.update_state("log", batch(&[json!("f"), json!("g")])).unwrap();
        store.update_state("log", batch(&[json!("h")])).unwrap();
        store
            .update_state("log", StateOperation::Append(serde_json::to_vec(&json!("i")).unwrap()))
            .unwrap();
        store.clear_state_caches();
        assert_eq!(value(&store, "log"), json!(["a", "b", "c", "d", "e", "f", "g", "h", "i"]));
        assert_eq!(store.get_compaction_stats("log").unwrap().delta_snapshots_since_full, 2);
    }
    let store = Store::open(config(dir.path())).unwrap();
    assert_eq!(value(&store, "log"), json!(["a", "b", "c", "d", "e", "f", "g", "h", "i"]));
    assert_eq!(store.get_state_len("log").unwrap(), Some(9));
    assert_eq!(
        text(store.get_state_tail("log", 4).unwrap()).unwrap(),
        r#"["f","g","h","i"]"#
    );
}

/// A batch is one record, one sequence and one operation: the snapshot
/// cadence advances by one for it, however many items it holds.
#[test]
fn a_batch_is_one_record_one_sequence_and_one_operation() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    register(&store, "log", 2, 100);
    let head = store.current_branch().head;
    let items: Vec<Value> = (0..50).map(|i| json!({ "i": i })).collect();
    let record = store.update_state("log", batch(&items)).unwrap();
    assert_eq!(record.sequence, Sequence(head.0 + 1));
    assert_eq!(store.current_branch().head, record.sequence, "no snapshot after one operation");
    assert_eq!(store.get_state_len("log").unwrap(), Some(50));
    assert_eq!(store.get_compaction_stats("log").unwrap().ops_since_last_full_snapshot, 1);
    assert_eq!(store.get_chain_stats("log").unwrap().unwrap().total_operations, 1);

    // The second operation reaches the delta cadence of 2.
    store.update_state("log", batch(&items[..3])).unwrap();
    let stats = store.get_compaction_stats("log").unwrap();
    assert_eq!(stats.delta_snapshots_since_full, 1);
    assert_eq!(store.get_state_len("log").unwrap(), Some(53));
}

/// An empty batch is refused before anything is written: it would be a
/// record that appends nothing.
#[test]
fn an_empty_batch_is_refused_and_writes_nothing() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    register(&store, "log", 10, 10);
    store.update_state("log", batch(&[json!(1)])).unwrap();
    let head = store.current_branch().head;
    let records = store.stats().unwrap().record_count;

    for op in [
        StateOperation::AppendMany(vec![]),
        StateOperation::Field {
            name: "f".to_string(),
            operation: Box::new(StateOperation::AppendMany(vec![])),
        },
    ] {
        match store.update_state("log", op) {
            Err(StoreError::InvalidOperation(message)) => {
                assert!(message.contains("AppendMany"), "{message}")
            }
            other => panic!("expected InvalidOperation, got {other:?}"),
        }
    }
    assert_eq!(store.current_branch().head, head);
    assert_eq!(store.stats().unwrap().record_count, records);
    assert_eq!(value(&store, "log"), json!([1]));
}

/// A write keeps a warm per-item cache current through a batch instead of
/// dropping it, and a cold lookup of the batch's last item is served from
/// the head record alone. Neither re-materializes the state.
#[test]
fn reads_after_a_batch_do_not_rematerialize() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    register(&store, "log", 1000, 1000);
    let mut expected: Vec<Value> = (0..40).map(|i| json!({ "i": i })).collect();
    store.update_state("log", batch(&expected)).unwrap();

    // Warm the item cache once.
    assert_eq!(store.get_state_slice("log", 0, usize::MAX).unwrap().map(|b| serde_json::from_slice::<Value>(&b).unwrap()), Some(json!(expected)));
    let warm = store.state_reconstruction_count();

    let more: Vec<Value> = (40..47).map(|i| json!({ "i": i, "z": [i] })).collect();
    store.update_state("log", batch(&more)).unwrap();
    expected.extend(more.iter().cloned());
    assert_eq!(
        store.get_state_slice("log", 38, 5).unwrap().map(|b| serde_json::from_slice::<Value>(&b).unwrap()),
        Some(json!(expected[38..43]))
    );
    for (i, item) in expected.iter().enumerate().skip(40) {
        assert_eq!(
            serde_json::from_slice::<Value>(&store.get_state_item("log", i).unwrap().unwrap()).unwrap(),
            *item
        );
    }
    assert_eq!(store.state_reconstruction_count(), warm, "the cache followed the batch");

    // Cold: the last item of the head batch comes from the head record.
    store.clear_state_caches();
    let last = store.get_state_item("log", expected.len() - 1).unwrap().unwrap();
    assert_eq!(serde_json::from_slice::<Value>(&last).unwrap(), expected[expected.len() - 1]);
    assert_eq!(store.state_reconstruction_count(), warm, "the head batch served its last item");
    // Any other index materializes, once, and serves the rest from cache.
    let first = store.get_state_item("log", 40).unwrap().unwrap();
    assert_eq!(serde_json::from_slice::<Value>(&first).unwrap(), expected[40]);
    assert_eq!(store.state_reconstruction_count(), warm + 1);
    // Written in the store's own spelling, the fast path's bytes are the
    // rebuilt value's bytes.
    let whole = store.get_state("log").unwrap().unwrap();
    let rebuilt: Vec<&serde_json::value::RawValue> = serde_json::from_slice(&whole).unwrap();
    assert_eq!(last, rebuilt.last().unwrap().get().as_bytes());
}

/// Field indexes registered on a branch take every item of a batch written
/// there, at consecutive ordinals, and a batch with an item that is not
/// JSON drops them (as an unparsable `Append` does) rather than leaving
/// them a few ordinals short.
#[test]
fn field_indexes_take_every_item_of_a_batch() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    register(&store, "log", 1000, 1000);
    store.register_state_field_index("log", "/id", FieldIndexKind::String).unwrap();
    store
        .update_state("log", batch(&[json!({"id": "1"}), json!({"id": "2"}), json!({"id": "1"})]))
        .unwrap();
    store
        .update_state("log", StateOperation::Append(serde_json::to_vec(&json!({"id": "3"})).unwrap()))
        .unwrap();
    store.update_state("log", batch(&[json!({"id": "2"}), json!({"id": "4"})])).unwrap();
    assert_eq!(store.query_state_index_eq("log", "/id", "1", None, None), Some(vec![0, 2]));
    assert_eq!(store.query_state_index_eq("log", "/id", "2", None, None), Some(vec![1, 4]));
    assert_eq!(store.query_state_index_eq("log", "/id", "4", None, None), Some(vec![5]));

    store
        .update_state(
            "log",
            StateOperation::AppendMany(vec![b"{\"id\":\"5\"}".to_vec(), b"{not json".to_vec()]),
        )
        .unwrap();
    assert_eq!(store.query_state_index_eq("log", "/id", "1", None, None), None, "poisoned, not short");
}

// ---------------------------------------------------------------------------
// MANIFEST
// ---------------------------------------------------------------------------

/// What every build before `AppendMany` checks when it opens a store: the
/// magic, then a version byte that must be 1. Copied from `verify_manifest`
/// as it stood, so the test states what such a build does with the bytes
/// this one writes.
fn older_build_opens(manifest: &[u8]) -> Result<(), String> {
    if manifest.len() < 5 || &manifest[..4] != b"RST\0" {
        return Err("Invalid store magic".into());
    }
    if manifest[4] != 1 {
        return Err(format!("Unsupported store version: {}", manifest[4]));
    }
    Ok(())
}

fn manifest(dir: &Path) -> Vec<u8> {
    std::fs::read(dir.join("MANIFEST")).unwrap()
}

#[test]
fn a_store_without_a_batch_keeps_the_manifest_older_builds_open() {
    let dir = TempDir::new().unwrap();
    {
        let store = Store::create(config(dir.path())).unwrap();
        register(&store, "log", 2, 2);
        for i in 0..10 {
            store
                .update_state("log", StateOperation::Append(serde_json::to_vec(&json!(i)).unwrap()))
                .unwrap();
        }
        store.update_state("log", StateOperation::Redact { start: 0, end: 1 }).unwrap();
        store.compact_state("log").unwrap();
    }
    drop(Store::open(config(dir.path())).unwrap());
    assert_eq!(manifest(dir.path()), b"RST\0\x01");
    assert_eq!(older_build_opens(&manifest(dir.path())), Ok(()));
}

#[test]
fn the_first_batch_marks_the_manifest_so_older_builds_refuse_the_store() {
    let dir = TempDir::new().unwrap();
    {
        let store = Store::create(config(dir.path())).unwrap();
        register(&store, "log", 2, 2);
        store
            .update_state("log", StateOperation::Append(serde_json::to_vec(&json!(0)).unwrap()))
            .unwrap();
        assert_eq!(manifest(dir.path()), b"RST\0\x01", "single appends leave the manifest alone");

        store.update_state("log", batch(&[json!(1), json!(2)])).unwrap();
        let marked = manifest(dir.path());
        assert_eq!(&marked[..5], b"RST\0\x02");
        assert_eq!(u64::from_le_bytes(marked[5..13].try_into().unwrap()) & 1, 1, "the AppendMany flag");
        assert_eq!(marked.len(), 13);
        assert_eq!(
            older_build_opens(&marked),
            Err("Unsupported store version: 2".to_string()),
            "an older build refuses the store at open"
        );

        // Later batches find the flag already set.
        let modified = std::fs::metadata(dir.path().join("MANIFEST")).unwrap().modified().unwrap();
        store.update_state("log", batch(&[json!(3)])).unwrap();
        assert_eq!(manifest(dir.path()), marked);
        assert_eq!(
            std::fs::metadata(dir.path().join("MANIFEST")).unwrap().modified().unwrap(),
            modified,
            "the manifest is written once"
        );
    }

    // This build opens it and keeps writing to it.
    let store = Store::open(config(dir.path())).unwrap();
    assert_eq!(value(&store, "log"), json!([0, 1, 2, 3]));
    store.update_state("log", batch(&[json!(4)])).unwrap();
    assert_eq!(store.get_state_len("log").unwrap(), Some(5));
    assert_eq!(&manifest(dir.path())[..5], b"RST\0\x02");
}

/// A manifest naming a format feature this build doesn't know was written
/// by a newer build: refused at open, before anything is read.
#[test]
fn a_manifest_with_an_unknown_feature_is_refused() {
    let dir = TempDir::new().unwrap();
    drop(Store::create(config(dir.path())).unwrap());
    let mut bytes = b"RST\0\x02".to_vec();
    bytes.extend_from_slice(&(1u64 | 1 << 40).to_le_bytes());
    std::fs::write(dir.path().join("MANIFEST"), &bytes).unwrap();
    match Store::open(config(dir.path())) {
        Err(StoreError::InvalidFormat(message)) => {
            assert!(message.contains("newer"), "{message}")
        }
        Err(other) => panic!("expected InvalidFormat, got {other:?}"),
        Ok(_) => panic!("opened a store with an unknown format feature"),
    }

    // A truncated version-2 manifest is refused too.
    std::fs::write(dir.path().join("MANIFEST"), b"RST\0\x02\x01\x00").unwrap();
    assert!(matches!(Store::open(config(dir.path())), Err(StoreError::InvalidFormat(_))));
}

/// The flag reaches the disk before the record does: when the manifest
/// can't be replaced, the batch fails and nothing is recorded.
#[cfg(unix)]
#[test]
fn a_batch_whose_manifest_mark_fails_is_not_recorded() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let store = Store::create(config(&path)).unwrap();
    register(&store, "log", 10, 10);
    store
        .update_state("log", StateOperation::Append(serde_json::to_vec(&json!(0)).unwrap()))
        .unwrap();
    let head = store.current_branch().head;
    let records = store.stats().unwrap().record_count;

    std::fs::rename(path.join("MANIFEST"), path.join("MANIFEST.aside")).unwrap();
    std::fs::create_dir(path.join("MANIFEST")).unwrap();
    let result = store.update_state("log", batch(&[json!(1), json!(2)]));
    std::fs::remove_dir(path.join("MANIFEST")).unwrap();
    std::fs::rename(path.join("MANIFEST.aside"), path.join("MANIFEST")).unwrap();
    match result {
        Err(StoreError::Io(error)) => {
            let message = error.to_string();
            assert!(
                message.contains(&*path.join("MANIFEST").to_string_lossy()) && message.contains("not recorded"),
                "{message}"
            );
        }
        Err(e) => panic!("expected the MANIFEST's I/O error, got {e}"),
        Ok(_) => panic!("the batch must fail when its manifest mark fails"),
    }
    assert_eq!(store.current_branch().head, head);
    assert_eq!(store.stats().unwrap().record_count, records);
    assert_eq!(value(&store, "log"), json!([0]));
    drop(store);

    let store = Store::open(config(&path)).unwrap();
    assert_eq!(store.current_branch().head, head);
    assert_eq!(store.stats().unwrap().record_count, records);
    assert_eq!(value(&store, "log"), json!([0]));
    assert_eq!(manifest(&path), b"RST\0\x01");

    store.update_state("log", batch(&[json!(1), json!(2)])).unwrap();
    assert_eq!(value(&store, "log"), json!([0, 1, 2]));
    assert_eq!(&manifest(&path)[..5], b"RST\0\x02");
}

/// A create aimed at a store that's open fails on the lock before it writes
/// anything. It used to rewrite the MANIFEST to version 1 first; the open
/// handle already named the feature, so later batches didn't mark it again,
/// and once they were behind the checkpoint no open did either, so a build
/// that predates batches opened the store and failed reading it.
#[test]
fn a_create_aimed_at_an_open_batched_store_leaves_its_manifest_alone() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let marked;
    {
        let store = Store::create(config(&path)).unwrap();
        register(&store, "log", 50, 10);
        store.update_state("log", batch(&[json!(1), json!(2), json!(3)])).unwrap();
        marked = manifest(&path);
        assert_eq!(&marked[..5], b"RST\0\x02");

        let created = Store::create(config(&path));
        assert!(
            matches!(created, Err(StoreError::Locked)),
            "got {:?}",
            created.err().map(|e| e.to_string())
        );
        assert_eq!(manifest(&path), marked, "the failed create left the MANIFEST as it was");

        store.update_state("log", batch(&[json!(4)])).unwrap();
        assert_eq!(manifest(&path), marked);
    }
    let store = Store::open(config(&path)).unwrap();
    assert_eq!(value(&store, "log"), json!([1, 2, 3, 4]));
    drop(store);
    assert_eq!(manifest(&path), marked);
    assert_eq!(
        older_build_opens(&manifest(&path)),
        Err("Unsupported store version: 2".to_string()),
        "an older build still refuses the store"
    );
}

/// A create over a closed batched store is refused, and the store, its
/// MANIFEST's mark among it, is left as it was.
#[test]
fn a_create_over_a_closed_batched_store_is_refused_and_keeps_its_mark() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    {
        let store = Store::create(config(&path)).unwrap();
        register(&store, "log", 50, 10);
        store.update_state("log", batch(&[json!(1), json!(2)])).unwrap();
    }
    let marked = manifest(&path);
    assert_eq!(&marked[..5], b"RST\0\x02");

    match Store::create(config(&path)) {
        Err(StoreError::InvalidOperation(message)) => {
            assert!(message.contains("already holds a Chronicle store"), "{message}")
        }
        Err(other) => panic!("expected InvalidOperation, got {other:?}"),
        Ok(_) => panic!("create made a new store over an existing one"),
    }
    assert_eq!(manifest(&path), marked);

    let store = Store::open(config(&path)).unwrap();
    assert_eq!(value(&store, "log"), json!([1, 2]));
    drop(store);
    assert_eq!(manifest(&path), marked);
}

// ---------------------------------------------------------------------------
// Identity-stamped batches
// ---------------------------------------------------------------------------

/// Objects for the identity workloads. Some already hold a value at a stamp
/// key, which the stamp overwrites, and some carry an `id` of their own, as
/// context-manager's window entries do, so a field index on it can be asked
/// where each one went.
fn identity_item(k: usize) -> Value {
    match k % 6 {
        0 => json!({ "id": format!("m{k}"), "n": k, "tag": "a" }),
        1 => json!({ "tag": "b", "n": 2.5, "nested": { "z": [k, k + 1] } }),
        2 => json!({ "id": format!("m{k}"), "by": "stale", "at": -1, "tag": "c" }),
        3 => json!({ "retract": format!("m{}", k / 2), "undone": true }),
        4 => json!({ "id": format!("m{k}"), "n": -0.0, "tag": null }),
        _ => json!({ "v": 2 }),
    }
}

/// `item` with the identity of `record` written in, by the test's own hand:
/// what an identity append is meant to store.
fn stamped(item: &Value, by: &str, at: &str, record: &chronicle::Record) -> Value {
    let mut item = item.clone();
    let map = item.as_object_mut().unwrap();
    map.insert(by.to_string(), json!(record.id.0.to_string()));
    map.insert(at.to_string(), json!(record.sequence.0));
    item
}

/// `observe`, plus the answers of the field indexes an identity reader
/// keeps: on the stamps, and on the items' own ids.
fn observe_stamped(store: &Store, id: &str) -> Value {
    json!({
        "reads": observe(store, id),
        "by": store.get_state_index_value_counts(id, "/by"),
        "at": store.query_state_index_range(id, "/at", None, None, None, None, false),
        "ids": store.get_state_index_value_counts(id, "/id"),
        "id_ordinals": (0..40)
            .map(|k| store.query_state_index_eq(id, "/id", &format!("m{k}"), None, None))
            .collect::<Vec<_>>(),
    })
}

/// The items the batch recorded as `id` holds, as stored.
fn batch_bytes(store: &Store, id: chronicle::RecordId) -> Vec<Vec<u8>> {
    match StateUpdateRecord::decode(&store.get_record(id).unwrap().unwrap()).unwrap().operation {
        StateOperation::AppendMany(items) => items,
        other => panic!("expected AppendMany, got {other:?}"),
    }
}

fn register_identity_indexes(store: &Store, id: &str) {
    for (path, kind) in [
        ("/tag", FieldIndexKind::String),
        ("/n", FieldIndexKind::Number),
        ("/by", FieldIndexKind::String),
        ("/at", FieldIndexKind::Number),
        ("/id", FieldIndexKind::String),
    ] {
        store.register_state_field_index(id, path, kind).unwrap();
    }
}

fn selection(id: &str) -> chronicle::StateSelection {
    chronicle::StateSelection {
        state_ids: vec![id.to_string()],
        state_prefixes: vec![],
    }
}

/// The same random workload on two stores: "stamping" writes its appends
/// with the identity forms, single and batched, and "plain" writes the same
/// appends with the plain forms, of the items as stamped with the record
/// "stamping" just took. The two stores record the same operations in the
/// same order, so each plain append takes the same record id and sequence
/// as its identity twin, and the records, every read, every field-index
/// answer and every branch cut must then agree byte for byte, after each
/// step and after a reopen. An identity batch is the batch of its items
/// stamped with its one record, and the existing differential above shows
/// that such a batch reads as its items appended one at a time.
#[test]
fn an_identity_batch_stores_its_items_stamped_with_its_one_record() {
    for seed in [5u64, 29, 777] {
        let stamping_dir = TempDir::new().unwrap();
        let plain_dir = TempDir::new().unwrap();
        let mut rng = seed.wrapping_mul(0x9E3779B97F4A7C15) | 1;
        let mut rnd = move |bound: usize| -> usize {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            (rng % bound.max(1) as u64) as usize
        };
        let mut model: Vec<Value> = Vec::new();
        // Each identity batch's record and the value as of it.
        let mut batches: Vec<(chronicle::Record, Vec<Value>)> = Vec::new();
        let mut k = 0usize;
        {
            let stamping = Store::create(config(stamping_dir.path())).unwrap();
            let plain = Store::create(config(plain_dir.path())).unwrap();
            for store in [&stamping, &plain] {
                register(store, "log", 3, 3);
                register_identity_indexes(store, "log");
            }

            for step in 0..120 {
                match rnd(100) {
                    0..=49 => {
                        let items: Vec<Value> = (0..1 + rnd(6))
                            .map(|_| {
                                k += 1;
                                identity_item(k)
                            })
                            .collect();
                        let len_before = model.len();
                        let a = stamping
                            .append_many_to_state_json_with_identity("log", &items, "by", "at")
                            .unwrap();
                        let expected: Vec<Value> =
                            items.iter().map(|item| stamped(item, "by", "at", &a.record)).collect();
                        let b = plain.append_many_to_state_json("log", &expected).unwrap();
                        assert_eq!(
                            (b.record.id, b.record.sequence, b.first_ordinal),
                            (a.record.id, a.record.sequence, a.first_ordinal),
                            "seed {seed} step {step}: the twin batch took the same record"
                        );
                        assert_eq!(a.first_ordinal, len_before, "seed {seed} step {step}");
                        // The records hold the same bytes.
                        assert_eq!(
                            batch_bytes(&stamping, a.record.id),
                            batch_bytes(&plain, b.record.id),
                            "seed {seed} step {step}: record payloads"
                        );
                        model.extend(expected);
                        batches.push((a.record, model.clone()));
                    }
                    50..=64 => {
                        k += 1;
                        let item = identity_item(k);
                        let a = stamping
                            .append_to_state_json_with_identity("log", item.clone(), "by", "at")
                            .unwrap();
                        let expected = stamped(&item, "by", "at", &a);
                        let b = plain
                            .update_state("log", StateOperation::Append(serde_json::to_vec(&expected).unwrap()))
                            .unwrap();
                        assert_eq!((b.id, b.sequence), (a.id, a.sequence), "seed {seed} step {step}");
                        model.push(expected);
                    }
                    65..=72 if !model.is_empty() => {
                        let index = rnd(model.len());
                        let raw = RAW_ITEMS[rnd(RAW_ITEMS.len())];
                        for store in [&stamping, &plain] {
                            store
                                .update_state("log", StateOperation::Edit { index, new_value: raw.to_vec() })
                                .unwrap();
                        }
                        model[index] = serde_json::from_slice(raw).unwrap();
                    }
                    73..=77 => {
                        let start = rnd(model.len() + 1);
                        let end = (start + rnd(3)).min(model.len());
                        for store in [&stamping, &plain] {
                            store.update_state("log", StateOperation::Redact { start, end }).unwrap();
                        }
                        model.drain(start..end);
                    }
                    78..=82 => {
                        stamping.clear_state_caches();
                        plain.clear_state_caches();
                    }
                    83..=86 => {
                        stamping.compact_state("log").unwrap();
                        plain.compact_state("log").unwrap();
                    }
                    _ => {}
                }
                assert_eq!(
                    observe_stamped(&stamping, "log"),
                    observe_stamped(&plain, "log"),
                    "seed {seed} step {step}: identity and plain reads differ"
                );
                if !model.is_empty() {
                    assert_eq!(value(&stamping, "log"), json!(model), "seed {seed} step {step}: model");
                }
            }
            assert!(batches.len() > 15, "seed {seed}: the workload must write batches");

            // Branch cuts at a batch's sequence keep the whole batch, and at
            // the sequence before it drop the whole batch, on both stores
            // alike.
            for (n, (record, expected)) in batches.iter().enumerate().step_by(4) {
                let batch_len = batch_bytes(&stamping, record.id).len();
                for (name, at, len) in [
                    (format!("at-{n}"), record.sequence, expected.len()),
                    (format!("before-{n}"), Sequence(record.sequence.0 - 1), expected.len() - batch_len),
                ] {
                    let mut cut = Vec::new();
                    for store in [&stamping, &plain] {
                        let branch = store.create_branch_at_with_states(&name, "main", at, &selection("log")).unwrap();
                        let items = store
                            .get_state_on(branch.id, "log")
                            .unwrap()
                            .map(|b| serde_json::from_slice::<Value>(&b).unwrap())
                            .unwrap_or(json!([]));
                        cut.push((text(store.get_state_on(branch.id, "log").unwrap()), items));
                    }
                    assert_eq!(cut[0], cut[1], "seed {seed}: the cut {name} differs");
                    assert_eq!(cut[0].1.as_array().unwrap().len(), len, "seed {seed}: the cut {name}");
                    if name.starts_with("at-") {
                        assert_eq!(cut[0].1, json!(expected), "seed {seed}: the cut {name} is the value as of the batch");
                    }
                }
            }
            stamping.sync().unwrap();
            plain.sync().unwrap();
        }

        // Cold, from the checkpoint and the log.
        let stamping = Store::open(config(stamping_dir.path())).unwrap();
        let plain = Store::open(config(plain_dir.path())).unwrap();
        assert_eq!(
            observe_stamped(&stamping, "log"),
            observe_stamped(&plain, "log"),
            "seed {seed}: after reopen"
        );
        if !model.is_empty() {
            assert_eq!(value(&stamping, "log"), json!(model), "seed {seed}: model after reopen");
        }
    }
}

/// n single identity appends and one identity batch of the same items: each
/// item is stamped with the record that holds it, which for the batch is its
/// one record for every item. Apart from the stamps the stored bytes are the
/// same, and with the single record's stamps swapped for the batch's they are
/// the batch's bytes exactly. A field index on the items' own ids answers
/// each item's ordinal in both, and a cut by an item's stamped sequence
/// keeps it, and the one before drops it: for a single append that is the
/// item alone, for the batch the whole batch.
#[test]
fn each_item_of_an_identity_batch_names_the_record_that_holds_it() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    for id in ["single", "batched"] {
        register(&store, id, 100, 100);
        register_identity_indexes(&store, id);
        store
            .append_to_state_json_with_identity(id, json!({ "id": "first" }), "by", "at")
            .unwrap();
    }
    let items: Vec<Value> = (0..6).map(identity_item).collect();
    let singles: Vec<chronicle::Record> = items
        .iter()
        .map(|item| {
            store
                .append_to_state_json_with_identity("single", item.clone(), "by", "at")
                .unwrap()
        })
        .collect();
    let batch = store
        .append_many_to_state_json_with_identity("batched", &items, "by", "at")
        .unwrap();
    assert_eq!(batch.first_ordinal, 1);
    for id in ["single", "batched"] {
        store
            .append_to_state_json_with_identity(id, json!({ "id": "last" }), "by", "at")
            .unwrap();
    }

    let read = |id: &str, ordinal: usize| -> (Vec<u8>, Value) {
        let bytes = store.get_state_item(id, ordinal).unwrap().unwrap();
        let value = serde_json::from_slice(&bytes).unwrap();
        (bytes, value)
    };
    for (i, item) in items.iter().enumerate() {
        let (single_bytes, single) = read("single", 1 + i);
        let (batched_bytes, batched) = read("batched", batch.first_ordinal + i);
        assert_eq!(single, stamped(item, "by", "at", &singles[i]), "item {i}: the single stamp");
        assert_eq!(batched, stamped(item, "by", "at", &batch.record), "item {i}: the batch stamp");
        assert_eq!(
            serde_json::to_vec(&stamped(&single, "by", "at", &batch.record)).unwrap(),
            batched_bytes,
            "item {i}: the single item, restamped with the batch's record, is the batch's bytes"
        );
        assert_ne!(single_bytes, batched_bytes, "item {i}: the stamps differ");

        // Each stamp names the record that holds the item, at its sequence.
        for (value, holder) in [(&single, &singles[i]), (&batched, &batch.record)] {
            let by: u64 = value["by"].as_str().unwrap().parse().unwrap();
            let record = store.get_record(chronicle::RecordId(by)).unwrap().unwrap();
            assert_eq!(record.id, holder.id);
            assert_eq!(value["at"], json!(record.sequence.0));
            let held = match StateUpdateRecord::decode(&record).unwrap().operation {
                StateOperation::Append(bytes) => vec![bytes],
                StateOperation::AppendMany(items) => items,
                other => panic!("item {i}: expected an append, got {other:?}"),
            };
            assert!(
                held.iter().any(|b| serde_json::from_slice::<Value>(b).unwrap() == *value),
                "item {i}: the record its stamp names holds it"
            );
        }
    }

    // The field index on the items' own ids answers each one's ordinal.
    for (i, item) in items.iter().enumerate() {
        if let Some(own) = item.get("id").and_then(Value::as_str) {
            for id in ["single", "batched"] {
                assert_eq!(
                    store.query_state_index_eq(id, "/id", own, None, None),
                    Some(vec![1 + i as u32]),
                    "{id}: {own}"
                );
            }
        }
    }
    // The stamp indexes: one ordinal per single record, the whole batch for
    // the batch's.
    let n = items.len() as u32;
    let batch_ordinals: Vec<u32> = (1..=n).collect();
    assert_eq!(
        store.query_state_index_eq("batched", "/by", &batch.record.id.0.to_string(), None, None),
        Some(batch_ordinals.clone())
    );
    let at = batch.record.sequence.0 as f64;
    assert_eq!(
        store.query_state_index_range("batched", "/at", Some(at), Some(at), None, None, false),
        Some(batch_ordinals)
    );
    for (i, record) in singles.iter().enumerate() {
        assert_eq!(
            store.query_state_index_eq("single", "/by", &record.id.0.to_string(), None, None),
            Some(vec![1 + i as u32])
        );
    }

    // Cuts by a stamped sequence.
    let cut_len = |id: &str, name: &str, at: Sequence| -> usize {
        let branch = store.create_branch_at_with_states(name, "main", at, &selection(id)).unwrap();
        store.get_state_len_on(branch.id, id).unwrap().unwrap_or(0)
    };
    for (i, record) in singles.iter().enumerate() {
        assert_eq!(cut_len("single", &format!("s-at-{i}"), record.sequence), 2 + i, "at single {i}");
        assert_eq!(
            cut_len("single", &format!("s-before-{i}"), Sequence(record.sequence.0 - 1)),
            1 + i,
            "before single {i}"
        );
    }
    let seq = batch.record.sequence;
    assert_eq!(cut_len("batched", "b-before", Sequence(seq.0 - 1)), 1, "before the batch: none of it");
    assert_eq!(cut_len("batched", "b-at", seq), 1 + items.len(), "at the batch: all of it");
    assert_eq!(
        cut_len("batched", "b-after", store.current_branch().head),
        2 + items.len(),
        "after the batch: all of it and what followed"
    );
}

/// An identity batch with an item that isn't a JSON object is refused
/// whole, as an empty one is, and nothing is recorded: no record, no
/// sequence, no items, and no MANIFEST mark, since the refusal comes before
/// the mark. So an older build refuses a store only once it holds a batch.
#[test]
fn an_identity_batch_that_is_refused_writes_nothing() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    register(&store, "log", 10, 10);
    store
        .append_to_state_json_with_identity("log", json!({ "n": 0 }), "by", "at")
        .unwrap();
    let head = store.current_branch().head;
    let records = store.stats().unwrap().record_count;
    let before = value(&store, "log");

    for (items, expected) in [
        (vec![json!({ "n": 1 }), json!([1, 2]), json!({ "n": 3 })], "item 1 of the batch"),
        (vec![json!("scalar")], "item 0 of the batch"),
        (vec![json!({ "n": 1 }), json!(42)], "item 1 of the batch"),
        (vec![], "AppendMany needs at least one item"),
    ] {
        match store.append_many_to_state_json_with_identity("log", &items, "by", "at") {
            Err(StoreError::InvalidOperation(message)) => {
                assert!(message.contains(expected), "{message}");
                if !items.is_empty() {
                    assert!(message.contains("JSON object"), "{message}");
                }
            }
            other => panic!("expected InvalidOperation, got {other:?}"),
        }
    }
    assert_eq!(store.current_branch().head, head);
    assert_eq!(store.stats().unwrap().record_count, records);
    assert_eq!(value(&store, "log"), before);
    assert_eq!(manifest(dir.path()), b"RST\0\x01", "a refused batch leaves the MANIFEST alone");
    assert_eq!(older_build_opens(&manifest(dir.path())), Ok(()));
}

/// Single identity appends leave a store in the format every release
/// opens. The first identity batch marks the MANIFEST, as any batch does,
/// so an older build refuses the store from then on, and this one reads
/// the stamped items back after a reopen.
#[test]
fn the_first_identity_batch_marks_the_manifest() {
    let dir = TempDir::new().unwrap();
    let expected;
    {
        let store = Store::create(config(dir.path())).unwrap();
        register(&store, "log", 2, 2);
        for n in 0..5 {
            store
                .append_to_state_json_with_identity("log", json!({ "n": n }), "by", "at")
                .unwrap();
        }
        store.sync().unwrap();
        assert_eq!(manifest(dir.path()), b"RST\0\x01");
        assert_eq!(older_build_opens(&manifest(dir.path())), Ok(()));

        let items = [json!({ "n": 5 }), json!({ "n": 6 })];
        let batch = store
            .append_many_to_state_json_with_identity("log", &items, "by", "at")
            .unwrap();
        assert_eq!(batch.first_ordinal, 5);
        let marked = manifest(dir.path());
        assert_eq!(&marked[..5], b"RST\0\x02");
        assert_eq!(u64::from_le_bytes(marked[5..13].try_into().unwrap()) & 1, 1, "the AppendMany flag");
        assert_eq!(
            older_build_opens(&marked),
            Err("Unsupported store version: 2".to_string())
        );
        expected = value(&store, "log");
        for item in &expected.as_array().unwrap()[5..] {
            assert_eq!(item["by"], json!(batch.record.id.0.to_string()));
            assert_eq!(item["at"], json!(batch.record.sequence.0));
        }
    }
    let store = Store::open(config(dir.path())).unwrap();
    assert_eq!(value(&store, "log"), expected);
    assert_eq!(&manifest(dir.path())[..5], b"RST\0\x02");
}

/// On a branch other than the current one, an identity batch counts that
/// branch's length and stamps the record it writes there.
#[test]
fn an_identity_batch_on_another_branch_counts_and_stamps_there() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    register(&store, "log", 10, 10);
    store
        .append_many_to_state_json_with_identity("log", &[json!({ "n": 0 }), json!({ "n": 1 })], "by", "at")
        .unwrap();
    let side = store.create_branch("side", None).unwrap();
    store
        .append_many_to_state_json_with_identity("log", &[json!({ "n": 2 })], "by", "at")
        .unwrap();
    let on_side = store
        .append_many_to_state_json_with_identity_on(
            side.id,
            "log",
            &[json!({ "side": 1 }), json!({ "side": 2 })],
            "by",
            "at",
        )
        .unwrap();
    assert_eq!(on_side.first_ordinal, 2, "the side branch doesn't hold main's second batch");
    let items: Value = serde_json::from_slice(&store.get_state_on(side.id, "log").unwrap().unwrap()).unwrap();
    assert_eq!(items.as_array().unwrap().len(), 4);
    for item in &items.as_array().unwrap()[2..] {
        assert_eq!(item["by"], json!(on_side.record.id.0.to_string()));
        assert_eq!(item["at"], json!(on_side.record.sequence.0));
    }
    assert_eq!(store.get_state_len("log").unwrap(), Some(3), "main is unchanged");
}
