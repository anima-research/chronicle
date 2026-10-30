//! The store's batched calls: `append_many_to_state` (a batch of items as
//! one record, and the ordinal its first item took),
//! `get_state_items_by_ordinal` (several items of a state in one read) and
//! `get_records` (several records by ID in one call).
//!
//! Each batched call must answer what the single calls it stands for would:
//! the batch's items sit at the ordinals it reports, a batched item read
//! returns the bytes `get_state_item` returns for each ordinal, and a
//! batched record read returns what `get_record` returns for each ID.

use chronicle::{
    BranchId, RecordId, RecordInput, Sequence, StateOperation, StateRegistration, StateStrategy,
    StateUpdateRecord, Store, StoreConfig, StoreError, StoreEvent, SubscriptionConfig,
    SubscriptionFilter,
};
use serde_json::{json, Value};
use std::path::Path;
use std::sync::Arc;
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

fn bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).unwrap()
}

fn append(store: &Store, id: &str, value: &Value) {
    store
        .update_state(id, StateOperation::Append(bytes(value)))
        .unwrap();
}

fn item(store: &Store, id: &str, ordinal: usize) -> Option<Value> {
    store
        .get_state_item(id, ordinal)
        .unwrap()
        .map(|b| serde_json::from_slice(&b).unwrap())
}

fn whole(store: &Store, id: &str) -> Value {
    store
        .get_state(id)
        .unwrap()
        .map(|b| serde_json::from_slice(&b).unwrap())
        .unwrap_or(Value::Null)
}

/// The items a recorded batch holds, as JSON values.
fn batch_items(store: &Store, id: RecordId) -> (Sequence, Vec<Value>) {
    let record = store.get_record(id).unwrap().expect("the batch's record");
    let update = StateUpdateRecord::decode(&record).unwrap();
    match update.operation {
        StateOperation::AppendMany(items) => (
            update.global_sequence,
            items
                .iter()
                .map(|b| serde_json::from_slice(b).unwrap())
                .collect(),
        ),
        other => panic!("expected AppendMany, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// append_many_to_state
// ---------------------------------------------------------------------------

/// A batch reports the ordinal its first item took, and its items are
/// readable at that ordinal and the ones after it, wherever single appends,
/// earlier batches and redacts have left the state's length.
#[test]
fn a_batch_reports_the_ordinal_of_its_first_item() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    register(&store, "log", 3, 4);
    let mut model: Vec<Value> = Vec::new();

    let first = vec![json!({"n": 0}), json!({"n": 1})];
    let a = store.append_many_to_state_json("log", &first).unwrap();
    assert_eq!(a.first_ordinal, 0, "a state with no value starts at 0");
    model.extend(first.iter().cloned());

    append(&store, "log", &json!({"n": 2}));
    model.push(json!({"n": 2}));

    let second = vec![json!({"n": 3}), json!({"n": 4}), json!({"n": 5})];
    let b = store.append_many_to_state_json("log", &second).unwrap();
    assert_eq!(b.first_ordinal, 3);
    model.extend(second.iter().cloned());
    for (i, value) in second.iter().enumerate() {
        assert_eq!(
            item(&store, "log", b.first_ordinal + i).as_ref(),
            Some(value)
        );
    }

    // The returned record is the one record holding the whole batch.
    let (sequence, recorded) = batch_items(&store, b.record.id);
    assert_eq!(sequence, b.record.sequence);
    assert_eq!(recorded, second);
    assert_eq!(b.record.record_type, "state_update");

    // A redact shortens the count the next batch starts from.
    store
        .update_state("log", StateOperation::Redact { start: 1, end: 3 })
        .unwrap();
    model.drain(1..3);
    let third = vec![json!({"n": 6}), json!({"n": 7})];
    let c = store
        .append_many_to_state("log", third.iter().map(bytes).collect())
        .unwrap();
    assert_eq!(c.first_ordinal, 4);
    model.extend(third.iter().cloned());

    assert_eq!(store.get_state_len("log").unwrap(), Some(model.len()));
    assert_eq!(whole(&store, "log"), json!(model));
    // Five writes, and the store's own snapshots among them.
    assert!(
        store.current_branch().head.0 > 5,
        "the workload crossed the snapshot cadence"
    );
}

/// The ordinal is counted on the branch the batch is written to: a branch
/// that has diverged reports its own length, and the other branch is not
/// moved.
#[test]
fn a_batch_counts_its_ordinal_on_its_own_branch() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    register(&store, "log", 100, 100);
    let main: BranchId = store.current_branch().id;
    store
        .append_many_to_state_json("log", &[json!(0), json!(1), json!(2)])
        .unwrap();
    let side = store.create_branch("side", None).unwrap();

    let on_main = store
        .append_many_to_state_json_on(main, "log", &[json!(3), json!(4)])
        .unwrap();
    assert_eq!(on_main.first_ordinal, 3);
    let on_side = store
        .append_many_to_state_json_on(side.id, "log", &[json!("s")])
        .unwrap();
    assert_eq!(
        on_side.first_ordinal, 3,
        "the side branch still holds three items"
    );
    assert_eq!(on_side.record.branch, side.id);

    assert_eq!(store.get_state_len_on(main, "log").unwrap(), Some(5));
    assert_eq!(store.get_state_len_on(side.id, "log").unwrap(), Some(4));
    assert_eq!(
        store.get_state_item_on(side.id, "log", 3).unwrap(),
        Some(bytes(&json!("s")))
    );
    assert_eq!(
        store.current_branch().id,
        main,
        "an explicit branch never moves the cursor"
    );
}

/// An empty batch is refused, whichever form it comes through, and nothing
/// is recorded.
#[test]
fn an_empty_batch_is_refused_and_writes_nothing() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    register(&store, "log", 100, 100);
    store.append_many_to_state_json("log", &[json!(1)]).unwrap();
    let head = store.current_branch().head;
    let records = store.stats().unwrap().record_count;

    let results = [
        store.append_many_to_state("log", Vec::new()),
        store.append_many_to_state_json("log", &[]),
    ];
    for result in results {
        match result {
            Err(StoreError::InvalidOperation(message)) => {
                assert!(
                    message.contains("AppendMany needs at least one item"),
                    "{message}"
                )
            }
            other => panic!("expected InvalidOperation, got {other:?}"),
        }
    }
    assert_eq!(store.current_branch().head, head);
    assert_eq!(store.stats().unwrap().record_count, records);
    assert_eq!(whole(&store, "log"), json!([1]));
}

/// Writers racing on one state each get the ordinals their own items took:
/// the count is read under the same lock as the write, so no other batch or
/// single append can land between them.
///
/// The cadence is set past the workload so that no automatic snapshot is
/// written. An automatic snapshot reads the state after the write lock is
/// released, and with several threads writing one state a write can land
/// between that read and the snapshot, which then holds stale content. That
/// corrupts the state with single appends alone, so it is not a property of
/// batches, and it is outside what this test checks.
#[test]
fn racing_batches_report_the_ordinals_their_items_took() {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(Store::create(config(dir.path())).unwrap());
    register(&store, "log", 1000, 1000);

    let batchers: Vec<_> = (0..4u64)
        .map(|t| {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                let mut written = Vec::new();
                for i in 0..20u64 {
                    let items: Vec<Value> = (0..=(i % 4))
                        .map(|k| json!({ "t": t, "i": i, "k": k }))
                        .collect();
                    let batch = store.append_many_to_state_json("log", &items).unwrap();
                    written.push((batch.first_ordinal, items));
                }
                written
            })
        })
        .collect();
    let single = {
        let store = Arc::clone(&store);
        std::thread::spawn(move || {
            for i in 0..30u64 {
                append(&store, "log", &json!({ "single": i }));
            }
        })
    };
    let written: Vec<(usize, Vec<Value>)> = batchers
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    single.join().unwrap();

    let mut taken = vec![false; store.get_state_len("log").unwrap().unwrap()];
    for (first, items) in &written {
        for (k, value) in items.iter().enumerate() {
            assert!(!taken[first + k], "ordinal {} reported twice", first + k);
            taken[first + k] = true;
            assert_eq!(item(&store, "log", first + k).as_ref(), Some(value));
        }
    }
    assert_eq!(
        taken.iter().filter(|t| !**t).count(),
        30,
        "only the single appends are left"
    );
}

/// What a batch reported still holds after a reopen: each item at its
/// ordinal, and each record holding its batch at its sequence. (The batch
/// form of `test_append_with_identity_round_trip_after_reopen`.)
#[test]
fn batches_read_back_at_their_ordinals_after_a_reopen() {
    let dir = TempDir::new().unwrap();
    let mut expected = Vec::new();
    {
        let store = Store::create(config(dir.path())).unwrap();
        register(&store, "msgs", 3, 4);
        for i in 0..9u64 {
            if i % 3 == 2 {
                append(&store, "msgs", &json!({ "single": i }));
                continue;
            }
            let items: Vec<Value> = (0..=i % 3).map(|k| json!({ "batch": i, "k": k })).collect();
            let batch = store.append_many_to_state_json("msgs", &items).unwrap();
            expected.push((
                batch.record.id,
                batch.record.sequence,
                batch.first_ordinal,
                items,
            ));
        }
    }

    let store = Store::open(StoreConfig {
        create_if_missing: false,
        ..config(dir.path())
    })
    .unwrap();
    for (id, sequence, first, items) in &expected {
        for (k, value) in items.iter().enumerate() {
            assert_eq!(item(&store, "msgs", first + k).as_ref(), Some(value));
        }
        assert_eq!(batch_items(&store, *id), (*sequence, items.clone()));
    }
}

/// Batches and single appends interleaved across several states, each with
/// a short snapshot cadence: every batch's ordinal is its state's length at
/// the time, and every state ends holding its own items in order. (The
/// batch form of `test_interleaved_multi_state_updates`.)
#[test]
fn interleaved_batches_across_states_keep_their_ordinals() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    let names = ["context", "discord", "minecraft", "agent_state"];
    for name in names {
        register(&store, name, 4, 3);
    }
    let mut models: Vec<Vec<Value>> = vec![Vec::new(); names.len()];

    for i in 1..=100usize {
        let s = i % names.len();
        if i % 5 == 0 {
            let value = json!(format!("event_{i}"));
            append(&store, names[s], &value);
            models[s].push(value);
        } else {
            let items: Vec<Value> = (0..(i % 3) + 1)
                .map(|k| json!(format!("event_{i}_{k}")))
                .collect();
            let batch = store.append_many_to_state_json(names[s], &items).unwrap();
            assert_eq!(
                batch.first_ordinal,
                models[s].len(),
                "{} at step {i}",
                names[s]
            );
            models[s].extend(items);
        }
    }
    for (s, name) in names.iter().enumerate() {
        assert_eq!(whole(&store, name), json!(models[s]), "{name}");
        assert_eq!(store.get_state_len(name).unwrap(), Some(models[s].len()));
    }
}

// ---------------------------------------------------------------------------
// get_state_items_by_ordinal
// ---------------------------------------------------------------------------

/// The ordinals to probe a state of `len` items with: in and out of range,
/// repeated, out of order, the last item twice.
fn probes(len: usize) -> Vec<usize> {
    let last = len.saturating_sub(1);
    vec![last, 0, len / 2, last, len, len + 7, 1, usize::MAX, len / 3]
}

/// A batched item read answers, for each ordinal, exactly the bytes a
/// single `get_state_item` gives, and reads the state once: after every
/// kind of write (singles, batches, edits, redacts, the store's own
/// snapshots), from a cold cache, from a warm one, and from the head record
/// when only the last item is asked for.
#[test]
fn batched_item_reads_equal_single_reads_byte_for_byte() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    register(&store, "log", 3, 4);
    let mut model: Vec<Value> = Vec::new();

    let check = |model: &[Value], step: &str| {
        let ordinals = probes(model.len());
        // Each single read from a cold cache, as a caller with no warm
        // state would make it.
        let singles: Vec<Option<Vec<u8>>> = ordinals
            .iter()
            .map(|&i| {
                store.clear_state_caches();
                store.get_state_item("log", i).unwrap()
            })
            .collect();
        let expected: Vec<Option<Vec<u8>>> =
            ordinals.iter().map(|&i| model.get(i).map(bytes)).collect();
        assert_eq!(singles, expected, "{step}: single reads against the model");

        store.clear_state_caches();
        let cold = store.state_reconstruction_count();
        assert_eq!(
            store.get_state_items_by_ordinal("log", &ordinals).unwrap(),
            singles,
            "{step}: cold"
        );
        assert!(
            store.state_reconstruction_count() <= cold + 1,
            "{step}: one materialization at most"
        );

        let warm = store.state_reconstruction_count();
        assert_eq!(
            store.get_state_items_by_ordinal("log", &ordinals).unwrap(),
            singles,
            "{step}: warm"
        );
        assert_eq!(
            store.state_reconstruction_count(),
            warm,
            "{step}: the warm cache answers"
        );
    };

    for round in 0..6usize {
        let items: Vec<Value> = (0..round + 1)
            .map(|k| json!({ "r": round, "k": k }))
            .collect();
        store.append_many_to_state_json("log", &items).unwrap();
        model.extend(items);
        check(&model, &format!("round {round}: batch"));

        let value = json!({ "r": round, "single": true });
        append(&store, "log", &value);
        model.push(value);
        check(&model, &format!("round {round}: single"));

        if round % 2 == 1 {
            let edited = json!({ "r": round, "edited": [1, 2] });
            store
                .update_state(
                    "log",
                    StateOperation::Edit {
                        index: 1,
                        new_value: bytes(&edited),
                    },
                )
                .unwrap();
            model[1] = edited;
            check(&model, &format!("round {round}: edit"));
        }
        if round % 3 == 2 {
            store
                .update_state("log", StateOperation::Redact { start: 2, end: 4 })
                .unwrap();
            model.drain(2..4);
            check(&model, &format!("round {round}: redact"));
        }
    }
}

/// Asked only for the last item, with the cache cold and a batch at the
/// head, a batched read is served from the head record as a single read
/// is, without materializing the state.
#[test]
fn the_last_item_alone_is_served_from_the_head_record() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    register(&store, "log", 1000, 1000);
    let items: Vec<Value> = (0..30).map(|i| json!({ "i": i })).collect();
    store.append_many_to_state_json("log", &items).unwrap();

    store.clear_state_caches();
    let before = store.state_reconstruction_count();
    assert_eq!(
        store.get_state_items_by_ordinal("log", &[29, 29]).unwrap(),
        vec![Some(bytes(&items[29])), Some(bytes(&items[29]))]
    );
    assert_eq!(
        store.state_reconstruction_count(),
        before,
        "no materialization"
    );

    // Any other ordinal in the request needs the whole state, once.
    assert_eq!(
        store.get_state_items_by_ordinal("log", &[29, 3]).unwrap(),
        vec![Some(bytes(&items[29])), Some(bytes(&items[3]))]
    );
    assert_eq!(store.state_reconstruction_count(), before + 1);
}

/// A batched read follows the branch it is given, answers `None` for every
/// ordinal of a state with no value, and an empty request answers an empty
/// list.
#[test]
fn batched_item_reads_on_a_branch_and_on_a_missing_state() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    register(&store, "log", 100, 100);
    register(&store, "never-written", 100, 100);
    let main = store.current_branch().id;
    store
        .append_many_to_state_json("log", &[json!("a"), json!("b")])
        .unwrap();
    let side = store.create_branch("side", None).unwrap();
    store
        .append_many_to_state_json_on(side.id, "log", &[json!("side")])
        .unwrap();
    store
        .append_many_to_state_json_on(main, "log", &[json!("main")])
        .unwrap();

    assert_eq!(
        store
            .get_state_items_by_ordinal_on(side.id, "log", &[2, 0])
            .unwrap(),
        vec![Some(bytes(&json!("side"))), Some(bytes(&json!("a")))]
    );
    assert_eq!(
        store
            .get_state_items_by_ordinal_on(main, "log", &[2])
            .unwrap(),
        vec![Some(bytes(&json!("main")))]
    );
    assert_eq!(
        store
            .get_state_items_by_ordinal("never-written", &[0, 1])
            .unwrap(),
        vec![None, None]
    );
    assert_eq!(
        store
            .get_state_items_by_ordinal("not-registered", &[0])
            .unwrap(),
        vec![None]
    );
    assert!(store
        .get_state_items_by_ordinal("log", &[])
        .unwrap()
        .is_empty());
}

/// A state whose value isn't an array has no item at any ordinal: a single
/// read and a batched one both answer `None`, for every ordinal and for a
/// request that mixes ordinals the head's item count calls in range with
/// ones past it. `get_state_slice` still refuses such a state.
#[test]
fn a_state_whose_value_is_not_an_array_has_no_items() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    for id in ["obj", "text"] {
        store
            .register_state(StateRegistration {
                id: id.to_string(),
                strategy: StateStrategy::Snapshot,
                initial_value: None,
            })
            .unwrap();
    }
    store
        .update_state("obj", StateOperation::Set(bytes(&json!({ "a": 1 }))))
        .unwrap();
    store
        .update_state("text", StateOperation::Set(bytes(&json!("not a list"))))
        .unwrap();
    // An append-log state replaced by an object: the head keeps the count
    // of three items it had, so ordinal 2 looks like the last item.
    register(&store, "log", 100, 100);
    store
        .append_many_to_state_json("log", &[json!(0), json!(1), json!(2)])
        .unwrap();
    store
        .update_state("log", StateOperation::Set(bytes(&json!({ "b": 2 }))))
        .unwrap();
    assert_eq!(store.get_state_len("log").unwrap(), Some(3));

    let ordinals = [0, 1, 2, 3, usize::MAX, 0];
    for id in ["obj", "text", "log"] {
        for cold in [true, false] {
            if cold {
                store.clear_state_caches();
            }
            for &i in &ordinals {
                assert_eq!(store.get_state_item(id, i).unwrap(), None, "{id}[{i}]");
            }
            assert_eq!(
                store.get_state_items_by_ordinal(id, &ordinals).unwrap(),
                vec![None; ordinals.len()],
                "{id}, cold: {cold}"
            );
            assert!(store
                .get_state_items_by_ordinal(id, &[])
                .unwrap()
                .is_empty());
        }
        assert!(
            matches!(
                store.get_state_slice(id, 0, 10),
                Err(StoreError::Deserialization(_))
            ),
            "{id}: the slice still refuses a value that isn't an array"
        );
    }
}

/// An empty request answers an empty list without reading the state, so a
/// cold state isn't materialized to answer nothing.
#[test]
fn an_empty_request_reads_nothing() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    register(&store, "log", 100, 100);
    let items: Vec<Value> = (0..20).map(|i| json!({ "i": i })).collect();
    store.append_many_to_state_json("log", &items).unwrap();
    append(&store, "log", &json!("last"));

    store.clear_state_caches();
    let before = store.state_reconstruction_count();
    assert!(store
        .get_state_items_by_ordinal("log", &[])
        .unwrap()
        .is_empty());
    assert_eq!(
        store.state_reconstruction_count(),
        before,
        "an empty request materialized the state"
    );

    // A request mixing ordinals in range with ones past the end answers
    // each as the single read does, from one materialization.
    let mixed = [20, 21, 0, usize::MAX, 7, 22, 20];
    let singles: Vec<Option<Vec<u8>>> = mixed
        .iter()
        .map(|&i| store.get_state_item("log", i).unwrap())
        .collect();
    assert_eq!(
        singles,
        vec![
            Some(bytes(&json!("last"))),
            None,
            Some(bytes(&items[0])),
            None,
            Some(bytes(&items[7])),
            None,
            Some(bytes(&json!("last"))),
        ]
    );
    store.clear_state_caches();
    let before = store.state_reconstruction_count();
    assert_eq!(
        store.get_state_items_by_ordinal("log", &mixed).unwrap(),
        singles
    );
    assert_eq!(store.state_reconstruction_count(), before + 1);
}

// ---------------------------------------------------------------------------
// get_records
// ---------------------------------------------------------------------------

/// `get_records` answers, for each ID in the order given, what `get_record`
/// answers: the record, or `None` for an ID with no record. Repeats are
/// answered each time, and records of every kind (plain and state updates)
/// come back whole.
#[test]
fn get_records_answers_each_id_in_order() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    register(&store, "log", 100, 100);
    let a = store
        .append(RecordInput::json("note", &json!({ "n": 1 })).unwrap())
        .unwrap();
    let b = store
        .append_many_to_state_json("log", &[json!(1), json!(2)])
        .unwrap()
        .record;
    let c = store
        .append(RecordInput::raw("blob-ish", vec![0, 1, 2, 255]))
        .unwrap();
    let unknown = RecordId(c.id.0 + 1000);

    let ids = [c.id, unknown, a.id, b.id, c.id];
    let batched = store.get_records(&ids).unwrap();
    assert_eq!(batched.len(), ids.len());
    for (id, got) in ids.iter().zip(&batched) {
        let single = store.get_record(*id).unwrap();
        assert_eq!(got.is_some(), single.is_some(), "{id:?}");
        if let (Some(got), Some(single)) = (got, single) {
            assert_eq!(got.id, *id);
            assert_eq!(
                (
                    got.sequence,
                    got.branch,
                    &got.record_type,
                    &got.payload,
                    got.timestamp.0
                ),
                (
                    single.sequence,
                    single.branch,
                    &single.record_type,
                    &single.payload,
                    single.timestamp.0
                )
            );
        }
    }
    assert!(
        batched[1].is_none(),
        "an unknown ID answers None in its place"
    );
    assert_eq!(batched[0].as_ref().unwrap().payload, vec![0, 1, 2, 255]);
    assert_eq!(
        batch_items(&store, batched[3].as_ref().unwrap().id).1,
        vec![json!(1), json!(2)]
    );
    assert!(store.get_records(&[]).unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// state-delta events
// ---------------------------------------------------------------------------

/// A batch reaches a state subscriber as one `StateDelta` carrying every
/// item, at the batch's sequence; in JSON the items are integer arrays, as
/// an `Append`'s item is.
#[test]
fn a_state_delta_event_carries_the_whole_batch() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    register(&store, "log", 100, 100);
    let handle = store.subscribe(SubscriptionConfig {
        filter: SubscriptionFilter::states(vec!["log".to_string()]),
        ..Default::default()
    });
    store.mark_subscription_caught_up(handle.id).unwrap();

    let items = vec![json!({ "id": "1" }), json!({ "id": "2" }), json!("three")];
    let batch = store.append_many_to_state_json("log", &items).unwrap();

    let mut deltas = Vec::new();
    while let Ok(event) = handle.try_recv() {
        if let StoreEvent::StateDelta { .. } = event {
            deltas.push(event);
        }
    }
    assert_eq!(deltas.len(), 1, "one event for the whole batch");
    let event = serde_json::to_value(&deltas[0]).unwrap();
    match &deltas[0] {
        StoreEvent::StateDelta {
            state_id,
            operation,
            sequence,
        } => {
            assert_eq!(state_id, "log");
            assert_eq!(*sequence, batch.record.sequence);
            match operation {
                StateOperation::AppendMany(carried) => {
                    assert_eq!(*carried, items.iter().map(bytes).collect::<Vec<_>>())
                }
                other => panic!("expected AppendMany, got {other:?}"),
            }
        }
        _ => unreachable!(),
    }
    assert_eq!(event["type"], "state_delta");
    let carried: Vec<Value> = event["operation"]["AppendMany"]
        .as_array()
        .expect("the operation keyed AppendMany")
        .iter()
        .map(|item| {
            let raw: Vec<u8> = serde_json::from_value(item.clone()).expect("an integer array");
            serde_json::from_slice(&raw).unwrap()
        })
        .collect();
    assert_eq!(carried, items);
    store.unsubscribe(handle.id);
}
