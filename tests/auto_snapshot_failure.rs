//! A write whose own auto-snapshot fails still returns its record (issue
//! #39).
//!
//! The store writes the record, moves the state's head and the branch head,
//! and only then takes the snapshot the write's thresholds call for. An
//! error from that snapshot used to come back from the write, so a caller
//! took a recorded write for one that didn't happen, and one that retried a
//! write that isn't idempotent made it twice. The failure is now logged, the
//! write returns its record, and the snapshot stays due for the next write
//! to the state.
//!
//! An item that isn't JSON stands in for the failure, as it does in the
//! reviewers' probe: the store records it as given, and the snapshot that
//! reads it back fails, as one on a full disk would fail its write.

use chronicle::{
    Record, Sequence, StateOperation, StateRegistration, StateStrategy, StateUpdateRecord, Store,
    StoreConfig, StoreError,
};
use serde_json::{json, Value};
use std::path::Path;
use tempfile::TempDir;

/// What `update_state` logs when the snapshot after a recorded write fails.
const WARNING: &str = "the snapshot after a recorded state write failed";

/// An Append the store records as given, and every snapshot that reads it
/// fails on.
const POISON: &[u8] = b"not json";

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

/// Collects what the test's tracing subscriber writes.
#[derive(Clone, Default)]
struct Logged(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for Logged {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Runs `f` under a subscriber of its own and returns what it logged.
fn logged<T>(f: impl FnOnce() -> T) -> (T, String) {
    let logged = Logged::default();
    let writer = logged.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .finish();
    let out = tracing::subscriber::with_default(subscriber, f);
    let text = String::from_utf8(logged.0.lock().unwrap().clone()).unwrap();
    (out, text)
}

fn head(store: &Store) -> Sequence {
    store.current_branch().head
}

fn len(store: &Store, state_id: &str) -> usize {
    store.get_state_len(state_id).unwrap().unwrap()
}

fn operation(store: &Store, record: &Record) -> StateOperation {
    StateUpdateRecord::decode(&store.get_record(record.id).unwrap().unwrap())
        .unwrap()
        .operation
}

/// The write forms that take a snapshot after their record: `update_state`
/// itself, which `appendToState` and every other single write goes
/// through, the single identity append, and the two batched appends.
#[derive(Clone, Copy, Debug)]
enum Form {
    Append,
    Identity,
    Batch,
    IdentityBatch,
}

const FORMS: [Form; 4] = [
    Form::Append,
    Form::Identity,
    Form::Batch,
    Form::IdentityBatch,
];

/// Writes `items` to `state_id` in `form`, returning the record (the one
/// item a single form takes is the first).
fn write(store: &Store, form: Form, state_id: &str, items: &[Value]) -> chronicle::Result<Record> {
    match form {
        Form::Append => store.update_state(
            state_id,
            StateOperation::Append(serde_json::to_vec(&items[0]).unwrap()),
        ),
        Form::Identity => {
            store.append_to_state_json_with_identity(state_id, items[0].clone(), "by", "at")
        }
        Form::Batch => store
            .append_many_to_state_json(state_id, items)
            .map(|b| b.record),
        Form::IdentityBatch => store
            .append_many_to_state_json_with_identity(state_id, items, "by", "at")
            .map(|b| b.record),
    }
}

fn written(form: Form, items: &[Value]) -> usize {
    match form {
        Form::Append | Form::Identity => 1,
        Form::Batch | Form::IdentityBatch => items.len(),
    }
}

#[test]
fn each_write_form_returns_its_record_when_its_own_snapshot_fails() {
    for form in FORMS {
        let dir = TempDir::new().unwrap();
        let items = [json!({"id": "a"}), json!({"id": "b"})];
        let (record, after) = {
            let store = Store::create(config(dir.path())).unwrap();
            // A delta every 2 operations: the write after the poison is the
            // second, so its snapshot is due, and it fails reading the poison.
            register(&store, "s", 2, 10);
            store
                .update_state("s", StateOperation::Append(POISON.to_vec()))
                .unwrap();
            let before = head(&store);
            assert_eq!(len(&store, "s"), 1);

            let (result, log) = logged(|| write(&store, form, "s", &items));
            let record = result.unwrap_or_else(|e| {
                panic!("{form:?}: the write is recorded, so it returns its record, got {e}")
            });
            assert_eq!(record.sequence, before.next(), "{form:?}");
            assert_eq!(
                head(&store),
                record.sequence,
                "{form:?}: no snapshot record followed it"
            );
            assert_eq!(len(&store, "s"), 1 + written(form, &items), "{form:?}");
            assert!(log.contains(WARNING), "{form:?}: {log}");
            assert!(log.contains("state_id=\"s\""), "{form:?}: {log}");
            assert!(log.contains("branch=main"), "{form:?}: {log}");
            assert!(
                log.contains("Deserialization"),
                "{form:?}: the cause is logged: {log}"
            );

            // The record is the write, stamped with itself where the form
            // stamps.
            match (form, operation(&store, &record)) {
                (Form::Append, StateOperation::Append(bytes)) => {
                    assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), items[0])
                }
                (Form::Identity, StateOperation::Append(bytes)) => assert_eq!(
                    serde_json::from_slice::<Value>(&bytes).unwrap(),
                    json!({"id": "a", "by": record.id.0.to_string(), "at": record.sequence.0})
                ),
                (Form::Batch, StateOperation::AppendMany(stored)) => assert_eq!(stored.len(), 2),
                (Form::IdentityBatch, StateOperation::AppendMany(stored)) => {
                    for (bytes, item) in stored.iter().zip(&items) {
                        let mut stamped = item.clone();
                        stamped["by"] = json!(record.id.0.to_string());
                        stamped["at"] = json!(record.sequence.0);
                        assert_eq!(serde_json::from_slice::<Value>(bytes).unwrap(), stamped);
                    }
                }
                (form, other) => panic!("{form:?} recorded {other:?}"),
            }

            // The snapshot stays due: the next write tries it again, fails
            // the same way on the poison, and still returns.
            let (next, log) = logged(|| write(&store, form, "s", &[json!({"id": "c"})]));
            let next = next.unwrap();
            assert_eq!(next.sequence, record.sequence.next(), "{form:?}");
            assert_eq!(head(&store), next.sequence, "{form:?}");
            assert!(
                log.contains(WARNING),
                "{form:?}: the next write tried again: {log}"
            );

            // Asked for directly, the snapshot's error is the caller's.
            assert!(
                matches!(
                    store.create_snapshot_if_needed("s"),
                    Err(StoreError::Deserialization(_))
                ),
                "{form:?}"
            );
            assert_eq!(head(&store), next.sequence, "{form:?}");
            (record, (next.sequence, len(&store, "s")))
        };

        // What the writes recorded survives a reopen, and the reopened store
        // still finds the snapshot due.
        let store = Store::open(config(dir.path())).unwrap();
        assert_eq!((head(&store), len(&store, "s")), after, "{form:?}");
        assert_eq!(
            store.get_record(record.id).unwrap().unwrap().sequence,
            record.sequence
        );
        let (again, log) = logged(|| write(&store, form, "s", &[json!({"id": "d"})]));
        assert_eq!(again.unwrap().sequence, after.0.next(), "{form:?}");
        assert!(log.contains(WARNING), "{form:?}: {log}");
    }
}

#[test]
fn the_snapshot_a_failure_left_due_is_taken_by_the_next_write_that_can() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(dir.path())).unwrap();
    // A full snapshot every 2 operations (the floor `2 * 1`): each one
    // materializes the whole state, so the poison fails it until a `Set`
    // replaces the value.
    register(&store, "s", 2, 1);
    store
        .update_state("s", StateOperation::Append(POISON.to_vec()))
        .unwrap();

    let items = [json!({"id": "a"}), json!({"id": "b"})];
    let (batch, log) =
        logged(|| store.append_many_to_state_json_with_identity("s", &items, "by", "at"));
    let batch = batch.unwrap();
    assert!(log.contains(WARNING), "{log}");
    assert_eq!(
        head(&store),
        batch.record.sequence,
        "no snapshot was written"
    );
    let stats = store.get_chain_stats("s").unwrap().unwrap();
    assert!(!stats.has_full_snapshot);

    // The cause goes away: a Set replaces the value, so a full snapshot no
    // longer reads the poison. The write that does it takes the snapshot
    // the failure left due, right after its own record.
    let value = json!([{"id": "a"}, {"id": "b"}]);
    let (set, log) = logged(|| {
        store.update_state(
            "s",
            StateOperation::Set(serde_json::to_vec(&value).unwrap()),
        )
    });
    let set = set.unwrap();
    assert!(!log.contains(WARNING), "{log}");
    assert_eq!(
        head(&store),
        set.sequence.next(),
        "the set and the snapshot after it"
    );
    let ids = store.get_records_by_type("state_update");
    let snapshot =
        StateUpdateRecord::decode(&store.get_record(*ids.last().unwrap()).unwrap().unwrap())
            .unwrap();
    match snapshot.operation {
        StateOperation::Snapshot(bytes) => {
            assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), value)
        }
        other => panic!("expected the full snapshot at the head, got {other:?}"),
    }
    assert!(
        store
            .get_chain_stats("s")
            .unwrap()
            .unwrap()
            .has_full_snapshot
    );

    // The snapshot reset the count: the next write is the first since it.
    let after = head(&store);
    let next = store
        .append_many_to_state_json_with_identity("s", &[json!({"id": "c"})], "by", "at")
        .unwrap();
    assert_eq!(next.record.sequence, after.next());
    assert_eq!(head(&store), next.record.sequence, "no snapshot is due yet");
    assert_eq!(
        serde_json::from_slice::<Value>(&store.get_state("s").unwrap().unwrap()).unwrap(),
        json!([
            {"id": "a"},
            {"id": "b"},
            {"id": "c", "by": next.record.id.0.to_string(), "at": next.record.sequence.0},
        ])
    );
}
