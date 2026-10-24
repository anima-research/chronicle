//! A branch made at a past sequence (`create_branch_at`) reads the parent's
//! value at that point, and keeps reading it through the snapshots the
//! branch takes later. An append-log delta snapshot holds only the Appends
//! since the last snapshot, and reconstruction reads it in place of every
//! operation behind it, so no delta may be taken across a Redact, an Edit or
//! a Set. A branch that inherits one from after the parent's last full
//! snapshot holds its deltas off until a full snapshot of its own, as the
//! parent does.

use chronicle::{
    SnapshotNeeded, StateOperation, StateRegistration, StateStrategy, Store, StoreConfig,
};
use serde_json::{json, Value};
use tempfile::TempDir;

fn config(dir: &TempDir, create: bool) -> StoreConfig {
    StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: create,
    }
}

/// A store whose state "s" takes a delta snapshot every two operations.
fn store(dir: &TempDir) -> Store {
    let store = Store::create(config(dir, true)).unwrap();
    store
        .register_state(StateRegistration {
            id: "s".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 2,
                full_snapshot_every: 100,
            },
            initial_value: None,
        })
        .unwrap();
    store
}

fn op(store: &Store, operation: StateOperation) {
    store.update_state("s", operation).unwrap();
}

fn append(store: &Store, v: Value) {
    op(store, StateOperation::Append(serde_json::to_vec(&v).unwrap()));
}

fn value(store: &Store) -> Vec<Value> {
    serde_json::from_slice(&store.get_state("s").unwrap().unwrap()).unwrap()
}

/// Branch "child" from main at `at`, and switch to it.
fn fork_at(store: &Store, at: chronicle::Sequence) {
    store.create_branch_at("child", "main", at).unwrap();
    store.switch_branch("child").unwrap();
}

/// Branch "child" from main at main's head, and switch to it.
fn fork(store: &Store) {
    fork_at(store, store.current_branch().head);
}

/// Append 0 and 1, and take their delta snapshot: auto-snapshots take it
/// themselves, and with them off this does.
fn append_0_and_1_and_their_delta(store: &Store) {
    append(store, json!(0));
    append(store, json!(1));
    if !store.auto_snapshot_enabled() {
        assert!(store.create_snapshot_if_needed("s").unwrap().is_some(), "setup: a delta");
    }
}

/// The changes a delta can't carry, each made after `append_0_and_1_and_their_delta`,
/// with the value the state then holds.
fn changes() -> Vec<(&'static str, fn(&Store), Vec<Value>)> {
    vec![
        ("Redact", |store| {
            op(store, StateOperation::Redact { start: 1, end: 100 });
            // An Append after the Redact, so the Redact isn't the branch's
            // own head.
            append(store, json!(5));
        }, vec![json!(0), json!(5)]),
        ("Edit", |store| {
            op(
                store,
                StateOperation::Edit { index: 0, new_value: serde_json::to_vec(&json!("edited")).unwrap() },
            );
        }, vec![json!("edited"), json!(1)]),
        ("Set", |store| {
            // An Append between the delta and the Set: a delta walking past
            // the Set would carry it back after the Set's value.
            append(store, json!(7));
            op(store, StateOperation::Set(serde_json::to_vec(&json!([9])).unwrap()));
        }, vec![json!(9)]),
    ]
}

/// Each change, made on main and then branched from main's head: the branch
/// reads main's value, through the point its first delta would come due,
/// then through a full snapshot of its own and the delta after it.
#[test]
fn a_fork_keeps_each_change_from_before_its_point() {
    for (name, change, before) in changes() {
        let dir = TempDir::new().unwrap();
        let store = store(&dir);
        append_0_and_1_and_their_delta(&store);
        change(&store);
        fork(&store);
        assert_eq!(value(&store), before, "{name}: setup: the fork starts as main was");
        let mut expected = before.clone();
        for v in [2, 3] {
            append(&store, json!(v));
            expected.push(json!(v));
        }
        assert_eq!(value(&store), expected, "{name}");
        assert_eq!(store.get_state_len("s").unwrap(), Some(expected.len()), "{name}");
        store.compact_state("s").unwrap();
        for v in [4, 5] {
            append(&store, json!(v));
            expected.push(json!(v));
        }
        let stats = store.get_compaction_stats("s").unwrap();
        assert_eq!(stats.delta_snapshots_since_full, 1, "{name}: setup: a delta after the branch's full");
        assert_eq!(value(&store), expected, "{name}");
        assert_eq!(store.get_state_len("s").unwrap(), Some(expected.len()), "{name}");
    }
}

/// Without auto-snapshots, `snapshot_needed` once two more Appends are made
/// after `setup` shows whether deltas are held off.
fn due_after(setup: impl FnOnce(&Store)) -> Option<SnapshotNeeded> {
    let dir = TempDir::new().unwrap();
    let store = store(&dir);
    store.set_auto_snapshot(false);
    setup(&store);
    append(&store, json!(2));
    append(&store, json!(3));
    store.snapshot_needed("s")
}

#[test]
fn each_change_holds_its_states_deltas_off() {
    for (name, change, _) in changes() {
        let due = due_after(|store| {
            append_0_and_1_and_their_delta(store);
            change(store);
        });
        // No delta, and no early full either: the raw tail grows until the
        // size-aware full.
        assert_eq!(due, None, "{name}");
    }
}

#[test]
fn a_fork_inherits_its_parents_hold_on_deltas_for_each_change() {
    for (name, change, _) in changes() {
        let due = due_after(|store| {
            append_0_and_1_and_their_delta(store);
            change(store);
            fork(store);
        });
        assert_eq!(due, None, "{name}");
    }
}

#[test]
fn a_fork_keeps_its_hold_on_deltas_across_a_reopen() {
    let dir = TempDir::new().unwrap();
    let store = store(&dir);
    store.set_auto_snapshot(false);
    append_0_and_1_and_their_delta(&store);
    // The Redact is the branch's own head here.
    op(&store, StateOperation::Redact { start: 1, end: 100 });
    fork(&store);
    store.sync().unwrap();
    drop(store);
    let store = Store::open(config(&dir, false)).unwrap();
    store.set_auto_snapshot(false);
    store.switch_branch("child").unwrap();
    append(&store, json!(2));
    append(&store, json!(3));
    assert_eq!(store.snapshot_needed("s"), None, "the inherited hold was saved with the branch");
    assert_eq!(value(&store), vec![json!(0), json!(2), json!(3)]);
}

#[test]
fn a_fork_at_a_past_point_inherits_the_hold_its_parent_had_there() {
    let due = due_after(|store| {
        append_0_and_1_and_their_delta(store);
        op(store, StateOperation::Redact { start: 1, end: 100 });
        let at = store.current_branch().head;
        // Main's own hold ends with a full snapshot after the branch's point.
        store.compact_state("s").unwrap();
        append(store, json!(6));
        assert_eq!(store.snapshot_needed("s"), None, "setup: main's count since its full is low");
        fork_at(store, at);
    });
    assert_eq!(due, None);
}

#[test]
fn a_change_on_the_parent_after_the_fork_point_doesnt_hold_the_fork_off() {
    let due = due_after(|store| {
        append_0_and_1_and_their_delta(store);
        append(store, json!(5));
        let at = store.current_branch().head;
        op(store, StateOperation::Redact { start: 0, end: 1 });
        fork_at(store, at);
    });
    assert_eq!(due, Some(SnapshotNeeded::Delta));
}

#[test]
fn a_fork_of_an_append_only_stretch_still_takes_deltas() {
    let due = due_after(|store| {
        append_0_and_1_and_their_delta(store);
        append(store, json!(5));
        fork(store);
    });
    assert_eq!(due, Some(SnapshotNeeded::Delta));
}

#[test]
fn a_change_behind_the_last_full_snapshot_doesnt_hold_a_forks_deltas_off() {
    let due = due_after(|store| {
        append_0_and_1_and_their_delta(store);
        op(store, StateOperation::Redact { start: 0, end: 1 });
        // A full snapshot behind the fork point consolidates the Redact.
        store.compact_state("s").unwrap();
        append(store, json!(5));
        fork(store);
    });
    assert_eq!(due, Some(SnapshotNeeded::Delta));
}
