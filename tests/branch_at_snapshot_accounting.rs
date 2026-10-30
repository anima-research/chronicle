//! Regression tests: a branch created with `create_branch_at` must carry the
//! snapshot accounting the parent's chain had at the branch point.
//!
//! The historical head used to be built with zeroed accounting
//! (`has_non_append_since_snapshot = false`, `ops_since_delta_snapshot = 0`).
//! When the inherited chain held an Edit / Redact / Set that no full snapshot
//! covered yet, the child's next threshold snapshot was therefore allowed to be
//! an append-only DELTA. `compute_delta_items` only collects Appends, so the
//! delta walked past the inherited mutation and the child's next read lost it
//! (Edit: the pre-edit value came back; Redact: the removed items came back;
//! Set: appends the Set had replaced were resurrected).
//!
//! Each test runs the same script against a `create_branch_at` child and an
//! ordinary `create_branch` child (the control, which copies the parent's head
//! verbatim and was never affected).
//!
//! These tests use only the cursor API so they compile against the unfixed
//! code; they fail there on the `create_branch_at` child.

use chronicle::{
    CompactionStats, Sequence, StateOperation, StateRegistration, StateStrategy, Store, StoreConfig,
};
use serde_json::{json, Value};
use tempfile::TempDir;

const DELTA_EVERY: u64 = 5;

fn test_store(dir: &TempDir) -> Store {
    Store::create(StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: true,
    })
    .unwrap()
}

fn open_store(dir: &TempDir) -> Store {
    Store::open(StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: false,
    })
    .unwrap()
}

fn register(store: &Store) {
    store
        .register_state(StateRegistration {
            id: "messages".to_string(),
            // Full interval floor = 5 * 100 = 500 ops, far beyond anything
            // these tests write, so only DELTA snapshots can trigger.
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: DELTA_EVERY,
                full_snapshot_every: 100,
            },
            initial_value: None,
        })
        .unwrap();
}

fn append(store: &Store, v: Value) {
    store
        .update_state("messages", StateOperation::Append(serde_json::to_vec(&v).unwrap()))
        .unwrap();
}

fn read(store: &Store) -> Vec<Value> {
    let bytes = store.get_state("messages").unwrap().unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

/// Seed three items and cover them with a FULL snapshot.
fn seed_with_full_snapshot(store: &Store) {
    register(store);
    for v in ["a", "b", "c"] {
        append(store, json!(v));
    }
    store.compact_state("messages").unwrap().unwrap();
}

/// Append exactly enough items on the current branch to cross the delta
/// threshold however the branch's accounting started, and prove that a
/// snapshot record of some kind was or was not written is NOT assumed —
/// only the materialized value is asserted by callers.
fn append_past_delta_threshold(store: &Store) -> Vec<Value> {
    let mut appended = Vec::new();
    for i in 0..(DELTA_EVERY * 2) {
        let v = json!(format!("child-{i}"));
        append(store, v.clone());
        appended.push(v);
    }
    appended
}

/// Run `mutate` on main after a full snapshot, fork both kinds of child at
/// main's head, drive each child past the delta threshold, and check both
/// children still read `expected_inherited ++ appended` — in-process and
/// again after a close/reopen (cold reconstruction from the log).
fn run_case(mutate: impl Fn(&Store), expected_inherited: Vec<Value>) {
    run_case_then(mutate, expected_inherited, |_, _, _| {});
}

fn run_case_then(
    mutate: impl Fn(&Store),
    expected_inherited: Vec<Value>,
    after_threshold: impl Fn(&str, &CompactionStats, &CompactionStats),
) {
    let dir = TempDir::new().unwrap();
    let mut expected_by_branch: Vec<(&str, Vec<Value>)> = Vec::new();
    {
        let store = test_store(&dir);
        seed_with_full_snapshot(&store);
        mutate(&store);
        assert_eq!(read(&store), expected_inherited, "parent after mutation");

        let at: Sequence = store.current_branch().head;
        store.create_branch("control", None).unwrap();
        store.create_branch_at("historical", "main", at).unwrap();

        for branch in ["control", "historical"] {
            store.switch_branch(branch).unwrap();
            assert_eq!(read(&store), expected_inherited, "{branch}: inherited value at fork");

            let before = store.get_compaction_stats("messages").unwrap();
            let appended = append_past_delta_threshold(&store);
            after_threshold(branch, &before, &store.get_compaction_stats("messages").unwrap());
            let mut expected = expected_inherited.clone();
            expected.extend(appended);

            assert_eq!(
                read(&store),
                expected,
                "{branch}: inherited mutation lost after the child crossed the delta-snapshot threshold"
            );
            expected_by_branch.push((branch, expected));
        }

        // The parent is untouched by either child.
        store.switch_branch("main").unwrap();
        assert_eq!(read(&store), expected_inherited, "parent after children wrote");
        store.sync().unwrap();
    }

    let store = open_store(&dir);
    for (branch, expected) in expected_by_branch {
        store.switch_branch(branch).unwrap();
        assert_eq!(read(&store), expected, "{branch}: after reopen");
    }
}

#[test]
fn branch_at_keeps_inherited_edit_across_child_delta_snapshot() {
    run_case(
        |store| {
            store
                .update_state(
                    "messages",
                    StateOperation::Edit {
                        index: 1,
                        new_value: serde_json::to_vec(&json!("b-edited")).unwrap(),
                    },
                )
                .unwrap();
            // One append after the edit, so the branch point is strictly
            // after the mutation rather than on it.
            append(store, json!("d"));
        },
        vec![json!("a"), json!("b-edited"), json!("c"), json!("d")],
    );
}

#[test]
fn branch_at_keeps_inherited_edit_when_branch_point_is_the_edit() {
    run_case(
        |store| {
            store
                .update_state(
                    "messages",
                    StateOperation::Edit {
                        index: 0,
                        new_value: serde_json::to_vec(&json!("a-edited")).unwrap(),
                    },
                )
                .unwrap();
        },
        vec![json!("a-edited"), json!("b"), json!("c")],
    );
}

#[test]
fn branch_at_keeps_inherited_redact_across_child_delta_snapshot() {
    run_case(
        |store| {
            append(store, json!("d"));
            store
                .update_state("messages", StateOperation::Redact { start: 1, end: 3 })
                .unwrap();
            append(store, json!("e"));
        },
        vec![json!("a"), json!("d"), json!("e")],
    );
}

#[test]
fn branch_at_keeps_inherited_set_across_child_delta_snapshot() {
    run_case(
        |store| {
            // An append between the full snapshot and the Set: a delta taken
            // after the Set would collect it and resurrect it behind the Set.
            append(store, json!("d"));
            store
                .update_state(
                    "messages",
                    StateOperation::Set(serde_json::to_vec(&json!(["x", "y"])).unwrap()),
                )
                .unwrap();
        },
        vec![json!("x"), json!("y")],
    );
}

fn append_many(store: &Store, values: &[&str]) {
    store
        .update_state(
            "messages",
            StateOperation::AppendMany(values.iter().map(|v| serde_json::to_vec(&json!(v)).unwrap()).collect()),
        )
        .unwrap();
}

/// A batch on the inherited chain is append-only, so it doesn't stop the
/// child's delta snapshots, and they keep every one of its items.
#[test]
fn branch_at_keeps_an_inherited_batch_across_child_delta_snapshot() {
    run_case_then(
        |store| {
            append_many(store, &["d", "e", "f"]);
            append(store, json!("g"));
        },
        vec![json!("a"), json!("b"), json!("c"), json!("d"), json!("e"), json!("f"), json!("g")],
        |branch, before, after| {
            assert!(
                after.delta_snapshots_since_full > before.delta_snapshots_since_full,
                "{branch}: setup: the child took a delta snapshot past the threshold ({before:?} -> {after:?})"
            );
            assert_eq!(after.last_full_snapshot_offset, before.last_full_snapshot_offset, "{branch}: and no full");
        },
    );
}

/// The chain head `create_branch_at` rebuilds counts a batch before the cut
/// as one operation and all of its items, exactly as the parent's head
/// counted it, whether the cut falls on the batch or after it.
#[test]
fn branch_at_counts_a_batch_before_the_cut_as_the_parent_did() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);
    seed_with_full_snapshot(&store);
    append_many(&store, &["d", "e", "f"]);
    let on_batch = store.current_branch().head;
    let at_batch = (
        store.get_compaction_stats("messages").unwrap(),
        store.get_state_len("messages").unwrap(),
    );
    append(&store, json!("g"));
    append_many(&store, &["h", "i"]);
    let at_head = store.current_branch().head;

    store.create_branch("control", None).unwrap();
    store.create_branch_at("historical", "main", at_head).unwrap();
    store.create_branch_at("on-batch", "main", on_batch).unwrap();

    let control = {
        store.switch_branch("control").unwrap();
        (store.get_compaction_stats("messages").unwrap(), store.get_state_len("messages").unwrap())
    };
    assert_eq!(control.0.ops_since_last_full_snapshot, 3, "two batches and an append");
    assert_eq!(control.1, Some(9));
    store.switch_branch("historical").unwrap();
    let historical = (store.get_compaction_stats("messages").unwrap(), store.get_state_len("messages").unwrap());
    assert_eq!(format!("{historical:?}"), format!("{control:?}"));

    store.switch_branch("on-batch").unwrap();
    let cut = (store.get_compaction_stats("messages").unwrap(), store.get_state_len("messages").unwrap());
    assert_eq!(format!("{cut:?}"), format!("{at_batch:?}"));
    assert_eq!(cut.0.ops_since_last_full_snapshot, 1);
    assert_eq!(cut.1, Some(6));
    assert_eq!(read(&store), ["a", "b", "c", "d", "e", "f"].map(|v| json!(v)).to_vec());
}
