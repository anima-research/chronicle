//! Materialized state is shared between branches whose chain head for a
//! state is at the same log offset, and the per-item cache follows writes
//! instead of being dropped by them.
//!
//! "Did this read re-materialize?" is observed through
//! `Store::state_reconstruction_count()`, never through timing.

use chronicle::{
    BranchId, Sequence, StateOperation, StateRegistration, StateStrategy, Store, StoreConfig,
    TreeEntry,
};
use serde_json::{json, Value};
use tempfile::TempDir;

const DELTA_EVERY: u64 = 5;
const FULL_EVERY: u64 = 4;

fn config(dir: &TempDir) -> StoreConfig {
    StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: true,
    }
}

fn register(store: &Store) {
    store
        .register_state(StateRegistration {
            id: "messages".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: DELTA_EVERY,
                full_snapshot_every: FULL_EVERY,
            },
            initial_value: None,
        })
        .unwrap();
    store
        .register_state(StateRegistration {
            id: "files".to_string(),
            strategy: StateStrategy::Tree {
                delta_snapshot_every: DELTA_EVERY,
                full_snapshot_every: FULL_EVERY,
            },
            initial_value: None,
        })
        .unwrap();
}

fn append(store: &Store, branch: BranchId, v: Value) {
    store
        .update_state_on(branch, "messages", StateOperation::Append(serde_json::to_vec(&v).unwrap()))
        .unwrap();
}

fn slice(store: &Store, branch: BranchId) -> Vec<Value> {
    serde_json::from_slice(
        &store
            .get_state_slice_on(branch, "messages", 0, usize::MAX)
            .unwrap()
            .unwrap(),
    )
    .unwrap()
}

/// A parent with `n` messages (crossing several snapshot thresholds) and a
/// small tree, reopened so that nothing is cached.
fn seeded(dir: &TempDir, n: usize) -> (Store, BranchId, Vec<Value>) {
    {
        let store = Store::create(config(dir)).unwrap();
        register(&store);
        let main = store.current_branch().id;
        for i in 0..n {
            append(&store, main, json!({ "i": i, "text": format!("message {i}") }));
        }
        for i in 0..6u64 {
            store
                .tree_set("files", &format!("f{i}"), &TreeEntry { blob_hash: format!("h{i}"), size: i, mode: 0o644 })
                .unwrap();
        }
    }
    let store = Store::open(config(dir)).unwrap();
    let main = store.current_branch().id;
    let expected: Vec<Value> = (0..n).map(|i| json!({ "i": i, "text": format!("message {i}") })).collect();
    assert_eq!(store.state_reconstruction_count(), 0, "nothing is materialized by open");
    (store, main, expected)
}

#[test]
fn children_share_the_parents_materialization() {
    let dir = TempDir::new().unwrap();
    let (store, main, expected) = seeded(&dir, 137);

    // Warm the parent: exactly one reconstruction for the messages...
    assert_eq!(slice(&store, main), expected);
    assert_eq!(store.state_reconstruction_count(), 1);
    // ...and one for the tree.
    assert_eq!(store.tree_list_on(main, "files", None).unwrap().len(), 6);
    assert_eq!(store.state_reconstruction_count(), 2);

    // Ten children, each reading the inherited states in every way.
    let mut children = Vec::new();
    for i in 0..10 {
        let child = store.create_branch(&format!("child-{i}"), None).unwrap().id;
        assert_eq!(slice(&store, child), expected);
        assert_eq!(
            serde_json::from_slice::<Value>(&store.get_state_item_on(child, "messages", 17).unwrap().unwrap())
                .unwrap(),
            expected[17]
        );
        assert_eq!(
            serde_json::from_slice::<Vec<Value>>(&store.get_state_on(child, "messages").unwrap().unwrap())
                .unwrap(),
            expected
        );
        assert_eq!(store.get_state_len_on(child, "messages").unwrap(), Some(expected.len()));
        assert!(store.tree_get_on(child, "files", "f3").unwrap().is_some());
        assert_eq!(store.tree_list_on(child, "files", Some("f")).unwrap().len(), 6);
        children.push(child);
    }
    assert_eq!(
        store.state_reconstruction_count(),
        2,
        "ten children read both inherited states without one re-materialization"
    );

    // The shared value is one allocation, not ten equal copies.
    let parent_items = store.state_items_on(main, "messages").unwrap().unwrap();
    for child in &children {
        let child_items = store.state_items_on(*child, "messages").unwrap().unwrap();
        assert!(
            std::sync::Arc::ptr_eq(&parent_items, &child_items),
            "a child's cached items must be the parent's, not a copy"
        );
    }

    // A selective fork and a historical fork at the head share it as well.
    let selective = store
        .create_branch_with_states(
            "selective",
            None,
            &chronicle::StateSelection { state_ids: vec!["messages".into()], state_prefixes: vec![] },
        )
        .unwrap()
        .id;
    let head: Sequence = store.current_branch().head;
    let at_head = store.create_branch_at("at-head", "main", head).unwrap().id;
    assert_eq!(slice(&store, selective), expected);
    assert_eq!(slice(&store, at_head), expected);
    assert_eq!(store.tree_list_on(selective, "files", None).unwrap().len(), 0);
    assert_eq!(store.state_reconstruction_count(), 2);
}

#[test]
fn a_child_forked_from_a_warm_parent_reads_for_free_even_after_the_parent_moved_on() {
    let dir = TempDir::new().unwrap();
    let (store, main, expected) = seeded(&dir, 60);
    assert_eq!(slice(&store, main), expected);
    assert!(store.tree_get_on(main, "files", "f0").unwrap().is_some());
    let warm = store.state_reconstruction_count();

    let child = store.create_branch("child", None).unwrap().id;

    // The parent keeps running before the child reads anything.
    let mut parent_expected = expected.clone();
    for i in 0..23 {
        let v = json!({ "parent-later": i });
        append(&store, main, v.clone());
        parent_expected.push(v);
    }
    store
        .update_state_on(main, "messages", StateOperation::Edit { index: 3, new_value: b"\"parent-edit\"".to_vec() })
        .unwrap();
    parent_expected[3] = json!("parent-edit");
    store.tree_remove_on(main, "files", "f0").unwrap();

    // The child still gets the value it inherited, with no log walk.
    assert_eq!(slice(&store, child), expected);
    assert!(store.tree_get_on(child, "files", "f0").unwrap().is_some());
    assert_eq!(store.state_reconstruction_count(), warm, "child's first read after the parent moved on");

    // The parent's own reads of the array did not re-materialize either.
    assert_eq!(slice(&store, main), parent_expected);
    assert_eq!(store.state_reconstruction_count(), warm);
}

#[test]
fn writes_keep_the_item_cache_current_instead_of_dropping_it() {
    let dir = TempDir::new().unwrap();
    let (store, main, expected) = seeded(&dir, 80);
    assert_eq!(slice(&store, main), expected);
    let warm = store.state_reconstruction_count();
    assert_eq!(warm, 1);

    let child = store.create_branch("child", None).unwrap().id;
    let mut child_expected = expected.clone();

    // First append on the child: no reconstruction, by the write or by the
    // read that follows it.
    append(&store, child, json!("child-0"));
    child_expected.push(json!("child-0"));
    assert_eq!(store.state_reconstruction_count(), warm, "child's first append");
    assert_eq!(slice(&store, child), child_expected);
    assert_eq!(store.state_reconstruction_count(), warm, "child's read after its first append");
    assert_eq!(slice(&store, main), expected, "parent unchanged");

    // Many more appends, crossing delta and full snapshot thresholds on the
    // child (a full snapshot needs the whole value: it is assembled from the
    // cached items, not rebuilt from the log), interleaved with edits,
    // redacts and reads of every kind.
    for i in 1..(DELTA_EVERY * FULL_EVERY * 4) as usize {
        let v = json!({ "child": i, "weird": [1.5, null, "a\"b"] });
        append(&store, child, v.clone());
        child_expected.push(v);
        if i % 7 == 0 {
            let idx = i % child_expected.len();
            store
                .update_state_on(child, "messages", StateOperation::Edit {
                    index: idx,
                    new_value: serde_json::to_vec(&json!({ "edited-at": i })).unwrap(),
                })
                .unwrap();
            child_expected[idx] = json!({ "edited-at": i });
        }
        if i % 11 == 0 {
            store.update_state_on(child, "messages", StateOperation::Redact { start: 2, end: 4 }).unwrap();
            child_expected.drain(2..4);
        }
        assert_eq!(store.get_state_len_on(child, "messages").unwrap(), Some(child_expected.len()));
        if i % 3 == 0 {
            assert_eq!(slice(&store, child), child_expected, "step {i}");
        }
        if i % 13 == 0 {
            assert_eq!(
                serde_json::from_slice::<Vec<Value>>(&store.get_state_on(child, "messages").unwrap().unwrap())
                    .unwrap(),
                child_expected
            );
        }
    }
    let stats = store.get_compaction_stats_on(child, "messages").unwrap();
    assert!(stats.last_full_snapshot_offset.is_some(), "the child must have taken a full snapshot");
    assert_eq!(slice(&store, child), child_expected);
    assert_eq!(
        store.state_reconstruction_count(),
        warm,
        "appends, edits, redacts and the child's own snapshots never forced a re-materialization"
    );
    assert_eq!(slice(&store, main), expected);
    assert_eq!(store.state_reconstruction_count(), warm);

    // What the cache says is what the log says.
    store.clear_state_caches();
    assert_eq!(slice(&store, child), child_expected);
    assert_eq!(slice(&store, main), expected);
    assert_eq!(store.state_reconstruction_count(), warm + 2);
}

#[test]
fn derived_versions_share_items_with_the_inherited_value() {
    let dir = TempDir::new().unwrap();
    // More than one chunk of items, so chunk sharing is observable.
    let (store, main, _) = seeded(&dir, 2500);
    let parent = store.state_items_on(main, "messages").unwrap().unwrap();
    let child = store.create_branch("child", None).unwrap().id;
    append(&store, child, json!("child-0"));
    store
        .update_state_on(child, "messages", StateOperation::Edit { index: 5, new_value: b"\"e\"".to_vec() })
        .unwrap();

    let derived = store.state_items_on(child, "messages").unwrap().unwrap();
    assert_eq!(derived.len(), parent.len() + 1);
    assert_eq!(&derived[5], b"\"e\"");
    assert_ne!(&parent[5], b"\"e\"");
    // Untouched items are the same bytes in memory, not equal copies.
    for i in [0usize, 6, 1023, 1024, 1500, 2499] {
        assert!(
            std::ptr::eq(parent.get(i).unwrap().as_ptr(), derived.get(i).unwrap().as_ptr()),
            "item {i} was copied"
        );
    }
    assert_eq!(store.state_reconstruction_count(), 1);
}

/// The oracle: the value rebuilt from the log by the historical-read path,
/// which uses no cache at all.
fn cold_bytes(store: &Store, branch: BranchId) -> Option<Vec<u8>> {
    let head = store.get_branch_by_id(branch).unwrap().head;
    store.get_state_at_on(branch, "messages", head).unwrap()
}

fn rejoin(bytes: &[u8]) -> Vec<u8> {
    if bytes.is_empty() {
        return b"[]".to_vec();
    }
    let raw: Vec<&serde_json::value::RawValue> = serde_json::from_slice(bytes).unwrap();
    let mut out = vec![b'['];
    for (i, r) in raw.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend_from_slice(r.get().as_bytes());
    }
    out.push(b']');
    out
}

/// Whatever mix of writes, forks, reads and cache drops happens, every
/// cached read must return byte for byte what a cache-less rebuild from the
/// log returns. The writes include batches (`AppendMany`) among the single
/// appends.
#[test]
fn cached_reads_equal_cold_reads_under_a_random_workload() {
    for seed in [1u64, 7, 42, 1234, 99999] {
        let dir = TempDir::new().unwrap();
        let store = Store::create(config(&dir)).unwrap();
        register(&store);

        let mut state = seed.wrapping_mul(0x9E3779B97F4A7C15) | 1;
        let mut rnd = move |bound: usize| -> usize {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % bound.max(1) as u64) as usize
        };

        // Appended payloads, several of them NOT in serde_json's own form
        // (unsorted keys, whitespace, exponent numbers, escapes).
        let raw_items: [&[u8]; 8] = [
            b"1",
            b"{\"z\":1,\"a\":[1, 2]}",
            b"  \"padded\"  ",
            b"1e2",
            b"{\"b\": {\"y\": null, \"x\": 1.50}}",
            b"\"\\u0041\\n\"",
            b"[ ]",
            b"-0.0",
        ];
        let raw_sets: [&[u8]; 4] = [
            b"[]",
            b"[ {\"b\":1, \"a\":2} , 3 ]",
            b"[1,2,3,4,5,6,7,8,9,10,11,12]",
            b"[\"x\"]",
        ];

        let main = store.current_branch().id;
        let mut branches: Vec<(BranchId, String, Vec<Value>)> = vec![(main, "main".to_string(), Vec::new())];
        let mut history: Vec<(String, Sequence, Vec<Value>)> = Vec::new();

        for step in 0..700 {
            let b = rnd(branches.len());
            let (branch, name) = (branches[b].0, branches[b].1.clone());
            let branch_count = branches.len();
            let len = branches[b].2.len();
            match rnd(100) {
                0..=54 => {
                    let raw = raw_items[rnd(raw_items.len())];
                    store.update_state_on(branch, "messages", StateOperation::Append(raw.to_vec())).unwrap();
                    branches[b].2.push(serde_json::from_slice(raw).unwrap());
                }
                55..=64 if len > 0 => {
                    let model = &mut branches[b].2;
                    let i = rnd(model.len());
                    let raw = raw_items[rnd(raw_items.len())];
                    store
                        .update_state_on(branch, "messages", StateOperation::Edit { index: i, new_value: raw.to_vec() })
                        .unwrap();
                    model[i] = serde_json::from_slice(raw).unwrap();
                }
                65..=70 => {
                    let model = &mut branches[b].2;
                    // Kept in range: the chain head's running item count
                    // (what `get_state_len` reports) is only exact for
                    // in-range redacts.
                    let start = rnd(model.len() + 1);
                    let end = (start + rnd(4)).min(model.len());
                    store.update_state_on(branch, "messages", StateOperation::Redact { start, end }).unwrap();
                    if start < end {
                        model.drain(start..end);
                    }
                }
                71..=73 => {
                    let raw = raw_sets[rnd(raw_sets.len())];
                    store.update_state_on(branch, "messages", StateOperation::Set(raw.to_vec())).unwrap();
                    branches[b].2 = serde_json::from_slice(raw).unwrap();
                }
                74..=76 => {
                    store.compact_state_on(branch, "messages").unwrap();
                }
                77..=82 if branch_count < 12 => {
                    // Head fork.
                    let child_name = format!("b{step}");
                    let model = branches[b].2.clone();
                    let id = store.create_branch(&child_name, Some(&name)).unwrap().id;
                    branches.push((id, child_name, model));
                }
                83..=86 if branch_count < 12 && !history.is_empty() => {
                    // Historical fork from a recorded point.
                    let (from, at, model) = history[rnd(history.len())].clone();
                    let child_name = format!("h{step}");
                    let id = store.create_branch_at(&child_name, &from, at).unwrap().id;
                    branches.push((id, child_name, model));
                }
                87..=88 => store.clear_state_caches(),
                89..=95 => {
                    // A batch of the same payloads, one record.
                    let raws: Vec<&[u8]> = (0..1 + rnd(4)).map(|_| raw_items[rnd(raw_items.len())]).collect();
                    store
                        .update_state_on(
                            branch,
                            "messages",
                            StateOperation::AppendMany(raws.iter().map(|raw| raw.to_vec()).collect()),
                        )
                        .unwrap();
                    for raw in raws {
                        branches[b].2.push(serde_json::from_slice(raw).unwrap());
                    }
                }
                _ => {}
            }

            // Record points to fork from later.
            let (branch, name, model) = &branches[b];
            if rnd(6) == 0 {
                let head = store.get_branch_by_id(*branch).unwrap().head;
                history.push((name.clone(), head, model.clone()));
            }

            // Warm some caches in a random pattern, then check every kind
            // of read on a random branch against the log.
            let check = rnd(branches.len());
            let (branch, name, model) = &branches[check];
            let cold = cold_bytes(&store, *branch);
            let order = rnd(3);
            let warm_slice = || store.get_state_slice_on(*branch, "messages", 0, usize::MAX).unwrap();
            let warm_state = || store.get_state_on(*branch, "messages").unwrap();
            let (got_slice, got_state) = match order {
                0 => {
                    let s = warm_slice();
                    (s, warm_state())
                }
                1 => {
                    let st = warm_state();
                    (warm_slice(), st)
                }
                _ => (warm_slice(), warm_state()),
            };
            assert_eq!(got_state, cold, "seed {seed} step {step} branch {name}: get_state differs from the log");
            match &cold {
                None => {
                    assert_eq!(got_slice, None);
                    assert!(model.is_empty());
                }
                Some(cold) => {
                    assert_eq!(
                        got_slice.as_deref(),
                        Some(rejoin(cold).as_slice()),
                        "seed {seed} step {step} branch {name}: slice differs from the log"
                    );
                    let parsed: Vec<Value> =
                        if cold.is_empty() { vec![] } else { serde_json::from_slice(cold).unwrap() };
                    assert_eq!(&parsed, model, "seed {seed} step {step} branch {name}: model");
                    assert_eq!(store.get_state_len_on(*branch, "messages").unwrap(), Some(model.len()));
                    if !model.is_empty() {
                        let i = rnd(model.len());
                        let item = store.get_state_item_on(*branch, "messages", i).unwrap().unwrap();
                        assert_eq!(serde_json::from_slice::<Value>(&item).unwrap(), model[i]);
                        let part = store.get_state_slice_on(*branch, "messages", i, 3).unwrap().unwrap();
                        let part: Vec<Value> = serde_json::from_slice(&part).unwrap();
                        assert_eq!(part[..], model[i..(i + 3).min(model.len())]);
                    }
                }
            }
        }
        assert!(branches.len() > 4, "seed {seed}: workload must fork");
    }
}
