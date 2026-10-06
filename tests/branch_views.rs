//! Branch-bound operations ("views"): every `*_on(branch, ..)` call acts on
//! the branch it names and never reads or moves the store-wide cursor, so
//! several callers can work on different branches of one open store at once.

use chronicle::{
    BranchId, Record, RecordInput, Sequence, StateOperation, StateRegistration, StateStrategy,
    StateUpdateRecord, Store, StoreConfig, StoreError, TreeEntry,
};
use serde_json::{json, Value};
use tempfile::TempDir;

const DELTA_EVERY: u64 = 4;
const FULL_EVERY: u64 = 3;

fn config(dir: &TempDir) -> StoreConfig {
    StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: true,
    }
}

fn register_all(store: &Store) {
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
            id: "config".to_string(),
            strategy: StateStrategy::Snapshot,
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

fn entry(n: u64) -> TreeEntry {
    TreeEntry {
        blob_hash: format!("hash-{n}"),
        size: n,
        mode: 0o644,
    }
}

fn append_on(store: &Store, branch: BranchId, v: Value) -> Record {
    store
        .update_state_on(
            branch,
            "messages",
            StateOperation::Append(serde_json::to_vec(&v).unwrap()),
        )
        .unwrap()
}

fn messages_on(store: &Store, branch: BranchId) -> Vec<Value> {
    match store.get_state_on(branch, "messages").unwrap() {
        Some(bytes) => serde_json::from_slice(&bytes).unwrap(),
        None => Vec::new(),
    }
}

fn config_on(store: &Store, branch: BranchId) -> Option<Value> {
    store
        .get_state_on(branch, "config")
        .unwrap()
        .map(|b| serde_json::from_slice(&b).unwrap())
}

fn files_on(store: &Store, branch: BranchId) -> Vec<(String, TreeEntry)> {
    store.tree_list_on(branch, "files", None).unwrap()
}

/// Seed main with a few values in each kind of state.
fn seed(store: &Store) -> BranchId {
    register_all(store);
    let main = store.current_branch().id;
    for i in 0..6 {
        append_on(store, main, json!({ "from": "main", "i": i }));
    }
    store
        .update_state("config", StateOperation::Set(b"{\"v\":0}".to_vec()))
        .unwrap();
    for i in 0..3 {
        store.tree_set("files", &format!("f{i}"), &entry(i)).unwrap();
    }
    main
}

/// Decoded state operations of the records written ON `branch` for `state_id`.
fn ops_on_branch(store: &Store, branch: BranchId, state_id: &str) -> Vec<StateOperation> {
    store
        .query_range_on(branch, None, None, 1_000_000, false, None)
        .unwrap()
        .into_iter()
        .inspect(|r| assert_eq!(r.branch, branch, "query_range_on returned a foreign record"))
        .filter(|r| r.record_type == "state_update")
        .map(|r| StateUpdateRecord::decode(&r).unwrap())
        .filter(|u| u.state_id == state_id)
        .map(|u| u.operation)
        .collect()
}

fn is_snapshot(op: &StateOperation) -> bool {
    matches!(
        op,
        StateOperation::Snapshot(_)
            | StateOperation::DeltaSnapshot(_)
            | StateOperation::TreeDeltaSnapshot(_)
    )
}

#[test]
fn sibling_views_interleave_while_the_cursor_stays_put() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(&dir)).unwrap();
    let main = seed(&store);

    let a = store.create_branch("a", None).unwrap().id;
    let b = store.create_branch("b", None).unwrap().id;
    // The cursor sits on a third branch the whole time.
    let elsewhere = store.create_branch("elsewhere", None).unwrap().id;
    store.switch_branch("elsewhere").unwrap();

    let inherited = messages_on(&store, main);
    let main_head_before = store.get_branch("main").unwrap().head;
    let elsewhere_head_before = store.current_branch().head;

    let mut expect_a = inherited.clone();
    let mut expect_b = inherited.clone();
    // Enough interleaved writes that both branches cross delta AND full
    // snapshot thresholds while the other is mid-interval.
    for i in 0..(DELTA_EVERY * FULL_EVERY * 2 + 3) {
        let va = json!({ "from": "a", "i": i });
        let ra = append_on(&store, a, va.clone());
        expect_a.push(va);
        assert_eq!(ra.branch, a);

        let vb = json!({ "from": "b", "i": i });
        let rb = append_on(&store, b, vb.clone());
        expect_b.push(vb);
        assert_eq!(rb.branch, b);

        if i % 5 == 0 {
            let vb2 = json!({ "from": "b", "extra": i });
            append_on(&store, b, vb2.clone());
            expect_b.push(vb2);
        }

        // Reads interleave too, each seeing only its own branch.
        assert_eq!(messages_on(&store, a), expect_a);
        assert_eq!(messages_on(&store, b), expect_b);
        assert_eq!(store.get_state_len_on(a, "messages").unwrap(), Some(expect_a.len()));
        assert_eq!(store.get_state_len_on(b, "messages").unwrap(), Some(expect_b.len()));
        assert_eq!(
            serde_json::from_slice::<Value>(
                &store.get_state_item_on(a, "messages", expect_a.len() - 1).unwrap().unwrap()
            )
            .unwrap(),
            *expect_a.last().unwrap()
        );
        assert_eq!(
            serde_json::from_slice::<Vec<Value>>(
                &store.get_state_tail_on(b, "messages", 2).unwrap().unwrap()
            )
            .unwrap(),
            expect_b[expect_b.len() - 2..].to_vec()
        );
        assert_eq!(
            serde_json::from_slice::<Vec<Value>>(
                &store.get_state_slice_on(a, "messages", 2, 5).unwrap().unwrap()
            )
            .unwrap(),
            expect_a[2..7].to_vec()
        );

        // The cursor never moved.
        assert_eq!(store.current_branch().id, elsewhere);
    }

    // The cursor's branch and main were not written to.
    assert_eq!(store.current_branch().head, elsewhere_head_before);
    assert_eq!(store.get_branch("main").unwrap().head, main_head_before);
    assert_eq!(messages_on(&store, main), inherited);
    assert_eq!(messages_on(&store, elsewhere), inherited);
    assert_eq!(store.get_state("messages").unwrap().map(|b| b.len()), store.get_state_on(main, "messages").unwrap().map(|b| b.len()));

    // Both siblings took snapshots of both kinds, each on its own branch.
    for (branch, name) in [(a, "a"), (b, "b")] {
        let ops = ops_on_branch(&store, branch, "messages");
        assert!(
            ops.iter().any(|op| matches!(op, StateOperation::DeltaSnapshot(_))),
            "{name}: expected a delta snapshot on the branch"
        );
        assert!(
            ops.iter().any(|op| matches!(op, StateOperation::Snapshot(_))),
            "{name}: expected a full snapshot on the branch"
        );
    }
    assert!(ops_on_branch(&store, elsewhere, "messages").is_empty());

    // Sequences are per branch: siblings forked from the same point both
    // continue from the parent's head, so their sequence ranges overlap.
    // Record ids are store-wide and unique.
    let recs_a = store.query_range_on(a, None, None, 10, false, None).unwrap();
    let recs_b = store.query_range_on(b, None, None, 10, false, None).unwrap();
    assert_eq!(recs_a[0].sequence, recs_b[0].sequence);
    assert_eq!(recs_a[0].sequence, Sequence(main_head_before.0 + 1));
    assert_ne!(recs_a[0].id, recs_b[0].id);
}

#[test]
fn parent_and_child_writes_are_mutually_invisible() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(&dir)).unwrap();
    let main = seed(&store);

    let child = store.create_branch("child", None).unwrap().id;
    // Leave the cursor on main: the parent "keeps running" through the
    // cursor while the child is driven through explicit-branch calls.
    let msgs_fork = messages_on(&store, main);
    let cfg_fork = config_on(&store, main);
    let files_fork = files_on(&store, main);
    assert_eq!(messages_on(&store, child), msgs_fork);
    assert_eq!(config_on(&store, child), cfg_fork);
    assert_eq!(files_on(&store, child), files_fork);

    // ---- parent moves on (appends, edit, redact, set, tree ops) ----
    for i in 0..9 {
        store
            .update_state("messages", StateOperation::Append(format!("\"main-later-{i}\"").into_bytes()))
            .unwrap();
    }
    store
        .update_state("messages", StateOperation::Edit { index: 0, new_value: b"\"main-edit\"".to_vec() })
        .unwrap();
    store
        .update_state("messages", StateOperation::Redact { start: 1, end: 3 })
        .unwrap();
    store
        .update_state("config", StateOperation::Set(b"{\"v\":\"main\"}".to_vec()))
        .unwrap();
    store.tree_set("files", "main-only", &entry(100)).unwrap();
    store.tree_remove("files", "f0").unwrap();

    assert_eq!(messages_on(&store, child), msgs_fork, "parent append/edit/redact leaked into child");
    assert_eq!(config_on(&store, child), cfg_fork, "parent Set leaked into child");
    assert_eq!(files_on(&store, child), files_fork, "parent tree ops leaked into child");

    let msgs_main = messages_on(&store, main);
    let cfg_main = config_on(&store, main);
    let files_main = files_on(&store, main);
    assert_eq!(msgs_main.len(), msgs_fork.len() + 9 - 2);
    assert_eq!(msgs_main[0], json!("main-edit"));

    // ---- child moves on (appends, edit, redact, set, tree ops) ----
    let mut expect = msgs_fork.clone();
    for i in 0..11 {
        let v = json!(format!("child-{i}"));
        append_on(&store, child, v.clone());
        expect.push(v);
    }
    store
        .update_state_on(child, "messages", StateOperation::Edit { index: 2, new_value: b"\"child-edit\"".to_vec() })
        .unwrap();
    expect[2] = json!("child-edit");
    store
        .update_state_on(child, "messages", StateOperation::Redact { start: 4, end: 6 })
        .unwrap();
    expect.drain(4..6);
    // More appends after the edit/redact, across further thresholds.
    for i in 11..30 {
        let v = json!(format!("child-{i}"));
        append_on(&store, child, v.clone());
        expect.push(v);
    }
    store
        .update_state_on(child, "config", StateOperation::Set(b"{\"v\":\"child\"}".to_vec()))
        .unwrap();
    store.tree_set_on(child, "files", "child-only", &entry(200)).unwrap();
    store.tree_remove_on(child, "files", "f1").unwrap();
    store
        .tree_batch_on(
            child,
            "files",
            &[
                chronicle::TreeOp::Set { path: "batch/a".into(), entry: entry(1) },
                chronicle::TreeOp::Remove { path: "f2".into() },
            ],
        )
        .unwrap();

    assert_eq!(messages_on(&store, child), expect);
    assert_eq!(config_on(&store, child), Some(json!({ "v": "child" })));
    let child_files: Vec<String> = files_on(&store, child).into_iter().map(|(p, _)| p).collect();
    assert_eq!(child_files, vec!["batch/a", "child-only", "f0"]);
    assert_eq!(store.tree_get_on(child, "files", "main-only").unwrap(), None);
    assert_eq!(store.tree_get_on(child, "files", "child-only").unwrap(), Some(entry(200)));

    assert_eq!(messages_on(&store, main), msgs_main, "child append/edit/redact leaked into parent");
    assert_eq!(config_on(&store, main), cfg_main, "child Set leaked into parent");
    assert_eq!(files_on(&store, main), files_main, "child tree ops leaked into parent");
    assert_eq!(store.tree_get("files", "child-only").unwrap(), None);

    // Historical reads are per branch too.
    let fork_seq = store.get_branch("child").unwrap().branch_point.unwrap();
    for branch in [main, child] {
        let at_fork: Vec<Value> = serde_json::from_slice(
            &store.get_state_at_on(branch, "messages", fork_seq).unwrap().unwrap(),
        )
        .unwrap();
        assert_eq!(at_fork, msgs_fork);
    }
    let diff = store
        .tree_diff_on(child, "files", fork_seq, store.get_branch("child").unwrap().head)
        .unwrap();
    assert_eq!(diff.len(), 4, "child-only added, batch/a added, f1 removed, f2 removed");
}

#[test]
fn auto_snapshots_from_a_view_land_on_its_branch_and_survive_reopen() {
    let dir = TempDir::new().unwrap();
    let (expect_msgs, expect_files, main_msgs, main_head, child_id);
    {
        let store = Store::create(config(&dir)).unwrap();
        let main = seed(&store);
        let child = store.create_branch("child", None).unwrap().id;
        child_id = child;
        // Cursor stays on main.
        main_head = store.current_branch().head;
        main_msgs = messages_on(&store, main);
        let main_stats_before = format!("{:?}", store.get_compaction_stats("messages"));
        let main_records_before = store.query_range(None, None, 1_000_000, false, None).unwrap().len();

        let mut msgs = main_msgs.clone();
        for i in 0..(DELTA_EVERY * FULL_EVERY * 3) {
            let v = json!({ "child": i });
            append_on(&store, child, v.clone());
            msgs.push(v);
            store.tree_set_on(child, "files", &format!("c{}", i % 7), &entry(i)).unwrap();
        }

        let msg_ops = ops_on_branch(&store, child, "messages");
        let file_ops = ops_on_branch(&store, child, "files");
        assert!(msg_ops.iter().any(|op| matches!(op, StateOperation::DeltaSnapshot(_))));
        assert!(msg_ops.iter().any(|op| matches!(op, StateOperation::Snapshot(_))));
        assert!(file_ops.iter().any(|op| matches!(op, StateOperation::TreeDeltaSnapshot(_))));
        assert!(file_ops.iter().any(|op| matches!(op, StateOperation::Snapshot(_))));

        // Nothing — no write, no snapshot — reached the cursor's branch.
        assert_eq!(store.current_branch().id, main);
        assert_eq!(store.current_branch().head, main_head);
        assert_eq!(format!("{:?}", store.get_compaction_stats("messages")), main_stats_before);
        assert_eq!(
            store.query_range(None, None, 1_000_000, false, None).unwrap().len(),
            main_records_before
        );
        assert!(!ops_on_branch(&store, main, "messages").iter().skip(6).any(is_snapshot));

        // The child's snapshots describe the CHILD's value: the last full
        // snapshot on the child, plus what follows it, is the child's state.
        let last_full = msg_ops
            .iter()
            .rev()
            .find_map(|op| match op {
                StateOperation::Snapshot(data) => Some(data.clone()),
                _ => None,
            })
            .unwrap();
        let in_snapshot: Vec<Value> = serde_json::from_slice(&last_full).unwrap();
        assert_eq!(in_snapshot[..], msgs[..in_snapshot.len()]);
        assert!(in_snapshot.len() > main_msgs.len());

        expect_files = files_on(&store, child);
        assert_eq!(messages_on(&store, child), msgs);
        expect_msgs = msgs;
        store.sync().unwrap();
    }

    // Cold reconstruction from the log.
    let store = Store::open(config(&dir)).unwrap();
    assert_eq!(store.current_branch().name, "main");
    assert_eq!(store.current_branch().head, main_head);
    let child = store.get_branch("child").unwrap().id;
    assert_eq!(child, child_id);
    assert_eq!(messages_on(&store, child), expect_msgs);
    assert_eq!(files_on(&store, child), expect_files);
    assert_eq!(messages_on(&store, store.current_branch().id), main_msgs);

    // And the reopened child keeps going.
    append_on(&store, child, json!("after-reopen"));
    assert_eq!(messages_on(&store, child).len(), expect_msgs.len() + 1);
    assert_eq!(messages_on(&store, store.current_branch().id), main_msgs);
}

#[test]
fn manual_compaction_and_snapshots_target_the_named_branch() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(&dir)).unwrap();
    let main = seed(&store);
    let child = store.create_branch("child", None).unwrap().id;
    append_on(&store, child, json!("child-0"));

    let main_head = store.current_branch().head;
    let child_expect = messages_on(&store, child);

    let rec = store.compact_state_on(child, "messages").unwrap().unwrap();
    assert_eq!(rec.branch, child);
    let update = StateUpdateRecord::decode(&rec).unwrap();
    match update.operation {
        StateOperation::Snapshot(data) => {
            assert_eq!(serde_json::from_slice::<Vec<Value>>(&data).unwrap(), child_expect);
        }
        other => panic!("expected a full snapshot, got {other:?}"),
    }
    assert_eq!(store.tree_snapshot_on(child, "files").unwrap().unwrap().branch, child);
    assert!(store.compact_all_states_on(child).unwrap() >= 3);
    assert_eq!(store.create_snapshot_if_needed_on(child, "messages").unwrap().map(|r| r.branch), None);

    assert_eq!(store.current_branch().head, main_head, "compaction on the child wrote to main");
    assert_eq!(messages_on(&store, child), child_expect);
    let child_stats = store.get_compaction_stats_on(child, "messages").unwrap();
    assert_eq!(child_stats.ops_since_last_full_snapshot, 0);
    let main_stats = store.get_compaction_stats_on(main, "messages").unwrap();
    assert!(main_stats.ops_since_last_full_snapshot > 0);
    assert!(store.get_chain_stats_on(child, "messages").unwrap().unwrap().has_full_snapshot);
    assert!(store.get_compaction_summary_on(child).unwrap().total_operations > 0);
}

#[test]
fn records_and_identity_appends_target_the_named_branch() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(&dir)).unwrap();
    let main = seed(&store);
    let child = store.create_branch("child", None).unwrap().id;
    let main_head = store.current_branch().head;

    let r1 = store
        .append_on(child, RecordInput::json("note", &json!({ "n": 1 })).unwrap())
        .unwrap();
    assert_eq!(r1.branch, child);
    assert_eq!(r1.sequence, Sequence(main_head.0 + 1));

    let r2 = store
        .append_to_state_json_with_identity_on(child, "messages", json!({ "text": "hi" }), "id", "seq")
        .unwrap();
    assert_eq!(r2.branch, child);
    assert_eq!(r2.sequence, Sequence(main_head.0 + 2));
    let last = messages_on(&store, child).pop().unwrap();
    assert_eq!(last["id"], json!(r2.id.0.to_string()));
    assert_eq!(last["seq"], json!(r2.sequence.0));

    // query_range_on returns the records written on the branch itself.
    let on_child = store.query_range_on(child, None, None, 100, false, None).unwrap();
    assert_eq!(on_child.iter().map(|r| r.id).collect::<Vec<_>>(), vec![r1.id, r2.id]);
    let notes = store
        .query_range_on(child, None, None, 100, false, Some(&["note".to_string()]))
        .unwrap();
    assert_eq!(notes.len(), 1);
    assert!(store
        .query_range_on(main, None, None, 1000, false, None)
        .unwrap()
        .iter()
        .all(|r| r.branch == main));

    assert_eq!(store.current_branch().id, main);
    assert_eq!(store.current_branch().head, main_head);
    assert_eq!(store.get_branch("child").unwrap().head, Sequence(main_head.0 + 2));
}

#[test]
fn operations_on_a_deleted_branch_fail_cleanly() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(&dir)).unwrap();
    let main = seed(&store);
    let child = store.create_branch("child", None).unwrap().id;
    append_on(&store, child, json!("child-0"));
    assert_eq!(messages_on(&store, child).len(), 7);

    store.delete_branch("child").unwrap();
    assert!(!store.branch_exists(child));

    fn not_found<T: std::fmt::Debug>(r: Result<T, StoreError>, what: &str) {
        match r {
            Err(StoreError::BranchNotFound(_)) => {}
            other => panic!("{what}: expected BranchNotFound, got {other:?}"),
        }
    }

    not_found(store.get_state_on(child, "messages"), "get_state_on");
    not_found(store.get_state_len_on(child, "messages"), "get_state_len_on");
    not_found(store.get_state_slice_on(child, "messages", 0, 1), "get_state_slice_on");
    not_found(store.get_state_item_on(child, "messages", 0), "get_state_item_on");
    not_found(store.get_state_tail_on(child, "messages", 1), "get_state_tail_on");
    not_found(store.get_state_at_on(child, "messages", Sequence(1)), "get_state_at_on");
    not_found(
        store.update_state_on(child, "messages", StateOperation::Append(b"1".to_vec())),
        "update_state_on",
    );
    not_found(
        store.append_on(child, RecordInput::json("note", &json!(1)).unwrap()),
        "append_on",
    );
    not_found(store.tree_get_on(child, "files", "f0"), "tree_get_on");
    not_found(store.tree_list_on(child, "files", None), "tree_list_on");
    not_found(store.tree_set_on(child, "files", "x", &entry(1)), "tree_set_on");
    not_found(store.compact_state_on(child, "messages"), "compact_state_on");
    not_found(store.create_snapshot_if_needed_on(child, "messages"), "create_snapshot_if_needed_on");
    not_found(store.get_chain_stats_on(child, "messages"), "get_chain_stats_on");
    not_found(store.query_range_on(child, None, None, 10, false, None), "query_range_on");
    not_found(
        store.register_state_field_index_on(child, "messages", "/i", chronicle::FieldIndexKind::Number),
        "register_state_field_index_on",
    );
    assert_eq!(store.snapshot_needed_on(child, "messages"), None);
    assert!(store.get_compaction_stats_on(child, "messages").is_none());
    assert_eq!(
        store.query_state_index_range_on(child, "messages", "/i", None, None, None, None, false),
        None
    );

    // The store is otherwise fine, and a new branch with the same name is a
    // different branch: the old id stays dead.
    assert_eq!(messages_on(&store, main).len(), 6);
    let reborn = store.create_branch("child", None).unwrap().id;
    assert_ne!(reborn, child);
    assert_eq!(messages_on(&store, reborn).len(), 6);
    not_found(store.get_state_on(child, "messages"), "old id after the name was reused");
}

#[test]
fn a_branch_written_only_through_explicit_calls_persists() {
    let dir = TempDir::new().unwrap();
    let (expect_msgs, expect_cfg, expect_files, grandchild_msgs);
    {
        let store = Store::create(config(&dir)).unwrap();
        seed(&store);
        let child = store.create_branch("child", None).unwrap().id;

        for i in 0..7 {
            append_on(&store, child, json!({ "c": i }));
        }
        store
            .update_state_on(child, "messages", StateOperation::Edit { index: 1, new_value: b"\"edited\"".to_vec() })
            .unwrap();
        store
            .update_state_on(child, "config", StateOperation::Set(b"{\"v\":\"child\"}".to_vec()))
            .unwrap();
        store.tree_set_on(child, "files", "child/file", &entry(9)).unwrap();

        // A grandchild forked from the child (not from the cursor's branch).
        let grandchild = store.create_branch("grandchild", Some("child")).unwrap().id;
        append_on(&store, grandchild, json!("gc"));

        expect_msgs = messages_on(&store, child);
        expect_cfg = config_on(&store, child);
        expect_files = files_on(&store, child);
        grandchild_msgs = messages_on(&store, grandchild);
        assert_eq!(grandchild_msgs.len(), expect_msgs.len() + 1);
        // No explicit sync: Drop syncs, as for any other write.
    }

    let store = Store::open(config(&dir)).unwrap();
    assert_eq!(store.current_branch().name, "main", "the cursor was never moved");
    let child = store.get_branch("child").unwrap();
    let grandchild = store.get_branch("grandchild").unwrap();
    assert_eq!(grandchild.parent, Some(child.id));
    assert_eq!(messages_on(&store, child.id), expect_msgs);
    assert_eq!(config_on(&store, child.id), expect_cfg);
    assert_eq!(files_on(&store, child.id), expect_files);
    assert_eq!(messages_on(&store, grandchild.id), grandchild_msgs);
    assert_eq!(messages_on(&store, store.current_branch().id).len(), 6);

    // Heads were reconciled from the log for branches the cursor never visited.
    assert_eq!(
        store.query_range_on(child.id, None, None, 1000, true, None).unwrap()[0].sequence,
        child.head
    );
}

#[test]
fn subscription_catch_up_replays_the_named_branch() {
    use chronicle::{StoreEvent, SubscriptionConfig, SubscriptionFilter};

    let dir = TempDir::new().unwrap();
    let store = Store::create(config(&dir)).unwrap();
    let main = seed(&store);
    let child = store.create_branch("child", None).unwrap().id;
    let fork = store.current_branch().head;
    store
        .append_on(child, RecordInput::json("note", &json!({ "on": "child" })).unwrap())
        .unwrap();
    store
        .append(RecordInput::json("note", &json!({ "on": "main" })).unwrap())
        .unwrap();

    for (branch, expected) in [(child, "child"), (main, "main")] {
        let handle = store.subscribe(SubscriptionConfig {
            filter: SubscriptionFilter {
                record_types: Some(vec!["note".to_string()]),
                include_records: true,
                ..Default::default()
            },
            from_sequence: Some(Sequence(fork.0 + 1)),
            ..Default::default()
        });
        store.catch_up_subscription_on(branch, handle.id).unwrap();
        let mut seen = Vec::new();
        while let Ok(event) = handle.try_recv() {
            if let StoreEvent::Record { record } = event {
                assert_eq!(record.branch, branch);
                seen.push(record.id);
            }
        }
        assert_eq!(seen.len(), 1, "exactly the {expected} note is replayed");
        store.unsubscribe(handle.id);
    }
}

// =============================================================================
// PER-BRANCH FIELD INDEXES
// =============================================================================

fn ts_range(store: &Store, branch: BranchId, gte: f64, lte: f64) -> Option<Vec<u32>> {
    store.query_state_index_range_on(branch, "messages", "/ts", Some(gte), Some(lte), None, None, false)
}

fn by_channel(store: &Store, branch: BranchId, channel: &str) -> Option<Vec<u32>> {
    store.query_state_index_eq_on(branch, "messages", "/channel", channel, None, None)
}

#[test]
fn parent_field_index_survives_child_writes() {
    use chronicle::FieldIndexKind;

    let dir = TempDir::new().unwrap();
    let (main, child);
    {
        let store = Store::create(config(&dir)).unwrap();
        register_all(&store);
        main = store.current_branch().id;
        for i in 0..10u32 {
            append_on(&store, main, json!({ "ts": i * 10, "channel": if i % 2 == 0 { "even" } else { "odd" } }));
        }
        store.register_state_field_index("messages", "/ts", FieldIndexKind::Number).unwrap();
        store.register_state_field_index("messages", "/channel", FieldIndexKind::String).unwrap();
        assert_eq!(ts_range(&store, main, 20.0, 40.0), Some(vec![2, 3, 4]));

        child = store.create_branch("child", None).unwrap().id;
        // Nothing is built for the new branch.
        assert_eq!(ts_range(&store, child, 0.0, 1000.0), None);
        assert_eq!(by_channel(&store, child, "even"), None);

        // The child writes every kind of op to the indexed slot, without an
        // index of its own. Before indexes were per branch, the first of
        // these dropped the parent's.
        for i in 0..7u32 {
            append_on(&store, child, json!({ "ts": 1000 + i, "channel": "child" }));
        }
        store
            .update_state_on(child, "messages", StateOperation::Edit {
                index: 0,
                new_value: serde_json::to_vec(&json!({ "ts": 5555, "channel": "child-edit" })).unwrap(),
            })
            .unwrap();
        store.update_state_on(child, "messages", StateOperation::Redact { start: 1, end: 4 }).unwrap();
        store.compact_state_on(child, "messages").unwrap();

        // The parent's indexes are still there and still exactly the parent's.
        assert_eq!(ts_range(&store, main, 20.0, 40.0), Some(vec![2, 3, 4]));
        assert_eq!(ts_range(&store, main, 0.0, 1e9), Some((0..10).collect()));
        assert_eq!(ts_range(&store, main, 1000.0, 1e9), Some(vec![]));
        assert_eq!(by_channel(&store, main, "even"), Some(vec![0, 2, 4, 6, 8]));
        assert_eq!(by_channel(&store, main, "child"), Some(vec![]));
        assert_eq!(
            store.get_state_index_value_counts_on(main, "messages", "/channel"),
            Some(vec![("even".to_string(), 5), ("odd".to_string(), 5)])
        );
        // The child still has none: its writes did not manufacture one.
        assert_eq!(ts_range(&store, child, 0.0, 1e9), None);

        // The parent keeps maintaining its own index as it writes.
        append_on(&store, main, json!({ "ts": 100, "channel": "even" }));
        assert_eq!(ts_range(&store, main, 90.0, 100.0), Some(vec![9, 10]));

        // The child registers its own: built from the child's content.
        store.register_state_field_index_on(child, "messages", "/ts", FieldIndexKind::Number).unwrap();
        store.register_state_field_index_on(child, "messages", "/channel", FieldIndexKind::String).unwrap();
        let child_items = messages_on(&store, child);
        assert_eq!(child_items.len(), 10 + 7 - 3);
        let expected: Vec<u32> = child_items
            .iter()
            .enumerate()
            .filter(|(_, v)| v["ts"].as_f64().unwrap() >= 1000.0)
            .map(|(i, _)| i as u32)
            .collect();
        let mut got = ts_range(&store, child, 1000.0, 1e9).unwrap();
        got.sort();
        assert_eq!(got, expected);
        assert_eq!(by_channel(&store, child, "child-edit"), Some(vec![0]));
        assert_eq!(by_channel(&store, child, "child").map(|v| v.len()), Some(7));

        // Both live side by side and stay independent as both keep writing.
        append_on(&store, child, json!({ "ts": 7, "channel": "odd" }));
        append_on(&store, main, json!({ "ts": 8, "channel": "odd" }));
        assert_eq!(ts_range(&store, child, 7.0, 8.0), Some(vec![child_items.len() as u32]));
        assert_eq!(ts_range(&store, main, 7.0, 8.0), Some(vec![11]));
        store.sync().unwrap();
    }

    // Both branches' indexes persist and reload against their own branch.
    let store = Store::open(config(&dir)).unwrap();
    assert_eq!(ts_range(&store, main, 7.0, 8.0), Some(vec![11]));
    assert_eq!(by_channel(&store, main, "even"), Some(vec![0, 2, 4, 6, 8, 10]));
    assert_eq!(by_channel(&store, child, "child-edit"), Some(vec![0]));
    assert_eq!(ts_range(&store, child, 7.0, 8.0), Some(vec![14]));
}

#[test]
fn a_child_registering_before_it_writes_reuses_the_parents_index() {
    use chronicle::FieldIndexKind;

    let dir = TempDir::new().unwrap();
    let store = Store::create(config(&dir)).unwrap();
    register_all(&store);
    let main = store.current_branch().id;
    for i in 0..40u32 {
        append_on(&store, main, json!({ "ts": i, "channel": "c" }));
    }
    store.register_state_field_index("messages", "/ts", FieldIndexKind::Number).unwrap();

    let child = store.create_branch("child", None).unwrap().id;
    store.clear_state_caches();
    let before = store.state_reconstruction_count();
    store.register_state_field_index_on(child, "messages", "/ts", FieldIndexKind::Number).unwrap();
    assert_eq!(
        store.state_reconstruction_count(),
        before,
        "registering an index the parent holds at the same chain head must not decode the slot"
    );
    assert_eq!(ts_range(&store, child, 10.0, 12.0), Some(vec![10, 11, 12]));

    // A different field has no donor and is built from the child's content.
    store.register_state_field_index_on(child, "messages", "/channel", FieldIndexKind::String).unwrap();
    assert_eq!(store.state_reconstruction_count(), before + 1);
    assert_eq!(by_channel(&store, child, "c").map(|v| v.len()), Some(40));
    assert_eq!(by_channel(&store, main, "c"), None, "the parent never registered /channel");

    // The copies are independent from here on.
    append_on(&store, child, json!({ "ts": 11, "channel": "c" }));
    assert_eq!(ts_range(&store, child, 11.0, 11.0), Some(vec![11, 40]));
    assert_eq!(ts_range(&store, main, 11.0, 11.0), Some(vec![11]));
}
