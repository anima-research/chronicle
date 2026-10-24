//! Selective forks: `create_branch_with_states` / `create_branch_at_with_states`
//! inherit exactly the named states and nothing else.

use chronicle::{
    Sequence, StateOperation, StateRegistration, StateSelection, StateStrategy, Store,
    StoreConfig, TreeEntry,
};
use serde_json::{json, Value};
use tempfile::TempDir;

fn config(dir: &TempDir) -> StoreConfig {
    StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: true,
    }
}

const LOGS: [&str; 5] = [
    "agent/a/messages",
    "agent/a/notes",
    "agent/ab/messages",
    "agent/b/messages",
    "shared/log",
];

fn seed(store: &Store) {
    for id in LOGS {
        store
            .register_state(StateRegistration {
                id: id.to_string(),
                strategy: StateStrategy::AppendLog {
                    delta_snapshot_every: 4,
                    full_snapshot_every: 3,
                },
                initial_value: None,
            })
            .unwrap();
    }
    store
        .register_state(StateRegistration {
            id: "agent/a/config".to_string(),
            strategy: StateStrategy::Snapshot,
            initial_value: None,
        })
        .unwrap();
    store
        .register_state(StateRegistration {
            id: "agent/a/files".to_string(),
            strategy: StateStrategy::Tree {
                delta_snapshot_every: 4,
                full_snapshot_every: 3,
            },
            initial_value: None,
        })
        .unwrap();
    store
        .register_state(StateRegistration {
            id: "never-written".to_string(),
            strategy: StateStrategy::Snapshot,
            initial_value: None,
        })
        .unwrap();

    for (n, id) in LOGS.iter().enumerate() {
        // Different lengths, so every state crosses a different mix of
        // delta/full snapshot boundaries.
        for i in 0..(7 + n * 6) {
            append(store, id, json!({ "state": id, "i": i }));
        }
    }
    store
        .update_state("agent/a/config", StateOperation::Set(b"{\"model\":\"x\"}".to_vec()))
        .unwrap();
    for i in 0..9u64 {
        store
            .tree_set(
                "agent/a/files",
                &format!("src/f{i}.rs"),
                &TreeEntry { blob_hash: format!("h{i}"), size: i, mode: 0o644 },
            )
            .unwrap();
    }
}

fn append(store: &Store, id: &str, v: Value) {
    store
        .update_state(id, StateOperation::Append(serde_json::to_vec(&v).unwrap()))
        .unwrap();
}

/// Everything observable about one state on the current branch.
fn observe(store: &Store, id: &str) -> (Option<Vec<u8>>, Option<usize>, Option<Vec<u8>>, usize) {
    (
        store.get_state(id).unwrap(),
        store.get_state_len(id).unwrap(),
        store.get_state_tail(id, 3).unwrap(),
        store.tree_list(id, None).map(|l| l.len()).unwrap_or(0),
    )
}

const ALL: [&str; 8] = [
    "agent/a/messages",
    "agent/a/notes",
    "agent/ab/messages",
    "agent/b/messages",
    "shared/log",
    "agent/a/config",
    "agent/a/files",
    "never-written",
];

fn selection() -> StateSelection {
    StateSelection {
        // An exact id, an id also covered by the prefix, and one naming nothing.
        state_ids: vec!["shared/log".into(), "agent/a/notes".into(), "no-such-state".into()],
        // Note "agent/a/" must not pull in "agent/ab/...".
        state_prefixes: vec!["agent/a/".into()],
    }
}

const INHERITED: [&str; 5] = [
    "agent/a/messages",
    "agent/a/notes",
    "agent/a/config",
    "agent/a/files",
    "shared/log",
];

#[test]
fn selective_fork_inherits_exactly_the_selected_states() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(&dir)).unwrap();
    seed(&store);

    let on_parent: Vec<_> = ALL.iter().map(|id| observe(&store, id)).collect();

    store.create_branch_with_states("child", None, &selection()).unwrap();
    store.create_empty_branch("empty", Some("main")).unwrap();

    store.switch_branch("empty").unwrap();
    let on_empty: Vec<_> = ALL.iter().map(|id| observe(&store, id)).collect();

    store.switch_branch("child").unwrap();
    for (i, id) in ALL.iter().enumerate() {
        let got = observe(&store, id);
        if INHERITED.contains(id) {
            assert_eq!(got, on_parent[i], "{id}: inherited state must read as on the parent");
            assert!(got.0.is_some(), "{id}: test must inherit a state that has a value");
        } else {
            assert_eq!(
                got, on_empty[i],
                "{id}: a state that was not inherited must read as on an empty branch"
            );
            assert_eq!(got.0, None, "{id}: not inherited");
        }
    }

    // A state that was not inherited starts a fresh chain on the child...
    append(&store, "agent/b/messages", json!("child-only"));
    assert_eq!(
        store.get_state("agent/b/messages").unwrap().unwrap(),
        b"[\"child-only\"]".to_vec()
    );
    // ...and an inherited one continues the parent's.
    let before = store.get_state_len("agent/a/messages").unwrap().unwrap();
    append(&store, "agent/a/messages", json!("child-continues"));
    assert_eq!(store.get_state_len("agent/a/messages").unwrap(), Some(before + 1));

    // Neither is visible on the parent.
    store.switch_branch("main").unwrap();
    for (i, id) in ALL.iter().enumerate() {
        assert_eq!(observe(&store, id), on_parent[i], "{id}: parent changed");
    }
}

#[test]
fn selective_fork_survives_reopen_and_child_snapshots() {
    let dir = TempDir::new().unwrap();
    let expected: Vec<Value>;
    let parent_before: Vec<u8>;
    {
        let store = Store::create(config(&dir)).unwrap();
        seed(&store);
        parent_before = store.get_state("agent/a/messages").unwrap().unwrap();

        store
            .create_branch_with_states("child", Some("main"), &selection())
            .unwrap();
        store.switch_branch("child").unwrap();
        // Enough appends to cross delta and full snapshot thresholds on the
        // child, continuing the accounting it inherited.
        for i in 0..40 {
            append(&store, "agent/a/messages", json!({ "child": i }));
        }
        expected = serde_json::from_slice(&store.get_state("agent/a/messages").unwrap().unwrap())
            .unwrap();
        assert_eq!(expected.len(), 7 + 40);
        store.sync().unwrap();
    }

    let store = Store::open(config(&dir)).unwrap();
    assert_eq!(store.current_branch().name, "child");
    let got: Vec<Value> =
        serde_json::from_slice(&store.get_state("agent/a/messages").unwrap().unwrap()).unwrap();
    assert_eq!(got, expected);
    assert_eq!(store.get_state("agent/b/messages").unwrap(), None);
    assert_eq!(store.get_state_len("agent/ab/messages").unwrap(), None);

    store.switch_branch("main").unwrap();
    assert_eq!(store.get_state("agent/a/messages").unwrap().unwrap(), parent_before);
    assert!(store.get_state("agent/b/messages").unwrap().is_some());
}

#[test]
fn selective_historical_fork_inherits_selected_states_at_the_sequence() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(&dir)).unwrap();
    seed(&store);

    let at: Sequence = store.current_branch().head;
    let at_fork: Vec<_> = ALL.iter().map(|id| observe(&store, id)).collect();

    // Move the parent on: some selected states change after `at`, some don't.
    for i in 0..11 {
        append(&store, "agent/a/messages", json!({ "later": i }));
        append(&store, "agent/b/messages", json!({ "later": i }));
    }
    store
        .update_state("agent/a/config", StateOperation::Set(b"{\"model\":\"y\"}".to_vec()))
        .unwrap();
    store.tree_remove("agent/a/files", "src/f0.rs").unwrap();

    store
        .create_branch_at_with_states("past", "main", at, &selection())
        .unwrap();
    store.switch_branch("past").unwrap();
    for (i, id) in ALL.iter().enumerate() {
        let got = observe(&store, id);
        if INHERITED.contains(id) {
            assert_eq!(got, at_fork[i], "{id}: must read as the parent did at the sequence");
        } else {
            assert_eq!(got.0, None, "{id}: not inherited");
            assert_eq!(got.1, None, "{id}: not inherited");
        }
    }

    // The full historical fork is unchanged by the selective variant existing.
    store.create_branch_at("past-all", "main", at).unwrap();
    store.switch_branch("past-all").unwrap();
    for (i, id) in ALL.iter().enumerate() {
        assert_eq!(observe(&store, id), at_fork[i], "{id}: full historical fork");
    }
}

#[test]
fn empty_selection_inherits_nothing_and_errors_match_create_branch() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(&dir)).unwrap();
    seed(&store);

    let branch = store
        .create_branch_with_states("bare", None, &StateSelection::default())
        .unwrap();
    assert_eq!(branch.parent, Some(store.current_branch().id));
    assert_eq!(branch.branch_point, Some(store.current_branch().head));
    store.switch_branch("bare").unwrap();
    for id in ALL {
        assert_eq!(store.get_state(id).unwrap(), None, "{id}");
    }
    store.switch_branch("main").unwrap();

    assert!(store
        .create_branch_with_states("bare", None, &selection())
        .is_err());
    assert!(store
        .create_branch_with_states("x", Some("no-such-parent"), &selection())
        .is_err());
    let head = store.current_branch().head;
    assert!(store
        .create_branch_at_with_states("y", "main", Sequence(head.0 + 1), &selection())
        .is_err());
    // A failed historical fork leaves no branch behind.
    assert!(store.list_branches().iter().all(|b| b.name != "y"));
}

/// `at` at or before the parent's own branch point names a sequence the
/// parent inherited rather than wrote, so no record of the parent's marks it:
/// every state takes the chain walk instead of the unchanged-since shortcut.
#[test]
fn historical_fork_at_a_sequence_the_parent_inherited() {
    let dir = TempDir::new().unwrap();
    let store = Store::create(config(&dir)).unwrap();
    seed(&store);

    let early: Sequence = store.current_branch().head;
    let at_early: Vec<_> = ALL.iter().map(|id| observe(&store, id)).collect();
    for i in 0..5 {
        append(&store, "agent/a/messages", json!({ "main-later": i }));
    }
    let fork_point: Sequence = store.current_branch().head;
    let at_fork_point: Vec<_> = ALL.iter().map(|id| observe(&store, id)).collect();

    store.create_branch("mid", None).unwrap();
    store.switch_branch("mid").unwrap();
    for i in 0..5 {
        append(&store, "agent/a/messages", json!({ "mid": i }));
        append(&store, "shared/log", json!({ "mid": i }));
    }
    let on_mid: Sequence = store.current_branch().head;
    let at_on_mid: Vec<_> = ALL.iter().map(|id| observe(&store, id)).collect();
    append(&store, "agent/a/notes", json!("mid-after"));

    for (name, at, expected) in [
        ("from-inherited", early, &at_early),
        ("from-branch-point", fork_point, &at_fork_point),
        ("from-own", on_mid, &at_on_mid),
    ] {
        store.create_branch_at(name, "mid", at).unwrap();
        store.switch_branch(name).unwrap();
        for (i, id) in ALL.iter().enumerate() {
            assert_eq!(&observe(&store, id), &expected[i], "{name}: {id}");
        }
        // The historical branch is writable and keeps its history.
        let before = store.get_state_len("agent/a/messages").unwrap().unwrap();
        for i in 0..9 {
            append(&store, "agent/a/messages", json!({ "hist": i }));
        }
        assert_eq!(store.get_state_len("agent/a/messages").unwrap(), Some(before + 9));
        let all: Vec<Value> =
            serde_json::from_slice(&store.get_state("agent/a/messages").unwrap().unwrap()).unwrap();
        let inherited: Vec<Value> = serde_json::from_slice(expected[0].0.as_ref().unwrap()).unwrap();
        assert_eq!(&all[..before], &inherited[..]);
        store.switch_branch("mid").unwrap();
    }
}
