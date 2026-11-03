//! Tests for long-running stores with automatic snapshots and deltas.
//!
//! These tests verify:
//! 1. Automatic delta snapshot creation at thresholds
//! 2. Automatic full snapshot creation at thresholds
//! 3. State reconstruction from stores with many snapshots
//! 4. Persistence and reopening with snapshots
//! 5. Snapshot behavior across branches
//! 6. Stress testing with many operations
//!
//! NOTE: The Store auto-snapshots after each update when thresholds are reached,
//! so `snapshot_needed()` typically returns None after an update.

use chronicle::{
    StateOperation, StateRegistration, StateStrategy, Store, StoreConfig,
};
use tempfile::TempDir;

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

// =============================================================================
// AUTOMATIC SNAPSHOT TRIGGERING TESTS
// =============================================================================

#[test]
fn test_delta_snapshot_auto_triggered_at_threshold() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store
        .register_state(StateRegistration {
            id: "data".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 5,
                full_snapshot_every: 100,
            },
            initial_value: None,
        })
        .unwrap();

    // Add 4 items - no snapshot yet
    for i in 1..=4 {
        store
            .update_state("data", StateOperation::Append(format!("{}", i).into_bytes()))
            .unwrap();
    }

    let stats = store.get_compaction_stats("data").unwrap();
    assert_eq!(stats.delta_snapshots_since_full, 0, "No delta snapshot yet after 4 ops");

    // Add 5th item - should AUTO-trigger delta snapshot
    store
        .update_state("data", StateOperation::Append(b"5".to_vec()))
        .unwrap();

    // After auto-snapshot, delta count should be 1
    let stats = store.get_compaction_stats("data").unwrap();
    assert_eq!(stats.delta_snapshots_since_full, 1, "Delta snapshot should be auto-triggered after 5 ops");
    assert!(stats.last_delta_snapshot_offset.is_some(), "Should have delta snapshot offset");

    // State should still be correct
    let state = store.get_state("data").unwrap().unwrap();
    let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
    assert_eq!(arr, vec![1, 2, 3, 4, 5]);
}

#[test]
fn test_full_snapshot_auto_triggered_at_threshold() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store
        .register_state(StateRegistration {
            id: "data".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 3,
                full_snapshot_every: 2, // Full snapshot after 2 delta snapshots
            },
            initial_value: None,
        })
        .unwrap();

    // First 3 ops: trigger first delta snapshot
    for i in 1..=3 {
        store
            .update_state("data", StateOperation::Append(format!("{}", i).into_bytes()))
            .unwrap();
    }
    let stats = store.get_compaction_stats("data").unwrap();
    assert_eq!(stats.delta_snapshots_since_full, 1, "First delta snapshot after 3 ops");

    // Next 3 ops: trigger second delta snapshot
    for i in 4..=6 {
        store
            .update_state("data", StateOperation::Append(format!("{}", i).into_bytes()))
            .unwrap();
    }
    let stats = store.get_compaction_stats("data").unwrap();
    assert!(stats.last_full_snapshot_offset.is_some(), "a full snapshot once D·F = 6 ops are covered");
    assert_eq!(stats.delta_snapshots_since_full, 0);

    // State should be correct regardless
    let state = store.get_state("data").unwrap().unwrap();
    let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
    assert_eq!(arr, vec![1, 2, 3, 4, 5, 6]);
}

#[test]
fn test_multiple_snapshot_cycles() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store
        .register_state(StateRegistration {
            id: "log".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 5,
                full_snapshot_every: 3,
            },
            initial_value: None,
        })
        .unwrap();

    let mut expected = Vec::new();

    // Run through multiple full snapshot cycles
    // Each cycle: 3 delta snapshots * 5 ops = 15 ops per full snapshot
    // Auto-snapshotting handles all compaction automatically
    for cycle in 0..3 {
        for delta in 0..3 {
            for op in 0..5 {
                let val = cycle * 15 + delta * 5 + op + 1;
                expected.push(val as i32);
                store
                    .update_state("log", StateOperation::Append(format!("{}", val).into_bytes()))
                    .unwrap();
            }
        }
    }

    // Verify all data is present
    let state = store.get_state("log").unwrap().unwrap();
    let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
    assert_eq!(arr.len(), 45);
    assert_eq!(arr, expected);

    // Verify snapshots were created
    let stats = store.get_compaction_stats("log").unwrap();
    // After 45 ops with delta_every=5, full_every=3:
    // We should have had 9 delta snapshots (45/5), and 3 full snapshots (9/3)
    // After the last full snapshot, delta count resets
    assert!(stats.last_full_snapshot_offset.is_some(), "Should have full snapshots");
}

// =============================================================================
// LONG STORE TESTS
// =============================================================================

#[test]
fn test_long_store_persistence() {
    let dir = TempDir::new().unwrap();

    // First session: create many entries (auto-snapshotting active)
    {
        let store = test_store(&dir);

        store
            .register_state(StateRegistration {
                id: "data".to_string(),
                strategy: StateStrategy::AppendLog {
                    delta_snapshot_every: 20,
                    full_snapshot_every: 3,
                },
                initial_value: None,
            })
            .unwrap();

        for i in 1..=500 {
            store
                .update_state("data", StateOperation::Append(format!("{}", i).into_bytes()))
                .unwrap();
        }

        store.sync().unwrap();
    }

    // Second session: verify data persists
    {
        let store = open_store(&dir);

        let state = store.get_state("data").unwrap().unwrap();
        let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
        assert_eq!(arr.len(), 500);
        assert_eq!(arr[0], 1);
        assert_eq!(arr[499], 500);
    }

    // Third session: add more data
    {
        let store = open_store(&dir);

        for i in 501..=600 {
            store
                .update_state("data", StateOperation::Append(format!("{}", i).into_bytes()))
                .unwrap();
        }

        store.sync().unwrap();
    }

    // Fourth session: verify all data
    {
        let store = open_store(&dir);

        let state = store.get_state("data").unwrap().unwrap();
        let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
        assert_eq!(arr.len(), 600);
        assert_eq!(arr[599], 600);
    }
}

// =============================================================================
// SNAPSHOT WITH BRANCHES TESTS
// =============================================================================

#[test]
fn test_snapshots_on_multiple_branches() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store
        .register_state(StateRegistration {
            id: "log".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 5,
                full_snapshot_every: 2,
            },
            initial_value: None,
        })
        .unwrap();

    // Build up main branch (auto-snapshotting active)
    for i in 1..=20 {
        store
            .update_state("log", StateOperation::Append(format!("\"main_{}\"", i).into_bytes()))
            .unwrap();
    }

    // Create branch and add more
    store.create_branch("feature", None).unwrap();
    store.switch_branch("feature").unwrap();

    for i in 1..=20 {
        store
            .update_state(
                "log",
                StateOperation::Append(format!("\"feature_{}\"", i).into_bytes()),
            )
            .unwrap();
    }

    // Verify feature branch has all data
    let state = store.get_state("log").unwrap().unwrap();
    let arr: Vec<String> = serde_json::from_slice(&state).unwrap();
    assert_eq!(arr.len(), 40); // 20 from main + 20 from feature

    // Verify main branch is unchanged
    store.switch_branch("main").unwrap();
    let state = store.get_state("log").unwrap().unwrap();
    let arr: Vec<String> = serde_json::from_slice(&state).unwrap();
    assert_eq!(arr.len(), 20);
}

#[test]
fn test_independent_snapshot_cycles_per_branch() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store
        .register_state(StateRegistration {
            id: "data".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 5,
                full_snapshot_every: 2,
            },
            initial_value: None,
        })
        .unwrap();

    // Add some data on main
    for i in 1..=10 {
        store
            .update_state("data", StateOperation::Append(format!("{}", i).into_bytes()))
            .unwrap();
    }

    // Create branch A
    store.create_branch("branch_a", None).unwrap();
    store.switch_branch("branch_a").unwrap();

    for i in 11..=25 {
        store
            .update_state("data", StateOperation::Append(format!("{}", i).into_bytes()))
            .unwrap();
    }

    // Create branch B from main
    store.switch_branch("main").unwrap();
    store.create_branch("branch_b", None).unwrap();
    store.switch_branch("branch_b").unwrap();

    for i in 100..=120 {
        store
            .update_state("data", StateOperation::Append(format!("{}", i).into_bytes()))
            .unwrap();
    }

    // Verify each branch
    store.switch_branch("main").unwrap();
    let state = store.get_state("data").unwrap().unwrap();
    let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
    assert_eq!(arr.len(), 10);

    store.switch_branch("branch_a").unwrap();
    let state = store.get_state("data").unwrap().unwrap();
    let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
    assert_eq!(arr.len(), 25);
    assert_eq!(arr[24], 25);

    store.switch_branch("branch_b").unwrap();
    let state = store.get_state("data").unwrap().unwrap();
    let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
    assert_eq!(arr.len(), 31); // 10 from main + 21 from branch_b
    assert_eq!(arr[30], 120);
}

// =============================================================================
// EDIT AND REDACT WITH SNAPSHOTS
// =============================================================================

#[test]
fn test_edit_operations_with_snapshots() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store
        .register_state(StateRegistration {
            id: "items".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 5,
                full_snapshot_every: 3,
            },
            initial_value: None,
        })
        .unwrap();

    // Add items (auto-snapshotting active)
    for i in 1..=10 {
        store
            .update_state("items", StateOperation::Append(format!("{}", i).into_bytes()))
            .unwrap();
    }

    // Edit some items
    store
        .update_state(
            "items",
            StateOperation::Edit {
                index: 0,
                new_value: b"100".to_vec(),
            },
        )
        .unwrap();

    store
        .update_state(
            "items",
            StateOperation::Edit {
                index: 5,
                new_value: b"500".to_vec(),
            },
        )
        .unwrap();

    // Verify
    let state = store.get_state("items").unwrap().unwrap();
    let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
    assert_eq!(arr[0], 100);
    assert_eq!(arr[5], 500);
    assert_eq!(arr[9], 10);
}

#[test]
fn test_redact_without_auto_snapshots() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    // Use high thresholds so auto-snapshotting won't trigger
    store
        .register_state(StateRegistration {
            id: "messages".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 1000,
                full_snapshot_every: 100,
            },
            initial_value: None,
        })
        .unwrap();

    // Add messages (NO auto-snapshotting with high thresholds)
    for i in 1..=20 {
        store
            .update_state(
                "messages",
                StateOperation::Append(format!("\"msg_{}\"", i).into_bytes()),
            )
            .unwrap();
    }

    let before = store.get_state("messages").unwrap().unwrap();
    let before_arr: Vec<String> = serde_json::from_slice(&before).unwrap();
    assert_eq!(before_arr.len(), 20);

    // Redact messages at indices 5-9 (end is exclusive based on unit test)
    store
        .update_state("messages", StateOperation::Redact { start: 5, end: 10 })
        .unwrap();

    // Verify redaction worked
    let state = store.get_state("messages").unwrap().unwrap();
    let arr: Vec<String> = serde_json::from_slice(&state).unwrap();

    assert_eq!(arr.len(), 15, "Should have 15 messages after redacting 5");
    assert_eq!(arr[4], "msg_5");
    assert_eq!(arr[5], "msg_11");
}

#[test]
fn test_redact_with_auto_snapshots() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    // Use LOW thresholds so auto-snapshotting WILL trigger
    store
        .register_state(StateRegistration {
            id: "messages".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 5,
                full_snapshot_every: 3,
            },
            initial_value: None,
        })
        .unwrap();

    // Add messages (auto-snapshotting active)
    for i in 1..=20 {
        store
            .update_state(
                "messages",
                StateOperation::Append(format!("\"msg_{}\"", i).into_bytes()),
            )
            .unwrap();
    }

    let before = store.get_state("messages").unwrap().unwrap();
    let before_arr: Vec<String> = serde_json::from_slice(&before).unwrap();
    println!("Before redact: len={}", before_arr.len());

    let stats_before = store.get_compaction_stats("messages").unwrap();
    println!("Stats before redact: {:?}", stats_before);

    // Redact messages at indices 5-9 (end is exclusive)
    store
        .update_state("messages", StateOperation::Redact { start: 5, end: 10 })
        .unwrap();

    let stats_after = store.get_compaction_stats("messages").unwrap();
    println!("Stats after redact: {:?}", stats_after);

    // Verify redaction worked
    let state = store.get_state("messages").unwrap().unwrap();
    let arr: Vec<String> = serde_json::from_slice(&state).unwrap();
    println!("After redact: len={}, first_few={:?}", arr.len(), &arr[..6.min(arr.len())]);

    assert_eq!(arr.len(), 15, "Should have 15 messages after redacting 5, got {}", arr.len());
}

#[test]
fn test_set_on_append_log_forces_full_snapshot_without_resurrection() {
    let dir = TempDir::new().unwrap();
    {
        let store = test_store(&dir);
        store
            .register_state(StateRegistration {
                id: "messages".to_string(),
                strategy: StateStrategy::AppendLog {
                    delta_snapshot_every: 3,
                    full_snapshot_every: 100,
                },
                initial_value: None,
            })
            .unwrap();

        // The Set is the third operation, exactly at the delta threshold.
        // A delta must not walk past it and collect these replaced appends.
        for value in ["removed_1", "removed_2"] {
            store
                .update_state(
                    "messages",
                    StateOperation::Append(serde_json::to_vec(value).unwrap()),
                )
                .unwrap();
        }
        store
            .update_state(
                "messages",
                StateOperation::Set(serde_json::to_vec(&vec!["kept"]).unwrap()),
            )
            .unwrap();

        let state = store.get_state("messages").unwrap().unwrap();
        let messages: Vec<String> = serde_json::from_slice(&state).unwrap();
        assert_eq!(messages, vec!["kept"]);

        // The Set blocks delta snapshots (a delta would walk past it and
        // resurrect the replaced appends). It no longer forces an immediate
        // full — the raw tail rides until the size-aware full fires.
        let stats = store.get_compaction_stats("messages").unwrap();
        assert!(stats.last_delta_snapshot_offset.is_none());

        // Drive past the size-aware full threshold (delta 3 × full 100 = 300
        // ops floor) and verify the scheduled full lands and content is right.
        for i in 0..300 {
            store
                .update_state(
                    "messages",
                    StateOperation::Append(serde_json::to_vec(&format!("m{}", i)).unwrap()),
                )
                .unwrap();
        }
        let stats = store.get_compaction_stats("messages").unwrap();
        assert!(
            stats.last_full_snapshot_offset.is_some(),
            "size-aware full snapshot should have fired after 300 ops"
        );
        let state = store.get_state("messages").unwrap().unwrap();
        let messages: Vec<String> = serde_json::from_slice(&state).unwrap();
        assert_eq!(messages[0], "kept");
        assert_eq!(messages.len(), 301);
    }

    // Verify the durable reconstruction, not only the writer's cache.
    let reopened = open_store(&dir);
    let state = reopened.get_state("messages").unwrap().unwrap();
    let messages: Vec<String> = serde_json::from_slice(&state).unwrap();
    assert_eq!(messages.len(), 301);
    assert_eq!(messages[0], "kept");
    assert!(
        !messages.iter().any(|m| m.starts_with("removed_")),
        "replaced appends resurrected across reopen"
    );
}

/// Regression: between a Set and the next full snapshot, the optimized read
/// paths (`get_state_tail` fast path, `iter_state_items`) must treat Set as a
/// full-state terminal. Both used to walk past it, resurrecting replaced
/// appends (and `iter_state_items` dropped the Set's value entirely).
#[test]
fn test_set_on_append_log_read_paths_before_snapshot() {
    let dir = TempDir::new().unwrap();
    {
        let store = test_store(&dir);
        store
            .register_state(StateRegistration {
                id: "messages".to_string(),
                strategy: StateStrategy::AppendLog {
                    // High thresholds: no snapshot fires, so reads cross the raw Set.
                    delta_snapshot_every: 100,
                    full_snapshot_every: 100,
                },
                initial_value: None,
            })
            .unwrap();

        for value in ["removed_1", "removed_2"] {
            store
                .update_state(
                    "messages",
                    StateOperation::Append(serde_json::to_vec(value).unwrap()),
                )
                .unwrap();
        }
        store
            .update_state(
                "messages",
                StateOperation::Set(
                    serde_json::to_vec(&vec!["kept_1", "kept_2", "kept_3"]).unwrap(),
                ),
            )
            .unwrap();
        store
            .update_state(
                "messages",
                StateOperation::Append(serde_json::to_vec("kept_4").unwrap()),
            )
            .unwrap();

        assert_eq!(store.get_state_len("messages").unwrap(), Some(4));

        // Tail fast path (count < item_count) must not walk past the Set.
        let tail = store.get_state_tail("messages", 2).unwrap().unwrap();
        let messages: Vec<String> = serde_json::from_slice(&tail).unwrap();
        assert_eq!(messages, vec!["kept_3", "kept_4"]);

        // Tail request larger than the Set's own array must stop at the Set.
        let tail = store.get_state_tail("messages", 3).unwrap().unwrap();
        let messages: Vec<String> = serde_json::from_slice(&tail).unwrap();
        assert_eq!(messages, vec!["kept_2", "kept_3", "kept_4"]);

        // Item iteration must apply the Set, not skip it.
        let items: Vec<String> = store
            .iter_state_items("messages")
            .unwrap()
            .unwrap()
            .map(|r| serde_json::from_value(r.unwrap()).unwrap())
            .collect();
        assert_eq!(items, vec!["kept_1", "kept_2", "kept_3", "kept_4"]);
    }

    // Same reads against the durable log after reopen.
    let reopened = open_store(&dir);
    let tail = reopened.get_state_tail("messages", 2).unwrap().unwrap();
    let messages: Vec<String> = serde_json::from_slice(&tail).unwrap();
    assert_eq!(messages, vec!["kept_3", "kept_4"]);
    let items: Vec<String> = reopened
        .iter_state_items("messages")
        .unwrap()
        .unwrap()
        .map(|r| serde_json::from_value(r.unwrap()).unwrap())
        .collect();
    assert_eq!(items, vec!["kept_1", "kept_2", "kept_3", "kept_4"]);
}

// =============================================================================
// PERFORMANCE TESTS
// =============================================================================

#[test]
fn test_length_tail_and_append_do_not_rebuild_the_state() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store
        .register_state(StateRegistration {
            id: "messages".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 100,
                full_snapshot_every: 10,
            },
            initial_value: None,
        })
        .unwrap();

    for i in 1..=1_500 {
        store
            .update_state(
                "messages",
                StateOperation::Append(format!("\"message_{}\"", i).into_bytes()),
            )
            .unwrap();
    }

    store.clear_state_caches();
    store.set_auto_snapshot(false);
    let rebuilds = store.state_reconstruction_count();
    assert_eq!(store.get_state_len("messages").unwrap().unwrap(), 1_500);
    let tail = store.get_state_tail("messages", 10).unwrap().unwrap();
    let arr: Vec<String> = serde_json::from_slice(&tail).unwrap();
    assert_eq!(arr.len(), 10);
    assert_eq!(arr[9], "message_1500");
    store
        .update_state(
            "messages",
            StateOperation::Append(b"\"final_message\"".to_vec()),
        )
        .unwrap();
    assert_eq!(
        store.state_reconstruction_count(),
        rebuilds,
        "a length, a tail or an append rebuilt the state"
    );
}

// =============================================================================
// STRESS TESTS
// =============================================================================

#[test]
fn test_mixed_operations_stress() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store
        .register_state(StateRegistration {
            id: "data".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 25,
                full_snapshot_every: 5,
            },
            initial_value: None,
        })
        .unwrap();

    let mut expected_len = 0;

    for cycle in 0..20 {
        // Add 30 items
        for i in 0..30 {
            let val = cycle * 100 + i;
            store
                .update_state("data", StateOperation::Append(format!("{}", val).into_bytes()))
                .unwrap();
            expected_len += 1;
        }

        // Edit 5 items (if we have enough)
        if expected_len >= 10 {
            for i in 0..5 {
                store
                    .update_state(
                        "data",
                        StateOperation::Edit {
                            index: i,
                            new_value: b"999".to_vec(),
                        },
                    )
                    .unwrap();
            }
        }

        // Redact 3 items from middle (if we have enough)
        if expected_len >= 20 {
            let mid = expected_len / 2;
            store
                .update_state(
                    "data",
                    StateOperation::Redact {
                        start: mid,
                        end: mid + 3,
                    },
                )
                .unwrap();
            expected_len -= 3;
        }
    }

    // Verify final state is reconstructable
    let state = store.get_state("data").unwrap().unwrap();
    let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();

    // First 5 should be edited to 999
    for i in 0..5 {
        assert_eq!(arr[i], 999, "Item {} should be 999", i);
    }
}

// =============================================================================
// PERSISTENCE WITH MANY SNAPSHOTS
// =============================================================================

#[test]
fn test_reopen_store_many_times_with_snapshots() {
    let dir = TempDir::new().unwrap();

    // Initial creation
    {
        let store = test_store(&dir);
        store
            .register_state(StateRegistration {
                id: "data".to_string(),
                strategy: StateStrategy::AppendLog {
                    delta_snapshot_every: 10,
                    full_snapshot_every: 3,
                },
                initial_value: None,
            })
            .unwrap();
        store.sync().unwrap();
    }

    // Many open/write/close cycles (auto-snapshotting handles compaction)
    for cycle in 0..10 {
        let store = open_store(&dir);

        let start = cycle * 20 + 1;
        for i in start..start + 20 {
            store
                .update_state("data", StateOperation::Append(format!("{}", i).into_bytes()))
                .unwrap();
        }

        store.sync().unwrap();
    }

    // Final verification
    {
        let store = open_store(&dir);
        let state = store.get_state("data").unwrap().unwrap();
        let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
        assert_eq!(arr.len(), 200);
        assert_eq!(arr[0], 1);
        assert_eq!(arr[199], 200);

        // Verify snapshots exist
        let stats = store.get_compaction_stats("data").unwrap();
        assert!(stats.last_full_snapshot_offset.is_some(), "Should have full snapshots");
    }
}
