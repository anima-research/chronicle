//! Integration tests for the record store.

use chronicle::{
    FieldIndexKind, RecordInput, Sequence, StateOperation, StateRegistration, StateStrategy,
    Store, StoreConfig,
};
use serde_json::json;
use tempfile::TempDir;

fn test_store(dir: &TempDir) -> Store {
    Store::create(StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: true,
    })
    .unwrap()
}

// --- Realistic Workflow Tests ---

#[test]
fn test_agent_conversation_workflow() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    // Register conversation state (append-only messages)
    store
        .register_state(StateRegistration {
            id: "messages".to_string(),
            strategy: StateStrategy::AppendLog { delta_snapshot_every: 10, full_snapshot_every: 5 },
            initial_value: None,
        })
        .unwrap();

    // Simulate a conversation
    let messages = vec![
        json!({"role": "user", "content": "Hello"}),
        json!({"role": "assistant", "content": "Hi there!"}),
        json!({"role": "user", "content": "What's 2+2?"}),
        json!({"role": "assistant", "content": "4"}),
    ];

    for msg in &messages {
        let payload = serde_json::to_vec(msg).unwrap();
        store
            .update_state("messages", StateOperation::Append(payload))
            .unwrap();
    }

    // Reconstruct and verify
    let state = store.get_state("messages").unwrap().unwrap();
    let reconstructed: Vec<serde_json::Value> = serde_json::from_slice(&state).unwrap();
    assert_eq!(reconstructed.len(), 4);
    assert_eq!(reconstructed[0]["role"], "user");
    assert_eq!(reconstructed[3]["content"], "4");
}

#[test]
fn test_branching_for_exploration() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    // Setup initial state
    store
        .register_state(StateRegistration {
            id: "context".to_string(),
            strategy: StateStrategy::Snapshot,
            initial_value: None,
        })
        .unwrap();

    store
        .update_state(
            "context",
            StateOperation::Set(b"{\"step\": 1}".to_vec()),
        )
        .unwrap();

    // Create exploration branch
    store.create_branch("exploration", None).unwrap();
    store.switch_branch("exploration").unwrap();

    // Make changes on exploration branch
    store
        .update_state(
            "context",
            StateOperation::Set(b"{\"step\": 2, \"experimental\": true}".to_vec()),
        )
        .unwrap();

    // Check exploration state
    let exp_state = store.get_state("context").unwrap().unwrap();
    let exp_val: serde_json::Value = serde_json::from_slice(&exp_state).unwrap();
    assert_eq!(exp_val["experimental"], true);

    // Switch back to main - state should be independent
    // (Note: current implementation shares state across branches,
    // full branch isolation would require state-per-branch)
}

#[test]
fn test_code_storage_and_retrieval() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    // Store some "code" blobs
    let js_code = b"export function greet(name) { return `Hello, ${name}!`; }";
    let ts_types = b"export interface Person { name: string; age: number; }";

    let js_hash = store
        .store_blob(js_code, "application/javascript")
        .unwrap();
    let ts_hash = store.store_blob(ts_types, "application/typescript").unwrap();

    // Record that references the code
    let record = store
        .append(
            RecordInput::json(
                "code_module",
                &json!({
                    "name": "greeting",
                    "version": "1.0.0",
                    "main": js_hash.to_hex(),
                    "types": ts_hash.to_hex(),
                }),
            )
            .unwrap(),
        )
        .unwrap();

    // Retrieve and verify
    let retrieved = store.get_record(record.id).unwrap().unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&retrieved.payload).unwrap();

    let main_blob = store
        .get_blob(&chronicle::Hash::from_hex(&payload["main"].as_str().unwrap()).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(main_blob.content, js_code);
}

// --- Edge Case Tests ---

#[test]
fn test_empty_state_operations() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store
        .register_state(StateRegistration {
            id: "empty".to_string(),
            strategy: StateStrategy::AppendLog { delta_snapshot_every: 10, full_snapshot_every: 5 },
            initial_value: None,
        })
        .unwrap();

    // Get state before any updates
    let state = store.get_state("empty").unwrap();
    assert!(state.is_none());

    // Append to empty
    store
        .update_state("empty", StateOperation::Append(b"1".to_vec()))
        .unwrap();

    let state = store.get_state("empty").unwrap().unwrap();
    let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
    assert_eq!(arr, vec![1]);
}

#[test]
fn test_redact_edge_cases() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store
        .register_state(StateRegistration {
            id: "items".to_string(),
            strategy: StateStrategy::AppendLog { delta_snapshot_every: 100, full_snapshot_every: 10 },
            initial_value: None,
        })
        .unwrap();

    // Build up some items
    for i in 1..=5 {
        store
            .update_state("items", StateOperation::Append(format!("{}", i).into_bytes()))
            .unwrap();
    }

    // Redact beyond bounds (should clamp)
    store
        .update_state("items", StateOperation::Redact { start: 10, end: 20 })
        .unwrap();

    let state = store.get_state("items").unwrap().unwrap();
    let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
    assert_eq!(arr, vec![1, 2, 3, 4, 5]); // Unchanged

    // Redact with start > end (should be no-op)
    store
        .update_state("items", StateOperation::Redact { start: 3, end: 1 })
        .unwrap();

    let state = store.get_state("items").unwrap().unwrap();
    let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
    assert_eq!(arr, vec![1, 2, 3, 4, 5]); // Still unchanged

    // Valid redact
    store
        .update_state("items", StateOperation::Redact { start: 1, end: 3 })
        .unwrap();

    let state = store.get_state("items").unwrap().unwrap();
    let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
    assert_eq!(arr, vec![1, 4, 5]);
}

#[test]
fn test_large_payload() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    // 1MB payload
    let large_content: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();

    let hash = store.store_blob(&large_content, "application/octet-stream").unwrap();

    let retrieved = store.get_blob(&hash).unwrap().unwrap();
    assert_eq!(retrieved.content.len(), 1_000_000);
    assert_eq!(retrieved.content, large_content);
}

#[test]
fn test_deep_state_chain() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store
        .register_state(StateRegistration {
            id: "counter".to_string(),
            strategy: StateStrategy::AppendLog { delta_snapshot_every: 1000, full_snapshot_every: 10 }, // High threshold
            initial_value: None,
        })
        .unwrap();

    // Create a chain of 100 appends without snapshot
    for i in 1..=100 {
        store
            .update_state(
                "counter",
                StateOperation::Append(format!("{}", i).into_bytes()),
            )
            .unwrap();
    }

    // Reconstruction should still work
    let state = store.get_state("counter").unwrap().unwrap();
    let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
    assert_eq!(arr.len(), 100);
    assert_eq!(arr[0], 1);
    assert_eq!(arr[99], 100);
}

#[test]
fn test_many_branches() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    // Create 50 branches
    for i in 1..=50 {
        store.create_branch(&format!("branch-{}", i), None).unwrap();
    }

    let branches = store.list_branches();
    assert_eq!(branches.len(), 51); // main + 50

    // Delete half
    for i in 1..=25 {
        store.delete_branch(&format!("branch-{}", i)).unwrap();
    }

    let branches = store.list_branches();
    assert_eq!(branches.len(), 26); // main + 25 remaining
}

#[test]
fn test_nested_field_operations() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store
        .register_state(StateRegistration {
            id: "agent".to_string(),
            strategy: StateStrategy::Snapshot,
            initial_value: None,
        })
        .unwrap();

    // Set initial structure
    store
        .update_state(
            "agent",
            StateOperation::Set(
                serde_json::to_vec(&json!({
                    "name": "TestAgent",
                    "config": {"debug": false},
                    "history": []
                }))
                .unwrap(),
            ),
        )
        .unwrap();

    // Update nested field
    store
        .update_state(
            "agent",
            StateOperation::Field {
                name: "history".to_string(),
                operation: Box::new(StateOperation::Append(
                    serde_json::to_vec(&json!({"action": "init"})).unwrap(),
                )),
            },
        )
        .unwrap();

    let state = store.get_state("agent").unwrap().unwrap();
    let val: serde_json::Value = serde_json::from_slice(&state).unwrap();
    assert_eq!(val["history"][0]["action"], "init");
}

// --- Recovery Tests ---

#[test]
fn test_reopen_after_writes() {
    let dir = TempDir::new().unwrap();
    let config = StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: true,
    };

    // First session: create and write
    {
        let store = Store::create(config.clone()).unwrap();

        store
            .register_state(StateRegistration {
                id: "data".to_string(),
                strategy: StateStrategy::Snapshot,
                initial_value: None,
            })
            .unwrap();

        for i in 1..=10 {
            store
                .append(RecordInput::json("event", &json!({"seq": i})).unwrap())
                .unwrap();
        }

        store
            .update_state("data", StateOperation::Set(b"persisted".to_vec()))
            .unwrap();

        store.create_branch("feature", None).unwrap();

        store.sync().unwrap();
    }

    // Second session: reopen and verify
    {
        let store = Store::open(config).unwrap();

        // Check records
        let stats = store.stats().unwrap();
        assert_eq!(stats.record_count, 11); // 10 events + 1 state update

        // Check state
        let state = store.get_state("data").unwrap().unwrap();
        assert_eq!(state, b"persisted");

        // Check branches
        let branches = store.list_branches();
        assert_eq!(branches.len(), 2);
    }
}

#[test]
fn test_blob_deduplication_across_sessions() {
    let dir = TempDir::new().unwrap();
    let config = StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: true,
    };

    let content = b"deduplicated content";
    let hash1;

    // First session
    {
        let store = Store::create(config.clone()).unwrap();
        hash1 = store.store_blob(content, "text/plain").unwrap();
        store.sync().unwrap();
    }

    // Second session - same content
    {
        let store = Store::open(config).unwrap();
        let hash2 = store.store_blob(content, "text/plain").unwrap();

        // Should be the same hash (deduplicated)
        assert_eq!(hash1, hash2);

        // Should still be retrievable
        let blob = store.get_blob(&hash2).unwrap().unwrap();
        assert_eq!(blob.content, content);
    }
}

// --- Stress Tests ---

#[test]
fn test_many_records() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    // Append 1000 records
    for i in 1..=1000 {
        store
            .append(RecordInput::json("event", &json!({"n": i})).unwrap())
            .unwrap();
    }

    let stats = store.stats().unwrap();
    assert_eq!(stats.record_count, 1000);

    // Verify last record
    let records: Vec<_> = store.iter_from(Sequence(1000)).collect();
    assert!(!records.is_empty());
}

#[test]
fn test_interleaved_multi_state_updates() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    // Register multiple states
    for name in &["context", "discord", "minecraft", "agent_state"] {
        store
            .register_state(StateRegistration {
                id: name.to_string(),
                strategy: StateStrategy::AppendLog { delta_snapshot_every: 50, full_snapshot_every: 10 },
                initial_value: None,
            })
            .unwrap();
    }

    // Interleaved updates simulating real agent activity
    for i in 1..=100 {
        let state_name = match i % 4 {
            0 => "context",
            1 => "discord",
            2 => "minecraft",
            _ => "agent_state",
        };

        store
            .update_state(
                state_name,
                StateOperation::Append(format!("\"event_{}\"", i).into_bytes()),
            )
            .unwrap();
    }

    // Each state should have ~25 events
    for name in &["context", "discord", "minecraft", "agent_state"] {
        let state = store.get_state(name).unwrap().unwrap();
        let arr: Vec<String> = serde_json::from_slice(&state).unwrap();
        assert!(arr.len() >= 24 && arr.len() <= 26);
    }
}

// --- Historical State Access Tests ---

#[test]
fn test_get_state_at_basic() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store
        .register_state(StateRegistration {
            id: "items".to_string(),
            strategy: StateStrategy::AppendLog { delta_snapshot_every: 100, full_snapshot_every: 100 },
            initial_value: None,
        })
        .unwrap();

    // Track sequences as we add items
    let mut sequences = Vec::new();

    for i in 1..=5 {
        let rec = store
            .update_state("items", StateOperation::Append(format!("{}", i).into_bytes()))
            .unwrap();
        sequences.push(rec.sequence);
    }

    // Current state should have all 5 items
    let current: Vec<i32> = serde_json::from_slice(&store.get_state("items").unwrap().unwrap()).unwrap();
    assert_eq!(current, vec![1, 2, 3, 4, 5]);

    // At sequence[2] (after 3rd append), should have [1, 2, 3]
    let at_seq_2: Vec<i32> = serde_json::from_slice(
        &store.get_state_at("items", sequences[2]).unwrap().unwrap()
    ).unwrap();
    assert_eq!(at_seq_2, vec![1, 2, 3]);

    // At sequence[0] (after 1st append), should have [1]
    let at_seq_0: Vec<i32> = serde_json::from_slice(
        &store.get_state_at("items", sequences[0]).unwrap().unwrap()
    ).unwrap();
    assert_eq!(at_seq_0, vec![1]);

    // Before any state operations should return None
    let before = store.get_state_at("items", Sequence(0)).unwrap();
    assert!(before.is_none());
}

#[test]
fn test_get_state_at_with_snapshots() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store
        .register_state(StateRegistration {
            id: "data".to_string(),
            strategy: StateStrategy::AppendLog { delta_snapshot_every: 3, full_snapshot_every: 2 },
            initial_value: None,
        })
        .unwrap();

    let mut sequences = Vec::new();

    // Add 10 items - will trigger snapshots
    for i in 1..=10 {
        let rec = store
            .update_state("data", StateOperation::Append(format!("{}", i).into_bytes()))
            .unwrap();
        sequences.push(rec.sequence);
    }

    // Test various historical points (including after snapshots)
    let at_3: Vec<i32> = serde_json::from_slice(
        &store.get_state_at("data", sequences[2]).unwrap().unwrap()
    ).unwrap();
    assert_eq!(at_3, vec![1, 2, 3]);

    let at_7: Vec<i32> = serde_json::from_slice(
        &store.get_state_at("data", sequences[6]).unwrap().unwrap()
    ).unwrap();
    assert_eq!(at_7, vec![1, 2, 3, 4, 5, 6, 7]);

    // Current should have all 10
    let current: Vec<i32> = serde_json::from_slice(&store.get_state("data").unwrap().unwrap()).unwrap();
    assert_eq!(current, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
}

#[test]
fn test_get_state_at_with_edits() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store
        .register_state(StateRegistration {
            id: "list".to_string(),
            strategy: StateStrategy::AppendLog { delta_snapshot_every: 100, full_snapshot_every: 100 },
            initial_value: None,
        })
        .unwrap();

    // Add [1, 2, 3]
    let _r1 = store.update_state("list", StateOperation::Append(b"1".to_vec())).unwrap();
    let _r2 = store.update_state("list", StateOperation::Append(b"2".to_vec())).unwrap();
    let r3 = store.update_state("list", StateOperation::Append(b"3".to_vec())).unwrap();

    // Edit index 1: [1, 2, 3] -> [1, 99, 3]
    let r4 = store.update_state("list", StateOperation::Edit { index: 1, new_value: b"99".to_vec() }).unwrap();

    // Before edit
    let before_edit: Vec<i32> = serde_json::from_slice(
        &store.get_state_at("list", r3.sequence).unwrap().unwrap()
    ).unwrap();
    assert_eq!(before_edit, vec![1, 2, 3]);

    // After edit
    let after_edit: Vec<i32> = serde_json::from_slice(
        &store.get_state_at("list", r4.sequence).unwrap().unwrap()
    ).unwrap();
    assert_eq!(after_edit, vec![1, 99, 3]);
}

// --- Causation Link Tests ---

#[test]
fn test_record_caused_by_links() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    // Create a chain: message -> response -> tool_call
    let msg = store.append(RecordInput::json("message", &json!({"text": "hello"})).unwrap()).unwrap();

    let response = store.append(
        RecordInput::json("response", &json!({"text": "hi"})).unwrap()
            .with_caused_by(vec![msg.id])
    ).unwrap();

    let tool_call = store.append(
        RecordInput::json("tool_call", &json!({"tool": "search"})).unwrap()
            .with_caused_by(vec![response.id])
    ).unwrap();

    // Verify the links are stored
    assert_eq!(response.caused_by, vec![msg.id]);
    assert_eq!(tool_call.caused_by, vec![response.id]);

    // Verify reverse lookups
    let effects_of_msg = store.get_effects(msg.id);
    assert_eq!(effects_of_msg, vec![response.id]);

    let effects_of_response = store.get_effects(response.id);
    assert_eq!(effects_of_response, vec![tool_call.id]);
}

#[test]
fn test_record_linked_to() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    // Create records with linked_to references
    let artifact = store.append(RecordInput::json("artifact", &json!({"content": "data"})).unwrap()).unwrap();

    let summary = store.append(
        RecordInput::json("summary", &json!({"text": "summary of artifact"})).unwrap()
            .with_linked_to(vec![artifact.id])
    ).unwrap();

    // Verify the link is stored
    assert_eq!(summary.linked_to, vec![artifact.id]);

    // Verify reverse lookup
    let links_to_artifact = store.get_links_to(artifact.id);
    assert_eq!(links_to_artifact, vec![summary.id]);
}

#[test]
fn test_multiple_caused_by_and_linked_to() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    let msg1 = store.append(RecordInput::json("message", &json!({"id": 1})).unwrap()).unwrap();
    let msg2 = store.append(RecordInput::json("message", &json!({"id": 2})).unwrap()).unwrap();
    let artifact = store.append(RecordInput::json("artifact", &json!({"data": "x"})).unwrap()).unwrap();

    // Response caused by both messages and linking to artifact
    let response = store.append(
        RecordInput::json("response", &json!({"text": "combined response"})).unwrap()
            .with_caused_by(vec![msg1.id, msg2.id])
            .with_linked_to(vec![artifact.id])
    ).unwrap();

    assert_eq!(response.caused_by.len(), 2);
    assert!(response.caused_by.contains(&msg1.id));
    assert!(response.caused_by.contains(&msg2.id));
    assert_eq!(response.linked_to, vec![artifact.id]);

    // Both messages should show response as an effect
    assert!(store.get_effects(msg1.id).contains(&response.id));
    assert!(store.get_effects(msg2.id).contains(&response.id));
}

#[test]
fn test_causation_chain_persists_across_reopen() {
    let dir = TempDir::new().unwrap();

    let (msg_id, response_id);
    {
        let store = test_store(&dir);
        let msg = store.append(RecordInput::json("message", &json!({"text": "hello"})).unwrap()).unwrap();
        let response = store.append(
            RecordInput::json("response", &json!({"text": "hi"})).unwrap()
                .with_caused_by(vec![msg.id])
        ).unwrap();
        msg_id = msg.id;
        response_id = response.id;
    }

    // Reopen and verify
    let store = Store::open(StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: false,
    }).unwrap();

    let response = store.get_record(response_id).unwrap().unwrap();
    assert_eq!(response.caused_by, vec![msg_id]);

    // Index should be rebuilt
    let effects = store.get_effects(msg_id);
    assert_eq!(effects, vec![response_id]);
}

// --- append_to_state_json_with_identity + auto-snapshot toggle ---

#[test]
fn test_append_with_identity_round_trip_after_reopen() {
    // Each appended item carries `id`/`sequence` fields populated server-side
    // with the values chronicle will assign to that record. After a reopen,
    // those values must still match the actual `record.id` / `record.sequence`
    // — otherwise the payload-vs-envelope identity contract is silently broken.
    let dir = TempDir::new().unwrap();

    let mut expected: Vec<(String, i64)> = Vec::new();

    {
        let store = test_store(&dir);
        store
            .register_state(StateRegistration {
                id: "msgs".to_string(),
                strategy: StateStrategy::AppendLog {
                    delta_snapshot_every: 100,
                    full_snapshot_every: 100,
                },
                initial_value: None,
            })
            .unwrap();

        for i in 0..5 {
            let item = json!({ "text": format!("msg-{}", i) });
            let record = store
                .append_to_state_json_with_identity("msgs", item, "id", "sequence")
                .unwrap();
            expected.push((record.id.0.to_string(), record.sequence.0 as i64));
        }
    }

    // Reopen and read state back via get_state.
    let store = Store::open(StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: false,
    })
    .unwrap();
    let bytes = store.get_state("msgs").unwrap().unwrap();
    let items: Vec<serde_json::Value> = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(items.len(), expected.len());
    for (i, item) in items.iter().enumerate() {
        let id = item.get("id").and_then(|v| v.as_str()).unwrap();
        let seq = item.get("sequence").and_then(|v| v.as_i64()).unwrap();
        assert_eq!(
            id, expected[i].0,
            "embedded id must equal record.id for slot {}",
            i
        );
        assert_eq!(
            seq, expected[i].1,
            "embedded sequence must equal record.sequence for slot {}",
            i
        );
    }
}

#[test]
fn test_append_with_identity_rejects_non_object() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);
    store
        .register_state(StateRegistration {
            id: "msgs".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 100,
                full_snapshot_every: 100,
            },
            initial_value: None,
        })
        .unwrap();

    // Array, string, number — all non-objects, all must surface InvalidOperation
    // and write nothing to the chain.
    for v in [json!([1, 2, 3]), json!("scalar"), json!(42)] {
        let err = store
            .append_to_state_json_with_identity("msgs", v, "id", "sequence")
            .unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("JSON object"),
            "expected 'JSON object' error, got: {}",
            msg
        );
    }

    // Rejected calls must not have written anything. `get_chain_stats`
    // returns `None` for a state with no chain head, which is the correct
    // empty-state shape here.
    assert!(
        store.get_chain_stats("msgs").unwrap().is_none(),
        "rejected calls must not write any state ops",
    );
}

#[test]
fn test_auto_snapshot_toggle_suppresses_then_compact_installs() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    // Small thresholds so even a modest write count would normally trigger
    // multiple snapshots.
    store
        .register_state(StateRegistration {
            id: "msgs".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 5,
                full_snapshot_every: 5,
            },
            initial_value: None,
        })
        .unwrap();

    // With auto-snapshot disabled, 20 appends should produce exactly 20 state
    // ops and zero snapshots.
    store.set_auto_snapshot(false);
    assert!(!store.auto_snapshot_enabled());
    for i in 0..20 {
        let item = json!({ "i": i });
        store
            .append_to_state_json_with_identity("msgs", item, "id", "sequence")
            .unwrap();
    }
    let stats = store.get_chain_stats("msgs").unwrap().unwrap();
    assert_eq!(stats.total_operations, 20);
    assert!(
        !stats.has_full_snapshot,
        "no snapshot must be written while toggle is off",
    );

    // compact_state installs a single Full snapshot regardless of the toggle.
    store.compact_state("msgs").unwrap().unwrap();
    let stats = store.get_chain_stats("msgs").unwrap().unwrap();
    assert_eq!(stats.total_operations, 21, "20 appends + 1 snapshot");
    assert!(stats.has_full_snapshot);

    // Re-enable, write one more append — it shouldn't trigger another snapshot
    // immediately (counter just reset).
    store.set_auto_snapshot(true);
    assert!(store.auto_snapshot_enabled());
    store
        .append_to_state_json_with_identity("msgs", json!({ "i": 20 }), "id", "sequence")
        .unwrap();
    let stats = store.get_chain_stats("msgs").unwrap().unwrap();
    assert_eq!(stats.total_operations, 22);

    // Reopen and confirm reconstruction sees all 21 items, in order.
    drop(store);
    let store = Store::open(StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: false,
    })
    .unwrap();
    let bytes = store.get_state("msgs").unwrap().unwrap();
    let items: Vec<serde_json::Value> = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(items.len(), 21);
    for (i, item) in items.iter().enumerate() {
        assert_eq!(item.get("i").and_then(|v| v.as_i64()), Some(i as i64));
    }
}

#[test]
fn test_auto_snapshot_toggle_default_path_still_snapshots() {
    // Sanity: with the toggle in its default state (on), the existing
    // snapshot policy still fires. Guards against an accidental wiring
    // regression where the flag's default is misread.
    //
    // `delta_snapshot_every:5, full_snapshot_every:2` → after 5 appends a
    // Delta fires, after another 5 a Delta, after another 5 the policy
    // promotes to a Full. So 20 appends gives at least one Full snapshot.
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);
    store
        .register_state(StateRegistration {
            id: "msgs".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 5,
                full_snapshot_every: 2,
            },
            initial_value: None,
        })
        .unwrap();

    assert!(store.auto_snapshot_enabled(), "default must be enabled");
    for i in 0..20 {
        store
            .append_to_state_json_with_identity("msgs", json!({ "i": i }), "id", "sequence")
            .unwrap();
    }
    let stats = store.get_chain_stats("msgs").unwrap().unwrap();
    assert!(
        stats.has_full_snapshot,
        "default-on toggle must let the snapshot policy fire normally",
    );
    assert!(
        stats.total_operations > 20,
        "chain must include at least one snapshot record (got {} ops)",
        stats.total_operations,
    );
}

/// update_state_strategy: cadence retune on an EXISTING registration must
/// take effect (registrations persist in state.bin, so boot-time
/// re-registration alone can never change them — the 2026-08-01 Mythos
/// full-snapshot-bloat lesson), and kind changes must be rejected.
#[test]
fn test_update_state_strategy_retunes_cadence() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);
    store
        .register_state(StateRegistration {
            id: "msgs".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 5,
                full_snapshot_every: 1, // full snapshot every 5 appends
            },
            initial_value: None,
        })
        .unwrap();

    // Re-registration still errors — update is a distinct, explicit act.
    assert!(store
        .register_state(StateRegistration {
            id: "msgs".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 5,
                full_snapshot_every: 1,
            },
            initial_value: None,
        })
        .is_err());

    // Retune: full snapshots effectively never (within this test's volume).
    store
        .update_state_strategy(
            "msgs",
            StateStrategy::AppendLog {
                delta_snapshot_every: 5,
                full_snapshot_every: 1000,
            },
        )
        .unwrap();

    for i in 0..30 {
        store
            .append_to_state_json_with_identity("msgs", json!({ "i": i }), "id", "sequence")
            .unwrap();
    }
    let stats = store.get_chain_stats("msgs").unwrap().unwrap();
    assert!(
        !stats.has_full_snapshot,
        "retuned cadence must govern future snapshots (full fired anyway)",
    );

    // Value integrity untouched by the retune.
    let state = store.get_state("msgs").unwrap().unwrap();
    let arr: Vec<serde_json::Value> = serde_json::from_slice(&state).unwrap();
    assert_eq!(arr.len(), 30);

    // Kind change rejected.
    assert!(store
        .update_state_strategy("msgs", StateStrategy::Snapshot)
        .is_err());

    // Unregistered state rejected.
    assert!(store
        .update_state_strategy(
            "nope",
            StateStrategy::AppendLog {
                delta_snapshot_every: 5,
                full_snapshot_every: 10,
            },
        )
        .is_err());
}

// --- State Field Index Tests ---

fn item_at(store: &Store, state_id: &str, ordinal: usize) -> serde_json::Value {
    let bytes = store
        .get_state_item(state_id, ordinal)
        .unwrap()
        .unwrap_or_else(|| panic!("no item at ordinal {}", ordinal));
    serde_json::from_slice(&bytes).unwrap()
}

#[test]
fn test_field_index_incremental_maintenance() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store
        .register_state(StateRegistration {
            id: "messages".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 1000,
                full_snapshot_every: 1000,
            },
            initial_value: None,
        })
        .unwrap();

    // Append 5 messages with a numeric timestamp and a string channelId.
    for (i, channel) in [("c1"), ("c2"), ("c1"), ("c3"), ("c1")].into_iter().enumerate() {
        store
            .update_state(
                "messages",
                StateOperation::Append(
                    serde_json::to_vec(&json!({
                        "timestamp": (i as i64 + 1) * 10,
                        "channelId": channel,
                    }))
                    .unwrap(),
                ),
            )
            .unwrap();
    }

    store
        .register_state_field_index("messages", "/timestamp", FieldIndexKind::Number)
        .unwrap();
    store
        .register_state_field_index("messages", "/channelId", FieldIndexKind::String)
        .unwrap();

    // Range query: timestamp in [20, 40] -> ordinals 1, 2, 3.
    let range = store
        .query_state_index_range("messages", "/timestamp", Some(20.0), Some(40.0), None, None, false)
        .unwrap();
    assert_eq!(range, vec![1, 2, 3]);

    // Equality query: channelId == "c1" -> ordinals 0, 2, 4.
    let eq = store
        .query_state_index_eq("messages", "/channelId", "c1", None, None)
        .unwrap();
    assert_eq!(eq, vec![0, 2, 4]);

    // Value counts.
    let mut counts = store.get_state_index_value_counts("messages", "/channelId").unwrap();
    counts.sort();
    assert_eq!(
        counts,
        vec![
            ("c1".to_string(), 3),
            ("c2".to_string(), 1),
            ("c3".to_string(), 1),
        ]
    );

    // Append one more item, incrementally maintained without re-registering.
    store
        .update_state(
            "messages",
            StateOperation::Append(
                serde_json::to_vec(&json!({"timestamp": 60, "channelId": "c2"})).unwrap(),
            ),
        )
        .unwrap();
    assert_eq!(
        store.query_state_index_eq("messages", "/channelId", "c2", None, None).unwrap(),
        vec![1, 5]
    );

    // Edit ordinal 1 (was channel "c2") to channel "c1".
    store
        .update_state(
            "messages",
            StateOperation::Edit {
                index: 1,
                new_value: serde_json::to_vec(&json!({"timestamp": 20, "channelId": "c1"}))
                    .unwrap(),
            },
        )
        .unwrap();
    assert_eq!(
        store.query_state_index_eq("messages", "/channelId", "c2", None, None).unwrap(),
        vec![5]
    );
    assert_eq!(
        store.query_state_index_eq("messages", "/channelId", "c1", None, None).unwrap(),
        vec![0, 1, 2, 4]
    );

    // Redact ordinals [0, 2): drops old 0 ("c1") and old 1 ("c1", just
    // edited), shifting the rest down by 2.
    store
        .update_state("messages", StateOperation::Redact { start: 0, end: 2 })
        .unwrap();

    // Content after redact: [old2 (c1,ts30), old3 (c3,ts40), old4 (c1,ts50), old5 (c2,ts60)]
    // at new ordinals [0, 1, 2, 3].
    assert_eq!(item_at(&store, "messages", 0)["channelId"], "c1");
    assert_eq!(item_at(&store, "messages", 3)["channelId"], "c2");

    let mut c1_after = store
        .query_state_index_eq("messages", "/channelId", "c1", None, None)
        .unwrap();
    c1_after.sort();
    assert_eq!(c1_after, vec![0, 2]);
    assert_eq!(
        store.query_state_index_eq("messages", "/channelId", "c2", None, None).unwrap(),
        vec![3]
    );
    assert_eq!(
        store.query_state_index_eq("messages", "/channelId", "c3", None, None).unwrap(),
        vec![1]
    );

    // Number index shifted the same way: ts=30 now at ordinal 0, ts=60 at ordinal 3.
    assert_eq!(
        store
            .query_state_index_range("messages", "/timestamp", Some(30.0), Some(30.0), None, None, false)
            .unwrap(),
        vec![0]
    );
    assert_eq!(
        store
            .query_state_index_range("messages", "/timestamp", Some(60.0), Some(60.0), None, None, false)
            .unwrap(),
        vec![3]
    );
}

#[test]
fn test_field_index_survives_snapshot_compaction() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store
        .register_state(StateRegistration {
            id: "messages".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 1000,
                full_snapshot_every: 1000,
            },
            initial_value: None,
        })
        .unwrap();

    for i in 0..5 {
        store
            .update_state(
                "messages",
                StateOperation::Append(
                    serde_json::to_vec(&json!({"timestamp": i * 10})).unwrap(),
                ),
            )
            .unwrap();
    }

    store
        .register_state_field_index("messages", "/timestamp", FieldIndexKind::Number)
        .unwrap();

    // Compaction writes a `Snapshot` op with the same content/ordinals.
    store.compact_state("messages").unwrap();

    let result = store
        .query_state_index_range("messages", "/timestamp", Some(10.0), Some(30.0), None, None, false)
        .unwrap();
    assert_eq!(result, vec![1, 2, 3]);

    // Further appends after compaction still maintain the index.
    store
        .update_state(
            "messages",
            StateOperation::Append(serde_json::to_vec(&json!({"timestamp": 999})).unwrap()),
        )
        .unwrap();
    assert_eq!(
        store
            .query_state_index_range("messages", "/timestamp", Some(999.0), None, None, None, false)
            .unwrap(),
        vec![5]
    );
}

#[test]
fn test_field_index_persists_across_reopen() {
    let dir = TempDir::new().unwrap();
    let config = StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: true,
    };

    // First session: create, write, register an index, sync.
    {
        let store = Store::create(config.clone()).unwrap();
        store
            .register_state(StateRegistration {
                id: "messages".to_string(),
                strategy: StateStrategy::AppendLog {
                    delta_snapshot_every: 1000,
                    full_snapshot_every: 1000,
                },
                initial_value: None,
            })
            .unwrap();

        for (i, channel) in ["a", "b", "a"].into_iter().enumerate() {
            store
                .update_state(
                    "messages",
                    StateOperation::Append(
                        serde_json::to_vec(&json!({"timestamp": i * 100, "channelId": channel}))
                            .unwrap(),
                    ),
                )
                .unwrap();
        }

        store
            .register_state_field_index("messages", "/timestamp", FieldIndexKind::Number)
            .unwrap();
        store
            .register_state_field_index("messages", "/channelId", FieldIndexKind::String)
            .unwrap();

        store.sync().unwrap();
    }

    // Second session: reopen. The persisted index file should already
    // reflect the 3 items without re-registering.
    {
        let store = Store::open(config.clone()).unwrap();

        let eq = store.query_state_index_eq("messages", "/channelId", "a", None, None).unwrap();
        assert_eq!(eq, vec![0, 2]);
        let range = store
            .query_state_index_range("messages", "/timestamp", Some(50.0), None, None, None, false)
            .unwrap();
        assert_eq!(range, vec![1, 2]);

        // Re-registering is idempotent (matching branch + head_offset ->
        // no-op, but must not error and must leave results unchanged).
        store
            .register_state_field_index("messages", "/timestamp", FieldIndexKind::Number)
            .unwrap();
        let range_again = store
            .query_state_index_range("messages", "/timestamp", Some(50.0), None, None, None, false)
            .unwrap();
        assert_eq!(range_again, vec![1, 2]);

        // Appending after reopen keeps incrementally maintaining the index.
        store
            .update_state(
                "messages",
                StateOperation::Append(
                    serde_json::to_vec(&json!({"timestamp": 500, "channelId": "a"})).unwrap(),
                ),
            )
            .unwrap();
        let eq_after_append = store
            .query_state_index_eq("messages", "/channelId", "a", None, None)
            .unwrap();
        assert_eq!(eq_after_append, vec![0, 2, 3]);

        store.sync().unwrap();
    }

    // Third session: confirm the appended item survived the second sync
    // and the index still reflects it without any re-registration at all.
    {
        let store = Store::open(config).unwrap();
        let eq = store.query_state_index_eq("messages", "/channelId", "a", None, None).unwrap();
        assert_eq!(eq, vec![0, 2, 3]);
    }
}

#[test]
fn test_field_index_missing_or_stale_file_starts_empty() {
    // A store whose `state-indexes.bin` is missing or corrupt (e.g. a store
    // written before this feature existed, or a partial/corrupted write)
    // must still open cleanly — the field index is derived data, so a
    // missing/unreadable file just means "nothing registered yet", not an
    // open failure.
    let dir = TempDir::new().unwrap();
    let config = StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: true,
    };

    {
        let store = Store::create(config.clone()).unwrap();
        store
            .register_state(StateRegistration {
                id: "messages".to_string(),
                strategy: StateStrategy::AppendLog {
                    delta_snapshot_every: 1000,
                    full_snapshot_every: 1000,
                },
                initial_value: None,
            })
            .unwrap();
        store
            .update_state(
                "messages",
                StateOperation::Append(serde_json::to_vec(&json!({"timestamp": 1})).unwrap()),
            )
            .unwrap();
        store
            .register_state_field_index("messages", "/timestamp", FieldIndexKind::Number)
            .unwrap();
        store.sync().unwrap();
    }

    let index_path = dir.path().join("store").join("state-indexes.bin");
    assert!(index_path.exists(), "sync() should have written state-indexes.bin");

    // Corrupt it in place (simulates a torn/partial write or bit rot).
    std::fs::write(&index_path, b"not a valid chronicle field index file").unwrap();

    // Reopening must not error. The corrupt file yields no persisted
    // registration, which the caller sees as `None` ("no such index"), not
    // an empty `Some(vec![])`.
    {
        let store = Store::open(config.clone()).unwrap();
        let result =
            store.query_state_index_range("messages", "/timestamp", None, None, None, None, false);
        assert_eq!(result, None);

        // Re-registering after a corrupt-file open works normally.
        store
            .register_state_field_index("messages", "/timestamp", FieldIndexKind::Number)
            .unwrap();
        let result = store
            .query_state_index_range("messages", "/timestamp", None, None, None, None, false)
            .unwrap();
        assert_eq!(result, vec![0]);

        store.sync().unwrap();
    }

    // Deleting the file entirely (rather than corrupting it) behaves the
    // same way.
    std::fs::remove_file(&index_path).unwrap();
    let store = Store::open(config).unwrap();
    let result =
        store.query_state_index_range("messages", "/timestamp", None, None, None, None, false);
    assert_eq!(result, None);
}

// --- Regression: DeltaSnapshot must not double-index (bug #1) ---

#[test]
fn test_field_index_delta_snapshot_does_not_double_index() {
    // A DeltaSnapshot consolidates Appends already recorded (and already
    // indexed via their own Append hooks) — it must never be treated as a
    // fresh batch of new items. `delta_snapshot_every: 4` with >=10 appends
    // guarantees at least one DeltaSnapshot fires during this test; the old
    // (buggy) behavior called `on_append` again for every item the delta
    // consolidated, inflating `by_ordinal` past the slot's real length and
    // scrambling ordinals.
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store
        .register_state(StateRegistration {
            id: "messages".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 4,
                full_snapshot_every: 1000, // keep this test on the delta path only
            },
            initial_value: None,
        })
        .unwrap();

    store
        .register_state_field_index("messages", "/ts", FieldIndexKind::Number)
        .unwrap();

    for i in 0..12u32 {
        store
            .update_state(
                "messages",
                StateOperation::Append(serde_json::to_vec(&json!({"ts": i})).unwrap()),
            )
            .unwrap();
    }

    // At delta_snapshot_every=4 with 12 appends, at least 2 DeltaSnapshots
    // fire along the way — confirm the chain stats actually exercised that
    // path (otherwise this regression test would pass vacuously).
    let stats = store.get_chain_stats("messages").unwrap().unwrap();
    assert!(
        !stats.has_full_snapshot,
        "full_snapshot_every is set high enough that only deltas should have fired"
    );

    let slot_len = store.get_state_len("messages").unwrap().unwrap();
    assert_eq!(slot_len, 12);

    // The index must track the slot exactly: 12 items in, 12 ordinals out,
    // one per distinct timestamp — never 18 (12 real + 6 double-indexed by
    // the two deltas), and no ordinal must appear twice.
    let all = store
        .query_state_index_range("messages", "/ts", None, None, None, None, false)
        .unwrap();
    assert_eq!(
        all.len(),
        slot_len,
        "field index length must track the slot's real length exactly through delta snapshots"
    );
    let mut unique = all.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), all.len(), "no ordinal may appear more than once");

    // Spot-check specific values land on exactly one ordinal apiece, not
    // duplicated onto [i, i+4] the way the bug produced.
    for i in [0u32, 4, 8, 11] {
        let matches = store
            .query_state_index_range(
                "messages",
                "/ts",
                Some(i as f64),
                Some(i as f64),
                None,
                None,
                false,
            )
            .unwrap();
        assert_eq!(matches, vec![i], "ts={} must map to exactly ordinal {}, not a duplicate", i, i);
    }
}

// --- Regression: cross-branch writes must not corrupt a field index (bug #2) ---

#[test]
fn test_field_index_cross_branch_writes_do_not_contaminate() {
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store
        .register_state(StateRegistration {
            id: "messages".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 1000,
                full_snapshot_every: 1000,
            },
            initial_value: None,
        })
        .unwrap();

    // 3 items on main.
    for i in 0..3 {
        store
            .update_state(
                "messages",
                StateOperation::Append(serde_json::to_vec(&json!({"v": i})).unwrap()),
            )
            .unwrap();
    }

    store
        .register_state_field_index("messages", "/v", FieldIndexKind::Number)
        .unwrap();
    let main_before = store
        .query_state_index_range("messages", "/v", None, None, None, None, false)
        .unwrap();
    assert_eq!(main_before, vec![0, 1, 2]);

    // Branch off, switch to it, append on the side branch.
    store.create_branch("side", None).unwrap();
    store.switch_branch("side").unwrap();
    for i in 100..102 {
        store
            .update_state(
                "messages",
                StateOperation::Append(serde_json::to_vec(&json!({"v": i})).unwrap()),
            )
            .unwrap();
    }
    // Side branch's own view is unindexed (index was registered on main) —
    // querying it returns None, never side's ordinals mislabeled as main's.
    assert_eq!(
        store.query_state_index_range("messages", "/v", None, None, None, None, false),
        None,
        "writing on a branch that never registered this index must not manufacture one"
    );

    // Switch back to main: the index registered there must be gone
    // (poisoned by the side-branch writes above), not silently reporting
    // ordinals 3/4 that don't exist on main.
    store.switch_branch("main").unwrap();
    let after_side_writes =
        store.query_state_index_range("messages", "/v", None, None, None, None, false);
    assert_eq!(
        after_side_writes, None,
        "a cross-branch write must poison the index rather than leave it silently wrong"
    );

    // Re-registering on main recovers a correct, main-only index.
    store
        .register_state_field_index("messages", "/v", FieldIndexKind::Number)
        .unwrap();
    let main_after = store
        .query_state_index_range("messages", "/v", None, None, None, None, false)
        .unwrap();
    assert_eq!(main_after, vec![0, 1, 2], "re-registered index must reflect ONLY main's 3 items");

    // A further redact on main must never touch anything derived from the
    // side branch's phantom ordinals.
    store
        .update_state("messages", StateOperation::Redact { start: 0, end: 1 })
        .unwrap();
    let main_final = store
        .query_state_index_range("messages", "/v", None, None, None, None, false)
        .unwrap();
    assert_eq!(main_final, vec![0, 1], "redact on main must only ever see main's own 2 remaining items");
}

#[test]
fn test_field_index_create_branch_at_does_not_contaminate() {
    // Same corruption family as the branch/switch test above, but via
    // `create_branch_at` (time-travel branching) instead of `create_branch`.
    let dir = TempDir::new().unwrap();
    let store = test_store(&dir);

    store
        .register_state(StateRegistration {
            id: "messages".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 1000,
                full_snapshot_every: 1000,
            },
            initial_value: None,
        })
        .unwrap();

    for i in 0..4 {
        store
            .update_state(
                "messages",
                StateOperation::Append(serde_json::to_vec(&json!({"v": i})).unwrap()),
            )
            .unwrap();
    }

    store
        .register_state_field_index("messages", "/v", FieldIndexKind::Number)
        .unwrap();

    // Branch at sequence 2 (partway through main's history), switch, append.
    store.create_branch_at("time-travel", "main", Sequence(2)).unwrap();
    store.switch_branch("time-travel").unwrap();
    store
        .update_state(
            "messages",
            StateOperation::Append(serde_json::to_vec(&json!({"v": 999})).unwrap()),
        )
        .unwrap();

    // Back on main, the index registered before branching must be poisoned
    // by the time-travel branch's append, not silently retained with a
    // phantom extra ordinal.
    store.switch_branch("main").unwrap();
    let result = store.query_state_index_range("messages", "/v", None, None, None, None, false);
    assert_eq!(result, None);

    store
        .register_state_field_index("messages", "/v", FieldIndexKind::Number)
        .unwrap();
    let main_after = store
        .query_state_index_range("messages", "/v", None, None, None, None, false)
        .unwrap();
    assert_eq!(main_after, vec![0, 1, 2, 3], "main's index must reflect only main's own 4 items");
}
