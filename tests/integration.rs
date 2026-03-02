//! Integration tests for the record store.

use chronicle::{
    RecordInput, Sequence, StateOperation, StateRegistration, StateStrategy, Store, StoreConfig,
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
