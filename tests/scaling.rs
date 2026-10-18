//! Scaling tests for Chronicle with synthetic 50k+ record stores.
//!
//! Tests various topologies and measures performance of key operations:
//! - Store creation and population
//! - Restart/rebuild from log
//! - Sync operations
//! - Queries by type, sequence range
//! - Subscriptions with catch-up
//! - Branch operations
//! - State reconstruction

use chronicle::{
    RecordInput, Sequence, StateOperation, StateRegistration, StateStrategy, Store, StoreConfig,
    SubscriptionConfig, SubscriptionFilter, StoreEvent,
};
use std::time::{Duration, Instant};
use tempfile::TempDir;

const RECORD_COUNT: usize = 50_000;

fn test_config(dir: &TempDir) -> StoreConfig {
    StoreConfig {
        path: dir.path().to_path_buf(),
        blob_cache_size: 100,
        create_if_missing: true,
    }
}

/// Timing helper
struct Timer {
    start: Instant,
    name: &'static str,
}

impl Timer {
    fn new(name: &'static str) -> Self {
        Self {
            start: Instant::now(),
            name,
        }
    }

    fn elapsed_ms(&self) -> f64 {
        self.start.elapsed().as_secs_f64() * 1000.0
    }

    fn report(&self) {
        println!("  {} took {:.2}ms", self.name, self.elapsed_ms());
    }

    fn report_with_count(&self, count: usize) {
        let ms = self.elapsed_ms();
        let per_item = if count > 0 { ms / count as f64 } else { 0.0 };
        println!(
            "  {} took {:.2}ms ({} items, {:.4}ms/item, {:.0} items/sec)",
            self.name,
            ms,
            count,
            per_item,
            if ms > 0.0 { count as f64 / (ms / 1000.0) } else { 0.0 }
        );
    }
}

// =============================================================================
// Test: Basic 50k records, single branch
// =============================================================================

#[test]
fn test_scaling_50k_single_branch() {
    println!("\n=== 50k Records, Single Branch ===");

    let dir = TempDir::new().unwrap();

    // Create and populate store
    let timer = Timer::new("Create store");
    let store = Store::create(test_config(&dir)).unwrap();
    timer.report();

    // Mix of generic record types. Internal state_update records must use
    // the state APIs; custom_event has the same byte length for this workload.
    let record_types = ["message", "tool_call", "tool_result", "custom_event", "system"];

    let timer = Timer::new("Append 50k records");
    for i in 0..RECORD_COUNT {
        let record_type = record_types[i % record_types.len()];
        let payload = serde_json::json!({
            "index": i,
            "data": format!("Record data for item {}", i),
            "timestamp": 1700000000 + i,
        });
        store
            .append(RecordInput::json(record_type, &payload).unwrap())
            .unwrap();
    }
    timer.report_with_count(RECORD_COUNT);

    // Sync
    let timer = Timer::new("Sync to disk");
    store.sync().unwrap();
    timer.report();

    // Stats
    let stats = store.stats().unwrap();
    println!("  Store stats: {} records, {} bytes", stats.record_count, stats.total_size_bytes);

    // Query by type
    let timer = Timer::new("Query by type 'message'");
    let message_ids = store.get_records_by_type("message");
    timer.report_with_count(message_ids.len());
    assert_eq!(message_ids.len(), RECORD_COUNT / record_types.len());

    // Close and reopen (test rebuild)
    drop(store);

    let timer = Timer::new("Reopen store (rebuild index)");
    let store = Store::open(test_config(&dir)).unwrap();
    timer.report();

    // Verify data integrity
    let timer = Timer::new("Verify all record types");
    for record_type in &record_types {
        let ids = store.get_records_by_type(record_type);
        assert_eq!(ids.len(), RECORD_COUNT / record_types.len());
    }
    timer.report();

    // Random access
    let timer = Timer::new("Random access 1000 records");
    for i in (0..RECORD_COUNT).step_by(50) {
        let id = chronicle::RecordId((i + 1) as u64);
        let record = store.get_record(id).unwrap();
        assert!(record.is_some());
    }
    timer.report_with_count(1000);

    println!("  ✓ Single branch test passed");
}

// =============================================================================
// Test: 50k records with multiple branches
// =============================================================================

#[test]
fn test_scaling_50k_multi_branch() {
    println!("\n=== 50k Records, Multiple Branches ===");

    let dir = TempDir::new().unwrap();
    let store = Store::create(test_config(&dir)).unwrap();

    let branch_count = 10;
    let records_per_branch = RECORD_COUNT / branch_count;

    // Create branches and populate
    let timer = Timer::new("Create branches and populate");
    for b in 0..branch_count {
        if b > 0 {
            let branch_name = format!("branch-{}", b);
            store.create_branch(&branch_name, None).unwrap();
            store.switch_branch(&branch_name).unwrap();
        }

        for i in 0..records_per_branch {
            let payload = serde_json::json!({
                "branch": b,
                "index": i,
            });
            store
                .append(RecordInput::json("message", &payload).unwrap())
                .unwrap();
        }
    }
    timer.report_with_count(RECORD_COUNT);

    // List branches
    let timer = Timer::new("List branches");
    let branches = store.list_branches();
    timer.report();
    assert_eq!(branches.len(), branch_count);

    // Switch between branches
    let timer = Timer::new("Switch branches 100 times");
    for i in 0..100 {
        let branch_name = if i % branch_count == 0 {
            "main".to_string()
        } else {
            format!("branch-{}", i % branch_count)
        };
        store.switch_branch(&branch_name).unwrap();
    }
    timer.report_with_count(100);

    // Sync and reopen
    store.sync().unwrap();
    drop(store);

    let timer = Timer::new("Reopen multi-branch store");
    let store = Store::open(test_config(&dir)).unwrap();
    timer.report();

    // Verify branch integrity
    let timer = Timer::new("Verify all branches");
    for b in 0..branch_count {
        let branch_name = if b == 0 {
            "main".to_string()
        } else {
            format!("branch-{}", b)
        };
        store.switch_branch(&branch_name).unwrap();
        let branch = store.current_branch();
        // Each branch should have records_per_branch records (cumulative from main)
        assert!(branch.head.0 >= records_per_branch as u64);
    }
    timer.report();

    println!("  ✓ Multi-branch test passed");
}

// =============================================================================
// Test: 50k records with state operations
// =============================================================================

#[test]
fn test_scaling_50k_with_states() {
    println!("\n=== 50k Records with State Operations ===");

    let dir = TempDir::new().unwrap();
    let store = Store::create(test_config(&dir)).unwrap();

    // Register states
    store
        .register_state(StateRegistration {
            id: "messages".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 500,
                full_snapshot_every: 10,
            },
            initial_value: None,
        })
        .unwrap();

    store
        .register_state(StateRegistration {
            id: "counter".to_string(),
            strategy: StateStrategy::Snapshot,
            initial_value: None,
        })
        .unwrap();

    // Mixed workload: records + state updates
    let timer = Timer::new("Mixed records + state updates");
    let mut message_count = 0;
    for i in 0..RECORD_COUNT {
        // Every 10th operation is a state update
        if i % 10 == 0 {
            let msg = serde_json::json!({
                "id": message_count,
                "text": format!("Message {}", message_count),
            });
            store
                .update_state(
                    "messages",
                    StateOperation::Append(serde_json::to_vec(&msg).unwrap()),
                )
                .unwrap();
            message_count += 1;
        }

        // Every 100th operation updates counter
        if i % 100 == 0 {
            let counter = serde_json::json!(i);
            store
                .update_state(
                    "counter",
                    StateOperation::Set(serde_json::to_vec(&counter).unwrap()),
                )
                .unwrap();
        }

        // Regular record
        let payload = serde_json::json!({ "i": i });
        store
            .append(RecordInput::json("event", &payload).unwrap())
            .unwrap();
    }
    timer.report_with_count(RECORD_COUNT);

    println!("  State 'messages' has {} items", message_count);

    // Get state
    let timer = Timer::new("Get 'messages' state");
    let messages = store.get_state("messages").unwrap().unwrap();
    timer.report();
    let messages: Vec<serde_json::Value> = serde_json::from_slice(&messages).unwrap();
    assert_eq!(messages.len(), message_count);

    // Get state length
    let timer = Timer::new("Get state length");
    let len = store.get_state_len("messages").unwrap().unwrap();
    timer.report();
    assert_eq!(len, message_count);

    // Get state slice
    let timer = Timer::new("Get state slice (last 100)");
    let slice = store
        .get_state_slice("messages", message_count - 100, 100)
        .unwrap()
        .unwrap();
    timer.report();
    let slice: Vec<serde_json::Value> = serde_json::from_slice(&slice).unwrap();
    assert_eq!(slice.len(), 100);

    // Sync and reopen
    store.sync().unwrap();
    drop(store);

    let timer = Timer::new("Reopen store with states");
    let store = Store::open(test_config(&dir)).unwrap();
    timer.report();

    // Verify state reconstruction
    let timer = Timer::new("Reconstruct 'messages' state");
    let messages = store.get_state("messages").unwrap().unwrap();
    timer.report();
    let messages: Vec<serde_json::Value> = serde_json::from_slice(&messages).unwrap();
    assert_eq!(messages.len(), message_count);

    // Historical state access
    let timer = Timer::new("Get state at sequence 25000");
    let historical = store.get_state_at("counter", Sequence(25000)).unwrap();
    timer.report();
    assert!(historical.is_some());

    println!("  ✓ State operations test passed");
}

// =============================================================================
// Test: Subscriptions with catch-up on large store
// =============================================================================

#[test]
fn test_scaling_subscriptions() {
    println!("\n=== Subscriptions on 50k Record Store ===");

    let dir = TempDir::new().unwrap();
    let store = Store::create(test_config(&dir)).unwrap();

    // Register state for subscription testing
    store
        .register_state(StateRegistration {
            id: "log".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 200,
                full_snapshot_every: 5,
            },
            initial_value: None,
        })
        .unwrap();

    // Populate store
    let timer = Timer::new("Populate store");
    for i in 0..RECORD_COUNT {
        let record_type = if i % 3 == 0 { "important" } else { "normal" };
        let payload = serde_json::json!({ "i": i });
        store
            .append(RecordInput::json(record_type, &payload).unwrap())
            .unwrap();

        // Add to state every 100 records
        if i % 100 == 0 {
            store
                .update_state(
                    "log",
                    StateOperation::Append(serde_json::to_vec(&payload).unwrap()),
                )
                .unwrap();
        }
    }
    timer.report_with_count(RECORD_COUNT);

    // Subscribe to records with catch-up from middle
    let timer = Timer::new("Subscribe + catch-up (records from seq 25000)");
    let config = SubscriptionConfig {
        filter: SubscriptionFilter::records(),
        from_sequence: Some(Sequence(25000)),
        buffer_size: 30000,
        ..Default::default()
    };
    let handle = store.subscribe(config);
    store.catch_up_subscription(handle.id).unwrap();
    timer.report();

    // Count received events
    let timer = Timer::new("Drain catch-up events");
    let mut record_count = 0;
    let mut caught_up = false;
    while let Ok(event) = handle.recv_timeout(Duration::from_millis(100)) {
        match event {
            StoreEvent::Record { .. } => record_count += 1,
            StoreEvent::CaughtUp => {
                caught_up = true;
                break;
            }
            _ => {}
        }
    }
    timer.report_with_count(record_count);
    assert!(caught_up, "Should receive CaughtUp event");
    // Should receive ~25000 records (from 25000 to 50000)
    assert!(
        record_count >= 24000,
        "Expected ~25000 records, got {}",
        record_count
    );

    // Subscribe to filtered records
    let timer = Timer::new("Subscribe + catch-up (filtered 'important' from seq 0)");
    let config = SubscriptionConfig {
        filter: SubscriptionFilter::record_types(vec!["important".to_string()]),
        from_sequence: Some(Sequence(1)),
        buffer_size: 20000,
        ..Default::default()
    };
    let handle2 = store.subscribe(config);
    store.catch_up_subscription(handle2.id).unwrap();

    let mut important_count = 0;
    while let Ok(event) = handle2.recv_timeout(Duration::from_millis(100)) {
        match event {
            StoreEvent::Record { record } => {
                assert_eq!(record.record_type, "important");
                important_count += 1;
            }
            StoreEvent::CaughtUp => break,
            _ => {}
        }
    }
    timer.report_with_count(important_count);
    // Every 3rd record is "important"
    let expected = RECORD_COUNT / 3;
    assert!(
        important_count >= expected - 100,
        "Expected ~{} important records, got {}",
        expected,
        important_count
    );

    // Subscribe to state with catch-up
    let timer = Timer::new("Subscribe + catch-up (state)");
    let config = SubscriptionConfig {
        filter: SubscriptionFilter::states(vec!["log".to_string()]),
        from_sequence: Some(Sequence(1)),
        buffer_size: 1000,
        max_snapshot_bytes: 1024 * 1024, // 1MB
        ..Default::default()
    };
    let handle3 = store.subscribe(config);
    store.catch_up_subscription(handle3.id).unwrap();

    let mut got_snapshot = false;
    while let Ok(event) = handle3.recv_timeout(Duration::from_millis(100)) {
        match event {
            StoreEvent::StateSnapshot { state_id, .. } => {
                assert_eq!(state_id, "log");
                got_snapshot = true;
            }
            StoreEvent::CaughtUp => break,
            _ => {}
        }
    }
    timer.report();
    assert!(got_snapshot, "Should receive state snapshot");

    // Live subscription test
    let timer = Timer::new("Live subscription (100 new records)");
    let config = SubscriptionConfig {
        filter: SubscriptionFilter::records(),
        from_sequence: None, // Live only
        ..Default::default()
    };
    let live_handle = store.subscribe(config);
    store.catch_up_subscription(live_handle.id).unwrap();

    // Drain CaughtUp
    let _ = live_handle.recv_timeout(Duration::from_millis(100));

    // Append new records
    for i in 0..100 {
        let payload = serde_json::json!({ "live": i });
        store
            .append(RecordInput::json("live", &payload).unwrap())
            .unwrap();
    }

    // Receive live events
    let mut live_count = 0;
    while let Ok(event) = live_handle.recv_timeout(Duration::from_millis(50)) {
        if matches!(event, StoreEvent::Record { .. }) {
            live_count += 1;
        }
        if live_count >= 100 {
            break;
        }
    }
    timer.report_with_count(live_count);
    assert_eq!(live_count, 100, "Should receive all 100 live records");

    // Cleanup
    store.unsubscribe(handle.id);
    store.unsubscribe(handle2.id);
    store.unsubscribe(handle3.id);
    store.unsubscribe(live_handle.id);
    assert_eq!(store.subscription_count(), 0);

    println!("  ✓ Subscription test passed");
}

// =============================================================================
// Test: Deep branch hierarchy
// =============================================================================

#[test]
fn test_scaling_deep_branches() {
    println!("\n=== Deep Branch Hierarchy ===");

    let dir = TempDir::new().unwrap();
    let store = Store::create(test_config(&dir)).unwrap();

    let depth = 20;
    let records_per_level = 2500;

    // Create deep hierarchy
    let timer = Timer::new("Create deep branch hierarchy");
    for level in 0..depth {
        // Add records at this level
        for i in 0..records_per_level {
            let payload = serde_json::json!({ "level": level, "i": i });
            store
                .append(RecordInput::json("data", &payload).unwrap())
                .unwrap();
        }

        // Create child branch (except at last level)
        if level < depth - 1 {
            let branch_name = format!("level-{}", level + 1);
            store.create_branch(&branch_name, None).unwrap();
            store.switch_branch(&branch_name).unwrap();
        }
    }
    timer.report_with_count(depth * records_per_level);

    // Navigate up and down the hierarchy
    let timer = Timer::new("Navigate hierarchy (100 switches)");
    for i in 0..100 {
        let level = i % depth;
        let branch_name = if level == 0 {
            "main".to_string()
        } else {
            format!("level-{}", level)
        };
        store.switch_branch(&branch_name).unwrap();
    }
    timer.report_with_count(100);

    // Sync and reopen
    store.sync().unwrap();
    drop(store);

    let timer = Timer::new("Reopen deep hierarchy store");
    let store = Store::open(test_config(&dir)).unwrap();
    timer.report();

    // Verify each branch has correct record count
    // In this test, each branch was created AFTER adding records at that level,
    // so each branch sees: records from all previous levels + its own records
    // Level 0 (main): 2500 records, then branch-1 created
    // Level 1: branch-1 has 2500 (inherited) + 2500 (own) = 5000, then branch-2 created
    // etc.
    // But actually, we add records THEN create branch, so each child starts at parent's head
    // which includes all parent records.
    // After 20 levels: main has records 1-2500, level-1 branches from 2500 and gets 2501-5000,
    // level-2 branches from 5000 and gets 5001-7500, etc.
    // The deepest branch (level-19) should have head = 20 * 2500 = 50000

    // Switch back to main and verify it only has first batch
    store.switch_branch("main").unwrap();
    let main_branch = store.current_branch();
    assert_eq!(
        main_branch.head.0 as usize,
        records_per_level,
        "Main branch should only have first batch of records"
    );

    // The deepest branch should have all records (since it was created last)
    store.switch_branch(&format!("level-{}", depth - 1)).unwrap();
    let deepest = store.current_branch();
    assert_eq!(
        deepest.head.0 as usize,
        depth * records_per_level,
        "Deepest branch should see all records"
    );

    println!("  ✓ Deep branch hierarchy test passed");
}

// =============================================================================
// Test: Many small states
// =============================================================================

#[test]
fn test_scaling_many_states() {
    println!("\n=== Many Small States ===");

    let dir = TempDir::new().unwrap();
    let store = Store::create(test_config(&dir)).unwrap();

    let state_count = 100;
    let updates_per_state = 500;

    // Register many states
    let timer = Timer::new("Register 100 states");
    for s in 0..state_count {
        store
            .register_state(StateRegistration {
                id: format!("state-{}", s),
                strategy: StateStrategy::AppendLog {
                    delta_snapshot_every: 50,
                    full_snapshot_every: 5,
                },
                initial_value: None,
            })
            .unwrap();
    }
    timer.report_with_count(state_count);

    // Update states in round-robin
    let timer = Timer::new("Update states (50k total operations)");
    for i in 0..(state_count * updates_per_state) {
        let state_id = format!("state-{}", i % state_count);
        let payload = serde_json::json!({ "update": i / state_count });
        store
            .update_state(
                &state_id,
                StateOperation::Append(serde_json::to_vec(&payload).unwrap()),
            )
            .unwrap();
    }
    timer.report_with_count(state_count * updates_per_state);

    // Verify all states exist by reading them
    let timer = Timer::new("Verify all states exist");
    for s in 0..state_count {
        let state_id = format!("state-{}", s);
        assert!(store.get_state(&state_id).unwrap().is_some());
    }
    timer.report_with_count(state_count);

    // Read all states
    let timer = Timer::new("Read all 100 states");
    for s in 0..state_count {
        let state_id = format!("state-{}", s);
        let data = store.get_state(&state_id).unwrap().unwrap();
        let items: Vec<serde_json::Value> = serde_json::from_slice(&data).unwrap();
        assert_eq!(items.len(), updates_per_state);
    }
    timer.report_with_count(state_count);

    // Sync and reopen
    store.sync().unwrap();
    drop(store);

    let timer = Timer::new("Reopen store with 100 states");
    let store = Store::open(test_config(&dir)).unwrap();
    timer.report();

    // Verify state reconstruction
    let timer = Timer::new("Reconstruct all 100 states");
    for s in 0..state_count {
        let state_id = format!("state-{}", s);
        let data = store.get_state(&state_id).unwrap().unwrap();
        let items: Vec<serde_json::Value> = serde_json::from_slice(&data).unwrap();
        assert_eq!(items.len(), updates_per_state);
    }
    timer.report_with_count(state_count);

    println!("  ✓ Many states test passed");
}

// =============================================================================
// Test: Sync performance
// =============================================================================

#[test]
fn test_scaling_sync_performance() {
    println!("\n=== Sync Performance ===");

    let dir = TempDir::new().unwrap();
    let store = Store::create(test_config(&dir)).unwrap();

    // Populate
    let timer = Timer::new("Populate 50k records");
    for i in 0..RECORD_COUNT {
        let payload = serde_json::json!({ "i": i });
        store
            .append(RecordInput::json("data", &payload).unwrap())
            .unwrap();
    }
    timer.report_with_count(RECORD_COUNT);

    // Multiple syncs (should be fast after first)
    let timer = Timer::new("First sync");
    store.sync().unwrap();
    timer.report();

    let timer = Timer::new("Second sync (no changes)");
    store.sync().unwrap();
    timer.report();

    // Add more records
    for i in 0..1000 {
        let payload = serde_json::json!({ "i": RECORD_COUNT + i });
        store
            .append(RecordInput::json("data", &payload).unwrap())
            .unwrap();
    }

    let timer = Timer::new("Sync after 1000 more records");
    store.sync().unwrap();
    timer.report();

    // Verify sync is O(1) not O(N) by timing
    // Add another batch
    for i in 0..1000 {
        let payload = serde_json::json!({ "extra": i });
        store
            .append(RecordInput::json("data", &payload).unwrap())
            .unwrap();
    }

    let timer = Timer::new("Another sync (should be similar time)");
    store.sync().unwrap();
    let sync_time = timer.elapsed_ms();
    timer.report();

    // Sync should be fast (< 100ms) since we removed index persistence
    assert!(
        sync_time < 100.0,
        "Sync should be O(1), took {}ms",
        sync_time
    );

    println!("  ✓ Sync performance test passed");
}

// =============================================================================
// Summary test that runs a mixed workload
// =============================================================================

#[test]
fn test_scaling_mixed_workload() {
    println!("\n=== Mixed Workload Simulation ===");
    println!("  Simulating realistic agent conversation workload...");

    let dir = TempDir::new().unwrap();
    let store = Store::create(test_config(&dir)).unwrap();

    // Setup: conversation state, tool states
    store
        .register_state(StateRegistration {
            id: "conversation".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 100,
                full_snapshot_every: 10,
            },
            initial_value: None,
        })
        .unwrap();

    store
        .register_state(StateRegistration {
            id: "tool-results".to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 50,
                full_snapshot_every: 5,
            },
            initial_value: None,
        })
        .unwrap();

    let total_ops = 50000;
    let mut stats = WorkloadStats::default();

    let timer = Timer::new("Mixed workload");
    for i in 0..total_ops {
        let op = i % 100;

        match op {
            // User message (5%)
            0..=4 => {
                let msg = serde_json::json!({
                    "role": "user",
                    "content": format!("User message {}", i),
                });
                store
                    .update_state(
                        "conversation",
                        StateOperation::Append(serde_json::to_vec(&msg).unwrap()),
                    )
                    .unwrap();
                stats.user_messages += 1;
            }
            // Assistant message (5%)
            5..=9 => {
                let msg = serde_json::json!({
                    "role": "assistant",
                    "content": format!("Assistant response {}", i),
                });
                store
                    .update_state(
                        "conversation",
                        StateOperation::Append(serde_json::to_vec(&msg).unwrap()),
                    )
                    .unwrap();
                stats.assistant_messages += 1;
            }
            // Tool call (15%)
            10..=24 => {
                let payload = serde_json::json!({
                    "tool": "read_file",
                    "args": { "path": format!("/file_{}.txt", i) },
                });
                store
                    .append(RecordInput::json("tool_call", &payload).unwrap())
                    .unwrap();
                stats.tool_calls += 1;
            }
            // Tool result (15%)
            25..=39 => {
                let result = serde_json::json!({
                    "output": format!("Tool output for {}", i),
                });
                store
                    .update_state(
                        "tool-results",
                        StateOperation::Append(serde_json::to_vec(&result).unwrap()),
                    )
                    .unwrap();
                stats.tool_results += 1;
            }
            // Inference log (20%)
            40..=59 => {
                let payload = serde_json::json!({
                    "model": "claude-3",
                    "tokens": 1500,
                    "latency_ms": 800,
                });
                store
                    .append(RecordInput::json("inference", &payload).unwrap())
                    .unwrap();
                stats.inferences += 1;
            }
            // Event record (40%)
            _ => {
                let payload = serde_json::json!({
                    "event": "framework_event",
                    "data": i,
                });
                store
                    .append(RecordInput::json("event", &payload).unwrap())
                    .unwrap();
                stats.events += 1;
            }
        }

        // Periodic sync (every 5000 ops)
        if i > 0 && i % 5000 == 0 {
            store.sync().unwrap();
            stats.syncs += 1;
        }
    }
    timer.report_with_count(total_ops);

    println!("  Workload breakdown:");
    println!("    User messages: {}", stats.user_messages);
    println!("    Assistant messages: {}", stats.assistant_messages);
    println!("    Tool calls: {}", stats.tool_calls);
    println!("    Tool results: {}", stats.tool_results);
    println!("    Inferences: {}", stats.inferences);
    println!("    Events: {}", stats.events);
    println!("    Syncs: {}", stats.syncs);

    // Final sync
    let timer = Timer::new("Final sync");
    store.sync().unwrap();
    timer.report();

    // Conversation length
    let conv_len = store.get_state_len("conversation").unwrap().unwrap();
    println!("  Conversation length: {} messages", conv_len);

    // Test subscription catch-up (simulating UI reconnection)
    let timer = Timer::new("UI reconnection (subscribe from halfway)");
    let halfway = store.current_branch().head.0 / 2;
    let config = SubscriptionConfig {
        filter: SubscriptionFilter::all(),
        from_sequence: Some(Sequence(halfway)),
        buffer_size: 30000,
        max_snapshot_bytes: 10 * 1024 * 1024,
        ..Default::default()
    };
    let handle = store.subscribe(config);
    store.catch_up_subscription(handle.id).unwrap();

    let mut event_count = 0;
    while let Ok(event) = handle.recv_timeout(Duration::from_millis(100)) {
        event_count += 1;
        if matches!(event, StoreEvent::CaughtUp) {
            break;
        }
    }
    timer.report_with_count(event_count);

    // Reopen test
    drop(store);

    let timer = Timer::new("Reopen after mixed workload");
    let store = Store::open(test_config(&dir)).unwrap();
    timer.report();

    // Verify conversation intact
    let timer = Timer::new("Verify conversation state");
    let conv = store.get_state("conversation").unwrap().unwrap();
    let msgs: Vec<serde_json::Value> = serde_json::from_slice(&conv).unwrap();
    timer.report();
    assert_eq!(msgs.len(), conv_len);

    println!("  ✓ Mixed workload test passed");
}

#[derive(Default)]
struct WorkloadStats {
    user_messages: usize,
    assistant_messages: usize,
    tool_calls: usize,
    tool_results: usize,
    inferences: usize,
    events: usize,
    syncs: usize,
}
