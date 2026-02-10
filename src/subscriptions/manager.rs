//! Subscription manager for broadcasting store events.

use crate::error::{Result, StoreError};
use crate::types::{Branch, Record, Sequence, StateOperation};
use crossbeam_channel::{bounded, Sender};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use super::types::{
    BranchSummary, DropReason, RecordSummary, StoreEvent, SubscriptionConfig, SubscriptionHandle,
    SubscriptionId,
};

/// Default threshold for including payload in record events (bytes).
const DEFAULT_PAYLOAD_THRESHOLD: usize = 4096;

/// Internal subscription state.
struct Subscription {
    config: SubscriptionConfig,
    sender: Sender<StoreEvent>,
    /// Whether catch-up is complete.
    caught_up: bool,
}

impl Subscription {
    /// Try to send an event. Returns false if buffer is full (subscriber will be dropped).
    fn try_send(&self, event: StoreEvent) -> bool {
        match self.sender.try_send(event) {
            Ok(()) => true,
            Err(crossbeam_channel::TrySendError::Full(_)) => false,
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => false,
        }
    }

    /// Check if this subscription matches a record.
    fn matches_record(&self, record: &Record) -> bool {
        if !self.config.filter.include_records {
            return false;
        }

        // Check record type filter
        if let Some(ref types) = self.config.filter.record_types {
            if !types.contains(&record.record_type) {
                return false;
            }
        }

        // TODO: Check branch filter

        true
    }

    /// Check if this subscription matches a state change.
    fn matches_state(&self, state_id: &str) -> bool {
        if !self.config.filter.include_state_changes {
            return false;
        }

        if let Some(ref ids) = self.config.filter.state_ids {
            return ids.iter().any(|id| id == state_id);
        }

        true
    }

    /// Check if this subscription wants branch events.
    fn wants_branch_events(&self) -> bool {
        self.config.filter.include_branch_events
    }
}

/// Manages subscriptions and broadcasts events.
pub struct SubscriptionManager {
    /// Active subscriptions by ID.
    subscriptions: RwLock<HashMap<SubscriptionId, Subscription>>,
    /// Counter for generating subscription IDs.
    next_id: AtomicU64,
    /// Threshold for including payload in record events.
    payload_threshold: usize,
}

impl SubscriptionManager {
    /// Create a new subscription manager.
    pub fn new() -> Self {
        Self {
            subscriptions: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            payload_threshold: DEFAULT_PAYLOAD_THRESHOLD,
        }
    }

    /// Create a new subscription manager with custom payload threshold.
    pub fn with_payload_threshold(threshold: usize) -> Self {
        Self {
            subscriptions: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            payload_threshold: threshold,
        }
    }

    /// Create a new subscription.
    ///
    /// Returns a handle for receiving events. The subscription is not yet
    /// "caught up" - call `catch_up_subscription` to replay historical data.
    pub fn subscribe(&self, config: SubscriptionConfig) -> SubscriptionHandle {
        let id = SubscriptionId(self.next_id.fetch_add(1, Ordering::SeqCst));
        let (sender, receiver) = bounded(config.buffer_size);

        let subscription = Subscription {
            config,
            sender,
            caught_up: false,
        };

        self.subscriptions.write().insert(id, subscription);

        SubscriptionHandle { id, receiver }
    }

    /// Unsubscribe and clean up.
    pub fn unsubscribe(&self, id: SubscriptionId) {
        let mut subs = self.subscriptions.write();
        if let Some(sub) = subs.remove(&id) {
            // Send dropped event (best effort)
            let _ = sub.sender.try_send(StoreEvent::Dropped {
                reason: DropReason::Unsubscribed,
            });
        }
    }

    /// Mark a subscription as caught up (finished historical replay).
    pub fn mark_caught_up(&self, id: SubscriptionId) -> Result<()> {
        let mut subs = self.subscriptions.write();
        if let Some(sub) = subs.get_mut(&id) {
            sub.caught_up = true;
            if !sub.try_send(StoreEvent::CaughtUp) {
                subs.remove(&id);
                return Err(StoreError::SubscriptionDropped);
            }
        }
        Ok(())
    }

    /// Get subscription count.
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.read().len()
    }

    // --- Broadcasting ---

    /// Broadcast a new record to matching subscriptions.
    pub fn broadcast_record(&self, record: &Record) {
        let summary = RecordSummary::from_record(record, self.payload_threshold);
        let event = StoreEvent::Record { record: summary };

        self.broadcast(|sub| sub.caught_up && sub.matches_record(record), event);
    }

    /// Broadcast a state snapshot to matching subscriptions.
    pub fn broadcast_state_snapshot(
        &self,
        state_id: &str,
        data: serde_json::Value,
        sequence: Sequence,
        truncated: bool,
        total_bytes: usize,
        from_index: Option<usize>,
        total_length: Option<usize>,
    ) {
        let event = StoreEvent::StateSnapshot {
            state_id: state_id.to_string(),
            data,
            sequence,
            truncated,
            total_bytes,
            from_index,
            total_length,
        };

        self.broadcast(|sub| sub.matches_state(state_id), event);
    }

    /// Broadcast a state delta to matching subscriptions.
    pub fn broadcast_state_delta(
        &self,
        state_id: &str,
        operation: StateOperation,
        sequence: Sequence,
    ) {
        let event = StoreEvent::StateDelta {
            state_id: state_id.to_string(),
            operation,
            sequence,
        };

        self.broadcast(|sub| sub.caught_up && sub.matches_state(state_id), event);
    }

    /// Broadcast branch head update.
    pub fn broadcast_branch_head(&self, branch_name: &str, head: Sequence) {
        let event = StoreEvent::BranchHead {
            branch: branch_name.to_string(),
            head,
        };

        self.broadcast(|sub| sub.caught_up && sub.wants_branch_events(), event);
    }

    /// Broadcast branch created event.
    pub fn broadcast_branch_created(&self, branch: &Branch, parent_name: Option<String>) {
        let summary = BranchSummary::from_branch(branch, parent_name);
        let event = StoreEvent::BranchCreated { branch: summary };

        self.broadcast(|sub| sub.caught_up && sub.wants_branch_events(), event);
    }

    /// Broadcast branch deleted event.
    pub fn broadcast_branch_deleted(&self, name: &str) {
        let event = StoreEvent::BranchDeleted {
            name: name.to_string(),
        };

        self.broadcast(|sub| sub.caught_up && sub.wants_branch_events(), event);
    }

    /// Internal broadcast helper. Drops subscribers that fail to receive.
    fn broadcast<F>(&self, filter: F, event: StoreEvent)
    where
        F: Fn(&Subscription) -> bool,
    {
        let mut to_remove = Vec::new();

        {
            let subs = self.subscriptions.read();
            for (id, sub) in subs.iter() {
                if filter(sub) {
                    if !sub.try_send(event.clone()) {
                        to_remove.push(*id);
                    }
                }
            }
        }

        // Remove dropped subscriptions
        if !to_remove.is_empty() {
            let mut subs = self.subscriptions.write();
            for id in to_remove {
                if let Some(sub) = subs.remove(&id) {
                    // Try to notify about the drop (might fail, that's ok)
                    let _ = sub.sender.try_send(StoreEvent::Dropped {
                        reason: DropReason::BufferOverflow,
                    });
                }
            }
        }
    }

    // --- Catch-up Helpers ---

    /// Send an event directly to a subscription (for catch-up).
    /// Returns false if the subscription was dropped.
    pub fn send_to(&self, id: SubscriptionId, event: StoreEvent) -> bool {
        let subs = self.subscriptions.read();
        if let Some(sub) = subs.get(&id) {
            sub.try_send(event)
        } else {
            false
        }
    }

    /// Get the config for a subscription (for catch-up logic).
    pub fn get_config(&self, id: SubscriptionId) -> Option<SubscriptionConfig> {
        self.subscriptions.read().get(&id).map(|s| s.config.clone())
    }
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscriptions::types::SubscriptionFilter;
    use crate::types::{BranchId, PayloadEncoding, RecordId, Timestamp};
    use std::time::Duration;

    fn make_test_record(record_type: &str) -> Record {
        Record {
            id: RecordId(1),
            sequence: Sequence(1),
            branch: BranchId(1),
            record_type: record_type.to_string(),
            timestamp: Timestamp::now(),
            caused_by: vec![],
            linked_to: vec![],
            payload: b"{}".to_vec(),
            encoding: PayloadEncoding::Json,
        }
    }

    #[test]
    fn test_subscribe_unsubscribe() {
        let manager = SubscriptionManager::new();

        let handle = manager.subscribe(SubscriptionConfig::default());
        assert_eq!(manager.subscription_count(), 1);

        manager.unsubscribe(handle.id);
        assert_eq!(manager.subscription_count(), 0);
    }

    #[test]
    fn test_broadcast_to_matching() {
        let manager = SubscriptionManager::new();

        // Subscribe to "message" records
        let config = SubscriptionConfig {
            filter: SubscriptionFilter::record_types(vec!["message".to_string()]),
            ..Default::default()
        };
        let handle = manager.subscribe(config);
        manager.mark_caught_up(handle.id).unwrap();

        // Drain the CaughtUp event
        let caught_up = handle.recv_timeout(Duration::from_millis(100)).unwrap();
        assert!(matches!(caught_up, StoreEvent::CaughtUp));

        // Broadcast matching record
        let record = make_test_record("message");
        manager.broadcast_record(&record);

        // Should receive
        let event = handle.recv_timeout(Duration::from_millis(100)).unwrap();
        match event {
            StoreEvent::Record { record: summary } => {
                assert_eq!(summary.record_type, "message");
            }
            _ => panic!("Expected Record event, got {:?}", event),
        }
    }

    #[test]
    fn test_broadcast_filters_non_matching() {
        let manager = SubscriptionManager::new();

        // Subscribe to "message" records
        let config = SubscriptionConfig {
            filter: SubscriptionFilter::record_types(vec!["message".to_string()]),
            ..Default::default()
        };
        let handle = manager.subscribe(config);
        manager.mark_caught_up(handle.id).unwrap();

        // Drain the CaughtUp event
        let caught_up = handle.recv_timeout(Duration::from_millis(100)).unwrap();
        assert!(matches!(caught_up, StoreEvent::CaughtUp));

        // Broadcast non-matching record
        let record = make_test_record("tool-call");
        manager.broadcast_record(&record);

        // Should NOT receive (no more events after CaughtUp)
        let result = handle.recv_timeout(Duration::from_millis(50));
        assert!(result.is_err());
    }

    #[test]
    fn test_drop_slow_subscriber() {
        // Small buffer
        let manager = SubscriptionManager::new();
        let config = SubscriptionConfig {
            buffer_size: 2,
            filter: SubscriptionFilter::records(),
            ..Default::default()
        };
        let handle = manager.subscribe(config);
        manager.mark_caught_up(handle.id).unwrap();

        // Flood with events
        for i in 0..10 {
            let mut record = make_test_record("message");
            record.id = RecordId(i);
            manager.broadcast_record(&record);
        }

        // Subscriber should be dropped
        assert_eq!(manager.subscription_count(), 0);
    }

    #[test]
    fn test_not_caught_up_doesnt_receive() {
        let manager = SubscriptionManager::new();

        let config = SubscriptionConfig {
            filter: SubscriptionFilter::records(),
            ..Default::default()
        };
        let handle = manager.subscribe(config);
        // NOT marking as caught up

        // Broadcast record
        let record = make_test_record("message");
        manager.broadcast_record(&record);

        // Should NOT receive (not caught up yet)
        let result = handle.recv_timeout(Duration::from_millis(50));
        assert!(result.is_err());
    }
}
