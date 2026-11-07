//! Subscription types for live store updates.

use crate::types::{Branch, BranchId, Record, Sequence, StateOperation};
use serde::{Deserialize, Serialize};

/// Configuration for a subscription.
#[derive(Clone, Debug)]
pub struct SubscriptionConfig {
    /// Max buffered events before dropping subscriber.
    /// Default: 1000
    pub buffer_size: usize,

    /// Max bytes for state snapshots (prevents OOM).
    /// Default: 10MB
    pub max_snapshot_bytes: usize,

    /// Starting sequence for catch-up (None = live only).
    pub from_sequence: Option<Sequence>,

    /// Filter criteria.
    pub filter: SubscriptionFilter,
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            buffer_size: 1000,
            max_snapshot_bytes: 10 * 1024 * 1024, // 10MB
            from_sequence: None,
            filter: SubscriptionFilter::default(),
        }
    }
}

/// Filter criteria for subscriptions.
#[derive(Clone, Debug, Default)]
pub struct SubscriptionFilter {
    /// Filter by record types (None = all types).
    pub record_types: Option<Vec<String>>,

    /// Filter by branch (None = current branch).
    pub branch: Option<String>,

    /// Subscribe to specific state IDs.
    pub state_ids: Option<Vec<String>>,

    /// Include record events.
    pub include_records: bool,

    /// Include state change events.
    pub include_state_changes: bool,

    /// Include branch events.
    pub include_branch_events: bool,
}

impl SubscriptionFilter {
    /// Subscribe to all records on current branch.
    pub fn records() -> Self {
        Self {
            include_records: true,
            ..Default::default()
        }
    }

    /// Subscribe to specific record types.
    pub fn record_types(types: Vec<String>) -> Self {
        Self {
            record_types: Some(types),
            include_records: true,
            ..Default::default()
        }
    }

    /// Subscribe to state changes for specific states.
    pub fn states(state_ids: Vec<String>) -> Self {
        Self {
            state_ids: Some(state_ids),
            include_state_changes: true,
            ..Default::default()
        }
    }

    /// Subscribe to branch events.
    pub fn branches() -> Self {
        Self {
            include_branch_events: true,
            ..Default::default()
        }
    }

    /// Subscribe to everything.
    pub fn all() -> Self {
        Self {
            include_records: true,
            include_state_changes: true,
            include_branch_events: true,
            ..Default::default()
        }
    }
}

/// Events emitted by subscriptions.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StoreEvent {
    // --- Record Events ---
    /// A new record was appended.
    Record {
        record: RecordSummary,
    },

    // --- State Events ---
    /// Initial state snapshot (may be truncated).
    StateSnapshot {
        state_id: String,
        /// JSON-encoded state data.
        data: serde_json::Value,
        /// Sequence number this snapshot represents.
        sequence: Sequence,
        /// True if data was truncated due to size limits.
        truncated: bool,
        /// Actual size in bytes (before truncation).
        total_bytes: usize,
        /// For append_log: starting index of this slice.
        from_index: Option<usize>,
        /// For append_log: total length of the log.
        total_length: Option<usize>,
    },

    /// A state delta (change operation): one per recorded operation, so a
    /// batched append (`AppendMany`) is one event holding all its items.
    StateDelta {
        state_id: String,
        operation: StateOperation,
        sequence: Sequence,
    },

    // --- Branch Events ---
    /// Branch head was updated.
    BranchHead {
        branch: String,
        head: Sequence,
    },

    /// A new branch was created.
    BranchCreated {
        branch: BranchSummary,
    },

    /// A branch was deleted.
    BranchDeleted {
        name: String,
    },

    // --- Lifecycle Events ---
    /// Finished historical catch-up, now streaming live.
    CaughtUp,

    /// Subscription was dropped.
    Dropped {
        reason: DropReason,
    },
}

/// Why a subscription was dropped.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DropReason {
    /// Send buffer overflowed (slow consumer).
    BufferOverflow,
    /// Client disconnected.
    Disconnected,
    /// Filter was invalid.
    InvalidFilter(String),
    /// Internal error.
    Error(String),
    /// Explicitly unsubscribed.
    Unsubscribed,
}

/// Summary of a record (for events, avoids sending full payload).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordSummary {
    pub id: u64,
    pub sequence: Sequence,
    pub branch: BranchId,
    pub record_type: String,
    pub timestamp: i64,
    /// Payload size in bytes.
    pub payload_size: usize,
    /// The actual payload (if small enough, otherwise None).
    pub payload: Option<serde_json::Value>,
}

impl RecordSummary {
    /// Create summary from a full record.
    pub fn from_record(record: &Record, include_payload_threshold: usize) -> Self {
        let payload_bytes = &record.payload;
        let payload_size = payload_bytes.len();

        let payload = if payload_size <= include_payload_threshold {
            serde_json::from_slice(payload_bytes).ok()
        } else {
            None
        };

        Self {
            id: record.id.0,
            sequence: record.sequence,
            branch: record.branch,
            record_type: record.record_type.clone(),
            timestamp: record.timestamp.0,
            payload_size,
            payload,
        }
    }
}

/// Summary of a branch (for events).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BranchSummary {
    pub id: u64,
    pub name: String,
    pub head: Sequence,
    pub parent: Option<String>,
    pub branch_point: Option<Sequence>,
    pub created: i64,
}

impl BranchSummary {
    pub fn from_branch(branch: &Branch, parent_name: Option<String>) -> Self {
        Self {
            id: branch.id.0,
            name: branch.name.clone(),
            head: branch.head,
            parent: parent_name,
            branch_point: branch.branch_point,
            created: branch.created.0,
        }
    }
}

/// Unique identifier for a subscription.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SubscriptionId(pub u64);

/// Handle to manage a subscription.
pub struct SubscriptionHandle {
    pub id: SubscriptionId,
    /// Channel to receive events.
    pub receiver: crossbeam_channel::Receiver<StoreEvent>,
}

impl SubscriptionHandle {
    /// Receive the next event (blocking).
    pub fn recv(&self) -> Result<StoreEvent, crossbeam_channel::RecvError> {
        self.receiver.recv()
    }

    /// Try to receive an event (non-blocking).
    pub fn try_recv(&self) -> Result<StoreEvent, crossbeam_channel::TryRecvError> {
        self.receiver.try_recv()
    }

    /// Receive with timeout.
    pub fn recv_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<StoreEvent, crossbeam_channel::RecvTimeoutError> {
        self.receiver.recv_timeout(timeout)
    }
}
