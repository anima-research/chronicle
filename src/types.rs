//! Core types for the record store.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

/// Unique identifier for a record.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RecordId(pub u64);

impl fmt::Debug for RecordId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordId({})", self.0)
    }
}

impl fmt::Display for RecordId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Position in the log (per-branch).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default)]
pub struct Sequence(pub u64);

impl fmt::Debug for Sequence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Seq({})", self.0)
    }
}

impl Sequence {
    pub fn next(self) -> Self {
        Sequence(self.0 + 1)
    }

    pub fn prev(self) -> Option<Self> {
        if self.0 > 0 {
            Some(Sequence(self.0 - 1))
        } else {
            None
        }
    }
}

/// Unique identifier for a branch.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BranchId(pub u64);

impl fmt::Debug for BranchId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BranchId({})", self.0)
    }
}

/// Content hash for blobs (SHA-256).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Hash(pub [u8; 32]);

impl Hash {
    /// Compute hash from bytes.
    pub fn from_bytes(data: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(data);
        Hash(hasher.finalize().into())
    }

    /// Convert to hex string.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Parse from hex string.
    pub fn from_hex(s: &str) -> Result<Self, hex::FromHexError> {
        let bytes = hex::decode(s)?;
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| hex::FromHexError::InvalidStringLength)?;
        Ok(Hash(arr))
    }

    /// Get the first two characters of the hex (for sharding).
    pub fn shard_prefix(&self) -> String {
        hex::encode(&self.0[0..1])
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hash({}...)", &self.to_hex()[..8])
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// Microseconds since Unix epoch.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Timestamp(pub i64);

impl Timestamp {
    /// Current time.
    pub fn now() -> Self {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        Timestamp(duration.as_micros() as i64)
    }
}

impl fmt::Debug for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Timestamp({})", self.0)
    }
}

/// Payload encoding format.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PayloadEncoding {
    Json,
    MessagePack,
    Raw,
}

impl Default for PayloadEncoding {
    fn default() -> Self {
        PayloadEncoding::Json
    }
}

/// A single record in the store.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Record {
    /// Unique identifier (assigned by store).
    pub id: RecordId,

    /// Position in branch (assigned by store).
    pub sequence: Sequence,

    /// Which branch this record belongs to.
    pub branch: BranchId,

    /// When the record was created.
    pub timestamp: Timestamp,

    /// Application-defined type (e.g., "data", "schema", "code").
    pub record_type: String,

    /// Application-defined payload.
    pub payload: Vec<u8>,

    /// Payload encoding.
    pub encoding: PayloadEncoding,

    /// Records that caused this one.
    pub caused_by: Vec<RecordId>,

    /// Related records.
    pub linked_to: Vec<RecordId>,
}

/// Input for creating a new record (before id/sequence assigned).
#[derive(Clone, Debug)]
pub struct RecordInput {
    pub record_type: String,
    pub payload: Vec<u8>,
    pub encoding: PayloadEncoding,
    pub caused_by: Vec<RecordId>,
    pub linked_to: Vec<RecordId>,
}

impl RecordInput {
    /// Create a new record input with JSON payload.
    pub fn json(record_type: impl Into<String>, payload: &impl Serialize) -> Result<Self, serde_json::Error> {
        Ok(Self {
            record_type: record_type.into(),
            payload: serde_json::to_vec(payload)?,
            encoding: PayloadEncoding::Json,
            caused_by: Vec::new(),
            linked_to: Vec::new(),
        })
    }

    /// Create a new record input with raw bytes.
    pub fn raw(record_type: impl Into<String>, payload: Vec<u8>) -> Self {
        Self {
            record_type: record_type.into(),
            payload,
            encoding: PayloadEncoding::Raw,
            caused_by: Vec::new(),
            linked_to: Vec::new(),
        }
    }

    /// Add caused_by links.
    pub fn with_caused_by(mut self, ids: Vec<RecordId>) -> Self {
        self.caused_by = ids;
        self
    }

    /// Add linked_to links.
    pub fn with_linked_to(mut self, ids: Vec<RecordId>) -> Self {
        self.linked_to = ids;
        self
    }
}

/// Branch metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Branch {
    pub id: BranchId,
    pub name: String,
    pub head: Sequence,
    pub parent: Option<BranchId>,
    pub branch_point: Option<Sequence>,
    pub created: Timestamp,
}

/// Content-addressed blob.
#[derive(Clone, Debug)]
pub struct Blob {
    pub hash: Hash,
    pub content: Vec<u8>,
    pub content_type: String,
}

/// How state is stored and reconstructed.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StateStrategy {
    /// Store full value on each change.
    Snapshot,

    /// Store deltas with periodic snapshots.
    Delta { snapshot_every: u64 },

    /// Store appends with incremental snapshots.
    /// Supports append, redact, and edit operations.
    ///
    /// Uses a two-level snapshot hierarchy:
    /// - Delta snapshots: Store items added since last delta snapshot (frequent)
    /// - Full snapshots: Store entire state (rare, for cold-start recovery)
    ///
    /// Reconstruction traverses delta snapshots (fast) rather than individual ops.
    AppendLog {
        /// Create a delta snapshot after this many operations.
        delta_snapshot_every: u64,
        /// Create a full snapshot after this many delta snapshots.
        full_snapshot_every: u64,
    },

    /// Filesystem-tree state: sorted map of path → TreeEntry.
    /// Each entry references a blob hash in blob storage.
    /// Uses periodic snapshots with delta-based compaction.
    Tree {
        /// Create a delta snapshot after this many operations.
        delta_snapshot_every: u64,
        /// Create a full snapshot after this many delta snapshots.
        full_snapshot_every: u64,
    },

    /// Nested structure with per-field strategies.
    Struct {
        fields: HashMap<String, Box<StateStrategy>>,
    },
}

impl Default for StateStrategy {
    fn default() -> Self {
        StateStrategy::Snapshot
    }
}

/// Operation on state (stored in chain).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StateOperation {
    /// Set entire value (Snapshot strategy).
    Set(Vec<u8>),

    /// Apply delta (Delta strategy).
    Delta { old_hash: Hash, new_value: Vec<u8> },

    /// Append to collection (AppendLog).
    Append(Vec<u8>),

    /// Remove range from collection (AppendLog).
    Redact { start: usize, end: usize },

    /// Edit item at index (AppendLog).
    Edit { index: usize, new_value: Vec<u8> },

    /// Full snapshot (any strategy, periodic).
    /// For AppendLog, this stores the complete array.
    Snapshot(Vec<u8>),

    /// Delta snapshot for AppendLog strategy.
    /// Stores items added since last delta or full snapshot.
    /// During reconstruction, delta snapshots are concatenated.
    DeltaSnapshot(Vec<u8>),

    /// Set a single path in a tree state (Tree strategy).
    TreeSet { path: String, entry: Vec<u8> },

    /// Remove a single path from a tree state (Tree strategy).
    TreeRemove { path: String },

    /// Batch of tree operations, applied atomically (Tree strategy).
    /// Payload is serialized `Vec<TreeOp>`.
    TreeBatch { ops: Vec<u8> },

    /// Delta snapshot for Tree strategy.
    /// Contains serialized `Vec<TreeOp>` of all changes since last snapshot.
    TreeDeltaSnapshot(Vec<u8>),

    /// Update specific field (Struct strategy).
    Field {
        name: String,
        operation: Box<StateOperation>,
    },
}

/// A state update in the chain.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StateUpdateRecord {
    /// Record ID in the main log.
    pub record_id: RecordId,

    /// Global sequence number.
    pub global_sequence: Sequence,

    /// Which state this update belongs to.
    pub state_id: String,

    /// File offset of previous update for THIS state (forms chain).
    /// Using offset instead of RecordId enables disk-based traversal
    /// without needing an in-memory index.
    pub prev_update_offset: Option<u64>,

    /// The operation.
    pub operation: StateOperation,

    /// Timestamp.
    pub timestamp: Timestamp,
}

/// Registration for a state slot.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StateRegistration {
    pub id: String,
    pub strategy: StateStrategy,
    #[serde(default)]
    pub initial_value: Option<Vec<u8>>,
}

/// A single entry in a tree state (represents a file).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeEntry {
    /// SHA-256 hash of the file content (stored in blob storage).
    pub blob_hash: String,
    /// Size of the content in bytes.
    pub size: u64,
    /// File mode (e.g., 0o644 for regular, 0o755 for executable, 0o120000 for symlink).
    pub mode: u32,
}

/// A single operation on a tree entry.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TreeOp {
    /// Set (add or update) a file at a path.
    Set { path: String, entry: TreeEntry },
    /// Remove a file at a path.
    Remove { path: String },
}

/// A change between two tree states (returned by tree_diff).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TreeChange {
    /// File was added.
    Added { path: String, entry: TreeEntry },
    /// File was modified (content or mode changed).
    Modified {
        path: String,
        old: TreeEntry,
        new: TreeEntry,
    },
    /// File was removed.
    Removed { path: String, entry: TreeEntry },
}

/// The tree state itself: a sorted map of path → TreeEntry.
pub type TreeState = BTreeMap<String, TreeEntry>;

/// Store statistics.
#[derive(Clone, Debug, Default)]
pub struct StoreStats {
    pub record_count: u64,
    pub blob_count: u64,
    pub branch_count: u64,
    pub state_slot_count: u64,
    pub total_size_bytes: u64,
    pub blob_size_bytes: u64,
    pub index_size_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_roundtrip() {
        let data = b"hello world";
        let hash = Hash::from_bytes(data);
        let hex = hash.to_hex();
        let parsed = Hash::from_hex(&hex).unwrap();
        assert_eq!(hash, parsed);
    }

    #[test]
    fn test_hash_shard_prefix() {
        let hash = Hash::from_bytes(b"test");
        let prefix = hash.shard_prefix();
        assert_eq!(prefix.len(), 2);
    }

    #[test]
    fn test_sequence_navigation() {
        let seq = Sequence(5);
        assert_eq!(seq.next(), Sequence(6));
        assert_eq!(seq.prev(), Some(Sequence(4)));
        assert_eq!(Sequence(0).prev(), None);
    }

    #[test]
    fn test_record_input_json() {
        #[derive(Serialize)]
        struct TestPayload {
            message: String,
        }

        let input = RecordInput::json(
            "test",
            &TestPayload {
                message: "hello".into(),
            },
        )
        .unwrap();

        assert_eq!(input.record_type, "test");
        assert_eq!(input.encoding, PayloadEncoding::Json);
    }
}
