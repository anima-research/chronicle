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
///
/// The `Vec<u8>` value fields carry `serde_bytes` so a bytes-aware format
/// (MessagePack — see [`StateUpdateRecord::encode`]) stores them as raw
/// `bin` payloads instead of per-byte integer sequences. JSON output is
/// unchanged by the attribute (serde_json renders bytes as an integer
/// array either way), so historical JSON-encoded records round-trip
/// identically.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StateOperation {
    /// Set entire value (Snapshot strategy).
    Set(#[serde(with = "serde_bytes")] Vec<u8>),

    /// Apply delta (Delta strategy).
    Delta {
        old_hash: Hash,
        #[serde(with = "serde_bytes")]
        new_value: Vec<u8>,
    },

    /// Append to collection (AppendLog).
    Append(#[serde(with = "serde_bytes")] Vec<u8>),

    /// Remove range from collection (AppendLog).
    Redact { start: usize, end: usize },

    /// Edit item at index (AppendLog).
    Edit {
        index: usize,
        #[serde(with = "serde_bytes")]
        new_value: Vec<u8>,
    },

    /// Full snapshot (any strategy, periodic).
    /// For AppendLog, this stores the complete array.
    Snapshot(#[serde(with = "serde_bytes")] Vec<u8>),

    /// Delta snapshot for AppendLog strategy.
    /// Stores items added since last delta or full snapshot.
    /// During reconstruction, delta snapshots are concatenated.
    DeltaSnapshot(#[serde(with = "serde_bytes")] Vec<u8>),

    /// Set a single path in a tree state (Tree strategy).
    TreeSet { path: String, entry: TreeEntry },

    /// Remove a single path from a tree state (Tree strategy).
    TreeRemove { path: String },

    /// Batch of tree operations, applied atomically (Tree strategy).
    TreeBatch { ops: Vec<TreeOp> },

    /// Delta snapshot for Tree strategy.
    /// Contains `Vec<TreeOp>` of all changes since last snapshot.
    TreeDeltaSnapshot(Vec<TreeOp>),

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

impl StateUpdateRecord {
    /// Encode for storage: MessagePack, so the operation's value bytes are
    /// stored as raw `bin` payloads. The previous JSON encoding rendered
    /// every value byte as a JSON integer (~4x size, per-byte parse cost) —
    /// on large states this made each chain read/write CPU-bound in serde
    /// (2026-08-01 Mythos turn-boundary stalls: 10-30s per boundary against
    /// a 114 MB messages snapshot).
    ///
    /// Returns the payload together with the encoding tag to stamp on the
    /// record, which is what [`Self::decode`] gates on.
    ///
    /// `to_vec_named` (map-keyed structs), not `to_vec` (positional arrays):
    /// this is a forever on-disk format — field-name keying survives field
    /// reordering and, like JSON, tolerates additive evolution. Serde's
    /// derive accepts both shapes on read, so this is also the safer target
    /// for any future re-encode. Cost is ~a hundred bytes of field names per
    /// record against multi-KB payloads.
    pub fn encode(&self) -> Result<(Vec<u8>, PayloadEncoding), crate::error::StoreError> {
        let payload = rmp_serde::to_vec_named(self)
            .map_err(|e| crate::error::StoreError::Serialization(e.to_string()))?;
        Ok((payload, PayloadEncoding::MessagePack))
    }

    /// Decode from a stored record, gated on the record's encoding tag.
    ///
    /// Historical state_update records were written via `RecordInput::raw`,
    /// so their encoding byte is `Raw` (and some test fixtures use `Json`)
    /// while the payload is actually JSON — treat everything that is not
    /// MessagePack as JSON. Once a store contains MessagePack state records
    /// it can no longer be opened by chronicle versions predating this
    /// method (forward-only migration).
    pub fn decode(record: &Record) -> Result<Self, crate::error::StoreError> {
        match record.encoding {
            PayloadEncoding::MessagePack => rmp_serde::from_slice(&record.payload)
                .map_err(|e| crate::error::StoreError::Deserialization(e.to_string())),
            PayloadEncoding::Json | PayloadEncoding::Raw => {
                serde_json::from_slice(&record.payload)
                    .map_err(|e| crate::error::StoreError::Deserialization(e.to_string()))
            }
        }
    }
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
