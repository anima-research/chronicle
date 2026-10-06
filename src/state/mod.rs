//! State management with per-state chains.
//!
//! State updates form independent chains per state ID, allowing O(k)
//! reconstruction where k = updates since last snapshot, regardless
//! of total events in the store.

mod field_index;
mod manager;
mod operations;

pub use field_index::{FieldIndexKind, FieldIndexManager};
pub use manager::{
    BranchHeads, ChainStats, CompactionStats, SnapshotNeeded, StateChainHead, StateIndex,
    StateManager, StateSelection,
};
pub use operations::{apply_operation, materialize_operations};
pub(crate) use operations::{array_len_after, snapshot_item_count};
