//! State management with per-state chains.
//!
//! State updates form independent chains per state ID, allowing O(k)
//! reconstruction where k = updates since last snapshot, regardless
//! of total events in the store.

mod chain_walk;
mod field_index;
mod manager;
mod operations;

pub use field_index::{FieldIndexKind, FieldIndexManager};
pub use manager::{
    ChainStats, CompactionStats, SnapshotNeeded, StateChainHead, StateIndex, StateManager,
};
pub(crate) use chain_walk::ChainWalk;
pub(crate) use manager::is_non_append_change;
pub use operations::{apply_operation, materialize_operations};
