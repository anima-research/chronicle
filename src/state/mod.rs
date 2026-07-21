//! State management with per-state chains.
//!
//! State updates form independent chains per state ID, allowing O(k)
//! reconstruction where k = updates since last snapshot, regardless
//! of total events in the store.

mod manager;
mod operations;

pub use manager::{
    ChainStats, CompactionStats, SnapshotNeeded, StateChainHead, StateIndex, StateManager,
};
pub use operations::{apply_operation, materialize_operations};
