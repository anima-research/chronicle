//! Branch management with copy-on-write semantics.
//!
//! Branches enable cheap forks at any point in history. They share
//! records with their parent up to the branch point, then diverge.

mod manager;

pub use manager::{
    BranchGcOptions, BranchGcResult, BranchManager, HeadReconciliation, ReconciliationReport,
};
