//! # Record Store
//!
//! A self-describing, branchable record store where code, schema, and data
//! are all versioned together.
//!
//! ## Core Concepts
//!
//! - **Records**: Append-only log entries with type, payload, and links
//! - **Blobs**: Content-addressed storage for code and large data
//! - **Branches**: Cheap copy-on-write branches at any point
//! - **State**: Efficient state reconstruction via chains
//!
//! ## Example
//!
//! ```ignore
//! use record_store::{Store, StoreConfig, RecordInput};
//!
//! let store = Store::open_or_create(StoreConfig {
//!     path: "./my-store".into(),
//!     ..Default::default()
//! })?;
//!
//! // Append a record
//! let record = store.append(RecordInput::json("message", &json!({
//!     "text": "Hello, world!"
//! }))?)?;
//!
//! // Store a blob
//! let hash = store.store_blob(b"console.log('hello');", "application/javascript")?;
//!
//! // Create a branch
//! store.create_branch("experiment", None)?;
//! ```

pub mod blobs;
pub mod branches;
pub mod error;
#[cfg(feature = "napi-bindings")]
pub mod napi;
pub mod records;
pub mod state;
pub mod store;
pub mod subscriptions;
pub mod types;
pub mod wal;

// Re-exports
pub use blobs::BlobStorage;
pub use branches::{
    BranchGcOptions, BranchGcResult, BranchManager, HeadReconciliation, ReconciliationReport,
};
pub use error::{Result, StoreError};
pub use records::{RecordIndex, RecordLog, RecoveryReport};
pub use state::{
    apply_operation, ChainStats, CompactionStats, SnapshotNeeded, StateChainHead, StateIndex,
    StateManager,
};
pub use store::{CompactionSummary, Store, StoreConfig};
pub use subscriptions::{
    BranchSummary, DropReason, RecordSummary, StoreEvent, SubscriptionConfig, SubscriptionFilter,
    SubscriptionHandle, SubscriptionId, SubscriptionManager,
};
pub use types::*;
pub use wal::{WalEntry, WalEntryStatus, WalOperation, WriteAheadLog};
