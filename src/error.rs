//! Error types for the record store.

use crate::types::{Hash, RecordId, Sequence};
use thiserror::Error;

/// Main error type for store operations.
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// An fsync of the record log failed, so records written before it may
    /// not have reached the disk. Every later call that would publish a
    /// checkpoint returns this until the store is reopened, which is the only
    /// way to resume durable writes.
    #[error("Durability lost: {0}")]
    DurabilityLost(String),

    #[error("Record not found: {0}")]
    RecordNotFound(RecordId),

    #[error("Branch not found: {0}")]
    BranchNotFound(String),

    #[error("Blob not found: {0}")]
    BlobNotFound(Hash),

    #[error("State not registered: {0}")]
    StateNotRegistered(String),

    #[error("State already exists: {0}")]
    StateExists(String),

    #[error("Invalid sequence: {0:?} (head is {1:?})")]
    InvalidSequence(Sequence, Sequence),

    #[error("Branch already exists: {0}")]
    BranchExists(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Deserialization error: {0}")]
    Deserialization(String),

    #[error("Corruption detected: {0}")]
    Corruption(String),

    #[error("State strategy mismatch: expected {expected}, got {got}")]
    StrategyMismatch { expected: String, got: String },

    #[error("Store is locked by another process")]
    Locked,

    #[error("Store not initialized")]
    NotInitialized,

    #[error("Invalid store format: {0}")]
    InvalidFormat(String),

    #[error("Checksum mismatch: expected {expected}, got {got}")]
    ChecksumMismatch { expected: u32, got: u32 },

    #[error("Hash mismatch: expected {expected}, got {got}")]
    HashMismatch { expected: Hash, got: Hash },

    #[error("Transaction error: {0}")]
    Transaction(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Subscription was dropped")]
    SubscriptionDropped,
}

impl From<serde_json::Error> for StoreError {
    fn from(e: serde_json::Error) -> Self {
        StoreError::Serialization(e.to_string())
    }
}

impl From<rmp_serde::encode::Error> for StoreError {
    fn from(e: rmp_serde::encode::Error) -> Self {
        StoreError::Serialization(e.to_string())
    }
}

impl From<rmp_serde::decode::Error> for StoreError {
    fn from(e: rmp_serde::decode::Error) -> Self {
        StoreError::Deserialization(e.to_string())
    }
}

/// Result type for store operations.
pub type Result<T> = std::result::Result<T, StoreError>;
