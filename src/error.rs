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

/// What kind of failure an error from the Node binding is. Each error the
/// binding raises for a store failure, or for a call it refuses, carries one
/// as its `code`, beside its unchanged message, so a caller can tell kinds
/// apart without matching message text. (An error napi raises while it
/// converts a call's arguments, before chronicle runs, keeps napi's own code
/// or none.) A code's name is its variant's identifier: napi's string enum
/// takes each value from it, and `as_str` spells the same name for
/// `error.code`. Renaming a variant would change a published code, so the
/// tests below pin every name, against both.
#[cfg_attr(feature = "napi-bindings", napi_derive::napi(string_enum))]
#[cfg_attr(not(feature = "napi-bindings"), derive(Clone, Copy))]
#[derive(Debug, PartialEq, Eq)]
pub enum ErrorCode {
    // One per `StoreError` variant.
    Io,
    DurabilityLost,
    RecordNotFound,
    BranchNotFound,
    BlobNotFound,
    StateNotRegistered,
    StateExists,
    InvalidSequence,
    BranchExists,
    Serialization,
    Deserialization,
    Corruption,
    StrategyMismatch,
    Locked,
    NotInitialized,
    InvalidFormat,
    ChecksumMismatch,
    HashMismatch,
    Transaction,
    InvalidOperation,
    SubscriptionDropped,
    // The binding's own, for calls it refuses or can't complete itself.
    /// The store was closed with `close()`.
    StoreClosed,
    /// A state registration named a strategy other than snapshot, append_log or tree.
    InvalidStrategy,
    /// A record ID that isn't a non-negative integer.
    InvalidRecordId,
    /// A blob hash that isn't valid hex of the right length.
    InvalidHash,
    /// A subscription ID that isn't a non-negative integer.
    InvalidSubscriptionId,
    /// No subscription has that ID: it was never made, or was unsubscribed.
    SubscriptionNotFound,
    /// The subscription's channel closed.
    SubscriptionDisconnected,
    /// A tree operation other than set or remove, or a set without an entry.
    InvalidTreeOp,
    /// A limit that isn't a non-negative integer.
    InvalidLimit,
    /// A field index kind other than number or string.
    InvalidFieldIndexKind,
}

impl ErrorCode {
    /// The code's name, as JavaScript sees it in `error.code`.
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorCode::Io => "Io",
            ErrorCode::DurabilityLost => "DurabilityLost",
            ErrorCode::RecordNotFound => "RecordNotFound",
            ErrorCode::BranchNotFound => "BranchNotFound",
            ErrorCode::BlobNotFound => "BlobNotFound",
            ErrorCode::StateNotRegistered => "StateNotRegistered",
            ErrorCode::StateExists => "StateExists",
            ErrorCode::InvalidSequence => "InvalidSequence",
            ErrorCode::BranchExists => "BranchExists",
            ErrorCode::Serialization => "Serialization",
            ErrorCode::Deserialization => "Deserialization",
            ErrorCode::Corruption => "Corruption",
            ErrorCode::StrategyMismatch => "StrategyMismatch",
            ErrorCode::Locked => "Locked",
            ErrorCode::NotInitialized => "NotInitialized",
            ErrorCode::InvalidFormat => "InvalidFormat",
            ErrorCode::ChecksumMismatch => "ChecksumMismatch",
            ErrorCode::HashMismatch => "HashMismatch",
            ErrorCode::Transaction => "Transaction",
            ErrorCode::InvalidOperation => "InvalidOperation",
            ErrorCode::SubscriptionDropped => "SubscriptionDropped",
            ErrorCode::StoreClosed => "StoreClosed",
            ErrorCode::InvalidStrategy => "InvalidStrategy",
            ErrorCode::InvalidRecordId => "InvalidRecordId",
            ErrorCode::InvalidHash => "InvalidHash",
            ErrorCode::InvalidSubscriptionId => "InvalidSubscriptionId",
            ErrorCode::SubscriptionNotFound => "SubscriptionNotFound",
            ErrorCode::SubscriptionDisconnected => "SubscriptionDisconnected",
            ErrorCode::InvalidTreeOp => "InvalidTreeOp",
            ErrorCode::InvalidLimit => "InvalidLimit",
            ErrorCode::InvalidFieldIndexKind => "InvalidFieldIndexKind",
        }
    }
}

impl AsRef<str> for ErrorCode {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl StoreError {
    /// This error's kind, which the Node binding gives as `error.code`.
    pub fn code(&self) -> ErrorCode {
        match self {
            StoreError::Io(_) => ErrorCode::Io,
            StoreError::DurabilityLost(_) => ErrorCode::DurabilityLost,
            StoreError::RecordNotFound(_) => ErrorCode::RecordNotFound,
            StoreError::BranchNotFound(_) => ErrorCode::BranchNotFound,
            StoreError::BlobNotFound(_) => ErrorCode::BlobNotFound,
            StoreError::StateNotRegistered(_) => ErrorCode::StateNotRegistered,
            StoreError::StateExists(_) => ErrorCode::StateExists,
            StoreError::InvalidSequence(..) => ErrorCode::InvalidSequence,
            StoreError::BranchExists(_) => ErrorCode::BranchExists,
            StoreError::Serialization(_) => ErrorCode::Serialization,
            StoreError::Deserialization(_) => ErrorCode::Deserialization,
            StoreError::Corruption(_) => ErrorCode::Corruption,
            StoreError::StrategyMismatch { .. } => ErrorCode::StrategyMismatch,
            StoreError::Locked => ErrorCode::Locked,
            StoreError::NotInitialized => ErrorCode::NotInitialized,
            StoreError::InvalidFormat(_) => ErrorCode::InvalidFormat,
            StoreError::ChecksumMismatch { .. } => ErrorCode::ChecksumMismatch,
            StoreError::HashMismatch { .. } => ErrorCode::HashMismatch,
            StoreError::Transaction(_) => ErrorCode::Transaction,
            StoreError::InvalidOperation(_) => ErrorCode::InvalidOperation,
            StoreError::SubscriptionDropped => ErrorCode::SubscriptionDropped,
        }
    }
}

/// Result type for store operations.
pub type Result<T> = std::result::Result<T, StoreError>;

#[cfg(test)]
mod tests {
    use super::*;

    /// Each `StoreError` variant's code, by the name JavaScript sees. A code
    /// is published as `error.code`, and as the value of `ErrorCode`'s
    /// member, which napi takes from the variant's identifier; so changing
    /// one breaks callers that branch on it, and this test pins both.
    #[test]
    fn each_store_error_has_its_published_code() {
        let hash = Hash([0u8; 32]);
        let cases: Vec<(StoreError, &str)> = vec![
            (StoreError::Io(std::io::Error::other("disk")), "Io"),
            (StoreError::RecordNotFound(RecordId(7)), "RecordNotFound"),
            (StoreError::BranchNotFound("main".into()), "BranchNotFound"),
            (StoreError::BlobNotFound(hash), "BlobNotFound"),
            (
                StoreError::StateNotRegistered("s".into()),
                "StateNotRegistered",
            ),
            (StoreError::StateExists("s".into()), "StateExists"),
            (
                StoreError::InvalidSequence(Sequence(1), Sequence(2)),
                "InvalidSequence",
            ),
            (StoreError::BranchExists("b".into()), "BranchExists"),
            (StoreError::Serialization("x".into()), "Serialization"),
            (StoreError::Deserialization("x".into()), "Deserialization"),
            (StoreError::Corruption("x".into()), "Corruption"),
            (
                StoreError::StrategyMismatch {
                    expected: "a".into(),
                    got: "b".into(),
                },
                "StrategyMismatch",
            ),
            (StoreError::Locked, "Locked"),
            (StoreError::NotInitialized, "NotInitialized"),
            (StoreError::InvalidFormat("x".into()), "InvalidFormat"),
            (
                StoreError::ChecksumMismatch {
                    expected: 1,
                    got: 2,
                },
                "ChecksumMismatch",
            ),
            (
                StoreError::HashMismatch {
                    expected: hash,
                    got: hash,
                },
                "HashMismatch",
            ),
            (StoreError::Transaction("x".into()), "Transaction"),
            (StoreError::InvalidOperation("x".into()), "InvalidOperation"),
            (StoreError::SubscriptionDropped, "SubscriptionDropped"),
        ];
        for (error, code) in cases {
            assert_eq!(error.code().as_str(), code, "{error}");
            assert_eq!(format!("{:?}", error.code()), code, "{error}");
        }
    }

    /// The binding's own codes, by the name JavaScript sees, in `error.code`
    /// and as the enum's value.
    #[test]
    fn the_bindings_own_codes_keep_their_names() {
        let cases = [
            (ErrorCode::StoreClosed, "StoreClosed"),
            (ErrorCode::InvalidStrategy, "InvalidStrategy"),
            (ErrorCode::InvalidRecordId, "InvalidRecordId"),
            (ErrorCode::InvalidHash, "InvalidHash"),
            (ErrorCode::InvalidSubscriptionId, "InvalidSubscriptionId"),
            (ErrorCode::SubscriptionNotFound, "SubscriptionNotFound"),
            (
                ErrorCode::SubscriptionDisconnected,
                "SubscriptionDisconnected",
            ),
            (ErrorCode::InvalidTreeOp, "InvalidTreeOp"),
            (ErrorCode::InvalidLimit, "InvalidLimit"),
            (ErrorCode::InvalidFieldIndexKind, "InvalidFieldIndexKind"),
        ];
        for (code, name) in cases {
            assert_eq!(code.as_str(), name);
            assert_eq!(format!("{code:?}"), name);
        }
    }
}
