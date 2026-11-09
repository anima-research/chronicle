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
/// as its `code`, beside its message, so a caller can tell kinds apart
/// without matching message text. (An error napi raises while it converts a
/// call's arguments, before chronicle runs, keeps napi's own code or none.)
/// Four errors of `storeBlobAsync` are neither, and carry no chronicle code:
/// it throws the error napi raises when it can't make its promise, and
/// napi's `GenericFailure` when it can't start the thread that stores blobs,
/// and its promise rejects with `GenericFailure` when storing panicked
/// partway or that thread has stopped, both faults in the binding rather
/// than the store's.
/// Each member's value is its name, kept across releases.
// A code's name is its variant's identifier: napi's string enum takes each
// value from it, and `as_str` spells the same name for `error.code`, so
// renaming a variant would change a published code.
#[cfg_attr(feature = "napi-bindings", napi_derive::napi(string_enum))]
#[cfg_attr(not(feature = "napi-bindings"), derive(Clone, Copy))]
#[derive(Debug, PartialEq, Eq)]
pub enum ErrorCode {
    /// A file operation failed: the operating system refused or failed it,
    /// a file ended before a read of it could finish, or chronicle refused a
    /// blob or checkpoint file that isn't a regular file, or a checkpoint
    /// reached through more links than it follows. The message carries its
    /// error.
    Io,
    /// An fsync of the record log failed. `sync()` throws it until the store
    /// is reopened, and so do the calls that checkpoint, unless a check of
    /// their own refuses them first.
    DurabilityLost,
    /// Not raised by this release.
    RecordNotFound,
    /// No branch has that name, or the branch a view is pinned to was
    /// deleted.
    BranchNotFound,
    /// Not raised by this release.
    BlobNotFound,
    /// No state is registered under that ID.
    StateNotRegistered,
    /// A state is already registered under that ID.
    StateExists,
    /// A branch was asked to start at a sequence past its parent's head.
    InvalidSequence,
    /// A branch already has that name.
    BranchExists,
    /// A value couldn't be encoded, as JSON or in the store's own format.
    Serialization,
    /// Bytes couldn't be decoded, as JSON or from the store's own format.
    Deserialization,
    /// A store file holds what no write leaves, such as a damaged log entry
    /// with valid ones after it, which open refuses.
    Corruption,
    /// Not raised by this release: a state's strategy conflicts are
    /// `InvalidOperation`.
    StrategyMismatch,
    /// The store is open already, in another process or through another
    /// open of it.
    Locked,
    /// Not raised through this binding: `open` of a path that holds no store
    /// fails with `InvalidFormat`.
    NotInitialized,
    /// A store or blob file isn't in a format this release reads, or is
    /// damaged, or the store's `LOCK` or `MANIFEST` isn't a regular file (a
    /// FIFO or a directory, say). Or the path holds
    /// no `MANIFEST`: `open` refuses such a path whatever else is there, and
    /// `openOrCreate` refuses it unless it holds nothing a new store can't
    /// sit beside, where it creates one. Both refuse a path whose `MANIFEST`
    /// is a link whose target doesn't exist. For a chunked blob (see
    /// `storeBlobAsync`), it's also the code when the blob's file is
    /// damaged, or a chunk it names is missing, damaged or holds other bytes.
    InvalidFormat,
    /// A record, or a blob stored whole, read from its file doesn't match its
    /// checksum. (A chunked blob's file or chunk that doesn't is
    /// `InvalidFormat`.)
    ChecksumMismatch,
    /// A blob's bytes don't match the hash they're stored under.
    HashMismatch,
    /// Not raised by this release.
    Transaction,
    /// The call was refused as asked: it was given an argument it rejects,
    /// such as a record ID that isn't an unsigned 64-bit integer, an edit
    /// index past the end or the reserved record type `state_update`; or
    /// the store or the handle doesn't allow it, such as deleting the main
    /// branch or the current one, `create` of a path that already holds a
    /// store, or `switchBranch` on a view. The message says which.
    InvalidOperation,
    /// The store doesn't hold the subscription. It dropped it because its
    /// buffer overran, or it was unsubscribed (to a poll, through another
    /// handle); or, to a catch-up, it was never made.
    SubscriptionDropped,
    /// The store, or this view of it, was closed with `close()`.
    StoreClosed,
    /// The handle polled holds no subscription with that ID: it wasn't made
    /// through that handle, was unsubscribed through it, or was the view's
    /// when the view was closed. A poll reaches only the subscriptions made
    /// through its own handle.
    SubscriptionNotFound,
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
            ErrorCode::SubscriptionNotFound => "SubscriptionNotFound",
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

    #[test]
    fn each_store_error_has_its_published_code() {
        let hash = Hash([0u8; 32]);
        let cases: Vec<(StoreError, &str)> = vec![
            (StoreError::Io(std::io::Error::other("disk")), "Io"),
            (StoreError::DurabilityLost("x".into()), "DurabilityLost"),
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

    #[test]
    fn the_bindings_own_codes_keep_their_names() {
        let cases = [
            (ErrorCode::StoreClosed, "StoreClosed"),
            (ErrorCode::SubscriptionNotFound, "SubscriptionNotFound"),
        ];
        for (code, name) in cases {
            assert_eq!(code.as_str(), name);
            assert_eq!(format!("{code:?}"), name);
        }
    }
}
