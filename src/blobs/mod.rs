//! Content-addressed blob storage.
//!
//! Blobs are stored by their SHA-256 hash, sharded into directories
//! by the first byte of the hash (like Git objects).

mod chunks;
mod storage;

pub use storage::BlobStorage;
