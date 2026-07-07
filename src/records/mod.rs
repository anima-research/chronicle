//! Record log implementation.
//!
//! Records are stored in an append-only log with a sequence index
//! for O(1) access by sequence number.

mod log;
mod index;

pub use log::{RecordLog, RecoveryReport};
pub use index::RecordIndex;
