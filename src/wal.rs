//! Write-Ahead Log for crash recovery.
//!
//! The WAL ensures durability by writing operations to a separate log
//! before they are committed to the main store. On recovery, uncommitted
//! operations can be replayed.

use crate::error::{Result, StoreError};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Magic bytes for WAL file.
const WAL_MAGIC: &[u8; 4] = b"WAL\0";

/// Current WAL format version.
const WAL_VERSION: u8 = 1;

/// WAL entry status.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalEntryStatus {
    /// Entry has been written but not yet committed.
    Pending,
    /// Entry has been committed to the main store.
    Committed,
    /// Entry was rolled back (not used, but reserved).
    RolledBack,
}

/// A single WAL entry.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalEntry {
    /// Unique sequence number for this entry.
    pub seq: u64,
    /// Entry status.
    pub status: WalEntryStatus,
    /// The operation type.
    pub operation: WalOperation,
    /// Timestamp when entry was created.
    pub timestamp: u64,
}

/// Operations that can be recorded in the WAL.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WalOperation {
    /// Append a record to the log.
    AppendRecord {
        record_type: String,
        payload: Vec<u8>,
    },
    /// Update a state.
    UpdateState {
        state_id: String,
        operation_data: Vec<u8>, // Serialized StateOperation
    },
    /// Store a blob.
    StoreBlob {
        content: Vec<u8>,
        content_type: String,
    },
    /// Create a branch.
    CreateBranch {
        name: String,
        from: Option<String>,
    },
}

/// Write-Ahead Log manager.
pub struct WriteAheadLog {
    /// Path to the WAL file.
    path: PathBuf,
    /// Current sequence number.
    next_seq: Mutex<u64>,
    /// Write handle.
    writer: Mutex<Option<BufWriter<File>>>,
}

impl WriteAheadLog {
    /// Create or open a WAL file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        let (next_seq, writer) = if path.exists() {
            // Open existing WAL and find highest sequence number
            let file = OpenOptions::new().read(true).write(true).open(&path)?;
            let mut reader = BufReader::new(file);

            // Verify header
            let mut magic = [0u8; 4];
            reader.read_exact(&mut magic)?;
            if &magic != WAL_MAGIC {
                return Err(StoreError::InvalidFormat("Invalid WAL magic".into()));
            }

            let mut version = [0u8; 1];
            reader.read_exact(&mut version)?;
            if version[0] != WAL_VERSION {
                return Err(StoreError::InvalidFormat(format!(
                    "Unsupported WAL version: {}",
                    version[0]
                )));
            }

            // Read entries to find max sequence
            let mut max_seq = 0u64;
            while let Ok(entry) = Self::read_entry(&mut reader) {
                max_seq = max_seq.max(entry.seq);
            }

            // Reopen for appending
            let file = OpenOptions::new().append(true).open(&path)?;

            (max_seq + 1, Some(BufWriter::new(file)))
        } else {
            // Create new WAL
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)?;

            // Write header
            file.write_all(WAL_MAGIC)?;
            file.write_all(&[WAL_VERSION])?;
            file.sync_all()?;

            (1, Some(BufWriter::new(file)))
        };

        Ok(Self {
            path,
            next_seq: Mutex::new(next_seq),
            writer: Mutex::new(writer),
        })
    }

    /// Log an operation (returns sequence number).
    pub fn log(&self, operation: WalOperation) -> Result<u64> {
        let mut next_seq = self.next_seq.lock();
        let seq = *next_seq;
        *next_seq += 1;

        let entry = WalEntry {
            seq,
            status: WalEntryStatus::Pending,
            operation,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };

        let mut writer = self.writer.lock();
        if let Some(ref mut w) = *writer {
            Self::write_entry(w, &entry)?;
            w.flush()?;
            // fsync for durability
            w.get_ref().sync_all()?;
        }

        Ok(seq)
    }

    /// Mark an entry as committed.
    pub fn commit(&self, seq: u64) -> Result<()> {
        // For simplicity, we'll write a new commit marker entry
        // A more sophisticated implementation would update in place
        let mut writer = self.writer.lock();
        if let Some(ref mut w) = *writer {
            let marker = WalEntry {
                seq,
                status: WalEntryStatus::Committed,
                operation: WalOperation::AppendRecord {
                    record_type: "_commit".to_string(),
                    payload: vec![],
                },
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            };
            Self::write_entry(w, &marker)?;
            w.flush()?;
            w.get_ref().sync_all()?;
        }
        Ok(())
    }

    /// Get all pending (uncommitted) entries.
    pub fn get_pending_entries(&self) -> Result<Vec<WalEntry>> {
        let mut file = File::open(&self.path)?;

        // Skip header
        file.seek(SeekFrom::Start(5))?;

        let mut reader = BufReader::new(file);
        let mut entries = std::collections::HashMap::new();
        let mut committed = std::collections::HashSet::new();

        // Read all entries
        while let Ok(entry) = Self::read_entry(&mut reader) {
            if entry.status == WalEntryStatus::Committed {
                committed.insert(entry.seq);
            } else if entry.status == WalEntryStatus::Pending {
                entries.insert(entry.seq, entry);
            }
        }

        // Filter out committed entries
        let pending: Vec<_> = entries
            .into_iter()
            .filter(|(seq, _)| !committed.contains(seq))
            .map(|(_, entry)| entry)
            .collect();

        Ok(pending)
    }

    /// Clear the WAL (called after successful checkpoint).
    pub fn clear(&self) -> Result<()> {
        let mut writer = self.writer.lock();
        *writer = None;

        // Truncate and reinitialize
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)?;

        file.write_all(WAL_MAGIC)?;
        file.write_all(&[WAL_VERSION])?;
        file.sync_all()?;

        *writer = Some(BufWriter::new(
            OpenOptions::new().append(true).open(&self.path)?,
        ));

        *self.next_seq.lock() = 1;

        Ok(())
    }

    /// Check if WAL has any pending entries.
    pub fn has_pending(&self) -> Result<bool> {
        Ok(!self.get_pending_entries()?.is_empty())
    }

    fn write_entry(writer: &mut BufWriter<File>, entry: &WalEntry) -> Result<()> {
        let encoded =
            rmp_serde::to_vec(entry).map_err(|e| StoreError::Serialization(e.to_string()))?;

        let len = encoded.len() as u32;
        writer.write_all(&len.to_le_bytes())?;
        writer.write_all(&encoded)?;

        // Write checksum (simple CRC32)
        let checksum = crc32fast::hash(&encoded);
        writer.write_all(&checksum.to_le_bytes())?;

        Ok(())
    }

    fn read_entry(reader: &mut BufReader<File>) -> Result<WalEntry> {
        let mut len_bytes = [0u8; 4];
        reader.read_exact(&mut len_bytes)?;
        let len = u32::from_le_bytes(len_bytes) as usize;

        if len > 100 * 1024 * 1024 {
            // 100MB sanity check
            return Err(StoreError::Corruption("WAL entry too large".into()));
        }

        let mut encoded = vec![0u8; len];
        reader.read_exact(&mut encoded)?;

        let mut checksum_bytes = [0u8; 4];
        reader.read_exact(&mut checksum_bytes)?;
        let stored_checksum = u32::from_le_bytes(checksum_bytes);

        // Verify checksum
        let computed_checksum = crc32fast::hash(&encoded);
        if stored_checksum != computed_checksum {
            return Err(StoreError::Corruption("WAL checksum mismatch".into()));
        }

        rmp_serde::from_slice(&encoded).map_err(|e| StoreError::Deserialization(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_wal_basic() {
        let dir = TempDir::new().unwrap();
        let wal = WriteAheadLog::open(dir.path().join("test.wal")).unwrap();

        // Log an operation
        let seq = wal
            .log(WalOperation::AppendRecord {
                record_type: "test".to_string(),
                payload: b"hello".to_vec(),
            })
            .unwrap();

        // Should have one pending entry
        let pending = wal.get_pending_entries().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].seq, seq);

        // Commit it
        wal.commit(seq).unwrap();

        // Should have no pending entries
        let pending = wal.get_pending_entries().unwrap();
        assert!(pending.is_empty());
    }

    #[test]
    fn test_wal_multiple_entries() {
        let dir = TempDir::new().unwrap();
        let wal = WriteAheadLog::open(dir.path().join("test.wal")).unwrap();

        // Log multiple operations
        let seq1 = wal
            .log(WalOperation::AppendRecord {
                record_type: "test1".to_string(),
                payload: b"one".to_vec(),
            })
            .unwrap();

        let seq2 = wal
            .log(WalOperation::AppendRecord {
                record_type: "test2".to_string(),
                payload: b"two".to_vec(),
            })
            .unwrap();

        let seq3 = wal
            .log(WalOperation::AppendRecord {
                record_type: "test3".to_string(),
                payload: b"three".to_vec(),
            })
            .unwrap();

        assert!(seq1 != seq2 && seq2 != seq3 && seq1 != seq3, "{seq1} {seq2} {seq3}");

        // Commit first and third
        wal.commit(seq1).unwrap();
        wal.commit(seq3).unwrap();

        // Only second should be pending
        let pending = wal.get_pending_entries().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].seq, seq2);
    }

    #[test]
    fn test_wal_persistence() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");

        // Write and close
        {
            let wal = WriteAheadLog::open(&wal_path).unwrap();
            wal.log(WalOperation::StoreBlob {
                content: b"blob content".to_vec(),
                content_type: "text/plain".to_string(),
            })
            .unwrap();
            // Drop without committing
        }

        // Reopen and check pending
        {
            let wal = WriteAheadLog::open(&wal_path).unwrap();
            let pending = wal.get_pending_entries().unwrap();
            assert_eq!(pending.len(), 1);

            if let WalOperation::StoreBlob { content, .. } = &pending[0].operation {
                assert_eq!(content, b"blob content");
            } else {
                panic!("Wrong operation type");
            }
        }
    }

    #[test]
    fn test_wal_clear() {
        let dir = TempDir::new().unwrap();
        let wal = WriteAheadLog::open(dir.path().join("test.wal")).unwrap();

        // Log some operations
        wal.log(WalOperation::AppendRecord {
            record_type: "test".to_string(),
            payload: vec![],
        })
        .unwrap();

        assert!(wal.has_pending().unwrap());

        // Clear
        wal.clear().unwrap();

        assert!(!wal.has_pending().unwrap());

        // Can log again
        let seq = wal
            .log(WalOperation::AppendRecord {
                record_type: "after_clear".to_string(),
                payload: vec![],
            })
            .unwrap();

        let pending = wal.get_pending_entries().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].seq, seq);
    }
}
