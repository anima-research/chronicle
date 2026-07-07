//! Append-only record log.

use crate::error::{Result, StoreError};
use crate::types::{BranchId, PayloadEncoding, Record, RecordId, RecordInput, Sequence, Timestamp};
use parking_lot::RwLock;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

/// Magic bytes for record log.
const LOG_MAGIC: &[u8; 4] = b"REC\0";

/// Current log format version.
const LOG_VERSION: u8 = 1;

/// Append-only record log.
pub struct RecordLog {

    /// Log file handle.
    file: RwLock<File>,

    /// Next record ID to assign.
    next_id: RwLock<u64>,

    /// Current file size (for appending).
    file_size: RwLock<u64>,

    /// Number of writes since last sync.
    writes_since_sync: RwLock<u64>,

    /// Sync every N writes (0 = sync every write, critical for durability vs performance)
    sync_interval: u64,
}

impl RecordLog {
    /// Default sync interval - sync every 100 writes for balance of durability and performance.
    const DEFAULT_SYNC_INTERVAL: u64 = 100;

    /// Open or create a record log with default sync interval.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_sync_interval(path, Self::DEFAULT_SYNC_INTERVAL)
    }

    /// Open or create a record log with custom sync interval.
    /// - sync_interval = 0: sync every write (safest, slowest)
    /// - sync_interval = 1: sync every write
    /// - sync_interval = 100: sync every 100 writes (good balance)
    /// - sync_interval = 1000: sync every 1000 writes (fastest, least durable)
    ///
    /// On open, the log is validated end-to-end (framing + checksums). A
    /// torn/partial record at the *tail* (the crash-during-append case) is
    /// recovered by truncating the file back to the last valid record; a
    /// warning is logged. Corruption in the *middle* of the log — i.e. a bad
    /// record with valid records after it — is NOT auto-recoverable and fails
    /// the open with `StoreError::Corruption`.
    pub fn open_with_sync_interval(path: impl AsRef<Path>, sync_interval: u64) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        let metadata = file.metadata()?;
        let file_size = metadata.len();

        // Validate the log and determine the next record ID. Recovers a torn
        // tail record (crash mid-append) by truncating to the last valid
        // record; fails on mid-log corruption.
        let (next_id, file_size) = if file_size > 0 {
            let (max_id, valid_len) = Self::scan_log(&mut file, file_size)?;
            if valid_len < file_size {
                let dropped = file_size - valid_len;
                tracing::warn!(
                    path = %path.display(),
                    offset = valid_len,
                    dropped_bytes = dropped,
                    "record log has a torn tail record; truncating to last valid record"
                );
                eprintln!(
                    "[chronicle] warning: record log {} has a torn tail record; \
                     truncating {} trailing byte(s) at offset {} (last valid record boundary)",
                    path.display(),
                    dropped,
                    valid_len
                );
                file.set_len(valid_len)?;
                file.sync_all()?;
            }
            (max_id + 1, valid_len)
        } else {
            (1, 0)
        };

        Ok(Self {
            file: RwLock::new(file),
            next_id: RwLock::new(next_id),
            file_size: RwLock::new(file_size),
            writes_since_sync: RwLock::new(0),
            sync_interval: if sync_interval == 0 { 1 } else { sync_interval },
        })
    }

    /// Append a record to the log.
    ///
    /// Returns the record and the offset where it was written.
    pub fn append(
        &self,
        input: RecordInput,
        branch: BranchId,
        sequence: Sequence,
    ) -> Result<(Record, u64)> {
        let mut file = self.file.write();

        // Assign ID
        let id = RecordId(*self.next_id.read());
        *self.next_id.write() += 1;

        let timestamp = Timestamp::now();

        // Build record
        let record = Record {
            id,
            sequence,
            branch,
            timestamp,
            record_type: input.record_type,
            payload: input.payload,
            encoding: input.encoding,
            caused_by: input.caused_by,
            linked_to: input.linked_to,
        };

        // Serialize and write
        let offset = *self.file_size.read();
        file.seek(SeekFrom::Start(offset))?;

        self.write_record(&mut *file, &record)?;

        let new_size = file.stream_position()?;
        *self.file_size.write() = new_size;

        // Sync periodically based on sync_interval
        let mut writes = self.writes_since_sync.write();
        *writes += 1;
        if *writes >= self.sync_interval {
            file.sync_all()?;
            *writes = 0;
        }

        Ok((record, offset))
    }

    /// Peek the record id the next `append` will assign, without consuming it.
    ///
    /// Only meaningful while the caller holds `Store::write_lock`; without
    /// that, another append can race in and the peeked value becomes stale.
    /// Crate-internal because there's no safe way to use it from outside
    /// `Store` — exposing it would invite the staleness footgun.
    pub(crate) fn peek_next_id(&self) -> crate::types::RecordId {
        crate::types::RecordId(*self.next_id.read())
    }

    /// Force sync all pending writes to disk.
    pub fn sync(&self) -> Result<()> {
        let file = self.file.write();
        file.sync_all()?;
        *self.writes_since_sync.write() = 0;
        Ok(())
    }

    /// Read a record at a given offset.
    pub fn read_at(&self, offset: u64) -> Result<Record> {
        let mut file = self.file.write();
        file.seek(SeekFrom::Start(offset))?;
        Self::read_record(&mut *file)
    }

    /// Iterate all records from the beginning.
    pub fn iter(&self) -> RecordIterator<'_> {
        self.iter_from(0)
    }

    /// Iterate all records from a given offset.
    pub fn iter_from(&self, offset: u64) -> RecordIterator<'_> {
        RecordIterator {
            log: self,
            offset,
            end: *self.file_size.read(),
        }
    }

    /// Get current file size.
    pub fn size(&self) -> u64 {
        *self.file_size.read()
    }

    /// Write a record to the file.
    fn write_record(&self, file: &mut File, record: &Record) -> Result<()> {
        // Magic
        file.write_all(LOG_MAGIC)?;

        // Version
        file.write_all(&[LOG_VERSION])?;

        // Flags (reserved)
        file.write_all(&[0u8])?;

        // Record ID
        file.write_all(&record.id.0.to_le_bytes())?;

        // Sequence
        file.write_all(&record.sequence.0.to_le_bytes())?;

        // Branch
        file.write_all(&record.branch.0.to_le_bytes())?;

        // Timestamp
        file.write_all(&record.timestamp.0.to_le_bytes())?;

        // Type
        let type_bytes = record.record_type.as_bytes();
        file.write_all(&(type_bytes.len() as u16).to_le_bytes())?;
        file.write_all(type_bytes)?;

        // Encoding
        let encoding_byte = match record.encoding {
            PayloadEncoding::Json => 0u8,
            PayloadEncoding::MessagePack => 1u8,
            PayloadEncoding::Raw => 2u8,
        };
        file.write_all(&[encoding_byte])?;

        // Payload
        file.write_all(&(record.payload.len() as u32).to_le_bytes())?;
        file.write_all(&record.payload)?;

        // Caused by
        file.write_all(&(record.caused_by.len() as u16).to_le_bytes())?;
        for id in &record.caused_by {
            file.write_all(&id.0.to_le_bytes())?;
        }

        // Linked to
        file.write_all(&(record.linked_to.len() as u16).to_le_bytes())?;
        for id in &record.linked_to {
            file.write_all(&id.0.to_le_bytes())?;
        }

        // Checksum of entire record (excluding checksum itself)
        // For simplicity, we'll compute checksum of payload only
        let checksum = crc32fast::hash(&record.payload);
        file.write_all(&checksum.to_le_bytes())?;

        Ok(())
    }

    /// Read a record from the file at current position.
    fn read_record(file: &mut File) -> Result<Record> {
        // Magic
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        if &magic != LOG_MAGIC {
            return Err(StoreError::InvalidFormat("Invalid record magic".into()));
        }

        // Version
        let mut version = [0u8; 1];
        file.read_exact(&mut version)?;
        if version[0] != LOG_VERSION {
            return Err(StoreError::InvalidFormat(format!(
                "Unsupported log version: {}",
                version[0]
            )));
        }

        // Flags
        let mut _flags = [0u8; 1];
        file.read_exact(&mut _flags)?;

        // Record ID
        let mut id_bytes = [0u8; 8];
        file.read_exact(&mut id_bytes)?;
        let id = RecordId(u64::from_le_bytes(id_bytes));

        // Sequence
        let mut seq_bytes = [0u8; 8];
        file.read_exact(&mut seq_bytes)?;
        let sequence = Sequence(u64::from_le_bytes(seq_bytes));

        // Branch
        let mut branch_bytes = [0u8; 8];
        file.read_exact(&mut branch_bytes)?;
        let branch = BranchId(u64::from_le_bytes(branch_bytes));

        // Timestamp
        let mut ts_bytes = [0u8; 8];
        file.read_exact(&mut ts_bytes)?;
        let timestamp = Timestamp(i64::from_le_bytes(ts_bytes));

        // Type
        let mut type_len_bytes = [0u8; 2];
        file.read_exact(&mut type_len_bytes)?;
        let type_len = u16::from_le_bytes(type_len_bytes) as usize;
        let mut type_bytes = vec![0u8; type_len];
        file.read_exact(&mut type_bytes)?;
        let record_type = String::from_utf8_lossy(&type_bytes).into_owned();

        // Encoding
        let mut encoding_byte = [0u8; 1];
        file.read_exact(&mut encoding_byte)?;
        let encoding = match encoding_byte[0] {
            0 => PayloadEncoding::Json,
            1 => PayloadEncoding::MessagePack,
            2 => PayloadEncoding::Raw,
            _ => PayloadEncoding::Raw,
        };

        // Payload
        let mut payload_len_bytes = [0u8; 4];
        file.read_exact(&mut payload_len_bytes)?;
        let payload_len = u32::from_le_bytes(payload_len_bytes) as usize;
        let mut payload = vec![0u8; payload_len];
        file.read_exact(&mut payload)?;

        // Caused by
        let mut caused_by_count_bytes = [0u8; 2];
        file.read_exact(&mut caused_by_count_bytes)?;
        let caused_by_count = u16::from_le_bytes(caused_by_count_bytes) as usize;
        let mut caused_by = Vec::with_capacity(caused_by_count);
        for _ in 0..caused_by_count {
            let mut id_bytes = [0u8; 8];
            file.read_exact(&mut id_bytes)?;
            caused_by.push(RecordId(u64::from_le_bytes(id_bytes)));
        }

        // Linked to
        let mut linked_to_count_bytes = [0u8; 2];
        file.read_exact(&mut linked_to_count_bytes)?;
        let linked_to_count = u16::from_le_bytes(linked_to_count_bytes) as usize;
        let mut linked_to = Vec::with_capacity(linked_to_count);
        for _ in 0..linked_to_count {
            let mut id_bytes = [0u8; 8];
            file.read_exact(&mut id_bytes)?;
            linked_to.push(RecordId(u64::from_le_bytes(id_bytes)));
        }

        // Checksum
        let mut checksum_bytes = [0u8; 4];
        file.read_exact(&mut checksum_bytes)?;
        let stored_checksum = u32::from_le_bytes(checksum_bytes);
        let computed_checksum = crc32fast::hash(&payload);

        if stored_checksum != computed_checksum {
            return Err(StoreError::ChecksumMismatch {
                expected: stored_checksum,
                got: computed_checksum,
            });
        }

        Ok(Record {
            id,
            sequence,
            branch,
            timestamp,
            record_type,
            payload,
            encoding,
            caused_by,
            linked_to,
        })
    }

    /// Validate the log from the beginning, returning the maximum record ID
    /// seen and the byte length of the valid prefix.
    ///
    /// Every record is fully parsed (framing + payload checksum). On the
    /// first invalid record:
    /// - if any *valid* record exists after the failure point, this is
    ///   mid-log corruption — unrecoverable without data loss — and an error
    ///   is returned;
    /// - otherwise the invalid bytes are a torn tail record (crash during the
    ///   last append) and `Ok((max_id, valid_len))` is returned with
    ///   `valid_len < file_size`, so the caller can truncate.
    fn scan_log(file: &mut File, file_size: u64) -> Result<(u64, u64)> {
        let mut max_id = 0u64;
        let mut valid_len = 0u64;

        while valid_len < file_size {
            file.seek(SeekFrom::Start(valid_len))?;
            match Self::read_record(file) {
                Ok(record) => {
                    max_id = max_id.max(record.id.0);
                    valid_len = file.stream_position()?;
                }
                Err(_) => {
                    if Self::has_valid_record_after(file, valid_len, file_size)? {
                        return Err(StoreError::Corruption(format!(
                            "corrupt record at offset {} with valid records after it; \
                             refusing to open (mid-log corruption is not auto-recoverable)",
                            valid_len
                        )));
                    }
                    // Torn tail: caller truncates to valid_len.
                    break;
                }
            }
        }

        Ok((max_id, valid_len))
    }

    /// Check whether any fully valid record starts strictly after `bad_offset`.
    ///
    /// Scans the remaining bytes for the record magic and attempts a full
    /// parse (including checksum) at each candidate. Used to distinguish a
    /// recoverable torn tail (no valid record follows) from mid-log
    /// corruption (valid records follow the bad one).
    fn has_valid_record_after(file: &mut File, bad_offset: u64, file_size: u64) -> Result<bool> {
        const CHUNK_SIZE: u64 = 64 * 1024;
        let magic_len = LOG_MAGIC.len(); // 4

        // Start strictly after the failed record's own start (its magic may
        // itself be valid — the record is torn further in).
        let mut pos = bad_offset + 1;

        while pos + magic_len as u64 <= file_size {
            let read_len = CHUNK_SIZE.min(file_size - pos) as usize;
            let mut buf = vec![0u8; read_len];
            file.seek(SeekFrom::Start(pos))?;
            file.read_exact(&mut buf)?;

            if buf.len() >= magic_len {
                for i in 0..=buf.len() - magic_len {
                    if &buf[i..i + magic_len] == LOG_MAGIC {
                        let candidate = pos + i as u64;
                        file.seek(SeekFrom::Start(candidate))?;
                        if Self::read_record(file).is_ok() {
                            return Ok(true);
                        }
                    }
                }
            }

            if read_len < magic_len {
                break;
            }
            // Overlap by magic_len - 1 bytes so a magic spanning two chunks
            // is still found.
            pos += (read_len - (magic_len - 1)) as u64;
        }

        Ok(false)
    }
}

/// Iterator over records in the log.
pub struct RecordIterator<'a> {
    log: &'a RecordLog,
    offset: u64,
    end: u64,
}

impl<'a> Iterator for RecordIterator<'a> {
    type Item = Result<(u64, Record)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.end {
            return None;
        }

        let current_offset = self.offset;
        match self.log.read_at(current_offset) {
            Ok(record) => {
                // Calculate next offset by re-reading position
                // This is a bit inefficient; we could track size during read
                let mut file = self.log.file.write();
                if let Ok(_) = file.seek(SeekFrom::Start(current_offset)) {
                    // Skip to end of record
                    if let Ok(rec) = RecordLog::read_record(&mut *file) {
                        drop(rec);
                        self.offset = file.stream_position().unwrap_or(self.end);
                    } else {
                        self.offset = self.end;
                    }
                } else {
                    self.offset = self.end;
                }
                Some(Ok((current_offset, record)))
            }
            Err(e) => {
                self.offset = self.end; // Stop iteration on error
                Some(Err(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_append_and_read() {
        let dir = TempDir::new().unwrap();
        let log = RecordLog::open(dir.path().join("log.bin")).unwrap();

        let input = RecordInput::raw("test", b"hello".to_vec());
        let (record, offset) = log.append(input, BranchId(1), Sequence(1)).unwrap();

        assert_eq!(record.id.0, 1);
        assert_eq!(record.sequence.0, 1);
        assert_eq!(record.payload, b"hello");
        assert_eq!(offset, 0);
    }

    #[test]
    fn test_multiple_records() {
        let dir = TempDir::new().unwrap();
        let log = RecordLog::open(dir.path().join("log.bin")).unwrap();

        for i in 1..=10 {
            let input = RecordInput::raw("test", format!("record {}", i).into_bytes());
            let (record, _offset) = log.append(input, BranchId(1), Sequence(i)).unwrap();
            assert_eq!(record.id.0, i);
        }

        // Iterate and verify
        let records: Vec<_> = log.iter_from(0).collect();
        assert_eq!(records.len(), 10);
    }

    /// Byte offset of the payload within a record, given its type-string length.
    /// Layout: magic(4) + version(1) + flags(1) + id(8) + seq(8) + branch(8)
    ///         + timestamp(8) + type_len(2) + type(n) + encoding(1) + payload_len(4)
    fn payload_offset(type_len: usize) -> u64 {
        (4 + 1 + 1 + 8 + 8 + 8 + 8 + 2 + type_len + 1 + 4) as u64
    }

    #[test]
    fn test_torn_tail_is_truncated_on_open() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("log.bin");

        {
            let log = RecordLog::open(&path).unwrap();
            for i in 1..=5u64 {
                let input = RecordInput::raw("test", format!("record {}", i).into_bytes());
                log.append(input, BranchId(1), Sequence(i)).unwrap();
            }
        }

        // Tear the tail: chop a few bytes off the last record.
        let len = std::fs::metadata(&path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(len - 3).unwrap();
        drop(file);

        // Reopen: must succeed with the torn record dropped.
        let log = RecordLog::open(&path).unwrap();
        let records: Vec<_> = log.iter().collect();
        assert_eq!(records.len(), 4);
        for (i, r) in records.iter().enumerate() {
            let (_, record) = r.as_ref().unwrap();
            assert_eq!(record.id.0, (i + 1) as u64);
        }

        // The file must have been truncated to the last valid record boundary.
        let new_len = std::fs::metadata(&path).unwrap().len();
        assert_eq!(new_len, log.size());
        assert!(new_len < len - 3);

        // Appends continue cleanly after recovery; the torn record's ID is
        // reissued (it was never durable).
        let (record, _) = log
            .append(RecordInput::raw("test", b"after recovery".to_vec()), BranchId(1), Sequence(5))
            .unwrap();
        assert_eq!(record.id.0, 5);
        let records: Vec<_> = log.iter().collect();
        assert_eq!(records.len(), 5);
    }

    #[test]
    fn test_torn_first_record_truncates_to_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("log.bin");

        {
            let log = RecordLog::open(&path).unwrap();
            log.append(RecordInput::raw("test", b"only".to_vec()), BranchId(1), Sequence(1))
                .unwrap();
        }

        // Tear the only record.
        let len = std::fs::metadata(&path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(len - 2).unwrap();
        drop(file);

        let log = RecordLog::open(&path).unwrap();
        assert_eq!(log.iter().count(), 0);
        assert_eq!(log.size(), 0);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);

        // IDs restart at 1 — nothing valid survived.
        let (record, _) = log
            .append(RecordInput::raw("test", b"fresh".to_vec()), BranchId(1), Sequence(1))
            .unwrap();
        assert_eq!(record.id.0, 1);
    }

    #[test]
    fn test_midlog_payload_corruption_fails_open() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("log.bin");

        let mut offsets = Vec::new();
        {
            let log = RecordLog::open(&path).unwrap();
            for i in 1..=5u64 {
                let input = RecordInput::raw("test", format!("record {}", i).into_bytes());
                let (_, offset) = log.append(input, BranchId(1), Sequence(i)).unwrap();
                offsets.push(offset);
            }
        }

        // Flip a payload byte of record 2 (framing intact, checksum broken).
        let target = offsets[1] + payload_offset("test".len());
        let mut file = OpenOptions::new().read(true).write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(target)).unwrap();
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte).unwrap();
        file.seek(SeekFrom::Start(target)).unwrap();
        file.write_all(&[byte[0] ^ 0xFF]).unwrap();
        drop(file);

        // Mid-log corruption must NOT be silently dropped: open fails.
        let err = match RecordLog::open(&path) {
            Ok(_) => panic!("expected open to fail on mid-log corruption"),
            Err(e) => e,
        };
        assert!(
            matches!(err, StoreError::Corruption(_)),
            "expected Corruption error, got: {:?}",
            err
        );
        // And the file was not truncated.
        assert!(std::fs::metadata(&path).unwrap().len() > offsets[4]);
    }

    #[test]
    fn test_midlog_framing_corruption_fails_open() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("log.bin");

        let mut offsets = Vec::new();
        {
            let log = RecordLog::open(&path).unwrap();
            for i in 1..=5u64 {
                let input = RecordInput::raw("test", format!("record {}", i).into_bytes());
                let (_, offset) = log.append(input, BranchId(1), Sequence(i)).unwrap();
                offsets.push(offset);
            }
        }

        // Destroy record 3's magic bytes.
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(offsets[2])).unwrap();
        file.write_all(b"XXXX").unwrap();
        drop(file);

        let err = match RecordLog::open(&path) {
            Ok(_) => panic!("expected open to fail on mid-log corruption"),
            Err(e) => e,
        };
        assert!(
            matches!(err, StoreError::Corruption(_)),
            "expected Corruption error, got: {:?}",
            err
        );
    }

    #[test]
    fn test_persistence() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("log.bin");

        // Write some records
        {
            let log = RecordLog::open(&path).unwrap();
            for i in 1..=5 {
                let input = RecordInput::raw("test", format!("record {}", i).into_bytes());
                log.append(input, BranchId(1), Sequence(i)).unwrap();
            }
        }

        // Reopen and verify
        {
            let log = RecordLog::open(&path).unwrap();
            let records: Vec<_> = log.iter_from(0).collect();
            assert_eq!(records.len(), 5);

            // Append more
            let input = RecordInput::raw("test", b"record 6".to_vec());
            let (record, _offset) = log.append(input, BranchId(1), Sequence(6)).unwrap();
            assert_eq!(record.id.0, 6); // Should continue from max ID
        }
    }
}
