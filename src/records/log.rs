//! Append-only record log.

use crate::error::{Result, StoreError};
use crate::types::{BranchId, PayloadEncoding, Record, RecordId, RecordInput, Sequence, Timestamp};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;

/// Magic bytes for record log.
const LOG_MAGIC: &[u8; 4] = b"REC\0";

/// Current log format version.
const LOG_VERSION: u8 = 1;

#[cfg(test)]
thread_local! {
    /// How many records this thread has begun to decode, so unit tests can
    /// count the reads a pass over the log makes.
    pub(crate) static RECORD_DECODES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// What an open-time scan of the log found and (if a torn tail was recovered)
/// what it dropped. Surfaced via [`RecordLog::recovery`] so callers can log or
/// react to silent data truncation instead of only seeing a stderr warning.
/// `Store::recovery` adds what the store's open found in the state-update
/// tail, and reports it even when no tail was torn.
#[derive(Clone, Debug)]
pub struct RecoveryReport {
    /// Byte offset the file was truncated back to (end of the last valid
    /// record), or the log's length when nothing was torn.
    pub truncated_at: u64,
    /// Number of trailing bytes discarded. NOTE: this is *all* bytes after the
    /// last valid record — with the default `sync_interval`, up to that many
    /// un-fsynced records. Bit-rot in the final fsynced record is
    /// indistinguishable from a tear and is counted here too. 0 when nothing
    /// was torn.
    pub dropped_bytes: u64,
    /// Number of valid records that survived (i.e. the length of the recovered log).
    pub valid_records: u64,
    /// Each state whose updates open replayed although `state.bin` holds no
    /// registration for it, with the number of updates replayed: a state
    /// written without a registration, or whose registration an earlier
    /// release lost in a crash. Empty from [`RecordLog::recovery`].
    pub unregistered_states: Vec<(String, u64)>,
    /// Records past the checkpoint typed `state_update` that don't decode as
    /// state updates, which open skipped. They stay in the log as ordinary
    /// records. 0 from [`RecordLog::recovery`].
    pub undecodable_state_updates: u64,
    /// Records past the checkpoint typed `state_update` on a branch that
    /// `branches.bin` doesn't hold, which open didn't replay, since nothing can
    /// read that branch: an earlier release saved a branch only at sync, so a
    /// crash after creating one can leave its updates without it. 0 from
    /// [`RecordLog::recovery`].
    pub updates_on_missing_branches: u64,
}

/// Outcome of a full validating scan of the log.
struct ScanResult {
    /// Maximum record ID observed across all valid records.
    max_id: u64,
    /// Byte length of the valid prefix (== file_size when nothing was torn).
    valid_len: u64,
    /// Count of valid records.
    valid_records: u64,
    /// Maximum sequence observed per branch — used to reconcile branch heads
    /// against the durable log (a stale `branches.bin` or a torn tail can leave
    /// the persisted head disagreeing with what the log actually holds).
    branch_max_seq: HashMap<BranchId, Sequence>,
}

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

    sync_bytes: usize,

    /// The first failed fsync of the log since its last good one. Linux
    /// reports a writeback error to one fsync on an open file, and a later
    /// fsync succeeds although the pages that failed may never reach the disk,
    /// so a later fsync alone can't show that the records written before it
    /// are durable. While a failure stands, `append` refuses, and
    /// [`RecordLog::sync`] writes the bytes in `unsynced` again before its
    /// fsync, which clears the failure when it succeeds.
    durability_failure: Mutex<Option<String>>,

    /// The bytes written to the log since its last good fsync, kept in memory
    /// so that a sync after a failed fsync can write them again. They come
    /// from memory and never from the file: after a failed write-back, the
    /// file can give the page cache's copy or the disk's older bytes. For the
    /// same reason, a read of a record among them decodes it from here.
    unsynced: Mutex<Unsynced>,

    /// How many of the next fsyncs fail without reaching the file, so unit
    /// tests can stand in for a device error.
    #[cfg(test)]
    pub(crate) fsync_failures: Mutex<u64>,

    #[cfg(test)]
    pub(crate) rewrite_failures: Mutex<u64>,

    #[cfg(test)]
    pub(crate) write_failures: Mutex<u64>,

    /// How many of the next `read_at` calls fail with an I/O error before
    /// reading, so unit tests can stand in for a failing device.
    #[cfg(test)]
    pub(crate) read_failures: Mutex<u64>,

    /// Highest sequence seen per branch during the open scan. Immutable after
    /// open; `Store::open` reconciles branch heads against this.
    branch_max_seq: HashMap<BranchId, Sequence>,

    /// If a torn tail was recovered on open, what was dropped. `None` otherwise.
    recovery: Option<RecoveryReport>,
}

struct Unsynced {
    synced_len: u64,
    bytes: Vec<u8>,
}

#[cfg(test)]
thread_local! {
    static OPEN_FSYNC_FAILURES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

impl RecordLog {
    /// Default sync interval - sync every 100 writes for balance of durability and performance.
    const DEFAULT_SYNC_INTERVAL: u64 = 100;

    const SYNC_BYTES: usize = 64 * 1024 * 1024;

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
    /// An append also syncs sooner, once the records since the last good
    /// fsync pass 64 MiB.
    ///
    /// A failed fsync doesn't fail the append that made it. The appends after
    /// it are refused until [`RecordLog::sync`] writes the records since the
    /// last good fsync again and its fsync succeeds, or the log is opened
    /// again.
    ///
    /// The open fsyncs the log before it takes the log's length as the point
    /// of its last good fsync, so that a later failure can only touch records
    /// that this handle wrote and keeps. If that fsync fails, the open fails
    /// with `StoreError::DurabilityLost`: records written before this open
    /// may not be on the disk, and a later open doesn't report the failure
    /// again.
    ///
    /// On open, the log is validated end-to-end (framing + checksums). A
    /// torn/partial *tail* — the crash-during-append case — is recovered by
    /// truncating the file back to the last valid record boundary. This drops
    /// **all** trailing bytes after that boundary, which (with a non-zero
    /// `sync_interval`) can be up to `sync_interval` un-fsynced records, not a
    /// single record; bit-rot in the final fsynced record is byte-for-byte
    /// indistinguishable from a tear and is dropped the same way. Details are
    /// logged and surfaced via [`RecordLog::recovery`].
    ///
    /// Only `UnexpectedEof`, `InvalidFormat`, and `ChecksumMismatch` are treated
    /// as torn-tail candidates. Any other I/O error (e.g. `EIO` from a flaky
    /// sector or a network filesystem) propagates and fails the open rather than
    /// triggering a truncation of possibly-valid data.
    ///
    /// Corruption in the *middle* of the log — a bad record with valid records
    /// after it — is NOT auto-recoverable and fails with `StoreError::Corruption`.
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
        let (next_id, file_size, branch_max_seq, recovery) = if file_size > 0 {
            let scan = Self::scan_log(&mut file, &path, file_size)?;
            let recovery = if scan.valid_len < file_size {
                let dropped = file_size - scan.valid_len;
                tracing::warn!(
                    path = %path.display(),
                    truncated_at = scan.valid_len,
                    dropped_bytes = dropped,
                    valid_records = scan.valid_records,
                    "record log tail was torn (crash during append); truncating ALL bytes \
                     after the last valid record. Up to `sync_interval` un-fsynced records may \
                     be lost, and bit-rot in the final fsynced record is indistinguishable from \
                     a tear and is dropped the same way"
                );
                file.set_len(scan.valid_len)?;
                Some(RecoveryReport {
                    truncated_at: scan.valid_len,
                    dropped_bytes: dropped,
                    valid_records: scan.valid_records,
                    unregistered_states: Vec::new(),
                    undecodable_state_updates: 0,
                    updates_on_missing_branches: 0,
                })
            } else {
                None
            };
            (scan.max_id + 1, scan.valid_len, scan.branch_max_seq, recovery)
        } else {
            (1, 0, HashMap::new(), None)
        };

        Self::fsync_at_open(&file).map_err(|error| {
            let truncated = match &recovery {
                Some(report) => format!(
                    ". Before the fsync, this open truncated a torn tail of {} bytes after the \
                     last whole record, and a later open won't report that either",
                    report.dropped_bytes
                ),
                None => String::new(),
            };
            StoreError::DurabilityLost(format!(
                "fsync of the record log {} at open failed ({error}): records written to it \
                 before this open may not be on the disk, and a later open won't report this \
                 failure again{truncated}",
                path.display()
            ))
        })?;

        Ok(Self {
            file: RwLock::new(file),
            next_id: RwLock::new(next_id),
            file_size: RwLock::new(file_size),
            writes_since_sync: RwLock::new(0),
            sync_interval: if sync_interval == 0 { 1 } else { sync_interval },
            sync_bytes: Self::SYNC_BYTES,
            durability_failure: Mutex::new(None),
            unsynced: Mutex::new(Unsynced {
                synced_len: file_size,
                bytes: Vec::new(),
            }),
            #[cfg(test)]
            fsync_failures: Mutex::new(0),
            #[cfg(test)]
            rewrite_failures: Mutex::new(0),
            #[cfg(test)]
            write_failures: Mutex::new(0),
            #[cfg(test)]
            read_failures: Mutex::new(0),
            branch_max_seq,
            recovery,
        })
    }

    /// Highest sequence observed per branch during the open scan.
    ///
    /// `Store::open` reconciles `BranchManager` heads against this so a stale
    /// `branches.bin` (only written on sync/Drop) can never hand out a sequence
    /// already occupied by a durable record, nor leave a persisted head pointing
    /// past a truncated tail.
    pub fn max_sequences(&self) -> &HashMap<BranchId, Sequence> {
        &self.branch_max_seq
    }

    /// Recovery report if a torn tail was truncated on open, else `None`.
    pub fn recovery(&self) -> Option<&RecoveryReport> {
        self.recovery.as_ref()
    }

    /// Append a record to the log.
    ///
    /// The `state_update` type is reserved for `Store::update_state`. Past
    /// the checkpoint, the next `Store::open` replays a record of that type
    /// that decodes as a state update, refusing the store if its predecessor
    /// isn't its state's head, and skips one that doesn't decode.
    ///
    /// Returns the record and the offset where it was written. Every
    /// `sync_interval`-th append also fsyncs the log. A failed fsync doesn't
    /// fail the append, since the record is already complete in the file:
    /// failing it would leave a record its caller never adopted, which the
    /// next record on the same chain would contradict at replay. From then
    /// on, an append is refused with `StoreError::DurabilityLost` before it
    /// writes or takes a record ID, until [`RecordLog::sync`] writes the
    /// records since the last good fsync again and its fsync succeeds, or
    /// the log is opened again. So the bytes that a sync writes again are at
    /// most the records since the last good fsync.
    pub fn append(
        &self,
        input: RecordInput,
        branch: BranchId,
        sequence: Sequence,
    ) -> Result<(Record, u64)> {
        let mut file = self.file.write();
        self.refuse_while_lost()?;

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

        let offset = *self.file_size.read();
        let (new_size, kept) = {
            let mut unsynced = self.unsynced.lock();
            let start = unsynced.bytes.len();
            let needed = start + Self::encoded_len(&record);
            if needed > unsynced.bytes.capacity() {
                let doubled = (unsynced.bytes.capacity() * 2).min(self.sync_bytes);
                unsynced.bytes.reserve_exact(needed.max(doubled) - start);
            }
            Self::encode_record_into(&record, &mut unsynced.bytes);
            let written = self.write_failure().and_then(|()| {
                file.seek(SeekFrom::Start(offset))?;
                file.write_all(&unsynced.bytes[start..])
            });
            if let Err(error) = written {
                unsynced.bytes.truncate(start);
                return Err(error.into());
            }
            (offset + (unsynced.bytes.len() - start) as u64, unsynced.bytes.len())
        };
        *self.file_size.write() = new_size;

        // Sync periodically based on sync_interval
        let mut writes = self.writes_since_sync.write();
        *writes += 1;
        if *writes >= self.sync_interval || kept > self.sync_bytes {
            match self.fsync(&file) {
                Ok(()) => {
                    *writes = 0;
                    self.synced_to(new_size);
                }
                Err(error) => self.keep_failure(&error),
            }
        }

        Ok((record, offset))
    }

    /// `StoreError::DurabilityLost` while an fsync failure of the log stands,
    /// which [`RecordLog::append`] returns before it writes. A caller with
    /// changes of its own to make before an append checks this first, so a
    /// refused write changes nothing.
    pub(crate) fn refuse_while_lost(&self) -> Result<()> {
        match &*self.durability_failure.lock() {
            Some(failure) => Err(StoreError::DurabilityLost(format!(
                "fsync of the record log failed ({failure}), so no record is written until \
                 sync() writes the records since the last good fsync again and its fsync \
                 succeeds, or the store is reopened"
            ))),
            None => Ok(()),
        }
    }

    fn synced_to(&self, size: u64) {
        let mut unsynced = self.unsynced.lock();
        unsynced.synced_len = size;
        unsynced.bytes = Vec::new();
    }

    fn write_failure(&self) -> std::io::Result<()> {
        #[cfg(test)]
        {
            let mut failures = self.write_failures.lock();
            if *failures > 0 {
                *failures -= 1;
                return Err(std::io::Error::other("injected write failure"));
            }
        }
        Ok(())
    }

    fn fsync_at_open(file: &File) -> std::io::Result<()> {
        #[cfg(test)]
        if OPEN_FSYNC_FAILURES.with(|failures| failures.replace(failures.get().saturating_sub(1)) > 0) {
            return Err(std::io::Error::other("injected fsync failure"));
        }
        file.sync_all()
    }

    /// fsync the log file.
    fn fsync(&self, file: &File) -> std::io::Result<()> {
        #[cfg(test)]
        {
            let mut failures = self.fsync_failures.lock();
            if *failures > 0 {
                *failures -= 1;
                return Err(std::io::Error::other("injected fsync failure"));
            }
        }
        file.sync_all()
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
    ///
    /// Once an fsync of the log has failed, this call's own or a periodic one
    /// that `append` made, the records written since the last good fsync may
    /// not have reached the disk, and a later fsync alone can't show that they
    /// did. So while that failure stands, this first writes those records
    /// again at their offsets, from the bytes kept in memory, and then
    /// fsyncs. When that fsync succeeds, every byte since the last good fsync
    /// has been written and synced again, so the failure ends and this
    /// returns `Ok`. When the write or the fsync fails, the failure stands
    /// and this returns it as `StoreError::DurabilityLost`.
    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.write();
        let standing = self.durability_failure.lock().is_some();
        let synced = if standing {
            self.write_unsynced_again(&mut file)
                .and_then(|()| self.fsync(&file))
        } else {
            self.fsync(&file)
        };
        match synced {
            Ok(()) => {
                *self.writes_since_sync.write() = 0;
                self.synced_to(*self.file_size.read());
                *self.durability_failure.lock() = None;
                Ok(())
            }
            Err(error) => {
                self.keep_failure(&error);
                let first = self.durability_failure.lock().clone().unwrap_or_default();
                let now = error.to_string();
                let failure = if standing {
                    format!("{first}, and this sync's retry failed: {now}")
                } else {
                    first
                };
                Err(StoreError::DurabilityLost(format!(
                    "fsync of the record log failed ({failure}); records written since the \
                     last good fsync may not have reached the disk. Until a sync() writes \
                     them again and its fsync succeeds, or the store is reopened, no record is \
                     written and the store publishes no checkpoint"
                )))
            }
        }
    }

    fn write_unsynced_again(&self, file: &mut File) -> std::io::Result<()> {
        let unsynced = self.unsynced.lock();
        file.seek(SeekFrom::Start(unsynced.synced_len))?;
        #[cfg(test)]
        {
            let mut failures = self.rewrite_failures.lock();
            if *failures > 0 {
                *failures -= 1;
                file.write_all(&unsynced.bytes[..unsynced.bytes.len() / 2])?;
                return Err(std::io::Error::other("injected rewrite failure"));
            }
        }
        file.write_all(&unsynced.bytes)
    }

    /// Keep the first failed fsync of the log, with one warning.
    fn keep_failure(&self, error: &std::io::Error) {
        let mut kept = self.durability_failure.lock();
        if kept.is_none() {
            tracing::warn!(
                %error,
                "record log fsync failed: records written since the last good fsync may not \
                 have reached the disk, so appends and every metadata checkpoint fail until a \
                 sync() writes them again and its fsync succeeds, or the store is reopened"
            );
            *kept = Some(error.to_string());
        }
    }

    /// Read a record at a given offset. A record written since the last good
    /// fsync of the log is read from the bytes kept for a retry, not from
    /// the file.
    pub fn read_at(&self, offset: u64) -> Result<Record> {
        #[cfg(test)]
        {
            let mut failures = self.read_failures.lock();
            if *failures > 0 {
                *failures -= 1;
                return Err(std::io::Error::other("injected read failure").into());
            }
        }
        let mut file = self.file.write();
        self.read_one(&mut file, offset).map(|(record, _)| record)
    }

    /// Read the records at several offsets, in the order given, taking the
    /// file lock once for them all rather than once per record. Each offset
    /// must be a record boundary, as for `read_at`.
    pub fn read_many_at(&self, offsets: &[u64]) -> Result<Vec<Record>> {
        let mut file = self.file.write();
        offsets
            .iter()
            .map(|&offset| self.read_one(&mut file, offset).map(|(record, _)| record))
            .collect()
    }

    fn read_one(&self, file: &mut File, offset: u64) -> Result<(Record, u64)> {
        {
            let unsynced = self.unsynced.lock();
            if let Some(start) = offset.checked_sub(unsynced.synced_len) {
                let kept = usize::try_from(start)
                    .ok()
                    .and_then(|start| unsynced.bytes.get(start..))
                    .unwrap_or_default();
                let mut reader = std::io::Cursor::new(kept);
                let record = Self::read_record_inner(&mut reader, None)?;
                return Ok((record, offset + reader.position()));
            }
        }
        file.seek(SeekFrom::Start(offset))?;
        let record = Self::read_record(file)?;
        Ok((record, file.stream_position()?))
    }

    /// Iterate all records from the beginning.
    pub fn iter(&self) -> RecordIterator<'_> {
        self.iter_from(0)
    }

    /// Iterate all records from a given offset. The records written since
    /// the last good fsync are read as [`RecordLog::read_at`] reads them.
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

    fn encoded_len(record: &Record) -> usize {
        LOG_MAGIC.len()
            + 2
            + 4 * 8
            + 2
            + record.record_type.len()
            + 1
            + 4
            + record.payload.len()
            + 2
            + 8 * record.caused_by.len()
            + 2
            + 8 * record.linked_to.len()
            + 4
    }

    fn encode_record_into(record: &Record, bytes: &mut Vec<u8>) {
        let start = bytes.len();

        // Magic
        bytes.extend_from_slice(LOG_MAGIC);

        // Version
        bytes.push(LOG_VERSION);

        // Flags (reserved)
        bytes.push(0u8);

        // Record ID
        bytes.extend_from_slice(&record.id.0.to_le_bytes());

        // Sequence
        bytes.extend_from_slice(&record.sequence.0.to_le_bytes());

        // Branch
        bytes.extend_from_slice(&record.branch.0.to_le_bytes());

        // Timestamp
        bytes.extend_from_slice(&record.timestamp.0.to_le_bytes());

        // Type
        let type_bytes = record.record_type.as_bytes();
        bytes.extend_from_slice(&(type_bytes.len() as u16).to_le_bytes());
        bytes.extend_from_slice(type_bytes);

        // Encoding
        let encoding_byte = match record.encoding {
            PayloadEncoding::Json => 0u8,
            PayloadEncoding::MessagePack => 1u8,
            PayloadEncoding::Raw => 2u8,
        };
        bytes.push(encoding_byte);

        // Payload
        bytes.extend_from_slice(&(record.payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&record.payload);

        // Caused by
        bytes.extend_from_slice(&(record.caused_by.len() as u16).to_le_bytes());
        for id in &record.caused_by {
            bytes.extend_from_slice(&id.0.to_le_bytes());
        }

        // Linked to
        bytes.extend_from_slice(&(record.linked_to.len() as u16).to_le_bytes());
        for id in &record.linked_to {
            bytes.extend_from_slice(&id.0.to_le_bytes());
        }

        // Checksum of entire record (excluding checksum itself)
        // For simplicity, we'll compute checksum of payload only
        let checksum = crc32fast::hash(&record.payload);
        bytes.extend_from_slice(&checksum.to_le_bytes());

        debug_assert_eq!(bytes.len() - start, Self::encoded_len(record));
    }

    /// Read a record from the file at current position.
    ///
    /// Hot path: trusts the framed length fields. Only call this on offsets
    /// known to be valid record boundaries.
    fn read_record(file: &mut File) -> Result<Record> {
        Self::read_record_inner(file, None)
    }

    /// Like [`Self::read_record`] but rejects any framed length field that
    /// exceeds `remaining` (bytes from the record start to EOF) *before*
    /// allocating. Used on the recovery paths ([`Self::scan_log`],
    /// [`Self::has_valid_record_after`]) where the length fields come from
    /// possibly-torn or garbage bytes: a corrupt `payload_len` of `0xFFFFFFFF`
    /// would otherwise request a 4 GiB allocation and OOM-kill the process
    /// mid-recovery (fatal inside a memory-cgroup'd container).
    fn read_record_bounded(file: &mut File, remaining: u64) -> Result<Record> {
        Self::read_record_inner(file, Some(remaining))
    }

    /// Shared record decoder. When `bound` is `Some(remaining)`, any length
    /// field larger than `remaining` is rejected as `InvalidFormat` before
    /// allocation.
    fn read_record_inner<R: Read>(file: &mut R, bound: Option<u64>) -> Result<Record> {
        #[cfg(test)]
        RECORD_DECODES.with(|decodes| decodes.set(decodes.get() + 1));

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
        if let Some(remaining) = bound {
            if type_len as u64 > remaining {
                return Err(StoreError::InvalidFormat(format!(
                    "record type length {} exceeds {} remaining byte(s)",
                    type_len, remaining
                )));
            }
        }
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
        if let Some(remaining) = bound {
            if payload_len as u64 > remaining {
                return Err(StoreError::InvalidFormat(format!(
                    "record payload length {} exceeds {} remaining byte(s)",
                    payload_len, remaining
                )));
            }
        }
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

    /// Validate the log from the beginning, returning a [`ScanResult`].
    ///
    /// Every record is fully parsed (framing + payload checksum) with the
    /// length-bounded reader so a torn length field can't trigger a huge
    /// allocation. On the first invalid record:
    /// - if the read error is *not* a torn-tail candidate (`UnexpectedEof`,
    ///   `InvalidFormat`, `ChecksumMismatch`), it propagates — a real I/O error
    ///   (e.g. `EIO`) must fail the open, never trigger truncation;
    /// - else if any *valid* record exists after the failure point, this is
    ///   mid-log corruption — unrecoverable without data loss — and an error
    ///   is returned;
    /// - otherwise the invalid bytes are a torn tail record (crash during the
    ///   last append) and the result is returned with `valid_len < file_size`,
    ///   so the caller can truncate.
    fn scan_log(file: &mut File, path: &Path, file_size: u64) -> Result<ScanResult> {
        let mut max_id = 0u64;
        let mut valid_len = 0u64;
        let mut valid_records = 0u64;
        let mut branch_max_seq: HashMap<BranchId, Sequence> = HashMap::new();

        while valid_len < file_size {
            file.seek(SeekFrom::Start(valid_len))?;
            let remaining = file_size - valid_len;
            match Self::read_record_bounded(file, remaining) {
                Ok(record) => {
                    max_id = max_id.max(record.id.0);
                    let entry = branch_max_seq
                        .entry(record.branch)
                        .or_insert(Sequence(0));
                    if record.sequence.0 > entry.0 {
                        *entry = record.sequence;
                    }
                    valid_records += 1;
                    valid_len = file.stream_position()?;
                }
                Err(e) => {
                    // Only framing/checksum/EOF failures are torn-tail
                    // candidates. A genuine I/O error (EIO, network-FS hiccup)
                    // must NOT be absolved into a truncation of valid data.
                    let torn_candidate = matches!(
                        &e,
                        StoreError::InvalidFormat(_) | StoreError::ChecksumMismatch { .. }
                    ) || matches!(&e, StoreError::Io(io) if io.kind() == ErrorKind::UnexpectedEof);

                    if !torn_candidate {
                        return Err(e);
                    }

                    if Self::has_valid_record_after(file, valid_len, file_size, max_id)? {
                        return Err(StoreError::Corruption(format!(
                            "corrupt record at offset {} with valid records after it; \
                             refusing to open (mid-log corruption is not auto-recoverable): {}",
                            valid_len, e
                        )));
                    }
                    // Torn tail: caller truncates to valid_len. Record which
                    // error made us classify the tail as torn.
                    tracing::warn!(
                        path = %path.display(),
                        offset = valid_len,
                        error = %e,
                        "record log tail classified as torn (no valid record follows)"
                    );
                    break;
                }
            }
        }

        Ok(ScanResult {
            max_id,
            valid_len,
            valid_records,
            branch_max_seq,
        })
    }

    /// Check whether any fully valid record starts strictly after `bad_offset`.
    ///
    /// Scans the remaining bytes for the record magic and attempts a
    /// length-bounded parse (including checksum) at each candidate. A candidate
    /// only counts if it also passes a plausibility gate: its record ID must be
    /// greater than every ID seen in the valid prefix (`max_id`), since IDs are
    /// assigned monotonically at append. This keeps a genuine following record
    /// (always id > max_id) recognised while making it far harder for arbitrary
    /// `Raw` payload bytes that happen to embed a valid record image to
    /// masquerade as mid-log corruption and needlessly brick the store.
    ///
    /// Used to distinguish a recoverable torn tail (no valid record follows)
    /// from mid-log corruption (valid records follow the bad one).
    fn has_valid_record_after(
        file: &mut File,
        bad_offset: u64,
        file_size: u64,
        max_id: u64,
    ) -> Result<bool> {
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
                        match Self::read_record_bounded(file, file_size - candidate) {
                            Ok(rec) => {
                                if rec.id.0 > max_id {
                                    return Ok(true);
                                }
                            }
                            Err(e) => {
                                // Only framing/checksum/EOF failures are expected
                                // while probing garbage bytes. A genuine I/O error
                                // (e.g. EIO) must propagate and fail the open — not
                                // be absolved into "no valid record here", which
                                // could flip a mid-log corruption verdict into a
                                // torn-tail truncation of possibly-valid data.
                                let torn_candidate = matches!(
                                    &e,
                                    StoreError::InvalidFormat(_)
                                        | StoreError::ChecksumMismatch { .. }
                                ) || matches!(
                                    &e,
                                    StoreError::Io(io) if io.kind() == ErrorKind::UnexpectedEof
                                );
                                if !torn_candidate {
                                    return Err(e);
                                }
                            }
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

    /// Read the record at the iterator's offset, once: it ends where that
    /// read stopped, which is where the next one starts. The file lock is
    /// held from the seek to that position, since any other read moves the
    /// shared file offset. An error ends the iteration after it is returned.
    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.end {
            return None;
        }

        let current_offset = self.offset;
        let read = {
            let mut file = self.log.file.write();
            self.log.read_one(&mut file, current_offset)
        };
        match read {
            Ok((record, next_offset)) => {
                self.offset = next_offset;
                Some(Ok((current_offset, record)))
            }
            Err(e) => {
                self.offset = self.end;
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

    fn lost<T: std::fmt::Debug>(result: Result<T>) -> String {
        let error = result.unwrap_err();
        assert!(matches!(error, StoreError::DurabilityLost(_)), "{error}");
        let text = error.to_string();
        assert!(text.starts_with("Durability lost:"), "{text}");
        assert!(text.contains("injected fsync failure"), "{text}");
        text
    }

    fn append_byte(log: &RecordLog, n: u64) -> Result<(Record, u64)> {
        log.append(
            RecordInput::raw("test", vec![n as u8]),
            BranchId(1),
            Sequence(n),
        )
    }

    fn zero(path: &Path, from: u64, to: u64) {
        let mut file = OpenOptions::new().write(true).open(path).unwrap();
        file.seek(SeekFrom::Start(from)).unwrap();
        file.write_all(&vec![0u8; (to - from) as usize]).unwrap();
    }

    fn payloads(path: &Path) -> Vec<Vec<u8>> {
        let log = RecordLog::open(path).unwrap();
        assert!(log.recovery().is_none());
        log.iter().map(|each| each.unwrap().1.payload).collect()
    }

    #[test]
    fn a_failed_periodic_fsync_keeps_its_record_and_refuses_appends_until_a_sync_succeeds() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("log.bin");
        {
            let log = RecordLog::open_with_sync_interval(&path, 2).unwrap();
            append_byte(&log, 1).unwrap();
            *log.fsync_failures.lock() = 1;
            let (record, offset) = append_byte(&log, 2).unwrap();
            assert_eq!(log.read_at(offset).unwrap().id, record.id);

            let size = std::fs::metadata(&path).unwrap().len();
            let text = lost(append_byte(&log, 3));
            assert!(text.contains("no record is written until"), "{text}");
            assert_eq!(log.size(), size);
            assert_eq!(std::fs::metadata(&path).unwrap().len(), size);

            log.sync().unwrap();
            assert_eq!(append_byte(&log, 3).unwrap().0.id, RecordId(3));
            log.sync().unwrap();
        }
        assert_eq!(payloads(&path), vec![vec![1], vec![2], vec![3]]);
    }

    #[test]
    fn a_failed_retry_keeps_every_kept_byte_for_the_next_one() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("log.bin");
        {
            let log = RecordLog::open(&path).unwrap();
            append_byte(&log, 1).unwrap();
            log.sync().unwrap();
            let synced = log.size();
            append_byte(&log, 2).unwrap();
            append_byte(&log, 3).unwrap();

            *log.fsync_failures.lock() = 2;
            let text = lost(log.sync());
            assert!(text.contains("Until a sync() writes them again"), "{text}");
            lost(log.sync());
            *log.rewrite_failures.lock() = 1;
            let text = lost(log.sync());
            assert!(text.contains("this sync's retry failed: injected rewrite failure"), "{text}");
            lost(append_byte(&log, 4));

            zero(&path, synced, log.size());
            log.sync().unwrap();
            append_byte(&log, 4).unwrap();
            log.sync().unwrap();
        }
        assert_eq!(payloads(&path), vec![vec![1], vec![2], vec![3], vec![4]]);
    }

    #[test]
    fn a_retry_writes_the_lost_records_again_from_memory() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("log.bin");
        let payload = |i: u64| format!("record {i}").into_bytes();
        {
            let log = RecordLog::open_with_sync_interval(&path, 1000).unwrap();
            let append = |i: u64| {
                log.append(RecordInput::raw("test", payload(i)), BranchId(1), Sequence(i))
                    .unwrap()
            };
            (1..=100u64).for_each(|i| {
                append(i);
            });
            log.sync().unwrap();
            let synced = log.size();
            (101..=200u64).for_each(|i| {
                append(i);
            });
            *log.fsync_failures.lock() = 1;
            lost(log.sync());

            zero(&path, synced, log.size());
            log.sync().unwrap();
        }
        assert_eq!(payloads(&path), (1..=200u64).map(payload).collect::<Vec<_>>());
    }

    #[test]
    fn a_read_past_the_last_good_fsync_decodes_the_kept_bytes() {
        for failed_sync in [false, true] {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("log.bin");
            let payload = |i: u64| format!("record {i}").into_bytes();
            let log = RecordLog::open_with_sync_interval(&path, 1000).unwrap();
            let append = |i: u64| {
                log.append(RecordInput::raw("test", payload(i)), BranchId(1), Sequence(i))
                    .unwrap()
            };
            let mut written: Vec<(Record, u64)> = (1..=3u64).map(append).collect();
            log.sync().unwrap();
            let synced = log.size();
            written.extend((4..=6u64).map(append));
            if failed_sync {
                *log.fsync_failures.lock() = 1;
                lost(log.sync());
            }
            zero(&path, synced, log.size());

            let expected: Vec<Vec<u8>> = (1..=6u64).map(payload).collect();
            let one: Vec<Vec<u8>> = written
                .iter()
                .map(|(record, offset)| {
                    let read = log.read_at(*offset).unwrap();
                    assert_eq!(read.id, record.id, "failed sync: {failed_sync}");
                    read.payload
                })
                .collect();
            assert_eq!(one, expected, "read_at, failed sync: {failed_sync}");
            let offsets: Vec<u64> = written.iter().map(|(_, offset)| *offset).collect();
            let many: Vec<Vec<u8>> = log
                .read_many_at(&offsets)
                .unwrap()
                .into_iter()
                .map(|record| record.payload)
                .collect();
            assert_eq!(many, expected, "read_many_at, failed sync: {failed_sync}");
            let iterated: Vec<Vec<u8>> = log.iter().map(|each| each.unwrap().1.payload).collect();
            assert_eq!(iterated, expected, "iter, failed sync: {failed_sync}");
        }
    }

    #[test]
    fn the_bytes_kept_for_a_retry_are_the_records_since_the_last_good_fsync() {
        let dir = TempDir::new().unwrap();
        let log = RecordLog::open_with_sync_interval(dir.path().join("log.bin"), 3).unwrap();
        let kept = || {
            let unsynced = log.unsynced.lock();
            (unsynced.synced_len, unsynced.bytes.len() as u64)
        };
        let released = || log.unsynced.lock().bytes.capacity() == 0;
        let append = |n: u64| append_byte(&log, n).unwrap().1;
        append(1);
        append(2);
        assert_eq!(kept(), (0, log.size()));
        append(3);
        assert_eq!(kept(), (log.size(), 0), "the interval's fsync");
        assert!(released(), "the interval's fsync");
        let fourth = append(4);
        assert_eq!(kept(), (fourth, log.size() - fourth));
        log.sync().unwrap();
        assert_eq!(kept(), (log.size(), 0), "sync()'s fsync");
        assert!(released(), "sync()'s fsync");
    }

    #[test]
    fn an_append_that_passes_the_byte_budget_fsyncs_at_once() {
        let dir = TempDir::new().unwrap();
        let mut log = RecordLog::open_with_sync_interval(dir.path().join("log.bin"), 1000).unwrap();
        log.sync_bytes = 1000;
        let kept = || log.unsynced.lock().bytes.len();
        let capacity = || log.unsynced.lock().bytes.capacity();
        let append = |n: u64| -> (usize, usize) {
            let before = kept();
            let (_, offset) = log
                .append(RecordInput::raw("test", vec![n as u8; 300]), BranchId(1), Sequence(n))
                .unwrap();
            (before, (log.size() - offset) as usize)
        };
        let mut syncs = 0;
        for n in 1..=10u64 {
            let (before, len) = append(n);
            assert!(capacity() <= 1000.max(before + len), "{} after append {n}", capacity());
            if before + len > 1000 {
                assert_eq!(kept(), 0, "append {n} passes the budget and syncs");
                syncs += 1;
            } else {
                assert_eq!(kept(), before + len, "append {n}");
            }
        }
        assert!(syncs >= 2, "{syncs}");

        *log.fsync_failures.lock() = 1;
        let mut n = 11;
        loop {
            let (before, len) = append(n);
            n += 1;
            assert!(capacity() <= 1000.max(before + len), "{} after append {n}", capacity());
            if before + len > 1000 {
                break;
            }
        }
        let text = lost(append_byte(&log, n));
        assert!(text.contains("no record is written until"), "{text}");
        log.sync().unwrap();
        append_byte(&log, n).unwrap();
    }

    #[test]
    fn an_append_whose_write_fails_keeps_no_bytes_of_it() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("log.bin");
        {
            let log = RecordLog::open(&path).unwrap();
            append_byte(&log, 1).unwrap();
            let kept = log.unsynced.lock().bytes.clone();
            *log.write_failures.lock() = 1;
            assert!(append_byte(&log, 2).is_err());
            assert_eq!(log.unsynced.lock().bytes, kept);
            append_byte(&log, 2).unwrap();
            *log.fsync_failures.lock() = 1;
            lost(log.sync());
            zero(&path, 0, log.size());
            log.sync().unwrap();
        }
        assert_eq!(payloads(&path), vec![vec![1], vec![2]]);
    }

    #[test]
    fn an_open_whose_fsync_fails_refuses_the_log() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("log.bin");
        {
            let log = RecordLog::open(&path).unwrap();
            append_byte(&log, 1).unwrap();
        }
        OPEN_FSYNC_FAILURES.with(|failures| failures.set(1));
        let error = RecordLog::open(&path).err().expect("the open refuses");
        let text = error.to_string();
        assert!(matches!(error, StoreError::DurabilityLost(_)), "{text}");
        assert!(text.contains(&path.display().to_string()), "{text}");
        assert!(text.contains("at open failed (injected fsync failure)"), "{text}");
        assert!(text.contains("a later open won't report this failure again"), "{text}");
        assert!(!text.contains("torn tail"), "{text}");
        assert_eq!(payloads(&path), vec![vec![1]]);

        let size = std::fs::metadata(&path).unwrap().len();
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"REC\0torn").unwrap();
        drop(file);
        OPEN_FSYNC_FAILURES.with(|failures| failures.set(1));
        let text = RecordLog::open(&path).err().expect("the open refuses").to_string();
        assert!(text.contains("truncated a torn tail of 8 bytes"), "{text}");
        assert_eq!(std::fs::metadata(&path).unwrap().len(), size);
    }

    // What a failed write-back can leave once the page cache no longer holds
    // the pages: a page of the log that never reached the disk, with records
    // after it that did. Open refuses the log rather than drop those records.
    #[test]
    fn a_lost_page_with_records_after_it_fails_open() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("log.bin");
        {
            let log = RecordLog::open(&path).unwrap();
            for i in 1..=200u64 {
                let input = RecordInput::raw("test", format!("record {i}").into_bytes());
                log.append(input, BranchId(1), Sequence(i)).unwrap();
            }
        }
        let len = std::fs::metadata(&path).unwrap().len();
        assert!(len > 3 * 4096, "{len}");
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(4096)).unwrap();
        file.write_all(&[0u8; 4096]).unwrap();
        drop(file);

        let err = match RecordLog::open(&path) {
            Ok(_) => panic!("expected open to fail on a lost page with records after it"),
            Err(e) => e,
        };
        assert!(matches!(err, StoreError::Corruption(_)), "{err:?}");
        assert_eq!(std::fs::metadata(&path).unwrap().len(), len);
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

    // The iterator reads each record once and finds the next one where that
    // read stopped. It used to read each record a second time to find its
    // end, which doubled the reads of the index rebuild at every open.
    #[test]
    fn iteration_reads_each_record_once_and_finds_every_boundary() {
        let dir = TempDir::new().unwrap();
        let log = RecordLog::open(dir.path().join("log.bin")).unwrap();
        let mut written = Vec::new();
        for i in 1..=12u64 {
            // Records of different lengths, with and without links, so a
            // boundary found from a wrong length would land mid-record.
            let mut input = RecordInput::raw(
                if i % 2 == 0 { "even" } else { "odd-type" },
                vec![i as u8; (i * 37 % 200) as usize],
            );
            if i > 1 {
                input.caused_by = vec![RecordId(i - 1)];
            }
            if i % 3 == 0 {
                input.linked_to = vec![RecordId(1), RecordId(2)];
            }
            written.push(log.append(input, BranchId(1), Sequence(i)).unwrap());
        }

        RECORD_DECODES.with(|decodes| decodes.set(0));
        let read: Vec<(u64, Record)> = log.iter().map(|r| r.unwrap()).collect();
        assert_eq!(RECORD_DECODES.with(|decodes| decodes.get()), 12);
        assert_eq!(read.len(), written.len());
        for ((offset, record), (expected, expected_offset)) in read.iter().zip(&written) {
            assert_eq!(offset, expected_offset);
            assert_eq!(record.id, expected.id);
            assert_eq!(record.payload, expected.payload);
            assert_eq!(record.caused_by, expected.caused_by);
            assert_eq!(record.linked_to, expected.linked_to);
        }

        // From a record in the middle, the same records from there on.
        let (_, middle) = written[5];
        RECORD_DECODES.with(|decodes| decodes.set(0));
        let ids: Vec<u64> = log.iter_from(middle).map(|r| r.unwrap().1.id.0).collect();
        assert_eq!(RECORD_DECODES.with(|decodes| decodes.get()), 7);
        assert_eq!(ids, (6..=12).collect::<Vec<_>>());
    }

    // Each record's read holds the file from its seek to the position where
    // the read stopped, so reads between steps, which move the shared file
    // offset, don't move the iterator.
    #[test]
    fn reads_between_iteration_steps_do_not_move_the_iterator() {
        let dir = TempDir::new().unwrap();
        let log = RecordLog::open(dir.path().join("log.bin")).unwrap();
        let mut offsets = Vec::new();
        for i in 1..=6u64 {
            let input = RecordInput::raw("test", vec![i as u8; 10 * i as usize]);
            offsets.push(log.append(input, BranchId(1), Sequence(i)).unwrap().1);
        }
        let mut ids = Vec::new();
        for result in log.iter() {
            ids.push(result.unwrap().1.id.0);
            log.read_at(offsets[0]).unwrap();
            log.read_many_at(&[offsets[5], offsets[2]]).unwrap();
        }
        assert_eq!(ids, vec![1, 2, 3, 4, 5, 6]);
    }

    // A record that no longer reads ends the iteration: the error is
    // returned once, and nothing after it.
    #[test]
    fn a_record_that_fails_to_read_ends_the_iteration() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("log.bin");
        let log = RecordLog::open(&path).unwrap();
        let mut offsets = Vec::new();
        for i in 1..=4u64 {
            let input = RecordInput::raw("test", format!("record {i}").into_bytes());
            offsets.push(log.append(input, BranchId(1), Sequence(i)).unwrap().1);
        }
        // Damage the third record's payload under the open log, after a sync,
        // since the log reads records past its last good fsync from memory.
        log.sync().unwrap();
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(offsets[2] + payload_offset(4)))
            .unwrap();
        file.write_all(b"XX").unwrap();
        drop(file);

        let results: Vec<_> = log.iter().collect();
        assert_eq!(results.len(), 3, "{results:?}");
        assert_eq!(results[0].as_ref().unwrap().1.id.0, 1);
        assert_eq!(results[1].as_ref().unwrap().1.id.0, 2);
        assert!(
            matches!(results[2], Err(StoreError::ChecksumMismatch { .. })),
            "{:?}",
            results[2]
        );
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
