//! Record indices for efficient lookups.
//!
//! Indices are rebuilt from the record log on startup rather than persisted.
//! This gives O(1) sync time at the cost of O(N) startup time, which is
//! acceptable for stores up to millions of records (rebuilds in seconds).

use crate::error::Result;
use crate::records::RecordLog;
use crate::types::{BranchId, RecordId, Sequence};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;

/// Index mapping sequence numbers to file offsets.
pub struct RecordIndex {
    /// In-memory index: (branch, sequence) -> offset.
    /// Uses BTreeMap for O(log n) range queries and ordered iteration.
    entries: RwLock<BTreeMap<(BranchId, Sequence), u64>>,

    /// Record ID to offset.
    id_to_offset: RwLock<HashMap<RecordId, u64>>,

    /// Record type to record IDs.
    type_index: RwLock<HashMap<String, Vec<RecordId>>>,

    /// caused_by index: record_id -> records that have it in caused_by.
    caused_by_index: RwLock<HashMap<RecordId, Vec<RecordId>>>,

    /// linked_to index: record_id -> records that have it in linked_to.
    linked_to_index: RwLock<HashMap<RecordId, Vec<RecordId>>>,
}

impl RecordIndex {
    /// Create a new empty index.
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        let _path = path;
        Ok(Self {
            entries: RwLock::new(BTreeMap::new()),
            id_to_offset: RwLock::new(HashMap::new()),
            type_index: RwLock::new(HashMap::new()),
            caused_by_index: RwLock::new(HashMap::new()),
            linked_to_index: RwLock::new(HashMap::new()),
        })
    }

    /// Create index by rebuilding from the record log.
    ///
    /// This scans the entire log sequentially and builds all indexes.
    /// For a store with 1M records, this typically takes 1-3 seconds on SSD.
    pub fn rebuild_from_log(path: impl AsRef<Path>, log: &RecordLog) -> Result<Self> {
        let index = Self::new(path)?;

        // Iterate through all records in the log
        for result in log.iter() {
            let (offset, record) = result?;

            // Add to all indexes
            index.add(
                record.id,
                record.branch,
                record.sequence,
                offset,
                &record.record_type,
                &record.caused_by,
                &record.linked_to,
            );
        }

        Ok(index)
    }

    /// Load index - for backwards compatibility, just creates empty index.
    /// Use `rebuild_from_log` after opening the log to populate it.
    #[deprecated(note = "Use rebuild_from_log instead")]
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        Self::new(path)
    }

    /// Add an entry to the index.
    pub fn add(
        &self,
        id: RecordId,
        branch: BranchId,
        sequence: Sequence,
        offset: u64,
        record_type: &str,
        caused_by: &[RecordId],
        linked_to: &[RecordId],
    ) {
        self.entries.write().insert((branch, sequence), offset);
        self.id_to_offset.write().insert(id, offset);

        self.type_index
            .write()
            .entry(record_type.to_string())
            .or_default()
            .push(id);

        for &cause in caused_by {
            self.caused_by_index
                .write()
                .entry(cause)
                .or_default()
                .push(id);
        }

        for &link in linked_to {
            self.linked_to_index
                .write()
                .entry(link)
                .or_default()
                .push(id);
        }
    }

    /// Get offset for a sequence on a branch.
    pub fn get_offset(&self, branch: BranchId, sequence: Sequence) -> Option<u64> {
        self.entries.read().get(&(branch, sequence)).copied()
    }

    /// Get offset for a record ID.
    pub fn get_offset_by_id(&self, id: RecordId) -> Option<u64> {
        self.id_to_offset.read().get(&id).copied()
    }

    /// Get all record IDs of a given type.
    pub fn get_by_type(&self, record_type: &str) -> Vec<RecordId> {
        self.type_index
            .read()
            .get(record_type)
            .cloned()
            .unwrap_or_default()
    }

    /// Get records that have `id` in their caused_by.
    pub fn get_caused_by(&self, id: RecordId) -> Vec<RecordId> {
        self.caused_by_index
            .read()
            .get(&id)
            .cloned()
            .unwrap_or_default()
    }

    /// Get records that have `id` in their linked_to.
    pub fn get_linked_to(&self, id: RecordId) -> Vec<RecordId> {
        self.linked_to_index
            .read()
            .get(&id)
            .cloned()
            .unwrap_or_default()
    }

    /// Rebuild causation indexes for a record (used when reopening store).
    pub fn rebuild_causation_for(&self, id: RecordId, caused_by: &[RecordId], linked_to: &[RecordId]) {
        for &cause in caused_by {
            self.caused_by_index
                .write()
                .entry(cause)
                .or_default()
                .push(id);
        }

        for &link in linked_to {
            self.linked_to_index
                .write()
                .entry(link)
                .or_default()
                .push(id);
        }
    }

    /// Get the highest sequence for a branch.
    pub fn max_sequence(&self, branch: BranchId) -> Option<Sequence> {
        let entries = self.entries.read();
        // With BTreeMap, we can efficiently find the max by ranging to the end
        let end_key = (branch, Sequence(u64::MAX));
        entries
            .range(..=end_key)
            .rev()
            .find(|((b, _), _)| *b == branch)
            .map(|((_, s), _)| *s)
    }

    /// Query records in a sequence range with efficient O(log n + k) lookup.
    ///
    /// Returns offsets for records on `branch` with sequences in [from, to].
    /// If `reverse` is true, returns offsets in descending sequence order.
    /// `limit` caps the number of results.
    pub fn query_range(
        &self,
        branch: BranchId,
        from: Option<Sequence>,
        to: Option<Sequence>,
        limit: usize,
        reverse: bool,
    ) -> Vec<(Sequence, u64)> {
        let entries = self.entries.read();

        let start = (branch, from.unwrap_or(Sequence(0)));
        let end = (branch, to.unwrap_or(Sequence(u64::MAX)));

        if reverse {
            // Iterate backwards from `end` to `start`
            entries
                .range(start..=end)
                .rev()
                .filter(|((b, _), _)| *b == branch)
                .take(limit)
                .map(|((_, seq), offset)| (*seq, *offset))
                .collect()
        } else {
            // Iterate forwards from `start` to `end`
            entries
                .range(start..=end)
                .filter(|((b, _), _)| *b == branch)
                .take(limit)
                .map(|((_, seq), offset)| (*seq, *offset))
                .collect()
        }
    }

    /// Get count of records.
    pub fn count(&self) -> usize {
        self.id_to_offset.read().len()
    }

    /// Save index to file - NO-OP.
    ///
    /// Index is no longer persisted; it's rebuilt from the log on startup.
    /// This method exists for API compatibility but does nothing.
    #[deprecated(note = "Index is no longer persisted, rebuilt from log on startup")]
    pub fn save(&self) -> Result<()> {
        // No-op: index is rebuilt from log on startup
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::RecordInput;
    use tempfile::TempDir;

    #[test]
    fn test_add_and_lookup() {
        let dir = TempDir::new().unwrap();
        let index = RecordIndex::new(dir.path().join("index.bin")).unwrap();

        let id = RecordId(1);
        let branch = BranchId(1);
        let seq = Sequence(1);

        index.add(id, branch, seq, 0, "test", &[], &[]);

        assert_eq!(index.get_offset(branch, seq), Some(0));
        assert_eq!(index.get_offset_by_id(id), Some(0));
    }

    #[test]
    fn test_type_index() {
        let dir = TempDir::new().unwrap();
        let index = RecordIndex::new(dir.path().join("index.bin")).unwrap();

        let branch = BranchId(1);

        index.add(RecordId(1), branch, Sequence(1), 0, "message", &[], &[]);
        index.add(RecordId(2), branch, Sequence(2), 100, "message", &[], &[]);
        index.add(RecordId(3), branch, Sequence(3), 200, "code", &[], &[]);

        let messages = index.get_by_type("message");
        assert_eq!(messages.len(), 2);

        let code = index.get_by_type("code");
        assert_eq!(code.len(), 1);
    }

    #[test]
    fn test_caused_by_index() {
        let dir = TempDir::new().unwrap();
        let index = RecordIndex::new(dir.path().join("index.bin")).unwrap();

        let branch = BranchId(1);
        let cause = RecordId(1);

        index.add(cause, branch, Sequence(1), 0, "cause", &[], &[]);
        index.add(
            RecordId(2),
            branch,
            Sequence(2),
            100,
            "effect",
            &[cause],
            &[],
        );
        index.add(
            RecordId(3),
            branch,
            Sequence(3),
            200,
            "effect",
            &[cause],
            &[],
        );

        let effects = index.get_caused_by(cause);
        assert_eq!(effects.len(), 2);
    }

    #[test]
    fn test_rebuild_from_log() {
        let dir = TempDir::new().unwrap();
        let log_path = dir.path().join("records.log");
        let index_path = dir.path().join("index.bin");

        // Create log with some records
        let log = RecordLog::open(&log_path).unwrap();
        let branch = BranchId(1);

        log.append(
            RecordInput::raw("message", b"hello".to_vec()),
            branch,
            Sequence(1),
        )
        .unwrap();
        log.append(
            RecordInput::raw("message", b"world".to_vec()),
            branch,
            Sequence(2),
        )
        .unwrap();
        log.append(
            RecordInput::raw("code", b"fn main() {}".to_vec()),
            branch,
            Sequence(3),
        )
        .unwrap();

        // Rebuild index from log
        let index = RecordIndex::rebuild_from_log(&index_path, &log).unwrap();

        // Verify all records are indexed
        assert_eq!(index.count(), 3);
        assert_eq!(index.get_by_type("message").len(), 2);
        assert_eq!(index.get_by_type("code").len(), 1);

        // Verify sequence lookups work
        assert!(index.get_offset(branch, Sequence(1)).is_some());
        assert!(index.get_offset(branch, Sequence(2)).is_some());
        assert!(index.get_offset(branch, Sequence(3)).is_some());
    }

    #[test]
    fn test_rebuild_from_empty_log() {
        let dir = TempDir::new().unwrap();
        let log_path = dir.path().join("records.log");
        let index_path = dir.path().join("index.bin");

        let log = RecordLog::open(&log_path).unwrap();
        let index = RecordIndex::rebuild_from_log(&index_path, &log).unwrap();

        assert_eq!(index.count(), 0);
    }

    #[test]
    fn test_query_range_forward() {
        let dir = TempDir::new().unwrap();
        let index = RecordIndex::new(dir.path().join("index.bin")).unwrap();
        let branch = BranchId(1);

        // Add records with sequences 1-10
        for i in 1..=10u64 {
            index.add(RecordId(i), branch, Sequence(i), i * 100, "test", &[], &[]);
        }

        // Query range [3, 7] forward
        let results = index.query_range(branch, Some(Sequence(3)), Some(Sequence(7)), 100, false);
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].0, Sequence(3)); // First result is seq 3
        assert_eq!(results[4].0, Sequence(7)); // Last result is seq 7
    }

    #[test]
    fn test_query_range_reverse() {
        let dir = TempDir::new().unwrap();
        let index = RecordIndex::new(dir.path().join("index.bin")).unwrap();
        let branch = BranchId(1);

        // Add records with sequences 1-10
        for i in 1..=10u64 {
            index.add(RecordId(i), branch, Sequence(i), i * 100, "test", &[], &[]);
        }

        // Query range [3, 7] reverse
        let results = index.query_range(branch, Some(Sequence(3)), Some(Sequence(7)), 100, true);
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].0, Sequence(7)); // First result is seq 7 (highest)
        assert_eq!(results[4].0, Sequence(3)); // Last result is seq 3 (lowest)
    }

    #[test]
    fn test_query_range_with_limit() {
        let dir = TempDir::new().unwrap();
        let index = RecordIndex::new(dir.path().join("index.bin")).unwrap();
        let branch = BranchId(1);

        // Add records with sequences 1-100
        for i in 1..=100u64 {
            index.add(RecordId(i), branch, Sequence(i), i * 100, "test", &[], &[]);
        }

        // Query all with limit 10, forward
        let results = index.query_range(branch, None, None, 10, false);
        assert_eq!(results.len(), 10);
        assert_eq!(results[0].0, Sequence(1)); // Starts at seq 1

        // Query all with limit 10, reverse
        let results = index.query_range(branch, None, None, 10, true);
        assert_eq!(results.len(), 10);
        assert_eq!(results[0].0, Sequence(100)); // Starts at seq 100 (newest)
    }

    #[test]
    fn test_query_range_open_ended() {
        let dir = TempDir::new().unwrap();
        let index = RecordIndex::new(dir.path().join("index.bin")).unwrap();
        let branch = BranchId(1);

        // Add records with sequences 1-10
        for i in 1..=10u64 {
            index.add(RecordId(i), branch, Sequence(i), i * 100, "test", &[], &[]);
        }

        // Query from seq 5 to end
        let results = index.query_range(branch, Some(Sequence(5)), None, 100, false);
        assert_eq!(results.len(), 6); // 5,6,7,8,9,10
        assert_eq!(results[0].0, Sequence(5));

        // Query from beginning to seq 5
        let results = index.query_range(branch, None, Some(Sequence(5)), 100, false);
        assert_eq!(results.len(), 5); // 1,2,3,4,5
        assert_eq!(results[4].0, Sequence(5));
    }
}
