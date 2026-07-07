//! Branch manager implementation.

use crate::error::{Result, StoreError};
use crate::types::{Branch, BranchId, Sequence, Timestamp};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A single branch head that was corrected during open-time reconciliation.
#[derive(Clone, Debug)]
pub struct HeadReconciliation {
    /// Branch that was adjusted.
    pub name: String,
    /// Branch id.
    pub id: BranchId,
    /// Head recorded in `branches.bin`.
    pub old_head: Sequence,
    /// Head the durable log implies.
    pub new_head: Sequence,
}

/// Outcome of reconciling persisted branch metadata against the durable log on
/// open. Returned by [`BranchManager::reconcile_heads`].
#[derive(Clone, Debug, Default)]
pub struct ReconciliationReport {
    /// Per-branch head corrections (empty when every head already agreed).
    pub head_fixes: Vec<HeadReconciliation>,
    /// BranchIds the durable log holds records for but that are absent from
    /// `branches.bin` — orphaned records left by a branch that was created and
    /// written to, then lost before its metadata was flushed. Their records
    /// remain in the log but the branch is unreachable; `next_id` is clamped
    /// above them so a future `create_branch` can never re-issue the id and
    /// adopt them as ghosts.
    pub orphaned_branch_ids: Vec<BranchId>,
}
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

/// Magic bytes for branch index file.
const BRANCH_INDEX_MAGIC: &[u8; 4] = b"BRI\0";

/// Current branch index format version.
const BRANCH_INDEX_VERSION: u8 = 1;

/// Name of the main branch.
pub const MAIN_BRANCH: &str = "main";

/// Options for branch garbage collection.
#[derive(Clone, Debug, Default)]
pub struct BranchGcOptions {
    /// Delete orphaned branches (branches whose parent was deleted).
    pub delete_orphaned: bool,
    /// Delete empty branches (branches with no commits).
    pub delete_empty: bool,
    /// Delete stale branches older than this Unix timestamp.
    pub delete_stale_older_than: Option<u64>,
    /// Only delete branches matching these patterns.
    pub name_patterns: Option<Vec<String>>,
    /// Force deletion even if branch has children.
    pub force: bool,
    /// Re-parent children to this branch when deleting.
    pub reparent_to: Option<String>,
}

/// Result of branch garbage collection.
#[derive(Clone, Debug, Default)]
pub struct BranchGcResult {
    /// Branches that were deleted.
    pub deleted: Vec<String>,
    /// Branches that were skipped (couldn't be deleted).
    pub skipped: Vec<String>,
    /// Number of branches that were re-parented.
    pub reparented: usize,
    /// Errors encountered during GC.
    pub errors: Vec<(String, String)>,
}

/// Branch index stored on disk.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct BranchIndex {
    /// All branches by ID.
    branches: HashMap<BranchId, Branch>,

    /// Branch name to ID mapping.
    name_to_id: HashMap<String, BranchId>,

    /// Next branch ID to assign.
    next_id: u64,
}

/// Manages branches with copy-on-write semantics.
pub struct BranchManager {
    /// Path to branch index file.
    path: PathBuf,

    /// In-memory index.
    index: RwLock<BranchIndex>,

    /// Current active branch.
    current: RwLock<BranchId>,
}

impl BranchManager {
    /// Create a new branch manager with a main branch.
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        let mut index = BranchIndex::default();

        // Create the main branch
        let main_id = BranchId(1);
        let main_branch = Branch {
            id: main_id,
            name: MAIN_BRANCH.to_string(),
            head: Sequence(0),
            parent: None,
            branch_point: None,
            created: Timestamp::now(),
        };

        index.branches.insert(main_id, main_branch);
        index.name_to_id.insert(MAIN_BRANCH.to_string(), main_id);
        index.next_id = 2;

        Ok(Self {
            path,
            index: RwLock::new(index),
            current: RwLock::new(main_id),
        })
    }

    /// Load branch manager from file.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        let manager = Self {
            path: path.clone(),
            index: RwLock::new(BranchIndex::default()),
            current: RwLock::new(BranchId(1)),
        };

        if path.exists() {
            manager.load_from_file()?;
        } else {
            // Initialize with main branch
            let mut index = BranchIndex::default();
            let main_id = BranchId(1);
            let main_branch = Branch {
                id: main_id,
                name: MAIN_BRANCH.to_string(),
                head: Sequence(0),
                parent: None,
                branch_point: None,
                created: Timestamp::now(),
            };

            index.branches.insert(main_id, main_branch);
            index.name_to_id.insert(MAIN_BRANCH.to_string(), main_id);
            index.next_id = 2;

            *manager.index.write() = index;
        }

        Ok(manager)
    }

    /// Create a new branch from the current head of another branch.
    pub fn create_branch(&self, name: &str, from: Option<&str>) -> Result<Branch> {
        let mut index = self.index.write();

        // Check if name already exists
        if index.name_to_id.contains_key(name) {
            return Err(StoreError::BranchExists(name.to_string()));
        }

        // Get parent branch
        let parent_name = from.unwrap_or(MAIN_BRANCH);
        let parent_id = *index
            .name_to_id
            .get(parent_name)
            .ok_or_else(|| StoreError::BranchNotFound(parent_name.to_string()))?;

        let parent = index
            .branches
            .get(&parent_id)
            .ok_or_else(|| StoreError::BranchNotFound(parent_name.to_string()))?
            .clone();

        // Create new branch
        let branch_id = BranchId(index.next_id);
        index.next_id += 1;

        let branch = Branch {
            id: branch_id,
            name: name.to_string(),
            head: parent.head,
            parent: Some(parent_id),
            branch_point: Some(parent.head),
            created: Timestamp::now(),
        };

        index.branches.insert(branch_id, branch.clone());
        index.name_to_id.insert(name.to_string(), branch_id);

        Ok(branch)
    }

    /// Create a branch from a specific sequence number.
    pub fn create_branch_at(&self, name: &str, from: &str, at: Sequence) -> Result<Branch> {
        let mut index = self.index.write();

        // Check if name already exists
        if index.name_to_id.contains_key(name) {
            return Err(StoreError::BranchExists(name.to_string()));
        }

        // Get parent branch
        let parent_id = *index
            .name_to_id
            .get(from)
            .ok_or_else(|| StoreError::BranchNotFound(from.to_string()))?;

        let parent = index
            .branches
            .get(&parent_id)
            .ok_or_else(|| StoreError::BranchNotFound(from.to_string()))?;

        // Validate sequence
        if at > parent.head {
            return Err(StoreError::InvalidSequence(at, parent.head));
        }

        // Create new branch
        let branch_id = BranchId(index.next_id);
        index.next_id += 1;

        let branch = Branch {
            id: branch_id,
            name: name.to_string(),
            head: at,
            parent: Some(parent_id),
            branch_point: Some(at),
            created: Timestamp::now(),
        };

        index.branches.insert(branch_id, branch.clone());
        index.name_to_id.insert(name.to_string(), branch_id);

        Ok(branch)
    }

    /// Get a branch by name.
    pub fn get_branch(&self, name: &str) -> Option<Branch> {
        let index = self.index.read();
        let id = index.name_to_id.get(name)?;
        index.branches.get(id).cloned()
    }

    /// Get a branch by ID.
    pub fn get_branch_by_id(&self, id: BranchId) -> Option<Branch> {
        self.index.read().branches.get(&id).cloned()
    }

    /// Get the current branch.
    pub fn current_branch(&self) -> Branch {
        let current_id = *self.current.read();
        self.index
            .read()
            .branches
            .get(&current_id)
            .cloned()
            .expect("Current branch should always exist")
    }

    /// Switch to a different branch.
    pub fn switch_branch(&self, name: &str) -> Result<Branch> {
        let index = self.index.read();
        let id = *index
            .name_to_id
            .get(name)
            .ok_or_else(|| StoreError::BranchNotFound(name.to_string()))?;

        let branch = index
            .branches
            .get(&id)
            .ok_or_else(|| StoreError::BranchNotFound(name.to_string()))?
            .clone();

        drop(index);

        *self.current.write() = id;
        Ok(branch)
    }

    /// Reconcile persisted branch heads against the durable record log.
    ///
    /// `branches.bin` is only written on `sync()`/`Drop`, so after an unclean
    /// shutdown it can disagree with the log in *both* directions:
    /// - **stale-low**: the log holds records newer than the persisted head
    ///   (crash after appends but before the metadata flush). Trusting the
    ///   stale head would make the next append reuse a sequence already
    ///   occupied by a durable record — a silent duplicate sequence.
    /// - **too-high**: a torn tail was truncated on log open, leaving the head
    ///   pointing past the last surviving record — a permanent hole.
    ///
    /// For each branch we clamp the head to `max(branch_point, log_max)` where
    /// `log_max` is the highest sequence the log actually holds for that branch
    /// (from [`RecordLog::max_sequences`]). The `branch_point` floor preserves
    /// heads inherited from a parent for branches that have no records of their
    /// own yet.
    ///
    /// The same stale-`branches.bin` mechanism also threatens branch *identity*:
    /// `create_branch` bumps `next_id` only in memory (persisted alongside the
    /// branches on sync/Drop), so a crash after creating a branch and appending
    /// to it — but before the metadata flush — leaves the log holding records
    /// for a BranchId that a rolled-back `next_id` would happily re-issue. The
    /// new branch would then adopt those durable records as ghosts and reissue
    /// their sequences. We therefore also clamp `next_id` above every BranchId
    /// the log actually holds, and report any such BranchId that is missing from
    /// `branches.bin` (orphaned records) so the caller can log it.
    ///
    /// Returns a [`ReconciliationReport`]: the head corrections made (empty when
    /// consistent) plus any orphaned BranchIds discovered.
    pub fn reconcile_heads(
        &self,
        log_max_seq: &HashMap<BranchId, Sequence>,
    ) -> ReconciliationReport {
        let mut index = self.index.write();
        let mut report = ReconciliationReport::default();

        for branch in index.branches.values_mut() {
            let floor = branch.branch_point.map(|s| s.0).unwrap_or(0);
            let log_max = log_max_seq.get(&branch.id).map(|s| s.0).unwrap_or(0);
            let reconciled = floor.max(log_max);

            if reconciled != branch.head.0 {
                report.head_fixes.push(HeadReconciliation {
                    name: branch.name.clone(),
                    id: branch.id,
                    old_head: branch.head,
                    new_head: Sequence(reconciled),
                });
                branch.head = Sequence(reconciled);
            }
        }

        // Clamp `next_id` above every BranchId present in the durable log, and
        // flag ids the log holds but `branches.bin` doesn't (orphaned records).
        // Because branches and `next_id` are serialised together in one file,
        // any id in the log at or above `next_id` is necessarily one that never
        // reached `branches.bin` — the reissue-danger case.
        for &branch_id in log_max_seq.keys() {
            if branch_id.0 >= index.next_id {
                index.next_id = branch_id.0 + 1;
            }
            if !index.branches.contains_key(&branch_id) {
                report.orphaned_branch_ids.push(branch_id);
            }
        }

        report
    }

    /// Update the head of a branch.
    pub fn update_head(&self, branch_id: BranchId, new_head: Sequence) -> Result<()> {
        let mut index = self.index.write();
        let branch = index
            .branches
            .get_mut(&branch_id)
            .ok_or_else(|| StoreError::BranchNotFound(format!("{:?}", branch_id)))?;

        branch.head = new_head;
        Ok(())
    }

    /// Get all branches.
    pub fn list_branches(&self) -> Vec<Branch> {
        self.index.read().branches.values().cloned().collect()
    }

    /// Get branch count.
    pub fn branch_count(&self) -> usize {
        self.index.read().branches.len()
    }

    /// Delete a branch (cannot delete main or current branch).
    pub fn delete_branch(&self, name: &str) -> Result<()> {
        if name == MAIN_BRANCH {
            return Err(StoreError::BranchNotFound(
                "Cannot delete main branch".to_string(),
            ));
        }

        let mut index = self.index.write();
        let current_id = *self.current.read();

        let id = index
            .name_to_id
            .get(name)
            .ok_or_else(|| StoreError::BranchNotFound(name.to_string()))?;

        if *id == current_id {
            return Err(StoreError::BranchNotFound(
                "Cannot delete current branch".to_string(),
            ));
        }

        let id = *id;
        index.branches.remove(&id);
        index.name_to_id.remove(name);

        Ok(())
    }

    /// Get branch ancestry (from child to root).
    pub fn get_ancestry(&self, name: &str) -> Result<Vec<Branch>> {
        let index = self.index.read();
        let mut ancestry = Vec::new();

        let id = *index
            .name_to_id
            .get(name)
            .ok_or_else(|| StoreError::BranchNotFound(name.to_string()))?;

        let mut current = index.branches.get(&id).cloned();

        while let Some(branch) = current {
            let parent_id = branch.parent;
            ancestry.push(branch);

            current = parent_id.and_then(|pid| index.branches.get(&pid).cloned());
        }

        Ok(ancestry)
    }

    /// Check if a sequence is visible from a branch.
    ///
    /// A sequence is visible if it's <= branch head and either:
    /// - On the same branch, or
    /// - On an ancestor branch before the branch point
    pub fn is_visible(&self, branch_name: &str, seq: Sequence) -> Result<bool> {
        let branch = self
            .get_branch(branch_name)
            .ok_or_else(|| StoreError::BranchNotFound(branch_name.to_string()))?;

        // If sequence is beyond head, not visible
        if seq > branch.head {
            return Ok(false);
        }

        // If no parent, this is main branch - all sequences up to head are visible
        if branch.parent.is_none() {
            return Ok(true);
        }

        // For child branches, need to check if sequence is after branch point
        // (meaning it was added on this branch) or before/at branch point
        // (inherited from parent)
        Ok(true) // Simplified - would need record-to-branch mapping for full check
    }

    // --- Garbage Collection ---

    /// Find orphaned branches (branches whose parent was deleted).
    ///
    /// These branches can't be properly merged and may need to be deleted or re-parented.
    pub fn get_orphaned_branches(&self) -> Vec<Branch> {
        let index = self.index.read();
        let mut orphans = Vec::new();

        for branch in index.branches.values() {
            if let Some(parent_id) = branch.parent {
                if !index.branches.contains_key(&parent_id) {
                    orphans.push(branch.clone());
                }
            }
        }

        orphans
    }

    /// Find stale branches (branches not updated since given timestamp).
    ///
    /// `older_than` is a Unix timestamp. Branches created before this time
    /// that haven't been updated are considered stale.
    pub fn get_stale_branches(&self, older_than: u64) -> Vec<Branch> {
        let index = self.index.read();
        let main_id = index.name_to_id.get(MAIN_BRANCH).copied();
        let older_than = older_than as i64;

        index
            .branches
            .values()
            .filter(|b| {
                // Never consider main branch as stale
                Some(b.id) != main_id && b.created.0 < older_than
            })
            .cloned()
            .collect()
    }

    /// Find empty branches (branches with no commits beyond branch point).
    ///
    /// These are branches that were created but never used.
    pub fn get_empty_branches(&self) -> Vec<Branch> {
        let index = self.index.read();
        let main_id = index.name_to_id.get(MAIN_BRANCH).copied();

        index
            .branches
            .values()
            .filter(|b| {
                // Skip main branch
                Some(b.id) != main_id
                    // Has branch point
                    && b.branch_point.is_some()
                    // Head equals branch point (no commits made)
                    && b.head == b.branch_point.unwrap()
            })
            .cloned()
            .collect()
    }

    /// Find branches that are children of a given branch.
    pub fn get_child_branches(&self, parent_id: BranchId) -> Vec<Branch> {
        self.index
            .read()
            .branches
            .values()
            .filter(|b| b.parent == Some(parent_id))
            .cloned()
            .collect()
    }

    /// Check if a branch can be safely deleted.
    ///
    /// A branch can be safely deleted if:
    /// - It's not the main branch
    /// - It's not the current branch
    /// - It has no child branches (unless force is true)
    pub fn can_delete(&self, name: &str, force: bool) -> Result<bool> {
        if name == MAIN_BRANCH {
            return Ok(false);
        }

        let index = self.index.read();
        let current_id = *self.current.read();

        let branch_id = match index.name_to_id.get(name) {
            Some(id) => *id,
            None => return Ok(false),
        };

        if branch_id == current_id {
            return Ok(false);
        }

        // Check for children
        if !force {
            for branch in index.branches.values() {
                if branch.parent == Some(branch_id) {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }

    /// Delete a branch and optionally re-parent its children.
    ///
    /// If `reparent_to` is Some, children of the deleted branch will be
    /// re-parented to that branch. If None, children will become orphaned.
    pub fn delete_branch_with_reparent(
        &self,
        name: &str,
        reparent_to: Option<&str>,
    ) -> Result<usize> {
        if name == MAIN_BRANCH {
            return Err(StoreError::BranchNotFound(
                "Cannot delete main branch".to_string(),
            ));
        }

        let mut index = self.index.write();
        let current_id = *self.current.read();

        let branch_id = *index
            .name_to_id
            .get(name)
            .ok_or_else(|| StoreError::BranchNotFound(name.to_string()))?;

        if branch_id == current_id {
            return Err(StoreError::BranchNotFound(
                "Cannot delete current branch".to_string(),
            ));
        }

        // Get new parent ID if reparenting
        let new_parent_id = match reparent_to {
            Some(parent_name) => Some(
                *index
                    .name_to_id
                    .get(parent_name)
                    .ok_or_else(|| StoreError::BranchNotFound(parent_name.to_string()))?,
            ),
            None => None,
        };

        // Reparent children
        let mut reparented = 0;
        let child_ids: Vec<_> = index
            .branches
            .values()
            .filter(|b| b.parent == Some(branch_id))
            .map(|b| b.id)
            .collect();

        for child_id in child_ids {
            if let Some(child) = index.branches.get_mut(&child_id) {
                child.parent = new_parent_id;
                reparented += 1;
            }
        }

        // Delete the branch
        index.branches.remove(&branch_id);
        index.name_to_id.remove(name);

        Ok(reparented)
    }

    /// Garbage collect branches based on criteria.
    ///
    /// Returns the names of branches that were deleted.
    pub fn gc(
        &self,
        options: BranchGcOptions,
    ) -> Result<BranchGcResult> {
        let mut result = BranchGcResult::default();

        // Collect candidates
        let mut candidates: Vec<String> = Vec::new();

        if options.delete_orphaned {
            for branch in self.get_orphaned_branches() {
                candidates.push(branch.name.clone());
            }
        }

        if options.delete_empty {
            for branch in self.get_empty_branches() {
                if !candidates.contains(&branch.name) {
                    candidates.push(branch.name.clone());
                }
            }
        }

        if let Some(older_than) = options.delete_stale_older_than {
            for branch in self.get_stale_branches(older_than) {
                if !candidates.contains(&branch.name) {
                    candidates.push(branch.name.clone());
                }
            }
        }

        // Filter by patterns if specified
        if let Some(ref patterns) = options.name_patterns {
            candidates.retain(|name| {
                patterns.iter().any(|p| name.contains(p))
            });
        }

        // Delete candidates
        for name in candidates {
            if self.can_delete(&name, options.force)? {
                match self.delete_branch_with_reparent(
                    &name,
                    options.reparent_to.as_deref(),
                ) {
                    Ok(reparented) => {
                        result.deleted.push(name);
                        result.reparented += reparented;
                    }
                    Err(e) => {
                        result.errors.push((name, e.to_string()));
                    }
                }
            } else {
                result.skipped.push(name);
            }
        }

        Ok(result)
    }

    /// Save branch index to file.
    pub fn save(&self) -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)?;

        // Write magic
        file.write_all(BRANCH_INDEX_MAGIC)?;

        // Write version
        file.write_all(&[BRANCH_INDEX_VERSION])?;

        // Write current branch
        let current_id = *self.current.read();
        file.write_all(&current_id.0.to_le_bytes())?;

        // Serialize index with MessagePack
        let index = self.index.read();
        let encoded = rmp_serde::to_vec(&*index)
            .map_err(|e| StoreError::Serialization(e.to_string()))?;

        file.write_all(&(encoded.len() as u64).to_le_bytes())?;
        file.write_all(&encoded)?;

        file.sync_all()?;
        Ok(())
    }

    /// Load branch index from file.
    fn load_from_file(&self) -> Result<()> {
        let mut file = File::open(&self.path)?;

        // Read magic
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        if &magic != BRANCH_INDEX_MAGIC {
            return Err(StoreError::InvalidFormat(
                "Invalid branch index magic".into(),
            ));
        }

        // Read version
        let mut version = [0u8; 1];
        file.read_exact(&mut version)?;
        if version[0] != BRANCH_INDEX_VERSION {
            return Err(StoreError::InvalidFormat(format!(
                "Unsupported branch index version: {}",
                version[0]
            )));
        }

        // Read current branch
        let mut current_bytes = [0u8; 8];
        file.read_exact(&mut current_bytes)?;
        let current_id = BranchId(u64::from_le_bytes(current_bytes));
        *self.current.write() = current_id;

        // Read index
        let mut len_bytes = [0u8; 8];
        file.read_exact(&mut len_bytes)?;
        let len = u64::from_le_bytes(len_bytes) as usize;

        let mut encoded = vec![0u8; len];
        file.read_exact(&mut encoded)?;

        let index: BranchIndex = rmp_serde::from_slice(&encoded)
            .map_err(|e| StoreError::Deserialization(e.to_string()))?;

        *self.index.write() = index;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_new_has_main_branch() {
        let dir = TempDir::new().unwrap();
        let manager = BranchManager::new(dir.path().join("branches.bin")).unwrap();

        let main = manager.get_branch(MAIN_BRANCH).unwrap();
        assert_eq!(main.name, MAIN_BRANCH);
        assert_eq!(main.head, Sequence(0));
        assert!(main.parent.is_none());
    }

    #[test]
    fn test_create_branch() {
        let dir = TempDir::new().unwrap();
        let manager = BranchManager::new(dir.path().join("branches.bin")).unwrap();

        // Update main head first
        manager.update_head(BranchId(1), Sequence(10)).unwrap();

        // Create branch from main
        let branch = manager.create_branch("feature", None).unwrap();
        assert_eq!(branch.name, "feature");
        assert_eq!(branch.head, Sequence(10));
        assert_eq!(branch.parent, Some(BranchId(1)));
        assert_eq!(branch.branch_point, Some(Sequence(10)));
    }

    #[test]
    fn test_create_branch_at_sequence() {
        let dir = TempDir::new().unwrap();
        let manager = BranchManager::new(dir.path().join("branches.bin")).unwrap();

        // Update main head
        manager.update_head(BranchId(1), Sequence(20)).unwrap();

        // Create branch at specific sequence
        let branch = manager.create_branch_at("hotfix", MAIN_BRANCH, Sequence(15)).unwrap();
        assert_eq!(branch.name, "hotfix");
        assert_eq!(branch.head, Sequence(15));
        assert_eq!(branch.branch_point, Some(Sequence(15)));
    }

    #[test]
    fn test_branch_already_exists() {
        let dir = TempDir::new().unwrap();
        let manager = BranchManager::new(dir.path().join("branches.bin")).unwrap();

        manager.create_branch("feature", None).unwrap();
        let result = manager.create_branch("feature", None);
        assert!(matches!(result, Err(StoreError::BranchExists(_))));
    }

    #[test]
    fn test_switch_branch() {
        let dir = TempDir::new().unwrap();
        let manager = BranchManager::new(dir.path().join("branches.bin")).unwrap();

        manager.create_branch("feature", None).unwrap();

        let current = manager.current_branch();
        assert_eq!(current.name, MAIN_BRANCH);

        manager.switch_branch("feature").unwrap();

        let current = manager.current_branch();
        assert_eq!(current.name, "feature");
    }

    #[test]
    fn test_list_branches() {
        let dir = TempDir::new().unwrap();
        let manager = BranchManager::new(dir.path().join("branches.bin")).unwrap();

        manager.create_branch("feature1", None).unwrap();
        manager.create_branch("feature2", None).unwrap();

        let branches = manager.list_branches();
        assert_eq!(branches.len(), 3); // main + 2 features
    }

    #[test]
    fn test_delete_branch() {
        let dir = TempDir::new().unwrap();
        let manager = BranchManager::new(dir.path().join("branches.bin")).unwrap();

        manager.create_branch("feature", None).unwrap();
        assert_eq!(manager.branch_count(), 2);

        manager.delete_branch("feature").unwrap();
        assert_eq!(manager.branch_count(), 1);

        assert!(manager.get_branch("feature").is_none());
    }

    #[test]
    fn test_cannot_delete_main() {
        let dir = TempDir::new().unwrap();
        let manager = BranchManager::new(dir.path().join("branches.bin")).unwrap();

        let result = manager.delete_branch(MAIN_BRANCH);
        assert!(result.is_err());
    }

    #[test]
    fn test_cannot_delete_current() {
        let dir = TempDir::new().unwrap();
        let manager = BranchManager::new(dir.path().join("branches.bin")).unwrap();

        manager.create_branch("feature", None).unwrap();
        manager.switch_branch("feature").unwrap();

        let result = manager.delete_branch("feature");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_ancestry() {
        let dir = TempDir::new().unwrap();
        let manager = BranchManager::new(dir.path().join("branches.bin")).unwrap();

        manager.create_branch("feature", None).unwrap();
        manager.create_branch("sub-feature", Some("feature")).unwrap();

        let ancestry = manager.get_ancestry("sub-feature").unwrap();
        assert_eq!(ancestry.len(), 3);
        assert_eq!(ancestry[0].name, "sub-feature");
        assert_eq!(ancestry[1].name, "feature");
        assert_eq!(ancestry[2].name, MAIN_BRANCH);
    }

    #[test]
    fn test_persistence() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("branches.bin");

        // Create and save
        {
            let manager = BranchManager::new(&path).unwrap();
            manager.update_head(BranchId(1), Sequence(5)).unwrap();
            manager.create_branch("feature", None).unwrap();
            manager.switch_branch("feature").unwrap();
            manager.save().unwrap();
        }

        // Load and verify
        {
            let manager = BranchManager::load(&path).unwrap();

            let current = manager.current_branch();
            assert_eq!(current.name, "feature");

            let branches = manager.list_branches();
            assert_eq!(branches.len(), 2);

            let main = manager.get_branch(MAIN_BRANCH).unwrap();
            assert_eq!(main.head, Sequence(5));
        }
    }

    #[test]
    fn test_reconcile_heads_both_directions_and_floor() {
        let dir = TempDir::new().unwrap();
        let manager = BranchManager::new(dir.path().join("branches.bin")).unwrap();

        // main: persisted head 5 (stale-low; log actually holds up to 10).
        manager.update_head(BranchId(1), Sequence(5)).unwrap();
        // child branched at main's (then-)head 5: inherits head 5, bp 5, and
        // has NO records of its own in the log.
        let child = manager.create_branch("child", None).unwrap();
        assert_eq!(child.branch_point, Some(Sequence(5)));

        let mut log_max = HashMap::new();
        log_max.insert(BranchId(1), Sequence(10)); // main durable to 10

        let report = manager.reconcile_heads(&log_max);

        // main raised 5 -> 10 (kills duplicate-sequence bug).
        assert_eq!(manager.get_branch(MAIN_BRANCH).unwrap().head, Sequence(10));
        // child has no log records: head stays at its branch_point floor (5),
        // NOT lowered to 0.
        assert_eq!(manager.get_branch("child").unwrap().head, Sequence(5));
        // Only main changed.
        assert_eq!(report.head_fixes.len(), 1);
        assert_eq!(report.head_fixes[0].name, MAIN_BRANCH);
        assert_eq!(report.head_fixes[0].old_head, Sequence(5));
        assert_eq!(report.head_fixes[0].new_head, Sequence(10));
        // main is a known branch: not orphaned.
        assert!(report.orphaned_branch_ids.is_empty());

        // Now the too-high direction: head persisted past a truncated tail.
        let mut log_max2 = HashMap::new();
        log_max2.insert(BranchId(1), Sequence(7)); // log truncated back to 7
        let report2 = manager.reconcile_heads(&log_max2);
        assert_eq!(manager.get_branch(MAIN_BRANCH).unwrap().head, Sequence(7));
        assert_eq!(report2.head_fixes.len(), 1);
        assert_eq!(report2.head_fixes[0].new_head, Sequence(7));
    }

    #[test]
    fn test_reconcile_clamps_next_id_over_orphaned_log_branch() {
        let dir = TempDir::new().unwrap();
        let manager = BranchManager::new(dir.path().join("branches.bin")).unwrap();

        // Simulate a crash: the log holds records for BranchId(2) (a branch that
        // was created and written to) but `branches.bin` never recorded it, so
        // `next_id` is still the freshly-initialised 2.
        let mut log_max = HashMap::new();
        log_max.insert(BranchId(1), Sequence(3)); // main
        log_max.insert(BranchId(2), Sequence(6)); // orphaned branch's records

        let report = manager.reconcile_heads(&log_max);

        // BranchId(2) is unreachable metadata-wise but its records are durable.
        assert_eq!(report.orphaned_branch_ids, vec![BranchId(2)]);

        // The next branch MUST NOT be handed BranchId(2) (which would adopt the
        // orphaned records); it must get BranchId(3).
        let next = manager.create_branch("other", None).unwrap();
        assert_eq!(
            next.id,
            BranchId(3),
            "next_id must be clamped above every BranchId present in the log"
        );
    }

    #[test]
    fn test_update_head() {
        let dir = TempDir::new().unwrap();
        let manager = BranchManager::new(dir.path().join("branches.bin")).unwrap();

        manager.update_head(BranchId(1), Sequence(100)).unwrap();

        let main = manager.get_branch(MAIN_BRANCH).unwrap();
        assert_eq!(main.head, Sequence(100));
    }

    #[test]
    fn test_get_empty_branches() {
        let dir = TempDir::new().unwrap();
        let manager = BranchManager::new(dir.path().join("branches.bin")).unwrap();

        // Create branches
        manager.create_branch("empty1", None).unwrap();
        manager.create_branch("empty2", None).unwrap();

        // Update one to have commits
        let branch = manager.get_branch("empty1").unwrap();
        manager.update_head(branch.id, Sequence(5)).unwrap();

        // Only empty2 should be considered empty
        let empty = manager.get_empty_branches();
        assert_eq!(empty.len(), 1);
        assert_eq!(empty[0].name, "empty2");
    }

    #[test]
    fn test_gc_empty_branches() {
        let dir = TempDir::new().unwrap();
        let manager = BranchManager::new(dir.path().join("branches.bin")).unwrap();

        // Create empty branches
        manager.create_branch("empty1", None).unwrap();
        manager.create_branch("empty2", None).unwrap();
        manager.create_branch("used", None).unwrap();

        // Make "used" have commits
        let used_branch = manager.get_branch("used").unwrap();
        manager.update_head(used_branch.id, Sequence(10)).unwrap();

        assert_eq!(manager.branch_count(), 4);

        // GC empty branches
        let result = manager.gc(BranchGcOptions {
            delete_empty: true,
            ..Default::default()
        }).unwrap();

        assert_eq!(result.deleted.len(), 2);
        assert!(result.deleted.contains(&"empty1".to_string()));
        assert!(result.deleted.contains(&"empty2".to_string()));
        assert_eq!(manager.branch_count(), 2); // main + used
    }

    #[test]
    fn test_gc_with_reparenting() {
        let dir = TempDir::new().unwrap();
        let manager = BranchManager::new(dir.path().join("branches.bin")).unwrap();

        // Create hierarchy: main -> feature -> sub-feature
        manager.create_branch("feature", None).unwrap();
        manager.create_branch("sub-feature", Some("feature")).unwrap();

        // GC feature with reparenting to main
        let result = manager.gc(BranchGcOptions {
            delete_empty: true,
            force: true,
            reparent_to: Some(MAIN_BRANCH.to_string()),
            ..Default::default()
        }).unwrap();

        // Both should be deleted since both are empty, but feature first
        // sub-feature gets reparented to main, then deleted too
        assert!(result.deleted.len() >= 1);

        // sub-feature should now have main as parent (or be deleted)
        let remaining = manager.list_branches();
        for branch in &remaining {
            if branch.name != MAIN_BRANCH {
                // Any remaining non-main branch should have main as parent
                assert_eq!(branch.parent, Some(BranchId(1)));
            }
        }
    }

    #[test]
    fn test_can_delete() {
        let dir = TempDir::new().unwrap();
        let manager = BranchManager::new(dir.path().join("branches.bin")).unwrap();

        // Cannot delete main
        assert!(!manager.can_delete(MAIN_BRANCH, false).unwrap());

        // Create branches
        manager.create_branch("feature", None).unwrap();
        manager.create_branch("child", Some("feature")).unwrap();

        // Cannot delete feature without force (has child)
        assert!(!manager.can_delete("feature", false).unwrap());

        // Can delete feature with force
        assert!(manager.can_delete("feature", true).unwrap());

        // Can delete child (no children)
        assert!(manager.can_delete("child", false).unwrap());

        // Switch to feature - cannot delete current
        manager.switch_branch("feature").unwrap();
        assert!(!manager.can_delete("feature", true).unwrap());
    }

    #[test]
    fn test_gc_with_patterns() {
        let dir = TempDir::new().unwrap();
        let manager = BranchManager::new(dir.path().join("branches.bin")).unwrap();

        // Create branches with different names
        manager.create_branch("feature-123", None).unwrap();
        manager.create_branch("feature-456", None).unwrap();
        manager.create_branch("hotfix-789", None).unwrap();

        // GC only feature branches
        let result = manager.gc(BranchGcOptions {
            delete_empty: true,
            name_patterns: Some(vec!["feature".to_string()]),
            ..Default::default()
        }).unwrap();

        assert_eq!(result.deleted.len(), 2);
        assert!(result.deleted.iter().all(|n| n.contains("feature")));

        // hotfix should still exist
        assert!(manager.get_branch("hotfix-789").is_some());
    }
}
