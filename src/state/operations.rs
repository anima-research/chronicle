//! State operation application.

use crate::error::{Result, StoreError};
use crate::types::{StateOperation, TreeEntry, TreeOp, TreeState};

/// Apply a state operation to a value.
///
/// The value is expected to be JSON-encoded for structured operations.
pub fn apply_operation(state: Vec<u8>, operation: StateOperation) -> Result<Vec<u8>> {
    match operation {
        StateOperation::Set(value) => Ok(value),

        StateOperation::Snapshot(value) => Ok(value),

        StateOperation::DeltaSnapshot(delta_items) => {
            // Delta snapshot contains items added since last delta/full snapshot.
            // Concatenate with current state (both are JSON arrays).
            let mut arr: Vec<serde_json::Value> = if state.is_empty() {
                Vec::new()
            } else {
                serde_json::from_slice(&state)
                    .map_err(|e| StoreError::Deserialization(e.to_string()))?
            };

            let delta_arr: Vec<serde_json::Value> = serde_json::from_slice(&delta_items)
                .map_err(|e| StoreError::Deserialization(e.to_string()))?;

            arr.extend(delta_arr);

            serde_json::to_vec(&arr).map_err(|e| StoreError::Serialization(e.to_string()))
        }

        StateOperation::Delta { new_value, .. } => {
            // For delta, we just use the new value
            // The old_hash is for verification (not implemented yet)
            Ok(new_value)
        }

        StateOperation::Append(item) => {
            // Parse state as JSON array, append item
            let mut arr: Vec<serde_json::Value> = if state.is_empty() {
                Vec::new()
            } else {
                serde_json::from_slice(&state)
                    .map_err(|e| StoreError::Deserialization(e.to_string()))?
            };

            let item_value: serde_json::Value = serde_json::from_slice(&item)
                .map_err(|e| StoreError::Deserialization(e.to_string()))?;

            arr.push(item_value);

            serde_json::to_vec(&arr).map_err(|e| StoreError::Serialization(e.to_string()))
        }

        StateOperation::Redact { start, end } => {
            // Parse state as JSON array, remove range
            let mut arr: Vec<serde_json::Value> = if state.is_empty() {
                Vec::new()
            } else {
                serde_json::from_slice(&state)
                    .map_err(|e| StoreError::Deserialization(e.to_string()))?
            };

            // Clamp indices to valid range
            let start = start.min(arr.len());
            let end = end.min(arr.len());

            if start < end {
                arr.drain(start..end);
            }

            serde_json::to_vec(&arr).map_err(|e| StoreError::Serialization(e.to_string()))
        }

        StateOperation::Edit { index, new_value } => {
            // Parse state as JSON array, edit item at index
            let mut arr: Vec<serde_json::Value> = if state.is_empty() {
                Vec::new()
            } else {
                serde_json::from_slice(&state)
                    .map_err(|e| StoreError::Deserialization(e.to_string()))?
            };

            if index >= arr.len() {
                return Err(StoreError::Corruption(format!(
                    "Edit index {} out of bounds (len {})",
                    index,
                    arr.len()
                )));
            }

            let new_item: serde_json::Value = serde_json::from_slice(&new_value)
                .map_err(|e| StoreError::Deserialization(e.to_string()))?;

            arr[index] = new_item;

            serde_json::to_vec(&arr).map_err(|e| StoreError::Serialization(e.to_string()))
        }

        StateOperation::TreeSet { path, entry } => {
            let mut tree: TreeState = if state.is_empty() {
                TreeState::new()
            } else {
                serde_json::from_slice(&state)
                    .map_err(|e| StoreError::Deserialization(e.to_string()))?
            };
            let tree_entry: TreeEntry = serde_json::from_slice(&entry)
                .map_err(|e| StoreError::Deserialization(e.to_string()))?;
            tree.insert(path, tree_entry);
            serde_json::to_vec(&tree).map_err(|e| StoreError::Serialization(e.to_string()))
        }

        StateOperation::TreeRemove { path } => {
            let mut tree: TreeState = if state.is_empty() {
                TreeState::new()
            } else {
                serde_json::from_slice(&state)
                    .map_err(|e| StoreError::Deserialization(e.to_string()))?
            };
            tree.remove(&path);
            serde_json::to_vec(&tree).map_err(|e| StoreError::Serialization(e.to_string()))
        }

        StateOperation::TreeBatch { ops } => {
            let mut tree: TreeState = if state.is_empty() {
                TreeState::new()
            } else {
                serde_json::from_slice(&state)
                    .map_err(|e| StoreError::Deserialization(e.to_string()))?
            };
            let tree_ops: Vec<TreeOp> = serde_json::from_slice(&ops)
                .map_err(|e| StoreError::Deserialization(e.to_string()))?;
            for op in tree_ops {
                match op {
                    TreeOp::Set { path, entry } => { tree.insert(path, entry); }
                    TreeOp::Remove { path } => { tree.remove(&path); }
                }
            }
            serde_json::to_vec(&tree).map_err(|e| StoreError::Serialization(e.to_string()))
        }

        StateOperation::TreeDeltaSnapshot(delta) => {
            let mut tree: TreeState = if state.is_empty() {
                TreeState::new()
            } else {
                serde_json::from_slice(&state)
                    .map_err(|e| StoreError::Deserialization(e.to_string()))?
            };
            let tree_ops: Vec<TreeOp> = serde_json::from_slice(&delta)
                .map_err(|e| StoreError::Deserialization(e.to_string()))?;
            for op in tree_ops {
                match op {
                    TreeOp::Set { path, entry } => { tree.insert(path, entry); }
                    TreeOp::Remove { path } => { tree.remove(&path); }
                }
            }
            serde_json::to_vec(&tree).map_err(|e| StoreError::Serialization(e.to_string()))
        }

        StateOperation::Field { name, operation } => {
            // Parse state as JSON object, apply operation to field
            let mut obj: serde_json::Map<String, serde_json::Value> = if state.is_empty() {
                serde_json::Map::new()
            } else {
                serde_json::from_slice(&state)
                    .map_err(|e| StoreError::Deserialization(e.to_string()))?
            };

            // Get current field value
            let field_state = obj
                .get(&name)
                .map(|v| serde_json::to_vec(v).unwrap_or_default())
                .unwrap_or_default();

            // Apply operation to field
            let new_field_state = apply_operation(field_state, *operation)?;

            // Parse and set new field value
            let new_field_value: serde_json::Value = serde_json::from_slice(&new_field_state)
                .map_err(|e| StoreError::Deserialization(e.to_string()))?;

            obj.insert(name, new_field_value);

            serde_json::to_vec(&obj).map_err(|e| StoreError::Serialization(e.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_set() {
        let state = vec![];
        let op = StateOperation::Set(b"\"hello\"".to_vec());
        let result = apply_operation(state, op).unwrap();
        assert_eq!(result, b"\"hello\"");
    }

    #[test]
    fn test_append() {
        // Start empty
        let state = vec![];
        let op = StateOperation::Append(b"1".to_vec());
        let state = apply_operation(state, op).unwrap();

        let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
        assert_eq!(arr, vec![1]);

        // Append more
        let op = StateOperation::Append(b"2".to_vec());
        let state = apply_operation(state, op).unwrap();

        let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
        assert_eq!(arr, vec![1, 2]);
    }

    #[test]
    fn test_redact() {
        let state = serde_json::to_vec(&json!([1, 2, 3, 4, 5])).unwrap();

        let op = StateOperation::Redact { start: 1, end: 3 };
        let state = apply_operation(state, op).unwrap();

        let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
        assert_eq!(arr, vec![1, 4, 5]);
    }

    #[test]
    fn test_edit() {
        let state = serde_json::to_vec(&json!(["a", "b", "c"])).unwrap();

        let op = StateOperation::Edit {
            index: 1,
            new_value: b"\"x\"".to_vec(),
        };
        let state = apply_operation(state, op).unwrap();

        let arr: Vec<String> = serde_json::from_slice(&state).unwrap();
        assert_eq!(arr, vec!["a", "x", "c"]);
    }

    #[test]
    fn test_field() {
        let state = serde_json::to_vec(&json!({"count": 0, "name": "test"})).unwrap();

        let op = StateOperation::Field {
            name: "count".to_string(),
            operation: Box::new(StateOperation::Set(b"42".to_vec())),
        };
        let state = apply_operation(state, op).unwrap();

        let obj: serde_json::Value = serde_json::from_slice(&state).unwrap();
        assert_eq!(obj["count"], 42);
        assert_eq!(obj["name"], "test");
    }

    #[test]
    fn test_nested_field_append() {
        let state = serde_json::to_vec(&json!({"items": [1, 2], "meta": {}})).unwrap();

        let op = StateOperation::Field {
            name: "items".to_string(),
            operation: Box::new(StateOperation::Append(b"3".to_vec())),
        };
        let state = apply_operation(state, op).unwrap();

        let obj: serde_json::Value = serde_json::from_slice(&state).unwrap();
        assert_eq!(obj["items"], json!([1, 2, 3]));
    }

    #[test]
    fn test_delta_snapshot() {
        // Start with base array
        let state = serde_json::to_vec(&json!([1, 2, 3])).unwrap();

        // Apply delta snapshot (new items)
        let delta = serde_json::to_vec(&json!([4, 5])).unwrap();
        let op = StateOperation::DeltaSnapshot(delta);
        let state = apply_operation(state, op).unwrap();

        let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
        assert_eq!(arr, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_delta_snapshot_on_empty() {
        let state = vec![];

        // Apply delta snapshot to empty state
        let delta = serde_json::to_vec(&json!([1, 2, 3])).unwrap();
        let op = StateOperation::DeltaSnapshot(delta);
        let state = apply_operation(state, op).unwrap();

        let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
        assert_eq!(arr, vec![1, 2, 3]);
    }

    fn make_tree_entry(hash: &str, size: u64) -> TreeEntry {
        TreeEntry {
            blob_hash: hash.to_string(),
            size,
            mode: 0o644,
        }
    }

    #[test]
    fn test_tree_set() {
        let state = vec![];
        let entry = serde_json::to_vec(&make_tree_entry("abc123", 100)).unwrap();
        let op = StateOperation::TreeSet {
            path: "src/main.rs".to_string(),
            entry,
        };
        let state = apply_operation(state, op).unwrap();
        let tree: TreeState = serde_json::from_slice(&state).unwrap();
        assert_eq!(tree.len(), 1);
        assert_eq!(tree["src/main.rs"].blob_hash, "abc123");
        assert_eq!(tree["src/main.rs"].size, 100);
    }

    #[test]
    fn test_tree_set_overwrite() {
        let state = vec![];
        let entry1 = serde_json::to_vec(&make_tree_entry("abc123", 100)).unwrap();
        let state = apply_operation(
            state,
            StateOperation::TreeSet {
                path: "file.txt".to_string(),
                entry: entry1,
            },
        )
        .unwrap();

        let entry2 = serde_json::to_vec(&make_tree_entry("def456", 200)).unwrap();
        let state = apply_operation(
            state,
            StateOperation::TreeSet {
                path: "file.txt".to_string(),
                entry: entry2,
            },
        )
        .unwrap();

        let tree: TreeState = serde_json::from_slice(&state).unwrap();
        assert_eq!(tree.len(), 1);
        assert_eq!(tree["file.txt"].blob_hash, "def456");
        assert_eq!(tree["file.txt"].size, 200);
    }

    #[test]
    fn test_tree_remove() {
        // Build tree with two files
        let mut tree = TreeState::new();
        tree.insert("a.txt".to_string(), make_tree_entry("aaa", 10));
        tree.insert("b.txt".to_string(), make_tree_entry("bbb", 20));
        let state = serde_json::to_vec(&tree).unwrap();

        let state = apply_operation(
            state,
            StateOperation::TreeRemove {
                path: "a.txt".to_string(),
            },
        )
        .unwrap();

        let tree: TreeState = serde_json::from_slice(&state).unwrap();
        assert_eq!(tree.len(), 1);
        assert!(tree.contains_key("b.txt"));
        assert!(!tree.contains_key("a.txt"));
    }

    #[test]
    fn test_tree_remove_nonexistent() {
        let state = vec![];
        let result = apply_operation(
            state,
            StateOperation::TreeRemove {
                path: "nope.txt".to_string(),
            },
        )
        .unwrap();
        let tree: TreeState = serde_json::from_slice(&result).unwrap();
        assert!(tree.is_empty());
    }

    #[test]
    fn test_tree_batch() {
        let state = vec![];
        let ops = vec![
            TreeOp::Set {
                path: "src/lib.rs".to_string(),
                entry: make_tree_entry("aaa", 50),
            },
            TreeOp::Set {
                path: "src/main.rs".to_string(),
                entry: make_tree_entry("bbb", 100),
            },
            TreeOp::Set {
                path: "README.md".to_string(),
                entry: make_tree_entry("ccc", 200),
            },
            TreeOp::Remove {
                path: "src/lib.rs".to_string(),
            },
        ];
        let ops_bytes = serde_json::to_vec(&ops).unwrap();
        let state = apply_operation(state, StateOperation::TreeBatch { ops: ops_bytes }).unwrap();

        let tree: TreeState = serde_json::from_slice(&state).unwrap();
        assert_eq!(tree.len(), 2);
        assert!(tree.contains_key("src/main.rs"));
        assert!(tree.contains_key("README.md"));
        assert!(!tree.contains_key("src/lib.rs"));
    }

    #[test]
    fn test_tree_delta_snapshot() {
        // Start with a tree
        let mut tree = TreeState::new();
        tree.insert("a.txt".to_string(), make_tree_entry("aaa", 10));
        let state = serde_json::to_vec(&tree).unwrap();

        // Apply delta: add b.txt, remove a.txt
        let delta_ops = vec![
            TreeOp::Set {
                path: "b.txt".to_string(),
                entry: make_tree_entry("bbb", 20),
            },
            TreeOp::Remove {
                path: "a.txt".to_string(),
            },
        ];
        let delta = serde_json::to_vec(&delta_ops).unwrap();
        let state = apply_operation(state, StateOperation::TreeDeltaSnapshot(delta)).unwrap();

        let tree: TreeState = serde_json::from_slice(&state).unwrap();
        assert_eq!(tree.len(), 1);
        assert!(tree.contains_key("b.txt"));
        assert!(!tree.contains_key("a.txt"));
    }

    #[test]
    fn test_multiple_delta_snapshots() {
        // Simulate reconstruction with multiple delta snapshots
        let mut state = vec![];

        // First delta snapshot
        let delta1 = serde_json::to_vec(&json!([1, 2])).unwrap();
        state = apply_operation(state, StateOperation::DeltaSnapshot(delta1)).unwrap();

        // Second delta snapshot
        let delta2 = serde_json::to_vec(&json!([3, 4])).unwrap();
        state = apply_operation(state, StateOperation::DeltaSnapshot(delta2)).unwrap();

        // Append after delta
        state = apply_operation(state, StateOperation::Append(b"5".to_vec())).unwrap();

        let arr: Vec<i32> = serde_json::from_slice(&state).unwrap();
        assert_eq!(arr, vec![1, 2, 3, 4, 5]);
    }
}
