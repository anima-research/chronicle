//! State operation application.

use crate::error::{Result, StoreError};
use crate::types::{StateOperation, TreeOp, TreeState};

/// Deserialize tree state from bytes, treating empty as a new tree.
fn deserialize_tree(state: &[u8]) -> Result<TreeState> {
    if state.is_empty() {
        Ok(TreeState::new())
    } else {
        serde_json::from_slice(state).map_err(|e| StoreError::Deserialization(e.to_string()))
    }
}

/// Apply a sequence of tree ops (Set/Remove) to a mutable tree.
fn apply_tree_ops(tree: &mut TreeState, ops: Vec<TreeOp>) {
    for op in ops {
        match op {
            TreeOp::Set { path, entry } => {
                tree.insert(path, entry);
            }
            TreeOp::Remove { path } => {
                tree.remove(&path);
            }
        }
    }
}

/// Accumulator for single-materialization op application: the state is held
/// in decoded form across the whole op sequence instead of being re-parsed
/// and re-serialized per op.
enum Materialized {
    /// Raw bytes — the initial/terminal form (empty, or a Set/Snapshot/Delta
    /// payload that nothing structured has touched yet).
    Raw(Vec<u8>),
    /// Decoded JSON array (AppendLog-family states).
    Arr(Vec<serde_json::Value>),
    /// Decoded path→entry map (Tree states).
    Tree(TreeState),
}

impl Materialized {
    fn as_arr(&mut self) -> Result<&mut Vec<serde_json::Value>> {
        if let Materialized::Raw(bytes) = self {
            let arr: Vec<serde_json::Value> = if bytes.is_empty() {
                Vec::new()
            } else {
                serde_json::from_slice(bytes)
                    .map_err(|e| StoreError::Deserialization(e.to_string()))?
            };
            *self = Materialized::Arr(arr);
        }
        match self {
            Materialized::Arr(a) => Ok(a),
            // A tree op followed by an array op (or vice versa) on one state
            // is malformed; the legacy per-op path fails at its serde parse
            // with the same error variant (message text differs).
            _ => Err(StoreError::Deserialization(
                "state is not a JSON array".to_string(),
            )),
        }
    }

    fn as_tree(&mut self) -> Result<&mut TreeState> {
        if let Materialized::Raw(bytes) = self {
            *self = Materialized::Tree(deserialize_tree(bytes)?);
        }
        match self {
            Materialized::Tree(t) => Ok(t),
            _ => Err(StoreError::Deserialization(
                "state is not a tree object".to_string(),
            )),
        }
    }

    fn into_bytes(self) -> Result<Vec<u8>> {
        match self {
            Materialized::Raw(bytes) => Ok(bytes),
            Materialized::Arr(arr) => serde_json::to_vec(&arr)
                .map_err(|e| StoreError::Serialization(e.to_string())),
            Materialized::Tree(tree) => serde_json::to_vec(&tree)
                .map_err(|e| StoreError::Serialization(e.to_string())),
        }
    }
}

/// Apply a whole op sequence with a single decode and a single encode.
///
/// Semantically identical to folding `apply_operation` over `ops` (same
/// clamping, same out-of-bounds errors, same serde value round-trip), but
/// O(state + ops) instead of O(state × ops): reconstruction chains — reopen,
/// cache miss, branch switch, time travel — stop paying a full parse and
/// re-serialize per op. Ops with no fast path (`Field`) fall back to
/// `apply_operation` for that op only.
pub fn materialize_operations(ops: Vec<StateOperation>) -> Result<Vec<u8>> {
    materialize_operations_tagged(ops).map(|(bytes, _)| bytes)
}

/// `materialize_operations`, also reporting whether the returned bytes are
/// this crate's own serialization of the decoded value (`true`) or a payload
/// passed through verbatim from a `Set`/`Snapshot`/`Delta` operation, or
/// nothing at all (`false`). Verbatim bytes may spell the same JSON
/// differently from how a later re-materialization would (key order,
/// whitespace, number form).
pub(crate) fn materialize_operations_tagged(ops: Vec<StateOperation>) -> Result<(Vec<u8>, bool)> {
    let mut acc = Materialized::Raw(Vec::new());

    for op in ops {
        match op {
            StateOperation::Set(value)
            | StateOperation::Snapshot(value)
            | StateOperation::Delta { new_value: value, .. } => {
                acc = Materialized::Raw(value);
            }
            StateOperation::Append(item) => {
                let item_value: serde_json::Value = serde_json::from_slice(&item)
                    .map_err(|e| StoreError::Deserialization(e.to_string()))?;
                acc.as_arr()?.push(item_value);
            }
            StateOperation::DeltaSnapshot(delta_items) => {
                let delta_arr: Vec<serde_json::Value> = serde_json::from_slice(&delta_items)
                    .map_err(|e| StoreError::Deserialization(e.to_string()))?;
                acc.as_arr()?.extend(delta_arr);
            }
            StateOperation::Redact { start, end } => {
                let arr = acc.as_arr()?;
                let start = start.min(arr.len());
                let end = end.min(arr.len());
                if start < end {
                    arr.drain(start..end);
                }
            }
            StateOperation::Edit { index, new_value } => {
                let arr = acc.as_arr()?;
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
            }
            StateOperation::TreeSet { path, entry } => {
                acc.as_tree()?.insert(path, entry);
            }
            StateOperation::TreeRemove { path } => {
                acc.as_tree()?.remove(&path);
            }
            StateOperation::TreeBatch { ops } | StateOperation::TreeDeltaSnapshot(ops) => {
                apply_tree_ops(acc.as_tree()?, ops);
            }
            other => {
                // No fast path (Field / future variants): round-trip through
                // the per-op application for this op only.
                let bytes = std::mem::replace(&mut acc, Materialized::Raw(Vec::new()))
                    .into_bytes()?;
                acc = Materialized::Raw(apply_operation(bytes, other)?);
            }
        }
    }

    let reserialized = !matches!(acc, Materialized::Raw(_));
    Ok((acc.into_bytes()?, reserialized))
}

/// Number of top-level elements of a JSON array or entries of a JSON object,
/// counted without building a `Value` tree. `is_array` reports which it was.
struct TopLevelLen {
    len: usize,
    is_array: bool,
}

impl<'de> serde::Deserialize<'de> for TopLevelLen {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        use serde::de::{IgnoredAny, MapAccess, SeqAccess, Visitor};

        struct LenVisitor;

        impl<'de> Visitor<'de> for LenVisitor {
            type Value = TopLevelLen;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a JSON array or object")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<TopLevelLen, A::Error> {
                let mut len = 0;
                while seq.next_element::<IgnoredAny>()?.is_some() {
                    len += 1;
                }
                Ok(TopLevelLen { len, is_array: true })
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<TopLevelLen, A::Error> {
                let mut len = 0;
                while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {
                    len += 1;
                }
                Ok(TopLevelLen { len, is_array: false })
            }
        }

        deserializer.deserialize_any(LenVisitor)
    }
}

/// Length of a JSON array given as bytes, or `None` if `data` is not a
/// well-formed JSON array. A boundary scan only: no `Value` tree is built, so
/// counting a large snapshot costs a pass over its bytes and no allocation.
pub(crate) fn json_array_len(data: &[u8]) -> Option<usize> {
    match serde_json::from_slice::<TopLevelLen>(data) {
        Ok(t) if t.is_array => Some(t.len),
        _ => None,
    }
}

/// The item count a full `Snapshot` stamps on a chain head: array elements
/// for array-shaped states, entries for map-shaped ones, 0 for anything else.
/// Same result as the `Value`-based count in `StateManager::record_update`
/// for every payload this store writes, without building the tree.
pub(crate) fn snapshot_item_count(data: &[u8]) -> usize {
    serde_json::from_slice::<TopLevelLen>(data)
        .map(|t| t.len)
        .unwrap_or(0)
}

/// Item count of the array-shaped state that `materialize_operations(ops)`
/// would produce, computed without materializing it.
///
/// Returns `None` whenever the count cannot be decided from lengths alone —
/// a non-array base value, a tree or struct operation, or anything that
/// `materialize_operations` would reject (an unparseable item, an Edit past
/// the end). Callers then fall back to real materialization, which either
/// yields the count or surfaces the same error it always did.
pub(crate) fn array_len_after(ops: &[StateOperation]) -> Option<usize> {
    fn is_json(bytes: &[u8]) -> bool {
        serde_json::from_slice::<serde::de::IgnoredAny>(bytes).is_ok()
    }

    let mut len = 0usize;
    for op in ops {
        match op {
            StateOperation::Set(value) | StateOperation::Snapshot(value) => {
                len = json_array_len(value)?;
            }
            StateOperation::Append(item) => {
                if !is_json(item) {
                    return None;
                }
                len += 1;
            }
            StateOperation::DeltaSnapshot(items) => {
                len += json_array_len(items)?;
            }
            StateOperation::Redact { start, end } => {
                let start = (*start).min(len);
                let end = (*end).min(len);
                if start < end {
                    len -= end - start;
                }
            }
            StateOperation::Edit { index, new_value } => {
                if *index >= len || !is_json(new_value) {
                    return None;
                }
            }
            _ => return None,
        }
    }
    Some(len)
}

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
            let mut tree = deserialize_tree(&state)?;
            tree.insert(path, entry);
            serde_json::to_vec(&tree).map_err(|e| StoreError::Serialization(e.to_string()))
        }

        StateOperation::TreeRemove { path } => {
            let mut tree = deserialize_tree(&state)?;
            tree.remove(&path);
            serde_json::to_vec(&tree).map_err(|e| StoreError::Serialization(e.to_string()))
        }

        StateOperation::TreeBatch { ops } => {
            let mut tree = deserialize_tree(&state)?;
            apply_tree_ops(&mut tree, ops);
            serde_json::to_vec(&tree).map_err(|e| StoreError::Serialization(e.to_string()))
        }

        StateOperation::TreeDeltaSnapshot(ops) => {
            let mut tree = deserialize_tree(&state)?;
            apply_tree_ops(&mut tree, ops);
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
    use crate::types::TreeEntry;
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
        let op = StateOperation::TreeSet {
            path: "src/main.rs".to_string(),
            entry: make_tree_entry("abc123", 100),
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
        let state = apply_operation(
            state,
            StateOperation::TreeSet {
                path: "file.txt".to_string(),
                entry: make_tree_entry("abc123", 100),
            },
        )
        .unwrap();

        let state = apply_operation(
            state,
            StateOperation::TreeSet {
                path: "file.txt".to_string(),
                entry: make_tree_entry("def456", 200),
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
        let state = apply_operation(state, StateOperation::TreeBatch { ops }).unwrap();

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
        let state = apply_operation(state, StateOperation::TreeDeltaSnapshot(delta_ops)).unwrap();

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
    /// `array_len_after` must agree with real materialization wherever it
    /// answers, and decline (`None`) wherever materialization would not
    /// produce a plain array.
    #[test]
    fn test_array_len_after_matches_materialization() {
        let arr = |v: serde_json::Value| serde_json::to_vec(&v).unwrap();
        let cases: Vec<Vec<StateOperation>> = vec![
            vec![],
            vec![StateOperation::Append(b"1".to_vec()), StateOperation::Append(b"{\"a\":[1,2]}".to_vec())],
            vec![
                StateOperation::Snapshot(arr(json!([1, [2, 3], {"k": [4]}, "x,y"]))),
                StateOperation::DeltaSnapshot(arr(json!([5, 6]))),
                StateOperation::Append(b"7".to_vec()),
                StateOperation::Edit { index: 0, new_value: b"9".to_vec() },
                StateOperation::Redact { start: 1, end: 3 },
                StateOperation::Redact { start: 50, end: 60 },
                StateOperation::Redact { start: 3, end: 1 },
            ],
            vec![
                StateOperation::Append(b"1".to_vec()),
                StateOperation::Set(arr(json!(["a", "b"]))),
                StateOperation::Append(b"2".to_vec()),
            ],
            vec![StateOperation::Set(arr(json!([])))],
        ];
        for ops in cases {
            let expected: Vec<serde_json::Value> = {
                let bytes = materialize_operations(ops.clone()).unwrap();
                if bytes.is_empty() { vec![] } else { serde_json::from_slice(&bytes).unwrap() }
            };
            assert_eq!(array_len_after(&ops), Some(expected.len()), "{:?}", ops);
        }

        let declined: Vec<Vec<StateOperation>> = vec![
            // Non-array base value.
            vec![StateOperation::Set(arr(json!({"a": 1})))],
            vec![StateOperation::Set(Vec::new())],
            // Materialization rejects these; the caller must see that error.
            vec![StateOperation::Append(b"not json".to_vec())],
            vec![StateOperation::Append(b"1".to_vec()), StateOperation::Edit { index: 1, new_value: b"2".to_vec() }],
            vec![StateOperation::Append(b"1".to_vec()), StateOperation::Edit { index: 0, new_value: b"{".to_vec() }],
            // Tree ops are not length-decidable.
            vec![StateOperation::TreeRemove { path: "a".to_string() }],
        ];
        for ops in declined {
            assert_eq!(array_len_after(&ops), None, "{:?}", ops);
        }
    }

    #[test]
    fn test_snapshot_item_count_matches_value_count() {
        for payload in [
            json!([]),
            json!([1, 2, 3]),
            json!({"a": {"x": 1}, "b": [1, 2]}),
            json!("scalar"),
            json!(7),
        ] {
            let bytes = serde_json::to_vec(&payload).unwrap();
            let expected = match &payload {
                serde_json::Value::Array(a) => a.len(),
                serde_json::Value::Object(m) => m.len(),
                _ => 0,
            };
            assert_eq!(snapshot_item_count(&bytes), expected, "{payload}");
        }
        assert_eq!(snapshot_item_count(b""), 0);
        assert_eq!(snapshot_item_count(b"[1, 2"), 0);
        assert_eq!(json_array_len(b"{\"a\":1}"), None);
        assert_eq!(json_array_len(b"[1,2] trailing"), None);
    }
}
