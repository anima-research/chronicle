//! Differential check: `materialize_operations` must agree with folding
//! `apply_operation` over the same op sequence — the equivalence the
//! `materialize_operations` doc comment claims. Error *variants* must match;
//! message text is allowed to differ.

use chronicle::state::{apply_operation, materialize_operations};
use chronicle::types::{StateOperation, TreeEntry, TreeOp};
use serde_json::json;

/// Compare error *variants* only — message text is allowed to differ.
fn variant(e: &chronicle::StoreError) -> String {
    match e {
        chronicle::StoreError::Deserialization(_) => "Deserialization".into(),
        chronicle::StoreError::Serialization(_) => "Serialization".into(),
        chronicle::StoreError::Corruption(m) => format!("Corruption({})", m),
        other => format!("{:?}", other),
    }
}

fn fold(ops: Vec<StateOperation>) -> Result<Vec<u8>, String> {
    let mut state = Vec::new();
    for op in ops {
        state = apply_operation(state, op).map_err(|e| variant(&e))?;
    }
    Ok(state)
}

fn mat(ops: Vec<StateOperation>) -> Result<Vec<u8>, String> {
    materialize_operations(ops).map_err(|e| variant(&e))
}

fn show(r: &Result<Vec<u8>, String>) -> String {
    match r {
        Ok(b) => format!("Ok({})", String::from_utf8_lossy(b)),
        Err(e) => format!("Err({})", e),
    }
}

fn check(name: &str, ops: Vec<StateOperation>) {
    let a = fold(ops.clone());
    let b = mat(ops);
    assert_eq!(
        show(&a),
        show(&b),
        "DIVERGENCE in `{}`:\n  fold        = {}\n  materialize = {}",
        name,
        show(&a),
        show(&b)
    );
}

fn app(v: serde_json::Value) -> StateOperation {
    StateOperation::Append(serde_json::to_vec(&v).unwrap())
}
fn set(v: serde_json::Value) -> StateOperation {
    StateOperation::Set(serde_json::to_vec(&v).unwrap())
}
fn edit(i: usize, v: serde_json::Value) -> StateOperation {
    StateOperation::Edit {
        index: i,
        new_value: serde_json::to_vec(&v).unwrap(),
    }
}
fn entry(h: &str) -> TreeEntry {
    TreeEntry {
        blob_hash: h.to_string(),
        size: 1,
        mode: 0o644,
    }
}
fn tset(p: &str, h: &str) -> StateOperation {
    StateOperation::TreeSet {
        path: p.to_string(),
        entry: entry(h),
    }
}

#[test]
fn differential_happy_paths() {
    check("empty", vec![]);
    check("appends", vec![app(json!(1)), app(json!(2)), app(json!(3))]);
    check("set_then_append", vec![set(json!([9])), app(json!(1))]);
    check("set_scalar_then_nothing", vec![set(json!("hello"))]);
    check(
        "append_edit_redact",
        vec![
            app(json!("a")),
            app(json!("b")),
            app(json!("c")),
            edit(1, json!("B")),
            StateOperation::Redact { start: 0, end: 1 },
        ],
    );
    check(
        "redact_clamped",
        vec![app(json!(1)), StateOperation::Redact { start: 5, end: 99 }],
    );
    check(
        "redact_inverted",
        vec![app(json!(1)), app(json!(2)), StateOperation::Redact { start: 2, end: 1 }],
    );
    check(
        "delta_snapshot_chain",
        vec![
            StateOperation::DeltaSnapshot(serde_json::to_vec(&json!([1, 2])).unwrap()),
            StateOperation::DeltaSnapshot(serde_json::to_vec(&json!([3])).unwrap()),
            app(json!(4)),
        ],
    );
    check(
        "snapshot_then_delta_op",
        vec![
            StateOperation::Snapshot(serde_json::to_vec(&json!([1, 2, 3])).unwrap()),
            StateOperation::Delta {
                old_hash: chronicle::Hash::from_bytes(b"x"),
                new_value: serde_json::to_vec(&json!({"x": 1})).unwrap(),
            },
        ],
    );
    check(
        "tree_ops",
        vec![
            tset("a", "aa"),
            tset("b", "bb"),
            StateOperation::TreeRemove {
                path: "a".to_string(),
            },
            StateOperation::TreeBatch {
                ops: vec![TreeOp::Set {
                    path: "c".to_string(),
                    entry: entry("cc"),
                }],
            },
        ],
    );
}

#[test]
fn differential_error_and_mixed_paths() {
    check("edit_out_of_bounds", vec![app(json!(1)), edit(5, json!(2))]);
    check("edit_on_empty", vec![edit(0, json!(1))]);
    check("append_onto_scalar", vec![set(json!("scalar")), app(json!(1))]);
    check("append_onto_object", vec![set(json!({"a": 1})), app(json!(1))]);
    check("append_onto_null", vec![set(json!(null)), app(json!(1))]);
    check("tree_op_onto_array", vec![app(json!(1)), tset("a", "aa")]);
    check("array_op_onto_tree", vec![tset("a", "aa"), app(json!(1))]);
    check("redact_onto_scalar", vec![set(json!(3)), StateOperation::Redact { start: 0, end: 1 }]);
    check("tree_onto_scalar", vec![set(json!(3)), tset("a", "aa")]);
}

#[test]
fn differential_field_fallback() {
    check(
        "field_on_empty",
        vec![StateOperation::Field {
            name: "count".to_string(),
            operation: Box::new(StateOperation::Set(b"42".to_vec())),
        }],
    );
    check(
        "field_after_array_ops",
        vec![
            app(json!(1)),
            StateOperation::Field {
                name: "count".to_string(),
                operation: Box::new(StateOperation::Set(b"42".to_vec())),
            },
        ],
    );
    check(
        "set_object_then_field_then_field",
        vec![
            set(json!({"items": [1, 2], "meta": {}})),
            StateOperation::Field {
                name: "items".to_string(),
                operation: Box::new(app(json!(3))),
            },
            StateOperation::Field {
                name: "items".to_string(),
                operation: Box::new(app(json!(4))),
            },
        ],
    );
    check(
        "field_then_append",
        vec![
            StateOperation::Field {
                name: "count".to_string(),
                operation: Box::new(StateOperation::Set(b"42".to_vec())),
            },
            app(json!(1)),
        ],
    );
    check(
        "field_after_tree_ops",
        vec![
            tset("a", "aa"),
            StateOperation::Field {
                name: "count".to_string(),
                operation: Box::new(StateOperation::Set(b"42".to_vec())),
            },
        ],
    );
}

/// Deterministic pseudo-random op sequences (xorshift, no dev-dep needed).
#[test]
fn differential_fuzz() {
    let mut s: u64 = 0x2545F4914F6CDD1D;
    let mut next = move || {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        s
    };
    for case in 0..2000u32 {
        let len = (next() % 12) as usize;
        let mut ops = Vec::new();
        for k in 0..len {
            let pick = next() % 10;
            ops.push(match pick {
                0 => set(json!([k])),
                1 => set(json!({"o": k})),
                2 => app(json!({"i": k})),
                3 => edit((next() % 6) as usize, json!("E")),
                4 => StateOperation::Redact {
                    start: (next() % 6) as usize,
                    end: (next() % 6) as usize,
                },
                5 => StateOperation::DeltaSnapshot(serde_json::to_vec(&json!([k, k])).unwrap()),
                6 => tset(&format!("p{}", k), "hh"),
                7 => StateOperation::TreeRemove {
                    path: format!("p{}", next() % 4),
                },
                8 => StateOperation::Field {
                    name: "f".to_string(),
                    operation: Box::new(app(json!(k))),
                },
                _ => StateOperation::Snapshot(serde_json::to_vec(&json!([1, 2, 3])).unwrap()),
            });
        }
        check(&format!("fuzz_case_{}", case), ops);
    }
}
