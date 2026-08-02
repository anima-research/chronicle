//! Coverage for the two 2026-08-01 turn-boundary-latency fixes:
//!
//! 1. `StateUpdateRecord` is now stored as MessagePack (encoding-tagged),
//!    with legacy JSON records (encoding `Raw`/`Json`) decoding through the
//!    fallback arm. The old JSON wire format rendered every value byte as a
//!    JSON integer; these tests pin both the new format's efficiency and
//!    the old format's continued readability — including a HANDWRITTEN
//!    legacy payload, not just what today's serializer emits.
//!
//! 2. `get_state_item` serves the just-appended last item from the head
//!    record in O(1). These tests pin byte-equivalence with the
//!    materialization path and the fallbacks (edit-at-head, redact,
//!    middle-index, reopen).

use chronicle::types::{
    PayloadEncoding, Record, RecordId, Sequence, StateOperation, StateRegistration,
    StateStrategy, StateUpdateRecord, Timestamp,
};
use chronicle::{BranchId, Store, StoreConfig};
use serde_json::json;
use tempfile::TempDir;

fn open_store(dir: &TempDir) -> Store {
    Store::create(StoreConfig {
        path: dir.path().to_path_buf(),
        blob_cache_size: 16,
        create_if_missing: true,
    })
    .unwrap()
}

fn register_log(store: &Store, id: &str) {
    store
        .register_state(StateRegistration {
            id: id.to_string(),
            strategy: StateStrategy::AppendLog {
                delta_snapshot_every: 100,
                full_snapshot_every: 100,
            },
            initial_value: None,
        })
        .unwrap();
}

fn update(op: StateOperation) -> StateUpdateRecord {
    StateUpdateRecord {
        record_id: RecordId(7),
        global_sequence: Sequence(42),
        state_id: "messages".to_string(),
        prev_update_offset: Some(1234),
        operation: op,
        timestamp: Timestamp(1_700_000_000_000_000),
    }
}

fn record_with(payload: Vec<u8>, encoding: PayloadEncoding) -> Record {
    Record {
        id: RecordId(7),
        sequence: Sequence(42),
        branch: BranchId(1),
        timestamp: Timestamp(1_700_000_000_000_000),
        record_type: "state_update".to_string(),
        payload,
        encoding,
        caused_by: vec![],
        linked_to: vec![],
    }
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

/// MessagePack roundtrip preserves every field, and the encoding tag returned
/// by `encode` is what `decode` gates on.
#[test]
fn msgpack_roundtrip() {
    let orig = update(StateOperation::Append(b"{\"a\":1}".to_vec()));
    let (payload, encoding) = orig.encode().unwrap();
    assert_eq!(encoding, PayloadEncoding::MessagePack);

    let decoded = StateUpdateRecord::decode(&record_with(payload, encoding)).unwrap();
    assert_eq!(decoded.record_id, orig.record_id);
    assert_eq!(decoded.global_sequence, orig.global_sequence);
    assert_eq!(decoded.state_id, orig.state_id);
    assert_eq!(decoded.prev_update_offset, orig.prev_update_offset);
    assert_eq!(decoded.timestamp, orig.timestamp);
    match decoded.operation {
        StateOperation::Append(bytes) => assert_eq!(bytes, b"{\"a\":1}"),
        other => panic!("wrong op after roundtrip: {:?}", other),
    }
}

/// The point of the migration: value bytes are stored as raw `bin`, not
/// per-byte integers. A 64 KB payload must encode with only framing overhead
/// (JSON int arrays ran ~4x the raw size).
#[test]
fn msgpack_stores_bytes_as_bin() {
    let big = vec![b'x'; 64 * 1024];
    let raw_len = big.len();

    let (msgpack, _) = update(StateOperation::Snapshot(big.clone())).encode().unwrap();
    let json = serde_json::to_vec(&update(StateOperation::Snapshot(big))).unwrap();

    assert!(
        msgpack.len() < raw_len + 256,
        "msgpack should be ~raw size, got {} for {} raw bytes",
        msgpack.len(),
        raw_len
    );
    assert!(
        json.len() > raw_len * 3,
        "sanity: the JSON encoding this replaces really was int-array bloated \
         (got {} for {} raw bytes)",
        json.len(),
        raw_len
    );
}

/// Legacy records: JSON payloads written by pre-migration chronicle carry
/// encoding `Raw` (production wrote via `RecordInput::raw`) or `Json`. Both
/// must decode through the fallback arm.
#[test]
fn legacy_json_payload_decodes_under_raw_and_json_tags() {
    let orig = update(StateOperation::Append(b"{\"hello\":\"world\"}".to_vec()));
    let json_payload = serde_json::to_vec(&orig).unwrap();

    for encoding in [PayloadEncoding::Raw, PayloadEncoding::Json] {
        let decoded =
            StateUpdateRecord::decode(&record_with(json_payload.clone(), encoding)).unwrap();
        match decoded.operation {
            StateOperation::Append(bytes) => assert_eq!(bytes, b"{\"hello\":\"world\"}"),
            other => panic!("wrong op under {:?}: {:?}", encoding, other),
        }
    }
}

/// A handwritten pre-migration payload — the exact int-array wire shape old
/// stores contain — must parse under the serde_bytes-annotated fields. This
/// pins compatibility against the historical bytes themselves rather than
/// against whatever today's serializer happens to emit.
#[test]
fn handwritten_legacy_int_array_payload_decodes() {
    // "hi" = [104, 105]
    let legacy = br#"{
        "record_id": 7,
        "global_sequence": 42,
        "state_id": "messages",
        "prev_update_offset": 1234,
        "operation": { "Append": [104, 105] },
        "timestamp": 1700000000000000
    }"#;
    let decoded =
        StateUpdateRecord::decode(&record_with(legacy.to_vec(), PayloadEncoding::Raw)).unwrap();
    match decoded.operation {
        StateOperation::Append(bytes) => assert_eq!(bytes, b"hi"),
        other => panic!("wrong op: {:?}", other),
    }

    // Struct-variant field too (Edit.new_value).
    let legacy_edit = br#"{
        "record_id": 7,
        "global_sequence": 42,
        "state_id": "messages",
        "prev_update_offset": null,
        "operation": { "Edit": { "index": 3, "new_value": [104, 105] } },
        "timestamp": 1700000000000000
    }"#;
    let decoded =
        StateUpdateRecord::decode(&record_with(legacy_edit.to_vec(), PayloadEncoding::Raw))
            .unwrap();
    match decoded.operation {
        StateOperation::Edit { index, new_value } => {
            assert_eq!(index, 3);
            assert_eq!(new_value, b"hi");
        }
        other => panic!("wrong op: {:?}", other),
    }
}

/// The serde_bytes attribute must NOT change the JSON wire format: fixtures,
/// diagnostics, and any straggler JSON writer keep producing the int-array
/// shape old readers expect.
#[test]
fn json_wire_format_unchanged_by_serde_bytes() {
    let bytes = serde_json::to_vec(&StateOperation::Append(b"hi".to_vec())).unwrap();
    assert_eq!(&bytes, br#"{"Append":[104,105]}"#);
}

/// End-to-end through a real store on disk: writes are MessagePack-tagged,
/// and a cold reopen reconstructs from those records.
#[test]
fn store_writes_msgpack_and_reopens_cold() {
    let dir = TempDir::new().unwrap();
    {
        let store = open_store(&dir);
        register_log(&store, "log");
        for i in 0..10 {
            let rec = store
                .append_to_state_json_with_identity("log", json!({"n": i}), "id", "seq")
                .unwrap();
            assert_eq!(
                rec.encoding,
                PayloadEncoding::MessagePack,
                "state_update records must be written MessagePack-tagged"
            );
        }
    }
    // Cold reopen: reconstruction reads the msgpack records from disk.
    let store = Store::open(StoreConfig {
        path: dir.path().to_path_buf(),
        blob_cache_size: 16,
        create_if_missing: false,
    })
    .unwrap();
    let state = store.get_state("log").unwrap().unwrap();
    let arr: Vec<serde_json::Value> = serde_json::from_slice(&state).unwrap();
    assert_eq!(arr.len(), 10);
    assert_eq!(arr[9]["n"], 9);
}

// ---------------------------------------------------------------------------
// Point-lookup fast path
// ---------------------------------------------------------------------------

/// Split a materialized JSON array into raw item spans, the way
/// `get_state_items` does — the reference the fast path must match.
fn last_item_via_materialization(store: &Store, state_id: &str) -> Vec<u8> {
    let state = store.get_state(state_id).unwrap().unwrap();
    let raw: Vec<&serde_json::value::RawValue> = serde_json::from_slice(&state).unwrap();
    raw.last().unwrap().get().as_bytes().to_vec()
}

/// The fast path (cold caches, immediately after append — exactly the
/// context-manager write-through pattern) must be byte-identical to the
/// materialization path.
#[test]
fn fast_path_matches_materialization_bytes() {
    let dir = TempDir::new().unwrap();
    let store = open_store(&dir);
    register_log(&store, "log");

    for i in 0..25 {
        store
            .append_to_state_json_with_identity(
                "log",
                json!({"n": i, "zeta": "later-key", "alpha": [1, 2, 3]}),
                "id",
                "seq",
            )
            .unwrap();

        // Caches were just invalidated by the append: this lookup takes the
        // head-record fast path.
        let fast = store.get_state_item("log", i).unwrap().unwrap();
        let slow = last_item_via_materialization(&store, "log");
        assert_eq!(
            String::from_utf8_lossy(&fast),
            String::from_utf8_lossy(&slow),
            "fast path diverged from materialization at item {}",
            i
        );
    }
}

/// Non-final indices and out-of-range lookups behave exactly as before.
#[test]
fn middle_and_out_of_range_lookups() {
    let dir = TempDir::new().unwrap();
    let store = open_store(&dir);
    register_log(&store, "log");
    for i in 0..5 {
        store
            .append_to_state_json_with_identity("log", json!({"n": i}), "id", "seq")
            .unwrap();
    }

    let item2: serde_json::Value =
        serde_json::from_slice(&store.get_state_item("log", 2).unwrap().unwrap()).unwrap();
    assert_eq!(item2["n"], 2);
    assert!(store.get_state_item("log", 5).unwrap().is_none());
    assert!(store.get_state_item("missing", 0).unwrap().is_none());
}

/// When the head record is NOT an Append the fast path must fall through:
/// an Edit of the last item, then a lookup of that index, must see the
/// edited value.
#[test]
fn edit_at_head_falls_back_to_materialization() {
    let dir = TempDir::new().unwrap();
    let store = open_store(&dir);
    register_log(&store, "log");
    for i in 0..5 {
        store
            .append_to_state_json_with_identity("log", json!({"n": i}), "id", "seq")
            .unwrap();
    }
    store
        .update_state(
            "log",
            StateOperation::Edit {
                index: 4,
                new_value: serde_json::to_vec(&json!({"n": "edited"})).unwrap(),
            },
        )
        .unwrap();

    let item: serde_json::Value =
        serde_json::from_slice(&store.get_state_item("log", 4).unwrap().unwrap()).unwrap();
    assert_eq!(item["n"], "edited");
}

/// A Redact at the head shrinks the array; a lookup of the NEW last index
/// must not be served from the head record (which is the Redact itself).
#[test]
fn redact_at_head_falls_back() {
    let dir = TempDir::new().unwrap();
    let store = open_store(&dir);
    register_log(&store, "log");
    for i in 0..5 {
        store
            .append_to_state_json_with_identity("log", json!({"n": i}), "id", "seq")
            .unwrap();
    }
    store
        .update_state("log", StateOperation::Redact { start: 3, end: 5 })
        .unwrap();

    assert_eq!(store.get_state_len("log").unwrap(), Some(3));
    let item: serde_json::Value =
        serde_json::from_slice(&store.get_state_item("log", 2).unwrap().unwrap()).unwrap();
    assert_eq!(item["n"], 2);
    assert!(store.get_state_item("log", 3).unwrap().is_none());
}

/// Reopen cold, then point-lookup the last item: the fast path must work
/// against a head loaded from disk, not just one created this process.
#[test]
fn fast_path_after_cold_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let store = open_store(&dir);
        register_log(&store, "log");
        for i in 0..8 {
            store
                .append_to_state_json_with_identity("log", json!({"n": i}), "id", "seq")
                .unwrap();
        }
    }
    let store = Store::open(StoreConfig {
        path: dir.path().to_path_buf(),
        blob_cache_size: 16,
        create_if_missing: false,
    })
    .unwrap();
    let item: serde_json::Value =
        serde_json::from_slice(&store.get_state_item("log", 7).unwrap().unwrap()).unwrap();
    assert_eq!(item["n"], 7);
}

/// The warm-cache path (repeated reads with no intervening write) still
/// serves item lookups correctly.
#[test]
fn warm_cache_path_still_serves() {
    let dir = TempDir::new().unwrap();
    let store = open_store(&dir);
    register_log(&store, "log");
    for i in 0..5 {
        store
            .append_to_state_json_with_identity("log", json!({"n": i}), "id", "seq")
            .unwrap();
    }
    // First read materializes + caches; second read hits the items cache.
    for _ in 0..2 {
        let item: serde_json::Value =
            serde_json::from_slice(&store.get_state_item("log", 4).unwrap().unwrap()).unwrap();
        assert_eq!(item["n"], 4);
    }
}
