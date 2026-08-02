//! Probe a (COPY of a) real store: quantify the point-lookup fast path
//! against full materialization on production-scale state.
//!
//!     cargo run --release --example store_probe -- /path/to/store-copy [state_id]
//!
//! Measures, on the given AppendLog state:
//!   1. get_state_item(last)  — cold caches → head-record fast path
//!   2. get_state(...)        — full materialization (the old per-append cost)
//!   3. get_state_item(last)  — warm items cache
//!   4. append + get_state_item(new last) — the production write-through
//!      pattern (THIS MUTATES THE STORE — only ever run on a copy)
//!
//! 2026-08-01: written to validate the Mythos turn-boundary-latency fixes
//! against a copy of the live store (114 MB messages snapshot, 6 GB log).

use chronicle::types::StateOperation;
use chronicle::{Store, StoreConfig};
use std::path::PathBuf;
use std::time::Instant;

fn main() {
    let mut args = std::env::args().skip(1);
    let path = PathBuf::from(args.next().expect("usage: store_probe <store-path> [state_id]"));
    let state_id = args.next().unwrap_or_else(|| "messages".to_string());

    let t = Instant::now();
    let store = Store::open(StoreConfig {
        path,
        blob_cache_size: 16,
        create_if_missing: false,
    })
    .expect("open store");
    println!("open: {:?}", t.elapsed());

    let len = store
        .get_state_len(&state_id)
        .expect("get_state_len")
        .unwrap_or(0);
    println!("state '{}': {} items", state_id, len);
    if len == 0 {
        println!("empty state, nothing to probe");
        return;
    }

    // 1. Cold point lookup of the last item (fast path when head is Append).
    let t = Instant::now();
    let item = store.get_state_item(&state_id, len - 1).expect("get_state_item");
    println!(
        "point lookup last (cold):  {:?}  ({} bytes)",
        t.elapsed(),
        item.as_ref().map(|i| i.len()).unwrap_or(0)
    );

    // 2. Full materialization — the cost every append used to pay via the
    //    write-through point lookup, and the cost any windowed read pays on
    //    a cache miss.
    let t = Instant::now();
    let state = store.get_state(&state_id).expect("get_state");
    println!(
        "full materialization:      {:?}  ({} bytes)",
        t.elapsed(),
        state.as_ref().map(|s| s.len()).unwrap_or(0)
    );

    // 3. Warm repeat of the point lookup.
    let t = Instant::now();
    let _ = store.get_state_item(&state_id, len - 1).expect("get_state_item");
    println!("point lookup last (warm):  {:?}", t.elapsed());

    // 4. Production write-through pattern: append, then point-lookup the
    //    new last item (caches just invalidated by the write).
    let t = Instant::now();
    let rec = store
        .append_to_state_json_with_identity(
            &state_id,
            serde_json::json!({
                "participant": "store-probe",
                "content": [{"type": "text", "text": "probe append (copy only)"}],
                "timestamp": 0
            }),
            "id",
            "sequence",
        )
        .expect("append");
    let append_t = t.elapsed();
    let t = Instant::now();
    let _ = store.get_state_item(&state_id, len).expect("get_state_item");
    println!(
        "append: {:?} (encoding {:?}) + point lookup after append: {:?}",
        append_t, rec.encoding, t.elapsed()
    );

    // 5. For contrast: what the append+lookup cost with the OLD behavior —
    //    force the materialization path by asking for a non-final index
    //    right after another append (caches invalid → full rebuild).
    let _ = store
        .append_to_state_json_with_identity(
            &state_id,
            serde_json::json!({"participant": "store-probe", "content": [], "timestamp": 0}),
            "id",
            "sequence",
        )
        .expect("append");
    let t = Instant::now();
    let _ = store.get_state_item(&state_id, 0).expect("get_state_item");
    println!("old-path equivalent (post-append materializing lookup): {:?}", t.elapsed());

    // Chain shape, for the record.
    let mut snapshot_note = String::new();
    if let Some(needed) = store.snapshot_needed(&state_id) {
        snapshot_note = format!(" (snapshot pending: {:?})", needed);
    }
    let _ = StateOperation::Redact { start: 0, end: 0 }; // keep the import honest
    println!("done{}", snapshot_note);
}
