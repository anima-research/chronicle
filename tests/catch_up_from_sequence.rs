//! A catch-up replays the records its branch has at or after its
//! `from_sequence`, and no others, wherever that sequence falls.

use chronicle::{
    BranchId, RecordInput, Sequence, Store, StoreConfig, StoreEvent, SubscriptionConfig,
    SubscriptionFilter,
};
use serde_json::json;
use tempfile::TempDir;

/// A store whose main branch holds five records, with their sequences.
fn store_with_five(dir: &TempDir) -> (Store, BranchId, Vec<u64>) {
    let store = Store::create(StoreConfig {
        path: dir.path().join("store"),
        blob_cache_size: 100,
        create_if_missing: true,
    })
    .unwrap();
    let main = store.current_branch().id;
    let sequences = (0..5).map(|i| append(&store, main, i)).collect();
    (store, main, sequences)
}

fn append(store: &Store, branch: BranchId, i: u64) -> u64 {
    store
        .append_on(branch, RecordInput::json("note", &json!({ "i": i })).unwrap())
        .unwrap()
        .sequence
        .0
}

/// The sequences a catch-up from `from` replays on `branch`, or its error.
fn replayed(store: &Store, branch: BranchId, from: u64, buffer_size: usize) -> Result<Vec<u64>, String> {
    let handle = store.subscribe(SubscriptionConfig {
        filter: SubscriptionFilter::records(),
        from_sequence: Some(Sequence(from)),
        buffer_size,
        ..Default::default()
    });
    let result = store.catch_up_subscription_on(branch, handle.id);
    let mut seen = Vec::new();
    while let Ok(event) = handle.try_recv() {
        if let StoreEvent::Record { record } = event {
            assert_eq!(record.branch, branch);
            seen.push(record.sequence.0);
        }
    }
    store.unsubscribe(handle.id);
    result.map(|()| seen).map_err(|error| error.to_string())
}

#[test]
fn from_a_sequence_the_branch_holds_it_replays_that_record_and_those_after() {
    let dir = TempDir::new().unwrap();
    let (store, main, seqs) = store_with_five(&dir);
    assert_eq!(replayed(&store, main, seqs[2], 1000), Ok(seqs[2..].to_vec()));
}

#[test]
fn from_past_the_head_it_replays_nothing() {
    let dir = TempDir::new().unwrap();
    let (store, main, seqs) = store_with_five(&dir);
    assert_eq!(replayed(&store, main, seqs[4] + 1, 1000), Ok(vec![]));
}

#[test]
fn from_past_the_head_a_buffer_smaller_than_the_branch_is_not_overrun() {
    let dir = TempDir::new().unwrap();
    let (store, main, seqs) = store_with_five(&dir);
    assert_eq!(replayed(&store, main, seqs[4] + 1, 3), Ok(vec![]));
}

#[test]
fn on_another_branch_from_a_sequence_both_branches_hold_it_replays_its_own_from_there() {
    let dir = TempDir::new().unwrap();
    let (store, main, _seqs) = store_with_five(&dir);
    let side = store.create_branch("side", None).unwrap().id;
    let on_side: Vec<u64> = (0..2).map(|i| append(&store, side, 10 + i)).collect();
    let on_main: Vec<u64> = (0..3).map(|i| append(&store, main, 20 + i)).collect();
    assert!(on_main.contains(&on_side[1]));
    assert_eq!(replayed(&store, side, on_side[1], 1000), Ok(vec![on_side[1]]));
    assert_eq!(replayed(&store, side, on_side[1] + 1, 1000), Ok(vec![]));
}

#[test]
fn on_another_branch_from_before_its_first_record_it_replays_all_of_its_own() {
    let dir = TempDir::new().unwrap();
    let (store, _main, seqs) = store_with_five(&dir);
    let side = store.create_branch("side", None).unwrap().id;
    let on_side: Vec<u64> = (0..2).map(|i| append(&store, side, 10 + i)).collect();
    assert_eq!(replayed(&store, side, seqs[1], 1000), Ok(on_side));
}
