# chronicle

**chronicle is an embedded store built on an append-only log of records, with copy-on-write branches and time travel.** Named states are kept as changes in that log, so any of them can be read as it was at an earlier point, and each branch changes its own. Large or shared data goes in a content-addressed blob store. The core is a Rust library, and Node.js uses it through `@animalabs/chronicle`.

chronicle is the storage layer of Connectome: [agent-framework](https://github.com/anima-research/agent-framework), [context-manager](https://github.com/anima-research/context-manager) and [connectome-host](https://github.com/anima-research/connectome-host) keep their records and state in it.

## Install

```sh
npm install @animalabs/chronicle
```

The package includes prebuilt native modules for Linux (x64 and arm64, glibc), macOS (x64 and arm64) and Windows (x64). On another platform, build the module from source (see [Building and contributing](#building-and-contributing)).

## A first look

In a new directory where you've installed the package, save this as `example.mjs` and run `node example.mjs`.

```js
import { JsStore } from '@animalabs/chronicle';

// A new store: registerState and createBranch throw for a state or branch
// that already exists, so run this once, or delete ./my-store first.
const store = JsStore.openOrCreate({ path: './my-store' });

// Records: an append-only log. IDs are decimal strings.
const asked = store.appendJson('message', { from: 'user', text: 'Hello' });
const answered = store.appendJsonWithLinks('message', { from: 'agent', text: 'Hi!' }, { causedBy: [asked.id] });
store.getEffects(asked.id);                // [answered.id]

// States: named values kept as changes in the log.
store.registerState({ id: 'messages', strategy: 'append_log' });
store.appendToStateJson('messages', { text: 'Hello' });
const before = store.currentSequence();
store.appendToStateJson('messages', { text: 'Hi!' });
store.getStateJson('messages');            // [{ text: 'Hello' }, { text: 'Hi!' }]
store.getStateJsonAt('messages', before);  // [{ text: 'Hello' }]

// Branches: copy-on-write. A store starts with one branch, `main`.
store.createBranch('experiment');
store.switchBranch('experiment');
store.appendToStateJson('messages', { text: 'Only on experiment' });
store.switchBranch('main');
store.getStateJson('messages');            // still the two items

// Blobs: bytes stored under their SHA-256 hash.
const hash = store.storeBlob(Buffer.from('large data'), 'text/plain');
store.getBlob(hash);                       // the same bytes

store.close();
```

Every method is synchronous: it runs on the calling thread, and `pollSubscriptionTimeout` blocks it for up to its timeout. The package's `index.d.ts`, generated from `src/napi.rs` with its doc comments, lists every method and its options.

## What a store keeps

### Records

Each record has an ID, a sequence number, a type, a payload, a timestamp, and two lists of links to other records: `causedBy` for what caused it, and `linkedTo` for any other relation. `append` takes the payload as bytes and `appendJson` as JSON, and the `WithLinks` forms of each add the links. `getEffects(id)` lists the records whose `causedBy` names that record, and `getLinksTo(id)` those whose `linkedTo` does.

`getRecord` reads any record by ID, and `getRecords(ids)` reads several in one call, in the order given, with `null` for an ID that has no record. `getRecordIdsByType` lists a type's record IDs across all branches, in ascending ID order, so the last is the newest; the IDs are decimal strings, so compare them as numbers. The records a torn tail drops at open are gone, and their IDs are issued again to new records, possibly of another type. So a last ID taken before a crash can name a different record afterwards, and a count taken then can skip new ones. `query` reads the records the current branch has appended, by sequence range and type, at most `limit` of them (10,000 if you give none).

### States

A state is a named value whose every change is a record in the log. That's what lets you read it as it was at an earlier sequence (`getStateAt`, `getStateJsonAt`), and lets each branch change it separately. Register a state with one of three strategies:

- **`snapshot`**: each change stores the whole value (`setState`, `setStateJson`).
- **`append_log`**: a list you append to (`appendToState`, `appendToStateJson`), edit by index (`editStateItem`) and redact by range (`redactStateItems`). Changes are stored as operations. A run of appends gets periodic delta snapshots, so a read doesn't replay the whole history, and full snapshots come further apart as the list grows. `deltaSnapshotEvery` (100 by default) and `fullSnapshotEvery` (10) set the base intervals. `getStateLen`, `getStateSlice`, `getStateTail` and `getStateItemJson` read part of a list, and `getStateItemsJson(id, ordinals)` reads several items by index in one call.

  `appendManyToStateJson(id, items)` appends several items as one record, at one sequence, and returns that record with `firstOrdinal`, the index its first item took (item `i` is at `firstOrdinal + i`). Reads see the items as if each were appended alone, except that a historical read or a branch cut by sequence keeps or drops the batch whole, as it has one sequence, and the batch counts as one operation toward the snapshot intervals. A crash keeps or loses a batch whole, never part of one. A store's first batch changes its format: see `MANIFEST` under [On disk](#on-disk).

  `appendToStateJsonWithIdentity(id, item, idField, sequenceField)` appends a JSON object with the id and sequence of the record it is written as at those two keys, so the item carries its own place in the log. `appendManyToStateJsonWithIdentity(id, items, idField, sequenceField)` is its batched form: every item of the batch carries the id and sequence of the batch's one record, so a branch cut at an item's sequence keeps the whole batch, and one at the sequence before drops it.
- **`tree`**: a map from paths to entries that name blobs (`treeSet`, `treeRemove`, `treeGet`, `treeList`, `treeDiff`, and others), with a delta snapshot every 50 operations by default.

A registration is kept in the store, so on a store opened again, `registerState` throws "State already exists" for a state registered before; `listStates()` lists them.

`registerStateFieldIndex` keeps an index on one field of a list's items, for range and equality queries (`queryStateIndexRange`, `queryStateIndexEq`). It belongs to the branch it was registered on: a change to the state on another branch drops it, and the queries return `null` until it's registered again.

### Branches

`createBranch(name, from)` starts a branch at the head of `from`, or of the current branch when `from` is left out. `createBranchAt(name, from, sequence)` starts one at an earlier sequence of `from`. A new branch starts with its parent's states as they were at that point. Records and state changes go to the current branch, which `switchBranch` chooses.

### Blobs

`storeBlob` keeps bytes under their SHA-256 hash, so the same content is stored once, and `getBlob` reads them back. `getBlob` checks the file's checksum and hash, and throws for a file that isn't whole; storing the same content again replaces such a file. A content type longer than 65,535 bytes is refused. An open store keeps the blobs it wrote or read most recently in memory (`blobCacheSize` of them, 1,000 by default) and answers from there, so damage done to a blob's file from outside the store shows only once the blob leaves that cache or the store is reopened.

### Subscriptions

`subscribe(config)` returns an ID. The config's `filter` names the record types and states to follow, and turns on each kind of event you want with `includeRecords`, `includeStateChanges` and `includeBranchEvents`, which are off unless set. It also takes a `branch`, which isn't applied yet: live events can arrive from any branch. Events go only to a subscription that has caught up, and `catchUpSubscription` is what lets live events through. With a `fromSequence` in the config, it first sends each followed state as it was then (or as it is now, if it didn't exist then) and replays the matching records the current branch has appended since. A state larger than the config's `maxSnapshotBytes` (10 MiB by default) is cut to fit: a list sends only its newest items, and any other value sends `null`. The event's `data`, a JSON string, then has `truncated` set, and for a list, `from_index` and `total_length` say which part arrived. Changes between `subscribe` and `catchUpSubscription` arrive only as far as that replay covers them. After that, each `pollSubscription` returns the next event, or `null` when none is waiting, and `pollSubscriptionTimeout` waits up to a timeout for one. A subscriber that lets `bufferSize` events (1,000 by default) pile up unpolled is dropped.

## On disk

A store is a directory. The log is `records.log`, and the state chains' heads are in `state.bin`, field indexes in `state-indexes.bin`, branches in `branches.bin`, and blobs under `blobs/`. `MANIFEST` names the format version: 1, which every release opens, until it moves to 2 just before the store's first batched append (`appendManyToStateJson` or `appendManyToStateJsonWithIdentity`, or `AppendMany` from Rust) is written. It stays 2 from then on, even if that batch then fails, and releases that predate batched appends refuse to open the store; if the move itself fails, the call throws and the batch isn't recorded, and it throws too if only the directory sync after the move fails, with the `MANIFEST` already at version 2. `create` refuses a directory that already holds a store, so it never writes a store's `MANIFEST` back to 1: open a store that may exist with `openOrCreate` or `open`. `LOCK` is held while the store is open. `LOCK` must be a regular file, or absent: open and create refuse a symlink or anything else there. Don't delete or replace `LOCK` while the store is open: the next open would lock a new file and open the store too.

- **One process at a time.** While a store is open, another `open` of it fails with "Store is locked by another process". With `LOCK` a regular file, a lock that fails for any other reason fails with that I/O error instead.
- **Durability.** The log is flushed to disk every 100 records, so a machine crash can lose up to the last 100. `sync()` and `close()` flush it and save `state.bin` and `branches.bin`, each written to a new file that then replaces the old one, so a crash during `sync()` leaves each file as it was before that save or after it. Registering a state, changing its strategy, and creating, deleting or switching a branch save them before returning. `storeBlob` writes a blob to a new file in `blobs/`, flushes it, and renames it into place before returning, so a crash leaves the blob whole or absent. When a process is killed or exits early without `close()` (a crash, a signal such as Ctrl-C, or `process.exit()`), the next open truncates a partly written last record, brings each branch's head up to the log, and replays the state changes the log holds since the last save, except those on a branch that save doesn't hold. `recovery()` reports what it found. Opening also removes the new files (`.chronicle-blob-*` in `blobs/`) of blob stores that were cut off before their rename. Once an fsync of the log fails, `sync()` throws an error beginning `Durability lost:` every time until the store is reopened, and so do the calls above that save before returning. Call `close()` before exiting, and `sync()` after changes you can't lose.
- **Looking inside.** Stores are binary. `tools/` has Python scripts that read `records.log` directly, and none of them takes the `LOCK`, so close the store before running one that writes.
  - `trim-records-log.py <store>/records.log` reports a torn tail, and trims it with `--apply` after the path.
  - `scan-delta-poison.py` checks `append_log` states for poisoned delta snapshots, `rebuild-state-index.py` rewrites `state.bin` from the log, and `chain-report.py` reports the update chains of a state named `messages`. These three read state changes as JSON, the encoding before 0.3.0, and stop with an error at one written since, which is MessagePack.

  `server/` is an Express middleware, with the Vue app in `ui/`, for browsing a store. Neither is in the npm package.

## From Rust

The library is the `chronicle` crate in `src/`. It isn't on crates.io, where `chronicle` is another project, so depend on it from Git:

```toml
[dependencies]
chronicle = { git = "https://github.com/anima-research/chronicle" }
serde_json = "1"
```

```rust
use chronicle::{RecordInput, Store, StoreConfig};
use serde_json::json;

fn main() -> chronicle::Result<()> {
    let store = Store::open_or_create(StoreConfig {
        path: "./my-store".into(),
        ..Default::default()
    })?;
    let record = store.append(RecordInput::json("message", &json!({ "text": "Hello" }))?)?;
    println!("record {} at sequence {}", record.id.0, record.sequence.0);
    Ok(())
}
```

`examples/store_probe.rs`, which measures state reads on a copy of a real store, also uses the crate directly. The Node binding is `src/napi.rs`, built with the `napi-bindings` feature.

## Building and contributing

[CONTRIBUTING.md](CONTRIBUTING.md) covers building and testing (`cargo test`, the native module's build, and `npm test`), what a pull request should contain, changelog fragments in `changelog.d/`, and releases.

## Documentation

- [docs/loom-of-looms.md](docs/loom-of-looms.md): the algebraic specification of branching logs, and of logs nested in logs.
- [docs/rfc-state-item-origins.md](docs/rfc-state-item-origins.md): a draft RFC on recording which record produced each item of an `append_log` state.
- [CHANGELOG.md](CHANGELOG.md): what each release changed.

## License

MIT, as `package.json` and `Cargo.toml` declare.
