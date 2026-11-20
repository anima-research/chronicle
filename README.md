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

Every method but `storeBlobAsync` is synchronous: it runs on the calling thread, and `pollSubscriptionTimeout` blocks it for up to its timeout. The package's `index.d.ts`, generated from `src/napi.rs` with its doc comments, lists every method and its options.

## What a store keeps

### Records

Each record has an ID, a sequence number, a type, a payload, a timestamp, and two lists of links to other records: `causedBy` for what caused it, and `linkedTo` for any other relation. `append` takes the payload as bytes and `appendJson` as JSON, and the `WithLinks` forms of each add the links. `getEffects(id)` lists the records whose `causedBy` names that record, and `getLinksTo(id)` those whose `linkedTo` does.

`getRecord` reads any record by ID, and `getRecords(ids)` reads several in one call, in the order given, with `null` for an ID that has no record. `getRecordIdsByType` lists a type's record IDs across all branches, in ascending ID order, so the last is the newest; the IDs are decimal strings, so compare them as numbers. The records a torn tail drops at open are gone, and their IDs are issued again to new records, possibly of another type. So a last ID taken before a crash can name a different record afterwards, and a count taken then can skip new ones. `query` reads the records the current branch has appended, by sequence range and type, at most `limit` of them (10,000 if you give none).

### States

A state is a named value whose every change is a record in the log. That's what lets you read it as it was at an earlier sequence (`getStateAt`, `getStateJsonAt`), and lets each branch change it separately. Register a state with one of three strategies:

- **`snapshot`**: each change stores the whole value (`setState`, `setStateJson`).
- **`append_log`**: a list you append to (`appendToState`, `appendToStateJson`), edit by index (`editStateItem`) and redact by range (`redactStateItems`). Changes are stored as operations. A run of appends gets periodic delta snapshots, so a read doesn't replay the whole history, and full snapshots come further apart as the list grows. `deltaSnapshotEvery` (100 by default) and `fullSnapshotEvery` (10) set the base intervals. `getStateLen`, `getStateSlice`, `getStateTail` and `getStateItemJson` read part of a list, and `getStateItemsJson(id, ordinals)` reads several items by index in one call.

  `appendManyToStateJson(id, items)` appends several items as one record, at one sequence, and returns that record with `firstOrdinal`, the index its first item took: the list's length just before the batch, as `getStateLen` counts it once the batch has counted the items (item `i` is at `firstOrdinal + i`; on a list whose value isn't a JSON array, or holds an item that isn't JSON, the batch takes the stored count, is recorded, and the state then fails to materialize, as after an `appendToState` to it, so the items can't be read at those indices). A count that chronicle 0.1.0 to 0.4.x left wrong is recounted at the list's first batch on each branch after an open, and by `compactState`. Reads see the items as if each were appended alone, except that a historical read or a branch cut by sequence keeps or drops the batch whole, as it has one sequence, and the batch counts as one operation toward the snapshot intervals. A crash keeps or loses a batch whole, never part of one. A store's first batch changes its format: see `MANIFEST` under [On disk](#on-disk).

  `appendToStateJsonWithIdentity(id, item, idField, sequenceField)` appends a JSON object with the id and sequence of the record it is written as at those two keys, so the item carries its own place in the log. `appendManyToStateJsonWithIdentity(id, items, idField, sequenceField)` is its batched form: every item of the batch carries the id and sequence of the batch's one record, so a branch cut at an item's sequence keeps the whole batch, and one at the sequence before drops it. It throws and writes nothing when `idField` and `sequenceField` name the same key; the single form takes such a pair and leaves the sequence at that key, over the id.
- **`tree`**: a map from paths to entries that name blobs (`treeSet`, `treeRemove`, `treeGet`, `treeList`, `treeDiff`, and others), with a delta snapshot every 50 operations by default.

A registration is kept in the store, so on a store opened again, `registerState` throws "State already exists", whose `code` is `StateExists`, for a state registered before; `listStates()` lists them.

`registerStateFieldIndex` keeps an index on one field of a list's items, for range and equality queries (`queryStateIndexRange`, `queryStateIndexEq`). It belongs to the branch it was registered on: a change to the state on another branch drops it, and the queries return `null` until it's registered again.

### Branches

`createBranch(name, from)` starts a branch at the head of `from`, or of the current branch when `from` is left out. `createBranchAt(name, from, sequence)` starts one at an earlier sequence of `from`. A new branch starts with its parent's states as they were at that point. Records and state changes go to the current branch, which `switchBranch` chooses.

### Blobs

`storeBlob` keeps bytes under their SHA-256 hash, so the same content is stored once, and `getBlob` reads them back. `getBlob` checks the file's checksum and hash, and throws for a file that isn't whole; storing the same content again replaces such a file, except one whose bytes this open store wrote, or read and checked whole while storing, and whose file still has the same length: that one is taken as stored until a `getBlob` of it fails or the store is reopened. A content type longer than 65,535 bytes is refused. An open store keeps the blobs it read most recently in memory (`blobCacheSize` of them, 1,000 by default) and answers from there, so damage done to a blob's file from outside the store shows only once the blob leaves that cache or the store is reopened. `storeBlobAsync` stores bytes without holding the calling thread: the open store's own thread, which its root handle and views share, takes them one at a time, in the order they're called, and the root's `close()` waits for those already called. With `chunked: true`, it stores content as chunks cut where its own bytes say, so content sharing long runs of bytes with earlier chunked content shares those chunks on disk. Each chunk is a file of its own, about 10 KiB on average, and `getBlob` reads each one the blob cache doesn't hold, so content sharing nothing with what's stored takes more files to store and to read than it would whole, and more disk, since each file takes whole blocks. Only `chunked: true` writes chunked blobs, and nothing else in a store changes with them. Releases from before `storeBlobAsync`, 0.6.0 included, read blobs stored whole as before, but can't read a chunked blob: their `getBlob` of it throws, naming an unsupported blob version. To go back, close the store and run `tools/unchunk-blobs.py <store> --apply` from chronicle's repository with Python 3: it rewrites each chunked blob whole under the same hash, and those releases then read it. It stops at a chunked blob whose list or a chunk it can't read, naming its file, and leaves that blob and those after it as they are.

### Subscriptions

`subscribe(config)` returns an ID. The config's `filter` names the record types and states to follow, and turns on each kind of event you want with `includeRecords`, `includeStateChanges` and `includeBranchEvents`, which are off unless set. It also takes a `branch`, which isn't applied yet: live events can arrive from any branch. Events go only to a subscription that has caught up, and `catchUpSubscription` is what lets live events through. With a `fromSequence` in the config, it first sends each followed state as it was then (or as it is now, if it didn't exist then) and replays the matching records appended since, both from the branch of the handle you call it on: the current branch on the root, its own branch on a view. A state larger than the config's `maxSnapshotBytes` (10 MiB by default) is cut to fit: a list sends only its newest items, and any other value sends `null`. The event's `data`, a JSON string, then has `truncated` set, and for a list, `from_index` and `total_length` say which part arrived. Changes between `subscribe` and `catchUpSubscription` arrive only as far as that replay covers them. After that, each `pollSubscription` returns the next event, or `null` when none is waiting, and `pollSubscriptionTimeout` waits up to a timeout for one. A subscriber that lets `bufferSize` events (1,000 by default) pile up unpolled is dropped, and so is one whose catch-up overruns its buffer, which `catchUpSubscription` throws as `SubscriptionDropped`. Once the events queued for a subscription that has ended are read, a poll throws `StoreClosed` after the store's `close()`, and otherwise `SubscriptionDropped`: the store dropped it, or it was unsubscribed through another handle, which also queues one last event, `dropped` with reason `unsubscribed`, when the buffer has room for it. Unsubscribed through the handle polling it, its queued events go with it, and the next poll throws `SubscriptionNotFound` at once. A catch-up of a subscription the store doesn't hold, dropped, unsubscribed or never made, throws `SubscriptionDropped`.

## On disk

A store is a directory. The log is `records.log`, and the state chains' heads are in `state.bin`, field indexes in `state-indexes.bin`, branches in `branches.bin`, and blobs under `blobs/`. `MANIFEST` names the format version: 1, which every release opens, until it moves to 2 just before the store's first batched append (`appendManyToStateJson` or `appendManyToStateJsonWithIdentity`, or `AppendMany` from Rust) is written. It stays 2 from then on, even if that batch then fails, and releases that predate batched appends refuse to open the store where the directory holding the `MANIFEST` (for a linked one, the one holding its target) is synced (on Windows, where this process may not open that directory, or on a system that doesn't sync directories, a power loss can undo the move to version 2 and keep the batch; see Breaking in the changelog); if the move itself fails, the call throws and the batch isn't recorded, and it throws too if only the directory sync after the move fails, with the `MANIFEST` already at version 2. `create` refuses a directory that already holds a store, so it never writes a store's `MANIFEST` back to 1: open a store that may exist with `openOrCreate` or `open`. `LOCK` is held while the store is open. `LOCK` must be a regular file, or absent: open and create refuse a symlink or anything else there, with `InvalidFormat`. Don't delete or replace `LOCK` while the store is open: the next open would lock a new file and open the store too.

- **One process at a time.** While a store is open, another `open` of it fails with "Store is locked by another process", whose `code` is `Locked`. With `LOCK` a regular file, a lock that fails for any other reason fails with that I/O error instead.
- **Durability.** The log is flushed to disk every 100 records, so a machine crash can lose up to the last 100. `sync()` and `close()` flush it and save `state.bin` and `branches.bin`, each written to a new file that then replaces the old one, so a crash during `sync()` leaves each file as it was before that save or after it. Registering a state, changing its strategy, and creating, deleting or switching a branch save them before returning. `storeBlob` writes a blob to a new file in `blobs/`, flushes it, and renames it into place before returning, so a crash leaves its file whole, or as it was before the call (absent, or the damaged copy it was replacing). When a process is killed or exits early without `close()` (a crash, a signal such as Ctrl-C, or `process.exit()`), the next open truncates a partly written last record, brings each branch's head up to the log, and replays the state changes the log holds since the last save, except those on a branch that save doesn't hold. `recovery()` reports what it found. Opening also removes the new files (`.chronicle-blob-*` in `blobs/`) of blob stores that were cut off before their rename. Once an fsync of the log fails, `sync()` throws an error beginning `Durability lost:`, whose `code` is `DurabilityLost`, every time until the store is reopened, and so do the calls above that save before returning, unless a check of their own refuses them first. Call `close()` before exiting, and `sync()` after changes you can't lose.
- **Looking inside.** Stores are binary. `tools/` has Python scripts that read a store's files directly. Only `compact-set-states.py` takes the `LOCK`, so close the store before running another one that writes.
  - `unchunk-blobs.py <store>` lists the store's chunked blobs, and with `--apply` after the path rewrites each one whole under the same hash, so a release from before `storeBlobAsync` can read it.
  - `trim-records-log.py <store>/records.log` reports a torn tail, and trims it with `--apply` after the path.
  - `scan-delta-poison.py` checks `append_log` states for poisoned delta snapshots, and `chain-report.py` reports the update chains of a state named `messages`. Both read state changes in either encoding: JSON, which releases before 0.3.0 wrote, and MessagePack, which 0.3.0 and later write. Each skips a record typed as a state change that doesn't decode as one, as chronicle does, and says so on stderr. Each stops with exit code 2 and no verdict at a record that fails its checksum, or at a kind of operation it doesn't read.
  - `rebuild-state-index.py` rewrites `state.bin` from the log of a store whose every state change is JSON. On a store with a MessagePack one, it writes nothing, exits 2 and says what holds instead.
  - `compact-set-states.py <store> --state <pattern>` frees the space in `records.log` that old values of the named states hold. A pattern is literal text in which each `*` stands for any text. Each update of a named state that the links from the heads reach must be a whole value, a `Set` or a `Snapshot`, in the named MessagePack form that chronicle writes. Of it the tool keeps the update that each head in `state.bin` names, and drops its other updates that the links from the heads reach. Every other record stays, and so does an old value of a named state that no head reaches, such as a deleted branch's; the dry run counts those. A state update that a head reaches along the links, for any state, then holds the new offset of the update it links to, or no link where that update was dropped. Every other record typed as a state change keeps its bytes, whatever it holds, so a deleted branch's leftover update can name an offset that holds another record afterwards. No read of a live state reaches it. `chain-report.py` reads every update's link, so it can show a wrong chain for such an update. Every count in a head stays as chronicle wrote it. `getStateLen` reads a head's item count, which still describes the value at that head. The other counts time a state's next snapshot, so a named state's next snapshot can come at another point. `listStates` returns one of them as `opsSinceSnapshot`, which for a named state can still count updates that the run dropped. It changes nothing unless `--apply` follows. What goes is the old values of each named state: a read of it at an older sequence can give nothing, and a branch made later at an older point can lack it. So name only a state whose old values nothing reads, such as agent-framework's `mcpl/coalescing` (anima-research/agent-framework#1028). A field index that a caller registered is gone after the run, and the caller registers it again. A record that stays and names a dropped update in `causedBy` or `linkedTo` still names it, and a read of that id gives nothing afterwards; the dry run counts such records. A query by record type with a limit can return more records afterwards. The offset of a record that stays can move, so a `stateHeadOffset` or a cursor kept by offset from before the run names another place. A client of agent-framework's API `subscribe` that resumes from a sequence it kept can get its branch's whole history again, where that sequence was a dropped change's. It refuses an open store, a store with no `LOCK`, a `MANIFEST` that names a version or feature it doesn't read, a named state that is not whole-value, a record typed as a state change that has a `state_id` and an operation, after the newest head on a live branch, which may be an update that `state.bin` doesn't have yet (from chronicle 0.5.0 an open of the store applies such an update when its link matches the head of its state, and refuses to open the store when it does not, and with 0.4.0, or for a record that chronicle doesn't decode, a write of any state and a close clear it), a torn log, a record that fails its checksum, a state change in the JSON or the positional form that would need a new link, and the files of an earlier run, and then changes nothing. It follows a linked `records.log`, `state.bin` or `state-indexes.bin` as chronicle does, and keeps each old file beside the file it replaced, as `records.log.pre-compact`, `state.bin.pre-compact` and `state-indexes.bin.pre-compact`, with the run's record, `state.bin.compact-run`. A run that stops at any point leaves a store that opens as before, or one that open refuses, or, after the last rename, the store that the run made. `--go-back` then undoes the run, and after a run that finished it puts the old files back while both are there and `records.log` and `state.bin` are as the run left them. It finds where the run is from the run's record. A field index registered since the run is gone after a going back, and so is the old one where its `.pre-compact` file is not there as the run left it, which `--go-back` says. It needs memory of a few hundred bytes for each record of the log, a few times the size of the largest record, and the size of `state-indexes.bin`, which it reads whole. It needs free space for the new log, and for the new `state.bin` and the run's record where `state.bin` is on another file system. On a host: rehearse the whole procedure on a copy of the store that can be thrown away; update the writer, stop the host, check the store with the chronicle release that the host uses, run the tool, check again and compare; if the checks differ, run `--go-back` before the host starts; start the host, and once it has run well delete the three `.pre-compact` files and `state.bin.compact-run`, at the paths that the run's report names. The check before the run opens the store, which saves `state.bin` and `branches.bin` and can cut off a torn tail, so the old files are from after that open. A newer chronicle release can apply updates at that open that the host's release doesn't, and it can write a `MANIFEST` that the host's release refuses, which the old files don't keep. An equal check is evidence for the reads that it lists, and by itself it doesn't make it safe to delete the old files.
  - `state-digests.mjs <store> [--state <pattern> ...]`, run with Node and `CHRONICLE_MODULE` set to a chronicle package's directory, prints digests of what chronicle's read gives on every branch, and the store's current branch, with no content, so two checks of a store compare with `diff`. Given the `--state` patterns of a compaction, it leaves out the older reads of the states they name, so the checks before and after a run that went right are equal. It reads one record at a time, and each branch through a view where the binding has one. It reads registered states only. It opens the store as a host does, which saves `state.bin` and `branches.bin` and cuts off a torn tail. It exits 1 when a read fails.

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
