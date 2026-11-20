# Changelog

Notable changes to `@animalabs/chronicle`, loosely following
[Keep a Changelog](https://keepachangelog.com/). Entries land with the change
that causes them, as fragment files in [`changelog.d/`](changelog.d/) that are
folded into a version section at release time — see
[CONTRIBUTING.md](CONTRIBUTING.md#changelog).

Releases up to and including 0.2.7 predate this file; for their contents see
`git log` and the
[releases page](https://github.com/anima-research/chronicle/releases).

## Unreleased

## 0.7.0 — 2026-11-09

### Breaking

- **Running an earlier chronicle on a store with chunked blobs:** releases
  before this one open such a store and read its blobs stored whole, but
  can't read a chunked one. In 0.5.0, 0.5.1 and 0.6.0, `getBlob` of it throws
  `Invalid store format: Unsupported blob version 2 in <file>: …`, and so
  does `storeBlob` of the same content, which leaves the chunked file as it
  is. In 0.4.0 and earlier, `getBlob` of it throws
  `Invalid store format: Unsupported blob version: 2`, and `storeBlob` of
  the same content returns the hash and leaves the chunked file as it is.
  Either way the blob stays unreadable there. **Who needs to act:** a host
  that stores blobs with `storeBlobAsync`'s `chunked: true` and may run an
  earlier chronicle on the same store. **Migration:** before running an
  earlier release on such a store, close it and run
  `tools/unchunk-blobs.py <store> --apply` from chronicle's repository
  with Python 3 (the npm package doesn't carry it), which rewrites each
  chunked blob whole under the same hash, so earlier releases read it;
  without `--apply` it lists them. A blob it stops at because its list or
  a chunk can't be read, such as one with a damaged chunk, can't be read by
  this release either. With the store open in this release, store its
  content again with `storeBlob`, which writes it
  whole. If that throws, as it does for a chunk in a newer format or one
  this account can't read, delete the blob's file, then store the content;
  without the content, only delete the file. Then close the store and run
  the script again. **Unchanged:** only `storeBlobAsync` with
  `chunked: true` writes a chunked blob, and nothing else in a store
  changes with it: blobs stored whole and the store's other files keep the
  formats earlier releases read. Opening a store reads no blob.

- **Hosts that check a chronicle error's `code`, or match its message:**
  every error the Node binding raised for chronicle carried napi's
  `GenericFailure` as `code`. It now carries the error's `ErrorCode`
  instead, so a check for `GenericFailure` no longer matches them. The four
  errors of the new `storeBlobAsync` that Added names carry no `ErrorCode`.
  Two kinds of error also change their message:
  - A poll of a subscription that has ended, once the events queued for it
    are read, throws `StoreClosed` (`Store has been closed`) after the
    root's `close()`, and otherwise `SubscriptionDropped`
    (`Subscription was dropped`), instead of `Subscription disconnected`:
    the store dropped it because its buffer overran, or it was
    unsubscribed through another handle.
  - Deleting the main branch or the current branch, or a view's deleting the
    branch it is pinned to, now throws `InvalidOperation` (in Rust,
    `StoreError::InvalidOperation`), whose message begins
    `Invalid operation:`, where it was `BranchNotFound` and began
    `Branch not found:`. In Rust, so does
    `BranchManager::delete_branch_with_reparent` naming a branch to reparent
    onto that doesn't exist, and `BranchManager::gc`, which calls it, lists
    that refusal's new message among its errors.

  **Who needs to act:** a host that compares `error.code` with
  `GenericFailure`, or that matches `Subscription disconnected`, or
  `Branch not found:` on one of those deletions; and Rust code that matches
  `StoreError::BranchNotFound` there. **Migration:** compare `error.code`
  with `ErrorCode`'s members (Added): an ended subscription's poll gives
  `SubscriptionDropped` or `StoreClosed`, and those deletions give
  `InvalidOperation`. **Unchanged:** nothing on disk; a store opens and
  reads as before.

### Added

- `storeBlobAsync(content, contentType, options)` stores a blob without
  holding the calling thread. It resolves with the blob's hash once it's
  stored, and rejects with the error that stopped it. Without `chunked`,
  it stores as `storeBlob` does, and rejects with the error `storeBlob`
  would throw. The bytes are copied before it returns, so the buffer can be
  reused at once. The open store's own thread, which its root handle and
  views share, takes these stores one at a time, in the order they're called
  on any of them; a `storeBlob` meanwhile isn't ordered with them. The
  root's `close()` waits for every one already called, holding the calling
  thread while it does, and a store whose root and views are all dropped
  without `close()` finishes them before it releases its lock.
- `storeBlobAsync` with `chunked: true` cuts content where its own bytes
  say, into chunks of at most 64 KiB, each at least 2 KiB but the last, and
  stores it as chunks wherever that cut makes more than one, which any
  content over 2 KiB can: every chunk a blob of its own, under a file at the
  whole content's hash that names them in order and is written only once
  every chunk is stored and each one this store wrote is synced. Where this
  store writes a chunk, its content type is `application/octet-stream`,
  which Rust's `Store::get_blob` returns for it (the Node API's `getBlob`
  returns bytes only). Like any blob already stored, a chunk that reads back
  whole is left as it is, content type included, whichever store of the
  same bytes finds it. Content sharing long runs of bytes with earlier
  chunked content shares those chunks, so storing each request of a growing
  conversation writes mostly what's new. `getBlob` returns the
  content, and its hash is still the content's SHA-256. `stats()` counts
  each chunk, and the file naming them, as a blob, and Rust's
  `BlobStorage::list` lists them. A store that finds a blob already at its
  hash in the other form leaves it when it reads back as the content.

- **Error codes:** an error the Node binding raises for a store failure, or for
  a call it refuses, now carries `code`, one of the values of the new
  `ErrorCode` enum, such as `BranchNotFound`, `Locked` or `StoreClosed`. A
  failed fsync of the record log gives `DurabilityLost`, which `sync()` then
  throws until the store is reopened, and so do the calls that checkpoint,
  unless a check of their own refuses them first. Messages
  are unchanged, except the ones Breaking and Changed name. Branch on
  `error.code` rather than on message text: compare it with `ErrorCode`'s
  members, whose names are kept across releases. An error napi raises
  while converting a call's arguments, before the call reaches chronicle,
  keeps napi's own code
  (`StringExpected`, `InvalidArg`) or has none. The four errors of
  `storeBlobAsync` that are neither a store failure nor a refusal, which
  `ErrorCode`'s docs name, carry no chronicle code: three carry napi's
  `GenericFailure`, and when it can't make its promise, `storeBlobAsync`
  throws the error napi raises.

### Changed

- `storeBlob` and `storeBlobAsync` no longer put the bytes they store in the
  blob cache, which now holds what `getBlob` reads, so storing large blobs
  no longer holds up to `blobCacheSize` of them in memory. A `getBlob` of a
  blob stored but not read yet reads its file. A store of bytes the open
  store has already stored whole, or read and checked whole while storing,
  still returns without reading their file again while a regular file
  of the same length is at their name, until a read of that file fails. A
  `getBlob` records nothing of the kind, so storing bytes only `getBlob` has
  read reads their file once. A check of such a file that races a store
  replacing it on another thread can drop the entry that store has just
  made, until the store that checked reads the file and records it again;
  a store of the same bytes in between reads the file once. A chunked
  blob's own file, which names its chunks, is read each time the same
  content is stored again: by a chunked store, with its chunks
  taken the same way, and by `storeBlob`, with its chunks read. `getBlob` of
  a chunked blob caches its chunks, not the whole.
- A store whose sync of the directories holding a blob's file fails now
  names that file as not written (`cannot sync the directories holding
  <dir>: <error>; <file> wasn't written, and storing the blob again tries
  the sync again`), where it said `nothing was stored`: a chunked store
  whose sync fails at a later chunk has stored the chunks before it.

### Fixed

- A catch-up that overruns the subscription's buffer (`catchUpSubscription`,
  and in Rust `Store::catch_up_subscription` and `catch_up_subscription_on`)
  now drops the subscription, as overrunning it with live events does. When
  the overrun fell on a replayed event, it used to fail with
  `Subscription was dropped` while the subscription stayed registered, and
  a poll of it returned `null`; now a poll throws `SubscriptionDropped`
  once the events queued for it are read. In
  Rust, `SubscriptionManager::send_to` now removes a subscription that can't
  take the event, and `Store::mark_subscription_caught_up` and
  `SubscriptionManager::mark_caught_up` fail with `SubscriptionDropped` for
  one that's gone, where they returned `Ok`.
- A handle, the root or a view, dropped without `close()` now gives up its
  subscriptions when it's garbage-collected. A subscription it had made and
  not caught up used to stay in the store, counted by `subscriptionCount()`,
  until the store closed.

- `Store::open` and `Store::open_or_create` (and `JsStore.open` and
  `openOrCreate`) on a store whose `MANIFEST` isn't a regular file now fail
  with `InvalidFormat`, naming the file, without opening it. On a FIFO they
  used to wait for a writer, though 0.6.0's notes said they fail there; on a
  directory they failed with an I/O error (#34).

## 0.6.0 — 2026-11-07

### Breaking

- **Hosts that open a store on an existing directory that holds no store:**
  with `create_if_missing`, which `JsStore.openOrCreate` always sets,
  `Store::open_or_create` now creates a store in a directory that's empty or
  holds nothing but a fresh volume's root entries, Finder's `.DS_Store`, or
  what a create left when it stopped before its manifest (see Fixed), where
  it used to fail. So a store started before its volume mounts (an unmounted
  mount point is an empty directory) now creates a fresh empty store that the
  volume hides once it mounts, and a pre-flight check that opens a path to see
  whether it's a store now makes one. **To act:** if you relied on that
  failure, check the mount or the directory before opening. connectome-host's
  store audit is one such caller. **Unchanged:** existing stores keep opening,
  and a missing directory is still created (#34).

- **On-disk format (stores that record a batched append):** a state update
  can now be a `StateOperation::AppendMany`, several items appended to an
  append-log state as one record (see Added). Earlier releases can't decode
  it, so the first `AppendMany` a store records rewrites its `MANIFEST` from
  format version 1 to version 2, naming the feature, before the record is
  written. An earlier release refuses a version 2 store at open with
  "Unsupported store version: 2". Without the mark, what an earlier release
  does depends on the release and on where the batch lies. Past the last
  checkpoint, 0.5.0 and 0.5.1 replay the log's state updates and skip the
  batch, which they can't decode; then, at the next update in that state's
  chain on that branch that isn't itself a batch, they refuse to open the
  store as corrupt ("cannot replay state ... does not match
  checkpoint/replayed head"). When only batches follow (the batch, or a run
  of them, is the newest), they open the store, and the state lacks all
  their items. The only sign is one `tracing` warning at each open,
  counting the state updates it skipped, which reaches only a host that
  installs a `tracing` subscriber; their Node binding installs none.
  Releases before 0.5.0 don't replay updates past the last checkpoint at
  all: they open the store without a word, and the state is what the
  checkpoint holds, without the batch or any other update to it past the
  checkpoint. Before the checkpoint, on 0.3.0 to 0.5.1, a read of that
  state that reaches the batch fails with "unknown variant `AppendMany`";
  releases before 0.3.0 can't read the MessagePack state updates 0.3.0 and
  later write at all.
  An open that replays a batch the `MANIFEST` doesn't name, as when a power
  loss undid the rewrite, names it again, but it sees only the batches past
  the last checkpoint: a mark lost once they are behind it stays lost.
  `Store::create` (and `JsStore.create`) now refuses a directory that
  already holds a store, and takes the lock before it writes anything (see
  Fixed), so it can't drop the mark. Before that fix, a create aimed at a
  version 2 store rewrote its `MANIFEST` to version 1, even when it then
  failed because the store was open, and an earlier release's `create`
  still does.
  **Who needs to act:**
  - **Hosts that may be rolled back:** anyone who may roll a host back to an
    earlier chronicle after the host has written a batch, from Rust or with
    the Node API's `appendManyToStateJson` or
    `appendManyToStateJsonWithIdentity`. context-manager's presentation
    index is meant to be its first writer. A host rolled back must not call
    `create` on a store it means to open. Where the directory holding the
    `MANIFEST` (for a linked one, the one holding its target) isn't synced
    (this process may not open it, the system doesn't sync
    directories, or the host runs Windows, where chronicle syncs none), a
    power loss can undo the `MANIFEST`'s move to version 2 and keep the
    batch's record, as it can undo a checkpoint save; an earlier release
    then meets the batch in a store without the mark, as described above.
  - **State-delta subscribers:** a batch reaches a subscription as one
    `state_delta` event, whose `operation` is an `AppendMany` holding all of
    its items (see Added). A subscriber that keeps its own copy of an
    append-log state from `Append` events must take each item of an
    `AppendMany` too, in order, as if each were an `Append`, or it misses
    every item a batch writes. That holds for a subscriber that never calls
    the batch APIs itself, once any writer to the store does. chronicle's
    dev UI was one: its append-log viewer took only `Append`, and in this
    release it takes the items of an `AppendMany` as well.

  **Migration:** none going forward. Going back, there is none for a store
  that holds a batch: keep a copy of the store from before the upgrade if a
  rollback must stay possible. A first batch whose rewritten `MANIFEST` is in
  place but whose record never is, because of a crash, an error syncing the
  `MANIFEST`'s directory or an error writing the record (a full disk, say),
  can leave a version 2 store that holds no batch, which earlier releases
  refuse all the same. This release refuses a version 2 `MANIFEST` naming a
  feature it doesn't know, as written by a later one.
  **Unchanged:** a store never asked to record a batch keeps its version 1
  `MANIFEST` and opens on every earlier release, as before, and existing
  stores open on this one unchanged. The record framing and the encodings of
  `state.bin`, `branches.bin` and `state-indexes.bin` are unchanged.
- **Rust embedders:** `StateOperation` has a new variant, `AppendMany`, for
  several items appended to an append-log state as one record (see Added).
  An exhaustive `match` on `StateOperation`, including one on the
  `operation` of a `StoreEvent::StateDelta`, needs an arm for it, even in a
  host that never writes a batch. **Unchanged:** the existing variants,
  their fields and their encoded names (see the MessagePack pin in
  `tests/state_update_encoding.rs`); only the added arm is needed.

### Added

- `appendManyToStateJsonWithIdentity(stateId, items, idField, sequenceField)`
  in the Node API is the batched form of `appendToStateJsonWithIdentity`, as
  `appendManyToStateJson` is of `appendToStateJson`. It appends several JSON
  objects to an append-log state as one record, at one sequence (an
  `AppendMany`), and returns `{ record, firstOrdinal }`. Before the record is
  written, each item gets that record's id (a string, as `JsRecord.id`) at
  `idField` and its sequence (a number) at `sequenceField`, overwriting what
  was there, as the single form stamps its one item. The batch is one
  record, so every item of it carries the same id and sequence, and a
  branch cut by an item's sequence keeps or drops the whole batch. Reads see
  the stamped items as if each had been appended alone, and field indexes
  take each at its own ordinal. An empty `items`, an item that isn't an
  object, or an `idField` equal to `sequenceField` (an item holds one value
  at a key, so it couldn't carry both the record id and the sequence)
  throws and writes nothing, and leaves the `MANIFEST` alone. (The single form takes equal
  names, and leaves the sequence at that key, over the id.) Once the batch
  is recorded the call returns it: a failure of the snapshot the write
  takes after its record goes into the store's snapshot-failure report
  (`snapshotFailures()`), not thrown, and the next write to the state
  takes the snapshot if one is still due (see Fixed, #39). Like any
  batch, the first one a store records moves its `MANIFEST` to version 2;
  a store never asked to record a batch keeps version 1 (a batch that
  fails after its mark still leaves version 2; see Breaking). In Rust:
  `Store::append_many_to_state_json_with_identity(_on)`, which returns an
  `AppendedBatch` and refuses those with `InvalidOperation`.

- `StateOperation::AppendMany(Vec<Vec<u8>>)` (Rust): appends several items to
  an append-log state as one record, at one sequence, through
  `update_state`. These reads see what the same items appended one `Append`
  at a time would give: the value, `get_state_len`, tails, slices, point
  lookups (except the last item on a value that doesn't materialize; see
  `get_state_item`), iteration, delta snapshots, field indexes (each item
  at its own ordinal), and replay after a crash, which keeps or drops a batch whole. A
  historical read or a fork cut by sequence keeps or drops it whole too,
  since it has one sequence. It counts as one operation toward the
  snapshot cadence, as a `TreeBatch` does, and is append-only, so delta
  snapshots go on as for single appends. The per-item cache advances through
  a batch without re-materializing, and the head-record fast path serves its
  last item. `update_state` refuses an empty batch with `InvalidOperation`.
  A state-delta event, and `getStateUpdateJson`, carry it as
  `{"AppendMany": [[...], ...]}`, each item an integer array as an `Append`'s
  is. Storing one changes the store's format; see Breaking.

- Batched calls in the Node API, each one call where the single forms take
  one per item:
  - `appendManyToStateJson(stateId, items)` appends several JSON items to an
    append-log state as one record, at one sequence (an `AppendMany`), and
    returns `{ record, firstOrdinal }`: the record, and the index the first
    item took, so item `i` is at `firstOrdinal + i` without a read. That
    index is the state's length just before the batch, as `getStateLen`
    counts it once the batch has counted the items, taken under the write
    lock. Reads see the items as if each
    were appended with `appendToStateJson`, except that a historical read or
    a branch cut by sequence keeps or drops the batch whole, as it has one
    sequence. An empty `items` throws and writes nothing. The first batch a
    store records moves its `MANIFEST` to version 2 (see Breaking).
    **Migration:** a store last written by chronicle 0.1.0 to 0.4.x can hold
    an append-log state whose stored count is wrong (see 0.5.0's Fixed): one
    whose chain holds, from those builds, a `Redact` running past the end, a
    `Delta` that replaced the value, or an empty `Set` or `Delta`. Such a
    count stays as it is until a full snapshot, or an array or empty
    `setState`, sets it again. For each state the store already held when
    it was opened, and each a `createBranchAt` branch starts with, this
    release counts the items at the state's first batch on each branch
    (`appendManyToStateJson`, `appendManyToStateJsonWithIdentity`, or
    `Store::append_many_to_state` and its forms in Rust), reading the
    state's whole value once, before the batch takes the write lock, so
    other writers wait only for each record it reads, not the whole count;
    if another write to the state lands during the count, the batch counts
    again under the write lock. That batch's `firstOrdinal`,
    `getStateLen` after it, and the index a later `editStateItem` is
    checked against then follow the state's items. A value that isn't a
    JSON array, or holds an item that isn't JSON, has no items to count:
    the batch takes the stored count, is recorded, and the state then fails
    to materialize, as after an `appendToState` to it, so the items can't be
    read at those indices. A record of the state's chain that
    fails to read (an I/O error, a failed checksum, a record whose framing
    is bad) fails the batch with that error, writing nothing; the error
    says the batch was not recorded, except a failed checksum's, which
    carries no message of its own. In Rust, a
    `DeltaSnapshot` a caller writes through `update_state` leaves the count
    as it was, and the next batch counts the items again the same way.
    `compactState` before the batch takes a full snapshot, which recounts
    them too.
  - `getStateItemsJson(stateId, ordinals)` returns, for each index in the
    order given, what `getStateItemJson` returns for it (except at the last
    index of a head whose count this session hasn't found exact (unchecked, or
    checked on a value that doesn't materialize), where `getStateItemJson`
    answers from the stored count and this from the value): `null` for an
    index that is negative or out of range, for a state with no value, and for
    a state whose value isn't an array (see Changed). Indices may repeat and
    come in any order. All the answers come from one version of the state,
    which a cold cache materializes at most once for the call, not once per
    index. A request with no index of zero or more (an empty one, or one of
    negative indices only) reads nothing. An index past the end costs what it
    costs `getStateItemJson`: on a cold cache, the one materialization that
    finds the end.
  - `getRecords(ids)` returns, for each ID in the order given, what
    `getRecord` returns for it, or `null` for an ID with no record. The IDs
    are looked up together and the records read under one hold of the log
    file, so a long list holds other reads and appends of the store until
    it is read; split it to let them in. A malformed ID throws before
    anything is read.
  - A batch reaches subscribers as one `state_delta` event, whose
    `operation` is `{"AppendMany": [[...], ...]}` with each item an integer
    array, as an `Append`'s item is. The `JsStoreEvent.data` doc now says so.
- Rust: `Store::append_many_to_state(_on)` and
  `append_many_to_state_json(_on)` return an `AppendedBatch { record,
  first_ordinal }`; `Store::get_state_items_by_ordinal(_on)` and
  `StateManager::get_state_items_by_ordinal` read several items of a state
  in one call; `Store::get_records` reads several records by ID, through
  `RecordIndex::get_offsets_by_id` and `RecordLog::read_many_at`.

- `snapshotFailures()` (`Store::snapshot_failures`) lists each state whose
  snapshot after a recorded write failed at its most recent attempt, per
  branch: the state, the branch, how many writes in a row failed to take
  it, the sequence of the latest of them, and its error. The writes are
  counted in sequence order: where writes to the state race, a failure that
  comes in after a later write's outcome isn't counted, so the count can be
  lower than the number that failed. A write whose own
  snapshot fails still returns its record (#39), and the store logs a
  `tracing` warning, which a Node process doesn't show. An entry goes once
  a snapshot of the state on that branch is recorded or a write finds none
  due. The list is kept in memory: a reopened store lists none until a
  snapshot fails again.

### Changed

- Opening a directory with no `MANIFEST` now fails with `InvalidFormat` naming the directory, and saying whether it holds a damaged store's files or no store at all, instead of a bare `IO error: No such file or directory` that named neither (#34).
- `Store::open` and `Store::open_or_create` (`open`, `openOrCreate`) on a `MANIFEST` that ends before its header does (empty, or cut short) now fail with `InvalidFormat` naming the file, instead of a bare `IO error: failed to fill whole buffer` that named none.
- `Store::open_or_create` on a directory with no `MANIFEST` now fails with `InvalidFormat`, whatever `create_if_missing` says, when the directory holds a store's data files (`records.log`, `state.bin` or `branches.bin`), naming the file as `open` does, or anything else a new store can't sit beside, naming one of them. On a directory holding neither, with `create_if_missing` unset, it now fails with `NotInitialized`. Each used to fail with the bare IO `NotFound` (#34).

- `getStateItemJson` and `Store::get_state_item(_on)` answer `null`
  (`None`) for an index of zero or more on a state whose value isn't an
  array, such as a Snapshot state holding an object, as they do for an
  index out of range. They threw the deserialization error (`invalid type:
  map, expected a sequence`). `getStateSlice` and `Store::get_state_slice(_on)`
  still throw for such a state. A state whose bytes aren't JSON at all (no
  bytes at all read as an empty list), such as `{`, still throws its
  deserialization error from these lookups, and from the new
  `getStateItemsJson` (see Added) too, except the last item served from the
  head record alone (the state's newest append), which reads no earlier
  item.

- Opening a store reads each record of the log twice, once to validate it
  and once to index it, where it read each three times: the iterator that
  rebuilds the index read every record a second time to find where it
  ended. It now reads each once, which also halves the reads of
  `Store::iter_from(_on)` and of the record replay in
  `catchUpSubscription`. Through a warm page cache, opening a store of
  200,000 small records took 2.0 s where it took 2.9 s, and one pass of the
  iterator 1.0 s where it took 1.9 s.

### Fixed

- `Store::create` (and `JsStore.create`) on a directory that already holds a store no longer resets it. It used to keep the records but publish empty state and branch indexes over the store's own at the first sync, losing every registration and branch with no error. It now refuses with `InvalidOperation`, naming the path, both for a store and for a damaged one (its `records.log`, `state.bin` or `branches.bin` without a `MANIFEST`), a store file left as a symbolic link counting even when its target is missing. It also takes the store's lock before writing anything, so a create aimed at a store another process has open no longer rewrites that store's `MANIFEST` before failing with `Locked`, and it writes `MANIFEST` atomically, so a crash can't leave one cut short (#34). Its refusals for a directory that already holds a store, and for a `MANIFEST` left as a symbolic link that leads to nothing, name the calls that open the store in both APIs: `Store::open` or `Store::open_or_create` (`open` or `openOrCreate` in the Node API). On a directory whose `MANIFEST` open can't read (empty, cut short, another file's bytes, a version or format features this build doesn't know) or isn't a regular file, it refuses with `InvalidOperation`, naming that file and what is wrong with it, and leaves it as it is, where it used to write over it; that refusal doesn't name `open` or `openOrCreate`, which fail on that file too. For a `MANIFEST` too short to hold its header (empty, as a create from an earlier release could leave it if interrupted), it says to remove the file and create again when the directory holds no `records.log`, `state.bin` or `branches.bin`.
- `Store::open_or_create` on a directory without a `MANIFEST` that holds nothing but what a fresh volume's root holds (`lost+found` from mke2fs; `.fseventsd`, `.Spotlight-V100`, `.Trashes` and `.TemporaryItems` from macOS; `System Volume Information` and `$RECYCLE.BIN` from Windows), Finder's `.DS_Store`, or the `LOCK` and `blobs/` of a create that stopped before its manifest now creates the store there (with `create_if_missing`), instead of failing to open it (#34).

- A state write no longer throws after its record is written when the
  snapshot it takes after that record fails (#39). A write that reaches a
  state's snapshot thresholds takes the snapshot right after its own record:
  `update_state` (Rust) and every call that goes through it, such as
  `appendToState`, `appendToStateJson`, `appendToStateJsonWithIdentity`,
  `setState`, `editStateItem`, `redactStateItems`, `treeSet`, `treeRemove`
  and `treeBatch`. If that snapshot failed, on a full disk, say, or
  reading back an item that isn't JSON, the call threw although the write
  was recorded, so a caller took it for a write that didn't happen, and one
  that retried a write that isn't idempotent made it twice. The call now
  returns its record, as the batched appends new in this release do (see
  Added), and the failure goes into the store's snapshot-failure report
  (`snapshotFailures()`; `Store::snapshot_failures`), which Rust and Node
  callers can read: it counts the failures and keeps
  the last one's error, state, branch and sequence. The store also
  logs a `tracing` warning ("the snapshot after a recorded state write
  failed") with the same fields, which reaches only a host that installs a
  `tracing` subscriber; the Node binding installs none. The snapshot stays
  due, and the next write to the state checks the cadence again and takes
  the snapshot if one is still due (after an `editStateItem` or
  `redactStateItems`, a delta snapshot waits for the next full one); a
  state whose
  snapshot keeps failing adds to the report, and logs, at each write that
  finds one due, except that where writes to the state race, a failure that
  comes in after a later write's outcome isn't counted.
  A call whose own work is the snapshot, `compactState`
  (`Store::compact_state`) or `Store::create_snapshot_if_needed`, still
  returns its error. Refusals before the record still throw and record
  nothing. For a batched append, those are an empty batch, an item that
  isn't an object, an identity batch whose `idField` equals its
  `sequenceField`, and a `MANIFEST` that can't be marked, and a stale count
  the refused batch had already counted keeps the corrected count, which
  the next sync saves.

- The native module now builds with `lru` 0.18.2 or a later 0.18 release,
  which fixes GHSA-rhfx-m35p-ff5j and RUSTSEC-2026-0253 (#68). chronicle's own
  calls to `lru` reach neither.

- A read of a state's items (`getStateItemJson`, `getStateSlice` and the
  other item reads), or of a tree state's map (`treeGet`, `treeList`,
  `treeDiff` and the other tree reads), that raced a write to the state on
  the same branch could file the newer value under the head it had read
  before the write. Another branch still at that head, a fork's, then read
  that value as its own (#70). A value is now filed only under a head that
  hasn't moved since it was read, and the batched calls new in this release
  (see Added) file theirs the same way. Only a host that calls the store
  from several threads can meet this: Node calls run one at a time.

## 0.5.1 — 2026-10-30

### Fixed

- JSON objects you write now come back with their keys in the order you wrote
  them (in a JavaScript object, its own property order), at every level and
  from every method that returns them, and `JsStore`'s docs say so. The tree
  methods still keep a tree state's entries by path, not in the order they
  were set. Before, the JSON-writing methods (`appendJson`, `setStateJson`,
  `appendToStateJson` and the rest) stored objects with their keys sorted,
  and every method that parses stored JSON (`getStateJson`,
  `getStateItemJson`, `getStateTail` and the rest) handed them back sorted.
  - Nothing is migrated. Objects stored before through the JSON-writing
    methods keep their sorted keys.
  - Objects stored before as raw bytes (`appendToState`, `setState`,
    `editStateItem`) came back sorted from the methods that parse them, and
    now come back in the order of their bytes (in a JavaScript object, its
    own property order), so a request built from them can change once after
    the upgrade.
  - A store this release writes still opens on 0.5.0, which hands back sorted
    every object it parses, and can re-sort the items it writes into a
    snapshot of an append log.

## 0.5.0 — 2026-10-29

### Breaking

- **Stores whose checkpoints have their own ACL or attributes, are linked or
  mounted one file at a time, or are saved by an account other than their
  owner or one that can't write to the store directory:** a save now writes
  each metadata checkpoint (`state.bin`, `branches.bin` and
  `state-indexes.bin`) to a new file beside it and renames that over the old
  one, where it used to rewrite the old file in place. What the rename
  changes:
  - **A per-file ACL, extended attribute or file flag** isn't carried over,
    such as an ACL that differs from the directory's default or an SELinux
    label set on the file itself. The new file gets what its directory gives
    any new file.
  - **Owner, group and mode.** On Unix the new file takes the old one's
    owner and group as far as the saving account may set them, and its mode.
    Where the saver can't keep the owner, the new file is the saver's,
    without setuid, so the previous owner keeps only what the group and other
    bits grant. Where the saver can't keep the group, the new file keeps the
    group it was created with, without setgid, and that group gets no access.
  - **A hard link** to a checkpoint stops pointing at it after the next save.
  - **A checkpoint bind-mounted as a single file** can't be replaced, so
    saving it fails.
  - **A symbolic link** at a checkpoint's name is still followed, and the
    file it leads to is replaced in its own directory. On Unix, a link that
    leads to something other than a regular file, such as a device, makes the
    save fail. A crash while saving a linked checkpoint can leave a
    `.chronicle-checkpoint-*` staging file beside the file the link leads to,
    and opening the store only removes the ones in the store directory. A
    store whose checkpoint links into another store's directory stages its
    saves there, so opening that other store during such a save fails the
    save, and the checkpoint keeps its previous contents.
  - **What a save needs** is what a rename needs, in the directory of the
    file it replaces: write access, to create the new file and rename it; in
    a directory with the sticky bit, ownership of the checkpoint or of the
    directory; free space for a whole new checkpoint while the old one still
    exists; and on Windows, permission to delete the checkpoint, with no
    other process holding it open without delete sharing. Replacing a
    checkpoint used to need write access to that file alone. On Unix, a
    read-only checkpoint therefore no longer stops a save by an account that
    can write its directory.

  **Who needs to act:** anyone whose stores are set up as above.
  **Migration:** before upgrading, move a per-file ACL onto the directory's
  default ACL, and a per-file SELinux type onto the directory, whose type new
  files take unless the policy names another; link or mount the store
  directory instead of single checkpoints; and grant the saving account those
  permissions.
  **Unchanged:** the checkpoint encodings, so existing stores keep opening,
  and 0.4.x can open a store this version has saved.

- **Rust/Node write APIs:** use `update_state` (or its Node state helpers)
  for internal `state_update` records. Generic appends with this reserved
  type now fail before writing; custom record producers must choose another
  type.
- **Rust embedders:** `StoreError` has a new variant, `DurabilityLost`, for
  a failed fsync of the record log (see Changed). An exhaustive `match` on
  `StoreError` needs an arm for it.
- **Stores an earlier release left after a crash:** opening one now replays
  the state updates its log holds past the last checkpoint, which earlier
  releases left out of the state (see Fixed). Four kinds of update there:
  - **Updates to a state with no registration in `state.bin`** replay like
    any other. A consumer can update a state it never registered, as
    0.4.x allows and this release still does, and an earlier release that
    crashes after registering a state and before its next sync leaves its
    updates without one. The state can be read and updated, and
    `register_state` gives it a strategy whenever it's called.
  - **A record typed `state_update` that doesn't decode as a state update,**
    such as one an earlier release let any append write, stays in the log as
    an ordinary record, and `recovery()` counts it. A generic record typed
    `state_update` whose payload does decode as a state update is replayed
    as one, and refused like the last kind below if its predecessor doesn't
    match.
  - **An update on a branch that `branches.bin` doesn't hold,** which an
    earlier release leaves if it crashes after creating a branch and before
    its next sync, isn't replayed, since nothing can read that branch, and
    `recovery()` counts it.
  - **An update whose predecessor isn't its state's head,** which an earlier
    release could leave if a periodic fsync of the log failed and the
    process crashed before its next sync, makes opening fail with a
    `Corruption` error naming its log offset. To open such a store, open it
    first with the earlier release, and either update any state, even one
    it never registered, or compact one that holds a value (`compact_state`,
    `compactState` in Node, which changes no value); then sync. The new
    checkpoint then lies past the broken update, and the updates the log held
    past the old checkpoint are lost, as that release always lost them.

  File encodings are unchanged.

- **Stores whose `LOCK` isn't a regular file:** opening or creating one now
  fails with `InvalidFormat`, naming what `LOCK` is. A `LOCK` symlink used to
  be followed, and its target truncated, on every open; a lock taken through
  a link doesn't keep a second process out once the target is replaced by a
  rename. Stop anything that uses the store, then move `LOCK` aside or
  remove it: the next open creates a regular one. Unchanged: stores whose
  `LOCK` is a regular file, which is what open and create make, keep opening
  as before.

- **Callers of `getStateTail` on a state whose value isn't an array:** read
  such a state whole with `getState`, or `getStateJson` for a JSON value.
  `getStateTail` of one item or more on such a value now throws a
  deserialization error. Under 0.4.0 it returned the whole value when the
  count asked for was at least the state's stored count (0 for a `snapshot`
  state never set to an array, so any count), and threw when it was
  smaller. Unchanged: a tail of 0 items is still `[]`, a state with no
  value still gives `null`, and existing stores open as before.

- **Node.js before 22.12:** `@animalabs/chronicle` now declares `engines.node`
  `>=22.12.0`, and CI tests Node 22.12.0, 24 and the latest Node. Node 20
  reached its end of life on April 30. **Who needs to act:** anyone installing
  on an older Node, including Node 22 before 22.12. npm warns there, and
  refuses with `engine-strict`; Yarn 1 refuses unless told to ignore engines.
  **Migration:** move to Node 22.12 or later. **Unchanged:** the native
  binding, its API and the on-disk format; existing stores keep opening.

### Added

- Branch-bound store handles ("views"): `JsStore.view(branchName)` returns a
  handle that shares the same open store — same log, caches and lock — but
  whose branch-dependent methods all act on that one branch, independently of
  the store-wide current branch. Any number of views and the root handle can
  be used side by side on different branches; a write through one never
  appears in, or moves, another. `JsStore.pinnedBranch()` returns the pinned
  branch name, or `null` on the root handle.
  - On a view, records, states, trees, `query`, field indexes, compaction,
    `listStates()` item counts, `currentSequence()`, `currentBranch()` and
    subscription catch-up use the pinned branch. `switchBranch` throws.
    `createBranch`, `createBranchWithStates` and `createEmptyBranch` with
    `from` omitted fork the pinned branch. Blobs, `registerState`,
    `getRecord`, `sync`, `stats`, `listBranches`, `setAutoSnapshot` and the
    rest are store-wide, as on the root.
  - `close()` on the root handle closes the store for every view and releases
    the lock file immediately, however many views are still referenced; they
    throw "Store has been closed" afterwards. `close()` on a view only
    detaches that view ("View has been closed") and syncs nothing.
  - If a view's branch is deleted, its branch-dependent methods throw
    "Branch not found". A view cannot delete the branch it is pinned to.
  - Sequences are per branch: sibling branches forked from the same point
    both continue from the parent's head, so their sequence ranges overlap
    (record ids stay store-wide unique). `query` on a branch returns the
    records written on that branch, not the ones it inherited — as it always
    has after `switchBranch`.
  - Subscriptions are still store-wide: a subscriber receives record and
    state-delta events from writes on every branch, whichever handle created
    it. Record events carry the branch id; state-delta events do not.
  - The root handle is unchanged.
- Rust: every branch-dependent `Store` method has an explicit-branch form
  taking a `BranchId` (`update_state_on`, `get_state_on`, `append_on`,
  `query_range_on`, `tree_set_on`, `compact_state_on`,
  `register_state_field_index_on`, `catch_up_subscription_on`, ...). The
  cursor-based methods delegate to them. A call resolves to one branch from
  start to finish, including the auto-snapshot that follows a write, which
  previously re-read the current branch after releasing the write lock.
  `Store::get_branch`, `get_branch_by_id` and `branch_exists` are new;
  `get_state_at_on` replaces the private `get_state_at_for_branch`.
- Selective forks: `createBranchWithStates(name, from, { stateIds?, statePrefixes? })`
  and `createBranchAtWithStates(name, from, atSequence, { stateIds?, statePrefixes? })`
  create a branch that inherits only the named states (exact ids and/or plain
  string prefixes of the id; a state covered twice is inherited once, names
  matching nothing are ignored, an empty filter inherits nothing). Inherited
  chain heads are copied verbatim, snapshot accounting included. A state that
  is not inherited has no value on the new branch — it reads exactly as a
  registered state does on a `createEmptyBranch` branch — and a write to it
  starts a fresh chain. Rust: `Store::create_branch_with_states`,
  `Store::create_branch_at_with_states`, `StateSelection`.
- `Store::state_reconstruction_count()` (Rust) counts how often a state value
  was rebuilt from the log, so tests and diagnostics can tell a cached read
  from a re-materialization without timing it; `Store::clear_state_caches()`
  drops the cached values.
- `stateHeadOffset(stateId)`: the log offset of a state's newest chain record on the handle's branch, or `null` — an O(1) revision key that changes on every write to that state on that branch (including count-preserving edits and the store's own snapshots) and on nothing else, equal for every handle on the same branch. Lets a cache above the store revalidate exactly instead of by item count.

### Changed

- Appends and state updates no longer fail when the record log's periodic
  fsync fails. The record is already complete in the log file and stays
  there, and the next append tries the fsync again.
- A failed fsync of the record log, periodic or in `sync()` itself, now stays
  reported until the store is reopened. Linux reports a write-back error to
  one fsync, and a later one can succeed although the records written before
  the failure never reached the disk, so a checkpoint published after it could
  name records the disk doesn't hold. Until the reopen, `sync()` returns the
  failure every time, even when its own fsync succeeds, and so do
  registration, strategy changes, and branch creation, deletion and
  switching, and any write while one of them waits for its checkpoint. They
  return it as a new error, `StoreError::DurabilityLost`, whose message
  begins `Durability lost:`, in Node too. Nothing publishes a checkpoint. The
  only way to resume durable writes is to reopen the store, which for a host
  that holds it open for its whole life means restarting the process.
  Reopening replays the log from the last checkpoint, and can't tell whether
  records the operating system still caches reached the disk. If they didn't
  while later records did, an open after the cache drops them refuses the
  store as mid-log corruption, as earlier releases do.
- `recovery()` also reports what opening found in the state-update tail: each
  state it replayed without a registration, with its number of updates
  (`unregistered_states`), how many records typed `state_update` it skipped
  because they don't decode as state updates (`undecodable_state_updates`),
  and how many updates it didn't replay because their branch isn't in
  `branches.bin` (`updates_on_missing_branches`). It returns a report whenever
  any of these or a torn tail happened, so a report no longer means a tail was
  torn. The records it skips stay past the checkpoint, so every open counts
  them again until a state update moves the checkpoint past them. Its
  `dropped_bytes` is 0 when nothing was. A state in
  `unregistered_states` isn't damage either. After a crash, `recovery()` also
  names a state written without a registration on purpose.

- Copying a fork's heads no longer grows with the number of branches. State
  chain heads are held per branch in memory, so `createBranch` copies the
  parent's heads (or, for a selective fork, just the selected ones) without
  scanning every head in the store. The fork still ends in a checkpoint that
  saves `state.bin`, which holds every branch's state heads, and
  `branches.bin`, which holds every branch, so a fork's time still grows
  with the number of branches: forking one append-log state took 5.7 ms with
  about 100 branches and 19.7 ms with about 1,000 (medians of five, in a
  debug build with real fsync on Linux). `state.bin`'s format is
  unchanged: 0.4.0 stores open as before and a store written by this
  version opens on 0.4.0.
- `createBranchAt` takes a state's current chain head verbatim, with no chain
  walk, when that state has not changed since the branch point (decided from
  log offsets; applies when the sequence is one the parent branch itself
  wrote, and not to tree states, whose historical entry count stays exact).
  For an append-log state copied this way `getStateLen` is the parent's
  running count, exactly as `createBranch` gives it. An append-log state
  that did change is walked back only as far as the parent's last full
  snapshot, which is no longer read when it predates the branch point (its
  position and length are already on the parent's chain head), so the cost
  is the number of records written to that state since the snapshot. The
  walk still reads a full snapshot — cost proportional to the state's size —
  when the parent took one after the branch point, when the sequence is one
  the parent inherited rather than wrote, for tree states, for parent heads
  written before 0.4.0 recorded the snapshot's length, and when a record
  after the snapshot can't be counted from lengths alone (a `Delta`, which
  only Rust callers write, or a `Set` whose bytes aren't a JSON array, an
  empty one included).
- `createBranch` / `createBranchAt` now hold the store's write lock while
  they capture the branch point and the chain heads, so the two always
  describe the same instant. `createBranchAt` validates the branch name and
  the sequence before walking any chain and no longer leaves a partly
  populated branch behind if a chain read fails.
- Rust API: `StateIndex.heads` is now a `BranchHeads` (was a
  `HashMap<(BranchId, String), StateChainHead>`); `StateManager::set_head_for_branch`
  is replaced by `insert_head_for_branch`, which takes the whole head; and
  `StateManager::copy_heads_for_branch` takes an optional `StateSelection`.
- State field indexes are now per branch. A write on one branch no longer
  drops ("poisons") the index another branch holds for the same state: the
  parent's index keeps answering, correctly, while child branches write to
  their own continuation of the slot, and several branches can each hold an
  index on the same field. A branch that has not registered an index still
  has none — nothing is built for a new branch, and its queries return `null`
  until it calls `registerStateFieldIndex` — and a query never serves another
  branch's ordinals. Callers that re-register after a `null` keep working;
  callers that relied on a cross-branch write invalidating the index must not.
  - Registering on a branch whose slot is still at the chain head where
    another branch built the same index (a child that has not yet written to
    an inherited slot) copies that index instead of decoding the slot.
  - Each branch's index is its own copy: memory proportional to the slot's
    length per branch that registers one.
  - `state-indexes.bin` keeps its format (version 2). Files from 0.4.0 load
    unchanged. When two branches index the same field the file holds one
    entry per branch under the same key; 0.4.0 opening such a file loads one
    of them and rebuilds the other on registration.
  - Rust: `FieldIndexManager` gains `has_indexes_on`, `poison_state_on`,
    `register_from_sibling` and `drop_branch`; `on_append` / `on_edit` /
    `on_redact` / `on_full_replace` / `touch_head_offset` act only on the
    branch they are given.
- Materialized state is shared across branches. A state's value is a
  function of its chain head, so two branches whose head for a state sits at
  the same log offset hold the same value. All three state caches (serialized
  bytes, per-item array form, decoded tree) now find a value another branch
  already holds for the same head instead of walking the log again, and
  share it by reference rather than copying. A fork also takes a reference to
  the parent's cached per-item and tree values for the states it inherits, so
  a child's first read of an inherited state is served from memory even if
  the parent has written since the fork.
- Writes no longer throw away the per-item cache of an array state. After an
  append, edit or redact (and after the snapshots the store itself takes),
  the cached items are advanced to the new value by copying a few pointers —
  the new version shares every untouched item with the old one and with any
  other branch still holding it. Previously each write dropped the cache and
  the next `getStateSlice` / `getStateItemJson` re-materialized the whole
  state from the log. `getState` on a state whose items are cached is now
  assembled from them rather than rebuilt.
  - The first write to an array whose cached value was read back verbatim
    from a full snapshot or a `setState` payload makes one pass over the
    items to bring them into the store's own JSON spelling (what a
    re-materialization would produce). The pass is done once per value and
    shared by every branch deriving from it; later writes are O(1).
  - Returned bytes are unchanged: every cached read equals a rebuild from the
    log byte for byte.
  - Rust: `StateManager::get_state_items` now returns `Arc<StateItems>` (was
    `Arc<Vec<Vec<u8>>>`); `StateItems` indexes and iterates as `&[u8]`.
    `Store::state_items_on` exposes it per branch.
- `deleteBranch` now releases what the store held for the branch: its cached
  state values and field indexes immediately, and its state chain heads once
  the deletion is on disk — at the end of the checkpoint the deletion makes
  before returning, or of the next successful `sync` if that checkpoint
  failed. `state.bin` is written before `branches.bin` within a checkpoint,
  and the heads leave only after both, so a crash between the two metadata
  files can bring a deleted branch back whole but never listed-and-empty; the
  heads then leave `state.bin` at the following `sync`. Memory and
  `state.bin` therefore stop growing with branches that were forked and
  deleted; previously every deleted branch left its heads in `state.bin` for
  good. Records the branch wrote remain in the append-only log, and heads
  left behind by branches deleted under earlier versions are not swept.
  `deleteBranch` also takes the store's write lock.

### Fixed

- `Store::sync()` now holds the record-write mutex through log sync and checkpoint publication, preventing concurrent syncs from publishing an older checkpoint after a newer one.
- Metadata checkpoints (`state.bin`, `branches.bin`, and `state-indexes.bin`) now replace the previous file atomically after writing and syncing a same-directory temporary file. An interrupted save leaves the previous complete checkpoint available instead of truncating it. Unix also syncs the containing directory after replacement, where the system can. Where the saving account may not open the directory, as in one it can write but not read, saves go without that sync, and the store logs a warning once for each checkpoint file while it's open. Existing file encodings are unchanged. Opening a store removes the temporary files (`.chronicle-checkpoint-*`) that saves killed before their rename left in the store directory, where it can list that directory.
- Repeated syncs skip metadata whose exact encoded content the same writer already published and whose destination file identity/metadata is unchanged, including a save that went without the directory sync. Changed checkpoints get the file sync and, where the system and the directory's permissions allow, the directory sync. Failed publication, reopening, or cloning a writer starts again with a full save. The cache retains a digest and file handle rather than a second copy of field-index data.

- After a metadata checkpoint fails, further mutations retry that checkpoint
  before accepting writes that could depend on an unpersisted state
  registration or branch. The tentative in-memory mutation remains available
  for inspection; `sync()` can retry explicitly, unless the record log's
  fsync failed (see Changed). Subscribers hear of a branch created or
  deleted once a checkpoint persists it. Historical branch creation
  completes its fallible chain reads before allocating a branch.
- Reopening a store replays complete state updates after its metadata
  checkpoint, except those on a branch that `branches.bin` doesn't hold,
  including snapshot accounting and the field indexes whose saved head
  matches `state.bin`. It drops any other field index, which registering
  it again rebuilds. State registration, strategy changes, and branch
  creation/deletion/switching now checkpoint before returning. Store
  checkpoints are serialized with writes, and branch identities found only
  in partially published state metadata are reserved against reuse. No
  on-disk format change; record updates still require `sync()` to flush
  contents. On Windows, a save doesn't wait for its rename to reach the
  disk, so after an OS crash or power loss the store may open from an
  earlier checkpoint.

- Opening or creating a store no longer truncates its `LOCK` file. A lock
  that fails for a reason other than another holder of it (another process,
  or another open in the same one) is now reported as that I/O error, rather
  than as "Store is locked by another process".

- `storeBlob` now writes each blob whole to a staging file in `blobs/`, syncs it, and renames it to the blob's own name. On Unix, before it returns, the directories holding the blob's name have been synced since this open store last made or renamed anything into them, where the system syncs directories and the account may open them; what an earlier process or an earlier open of the store made is taken as it is. A store cut short used to leave a truncated file under the blob's name, which `getBlob` couldn't read. Opening a store removes the staging files (`.chronicle-blob-*`) that stores killed before their rename left in `blobs/`, where it can list `blobs/`.
- `storeBlob` of bytes whose stored file is cut short, damaged or holds other bytes now replaces that file. It used to keep the file and return the hash, so the blob stayed unreadable. To tell, a store of bytes that aren't in the cache now reads their stored file once, where it used to check only that a file was there. Something other than a regular file at the blob's name or a link's target, or a file that can't be opened or read, such as for its permissions, now fails the store and is left as it is, and so does a blob file in a format version newer than this chronicle reads, such as one a newer chronicle wrote, where the store used to return the hash. A store of cached bytes whose file has gone writes the file again. A symbolic link at a blob's name is read through: one whose target reads back as the bytes is left, and one whose target is cut short, damaged, holds other bytes or doesn't exist is itself replaced by the blob, leaving any target as it was; one that loops fails the store.
- `getBlob` of a file cut short, or whose header names more content than the file holds, now fails with an error that names the file and says storing the same bytes again replaces it. A cut-short file used to fail with `failed to fill whole buffer`, and a header naming `u64::MAX` bytes made `getBlob` panic. A name `getBlob` can't look at, such as a link that loops, now fails it with an error where it used to read as no blob. A blob whose bytes are cached is still answered from memory.
- `storeBlob` refuses a content type longer than 65,535 bytes. It used to write the type's length cut to 16 bits and return the hash, and a later `getBlob` of that blob read a length from the type's own bytes, which for a type of 65,536 `t`s aborted the process. Such a blob written before this release now fails `getBlob` with an error instead.

- A `Delta` that replaces an append-log state's value with an array now sets
  the state's count to that array's length, as a `Set` does, and a `Set` or
  `Delta` with no bytes, which later array operations treat as an empty
  array, sets it to 0. Both left the count as it was, so afterwards
  `getStateLen` reported the old length, an Edit was checked against it,
  after a later append `getStateItemJson` could return that item for the
  wrong index, and after an empty `setState` a `getStateTail` of at least
  one item but fewer than the old length threw. Only Rust callers can write
  a `Delta`; an empty `setState` reaches the second case from Node. A state
  whose stored count is already wrong stays wrong until its next full
  snapshot or an array or empty `setState` (from Rust, also such a `Delta`);
  `compactState` takes a full snapshot.

- `createBranchAt` no longer loses an inherited Edit, Redact or Set once the
  new branch takes its first delta snapshot. The historical chain head was
  installed with zeroed snapshot accounting, so when the inherited chain held
  a non-append operation that no full snapshot covered yet, the branch's next
  threshold snapshot could be an append-only delta that walked past it: the
  pre-edit value came back, redacted items reappeared, or appends replaced by
  a Set were resurrected. The head is now rebuilt from the chain walk with the
  accounting the parent carried at that sequence (`has_non_append_since_snapshot`,
  delta/full snapshot offsets and counts, `item_count_at_last_full`), so a
  historical branch schedules snapshots exactly as an ordinary `createBranch`
  taken at that moment would have.
  - No on-disk format change. Branches created by earlier versions keep the
    head they were given; a branch that already took a bad delta snapshot is
    not repaired (`tools/scan-delta-poison.py` finds those chains).
  - A historical branch's first full snapshot now follows the parent's
    schedule instead of firing once the branch has grown by its own size at
    the branch point.

- A branch made with `createBranchAt` no longer brings back items redacted
  before its point or replaced there by a `Set`, and no longer undoes an Edit
  made there. The branch started as if its append-log state had seen only
  Appends since its last snapshot, so its first delta snapshot walked past
  such a change, and from then on the branch read back what the change had
  removed or replaced. The branch now takes that record from the chain at
  its point, and takes no delta snapshot until its next full one. A branch
  made before this release may have that record wrong: when its delta
  snapshot comes due across such a change, the store now takes a full
  snapshot of its value instead.
- A branch that already took such a delta snapshot under an earlier release
  reads as it did under 0.4.0. After a Redact or a `Set`, that reading can
  hold items the change had removed, while `getStateLen` counts without
  them, so a read bounded by the count, such as `getStateSlice` up to the
  length, misses the newest items until the branch's next full snapshot,
  which then keeps that reading and counts it. Until then, `getStateLen`
  short of the length of `getStateJson`'s array marks such a branch, though
  a later Redact that runs past the count can bring the two level without
  removing those items. A branch that 0.2.2 or an earlier release damaged
  after an Append and a `Set` can instead have a `getStateLen` above that
  length. After an Edit, the edit stays lost, and nothing marks the branch.
  After a `Delta`, which only Rust callers can write, the reading holds the
  values it replaced. A later Redact or Edit applies to that reading as
  under 0.4.0, which also refuses an Edit at or past the count; only the
  count that a Redact running past the count leaves can differ, as this
  release's fix to that count says. On a branch counted above its reading,
  an Edit at an index past the reading's end but below the count is
  accepted, after which `getState` fails with a corruption error, as under
  0.4.0; `compactState` before any such Edit avoids it. On any branch that
  took such a delta snapshot, while the newest operation on it is an Append,
  with no snapshot taken since, `getStateItemJson` at `getStateLen()` less
  one can answer with that appended item rather than what the reading holds
  at that position, depending on what the store has cached, as under 0.4.0.
  On a branch counted above its reading, the reading holds nothing there,
  and the call otherwise answers null. `compactState` on the branch makes
  `getStateLen` count the reading, and keeps the reading.
- `getStateTail` no longer returns some items twice. Once it had taken a
  delta snapshot's items and needed more, it read on into the Appends that
  snapshot stood for and returned them again: on a state taking a delta
  every two operations, appends 0 to 4 gave a tail of 4 as `[3, 2, 3, 4]`.
  It also returns at most the items asked for where the stored count is
  short of the value.
- A `Delta` on an append-log state now holds delta snapshots off as a
  Redact, an Edit or a Set does, so the next delta no longer walks past the
  value it replaced, and `iter_state_items` applies it as other reads do,
  in place of passing over it. The Node binding exposes neither.
- A `Redact` whose range ends at or past 2^32 now leaves a field index as it
  leaves the value. The index narrowed the range to 32 bits before clamping
  it to its length, so a wrapped end removed or kept the wrong entries.

- A `Redact` whose range runs past the end of an append-log state now
  counts only the items it removes. Its count clamped only the length of the
  range, so `Redact { start: 4, end: 100 }` on five items set the count to 0
  where the value kept four: `getStateLen` reported 0, an Edit of an item
  that exists was refused as out of bounds, and after a later append
  `getStateItemJson` could return that later item for an earlier index. A
  state whose stored count is already wrong can stay wrong until its next
  full snapshot or array `Set`, either of which counts the items again;
  `compactState` takes a full snapshot.

## 0.4.0 — 2026-09-17

### Added

- Added a persisted secondary index on a JSON field (by JSON-pointer path)
  of every item in a state slot, incrementally maintained as the slot
  mutates (append, edit, redact, set/snapshot) and queryable by numeric
  range or string equality, returning matching ordinals rather than item
  content. New napi methods: `registerStateFieldIndex`,
  `queryStateIndexRange`, `queryStateIndexEq`, `getStateIndexValueCounts`.
  Indexes persist to `state-indexes.bin` alongside `state.bin`; a
  missing/stale/corrupt index file is not fatal on open — it starts empty
  rather than failing the store.
  - Query methods return `null` when no such index is registered (never
    registered, wrong kind, or dropped — see below), distinct from `[]`,
    an index that exists but matched nothing.
  - Only one branch's view of a slot can have a live index at a time: a
    write on a different branch than the one an index was registered
    against drops that index rather than mixing ordinals across branches.
    Re-register to rebuild it for the branch you're on.

### Changed

- Changelog entries now land as per-change fragment files in `changelog.d/`
  (`<slug>.<breaking|added|changed|fixed>.md`), folded into the version
  section at release time — concurrent PRs no longer conflict in
  `CHANGELOG.md`. Editing `## Unreleased` directly still works and is merged
  at the same point.

## 0.3.0 — 2026-08-01

### Breaking (on-disk format)

- **`state_update` records are now MessagePack-encoded, and a store that has
  been written by 0.3.0 cannot be read by earlier chronicle.** New records are
  written with `to_vec_named` and `serde_bytes` on the value fields, so
  `StateOperation` payloads travel as raw binary instead of arrays of JSON
  integers.
  - **Who needs to act:** anyone who may need to roll a store back to a
    pre-0.3.0 chronicle. The format change is forward-only — take a copy
    before upgrading if rollback has to stay open.
  - **Migration:** none. Nothing rewrites existing records; the new encoding
    applies to records written from here on.
  - **Unchanged:** existing stores open and read normally. `decode` gates on
    the record's encoding tag and historical JSON payloads (written as
    `RecordInput::raw`) decode through the fallback arm, pinned by a
    handwritten-legacy-payload test. Mixed JSON-history/MessagePack-tail
    stores were verified against a copy of a live 6 GB store. The JSON wire
    format itself is unchanged.
  - JS consumers that previously reached into record payloads with
    `JSON.parse` should move to the new `getStateUpdateJson(id)`, which is
    encoding-agnostic.

### Added

- **`update_state_strategy`** (napi: `updateStateStrategy(registration)`) — the
  explicit upsert leg for snapshot cadence. Registrations persist in
  `state.bin`, so consumers that re-register on boot could never change cadence
  on an existing store: `register_state` errors with `StateExists` and the
  first-registration values won forever. Restricted to the same strategy kind,
  because changing the kind under a live chain would change reconstruction
  semantics for records already on disk; cadence fields steer future snapshot
  scheduling only. `initialValue` is ignored, and `register_state` deliberately
  stays non-upserting.
- **`getStateUpdateJson(id)`** — encoding-agnostic `state_update` reader for JS
  consumers.
- **`materialize_operations`** re-exported at `lib.rs` beside
  `apply_operation`.
- **Scaling gates as CI tests** — `tests/disk_scaling.rs` (on-disk bytes vs op
  count), `tests/reconstruction_scaling.rs` (cold-read latency at worst-phase
  sizes), and `tests/materialize_equivalence.rs` (differential
  fold-vs-single-pass equivalence, including a 2000-case fuzz). Plus manual
  probes: `tests/perf_probe.rs` for fixed-size per-op numbers and
  `examples/store_probe.rs` for measuring against a store copy.
- The criterion bench suite compiles again — it had imported the crate under
  its pre-rename name (`record_store`) since the initial release — and now
  covers point reads, JSON appends and tree ops.

### Changed

- **Full-snapshot spacing is size-aware** (#11). Fulls fired on a fixed
  `delta × full` op interval and embed the whole state, so growing AppendLog
  and Tree states paid O(N²/2K) disk and O(N/K) append latency. The interval
  now grows with the state — a full fires once the ops covered since the last
  full reach the item count at that full — with the configured interval as a
  floor, so small states keep their configured cadence exactly. Total snapshot
  bytes telescope to ≤ 2× final state size. JSON appends measured 77 µs →
  1,709 µs per op across 1k → 50k accumulated entries under the old cadence;
  they are now flat at ~20 µs regardless of accumulated size.
- **Reconstruction is single-pass.** Reopen, cache miss, branch switch and time
  travel folded `apply_operation` over the op chain, re-parsing and
  re-serializing the entire state per op — O(tail × N). `materialize_operations`
  decodes the base once, applies the tail in place, and encodes once —
  O(state + ops), with semantics preserved exactly. Cold reads went from
  22.4 → 10.7 ms and 259.7 → 44.2 ms at 3.9k/15.9k appends (11.59× → 4.13×
  per 4× N: linear).
- **Edits no longer force early full snapshots.** With tails cheap to traverse,
  the `has_non_append` forced-full is retired: a non-Append op still blocks
  delta snapshots, but the raw tail now rides until the size-aware full fires.
  The old behavior cost O(N²/D) disk on any growing log taking regular edits —
  the summary-merge pattern measured 439 MB of store for 16k logical ops,
  now 18.2 MB (24×).
- **Point lookups of the last item are O(1).** Every state write pops the LRU
  caches, and a write-through consumer's lookup of the item it just appended
  re-materialized the whole state inside every append. When the head record is
  an Append and the caller asks for the last index, the item is now served from
  the head record alone. Against a copy of a live store (19,767 messages, 33 MB
  state, 6 GB log), append+lookup went 421 ms → 94 µs.
- **Tree point reads are cached in decoded form.** `tree_get`/`tree_list`
  deserialized the entire path→entry map per call — 16.7 µs → 2.28 ms across
  100 → 10k entries. The state manager now caches the decoded `TreeState` per
  `head_offset` (same key scheme and staleness discipline as the per-item
  cache); `tree_get` is flat at ~150 ns at all measured sizes. The trade is
  that the decoded-tree LRU is bounded by entries, not bytes.
- Tree op counts are tracked as an upper bound (overwrites count as inserts,
  corrected at each full snapshot) — sufficient for spacing, and it only ever
  spaces fulls further apart.

### Fixed

- **Snapshot-strategy states no longer walk the entire chain on cold reads.**
  `reconstruct_from_disk` / `get_state_at` / `find_chain_info_at` broke the
  backward walk only on `Snapshot` records; `Set` fell through to the catch-all
  and the walk continued to sequence 0. Snapshot-strategy states are written
  via `Set` and never get a periodic `Snapshot` record, so cold reads
  deserialized every full-state `Set` back to the beginning and discarded all
  but the last — chains up to 3,928 deep across an 11.7 GB log, measured at
  6–8 minutes to boot. `Set` replaces the whole state exactly like `Snapshot`,
  so the newest one is a valid terminal; these reads are now O(1).
- `CompactionStats::ops_since_last_full_snapshot` counted ops plus delta count
  instead of ops (deltas × `delta_snapshot_every` + raw ops), undercounting
  roughly 100×.
- Interval-floor arithmetic in both strategy arms uses `saturating_mul`; an
  overflowing config previously wrapped.
- Legacy indexes deserialize the new head fields to 0 and keep the configured
  cadence until the first full snapshot stamps a baseline. `StateChainHead`'s
  positional wire format is now documented and guarded against both the v2
  (pre-#11) and v1 (pre-`item_count`) layouts.
