- **On-disk format (stores that record a batched append):** a state update
  can now be a `StateOperation::AppendMany`, several items appended to an
  append-log state as one record (see Added). Earlier releases can't decode
  it, so the first `AppendMany` a store records rewrites its `MANIFEST` from
  format version 1 to version 2, naming the feature, before the record is
  written. An earlier release refuses a version 2 store at open with
  "Unsupported store version: 2". Without the mark, what an earlier release
  does depends on the release and on where the batch lies. Past the last
  checkpoint, 0.5.0 replays the log's state updates and skips the batch,
  which it can't decode, logging a warning; then, at the next update in
  that state's chain on that branch that isn't itself a batch, it refuses
  to open the store as corrupt ("cannot replay state ... does not match
  checkpoint/replayed head"). When only batches follow (the batch, or a run
  of them, is the newest), 0.5.0 opens the store, and the state lacks all
  their items with only those warnings to say so. Releases before 0.5.0
  don't replay updates past the last checkpoint at all: they open the store
  without a word, and the state is what the checkpoint holds, without the
  batch or any other update to it past the checkpoint. Before the
  checkpoint, on 0.3.0 to 0.5.0, a read of that state that reaches the batch
  fails with "unknown variant `AppendMany`"; releases before 0.3.0 can't
  read the MessagePack state updates 0.3.0 and later write at all.
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
    `create` on a store it means to open. Where the store directory isn't
    synced (this process may not open it, the system doesn't sync
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
  host that never writes a batch.
