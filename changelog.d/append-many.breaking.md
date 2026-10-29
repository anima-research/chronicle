- **On-disk format (stores that record a batched append):** a state update
  can now be a `StateOperation::AppendMany`, several items appended to an
  append-log state as one record (see Added). Earlier releases can't decode
  it, so the first `AppendMany` a store records rewrites its `MANIFEST` from
  format version 1 to version 2, naming the feature, before the record is
  written. An earlier release refuses a version 2 store at open with
  "Unsupported store version: 2". Without the mark, what an earlier release
  does depends on the release and on where the batch lies. Past the last
  checkpoint, 0.5.0 replays the log's state updates and skips the batch,
  which it can't decode, logging a warning; then, at the update that follows
  the batch in that state's chain (its next update on that branch), it
  refuses to open the store as corrupt ("cannot replay state ... does not
  match checkpoint/replayed head"). When the batch is that state's newest
  update there is no next one, so 0.5.0 opens the store, and the state lacks
  the batch's items with only that warning to say so. Releases before 0.5.0
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
  **Who needs to act:** anyone who may roll a host back to an earlier
  chronicle after the host has written a batch, from Rust or with the Node
  API's `appendManyToStateJson` or `appendManyToStateJsonWithIdentity`.
  context-manager's presentation index is meant to be its first writer. A
  host rolled back must not call `create` on a store it means to open.
  **Migration:** none going forward. Going back, there is none for a store
  that holds a batch: keep a copy of the store from before the upgrade if a
  rollback must stay possible. A first batch that fails once the rewritten
  `MANIFEST` is in place, by a crash, by an error syncing its directory or by
  an error writing its record (a full disk, say), leaves a version 2 store
  that holds no batch, which earlier releases refuse all the same. This
  release refuses a version 2 `MANIFEST` naming a feature it doesn't know,
  as written by a later one.
  **Unchanged:** a store never asked to record a batch keeps its version 1
  `MANIFEST` and opens on every earlier release, as before, and existing
  stores open on this one unchanged. The record framing and the encodings of
  `state.bin`, `branches.bin` and `state-indexes.bin` are unchanged.
