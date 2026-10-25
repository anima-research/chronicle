- **On-disk format (stores that record a batched append):** a state update
  can now be a `StateOperation::AppendMany`, several items appended to an
  append-log state as one record (see Added). Earlier releases can't decode
  it, so the first `AppendMany` a store records rewrites its `MANIFEST` from
  format version 1 to version 2, naming the feature, before the record is
  written. An earlier release refuses a version 2 store at open with
  "Unsupported store version: 2". Without the mark it would fail part way
  through instead: past the last checkpoint it skips the batch it can't
  decode, then refuses to open the store, as corrupt, at the next update to
  the same state, and before the checkpoint, any read of that state that
  reaches the batch fails with "unknown variant `AppendMany`". An open that
  replays a batch the `MANIFEST` doesn't name, as when a power loss undid
  the rewrite, names it again.
  **Who needs to act:** anyone who may roll a host back to an earlier
  chronicle after the host has written a batch. Nothing writes one yet: the
  Node API gains it separately, and context-manager's presentation index is
  meant to be its first writer.
  **Migration:** none going forward. Going back, there is none for a store
  that holds a batch: keep a copy of the store from before the upgrade if a
  rollback must stay possible. A crash between the `MANIFEST` rewrite and the
  record leaves a version 2 store that holds no batch, which earlier releases
  refuse all the same. This release refuses a version 2 `MANIFEST` naming a
  feature it doesn't know, as written by a later one.
  **Unchanged:** a store that never records a batch keeps its version 1
  `MANIFEST` and opens on every earlier release, as before, and existing
  stores open on this one unchanged. The record framing and the encodings of
  `state.bin`, `branches.bin` and `state-indexes.bin` are unchanged.
