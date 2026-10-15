- **Rust/Node write APIs:** Register states before updating them, and use `update_state` (or its Node state helpers) for internal `state_update` records. Unregistered state updates and generic appends with this reserved type now fail before writing; custom record producers must choose another type.
- **Stores an earlier release left after a crash:** opening one now replays
  the state updates its log holds past the last checkpoint, which earlier
  releases left out of the state (see Fixed). Three kinds of update there
  are handled differently:
  - **Updates to a state whose registration isn't in `state.bin`,** which an
    earlier release leaves if it crashes after registering a state and
    before its next sync, are replayed. The state can be read, and
    `recovery()` names it, but updating it fails with `StateNotRegistered`
    until `register_state` supplies its strategy again.
  - **A record typed `state_update` that isn't a state update,** which an
    earlier release let any append write, is skipped and counted in
    `recovery()`. It stays in the log as an ordinary record.
  - **An update whose predecessor isn't its state's head,** which an earlier
    release could leave if a periodic fsync of the log failed and the
    process crashed before its next sync, makes opening fail with a
    `Corruption` error naming its log offset. To open such a store, open it
    first with the earlier release, and either update any registered state
    or compact one that holds a value (`compact_state`, `compactState` in
    Node, which changes no value); then sync. The new checkpoint then lies
    past the broken update, and the updates the log held past the old
    checkpoint are lost, as that release always lost them.

  File encodings are unchanged.
