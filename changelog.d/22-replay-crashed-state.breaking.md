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
