- Fork cost no longer grows with the number of branches. State chain heads
  are held per branch in memory, so `createBranch` copies the parent's heads
  (or, for a selective fork, just the selected ones) without scanning every
  head in the store. `state.bin` is unchanged on disk: 0.4.0 stores open as
  before and a store written by this version opens on 0.4.0.
- `createBranchAt` takes a state's current chain head verbatim, with no chain
  walk, when that state has not changed since the branch point (decided from
  log offsets; applies when the sequence is one the parent branch itself
  wrote, and not to tree states, whose historical entry count stays exact).
  For an append-log state copied this way `getStateLen` is the parent's
  running count, exactly as `createBranch` gives it. States that did change
  still walk back to their last full snapshot — that cost is proportional to
  the state's size, not to the store's.
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
