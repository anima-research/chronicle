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
