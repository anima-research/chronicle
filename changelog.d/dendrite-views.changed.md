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
  running count, exactly as `createBranch` gives it. An append-log state
  that did change is walked back only as far as the parent's last full
  snapshot, which is no longer read when it predates the branch point (its
  position and length are already on the parent's chain head), so the cost
  is the number of records written to that state since the snapshot. The
  walk still reads a full snapshot — cost proportional to the state's size —
  when the parent took one after the branch point, when the sequence is one
  the parent inherited rather than wrote, for tree states, and for parent
  heads written before 0.4.0 recorded the snapshot's length.
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
