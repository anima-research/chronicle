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
