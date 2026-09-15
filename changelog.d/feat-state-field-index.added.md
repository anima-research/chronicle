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
