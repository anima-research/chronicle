- A state write no longer throws after its record is written when the
  snapshot it takes after that record fails (#39). A write that reaches a
  state's snapshot thresholds takes the snapshot right after its own record:
  `update_state` (Rust) and every call that goes through it, such as
  `appendToState`, `appendToStateJson`, `appendToStateJsonWithIdentity`,
  `appendManyToStateJson`, `appendManyToStateJsonWithIdentity`, `setState`,
  `editStateItem`, `redactStateItems`, `treeSet`, `treeRemove` and
  `treeBatch`. If that snapshot failed, on a full disk, say, or reading back
  an item that isn't JSON, the call threw although the write was recorded,
  so a caller took it for a write that didn't happen, and one that retried
  a write that isn't idempotent made it twice. The call now returns its
  record, and the failure goes into the store's snapshot-failure report,
  which Rust and Node callers can read: it counts the failures and keeps
  the last one's error, state, branch and sequence. The store also
  logs a `tracing` warning ("the snapshot after a recorded state write
  failed") with the same fields, which reaches only a host that installs a
  `tracing` subscriber; the Node binding installs none. The snapshot stays
  due, and the next write to the state tries it again; a state whose
  snapshot keeps failing adds to the report, and logs, at each write that
  finds one due.
  A call whose own work is the snapshot, `compactState`
  (`Store::compact_state`) or `Store::create_snapshot_if_needed`, still
  returns its error. Refusals before the record (an empty batch, an item
  that isn't an object, an identity batch whose `idField` equals its
  `sequenceField`, a `MANIFEST` that can't be marked) still throw and write
  nothing.
