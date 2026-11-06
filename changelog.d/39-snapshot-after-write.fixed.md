- A state write no longer throws after its record is written when the
  snapshot it takes after that record fails (#39). A write that reaches a
  state's snapshot thresholds takes the snapshot right after its own record:
  `update_state` (Rust) and every call that goes through it, such as
  `appendToState`, `appendToStateJson`, `appendToStateJsonWithIdentity`,
  `setState`, `editStateItem`, `redactStateItems`, `treeSet`, `treeRemove`
  and `treeBatch`. If that snapshot failed, on a full disk, say, or
  reading back an item that isn't JSON, the call threw although the write
  was recorded, so a caller took it for a write that didn't happen, and one
  that retried a write that isn't idempotent made it twice. The call now
  returns its record, as the batched appends new in this release do (see
  Added), and the failure goes into the store's snapshot-failure report
  (`snapshotFailures()`; `Store::snapshot_failures`), which Rust and Node
  callers can read: it counts the failures and keeps
  the last one's error, state, branch and sequence. The store also
  logs a `tracing` warning ("the snapshot after a recorded state write
  failed") with the same fields, which reaches only a host that installs a
  `tracing` subscriber; the Node binding installs none. The snapshot stays
  due, and the next write to the state checks the cadence again and takes
  the snapshot if one is still due (after an `editStateItem` or
  `redactStateItems`, a delta snapshot waits for the next full one); a
  state whose
  snapshot keeps failing adds to the report, and logs, at each write that
  finds one due, except that where writes to the state race, a failure that
  comes in after a later write's outcome isn't counted.
  A call whose own work is the snapshot, `compactState`
  (`Store::compact_state`) or `Store::create_snapshot_if_needed`, still
  returns its error. Refusals before the record still throw and record
  nothing. For a batched append, those are an empty batch, an item that
  isn't an object, an identity batch whose `idField` equals its
  `sequenceField`, and a `MANIFEST` that can't be marked, and a stale count
  the refused batch had already counted keeps the corrected count, which
  the next sync saves.
