- `snapshotFailures()` (`Store::snapshot_failures`) lists each state whose
  snapshot after a recorded write failed at its most recent attempt, per
  branch: the state, the branch, how many writes in a row failed to take
  it, the sequence of the latest of them, and its error. A write whose own
  snapshot fails still returns its record (#39), and the store logs a
  `tracing` warning, which a Node process doesn't show. An entry goes once
  a snapshot of the state on that branch is recorded or a write finds none
  due. The list is kept in memory: a reopened store lists none until a
  snapshot fails again.
