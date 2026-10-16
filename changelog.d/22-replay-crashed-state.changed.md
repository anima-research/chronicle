- Appends and state updates no longer fail when the record log's periodic
  fsync fails. The record is already complete in the log file and stays
  there, and the next append tries the fsync again.
- A failed fsync of the record log, periodic or in `sync()` itself, now stays
  reported until the store is reopened. Linux reports a write-back error to
  one fsync, and a later one can succeed although the records written before
  the failure never reached the disk, so a checkpoint published after it could
  name records the disk doesn't hold. Until the reopen, `sync()` returns the
  failure every time, even when its own fsync succeeds, and so do every
  metadata mutation and any write while one waits for its checkpoint. Nothing
  publishes a checkpoint. Reopening replays the log from the last checkpoint,
  and can't tell whether records the operating system still caches reached the
  disk. If they didn't while later records did, an open after the cache drops
  them refuses the store as mid-log corruption, as earlier releases do.
- `recovery()` also reports what opening found in the state-update tail: each
  state it replayed without a registration, with its number of updates
  (`unregistered_states`), how many records typed `state_update` it skipped
  because they don't decode as state updates (`undecodable_state_updates`),
  and how many updates it didn't replay because their branch isn't in
  `branches.bin` (`updates_on_missing_branches`). It returns a report whenever
  any of these or a torn tail happened, so a report no longer means a tail was
  torn. Its `dropped_bytes` is 0 when nothing was. A state in
  `unregistered_states` isn't damage either. After a crash, `recovery()` also
  names a state written without a registration on purpose.
