- Appends and state updates no longer fail when the record log's periodic
  fsync fails. The record is already complete in the log and stays there, the
  next append tries the fsync again, and `sync()` reports the failure once,
  even when its own fsync succeeds.
- `recovery()` also reports what opening found in the state-update tail: each
  state it replayed without a registration, with its number of updates
  (`unregistered_states`), and how many records typed `state_update` it
  skipped because they aren't state updates (`undecodable_state_updates`). It
  returns a report whenever any of these or a torn tail happened, so a report
  no longer means a tail was torn. Its `dropped_bytes` is 0 when nothing was.
  A state in `unregistered_states` isn't damage either: after a crash, a state
  written without a registration on purpose is named there too.
