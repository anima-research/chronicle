- Appends and state updates no longer fail when the record log's periodic
  fsync fails. The record is already complete in the log and stays there, the
  next append tries the fsync again, and `sync()` reports the failure once,
  even when its own fsync succeeds.
