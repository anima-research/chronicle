- `tools/compact-set-states.py` frees the space in `records.log` that old
  values of named states hold. For each state a `--state` glob names, it
  keeps what chronicle's read of that state reaches on every branch, from
  each head in `state.bin` and from each state change after the newest head,
  and drops that state's other changes. Every other record stays, and a
  state change is rewritten only where its link to the one before must
  change. What goes is the old values of each named state: a read of it at
  an older sequence gives nothing, and a branch made later at an older point
  doesn't hold it. A field index that a caller registered is gone after the
  run, and the caller registers it again. The tool changes nothing unless
  `--apply` follows, and it refuses an open store, a `state.bin` older than
  the log, a torn log, a record that fails its checksum, a JSON state change
  that would need a new link, and the files of an earlier run. A run that
  stops at any point leaves a store that opens as before, or one that open
  refuses, whose `state.bin` says how to go back.
- `tools/state-digests.mjs` prints digests of what chronicle's read gives on
  every branch of a store, and no content, so a store can be compared with
  itself before and after the tool. It exits 1 when a read fails.
