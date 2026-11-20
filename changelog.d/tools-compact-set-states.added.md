- `tools/compact-set-states.py` frees the space in `records.log` that old
  values of named states hold. For each state a `--state` glob names, it
  keeps what chronicle's read of that state reaches on every branch, from
  each head in `state.bin` and from each state change after the newest head,
  and drops that state's other changes. Every other record stays, and a
  state change is rewritten only where its link to the one before must
  change, which then holds the new offset. What goes is the old values of
  each named state: a read of it at an older sequence can give nothing, and
  a branch made later at an older point can lack it. A field index that a
  caller registered is gone after the run, and the caller registers it
  again. A query by record type with a limit can return more records
  afterwards, and a client of agent-framework's API `subscribe` that resumes
  from a sequence it kept can get its branch's whole history again, where
  that sequence was a dropped change's. The tool changes nothing unless
  `--apply` follows, and it refuses an open store, a `state.bin` older than
  the log, a torn log, a record that fails its checksum, a state change in
  the JSON or the positional form that would need a new link, and the files
  of an earlier run. It follows a linked `records.log`, `state.bin` or
  `state-indexes.bin` as chronicle does. A run that stops at any point
  leaves a store that opens as before, or one that open refuses, and
  `--go-back` then puts the old files back, or, where the swap hadn't
  begun, removes the run's files.
- `tools/state-digests.mjs` prints digests of what chronicle's read gives on
  every branch of a store, and no content, so a store can be compared with
  itself before and after the tool. Given the tool's `--state` globs, it
  leaves out the older reads of the states they name. It exits 1 when a
  read fails.
