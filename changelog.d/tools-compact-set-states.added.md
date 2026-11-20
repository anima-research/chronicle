- `tools/compact-set-states.py` frees the space in `records.log` that old
  values of named states hold. Each update of a named state that the links
  from the heads reach must be a whole value, a `Set` or a `Snapshot`; of it
  the tool keeps the update that each head in `state.bin` names, on every
  branch, and drops its other updates that the links from the heads reach.
  Every other record stays, and so does an old value of a named state that
  no head reaches, such as a deleted branch's; the dry run counts those. A
  state change that a head reaches along the links is rewritten only where
  its link must change, and every other record typed as a state change keeps
  its bytes. What goes is the old values of each named state: a read of it
  at an older sequence can give nothing, and a branch made later at an older
  point can lack it. A field index that a caller registered is gone after
  the run, and the caller registers it again. A record that stays and names
  a dropped change in `causedBy` or `linkedTo` still names it, and a read of
  that id gives nothing; the dry run counts such records. A query by record
  type with a limit can return more records afterwards, and a client of
  agent-framework's API `subscribe` that resumes from a sequence it kept can
  get its branch's whole history again, where that sequence was a dropped
  change's. The tool changes nothing unless `--apply` follows, and it
  refuses an open store, a store with no `LOCK`, a `MANIFEST` that names a
  version or feature it doesn't read, a named state that is not whole-value,
  a record typed as a state change after the newest head on a live branch,
  which may be an update that `state.bin` doesn't have yet (from chronicle
  0.5.0 an open and a close apply such an update, and with 0.4.0, or for a
  record that chronicle doesn't decode, a write of any state and a close
  clear it), a
  torn log, a record that fails its checksum, a state change in the JSON or
  the positional form that would need a new link, and the files of an
  earlier run. It follows a linked `records.log`, `state.bin` or
  `state-indexes.bin` as chronicle does. A run that stops at any point
  leaves a store that opens as before, or one that open refuses, and
  `--go-back` then undoes the run; after a run that finished it puts the old
  files back while both are there and `records.log` and `state.bin` are as
  the run left them.
- `tools/state-digests.mjs` prints digests of what chronicle's read gives on
  every branch of a store, and its current branch, with no content, so a
  store can be compared with itself before and after the tool. Given the
  tool's `--state` patterns, it leaves out the older reads of the states
  they name. It reads one record at a time, and each branch through a view
  where the binding has one. It exits 1 when a read fails.
