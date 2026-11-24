- For an `append_log`, `getStateLen` now gives the number of items in the
  list, as a read returns them. It gave the count that the store keeps, and
  that count can be wrong. Releases 0.1.0 to 0.4.x left wrong counts in some
  stores. A `DeltaSnapshot` written from Rust, or a damaged branch, can leave
  one too. For example, the count was 0 for the list `[0, 1, 2]`. The store
  now checks each list's count once. The first `getStateLen` or batch on a
  branch reads an unchecked list whole, in the calling thread. The read throws
  where it meets a record that it can't read, such as one that fails its
  checksum (`ChecksumMismatch`). A list whose value isn't a JSON array has no
  items to count, so `getStateLen` gives the count that the store keeps.
  `listStates()` still gives the count that the store keeps. For a state of
  another strategy, `getStateLen` still gives that count too.
- The store saves its checks in `state.bin`, after the index, at each `sync()`
  and `close()`. A list that the store counted before a sync isn't read for
  its count again after an open. A later write, such as a `DeltaSnapshot`
  written from Rust, can leave the head unchecked, and the list is then read
  again. Earlier releases read only the index in `state.bin`, so they can open
  a store that this release wrote. This release opens a store that has no
  checks, and counts each of its lists at first use.
- `tools/compact-set-states.py` reads a `state.bin` that holds the checks, and
  writes the new one without them. An earlier copy of the tool refuses such a
  `state.bin` and changes nothing. It says that `state.bin` "is not the length
  its index says".
