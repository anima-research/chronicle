- For an `append_log`, `getStateLen` now gives the number of items in the
  list, as a read returns them. It gave the count that the store keeps, and
  that count can be wrong. Releases 0.1.0 to 0.4.x left wrong counts in some
  stores. A `DeltaSnapshot` written from Rust, or a damaged branch, can leave
  one too. For example, the count was 0 for the list `[0, 1, 2]`. The store
  now checks each list's count once. The first `getStateLen` or batch on a
  branch reads an unchecked list whole, in the calling thread. The read throws
  on an I/O error (`Io`), a record that fails its checksum
  (`ChecksumMismatch`) or a record whose framing is bad (`InvalidFormat`). A
  list whose value can't be rebuilt as a JSON array, such as one that holds an
  item that isn't JSON, has no items to count, so `getStateLen` gives the
  count that the store keeps. `getStateItemJson` on an unchecked list counts
  it first in the same way. Before, at the stale count's last index, it
  returned the list's last item. `listStates()` still gives the count that the
  store keeps. For a state of another strategy, `getStateLen` still gives that
  count too.
- The store saves its checks in `state.bin`, after the index, at each `sync()`
  and `close()`. A list that the store counted before a sync isn't read for
  its count again after an open. A later write, such as a `DeltaSnapshot`
  written from Rust, can leave the head unchecked, and the list is then read
  again. After an open, a saved check also lets `getStateItemsJson` read a
  list's last item from its record alone. So does `getStateItemJson` after a
  batch. Before, they did so only once a session had checked the count. That
  read meets no earlier item, so an earlier item that isn't JSON no longer
  makes it throw. Earlier releases read only the index in `state.bin`, so they
  can open a store that this release wrote. This release opens a store that
  has no checks, and counts each of its lists at first use. So after the
  update, each list's first `getStateLen` or batch blocks the calling thread
  for that read.
- `tools/compact-set-states.py` reads a `state.bin` that holds the checks, and
  writes the new one without them. An earlier copy of the tool refuses such a
  `state.bin` and changes nothing. It says that `state.bin` "is not the length
  its index says".
