- The first batched append to a state the store opened with
  (`appendManyToStateJson`, `appendManyToStateJsonWithIdentity`, and
  `Store::append_many_to_state` and its JSON forms) counts the state's
  items before it takes its ordinals, reading the state's whole value once
  under the write lock. The count a release before 0.5.0 stored can be off
  (after a Redact past the end, or a Delta or an empty set that replaced
  the value), and `getStateLen` reports a stored count as it is. The
  batch's `firstOrdinal`, `getStateLen` after it, and the index a later
  `editStateItem` is checked against now follow the state's items, where
  they followed that count. In Rust, a `DeltaSnapshot` a caller writes
  through `update_state` leaves the count as it was, and the next batch
  counts the items again the same way. A value that isn't a JSON array,
  or holds an item that isn't JSON, has no items to count: the batch takes
  the stored count. A record of the state's chain that fails to read (an
  I/O error, a failed checksum, a record whose framing is bad) fails the
  batch with that error, writing nothing.
