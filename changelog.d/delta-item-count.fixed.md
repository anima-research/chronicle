- A `Delta` that replaces an append-log state's value with an array now sets
  the state's count to that array's length, as a `Set` does, and a `Set` or
  `Delta` with no bytes, which later array operations treat as an empty
  array, sets it to 0. Both left the count as it was, so afterwards
  `getStateLen` reported the old length, an Edit was checked against it,
  after a later append `getStateItemJson` could return that item for the
  wrong index, and after an empty `setState` a `getStateTail` of at least
  one item but fewer than the old length threw. Only Rust callers can write
  a `Delta`; an empty `setState` reaches the second case from Node. A state
  whose stored count is already wrong stays wrong until its next full
  snapshot or an array or empty `setState` (from Rust, also such a `Delta`);
  `compactState` takes a full snapshot.
