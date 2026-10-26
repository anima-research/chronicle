- A branch made with `createBranchAt` no longer brings back items redacted
  before its point or replaced there by a `Set`, and no longer undoes an Edit
  made there. The branch started as if its append-log state had seen only
  Appends since its last snapshot, so its first delta snapshot walked past
  such a change, and from then on the branch read back what the change had
  removed or replaced. The branch now takes that record from the chain at
  its point, and takes no delta snapshot until its next full one, as the
  parent does. A branch made before this release may have that record
  wrong: when its delta snapshot comes due across such a change, the store
  now takes a full snapshot of its value instead.
- A branch that already took such a delta snapshot under an earlier release
  keeps reading what it read there. After a Redact or a `Set`, that reading
  holds items the change had removed, while `getStateLen` counts without
  them, so a read bounded by the count, such as `getStateSlice` up to the
  length, misses the newest items until the branch's next full snapshot,
  which then keeps that reading and counts it. Until then, `getStateLen`
  short of the length of `getState`'s value marks such a branch, though a
  later Redact that runs past the count can bring the two level without
  removing those items. After an Edit, the edit stays lost, and nothing
  marks the branch. A later Redact or Edit applies to that reading as under
  earlier releases, which also refuse an Edit at or past the count.
- `getStateTail` no longer returns some items twice. Once it had taken a
  delta snapshot's items and needed more, it read on into the Appends that
  snapshot stood for and returned them again: on a state taking a delta
  every two operations, appends 0 to 4 gave a tail of 4 as `[3, 2, 3, 4]`.
  It also returns at most the items asked for where the stored count is
  short of the value.
- A `Delta` on an append-log state now holds delta snapshots off as a
  Redact, an Edit or a Set does, so the next delta no longer walks past the
  value it replaced, and `iter_state_items` applies it as other reads do,
  in place of passing over it. The Node binding exposes neither.
- A `Redact` whose range ends at or past 2^32 now leaves a field index as it
  leaves the value. The index narrowed the range to 32 bits before clamping
  it to its length, so a wrapped end removed or kept the wrong entries.
