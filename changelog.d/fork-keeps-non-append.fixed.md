- A branch made with `createBranchAt` no longer brings back items redacted
  before its point or replaced there by a `Set`, and no longer undoes an Edit
  made there. The branch started as if its append-log state had seen only
  Appends since its last snapshot, so its first delta snapshot walked past
  such a change, and from then on the branch read back what the change had
  removed or replaced. The branch now inherits the parent's record of such a
  change, and takes no delta snapshot until its next full one, just as the
  parent takes none. A branch made before this release may have that record
  wrong: when its delta snapshot comes due, the store now takes a full
  snapshot of its value instead. One that already took its first delta
  snapshot still holds what it brought back; a branch made again at the same
  point reads the parent's value.
- A `Redact` whose range ends at or past 2^32 now leaves a field index as it
  leaves the value. The index narrowed the range to 32 bits before clamping
  it to its length, so a wrapped end removed or kept the wrong entries.
