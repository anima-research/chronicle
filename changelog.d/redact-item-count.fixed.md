- A `Redact` whose range runs past the end of an append-log state now
  counts only the items it removes. Its count clamped only the length of the
  range, so `Redact { start: 4, end: 100 }` on five items set the count to 0
  where the value kept four: `getStateLen` reported 0, an Edit of an item
  that exists was refused as out of bounds, and after a later append
  `getStateItemJson` could return that later item for an earlier index. A
  state whose stored count is already wrong can stay wrong until its next
  full snapshot or array `Set`, either of which counts the items again;
  `compactState` takes a full snapshot.
