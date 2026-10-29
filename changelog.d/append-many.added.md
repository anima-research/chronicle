- `StateOperation::AppendMany(Vec<Vec<u8>>)` (Rust): appends several items to
  an append-log state as one record, at one sequence, through
  `update_state`. These reads see what the same items appended one `Append`
  at a time would give: the value, `get_state_len`, tails, slices, point
  lookups, iteration, delta snapshots, field indexes (each item at its own
  ordinal), and replay after a crash, which keeps or drops a batch whole. A
  historical read or a fork cut by sequence keeps or drops it whole too,
  since it has one sequence. It counts as one operation toward the
  snapshot cadence, as a `TreeBatch` does, and is append-only, so delta
  snapshots go on as for single appends. The per-item cache advances through
  a batch without re-materializing, and the head-record fast path serves its
  last item. `update_state` refuses an empty batch with `InvalidOperation`.
  A state-delta event, and `getStateUpdateJson`, carry it as
  `{"AppendMany": [[...], ...]}`, each item an integer array as an `Append`'s
  is. Storing one changes the store's format; see Breaking.
