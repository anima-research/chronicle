- `appendManyToStateJsonWithIdentity(stateId, items, idField, sequenceField)`
  in the Node API is the batched form of `appendToStateJsonWithIdentity`, as
  `appendManyToStateJson` is of `appendToStateJson`. It appends several JSON
  objects to an append-log state as one record, at one sequence (an
  `AppendMany`), and returns `{ record, firstOrdinal }`. Before the record is
  written, each item gets that record's id (a string, as `JsRecord.id`) at
  `idField` and its sequence (a number) at `sequenceField`, overwriting what
  was there, as the single form stamps its one item. The batch is one
  record, so every item of it carries the same id and sequence, and a
  branch cut by an item's sequence keeps or drops the whole batch. Reads see
  the stamped items as if each had been appended alone, and field indexes
  take each at its own ordinal. An empty `items`, an item that isn't an
  object, or an `idField` equal to `sequenceField` (an item holds one value
  at a key, so it couldn't carry both the record id and the sequence)
  throws and writes nothing, and leaves the `MANIFEST` alone. (The single form takes equal
  names, and leaves the sequence at that key, over the id.) Once the batch
  is recorded the call returns it: a failure of the snapshot the write
  takes after its record goes into the store's snapshot-failure report
  (`snapshotFailures()`) and is tried again by the next write, not thrown (see Fixed, #39). Like any
  batch, the first one a store records moves its `MANIFEST` to version 2;
  a store never asked to record a batch keeps version 1 (a batch that
  fails after its mark still leaves version 2; see Breaking). In Rust:
  `Store::append_many_to_state_json_with_identity(_on)`, which returns an
  `AppendedBatch` and refuses those with `InvalidOperation`.
