- `appendManyToStateJsonWithIdentity(stateId, items, idField, sequenceField)`
  in the Node API is the batched form of `appendToStateJsonWithIdentity`, as
  `appendManyToStateJson` is of `appendToStateJson`. It appends several JSON
  objects to an append-log state as one record, at one sequence (an
  `AppendMany`), and returns `{ record, firstOrdinal }`. Before the record is
  written, each item gets that record's id (a string, as `JsRecord.id`) at
  `idField` and its sequence (a number) at `sequenceField`, overwriting what
  was there, exactly as the single form stamps its one item. The batch is
  one record, so every item of it carries the same id and sequence, and a
  branch cut by an item's sequence keeps or drops the whole batch. Reads see
  the stamped items as if each had been appended alone, and field indexes
  take each at its own ordinal. An empty `items`, or an item that isn't an
  object, throws and writes nothing, and leaves the `MANIFEST` alone. Like
  any batch, the first one a store records moves its `MANIFEST` to version 2
  (see Breaking); a store that only ever takes single identity appends keeps
  version 1. In Rust: `Store::append_many_to_state_json_with_identity(_on)`,
  which returns an `AppendedBatch`.
