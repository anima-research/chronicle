- Batched calls in the Node API, each one call where the single forms take
  one per item:
  - `appendManyToStateJson(stateId, items)` appends several JSON items to an
    append-log state as one record, at one sequence (an `AppendMany`), and
    returns `{ record, firstOrdinal }`: the record, and the index the first
    item took, so item `i` is at `firstOrdinal + i` without a read. That
    index is the state's length just before the batch, read under the write
    lock. Reads see the items as if each were appended with
    `appendToStateJson`, except that a historical read or a branch cut by
    sequence keeps or drops the batch whole, as it has one sequence. An
    empty `items` throws and writes nothing. The
    first batch a store records moves its `MANIFEST` to version 2 (see
    Breaking).
  - `getStateItemsJson(stateId, ordinals)` returns, for each index in the
    order given, what `getStateItemJson` returns for it: `null` for an
    index that is negative or out of range, for a state with no value, and
    for a state whose value isn't an array (see Changed). Indices may
    repeat and come in any order. All the answers come from one version of
    the state, which a cold cache materializes at most once for the call,
    not once per index. A request with no index of zero or more (an empty
    one, or one of negative indices only) reads nothing. An index past the
    end costs what it costs `getStateItemJson`: on a cold cache, the one
    materialization that finds the end.
  - `getRecords(ids)` returns, for each ID in the order given, what
    `getRecord` returns for it, or `null` for an ID with no record. The IDs
    are looked up together and the records read under one hold of the log
    file. A malformed ID throws before anything is read.
  - A batch reaches subscribers as one `state_delta` event, whose
    `operation` is `{"AppendMany": [[...], ...]}` with each item an integer
    array, as an `Append`'s item is. The `JsStoreEvent.data` doc now says so.
- Rust: `Store::append_many_to_state(_on)` and
  `append_many_to_state_json(_on)` return an `AppendedBatch { record,
  first_ordinal }`; `Store::get_state_items_by_ordinal(_on)` and
  `StateManager::get_state_items_by_ordinal` read several items of a state
  in one call; `Store::get_records` reads several records by ID, through
  `RecordIndex::get_offsets_by_id` and `RecordLog::read_many_at`.
