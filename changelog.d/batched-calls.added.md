- Batched calls in the Node API, each one call where the single forms take
  one per item:
  - `appendManyToStateJson(stateId, items)` appends several JSON items to an
    append-log state as one record, at one sequence (an `AppendMany`), and
    returns `{ record, firstOrdinal }`: the record, and the index the first
    item took, so item `i` is at `firstOrdinal + i` without a read. That
    index is the state's length just before the batch, as `getStateLen`
    counts it once the batch has counted the items, taken under the write
    lock. Reads see the items as if each
    were appended with `appendToStateJson`, except that a historical read or
    a branch cut by sequence keeps or drops the batch whole, as it has one
    sequence. An empty `items` throws and writes nothing. The first batch a
    store records moves its `MANIFEST` to version 2 (see Breaking).
    **Migration:** a store last written by chronicle 0.1.0 to 0.4.x can hold
    an append-log state whose stored count is wrong (see 0.5.0's Fixed): one
    whose chain holds, from those builds, a `Redact` running past the end, a
    `Delta` that replaced the value, or an empty `Set` or `Delta`. Such a
    count stays as it is until a full snapshot, or an array or empty
    `setState`, sets it again. For each state the store already held when
    it was opened, and each a `createBranchAt` branch starts with, this
    release counts the items at the state's first batch on each branch
    (`appendManyToStateJson`, `appendManyToStateJsonWithIdentity`, or
    `Store::append_many_to_state` and its forms in Rust), reading the
    state's whole value once, before the batch takes the write lock, so
    other writers wait only for each record it reads, not the whole count;
    if another write to the state lands during the count, the batch counts
    again under the write lock. That batch's `firstOrdinal`,
    `getStateLen` after it, and the index a later `editStateItem` is
    checked against then follow the state's items. A value that isn't a
    JSON array, or holds an item that isn't JSON, has no items to count:
    the batch takes the stored count. A record of the state's chain that
    fails to read (an I/O error, a failed checksum, a record whose framing
    is bad) fails the batch with that error, writing nothing; the error
    says the batch was not recorded, except a failed checksum's, which
    carries no message of its own. In Rust, a
    `DeltaSnapshot` a caller writes through `update_state` leaves the count
    as it was, and the next batch counts the items again the same way.
    `compactState` before the batch takes a full snapshot, which recounts
    them too.
  - `getStateItemsJson(stateId, ordinals)` returns, for each index in the
    order given, what `getStateItemJson` returns for it (except at the last
    index of a head whose count this session hasn't found exact (unchecked, or
    checked on a value that doesn't materialize), where `getStateItemJson`
    answers from the stored count and this from the value): `null` for an
    index that is negative or out of range, for a state with no value, and for
    a state whose value isn't an array (see Changed). Indices may repeat and
    come in any order. All the answers come from one version of the state,
    which a cold cache materializes at most once for the call, not once per
    index. A request with no index of zero or more (an empty one, or one of
    negative indices only) reads nothing. An index past the end costs what it
    costs `getStateItemJson`: on a cold cache, the one materialization that
    finds the end.
  - `getRecords(ids)` returns, for each ID in the order given, what
    `getRecord` returns for it, or `null` for an ID with no record. The IDs
    are looked up together and the records read under one hold of the log
    file, so a long list holds other reads and appends of the store until
    it is read; split it to let them in. A malformed ID throws before
    anything is read.
  - A batch reaches subscribers as one `state_delta` event, whose
    `operation` is `{"AppendMany": [[...], ...]}` with each item an integer
    array, as an `Append`'s item is. The `JsStoreEvent.data` doc now says so.
- Rust: `Store::append_many_to_state(_on)` and
  `append_many_to_state_json(_on)` return an `AppendedBatch { record,
  first_ordinal }`; `Store::get_state_items_by_ordinal(_on)` and
  `StateManager::get_state_items_by_ordinal` read several items of a state
  in one call; `Store::get_records` reads several records by ID, through
  `RecordIndex::get_offsets_by_id` and `RecordLog::read_many_at`.
