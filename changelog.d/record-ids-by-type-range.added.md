- **Ranged record-ID queries by type.** `getRecordIdsByType(type, options?)`
  takes optional `{ after, before, limit, reverse }`: only IDs greater than
  `after` and less than `before` (both exclusive, and neither need be an ID of
  that type), at most `limit` of them, oldest first or with `reverse` newest
  first. `{ reverse: true, limit: 1 }` gives a type's newest record. A type's
  IDs are kept in ascending order, so the bounds are found by binary search,
  and paging through a large type or asking for its newest record costs what
  it returns rather than a copy of every ID. A malformed bound is rejected
  with `Invalid record ID for after` (or `before`). Without options the call
  is unchanged: every ID of the type, oldest first. In Rust, the same query is
  `Store::get_records_by_type_range`. Nothing about stores changes, so stores
  open as before in either direction.
