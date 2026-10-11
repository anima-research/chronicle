- **Ranged record-ID queries by type.** `getRecordIdsByType(type, options?)`
  takes an optional `JsRecordIdsByTypeOptions`, `{ after?, before?, limit?,
  reverse? }`: only IDs greater than `after` and less than `before` (both
  exclusive decimal-string record IDs, and neither need be an ID of that
  type), at most `limit` of them (a number), oldest first or, with `reverse:
  true`, newest first. A type's IDs are kept in ascending order, so the bounds
  are found by binary search, and paging through a large type costs what it
  returns rather than a copy of every ID. Without options the call is
  unchanged: every ID of the type, oldest first. In Rust, the same query is
  `Store::get_records_by_type_range`.
  - **A type spans branches.** The type index isn't per branch, so
    `{ reverse: true, limit: 1 }` is the type's newest record on any branch.
    A record read from JS doesn't carry its branch, so a caller that wants a
    branch's newest records the branch in its own payload, reads the type
    back in reverse pages (`reverse`, a `limit` and a `before` cursor), and
    filters on that. Deleting a branch removes only its entry in the branch
    list: its records stay in the log and stay listed by type, so a reverse
    page can still return them.
  - **Refusals.** A bound string that isn't a record ID is rejected with
    `Invalid record ID for after` (or `before`), and a `limit` that isn't a
    non-negative integer with `Invalid limit`; a limit past what an index can
    hold returns every ID. A value of another type (a numeric bound, a string
    limit) fails the binding's own conversion first, with its message.
  - **Cursors and crashes.** IDs lost to a torn tail are issued again after
    recovery, so an `after` or `before` cursor kept across a crash can pass
    over, or name, a different record from the one it was taken from.
  - **Version skew.** A caller passing options needs a chronicle that has
    them: an older binary ignores the extra argument and answers every ID,
    oldest first, without an error.
  - Nothing about stores changes, so stores open as before in either direction.
