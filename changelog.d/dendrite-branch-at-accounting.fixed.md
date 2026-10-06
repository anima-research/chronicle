- `createBranchAt` no longer loses an inherited Edit, Redact or Set once the
  new branch takes its first delta snapshot. The historical chain head was
  installed with zeroed snapshot accounting, so when the inherited chain held
  a non-append operation that no full snapshot covered yet, the branch's next
  threshold snapshot could be an append-only delta that walked past it: the
  pre-edit value came back, redacted items reappeared, or appends replaced by
  a Set were resurrected. The head is now rebuilt from the chain walk with the
  accounting the parent carried at that sequence (`has_non_append_since_snapshot`,
  delta/full snapshot offsets and counts, `item_count_at_last_full`), so a
  historical branch schedules snapshots exactly as an ordinary `createBranch`
  taken at that moment would have.
  - No on-disk format change. Branches created by earlier versions keep the
    head they were given; a branch that already took a bad delta snapshot is
    not repaired (`tools/scan-delta-poison.py` finds those chains).
  - A historical branch's first full snapshot now follows the parent's
    schedule instead of firing once the branch has grown by its own size at
    the branch point.
