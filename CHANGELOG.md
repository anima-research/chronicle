# Changelog

Notable changes to `@animalabs/chronicle`, loosely following
[Keep a Changelog](https://keepachangelog.com/). Entries land with the change
that causes them, as fragment files in [`changelog.d/`](changelog.d/) that are
folded into a version section at release time — see
[CONTRIBUTING.md](CONTRIBUTING.md#changelog).

Releases up to and including 0.2.7 predate this file; for their contents see
`git log` and the
[releases page](https://github.com/anima-research/chronicle/releases).

## Unreleased

## 0.4.0 — 2026-09-17

### Added

- Added a persisted secondary index on a JSON field (by JSON-pointer path)
  of every item in a state slot, incrementally maintained as the slot
  mutates (append, edit, redact, set/snapshot) and queryable by numeric
  range or string equality, returning matching ordinals rather than item
  content. New napi methods: `registerStateFieldIndex`,
  `queryStateIndexRange`, `queryStateIndexEq`, `getStateIndexValueCounts`.
  Indexes persist to `state-indexes.bin` alongside `state.bin`; a
  missing/stale/corrupt index file is not fatal on open — it starts empty
  rather than failing the store.
  - Query methods return `null` when no such index is registered (never
    registered, wrong kind, or dropped — see below), distinct from `[]`,
    an index that exists but matched nothing.
  - Only one branch's view of a slot can have a live index at a time: a
    write on a different branch than the one an index was registered
    against drops that index rather than mixing ordinals across branches.
    Re-register to rebuild it for the branch you're on.

### Changed

- Changelog entries now land as per-change fragment files in `changelog.d/`
  (`<slug>.<breaking|added|changed|fixed>.md`), folded into the version
  section at release time — concurrent PRs no longer conflict in
  `CHANGELOG.md`. Editing `## Unreleased` directly still works and is merged
  at the same point.

## 0.3.0 — 2026-08-01

### Breaking (on-disk format)

- **`state_update` records are now MessagePack-encoded, and a store that has
  been written by 0.3.0 cannot be read by earlier chronicle.** New records are
  written with `to_vec_named` and `serde_bytes` on the value fields, so
  `StateOperation` payloads travel as raw binary instead of arrays of JSON
  integers.
  - **Who needs to act:** anyone who may need to roll a store back to a
    pre-0.3.0 chronicle. The format change is forward-only — take a copy
    before upgrading if rollback has to stay open.
  - **Migration:** none. Nothing rewrites existing records; the new encoding
    applies to records written from here on.
  - **Unchanged:** existing stores open and read normally. `decode` gates on
    the record's encoding tag and historical JSON payloads (written as
    `RecordInput::raw`) decode through the fallback arm, pinned by a
    handwritten-legacy-payload test. Mixed JSON-history/MessagePack-tail
    stores were verified against a copy of a live 6 GB store. The JSON wire
    format itself is unchanged.
  - JS consumers that previously reached into record payloads with
    `JSON.parse` should move to the new `getStateUpdateJson(id)`, which is
    encoding-agnostic.

### Added

- **`update_state_strategy`** (napi: `updateStateStrategy(registration)`) — the
  explicit upsert leg for snapshot cadence. Registrations persist in
  `state.bin`, so consumers that re-register on boot could never change cadence
  on an existing store: `register_state` errors with `StateExists` and the
  first-registration values won forever. Restricted to the same strategy kind,
  because changing the kind under a live chain would change reconstruction
  semantics for records already on disk; cadence fields steer future snapshot
  scheduling only. `initialValue` is ignored, and `register_state` deliberately
  stays non-upserting.
- **`getStateUpdateJson(id)`** — encoding-agnostic `state_update` reader for JS
  consumers.
- **`materialize_operations`** re-exported at `lib.rs` beside
  `apply_operation`.
- **Scaling gates as CI tests** — `tests/disk_scaling.rs` (on-disk bytes vs op
  count), `tests/reconstruction_scaling.rs` (cold-read latency at worst-phase
  sizes), and `tests/materialize_equivalence.rs` (differential
  fold-vs-single-pass equivalence, including a 2000-case fuzz). Plus manual
  probes: `tests/perf_probe.rs` for fixed-size per-op numbers and
  `examples/store_probe.rs` for measuring against a store copy.
- The criterion bench suite compiles again — it had imported the crate under
  its pre-rename name (`record_store`) since the initial release — and now
  covers point reads, JSON appends and tree ops.

### Changed

- **Full-snapshot spacing is size-aware** (#11). Fulls fired on a fixed
  `delta × full` op interval and embed the whole state, so growing AppendLog
  and Tree states paid O(N²/2K) disk and O(N/K) append latency. The interval
  now grows with the state — a full fires once the ops covered since the last
  full reach the item count at that full — with the configured interval as a
  floor, so small states keep their configured cadence exactly. Total snapshot
  bytes telescope to ≤ 2× final state size. JSON appends measured 77 µs →
  1,709 µs per op across 1k → 50k accumulated entries under the old cadence;
  they are now flat at ~20 µs regardless of accumulated size.
- **Reconstruction is single-pass.** Reopen, cache miss, branch switch and time
  travel folded `apply_operation` over the op chain, re-parsing and
  re-serializing the entire state per op — O(tail × N). `materialize_operations`
  decodes the base once, applies the tail in place, and encodes once —
  O(state + ops), with semantics preserved exactly. Cold reads went from
  22.4 → 10.7 ms and 259.7 → 44.2 ms at 3.9k/15.9k appends (11.59× → 4.13×
  per 4× N: linear).
- **Edits no longer force early full snapshots.** With tails cheap to traverse,
  the `has_non_append` forced-full is retired: a non-Append op still blocks
  delta snapshots, but the raw tail now rides until the size-aware full fires.
  The old behavior cost O(N²/D) disk on any growing log taking regular edits —
  the summary-merge pattern measured 439 MB of store for 16k logical ops,
  now 18.2 MB (24×).
- **Point lookups of the last item are O(1).** Every state write pops the LRU
  caches, and a write-through consumer's lookup of the item it just appended
  re-materialized the whole state inside every append. When the head record is
  an Append and the caller asks for the last index, the item is now served from
  the head record alone. Against a copy of a live store (19,767 messages, 33 MB
  state, 6 GB log), append+lookup went 421 ms → 94 µs.
- **Tree point reads are cached in decoded form.** `tree_get`/`tree_list`
  deserialized the entire path→entry map per call — 16.7 µs → 2.28 ms across
  100 → 10k entries. The state manager now caches the decoded `TreeState` per
  `head_offset` (same key scheme and staleness discipline as the per-item
  cache); `tree_get` is flat at ~150 ns at all measured sizes. The trade is
  that the decoded-tree LRU is bounded by entries, not bytes.
- Tree op counts are tracked as an upper bound (overwrites count as inserts,
  corrected at each full snapshot) — sufficient for spacing, and it only ever
  spaces fulls further apart.

### Fixed

- **Snapshot-strategy states no longer walk the entire chain on cold reads.**
  `reconstruct_from_disk` / `get_state_at` / `find_chain_info_at` broke the
  backward walk only on `Snapshot` records; `Set` fell through to the catch-all
  and the walk continued to sequence 0. Snapshot-strategy states are written
  via `Set` and never get a periodic `Snapshot` record, so cold reads
  deserialized every full-state `Set` back to the beginning and discarded all
  but the last — chains up to 3,928 deep across an 11.7 GB log, measured at
  6–8 minutes to boot. `Set` replaces the whole state exactly like `Snapshot`,
  so the newest one is a valid terminal; these reads are now O(1).
- `CompactionStats::ops_since_last_full_snapshot` counted ops plus delta count
  instead of ops (deltas × `delta_snapshot_every` + raw ops), undercounting
  roughly 100×.
- Interval-floor arithmetic in both strategy arms uses `saturating_mul`; an
  overflowing config previously wrapped.
- Legacy indexes deserialize the new head fields to 0 and keep the configured
  cadence until the first full snapshot stamps a baseline. `StateChainHead`'s
  positional wire format is now documented and guarded against both the v2
  (pre-#11) and v1 (pre-`item_count`) layouts.
