# Changelog — 0.2.x maintenance line

Notable changes to `@animalabs/chronicle` on the `v0.2-maint` branch,
loosely following [Keep a Changelog](https://keepachangelog.com/). The
`main` branch's changelog documents 0.3.0 and later; this file documents
maintenance releases for stores that must remain on the 0.2.x on-disk
format. Releases up to and including 0.2.7 predate changelogs — see
`git log` and the
[releases page](https://github.com/anima-research/chronicle/releases).

## 0.2.8 — Unreleased

Backport release (issue #15): the issue #11 snapshot-spacing relief
without the 0.3.0 on-disk format change, so existing stores can adopt it
as an ordinary dependency upgrade — no migration, no loss of rollback.

### Fixed

- **Size-aware full-snapshot spacing** (issue #11, PR #12). Full
  snapshots fire on state-size doubling instead of a fixed interval, so
  total snapshot bytes stay amortized-linear in state size. On a
  pre-existing store the first full after upgrade fires at the old
  cadence and stamps the baseline; from then on fulls become
  exponentially rarer. Use `update_state_strategy` (below) to retune an
  already-large store up front instead of paying that first full at the
  legacy interval.
- **Single-pass state materialization** (PR #13) — cold reconstruction
  is `O(state + ops)` instead of `O(tail × N)`.
- **`Set` treated as a full-state terminal in chain reconstruction**
  (70e8729).

### Added

- **`update_state_strategy`** (napi: `updateStateStrategy(registration)`)
  — backported from 0.3.0: retune snapshot cadence on an existing store.
  Same strategy kind only; cadence fields steer future snapshot
  scheduling only; `initialValue` is ignored; `register_state` stays
  non-upserting.

### On-disk format — unchanged, by construction

- `state_update` records remain JSON, and `state.bin` keeps the exact
  0.2.6 head layout. The spacing baseline is persisted in a new sidecar
  file, `state.bin.baselines`, which older versions ignore. **A store
  written by 0.2.8 remains fully readable and writable by
  0.2.6/0.2.7** — rolling back is a plain dependency downgrade. The
  stale sidecar a rollback leaves behind only re-seeds snapshot cadence
  and is re-stamped at the next full snapshot; a missing or corrupt
  sidecar likewise degrades to the legacy cadence, never to a failed
  open.
- A `state.bin` written by a bare 0.3.0 **open** (eight-element heads)
  now opens on 0.2.8 and is healed back to the legacy layout on the
  next save. A store 0.3.0 has **written records into** remains
  0.3.0-only — that direction is 0.3.0's documented format break, not
  something this line can read.
- Verified by `scripts/cross-version-compat.sh`, which round-trips one
  store between a real v0.2.6 build and this tree (open, materialize,
  time-travel, branch, keep writing, in both directions), plus
  strict-layout unit tests in `src/state/manager.rs`.

### Release mechanics

- `publish.yml` on this branch publishes under the `maintenance` npm
  dist-tag, so 0.2.x releases never move `latest` (which 0.3.0+ owns).
