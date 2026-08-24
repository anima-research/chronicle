## Problem

<!-- What is wrong / missing, and for whom. Link the issue if one exists. -->

## Changes

<!-- What this PR does. For cross-repo or stacked work, list companion PRs
     and merge-order guidance ("safe in either order because…"). -->

## Tests

<!-- Evidence, not assertion: paste the numbers.
     e.g. `cargo test`: 19 passed / 0 failed; `npm test` smoke green.
     `npm test` loads the built .node — build before testing. -->

## Compatibility

<!-- Does this touch the record log, blob layout, snapshot encoding or wire
     format? If so: do existing stores still open, and does a store written
     by this branch still open on current main? "No format change" is a fine
     answer. -->

## Not verified

<!-- Store size exercised, whether recovery / torn-tail paths were hit, which
     targets you built. "Nothing — full suite plus a real store" is a fine
     answer; silence is not. -->

---

- [ ] Changelog fragment added — `changelog.d/<slug>.<breaking|added|changed|fixed>.md`
      (see `changelog.d/README.md`) — or this change is internal-only /
      test-only / docs-only (apply the `no-changelog` label).

<!-- AI-assisted contributions are welcome and normal here — see
     CONTRIBUTING.md for the attribution convention (footer + Co-Authored-By). -->
