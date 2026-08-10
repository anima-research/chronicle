# Contributing to chronicle

chronicle is the storage layer of the Connectome ecosystem
([agent-framework](https://github.com/anima-research/agent-framework),
[membrane](https://github.com/antra-tess/membrane),
[context-manager](https://github.com/anima-research/context-manager),
[connectome-host](https://github.com/anima-research/connectome-host)). These
conventions describe how work actually lands here — they codify existing
practice rather than aspiration. When in doubt, recent merged PRs are the best
reference.

Everything below applies to every change however it lands — external PR or
maintainer direct push — and to human and AI authors identically. There is
no separate rulebook for either.

## How changes land

- External contributions come as PRs against `main`, from a fork or a repo
  branch. Maintainers also land small changes directly on `main`; don't be
  surprised by history that never saw a PR.
- Branch names: `feat/<kebab-case>`, `fix/<kebab-case>`, `docs/`, `chore/`.
  Descriptive bare names (`fix-branch-at-state`) are also common here.
- PRs are merged as **true merge commits** — no squash, no rebase-merge.
  Because nothing is squashed, keep individual commits coherent.
- To update a stale branch, rebase onto `main` or merge `main` in; both are
  accepted.
- Stacked PRs and cross-repo companion PRs are fine, but **declare them** in
  the body with merge-order guidance ("stacked on #7 — review that first";
  "safe to merge in either order because …"). Everything in the ecosystem
  sits on this package, so a change here can require companion work in
  context-manager or agent-framework; say which side is safe to land first.

## What a PR should contain

Body shape (the PR template mirrors this): **Problem / Changes / Tests**,
plus, when applicable, **Not verified**, **Out of scope**, and
**Companion PRs**. The conventions that matter:

- **Evidence over assertion.** State the test baseline numerically:
  "`cargo test`: N passed / 0 failed; `npm test` smoke green." A claim like
  "all tests pass" without the count will be re-verified anyway, so save the
  reviewer the trip.
- **Say what you did NOT verify.** This is a persistence layer: the failures
  that matter are the ones that only appear on real stores, across a crash,
  or on another platform. Be explicit about what you exercised — store size,
  whether recovery/torn-tail paths were hit, which targets built — and what
  you did not.
- **Format and on-disk compatibility is the sharp edge.** If a change alters
  the record log, blob layout, snapshot encoding, or wire format, say
  whether existing stores still open, and whether a store written by the new
  code still opens on the old. Tests accompany behavior changes, and review
  scrutinizes test substance, not mere presence — a test that can't fail on
  the unfixed code will be called out.
- **Changelog entry** under `## Unreleased` for anything behavior-affecting
  (see below).

Conventional-commit-style titles (`feat(state): …`, `fix(blobs): …`) are
recommended but not required — much of this repo's history predates the
habit, and plain descriptive titles are perfectly normal here.

## Review process — what to expect

- Review arrives as **ordinary PR comments**, not GitHub review approvals —
  the comment thread is the gate. Reviews are frequently AI-generated and
  explicitly labeled as such, with a severity verdict and itemized findings.
- The reviewer will typically **run your branch** (`cargo test`, the napi
  build, the node smoke test, sometimes opening a real store with the
  inspector tools) and paste transcripts. Claims are checked, not trusted.
- Respond by pushing fix commits and replying per finding — "Addressed in
  `<sha>`" — rather than force-pushing a rewritten branch. A re-review then
  flips the verdict.
- Maintainers may push small review fixes **directly to your branch** to keep
  things moving. Say so in the PR body if you'd rather they didn't.
- PRs are never closed silently: a closed PR gets a one-line disposition
  comment (usually supersession by another PR).

## AI-assisted contributions

AI-written code is the norm in this ecosystem, welcome from everyone, and
held to exactly the same evidence standards as anything else. Declare it the
way we do:

- the `🤖 Generated with [Claude Code](https://claude.com/claude-code)`
  footer (or equivalent for your tooling) in the PR body, and
- a `Co-Authored-By:` trailer naming the model in commits.

What earns an automated contribution a changes-requested review is not being
AI-generated — it's arriving without the suite having been run, with tests
that don't fail on unfixed code, or with claims the branch itself disproves.

## Changelog

`CHANGELOG.md` keeps a standing `## Unreleased` section with
`### Breaking` / `### Added` / `### Changed` / `### Fixed` subsections
(loosely [Keep a Changelog](https://keepachangelog.com/)).

- **The entry lands with the change** — same commit, or at least the same
  PR. This binds direct pushes to `main` just as much as PRs. On PRs, CI
  enforces it softly: touching `src/` without touching `CHANGELOG.md` fails
  the `changelog` check unless the `no-changelog` label is applied.
- **What needs an entry:** anything a consumer would notice — the napi
  surface, store/branch/state semantics, on-disk or wire format, recovery
  behavior, performance characteristics that change how callers should use
  it, packaging (which platform binaries ship), defaults. Internal refactors,
  test-only, and docs-only changes don't.
- **Breaking entries are audience-scoped.** Name the audience in the heading
  (`### Breaking (on-disk format)`) and cover: **who needs to act**,
  **migration**, and **unchanged** (what readers might fear broke but
  didn't). For this package that last line carries real weight: say plainly
  whether existing stores keep opening, because that is the first thing
  every reader wants to know.
- **Keep one `## Unreleased` heading.** Add entries under the existing one;
  don't open a second. Only the first is cut at release time, so entries
  filed under a later heading are silently never released — the release
  script refuses to run if it finds more than one.
- **Releases** (maintainers): `npm version <patch|minor|major>` does the
  whole cut — the `version` hook retitles `Unreleased` to
  `## X.Y.Z — YYYY-MM-DD` (keeping a fresh `Unreleased` above it, and
  refusing to release when there are no entries), then npm commits and tags.
  `git push --follow-tags` triggers CI, which cross-builds the native module
  for all five targets, refuses a tag with no matching changelog section,
  publishes `@animalabs/chronicle` to npm, and creates the GitHub release
  with that section as its notes. The two release jobs are independent: some
  consumers run github-clone checkouts, so release notes must exist even
  when npm publish fails. Version bumps are a maintainer release-time
  action, not part of feature PRs.

## Building and testing

```bash
npm ci                                          # strict lockfile install
cargo test                                      # Rust suite
npx napi build --platform --features napi-bindings   # native module (debug)
npm test                                        # node smoke test (test.mjs)
npm run build                                   # release build of the above
```

Two things about the test setup are easy to trip over:

- **`cargo test` runs without the `napi-bindings` feature.** napi symbols
  only resolve inside a Node process, so test executables can't link against
  them — only the cdylib can. The napi surface is therefore covered by the
  napi build plus the node smoke test, not by `cargo test`.
- **`npm test` loads the built `.node`**, so build before testing and after
  switching branches, or you will be testing the previous artifact.

`Cargo.lock` is deliberately gitignored; `package-lock.json` is committed and
CI installs it with `npm ci`, which unlike `npm install` fails loudly on a
lock that is broken or out of sync.

Push-time CI (`ci.yml`) runs the Rust suite, the debug napi build and the node
smoke test on every push and PR (ubuntu only — the cross-target matrix runs at
release time).

Binaries ship inside the single published tarball. The platform packages that
`napi prepublish` would name were never published, and a manifest declaring
them as `optionalDependencies` breaks `npm ci` for every consumer on npm >= 11,
so the release workflow fails if they reappear — don't add them back.

Stores are binary; use the inspector tooling in `tools/` and `ui/` rather than
reading them by hand. `docs/loom-of-looms.md` is the algebraic spec.
