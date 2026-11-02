- The native module now builds with `lru` 0.18.2 or a later 0.18 release,
  which fixes GHSA-rhfx-m35p-ff5j and RUSTSEC-2026-0253 (#68). chronicle's own
  calls to `lru` reach neither.
