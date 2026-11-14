- **Hosts that keep or seed a `blobs/` in a store's directory before its
  first open, or match errors from opening a directory with no
  `MANIFEST`:** a directory with no `MANIFEST` that holds
  `state-indexes.bin`, or a `blobs/` with anything in it but what a fresh
  volume's root holds or `.DS_Store`, is now a damaged store, and a `blobs`
  link is read as what it leads to. `Store::create` refuses it with
  `InvalidOperation`, writing nothing there but its `LOCK`, and
  `Store::open_or_create` (`openOrCreate`) refuses it with `InvalidFormat`,
  writing nothing there; both leave the data. `create` used to make a new
  store there, beside what an earlier one left, and so did `open_or_create`
  over a `blobs/` with data, in place or through a link (#34).
  **Who needs to act:** a host that resets a store by deleting its files and
  keeping `blobs/`, or restores `blobs/` from a backup before the first
  open, now gets that refusal at startup; and a host or Rust caller that
  checks the error `code` or message in the cases below. **Migration:** move
  `blobs/` aside before creating, which loses nothing, since blobs are
  content-addressed; the refusal says so. **Unchanged:** a directory with a
  `MANIFEST` opens as before, since these checks run only where `MANIFEST`
  is absent, and an empty `blobs/`, or one holding only a fresh volume's
  names, is still taken. **Changed codes:**
  - `open` on a directory with no `MANIFEST`, no other data file, and a
    `blobs/` it can't list: `InvalidFormat` becomes `Io`, naming `blobs/`;
    `open_or_create` there used to create a store and now fails the same way.
  - In Rust, `open_or_create` with `create_if_missing: false` over that
    directory: `NotInitialized` becomes `Io`; over a `blobs/` with data,
    `NotInitialized` becomes `InvalidFormat`. (The Node binding always
    creates if missing, so these two don't reach it.)
  - A `blobs` link that leads to nothing, beside no `MANIFEST`: `create`
    and `open_or_create` used to fail with `Io` (`File exists (os error
    17)`, naming no path, leaving a `LOCK`); now `create` refuses with
    `InvalidOperation` and `open_or_create` with `InvalidFormat`, each
    naming the link and its target, as a store file's link to nothing is
    named. `open` there keeps `InvalidFormat`, and its message names the
    link where it said the directory isn't a store.

  **Changed messages:** `open`'s for a directory holding only
  `state-indexes.bin`, or a `blobs/` with data, goes from
  `<dir> has no MANIFEST, so it isn't a Chronicle store` to
  `<dir> has no MANIFEST but holds <what>: a damaged Chronicle store, which
  neither open nor create will take as it is` (for `blobs/`, with
  `; to create a new store here, move blobs/ aside` after it);
  `open_or_create`'s for `state-indexes.bin` alone moves from
  `… so it isn't a Chronicle store or an empty place for one` to the same
  damaged-store wording. `Store::create`'s advice for a `MANIFEST` cut short
  of its header now names `state-indexes.bin` and a `blobs/` with data among
  what must be absent before removing it.
