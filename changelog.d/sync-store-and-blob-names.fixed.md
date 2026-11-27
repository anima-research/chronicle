- On Unix, `sync()` and `close()` now sync the directories that hold the
  store's directory, up to the root of its filesystem or to the first one
  that this process may not look at. They do it on the first call on each
  open handle. Before, a power loss after `sync()` could lose a new store.
  New parent directories could be lost the same way.
- That first call also syncs the store's directory, `blobs/` and each
  shard directory. Before, a power loss could lose a blob whose shard was
  never synced.
- A store with all 256 shards gets up to 258 more directory syncs on a
  handle's first `sync()`, plus one for each directory that holds the
  store. On macOS these use `fsync`, and the pass then asks each drive to
  write back its cache once (`F_FULLFSYNC`), not once for each directory.
  Its cost on a Mac's own disk isn't measured.
- `sync()` and `close()` also retry a shard sync that failed in a blob
  store. Before, `sync()` skipped such a directory.
- When a directory sync fails, `sync()` and `close()` throw an error with
  the `code` `Io`. The next `sync()` tries it again. After `close()`
  throws, the reopened store's first `sync()` also tries again. Appends,
  state writes and blob stores go on meanwhile. On ext4, a retry just
  after a failed journal commit can succeed although the name never
  reached the disk, so a successful retry doesn't prove that it did.
- Until the first `sync()` or `close()`, a power loss can take a new
  store's directory and the parents that `create` made, even after a
  registration or a branch change has saved its checkpoint.
- As with a checkpoint save, `sync()` skips a directory that this process
  may not open. On a system that doesn't sync directories, it skips them
  all. It doesn't sync the name of a symbolic link on the store's path,
  and the first-time pass doesn't follow a link in `blobs/`. Nothing on
  disk changes, and stores open as before.
- The errors of a failed blob directory sync now end with the system's
  error, as `IO error: … (os error N)`, so a caller can read the error
  number. The first one also names the directory whose sync failed. These
  are the new messages:
  - `cannot sync …, so … wasn't written, and storing the blob again or the
    next sync tries again: …`
  - `the blob … is stored, but its directory isn't synced yet, and storing it
    again or the next sync tries again: …`
  - `the new blob … is in place, but its directory isn't synced yet, and
    storing it again or the next sync tries again: …`
