- On Unix, `sync()` and `close()` now sync the directories that hold the
  store's directory, up to the root of its filesystem. They do it on the
  first call on each open handle. Before, a power loss after `sync()` could
  lose a new store. New parent directories could be lost the same way.
- That first call also syncs `blobs/` and each shard directory in it.
  Before, a power loss could lose a blob whose shard was never synced.
- A store with all 256 shards gets 257 more directory syncs on a handle's
  first `sync()`.
- `sync()` and `close()` also retry a shard sync that failed in a blob
  store. Before, `sync()` skipped such a directory.
- When a directory sync is refused, `sync()` and `close()` throw an error
  with the `code` `Io`. The next `sync()` tries it again. After `close()`
  throws, the reopened store's first `sync()` also tries again. Appends,
  state writes and blob stores go on meanwhile.
- As with a checkpoint save, `sync()` skips a directory that this process
  may not open. `sync()` doesn't sync a symbolic link on the store's path
  either. Nothing on disk changes, and stores open as before.
- The errors of a failed blob directory sync now end with the system's
  error, as `IO error: … (os error N)`, so a caller can read the error
  number. These are the new messages:
  - `cannot sync the directories holding …, so … wasn't written, and storing
    the blob again tries the sync again: …`
  - `the blob … is stored, but its directory isn't synced yet, and storing it
    again or the next sync tries again: …`
  - `the new blob … is in place, but its directory isn't synced yet, and
    storing it again or the next sync tries again: …`
