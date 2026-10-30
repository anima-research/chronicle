- `storeBlobAsync(content, contentType, options)` stores a blob without
  holding the calling thread. It resolves with the blob's hash once it's
  stored, and rejects with the error that stopped it. Without `chunked`,
  it stores as `storeBlob` does, and rejects with the error `storeBlob`
  would throw. The bytes are copied before it returns, so the buffer can be
  reused at once. The open store's own thread, which its root handle and
  views share, takes these stores one at a time, in the order they're called
  on any of them; a `storeBlob` meanwhile isn't ordered with them. The
  root's `close()` waits for every one already called, holding the calling
  thread while it does, and a store whose root and views are all dropped
  without `close()` finishes them before it releases its lock.
- `storeBlobAsync` with `chunked: true` stores content longer than one chunk
  as chunks cut where its own bytes say, at most 64 KiB each and at least
  2 KiB but the last, every chunk a blob of its own, under a file at the
  whole content's hash that names them in order and is written only once
  every chunk is stored and synced. Content sharing long runs of bytes with
  earlier chunked content shares those chunks, so storing each request of a
  growing conversation writes mostly what's new. `getBlob` returns the
  content, and its hash is still the content's SHA-256. `stats()` counts
  each chunk, and the file naming them, as a blob, and Rust's
  `BlobStorage::list` lists them. A store that finds a blob already at its
  hash in the other form leaves it when it reads back as the content.
