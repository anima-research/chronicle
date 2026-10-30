- **Running an earlier chronicle on a store with chunked blobs:** releases
  before this one open such a store and read its blobs stored whole, but
  can't read a chunked one. In 0.5.0 and 0.5.1, `getBlob` of it throws
  `Invalid store format: Unsupported blob version 2 in <file>: …`, and so
  does `storeBlob` of the same content, which leaves the chunked file as it
  is. In 0.4.0 and earlier, `getBlob` of it throws
  `Invalid store format: Unsupported blob version: 2`, and `storeBlob` of
  the same content returns the hash and leaves the chunked file as it is.
  Either way the blob stays unreadable there. **Who needs to act:** a host
  that stores blobs with `storeBlobAsync`'s `chunked: true` and may run an
  earlier chronicle on the same store. **Migration:** before running an
  earlier release on such a store, close it and run
  `tools/unchunk-blobs.py <store> --apply` from chronicle's repository,
  which rewrites each chunked blob whole under the same hash; without
  `--apply` it lists them. A blob it stops at, such as one with a damaged
  chunk, can't be read by this release either: store its content again
  with `storeBlob`, which writes it whole; if that throws, as it does for a
  chunk in a newer format or one this account can't read, or without the
  content, delete the blob's file; then run the script again.
  **Unchanged:** nothing stores a chunked blob unless `storeBlobAsync` is
  asked to, and opening a store reads no blob.
