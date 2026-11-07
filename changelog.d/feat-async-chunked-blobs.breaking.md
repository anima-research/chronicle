- **Running an earlier chronicle on a store with chunked blobs:** releases
  before this one open such a store and read its blobs stored whole, but
  can't read a chunked one. In 0.5.0, 0.5.1 and 0.6.0, `getBlob` of it throws
  `Invalid store format: Unsupported blob version 2 in <file>: …`, and so
  does `storeBlob` of the same content, which leaves the chunked file as it
  is. In 0.4.0 and earlier, `getBlob` of it throws
  `Invalid store format: Unsupported blob version: 2`, and `storeBlob` of
  the same content returns the hash and leaves the chunked file as it is.
  Either way the blob stays unreadable there. **Who needs to act:** a host
  that stores blobs with `storeBlobAsync`'s `chunked: true` and may run an
  earlier chronicle on the same store. **Migration:** before running an
  earlier release on such a store, close it and run
  `tools/unchunk-blobs.py <store> --apply` from chronicle's repository
  with Python 3 (the npm package doesn't carry it), which rewrites each
  chunked blob whole under the same hash, so earlier releases read it;
  without `--apply` it lists them. A blob it stops at because its list or
  a chunk can't be read, such as one with a damaged chunk, can't be read by
  this release either. With the store open in this release, store its
  content again with `storeBlob`, which writes it
  whole. If that throws, as it does for a chunk in a newer format or one
  this account can't read, delete the blob's file, then store the content;
  without the content, only delete the file. Then close the store and run
  the script again. **Unchanged:** only `storeBlobAsync` with
  `chunked: true` writes a chunked blob, and nothing else in a store
  changes with it: blobs stored whole and the store's other files keep the
  formats earlier releases read. Opening a store reads no blob.
