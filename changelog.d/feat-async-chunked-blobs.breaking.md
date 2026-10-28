- **Running an earlier chronicle on a store with chunked blobs:** 0.1.1
  through 0.5.0 open such a store and read its blobs stored whole, but
  can't read a chunked one. In 0.5.0, `getBlob` of it throws
  `Invalid store format: Unsupported blob version 2 in <file>: …`, and so
  does `storeBlob` of the same content, which leaves the chunked file as it
  is. In 0.4.0 and earlier, `getBlob` of it throws
  `Invalid store format: Unsupported blob version: 2`, and `storeBlob` of
  the same content returns the hash and leaves the chunked file as it is.
  Either way the blob stays unreadable there. **Who needs to act:** a host
  that stores blobs with `storeBlobAsync`'s `chunked: true` and may run an
  earlier chronicle on the same store. **Unchanged:** nothing stores a
  chunked blob unless `storeBlobAsync` is asked to, and opening a store
  reads no blob.
