- `storeBlob` and `storeBlobAsync` no longer put the bytes they store in the
  blob cache, which now holds what `getBlob` reads, so storing large blobs
  no longer holds up to `blobCacheSize` of them in memory. A `getBlob` of a
  blob stored but not read yet reads its file. A store of bytes the open
  store has already stored whole, or read and checked whole while storing,
  still returns without reading their file again while a regular file
  of the same length is at their name, until a read of that file fails. A
  `getBlob` records nothing of the kind, so storing bytes only `getBlob` has
  read reads their file once. A check of such a file that races a store
  replacing it on another thread can drop the entry that store has just
  made, until the store that checked reads the file and records it again;
  a store of the same bytes in between reads the file once. A chunked
  blob's own file, which names its chunks, is read each time the same
  content is stored again: by a chunked store, with its chunks
  taken the same way, and by `storeBlob`, with its chunks read. `getBlob` of
  a chunked blob caches its chunks, not the whole.
- A store whose sync of the directories holding a blob's file fails now
  names that file as not written (`cannot sync the directories holding
  <dir>: <error>; <file> wasn't written, and storing the blob again tries
  the sync again`), where it said `nothing was stored`: a chunked store
  whose sync fails at a later chunk has stored the chunks before it.
