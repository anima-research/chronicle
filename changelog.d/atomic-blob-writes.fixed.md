- `storeBlob` (`Store::store_blob`) writes a blob to a new file in `blobs/`,
  syncs it, and renames it into its shard, where it used to write the file
  in place. A crash now leaves the blob's file whole or absent, never cut
  short. On Unix it then syncs the shard's directory, and the blob directory
  when the shard is new, so a blob whose store returned survives a power
  loss, where the system syncs directories and the process may open them;
  where it may not, the store goes without that sync and logs a warning
  once. Opening a store removes the new files (`.chronicle-blob-*`) that
  stores killed before their rename left in `blobs/`.
- `storeBlob` replaces a stored file that isn't whole, such as one cut short
  by a crash during an in-place write. It used to find a file there and
  return, so the blob stayed torn and every `getBlob` of it failed. For a
  blob the store hasn't cached, it now reads the file and checks it as
  `getBlob` does, where it used to check only that a file exists. A whole
  file is left as it is, with the content type it was first stored with, as
  before.
- `getBlob` (`Store::get_blob`) reads a blob's file in one read and checks
  each length field against the file's size before using it, so a damaged
  length is an error rather than an allocation of that many bytes, which
  aborted the process for a length near `u64::MAX`. Its errors are otherwise
  the ones it gave before.
- `storeBlob` refuses a content type longer than 65,535 bytes with an
  `InvalidOperation` error before it does anything else, whether or not the
  content is already stored. For new content its length field wrapped, and
  the file written couldn't be read; for stored content the call returned
  the hash.
- The blob file format is unchanged, so existing stores keep opening, and
  earlier releases open a store this version wrote. A leftover
  `.chronicle-blob-*` file lies beside the shards, where the blob listing
  and size of every release skip it.
