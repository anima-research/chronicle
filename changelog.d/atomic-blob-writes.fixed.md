- The blob file format is unchanged by the staging-name writes: earlier
  releases read the blob files this release writes. A leftover
  `.chronicle-blob-*` staging file lies beside the shards, where every
  release's blob listing and size skip it.
