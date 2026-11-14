- `stats()` counts only a blob directory's shards, so a `blobs/` that is a
  mounted volume's root no longer makes it fail on a `lost+found` (or
  `System Volume Information`) this account can't list, and files there no
  longer count as blobs (#34).
