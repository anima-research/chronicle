- A directory with no `MANIFEST` that holds `state-indexes.bin`, or a `blobs/`
  with anything in it but `.DS_Store` or `lost+found`, is now a damaged store:
  `Store::create` refuses it with `InvalidOperation` and
  `Store::open_or_create` with `InvalidFormat`, writing nothing there and
  leaving the data. Both used to make a new store there, beside what an earlier
  one left (#34). A `blobs` link is still taken as it is.
- `Store::open_or_create` on a directory with no `MANIFEST` whose `blobs/`
  can't be listed now fails with that listing's I/O error, naming `blobs/`,
  where it used to create a store there; `Store::open` gives the same error
  where it used to say the directory holds no store. `Store::create`'s advice
  for a `MANIFEST` cut short of its header now names `state-indexes.bin` and a
  `blobs/` with data among what must be absent before removing it (#34).
