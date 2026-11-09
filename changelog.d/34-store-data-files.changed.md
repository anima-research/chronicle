- A directory with no `MANIFEST` that holds `state-indexes.bin`, or a `blobs/`
  with anything in it but what a fresh volume's root holds or `.DS_Store`, is
  now a damaged store. `Store::create` refuses it with `InvalidOperation`,
  writing nothing there but its `LOCK`, and `Store::open_or_create` refuses it
  with `InvalidFormat`, writing nothing there; both leave the data. `create`
  used to make a new store there, beside what an earlier one left, and so did
  `open_or_create` over a `blobs/` with data (it already refused
  `state-indexes.bin` alone, as an entry a new store can't sit beside). A
  `blobs` link is now read as what it leads to, so one leading to a directory
  with data in it is refused the same way, where both used to take it (#34).
- `Store::open_or_create` on a directory with no `MANIFEST` whose `blobs/`
  can't be listed now fails with that listing's I/O error, naming `blobs/`,
  where it used to create a store there; `Store::open` gives the same error
  where it used to say the directory holds no store. `Store::create`'s advice
  for a `MANIFEST` cut short of its header now names `state-indexes.bin` and a
  `blobs/` with data among what must be absent before removing it (#34).
