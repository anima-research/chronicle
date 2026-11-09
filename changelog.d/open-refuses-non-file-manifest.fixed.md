- `Store::open` and `Store::open_or_create` (and `JsStore.open` and
  `openOrCreate`) on a store whose `MANIFEST` isn't a regular file now fail
  with `InvalidFormat`, naming the file, without opening it. On a FIFO they
  used to wait for a writer, though 0.6.0's notes said they fail there; on a
  directory they failed with an I/O error (#34).
