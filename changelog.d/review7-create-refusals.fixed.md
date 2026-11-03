- The refusals `Store::create` (`JsStore.create`) gives for a directory
  that already holds a store, and for a `MANIFEST` left as a symbolic link
  that leads to nothing, name the calls that open the store in both APIs:
  `Store::open` or `Store::open_or_create` (`open` or `openOrCreate` in the
  Node API).
