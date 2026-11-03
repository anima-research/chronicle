- The refusals `Store::create` (`JsStore.create`) gives for a directory
  that already holds a store, and for a `MANIFEST` left as a symbolic link
  that leads to nothing, name the calls that open the store in both APIs:
  `Store::open` or `Store::open_or_create` (`open` or `openOrCreate` in the
  Node API).
- `Store::create` (`JsStore.create`) on a directory whose `MANIFEST` open
  can't read (empty, cut short, another file's bytes, a version or format
  features this build doesn't know) or isn't a regular file refuses with
  `InvalidOperation`, naming that file and what is wrong with it, and
  leaves it as it is. The refusal
  doesn't name `open` or `openOrCreate`, which fail on that file too.
