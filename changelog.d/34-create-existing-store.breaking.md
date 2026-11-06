- **Hosts that open a store on an existing directory that holds no store:**
  with `create_if_missing`, which `JsStore.openOrCreate` always sets,
  `Store::open_or_create` now creates a store in a directory that's empty or
  holds nothing but a fresh volume's root entries, Finder's `.DS_Store`, or
  what a create left when it stopped before its manifest (see Fixed), where
  it used to fail. So a store started before its volume mounts (an unmounted
  mount point is an empty directory) now creates a fresh empty store that the
  volume hides once it mounts, and a pre-flight check that opens a path to see
  whether it's a store now makes one. **To act:** if you relied on that
  failure, check the mount or the directory before opening. connectome-host's
  store audit is one such caller. **Unchanged:** existing stores keep opening,
  and a missing directory is still created (#34).
