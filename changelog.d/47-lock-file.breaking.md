- **Stores whose `LOCK` isn't a regular file:** opening or creating one now
  fails with `InvalidFormat`, naming what `LOCK` is. A `LOCK` symlink used to
  be followed, and its target truncated, on every open; a lock taken through
  a link doesn't keep a second process out once the target is replaced by a
  rename. Stop anything that uses the store, then move `LOCK` aside or
  remove it: the next open creates a regular one. Unchanged: stores whose
  `LOCK` is a regular file, which is what open and create make, keep opening
  as before.
