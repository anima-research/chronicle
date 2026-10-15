- **Stores whose checkpoints have their own ACL or attributes, are linked or
  mounted one file at a time, or are saved by an account other than their
  owner or one that can't write to the store directory:** a save now writes
  each metadata checkpoint (`state.bin`, `branches.bin` and
  `state-indexes.bin`) to a new file beside it and renames that over the old
  one, where it used to rewrite the old file in place. What the rename
  changes:
  - **A per-file ACL, extended attribute or file flag** isn't carried over,
    such as an ACL that differs from the directory's default or an SELinux
    label set on the file itself. The new file gets what its directory gives
    any new file.
  - **Owner, group and mode.** On Unix the new file takes the old one's
    owner and group as far as the saving account may set them, and its mode.
    Where the saver can't keep the owner, the new file is the saver's,
    without setuid, so the previous owner keeps only what the group and other
    bits grant. Where the saver can't keep the group, the new file keeps the
    group it was created with, without setgid, and that group gets no access.
  - **A hard link** to a checkpoint stops pointing at it after the next save.
  - **A checkpoint bind-mounted as a single file** can't be replaced, so
    saving it fails.
  - **A symbolic link** at a checkpoint's name is still followed, and the
    file it leads to is replaced in its own directory. A crash while saving it
    can leave a `.chronicle-checkpoint-*` staging file beside that file, and
    opening the store only removes the ones in the store directory.
  - **What a save needs** is what a rename needs, in the directory of the
    file it replaces: write access, to create the new file and rename it;
    read access, to sync the directory afterwards, without which the save
    fails with a `PermissionDenied` error that says so before anything is
    written; in a directory with the sticky bit, ownership of the checkpoint
    or of the directory; free space for a whole new checkpoint while the old
    one still exists; and on Windows, permission to delete the checkpoint,
    with no other process holding it open without delete sharing. Replacing a
    checkpoint used to need write access to that file alone, so a read-only
    checkpoint no longer stops a save by an account that can write its
    directory.

  **Who needs to act:** anyone whose stores are set up as above.
  **Migration:** before upgrading, move a per-file ACL onto the directory's
  default ACL, and a per-file SELinux type onto the directory, whose type new
  files take unless the policy names another; link or mount the store
  directory instead of single checkpoints; and grant the saving account those
  permissions.
  **Unchanged:** the checkpoint encodings, so existing stores keep opening,
  and 0.4.x can open a store this version has saved.
