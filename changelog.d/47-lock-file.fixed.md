- Opening or creating a store no longer truncates its `LOCK` file. A lock
  that fails for a reason other than another holder of it (another process,
  or another open in the same one) is now reported as that I/O error, rather
  than as "Store is locked by another process".
