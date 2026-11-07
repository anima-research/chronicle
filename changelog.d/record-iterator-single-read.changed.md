- Opening a store reads each record of the log twice, once to validate it
  and once to index it, where it read each three times: the iterator that
  rebuilds the index read every record a second time to find where it
  ended. It now reads each once, which also halves the reads of
  `Store::iter_from(_on)` and of the record replay in
  `catchUpSubscription`. Through a warm page cache, opening a store of
  200,000 small records took 2.0 s where it took 2.9 s, and one pass of the
  iterator 1.0 s where it took 1.9 s.
