- JSON objects you write now come back with their keys in the order you wrote
  them (for a JavaScript object, its own property order), at every level and
  from every method that returns them, and `JsStore`'s docs say so. Before,
  the JSON-writing methods (`appendJson`, `setStateJson`, `appendToStateJson`
  and the rest) stored objects with their keys sorted, and every method that
  parses stored JSON (`getStateJson`, `getStateItemJson`, `getStateTail` and
  the rest) handed them back sorted. Objects stored by 0.5.0 or earlier keep
  the order they were stored in: sorted for those written through the
  JSON-writing methods, the order of their bytes for those written as raw
  bytes. Nothing stored is rewritten.
