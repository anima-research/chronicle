- A JSON object now reads back with its keys in the order they were written,
  at every level, from every method that returns it. Before, the JSON-writing
  methods (`appendJson`, `setStateJson`, `appendToStateJson` and the rest)
  stored objects with their keys sorted, and every method that parses stored
  JSON (`getStateJson`, `getStateItemJson`, `getStateTail` and the rest)
  handed them back sorted. Nothing stored is rewritten: objects stored before
  through the JSON-writing methods keep their sorted keys, and ones stored
  before as raw bytes now read back in the order of those bytes.
