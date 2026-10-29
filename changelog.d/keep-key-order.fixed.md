- JSON objects you write now come back with their keys in the order you wrote
  them (in a JavaScript object, its own property order), at every level and
  from every method that returns them, and `JsStore`'s docs say so. A tree
  state's entries still come back sorted by path. Before, the JSON-writing
  methods (`appendJson`, `setStateJson`, `appendToStateJson` and the rest)
  stored objects with their keys sorted, and every method that parses stored
  JSON (`getStateJson`, `getStateItemJson`, `getStateTail` and the rest)
  handed them back sorted.
  - Nothing is migrated. Objects stored before through the JSON-writing
    methods keep their sorted keys.
  - Objects stored before as raw bytes (`appendToState`, `setState`,
    `editStateItem`) came back sorted from the methods that parse them, and
    now come back in the order of their bytes, so a request built from them
    can change once after the upgrade.
  - A store this release writes still opens on 0.5.0, which hands back sorted
    every object it parses and re-sorts the objects in any snapshot it writes.
