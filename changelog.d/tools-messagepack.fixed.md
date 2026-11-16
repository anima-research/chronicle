- `tools/chain-report.py` and `tools/scan-delta-poison.py` read stores
  written by 0.3.0 and later, whose state updates are MessagePack. Before,
  each stopped at the first such update with a `UnicodeDecodeError`. Both
  now read either encoding and count a batched append as its items. Each
  skips a record typed `state_update` that doesn't decode as one, as
  chronicle does, and says so on stderr. Each stops with exit code 2 and no
  verdict at a kind of operation it doesn't read.
  `tools/rebuild-state-index.py`, which writes `state.bin` for a store
  whose every state update is JSON, stopped the same way on a newer store.
  It now writes nothing there, exits 2 and says what holds instead. The
  tools aren't in the npm package: run them from chronicle's repository
  with Python 3.
