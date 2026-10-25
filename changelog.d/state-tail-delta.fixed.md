- `getStateTail` (`get_state_tail`) no longer repeats items when the tail
  reaches past the newest delta snapshot into the one before it. The walk
  took the newest delta's items and then the appends that delta already
  holds, so with a delta every 3 appends, the last 6 of 11 items came back
  as `[9, 7, 8, 9, 10, 11]`. It now skips what a delta covers, as a full
  read does. A tail within the newest delta, or as long as the state, was
  right before.
