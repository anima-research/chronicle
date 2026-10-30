- `getStateItemJson` and `Store::get_state_item(_on)` answer `null`
  (`None`) for an index of zero or more on a state whose value isn't an
  array, such as a Snapshot state holding an object, as they do for an
  index out of range. They threw the deserialization error (`invalid type:
  map, expected a sequence`). `getStateSlice` and `Store::get_state_slice(_on)`
  still throw for such a state.
