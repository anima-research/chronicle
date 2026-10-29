- **Callers of `getStateTail` on a state whose value isn't an array:** read
  such a state whole with `getState`, or `getStateJson` for a JSON value.
  `getStateTail` of one item or more on such a value now throws a
  deserialization error. Under 0.4.0 it returned the whole value when the
  count asked for was at least the state's stored count (0 for a `snapshot`
  state never set to an array, so any count), and threw when it was
  smaller. Unchanged: a tail of 0 items is still `[]`, a state with no
  value still gives `null`, and existing stores open as before.
