- A catch-up from a `fromSequence` past its branch's head replays nothing,
  as the README says, where it replayed every matching record on the
  branch again, and dropped the subscription when those overran its
  buffer.
- In a store written before 0.2.6 that holds a sequence reissued after an
  unclean shutdown, a catch-up no longer replays a record below its
  `fromSequence`, and one from a reissued sequence reaches the higher
  records written before it, where it started at the reissued record.
