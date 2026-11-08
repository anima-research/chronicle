- A catch-up from a `fromSequence` past its branch's head replays nothing,
  as the README says, where it replayed every matching record on the
  branch again, and dropped the subscription when those overran its
  buffer.
