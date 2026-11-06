- A read of a state's items (`getStateItemJson`, `getStateItemsJson`,
  `getStateSlice` and the other item reads, and the count a batched append
  takes) that raced a write to the state on the same branch could file the
  newer value's items under the head it had read before the write. Another
  branch still at that head, a fork's, then read those items as its own,
  and its first batched append counted them for its `firstOrdinal` and
  `getStateLen` (#70). Items are now filed only under a head that hasn't
  moved since it was read. Only a host that calls the store from several
  threads can meet this: Node calls run one at a time.
