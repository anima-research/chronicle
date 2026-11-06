- A read of a state's items (`getStateItemJson`, `getStateSlice` and the
  other item reads), or of a tree state's map (`treeGet`, `treeList`,
  `treeDiff` and the other tree reads), that raced a write to the state on
  the same branch could file the newer value under the head it had read
  before the write. Another branch still at that head, a fork's, then read
  that value as its own (#70). A value is now filed only under a head that
  hasn't moved since it was read, and the batched calls new in this release
  (see Added) file theirs the same way. Only a host that calls the store
  from several threads can meet this: Node calls run one at a time.
