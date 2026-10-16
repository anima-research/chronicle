- After a metadata checkpoint fails, further mutations retry that checkpoint
  before accepting writes that could depend on an unpersisted state
  registration or branch. The tentative in-memory mutation remains available
  for inspection; `sync()` can retry explicitly. Subscribers hear of a branch
  created or deleted once a checkpoint persists it. Historical branch
  creation completes its fallible chain reads before allocating a branch.
- Reopening a store replays complete state updates after its metadata
  checkpoint, including snapshot accounting and the field indexes whose saved
  head matches `state.bin`. It drops any other field index, which registering
  it again rebuilds. State registration, strategy changes, and branch
  creation/deletion/switching now checkpoint before returning. Store
  checkpoints are serialized with writes, and branch identities found only
  in partially published state metadata are reserved against reuse. No
  on-disk format change; record updates still require `sync()` to flush
  contents. On Windows, a save doesn't wait for its rename to reach the
  disk, so after an OS crash or power loss the store may open from an
  earlier checkpoint.
