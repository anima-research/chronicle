- **`error.code` on the binding's errors:** every error the Node binding raised
  for chronicle carried napi's `GenericFailure` as `code`. It now carries the
  error's `ErrorCode` instead, so a check for `GenericFailure` no longer
  matches them. Two kinds of error also change their message:
  - A poll of a subscription that has ended, once the events queued for it
    are read, throws `StoreClosed` (`Store has been closed`) after the
    root's `close()`, and otherwise `SubscriptionDropped`
    (`Subscription was dropped`), instead of `Subscription disconnected`:
    the store dropped it because its buffer overran, or it was
    unsubscribed through another handle.
  - Deleting the main branch or the current branch, or a view's deleting the
    branch it is pinned to, now throws `InvalidOperation` (in Rust,
    `StoreError::InvalidOperation`), whose message begins
    `Invalid operation:`, where it was `BranchNotFound` and began
    `Branch not found:`. In Rust, so does
    `BranchManager::delete_branch_with_reparent` naming a branch to reparent
    onto that doesn't exist.
