- **`error.code` on the binding's errors:** every error the Node binding raised
  for chronicle carried napi's `GenericFailure` as `code`. It now carries the
  error's `ErrorCode` instead, so a check for `GenericFailure` no longer
  matches them. Three kinds of error also change their message:
  - `catchUpSubscription` on an ID that was never made, was unsubscribed
    through any handle, was a view's when the view was closed, or was a
    handle's when the handle was garbage-collected now throws
    `SubscriptionNotFound` with the message `Subscription not found`, as
    polling does, instead of `Subscription was dropped`.
  - A poll of a subscription that has ended, once the events queued for it
    are read, throws why it ended instead of `Subscription disconnected`:
    `StoreClosed` (`Store has been closed`) after the root's `close()`,
    `SubscriptionNotFound` (`Subscription not found`) once it was
    unsubscribed through another handle, and `SubscriptionDropped`
    (`Subscription was dropped`) once the store dropped it because its
    buffer overran.
  - Deleting the main branch or the current branch, or a view's deleting the
    branch it is pinned to, now throws `InvalidOperation` (in Rust,
    `StoreError::InvalidOperation`), whose message begins
    `Invalid operation:`, where it was `BranchNotFound` and began
    `Branch not found:`. In Rust, so does
    `BranchManager::delete_branch_with_reparent` naming a branch to reparent
    onto that doesn't exist.
