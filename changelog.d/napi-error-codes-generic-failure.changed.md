- **`error.code` on the binding's errors:** every error the Node binding raised
  for chronicle carried napi's `GenericFailure` as `code`. It now carries the
  error's `ErrorCode` instead, so a check for `GenericFailure` no longer
  matches them. Two refusals also change their message:
  - `catchUpSubscription` on an ID that no handle of the store holds, because
    it was never made or was unsubscribed, now throws `SubscriptionNotFound`
    with the message `Subscription not found`, as polling does, instead of
    `Subscription was dropped`.
  - Deleting the main branch or the current branch, or a view's deleting the
    branch it is pinned to, now throws `InvalidOperation` (in Rust,
    `StoreError::InvalidOperation`), whose message begins
    `Invalid operation:`, where it was `BranchNotFound` and began
    `Branch not found:`.
