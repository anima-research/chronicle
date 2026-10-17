- **`error.code` on the binding's errors:** every error the Node binding raised
  for chronicle carried napi's `GenericFailure` as `code`. It now carries the
  error's `ErrorCode` instead, so a check for `GenericFailure` no longer
  matches them. `catchUpSubscription` on an ID that was never made, or was
  unsubscribed, now throws `SubscriptionNotFound` with the message
  `Subscription not found`, as polling does, instead of `Subscription was
  dropped`.
