- A catch-up that overruns the subscription's buffer (`catchUpSubscription`,
  and in Rust `Store::catch_up_subscription` and `catch_up_subscription_on`)
  now drops the subscription, as overrunning it with live events does. It
  used to fail with `Subscription was dropped` while the subscription stayed
  registered.
- A view dropped without `close()` now gives up its subscriptions when it's
  garbage-collected. A subscription it had made and not caught up used to
  stay in the store, counted by `subscriptionCount()`, until the store
  closed.
