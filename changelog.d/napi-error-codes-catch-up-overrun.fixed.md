- A catch-up that overruns the subscription's buffer (`catchUpSubscription`,
  and in Rust `Store::catch_up_subscription` and `catch_up_subscription_on`)
  now drops the subscription, as overrunning it with live events does. When
  the overrun fell on a replayed event, it used to fail with
  `Subscription was dropped` while the subscription stayed registered. In
  Rust, `SubscriptionManager::send_to` now removes a subscription that can't
  take the event, and `mark_caught_up` fails with `SubscriptionDropped` for
  one that's gone, where it returned `Ok`.
- A view dropped without `close()` now gives up its subscriptions when it's
  garbage-collected. A subscription it had made and not caught up used to
  stay in the store, counted by `subscriptionCount()`, until the store
  closed.
