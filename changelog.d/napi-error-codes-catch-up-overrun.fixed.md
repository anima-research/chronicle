- A catch-up that overruns the subscription's buffer (`catchUpSubscription`,
  and in Rust `Store::catch_up_subscription` and `catch_up_subscription_on`)
  now drops the subscription, as overrunning it with live events does. When
  the overrun fell on a replayed event, it used to fail with
  `Subscription was dropped` while the subscription stayed registered, and
  a poll of it returned `null`; now a poll throws `SubscriptionDropped`
  once the events queued for it are read. In
  Rust, `SubscriptionManager::send_to` now removes a subscription that can't
  take the event, and `Store::mark_subscription_caught_up` and
  `SubscriptionManager::mark_caught_up` fail with `SubscriptionDropped` for
  one that's gone, where they returned `Ok`.
- A handle, the root or a view, dropped without `close()` now gives up its
  subscriptions when it's garbage-collected. A subscription it had made and
  not caught up used to stay in the store, counted by `subscriptionCount()`,
  until the store closed.
