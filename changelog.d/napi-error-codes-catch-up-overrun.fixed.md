- A `catchUpSubscription` that overruns the subscription's buffer now drops
  the subscription, as overrunning it with live events does. It used to throw
  `Subscription was dropped` while the subscription stayed registered.
