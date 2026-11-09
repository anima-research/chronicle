- **Hosts that check a chronicle error's `code`, or match its message:**
  every error the Node binding raised for chronicle carried napi's
  `GenericFailure` as `code`. It now carries the error's `ErrorCode`
  instead, so a check for `GenericFailure` no longer matches them. The four
  errors of the new `storeBlobAsync` that Added names carry no `ErrorCode`.
  Two kinds of error also change their message:
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
    onto that doesn't exist, and `BranchManager::gc`, which calls it, lists
    that refusal's new message among its errors.

  **Who needs to act:** a host that compares `error.code` with
  `GenericFailure`, or that matches `Subscription disconnected`, or
  `Branch not found:` on one of those deletions; and Rust code that matches
  `StoreError::BranchNotFound` there. **Migration:** compare `error.code`
  with `ErrorCode`'s members (Added): an ended subscription's poll gives
  `SubscriptionDropped` or `StoreClosed`, and those deletions give
  `InvalidOperation`. **Unchanged:** nothing on disk; a store opens and
  reads as before.
