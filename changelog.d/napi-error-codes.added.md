- **Error codes:** an error the Node binding raises for a store failure, or for
  a call it refuses, now carries `code`, one of the values of the new
  `ErrorCode` enum, such as `BranchNotFound`, `Locked` or `StoreClosed`. Its
  message is unchanged, except `catchUpSubscription`'s for an ID the binding
  doesn't hold (see Changed). Branch on `error.code` rather than on message
  text: compare it with `ErrorCode`'s members, whose names are kept across
  releases. An error napi raises while converting a call's arguments, before
  the call reaches chronicle, keeps napi's own code (`StringExpected`,
  `InvalidArg`) or has none.
