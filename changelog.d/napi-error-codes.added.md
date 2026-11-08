- **Error codes:** an error the Node binding raises for a store failure, or for
  a call it refuses, now carries `code`, one of the values of the new
  `ErrorCode` enum, such as `BranchNotFound`, `Locked` or `StoreClosed`. A
  failed fsync of the record log gives `DurabilityLost`, which `sync()` then
  throws until the store is reopened, and so do the calls that checkpoint,
  unless a check of their own refuses them first. Messages
  are unchanged, except the ones Changed names. Branch on `error.code` rather
  than on message text: compare it with `ErrorCode`'s members, whose names
  are kept across releases. An error napi raises while converting a call's
  arguments, before the call reaches chronicle, keeps napi's own code
  (`StringExpected`, `InvalidArg`) or has none. The four errors of
  `storeBlobAsync` that are neither a store failure nor a refusal, which
  `ErrorCode`'s docs name, carry no chronicle code: three carry napi's
  `GenericFailure`, and when it can't make its promise, `storeBlobAsync`
  throws the error napi raises.
