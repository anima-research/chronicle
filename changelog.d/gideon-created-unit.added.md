- The napi type declarations now say that `JsBranch.created` and
  `JsRecord.timestamp` are microseconds since the Unix epoch. They say the
  same of `record.timestamp` and `branch.created` in a subscription event's
  `data`. The values don't change. For a JavaScript `Date`, divide by 1,000
  first, as in `new Date(branch.created / 1000)`.
