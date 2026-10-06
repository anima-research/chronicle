- Selective forks: `createBranchWithStates(name, from, { stateIds?, statePrefixes? })`
  and `createBranchAtWithStates(name, from, atSequence, { stateIds?, statePrefixes? })`
  create a branch that inherits only the named states (exact ids and/or plain
  string prefixes of the id; a state covered twice is inherited once, names
  matching nothing are ignored, an empty filter inherits nothing). Inherited
  chain heads are copied verbatim, snapshot accounting included. A state that
  is not inherited has no value on the new branch — it reads exactly as a
  registered state does on a `createEmptyBranch` branch — and a write to it
  starts a fresh chain. Rust: `Store::create_branch_with_states`,
  `Store::create_branch_at_with_states`, `StateSelection`.
