- `appendManyToStateJsonWithIdentity` (and
  `Store::append_many_to_state_json_with_identity`) refuses an `idField`
  equal to `sequenceField` with `InvalidOperation`, writing nothing. An
  item holds one value at a key, and the sequence overwrote the record id
  there, so the stored items carried no record id. The single form,
  `appendToStateJsonWithIdentity`, still writes the sequence over the id at
  such a key.
