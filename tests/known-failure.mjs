import assert from 'node:assert/strict';

export const knownFailure = (reason, body) => async t => {
  try {
    await body(t);
  } catch (error) {
    t.todo(reason);
    throw error;
  }
  assert.fail(`The todo no longer fails here: ${reason}`);
};
