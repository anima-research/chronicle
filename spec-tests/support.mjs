import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after } from 'node:test';
import { ErrorCode, JsStore } from '../index.js';

export { ErrorCode, JsStore };

const parents = [];
after(() => {
  for (const parent of parents) rmSync(parent, { recursive: true, force: true });
});

export function freshDir() {
  const parent = mkdtempSync(join(tmpdir(), 'chronicle-spec-'));
  parents.push(parent);
  return join(parent, 'store');
}

export function freshStore() {
  const dir = freshDir();
  return { store: JsStore.openOrCreate({ path: dir }), dir };
}

export function refuses(call, code, message) {
  assert.throws(call, (error) => {
    assert.equal(error.code, code, `code of: ${error.message}`);
    if (message instanceof RegExp) assert.match(error.message, message);
    else assert.equal(error.message, message);
    return true;
  });
}

const E2_FORMS = {
  Io: /^IO error: /,
  DurabilityLost: /^Durability lost: /,
  RecordNotFound: /^Record not found: /,
  BranchNotFound: /^Branch not found: /,
  BlobNotFound: /^Blob not found: /,
  StateNotRegistered: /^State not registered: /,
  StateExists: /^State already exists: /,
  InvalidSequence: /^Invalid sequence: .* \(head is .*\)$/,
  BranchExists: /^Branch already exists: /,
  Serialization: /^Serialization error: /,
  Deserialization: /^Deserialization error: /,
  Corruption: /^Corruption detected: /,
  StrategyMismatch: /^State strategy mismatch: expected .*, got /,
  Locked: /^Store is locked by another process$/,
  NotInitialized: /^Store not initialized$/,
  InvalidFormat: /^Invalid store format: /,
  ChecksumMismatch: /^Checksum mismatch: expected .*, got /,
  HashMismatch: /^Hash mismatch: expected .*, got /,
  Transaction: /^Transaction error: /,
  InvalidOperation: /^Invalid operation: /,
  SubscriptionDropped: /^Subscription was dropped$/,
  StoreClosed: /^(Store|View) has been closed$/,
  SubscriptionNotFound: /^Subscription not found$/,
};

export function refusesInAForm(call) {
  assert.throws(call, (error) => {
    assert.ok(Object.values(ErrorCode).includes(error.code), `code of: ${error.message}`);
    assert.match(error.message, E2_FORMS[error.code]);
    return true;
  });
}
