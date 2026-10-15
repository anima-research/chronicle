// Each error the binding throws carries an `ErrorCode` as `error.code`,
// beside its message, which is unchanged. These run the built binding
// (index.js), so they need the napi build first, as test.mjs does.
import { after, test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ErrorCode, JsStore } from './index.js';

const dirs = [];
after(() => {
  for (const dir of dirs) rmSync(dir, { recursive: true, force: true });
});

function freshStore() {
  const parent = mkdtempSync(join(tmpdir(), 'chronicle-codes-'));
  dirs.push(parent);
  const dir = join(parent, 'store');
  return { store: JsStore.openOrCreate({ path: dir }), dir };
}

/** Runs `call` and checks it throws `code` with `message` (a string for
 *  the whole message, or a RegExp). */
function throwsCoded(call, code, message) {
  assert.throws(call, (error) => {
    assert.equal(error.code, code);
    if (typeof message === 'string') assert.equal(error.message, message);
    else assert.match(error.message, message);
    return true;
  });
}

test('ErrorCode lists every code, each named as its value', () => {
  assert.deepEqual(Object.keys(ErrorCode).sort(), [
    'BlobNotFound', 'BranchExists', 'BranchNotFound', 'ChecksumMismatch', 'Corruption',
    'Deserialization', 'HashMismatch', 'InvalidFieldIndexKind', 'InvalidFormat', 'InvalidHash',
    'InvalidLimit', 'InvalidOperation', 'InvalidRecordId', 'InvalidSequence', 'InvalidStrategy',
    'InvalidSubscriptionId', 'InvalidTreeOp', 'Io', 'Locked', 'NotInitialized',
    'RecordNotFound', 'Serialization', 'StateExists', 'StateNotRegistered', 'StoreClosed',
    'StrategyMismatch', 'SubscriptionDisconnected', 'SubscriptionDropped', 'SubscriptionNotFound',
    'Transaction',
  ]);
  for (const [key, value] of Object.entries(ErrorCode)) assert.equal(value, key);
});

test("the store's errors carry their kind", () => {
  const { store } = freshStore();
  throwsCoded(() => store.switchBranch('nowhere'), ErrorCode.BranchNotFound, 'Branch not found: nowhere');
  throwsCoded(() => store.createBranch('main'), ErrorCode.BranchExists, 'Branch already exists: main');
  store.registerState({ id: 'notes', strategy: 'snapshot' });
  throwsCoded(
    () => store.registerState({ id: 'notes', strategy: 'snapshot' }),
    ErrorCode.StateExists,
    'State already exists: notes',
  );
  throwsCoded(
    () => store.updateStateStrategy({ id: 'unknown', strategy: 'snapshot' }),
    ErrorCode.StateNotRegistered,
    'State not registered: unknown',
  );
  throwsCoded(
    () => store.editStateItem('notes', 0, Buffer.from('x')),
    ErrorCode.InvalidOperation,
    'Invalid operation: Edit index 0 out of bounds (len=0)',
  );
  store.close();
});

test("the binding's own refusals carry theirs", () => {
  const { store } = freshStore();
  throwsCoded(() => store.getRecord('abc'), ErrorCode.InvalidRecordId, 'Invalid record ID');
  throwsCoded(() => store.getBlob('zz'), ErrorCode.InvalidHash, 'Invalid hash');
  throwsCoded(
    () => store.getRecordIdsByType('note', { after: 'x' }),
    ErrorCode.InvalidRecordId,
    'Invalid record ID for after: "x"',
  );
  throwsCoded(
    () => store.getRecordIdsByType('note', { limit: -1 }),
    ErrorCode.InvalidLimit,
    'Invalid limit: -1 (a non-negative integer)',
  );
  store.registerState({ id: 'log', strategy: 'append_log' });
  throwsCoded(
    () => store.registerStateFieldIndex('log', 'when', 'date'),
    ErrorCode.InvalidFieldIndexKind,
    'Invalid field index kind \'date\': expected "number" or "string"',
  );
  throwsCoded(
    () => store.registerState({ id: 'odd', strategy: 'bogus' }),
    ErrorCode.InvalidStrategy,
    'Invalid strategy',
  );
  throwsCoded(() => store.pollSubscription('abc'), ErrorCode.InvalidSubscriptionId, 'Invalid subscription ID');
  throwsCoded(() => store.pollSubscription('4242'), ErrorCode.SubscriptionNotFound, 'Subscription not found');
  store.registerState({ id: 'tree', strategy: 'tree' });
  throwsCoded(
    () => store.treeBatch('tree', [{ op: 'rename', path: 'a' }]),
    ErrorCode.InvalidTreeOp,
    "Invalid tree op: 'rename', expected 'set' or 'remove'",
  );
  throwsCoded(() => store.treeBatch('tree', [{ op: 'set', path: 'a' }]), ErrorCode.InvalidTreeOp, "TreeOp 'set' requires entry");
  store.close();
  throwsCoded(() => store.stats(), ErrorCode.StoreClosed, 'Store has been closed');
});

test('a second open of a store in use is Locked', () => {
  const { store, dir } = freshStore();
  throwsCoded(() => JsStore.open({ path: dir }), ErrorCode.Locked, 'Store is locked by another process');
  store.close();
});
