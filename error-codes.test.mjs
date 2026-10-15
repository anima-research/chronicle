// Each error the binding raises for a store failure, or for a call it
// refuses, carries an `ErrorCode` as `error.code`, beside its message, which
// is unchanged. An error napi raises while converting a call's arguments,
// before chronicle runs, keeps napi's own code or none. These run the built
// binding (index.js and index.d.ts), so they need the napi build first, as
// test.mjs does.
import { after, test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
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

test('index.d.ts declares ErrorCode as a regular enum, which TypeScript can use as a value', () => {
  const declarations = readFileSync(new URL('./index.d.ts', import.meta.url), 'utf8');
  assert.match(declarations, /^export enum ErrorCode \{$/m);
  assert.doesNotMatch(declarations, /const enum ErrorCode/);
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
  store.append('note', Buffer.from('a'));
  throwsCoded(
    () => store.createBranchAt('later', 'main', store.currentSequence() + 1000),
    ErrorCode.InvalidSequence,
    /^Invalid sequence: Seq\(\d+\) \(head is Seq\(\d+\)\)$/,
  );
  // State bytes that aren't JSON, read as JSON.
  store.setState('notes', Buffer.from('not json'));
  throwsCoded(() => store.getStateJson('notes'), ErrorCode.Deserialization, /at line 1 column \d+$/);
  store.close();
});

test("the binding's own refusals carry theirs", () => {
  const { store } = freshStore();
  throwsCoded(() => store.getRecord('abc'), ErrorCode.InvalidRecordId, 'Invalid record ID');
  throwsCoded(() => store.getBlob('zz'), ErrorCode.InvalidHash, 'Invalid hash');
  // Hex, of the wrong length.
  throwsCoded(() => store.getBlob('abcd'), ErrorCode.InvalidHash, 'Invalid hash');
  throwsCoded(() => store.getStateUpdateJson('abc'), ErrorCode.InvalidRecordId, 'Invalid record ID');
  throwsCoded(() => store.getEffects('abc'), ErrorCode.InvalidRecordId, 'Invalid record ID');
  throwsCoded(() => store.getLinksTo('abc'), ErrorCode.InvalidRecordId, 'Invalid record ID');
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
  throwsCoded(
    () => store.getRecordIdsByType('note', { limit: 1.5 }),
    ErrorCode.InvalidLimit,
    'Invalid limit: 1.5 (a non-negative integer)',
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
  throwsCoded(() => store.pollSubscriptionTimeout('abc', 1), ErrorCode.InvalidSubscriptionId, 'Invalid subscription ID');
  throwsCoded(() => store.pollSubscriptionTimeout('4242', 1), ErrorCode.SubscriptionNotFound, 'Subscription not found');
  throwsCoded(() => store.unsubscribe('abc'), ErrorCode.InvalidSubscriptionId, 'Invalid subscription ID');
  throwsCoded(() => store.catchUpSubscription('abc'), ErrorCode.InvalidSubscriptionId, 'Invalid subscription ID');
  // Never made, or unsubscribed: not found, whichever method asks.
  throwsCoded(() => store.catchUpSubscription('4242'), ErrorCode.SubscriptionNotFound, 'Subscription not found');
  const gone = store.subscribe();
  store.unsubscribe(gone);
  throwsCoded(() => store.catchUpSubscription(gone), ErrorCode.SubscriptionNotFound, 'Subscription not found');
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

test('after close(), polling a subscription says it is disconnected, not that the store is closed', () => {
  const { store } = freshStore();
  const id = store.subscribe();
  store.close();
  throwsCoded(() => store.pollSubscription(id), ErrorCode.SubscriptionDisconnected, 'Subscription disconnected');
  throwsCoded(() => store.pollSubscriptionTimeout(id, 1), ErrorCode.SubscriptionDisconnected, 'Subscription disconnected');
});

test('a catch-up that overruns the subscription\'s buffer drops it', () => {
  const { store } = freshStore();
  for (const n of [1, 2, 3]) store.append('note', Buffer.from(String(n)));
  const id = store.subscribe({ bufferSize: 1, fromSequence: 0, filter: { includeRecords: true } });
  throwsCoded(() => store.catchUpSubscription(id), ErrorCode.SubscriptionDropped, 'Subscription was dropped');
  store.close();
});

test('opening a store that is not there is an Io error', () => {
  const parent = mkdtempSync(join(tmpdir(), 'chronicle-codes-'));
  dirs.push(parent);
  throwsCoded(() => JsStore.open({ path: join(parent, 'nothing-here') }), ErrorCode.Io, /^IO error: /);
});

test("an error napi raises converting a call's arguments keeps napi's code, or has none", () => {
  const { store } = freshStore();
  const napiCode = (call) => {
    try {
      call();
    } catch (error) {
      return { code: error.code, typeError: error instanceof TypeError };
    }
    assert.fail('expected a throw');
  };
  assert.deepEqual(napiCode(() => store.getRecord(42)), { code: 'StringExpected', typeError: false });
  assert.deepEqual(napiCode(() => store.storeBlob('a string', 'text/plain')), { code: 'InvalidArg', typeError: false });
  assert.deepEqual(napiCode(() => store.registerState(null)), { code: undefined, typeError: true });
  for (const code of ['StringExpected', 'InvalidArg']) assert.equal(Object.values(ErrorCode).includes(code), false);
  store.close();
});
