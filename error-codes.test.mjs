// Each error the binding raises for a store failure, or for a call it
// refuses, carries an `ErrorCode` as `error.code`, beside its message. An
// error napi raises while converting a call's arguments, before chronicle
// runs, keeps napi's own code or none. These run the built binding (index.js
// and index.d.ts), so they need the napi build first, as test.mjs does.
import { after, test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setFlagsFromString } from 'node:v8';
import { runInNewContext } from 'node:vm';
import { ErrorCode, JsStore } from './index.js';

setFlagsFromString('--expose-gc');
const gc = runInNewContext('gc');

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
    'Deserialization', 'DurabilityLost', 'HashMismatch', 'InvalidFieldIndexKind',
    'InvalidFormat', 'InvalidHash', 'InvalidOperation', 'InvalidRecordId',
    'InvalidSequence', 'InvalidStrategy', 'InvalidSubscriptionId', 'InvalidTreeOp', 'Io',
    'Locked', 'NotInitialized', 'RecordNotFound', 'Serialization', 'StateExists',
    'StateNotRegistered', 'StoreClosed', 'StrategyMismatch', 'SubscriptionDropped',
    'SubscriptionNotFound', 'Transaction',
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
  throwsCoded(() => store.getStateJsonAt('notes', store.currentSequence()), ErrorCode.Deserialization, /at line 1 column \d+$/);
  store.registerState({ id: 'items', strategy: 'append_log' });
  store.appendToState('items', Buffer.from('not json'));
  throwsCoded(() => store.getStateItemJson('items', 0), ErrorCode.Deserialization, /at line 1 column \d+$/);
  throwsCoded(() => store.deleteBranch('main'), ErrorCode.InvalidOperation, 'Invalid operation: Cannot delete main branch');
  store.createBranch('side');
  store.switchBranch('side');
  throwsCoded(() => store.deleteBranch('side'), ErrorCode.InvalidOperation, 'Invalid operation: Cannot delete current branch');
  store.close();
});

test("a view's refusals carry theirs", () => {
  const { store } = freshStore();
  store.createBranch('pinned');
  const view = store.view('pinned');
  throwsCoded(() => view.switchBranch('main'), ErrorCode.InvalidOperation, /^switchBranch is not available on a view/);
  throwsCoded(
    () => view.deleteBranch('pinned'),
    ErrorCode.InvalidOperation,
    "Invalid operation: Cannot delete the branch this view is pinned to ('pinned')",
  );
  store.deleteBranch('pinned');
  throwsCoded(
    () => view.getStateJson('anything'),
    ErrorCode.BranchNotFound,
    "Branch not found: 'pinned' (the branch this view is pinned to has been deleted)",
  );
  const closed = store.view('main');
  closed.close();
  throwsCoded(() => closed.getStateJson('anything'), ErrorCode.StoreClosed, 'View has been closed');
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

const owners = [['the root', (store) => store], ['a view', (store) => store.view('main')]];

for (const [made, ownerOf] of owners) {
  test(`after the root's close(), a poll of a subscription made through ${made} returns what was queued, then says the store is closed`, () => {
    const { store } = freshStore();
    const owner = ownerOf(store);
    const id = owner.subscribe();
    owner.catchUpSubscription(id);
    store.close();
    assert.equal(owner.pollSubscription(id)?.eventType, 'caught_up');
    throwsCoded(() => owner.pollSubscription(id), ErrorCode.StoreClosed, 'Store has been closed');
    throwsCoded(() => owner.pollSubscriptionTimeout(id, 1), ErrorCode.StoreClosed, 'Store has been closed');
  });

  test(`a poll of a subscription made through ${made} that its buffer overflowed returns what was queued, then says it was dropped`, () => {
    const { store } = freshStore();
    const owner = ownerOf(store);
    const id = owner.subscribe({ bufferSize: 1, filter: { includeRecords: true } });
    owner.catchUpSubscription(id);
    store.append('note', Buffer.from('a'));
    assert.equal(store.subscriptionCount(), 0);
    assert.equal(owner.pollSubscription(id)?.eventType, 'caught_up');
    throwsCoded(() => owner.pollSubscription(id), ErrorCode.SubscriptionDropped, 'Subscription was dropped');
    throwsCoded(() => owner.pollSubscriptionTimeout(id, 1), ErrorCode.SubscriptionDropped, 'Subscription was dropped');
    store.close();
  });

  test(`a poll of a subscription made through ${made} and unsubscribed through another handle returns what was queued, then says it is not found`, () => {
    const { store } = freshStore();
    const owner = ownerOf(store);
    const other = owner === store ? store.view('main') : store;
    const id = owner.subscribe();
    other.unsubscribe(id);
    assert.equal(owner.pollSubscription(id)?.eventType, 'dropped');
    throwsCoded(() => owner.pollSubscription(id), ErrorCode.SubscriptionNotFound, 'Subscription not found');
    throwsCoded(() => owner.pollSubscriptionTimeout(id, 1), ErrorCode.SubscriptionNotFound, 'Subscription not found');
    store.close();
  });
}

test("a poll through a view that was closed of the view's own subscription says it is not found", () => {
  const { store } = freshStore();
  const view = store.view('main');
  const id = view.subscribe();
  assert.equal(store.subscriptionCount(), 1);
  view.close();
  throwsCoded(() => view.pollSubscription(id), ErrorCode.SubscriptionNotFound, 'Subscription not found');
  throwsCoded(() => view.pollSubscriptionTimeout(id, 1), ErrorCode.SubscriptionNotFound, 'Subscription not found');
  throwsCoded(() => store.catchUpSubscription(id), ErrorCode.SubscriptionNotFound, 'Subscription not found');
  assert.equal(store.subscriptionCount(), 0);
  store.close();
});

test('a root dropped without close() takes its own subscriptions with it, and a view it leaves keeps its own', async () => {
  const { store, dir } = freshStore();
  store.close();
  let view;
  const ids = [];
  (() => {
    const root = JsStore.open({ path: dir });
    view = root.view('main');
    for (let i = 0; i < 20; i++) ids.push(root.subscribe());
  })();
  const own = view.subscribe();
  assert.equal(view.subscriptionCount(), 21);
  for (let i = 0; i < 200 && view.subscriptionCount() > 1; i++) {
    gc();
    await new Promise((resolve) => setImmediate(resolve));
  }
  assert.equal(view.subscriptionCount(), 1);
  for (const id of ids) throwsCoded(() => view.catchUpSubscription(id), ErrorCode.SubscriptionNotFound, 'Subscription not found');
  view.catchUpSubscription(own);
  assert.equal(view.pollSubscription(own)?.eventType, 'caught_up');
  view = null;
  let reopened = null;
  for (let i = 0; i < 200 && reopened === null; i++) {
    gc();
    await new Promise((resolve) => setImmediate(resolve));
    try {
      reopened = JsStore.open({ path: dir });
    } catch (error) {
      if (error.code !== ErrorCode.Locked) throw error;
    }
  }
  assert.notEqual(reopened, null);
  reopened.close();
});

test('a view dropped without close() takes its subscriptions with it, and the root keeps its own', async () => {
  const { store } = freshStore();
  const own = store.subscribe();
  const ids = [];
  (() => {
    for (let i = 0; i < 20; i++) ids.push(store.view('main').subscribe());
  })();
  assert.equal(store.subscriptionCount(), 21);
  for (let i = 0; i < 200 && store.subscriptionCount() > 1; i++) {
    gc();
    await new Promise((resolve) => setImmediate(resolve));
  }
  assert.equal(store.subscriptionCount(), 1);
  for (const id of ids) throwsCoded(() => store.catchUpSubscription(id), ErrorCode.SubscriptionNotFound, 'Subscription not found');
  store.catchUpSubscription(own);
  assert.equal(store.pollSubscription(own)?.eventType, 'caught_up');
  store.close();
});

test('a catch-up replays the branch of the handle it is called on', () => {
  const { store } = freshStore();
  store.append('on-main', Buffer.from('m'));
  store.createBranch('side');
  const side = store.view('side');
  side.append('on-side', Buffer.from('s'));
  const config = { fromSequence: 0, filter: { includeRecords: true } };
  const events = (id) => {
    const out = [];
    for (let event = side.pollSubscription(id); event; event = side.pollSubscription(id)) out.push(event);
    return out;
  };
  const throughRoot = side.subscribe(config);
  store.catchUpSubscription(throughRoot);
  const fromRoot = events(throughRoot);
  assert.equal(fromRoot.at(-1)?.eventType, 'caught_up');
  assert.ok(fromRoot.some((e) => e.eventType === 'record' && e.data.includes('"on-main"')), JSON.stringify(fromRoot));
  assert.ok(!fromRoot.some((e) => e.data.includes('"on-side"')), JSON.stringify(fromRoot));
  const throughSide = side.subscribe(config);
  side.catchUpSubscription(throughSide);
  const fromSide = events(throughSide);
  assert.equal(fromSide.at(-1)?.eventType, 'caught_up');
  assert.ok(fromSide.some((e) => e.eventType === 'record' && e.data.includes('"on-side"')), JSON.stringify(fromSide));
  store.close();
});

test('a catch-up that overruns the subscription\'s buffer drops it', () => {
  const { store } = freshStore();
  for (const n of [1, 2, 3]) store.append('note', Buffer.from(String(n)));
  const id = store.subscribe({ bufferSize: 1, fromSequence: 0, filter: { includeRecords: true } });
  assert.equal(store.subscriptionCount(), 1);
  throwsCoded(() => store.catchUpSubscription(id), ErrorCode.SubscriptionDropped, 'Subscription was dropped');
  assert.equal(store.subscriptionCount(), 0);
  throwsCoded(() => store.catchUpSubscription(id), ErrorCode.SubscriptionDropped, 'Subscription was dropped');
  store.close();
});

test('a subscription is caught up, or not found once unsubscribed, through any handle of its store', () => {
  const { store } = freshStore();
  const view = store.view('main');
  const onRoot = store.subscribe();
  const onView = view.subscribe();
  view.catchUpSubscription(onRoot);
  store.catchUpSubscription(onView);
  assert.equal(store.pollSubscription(onRoot)?.eventType, 'caught_up');
  assert.equal(view.pollSubscription(onView)?.eventType, 'caught_up');
  view.unsubscribe(onRoot);
  store.unsubscribe(onView);
  for (const handle of [store, view]) {
    for (const id of [onRoot, onView]) {
      throwsCoded(() => handle.catchUpSubscription(id), ErrorCode.SubscriptionNotFound, 'Subscription not found');
    }
  }
  const closing = store.view('main');
  const onClosing = closing.subscribe();
  assert.equal(store.subscriptionCount(), 1);
  closing.close();
  throwsCoded(() => store.catchUpSubscription(onClosing), ErrorCode.SubscriptionNotFound, 'Subscription not found');
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
