import { test } from 'node:test';
import assert from 'node:assert/strict';
import { cpSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, truncateSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setImmediate as nextTurn } from 'node:timers/promises';
import { JsStore } from '../index.js';

function workspace(t) {
  const dir = mkdtempSync(join(tmpdir(), 'chronicle-lifecycle-'));
  const roots = [];
  t.after(() => {
    try {
      for (const root of roots) if (!root.isClosed()) root.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
  return { dir, keep(root) { roots.push(root); return root; } };
}

test('Dead handles throw instead of reporting empty inventories, records or revisions', (t) => {
  const { dir, keep } = workspace(t);
  const root = keep(JsStore.create({ path: join(dir, 'store') }));
  const message = { text: 'still here' };
  root.registerState({ id: 'messages', strategy: 'append_log' });
  root.appendToStateJson('messages', { text: 'remove me' });
  root.appendToStateJson('messages', message);
  root.redactStateItems('messages', 0, 1);
  assert.deepEqual(root.getStateJson('messages'), [message]);
  const receiptType = 'context-manager/accepted-layout';
  const receipt = root.appendJson(receiptType, { receipt: { note: 'accepted' } });
  const subId = root.subscribe({});
  root.catchUpSubscription(subId);
  root.unsubscribe(root.subscribe({}));

  const detached = root.view('main');
  const held = root.view('main');
  for (const handle of [root, detached, held]) {
    assert.equal(handle.currentBranch().name, 'main');
    assert.equal(handle.currentSequence(), receipt.sequence);
    assert.ok(handle.listBranches().some((b) => b.name === 'main'));
    assert.ok(handle.listStates().some((s) => s.id === 'messages'));
    assert.deepEqual(handle.getStateJson('messages'), [message]);
    assert.equal(handle.getStateLen('messages'), 1);
    assert.notEqual(handle.stateHeadOffset('messages'), null);
    assert.deepEqual(handle.getRecordIdsByType(receiptType), [receipt.id]);
    assert.equal(handle.getRecord(receipt.id).id, receipt.id);
  }

  function assertDead(handle, which) {
    assert.throws(() => handle.currentBranch(), which);
    assert.throws(() => handle.currentSequence(), which);
    assert.throws(() => handle.listStates(), which);
    assert.throws(() => handle.listBranches(), which);
    assert.throws(() => handle.getStateJson('messages'), which);
    assert.throws(() => handle.getRecord(receipt.id), which);
    assert.throws(() => handle.getStateLen('messages'), which);
    assert.throws(() => handle.stateHeadOffset('messages'), which);
    assert.throws(() => handle.getState('messages'), (error) => !error.message.startsWith('Deserialization error: '), which);
    assert.throws(() => handle.getRecordIdsByType(receiptType), which);
    assert.throws(() => handle.redactStateItems('messages', 0, 1), which);
    assert.throws(() => handle.subscribe({}), which);
    assert.throws(() => handle.unsubscribe(subId), which);
  }

  detached.close();
  assertDead(detached, 'individually detached view');
  assert.equal(root.getStateLen('messages'), 1);
  root.close();
  assertDead(root, 'retained closed root');
  assertDead(held, 'view closed by its root');
});

test('Liveness is sampled on the handle that is about to read', async (t) => {
  const { dir, keep } = workspace(t);
  const root = keep(JsStore.create({ path: join(dir, 'store') }));
  const detached = root.view('main');
  const type = 'context-manager/store-identity';
  const storeId = '8e8e7d9c-ea0f-44d6-a1ac-7013852a6f08';
  const record = detached.appendJson(type, { v: 1, storeId });
  const countBefore = root.stats().recordCount;
  detached.close();
  assert.equal(detached.isClosed(), true);
  assert.equal(root.isClosed(), false);
  const ids = root.getRecordIdsByType(type);
  assert.deepEqual(ids, [record.id]);
  assert.equal(JSON.parse(root.getRecord(ids[0]).payload.toString()).storeId, storeId);
  assert.equal(root.stats().recordCount, countBefore);

  const held = root.view('main');
  assert.equal(held.isClosed(), false);
  const probe = nextTurn().then(() => {
    assert.equal(held.isClosed(), true);
    assert.throws(() => held.getRecord(record.id), /closed/);
  });
  root.close();
  await probe;
  assert.equal(root.isClosed(), true);
});

test('Fresh factory handles advertise capabilities before state setup', (t) => {
  const { dir, keep } = workspace(t);
  function checkReadAndIndexCapabilities(handle) {
    assert.equal(typeof handle.stateHeadOffset, 'function');
    assert.equal(typeof handle.registerStateFieldIndex, 'function');
    assert.equal(typeof handle.queryStateIndexRange, 'function');
    assert.equal(typeof handle.getStateIndexValueCounts, 'function');
    assert.equal(typeof handle.getStateSlice, 'function');
  }

  const path = join(dir, 'created');
  for (const factory of ['create', 'open']) {
    const handle = keep(JsStore[factory]({ path }));
    assert.equal(typeof handle.appendManyToStateJsonWithIdentity, 'function', factory);
    checkReadAndIndexCapabilities(handle);
    handle.close();
  }
  const fresh = keep(JsStore.openOrCreate({ path: join(dir, 'absent') }));
  assert.equal(typeof fresh.appendManyToStateJsonWithIdentity, 'function');
  checkReadAndIndexCapabilities(fresh);
});

test('Session directories isolate stores and preserve application-owned neighbors', (t) => {
  const { dir, keep } = workspace(t);
  const path = join(dir, 'first-session');
  const first = keep(JsStore.openOrCreate({ path }));
  const state = { session: 'first' };
  const bytes = Buffer.from('first session attachment');
  first.registerState({ id: 'session', strategy: 'snapshot' });
  first.setStateJson('session', state);
  const hash = first.storeBlob(bytes, 'text/plain');

  const second = keep(JsStore.openOrCreate({ path: join(dir, 'second-session') }));
  assert.deepEqual([second.stats().recordCount, second.listStates(), second.getBlob(hash)], [0, [], null]);

  const ledgerDir = join(path, 'recovery');
  mkdirSync(ledgerDir, { recursive: true });
  const ledger = join(ledgerDir, 'discord-awareness-outbox.json');
  const ledgerBytes = Buffer.from('{ "version": 3, "moment": 4, "batches": [], "retracts": [], "entries": [] }\n');
  writeFileSync(ledger, ledgerBytes);
  const caches = new Map([[first, state]]);
  first.close();
  const reopened = keep(JsStore.openOrCreate({ path }));
  assert.deepEqual(readFileSync(ledger), ledgerBytes);
  assert.deepEqual(reopened.getStateJson('session'), state);
  assert.deepEqual(reopened.getBlob(hash), bytes);
  assert.equal(caches.has(reopened), false);
});

test('Repair on a stopped copy preserves historical coordinates and leaves the original alone', (t) => {
  const { dir, keep } = workspace(t);
  const residentPath = join(dir, 'resident');
  const repairPath = join(dir, 'repair');
  const readbackPath = join(dir, 'readback');
  const slot = 'window/summaries';
  const old = [{ id: 'keep', text: 'retained' }, { id: 'drop', text: 'recover this text' }];
  const repaired = [old[0]];
  const message = { id: 'last-write', text: 'in another slot' };
  const resident = keep(JsStore.create({ path: residentPath }));
  resident.registerState({ id: slot, strategy: 'snapshot' });
  resident.registerState({ id: 'messages', strategy: 'append_log' });
  resident.setStateJson(slot, [{ id: 'main', text: 'different branch' }]);
  resident.createBranch('work');
  resident.switchBranch('work');
  resident.setStateJson(slot, old);
  const lastWrite = resident.appendToStateJson('messages', message);
  const originalHead = resident.currentSequence();
  resident.close();

  cpSync(residentPath, repairPath, { recursive: true, verbatimSymlinks: true });
  const repair = keep(JsStore.open({ path: repairPath }));
  const report = { branch: repair.currentBranch().name, stateId: slot, sequenceBefore: repair.currentSequence() };
  assert.equal(report.branch, 'work');
  repair.setStateJson(slot, repaired);
  assert.deepEqual(repair.getStateJson(slot), repaired);
  assert.deepEqual([report.sequenceBefore, repair.getStateJsonAt(slot, report.sequenceBefore)], [lastWrite.sequence, old]);
  repair.redactStateItems('messages', 0, 1);
  assert.deepEqual(repair.getStateJsonAt('messages', report.sequenceBefore), [message]);
  assert.deepEqual(repair.getStateJsonAt(report.stateId, report.sequenceBefore), old);
  assert.equal(repair.getStateJsonAt('no-such-slot', report.sequenceBefore), null);

  repair.switchBranch('main');
  repair.switchBranch(report.branch);
  assert.deepEqual(repair.getStateJsonAt(report.stateId, report.sequenceBefore), old);
  repair.switchBranch('main');
  repair.close();
  cpSync(repairPath, readbackPath, { recursive: true, verbatimSymlinks: true });
  const readback = keep(JsStore.open({ path: readbackPath }));
  readback.switchBranch(report.branch);
  assert.deepEqual(readback.getStateJsonAt(report.stateId, report.sequenceBefore), old);
  assert.deepEqual(readback.getStateJson(report.stateId), repaired);
  assert.equal(readback.getStateJsonAt('no-such-slot', report.sequenceBefore), null);

  const original = keep(JsStore.open({ path: residentPath }));
  assert.deepEqual([original.currentBranch().name, original.currentSequence(), original.getStateJson(slot)], ['work', originalHead, old]);
});

test('A copy with a torn tail recovers at its own path while the resident stays open and whole', (t) => {
  const { dir, keep } = workspace(t);
  const residentPath = join(dir, 'resident');
  const copyPath = join(dir, 'copy');
  const resident = keep(JsStore.create({ path: residentPath }));
  const first = resident.appendJson('journal', { n: 1 });
  const last = resident.appendJson('journal', { n: 2 });
  resident.sync();
  const residentLog = readFileSync(join(residentPath, 'records.log'));

  cpSync(residentPath, copyPath, { recursive: true, verbatimSymlinks: true });
  const copyLog = join(copyPath, 'records.log');
  truncateSync(copyLog, statSync(copyLog).size - 1);
  const copy = keep(JsStore.open({ path: copyPath }));
  const report = copy.recovery();
  assert.deepEqual([report?.droppedBytes > 0, report?.validRecords], [true, 1]);
  assert.deepEqual([copy.getRecord(first.id)?.id, copy.getRecord(last.id)], [first.id, null]);
  assert.deepEqual(copy.getRecordIdsByType('journal'), [first.id]);

  assert.deepEqual(readFileSync(join(residentPath, 'records.log')), residentLog);
  assert.deepEqual(resident.getRecordIdsByType('journal'), [first.id, last.id]);
  assert.equal(resident.getRecord(last.id).id, last.id);
  resident.close();
  const reopened = keep(JsStore.open({ path: residentPath }));
  assert.equal(reopened.recovery(), null);
  assert.deepEqual(reopened.getRecordIdsByType('journal'), [first.id, last.id]);
});

test('Clearing a slot whole advances the branch and leaves its earlier rows at the saved sequence', (t) => {
  const { dir, keep } = workspace(t);
  const path = join(dir, 'store');
  const slot = 'window/summaries';
  const old = [{ id: 'keep', text: 'retained' }, { id: 'drop', text: 'recover this text' }];
  let store = keep(JsStore.create({ path }));
  store.registerState({ id: slot, strategy: 'snapshot' });
  store.setStateJson(slot, old);
  const cleared = [];
  for (const empty of [[], null]) {
    const saved = store.currentSequence();
    const write = store.setStateJson(slot, empty);
    assert.deepEqual([store.currentSequence() > saved, write.sequence, store.getStateJson(slot)], [true, store.currentSequence(), empty]);
    assert.deepEqual(store.getStateJsonAt(slot, saved), old);
    cleared.push(saved);
    store.setStateJson(slot, old);
  }
  store.close();
  store = keep(JsStore.open({ path }));
  for (const saved of cleared) assert.deepEqual(store.getStateJsonAt(slot, saved), old);
});
