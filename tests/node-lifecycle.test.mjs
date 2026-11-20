import { test } from 'node:test';
import assert from 'node:assert/strict';
import { cpSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
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
  // pins redactStateItems.4 normal return already made the redaction visible
  assert.deepEqual(root.getStateJson('messages'), [message]);
  const receiptType = 'context-manager/accepted-layout';
  const receipt = root.appendJson(receiptType, { receipt: { note: 'accepted' } });
  const subId = root.subscribe({});
  root.catchUpSubscription(subId); // the ID to be passed after close is a live subscription
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
    // pins currentBranch.7 unavailable selection throws, not a default branch
    assert.throws(() => handle.currentBranch(), which);
    // pins currentSequence.5 unreadable head throws on the calling stack
    assert.throws(() => handle.currentSequence(), which);
    // pins listStates.4 an unreadable registration inventory is not a successful list
    assert.throws(() => handle.listStates(), which);
    // pins listBranches.3 failure to list branches synchronously signals unknown
    assert.throws(() => handle.listBranches(), which);
    // pins close.9 a retained closed handle no longer reads its messages
    assert.throws(() => handle.getStateJson('messages'), which);
    // pins close.9 receipt inspection on that same handle no longer works
    assert.throws(() => handle.getRecord(receipt.id), which);
    // pins getStateLen.2 an existing branch's unreadable count throws
    assert.throws(() => handle.getStateLen('messages'), which);
    // pins stateHeadOffset.7 an unreadable revision is neither stale nor absent
    assert.throws(() => handle.stateHeadOffset('messages'), which);
    // pins getRecordIdsByType.7 unreadable type history throws rather than listing IDs
    assert.throws(() => handle.getRecordIdsByType(receiptType), which);
    // pins redactStateItems.4 a failed, well-formed redaction throws at the call
    assert.throws(() => handle.redactStateItems('messages', 0, 1), which);
    // pins subscribe.14 failed creation throws rather than returning an ID
    assert.throws(() => handle.subscribe({}), which);
    // pins unsubscribe.4 removal of a valid ID on a dead handle throws at the call
    assert.throws(() => handle.unsubscribe(subId), which);
  }

  detached.close();
  assertDead(detached, 'individually detached view');
  assert.equal(root.getStateLen('messages'), 1); // the branch itself is still live
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
  // pins isClosed.6 the detached handle reports that it cannot serve an identity lookup
  assert.equal(detached.isClosed(), true);
  // pins isClosed.6 closing a sibling does not direct the open handle to a cache-only lookup
  assert.equal(root.isClosed(), false);
  const ids = root.getRecordIdsByType(type);
  // pins isClosed.6 the surviving handle still resolves the existing identity, without a mint
  assert.deepEqual(ids, [record.id]);
  assert.equal(JSON.parse(root.getRecord(ids[0]).payload.toString()).storeId, storeId);
  assert.equal(root.stats().recordCount, countBefore);

  const held = root.view('main');
  assert.equal(held.isClosed(), false);
  const probe = nextTurn().then(() => {
    // pins isClosed.7 the scheduled callback gets true (not a throw or a promise) when it runs
    assert.equal(held.isClosed(), true);
  });
  root.close();
  await probe;
  // pins isClosed.6 the retained root now reports closed to the identity reader
  assert.equal(root.isClosed(), true);
});

test('Fresh factory handles advertise capabilities before state setup', (t) => {
  const { dir, keep } = workspace(t);
  function checkReadAndIndexCapabilities(handle) {
    // pins stateHeadOffset.8 revision support is detected without a trial call
    assert.equal(typeof handle.stateHeadOffset, 'function');
    // pins registerStateFieldIndex.1 index registration is function-detectable
    assert.equal(typeof handle.registerStateFieldIndex, 'function');
    // pins queryStateIndexRange.1 the range query is exposed under this name
    assert.equal(typeof handle.queryStateIndexRange, 'function');
    // pins getStateIndexValueCounts.1 the indexed history count capability is callable
    assert.equal(typeof handle.getStateIndexValueCounts, 'function');
    // pins getStateSlice.1 slice support is detected without a state to slice
    assert.equal(typeof handle.getStateSlice, 'function');
  }

  const path = join(dir, 'created');
  for (const factory of ['create', 'open']) {
    const handle = keep(JsStore[factory]({ path }));
    // pins appendManyToStateJsonWithIdentity.1 both creation and cold open advertise the batch before a write
    assert.equal(typeof handle.appendManyToStateJsonWithIdentity, 'function', factory);
    checkReadAndIndexCapabilities(handle);
    handle.close();
  }
  const fresh = keep(JsStore.openOrCreate({ path: join(dir, 'absent') }));
  // pins openOrCreate.5 the just-returned session handle supports batches before registration
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
  // pins openOrCreate.10 the absent sibling starts with no records or registrations and no access to the first blob
  assert.deepEqual([second.stats().recordCount, second.listStates(), second.getBlob(hash)], [0, [], null]);

  const ledgerDir = join(path, 'recovery');
  mkdirSync(ledgerDir, { recursive: true });
  const ledger = join(ledgerDir, 'discord-awareness-outbox.json');
  const ledgerBytes = Buffer.from('{ "version": 3, "moment": 4, "batches": [], "retracts": [], "entries": [] }\n');
  writeFileSync(ledger, ledgerBytes);
  const caches = new Map([[first, state]]);
  first.close();
  const reopened = keep(JsStore.openOrCreate({ path }));
  // pins openOrCreate.13 a valid store reopens without disturbing the application's ledger bytes
  assert.deepEqual(readFileSync(ledger), ledgerBytes);
  assert.deepEqual(reopened.getStateJson('session'), state);
  assert.deepEqual(reopened.getBlob(hash), bytes);
  // pins openOrCreate.14 the next open is a distinct key, not the cached closed object
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

  cpSync(residentPath, repairPath, { recursive: true });
  const repair = keep(JsStore.open({ path: repairPath }));
  const report = { branch: repair.currentBranch().name, stateId: slot, sequenceBefore: repair.currentSequence() };
  assert.equal(report.branch, 'work');
  repair.setStateJson(slot, repaired);
  assert.deepEqual(repair.getStateJson(slot), repaired);
  // pins currentSequence.12 the captured head includes the other slot's write and still addresses the old summaries
  assert.deepEqual([report.sequenceBefore, repair.getStateJsonAt(slot, report.sequenceBefore)], [lastWrite.sequence, old]);
  repair.redactStateItems('messages', 0, 1);
  // pins getStateJsonAt.3 both slots as of the last append: the newly added message and the not-yet-repaired summaries
  assert.deepEqual(repair.getStateJsonAt('messages', report.sequenceBefore), [message]);
  assert.deepEqual(repair.getStateJsonAt(report.stateId, report.sequenceBefore), old);
  // pins getStateJsonAt.7 an absent historical slot returns strictly null
  assert.equal(repair.getStateJsonAt('no-such-slot', report.sequenceBefore), null);

  repair.switchBranch('main');
  repair.switchBranch(report.branch);
  // pins open.12 reselecting the reopened root's reported branch recovers the dropped text at the saved head
  assert.deepEqual(repair.getStateJsonAt(report.stateId, report.sequenceBefore), old);
  repair.switchBranch('main'); // cold readback must select the branch named in the report
  repair.close();
  cpSync(repairPath, readbackPath, { recursive: true });
  const readback = keep(JsStore.open({ path: readbackPath }));
  readback.switchBranch(report.branch);
  // pins getStateJsonAt.8 the report's coordinates on a stopped copy recover the persisted pre-repair text
  assert.deepEqual(readback.getStateJsonAt(report.stateId, report.sequenceBefore), old);
  assert.deepEqual(readback.getStateJson(report.stateId), repaired);
  // pins getStateJsonAt.7 cold readback also distinguishes an absent slot from a failure
  assert.equal(readback.getStateJsonAt('no-such-slot', report.sequenceBefore), null);

  const original = keep(JsStore.open({ path: residentPath }));
  // pins open.4 repairing the copy left the resident's selection, head and summaries alone
  assert.deepEqual([original.currentBranch().name, original.currentSequence(), original.getStateJson(slot)], ['work', originalHead, old]);
});
