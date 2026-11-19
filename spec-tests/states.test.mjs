import assert from 'node:assert/strict';
import { test } from 'node:test';
import { ErrorCode, JsStore, freshStore, refuses } from './support.mjs';

const lastKind = (store) => Object.keys(JSON.parse(store.getStateUpdateJson(store.getRecordIdsByType('state_update').at(-1))).operation)[0];

function shortCutsAfter(store, write, count) {
  const before = store.currentSequence();
  for (let n = 0; n < count; n++) write(n);
  return store.currentSequence() - before - count;
}

test('A28 T11 when left out, a list takes a small short cut after 100 changes and a tree after 50', () => {
  const { store } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log' });
  store.registerState({ id: 'tree', strategy: 'tree' });
  assert.equal(shortCutsAfter(store, (n) => store.appendToStateJson('list', n), 99), 0);
  assert.equal(shortCutsAfter(store, (n) => store.appendToStateJson('list', n), 1), 1);
  assert.equal(lastKind(store), 'DeltaSnapshot');
  assert.equal(shortCutsAfter(store, (n) => store.treeSet('tree', `p${n}`, { blobHash: 'h', size: 1, mode: 0 }), 49), 0);
  assert.equal(shortCutsAfter(store, (n) => store.treeSet('tree', `q${n}`, { blobHash: 'h', size: 1, mode: 0 }), 1), 1);
  assert.equal(lastKind(store), 'TreeDeltaSnapshot');
  store.close();
});

test('A28 T11 when left out, fullSnapshotEvery is 10: the first nine short cuts of a new list are small, and the tenth is whole', () => {
  const { store } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log', deltaSnapshotEvery: 1 });
  for (let n = 0; n < 10; n++) store.appendToStateJson('list', n);
  const shortCuts = store
    .getRecordIdsByType('state_update')
    .map((id) => Object.keys(JSON.parse(store.getStateUpdateJson(id)).operation)[0])
    .filter((kind) => kind !== 'Append');
  assert.deepEqual(shortCuts, [...Array(9).fill('DeltaSnapshot'), 'Snapshot']);
  store.close();
});

const shortCutKinds = (store) => store
  .getRecordIdsByType('state_update')
  .map((id) => Object.keys(JSON.parse(store.getStateUpdateJson(id)).operation)[0])
  .filter((kind) => kind !== 'Append' && kind !== 'Edit');

test('T11 a whole short cut comes when the changes since the last whole one reach the larger of deltaSnapshotEvery times fullSnapshotEvery and the items at the last whole one', () => {
  const { store } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log', deltaSnapshotEvery: 1, fullSnapshotEvery: 2 });
  for (let n = 0; n < 16; n++) store.appendToStateJson('list', n);
  const kinds = shortCutKinds(store);
  assert.equal(kinds.length, 16);
  assert.deepEqual(kinds.flatMap((kind, n) => (kind === 'Snapshot' ? [n + 1] : [])), [2, 4, 8, 16]);
  store.close();
});

test('T11 the store adds no small short cut while the list has had an edit since its last short cut', () => {
  for (const edited of [false, true]) {
    const { store } = freshStore();
    store.registerState({ id: 'list', strategy: 'append_log', deltaSnapshotEvery: 2, fullSnapshotEvery: 100 });
    store.appendToStateJson('list', 0);
    if (edited) store.editStateItem('list', 0, Buffer.from('1'));
    for (let n = 0; n < 5; n++) store.appendToStateJson('list', n);
    assert.equal(shortCutKinds(store).includes('DeltaSnapshot'), !edited, `edited: ${edited}`);
    store.close();
  }
});

test('A28 T11 initialValue is taken and not used', () => {
  const { store } = freshStore();
  store.registerState({ id: 'value', strategy: 'snapshot', initialValue: Buffer.from('"start"') });
  store.registerState({ id: 'list', strategy: 'append_log', initialValue: Buffer.from('[1]') });
  assert.equal(store.getState('value'), null);
  assert.equal(store.getState('list'), null);
  store.close();
});

test("A29 updateStateStrategy gives a registered list the caller's numbers, where registerState throws StateExists", () => {
  const { store } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log' });
  refuses(() => store.registerState({ id: 'list', strategy: 'append_log', deltaSnapshotEvery: 3 }), ErrorCode.StateExists, 'State already exists: list');
  store.updateStateStrategy({ id: 'list', strategy: 'append_log', deltaSnapshotEvery: 3 });
  assert.equal(shortCutsAfter(store, (n) => store.appendToStateJson('list', n), 2), 0);
  assert.equal(shortCutsAfter(store, (n) => store.appendToStateJson('list', n), 1), 1);
  assert.deepEqual(store.listStates().map((state) => [state.id, state.strategy]), [['list', 'append_log']]);
  store.close();
});

test("A29 E2 updateStateStrategy does not change a state's kind, and does not use initialValue", () => {
  const { store } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log' });
  store.appendToStateJson('list', 1);
  refuses(() => store.updateStateStrategy({ id: 'list', strategy: 'snapshot' }), ErrorCode.InvalidOperation, /^Invalid operation: /);
  store.updateStateStrategy({ id: 'list', strategy: 'append_log', initialValue: Buffer.from('[9]') });
  assert.deepEqual(store.listStates().map((state) => state.strategy), ['append_log']);
  assert.deepEqual(store.getStateJson('list'), [1]);
  store.close();
});

test('A30 a list that a setState gave other bytes reads as those bytes', () => {
  const { store } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log' });
  store.appendToStateJson('list', 1);
  store.setState('list', Buffer.from('{"not":"an array"}'));
  assert.deepEqual(store.getState('list'), Buffer.from('{"not":"an array"}'));
  store.close();
});

test('A31 getStateJson is null for a stored JSON null, which getState reads as the bytes null', () => {
  const { store } = freshStore();
  store.registerState({ id: 'value', strategy: 'snapshot' });
  store.setStateJson('value', null);
  assert.equal(store.getStateJson('value'), null);
  assert.deepEqual(store.getState('value'), Buffer.from('null'));
  store.close();
});

test('A32 setState sets the whole value of a list, of a tree and of a name that is not registered, and answers with a state_update record', () => {
  const { store } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log' });
  store.registerState({ id: 'tree', strategy: 'tree' });
  store.appendToStateJson('list', 1);
  for (const id of ['list', 'tree', 'free']) {
    const record = store.setState(id, Buffer.from('[2,3]'));
    assert.equal(record.recordType, 'state_update');
    assert.equal(record.sequence, store.currentSequence());
    assert.deepEqual(store.getState(id), Buffer.from('[2,3]'), id);
  }
  store.close();
});

test('A47 A48 setAutoSnapshot(false) stops the short cuts of the whole store, true lets them again, and a store opens with true', () => {
  const { store, dir } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log', deltaSnapshotEvery: 2 });
  assert.equal(store.autoSnapshotEnabled(), true);
  store.view('main').setAutoSnapshot(false);
  assert.equal(store.autoSnapshotEnabled(), false);
  assert.equal(shortCutsAfter(store, (n) => store.appendToStateJson('list', n), 4), 0);
  store.setAutoSnapshot(true);
  assert.equal(store.autoSnapshotEnabled(), true);
  assert.equal(shortCutsAfter(store, (n) => store.appendToStateJson('list', n), 1), 1);
  store.setAutoSnapshot(false);
  store.close();
  const reopened = JsStore.open({ path: dir });
  assert.equal(reopened.autoSnapshotEnabled(), true);
  reopened.close();
});

test("A49 compactState adds a short cut of the whole value as one record and answers with it, or null when the state has no value there", () => {
  const { store } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log' });
  store.registerState({ id: 'empty', strategy: 'append_log' });
  store.appendToStateJson('list', { n: 1 });
  store.appendToStateJson('list', { n: 2 });
  const before = store.currentSequence();
  const record = store.compactState('list');
  assert.equal(record.sequence, before + 1);
  assert.equal(store.currentSequence(), before + 1);
  const { operation } = JSON.parse(store.getStateUpdateJson(record.id));
  assert.deepEqual(Object.keys(operation), ['Snapshot']);
  assert.deepEqual(JSON.parse(Buffer.from(operation.Snapshot).toString('utf8')), [{ n: 1 }, { n: 2 }]);
  assert.deepEqual(store.getStateJson('list'), [{ n: 1 }, { n: 2 }]);
  assert.equal(store.compactState('empty'), null);
  assert.equal(store.currentSequence(), before + 1);
  store.close();
});

test("A50 compactAllStates compacts each registered state on the handle's branch, and answers the number it added a record for", () => {
  const { store } = freshStore();
  for (const id of ['a', 'b', 'c']) store.registerState({ id, strategy: 'append_log' });
  store.appendToStateJson('a', 1);
  store.appendToStateJson('b', 1);
  const before = store.currentSequence();
  assert.equal(store.compactAllStates(), 2);
  assert.equal(store.currentSequence(), before + 2);
  const added = store.getRecordIdsByType('state_update').slice(-2).map((id) => JSON.parse(store.getStateUpdateJson(id)));
  assert.deepEqual(added.map((update) => [update.state_id, Object.keys(update.operation)[0]]).sort(), [['a', 'Snapshot'], ['b', 'Snapshot']]);
  store.close();
});

test('A51 T14 getCompactionSummary is zero for a store with no change of a state, and counts the changes of the states', () => {
  const { store } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log' });
  assert.deepEqual(store.getCompactionSummary(), { totalOperations: 0, compactableOperations: 0, totalBytes: 0, compactableBytes: 0, statesNeedingCompaction: 0 });
  for (let n = 0; n < 5; n++) store.appendToStateJson('list', n);
  const summary = store.getCompactionSummary();
  assert.equal(summary.totalOperations, 5);
  assert.ok(summary.totalBytes > 0);
  store.close();
});

test("A56 T20 listStates gives one entry for each registered state, with its kind as a text and its counts on the handle's branch", () => {
  const { store } = freshStore();
  store.registerState({ id: 'value', strategy: 'snapshot' });
  store.registerState({ id: 'list', strategy: 'append_log', deltaSnapshotEvery: 100 });
  store.registerState({ id: 'tree', strategy: 'tree' });
  store.appendToStateJson('list', 1);
  store.appendToStateJson('list', 2);
  store.createBranch('side');
  store.view('side').appendToStateJson('list', 3);
  const info = (handle) => Object.fromEntries(handle.listStates().map((state) => [state.id, state]));
  const main = info(store);
  assert.deepEqual(Object.keys(main).sort(), ['list', 'tree', 'value']);
  assert.deepEqual([main.value.strategy, main.list.strategy, main.tree.strategy], ['snapshot', 'append_log', 'tree']);
  assert.equal(main.list.opsSinceSnapshot, 2);
  assert.equal(main.value.opsSinceSnapshot, 0);
  assert.equal(info(store.view('side')).list.opsSinceSnapshot, 3);
  store.close();
});

test('A58 getStateAt is null when the state had no value at that sequence', () => {
  const { store } = freshStore();
  store.registerState({ id: 'value', strategy: 'snapshot' });
  store.appendJson('note', {});
  const before = store.currentSequence();
  store.setStateJson('value', 1);
  assert.equal(store.getStateAt('value', before), null);
  assert.deepEqual(store.getStateAt('value', store.currentSequence()), Buffer.from('1'));
  store.close();
});
