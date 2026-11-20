import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { JsStore } from '../index.js';

function freshStore(t) {
  const parent = mkdtempSync(join(tmpdir(), 'chronicle-state-catalog-'));
  const path = join(parent, 'store');
  let root;
  t.after(() => {
    try { root?.close(); } finally { rmSync(parent, { recursive: true, force: true }); }
  });
  root = JsStore.openOrCreate({ path });
  return {
    store: root,
    reopen() {
      root.close();
      root = JsStore.open({ path });
      return root;
    },
  };
}

const baseId = 'default/autobio';
const summariesId = `${baseId}:summaries`;
const resolutionsId = `${baseId}:resolutions`;
const counterId = `${baseId}:counter`;
const receiptId = 'default/kvunified:presentation-receipt';
const snapshot = (id) => ({ id, strategy: 'snapshot' });
const log = (id) => ({ id, strategy: 'append_log' });
const idsOf = (rows) => rows.map(({ id }) => id);
const pairsOf = (rows) => rows.map(({ id, strategy }) => [id, strategy]).sort(([a], [b]) => a.localeCompare(b));
const readValues = (handle, ids) => Object.fromEntries(ids.map((id) => [id, handle.getStateJson(id)]));
function revisions(handle, ids) {
  const { recordCount, stateSlotCount } = handle.stats();
  return {
    branchHead: handle.currentSequence(),
    stateHeads: ids.map((id) => handle.stateHeadOffset(id)),
    records: recordCount,
    slots: stateSlotCount,
  };
}

test('Registration inventory is global, exact, read-only and usable by ID after reopen', (t) => {
  const { store, reopen } = freshStore(t);
  assert.deepEqual(store.listStates(), []);

  store.createBranch('draft');
  const draft = store.view('draft');
  const adaId = 'agents/Ada/messages';
  const treeId = 'workspace/intent:tree';
  const registrations = [snapshot(baseId), log('messages'), log(adaId), log(summariesId),
    snapshot(resolutionsId), { id: treeId, strategy: 'tree' }];
  const ids = idsOf(registrations);
  const missing = 'other/autobio:summaries';

  store.registerState(registrations[0]);
  assert.deepEqual(pairsOf(draft.listStates()), [[baseId, 'snapshot']]);
  store.setStateJson(baseId, { profile: 'main' });
  draft.setStateJson(baseId, { profile: 'draft' });
  for (const reg of registrations.slice(1)) draft.registerState(reg);
  assert.deepEqual(pairsOf(store.listStates()), pairsOf(registrations));

  for (const branch of ['draft', 'main']) {
    store.switchBranch(branch);
    assert.deepEqual({ ids: idsOf(store.listStates()).sort(), profile: store.getStateJson(baseId),
      newHeads: ids.slice(1).map((id) => store.stateHeadOffset(id)) }, {
      ids: [...ids].sort(), profile: { profile: branch }, newHeads: ids.slice(1).map(() => null),
    });
  }

  const expected = (branch) => ({
    [baseId]: { profile: branch },
    messages: [{ text: `bare/${branch}` }],
    [adaId]: [{ text: `Ada/${branch}` }],
    [summariesId]: [{ summary: branch }],
    [resolutionsId]: { level: branch === 'main' ? 1 : 3 },
    [treeId]: null,
  });
  const branches = [[store, 'main'], [draft, 'draft']];
  for (const [handle, branch] of branches) {
    const values = expected(branch);
    for (const id of ['messages', adaId, summariesId]) handle.appendToStateJson(id, values[id][0]);
    handle.setStateJson(resolutionsId, values[resolutionsId]);
    assert.deepEqual(readValues(handle, ids), values);
  }

  const before = branches.map(([handle]) => revisions(handle, ids));
  for (const [handle, branch] of branches) {
    const rows = handle.listStates();
    assert.ok(Array.isArray(rows));
    assert.equal(rows.find((r) => r.id === summariesId)?.id, 'default/autobio:summaries');
    assert.deepEqual(idsOf(rows).sort(), [...ids].sort());
    for (const id of ids) assert.equal(rows.filter((r) => r.id === id).length, 1, id);
    for (const { id, strategy } of registrations) assert.equal(rows.find((r) => r.id === id)?.strategy, strategy, id);
    assert.deepEqual(readValues(handle, idsOf(rows)), expected(branch));

    const fullOrder = idsOf(rows);
    const matches = (names) => names.filter((id) => id.startsWith(baseId));
    const order = matches(fullOrder);
    for (let i = 0; i < 4; i++) {
      const next = idsOf(handle.listStates());
      assert.deepEqual(matches(next), order);
      assert.deepEqual(next, fullOrder);
    }
    assert.equal(handle.getStateJson(missing), null);
    const after = handle.listStates();
    assert.deepEqual(pairsOf(after), pairsOf(registrations));
    assert.equal(after.some((r) => r.id === missing), false);
  }
  assert.deepEqual(branches.map(([handle]) => revisions(handle, ids)), before);
  for (const [handle, branch] of branches) assert.deepEqual(readValues(handle, ids), expected(branch));

  const opened = reopen();
  assert.deepEqual(idsOf(opened.listStates()).sort(), [...ids].sort());
  for (const branch of ['main', 'draft']) {
    const handle = opened.view(branch);
    assert.equal(handle.getStateJson(missing), null);
    const rows = handle.listStates();
    assert.deepEqual(pairsOf(rows), pairsOf(registrations));
    assert.equal(rows.find((r) => r.id === missing), undefined);
    assert.deepEqual(readValues(handle, idsOf(rows)), expected(branch));
  }
});

test('No record, stored null and other falsy JSON values are different states', (t) => {
  const { store } = freshStore(t);
  const emptyId = 'module/snapshot';
  const windowId = 'default/window';
  store.registerState(snapshot(emptyId));
  assert.equal(store.getStateJson(emptyId), null);
  store.registerState(log(windowId));
  assert.equal(store.getStateJson(windowId), null);
  assert.equal(store.getStateLen(windowId), null);
  for (const id of [emptyId, windowId]) assert.equal(store.stateHeadOffset(id), null);

  const scalars = { 'value/null': null, 'value/off': false, 'value/zero': 0, 'value/text': '',
    'value/list': [], 'value/document-text': '{"is":"just text"}', [counterId]: 42 };
  for (const [id, value] of Object.entries(scalars)) {
    store.registerState(snapshot(id));
    store.setStateJson(id, value);
  }
  assert.equal(store.getStateJson('value/document-text'), '{"is":"just text"}');
  for (const id of ['value/off', 'value/zero', 'value/text', 'value/list']) assert.deepEqual(store.getStateJson(id), scalars[id], id);
  assert.equal(store.getStateJson(counterId), 42);
  const reader = store.view('main');
  assert.equal(reader.getStateJson(counterId), 42);
  const replaced = store.setStateJson(counterId, 43);
  assert.deepEqual([reader.getStateJson(counterId), store.getStateJsonAt(counterId, replaced.sequence)], [43, 43]);
  assert.deepEqual([store.getStateJson('value/null'), typeof store.stateHeadOffset('value/null'),
    store.getStateJson(emptyId), store.stateHeadOffset(emptyId)], [null, 'number', null, null]);
  for (const id of Object.keys(scalars)) assert.equal(typeof store.stateHeadOffset(id), 'number', id);

  store.appendToStateJson(windowId, { id: 'first' });
  store.createBranchWithStates('without-index', 'main', { stateIds: [counterId] });
  const child = store.view('without-index');
  assert.equal(child.getStateLen(windowId), null);
  assert.equal(child.stateHeadOffset(windowId), null);
  store.redactStateItems(windowId, 0, 1);
  assert.deepEqual(store.getStateJson(windowId), []);
  assert.equal(store.getStateLen(windowId), 0);
  assert.equal(store.getStateLen('value/list'), 0);
  assert.equal(typeof store.getStateLen('value/null'), 'number');
});

test('A registered append log that was never written still counts as no items after a reopen', (t) => {
  const fixture = freshStore(t);
  fixture.store.registerState(log('default/window'));
  const reopened = fixture.reopen();
  assert.deepEqual([reopened.getStateLen('default/window'), reopened.getStateJson('default/window')], [null, null]);
});

test('A listed populated receipt can be cleared and reused without registration', (t) => {
  const { store } = freshStore(t);
  for (const id of [resolutionsId, receiptId]) store.registerState(snapshot(id));
  const levels = { 'leaf-a': 1, 'leaf-b': 2 };
  store.setState(resolutionsId, Buffer.from(JSON.stringify(levels)));
  assert.deepEqual(store.getStateJson(resolutionsId), levels);
  store.setState(resolutionsId, Buffer.from('{}'));
  assert.deepEqual(store.getStateJson(resolutionsId), {});

  const receipt = { head: { sequence: 17 }, leaves: [['a', { level: 1 }]] };
  store.setState(receiptId, Buffer.from(JSON.stringify(receipt)));
  const matched = store.listStates().find((r) => r.id === receiptId);
  assert.ok(matched);
  assert.deepEqual(store.getStateJson(matched.id), receipt);
  store.setStateJson(matched.id, null);
  assert.equal(store.getStateJson(matched.id), null);
  assert.deepEqual([store.getStateJson(receiptId), store.listStates().find((r) => r.id === receiptId)?.strategy], [null, 'snapshot']);
  const replacement = { head: { sequence: 18 }, leaves: [] };
  store.setStateJson(matched.id, replacement);
  assert.deepEqual(store.getStateJson(receiptId), replacement);
});

test('Duplicate by exact ID refuses even a different strategy on reopen', (t) => {
  const { store, reopen } = freshStore(t);
  const messagesId = 'agents/Ada/messages';
  const cadence = { deltaSnapshotEvery: 3, fullSnapshotEvery: 2 };
  store.registerState({ ...log(messagesId), ...cadence });
  store.registerState(snapshot(counterId));
  store.appendToStateJson(messagesId, { n: 1 });
  store.setStateJson(counterId, 87);
  const opened = reopen();
  for (const reg of [snapshot(messagesId), log(counterId), { ...log(messagesId), deltaSnapshotEvery: 8, fullSnapshotEvery: 5 }]) {
    assert.throws(() => opened.registerState(reg), (error) => {
      assert.ok(error instanceof Error);
      assert.equal(error.message, `State already exists: ${reg.id}`);
      assert.ok(error instanceof Error && error.message.includes('State already exists'));
      return true;
    });
    assert.deepEqual(readValues(opened, [messagesId, counterId]), { [messagesId]: [{ n: 1 }], [counterId]: 87 });
    assert.deepEqual(pairsOf(opened.listStates()), pairsOf([log(messagesId), snapshot(counterId)]));
  }
  for (let n = 2; n <= 6; n++) {
    const start = opened.getRecordIdsByType('state_update').length;
    const append = opened.appendToStateJson(messagesId, { n });
    const extra = opened.getRecordIdsByType('state_update').slice(start).filter((id) => id !== append.id);
    const kinds = extra.flatMap((id) => Object.keys(JSON.parse(opened.getStateUpdateJson(id)).operation));
    assert.deepEqual(kinds, n === 3 ? ['DeltaSnapshot'] : n === 6 ? ['Snapshot'] : [], `append ${n}`);
  }
});

test('Open does not initialize, settle or sanitize application state', (t) => {
  const { store, reopen } = freshStore(t);
  store.registerState(log(summariesId));
  store.registerState(snapshot(counterId));
  store.registerState(snapshot(receiptId));
  const summaries = [
    { id: 's-5', text: 'first', chunkIds: ['c-7'] },
    { id: 's-5', text: 'unsettled duplicate', chunkIds: ['c-8'] },
    { id: 's-6', text: '', chunkIds: [] },
  ];
  for (const row of summaries) store.appendToStateJson(summariesId, row);
  const invalidCounter = { version: 'foreign', counter: 'not a number' };
  store.setStateJson(counterId, invalidCounter);
  store.setState(receiptId, Buffer.from('[unterminated'));

  const opened = reopen();
  assert.deepEqual(opened.getStateJson(summariesId), summaries);
  assert.deepEqual(opened.getStateJson(counterId), invalidCounter);
  assert.throws(() => opened.getStateJson(receiptId));
  assert.deepEqual(opened.getState(receiptId), Buffer.from('[unterminated'));
  assert.deepEqual(idsOf(opened.listStates()).sort(), [summariesId, counterId, receiptId].sort());
});

test('Planning edits to materialized JS data does not write on mutation or close', (t) => {
  const { store, reopen } = freshStore(t);
  const ids = [resolutionsId, summariesId];
  const original = { [resolutionsId]: { left: 1, right: 2 }, [summariesId]: [{ id: 's-1', text: 'old' }] };
  store.registerState(snapshot(resolutionsId));
  store.registerState(log(summariesId));
  store.setState(resolutionsId, Buffer.from(JSON.stringify(original[resolutionsId])));
  store.appendToState(summariesId, Buffer.from(JSON.stringify(original[summariesId][0])));
  const before = revisions(store, ids);
  const plan = readValues(store, ids);
  plan[resolutionsId].left = 3;
  delete plan[resolutionsId].right;
  plan[summariesId][0] = { id: 's-1', text: 'revised' };
  plan[summariesId].push({ id: 's-2', text: 'new' });
  assert.deepEqual(plan, { [resolutionsId]: { left: 3 }, [summariesId]: [{ id: 's-1', text: 'revised' }, { id: 's-2', text: 'new' }] });
  assert.deepEqual(readValues(store, ids), original);
  assert.deepEqual(revisions(store, ids), before);
  const opened = reopen();
  assert.deepEqual(readValues(opened, ids), original);
  assert.deepEqual(revisions(opened, ids), before);
});

test('Byte and object setters round-trip the same large receipt', (t) => {
  const { store } = freshStore(t);
  const fallbackId = 'other/kvunified:presentation-receipt';
  store.registerState(snapshot(receiptId));
  store.registerState(snapshot(fallbackId));
  const receipt = {
    head: { sequence: 97, submissionId: 'presentation-97', changes: [] },
    leaves: Array.from({ length: 75_000 }, (_, n) => {
      const id = `msg-${n}`;
      return [id, { repHash: `raw:${id}`, level: n % 3, lastChangedSeq: n % 97 }];
    }),
    cache: null, settledSubmissionIds: ['presentation-97'], wireReceipt: null,
  };
  const json = JSON.stringify(receipt);
  const expected = JSON.parse(json);
  const bytes = Buffer.from(json);
  let propertyReads = 0;
  Object.defineProperty(bytes, 'traversalProbe', { enumerable: true, get() { propertyReads++; return null; } });
  store.setState(receiptId, bytes);
  assert.equal(propertyReads, 0);
  const byteValue = store.getStateJson(receiptId);
  assert.deepEqual(byteValue, expected);
  store.setStateJson(fallbackId, receipt);
  const objectValue = store.getStateJson(fallbackId);
  assert.deepEqual(objectValue, expected);
  assert.deepEqual(objectValue, byteValue);
});

test('The head after a lone edit or removal is the sequence of the record that it wrote', (t) => {
  const { store } = freshStore(t);
  store.setAutoSnapshot(false);
  store.registerState(log('messages'));
  for (const text of ['a', 'b', 'c']) store.appendToStateJson('messages', { text });
  const written = (record) => [record.sequence, store.getRecord(record.id).sequence, store.currentSequence(), store.currentSequence()];
  const beforeEdit = store.currentSequence();
  const edit = store.editStateItem('messages', 1, Buffer.from(JSON.stringify({ text: 'B' })));
  assert.deepEqual(written(edit), Array(4).fill(beforeEdit + 1));
  const beforeRemoval = store.currentSequence();
  const removal = store.redactStateItems('messages', 0, 1);
  assert.deepEqual(written(removal), Array(4).fill(beforeRemoval + 1));
  assert.deepEqual(store.getStateJson('messages'), [{ text: 'B' }, { text: 'c' }]);
});
