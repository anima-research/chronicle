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
  // pins listStates.1: no arguments or await, and no registrations is [].
  assert.deepEqual(store.listStates(), []);

  store.createBranch('draft');
  const draft = store.view('draft');
  const adaId = 'agents/Ada/messages';
  const treeId = 'workspace/intent:tree'; // Registered, never written on either branch.
  const registrations = [snapshot(baseId), log('messages'), log(adaId), log(summariesId),
    snapshot(resolutionsId), { id: treeId, strategy: 'tree' }];
  const ids = idsOf(registrations);
  const missing = 'other/autobio:summaries';

  store.registerState(registrations[0]);
  // pins registerState.10: discover a slot and its strategy before either branch writes it.
  assert.deepEqual(pairsOf(draft.listStates()), [[baseId, 'snapshot']]);
  store.setStateJson(baseId, { profile: 'main' });
  draft.setStateJson(baseId, { profile: 'draft' });
  for (const reg of registrations.slice(1)) draft.registerState(reg);
  // pins registerState.10: including unwritten log and tree registrations made through a view.
  assert.deepEqual(pairsOf(store.listStates()), pairsOf(registrations));

  for (const branch of ['draft', 'main']) {
    store.switchBranch(branch);
    // pins registerState.9: a switch sees all view registrations, without overwriting branch data or seeding the new slots.
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
    // pins registerState.2: names differing by namespace or a colon suffix resolve independently.
    assert.deepEqual(readValues(handle, ids), values);
  }

  // Capture *both* branches before listing either: a listing could write to the other branch.
  const before = branches.map(([handle]) => revisions(handle, ids));
  for (const [handle, branch] of branches) {
    const rows = handle.listStates();
    // pins listStates.1: populated result is a synchronous JS Array too.
    assert.ok(Array.isArray(rows));
    // pins listStates.2: not merely a leaf name or the namespace with its suffix stripped.
    assert.equal(rows.find((r) => r.id === summariesId)?.id, 'default/autobio:summaries');
    // pins listStates.3: the complete cross-namespace inventory, including the unwritten tree.
    assert.deepEqual(idsOf(rows).sort(), [...ids].sort());
    // pins listStates.7: count occurrences in the returned rows, not just unique names.
    for (const id of ids) assert.equal(rows.filter((r) => r.id === id).length, 1, id);
    // pins listStates.9: each exact name forwards its own registered strategy string.
    for (const { id, strategy } of registrations) assert.equal(rows.find((r) => r.id === id)?.strategy, strategy, id);
    // pins listStates.5: read through the returned IDs, on the branch that supplied them.
    assert.deepEqual(readValues(handle, idsOf(rows)), expected(branch));

    const fullOrder = idsOf(rows);
    const matches = (names) => names.filter((id) => id.startsWith(baseId));
    const order = matches(fullOrder);
    for (let i = 0; i < 4; i++) {
      const next = idsOf(handle.listStates());
      // pins listStates.8: successive searches partition the same order; no particular sort is required.
      assert.deepEqual(matches(next), order);
      // pins listStates.8: unfiltered store.states also observes that order.
      assert.deepEqual(next, fullOrder);
    }
    const after = handle.listStates();
    // pins listStates.6: inspection left registrations intact; in particular no missing slot was created.
    assert.deepEqual(pairsOf(after), pairsOf(registrations));
    assert.equal(after.some((r) => r.id === missing), false);
  }
  // pins listStates.6: no rewrite to either branch, not even one that reproduced the same value.
  assert.deepEqual(branches.map(([handle]) => revisions(handle, ids)), before);
  for (const [handle, branch] of branches) assert.deepEqual(readValues(handle, ids), expected(branch));

  const opened = reopen();
  // pins open.8: immediately rediscover the entire inventory, not just slots with chain records.
  assert.deepEqual(idsOf(opened.listStates()).sort(), [...ids].sort());
  for (const branch of ['main', 'draft']) {
    const handle = opened.view(branch);
    const rows = handle.listStates();
    // pins listStates.3: reopened membership and strategies (irrespective of order) and no unregistered match.
    assert.deepEqual(pairsOf(rows), pairsOf(registrations));
    assert.equal(rows.find((r) => r.id === missing), undefined);
    // pins listStates.5: the discovered IDs still refer to these branch-local persisted payloads.
    assert.deepEqual(readValues(handle, idsOf(rows)), expected(branch));
  }
});

test('No record, stored null and other falsy JSON values are different states', (t) => {
  const { store } = freshStore(t);
  const emptyId = 'module/snapshot';
  const windowId = 'default/window';
  store.registerState(snapshot(emptyId));
  // pins registerState.8: registration supplies no initial module snapshot.
  assert.equal(store.getStateJson(emptyId), null);
  store.registerState(log(windowId));
  // pins getStateJson.12: a registered but never-written state yields exactly null, including a log.
  assert.equal(store.getStateJson(windowId), null);
  // pins getStateLen.3: the caller's nullish-default count on a never-written log is zero.
  assert.equal(store.getStateLen(windowId) ?? 0, 0);
  // pins stateHeadOffset.2: no chain record immediately after snapshot or log registration.
  for (const id of [emptyId, windowId]) assert.equal(store.stateHeadOffset(id), null);

  const scalars = { 'value/null': null, 'value/off': false, 'value/zero': 0, 'value/text': '',
    'value/list': [], 'value/document-text': '{"is":"just text"}', [counterId]: 42 };
  for (const [id, value] of Object.entries(scalars)) {
    store.registerState(snapshot(id));
    store.setStateJson(id, value);
  }
  // pins getStateJson.3: unexpected scalar JSON is decoded once; a string containing JSON is still that string.
  assert.equal(store.getStateJson('value/document-text'), '{"is":"just text"}');
  // pins getStateJson.12: false, zero, empty text and an empty array are not absent.
  for (const id of ['value/off', 'value/zero', 'value/text', 'value/list']) assert.deepEqual(store.getStateJson(id), scalars[id], id);
  // pins getStateJson.13: the summary counter is a number, not text or a wrapper.
  assert.equal(store.getStateJson(counterId), 42);
  // pins getStateJson.20: stored JSON null reads as null but has a revision, unlike a never-written slot.
  assert.deepEqual([store.getStateJson('value/null'), typeof store.stateHeadOffset('value/null'),
    store.getStateJson(emptyId), store.stateHeadOffset(emptyId)], [null, 'number', null, null]);
  // pins stateHeadOffset.2: numeric head even for null or a value that isn't a usable list.
  for (const id of Object.keys(scalars)) assert.equal(typeof store.stateHeadOffset(id), 'number', id);

  store.appendToStateJson(windowId, { id: 'first' });
  store.createBranchWithStates('without-index', 'main', { stateIds: [counterId] });
  const child = store.view('without-index');
  // pins getStateLen.10: omission means strict null on the receiving branch; inspection creates no head.
  assert.equal(child.getStateLen(windowId), null);
  assert.equal(child.stateHeadOffset(windowId), null);
  store.redactStateItems(windowId, 0, 1);
  assert.deepEqual(store.getStateJson(windowId), []);
  // pins getStateLen.10: a written empty list has a numeric zero length, not absence.
  assert.equal(store.getStateLen(windowId), 0);
  assert.equal(store.getStateLen('value/list'), 0);
  // pins getStateLen.10: a record of a foreign (non-list) format must not skip the caller's value read.
  assert.equal(typeof store.getStateLen('value/null'), 'number');
});

test('A listed populated receipt can be cleared and reused without registration', (t) => {
  const { store } = freshStore(t);
  for (const id of [resolutionsId, receiptId]) store.registerState(snapshot(id));
  const levels = { 'leaf-a': 1, 'leaf-b': 2 };
  store.setState(resolutionsId, Buffer.from(JSON.stringify(levels)));
  assert.deepEqual(store.getStateJson(resolutionsId), levels);
  store.setState(resolutionsId, Buffer.from('{}'));
  // pins setState.2: byte-setting an empty map clears every old level rather than merging keys.
  assert.deepEqual(store.getStateJson(resolutionsId), {});

  const receipt = { head: { sequence: 17 }, leaves: [['a', { level: 1 }]] };
  store.setState(receiptId, Buffer.from(JSON.stringify(receipt)));
  const matched = store.listStates().find((r) => r.id === receiptId);
  assert.ok(matched);
  assert.deepEqual(store.getStateJson(matched.id), receipt);
  store.setStateJson(matched.id, null);
  // pins listStates.10: the listed, populated receipt ID is a valid target for the later null clear.
  assert.equal(store.getStateJson(matched.id), null);
  // pins setStateJson.10: JS null replaces the byte-written receipt without dropping its registration.
  assert.deepEqual([store.getStateJson(receiptId), store.listStates().find((r) => r.id === receiptId)?.strategy], [null, 'snapshot']);
  const replacement = { head: { sequence: 18 }, leaves: [] };
  store.setStateJson(matched.id, replacement);
  // pins setStateJson.10: the same slot can be reused without registering again.
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
    // pins registerState.11: a synchronous Error with the *entire* ID-specific message, for more than one ID.
    assert.throws(() => opened.registerState(reg), (error) => {
      assert.ok(error instanceof Error);
      assert.equal(error.message, `State already exists: ${reg.id}`);
      // pins registerState.15: the framework's separate Error and case-sensitive substring predicate.
      assert.ok(error instanceof Error && error.message.includes('State already exists'));
      return true;
    });
    // pins registerState.5: the old data is readable after each duplicate attempt on a reopened, populated slot.
    assert.deepEqual(readValues(opened, [messagesId, counterId]), { [messagesId]: [{ n: 1 }], [counterId]: 87 });
    // pins registerState.6: neither a changed kind nor a changed interval replaces the original registration.
    assert.deepEqual(pairsOf(opened.listStates()), pairsOf([log(messagesId), snapshot(counterId)]));
  }
  // Watch for extra snapshot records at the old delta and full boundaries, not just a preserved kind string.
  for (let n = 2; n <= 6; n++) {
    const start = opened.getRecordIdsByType('state_update').length;
    const append = opened.appendToStateJson(messagesId, { n });
    const extra = opened.getRecordIdsByType('state_update').slice(start).filter((id) => id !== append.id);
    const kinds = extra.flatMap((id) => Object.keys(JSON.parse(opened.getStateUpdateJson(id)).operation));
    // pins registerState.6: original cadence still makes the third append a delta boundary and the sixth a full boundary.
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
  // pins open.3: duplicates and unsettled entries survive in their original order.
  assert.deepEqual(opened.getStateJson(summariesId), summaries);
  // pins open.3: validation of parseable but foreign-schema data belongs to the caller.
  assert.deepEqual(opened.getStateJson(counterId), invalidCounter);
  // pins open.3: malformed bytes aren't silently converted to null or an empty receipt.
  assert.throws(() => opened.getStateJson(receiptId));
  // pins open.3: the raw handle still holds those very bytes for the caller to inspect or repair.
  assert.deepEqual(opened.getState(receiptId), Buffer.from('[unterminated'));
  // pins open.3: opening added no missing application slot such as the resolutions map.
  assert.deepEqual(idsOf(opened.listStates()).sort(), [summariesId, counterId, receiptId].sort());
});

test('Planning edits to materialized JS data does not write on mutation or close', (t) => {
  const { store, reopen } = freshStore(t);
  const ids = [resolutionsId, summariesId];
  const original = { [resolutionsId]: { left: 1, right: 2 }, [summariesId]: [{ id: 's-1', text: 'old' }] };
  store.registerState(snapshot(resolutionsId));
  store.registerState(log(summariesId));
  // Keep the expected JS values independent even if a future binding caches references.
  store.setState(resolutionsId, Buffer.from(JSON.stringify(original[resolutionsId])));
  store.appendToState(summariesId, Buffer.from(JSON.stringify(original[summariesId][0])));
  const before = revisions(store, ids);
  const plan = readValues(store, ids);
  plan[resolutionsId].left = 3;
  delete plan[resolutionsId].right;
  plan[summariesId][0] = { id: 's-1', text: 'revised' };
  plan[summariesId].push({ id: 's-2', text: 'new' });
  // pins getStateJson.7: assign/delete/replace/extend all took effect in JS.
  assert.deepEqual(plan, { [resolutionsId]: { left: 3 }, [summariesId]: [{ id: 's-1', text: 'revised' }, { id: 's-2', text: 'new' }] });
  // pins getStateJson.7: without a setter, fresh reads and revisions still reflect the originals.
  assert.deepEqual(readValues(store, ids), original);
  assert.deepEqual(revisions(store, ids), before);
  const opened = reopen();
  // pins close.5: closing did not commit the dry run, not even a same-value rewrite.
  assert.deepEqual(readValues(opened, ids), original);
  assert.deepEqual(revisions(opened, ids), before);
});

test('Byte and object setters round-trip the same large receipt', (t) => {
  const { store } = freshStore(t);
  const fallbackId = 'other/kvunified:presentation-receipt';
  store.registerState(snapshot(receiptId));
  store.registerState(snapshot(fallbackId));
  // The earlier persisted receipt shape held one [id, presented leaf] pair per message.
  const receipt = {
    head: { sequence: 97, submissionId: 'presentation-97', changes: [] },
    leaves: Array.from({ length: 75_000 }, (_, n) => {
      const id = `msg-${n}`;
      return [id, { repHash: `raw:${id}`, level: n % 3, lastChangedSeq: n % 97 }];
    }),
    cache: null, settledSubmissionIds: ['presentation-97'], wireReceipt: null,
  };
  const json = JSON.stringify(receipt);
  const expected = JSON.parse(json); // independent of either setter's argument
  const bytes = Buffer.from(json);
  let propertyReads = 0;
  Object.defineProperty(bytes, 'traversalProbe', { enumerable: true, get() { propertyReads++; return null; } });
  store.setState(receiptId, bytes); // no JS leaf graph crosses the binding here
  // pins setState.9: the Buffer suffices for the whole receipt without reading enumerable JS metadata off it.
  assert.equal(propertyReads, 0);
  const byteValue = store.getStateJson(receiptId);
  // pins setState.9: the large pre-serialized input round-trips every leaf, not just its head and size.
  assert.deepEqual(byteValue, expected);
  store.setStateJson(fallbackId, receipt);
  const objectValue = store.getStateJson(fallbackId);
  // pins setStateJson.18: the JS object fallback yields the same full parsed receipt, trailing leaves included.
  assert.deepEqual(objectValue, expected);
  assert.deepEqual(objectValue, byteValue);
});
