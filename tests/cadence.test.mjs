import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readdirSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { JsStore } from '../index.js';

function freshStore(t) {
  const path = mkdtempSync(join(tmpdir(), 'chronicle-cadence-'));
  let store;
  t.after(() => {
    try {
      if (store && !store.isClosed()) store.close();
    } finally {
      rmSync(path, { recursive: true, force: true });
    }
  });
  store = JsStore.openOrCreate({ path });
  return { store, path, reopen() { store = JsStore.open({ path }); return store; } };
}

const appendLog = (id, deltaSnapshotEvery, fullSnapshotEvery) =>
  ({ id, strategy: 'append_log', deltaSnapshotEvery, fullSnapshotEvery });

const mark = (store) => store.getRecordIdsByType('state_update').length;
function operations(store, state, since = 0) {
  const found = [];
  for (const id of store.getRecordIdsByType('state_update').slice(since)) {
    const { state_id, operation } = JSON.parse(store.getStateUpdateJson(id));
    if (state_id === state) found.push(operation);
  }
  return found;
}
function operationKinds(store, state, since = 0) {
  return operations(store, state, since).map((op) => Object.keys(op)[0]);
}

function addMessages(store, id, expected, count) {
  for (let i = 0; i < count; i++) {
    const message = { participant: 'User', content: [{ type: 'text', text: `${id} row ${expected.length}` }] };
    const record = store.appendToStateJsonWithIdentity(id, message, 'id', 'sequence');
    expected.push({ ...message, id: record.id, sequence: record.sequence });
  }
}

test('Toggling automatic snapshots acts immediately without losing disabled-period messages', (t) => {
  for (const exit of ['append', 'compact', 'close']) {
    const { store, reopen } = freshStore(t);
    const id = exit === 'close' ? 'bench/messages' : 'messages';
    store.registerState(appendLog(id, 1, 100));
    const messages = [];
    const start = mark(store);
    store.setAutoSnapshot(false);
    addMessages(store, id, messages, 6);
    assert.deepEqual(operationKinds(store, id, start), Array(6).fill('Append'), exit);

    const endOfSetup = mark(store);
    store.setAutoSnapshot(true);
    if (exit === 'append') {
      addMessages(store, id, messages, 1);
      assert.deepEqual(operationKinds(store, id, endOfSetup).slice(-2), ['Append', 'DeltaSnapshot']);
      store.close();
      assert.deepEqual(reopen().getStateJson(id), messages);
    } else if (exit === 'compact') {
      const beforeCompaction = mark(store);
      const headBefore = store.stateHeadOffset(id);
      assert.ok(store.listStates().find((state) => state.id === id)?.opsSinceSnapshot > 0);
      const compacted = store.compactState(id);
      const headAfter = store.stateHeadOffset(id);
      const bytes = operations(store, id, beforeCompaction).at(-1)?.Snapshot;
      assert.deepEqual(bytes && JSON.parse(Buffer.from(bytes).toString('utf8')), messages);
      assert.deepEqual({
        written: store.getRecordIdsByType('state_update').slice(beforeCompaction),
        moved: headAfter !== headBefore,
        opsSinceSnapshot: store.listStates().find((state) => state.id === id)?.opsSinceSnapshot,
      }, { written: [compacted?.id], moved: true, opsSinceSnapshot: 0 });
      store.createBranch('child', 'main');
      assert.deepEqual(store.view('child').getStateJson(id), messages);
      const next = store.appendToStateJson(id, { participant: 'User', content: [] });
      assert.equal(JSON.parse(store.getStateUpdateJson(next.id)).prev_update_offset, headAfter);
    } else {
      store.close();
      assert.deepEqual(reopen().getStateJson(id), messages);
    }
  }
});

test('Reapplying a retune on reopen changes scheduling without replacing the named log', (t) => {
  const { store, reopen } = freshStore(t);
  const shared = 'messages', isolated = 'agents/tern/messages';
  const sharedRows = [], isolatedRows = [];
  store.registerState(appendLog(shared, 3, 1));
  store.registerState(appendLog(isolated, 4, 1));
  addMessages(store, shared, sharedRows, 1);
  addMessages(store, isolated, isolatedRows, 4);

  const reg = appendLog(isolated, 2, 10);
  const upsert = store.updateStateStrategy;
  const before = mark(store);
  assert.doesNotThrow(() => upsert.call(store, reg));
  addMessages(store, isolated, isolatedRows, 2);
  assert.deepEqual(operationKinds(store, isolated, before), ['Append', 'Append', 'DeltaSnapshot']);
  assert.deepEqual({
    strategy: store.listStates().find((s) => s.id === isolated)?.strategy,
    rows: store.getStateJson(isolated),
  }, { strategy: 'append_log', rows: isolatedRows });

  addMessages(store, shared, sharedRows, 2);
  assert.deepEqual(operationKinds(store, shared, before), ['Append', 'Append', 'Snapshot']);
  store.close();
  const next = reopen();
  const opened = mark(next);
  addMessages(next, isolated, isolatedRows, 2);
  assert.deepEqual(operationKinds(next, isolated, opened), ['Append', 'Append', 'DeltaSnapshot']);

  const beforeRepeat = mark(next);
  next.updateStateStrategy(reg);
  addMessages(next, isolated, isolatedRows, 2);
  assert.deepEqual(operationKinds(next, isolated, beforeRepeat), ['Append', 'Append', 'DeltaSnapshot']);
  addMessages(next, shared, sharedRows, 3);
  assert.deepEqual(operationKinds(next, shared, beforeRepeat), ['Append', 'Append', 'Append', 'Snapshot']);
  const slots = next.listStates().filter((s) => s.id === shared || s.id === isolated);
  assert.deepEqual({
    ids: slots.map((s) => [s.id, s.strategy]).sort(),
    shared: next.getStateJson(shared),
    isolated: next.getStateJson(isolated),
  }, {
    ids: [[isolated, 'append_log'], [shared, 'append_log']].sort(),
    shared: sharedRows,
    isolated: isolatedRows,
  });
});

function appendSchedule(kinds) {
  let writes = 0;
  const boundaries = [];
  for (const kind of kinds) {
    if (kind === 'Append') writes++;
    else boundaries.push([writes, kind]);
  }
  return { writes, boundaries };
}

test('The first full cadence is measured in delta intervals', (t) => {
  const { store } = freshStore(t);
  const journal = 'mcpl/coalescing-journal';
  store.registerState(appendLog(journal, Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER));
  for (let i = 0; i < 120; i++) store.appendToStateJson(journal, { seq: i });
  assert.deepEqual(appendSchedule(operationKinds(store, journal)), { writes: 120, boundaries: [] });

  const longFull = 'short-delta-control';
  store.registerState(appendLog(longFull, 2, Number.MAX_SAFE_INTEGER));
  for (let i = 0; i < 24; i++) store.appendToStateJson(longFull, i);
  assert.deepEqual(appendSchedule(operationKinds(store, longFull)), {
    writes: 24, boundaries: Array.from({ length: 12 }, (_, i) => [2 * (i + 1), 'DeltaSnapshot']),
  });

  const id = 'messages';
  store.registerState(appendLog(id, 5, 2));
  store.updateStateStrategy(appendLog(id, 50, 100));
  const start = mark(store);
  for (let i = 0; i < 5000; i++) store.appendToStateJsonWithIdentity(id, { n: i }, 'id', 'sequence');
  const deltas = Array.from({ length: 99 }, (_, i) => [50 * (i + 1), 'DeltaSnapshot']);
  assert.deepEqual(appendSchedule(operationKinds(store, id, start)), {
    writes: 5000, boundaries: [...deltas, [5000, 'Snapshot']],
  });
});

test('After the first full snapshot, a growing log spaces the next by its length at the last', (t) => {
  const { store } = freshStore(t);
  const id = 'messages';
  store.registerState(appendLog(id, 3, 7));
  store.updateStateStrategy(appendLog(id, 5, 2));
  for (let i = 0; i < 80; i++) store.appendToStateJsonWithIdentity(id, { n: i }, 'id', 'sequence');
  const fulls = [10, 20, 40, 80];
  assert.deepEqual(appendSchedule(operationKinds(store, id)), {
    writes: 80,
    boundaries: Array.from({ length: 16 }, (_, i) => 5 * (i + 1))
      .map((at) => [at, fulls.includes(at) ? 'Snapshot' : 'DeltaSnapshot']),
  });
});

test('Explicit tree cadence fields drive keyed snapshot boundaries', (t) => {
  const { store } = freshStore(t);
  const id = 'workspace';
  store.registerState({ id, strategy: 'tree', deltaSnapshotEvery: 3, fullSnapshotEvery: 2 });
  const files = Array.from({ length: 6 }, (_, i) => ({
    path: `src/file-${i}`, entry: { blobHash: `hash-${i}`, size: i + 10, mode: 0o644 },
  }));
  for (const { path, entry } of files) store.treeSet(id, path, entry);
  assert.deepEqual(operationKinds(store, id), [
    'TreeSet', 'TreeSet', 'TreeSet', 'TreeDeltaSnapshot', 'TreeSet', 'TreeSet', 'TreeSet', 'Snapshot',
  ]);
  assert.deepEqual(files.map(({ path }) => store.treeGet(id, path)), files.map(({ entry }) => entry));
  assert.deepEqual(store.treeList(id).sort((a, b) => a.path.localeCompare(b.path)),
    files.map(({ path, entry }) => ({ path, ...entry })));
});

test('A fresh registration takes its cadence fields with no retune', (t) => {
  const { store } = freshStore(t);
  const messages = 'messages';
  const context = 'context-log';
  store.registerState(appendLog(messages, 50, 100));
  store.registerState(appendLog(context, 50, 10));
  for (let i = 0; i < 5000; i++) store.appendToStateJsonWithIdentity(messages, { n: i }, 'id', 'sequence');
  for (let i = 0; i < 500; i++) store.appendToStateJson(context, { n: i });
  const deltas = (count) => Array.from({ length: count }, (_, i) => [50 * (i + 1), 'DeltaSnapshot']);
  assert.deepEqual(appendSchedule(operationKinds(store, messages)), { writes: 5000, boundaries: [...deltas(99), [5000, 'Snapshot']] });
  assert.deepEqual(appendSchedule(operationKinds(store, context)), { writes: 500, boundaries: [...deltas(9), [500, 'Snapshot']] });
});

test('An ordinary append writes a record whose size does not grow with the log', (t) => {
  const { store, path } = freshStore(t);
  const storeBytes = () => readdirSync(path, { recursive: true }).reduce((sum, name) => {
    const stat = statSync(join(path, name));
    return stat.isFile() ? sum + stat.size : sum;
  }, 0);
  const logs = ['log-a', 'log-b'];
  for (const id of logs) store.registerState(appendLog(id, Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER));
  for (let i = 0; i < 4; i++) store.appendToStateJson(logs[0], { n: i });
  for (let i = 0; i < 4096; i++) store.appendToStateJson(logs[1], { n: i });
  const item = { text: 'the same small item' };
  const [short, long] = logs.map((id) => {
    const before = storeBytes();
    const { id: record } = store.appendToStateJson(id, item);
    return { payload: store.getRecord(record).payload.length, stored: storeBytes() - before };
  });
  assert.deepEqual(logs.map((id) => operationKinds(store, id).at(-1)), ['Append', 'Append']);
  assert.ok(short.stored > 0 && Math.abs(long.stored - short.stored) <= 8 && Math.abs(long.payload - short.payload) <= 8,
    `an append to a log of 4 items stored ${short.stored} bytes with a ${short.payload}-byte record, and to a log of 4,096 items ${long.stored} with ${long.payload}`);
});
