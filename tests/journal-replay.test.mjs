import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, renameSync, rmSync, statSync, truncateSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { JsStore } from '../index.js';

const entry = 'journal/workspace';
const checkpoint = `${entry}/checkpoint`;
const identity = 'context-manager/store-identity';
const layout = 'context-manager/accepted-layout';

function fixture(t) {
  const dir = mkdtempSync(join(tmpdir(), 'chronicle-replay-'));
  const path = join(dir, 'store');
  const f = {
    path,
    reopen(whileClosed) {
      this.store.close();
      whileClosed?.();
      return this.store = JsStore.open({ path });
    },
  };
  t.after(() => {
    try { f.store?.close(); } finally { rmSync(dir, { recursive: true, force: true }); }
  });
  f.store = JsStore.openOrCreate({ path });
  return f;
}

const positiveDecimal = (id) => typeof id === 'string' && /^\d+$/.test(id) && BigInt(id) > 0n;
const increasing = (ns) => ns.every((n, i) => i === 0 || n > ns[i - 1]);

function readEntry(store, id) {
  const record = store.getRecord(id);
  if (record === null) return null;
  return { id: record.id, payload: JSON.parse(Buffer.from(record.payload).toString('utf8')) };
}

test('Type IDs and record sequences use different coordinates and survive reopen', (t) => {
  const f = fixture(t);
  let store = f.store;
  store.registerState({ id: 'messages', strategy: 'append_log' });
  const head = store.currentSequence();
  store.createBranch('left');
  store.createBranch('right');

  const writes = [];
  function write(type, payload) {
    const expectedSequence = store.currentSequence() + 1;
    const ref = store.appendJson(type, payload);
    assert.equal(ref.sequence, expectedSequence);
    assert.ok(positiveDecimal(ref.id) && BigInt(ref.id) > BigInt(writes.at(-1)?.id ?? '0'));
    const ordinal = writes.filter((w) => w.type === type).length;
    writes.push({ type, ordinal, payload, id: ref.id, sequence: ref.sequence });
    return ref;
  }

  store.switchBranch('left');
  const first = write(entry, { id: 'external-1', branch: 'right', text: 'café' });
  store.switchBranch('right');
  write(checkpoint, { through: first.id, snapshot: null });
  store.switchBranch('left');
  const before = store.appendToStateJson('messages', { text: 'one' });
  const note = write(entry, { id: 'application-id', branch: 'right', text: 'note between messages' });
  const after = store.appendToStateJson('messages', { text: 'two' });
  assert.deepEqual([first.sequence, before.sequence, note.sequence, after.sequence],
    [head + 1, head + 2, head + 3, head + 4]);
  store.switchBranch('right');
  const badCheckpoint = write(checkpoint, null);
  const badIdentity = write(identity, { v: 1 });
  store.switchBranch('left');
  write(layout, { v: 3, ns: 'conversation', compileId: 'good', branch: store.currentBranch() });
  store.switchBranch('right');
  write(identity, { v: 1, storeId: 'actual-store' });
  const badLayout = write(layout, { compileId: false });
  write(entry, { id: 'external-tail', branch: 'left' });

  const bytes = Buffer.from(' {"answer": 1.0, "text": "café"}\n');
  const raw = store.append('journal/raw-json', bytes);
  const types = [entry, checkpoint, identity, layout];
  const saved = types.map((type) => writes.filter((w) => w.type === type).map((w) => w.id));
  function replay() {
    assert.deepEqual(Buffer.from(store.getRecord(raw.id).payload), bytes);
    const lists = new Map(types.map((type) => [type, store.getRecordIdsByType(type)]));
    for (const type of types) {
      const ids = lists.get(type);
      const expected = writes.filter((w) => w.type === type);
      assert.ok(Array.isArray(ids) && ids.length === expected.length && ids.every((id) => typeof id === 'string'));
      assert.deepEqual(ids, expected.map((w) => w.id));
      assert.ok(ids.every(positiveDecimal));
      assert.deepEqual(ids.map((id) => readEntry(store, id)), expected.map(({ id, payload }) => ({ id, payload })));
      for (const w of expected) {
        assert.deepEqual([ids.includes(w.id), readEntry(store, w.id)], [true, { id: w.id, payload: w.payload }]);
        const record = store.getRecord(w.id);
        assert.equal(record.sequence, w.sequence);
      }
    }
    assert.deepEqual([lists.get(checkpoint).includes(badCheckpoint.id), lists.get(identity).includes(badIdentity.id),
      lists.get(layout).includes(badLayout.id)], [true, true, true]);
    assert.ok(increasing(writes.map(({ type, ordinal }) => BigInt(lists.get(type)[ordinal]))));
    return [...lists.values()];
  }
  const initialLists = replay();
  store = f.reopen();
  assert.deepEqual([initialLists, replay()], [saved, saved]);
});

test('A typed journal outlives historical cuts and deletion of a contributing branch', (t) => {
  const f = fixture(t);
  let store = f.store;
  store.registerState({ id: 'settings', strategy: 'snapshot' });
  store.registerState({ id: 'messages', strategy: 'append_log' });
  store.registerState({ id: 'files', strategy: 'tree' });
  const fields = (e) => e && { blobHash: e.blobHash, size: e.size, mode: e.mode };
  const state = () => [store.getStateJson('settings'), store.getStateJson('messages'), fields(store.treeGet('files', 'note'))];
  const oldFile = { blobHash: 'old', size: 3, mode: 0o644 };
  const newFile = { ...oldFile, blobHash: 'new' };
  store.setStateJson('settings', 'old');
  store.appendToStateJson('messages', 'before');
  store.treeSet('files', 'note', oldFile);
  const cut = store.currentSequence();
  const writer = store.createBranch('writer');
  store.createBranch('survivor');

  const expected = [];
  function append(compileId, branch) {
    const payload = { v: 3, ns: 'test', compileId, branch: { id: branch.id, name: branch.name, created: branch.created },
      receipt: { acceptedAt: '2026-05-01T00:00:00Z' } };
    const { id } = store.appendJson(layout, payload);
    const saved = { id, payload };
    expected.push(saved);
    return saved;
  }
  store.switchBranch('writer');
  append('on-writer', writer);
  store.switchBranch('survivor');
  store.setStateJson('settings', 'new');
  store.appendToStateJson('messages', 'later');
  store.treeSet('files', 'note', newFile);
  assert.deepEqual(state(), ['new', ['before', 'later'], newFile], 'all three states advanced past the cut');
  const foreign = append('compiled-on-writer', writer);
  store.switchBranch('writer');
  append('also-on-writer', writer);
  store.switchBranch('survivor');
  append('on-survivor', store.currentBranch());
  const ids = expected.map((r) => r.id);
  function observe() {
    const listed = store.getRecordIdsByType(layout);
    return { listed, entries: listed.map((id) => readEntry(store, id)) };
  }

  store.switchBranch('main');
  const onMain = observe();
  assert.deepEqual(onMain, { listed: ids, entries: expected });
  store.createBranchAt('past', 'survivor', cut);
  store.switchBranch('past');
  const onPast = observe();
  assert.deepEqual([state(), onPast.listed], [['old', ['before'], oldFile], ids]);
  assert.deepEqual(ids.map((id) => readEntry(store, id)), expected);
  store.deleteBranch('writer');
  assert.ok(!store.listBranches().some(({ name }) => name === 'writer'), 'the contributing branch was deleted');
  const afterDelete = observe();
  assert.deepEqual(ids.map((id) => readEntry(store, id)), expected);
  store = f.reopen();
  const afterOpen = observe();
  assert.deepEqual([afterDelete, afterOpen], Array(2).fill({ listed: ids, entries: expected }));
  for (const observation of [onMain, onPast, afterDelete, afterOpen]) {
    assert.deepEqual(observation.listed, ids);
    assert.deepEqual(observation.entries.find((e) => e?.id === foreign.id), foreign);
  }
});

test('Journal lists are growable snapshots with append-only visible suffixes', {
  skip: process.platform === 'win32' && 'renames the directory of an open store, which Windows refuses',
}, (t) => {
  const { store, path } = fixture(t);
  const position = () => [store.stats().recordCount, store.currentSequence()];
  const beforeReads = position();
  for (let i = 0; i < 2; ++i) {
    assert.deepEqual([store.getRecordIdsByType(identity), position()], [[], beforeReads]);
    assert.deepEqual([store.getRecordIdsByType(checkpoint), position()], [[], beforeReads]);
  }
  const cp = store.appendJson(checkpoint, { through: '0', snapshot: null });
  assert.deepEqual(store.getRecordIdsByType(checkpoint), [cp.id]);

  const list = () => store.getRecordIdsByType(layout);
  function appendLayout(compileId) {
    const prefix = [...list()];
    const { id } = store.appendJson(layout, { compileId });
    const now = list();
    assert.deepEqual(now, [...prefix, id]);
    assert.ok(increasing(now.map(Number)));
    return id;
  }
  const prefix = [appendLayout('a'), appendLayout('b')];
  const held = list();
  for (let i = 0; i < 6; i++) store.appendJson(entry, { i });
  const next = appendLayout('c');
  const fresh = list();
  assert.deepEqual(held, prefix);
  held.push(next);
  held.push('only-in-this-array');
  const accounted = [...prefix, next];
  assert.deepEqual([held, fresh, list()], [[...accounted, 'only-in-this-array'], accounted, accounted]);

  function expectBarrierFailure(operation) {
    const moved = `${path}-moved`;
    renameSync(path, moved);
    try {
      writeFileSync(path, 'not a directory');
      assert.throws(operation);
    } finally {
      rmSync(path, { force: true });
      renameSync(moved, path);
    }
  }
  expectBarrierFailure(() => store.sync());
  assert.deepEqual(list(), accounted);
  store.sync();
  const landed = store.appendJson(layout, { compileId: 'landed-before-barrier' });
  expectBarrierFailure(() => store.sync());
  assert.deepEqual(list(), [...accounted, landed.id]);
});

test('A repaired-away ID has the ordinary JS null miss until reissued', (t) => {
  const f = fixture(t);
  const first = f.store.appendJson(layout, { compileId: 'intact' });
  f.store.sync();
  const last = f.store.appendJson(layout, { compileId: 'old-mark', text: 'tail'.repeat(2048) });
  const listedMark = f.store.getRecordIdsByType(layout).at(-1);
  assert.equal(listedMark, last.id);
  const log = join(f.path, 'records.log');
  const store = f.reopen(() => truncateSync(log, statSync(log).size - 1));
  assert.ok(store.recovery()?.droppedBytes > 0, 'fixture tore the trailing record');
  assert.deepEqual(store.getRecordIdsByType(layout), [first.id]);
  assert.equal(store.getRecord(listedMark), null);
  const payload = { compileId: 'replacement', text: 'new acceptance' };
  const replacement = store.appendJson(layout, payload);
  assert.equal(replacement.id, listedMark);
  assert.deepEqual(readEntry(store, listedMark), { id: listedMark, payload });
});
