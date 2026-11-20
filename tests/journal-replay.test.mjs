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
    // pins appendJson.13: the synchronous result gives the numeric sequence on the writing branch.
    assert.equal(ref.sequence, expectedSequence);
    // pins appendJson.9: even when the type or branch changes, the returned decimal ID advances.
    assert.ok(positiveDecimal(ref.id) && BigInt(ref.id) > BigInt(writes.at(-1)?.id ?? '0'));
    const ordinal = writes.filter((w) => w.type === type).length;
    writes.push({ type, ordinal, payload, id: ref.id, sequence: ref.sequence });
    return ref;
  }

  store.switchBranch('left');
  const first = write(entry, { id: 'external-1', branch: 'right', text: 'café' });
  store.switchBranch('right');
  write(checkpoint, { through: first.id, snapshot: null }); // same sequence as first
  store.switchBranch('left');
  const before = store.appendToStateJson('messages', { text: 'one' });
  const note = write(entry, { id: 'application-id', branch: 'right', text: 'note between messages' });
  const after = store.appendToStateJson('messages', { text: 'two' });
  // pins appendJson.5: the note names the lagging right branch but consumes the left messages' next slot.
  assert.deepEqual([first.sequence, before.sequence, note.sequence, after.sequence],
    [head + 1, head + 2, head + 3, head + 4]);
  store.switchBranch('right');
  const badCheckpoint = write(checkpoint, null);
  const badIdentity = write(identity, { v: 1 }); // no storeId
  store.switchBranch('left');
  write(layout, { v: 3, ns: 'conversation', compileId: 'good', branch: store.currentBranch() });
  store.switchBranch('right');
  write(identity, { v: 1, storeId: 'actual-store' });
  const badLayout = write(layout, { compileId: false });
  write(entry, { id: 'external-tail', branch: 'left' });

  // For a byte-for-byte check, supply the bytes ourselves: a JSON append
  // promises a decodable value, not JSON.stringify's spelling of a number.
  const bytes = Buffer.from(' {"answer": 1.0, "text": "café"}\n');
  const raw = store.append('journal/raw-json', bytes);
  const types = [entry, checkpoint, identity, layout];
  const saved = types.map((type) => writes.filter((w) => w.type === type).map((w) => w.id));
  function replay() {
    // pins getRecord.10: Buffer.from on the stored payload recovers the exact bytes, not reserialized JSON.
    assert.deepEqual(Buffer.from(store.getRecord(raw.id).payload), bytes);
    const lists = new Map(types.map((type) => [type, store.getRecordIdsByType(type)]));
    for (const type of types) {
      const ids = lists.get(type);
      const expected = writes.filter((w) => w.type === type);
      // pins getRecordIdsByType.1: callers can use the complete array of strings directly, without awaiting.
      assert.ok(Array.isArray(ids) && ids.length === expected.length && ids.every((id) => typeof id === 'string'));
      // pins getRecordIdsByType.2: exact type, not a prefix match (notably entry vs entry/checkpoint).
      assert.deepEqual(ids, expected.map((w) => w.id));
      // pins getRecordIdsByType.12: every listed key is decimal and strictly above the empty cursor.
      assert.ok(ids.every(positiveDecimal));
      // pins getRecordIdsByType.10: each listed key resolves the matching append ID and payload, not a sibling's slot.
      assert.deepEqual(ids.map((id) => readEntry(store, id)), expected.map(({ id, payload }) => ({ id, payload })));
      for (const w of expected) {
        // pins appendJson.8: the append reference itself both occurs in the list and addresses the retained record.
        assert.deepEqual([ids.includes(w.id), readEntry(store, w.id)], [true, { id: w.id, payload: w.payload }]);
        const record = store.getRecord(w.id);
        // pins getRecord.11: older records keep their append-time numeric sequence, including overlapping siblings.
        assert.equal(record.sequence, w.sequence);
      }
    }
    // pins getRecordIdsByType.4: even unusable application envelopes are present under their stored types.
    assert.deepEqual([lists.get(checkpoint).includes(badCheckpoint.id), lists.get(identity).includes(badIdentity.id),
      lists.get(layout).includes(badLayout.id)], [true, true, true]);
    // pins getRecordIdsByType.13: take each ID *from its listing* in the chronology of the cross-branch writes.
    assert.ok(increasing(writes.map(({ type, ordinal }) => BigInt(lists.get(type)[ordinal]))));
    return [...lists.values()];
  }
  const initialLists = replay();
  store = f.reopen();
  // pins getRecordIdsByType.14: retained append references are the exact type lists on both sides of reopen.
  assert.deepEqual([initialLists, replay()], [saved, saved]);
});

test('A typed journal outlives historical cuts and deletion of a contributing branch', (t) => {
  const f = fixture(t);
  let store = f.store;
  store.registerState({ id: 'settings', strategy: 'snapshot' });
  store.registerState({ id: 'messages', strategy: 'append_log' });
  store.registerState({ id: 'files', strategy: 'tree' });
  // The tree entry is compared by the fields callers use, so a field the
  // store adds can't break the comparison.
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
  const foreign = append('compiled-on-writer', writer); // no switch back for the receipt
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
  // pins switchBranch.8: the switched root can enumerate and read both siblings' receipts.
  assert.deepEqual(onMain, { listed: ids, entries: expected });
  store.createBranchAt('past', 'survivor', cut);
  store.switchBranch('past');
  const onPast = observe();
  // pins createBranchAt.13: snapshot, log and tree go back to the cut; not one post-cut ID is lost or duplicated.
  assert.deepEqual([state(), onPast.listed], [['old', ['before'], oldFile], ids]);
  // pins getRecord.6: saved IDs resolve independently of whether they're in the historical handle's history.
  assert.deepEqual(ids.map((id) => readEntry(store, id)), expected);
  store.deleteBranch('writer');
  assert.ok(!store.listBranches().some(({ name }) => name === 'writer'), 'the contributing branch was deleted');
  const afterDelete = observe();
  // pins getRecord.6: deleting the writer doesn't invalidate a survivor's lookups by saved ID.
  assert.deepEqual(ids.map((id) => readEntry(store, id)), expected);
  store = f.reopen();
  const afterOpen = observe();
  // pins deleteBranch.5: interleaved IDs and payloads from the deleted branch survive immediately and after reopen.
  assert.deepEqual([afterDelete, afterOpen], Array(2).fill({ listed: ids, entries: expected }));
  for (const observation of [onMain, onPast, afterDelete, afterOpen]) {
    // pins getRecordIdsByType.5: exactly the store-wide order on switch, historical rollback, deletion and reopen.
    assert.deepEqual(observation.listed, ids);
    // pins appendJson.4: the cross-branch receipt is discoverable and intact through every change of history.
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
    // pins getRecordIdsByType.16: even repeated lookups of an absent identity mint nothing and move no head.
    assert.deepEqual([store.getRecordIdsByType(identity), position()], [[], beforeReads]);
    // pins getRecordIdsByType.3: an absent checkpoint is a successful empty array.
    assert.deepEqual([store.getRecordIdsByType(checkpoint), position()], [[], beforeReads]);
  }
  const cp = store.appendJson(checkpoint, { through: '0', snapshot: null });
  // pins getRecordIdsByType.3: null snapshot in a present checkpoint is not absence of the record.
  assert.deepEqual(store.getRecordIdsByType(checkpoint), [cp.id]);

  const list = () => store.getRecordIdsByType(layout);
  function appendLayout(compileId) {
    const prefix = [...list()];
    const { id } = store.appendJson(layout, { compileId });
    const now = list();
    // pins appendJson.12: the accepted layout extends exactly the accounted-for prefix by its returned ID.
    assert.deepEqual(now, [...prefix, id]);
    // The IDs ascend as Numbers too, across a change in their count of digits. The
    // part of appendJson.12 at 2 ** 53, where two IDs compare equal as Numbers,
    // has no test: no call makes a record ID that large.
    assert.ok(increasing(now.map(Number)));
    return id;
  }
  const prefix = [appendLayout('a'), appendLayout('b')];
  const held = list();
  for (let i = 0; i < 6; i++) store.appendJson(entry, { i });
  const next = appendLayout('c');
  const fresh = list();
  // pins getRecordIdsByType.18: neither appending nor re-listing grows the caller's held snapshot.
  assert.deepEqual(held, prefix);
  held.push(next); // exactly what the fold index does on success
  held.push('only-in-this-array');
  const accounted = [...prefix, next];
  // pins getRecordIdsByType.18: a push succeeds locally and changes neither a separate result nor the store index.
  assert.deepEqual([held, fresh, list()], [[...accounted, 'only-in-this-array'], accounted, accounted]);

  // Fold acceptance syncs on either side of appendJson. Fail each barrier
  // in turn by obstructing the store path, not any particular store file.
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
  // The caller appends only after the first barrier succeeds, so a refused
  // barrier means no append.
  expectBarrierFailure(() => store.sync());
  // pins getRecordIdsByType.19: acceptance refused at the first barrier leaves the exact accounted prefix alone.
  assert.deepEqual(list(), accounted);
  store.sync();
  const landed = store.appendJson(layout, { compileId: 'landed-before-barrier' });
  expectBarrierFailure(() => store.sync());
  // pins getRecordIdsByType.19: refresh on the same handle finds the landed acceptance just in the new suffix.
  assert.deepEqual(list(), [...accounted, landed.id]);
});

test('A repaired-away ID has the ordinary JS null miss until reissued', (t) => {
  const f = fixture(t);
  const first = f.store.appendJson(layout, { compileId: 'intact' });
  f.store.sync();
  const last = f.store.appendJson(layout, { compileId: 'old-mark', text: 'tail'.repeat(2048) });
  const listedMark = f.store.getRecordIdsByType(layout).at(-1);
  assert.equal(listedMark, last.id);
  // The fixture tears the last byte of the record log, as a crash in the middle
  // of the append would. The evidence comes from the reopened handle.
  const log = join(f.path, 'records.log');
  const store = f.reopen(() => truncateSync(log, statSync(log).size - 1));
  assert.ok(store.recovery()?.droppedBytes > 0, 'fixture tore the trailing record');
  assert.deepEqual(store.getRecordIdsByType(layout), [first.id]);
  // pins getRecord.4: a single lookup of the formerly listed mark is strictly null before any ID can be reissued.
  assert.equal(store.getRecord(listedMark), null);
  const payload = { compileId: 'replacement', text: 'new acceptance' };
  const replacement = store.appendJson(layout, payload);
  // pins getRecord.13: if reissued, the saved key resolves the replacement compile; otherwise it is still missing.
  assert.deepEqual(readEntry(store, listedMark), replacement.id === listedMark ? { id: replacement.id, payload } : null);
});
