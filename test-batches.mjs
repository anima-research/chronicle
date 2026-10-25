// Node test for the batched calls, through the napi binding:
// appendManyToStateJson (a batch of items as one record, and the index its
// first item took), getStateItemsJson (several items by index in one call)
// and getRecords (several records by ID in one call).
//
//   npm run build && node test-batches.mjs
//
// Uses a fresh temporary store and removes it afterwards, so it can be re-run.

import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { JsStore } from './index.js';

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'chronicle-batches-'));
const storePath = path.join(dir, 'store');
let step = 0;
const ok = (msg) => console.log(`✓ ${++step}. ${msg}`);
const manifestVersion = () => fs.readFileSync(path.join(storePath, 'MANIFEST'))[4];
const decode = (bytes) => JSON.parse(Buffer.from(bytes).toString('utf8'));

try {
  let store = JsStore.openOrCreate({ path: storePath });
  store.registerState({ id: 'log', strategy: 'append_log', deltaSnapshotEvery: 3, fullSnapshotEvery: 4 });
  store.registerState({ id: 'never-written', strategy: 'append_log' });
  const model = [];

  // --- appendManyToStateJson ---------------------------------------------
  store.appendToStateJson('log', { n: 0 });
  model.push({ n: 0 });
  assert.equal(manifestVersion(), 1, 'a store without a batch keeps MANIFEST version 1');

  const first = [{ n: 1 }, { n: 2, tags: ['a', 'b'] }, 'three'];
  const a = store.appendManyToStateJson('log', first);
  model.push(...first);
  assert.equal(a.firstOrdinal, 1);
  assert.equal(typeof a.record.id, 'string');
  assert.equal(a.record.recordType, 'state_update');
  for (const [i, item] of first.entries()) {
    assert.deepEqual(store.getStateItemJson('log', a.firstOrdinal + i), item);
  }
  const update = JSON.parse(store.getStateUpdateJson(a.record.id));
  assert.deepEqual(update.operation.AppendMany.map(decode), first, 'the record holds the whole batch');
  assert.equal(update.global_sequence, a.record.sequence);
  assert.equal(manifestVersion(), 2, 'the first batch marks the MANIFEST');
  ok('appendManyToStateJson: one record holding the batch, items at firstOrdinal + i, MANIFEST marked');

  // A create aimed at the open store fails on the lock before it writes
  // anything. It used to rewrite MANIFEST to version 1 first, dropping the mark.
  const manifestBytes = () => fs.readFileSync(path.join(storePath, 'MANIFEST'));
  const marked = manifestBytes();
  assert.throws(() => JsStore.create({ path: storePath }), /locked/i);
  assert.deepEqual(manifestBytes(), marked, 'the failed create left the MANIFEST as it was');
  ok('JsStore.create on the open store fails on the lock and leaves the MANIFEST marked');

  for (let round = 0; round < 5; round++) {
    const items = Array.from({ length: round + 1 }, (_, k) => ({ round, k }));
    const before = store.getStateLen('log');
    const batch = store.appendManyToStateJson('log', items);
    assert.equal(batch.firstOrdinal, before, `round ${round}`);
    model.push(...items);
    store.appendToStateJson('log', { round, single: true });
    model.push({ round, single: true });
  }
  store.redactStateItems('log', 2, 4);
  model.splice(2, 2);
  const afterRedact = store.appendManyToStateJson('log', [{ late: 1 }, { late: 2 }]);
  model.push({ late: 1 }, { late: 2 });
  assert.equal(afterRedact.firstOrdinal, model.length - 2, 'a redact shortens the count');
  assert.deepEqual(store.getStateJson('log'), model);
  assert.equal(store.getStateLen('log'), model.length);
  ok('firstOrdinal follows single appends, earlier batches, snapshots and redacts');

  const seq = store.currentSequence();
  assert.throws(() => store.appendManyToStateJson('log', []), /AppendMany needs at least one item/);
  assert.equal(store.currentSequence(), seq, 'an empty batch writes nothing');
  assert.equal(store.getStateLen('log'), model.length);
  ok('an empty batch throws and writes nothing');

  // On a view, the batch goes to the view's branch and counts its length.
  store.createBranch('side');
  const side = store.view('side');
  store.appendManyToStateJson('log', [{ main: true }]);
  model.push({ main: true });
  const onSide = side.appendManyToStateJson('log', [{ side: 1 }, { side: 2 }]);
  assert.equal(onSide.firstOrdinal, model.length - 1, 'the side branch does not hold the main batch');
  assert.deepEqual(side.getStateItemJson('log', onSide.firstOrdinal + 1), { side: 2 });
  assert.deepEqual(store.getStateJson('log'), model, 'main is unchanged');
  ok('on a view, a batch goes to and counts the view\'s branch');

  // --- state-delta events --------------------------------------------------
  const sub = store.subscribe({ filter: { stateIds: ['log'], includeStateChanges: true } });
  store.catchUpSubscription(sub);
  const delta = [{ d: 1 }, { d: 2 }];
  const d = store.appendManyToStateJson('log', delta);
  model.push(...delta);
  const events = [];
  for (let e = store.pollSubscription(sub); e; e = store.pollSubscription(sub)) events.push(e);
  const deltas = events.filter((e) => e.eventType === 'state_delta').map((e) => JSON.parse(e.data));
  assert.equal(deltas[0].state_id, 'log');
  assert.equal(deltas[0].sequence, d.record.sequence);
  assert.deepEqual(deltas[0].operation.AppendMany.map(decode), delta);
  assert.ok(
    deltas.slice(1).every((e) => !('AppendMany' in e.operation) && !('Append' in e.operation)),
    'one event for the whole batch (any other is the store\'s own snapshot)',
  );
  store.unsubscribe(sub);
  ok('a batch reaches a subscriber as one state_delta event carrying {"AppendMany": [[..], ..]}');

  // --- getStateItemsJson ---------------------------------------------------
  const len = model.length;
  const probes = [len - 1, 0, Math.floor(len / 2), len - 1, len, len + 7, -1, 1, 3];
  const expected = probes.map((i) => (i >= 0 && i < len ? model[i] : null));
  const singles = (handle) => probes.map((i) => handle.getStateItemJson('log', i));
  assert.deepEqual(singles(store), expected);
  assert.deepEqual(store.getStateItemsJson('log', probes), expected, 'warm');
  assert.deepEqual(store.getStateItemsJson('log', []), []);
  assert.deepEqual(store.getStateItemsJson('never-written', [0, 1]), [null, null]);
  assert.deepEqual(store.getStateItemsJson('not-registered', [0]), [null]);
  const sideLen = side.getStateLen('log');
  assert.deepEqual(side.getStateItemsJson('log', [sideLen - 1, sideLen]), [{ side: 2 }, null], 'a view reads its branch');
  ok('getStateItemsJson: one entry per index, in order, null where getStateItemJson is null');

  // A reopened store has cold caches: the batched read must still match.
  const records = [a.record, d.record, afterRedact.record];
  store.close();
  assert.throws(() => JsStore.create({ path: storePath }), /already holds a Chronicle store/);
  assert.deepEqual(manifestBytes(), marked, 'a create over the closed store is refused and leaves the mark');
  store = JsStore.open({ path: storePath });
  assert.equal(manifestVersion(), 2);
  assert.deepEqual(store.getStateItemsJson('log', [len - 1]), [model[len - 1]], 'the last item alone, cold');
  assert.deepEqual(store.getStateItemsJson('log', probes), expected, 'cold');
  assert.deepEqual(singles(store), expected);
  ok('getStateItemsJson after a reopen (cold) equals the single reads');

  // --- getRecords ------------------------------------------------------------
  const note = store.appendJson('note', { text: 'plain record' });
  const unknown = String(BigInt(note.id) + 1000n);
  const ids = [note.id, unknown, records[0].id, records[1].id, note.id, records[2].id];
  const got = store.getRecords(ids);
  assert.equal(got.length, ids.length);
  for (const [i, id] of ids.entries()) {
    assert.deepEqual(got[i], store.getRecord(id), `entry ${i}`);
  }
  assert.equal(got[1], null, 'an unknown ID answers null in its place');
  assert.equal(got[0].recordType, 'note');
  assert.equal(got[2].id, records[0].id);
  assert.deepEqual(store.getRecords([]), []);
  assert.throws(() => store.getRecords([note.id, 'not-an-id']), /Invalid record ID: "not-an-id"/);
  ok('getRecords: one entry per ID, in order, null for an unknown ID; a malformed ID throws');

  store.close();
  console.log('\n✅ All batch tests passed!');
} finally {
  fs.rmSync(dir, { recursive: true, force: true });
}
