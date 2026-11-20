// Node test for the batched calls, through the napi binding:
// appendManyToStateJson (a batch of items as one record, and the index its
// first item took), appendManyToStateJsonWithIdentity (the same, with that
// record's id and sequence written into each item), getStateItemsJson
// (several items by index in one call) and getRecords (several records by
// ID in one call). Also that a write whose own threshold snapshot fails still
// returns its record (issue #39).
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

  // A request of negative indices only, and one mixing indices in range with
  // ones past either end, answer each index as getStateItemJson does.
  assert.deepEqual(store.getStateItemsJson('log', [-1, -2, -1]), [null, null, null]);
  const mixed = [-1, len, 0, len - 1, -5, len + 3, 2];
  assert.deepEqual(store.getStateItemsJson('log', mixed), [null, null, model[0], model[len - 1], null, null, model[2]]);
  assert.deepEqual(store.getStateItemsJson('log', mixed), mixed.map((i) => store.getStateItemJson('log', i)));
  // A state whose value isn't an array has no item at any index: both forms
  // answer null. The batched form used to throw for every request on it,
  // [-1] and [] included, and the single form for an index of zero or more.
  store.registerState({ id: 'obj', strategy: 'snapshot' });
  store.setStateJson('obj', { a: 1 });
  const objProbes = [0, -1, 1, 5, -3, 0];
  assert.deepEqual(objProbes.map((i) => store.getStateItemJson('obj', i)), objProbes.map(() => null));
  assert.deepEqual(store.getStateItemsJson('obj', [-1]), [null]);
  assert.deepEqual(store.getStateItemsJson('obj', []), []);
  assert.deepEqual(store.getStateItemsJson('obj', [0]), [null]);
  assert.deepEqual(store.getStateItemsJson('obj', objProbes), objProbes.map(() => null));
  // Bytes that aren't one JSON value are an error, not a value of another
  // type: both forms throw the deserialization error.
  store.registerState({ id: 'malformed', strategy: 'snapshot' });
  store.setState('malformed', Buffer.from('{'));
  assert.throws(() => store.getStateItemJson('malformed', 0), /Deserialization/);
  assert.throws(() => store.getStateItemsJson('malformed', [0]), /Deserialization/);
  ok('getStateItemsJson: negative-only and mixed requests, and a state that is not an array, answer as getStateItemJson does; malformed bytes throw from both');

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

  // --- appendManyToStateJsonWithIdentity -----------------------------------
  // A fresh store, so the MANIFEST mark can be watched from the start.
  const idPath = path.join(dir, 'identity');
  const idManifest = () => fs.readFileSync(path.join(idPath, 'MANIFEST'))[4];
  store = JsStore.openOrCreate({ path: idPath });
  for (const id of ['win', 'win-single']) {
    store.registerState({ id, strategy: 'append_log', deltaSnapshotEvery: 3, fullSnapshotEvery: 4 });
    store.registerStateFieldIndex(id, '/id', 'string');
    store.appendToStateJsonWithIdentity(id, { id: 'm0' }, 'by', 'at');
  }
  assert.equal(idManifest(), 1, 'single identity appends keep MANIFEST version 1');

  const entries = [{ id: 'm1' }, { id: 'm2', by: 'stale', at: -1 }, { retract: 'm0' }, { id: 'm3', n: 3 }];
  const singleRecords = entries.map((entry) => store.appendToStateJsonWithIdentity('win-single', entry, 'by', 'at'));
  const stamped = store.appendManyToStateJsonWithIdentity('win', entries, 'by', 'at');
  assert.equal(stamped.firstOrdinal, 1);
  assert.equal(stamped.record.recordType, 'state_update');
  assert.equal(idManifest(), 2, 'the first identity batch marks the MANIFEST');
  const stamp = (entry, record) => ({ ...entry, by: record.id, at: record.sequence });
  for (const [i, entry] of entries.entries()) {
    const item = store.getStateItemJson('win', stamped.firstOrdinal + i);
    assert.deepEqual(item, stamp(entry, stamped.record), `entry ${i} carries the batch's record`);
    const single = store.getStateItemJson('win-single', 1 + i);
    assert.deepEqual(single, stamp(entry, singleRecords[i]), `entry ${i} alone carries its own record`);
    const { by: _b, at: _a, ...rest } = item;
    const { by: _sb, at: _sa, ...singleRest } = single;
    assert.deepEqual(rest, singleRest, `entry ${i}: the same item apart from the stamps`);
  }
  const recorded = JSON.parse(store.getStateUpdateJson(stamped.record.id));
  assert.equal(recorded.global_sequence, stamped.record.sequence);
  assert.deepEqual(
    recorded.operation.AppendMany.map(decode),
    entries.map((entry) => stamp(entry, stamped.record)),
    'the one record holds every stamped item',
  );
  ok('appendManyToStateJsonWithIdentity: every item stamped with the one record, items at firstOrdinal + i, MANIFEST marked');

  for (const handle of ['win', 'win-single']) {
    assert.deepEqual(store.queryStateIndexEq(handle, '/id', 'm0', {}), [0], handle);
    assert.deepEqual(store.queryStateIndexEq(handle, '/id', 'm1', {}), [1], handle);
    assert.deepEqual(store.queryStateIndexEq(handle, '/id', 'm2', {}), [2], handle);
    assert.deepEqual(store.queryStateIndexEq(handle, '/id', 'm3', {}), [4], handle);
  }
  ok('a field index on the items\' own id answers each item\'s ordinal, batched as single');

  const cut = (name, at) => {
    store.createBranchAtWithStates(name, 'main', at, { stateIds: ['win'] });
    return store.view(name).getStateLen('win');
  };
  assert.equal(cut('before-batch', stamped.record.sequence - 1), 1, 'a cut before the batch drops all of it');
  assert.equal(cut('at-batch', stamped.record.sequence), 1 + entries.length, 'a cut at its sequence keeps all of it');
  ok('a branch cut by the batch\'s stamped sequence keeps or drops the whole batch');

  const idSeq = store.currentSequence();
  assert.throws(() => store.appendManyToStateJsonWithIdentity('win', [], 'by', 'at'), /AppendMany needs at least one item/);
  assert.throws(
    () => store.appendManyToStateJsonWithIdentity('win', [{ id: 'm9' }, ['not', 'an', 'object']], 'by', 'at'),
    /JSON object: item 1 of the batch/,
  );
  assert.throws(() => store.appendManyToStateJsonWithIdentity('win', ['m9'], 'by', 'at'), /JSON object/);
  assert.equal(store.currentSequence(), idSeq, 'a refused batch writes nothing');
  assert.equal(store.getStateLen('win'), 1 + entries.length);
  assert.deepEqual(store.queryStateIndexEq('win', '/id', 'm9', {}), []);
  ok('an empty batch, or one with an item that isn\'t an object, throws and writes nothing');

  store.createBranch('id-side');
  const idSide = store.view('id-side');
  store.appendManyToStateJsonWithIdentity('win', [{ id: 'main-only' }], 'by', 'at');
  const sideBatch = idSide.appendManyToStateJsonWithIdentity('win', [{ id: 's1' }, { id: 's2' }], 'by', 'at');
  assert.equal(sideBatch.firstOrdinal, 1 + entries.length, 'the side branch does not hold the main batch');
  assert.deepEqual(idSide.getStateItemJson('win', sideBatch.firstOrdinal + 1), stamp({ id: 's2' }, sideBatch.record));
  assert.equal(store.getStateLen('win'), 2 + entries.length, 'main is unchanged');
  {
    const peer = store.view('id-side');
    const cachedLen = idSide.getStateLen('win');
    const cachedHead = idSide.stateHeadOffset('win');
    const mainHead = store.stateHeadOffset('win');
    // sideBatch took the third-op snapshot. Two intervening writes make
    // this writer's next batch the third operation again.
    peer.appendManyToStateJson('win', [{ id: 'peer-1' }, { id: 'peer-2' }]);
    peer.appendToStateJson('win', { id: 'peer-3' });
    const interveningHead = peer.stateHeadOffset('win');
    const pre = idSide.stateHeadOffset('win');
    // pins stateHeadOffset.6: before writing, see the head the intervening writer left.
    assert.notEqual(pre, cachedHead);
    assert.equal(pre, interveningHead);

    const rows = [{ id: 's3' }, { id: 's4' }];
    const landed = idSide.appendManyToStateJsonWithIdentity('win', rows, 'by', 'at');
    const post = idSide.stateHeadOffset('win');
    // pins appendManyToStateJsonWithIdentity.6: on return both side handles see the change, main does not.
    assert.notEqual(post, pre);
    assert.equal(peer.stateHeadOffset('win'), post);
    assert.equal(store.stateHeadOffset('win'), mainHead);
    // pins appendManyToStateJsonWithIdentity.11: the ordinal counts all three peer items, not the two writes.
    assert.equal(landed.firstOrdinal, cachedLen + 3);
    assert.deepEqual(peer.getStateItemJson('win', landed.firstOrdinal), stamp({ id: 's3' }, landed.record));

    const updateOf = (id) => JSON.parse(store.getStateUpdateJson(id));
    const latest = updateOf(store.getRecordIdsByType('state_update').at(-1));
    assert.equal(latest.state_id, 'win');
    assert.equal(latest.global_sequence, idSide.currentSequence());
    assert.ok('DeltaSnapshot' in latest.operation || 'Snapshot' in latest.operation, 'the batch took its due snapshot');
    const next = peer.appendToStateJson('win', { id: 'after-snapshot' });
    // pins stateHeadOffset.6: the chain's public predecessor links witness both checks, including the snapshot head.
    assert.equal(pre, updateOf(landed.record.id).prev_update_offset);
    assert.notEqual(post, latest.prev_update_offset, 'post is not the batch record that preceded the snapshot');
    assert.equal(post, updateOf(next.id).prev_update_offset);
  }
  ok('on a view, an identity batch counts and stamps on the view\'s branch');

  // The store-wide record inventory too: a record written and then not
  // adopted as a head would leave the heads alone but not these.
  const observed = () => ({
    sequence: store.currentSequence(), head: store.currentBranch().head,
    revision: store.stateHeadOffset('win'), count: store.getStateLen('win'), items: store.getStateJson('win'),
    records: store.stats().recordCount, updates: store.getRecordIdsByType('state_update'),
  });
  const prior = observed();
  // pins appendToStateJsonWithIdentity.8: the single form synchronously throws, leaving the log and populated state alone.
  assert.throws(() => store.appendToStateJsonWithIdentity('win', ['not an object'], 'by', 'at'));
  assert.deepEqual(observed(), prior);
  ok('A refused single stamped append does not advance the log');

  const before = store.getStateJson('win');
  store.close();
  store = JsStore.open({ path: idPath });
  assert.deepEqual(store.getStateJson('win'), before, 'the stamped items after a reopen');
  // pins appendToStateJsonWithIdentity.8: nor does a reopen find a record of the refused append, or another head.
  assert.deepEqual(observed(), prior);
  assert.equal(idManifest(), 2);
  store.close();
  ok('the stamped items read back after a reopen');

  // --- inherited stamps and independent presentations ------------------------
  {
    const windowPath = path.join(dir, 'sibling-windows');
    store = JsStore.openOrCreate({ path: windowPath });
    store.registerState({ id: 'window', strategy: 'append_log' });
    const seed = { id: 'seed-row' };
    const shared = [{ id: 'red' }, { id: 'blue' }];
    const inherited = store.appendToStateJsonWithIdentity('window', { ...seed }, 'by', 'at');
    const sharedWrite = store.appendManyToStateJsonWithIdentity('window', structuredClone(shared), 'by', 'at');
    // Identical row IDs on the two siblings: only the presentation record
    // distinguishes one insertion from the other at this ordinal.
    const row = { id: 'green' };
    const batchRows = [{ retract: 'red' }, { id: 'yellow' }];
    const names = ['left', 'right'];
    for (const name of names) store.createBranch(name);
    const writes = names.map((name) => {
      const view = store.view(name);
      const single = view.appendToStateJsonWithIdentity('window', { ...row }, 'by', 'at');
      const batch = view.appendManyToStateJsonWithIdentity('window', structuredClone(batchRows), 'by', 'at');
      assert.equal(batch.firstOrdinal, 4);
      return { name, single, batch: batch.record };
    });
    assert.equal(writes[0].single.sequence, writes[1].single.sequence);
    assert.equal(writes[0].batch.sequence, writes[1].batch.sequence);
    // pins appendManyToStateJsonWithIdentity.12: even at that same ordinal and sequence the returned batch IDs differ.
    assert.notEqual(writes[0].batch.id, writes[1].batch.id);

    const checkWindows = () => {
      const writingRecord = (write) => {
        const record = store.getRecord(write.id);
        assert.equal(typeof record.id, 'string');
        assert.equal(typeof record.sequence, 'number');
        assert.equal(record.id, write.id);
        assert.equal(record.sequence, write.sequence);
        return record;
      };
      const prefixBatch = writingRecord(sharedWrite.record);
      const prefix = [stamp(seed, writingRecord(inherited)), ...shared.map((item) => stamp(item, prefixBatch))];
      const [left, right] = writes.map(({ name, single, batch }) => {
        const view = store.view(name);
        const batchRecord = writingRecord(batch);
        const tail = [stamp(row, writingRecord(single)), ...batchRows.map((item) => stamp(item, batchRecord))];
        const read = (start, limit) => decode(view.getStateSlice('window', start, limit));
        const sharedSlice = read(0, 3), divergentSlice = read(3, 3), batchSlice = read(4, 2);
        const full = view.getStateJson('window');
        // pins appendManyToStateJsonWithIdentity.7: both full and sliced reads retain each stamped batch row in place.
        assert.deepEqual(full, [...prefix, ...tail]);
        assert.deepEqual(divergentSlice, tail);
        // pins getStateSlice.8: stamps at each slice boundary and on batch peers are those of their writing records.
        assert.deepEqual(sharedSlice, prefix);
        assert.deepEqual(batchSlice, tail.slice(1));
        return { full, sharedSlice, divergentSlice, batchSlice };
      });
      // pins getStateSlice.8: equality of the first sliced by recognizes the shared prefix, not independent presentations.
      assert.equal(left.sharedSlice[0].by, right.sharedSlice[0].by);
      assert.notEqual(left.divergentSlice[0].by, right.divergentSlice[0].by);
      assert.notEqual(left.batchSlice[0].by, right.batchSlice[0].by);
      // pins appendToStateJsonWithIdentity.7: the inherited single retains its by, the two new singles have distinct IDs/stamps.
      assert.deepEqual([left.full[0].by, right.full[0].by], [inherited.id, inherited.id]);
      assert.notEqual(writes[0].single.id, writes[1].single.id);
      assert.notEqual(left.full[3].by, right.full[3].by);
      // pins appendManyToStateJsonWithIdentity.12: an inherited batch keeps its by while independent batch stamps diverge.
      assert.deepEqual([left.full[1].by, right.full[1].by], [sharedWrite.record.id, sharedWrite.record.id]);
      assert.notEqual(left.full[4].by, right.full[4].by);
    };
    checkWindows();
    store.close();
    store = JsStore.open({ path: windowPath });
    checkWindows();
    store.close();
  }
  ok('Slices share prefix record stamps but distinguish independent sibling presentations');

  // --- single-write payloads and quarantine ledger references ----------------
  {
    const ledgerPath = path.join(dir, 'quarantine');
    const messages = 'agent/messages';
    const ledger = 'agent/autobio:compression-refusal-quarantine-events';
    store = JsStore.openOrCreate({ path: ledgerPath });
    for (const id of [messages, ledger]) store.registerState({ id, strategy: 'append_log' });
    const message = {
      participant: 'user', timestamp: 100,
      content: [
        { type: 'text', text: 'Page two' },
        { type: 'blob_ref', ref: { hash: 'ab'.repeat(32), originalType: 'image', mediaType: 'image/png' } },
      ],
      metadata: { source: { id: 'scan', sequence: 99 } },
      bodyGroupId: 'pages', shardIndex: 1, shardCount: 2, currentResolution: 0, lockedByAgent: false,
    };
    const messageWrite = store.appendToStateJsonWithIdentity(messages, structuredClone(message), 'id', 'sequence');
    const expectedMessage = { ...message, id: messageWrite.id, sequence: messageWrite.sequence };
    // pins appendToStateJsonWithIdentity.3: the complete message keeps its nested blob reference and shard metadata.
    assert.deepEqual(store.getStateItemJson(messages, 0), expectedMessage);

    const expectedEvents = [];
    const writeEvent = (event) => {
      const record = store.appendToStateJsonWithIdentity(ledger, structuredClone(event), 'eventId', 'sequence');
      // pins appendToStateJsonWithIdentity.6: synchronous result is the whole writing record, not just its stamps.
      assert.equal(typeof record.id, 'string');
      assert.equal(typeof record.sequence, 'number');
      assert.equal(record.recordType, 'state_update');
      assert.deepEqual(record, store.getRecord(record.id));
      const item = store.getStateItemJson(ledger, expectedEvents.length);
      // pins appendToStateJsonWithIdentity.4: these arguments select eventId and sequence on the top-level object.
      assert.deepEqual([item.eventId, item.sequence], [record.id, record.sequence]);
      // pins appendToStateJsonWithIdentity.15: each stamped event can supply a nonempty ID for a later ledger write.
      assert.ok(item.eventId.length > 0);
      expectedEvents.push({ ...event, eventId: record.id, sequence: record.sequence });
      return record;
    };
    const key = 'chunk/family';
    const exhaustion = (created) => ({
      kind: 'exhausted', key, created,
      record: { key, created, chunkSourceHash: 'chunk', sourceIds: [messageWrite.id],
        plan: [{ parentId: 'root', childIds: ['a', 'b'], requestHash: 'bounded' }] },
      outcomes: [{ curveLabel: 'canonical', requestHash: 'bounded', outcome: 'admission_rejected' }],
    });
    const first = writeEvent(exhaustion(101));
    writeEvent({ kind: 'clear', key, created: 102, targetClaimId: first.id });
    const event = exhaustion(103);
    const second = writeEvent(event);
    const pending = writeEvent({ kind: 'alert_pending', key, created: 104, targetClaimId: second.id, alertKey: key });
    writeEvent({ kind: 'checkpoint', key: '__checkpoint__', created: 105, active: [{
      generationId: second.id, record: event.record, outcomes: event.outcomes,
      pendingAlert: { eventId: pending.id, alertKey: key }, sentAlertKeys: [],
    }] });
    writeEvent({ kind: 'alert_sent', key, created: 106, targetClaimId: second.id, alertKey: key, pendingEventId: pending.id });

    store.close();
    store = JsStore.open({ path: ledgerPath });
    const replayed = store.getStateJson(ledger);
    // pins appendToStateJsonWithIdentity.3: only the requested stamps are added; the nested records, outcomes and checkpoint survive.
    assert.deepEqual(replayed, expectedEvents);
    assert.deepEqual(store.getStateJson(messages), [expectedMessage]);
    const [old, clear, current, alert, checkpoint, sent] = replayed;
    // pins appendToStateJsonWithIdentity.15: replay keeps the exact old/new generation and acknowledged pending-event IDs.
    assert.notEqual(old.eventId, current.eventId);
    assert.notEqual(current.eventId, alert.eventId);
    assert.equal(clear.targetClaimId, old.eventId);
    assert.equal(alert.targetClaimId, current.eventId);
    assert.equal(sent.targetClaimId, current.eventId);
    assert.equal(sent.pendingEventId, alert.eventId);
    assert.equal(checkpoint.active[0].pendingAlert.eventId, alert.eventId);
    store.close();
  }
  ok('Quarantine generation links name complete identity writes after replay');

  // --- a write whose own snapshot fails --------------------------------------
  // A write that reaches the state's snapshot cadence takes the snapshot
  // right after its record. If that snapshot fails, the write is recorded
  // all the same, so it returns its record and doesn't throw (issue #39); the
  // snapshot stays due for the next write. An item that isn't JSON makes
  // every snapshot that reads it fail, as a full disk would.
  const writeIn = (form, handle, items) => {
    if (form === 'identity-batch') return handle.appendManyToStateJsonWithIdentity('s', items, 'by', 'at').record;
    if (form === 'identity-single') return handle.appendToStateJsonWithIdentity('s', items[0], 'by', 'at');
    return handle.appendManyToStateJson('s', items).record;
  };
  for (const form of ['identity-batch', 'identity-single', 'plain-batch']) {
    const failPath = path.join(dir, `snapshot-fails-${form}`);
    store = JsStore.openOrCreate({ path: failPath });
    // A delta every 2 operations: the write after the poison is the second.
    store.registerState({ id: 's', strategy: 'append_log', deltaSnapshotEvery: 2, fullSnapshotEvery: 10 });
    store.appendToState('s', Buffer.from('not json'));
    const seq = store.currentSequence();
    const written = form === 'identity-single' ? 1 : 2;

    const record = writeIn(form, store, [{ id: 'a' }, { id: 'b' }]);
    assert.equal(record.sequence, seq + 1, form);
    assert.equal(store.currentSequence(), record.sequence, `${form}: no snapshot record followed`);
    assert.equal(store.getStateLen('s'), 1 + written, form);
    const held = JSON.parse(store.getStateUpdateJson(record.id)).operation;
    const heldItems = form === 'identity-single' ? [held.Append] : held.AppendMany;
    const expected = [{ id: 'a' }, { id: 'b' }].slice(0, written);
    assert.deepEqual(
      heldItems.map(decode),
      form === 'plain-batch' ? expected : expected.map((item) => stamp(item, record)),
      `${form}: the record holds the write`,
    );

    // Still due: the next write tries the snapshot again, fails again on
    // the poison, and returns too.
    const next = writeIn(form, store, [{ id: 'c' }]);
    assert.equal(next.sequence, record.sequence + 1, form);
    assert.equal(store.currentSequence(), next.sequence, form);
    // Both failures are in the report, with the latest one's sequence and
    // error.
    const failures = store.snapshotFailures();
    assert.equal(failures.length, 1, form);
    assert.match(failures[0].error, /Deserialization/, form);
    assert.deepEqual(
      { ...failures[0], error: undefined },
      { stateId: 's', branch: 'main', failures: 2, sequence: next.sequence, error: undefined },
      `${form}: snapshotFailures() lists the state`,
    );
    store.close();

    store = JsStore.open({ path: failPath });
    assert.deepEqual(store.snapshotFailures(), [], `${form}: the report is kept in memory only`);
    assert.equal(store.currentSequence(), next.sequence, `${form}: the writes survive a reopen`);
    assert.equal(store.getStateLen('s'), 2 + written, form);
    assert.equal(writeIn(form, store, [{ id: 'd' }]).sequence, next.sequence + 1, `${form}: after the reopen too`);
    store.close();
  }
  ok('a write whose own snapshot fails returns its record and goes into snapshotFailures(), and the writes stay recorded across a reopen');

  // The snapshot a failure left due is taken by the next write that can
  // take it. A full snapshot every 2 operations reads the whole value, so
  // it fails on the poison until a set replaces the value.
  const retryPath = path.join(dir, 'snapshot-retried');
  store = JsStore.openOrCreate({ path: retryPath });
  store.registerState({ id: 's', strategy: 'append_log', deltaSnapshotEvery: 2, fullSnapshotEvery: 1 });
  store.appendToState('s', Buffer.from('not json'));
  const failed = store.appendManyToStateJsonWithIdentity('s', [{ id: 'a' }], 'by', 'at');
  assert.equal(store.currentSequence(), failed.record.sequence, 'the snapshot failed, nothing followed');
  assert.deepEqual(store.snapshotFailures().map((f) => [f.stateId, f.failures, f.sequence]), [['s', 1, failed.record.sequence]]);
  const value = [{ id: 'a' }, { id: 'b' }];
  const set = store.setStateJson('s', value);
  assert.equal(store.currentSequence(), set.sequence + 1, 'the set, then the snapshot left due');
  assert.deepEqual(store.snapshotFailures(), [], 'the snapshot ends the entry');
  const updates = store.getRecordIdsByType('state_update');
  const snapshot = JSON.parse(store.getStateUpdateJson(updates[updates.length - 1])).operation;
  assert.deepEqual(decode(snapshot.Snapshot), value, 'a full snapshot of the value the set wrote');
  const later = store.appendManyToStateJsonWithIdentity('s', [{ id: 'c' }], 'by', 'at');
  assert.equal(store.currentSequence(), later.record.sequence, 'the snapshot reset the count');
  assert.deepEqual(store.getStateJson('s'), [...value, stamp({ id: 'c' }, later.record)]);
  store.close();
  ok('the snapshot a failure left due is taken by the next write that can take it');

  console.log(`\n✅ All batch tests passed! (${step} passed, 0 failed, 0 skipped)`);
} finally {
  fs.rmSync(dir, { recursive: true, force: true });
}
