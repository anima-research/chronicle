// Node test for key order: a JSON object written to the store reads back with
// its keys in the order they were written, at every level, through every
// surface that returns it, fresh, after a reopen, and after compaction.
//
//   npx napi build --platform --features napi-bindings && node test-key-order.mjs
//
// Uses a fresh temporary store and removes it afterwards, so it can be re-run.

import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { JsStore } from './index.js';

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'chronicle-key-order-'));
const storePath = path.join(dir, 'store');
let step = 0;
const ok = (msg) => console.log(`✓ ${++step}. ${msg}`);

// Keys out of sorted order at the top, in a nested object, and in objects
// inside an array.
const item = (n) => ({
  zeta: n,
  alpha: { round: n, call: 1, beta: [{ y: 1, x: 2 }, { b: { d: 1, c: 2 }, a: 3 }] },
  mid: 'm',
});
// The same, spelled the way the bytes surfaces are compared.
const spelled = (value) => JSON.stringify(value);
// Bytes as JSON, in the order the bytes hold.
const parsed = (buffer) => JSON.parse(buffer.toString('utf8'));
// Every surface is checked before the phase fails, so a failure names each
// surface that lost the order, not only the first.
let failures = [];
const check = (label, actual, expected) => {
  if (spelled(actual) !== spelled(expected)) failures.push(`${label}: got ${spelled(actual)}, want ${spelled(expected)}`);
};
const phaseHolds = () => {
  const found = failures;
  failures = [];
  assert.deepEqual(found, []);
};

try {
  let store = JsStore.openOrCreate({ path: storePath });
  store.registerState({ id: 'snap', strategy: 'snapshot' });
  store.registerState({ id: 'log', strategy: 'append_log', deltaSnapshotEvery: 2, fullSnapshotEvery: 4 });

  // --- records ------------------------------------------------------------
  const plain = store.appendJson('t.order', item(0));
  const linked = store.appendJsonWithLinks('t.order', item(1), { causedBy: [plain.id] });
  const records = [[plain.id, item(0)], [linked.id, item(1)]];

  // --- a snapshot state -----------------------------------------------------
  const snapRecord = store.setStateJson('snap', item(2));
  let snapValue = item(2);
  const snapAt = Number(snapRecord.sequence);
  store.setState('snap', Buffer.from(spelled(item(3))));
  snapValue = item(3);

  // --- an append log, crossing delta and full snapshots ----------------------
  const log = [];
  const lastItemReads = [];
  const appendAndReadLast = (append) => {
    const expected = append();
    log.push(expected);
    // Straight after the append, before anything warms a cache: the last item
    // comes from the head record.
    lastItemReads.push([log.length - 1, store.getStateItemJson('log', log.length - 1), expected]);
  };
  appendAndReadLast(() => {
    store.appendToStateJson('log', item(4));
    return item(4);
  });
  appendAndReadLast(() => {
    const r = store.appendToStateJsonWithIdentity('log', item(5), 'id', 'sequence');
    // The item lacks both fields: they follow the caller's keys, id first.
    return { ...item(5), id: r.id, sequence: Number(r.sequence) };
  });
  appendAndReadLast(() => {
    const own = { id: 'mine', zeta: 6, sequence: -1, alpha: { round: 6, call: 1 } };
    const r = store.appendToStateJsonWithIdentity('log', own, 'id', 'sequence');
    // The item has both fields: they keep their place and take the new values.
    return { id: r.id, zeta: 6, sequence: Number(r.sequence), alpha: { round: 6, call: 1 } };
  });
  appendAndReadLast(() => {
    store.appendToState('log', Buffer.from(spelled(item(7))));
    return item(7);
  });
  for (let n = 8; n < 13; n++) {
    appendAndReadLast(() => {
      store.appendToStateJson('log', item(n));
      return item(n);
    });
  }
  const logAtRecord = store.appendToStateJson('log', item(13));
  log.push(item(13));
  const logAt = Number(logAtRecord.sequence);
  const logAtValue = log.slice();
  store.editStateItem('log', 0, Buffer.from(spelled(item(14))));
  log[0] = item(14);
  appendAndReadLast(() => {
    store.appendToStateJson('log', item(15));
    return item(15);
  });

  for (const [index, value, expected] of lastItemReads) {
    check(`getStateItemJson(log, ${index}) straight after its append`, value, expected);
  }
  phaseHolds();
  ok('the last item, read straight after each append, keeps its keys in the order written');

  const readAll = (label) => {
    for (const [id, expected] of records) {
      check(`${label}: getRecord(${id}).payload`, parsed(store.getRecord(id).payload), expected);
    }
    const queried = store.query({ types: ['t.order'] });
    check(`${label}: query payloads`, queried.map((r) => parsed(r.payload)), records.map(([, v]) => v));

    check(`${label}: getStateJson(snap)`, store.getStateJson('snap'), snapValue);
    check(`${label}: getState(snap)`, parsed(store.getState('snap')), snapValue);
    check(`${label}: getStateJsonAt(snap)`, store.getStateJsonAt('snap', snapAt), item(2));
    check(`${label}: getStateAt(snap)`, parsed(store.getStateAt('snap', snapAt)), item(2));

    check(`${label}: getStateJson(log)`, store.getStateJson('log'), log);
    check(`${label}: getState(log)`, parsed(store.getState('log')), log);
    for (let i = 0; i < log.length; i++) {
      check(`${label}: getStateItemJson(log, ${i})`, store.getStateItemJson('log', i), log[i]);
    }
    check(`${label}: getStateSlice(log)`, parsed(store.getStateSlice('log', 2, 5)), log.slice(2, 7));
    check(`${label}: getStateTail(log)`, parsed(store.getStateTail('log', 4)), log.slice(-4));
    check(`${label}: getStateJsonAt(log)`, store.getStateJsonAt('log', logAt), logAtValue);
    check(`${label}: getStateAt(log)`, parsed(store.getStateAt('log', logAt)), logAtValue);

    const sub = store.subscribe({
      fromSequence: 1,
      filter: { recordTypes: ['t.order'], stateIds: ['snap', 'log'], includeRecords: true, includeStateChanges: true },
    });
    store.catchUpSubscription(sub);
    const snapshots = new Map();
    const payloads = new Map();
    for (let event = store.pollSubscription(sub); event; event = store.pollSubscription(sub)) {
      const data = JSON.parse(event.data);
      if (event.eventType === 'state_snapshot') snapshots.set(data.state_id, data.data);
      if (event.eventType === 'record' && data.record.payload) payloads.set(String(data.record.id), data.record.payload);
    }
    store.unsubscribe(sub);
    check(`${label}: subscription snapshot of snap`, snapshots.get('snap'), snapValue);
    check(`${label}: subscription snapshot of log`, snapshots.get('log'), log);
    for (const [id, expected] of records) {
      check(`${label}: subscription record payload ${id}`, payloads.get(id), expected);
    }
  };

  readAll('fresh');
  phaseHolds();
  ok('every read surface hands back each object with its keys in the order written');

  store.close();
  store = JsStore.openOrCreate({ path: storePath });
  readAll('reopened');
  phaseHolds();
  ok('after close and reopen, the order holds');

  store.compactState('log');
  store.compactState('snap');
  readAll('compacted');
  phaseHolds();
  store.close();
  store = JsStore.openOrCreate({ path: storePath });
  readAll('compacted and reopened');
  phaseHolds();
  store.close();
  ok('after compaction, and a reopen after it, the order holds');

  console.log('\n✅ Key order tests passed!');
} finally {
  fs.rmSync(dir, { recursive: true, force: true });
}
