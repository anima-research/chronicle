//   npx napi build --platform --features napi-bindings && node test-key-order.mjs

import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { JsStore } from './index.js';

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'chronicle-key-order-'));
const storePath = path.join(dir, 'store');
let step = 0;
const ok = (msg) => console.log(`✓ ${++step}. ${msg}`);

const item = (n) => ({
  zeta: n,
  alpha: { round: n, call: 1, beta: [{ y: 1, x: 2 }, { b: { d: 1, c: 2 }, a: 3 }] },
  mid: 'm',
});
const bytesOf = (value) => JSON.stringify(value);
const RAW_INTEGER_KEYS = '{"zeta":1,"b":1,"10":2,"2":{"z":1,"0":2,"y":3},"alpha":3}';
const text = (buffer) => buffer.toString('utf8');
const list = (entries) => `[${entries.join(',')}]`;

let failures = [];
const check = (label, actual, expected) => {
  const got = typeof expected === 'string' ? actual : JSON.stringify(actual);
  const want = typeof expected === 'string' ? expected : JSON.stringify(expected);
  if (got !== want) failures.push(`${label}: got ${got}, want ${want}`);
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

  const plain = store.appendJson('t.order', item(0));
  const linked = store.appendJsonWithLinks('t.order', item(1), { causedBy: [plain.id] });
  const records = [[plain.id, bytesOf(item(0))], [linked.id, bytesOf(item(1))]];

  const snapRecord = store.setStateJson('snap', item(2));
  const snapAt = Number(snapRecord.sequence);
  store.setState('snap', Buffer.from(RAW_INTEGER_KEYS));
  const snapBytes = RAW_INTEGER_KEYS;

  const log = [];
  const lastItemReads = [];
  const appendAndReadLast = (append) => {
    const expected = append();
    log.push(expected);
    lastItemReads.push([log.length - 1, store.getStateItemJson('log', log.length - 1), expected]);
  };
  appendAndReadLast(() => {
    store.appendToStateJson('log', item(4));
    return bytesOf(item(4));
  });
  appendAndReadLast(() => {
    const r = store.appendToStateJsonWithIdentity('log', item(5), 'id', 'sequence');
    return bytesOf({ ...item(5), id: r.id, sequence: Number(r.sequence) });
  });
  appendAndReadLast(() => {
    const own = { id: 'mine', zeta: 6, sequence: -1, alpha: { round: 6, call: 1 } };
    const r = store.appendToStateJsonWithIdentity('log', own, 'id', 'sequence');
    return bytesOf({ id: r.id, zeta: 6, sequence: Number(r.sequence), alpha: { round: 6, call: 1 } });
  });
  appendAndReadLast(() => {
    store.appendToState('log', Buffer.from(bytesOf(item(7))));
    return bytesOf(item(7));
  });
  appendAndReadLast(() => {
    store.appendToState('log', Buffer.from(RAW_INTEGER_KEYS));
    return RAW_INTEGER_KEYS;
  });
  const batchReads = [];
  const appendBatchAndRead = (append) => {
    const first = log.length;
    const { batch, written } = append();
    check(`firstOrdinal of the batch at ${first}`, batch.firstOrdinal, first);
    log.push(...written.map(bytesOf));
    const last = first + written.length - 1;
    batchReads.push([[last], store.getStateItemsJson('log', [last]), [written.at(-1)]]);
    const all = written.map((_, i) => first + i);
    batchReads.push([all, store.getStateItemsJson('log', all), written]);
  };
  appendBatchAndRead(() => ({
    batch: store.appendManyToStateJson('log', [item(16), item(17)]),
    written: [item(16), item(17)],
  }));
  appendBatchAndRead(() => {
    const own = { id: 'mine', zeta: 18, sequence: -1, alpha: { round: 18, call: 1 } };
    const batch = store.appendManyToStateJsonWithIdentity('log', [own, item(19)], 'id', 'sequence');
    const id = batch.record.id;
    const sequence = Number(batch.record.sequence);
    return {
      batch,
      written: [{ id, zeta: 18, sequence, alpha: { round: 18, call: 1 } }, { ...item(19), id, sequence }],
    };
  });
  for (let n = 9; n < 13; n++) {
    appendAndReadLast(() => {
      store.appendToStateJson('log', item(n));
      return bytesOf(item(n));
    });
  }
  const logAtRecord = store.appendToStateJson('log', item(13));
  log.push(bytesOf(item(13)));
  const logAt = Number(logAtRecord.sequence);
  const logAtBytes = log.slice();
  store.editStateItem('log', 0, Buffer.from(bytesOf(item(14))));
  log[0] = bytesOf(item(14));
  appendAndReadLast(() => {
    store.appendToStateJson('log', item(15));
    return bytesOf(item(15));
  });

  for (const [index, value, expected] of lastItemReads) {
    check(`getStateItemJson(log, ${index}) straight after its append`, value, JSON.parse(expected));
  }
  for (const [indices, values, expected] of batchReads) {
    check(`getStateItemsJson(log, [${indices}]) straight after its batch`, values, expected);
  }
  phaseHolds();
  ok('the last item, read straight after each append, and a batch\'s items, read straight after it, keep their keys in the order written');

  const readAll = (label) => {
    for (const [id, expected] of records) {
      check(`${label}: getRecord(${id}).payload`, text(store.getRecord(id).payload), expected);
    }
    const queried = store.query({ types: ['t.order'] });
    check(`${label}: query payloads`, list(queried.map((r) => text(r.payload))), list(records.map(([, bytes]) => bytes)));

    check(`${label}: getStateJson(snap)`, store.getStateJson('snap'), JSON.parse(snapBytes));
    check(`${label}: getState(snap)`, text(store.getState('snap')), snapBytes);
    check(`${label}: getStateJsonAt(snap)`, store.getStateJsonAt('snap', snapAt), item(2));
    check(`${label}: getStateAt(snap)`, text(store.getStateAt('snap', snapAt)), bytesOf(item(2)));

    check(`${label}: getStateJson(log)`, store.getStateJson('log'), JSON.parse(list(log)));
    check(`${label}: getState(log)`, text(store.getState('log')), list(log));
    for (let i = 0; i < log.length; i++) {
      check(`${label}: getStateItemJson(log, ${i})`, store.getStateItemJson('log', i), JSON.parse(log[i]));
    }
    check(`${label}: getStateItemsJson(log)`, store.getStateItemsJson('log', log.map((_, i) => i)), JSON.parse(list(log)));
    check(`${label}: getStateSlice(log)`, text(store.getStateSlice('log', 2, 5)), list(log.slice(2, 7)));
    check(`${label}: getStateTail(log)`, text(store.getStateTail('log', 7)), list(log.slice(-7)));
    check(`${label}: getStateJsonAt(log)`, store.getStateJsonAt('log', logAt), JSON.parse(list(logAtBytes)));
    check(`${label}: getStateAt(log)`, text(store.getStateAt('log', logAt)), list(logAtBytes));

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
    check(`${label}: subscription snapshot of snap`, snapshots.get('snap'), JSON.parse(snapBytes));
    check(`${label}: subscription snapshot of log`, snapshots.get('log'), JSON.parse(list(log)));
    for (const [id, expected] of records) {
      check(`${label}: subscription record payload ${id}`, payloads.get(id), JSON.parse(expected));
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
