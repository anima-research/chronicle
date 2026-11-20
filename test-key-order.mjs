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

const scenario = (name, run) => {
  const open = () => JsStore.openOrCreate({ path: path.join(dir, name) });
  let store = open();
  const reopen = () => {
    store.close();
    store = open();
    return store;
  };
  try {
    run(store, reopen);
  } finally {
    store.close();
  }
  ok(name);
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

  scenario('JS snapshot replacement preserves caller-owned keys and normalized JSON values', (store) => {
    const slot = 'checkpoints';
    store.registerState({ id: slot, strategy: 'snapshot' });
    const first = { kept: 'root', deleted: 'old', path: ['a', 'b', 'c'] };
    store.setStateJson(slot, first);
    assert.deepEqual(store.getStateJson(slot), first);

    const patch = [{ op: 'replace', path: '/title', value: 'new' }];
    const pin = { id: 'p1', firstMessageId: 'a', lastMessageId: 'b', kind: 'pin' };
    const limits = { high: Number.MAX_VALUE, low: -Number.MAX_VALUE, tiny: Number.MIN_VALUE, overflow: null };
    const expected = {
      kept: 'root', path: ['b', 'a'], destinations: { constructor: 'channel-c' },
      checkpoint: { patch }, pins: [pin], limits, summary: 'before\ufffdafter',
    };
    store.setStateJson(slot, { ...expected, checkpoint: { data: undefined, patch }, summary: 'before\ud800after' });
    const actual = store.getStateJson(slot);
    assert.deepEqual([Object.hasOwn(actual.destinations, 'constructor'), actual.destinations.constructor], [true, 'channel-c']);
    assert.deepEqual([Object.hasOwn(actual.checkpoint, 'data'), actual.checkpoint.patch,
      Object.hasOwn(actual.pins[0], 'name'), actual.pins], [false, patch, false, [pin]]);
    assert.deepEqual(actual.limits, { high: Number.MAX_VALUE, low: -Number.MAX_VALUE, tiny: Number.MIN_VALUE, overflow: null });
    assert.equal(actual.summary, 'before\ufffdafter');
    assert.deepEqual(actual, expected);
  });

  scenario('A lone low surrogate half in summary text reads back as U+FFFD', (store, reopen) => {
    const slot = 'checkpoints';
    store.registerState({ id: slot, strategy: 'snapshot' });
    store.setStateJson(slot, { summary: 'before\udc00after' });
    assert.equal(store.getStateJson(slot).summary, 'before\ufffdafter');
    assert.equal(reopen().getStateJson(slot).summary, 'before\ufffdafter');
  });

  scenario('A number that is not finite is refused at the call or never read back', (store, reopen) => {
    const values = [Infinity, -Infinity, NaN].flatMap((bad) => [bad, { nested: { bad } }, [1, bad]]);
    const finite = (value) => JSON.stringify(value, (_, v) => {
      if (typeof v === 'number' && !Number.isFinite(v)) throw new Error(`a stored ${v}`);
      return v;
    });
    const slots = values.map((value, i) => {
      const slot = `value/${i}`;
      store.registerState({ id: slot, strategy: 'snapshot' });
      let refused = false;
      try { store.setStateJson(slot, value); } catch { refused = true; }
      if (!refused) finite(store.getStateJson(slot));
      return slot;
    });
    const reopened = reopen();
    for (const slot of slots) finite(reopened.getStateJson(slot));
  });

  scenario('Byte-written namespaced snapshots preserve caller-normalized JSON on the intended branches', (store, reopen) => {
    const slot = 'agent/context/kvunified:presentation-receipt';
    store.registerState({ id: slot, strategy: 'snapshot' });
    store.createBranch('draft');
    store.createBranch('revision');
    store.switchBranch('draft');
    const draft = store.view('draft');
    store.switchBranch('revision');

    const receiptBytes = (level) => Buffer.from(String.raw`{"entries":[[{"level":${level},"café\ufffd":"first","café\ufffd":"last\ufffd","literal":"\\ud800"}]]}`);
    const receipt = (level) => ({ entries: [[{ level, 'café\ufffd': 'last\ufffd', literal: '\\ud800' }]] });
    const revisionBytes = receiptBytes(2);
    const draftBytes = receiptBytes(5);
    const draftSeed = receiptBytes(1);
    draft.setState(slot, draftSeed);
    store.setState(slot, revisionBytes);
    assert.deepEqual([store.getState(slot), draft.getState(slot)], [revisionBytes, draftSeed]);
    draft.setState(slot, draftBytes);
    assert.deepEqual([store.getState(slot), draft.getState(slot)], [revisionBytes, draftBytes]);
    assert.deepEqual(store.getStateJson(slot), receipt(2));
    store.switchBranch('draft');
    assert.deepEqual([store.getStateJson(slot), store.view('revision').getStateJson(slot)], [receipt(5), receipt(2)]);
    const cold = reopen();
    assert.deepEqual([cold.view('draft').getStateJson(slot), cold.view('revision').getStateJson(slot)], [receipt(5), receipt(2)]);
  });

  scenario('Append, identity append and prepared byte edit agree on stored JSON normalization', (store, reopen) => {
    const slot = 'messages';
    store.registerState({ id: slot, strategy: 'append_log' });
    store.appendToStateJson(slot, [{ zeta: [2, 1], alpha: 'one', optional: undefined }, 'last']);
    const first = [{ zeta: [2, 1], alpha: 'one' }, 'last'];
    assert.deepEqual(store.getStateItemJson(slot, 0), first);

    store.appendToStateJson(slot, { zeta: 'x\ud800y', optional: undefined, alpha: { low: 'a\udc00b', literal: '\\ud800' } });
    const plain = { zeta: 'x\ufffdy', alpha: { low: 'a\ufffdb', literal: '\\ud800' } };
    const warmPoint = store.getStateItemJson(slot, 1);
    assert.deepEqual([warmPoint.zeta, warmPoint.alpha.low], ['x\ufffdy', 'a\ufffdb']);

    const record = store.appendToStateJsonWithIdentity(slot, { zeta: '\udc00!', optional: undefined, alpha: ['\\udc00'] }, 'id', 'sequence');
    const stamped = { zeta: '\ufffd!', alpha: ['\\udc00'], id: record.id, sequence: Number(record.sequence) };
    const warmTail = store.getStateItemJson(slot, 2);
    const checkLog = (full, point, tail, head) => {
      assert.deepEqual([bytesOf(full), bytesOf(point), Object.hasOwn(full[1], 'optional'), Object.hasOwn(point, 'optional')],
        [bytesOf([head, plain, stamped]), bytesOf(plain), false, false]);
      assert.deepEqual([bytesOf(tail), bytesOf(full.at(-1)), Object.hasOwn(tail, 'optional'), Object.hasOwn(full.at(-1), 'optional')],
        [bytesOf(stamped), bytesOf(stamped), false, false]);
    };
    checkLog(store.getStateJson(slot), warmPoint, warmTail, first);
    const warmStrings = [bytesOf(warmPoint), bytesOf(warmTail)];

    store.editStateItem(slot, 0, Buffer.from(String.raw`{"zeta":"x\ufffdy","literal":"\\ud800","kind":"edited"}`));
    const edited = { zeta: 'x\ufffdy', literal: '\\ud800', kind: 'edited' };
    const checkEdit = (handle) => {
      assert.deepEqual([handle.getStateItemJson(slot, 0), handle.getStateJson(slot)[0]], [edited, edited]);
    };
    checkEdit(store);
    const cold = reopen();
    checkEdit(cold);
    const rebuilt = cold.getStateJson(slot);
    checkLog(rebuilt, cold.getStateItemJson(slot, 1), cold.getStateItemJson(slot, 2), edited);
    assert.deepEqual(warmStrings, rebuilt.slice(1).map(bytesOf));
  });

  scenario('Editing stamped content returns a new canonical point value, not a mutated cached object', (store) => {
    const slot = 'messages';
    store.registerState({ id: slot, strategy: 'append_log' });
    const record = store.appendToStateJsonWithIdentity(slot, { content: [{ text: 'before' }], obsolete: true }, 'id', 'sequence');
    assert.equal(typeof record.sequence, 'number');
    const stamp = { id: record.id, sequence: record.sequence };
    const original = { content: [{ text: 'before' }], obsolete: true, ...stamp };
    const retained = store.getStateItemJson(slot, 0);
    const replacement = { content: [{ text: 'edited ✓' }], ...stamp };
    store.editStateItem(slot, 0, Buffer.from(bytesOf(replacement)));
    const reread = store.getStateItemJson(slot, 0);
    assert.deepEqual(reread, replacement);
    assert.deepEqual([reread.id, reread.sequence], [record.id, record.sequence]);
    assert.equal(bytesOf(reread), bytesOf(store.getStateJson(slot)[0]));
    assert.notStrictEqual(reread, retained);
    assert.deepEqual(retained, original);
  });

  console.log(`\n✅ Key order tests passed! (${step} passed, 0 failed, 0 skipped)`);
} finally {
  fs.rmSync(dir, { recursive: true, force: true });
}
