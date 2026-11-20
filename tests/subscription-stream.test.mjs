import { before, test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ErrorCode, JsStore } from '../index.js';

function freshStore(t) {
  const dir = mkdtempSync(join(tmpdir(), 'chronicle-stream-'));
  let store;
  t.after(() => {
    try { store?.close(); } finally { rmSync(dir, { recursive: true, force: true }); }
  });
  store = JsStore.openOrCreate({ path: join(dir, 'store') });
  return store;
}

function drain(store, id) {
  const events = [];
  for (let i = 0; i < 32; i++) {
    const event = store.pollSubscription(id);
    if (!event) return events;
    events.push(event);
  }
  assert.fail('small subscription queue did not drain');
}

const recordId = event => String(JSON.parse(event.data).record.id);
const idsOf = records => records.map(record => record.id);
const typesOf = events => events.map(event => event.eventType);

function startLive(store, filter) {
  const id = store.subscribe({ filter });
  store.catchUpSubscription(id);
  assert.equal(store.pollSubscription(id)?.eventType, 'caught_up');
  return id;
}

before(() => {
  const dir = mkdtempSync(join(tmpdir(), 'chronicle-poll-probe-'));
  const code = `
    import assert from 'node:assert/strict';
    import { JsStore } from ${JSON.stringify(new URL('../index.js', import.meta.url).href)};
    const store = JsStore.openOrCreate({ path: process.argv[1] });
    try {
      const id = store.subscribe();
      store.catchUpSubscription(id);
      assert.equal(store.pollSubscription(id)?.eventType, 'caught_up');
      for (let i = 0; i < 200000; i++) assert.equal(store.pollSubscription(id), null);
    } finally { store.close(); }
  `;
  try {
    const probe = spawnSync(process.execPath, ['--input-type=module', '-e', code, join(dir, 'store')], {
      timeout: 60000, killSignal: 'SIGKILL', encoding: 'utf8',
    });
    assert.equal(probe.status, 0, probe.error?.message ?? probe.stderr);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('Interleaved polls drain distinct filtered streams in order without consuming each other', t => {
  const store = freshStore(t);
  const types = ['note', 'flag', 'post', 'flag', 'flag', 'note', 'flag', 'post'];
  const records = types.map((type, i) => store.appendJson(type, i));
  const watch = recordTypes => store.subscribe({ fromSequence: 0, filter: { includeRecords: true, recordTypes } });
  const a = watch(['note', 'post']), b = watch(['flag']), all = watch(undefined);
  const ids = [a, b, all];
  assert.ok(ids.every(id => typeof id === 'string' && id.length > 0));
  assert.equal(new Set(ids).size, ids.length);
  for (const id of ids) store.catchUpSubscription(id);

  const queues = new Map([[a, []], [b, []]]);
  for (let i = 0; i < 4; i++) {
    for (const id of i % 2 === 0 ? [a, b] : [b, a]) queues.get(id).push(store.pollSubscription(id));
  }
  assert.ok([...queues.get(a), ...queues.get(b)].every(event => event?.eventType === 'record'));
  const aIds = queues.get(a).map(recordId), bIds = queues.get(b).map(recordId);
  assert.deepEqual([aIds, bIds], [idsOf(records.filter(r => r.recordType !== 'flag')), idsOf(records.filter(r => r.recordType === 'flag'))]);

  const unrestricted = records.map(() => store.pollSubscription(all));
  assert.deepEqual([...unrestricted.map(recordId), store.pollSubscription(all)?.eventType], [...idsOf(records), 'caught_up']);
  const recordType = event => JSON.parse(event.data).record.record_type;
  assert.deepEqual([queues.get(a).map(recordType), unrestricted.map(recordType)], [types.filter(type => type !== 'flag'), types]);

  for (const id of [a, b]) assert.equal(store.pollSubscription(id)?.eventType, 'caught_up');
  assert.deepEqual(ids.map(id => store.pollSubscription(id)), [null, null, null]);
  const note = store.appendJson('note', 'later'), post = store.appendJson('post', 'later'), flag = store.appendJson('flag', 'later');
  assert.deepEqual(ids.map(id => drain(store, id).map(recordId)), [[note.id, post.id], [flag.id], [note.id, post.id, flag.id]]);

  store.unsubscribe(a);
  const next = store.appendJson('flag', 'after cancellation');
  assert.deepEqual([store.subscriptionCount(), ...[b, all].map(id => recordId(store.pollSubscription(id)))], [2, next.id, next.id]);
});

test('Event inclusion flags select replay and live classes with bare names and JSON data', t => {
  const store = freshStore(t);
  store.registerState({ id: 'mood', strategy: 'snapshot' });
  const initialState = store.setStateJson('mood', 'quiet');
  const earlier = store.appendJson('note', 'yesterday');
  const filter = { includeRecords: false, includeStateChanges: false, includeBranchEvents: false };
  const subscriptions = [
    store.subscribe({ fromSequence: 0, filter: { ...filter, includeRecords: true, recordTypes: ['note'] } }),
    store.subscribe({ fromSequence: initialState.sequence, filter: { ...filter, includeStateChanges: true } }),
    store.subscribe({ filter: { ...filter, includeBranchEvents: true } }),
  ];
  const replay = subscriptions.map(id => { store.catchUpSubscription(id); return drain(store, id); });
  const later = store.appendJson('note', 'today');
  const delta = store.setStateJson('mood', 'busy');
  const main = store.currentBranch();
  const side = store.createBranch('short-lived', main.name);
  store.deleteBranch(side.name);
  const live = subscriptions.map(id => drain(store, id));
  const other = store.view(main.name);
  other.unsubscribe(subscriptions[2]);
  other.close();
  const dropped = store.pollSubscription(subscriptions[2]);

  const events = [...replay.flat(), ...live.flat(), dropped];
  assert.deepEqual(new Set(typesOf(events)), new Set(['record', 'state_snapshot', 'state_delta', 'branch_head', 'branch_created', 'branch_deleted', 'caught_up', 'dropped']));
  for (const event of events) {
    assert.equal(typeof event.data, 'string', event.eventType);
    assert.doesNotThrow(() => JSON.parse(event.data), event.eventType);
  }
  assert.deepEqual([replay.map(typesOf), live.map(typesOf)], [
    [['record', 'caught_up'], ['state_snapshot', 'caught_up'], ['caught_up']],
    [['record'], ['state_delta'], ['branch_head', 'branch_head', 'branch_created', 'branch_deleted']],
  ]);
  assert.equal(replay[2][0]?.eventType, 'caught_up');
  assert.doesNotThrow(() => JSON.parse(replay[2][0].data));
  const deletion = live[2].find(event => event.eventType === 'branch_deleted');
  assert.deepEqual([deletion?.eventType, JSON.parse(deletion.data).name], ['branch_deleted', side.name]);

  const branchData = type => JSON.parse(live[2].find(e => e.eventType === type).data);
  const heads = live[2].filter(e => e.eventType === 'branch_head').map(e => JSON.parse(e.data))
    .map(({ branch, head }) => [branch, head]);
  const created = branchData('branch_created').branch;
  assert.deepEqual([heads, created.id, created.name, created.head, created.created,
    branchData('branch_deleted').name, JSON.parse(dropped.data).reason],
  [[[main.name, later.sequence], [main.name, delta.sequence]], Number(side.id), side.name, side.head, side.created, side.name, 'unsubscribed']);
  for (const [i, id] of subscriptions.entries()) {
    const events = [...replay[i], ...live[i]];
    if (i === 2) events.push(dropped);
    for (const event of events) {
      assert.equal({ subscriptionId: id, ...JSON.parse(event.data) }.subscriptionId, id);
    }
  }
  const summaries = [replay[0][0], live[0][0]].map(e => JSON.parse(e.data).record);
  const snap = JSON.parse(replay[1][0].data), change = JSON.parse(live[1][0].data);
  assert.deepEqual({
    records: summaries.map(r => [r.id, r.sequence, r.branch, r.record_type, r.timestamp, r.payload_size]),
    snapshot: [snap.state_id, snap.data, snap.sequence, snap.truncated, snap.total_bytes],
    delta: [change.state_id, change.operation, change.sequence],
  }, {
    records: [earlier, later].map(r => [Number(r.id), r.sequence, Number(main.id), r.recordType, r.timestamp, r.payload.length]),
    snapshot: ['mood', 'quiet', initialState.sequence, false, Buffer.byteLength('"quiet"')],
    delta: ['mood', { Set: [...Buffer.from('"busy"')] }, delta.sequence],
  });
});

test('A branch selector left undefined selects what an omitted one does', t => {
  const store = freshStore(t);
  store.createBranch('work');
  const base = { includeRecords: true };
  const unspecified = startLive(store, { ...base, branch: undefined });
  const omitted = startLive(store, base);
  const main = store.appendJson('note', 'on main');
  const work = store.view('work');
  work.appendJson('note', 'on work');
  work.close();
  const control = drain(store, omitted).map(recordId);
  assert.ok(control.includes(main.id));
  assert.deepEqual(drain(store, unspecified).map(recordId), control);
});

test('An explicit branch name selects the live records of that branch', {
  todo: 'The live record filter ignores the branch name: a subscription to work receives records written to both main and work.',
}, t => {
  const store = freshStore(t);
  store.createBranch('work');
  const named = startLive(store, { includeRecords: true, branch: 'work' });
  store.appendJson('note', 'on main');
  const work = store.view('work');
  const onWork = work.appendJson('note', 'on work');
  work.close();
  assert.deepEqual(drain(store, named).map(recordId), [onWork.id]);
});

test('An explicit branch name selects the replayed and live states of that branch', {
  todo: "A state subscription ignores the branch name: a catch-up replays the value on the handle's branch, and live changes come from every branch.",
}, t => {
  const store = freshStore(t);
  setSnapshot(store, 'mood', 'base');
  store.createBranch('work');
  const work = store.view('work');
  work.setStateJson('mood', 'on work');
  store.setStateJson('mood', 'on main');
  const named = watchSnapshots(store, { filter: { includeStateChanges: true, stateIds: ['mood'], branch: 'work' } });
  assert.deepEqual(replaySnapshots(store, named).map(snapshot => snapshot.data), ['on work']);
  work.setStateJson('mood', 'work again');
  store.setStateJson('mood', 'main again');
  work.close();
  assert.deepEqual(drain(store, named).map(event => JSON.parse(event.data).operation), [{ Set: [...Buffer.from('"work again"')] }]);
});

test('An explicit branch name selects the replayed records of that branch', {
  todo: "A catch-up replays the branch of the handle that catches up, and ignores the subscription's branch name.",
}, t => {
  const store = freshStore(t);
  store.createBranch('work');
  store.appendJson('note', 'on main');
  const work = store.view('work');
  const onWork = work.appendJson('note', 'on work');
  work.close();
  const named = store.subscribe({ fromSequence: 0, filter: { includeRecords: true, branch: 'work' } });
  store.catchUpSubscription(named);
  const events = drain(store, named);
  assert.equal(events.pop()?.eventType, 'caught_up');
  assert.deepEqual(events.map(recordId), [onWork.id]);
});

function setSnapshot(store, id, data) {
  store.registerState({ id, strategy: 'snapshot' });
  store.setStateJson(id, data);
}

function watchSnapshots(store, config) {
  return store.subscribe({ fromSequence: store.currentSequence(), ...config });
}

function replaySnapshots(store, id) {
  store.catchUpSubscription(id);
  const events = drain(store, id);
  assert.equal(events.pop()?.eventType, 'caught_up');
  return events.map(event => {
    assert.equal(event.eventType, 'state_snapshot');
    return JSON.parse(event.data);
  });
}

const snapshotId = snapshot => snapshot.state_id;
const jsonBytes = value => Buffer.byteLength(JSON.stringify(value));

test('Replay honors selected state IDs and explicit and default snapshot byte limits', t => {
  const store = freshStore(t);
  const fits = 'é'.repeat(6), tooBig = fits + 'é';
  const limit = jsonBytes(fits);
  setSnapshot(store, 'fits', fits);
  setSnapshot(store, 'over', tooBig);
  setSnapshot(store, 'unselected', 'keep out');
  const defaultLimit = 10 * 1024 * 1024;
  const atDefault = 'x'.repeat(defaultLimit - 2);
  setSnapshot(store, 'at-default', atDefault);
  setSnapshot(store, 'over-default', atDefault + 'x');

  const selectedIds = ['fits', 'over'];
  const smallId = watchSnapshots(store, { maxSnapshotBytes: limit, filter: { includeStateChanges: true, stateIds: selectedIds } });
  const defaultId = watchSnapshots(store, { filter: { includeStateChanges: true, stateIds: undefined } });
  const selected = replaySnapshots(store, smallId);
  assert.deepEqual(selected.map(snapshotId).sort(), selectedIds);
  const small = selected.find(s => snapshotId(s) === 'fits'), over = selected.find(s => snapshotId(s) === 'over');
  assert.deepEqual([small.truncated, small.data, over.truncated, over.data], [false, fits, true, null]);
  assert.deepEqual(selected.map(s => [snapshotId(s), s.truncated, jsonBytes(s.data) <= limit]).sort(), [['fits', false, true], ['over', true, true]]);

  const all = replaySnapshots(store, defaultId);
  assert.deepEqual(all.map(snapshotId).sort(), ['at-default', 'fits', 'over', 'over-default', 'unselected']);
  const at = all.find(s => snapshotId(s) === 'at-default'), above = all.find(s => snapshotId(s) === 'over-default');
  assert.deepEqual([at.truncated, at.data === atDefault, above.truncated, above.data], [false, true, true, null]);

  store.setStateJson('fits', 'é');
  store.setStateJson('over', 'é');
  store.setStateJson('unselected', 'still out');
  const changed = id => drain(store, id).map(event => {
    assert.equal(event.eventType, 'state_delta');
    return JSON.parse(event.data).state_id;
  });
  assert.deepEqual(changed(smallId), ['fits', 'over']);
  assert.deepEqual(changed(defaultId), ['fits', 'over', 'unselected']);
});

function cutArray(t, items, cap) {
  const store = freshStore(t);
  setSnapshot(store, 'items', items);
  const id = watchSnapshots(store, { maxSnapshotBytes: cap, filter: { includeStateChanges: true, stateIds: ['items'] } });
  const [snapshot] = replaySnapshots(store, id);
  return snapshot;
}

test('A replayed JSON array over the snapshot byte cap keeps its newest items and says where they start', t => {
  const snapshot = cutArray(t, ['aa', 'bb', 'cc'], 7);
  assert.deepEqual(
    [snapshot.truncated, snapshot.data, snapshot.from_index, snapshot.total_length, snapshot.total_bytes],
    [true, ['cc'], 2, 3, jsonBytes(['aa', 'bb', 'cc'])],
  );
});

test('The snapshot byte cap includes the delimiters of a replayed JSON array', {
  todo: 'Array truncation budgets the elements but not the brackets and commas in the delivered snapshot.',
}, t => {
  const snapshot = cutArray(t, ['é', 'é'], 9);
  assert.ok(jsonBytes(snapshot.data) <= 9, `${jsonBytes(snapshot.data)} snapshot bytes exceed the limit of 9`);
});

test('The default queue holds a thousand events and cleanup also removes overflowed IDs', t => {
  const store = freshStore(t);
  const filter = { includeRecords: true, includeStateChanges: false, includeBranchEvents: false };
  const drained = startLive(store, filter), untouched = startLive(store, filter);
  const ids = Array.from({ length: 1000 }, (_, i) => store.appendJson('note', i).id);
  assert.equal(store.subscriptionCount(), 2);
  assert.deepEqual(ids.map(() => recordId(store.pollSubscription(drained))), ids);
  assert.equal(store.pollSubscription(drained), null);
  const extra = store.appendJson('note', 1000);
  assert.deepEqual([store.subscriptionCount(), recordId(store.pollSubscription(drained))], [1, extra.id]);

  const stillLive = store.appendJson('note', 1001);
  assert.equal(recordId(store.pollSubscription(drained)), stillLive.id);

  const another = startLive(store, filter);
  const tracked = new Set([untouched, drained, another]);
  assert.doesNotThrow(() => { for (const id of tracked) store.unsubscribe(id); });
  assert.equal(store.subscriptionCount(), 0);
  assert.throws(() => store.pollSubscription(untouched), { code: ErrorCode.SubscriptionNotFound });
});
