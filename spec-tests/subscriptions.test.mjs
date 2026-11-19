import assert from 'node:assert/strict';
import { test } from 'node:test';
import { ErrorCode, freshStore, refuses } from './support.mjs';

const MiB = 1024 * 1024;

function drain(store, id) {
  const events = [];
  for (let event = store.pollSubscription(id); event; event = store.pollSubscription(id)) {
    events.push({ eventType: event.eventType, ...JSON.parse(event.data) });
  }
  return events;
}

function caughtUp(store, config) {
  const id = store.subscribe(config);
  store.catchUpSubscription(id);
  return id;
}

function snapshotsOf(store, value, maxSnapshotBytes) {
  store.registerState({ id: 's', strategy: 'snapshot' });
  store.setState('s', Buffer.from(value));
  const config = { fromSequence: store.currentSequence(), filter: { includeStateChanges: true, stateIds: ['s'] } };
  if (maxSnapshotBytes !== undefined) config.maxSnapshotBytes = maxSnapshotBytes;
  const [snapshot] = drain(store, caughtUp(store, config));
  return snapshot;
}

test('A63 T21 T22 with no config, and with no filter, a subscription follows nothing', () => {
  const { store } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log' });
  store.appendToStateJson('list', 1);
  const ids = [caughtUp(store), caughtUp(store, { fromSequence: 1 })];
  store.appendJson('note', {});
  store.appendToStateJson('list', 2);
  store.createBranch('side');
  for (const id of ids) assert.deepEqual(drain(store, id).map((event) => event.eventType), ['caught_up']);
  store.close();
});

test('A63 T21 with no bufferSize, 1,000 events can wait for a subscription, and one more drops it', () => {
  const { store } = freshStore();
  const id = caughtUp(store, { filter: { includeRecords: true } });
  for (let n = 0; n < 999; n++) store.append('note', Buffer.from('x'));
  assert.equal(store.subscriptionCount(), 1);
  store.append('note', Buffer.from('x'));
  assert.equal(store.subscriptionCount(), 0);
  for (let n = 0; n < 1000; n++) assert.ok(store.pollSubscription(id));
  refuses(() => store.pollSubscription(id), ErrorCode.SubscriptionDropped, 'Subscription was dropped');
  store.close();
});

test('A63 T21 with no maxSnapshotBytes, a catch-up sends a state of 10 MiB whole, and cuts a state one byte larger', () => {
  const { store } = freshStore();
  store.registerState({ id: 'whole', strategy: 'snapshot' });
  store.registerState({ id: 'cut', strategy: 'snapshot' });
  store.setStateJson('whole', 'x'.repeat(10 * MiB - 2));
  store.setStateJson('cut', 'x'.repeat(10 * MiB - 1));
  const events = drain(store, caughtUp(store, { fromSequence: store.currentSequence(), filter: { includeStateChanges: true } }));
  const byId = Object.fromEntries(events.filter((event) => event.eventType === 'state_snapshot').map((event) => [event.state_id, event]));
  assert.equal(byId.whole.truncated, false);
  assert.equal(byId.whole.total_bytes, 10 * MiB);
  assert.equal(byId.whole.data.length, 10 * MiB - 2);
  assert.equal(byId.cut.truncated, true);
  assert.equal(byId.cut.total_bytes, 10 * MiB + 1);
  assert.equal(byId.cut.data, null);
  store.close();
});

test('A65 T21 with no fromSequence, a catch-up sends only caught_up, on a store that holds followed records and states', () => {
  const { store } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log' });
  store.appendToStateJson('list', 1);
  store.appendJson('note', {});
  const id = caughtUp(store, { filter: { includeRecords: true, includeStateChanges: true } });
  assert.deepEqual(drain(store, id).map((event) => event.eventType), ['caught_up']);
  store.close();
});

test("A65 a catch-up replays a change of a state as a record event of the type state_update, whose payload is null and whose change getStateUpdateJson reads, when the subscription's types let it through", () => {
  const { store } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log' });
  const change = store.appendToStateJson('list', { n: 1 });
  const note = store.appendJson('note', { n: 2 });
  const all = drain(store, caughtUp(store, { fromSequence: change.sequence, filter: { includeRecords: true } }));
  const records = all.filter((event) => event.eventType === 'record').map((event) => event.record);
  assert.deepEqual(records.map((record) => [String(record.id), record.record_type]), [[change.id, 'state_update'], [note.id, 'note']]);
  assert.equal(records[0].payload, null);
  assert.equal(JSON.parse(store.getStateUpdateJson(String(records[0].id))).state_id, 'list');
  const notesOnly = drain(store, caughtUp(store, { fromSequence: change.sequence, filter: { includeRecords: true, recordTypes: ['note'] } }));
  assert.deepEqual(notesOnly.filter((event) => event.eventType === 'record').map((event) => String(event.record.id)), [note.id]);
  store.close();
});

test('T21 T23 a catch-up sends a state of maxSnapshotBytes or fewer whole, with its size and no cut', () => {
  const { store } = freshStore();
  const snapshot = snapshotsOf(store, '{"k":[1,2,3]}', 13);
  assert.deepEqual(
    { ...snapshot, sequence: undefined },
    { eventType: 'state_snapshot', type: 'state_snapshot', state_id: 's', data: { k: [1, 2, 3] }, sequence: undefined, truncated: false, total_bytes: 13, from_index: null, total_length: null },
  );
  assert.equal(snapshot.sequence, store.currentSequence());
  store.close();
});

test('T21 T23 for a list larger than maxSnapshotBytes, a catch-up sends the newest items, where they start and how many items the list has', () => {
  const { store } = freshStore();
  const items = ['aaaa', 'bbbb', 'cccc', 'dddd', 'eeee'];
  const value = JSON.stringify(items);
  const snapshot = snapshotsOf(store, value, 20);
  assert.equal(snapshot.truncated, true);
  assert.equal(snapshot.total_bytes, Buffer.byteLength(value));
  assert.equal(snapshot.total_length, 5);
  assert.ok(snapshot.from_index >= 1 && snapshot.from_index <= 4, String(snapshot.from_index));
  assert.deepEqual(snapshot.data, items.slice(snapshot.from_index));
  store.close();
});

test('T21 T23 for another value larger than maxSnapshotBytes, a catch-up sends no value and says it is cut', () => {
  const { store } = freshStore();
  const snapshot = snapshotsOf(store, '{"k":"a long enough value"}', 10);
  assert.equal(snapshot.truncated, true);
  assert.equal(snapshot.data, null);
  assert.equal(snapshot.total_bytes, 27);
  store.close();
});

test("T21 maxSnapshotBytes counts the stored state's bytes, not its characters or a respaced copy of it", () => {
  for (const [value, limit] of [[JSON.stringify('é'.repeat(10)), 15], ['{ "k" :  1 }', 10]]) {
    const { store } = freshStore();
    const snapshot = snapshotsOf(store, value, limit);
    assert.equal(snapshot.truncated, true, value);
    assert.equal(snapshot.total_bytes, Buffer.byteLength(value));
    store.close();
  }
});

test.todo('A65 T21 L8 a catch-up does not send whole a list larger than maxSnapshotBytes', () => {
  const { store } = freshStore();
  const snapshot = snapshotsOf(store, '[1,1,1]', 5);
  assert.ok(snapshot.from_index > 0, String(snapshot.from_index));
  assert.ok(JSON.stringify(snapshot.data).length <= 5, JSON.stringify(snapshot.data));
  store.close();
});

test.todo('A65 L6 a catch-up from a sequence above the head of the branch sends no record', () => {
  const { store } = freshStore();
  store.appendJson('note', {});
  const id = caughtUp(store, { fromSequence: store.currentSequence() + 10, filter: { includeRecords: true } });
  assert.deepEqual(drain(store, id).map((event) => event.eventType), ['caught_up']);
  store.close();
});

test.todo('A65 L7 a catch-up sends a followed state whose bytes are no JSON with its value, or says that its value is missing', () => {
  const { store } = freshStore();
  const snapshot = snapshotsOf(store, 'not json');
  assert.equal(snapshot?.eventType, 'state_snapshot');
  assert.ok(snapshot.data !== null || snapshot.truncated === true, JSON.stringify(snapshot));
  store.close();
});

test('T22 recordTypes limits the records to those types, in the catch-up and live', () => {
  const { store } = freshStore();
  const early = store.appendJson('keep', { n: 1 });
  store.appendJson('skip', { n: 2 });
  const id = caughtUp(store, { fromSequence: early.sequence, filter: { includeRecords: true, recordTypes: ['keep'] } });
  store.appendJson('skip', { n: 3 });
  const late = store.appendJson('keep', { n: 4 });
  const records = drain(store, id).filter((event) => event.eventType === 'record');
  assert.deepEqual(records.map((event) => String(event.record.id)), [early.id, late.id]);
  store.close();
});

test('T22 the store takes a branch in the filter and does not apply it: live records come from every branch', () => {
  const { store } = freshStore();
  store.createBranch('side');
  const id = caughtUp(store, { filter: { includeRecords: true, branch: 'nowhere' } });
  const onMain = store.appendJson('note', {});
  const onSide = store.view('side').appendJson('note', {});
  const records = drain(store, id).filter((event) => event.eventType === 'record');
  assert.deepEqual(records.map((event) => String(event.record.id)), [onMain.id, onSide.id]);
  store.close();
});

test('T22 stateIds leaves out every other state, in the catch-up and live', () => {
  const { store } = freshStore();
  store.registerState({ id: 'a', strategy: 'append_log' });
  store.registerState({ id: 'b', strategy: 'append_log' });
  store.appendToStateJson('a', 1);
  store.appendToStateJson('b', 1);
  const id = caughtUp(store, { fromSequence: store.currentSequence(), filter: { includeStateChanges: true, stateIds: ['a'] } });
  store.appendToStateJson('a', 2);
  store.appendToStateJson('b', 2);
  const states = drain(store, id).filter((event) => event.state_id !== undefined).map((event) => [event.eventType, event.state_id]);
  assert.deepEqual(states, [['state_snapshot', 'a'], ['state_delta', 'a']]);
  store.close();
});

test('T22 with no stateIds, the changes of every state are followed', () => {
  const { store } = freshStore();
  store.registerState({ id: 'a', strategy: 'append_log' });
  store.registerState({ id: 'b', strategy: 'snapshot' });
  const id = caughtUp(store, { filter: { includeStateChanges: true } });
  store.appendToStateJson('a', 1);
  store.setStateJson('b', 2);
  assert.deepEqual(drain(store, id).filter((event) => event.eventType === 'state_delta').map((event) => event.state_id), ['a', 'b']);
  store.close();
});

test('T22 each kind of change is followed only when its flag is true', () => {
  const { store } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log' });
  const ids = {
    record: caughtUp(store, { filter: { includeRecords: true } }),
    state_delta: caughtUp(store, { filter: { includeStateChanges: true } }),
    branch: caughtUp(store, { filter: { includeBranchEvents: true } }),
  };
  store.appendJson('note', {});
  store.appendToStateJson('list', 1);
  store.createBranch('side');
  store.deleteBranch('side');
  const kinds = (id) => [...new Set(drain(store, id).map((event) => event.eventType))].sort();
  assert.deepEqual(kinds(ids.record), ['caught_up', 'record']);
  assert.deepEqual(kinds(ids.state_delta), ['caught_up', 'state_delta']);
  assert.deepEqual(kinds(ids.branch), ['branch_created', 'branch_deleted', 'branch_head', 'caught_up']);
  store.close();
});

test('T22 T23 the branch events: a new head with the branch and its head, a created branch, and a deleted branch by its name', () => {
  const { store } = freshStore();
  const id = caughtUp(store, { filter: { includeBranchEvents: true } });
  const record = store.appendJson('note', {});
  store.createBranch('side');
  store.deleteBranch('side');
  const events = drain(store, id).filter((event) => event.eventType !== 'caught_up');
  assert.deepEqual(events.map((event) => event.eventType), ['branch_head', 'branch_created', 'branch_deleted']);
  assert.deepEqual(events[0], { eventType: 'branch_head', type: 'branch_head', branch: 'main', head: record.sequence });
  assert.equal(events[1].branch.name, 'side');
  assert.equal(typeof events[1].branch.id, 'number');
  assert.deepEqual(events[2], { eventType: 'branch_deleted', type: 'branch_deleted', name: 'side' });
  store.close();
});

test("T23 each event's data has the key type, with the same text as eventType", () => {
  const { store } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log' });
  store.appendToStateJson('list', 1);
  const id = caughtUp(store, { fromSequence: 1, filter: { includeRecords: true, includeStateChanges: true, includeBranchEvents: true } });
  store.appendJson('note', {});
  store.appendToStateJson('list', 2);
  store.createBranch('side');
  store.deleteBranch('side');
  store.view('main').unsubscribe(id);
  const events = [];
  do {
    const event = store.pollSubscription(id);
    events.push({ eventType: event.eventType, ...JSON.parse(event.data) });
  } while (events.at(-1).eventType !== 'dropped');
  assert.deepEqual(
    [...new Set(events.map((event) => event.eventType))].sort(),
    ['branch_created', 'branch_deleted', 'branch_head', 'caught_up', 'dropped', 'record', 'state_delta', 'state_snapshot'],
  );
  for (const event of events) assert.equal(event.type, event.eventType);
  store.close();
});

test("T23 a record event has the record's id as a number, sequence, branch, record_type, timestamp, payload_size, and payload read as JSON up to 4,096 bytes", () => {
  const { store } = freshStore();
  const id = caughtUp(store, { filter: { includeRecords: true } });
  const fits = store.appendJson('fits', { s: 'x'.repeat(4096 - 8) });
  const large = store.appendJson('large', { s: 'x'.repeat(4096 - 7) });
  const raw = store.append('raw', Buffer.from('not json'));
  assert.equal(fits.payload.length, 4096);
  assert.equal(large.payload.length, 4097);
  const records = drain(store, id).filter((event) => event.eventType === 'record').map((event) => event.record);
  const expected = (record, payload) => ({
    id: Number(record.id),
    sequence: record.sequence,
    branch: Number(store.currentBranch().id),
    record_type: record.recordType,
    timestamp: record.timestamp,
    payload_size: record.payload.length,
    payload,
  });
  const want = [expected(fits, { s: 'x'.repeat(4096 - 8) }), expected(large, null), expected(raw, null)];
  assert.deepEqual(records, want);
  const replayed = drain(store, caughtUp(store, { fromSequence: fits.sequence, filter: { includeRecords: true } }));
  assert.deepEqual(replayed.filter((event) => event.eventType === 'record').map((event) => event.record), want);
  store.close();
});

test('T23 A14 a state_delta event has the state, the sequence, and the operation in the form that getStateUpdateJson gives', () => {
  const { store } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log' });
  const id = caughtUp(store, { filter: { includeStateChanges: true } });
  const record = store.appendToStateJson('list', { n: 1 });
  const [delta] = drain(store, id).filter((event) => event.eventType === 'state_delta');
  assert.deepEqual(delta, {
    eventType: 'state_delta',
    type: 'state_delta',
    state_id: 'list',
    operation: JSON.parse(store.getStateUpdateJson(record.id)).operation,
    sequence: record.sequence,
  });
  store.close();
});

test('A64 unsubscribe through the handle that made the subscription takes the waiting events with it, and a later poll throws SubscriptionNotFound', () => {
  const { store } = freshStore();
  const view = store.view('main');
  for (const owner of [store, view]) {
    const id = caughtUp(owner, { filter: { includeRecords: true } });
    const twin = caughtUp(owner, { filter: { includeRecords: true } });
    store.appendJson('note', {});
    assert.deepEqual(drain(owner, twin).map((event) => event.eventType), ['caught_up', 'record']);
    owner.unsubscribe(id);
    refuses(() => owner.pollSubscription(id), ErrorCode.SubscriptionNotFound, 'Subscription not found');
    owner.unsubscribe(twin);
  }
  store.close();
});

test('A64 unsubscribe does not throw for an ID that names no subscription', () => {
  const { store } = freshStore();
  const id = store.subscribe();
  store.unsubscribe(id);
  store.unsubscribe(id);
  store.unsubscribe('4242');
  assert.equal(store.subscriptionCount(), 0);
  store.close();
});

test('A66 a poll through another live handle of the store throws SubscriptionNotFound', () => {
  const { store } = freshStore();
  const view = store.view('main');
  const id = caughtUp(store);
  refuses(() => view.pollSubscription(id), ErrorCode.SubscriptionNotFound, 'Subscription not found');
  refuses(() => view.pollSubscriptionTimeout(id, 1), ErrorCode.SubscriptionNotFound, 'Subscription not found');
  assert.equal(store.pollSubscription(id).eventType, 'caught_up');
  store.close();
});

test('A67 with no event waiting, pollSubscriptionTimeout holds the caller for timeoutMs and answers null, and with an event waiting it answers at once', () => {
  const { store } = freshStore();
  const id = store.subscribe();
  let started = performance.now();
  assert.equal(store.pollSubscriptionTimeout(id, 200), null);
  const held = performance.now() - started;
  assert.ok(held >= 190 && held < 30_000, String(held));
  store.catchUpSubscription(id);
  started = performance.now();
  assert.equal(store.pollSubscriptionTimeout(id, 60_000).eventType, 'caught_up');
  assert.ok(performance.now() - started < 30_000);
  store.close();
});
