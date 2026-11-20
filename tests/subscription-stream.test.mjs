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

// Bound the small-fixture drains: a regression to peeking must not spin forever.
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
  // Check before *any* in-process drain so a blocking regression cannot hang
  // the rest of the file. A native receive would block node:test's timer. Run
  // the empty-poll probe out of process. Repetition distinguishes even a 1 ms
  // receive timeout from try-receive without imposing sub-ms timing on CI.
  const dir = mkdtempSync(join(tmpdir(), 'chronicle-poll-probe-'));
  const code = `
    import assert from 'node:assert/strict';
    import { JsStore } from ${JSON.stringify(new URL('../index.js', import.meta.url).href)};
    const store = JsStore.openOrCreate({ path: process.argv[1] });
    try {
      const id = store.subscribe();
      store.catchUpSubscription(id);
      assert.equal(store.pollSubscription(id)?.eventType, 'caught_up');
      for (let i = 0; i < 40000; i++) assert.notEqual(typeof store.pollSubscription(id)?.then, 'function');
    } finally { store.close(); }
  `;
  try {
    const probe = spawnSync(process.execPath, ['--input-type=module', '-e', code, join(dir, 'store')], {
      timeout: 15000, killSignal: 'SIGKILL', encoding: 'utf8',
    });
    // pins pollSubscription.2: empty active polls neither return promises nor wait on a receive timeout.
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
  // pins subscribe.9: the unawaited return values themselves are nonempty strings.
  assert.ok(ids.every(id => typeof id === 'string' && id.length > 0));
  // pins subscribe.10: concurrent subscriptions on this very handle have distinct IDs.
  assert.equal(new Set(ids).size, ids.length);
  for (const id of ids) store.catchUpSubscription(id);

  const queues = new Map([[a, []], [b, []]]);
  for (let i = 0; i < 4; i++) {
    for (const id of i % 2 === 0 ? [a, b] : [b, a]) queues.get(id).push(store.pollSubscription(id));
  }
  // pins pollSubscription.2: use each queued event immediately, without awaiting a promise.
  assert.ok([...queues.get(a), ...queues.get(b)].every(event => event?.eventType === 'record'));
  const aIds = queues.get(a).map(recordId), bIds = queues.get(b).map(recordId);
  // pins pollSubscription.1: reversing the poll order does not swap queues or consume the other's records.
  assert.deepEqual([aIds, bIds], [idsOf(records.filter(r => r.recordType !== 'flag')), idsOf(records.filter(r => r.recordType === 'flag'))]);

  const unrestricted = records.map(() => store.pollSubscription(all));
  // pins pollSubscription.4: each poll advances by exactly one record, in append order, before completion.
  assert.deepEqual([...unrestricted.map(recordId), store.pollSubscription(all)?.eventType], [...idsOf(records), 'caught_up']);
  const recordType = event => JSON.parse(event.data).record.record_type;
  // pins subscribe.5: two named types exclude the third; explicit undefined admits all three.
  assert.deepEqual([queues.get(a).map(recordType), unrestricted.map(recordType)], [types.filter(type => type !== 'flag'), types]);

  for (const id of [a, b]) assert.equal(store.pollSubscription(id)?.eventType, 'caught_up');
  // pins pollSubscription.3: an exhausted active queue gives strictly null without ending the subscription.
  assert.deepEqual(ids.map(id => store.pollSubscription(id)), [null, null, null]);
  const note = store.appendJson('note', 'later'), flag = store.appendJson('flag', 'later');
  // pins pollSubscription.3: the same IDs resume on later writes, without another catch-up.
  assert.deepEqual(ids.map(id => drain(store, id).map(recordId)), [[note.id], [flag.id], [note.id, flag.id]]);

  store.unsubscribe(a);
  const next = store.appendJson('flag', 'after cancellation');
  // pins unsubscribe.1: cancelling just the returned ID removes it while both other streams still deliver.
  assert.deepEqual([store.subscriptionCount(), ...[b, all].map(id => recordId(store.pollSubscription(id)))], [2, next.id, next.id]);
});

// Replay on record/state streams, live-only on the branch stream. Each
// subscription sees the same writes, with exactly one inclusion flag enabled.
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
  // Cancel through another handle so the owner retains the termination event.
  const other = store.view(main.name);
  other.unsubscribe(subscriptions[2]);
  other.close();
  const dropped = store.pollSubscription(subscriptions[2]);

  const events = [...replay.flat(), ...live.flat(), dropped];
  // pins pollSubscription.5: every event name, including all three branch names, is bare and spelled for store:<eventType>.
  assert.deepEqual(new Set(typesOf(events)), new Set(['record', 'state_snapshot', 'state_delta', 'branch_head', 'branch_created', 'branch_deleted', 'caught_up', 'dropped']));
  for (const event of events) {
    // pins pollSubscription.6: each variant supplies text, not merely a value JSON.parse can coerce.
    assert.equal(typeof event.data, 'string', event.eventType);
    assert.doesNotThrow(() => JSON.parse(event.data), event.eventType);
  }
  // pins subscribe.8: each subscription includes its requested class and excludes the other two.
  assert.deepEqual([replay.map(typesOf), live.map(group => new Set(typesOf(group)))], [
    [['record', 'caught_up'], ['state_snapshot', 'caught_up'], ['caught_up']],
    [new Set(['record']), new Set(['state_delta']), new Set(['branch_created', 'branch_head', 'branch_deleted'])],
  ]);
  // pins catchUpSubscription.5: even the live-only subscription supplies an unprefixed, parseable completion marker.
  assert.equal(replay[2][0]?.eventType, 'caught_up');
  assert.doesNotThrow(() => JSON.parse(replay[2][0].data));
  const deletion = live[2].find(event => event.eventType === 'branch_deleted');
  // pins deleteBranch.8: successful deletion delivers branch_deleted with the deleted name to the matching stream.
  assert.deepEqual([deletion?.eventType, JSON.parse(deletion.data).name], ['branch_deleted', side.name]);

  const branchData = type => JSON.parse(live[2].find(e => e.eventType === type).data);
  const { branch, head } = branchData('branch_head');
  const created = branchData('branch_created').branch;
  // pins pollSubscription.7: branch and dropped payload fields live directly in the decoded API object.
  assert.deepEqual([branch, head, created.id, created.name, created.head, created.created,
    branchData('branch_deleted').name, JSON.parse(dropped.data).reason],
  [main.name, later.sequence, Number(side.id), side.name, side.head, side.created, side.name, 'unsubscribed']);
  for (const [i, id] of subscriptions.entries()) {
    const events = [...replay[i], ...live[i]];
    if (i === 2) events.push(dropped);
    for (const event of events) {
      // pins pollSubscription.7: spreading data after the server-supplied ID cannot replace it with another ID.
      assert.equal({ subscriptionId: id, ...JSON.parse(event.data) }.subscriptionId, id);
    }
  }
  const summaries = [replay[0][0], live[0][0]].map(e => JSON.parse(e.data).record);
  const snap = JSON.parse(replay[1][0].data), change = JSON.parse(live[1][0].data);
  // pins pollSubscription.7: the store's JSON has a top-level record summary and snake_case record and state fields.
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

test('An explicit branch name selects records rather than behaving like no selector', {
  todo: 'The live record filter ignores the branch name: a subscription to work receives records written to both main and work.',
}, t => {
  const store = freshStore(t);
  store.createBranch('work');
  const base = { includeRecords: true };
  const named = startLive(store, { ...base, branch: 'work' });
  const unspecified = startLive(store, { ...base, branch: undefined });
  const omitted = startLive(store, base);
  const main = store.appendJson('note', 'on main');
  const work = store.view('work');
  const onWork = work.appendJson('note', 'on work');
  work.close();
  const control = drain(store, omitted).map(recordId);
  // pins subscribe.6: explicit undefined behaves like omitting the selector (and includes the current branch).
  assert.ok(control.includes(main.id));
  assert.deepEqual(drain(store, unspecified).map(recordId), control);
  // pins subscribe.6: a non-current branch selected by name gets its write and excludes the current branch's.
  assert.deepEqual(drain(store, named).map(recordId), [onWork.id]);
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
  const limit = jsonBytes(fits); // 14 UTF-8 bytes, though JS string length is much smaller.
  setSnapshot(store, 'fits', fits);
  setSnapshot(store, 'over', tooBig);
  setSnapshot(store, 'unselected', 'keep out');
  const defaultLimit = 10 * 1024 * 1024;
  const atDefault = 'x'.repeat(defaultLimit - 2); // the two JSON string quotes count
  setSnapshot(store, 'at-default', atDefault);
  setSnapshot(store, 'over-default', atDefault + 'x');

  const selectedIds = ['fits', 'over'];
  // Both configs are active before either catch-up: replay cannot simply use
  // the filter and limit from the last subscribe call on the handle.
  const smallId = watchSnapshots(store, { maxSnapshotBytes: limit, filter: { includeStateChanges: true, stateIds: selectedIds } });
  const defaultId = watchSnapshots(store, { filter: { includeStateChanges: true, stateIds: undefined } });
  const selected = replaySnapshots(store, smallId);
  // pins subscribe.7: stateIds includes both listed states but excludes a written third state.
  assert.deepEqual(selected.map(snapshotId).sort(), selectedIds);
  const small = selected.find(s => snapshotId(s) === 'fits'), over = selected.find(s => snapshotId(s) === 'over');
  // pins subscribe.3: an at-limit snapshot stays intact; byte length, not JS character count, limits the next one.
  assert.deepEqual([small.truncated, small.data, over.truncated, jsonBytes(over.data) <= limit], [false, fits, true, true]);
  // pins catchUpSubscription.3: catch-up itself uses the selection and byte cap configured only at subscribe time.
  assert.deepEqual(selected.map(s => [snapshotId(s), s.truncated, jsonBytes(s.data) <= limit]).sort(), [['fits', false, true], ['over', true, true]]);

  const all = replaySnapshots(store, defaultId);
  // pins subscribe.7: explicit undefined selects all written states, including the one previously excluded.
  assert.deepEqual(all.map(snapshotId).sort(), ['at-default', 'fits', 'over', 'over-default', 'unselected']);
  const at = all.find(s => snapshotId(s) === 'at-default'), above = all.find(s => snapshotId(s) === 'over-default');
  // pins subscribe.3: omitting maxSnapshotBytes preserves all 10 MiB but limits a value one byte larger.
  assert.deepEqual([at.truncated, at.data === atDefault, above.truncated, jsonBytes(above.data) <= defaultLimit], [false, true, true, true]);
});

test('The snapshot byte cap includes the delimiters of a replayed JSON array', {
  todo: 'Array truncation budgets the elements but not the brackets and commas in the delivered snapshot.',
}, t => {
  const store = freshStore(t);
  setSnapshot(store, 'items', ['é', 'é']); // 8 bytes of elements, 11 bytes of array
  const limit = 9;
  const id = watchSnapshots(store, { maxSnapshotBytes: limit, filter: { includeStateChanges: true, stateIds: ['items'] } });
  const [snapshot] = replaySnapshots(store, id);
  // pins subscribe.3: marking an array truncated isn't enough if its delivered data still exceeds the byte cap.
  assert.equal(snapshot.truncated, true);
  assert.ok(jsonBytes(snapshot.data) <= limit, `${jsonBytes(snapshot.data)} snapshot bytes exceed the limit of ${limit}`);
});

test('The default queue holds a thousand events and cleanup also removes overflowed IDs', t => {
  const store = freshStore(t);
  const filter = { includeRecords: true, includeStateChanges: false, includeBranchEvents: false };
  const drained = startLive(store, filter), untouched = startLive(store, filter);
  // Identical queues up to the limit. Drain one, keeping the other full.
  const ids = Array.from({ length: 1000 }, (_, i) => store.appendJson('note', i).id);
  // pins subscribe.2: without bufferSize all thousand events fit and neither subscription is dropped.
  assert.equal(store.subscriptionCount(), 2);
  assert.deepEqual(ids.map(() => recordId(store.pollSubscription(drained))), ids);
  assert.equal(store.pollSubscription(drained), null);
  const extra = store.appendJson('note', 1000);
  // pins subscribe.2: the 1001st event drops the undrained queue, not the drained one.
  assert.deepEqual([store.subscriptionCount(), recordId(store.pollSubscription(drained))], [1, extra.id]);

  const stillLive = store.appendJson('note', 1001);
  assert.equal(recordId(store.pollSubscription(drained)), stillLive.id);

  const another = startLive(store, filter);
  const tracked = new Set([untouched, drained, another]);
  // pins unsubscribe.3: cleanup through one handle can remove an overflowed ID and every still-live ID without throwing.
  assert.doesNotThrow(() => { for (const id of tracked) store.unsubscribe(id); });
  assert.equal(store.subscriptionCount(), 0);
  // pins unsubscribe.3: cleanup freed the overflowed ID too, not just the registrations still live in the store.
  assert.throws(() => store.pollSubscription(untouched), { code: ErrorCode.SubscriptionNotFound });
});
