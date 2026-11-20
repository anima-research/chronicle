import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { JsStore } from '../index.js';

function freshStore(t) {
  const path = mkdtempSync(join(tmpdir(), 'chronicle-log-readback-'));
  let store;
  t.after(() => {
    try {
      store?.close();
    } finally {
      rmSync(path, { recursive: true, force: true });
    }
  });
  store = JsStore.openOrCreate({ path });
  return {
    store,
    reopen() {
      store.close();
      store = JsStore.open({ path });
      return store;
    },
  };
}

function registerLogs(store, ...ids) {
  for (const id of ids) store.registerState({ id, strategy: 'append_log' });
}

const context = 'workshop/context';
const messages = 'workshop/messages';
const ledger = 'workshop/autobio:compression-refusal-quarantine-events';
const utf8Json = (bytes) => JSON.parse(bytes.toString('utf8'));
const jsonBytes = (value) => Buffer.from(JSON.stringify(value), 'utf8');

test('Replacing or redacting a log down to zero is not an absent registration', (t) => {
  const { store } = freshStore(t);
  registerLogs(store, context, ledger);
  for (const id of ['old-a', 'old-b', 'old-c']) store.appendToStateJson(context, { id });
  store.setStateJson(context, [{ id: 'kept' }]);
  assert.deepEqual(store.getStateJson(context), [{ id: 'kept' }]);
  store.appendToStateJson(context, { id: 'added' });
  store.editStateItem(context, 0, jsonBytes({ id: 'edited' }));
  assert.deepEqual(store.getStateJson(context), [{ id: 'edited' }, { id: 'added' }]);
  store.setStateJson(context, []);
  assert.deepEqual(store.getStateJson(context), []);

  for (const id of ['again-a', 'again-b', 'again-c']) store.appendToStateJson(context, { id });
  const end = store.getStateLen(context);
  assert.equal(end, 3);
  store.redactStateItems(context, 0, end);
  assert.equal(store.listStates().find(({ id }) => id === context)?.strategy, 'append_log');
  assert.equal(store.getStateLen(context), 0);
  store.appendToStateJson(context, { id: 'new-zero' });
  assert.deepEqual(store.getStateItemJson(context, 0), { id: 'new-zero' });

  const events = Array.from({ length: 256 }, (_, i) => ({ kind: 'clear', key: `chunk-${i}`, targetClaimId: String(i) }));
  store.appendManyToStateJson(ledger, events);
  const saved = store.getStateLen(ledger);
  assert.equal(saved, 256);
  const checkpoint = { kind: 'checkpoint', key: '__checkpoint__', active: [] };
  store.appendToStateJson(ledger, checkpoint);
  store.redactStateItems(ledger, 0, saved);
  assert.deepEqual(store.getStateJson(ledger), [checkpoint]);
});

test('A stopped log can be backfilled by its persisted duplicate positions', (t) => {
  let { store, reopen } = freshStore(t);
  registerLogs(store, messages);
  const left = { id: '10', metadata: { z: 0, a: 1 } };
  const gone = { id: '11', content: 'redacted' };
  const first = { id: '91', sequence: 503, content: 'earlier copy' };
  const middle = { id: '12', metadata: { y: ['middle'], b: 2 } };
  const second = { id: '91', sequence: 509, content: 'later copy' };
  const right = { id: '13', metadata: { c: 3, a: 'right' } };
  for (const row of [left, gone, first, middle, second, right]) store.appendToStateJson(messages, structuredClone(row));
  const spacedBytes = Buffer.from('{ "id": "14",  "metadata": { "d": [4] } }', 'utf8');
  const spaced = utf8Json(spacedBytes);
  store.appendToState(messages, spacedBytes);
  const removed = store.getStateJson(messages).findIndex(({ id }) => id === gone.id);
  store.redactStateItems(messages, removed, removed + 1);
  store = reopen();

  const persisted = store.getStateJson(messages);
  const positions = persisted.flatMap((row, i) => row.id === '91' ? [i] : []);
  assert.deepEqual(positions, [1, 3]);
  const [a, b] = positions;
  const neighborBytes = () => [0, 2, 4, 5].map((i) => Buffer.from(store.getStateSlice(messages, i, 1)));
  const before = neighborBytes();
  const updateIds = () => store.getRecordIdsByType('state_update');
  const earlier = updateIds();
  const records = () => earlier.map((id) => store.getStateUpdateJson(id));
  const originalRecords = records();
  assert.ok(originalRecords.some((json) => json.includes(JSON.stringify([...spacedBytes]))));
  const replacement = { id: '91', sequence: 503, content: [{ type: 'text', text: 'backfilled' }] };
  const bytes = jsonBytes(replacement);
  const originalBytes = Buffer.from(bytes);
  const editA = store.editStateItem(messages, a, bytes);
  assert.deepEqual(store.getStateJson(messages), [left, replacement, middle, second, right, spaced]);
  assert.deepEqual(bytes, originalBytes);
  const editB = store.editStateItem(messages, b, bytes);
  assert.deepEqual(bytes, originalBytes);
  assert.deepEqual(positions.map((i) => store.getStateItemJson(messages, i)), [replacement, replacement]);
  const expected = [left, replacement, middle, replacement, right, spaced];
  assert.deepEqual(store.getStateJson(messages), expected);
  assert.deepEqual(neighborBytes(), before);

  store = reopen();
  const now = updateIds();
  assert.deepEqual(now.slice(0, earlier.length), earlier);
  const added = now.slice(earlier.length).map((id) => ({ id, ...JSON.parse(store.getStateUpdateJson(id)) }));
  const written = added.filter(({ operation }) => !('Snapshot' in operation || 'DeltaSnapshot' in operation));
  assert.deepEqual(written.map(({ id, state_id, operation }) => [id, state_id, Object.keys(operation), operation.Edit?.index]),
    [[editA.id, messages, ['Edit'], a], [editB.id, messages, ['Edit'], b]]);
  assert.deepEqual(records(), originalRecords);
  assert.deepEqual(store.getStateJson(messages), expected);
});

test('Message and context read paths preserve inline and referenced content as stored', (t) => {
  const { store } = freshStore(t);
  const inference = 'framework/inference-log';
  registerLogs(store, context, messages, inference, ledger);
  const document = Buffer.from('A document kept outside the log.\n', 'utf8');
  const hash = store.storeBlob(document, 'text/plain');
  const media = { type: 'blob_ref', ref: { hash, originalType: 'document', mediaType: 'text/plain' } };
  const text = { type: 'text', text: 'Notes for Zoë — 第三页' };
  const timestamp = 1746523650123;
  const requestBytes = jsonBytes({ prompt: ['one', 'two'], opts: { echo: false } });
  const blobId = store.storeBlob(requestBytes, 'application/json');
  const inlineRequest = { timestamp, agentName: 'reader', requestId: 'q0', success: true, durationMs: 7, request: { prompt: ['inline'] } };
  const referencedRequest = { ...inlineRequest, requestId: 'q1', request: { blobId } };
  for (const item of [inlineRequest, referencedRequest]) store.appendToStateJson(inference, structuredClone(item));
  const requests = store.getStateJson(inference);
  assert.deepEqual(requests, [inlineRequest, referencedRequest]);
  assert.deepEqual(store.getBlob(requests[1].request.blobId), requestBytes);

  function appendMessage(payload) {
    const record = store.appendToStateJsonWithIdentity(messages, structuredClone(payload), 'id', 'sequence');
    return { ...payload, id: record.id, sequence: record.sequence };
  }
  const prefix = ['earlier', 'question'].map((s) => appendMessage({ participant: 'editor', timestamp: timestamp - 1, content: [{ type: 'text', text: s }] }));
  const message = appendMessage({
    participant: 'reader', metadata: { channel: 'archive', labels: ['source', 'attachment'] },
    timestamp, causedBy: [prefix[1].id], content: [text, media],
    bodyGroupId: 'body-a', shardIndex: 1, shardCount: 3, currentResolution: 2, lockedByAgent: true,
  });
  const later = appendMessage({ participant: 'editor', timestamp: timestamp + 1, content: [text] });
  assert.deepEqual(store.getStateJson(messages), [...prefix, message, later]);
  assert.deepEqual(store.getStateJsonAt(messages, message.sequence), [...prefix, message]);
  assert.deepEqual(store.getStateItemJson(messages, 2), message);
  const slice = store.getStateSlice(messages, 2, 1);
  assert.ok(Buffer.isBuffer(slice));
  const one = utf8Json(slice);
  assert.ok(Array.isArray(one) && one.length === 1);
  assert.deepEqual(one[0], message);

  const inlineEntry = { index: 0, participant: 'reader', content: [text] };
  const refEntry = { index: 1, participant: 'reader', content: [text, media], sourceMessageId: message.id, sourceRelation: 'copy', cacheMarker: true };
  for (const entry of [inlineEntry, refEntry]) store.appendToStateJson(context, structuredClone(entry));
  assert.deepEqual([store.getStateJson(context), store.getStateItemJson(inference, 1)], [[inlineEntry, refEntry], referencedRequest]);
  const entry = store.getStateItemJson(context, 1);
  assert.deepEqual(entry, refEntry);
  assert.deepEqual(store.getBlob(entry.content[1].ref.hash), document);

  const checkpoint = {
    kind: 'checkpoint', key: '__checkpoint__', created: timestamp,
    active: [{
      generationId: '83', record: { key: 'chunk-a', chunkSourceHash: 'hash-a', sourceIds: [message.id] },
      outcomes: [{ curveLabel: 'canonical', requestHash: 'request-a', outcome: 'provider_error' }],
      pendingAlert: { eventId: '89', alertKey: 'review' }, sentAlertKeys: ['notice'],
    }],
  };
  const event = store.appendToStateJsonWithIdentity(ledger, structuredClone(checkpoint), 'eventId', 'sequence');
  assert.deepEqual(store.getStateJson(ledger), [{ ...checkpoint, eventId: event.id, sequence: event.sequence }]);
});

test('Suffix reads preserve the boundary, even with a bad window item or an enormous limit', (t) => {
  const { store } = freshStore(t);
  const window = 'workshop/window';
  registerLogs(store, window);
  const limit = 2 ** 31 - 1;
  const empty = store.getStateSlice(window, 0, limit);
  assert.ok(empty === null || Buffer.isBuffer(empty));
  assert.deepEqual(empty === null ? [] : utf8Json(empty), []);
  store.appendToStateJson(window, { v: 1 });
  const record = store.appendToStateJsonWithIdentity(window, { id: 'row-0' }, 'by', 'at');
  const boundary = { id: 'row-0', by: record.id, at: record.sequence };
  const last = store.getStateLen(window) - 1;
  const revision = store.stateHeadOffset(window);
  store.compactState(window);
  assert.notEqual(store.stateHeadOffset(window), revision, 'exercise a snapshot-only revision');
  const boundaryOnly = store.getStateSlice(window, last, limit);
  assert.notEqual(boundaryOnly, null);
  assert.deepEqual(utf8Json(boundaryOnly), [boundary]);

  const additions = Array.from({ length: 2049 }, (_, i) => ({ id: `added-${i}` }));
  const badIndex = 1024;
  additions[badIndex] = { unknownPresentation: 'future format' };
  const batch = store.appendManyToStateJsonWithIdentity(window, structuredClone(additions), 'by', 'at');
  const suffix = additions.map((row) => ({ ...row, by: batch.record.id, at: batch.record.sequence }));
  const read = utf8Json(store.getStateSlice(window, last, limit));
  assert.deepEqual(read[1 + badIndex], suffix[badIndex]);
  assert.deepEqual(read, [boundary, ...suffix]);

  store.appendToState(window, Buffer.from('not JSON'));
  store.appendToStateJson(window, { id: 'after-corrupt-item' });
  assert.throws(() => {
    const bytes = store.getStateSlice(window, last, limit);
    if (bytes) utf8Json(bytes);
  });
});

function endedRecord(n) {
  const createdAt = 1746523650123 + n * 100;
  return {
    name: `task-${n}`, kind: 'task-fork', incarnation: n + 1,
    namespace: `jobs/task-${n}`, inherit: { from: 'planner', mode: 'shared', ownBranch: `job-${n}` },
    roles: { defaultDelivery: false, receivesUntargeted: false, receivesGateWakes: false, ownsProviderScheduling: false },
    lifetime: { kind: 'task', deadlineMs: 60_000 }, onParentEnd: 'orphan',
    relationships: { spawnedBy: 'planner', observes: [], messagePeers: ['planner'] }, readsSharedSlot: false,
    createdAt, ended: { at: createdAt + 30, reason: 'completed', by: 'planner' },
  };
}

test('The ended log returns a bounded suffix of complete AgentRecords', (t) => {
  const { store } = freshStore(t);
  const ended = 'framework/dendrite/ended';
  registerLogs(store, ended);
  const empty = store.getStateTail(ended, 256);
  assert.ok(empty === null || Buffer.isBuffer(empty));
  assert.deepEqual(empty === null ? [] : utf8Json(empty), []);
  const records = Array.from({ length: 261 }, (_, i) => endedRecord(i));
  store.appendToStateJson(ended, structuredClone(records[0]));
  const one = utf8Json(store.getStateTail(ended, 256));
  assert.ok(Array.isArray(one) && one.length === 1);
  assert.deepEqual(one[0], records[0]);
  assert.deepEqual(store.getStateItemJson(ended, 0), records[0]);
  for (const row of records.slice(1)) store.appendToStateJson(ended, structuredClone(row));
  assert.deepEqual(utf8Json(store.getStateTail(ended, 256)), records.slice(-256));
});

test('The ended log is read from the branch that the root has selected', (t) => {
  const fixture = freshStore(t);
  let store = fixture.store;
  const ended = 'framework/dendrite/ended';
  registerLogs(store, ended);
  const shared = endedRecord(0);
  store.appendToStateJson(ended, structuredClone(shared));
  const fork = store.createBranch('fork');
  const onMain = endedRecord(1);
  const onFork = endedRecord(2);
  store.appendToStateJson(ended, structuredClone(onMain));
  store.view(fork.name).appendToStateJson(ended, structuredClone(onFork));
  const tails = (handle) => {
    const read = {};
    for (const name of ['main', fork.name, 'main']) {
      handle.switchBranch(name);
      read[name] = utf8Json(handle.getStateTail(ended, 256));
    }
    return read;
  };
  const expected = { main: [shared, onMain], [fork.name]: [shared, onFork] };
  assert.deepEqual(tails(store), expected);
  store = fixture.reopen();
  assert.deepEqual(tails(store), expected);
});
