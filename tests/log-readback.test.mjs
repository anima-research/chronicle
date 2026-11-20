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
  // pins setStateJson.6 — the supplied row replaces even the old prefix, not just the excess tail
  assert.deepEqual(store.getStateJson(context), [{ id: 'kept' }]);
  store.appendToStateJson(context, { id: 'added' });
  store.editStateItem(context, 0, jsonBytes({ id: 'edited' }));
  // pins setStateJson.6 — the shorter replacement, not the old list, is the base for appends and edits
  assert.deepEqual(store.getStateJson(context), [{ id: 'edited' }, { id: 'added' }]);
  store.setStateJson(context, []);
  // pins setStateJson.6 — an empty array set reads back as [], not null or unparsable bytes
  assert.deepEqual(store.getStateJson(context), []);

  for (const id of ['again-a', 'again-b', 'again-c']) store.appendToStateJson(context, { id });
  const end = store.getStateLen(context);
  // pins getStateLen.8 — the measured count is the end of the populated range
  assert.equal(end, 3);
  store.redactStateItems(context, 0, end);
  // pins redactStateItems.9 — even before another write the cleared append log remains registered
  assert.equal(store.listStates().find(({ id }) => id === context)?.strategy, 'append_log');
  // pins redactStateItems.9 — clearing without replacing leaves no entries
  assert.equal(store.getStateLen(context), 0);
  store.appendToStateJson(context, { id: 'new-zero' });
  // pins redactStateItems.9 — no re-registration is needed and the next item occupies zero
  assert.deepEqual(store.getStateItemJson(context, 0), { id: 'new-zero' });

  // Save the count before the checkpoint: clamping an inclusive end at the
  // old last item would look right on a clear, but here it would eat the checkpoint.
  const events = Array.from({ length: 256 }, (_, i) => ({ kind: 'clear', key: `chunk-${i}`, targetClaimId: String(i) }));
  store.appendManyToStateJson(ledger, events);
  const saved = store.getStateLen(ledger);
  // pins getStateLen.8 — the count at the caller's checkpoint threshold counts events, not writes
  assert.equal(saved, 256);
  const checkpoint = { kind: 'checkpoint', key: '__checkpoint__', active: [] };
  store.appendToStateJson(ledger, checkpoint);
  store.redactStateItems(ledger, 0, saved);
  // pins getStateLen.8 — redacting to the saved exclusive end spares the checkpoint
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
  const removed = store.getStateJson(messages).findIndex(({ id }) => id === gone.id);
  store.redactStateItems(messages, removed, removed + 1);
  store = reopen();

  const persisted = store.getStateJson(messages);
  const positions = persisted.flatMap((row, i) => row.id === '91' ? [i] : []);
  // pins getStateJson.9 — positions in the full read have shifted past the removed item
  assert.deepEqual(positions, [1, 3]);
  const [a, b] = positions;
  const neighborBytes = () => [0, 2, 4].map((i) => Buffer.from(store.getStateSlice(messages, i, 1)));
  const before = neighborBytes();
  const replacement = { id: '91', sequence: 503, content: [{ type: 'text', text: 'backfilled' }] };
  const bytes = jsonBytes(replacement);
  const originalBytes = Buffer.from(bytes);
  const editA = store.editStateItem(messages, a, bytes);
  // pins open.16 — the reopened handle edits the slot found in its read, leaving the distinguishable other copy alone
  assert.deepEqual(store.getStateJson(messages), [left, replacement, middle, second, right]);
  // pins editStateItem.11 — the same serialized bytes are available for the next edit
  assert.deepEqual(bytes, originalBytes);
  const editB = store.editStateItem(messages, b, bytes);
  // pins editStateItem.11 — passing the same Buffer twice does not consume or alter it
  assert.deepEqual(bytes, originalBytes);
  // pins editStateItem.1 — both independently addressed slots now hold the supplied item, not just the first ID match
  assert.deepEqual(positions.map((i) => store.getStateItemJson(messages, i)), [replacement, replacement]);
  const expected = [left, replacement, middle, replacement, right];
  // pins getStateJson.9 — editing the two positions taken from the full read moves or replaces no neighbor
  assert.deepEqual(store.getStateJson(messages), expected);
  // pins editStateItem.13 — even the bytes read at the unrelated positions have not changed
  assert.deepEqual(neighborBytes(), before);

  store = reopen();
  // pins editStateItem.13 — both persisted updates decode as per-slot Edits, not Sets
  assert.deepEqual([editA, editB].map(({ id }) => JSON.parse(store.getStateUpdateJson(id)).operation.Edit?.index), positions);
  // pins editStateItem.13 — replay keeps the earlier redaction; it does not bring '11' back
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
  // pins getStateJson.8 — full reads distinguish an inline request from its neighbor's unexpanded {blobId}
  assert.deepEqual(requests, [inlineRequest, referencedRequest]);
  // pins getStateJson.8 — the caller can fetch exactly the bytes named in that full read
  assert.deepEqual(store.getBlob(requests[1].request.blobId), requestBytes);

  // Keep the expected message independent of any read or mutation of the JS input.
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
  // pins appendToStateJsonWithIdentity.3 — all JSON fields of the message survive alongside the actual record stamps
  assert.deepEqual(store.getStateJson(messages), [...prefix, message, later]);
  // pins getStateJsonAt.6 — the cut at the append includes the complete stamped message, but not the later one
  assert.deepEqual(store.getStateJsonAt(messages, message.sequence), [...prefix, message]);
  // pins getStateItemJson.9 — the message point lookup keeps metadata and unresolved content alongside the stamps
  assert.deepEqual(store.getStateItemJson(messages, 2), message);
  const slice = store.getStateSlice(messages, 2, 1);
  // pins getStateSlice.2 — the positional call returns a Buffer immediately, not a Promise, object, or string
  assert.ok(Buffer.isBuffer(slice));
  const one = utf8Json(slice);
  // pins getStateSlice.2 — the UTF-8 document for limit 1 is an array containing exactly one item
  assert.ok(Array.isArray(one) && one.length === 1);
  // pins getStateSlice.7 — the sliced message carries its append-time identity, original shard fields and reference
  assert.deepEqual(one[0], message);

  const inlineEntry = { index: 0, participant: 'reader', content: [text] };
  const refEntry = { index: 1, participant: 'reader', content: [text, media], sourceMessageId: message.id, sourceRelation: 'copy', cacheMarker: true };
  for (const entry of [inlineEntry, refEntry]) store.appendToStateJson(context, structuredClone(entry));
  // pins appendToStateJson.7 — plain appends retain inline JSON and the distinct caller-provided blob_ref and blobId forms
  assert.deepEqual([store.getStateJson(context), store.getStateItemJson(inference, 1)], [[inlineEntry, refEntry], referencedRequest]);
  const entry = store.getStateItemJson(context, 1);
  // pins getStateItemJson.9 — context provenance and cache fields round-trip together with the original block
  assert.deepEqual(entry, refEntry);
  // pins appendToStateJson.7 — a separately fetched block still identifies the precise document appended by reference
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
  // pins appendToStateJsonWithIdentity.3 — stamping a checkpoint preserves its nested records, outcomes and distinct nested eventId
  assert.deepEqual(store.getStateJson(ledger), [{ ...checkpoint, eventId: event.id, sequence: event.sequence }]);
});

test('Suffix reads preserve the boundary, even with a bad window item or an enormous limit', (t) => {
  const { store } = freshStore(t);
  const window = 'workshop/window';
  registerLogs(store, window);
  const limit = 2 ** 31 - 1;
  // pins getStateSlice.9 — a registered slot with no written value yields null
  assert.equal(store.getStateSlice(window, 0, limit), null);
  store.appendToStateJson(window, { v: 1 });
  const record = store.appendToStateJsonWithIdentity(window, { id: 'row-0' }, 'by', 'at');
  const boundary = { id: 'row-0', by: record.id, at: record.sequence };
  const last = store.getStateLen(window) - 1;
  const revision = store.stateHeadOffset(window);
  store.compactState(window);
  assert.notEqual(store.stateHeadOffset(window), revision, 'exercise a snapshot-only revision');
  const boundaryOnly = store.getStateSlice(window, last, limit);
  // pins getStateSlice.9 — null cannot substitute for a known nonempty window
  assert.notEqual(boundaryOnly, null);
  // pins getStateSlice.6 — the previous final item is the whole suffix when only a snapshot has happened
  assert.deepEqual(utf8Json(boundaryOnly), [boundary]);

  const additions = Array.from({ length: 2049 }, (_, i) => ({ id: `added-${i}` }));
  const badIndex = 1024;
  additions[badIndex] = { unknownPresentation: 'future format' }; // valid JSON, not a presentation, retract or header
  const batch = store.appendManyToStateJsonWithIdentity(window, structuredClone(additions), 'by', 'at');
  const suffix = additions.map((row) => ({ ...row, by: batch.record.id, at: batch.record.sequence }));
  const read = utf8Json(store.getStateSlice(window, last, limit));
  // pins getStateSlice.10 — a schema-invalid item in otherwise valid JSON is visible, never silently filtered out
  assert.deepEqual(read[1 + badIndex], suffix[badIndex]);
  // pins getStateSlice.6 — at the former last ordinal this huge limit returns the boundary plus every addition, with no cap or padding
  assert.deepEqual(read, [boundary, ...suffix]);

  store.appendToState(window, Buffer.from('not JSON'));
  store.appendToStateJson(window, { id: 'after-corrupt-item' });
  // pins getStateSlice.9 — unreconstructable bytes must cause a read or parse failure, not appear as a no-value null or a partial window
  assert.throws(() => {
    const bytes = store.getStateSlice(window, last, limit);
    if (bytes) utf8Json(bytes); // a null would take MessageStore's successful empty path and not throw
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
  // pins getStateTail.7 — a registered never-written log succeeds as null or an encoded empty array
  assert.ok(empty === null || Buffer.isBuffer(empty));
  // pins getStateTail.7 — if encoded, the empty history really decodes to []
  assert.deepEqual(empty === null ? [] : utf8Json(empty), []);
  const records = Array.from({ length: 261 }, (_, i) => endedRecord(i));
  store.appendToStateJson(ended, structuredClone(records[0]));
  const one = utf8Json(store.getStateTail(ended, 256));
  // pins getStateTail.5 — a one-record history is an array of one, not a record or a recovery wrapper
  assert.ok(Array.isArray(one) && one.length === 1);
  // pins getStateTail.6 — the ended marker, name, incarnation and all other fields round-trip
  assert.deepEqual(one[0], records[0]);
  // pins getStateItemJson.13 — the indexed record carries the exact name, incarnation, end time, namespace and inheritance
  assert.deepEqual(store.getStateItemJson(ended, 0), records[0]);
  for (const row of records.slice(1)) store.appendToStateJson(ended, structuredClone(row));
  // pins getStateTail.2 — 256 counts complete records and selects precisely the newest 256 in order
  assert.deepEqual(utf8Json(store.getStateTail(ended, 256)), records.slice(-256));
});
