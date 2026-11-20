import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { cpSync, mkdtempSync, renameSync, rmSync, statSync, truncateSync, writeFileSync, writeSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { JsStore } from '../index.js';

// The writer runs this same file as a script, reports caller-visible coordinates,
// and dies by signal. No close, graceful Node shutdown, or GC of the writer's
// handles can supply a second flush. This models process death, not power loss.
function runWriter(path, action, options = {}) {
  const child = spawnSync(process.execPath, [fileURLToPath(import.meta.url), '--crash-writer',
    path, action, JSON.stringify(options)], { encoding: 'utf8', timeout: 30_000 });
  if (child.error) throw child.error;
  assert.equal(child.signal, 'SIGKILL', `${action}: writer exited ${child.status}\n${child.stderr}`);
  return JSON.parse(child.stdout);
}

function setup(t, init) {
  const dir = mkdtempSync(join(tmpdir(), 'chronicle-boundary-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const path = join(dir, 'store');
  const root = JsStore.create({ path });
  try { init(root); } finally { root.close(); }
  return path;
}

function reopen(path, inspect) {
  const root = JsStore.open({ path });
  try { return inspect(root); } finally { root.close(); }
}

const branch = 'recovery-work';
const ledger = 'agents/wren/autobio:compression-refusal-quarantine-events';
const legacy = 'agents/wren/autobio:compression-refusal-quarantine';
const legacyRecord = { key: 'retired', chunkSourceHash: 'old-span' };
const mainEvents = [{ kind: 'clear', key: 'retired', targetClaimId: 'legacy:retired', created: 10 }];
const record = { key: 'bounded-family', chunkSourceHash: 'current-span', created: 11 };
const outcomes = [{ curveLabel: 'canonical', outcome: 'refusal', requestHash: 'shape-1' }];
const pendingEvent = (generationId) => ({
  kind: 'alert_pending', key: record.key, targetClaimId: generationId, alertKey: 'ops/pending', created: 14,
});
const checkpointEvent = ({ generationId, pendingId, empty }) => ({
  kind: 'checkpoint', key: '__checkpoint__', created: 16,
  active: empty ? [] : [{
    generationId, record, outcomes, pendingAlert: { eventId: pendingId, alertKey: 'ops/pending' },
    sentAlertKeys: ['ops/earlier'],
  }],
});

function appendEvent(view, event) {
  return view.appendToStateJsonWithIdentity(ledger, event, 'eventId', 'sequence');
}

function pendingWriter(_root, view) {
  const { id: generationId } = appendEvent(view, {
    kind: 'exhausted', key: record.key, record, outcomes, created: 11,
  });
  const earlier = appendEvent(view, {
    ...pendingEvent(generationId), alertKey: 'ops/earlier', created: 12,
  });
  appendEvent(view, { kind: 'alert_sent', key: record.key, targetClaimId: generationId,
    pendingEventId: earlier.id, alertKey: 'ops/earlier', created: 13 });
  const { id: pendingId, sequence } = appendEvent(view, pendingEvent(generationId));
  // External delivery can start now. Report this attempt and die without a
  // sync, an alert_sent acknowledgement, or another state write.
  return { generationId, pendingId, sequence };
}

function compactChecked(root, view, checkpoint) {
  const mainHead = root.stateHeadOffset(ledger);
  const oldHead = view.stateHeadOffset(ledger);
  const before = view.currentSequence();
  view.compactState(ledger);
  // pins compactState.2 the state head changes on return, with no await or use of the result
  assert.notEqual(view.stateHeadOffset(ledger), oldHead);
  const updates = view.query({ fromSequence: before + 1, types: ['state_update'] })
    .map(({ id }) => JSON.parse(view.getStateUpdateJson(id)));
  const snapshots = updates.filter(u => u.state_id === ledger && u.operation.Snapshot !== undefined);
  // pins compactState.1 one namespaced string installs a full snapshot of that ledger
  assert.ok(snapshots.some(u => {
    const entries = JSON.parse(Buffer.from(u.operation.Snapshot).toString());
    return entries.length === 1 && entries[0].eventId === checkpoint.eventId;
  }));
  // pins compactState.6 the view got the snapshot, not the root's same-named ledger
  assert.deepEqual({ head: root.stateHeadOffset(ledger), events: root.getStateJson(ledger) },
    { head: mainHead, events: mainEvents });
  // pins compactState.7 the immediate logical count and array exclude the entire prefix
  assert.deepEqual([view.getStateLen(ledger), view.getStateJson(ledger)], [1, [checkpoint]]);
  return { compactedFrom: oldHead };
}

function checkpointWriter(root, view, options) {
  if (options.empty) appendEvent(view, {
    kind: 'clear', key: record.key, targetClaimId: options.generationId, created: 15,
  });
  const oldCount = view.getStateLen(ledger);
  const event = checkpointEvent(options);
  const { id: eventId, sequence } = appendEvent(view, event);
  const checkpoint = { ...event, eventId, sequence };
  // pins appendToStateJsonWithIdentity.16 the stamped checkpoint lands at the saved length
  assert.deepEqual(view.getStateItemJson(ledger, oldCount), checkpoint);
  view.redactStateItems(ledger, 0, oldCount);
  // pins getStateLen.8 the saved count is an exclusive end and removes precisely the old events
  assert.deepEqual(view.getStateJson(ledger), [checkpoint]);
  const compacted = options.compact ? compactChecked(root, view, checkpoint) : {};
  return { stamp: { eventId, sequence }, ...compacted };
}

function checkRecoveredCheckpoint(path, checkpoint) {
  reopen(path, root => {
    const view = root.view(branch);
    // pins redactStateItems.10 replay after the writer died retains just the appended checkpoint
    assert.deepEqual([view.getStateLen(ledger), view.getStateItemJson(ledger, 0), view.getStateJson(ledger)],
      [1, checkpoint, [checkpoint]]);
    // pins compactState.8 even an empty checkpoint is replayable beside a populated legacy snapshot
    assert.deepEqual({ legacy: view.getStateJson(legacy), ledger: view.getStateJson(ledger) },
      { legacy: [legacyRecord], ledger: [checkpoint] });
    // pins compactState.6 the other branch still reads its own original ledger after recovery
    assert.deepEqual(root.getStateJson(ledger), mainEvents);
  });
}

// Compaction writes one snapshot record, and it is the last record the writer
// wrote before it died. A copy of the store whose log has lost that record's
// last byte is what a death in the middle of the write leaves. No call stops
// a write partway, so the fault setup cuts the store's record log.
function checkTornCompaction(path, checkpoint, compactedFrom) {
  const torn = `${path}-torn`;
  cpSync(path, torn, { recursive: true, verbatimSymlinks: true });
  try {
    const log = join(torn, 'records.log');
    truncateSync(log, statSync(log).size - 1);
    reopen(torn, root => {
      assert.ok(root.recovery()?.droppedBytes > 0, 'the cut tore the last record');
      const view = root.view(branch);
      assert.equal(view.stateHeadOffset(ledger), compactedFrom, 'the torn record was the compaction');
      // pins compactState.8 a death in the middle of compaction's write leaves the reader the checkpoint, beside the legacy snapshot
      assert.deepEqual({ length: view.getStateLen(ledger), ledger: view.getStateJson(ledger), legacy: view.getStateJson(legacy) },
        { length: 1, ledger: [checkpoint], legacy: [legacyRecord] });
    });
  } finally {
    rmSync(torn, { recursive: true, force: true });
  }
}

const audit = 'framework/tool-result-guard';
const messages = 'messages';
const presentations = 'agents/wren/presentations';
const framework = 'framework/state';
const summaries = 'agents/wren/autobio:summaries';
const hostMode = { hostMode: { quiesced: true, reason: 'repair summary', since: 51 } };
const repaired = [{ id: 'L4-8', content: 'Corrected account of both sources.', sourceIds: ['L3-1', 'L3-5'] }];
const rows = [
  { id: 'tool-row', participant: 'user', content: [{ type: 'tool_result', toolUseId: 'read-1', content: 'result withheld' }] },
  { id: 'mail-row', participant: 'user', text: 'Delivered after restart', deferredWriteId: 'queued/91' },
  { id: 'notice-row', participant: 'system', text: 'Agent ended without a result', noticeKey: 'interrupted/45' },
];
const presented = rows.map(({ id }, i) => ({ messageId: id, agent: 'wren', ordinal: 8 + i }));
const original = Buffer.from(JSON.stringify([{ id: 'read-1', result: { text: 'résultat\n'.repeat(1200) } }]));
const content = Buffer.from(JSON.stringify([{ type: 'tool_result', toolUseId: 'read-1', content: 'archived rendering' }]));
const receipts = [
  { type: 'workspace-intent', data: { path: 'notes', revision: 8 } },
  { type: 'fold-receipt', data: { folded: ['L3-1', 'L3-5'], into: 'L4-8' } },
];
const stagedEntry = (originalId, contentId) => ({ type: 'staged', batchId: 'batch-39',
  originals: { blobId: originalId }, content: { blobId: contentId } });
const linkedEntry = { type: 'linked', batchId: 'batch-39', messageId: 'tool-row' };

function mixedWriter(root, view) {
  const originalId = view.storeBlob(original, 'application/json');
  const contentId = view.storeBlob(content, 'application/json');
  view.appendToStateJson(audit, stagedEntry(originalId, contentId));
  rows.forEach(row => view.appendToStateJson(messages, row));
  presented.forEach(p => view.appendToStateJson(presentations, p));
  view.appendToStateJson(audit, linkedEntry);
  root.setStateJson(framework, hostMode);
  root.setStateJson(summaries, repaired);
  const first = root.appendJson(receipts[0].type, receipts[0].data);
  const last = view.appendJsonWithLinks(receipts[1].type, receipts[1].data, { linkedTo: [first.id] });
  const coordinates = { branch: view.pinnedBranch(), ids: [first.id, last.id], originalId, contentId };
  // A reader found the records under these very IDs before asking for stability.
  for (const [i, id] of [first.id, last.id].entries()) {
    assert.deepEqual(JSON.parse(root.getRecord(id).payload.toString()), receipts[i].data);
  }
  // pins sync.1 normal synchronous completion of the zero-argument barrier, not a promise or receipt
  assert.equal(root.sync(), undefined);
  return coordinates;
}

const queue = 'framework/deferred-writes';
const familyProgress = 'agents/wren/autobio:compression-family-progress';
const mergeQueue = 'agents/wren/autobio:mergeQueue';
const mergeQuarantine = 'agents/wren/autobio:merge-quarantine';
const oldEntries = ['first', 'second'].map((text, i) => ({ seq: i + 1, participant: 'user',
  forAgent: 'wren', content: [{ type: 'text', text }] }));
const assigned = { version: 2, pending: oldEntries.map((entry, i) => ({ ...entry, id: `queued/${i + 40}` })) };
const merge = { level: 3, sourceIds: ['L2-a', 'L2-b'] };
const atLimit = { level: 2, sourceIds: ['L1-x', 'L1-y'] };
const family = { familyKey: 'family/a', chunkSourceHash: 'span-9' };
const oldProgress = {
  [familyProgress]: [{ ...family, outcomes: [] }],
  [mergeQueue]: [{ ...merge, attempts: 1, lastOutcome: 'provider_error' }, { ...atLimit, attempts: 4 }],
  [mergeQuarantine]: [],
};
const newestProgress = {
  [familyProgress]: [{ ...family, outcomes, canonicalProviderInputTokens: 137 }],
  [mergeQueue]: [{ ...merge, attempts: 2, lastOutcome: 'refusal', lastStopReason: 'refusal', hadRefusal: true }],
  [mergeQuarantine]: [{ ...atLimit, key: 'quarantine/x-y', attempts: 5, lastOutcome: 'incomplete',
    lastStopReason: 'max_tokens', lastRequestHash: 'final-rung', quarantinedAt: 63 }],
};

function progressWriter(root, _view, { empty }) {
  for (const [slot, value] of Object.entries(newestProgress)) root.setStateJson(slot, value);
  root.setStateJson(queue, empty ? [] : assigned);
  root.sync();
  return {};
}

if (process.argv[2] === '--crash-writer') {
  const [, , , path, action, options] = process.argv;
  const root = JsStore.open({ path });
  const view = action === 'progress' ? undefined : root.view(branch);
  globalThis.liveWriterHandles = [root, view];
  const writers = { pending: pendingWriter, checkpoint: checkpointWriter,
    compact: compactChecked,
    mixed: mixedWriter, progress: progressWriter };
  const coordinates = writers[action](root, view, JSON.parse(options));
  writeSync(1, JSON.stringify(coordinates));
  process.kill(process.pid, 'SIGKILL');
} else {
  // The writer ends itself with SIGKILL. The system keeps what the process
  // wrote, so these tests show what survives the death of the process after a
  // sync, not what survives a loss of power. One test also renames the
  // directory of an open store. Windows reports neither as these tests need.
  const posix = { skip: process.platform === 'win32' && 'needs a POSIX signal and a rename of an open directory' };

  test('The one surviving ledger checkpoint outlives prefix redaction and compaction', posix, t => {
    for (const [compact, empty] of [[false, false], [true, false], [true, true]]) {
      const path = setup(t, root => {
        root.registerState({ id: legacy, strategy: 'snapshot' });
        root.setStateJson(legacy, [legacyRecord]);
        root.registerState({ id: ledger, strategy: 'append_log' });
        root.appendToStateJson(ledger, mainEvents[0]);
        root.createBranch(branch);
      });
      const attempt = runWriter(path, 'pending');
      reopen(path, root => {
        const events = root.view(branch).getStateJson(ledger);
        // pins appendToStateJsonWithIdentity.16 pending alert is stored before the delivery attempt
        assert.deepEqual(events.at(-1), { ...pendingEvent(attempt.generationId),
          eventId: attempt.pendingId, sequence: attempt.sequence });
      });
      const options = { ...attempt, compact, empty };
      const { stamp, compactedFrom } = runWriter(path, 'checkpoint', options);
      const checkpoint = { ...checkpointEvent(options), ...stamp };
      if (compact) checkTornCompaction(path, checkpoint, compactedFrom);
      checkRecoveredCheckpoint(path, checkpoint);
      if (!compact) {
        // A restart interrupted the sequence between redact and compact. Finish
        // the same operation on the recovered survivor and die at its return.
        const { compactedFrom } = runWriter(path, 'compact', checkpoint);
        checkTornCompaction(path, checkpoint, compactedFrom);
        checkRecoveredCheckpoint(path, checkpoint);
      }
    }
  });

  test('An unawaited repair barrier is complete before close and mixed references recover after process death', posix, t => {
    const path = setup(t, root => {
      for (const id of [messages, presentations, audit]) root.registerState({ id, strategy: 'append_log' });
      for (const id of [framework, summaries]) root.registerState({ id, strategy: 'snapshot' });
      root.setStateJson(framework, { hostMode: null });
      root.setStateJson(summaries, [{ ...repaired[0], content: 'Old summary in need of repair.' }]);
      root.createBranch(branch);
    });
    const saved = runWriter(path, 'mixed');
    reopen(path, root => {
      // pins open.17 after a sync through the open handle, the repair survives the death of the process, before close could flush it
      assert.deepEqual(root.getStateJson(summaries), repaired);
      // pins sync.9 a synced JSON replacement preserves the chosen host mode across process death
      assert.deepEqual(root.getStateJson(framework), hostMode);
      // pins sync.6 after a read then sync and the death of the process, every pre-barrier typed ID still names its original type, data and links
      assert.deepEqual(saved.ids.map(id => {
        const r = root.getRecord(id);
        return { id: r.id, type: r.recordType, data: JSON.parse(r.payload.toString()), links: r.linkedTo };
      }), receipts.map((r, i) => ({ id: saved.ids[i], ...r, links: i ? [saved.ids[0]] : [] })));

      const view = root.view(saved.branch);
      const delivered = view.getStateJson(messages);
      // pins sync.8 after the delivery barrier and the death of the process, every delivered row, including mail and the distinct notice, is present
      assert.deepEqual(delivered, rows);
      const events = view.getStateJson(audit);
      const staged = events.find(e => e.type === 'staged');
      const linked = events.find(e => e.type === 'linked' && e.batchId === staged.batchId);
      const originals = view.getBlob(staged.originals.blobId);
      const presentation = view.getStateJson(presentations);
      // pins sync.12 after one barrier and the death of the process, the audit links, referenced bytes, guarded row and its presentation survive together
      assert.deepEqual({ events, originals, rendering: view.getBlob(staged.content.blobId),
        row: delivered.find(r => r.id === linked.messageId), presentation }, {
        events: [stagedEntry(saved.originalId, saved.contentId), linkedEntry], originals: original, rendering: content,
        row: rows[0], presentation: presented,
      });
      // pins getBlob.4 boot resolves original tool results under the key in the recovered reference
      assert.deepEqual([staged.originals.blobId, originals], [saved.originalId, original]);
      const next = root.appendJson('workspace-intent', { revision: 9 });
      // pins sync.6 the first post-crash append cannot reuse a synced receipt's ID
      assert.ok(!saved.ids.includes(next.id));
      root.switchBranch(saved.branch);
      const archive = root.getStateJson(audit).find(e => e.type === 'staged');
      // pins storeBlob.4 after a sync and the death of the process, returning to a branch with the reference still gives the bytes, without restaging
      assert.deepEqual(root.getBlob(archive.content.blobId), content);

      // Make the *configured store path*, not any particular internal file,
      // unavailable while the dirty replacement's checkpoint is attempted.
      const choice = { hostMode: { ...hostMode.hostMode, reason: 'second repair' } };
      root.setStateJson(framework, choice);
      root.appendToStateJson(messages, { ...rows[1], id: 'mail-row-2', deferredWriteId: 'queued/92' });
      const aside = `${path}-aside`;
      renameSync(path, aside);
      try {
        writeFileSync(path, 'temporarily not a directory');
        // pins sync.8 a failed delivery checkpoint throws, so the caller cannot mistake it for acknowledgement
        assert.throws(() => root.sync(), 'checkpoint of dirty delivery and host mode at an unavailable path');
      } finally {
        rmSync(path, { force: true });
        renameSync(aside, path);
      }
      // pins sync.9 a thrown sync is not rollback of the already written choice on the live handle
      assert.deepEqual(root.getStateJson(framework), choice);
    });
  });

  test('Synced progress and queue replacements recover without another flush', posix, t => {
    for (const empty of [false, true]) {
      const path = setup(t, root => {
        const older = { ...oldProgress, [queue]: empty ? assigned : { version: 1, pending: oldEntries } };
        for (const [id, value] of Object.entries(older)) {
          root.registerState({ id, strategy: 'snapshot' });
          root.setStateJson(id, value);
        }
      });
      // No importer message write between the newly assigned IDs and the crash.
      runWriter(path, 'progress', { empty });
      reopen(path, root => {
        // pins sync.11 after a sync and the death of the process: the exact replacement with assigned IDs, or a stored [] distinct from absence
        assert.deepEqual(root.getStateJson(queue), empty ? [] : assigned);
        // pins sync.10 paid answer, counted attempt with its new outcome, and newly quarantined merge survive a sync and the death of the process
        assert.deepEqual(Object.fromEntries(Object.keys(newestProgress).map(id => [id, root.getStateJson(id)])),
          newestProgress);
      });
    }
  });
}
