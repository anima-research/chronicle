import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, renameSync, rmSync, rmdirSync, truncateSync, writeFileSync, writeSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { ErrorCode, JsStore } from '../index.js';

function fixture(t) {
  const path = mkdtempSync(join(tmpdir(), 'chronicle-checkpoint-'));
  const roots = [];
  function keep(store) { roots.push(store); return store; }
  t.after(() => {
    try {
      for (const root of roots) if (!root.isClosed()) root.close();
    } finally {
      rmSync(path, { recursive: true, force: true });
    }
  });
  return { path, create: () => keep(JsStore.create({ path })), open: () => keep(JsStore.open({ path })) };
}

// As in checkpoint_failures.rs: a checkpoint cannot be renamed over a
// directory, even if the tests run as root. Restore the input on every exit.
function withBlockedCheckpoint(path, file, run) {
  const target = join(path, file);
  const saved = `${target}.before-test`;
  const existed = existsSync(target);
  if (existed) renameSync(target, saved);
  try {
    mkdirSync(target);
    try { run(); } finally { rmdirSync(target); }
  } finally {
    if (existed) renameSync(saved, target);
  }
}

// Re-open *before* the fault, so open sees a healthy log but the cold reads
// cannot fetch even a whole record. No decoding or knowledge of log offsets.
function withUnreadableRecords(path, run) {
  const log = join(path, 'records.log');
  const saved = readFileSync(log);
  try {
    truncateSync(log, 0);
    run();
  } finally {
    writeFileSync(log, saved);
  }
}

const layout = 'fold.layout';
const newMode = { mode: 'quiesced' };
const newQueue = [{ retry: 'render-17' }];
const payload = (record) => JSON.parse(record.payload.toString('utf8'));

function appendEntry(store, type, value) {
  return { type, id: store.appendJson(type, value).id, value };
}

function readEntry(store, id) {
  const record = store.getRecord(id);
  return { type: record.recordType, id: record.id, value: payload(record) };
}

function journalWriter(path) {
  const store = JsStore.create({ path });
  const sibling = store.view('main');
  store.registerState({ id: 'mode', strategy: 'snapshot' });
  store.registerState({ id: 'queue', strategy: 'snapshot' });
  store.setStateJson('mode', { mode: 'active' });
  store.setStateJson('queue', [{ retry: 'accept-17' }]);
  const old = [appendEntry(store, layout, { fold: 1 }), appendEntry(store, layout, { fold: 2 })];
  const prefix = store.getRecordIdsByType(layout);
  assert.deepEqual(prefix, old.map(({ id }) => id));

  // Dirty state heads, but no new layout yet. Make the proposed append
  // conditional on the barrier, just as the journal writer does.
  withBlockedCheckpoint(path, 'state.bin', () => {
    let reachedAppend = false;
    assert.throws(() => {
      store.sync();
      reachedAppend = true;
      store.appendJson(layout, { fold: 'proposed' });
    });
    assert.equal(reachedAppend, false, 'the barrier threw, not the append');
    // pins getRecordIdsByType.19: nothing landed, so the accounted-for list is unchanged.
    assert.deepEqual(store.getRecordIdsByType(layout), prefix);
    // pins sync.5: live replay finds only the old records, no proposed entry.
    assert.deepEqual(store.getRecordIdsByType(layout).map(id => readEntry(store, id)), old);
  });
  store.sync();

  const receipt = appendEntry(store, 'push.receipt', { occurrence: 17 });
  const accepted = appendEntry(store, layout, { accepted: 17 });
  const start = appendEntry(store, 'push.render-start', { request: 'render-17' });
  store.setStateJson('mode', newMode);
  store.setStateJson('queue', newQueue);
  withBlockedCheckpoint(path, 'state.bin', () => {
    // pins sync.3: failure to publish dirty metadata throws in this stack.
    assert.throws(() => store.sync());
    // pins sync.5: the post-append failure hasn't rolled back the returned ID or its data.
    assert.deepEqual(readEntry(store, accepted.id), accepted);
    // pins getRecordIdsByType.19: a landed acceptance is exactly the suffix of the old list.
    assert.deepEqual(store.getRecordIdsByType(layout), [...prefix, accepted.id]);
    // pins getRecordIdsByType.11: refresh on the appending handle still discovers its new receipt.
    assert.deepEqual(store.getRecordIdsByType(receipt.type), [receipt.id]);
    // pins appendJson.3: replay by type and then ID finds the returned record on the live writer.
    assert.deepEqual(store.getRecordIdsByType(start.type).map(id => readEntry(store, id)), [start]);
    // pins getRecord.7: another handle sharing the writer resolves all three landed IDs.
    assert.deepEqual([receipt, accepted, start].map(({ id }) => readEntry(sibling, id)), [receipt, accepted, start]);
    // pins sync.9: a throw is not a rollback of the live host-mode replacement.
    assert.deepEqual(store.getStateJson('mode'), newMode);
    // pins setStateJson.5: even without a successful barrier the completed replacement stands.
    assert.deepEqual(sibling.getStateJson('queue'), newQueue);
  });

  // pins sync.13: retry the barrier on the same work, without another append.
  assert.doesNotThrow(() => store.sync());
  writeSync(1, JSON.stringify([...old, receipt, accepted, start]));
  // A successful close would supply a second barrier and mask a broken retry.
  process.kill(process.pid, 'SIGKILL');
}

// Run just the writer, not this test suite, in the process that dies.
if (process.argv[2] === '--journal-writer') journalWriter(process.argv[3]);

test('The cursor distinguishes selection refusal from selection whose checkpoint failed', t => {
  const f = fixture(t);
  const store = f.create();
  store.registerState({ id: 'work', strategy: 'snapshot' });
  store.setStateJson('work', 'target work');
  const target = store.createBranch('target');
  store.createBranch('source');
  const source = store.switchBranch('source');
  store.setStateJson('work', 'source work');
  const sourceView = store.view('source');
  function selected() {
    const { name, id } = store.currentBranch();
    return [name, id];
  }

  // pins switchBranch.10: an unknown name is a synchronous refusal, not a failure value.
  assert.throws(() => store.switchBranch('no such branch'));
  // pins currentBranch.6: the affected open handle still identifies the source after refusal.
  assert.deepEqual(selected(), [source.name, source.id]);
  // pins switchBranch.10: source state is still readable and receives the next write.
  assert.equal(store.getStateJson('work'), 'source work');
  store.setStateJson('work', 'after missing name');
  assert.equal(sourceView.getStateJson('work'), 'after missing name');

  withBlockedCheckpoint(f.path, 'state.bin', () => {
    // Make a checkpoint retry pending *before* attempting the switch.
    assert.throws(() => store.registerState({ id: 'pending', strategy: 'snapshot' }));
    // pins switchBranch.10: failed admission of a switch also throws synchronously.
    assert.throws(() => store.switchBranch('target'));
    // pins currentBranch.6: a refused switch to a real branch has not moved the cursor.
    assert.deepEqual(selected(), [source.name, source.id]);
    assert.equal(store.getStateJson('work'), 'after missing name');
  });
  store.setStateJson('work', 'after refused switch');
  // pins switchBranch.10: the original branch is writable when admission is repaired.
  assert.equal(sourceView.getStateJson('work'), 'after refused switch');

  withBlockedCheckpoint(f.path, 'branches.bin', () => {
    // No retry pending this time: selection succeeds and *its* publication fails.
    // The store throws after it has moved.
    assert.throws(() => store.switchBranch('target'));
    // pins currentBranch.6: the target is inspectable even though the call threw.
    assert.deepEqual(selected(), [target.name, target.id]);
    assert.equal(store.getStateJson('work'), 'target work');
  });
  store.setStateJson('work', 'on landed target');
  assert.equal(store.view('target').getStateJson('work'), 'on landed target');
  assert.equal(sourceView.getStateJson('work'), 'after refused switch');
});

test('Registration and retuning report non-duplicate refusals at the call', t => {
  const first = fixture(t);
  const store = first.create();
  // Framework's substring check also covers context-manager's exact-match check.
  const nonDuplicate = e => !String(e.message).includes('State already exists');
  // pins registerState.7: malformed ID input throws a non-duplicate at registration.
  assert.throws(() => store.registerState({ id: 17, strategy: 'snapshot' }), nonDuplicate);
  withBlockedCheckpoint(first.path, 'state.bin', () => {
    // pins registerState.7: checkpoint I/O failure is immediate and isn't a duplicate.
    assert.throws(() => store.registerState({ id: 'blocked', strategy: 'snapshot' }), nonDuplicate);
  });
  store.close();
  // pins registerState.7: a closed store doesn't report an already-initialized slot.
  assert.throws(() => store.registerState({ id: 'closed', strategy: 'snapshot' }), nonDuplicate);

  const second = fixture(t);
  const log = second.create();
  log.registerState({ id: 'rows', strategy: 'append_log' });
  log.appendToStateJson('rows', 'original');
  // pins updateStateStrategy.6: a kind mismatch throws without making the handle unusable.
  assert.throws(() => log.updateStateStrategy({ id: 'rows', strategy: 'snapshot' }));
  log.appendToStateJson('rows', 'after mismatch');
  assert.deepEqual(log.getStateJson('rows'), ['original', 'after mismatch']);
  withBlockedCheckpoint(second.path, 'state.bin', () => {
    // pins updateStateStrategy.6: even a valid same-kind retune can throw synchronously.
    assert.throws(() => log.updateStateStrategy({ id: 'rows', strategy: 'append_log', deltaSnapshotEvery: 51 }));
  });
  log.appendToStateJson('rows', 'after repair');
  // pins updateStateStrategy.6: continue on the same populated store, regardless of cadence applied.
  assert.deepEqual(log.getStateJson('rows'), ['original', 'after mismatch', 'after repair']);
});

test('Temporary damage to a cold record causes real read errors, not absent data', t => {
  const f = fixture(t);
  const seed = f.create();
  const binding = 'context/thinking-binding';
  const bound = { compileId: 'cold-compile' };
  const ended = { namespace: 'owned', name: 'ended' };
  seed.createBranch('session');
  seed.switchBranch('session');
  seed.registerState({ id: binding, strategy: 'snapshot' });
  seed.registerState({ id: 'ended-log', strategy: 'append_log' });
  const mark = appendEntry(seed, 'journal.mark', { position: 9 });
  seed.setStateJson(binding, bound);
  seed.appendToStateJson('ended-log', ended);
  seed.close();

  const store = f.open();
  withUnreadableRecords(f.path, () => {
    // pins getRecord.5: a valid landed ID fails at the store read, not at payload parsing.
    assert.throws(() => store.getRecord(mark.id));
    // pins getStateItemJson.14: the unreadable owned entry must not appear absent.
    assert.throws(() => store.getStateItemJson('ended-log', 0));
    assert.throws(() => store.getStateJson(binding));
    // pins currentBranch.12: the binding read's error handler can still name its branch.
    assert.equal(store.currentBranch().name, 'session');
    // pins getState.6: an operational raw-read error isn't the permanent deserialization diagnostic.
    assert.throws(() => store.getState(binding), e => !e.message.startsWith('Deserialization error: '));
  });
  // pins getState.6: once the fault clears, raw bytes on this same handle give the original JSON.
  assert.deepEqual(JSON.parse(store.getState(binding).toString('utf8')), bound);
  assert.deepEqual(readEntry(store, mark.id), mark);
  assert.deepEqual(store.getStateItemJson('ended-log', 0), ended);

  withBlockedCheckpoint(f.path, 'state.bin', () => {
    assert.throws(() => store.registerState({ id: 'pending', strategy: 'snapshot' }));
    assert.throws(() => store.setStateJson(binding, { compileId: 'new-compile' }));
    // pins currentBranch.12: a failure writing a binding doesn't replace the branch diagnostic.
    assert.equal(store.currentBranch().name, 'session');
  });
});

test('Failing a barrier leaves exactly the landed journal suffix and state readable', {
  skip: process.platform === 'win32' && 'the writer ends itself with SIGKILL, which Windows does not report as a signal',
}, t => {
  const f = fixture(t);
  const child = spawnSync(process.execPath, [fileURLToPath(import.meta.url), '--journal-writer', f.path], {
    encoding: 'utf8', timeout: 30_000,
  });
  if (child.error) throw child.error;
  assert.equal(child.signal, 'SIGKILL', child.stderr);
  const entries = JSON.parse(child.stdout);
  const recovered = f.open();
  // The entries and the state updates were in the log before the retry, so
  // they would be found here if it did nothing: this shows that the retry
  // repeated and lost none of them, not that it made them durable, which only
  // a loss of power can show.
  for (const type of new Set(entries.map(entry => entry.type))) {
    // pins sync.13: after the retried barrier and the death of the process, receipts, acceptances and starts are each there once.
    assert.deepEqual(recovered.getRecordIdsByType(type).map(id => readEntry(recovered, id)),
      entries.filter(entry => entry.type === type));
  }
  // pins sync.9: successful sync preserves the replacement mode across death without close.
  assert.deepEqual(recovered.getStateJson('mode'), newMode);
  // pins setStateJson.5: after the retried barrier and the death of the process, the already-written replacement queue is there.
  assert.deepEqual(recovered.getStateJson('queue'), newQueue);
});

test('Close consumes the root even when its final barrier throws', t => {
  const f = fixture(t);
  const store = f.create();
  const view = store.view('main');
  store.registerState({ id: 'work', strategy: 'snapshot' });
  store.setStateJson('work', 'dirty state head');
  withBlockedCheckpoint(f.path, 'state.bin', () => {
    let syncError, closeError;
    assert.throws(() => store.sync(), e => { syncError = e; return true; });
    // pins close.7: the failed explicit sync hasn't consumed the root.
    assert.equal(store.isClosed(), false);
    // pins close.3: close itself throws the failure in the calling stack.
    assert.throws(() => store.close(), e => { closeError = e; return true; });
    // pins isClosed.3: consumption, not successful return, determines the answer after close.
    assert.equal(store.isClosed(), true);
    // pins close.7: both live references accurately report consumption immediately after the throw.
    assert.deepEqual([store.isClosed(), view.isClosed()], [true, true]);
    // pins close.3: the close error is the store's error for the failed barrier, of the same kind as the explicit sync's.
    assert.ok(Object.values(ErrorCode).includes(syncError.code), `sync threw ${syncError.code}`);
    assert.equal(closeError.code, syncError.code);
    // pins close.10: the close error has an operator-facing message, with no literal required.
    assert.ok(typeof closeError.message === 'string' && closeError.message.trim().length > 0);
    // pins close.12: both attempts yielded separately catchable store errors, neither lost.
    assert.ok(syncError instanceof Error && closeError instanceof Error && closeError !== syncError);
    // pins close.8: failure of the final barrier leaves neither handle usable.
    assert.throws(() => store.getStateJson('work'));
    assert.throws(() => view.getStateJson('work'));
  });
  // pins close.8: the lock is free without collecting either of the old JS objects.
  const reopened = f.open();
  assert.equal(reopened.isClosed(), false);
  assert.throws(() => store.currentBranch());
  assert.throws(() => view.currentBranch());
});

test('Close reports the failure of its own barrier when no sync failed before it', t => {
  const f = fixture(t);
  const store = f.create();
  store.registerState({ id: 'work', strategy: 'snapshot' });
  store.setStateJson('work', 'dirty state head');
  withBlockedCheckpoint(f.path, 'state.bin', () => {
    // pins close.3: with no earlier failure to hand back, close throws the store's error from its own final sync.
    assert.throws(() => store.close(), e => e instanceof Error && Object.values(ErrorCode).includes(e.code));
    assert.equal(store.isClosed(), true);
  });
  assert.equal(f.open().isClosed(), false);
});
