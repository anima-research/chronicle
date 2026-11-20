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

  withBlockedCheckpoint(path, 'state.bin', () => {
    let reachedAppend = false;
    assert.throws(() => {
      store.sync();
      reachedAppend = true;
      store.appendJson(layout, { fold: 'proposed' });
    });
    assert.equal(reachedAppend, false, 'the barrier threw, not the append');
    assert.deepEqual(store.getRecordIdsByType(layout), prefix);
    assert.deepEqual(store.getRecordIdsByType(layout).map(id => readEntry(store, id)), old);
  });
  store.sync();

  const receipt = appendEntry(store, 'push.receipt', { occurrence: 17 });
  const accepted = appendEntry(store, layout, { accepted: 17 });
  const start = appendEntry(store, 'push.render-start', { request: 'render-17' });
  store.setStateJson('mode', newMode);
  store.setStateJson('queue', newQueue);
  withBlockedCheckpoint(path, 'state.bin', () => {
    assert.throws(() => store.sync());
    assert.deepEqual(readEntry(store, accepted.id), accepted);
    assert.deepEqual(store.getRecordIdsByType(layout), [...prefix, accepted.id]);
    assert.deepEqual(store.getRecordIdsByType(receipt.type), [receipt.id]);
    assert.deepEqual(store.getRecordIdsByType(start.type).map(id => readEntry(store, id)), [start]);
    assert.deepEqual([receipt, accepted, start].map(({ id }) => readEntry(sibling, id)), [receipt, accepted, start]);
    assert.deepEqual(store.getStateJson('mode'), newMode);
    assert.deepEqual(sibling.getStateJson('queue'), newQueue);
  });

  assert.doesNotThrow(() => store.sync());
  writeSync(1, JSON.stringify([...old, receipt, accepted, start]));
  process.kill(process.pid, 'SIGKILL');
}

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

  assert.throws(() => store.switchBranch('no such branch'));
  assert.deepEqual(selected(), [source.name, source.id]);
  assert.equal(store.getStateJson('work'), 'source work');
  store.setStateJson('work', 'after missing name');
  assert.equal(sourceView.getStateJson('work'), 'after missing name');

  withBlockedCheckpoint(f.path, 'state.bin', () => {
    assert.throws(() => store.registerState({ id: 'pending', strategy: 'snapshot' }));
    assert.throws(() => store.switchBranch('target'));
    assert.deepEqual(selected(), [source.name, source.id]);
    assert.equal(store.getStateJson('work'), 'after missing name');
  });
  store.setStateJson('work', 'after refused switch');
  assert.equal(sourceView.getStateJson('work'), 'after refused switch');

  withBlockedCheckpoint(f.path, 'branches.bin', () => {
    assert.throws(() => store.switchBranch('target'));
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
  const nonDuplicate = e => !String(e.message).includes('State already exists');
  assert.throws(() => store.registerState({ id: 17, strategy: 'snapshot' }), nonDuplicate);
  withBlockedCheckpoint(first.path, 'state.bin', () => {
    assert.throws(() => store.registerState({ id: 'blocked', strategy: 'snapshot' }), nonDuplicate);
  });
  store.close();
  assert.throws(() => store.registerState({ id: 'closed', strategy: 'snapshot' }), nonDuplicate);

  const second = fixture(t);
  const log = second.create();
  log.registerState({ id: 'rows', strategy: 'append_log' });
  log.appendToStateJson('rows', 'original');
  assert.throws(() => log.updateStateStrategy({ id: 'rows', strategy: 'snapshot' }));
  log.appendToStateJson('rows', 'after mismatch');
  assert.deepEqual(log.getStateJson('rows'), ['original', 'after mismatch']);
  withBlockedCheckpoint(second.path, 'state.bin', () => {
    assert.throws(() => log.updateStateStrategy({ id: 'rows', strategy: 'append_log', deltaSnapshotEvery: 51 }));
  });
  log.appendToStateJson('rows', 'after repair');
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
    assert.throws(() => store.getRecord(mark.id));
    assert.throws(() => store.getStateItemJson('ended-log', 0));
    assert.throws(() => store.getStateJson(binding));
    assert.equal(store.currentBranch().name, 'session');
    assert.throws(() => store.getState(binding), e => !e.message.startsWith('Deserialization error: '));
  });
  assert.deepEqual(JSON.parse(store.getState(binding).toString('utf8')), bound);
  assert.deepEqual(readEntry(store, mark.id), mark);
  assert.deepEqual(store.getStateItemJson('ended-log', 0), ended);

  withBlockedCheckpoint(f.path, 'state.bin', () => {
    assert.throws(() => store.registerState({ id: 'pending', strategy: 'snapshot' }));
    assert.throws(() => store.setStateJson(binding, { compileId: 'new-compile' }));
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
  for (const type of new Set(entries.map(entry => entry.type))) {
    assert.deepEqual(recovered.getRecordIdsByType(type).map(id => readEntry(recovered, id)),
      entries.filter(entry => entry.type === type));
  }
  assert.deepEqual(recovered.getStateJson('mode'), newMode);
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
    assert.equal(store.isClosed(), false);
    assert.throws(() => store.close(), e => { closeError = e; return true; });
    assert.equal(store.isClosed(), true);
    assert.deepEqual([store.isClosed(), view.isClosed()], [true, true]);
    assert.ok(Object.values(ErrorCode).includes(syncError.code), `sync threw ${syncError.code}`);
    assert.equal(closeError.code, syncError.code);
    assert.ok(typeof closeError.message === 'string' && closeError.message.trim().length > 0);
    assert.ok(syncError instanceof Error && closeError instanceof Error && closeError !== syncError);
    assert.throws(() => store.getStateJson('work'));
    assert.throws(() => view.getStateJson('work'));
  });
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
    assert.throws(() => store.close(), e => e instanceof Error && Object.values(ErrorCode).includes(e.code));
    assert.equal(store.isClosed(), true);
  });
  assert.equal(f.open().isClosed(), false);
});
