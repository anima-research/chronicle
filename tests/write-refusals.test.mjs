import test from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { mkdtempSync, readFileSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { constants, tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { JsStore } from '../index.js';

const posix = { skip: process.platform === 'win32' && 'uses POSIX open-directory renames or RLIMIT_FSIZE' };

function freshPath(t) {
  const parent = mkdtempSync(join(tmpdir(), 'chronicle-write-refusal-'));
  t.after(() => rmSync(parent, { recursive: true, force: true }));
  return join(parent, 'store');
}

function withPendingCheckpoint(store, path, tryWrites) {
  const saved = `${path}-saved`;
  renameSync(path, saved);
  try {
    writeFileSync(path, 'not a directory');
    assert.throws(() => store.registerState({ id: 'pending', strategy: 'snapshot' }));
    tryWrites();
  } finally {
    rmSync(path, { force: true });
    renameSync(saved, path);
  }
}

function runLimited(fixture, blocks) {
  execFileSync('sh', ['-c', `ulimit -f ${blocks} && exec "$@"`, 'limited-store',
    process.execPath, fileURLToPath(import.meta.url), '--limited'], {
    input: JSON.stringify(fixture), timeout: 30_000,
  });
}

function errnoCode(error) {
  if (typeof error?.code === 'string' && /^E[A-Z0-9]+$/.test(error.code)) return error.code;
  const match = error instanceof Error && /^IO error: .*\(os error (\d+)\)$/s.exec(error.message);
  if (match) return Object.keys(constants.errno).find(code => constants.errno[code] === Number(match[1]));
}

const tree = 'workspace';
const journal = 'workspace-journal';
const file = 'files/report';
const checkpoint = 'checkpoints/agent';
const added = 'files/new';
const updateIds = store => store.getRecordIdsByType('state_update');
const tuples = side => [file, checkpoint, added].map(path => side.treeGet(tree, path));
const treeOps = ({ revised }) => [
  { op: 'set', path: file, entry: revised },
  { op: 'remove', path: checkpoint },
  { op: 'set', path: added, entry: revised },
];

function savedCheckpoint(side) {
  const entry = side.treeGet(tree, checkpoint);
  return { entry, list: entry && JSON.parse(side.getBlob(entry.blobHash).toString('utf8')) };
}

function tryTreeWrites(side, fixture) {
  const { original, revised, saved, replacement, ids, journalIds } = fixture;
  let cause;
  const caught = error => { cause = error; return true; };

  assert.throws(() => side.treeSet(tree, file, revised), caught);
  assert.equal(errnoCode(cause), 'EFBIG', String(cause));
  assert.deepEqual([side.treeGet(tree, file), updateIds(side)], [original, ids]);

  assert.throws(() => side.treeSet(tree, checkpoint, replacement), caught);
  assert.equal(errnoCode(cause), 'EFBIG', String(cause));
  assert.deepEqual(savedCheckpoint(side), saved);

  assert.throws(() => side.treeRemove(tree, checkpoint), caught);
  assert.equal(errnoCode(cause), 'EFBIG', String(cause));
  assert.deepEqual(side.treeGet(tree, checkpoint), saved.entry);

  assert.throws(() => side.treeBatch(tree, treeOps(fixture)), caught);
  assert.equal(errnoCode(cause), 'EFBIG', String(cause));
  assert.deepEqual(tuples(side), [original, saved.entry, null]);
  assert.deepEqual(updateIds(side), ids);

  assert.throws(() => side.appendJson(journal, { event: 'not published' }), caught);
  assert.equal(errnoCode(cause), 'EFBIG', String(cause));
  assert.deepEqual(side.getRecordIdsByType(journal), journalIds);
}

const stamp = (item, record) => ({ ...item, by: record.id, at: record.sequence });

function readBack(read, whileFaulted) {
  try { return { value: read() }; } catch (error) {
    if (!whileFaulted) throw error;
    return { unavailable: true };
  }
}

function checkIdentity(side, { kind, rows, ids }, whileFaulted) {
  assert.deepEqual(updateIds(side), ids, 'the torn write published no update record');
  const length = readBack(() => side.getStateLen('index'), whileFaulted);
  if (!length.unavailable) {
    assert.equal(length.value, rows.length);
  }
  if (kind === 'single') {
    const next = readBack(() => side.getStateItemJson('index', rows.length), whileFaulted);
    if (!next.unavailable) {
      assert.equal(next.value, null);
    }
  }
  const full = readBack(() => side.getStateJson('index'), whileFaulted);
  if (!full.unavailable) {
    assert.deepEqual(full.value, rows);
  }
}

if (process.argv[2] === '--limited') {
  const hitLimit = new Promise(resolve => process.on('SIGXFSZ', resolve));
  const fixture = JSON.parse(readFileSync(0, 'utf8'));
  const store = JsStore.open({ path: fixture.path });
  store.setAutoSnapshot(false);
  const side = store.view('side');
  if (fixture.kind === 'tree') {
    tryTreeWrites(side, fixture);
  } else {
    if (fixture.kind === 'single') {
      assert.throws(() => side.appendToStateJsonWithIdentity('index', fixture.items[0], 'by', 'at'));
    } else {
      assert.throws(() => side.appendManyToStateJsonWithIdentity('index', fixture.items, 'by', 'at'));
    }
    checkIdentity(side, fixture, true);
  }
  const deadline = setTimeout(() => assert.fail('the child saw no SIGXFSZ for the attempted writes'), 5000);
  await hitLimit;
  clearTimeout(deadline);
  if (fixture.kind === 'tree') {
    assert.deepEqual(tuples(side), [fixture.original, fixture.saved.entry, null]);
    assert.deepEqual(updateIds(side), fixture.ids);
  }
  process.exit(0);
}

test('Pre-record I/O refusals preserve whole branch-local JSON values', posix, t => {
  const path = freshPath(t);
  const root = JsStore.openOrCreate({ path });
  try {
    const binding = 'agent/thinking-binding';
    const before = { compileId: 'a', sent: ['m1', 'm2'], identity: { model: 'alpha' } };
    const after = { compileId: 'b', sent: ['m3'], identity: { model: 'beta', turn: 4 } };
    const saved = { windows: [{ first: 2, last: 5 }], scopes: { agent: ['work', 'draft'] } };
    const unfolded = { windows: [], scopes: { agent: ['final'] }, note: 'new field' };
    for (const id of [binding, 'unfold', 'receipt']) root.registerState({ id, strategy: 'snapshot' });
    root.registerState({ id: 'log', strategy: 'append_log' });
    root.setStateJson(binding, { branch: 'main' });
    root.createBranch('side');
    const side = root.view('side');
    side.setState(binding, Buffer.from('not JSON'));
    assert.throws(() => side.getStateJson(binding));
    assert.equal(side.currentBranch().name, 'side');
    side.setStateJson(binding, before);
    side.setStateJson('unfold', saved);
    const receipt = Buffer.from([0x02, 0x00, 0xfe]);
    side.setState('receipt', receipt);
    const rows = [{ message: 'one' }, { message: 'two' }];
    for (const row of rows) side.appendToStateJson('log', row);
    const revision = side.stateHeadOffset('unfold');
    const sequence = side.currentSequence();
    withPendingCheckpoint(root, path, () => {
      assert.throws(() => side.setStateJson(binding, after));
      assert.equal(side.currentBranch().name, 'side');
      assert.throws(() => side.setStateJson('unfold', unfolded));
      assert.throws(() => side.setState('receipt', Buffer.from('replacement')));
      assert.throws(() => side.appendToStateJson('log', { message: 'three' }));
    });
    assert.deepEqual(root.view('side').getStateJson(binding), before);
    assert.deepEqual([side.getStateJson('unfold'), side.stateHeadOffset('unfold'), side.currentSequence()],
      [saved, revision, sequence]);
    assert.deepEqual(side.getState('receipt'), receipt);
    assert.deepEqual(side.getStateJson('log'), rows);
    side.setStateJson(binding, after);
    side.setState('receipt', Buffer.from('retried'));
    side.appendToStateJson('log', { message: 'retry' });
    assert.deepEqual(side.getStateJson(binding), after);
    assert.deepEqual(side.getState('receipt'), Buffer.from('retried'));
    assert.deepEqual(side.getStateJson('log'), [...rows, { message: 'retry' }]);
    assert.deepEqual(root.getStateJson(binding), { branch: 'main' });
  } finally { root.close(); }
});

test('Tree and journal mutations preserve the OS cause and do not publish a refused edit', posix, t => {
  const path = freshPath(t);
  const root = JsStore.openOrCreate({ path });
  let fixture;
  try {
    root.registerState({ id: tree, strategy: 'tree' });
    root.createBranch('side');
    const side = root.view('side');
    const entry = (text, mode) => {
      const bytes = Buffer.from(text);
      return { blobHash: side.storeBlob(bytes, 'application/octet-stream'), size: bytes.length, mode };
    };
    const original = entry('draft', 0o644);
    const revised = entry('replacement file', 0o755);
    const list = [{ window: 7, operations: ['create', 'edit'] }, { window: 9, operations: [] }];
    const saved = { entry: entry(JSON.stringify(list), 0o600), list };
    const replacement = entry(JSON.stringify(list.slice(1)), 0o640);
    side.treeSet(tree, file, original);
    side.treeSet(tree, checkpoint, saved.entry);
    side.appendJson(journal, { event: 'seed' });
    fixture = { path, kind: 'tree', original, revised, saved, replacement,
      ids: updateIds(side), journalIds: side.getRecordIdsByType(journal) };
  } finally { root.close(); }
  runLimited(fixture, 0);

  const reopened = JsStore.open({ path });
  try {
    reopened.setAutoSnapshot(false);
    const side = reopened.view('side');
    assert.deepEqual(tuples(side), [fixture.original, fixture.saved.entry, null]);
    assert.deepEqual(savedCheckpoint(side), fixture.saved);
    assert.deepEqual(updateIds(side), fixture.ids);
    assert.deepEqual(side.getRecordIdsByType(journal), fixture.journalIds);
    side.treeBatch(tree, treeOps(fixture));
    const ids = updateIds(side);
    assert.deepEqual([ids.slice(0, -1), JSON.parse(side.getStateUpdateJson(ids.at(-1))).state_id], [fixture.ids, tree]);
    assert.deepEqual(tuples(side), [fixture.revised, null, fixture.revised]);
  } finally { reopened.close(); }
});

test('Interrupted identity writes expose no partial stamped row or batch', posix, t => {
  for (const kind of ['single', 'batch']) {
    const path = freshPath(t);
    const large = { group: 'reply', body: { chunk: randomBytes(64 * 1024).toString('base64') }, by: 'stale', at: -1 };
    const items = kind === 'single' ? [large] : [{ group: 'open' }, large, { group: 'end' }];
    const root = JsStore.openOrCreate({ path });
    let fixture;
    try {
      root.setAutoSnapshot(false);
      root.registerState({ id: 'index', strategy: 'append_log' });
      root.createBranch('side');
      const side = root.view('side');
      const seed = [{ group: 'seed', body: { n: 1 } }, { group: 'seed', body: { n: 2 } }];
      const { record } = side.appendManyToStateJsonWithIdentity('index', seed, 'by', 'at');
      const rows = seed.map(item => stamp(item, record));
      assert.deepEqual(side.getStateJson('index'), rows);
      fixture = { kind, path, ids: updateIds(side), rows, items };
    } finally { root.close(); }
    runLimited(fixture, 32);
    const recovered = JsStore.open({ path });
    try { checkIdentity(recovered.view('side'), fixture, false); }
    finally { recovered.close(); }
  }
});
