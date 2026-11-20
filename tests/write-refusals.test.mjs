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
  // Obstruct pathname lookups, not the contents of any particular store file.
  // Registration needs a checkpoint; while it is pending, the other writes
  // should fail before their records are admitted. Restore before close().
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

// sh's limit of 32 blocks admits small seed records but not a 64 KiB payload
// (whether the shell counts in 512-byte or 1024-byte blocks). Only this child
// is limited; the test can open the store again with no limit. The pipe keeps
// fixtures, including large attempted payloads, out of argv and off disk.
function runLimited(fixture) {
  execFileSync('sh', ['-c', 'ulimit -f 32 && exec "$@"', 'limited-store',
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

  // pins treeSet.5: failure replacing a real file throws in the call.
  assert.throws(() => side.treeSet(tree, file, revised), caught);
  // pins treeSet.9: report the file-size limit's errno to workspace callers.
  assert.equal(errnoCode(cause), 'EFBIG', String(cause));
  // pins treeSet.10: read the complete previous tuple, consistent with no new record.
  assert.deepEqual([side.treeGet(tree, file), updateIds(side)], [original, ids]);

  assert.throws(() => side.treeSet(tree, checkpoint, replacement), caught);
  assert.equal(errnoCode(cause), 'EFBIG', String(cause));
  // pins treeSet.13: a refused checkpoint save leaves the old entry AND its list available to undo.
  assert.deepEqual(savedCheckpoint(side), saved);

  // pins treeRemove.2: a failure evicting an existing key throws synchronously.
  assert.throws(() => side.treeRemove(tree, checkpoint), caught);
  // pins treeRemove.6: the removal failure retains the OS cause.
  assert.equal(errnoCode(cause), 'EFBIG', String(cause));
  // pins treeRemove.12: eviction did not discard the checkpoint entry before throwing.
  assert.deepEqual(side.treeGet(tree, checkpoint), saved.entry);

  // pins treeBatch.4: the well-formed mixed batch fails in the core and throws in JS.
  assert.throws(() => side.treeBatch(tree, treeOps(fixture)), caught);
  // pins treeBatch.7: the workspace can report the batch's specific OS failure.
  assert.equal(errnoCode(cause), 'EFBIG', String(cause));
  // pins treeGet.12: check each attempted set and removal, including the absent key.
  assert.deepEqual(tuples(side), [original, saved.entry, null]);
  // pins treeBatch.3: no batch record (or first few edit records) landed.
  assert.deepEqual(updateIds(side), ids);

  // pins appendJson.7: a journal append refusal throws before the caller can cache success.
  assert.throws(() => side.appendJson(journal, { event: 'not published' }), caught);
  // pins appendJson.17: expose the actual size-limit errno for the journal failure.
  assert.equal(errnoCode(cause), 'EFBIG', String(cause));
  assert.deepEqual(side.getRecordIdsByType(journal), journalIds);
}

const stamp = (item, record) => ({ ...item, by: record.id, at: record.sequence });

// A read error while the write fault is in force leaves the outcome unknown.
// Catch only the read itself, never an assertion or a mismatching result. The
// cold pass requires a successful read, so this can't vacuously prove recovery.
function readBack(read, whileFaulted) {
  try { return { value: read() }; } catch (error) {
    if (!whileFaulted) throw error;
    return { unavailable: true };
  }
}

function checkIdentity(side, { kind, rows, ids, items }, whileFaulted) {
  // Decide what landed from the records, NOT from the state read under test.
  const now = updateIds(side);
  assert.deepEqual(now.slice(0, ids.length), ids);
  const extra = now.slice(ids.length);
  assert.ok(extra.length <= 1, 'one write can publish at most one owning record');
  let landed = [];
  if (extra.length) {
    const record = side.getRecord(extra[0]);
    const update = JSON.parse(side.getStateUpdateJson(extra[0]));
    assert.equal(update.state_id, 'index');
    const bodies = kind === 'single' ? [update.operation.Append] : update.operation.AppendMany;
    landed = bodies.map(bytes => JSON.parse(Buffer.from(bytes).toString('utf8')));
    // The owner's actual id and sequence are the oracle for the rereads below.
    assert.deepEqual(landed, items.map(item => stamp(item, record)), 'only a complete, correctly stamped update may land');
  }
  if (kind === 'single') {
    const next = readBack(() => side.getStateItemJson('index', rows.length), whileFaulted);
    if (!next.unavailable) {
      // pins appendToStateJsonWithIdentity.11: the pre-append ordinal is absent or holds the entire owning record's stamped item.
      assert.deepEqual(next.value, landed[0] ?? null);
    }
  }
  const full = readBack(() => side.getStateJson('index'), whileFaulted);
  if (!full.unavailable) {
    // pins getStateJson.15: successful reread agrees with independently recorded items, never a partial batch.
    assert.deepEqual(full.value, [...rows, ...landed]);
  }
}

// Re-enter just this file as an ordinary Node process, not another test runner.
// Don't close the limited writer: the parent checks what a cold reader finds
// without a graceful shutdown checkpoint covering up a partial publication.
if (process.argv[2] === '--limited') {
  // Suppress the signal's default termination. Observing it distinguishes an
  // actual limit hit from a validation throw, without requiring the identity
  // methods to expose any particular error code or message. The callback runs
  // only when JS yields, AFTER the synchronous throw and immediate rereads.
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
      // pins appendManyToStateJsonWithIdentity.15: a mid-write error, not just validation, throws before returning.
      assert.throws(() => side.appendManyToStateJsonWithIdentity('index', fixture.items, 'by', 'at'));
    }
    checkIdentity(side, fixture, true);
  }
  const deadline = setTimeout(() => assert.fail('the child saw no SIGXFSZ for the attempted writes'), 5000);
  await hitLimit;
  clearTimeout(deadline);
  if (fixture.kind === 'tree') {
    // No deferred branch edit became visible after the synchronous checks.
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
    // pins currentBranch.12: a failing binding-record read does not mask the branch name.
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
      // pins setStateJson.4: a valid replacement on an open branch throws in the calling stack.
      assert.throws(() => side.setStateJson(binding, after));
      // pins currentBranch.12: a failing binding write leaves its branch identifiable too.
      assert.equal(side.currentBranch().name, 'side');
      assert.throws(() => side.setStateJson('unfold', unfolded));
      // pins setState.8: an I/O refusal of a valid byte write throws during setState itself.
      assert.throws(() => side.setState('receipt', Buffer.from('replacement')));
      // pins appendToStateJson.3: core refusal throws, not a successful or asynchronous return.
      assert.throws(() => side.appendToStateJson('log', { message: 'three' }));
    });
    // Path restored for reads, but no compensating write (or sync) yet.
    // pins setStateJson.13: the previously stored whole branch-local snapshot remains in force.
    assert.deepEqual(root.view('side').getStateJson(binding), before);
    // pins setStateJson.16: known pre-record failure; reread the whole prior unfold snapshot at its prior revision.
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
    // The seed ends beyond the child's ceiling: even a small treeRemove hits
    // EFBIG at record publication. No need to measure/manipulate the log file.
    side.appendJson(journal, { event: 'seed', data: randomBytes(64 * 1024).toString('base64') });
    fixture = { path, kind: 'tree', original, revised, saved, replacement,
      ids: updateIds(side), journalIds: side.getRecordIdsByType(journal) };
  } finally { root.close(); }
  runLimited(fixture);

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
    // pins treeBatch.3: successful mixed edits add exactly one update for this tree, not separate records per path.
    assert.deepEqual([ids.slice(0, -1), JSON.parse(side.getStateUpdateJson(ids.at(-1))).state_id], [fixture.ids, tree]);
    // pins treeBatch.4: the entire batch is visible on return, without awaiting it.
    assert.deepEqual(tuples(side), [fixture.revised, null, fixture.revised]);
  } finally { reopened.close(); }
});

test('Interrupted identity writes expose no partial stamped row or batch', posix, t => {
  for (const kind of ['single', 'batch']) {
    const path = freshPath(t);
    // Incompressible body, with small distinct rows on either side in the
    // batch: a mistaken publication of just the first row is observable.
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
    runLimited(fixture);
    const recovered = JsStore.open({ path });
    try { checkIdentity(recovered.view('side'), fixture, false); }
    finally { recovered.close(); }
  }
});
