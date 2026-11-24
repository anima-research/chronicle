import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, posix } from 'node:path';
import { after, test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { readTrace } from './durability-trace.mjs';

const WRITER_BOUND_MS = 60_000;
const CALLS = '%file,%desc,mmap,munmap,msync,sync,syncfs,io_uring_setup,io_uring_enter,io_uring_register,io_setup,io_submit';
const WRITER = fileURLToPath(new URL('./durability-writer.mjs', import.meta.url));
const CONTROL_MARKS = ['control-1', 'control-2', 'control-3', 'control-4'];

const inside = (paths, dir) => paths.filter(path => path === dir || path.startsWith(`${dir}/`));
const relative = (paths, dir) => paths.map(path => posix.relative(dir, path)).sort();

function straceProblem() {
  const probe = spawnSync('strace', ['--kill-on-exit', '-f', '-y', '-o', '/dev/null', '-e', `trace=${CALLS}`, 'true'], { encoding: 'utf8', timeout: 10_000, killSignal: 'SIGKILL' });
  if (probe.error?.code === 'ETIMEDOUT') return 'strace did not trace true within 10 s';
  if (probe.error) return `strace is not on the path (${probe.error.code})`;
  if (probe.status !== 0) return `strace may not trace here: ${probe.stderr.trim().split('\n')[0]}`;
  return null;
}

function traceWriter() {
  const base = realpathSync(mkdtempSync(join(tmpdir(), 'chronicle-durability-')));
  after(() => rmSync(base, { recursive: true, force: true }));
  const root = join(base, 'root');
  const control = join(root, 'control');
  mkdirSync(control, { recursive: true });
  const record = join(base, 'record');
  const env = { ...process.env };
  delete env.LD_PRELOAD;
  const child = spawnSync('strace', ['--kill-on-exit', '-f', '-y', '-o', record, '-e', `trace=${CALLS}`, process.execPath, WRITER, root],
    { env, encoding: 'utf8', maxBuffer: 1 << 26, timeout: WRITER_BOUND_MS, killSignal: 'SIGKILL' });
  if (child.error?.code === 'ETIMEDOUT') {
    return { labels: null, failure: `the writer under strace did not end within ${WRITER_BOUND_MS / 1000} s` };
  }
  const firstLine = child.stdout.split('\n')[0];
  const barriers = firstLine.startsWith('[') ? JSON.parse(firstLine) : null;
  const labels = barriers === null ? null : barriers.map(({ label }) => label);
  if (child.status !== 0) {
    const stderr = child.stderr.trim().split('\n').slice(-5).join(' | ');
    return { labels, failure: `the writer under strace ended with status ${child.status}: ${stderr}` };
  }
  if (barriers === null) {
    return { labels, failure: `the writer's first line is not its list of barriers: ${JSON.stringify(firstLine.slice(0, 200))}` };
  }
  const trace = readTrace(readFileSync(record, 'utf8'), { root, cwd: process.cwd(), names: [root, control] });
  const expected = ['begin', ...CONTROL_MARKS,
    ...barriers.flatMap(({ pending, opens }, index) => [...(opens ? [`opened-${index}`] : []), ...(pending ? [`pending-${index}`] : []), `barrier-${index}`]), 'end'];
  const found = trace.marks.map(({ name }) => name);
  const run = { labels, barriers, root, control, stores: barriers.map(({ store }) => join(root, store)), trace, failure: null };
  if (found.join('\n') !== expected.join('\n')) {
    run.failure = `the record's marks are not the writer's marks, each once and in order: found ${found.join(', ')}`;
  } else if (trace.problems.length > 0) {
    const shown = trace.problems.slice(0, 5).map(({ line, why, call }) => `line ${line}, ${why}: ${call}`);
    run.failure = `the record cannot be judged (${trace.problems.length} calls): ${shown.join(' | ')}`;
  }
  return run;
}

const notLinux = process.platform !== 'linux' && 'the record of system calls comes from strace, which is for Linux';
const escaped = /[\x00-\x1f\x7f-\uffff"\\<>]/.test(realpathSync(tmpdir()))
  && `the temporary directory ${JSON.stringify(realpathSync(tmpdir()))} has characters that strace escapes in its record, and the check reads paths unescaped`;
const missing = notLinux ? null : escaped || straceProblem();
const skip = notLinux || (missing !== null && !process.env.CI && missing);
const run = skip ? null : missing !== null ? { labels: null, failure: missing } : traceWriter();

const judged = () => {
  if (run.failure) assert.fail(run.failure);
};
const at = name => run.trace.marks.find(mark => mark.name === name);
const unsyncedIn = (mark, dir) => ({
  files: relative(inside(mark.files, dir), dir),
  names: relative(inside(mark.names, dir).filter(path => path !== dir), dir),
});

const declaredLabels = new Set();
function atBarrier(label, { writesNothing = false, leavesUnsynced = false, makesItsPath = false } = {}) {
  declaredLabels.add(label);
  return () => {
    judged();
    const index = run.labels.indexOf(label);
    assert.ok(index >= 0, `tests/durability-writer.mjs has no barrier named ${label}`);
    const store = run.stores[index];
    if (!writesNothing) {
      const written = relative(inside(at(`pending-${index}`).written, store), store);
      assert.ok(written.length > 0, `Before ${label}, its mutation wrote no file of the store, so the barrier had nothing to do`);
    }
    if (leavesUnsynced) {
      const pending = unsyncedIn(at(`pending-${index}`), store);
      assert.ok(pending.files.length > 0, `Before ${label}, the store had no unsynced file, so the barrier had nothing to do`);
    }
    if (makesItsPath) {
      const parts = posix.relative(run.root, store).split('/');
      const waiting = at(`pending-${index}`).names;
      const paid = parts.map((_, i) => posix.join(run.root, ...parts.slice(0, i + 1)))
        .filter(name => !waiting.includes(name)).map(name => posix.relative(run.root, name));
      assert.deepEqual(paid, [], `Before ${label}, these names on the path to the store were already on disk, so the barrier had none of them to pay: ${paid.join(', ')}`);
    }
    const barrier = at(`barrier-${index}`);
    const unsynced = unsyncedIn(barrier, store);
    const path = barrier.names.filter(name => name === store || store.startsWith(`${name}/`)).map(name => posix.relative(run.root, name));
    assert.deepEqual({ ...unsynced, path }, { files: [], names: [], path: [] },
      `After ${label}, the store had not asked to put these on disk: files [${unsynced.files.join(', ')}], names [${unsynced.names.join(', ')}], names on its path [${path.join(', ')}]`);
  };
}

if (skip) {
  test('sync and close ask for what the store wrote to be put on disk', { skip }, () => {});
} else if (run.labels === null) {
  test('strace records the writer from its start to its end', () => judged());
} else {
  test('The record shows writes, an fsync of one file, an fsync of a directory and a rename of a file that was not synced', () => {
    judged();
    const notMeasured = 'The instrument does not measure';
    assert.deepEqual(unsyncedIn(at('control-1'), run.control), { files: ['first', 'other'], names: ['first', 'other'] },
      `${notMeasured}: the record does not show two writes that were not synced.`);
    assert.deepEqual(unsyncedIn(at('control-2'), run.control), { files: ['other'], names: ['first', 'other'] },
      `${notMeasured}: the record does not show the fsync of one file, or the fsync clears more than that file. A library that LD_PRELOAD or /etc/ld.so.preload loads can answer fsync inside the C library.`);
    assert.deepEqual(unsyncedIn(at('control-3'), run.control), { files: ['other'], names: [] },
      `${notMeasured}: the record does not show the fsync of a directory, or the fsync clears a file.`);
    assert.deepEqual(unsyncedIn(at('control-4'), run.control), { files: ['third'], names: ['third'] },
      `${notMeasured}: the record does not follow a file and its name through a rename.`);
    const renames = run.trace.unsyncedRenames
      .filter(({ from, to }) => inside([from, to], run.control).length === 2)
      .map(({ from, to }) => `${posix.relative(run.control, from)} -> ${posix.relative(run.control, to)}`);
    assert.deepEqual(renames, ['other -> third'],
      `${notMeasured}: the record does not show the rename of a file that was not synced.`);
  });

  test('sync on a new store after one appendJson returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync on a new store after one appendJson', { makesItsPath: true }));
  test('sync after registerState returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync after registerState'));
  test('sync after appendJson returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync after appendJson'));
  test('sync after setStateJson returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync after setStateJson'));
  test('sync after setState returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync after setState'));
  test('sync after appendToStateJson returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync after appendToStateJson'));
  test('sync after appendToStateJsonWithIdentity returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync after appendToStateJsonWithIdentity'));
  test('sync after appendManyToStateJsonWithIdentity returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync after appendManyToStateJsonWithIdentity'));
  test('sync after editStateItem returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync after editStateItem'));
  test('sync after redactStateItems returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync after redactStateItems'));
  test('sync after registerStateFieldIndex returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync after registerStateFieldIndex', { writesNothing: true }));
  test('sync after storeBlob returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync after storeBlob'));
  test('sync after treeSet returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync after treeSet'));
  test('sync after treeRemove returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync after treeRemove'));
  test('sync after treeBatch returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync after treeBatch'));
  test('sync after compactState returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync after compactState'));
  test('sync after updateStateStrategy returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync after updateStateStrategy'));
  test('sync after setAutoSnapshot returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync after setAutoSnapshot', { writesNothing: true }));
  test('sync after createBranch returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync after createBranch'));
  test('sync after switchBranch returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync after switchBranch'));
  test('sync after createBranchWithStates returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync after createBranchWithStates'));
  test('sync after createBranchAt returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync after createBranchAt'));
  test('sync after createBranchAtWithStates returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync after createBranchAtWithStates'));
  test('sync after deleteBranch returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync after deleteBranch'));
  test('sync after a write through a view of a branch that is not the current one returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync after a write through a view of a branch that is not the current one'));
  test('close with writes pending returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('close with writes pending', { leavesUnsynced: true }));
  test('sync on a reopened store after appendJson returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync on a reopened store after appendJson'));
  test('close on a reopened store with a write pending returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('close on a reopened store with a write pending', { leavesUnsynced: true }));
  test('sync on a new store whose parents create made returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync on a new store whose parents create made', { makesItsPath: true }));
  test('sync on a store moved into a directory that chronicle did not make returns after the store asks for each file that it wrote and each name that it made to be put on disk',
    atBarrier('sync on a store moved into a directory that chronicle did not make', { makesItsPath: true }));

  test('Each barrier of the writer has its test', () => {
    judged();
    const untested = run.labels.filter(label => !declaredLabels.has(label));
    assert.deepEqual(untested, [], 'no test names these barriers of tests/durability-writer.mjs');
  });

  test('The store renames no file before it asks for that file to be put on disk', () => {
    judged();
    const renames = run.trace.unsyncedRenames
      .filter(({ from, to }) => run.stores.some(store => inside([from, to], store).length > 0))
      .map(({ from, to }) => `${posix.relative(run.root, from)} -> ${posix.relative(run.root, to)}`);
    assert.deepEqual(renames, [], `The store renamed these files before it asked to put them on disk: ${renames.join(', ')}`);
  });

  test("Each barrier asks for the name of the store's own directory, and of each directory on the path to it, to be put on disk", () => {
    judged();
    const unsynced = run.labels.flatMap((label, index) => {
      const store = run.stores[index];
      return at(`barrier-${index}`).names
        .filter(name => name === store || store.startsWith(`${name}/`))
        .map(name => `after ${label}: ${posix.relative(run.root, name)}`);
    });
    assert.deepEqual(unsynced, [], `The store had not asked to put these names on disk: ${unsynced.join(', ')}`);
  });
}
