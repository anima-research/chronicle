import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, posix } from 'node:path';
import { after, test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { readTrace } from './durability-trace.mjs';

const CALLS = '%file,%desc,mmap,munmap,msync,sync,syncfs,io_uring_setup,io_uring_enter,io_uring_register';
const WRITER = fileURLToPath(new URL('./durability-writer.mjs', import.meta.url));
const CONTROL_MARKS = ['control-1', 'control-2', 'control-3', 'control-4'];

const inside = (paths, dir) => paths.filter(path => path === dir || path.startsWith(`${dir}/`));
const relative = (paths, dir) => paths.map(path => posix.relative(dir, path)).sort();

function straceProblem() {
  const probe = spawnSync('strace', ['-o', '/dev/null', 'true'], { encoding: 'utf8' });
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
  const child = spawnSync('strace', ['-f', '-y', '-o', record, '-e', `trace=${CALLS}`, process.execPath, WRITER, root],
    { env, encoding: 'utf8', maxBuffer: 1 << 26 });
  const firstLine = child.stdout.split('\n')[0];
  const labels = firstLine.startsWith('[') ? JSON.parse(firstLine) : null;
  if (child.status !== 0) {
    const stderr = child.stderr.trim().split('\n').slice(-5).join(' | ');
    return { labels, failure: `the writer under strace ended with status ${child.status}: ${stderr}` };
  }
  const trace = readTrace(readFileSync(record, 'utf8'), { root, cwd: process.cwd(), names: [root, control] });
  const expected = ['begin', ...CONTROL_MARKS, ...labels.map((_, index) => `barrier-${index}`), 'end'];
  const found = trace.marks.map(({ name }) => name);
  const run = { labels, root, control, store: join(root, 'store'), trace, failure: null };
  if (found.join('\n') !== expected.join('\n')) {
    run.failure = `the record's marks are not the writer's marks, each once and in order: found ${found.join(', ')}`;
  } else if (trace.problems.length > 0) {
    const shown = trace.problems.slice(0, 5).map(({ line, why, call }) => `line ${line}, ${why}: ${call}`);
    run.failure = `the record cannot be judged (${trace.problems.length} calls): ${shown.join(' | ')}`;
  }
  return run;
}

const notLinux = process.platform !== 'linux' && 'the record of system calls comes from strace, which is for Linux';
const missing = notLinux ? null : straceProblem();
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

if (skip) {
  test('sync and close ask for what the store wrote to be put on disk', { skip }, () => {});
} else if (run.labels === null) {
  test('strace records the writer from its start to its end', () => judged());
} else {
  test('The record shows a write, an fsync of a file, an fsync of a directory and a rename of a file that was not synced', () => {
    judged();
    const notMeasured = 'The instrument does not measure';
    assert.deepEqual(unsyncedIn(at('control-1'), run.control), { files: ['first'], names: ['first'] },
      `${notMeasured}: the record does not show a write that was not synced.`);
    assert.deepEqual(unsyncedIn(at('control-2'), run.control), { files: [], names: ['first'] },
      `${notMeasured}: the record does not show the fsync of a file. A library that LD_PRELOAD or /etc/ld.so.preload loads can answer fsync inside the C library.`);
    assert.deepEqual(unsyncedIn(at('control-3'), run.control), { files: [], names: [] },
      `${notMeasured}: the record does not show the fsync of a directory.`);
    const renames = run.trace.unsyncedRenames
      .filter(({ from, to }) => inside([from, to], run.control).length === 2)
      .map(({ from, to }) => `${posix.relative(run.control, from)} -> ${posix.relative(run.control, to)}`);
    assert.deepEqual(renames, ['second -> third'],
      `${notMeasured}: the record does not show the rename of a file that was not synced.`);
  });

  run.labels.forEach((label, index) => {
    test(`${label} returns after the store asks for each file that it wrote and each name that it made to be put on disk`, () => {
      judged();
      const unsynced = unsyncedIn(at(`barrier-${index}`), run.store);
      assert.deepEqual(unsynced, { files: [], names: [] },
        `After ${label}, the store had not asked to put these on disk: files [${unsynced.files.join(', ')}], names [${unsynced.names.join(', ')}]`);
    });
  });

  test('The store renames no file before it asks for that file to be put on disk', () => {
    judged();
    const renames = run.trace.unsyncedRenames
      .filter(({ from, to }) => inside([from, to], run.store).length > 0)
      .map(({ from, to }) => `${posix.relative(run.store, from)} -> ${posix.relative(run.store, to)}`);
    assert.deepEqual(renames, [], `The store renamed these files before it asked to put them on disk: ${renames.join(', ')}`);
  });

  test("Each barrier asks for the name of the store's own directory to be put on disk", {
    todo: 'The store makes its directory and never syncs the directory that holds it.',
  }, () => {
    judged();
    const holder = posix.dirname(run.store);
    const unsynced = run.labels.flatMap((label, index) => at(`barrier-${index}`).names
      .filter(name => posix.dirname(name) === holder)
      .map(name => `after ${label}: ${posix.basename(name)}`));
    assert.deepEqual(unsynced, [], `The store had not asked to put these names on disk: ${unsynced.join(', ')}`);
  });
}
