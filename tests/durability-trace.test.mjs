import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readTrace } from './durability-trace.mjs';

const FILE = '/r/store/records.log';
const OTHER = '/r/other';
const opened = flags => `1 openat(AT_FDCWD</r>, "${FILE}", ${flags}, 0600) = 3<${FILE}>`;
const write = (bytes, fd = 3) => `1 write(${fd}<${FILE}>, "${'x'.repeat(Math.min(bytes, 32))}"..., ${bytes}) = ${bytes}`;
const pwrite = (bytes, offset) => `1 pwrite64(3<${FILE}>, "${'x'.repeat(Math.min(bytes, 32))}"..., ${bytes}, ${offset}) = ${bytes}`;
const fsync = `1 fsync(3<${FILE}>) = 0`;
const refused = `1 fsync(3<${FILE}>) = -1 EIO (Input/output error) (INJECTED)`;

function unsyncedAtEnd(...calls) {
  const text = [...calls, '1 access("/chronicle-mark/end", F_OK) = -1 ENOENT (No such file or directory)'].join('\n');
  const trace = readTrace(text, { root: '/r/store', cwd: '/r', names: ['/r', '/r/store'] });
  assert.deepEqual(trace.problems, []);
  return trace.marks.at(-1).files;
}

const created = opened('O_RDWR|O_CREAT|O_EXCL|O_CLOEXEC');
const appending = opened('O_WRONLY|O_CREAT|O_EXCL|O_APPEND|O_CLOEXEC');

test('A write elsewhere in the file after a refused fsync leaves the lost bytes unsynced', () => {
  assert.deepEqual(unsyncedAtEnd(created, write(10), refused, pwrite(10, 1000), fsync), [FILE]);
});

test('A write over the lost bytes after a refused fsync puts them on disk at the next fsync', () => {
  assert.deepEqual(unsyncedAtEnd(created, write(10), refused, pwrite(10, 0), fsync), []);
});

test('A write over a part of the lost bytes leaves the rest lost', () => {
  assert.deepEqual(unsyncedAtEnd(created, write(10), refused, pwrite(5, 0), fsync), [FILE]);
  assert.deepEqual(unsyncedAtEnd(created, write(10), refused, pwrite(5, 0), pwrite(5, 5), fsync), []);
});

test('On a descriptor opened with O_APPEND, a cut back to the old length and the same append put the lost bytes back', () => {
  assert.deepEqual(unsyncedAtEnd(appending, write(10), fsync, write(10), refused, `1 ftruncate(3<${FILE}>, 10) = 0`, write(10), fsync), []);
});

test('On a descriptor opened with O_APPEND, an append at the new end leaves the lost bytes unsynced, also when it is longer', () => {
  assert.deepEqual(unsyncedAtEnd(appending, write(10), fsync, write(10), refused, write(10), fsync), [FILE]);
  assert.deepEqual(unsyncedAtEnd(appending, write(10), fsync, write(10), refused, write(30), fsync), [FILE]);
});

test('A pwrite64 on a descriptor opened with O_APPEND lands at the end of the file', () => {
  assert.deepEqual(unsyncedAtEnd(appending, write(10), fsync, write(10), refused, pwrite(10, 10), fsync), [FILE]);
});

test('After a write through a duplicated descriptor and a refused fsync, no later write puts the lost bytes back', () => {
  assert.deepEqual(unsyncedAtEnd(created, `1 dup(3<${FILE}>) = 4<${FILE}>`, write(10, 4), refused, pwrite(10, 0), pwrite(100, 0), fsync), [FILE]);
});

test('A read moves the position of its descriptor, and a pread64 does not', () => {
  const rewind = `1 lseek(3<${FILE}>, 0, SEEK_SET) = 0`;
  assert.deepEqual(unsyncedAtEnd(created, write(10), fsync, write(10), refused, rewind, `1 read(3<${FILE}>, "xxxxxxxxxx", 10) = 10`, write(10), fsync), []);
  assert.deepEqual(unsyncedAtEnd(created, write(10), fsync, write(10), refused, rewind, `1 pread64(3<${FILE}>, "xxxxxxxxxx", 10, 0) = 10`, write(10), fsync), [FILE]);
});

test('After a call that moves the position by an amount that the record does not show, a write lands at no known place until an lseek', () => {
  const copied = `1 sendfile(3<${FILE}>, 5<${OTHER}>, NULL, 4) = 4`;
  assert.deepEqual(unsyncedAtEnd(created, write(10), fsync, write(10), refused, copied, fsync, `1 lseek(3<${FILE}>, 0, SEEK_SET) = 0`, write(20), fsync), []);
  assert.deepEqual(unsyncedAtEnd(created, write(10), fsync, write(10), refused, `1 lseek(3<${FILE}>, 10, SEEK_SET) = 10`, copied, write(10), fsync), [FILE]);
});
