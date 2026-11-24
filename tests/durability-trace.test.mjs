import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readTrace } from './durability-trace.mjs';

const FILE = '/r/store/records.log';
const OTHER = '/r/other';
const opened = flags => `1 openat(AT_FDCWD</r>, "${FILE}", ${flags}, 0600) = 3<${FILE}>`;
const shown = bytes => `"${'x'.repeat(Math.min(bytes, 32))}"${bytes > 32 ? '...' : ''}`;
const write = (bytes, fd = 3) => `1 write(${fd}<${FILE}>, ${shown(bytes)}, ${bytes}) = ${bytes}`;
const pwrite = (bytes, offset, fd = 3) => `1 pwrite64(${fd}<${FILE}>, ${shown(bytes)}, ${bytes}, ${offset}) = ${bytes}`;
const seek = (offset, fd = 3) => `1 lseek(${fd}<${FILE}>, ${offset}, SEEK_SET) = ${offset}`;
const fsync = `1 fsync(3<${FILE}>) = 0`;
const refused = `1 fsync(3<${FILE}>) = -1 EIO (Input/output error) (INJECTED)`;

function atEnd(...calls) {
  const text = [...calls, '1 access("/chronicle-mark/end", F_OK) = -1 ENOENT (No such file or directory)'].join('\n');
  const trace = readTrace(text, { root: '/r/store', cwd: '/r', names: ['/r', '/r/store'] });
  assert.deepEqual(trace.problems, []);
  return trace.marks.at(-1);
}
const unsyncedAtEnd = (...calls) => atEnd(...calls).files;

const created = opened('O_RDWR|O_CREAT|O_EXCL|O_CLOEXEC');
const appending = opened('O_WRONLY|O_CREAT|O_EXCL|O_APPEND|O_CLOEXEC');
const second = `1 openat(AT_FDCWD</r>, "${FILE}", O_RDWR|O_CLOEXEC) = 6<${FILE}>`;

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

test('An append on a descriptor opened with O_APPEND lands at the known end of the file, where another descriptor can write it again', () => {
  assert.deepEqual(unsyncedAtEnd(appending, write(10), fsync, write(10), refused, second, pwrite(10, 10, 6), fsync), []);
});

test('A pwrite64 on a descriptor opened with O_APPEND lands at the end of the file', () => {
  assert.deepEqual(unsyncedAtEnd(appending, write(10), fsync, write(10), refused, pwrite(10, 10), fsync), [FILE]);
  assert.deepEqual(unsyncedAtEnd(appending, write(10), fsync, pwrite(10, 0), refused, second, pwrite(10, 10, 6), fsync), []);
});

test('A pwritev2 with RWF_APPEND lands at the end of the file, on any descriptor', () => {
  const appended = `1 pwritev2(3<${FILE}>, [{iov_base="xxxxxxxxxx", iov_len=10}], 1, -1, RWF_APPEND) = 10`;
  assert.deepEqual(unsyncedAtEnd(created, write(10), fsync, write(10), refused, seek(10), appended, fsync), [FILE]);
});

test('After fcntl sets O_APPEND on a descriptor, its writes land at the end of the file', () => {
  const setAppend = `1 fcntl(3<${FILE}>, F_SETFL, O_RDWR|O_APPEND) = 0`;
  assert.deepEqual(unsyncedAtEnd(created, write(10), fsync, write(10), refused, seek(10), setAppend, write(10), fsync), [FILE]);
});

test('After a duplicate of a descriptor moves their shared position, a write through the first lands at no known place', () => {
  const duplicated = `1 dup(3<${FILE}>) = 4<${FILE}>`;
  assert.deepEqual(unsyncedAtEnd(created, write(10), refused, seek(0), duplicated, seek(100, 4), write(10), fsync), [FILE]);
});

test('A write of no bytes writes no file, also at no known place', () => {
  const duplicated = `1 dup(3<${FILE}>) = 4<${FILE}>`;
  assert.deepEqual(atEnd(created, duplicated, `1 write(4<${FILE}>, "", 0) = 0`).written, []);
});

test('A truncating open of a file that O_EXCL creates cuts nothing', () => {
  assert.deepEqual(unsyncedAtEnd(opened('O_RDWR|O_CREAT|O_EXCL|O_TRUNC|O_CLOEXEC'), write(10), refused, pwrite(10, 0), fsync), []);
});

test('After a rename replaces a file, its old descriptor does not give the new file its size', () => {
  const STAGED = '/r/store/staged';
  const staged = `1 openat(AT_FDCWD</r>, "${STAGED}", O_WRONLY|O_CREAT|O_EXCL|O_APPEND|O_CLOEXEC) = 5<${STAGED}>`;
  const stagedWrite = `1 write(5<${STAGED}>, ${shown(25)}, 25) = 25`;
  const stagedSync = `1 fsync(5<${STAGED}>) = 0`;
  const renamed = `1 rename("${STAGED}", "${FILE}") = 0`;
  const oldEnd = `1 lseek(3<${FILE} (deleted)>, 0, SEEK_END) = 10`;
  const refusedNew = `1 fsync(5<${FILE}>) = -1 EIO (Input/output error) (INJECTED)`;
  assert.deepEqual(unsyncedAtEnd(created, write(10), fsync, staged, stagedWrite, stagedSync, renamed, oldEnd,
    `1 write(5<${FILE}>, ${shown(10)}, 10) = 10`, refusedNew, second, pwrite(10, 10, 6), `1 fsync(6<${FILE}>) = 0`), [FILE]);
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
