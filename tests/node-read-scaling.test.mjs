import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { JsStore } from '../index.js';

const body = 'a message '.repeat(8);
function item(n) {
  return { by: String(n).padStart(7, '0'), body };
}

function cpuMicrosSince(start) {
  const { user, system } = process.cpuUsage(start);
  return user + system;
}

function measure(prefix) {
  const dir = mkdtempSync(join(tmpdir(), 'chronicle-read-scaling-'));
  let reader, writer;
  try {
    reader = JsStore.openOrCreate({ path: join(dir, 'store') });
    writer = reader.view('main');
    writer.registerState({ id: 'log', strategy: 'append_log', deltaSnapshotEvery: 1000, fullSnapshotEvery: 1000 });
    writer.appendManyToStateJson('log', Array.from({ length: prefix }, (_, i) => item(i - prefix)));

    assert.equal(JSON.parse(reader.getStateSlice('log', 0, prefix).toString('utf8')).length, prefix);
    assert.deepEqual(reader.getStateItemJson('log', prefix - 2), item(-2));

    const pointTimes = [], sliceTimes = [], windowTimes = [];
    let added = 0;
    for (let sample = 0; sample < 5; ++sample) {
      let sliceTime = 0;
      for (let i = 0; i < 16; ++i) {
        const more = [item(added), item(added + 1)];
        const boundary = prefix + added - 1;
        writer.appendManyToStateJson('log', more);
        const start = process.cpuUsage();
        const bytes = reader.getStateSlice('log', boundary, 2 ** 31 - 1);
        sliceTime += cpuMicrosSince(start);
        assert.deepEqual(JSON.parse(bytes.toString('utf8')), [item(added - 1), ...more]);
        added += more.length;
      }
      sliceTimes.push(sliceTime);

      const total = prefix + added;
      const positions = Array.from({ length: 96 }, (_, i) => Math.floor(((i * 0.6180339887498949) % 1) * total));
      const found = new Array(positions.length);
      const start = process.cpuUsage();
      for (let i = 0; i < positions.length; ++i) found[i] = reader.getStateItemJson('log', positions[i]);
      pointTimes.push(cpuMicrosSince(start));
      assert.deepEqual(found, positions.map((position) => item(position - prefix)));

      const width = 3;
      const offsets = Array.from({ length: 96 }, (_, i) => Math.floor(((i * 0.7548776662466927 + 0.5) % 1) * (total - width)));
      const pages = new Array(offsets.length);
      const windowStart = process.cpuUsage();
      for (let i = 0; i < offsets.length; ++i) pages[i] = reader.getStateSlice('log', offsets[i], width);
      windowTimes.push(cpuMicrosSince(windowStart));
      assert.deepEqual(pages.map((page) => JSON.parse(page.toString('utf8'))),
        offsets.map((offset) => Array.from({ length: width }, (_, k) => item(offset + k - prefix))));
    }
    return { point: Math.min(...pointTimes), slice: Math.min(...sliceTimes), window: Math.min(...windowTimes) };
  } finally {
    writer?.close();
    reader?.close();
    rmSync(dir, { recursive: true, force: true });
  }
}

test('Warm point, page and suffix conversion do not scale with the untouched prefix', () => {
  const small = measure(256);
  const large = measure(32768);
  const floor = 20_000;
  assert.ok(large.point < 6 * small.point + floor, `point batch grew from ${small.point} to ${large.point} µs CPU`);
  assert.ok(large.slice < 6 * small.slice + floor, `suffix batch grew from ${small.slice} to ${large.slice} µs CPU`);
  assert.ok(large.window < 6 * small.window + floor, `page batch grew from ${small.window} to ${large.window} µs CPU`);
});

function branchIdTime({ messages, branches }) {
  const dir = mkdtempSync(join(tmpdir(), 'chronicle-branch-id-'));
  let store;
  try {
    store = JsStore.openOrCreate({ path: join(dir, 'store') });
    store.registerState({ id: 'log', strategy: 'append_log', deltaSnapshotEvery: 1000, fullSnapshotEvery: 1000 });
    store.appendManyToStateJson('log', Array.from({ length: messages }, (_, i) => item(i)));
    let parent = 'main';
    for (let i = 0; i < branches; ++i) parent = store.createEmptyBranch(`other-${i}`, parent).name;
    const active = store.createBranch('active');
    store.switchBranch('active');
    assert.equal(store.currentBranch().id, active.id);
    const times = [];
    for (let sample = 0; sample < 5; ++sample) {
      const start = process.cpuUsage();
      for (let i = 0; i < 2000; ++i) store.currentBranch().id;
      times.push(cpuMicrosSince(start));
    }
    return Math.min(...times);
  } finally {
    store?.close();
    rmSync(dir, { recursive: true, force: true });
  }
}

test("The current branch's ID costs the same with a long history or many branches", () => {
  const small = branchIdTime({ messages: 128, branches: 2 });
  const deep = branchIdTime({ messages: 8192, branches: 2 });
  const wide = branchIdTime({ messages: 128, branches: 256 });
  const floor = 20_000;
  assert.ok(deep < 3 * small + floor && wide < 3 * small + floor,
    `2,000 reads took ${small} µs CPU, ${deep} with a long history, ${wide} with many branches`);
});
