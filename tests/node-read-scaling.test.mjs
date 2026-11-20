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

function readFixture(prefix) {
  const dir = mkdtempSync(join(tmpdir(), 'chronicle-read-scaling-'));
  const reader = JsStore.openOrCreate({ path: join(dir, 'store') });
  const writer = reader.view('main');
  const close = () => {
    writer.close();
    reader.close();
    rmSync(dir, { recursive: true, force: true });
  };
  try {
    writer.registerState({ id: 'log', strategy: 'append_log', deltaSnapshotEvery: 1000, fullSnapshotEvery: 1000 });
    const before = writer.currentSequence();
    for (let start = 0; start < prefix; start += 16) {
      writer.appendManyToStateJson('log', Array.from({ length: 16 }, (_, i) => item(start + i - prefix)));
    }
    assert.ok(writer.currentSequence() - before >= prefix / 16);

    assert.equal(JSON.parse(reader.getStateSlice('log', 0, prefix).toString('utf8')).length, prefix);
    assert.deepEqual(reader.getStateItemJson('log', prefix - 2), item(-2));
  } catch (error) {
    close();
    throw error;
  }
  let added = 0;
  const sample = () => {
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

    const total = prefix + added;
    const positions = Array.from({ length: 96 }, (_, i) => Math.floor(((i * 0.6180339887498949) % 1) * total));
    const found = new Array(positions.length);
    const start = process.cpuUsage();
    for (let i = 0; i < positions.length; ++i) found[i] = reader.getStateItemJson('log', positions[i]);
    const pointTime = cpuMicrosSince(start);
    assert.deepEqual(found, positions.map((position) => item(position - prefix)));

    const width = 3;
    const offsets = Array.from({ length: 96 }, (_, i) => Math.floor(((i * 0.7548776662466927 + 0.5) % 1) * (total - width)));
    const pages = new Array(offsets.length);
    const windowStart = process.cpuUsage();
    for (let i = 0; i < offsets.length; ++i) pages[i] = reader.getStateSlice('log', offsets[i], width);
    const windowTime = cpuMicrosSince(windowStart);
    assert.deepEqual(pages.map((page) => JSON.parse(page.toString('utf8'))),
      offsets.map((offset) => Array.from({ length: width }, (_, k) => item(offset + k - prefix))));
    return { point: pointTime, slice: sliceTime, window: windowTime };
  };
  return { sample, close };
}

function interleaved(fixtures, rounds = 5) {
  const samples = fixtures.map(() => []);
  for (let round = 0; round < rounds; ++round) fixtures.forEach((fixture, i) => samples[i].push(fixture.sample()));
  return samples;
}

const least = (samples, key) => Math.min(...samples.map((sample) => (key === undefined ? sample : sample[key])));

test('Warm point, page and suffix conversion do not scale with the untouched prefix', (t) => {
  const fixtures = [readFixture(256), readFixture(32768)];
  t.after(() => fixtures.forEach((fixture) => fixture.close()));
  const [small, large] = interleaved(fixtures);
  const floor = 3_000;
  for (const [key, name] of [['point', 'point'], ['slice', 'suffix'], ['window', 'page']]) {
    assert.ok(least(small, key) > 0, `the CPU clock counted no time for the ${name} batch`);
    assert.ok(least(large, key) < 6 * least(small, key) + floor,
      `${name} batch grew from ${least(small, key)} to ${least(large, key)} µs CPU`);
  }
});

function branchIdFixture({ messages, branches }) {
  const dir = mkdtempSync(join(tmpdir(), 'chronicle-branch-id-'));
  const store = JsStore.openOrCreate({ path: join(dir, 'store') });
  const close = () => {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  };
  try {
    store.registerState({ id: 'log', strategy: 'append_log', deltaSnapshotEvery: 1000, fullSnapshotEvery: 1000 });
    let parent = 'main';
    for (let i = 0; i < branches; ++i) parent = store.createEmptyBranch(`other-${i}`, parent).name;
    store.switchBranch(parent);
    const active = store.createBranch('active');
    store.switchBranch('active');
    assert.equal(store.currentBranch().id, active.id);
    const before = store.currentSequence();
    for (let i = 0; i < messages; ++i) store.appendToStateJson('log', item(i));
    assert.ok(store.currentSequence() - before >= messages);
  } catch (error) {
    close();
    throw error;
  }
  const sample = () => {
    const start = process.cpuUsage();
    for (let i = 0; i < 2000; ++i) store.currentBranch().id;
    return cpuMicrosSince(start);
  };
  return { sample, close };
}

test("The current branch's ID costs the same with a long history or many branches", (t) => {
  const fixtures = [{ messages: 128, branches: 2 }, { messages: 8192, branches: 2 }, { messages: 128, branches: 256 }].map(branchIdFixture);
  t.after(() => fixtures.forEach((fixture) => fixture.close()));
  const [small, deep, wide] = interleaved(fixtures).map((samples) => least(samples));
  const floor = 3_000;
  assert.ok(small > 0, 'the CPU clock counted no time for 2,000 reads');
  assert.ok(deep < 3 * small + floor && wide < 3 * small + floor,
    `2,000 reads took ${small} µs CPU, ${deep} with a long history, ${wide} with many branches`);
});
