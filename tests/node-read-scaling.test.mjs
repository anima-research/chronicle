import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { JsStore } from '../index.js';

const body = 'a message '.repeat(8);
// Number relative to the start of the suffix, so the two sizes receive
// exactly the same queries and return exactly the same JSON, even at -1.
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

    // Don't charge either lookup for its first (possibly cold) build.
    assert.equal(JSON.parse(reader.getStateSlice('log', 0, prefix).toString('utf8')).length, prefix);
    assert.deepEqual(reader.getStateItemJson('log', prefix - 2), item(-2));

    const pointTimes = [], sliceTimes = [];
    let added = 0;
    for (let sample = 0; sample < 5; ++sample) {
      let sliceTime = 0;
      for (let i = 0; i < 16; ++i) {
        // Every slice is the FIRST read of a new head. Re-reading a single
        // unchanged window could hide one full conversion per sibling append.
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

      // Vary the ordinals: a fast path only for the head's last item (or
      // memoizing the last lookup) doesn't make general point reads cheap.
      const offsets = Array.from({ length: 96 }, (_, i) => added - 1 - ((37 * i) % 128));
      const found = new Array(offsets.length);
      const start = process.cpuUsage();
      for (let i = 0; i < offsets.length; ++i) found[i] = reader.getStateItemJson('log', prefix + offsets[i]);
      pointTimes.push(cpuMicrosSince(start));
      assert.deepEqual(found, offsets.map(item));
    }
    // As in reconstruction_scaling.rs, take the best batch CPU time so a
    // single GC pause or a busy host doesn't determine the scaling result.
    return { point: Math.min(...pointTimes), slice: Math.min(...sliceTimes) };
  } finally {
    writer?.close();
    reader?.close();
    rmSync(dir, { recursive: true, force: true });
  }
}

test('Warm point and suffix conversion do not scale with the untouched prefix', () => {
  const small = measure(256);
  const large = measure(32768);
  // The prefix grows 128x (>78x even counting all of the new suffixes).
  // Allow considerable noise, but not the work of parsing or copying it.
  // pins getStateItemJson.7: warm Node lookups convert only the requested item.
  assert.ok(large.point < 6 * small.point, `point batch grew from ${small.point} to ${large.point} µs CPU`);
  // pins getStateSlice.5: reading the old boundary plus new items costs the window, not the accumulated log.
  assert.ok(large.slice < 6 * small.slice, `suffix batch grew from ${small.slice} to ${large.slice} µs CPU`);
});
