import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { closeSync, constants, mkdtempSync, openSync, readFileSync, rmSync } from 'node:fs';
import { open } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { JsStore } from '../index.js';

const dir = mkdtempSync(join(tmpdir(), 'chronicle-blob-async-'));
const path = join(dir, 'store');
const varied = (len, seed) => {
  const bytes = Buffer.alloc(len);
  let state = seed | 1;
  for (let i = 0; i < len; i++) {
    state ^= state << 13; state ^= state >>> 17; state ^= state << 5;
    bytes[i] = state & 0xff;
  }
  return bytes;
};

try {
  let store = JsStore.openOrCreate({ path });

  for (const [i, chunked] of [false, true].entries()) {
    for (const content of [Buffer.from(`small ${i}`), varied(100_000, 1 + i)]) {
      const hash = await store.storeBlobAsync(content, 'application/json', { chunked });
      assert.deepEqual(store.getBlob(hash), content);
      assert.equal(hash, store.storeBlob(content, 'application/json'));
    }
  }

  for (const chunked of [false, true]) {
    const fresh = JsStore.openOrCreate({ path: join(dir, `count-${chunked}`) });
    await fresh.storeBlobAsync(varied(100_000, 3), 'application/json', { chunked });
    const { blobCount } = fresh.stats();
    if (chunked) assert.ok(blobCount > 1, `a chunked store wrote ${blobCount} blob`);
    else assert.equal(blobCount, 1);
    fresh.close();
  }

  const original = varied(100_000, 4);
  const reused = Buffer.from(original);
  const pending = store.storeBlobAsync(reused, 'application/json', { chunked: true });
  reused.fill(0);
  assert.deepEqual(store.getBlob(await pending), original);

  if (process.platform !== 'win32' && !process.versions.bun) {
    const fifos = Array.from({ length: Number(process.env.UV_THREADPOOL_SIZE) || 4 }, (_, i) => join(dir, `fifo-${i}`));
    for (const fifo of fifos) execFileSync('mkfifo', [fifo]);
    const held = fifos.map((fifo) => open(fifo, 'r'));
    let timer;
    try {
      const pooled = [varied(50_000, 30), varied(50_000, 31)];
      const stored = await Promise.race([
        Promise.all(pooled.map((content, i) => store.storeBlobAsync(content, 'application/json', { chunked: i === 1 }))),
        new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('storeBlobAsync waited behind a full libuv pool')), 10_000); }),
      ]);
      assert.deepEqual(stored, pooled.map((content) => createHash('sha256').update(content).digest('hex')));
    } finally {
      clearTimeout(timer);
      const writer = async (fifo) => {
        for (;;) {
          try {
            return openSync(fifo, constants.O_WRONLY | constants.O_NONBLOCK);
          } catch (error) {
            if (error.code !== 'ENXIO') throw error;
            await new Promise((resolve) => setImmediate(resolve));
          }
        }
      };
      for (const fifo of fifos) closeSync(await writer(fifo));
      for (const handle of await Promise.all(held)) await handle.close();
    }
  }

  const view = store.view('main');
  const handles = [store, view];
  const contents = Array.from({ length: 12 }, (_, i) => varied(i % 2 ? 50 : 100_000, 10 + i));
  const hashes = await Promise.all(contents.map((content, i) =>
    handles[i % 2].storeBlobAsync(content, 'application/json', { chunked: i % 3 === 0 })));

  const versionAt = (hash) => readFileSync(join(path, 'blobs', hash.slice(0, 2), hash))[4];
  for (const [i, firstChunked] of [true, false].entries()) {
    const content = varied(100_000, 51 + 2 * i);
    const [first, second] = await Promise.all([
      handles[i].storeBlobAsync(content, 'application/json', { chunked: firstChunked }),
      handles[1 - i].storeBlobAsync(content, 'application/json', { chunked: !firstChunked }),
    ]);
    assert.equal(first, second);
    assert.equal(versionAt(first), firstChunked ? 2 : 1);
  }

  const detached = store.view('main');
  const viewContent = varied(100_000, 40);
  const fromView = detached.storeBlobAsync(viewContent, 'application/json', { chunked: true });
  detached.close();
  assert.throws(() => detached.storeBlobAsync(Buffer.from('x'), 'text/plain'), /View has been closed/);
  assert.deepEqual(store.getBlob(await fromView), viewContent);

  const sync = (() => { try { store.storeBlob(Buffer.from('x'), 'x'.repeat(65_536)); } catch (error) { return error.message; } })();
  await assert.rejects(store.storeBlobAsync(Buffer.from('x'), 'x'.repeat(65_536)), (error) => error.message === sync);

  const lateContents = contents.map((content, i) => Buffer.concat([content, Buffer.from([i])]));
  const late = lateContents.map((content, i) => handles[(i + 1) % 2].storeBlobAsync(content, 'application/json', { chunked: true }));
  store.close();
  assert.throws(() => store.storeBlobAsync(Buffer.from('x'), 'text/plain'), /closed/);
  assert.throws(() => view.storeBlobAsync(Buffer.from('x'), 'text/plain'), /Store has been closed/);
  const reopened = JsStore.open({ path });
  for (const content of lateContents) {
    assert.deepEqual(reopened.getBlob(createHash('sha256').update(content).digest('hex')), content);
  }
  const lateHashes = await Promise.all(late);
  for (const [i, hash] of lateHashes.entries()) assert.deepEqual(reopened.getBlob(hash), lateContents[i]);
  for (const [i, hash] of hashes.entries()) assert.deepEqual(reopened.getBlob(hash), contents[i]);
  reopened.close();
  console.log('storeBlobAsync: all checks passed');
} finally {
  rmSync(dir, { recursive: true, force: true });
}
