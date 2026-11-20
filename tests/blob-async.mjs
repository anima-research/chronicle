import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { closeSync, constants, mkdtempSync, openSync, readFileSync, renameSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { open } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
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
  await test('Synchronous blob keys address exact bytes independently of MIME or provenance', (t) => {
    const syncPath = join(dir, 'sync-blobs');
    let store = JsStore.openOrCreate({ path: syncPath });
    t.after(() => store.close());
    const digest = (bytes) => createHash('sha256').update(bytes).digest('hex');
    const bytes = Buffer.from([0xfb, 0xff]);
    const expectedKey = digest(bytes);
    const key = store.storeBlob(bytes, 'application/vnd.rookery.media');
    const retained = store.getBlob(key);
    // pins storeBlob.1: fresh Buffer bytes round-trip with a caller-defined media type.
    assert.deepEqual(retained, bytes);
    // pins storeBlob.7: both synchronous writes return the independently hashed bytes alone.
    assert.deepEqual([key, store.storeBlob(bytes, 'image/jpeg')], [expectedKey, expectedKey]);
    // pins getBlob.6: binary bytes encode with the standard base64 alphabet and padding.
    assert.equal(retained.toString('base64'), '+/8=');

    const emptyKey = store.storeBlob(Buffer.alloc(0), 'application/octet-stream');
    // pins getBlob.2: zero bytes are still a complete Buffer hit.
    assert.deepEqual(store.getBlob(emptyKey), Buffer.alloc(0));
    // pins getBlob.3: an ordinary miss is exactly null.
    assert.equal(store.getBlob(digest('never stored')), null);

    const json = '{\n  "bird": "Mésange 🪶",\n  "count": 3\n}\n';
    const saved = store.getBlob(store.storeBlob(Buffer.from(json), 'application/json'));
    // pins getBlob.5: default and explicit UTF-8 decodings preserve the authored JSON text.
    assert.deepEqual([saved.toString(), saved.toString('utf8'), saved.toString('utf-8')], [json, json, json]);

    const nextKey = store.storeBlob(Buffer.from([0x12, 0x34]), 'application/octet-stream');
    writeFileSync(join(dir, 'materialized'), store.getBlob(nextKey));
    // pins getBlob.11: a retained read survives another same-size read and intervening I/O.
    assert.deepEqual(retained, Buffer.from([0xfb, 0xff]));

    const request = { prompt: 'draft' };
    const provenanceKey = digest(JSON.stringify(request));
    request.prompt = 'revised';
    const preimage = Buffer.from(JSON.stringify(request));
    const actualKey = digest(preimage);
    store.storeBlob(preimage, 'application/json'); // No reference published, return value discarded.
    store.close();
    store = JsStore.open({ path: syncPath });
    // pins storeBlob.12: cold external lookup finds the actual request bytes, not the stale preimage.
    assert.deepEqual([store.getBlob(actualKey), store.getBlob(provenanceKey)], [preimage, null]);

    for (const invalidType of [undefined, null, 42]) {
      // pins storeBlob.13: the synchronous API must refuse a non-string media type.
      assert.throws(() => store.storeBlob(Buffer.from('untyped media'), invalidType));
    }
  });

  await test('References on distinct branches resolve the same shared bytes after rebind', () => {
    const branchPath = join(dir, 'shared-blobs');
    const source = JsStore.openOrCreate({ path: branchPath });
    const original = Buffer.from('original file bytes');
    let key, sourceId, childId;
    try {
      source.createBranch('child', 'main');
      const child = source.view('child');
      key = source.storeBlob(original, 'text/plain');
      // pins view.8: a non-main view can read bytes newly stored by its source.
      assert.deepEqual(child.getBlob(key), original);
      const other = Buffer.from('child-only write');
      const otherKey = child.storeBlob(other, 'text/plain');
      // pins getBlob.10: source resolves a different branch's write by hash alone.
      assert.deepEqual(source.getBlob(otherKey), other);

      sourceId = source.appendJson('file-version', { path: 'first.txt', blobHash: key }).id;
      childId = child.appendJson('file-version', { path: 'second.txt', blobHash: key }).id;
      assert.notEqual(sourceId, childId);
      child.close();
    } finally {
      source.close();
    }

    const reopened = JsStore.open({ path: branchPath });
    try {
      const rebound = reopened.view('child');
      // pins view.8: even the first read through a fresh view resolves the original source key.
      assert.deepEqual(rebound.getBlob(key), original);
      for (const [handle, id] of [[reopened, sourceId], [rebound, childId]]) {
        const { blobHash } = JSON.parse(handle.getRecord(id).payload.toString());
        assert.equal(blobHash, key);
        // pins getBlob.8: each distinct persisted reference still resolves to the same bytes.
        assert.deepEqual(handle.getBlob(blobHash), original);
      }
      rebound.close();
    } finally {
      reopened.close();
    }
  });

  await test('openOrCreate honors the supplied blob read-cache capacity', () => {
    for (const capacity of [1, 2]) {
      const at = join(dir, `cache-${capacity}`);
      const offline = `${at}-offline`;
      if (process.platform === 'win32') {
        // Move a junction rather than a directory with open files on Windows.
        symlinkSync(mkdtempSync(join(dir, 'backing-')), at, 'junction');
      }
      const handle = JsStore.openOrCreate({ path: at, blobCacheSize: capacity });
      try {
        const contents = ['oldest', 'middle', 'newest'].map((s) => Buffer.from(s));
        const keys = contents.map((b) => handle.storeBlob(b, 'text/plain'));
        for (const [i, key] of keys.entries()) assert.deepEqual(handle.getBlob(key), contents[i]);

        // Take the configured path offline, without depending on any files inside it.
        renameSync(at, offline);
        try {
          const reads = keys.map((key) => {
            try { return handle.getBlob(key); }
            catch { return null; } // An uncached read may fail while storage is unavailable.
          });
          // pins openOrCreate.17: exactly the configured number of recent reads remain available.
          assert.deepEqual(reads, capacity === 1 ? [null, null, contents[2]] : [null, contents[1], contents[2]]);
        } finally {
          renameSync(offline, at);
        }
      } finally {
        handle.close();
      }
    }
  });

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

  const versionAt = (hash, at = path) => readFileSync(join(at, 'blobs', hash.slice(0, 2), hash))[4];
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

  const python = process.platform === 'win32' ? 'python' : 'python3';
  const unchunkAt = (store, ...args) => execFileSync(python, [join(import.meta.dirname, '..', 'tools', 'unchunk-blobs.py'), store, ...args], { encoding: 'utf8', stdio: 'pipe' });
  const unchunked = join(dir, 'unchunk');
  const unchunk = (...args) => unchunkAt(unchunked, ...args);
  const before = JsStore.openOrCreate({ path: unchunked });
  const blobs = [varied(100_000, 61), varied(100_000, 63)];
  const blobHashes = [
    await before.storeBlobAsync(blobs[0], 'application/json', { chunked: true }),
    await before.storeBlobAsync(blobs[1], 'application/json'),
  ];
  before.close();
  assert.match(unchunk(), new RegExp(`^${blobHashes[0]}: 100000 bytes in \\d+ chunks\\r?\\n1 chunked blob`));
  assert.equal(versionAt(blobHashes[0], unchunked), 2);
  unchunk('--apply');
  for (const hash of blobHashes) assert.equal(versionAt(hash, unchunked), 1);
  assert.match(unchunk(), /^0 chunked blob/);
  const after = JsStore.open({ path: unchunked });
  for (const [i, hash] of blobHashes.entries()) assert.deepEqual(after.getBlob(hash), blobs[i]);
  after.close();

  const damagedAt = join(dir, 'unchunk-damaged');
  const damaged = JsStore.openOrCreate({ path: damagedAt });
  const listHash = await damaged.storeBlobAsync(varied(100_000, 65), 'application/json', { chunked: true });
  damaged.close();
  const firstChunkPath = (at, hash) => {
    const list = readFileSync(join(at, 'blobs', hash.slice(0, 2), hash));
    const chunk = list.subarray(7 + list.readUInt16LE(5) + 12, 7 + list.readUInt16LE(5) + 44).toString('hex');
    return join(at, 'blobs', chunk.slice(0, 2), chunk);
  };
  writeFileSync(firstChunkPath(damagedAt, listHash), 'not a blob');
  assert.throws(() => unchunkAt(damagedAt, '--apply'), (error) => error.status !== 0 && /nothing after it was listed or rewritten/.test(error.stderr));
  assert.equal(versionAt(listHash, damagedAt), 2);

  if (process.platform !== 'win32') {
    const fifoAt = join(dir, 'unchunk-fifo');
    const fifo = JsStore.openOrCreate({ path: fifoAt });
    const fifoHash = await fifo.storeBlobAsync(varied(100_000, 67), 'application/json', { chunked: true });
    fifo.close();
    rmSync(firstChunkPath(fifoAt, fifoHash));
    execFileSync('mkfifo', [firstChunkPath(fifoAt, fifoHash)]);
    assert.throws(
      () => execFileSync(python, [join(import.meta.dirname, '..', 'tools', 'unchunk-blobs.py'), fifoAt], { encoding: 'utf8', stdio: 'pipe', timeout: 30_000 }),
      (error) => error.status !== 0 && /is not a regular file/.test(error.stderr),
    );
  }
  console.log('storeBlobAsync: all checks passed');
} finally {
  rmSync(dir, { recursive: true, force: true });
}
