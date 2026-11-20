import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtempSync, renameSync, rmSync, statSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, test } from 'node:test';
import { JsStore } from '../index.js';

const dir = mkdtempSync(join(tmpdir(), 'chronicle-blob-keys-'));
after(() => rmSync(dir, { recursive: true, force: true }));

test('Synchronous blob keys address exact bytes independently of MIME or provenance', (t) => {
  const syncPath = join(dir, 'sync-blobs');
  let store = JsStore.openOrCreate({ path: syncPath });
  t.after(() => store.close());
  const digest = (bytes) => createHash('sha256').update(bytes).digest('hex');
  const bytes = Buffer.from([0xfb, 0xff]);
  const expectedKey = digest(bytes);
  const key = store.storeBlob(bytes, 'application/vnd.rookery.media');
  const retained = store.getBlob(key);
  assert.deepEqual(retained, bytes);
  assert.deepEqual([key, store.storeBlob(bytes, 'image/jpeg')], [expectedKey, expectedKey]);
  assert.equal(retained.toString('base64'), '+/8=');

  const emptyKey = store.storeBlob(Buffer.alloc(0), 'application/octet-stream');
  assert.deepEqual(store.getBlob(emptyKey), Buffer.alloc(0));
  assert.equal(store.getBlob(digest('never stored')), null);

  const json = '{\n  "bird": "Mésange 🪶",\n  "count": 3\n}\n';
  const saved = store.getBlob(store.storeBlob(Buffer.from(json), 'application/json'));
  assert.deepEqual([saved.toString(), saved.toString('utf8'), saved.toString('utf-8')], [json, json, json]);

  const nextKey = store.storeBlob(Buffer.from([0x12, 0x34]), 'application/octet-stream');
  writeFileSync(join(dir, 'materialized'), store.getBlob(nextKey));
  assert.deepEqual(retained, Buffer.from([0xfb, 0xff]));

  const request = { prompt: 'draft' };
  const provenanceKey = digest(JSON.stringify(request));
  request.prompt = 'revised';
  const preimage = Buffer.from(JSON.stringify(request));
  const actualKey = digest(preimage);
  store.storeBlob(preimage, 'application/json');
  store.close();
  store = JsStore.open({ path: syncPath });
  assert.deepEqual([store.getBlob(actualKey), store.getBlob(provenanceKey)], [preimage, null]);

  for (const invalidType of [undefined, null, 42]) {
    assert.throws(() => store.storeBlob(Buffer.from('untyped media'), invalidType));
  }
});

test('References on distinct branches resolve the same shared bytes after rebind', () => {
  const branchPath = join(dir, 'shared-blobs');
  const source = JsStore.openOrCreate({ path: branchPath });
  const original = Buffer.from('original file bytes');
  let key, sourceId, childId;
  try {
    source.createBranch('child', 'main');
    const child = source.view('child');
    key = source.storeBlob(original, 'text/plain');
    assert.deepEqual(child.getBlob(key), original);
    const other = Buffer.from('child-only write');
    const otherKey = child.storeBlob(other, 'text/plain');
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
    assert.deepEqual(rebound.getBlob(key), original);
    for (const [handle, id] of [[reopened, sourceId], [rebound, childId]]) {
      const { blobHash } = JSON.parse(handle.getRecord(id).payload.toString());
      assert.equal(blobHash, key);
      assert.deepEqual(handle.getBlob(blobHash), original);
    }
    rebound.close();
  } finally {
    reopened.close();
  }
});

test('openOrCreate honors the supplied blob read-cache capacity, for a new store and an existing one', () => {
  const contents = ['oldest', 'middle', 'newest'].map((s) => Buffer.from(s));
  const readOrder = [2, 0, 1];
  for (const capacity of [1, 2]) {
    for (const existing of [false, true]) {
      const at = join(dir, `cache-${capacity}-${existing ? 'existing' : 'new'}`);
      const offline = `${at}-offline`;
      if (process.platform === 'win32') {
        symlinkSync(mkdtempSync(join(dir, 'backing-')), at, 'junction');
      }
      let keys;
      if (existing) {
        const seed = JsStore.openOrCreate({ path: at });
        try { keys = contents.map((b) => seed.storeBlob(b, 'text/plain')); } finally { seed.close(); }
      }
      const handle = JsStore.openOrCreate({ path: at, blobCacheSize: capacity });
      try {
        keys ??= contents.map((b) => handle.storeBlob(b, 'text/plain'));
        for (const i of readOrder) assert.deepEqual(handle.getBlob(keys[i]), contents[i]);

        renameSync(at, offline);
        try {
          const reads = keys.map((key) => {
            try { return handle.getBlob(key); }
            catch { return null; }
          });
          const kept = new Set(readOrder.slice(-capacity));
          assert.deepEqual(reads, contents.map((b, i) => kept.has(i) ? b : null),
            `${existing ? 'an existing' : 'a new'} store opened with blobCacheSize ${capacity}`);
        } finally {
          renameSync(offline, at);
        }
      } finally {
        handle.close();
      }
    }
  }
});

test('After a repair of a damaged blob, the handle that repaired it and a reopened store read the same thing', {
  todo: 'After a get has read a blob, storeBlob returns without rewriting a damaged file of the same length: the handle reads its cached bytes, and a reopened store finds the file damaged.',
}, (t) => {
  const path = join(dir, 'repaired-blobs');
  let store = JsStore.openOrCreate({ path });
  t.after(() => store.close());
  const bytes = Buffer.from(Array.from({ length: 4096 }, (_, i) => (i * 7) % 251));
  const key = store.storeBlob(bytes, 'application/octet-stream');
  assert.deepEqual(store.getBlob(key), bytes);
  const file = join(path, 'blobs', key.slice(0, 2), key);
  writeFileSync(file, Buffer.alloc(statSync(file).size));
  assert.equal(store.storeBlob(bytes, 'application/octet-stream'), key);
  const read = () => {
    try { return { bytes: store.getBlob(key) }; } catch (error) { return { code: error.code }; }
  };
  const repairer = read();
  store.close();
  store = JsStore.open({ path });
  assert.deepEqual(read(), repairer);
});
