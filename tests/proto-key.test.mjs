import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, test } from 'node:test';
import { JsStore } from '../index.js';

const dir = mkdtempSync(join(tmpdir(), 'chronicle-proto-key-'));
after(() => rmSync(dir, { recursive: true, force: true }));
const text = (buffer) => buffer.toString('utf8');

const protoSlot = 'conversation-router';
const storeProtoChannel = (at) => {
  const store = JsStore.openOrCreate({ path: at });
  store.registerState({ id: protoSlot, strategy: 'snapshot' });
  store.setStateJson(protoSlot, { generations: Object.fromEntries(new Map([['__proto__', 7], ['general', 2]])) });
  return store;
};

test('A channel named __proto__ keeps its key in the stored bytes', () => {
  const at = join(dir, 'proto-bytes');
  let store = storeProtoChannel(at);
  try {
    assert.equal(text(store.getState(protoSlot)), '{"generations":{"__proto__":7,"general":2}}');
    store.close();
    store = JsStore.openOrCreate({ path: at });
    assert.equal(text(store.getState(protoSlot)), '{"generations":{"__proto__":7,"general":2}}');
  } finally {
    store.close();
  }
});

test('A channel named __proto__ keeps its own generation counter through a snapshot', {
  todo: 'getStateJson reads an own __proto__ key back as nothing: the binding assigns each key of the object it builds, and the __proto__ setter ignores a number.',
}, () => {
  const at = join(dir, 'proto-key');
  let store = storeProtoChannel(at);
  try {
    const own = (handle) => {
      const read = handle.getStateJson(protoSlot).generations;
      return [Object.getPrototypeOf(read) === Object.prototype, Object.getOwnPropertyDescriptor(read, '__proto__')?.value, read.general];
    };
    assert.deepEqual(own(store), [true, 7, 2]);
    store.close();
    store = JsStore.openOrCreate({ path: at });
    assert.deepEqual(own(store), [true, 7, 2]);
  } finally {
    store.close();
  }
});
