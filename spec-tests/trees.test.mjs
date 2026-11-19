import assert from 'node:assert/strict';
import { test } from 'node:test';
import { ErrorCode, freshStore, refuses } from './support.mjs';

const entry = (n) => ({ blobHash: `hash-${n}`, size: 1000 + n, mode: 0o100644 + n });

function treeStore() {
  const { store } = freshStore();
  store.registerState({ id: 'files', strategy: 'tree' });
  return store;
}

test('A69 A72 T24 treeSet sets the entry of a path on the handle\'s branch, and treeGet gives it back as the caller gave it', () => {
  const store = treeStore();
  store.createBranch('side');
  const side = store.view('side');
  const big = { blobHash: 'any text', size: 123_456_789, mode: 0o100755 };
  side.treeSet('files', 'dir/a.txt', big);
  assert.deepEqual(side.treeGet('files', 'dir/a.txt'), big);
  assert.equal(store.treeGet('files', 'dir/a.txt'), null);
  store.close();
});

test('A69 A70 A71 treeSet, treeRemove and treeBatch each answer with the record of the change', () => {
  const store = treeStore();
  for (const write of [
    () => store.treeSet('files', 'a', entry(1)),
    () => store.treeRemove('files', 'a'),
    () => store.treeBatch('files', [{ op: 'set', path: 'b', entry: entry(2) }]),
  ]) {
    const before = store.currentSequence();
    const record = write();
    assert.equal(record.recordType, 'state_update');
    assert.equal(record.sequence, before + 1);
    assert.equal(JSON.parse(store.getStateUpdateJson(record.id)).state_id, 'files');
  }
  store.close();
});

test('A69 A70 A71 E2 treeSet, treeRemove and treeBatch throw InvalidOperation for a state not registered as a tree', () => {
  const { store } = freshStore();
  store.registerState({ id: 'value', strategy: 'snapshot' });
  for (const id of ['value', 'unregistered']) {
    refuses(() => store.treeSet(id, 'a', entry(1)), ErrorCode.InvalidOperation, /^Invalid operation: /);
    refuses(() => store.treeRemove(id, 'a'), ErrorCode.InvalidOperation, /^Invalid operation: /);
    refuses(() => store.treeBatch(id, [{ op: 'set', path: 'a', entry: entry(1) }]), ErrorCode.InvalidOperation, /^Invalid operation: /);
  }
  store.close();
});

test('A70 treeRemove removes the path on the handle\'s branch alone', () => {
  const store = treeStore();
  store.treeSet('files', 'a', entry(1));
  store.createBranch('side');
  store.view('side').treeRemove('files', 'a');
  assert.equal(store.view('side').treeGet('files', 'a'), null);
  assert.deepEqual(store.treeGet('files', 'a'), entry(1));
  store.close();
});

test('A71 treeBatch makes its changes in order, as one change with one record', () => {
  const store = treeStore();
  const before = store.currentSequence();
  store.treeBatch('files', [
    { op: 'set', path: 'gone', entry: entry(1) },
    { op: 'remove', path: 'gone' },
    { op: 'remove', path: 'back' },
    { op: 'set', path: 'back', entry: entry(2) },
    { op: 'set', path: 'twice', entry: entry(3) },
    { op: 'set', path: 'twice', entry: entry(4) },
  ]);
  assert.equal(store.currentSequence(), before + 1);
  assert.equal(store.treeGet('files', 'gone'), null);
  assert.deepEqual(store.treeGet('files', 'back'), entry(2));
  assert.deepEqual(store.treeGet('files', 'twice'), entry(4));
  store.close();
});

test('A71 a refused treeBatch changes nothing in the tree', () => {
  const store = treeStore();
  store.treeSet('files', 'kept', entry(1));
  const before = store.currentSequence();
  for (const [bad, message] of [[{ op: 'rename', path: 'x' }, /'rename'/], [{ op: 'set', path: 'x' }, /'set' requires entry/]]) {
    refuses(() => store.treeBatch('files', [{ op: 'set', path: 'new', entry: entry(2) }, { op: 'remove', path: 'kept' }, bad]), ErrorCode.InvalidOperation, message);
  }
  assert.equal(store.currentSequence(), before);
  assert.equal(store.treeGet('files', 'new'), null);
  assert.deepEqual(store.treeGet('files', 'kept'), entry(1));
  store.close();
});

test('A73 T27 treeList gives each path with its entry, or the paths that start with a prefix', () => {
  const store = treeStore();
  for (const [n, path] of ['a/1', 'a/2', 'b/1'].entries()) store.treeSet('files', path, entry(n));
  const listed = store.treeList('files');
  assert.deepEqual(new Set(listed.map((e) => JSON.stringify(e))), new Set(['a/1', 'a/2', 'b/1'].map((path, n) => JSON.stringify({ path, ...entry(n) }))));
  assert.deepEqual(store.treeList('files', 'a/').map((e) => e.path).sort(), ['a/1', 'a/2']);
  assert.deepEqual(store.treeList('files', 'c'), []);
  store.close();
});

test('A74 T26 treeDiff gives one entry for each path that differs between two sequences, added, modified or removed', () => {
  const store = treeStore();
  store.treeSet('files', 'same', entry(1));
  store.treeSet('files', 'changed', entry(2));
  store.treeSet('files', 'removed', entry(3));
  const from = store.currentSequence();
  store.treeSet('files', 'changed', entry(4));
  store.treeRemove('files', 'removed');
  store.treeSet('files', 'added', entry(5));
  const to = store.currentSequence();
  const changes = store.treeDiff('files', from, to);
  assert.equal(changes.length, 3);
  const byPath = Object.fromEntries(changes.map((change) => [change.path, change]));
  assert.deepEqual(Object.keys(byPath).sort(), ['added', 'changed', 'removed']);
  assert.deepEqual(byPath.added, { changeType: 'added', path: 'added', newEntry: entry(5) });
  assert.deepEqual(byPath.changed, { changeType: 'modified', path: 'changed', oldEntry: entry(2), newEntry: entry(4) });
  assert.deepEqual(byPath.removed, { changeType: 'removed', path: 'removed', oldEntry: entry(3) });
  assert.deepEqual(store.treeDiff('files', to, to), []);
  store.close();
});

test("A74 treeDiff compares the trees on the handle's branch", () => {
  const store = treeStore();
  store.treeSet('files', 'a', entry(1));
  const from = store.currentSequence();
  store.createBranch('side');
  const side = store.view('side');
  side.treeSet('files', 'b', entry(2));
  assert.deepEqual(side.treeDiff('files', from, side.currentSequence()).map((c) => c.path), ['b']);
  store.close();
});

test('A75 treeSnapshot adds a short cut of the tree as one record, and the tree reads the same', () => {
  const store = treeStore();
  store.treeSet('files', 'a', entry(1));
  store.treeSet('files', 'b', entry(2));
  const before = store.currentSequence();
  const listed = store.treeList('files');
  const record = store.treeSnapshot('files');
  assert.equal(record.sequence, before + 1);
  assert.equal(store.currentSequence(), before + 1);
  assert.deepEqual(store.treeList('files'), listed);
  store.close();
});

test('A75 treeSnapshot is null when the tree has no value on the branch', () => {
  const store = treeStore();
  const before = store.currentSequence();
  assert.equal(store.treeSnapshot('files'), null);
  assert.equal(store.currentSequence(), before);
  store.close();
});
