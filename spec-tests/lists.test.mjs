import assert from 'node:assert/strict';
import { test } from 'node:test';
import { ErrorCode, freshStore, refuses } from './support.mjs';

function listStore(...items) {
  const { store } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log' });
  for (const item of items) store.appendToStateJson('list', item);
  return store;
}

test('A34 A39 appendToState and editStateItem answer with the record of the change', () => {
  const store = listStore(1);
  for (const write of [() => store.appendToState('list', Buffer.from('2')), () => store.editStateItem('list', 0, Buffer.from('3'))]) {
    const record = write();
    assert.equal(record.recordType, 'state_update');
    assert.equal(record.sequence, store.currentSequence());
  }
  assert.deepEqual(store.getStateJson('list'), [3, 2]);
  store.close();
});

test('A36 when idField and sequenceField are the same key, the item holds the sequence there', () => {
  const store = listStore();
  const record = store.appendToStateJsonWithIdentity('list', { k: 'mine', n: 1 }, 'k', 'k');
  assert.deepEqual(store.getStateItemJson('list', 0), { k: record.sequence, n: 1 });
  store.close();
});

test('A36 E2 appendToStateJsonWithIdentity throws for an item that is no JSON object, and adds no record', () => {
  const store = listStore();
  const before = store.currentSequence();
  for (const item of [1, 'text', [1], null]) {
    refuses(() => store.appendToStateJsonWithIdentity('list', item, 'id', 'seq'), ErrorCode.InvalidOperation, /^Invalid operation: /);
  }
  assert.equal(store.currentSequence(), before);
  assert.equal(store.getStateLen('list'), null);
  store.close();
});

test('A39 A44 editStateItem does not check that the new value is JSON, and a read of an item then throws Deserialization', () => {
  const store = listStore(1, 2);
  const record = store.editStateItem('list', 1, Buffer.from('not json'));
  assert.equal(record.sequence, store.currentSequence());
  assert.equal(store.getStateLen('list'), 2);
  refuses(() => store.getStateItemJson('list', 0), ErrorCode.Deserialization, /^Deserialization error: /);
  store.close();
});

test('A40 redactStateItems ignores the part of a range beyond the end, removes nothing for a start not below the end, and answers with its record either way', () => {
  const store = listStore(0, 1, 2, 3);
  const past = store.redactStateItems('list', 2, 10);
  assert.deepEqual(store.getStateJson('list'), [0, 1]);
  assert.equal(past.sequence, store.currentSequence());
  for (const [start, end] of [[1, 1], [1, 0], [5, 9]]) {
    const record = store.redactStateItems('list', start, end);
    assert.equal(record.recordType, 'state_update');
    assert.equal(record.sequence, store.currentSequence());
    assert.deepEqual(store.getStateJson('list'), [0, 1]);
  }
  store.close();
});

test("A41 getStateLen is zero for a list with no items, and null when the state has no value on the handle's branch", () => {
  const store = listStore(1);
  store.registerState({ id: 'unwritten', strategy: 'append_log' });
  assert.equal(store.getStateLen('unwritten'), null);
  store.redactStateItems('list', 0, 1);
  assert.equal(store.getStateLen('list'), 0);
  store.close();
});

test('A42 stateHeadOffset changes when the store adds a record of its own for the state, and two branches that share an unchanged value get the same number', () => {
  const { store } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log', deltaSnapshotEvery: 2 });
  store.appendToStateJson('list', 1);
  store.createBranch('side');
  assert.equal(store.view('side').stateHeadOffset('list'), store.stateHeadOffset('list'));
  const before = store.stateHeadOffset('list');
  store.compactState('list');
  assert.deepEqual(store.getStateJson('list'), [1]);
  assert.notEqual(store.stateHeadOffset('list'), before);
  assert.equal(store.view('side').stateHeadOffset('list'), before);
  store.close();
});

test("A43 A46 getStateSlice and getStateTail are null when the state has no value on the handle's branch", () => {
  const store = listStore();
  assert.equal(store.getStateSlice('list', 0, 1), null);
  assert.equal(store.getStateTail('list', 1), null);
  store.close();
});

test('A43 N9 getStateSlice gives the JSON text of an array of the items, each with its keys in the order written', () => {
  const store = listStore();
  store.appendToState('list', Buffer.from('{ "z" : 1,  "a" : {"y":2,"b":3} }'));
  store.appendToStateJson('list', { m: 1, c: [{ x: 1, d: 2 }] });
  const slice = store.getStateSlice('list', 0, 2);
  assert.deepEqual(JSON.parse(slice.toString()), [{ z: 1, a: { y: 2, b: 3 } }, { m: 1, c: [{ x: 1, d: 2 }] }]);
  assert.deepEqual(JSON.parse(slice.toString()).map((item) => JSON.stringify(item)), ['{"z":1,"a":{"y":2,"b":3}}', '{"m":1,"c":[{"x":1,"d":2}]}']);
  store.close();
});

test('A44 getStateItemJson is null on a branch where the state has no value, and bytes of length zero read as a list with no items', () => {
  const store = listStore(1);
  store.createEmptyBranch('bare', 'main');
  assert.equal(store.view('bare').getStateItemJson('list', 0), null);
  store.setState('list', Buffer.alloc(0));
  assert.equal(store.getStateItemJson('list', 0), null);
  assert.deepEqual(store.getStateItemsJson('list', [0]), [null]);
  store.close();
});

test('A45 with no index of zero or more, getStateItemsJson reads nothing and gives null for each entry, also for bytes that are no JSON', () => {
  const { store } = freshStore();
  store.registerState({ id: 'broken', strategy: 'snapshot' });
  store.setState('broken', Buffer.from('{'));
  assert.deepEqual(store.getStateItemsJson('broken', [-1, -3]), [null, null]);
  refuses(() => store.getStateItemsJson('broken', [0]), ErrorCode.Deserialization, /^Deserialization error: /);
  store.close();
});

test('A46 getStateTail gives every item when the list has fewer than count', () => {
  const store = listStore(1, 2);
  assert.deepEqual(JSON.parse(store.getStateTail('list', 5).toString()), [1, 2]);
  store.close();
});

function indexed() {
  const store = listStore(
    { t: 30, tag: 'b' }, { t: 10, tag: 'a' }, { t: 20, tag: 'b' }, { t: 'text', tag: 'c' }, { other: 1 }, { t: 20, tag: 'a' }, { t: 40, tag: 'b' },
  );
  store.registerStateFieldIndex('list', '/t', 'number');
  store.registerStateFieldIndex('list', '/tag', 'string');
  return store;
}

test('A76 A77 T17 queryStateIndexRange gives the indexes of the items whose number lies in the range, both bounds included, a bound left out open, in rising order of the field', () => {
  const store = indexed();
  assert.deepEqual(store.queryStateIndexRange('list', '/t', { gte: 20, lte: 30 }), [2, 5, 0]);
  assert.deepEqual(store.queryStateIndexRange('list', '/t', { gte: 25 }), [0, 6]);
  assert.deepEqual(store.queryStateIndexRange('list', '/t', { lte: 10 }), [1]);
  assert.deepEqual(store.queryStateIndexRange('list', '/t', {}), [1, 2, 5, 0, 6]);
  assert.deepEqual(store.queryStateIndexRange('list', '/t', { gte: 50 }), []);
  store.close();
});

test('A77 T17 queryStateIndexRange takes reverse, offset and limit', () => {
  const store = indexed();
  assert.deepEqual(store.queryStateIndexRange('list', '/t', { reverse: true }), [6, 0, 5, 2, 1]);
  assert.deepEqual(store.queryStateIndexRange('list', '/t', { offset: 1, limit: 2 }), [2, 5]);
  assert.deepEqual(store.queryStateIndexRange('list', '/t', { reverse: true, offset: 1, limit: 2 }), [0, 5]);
  store.close();
});

test("A77 queryStateIndexRange is null where the handle's branch has no index of the kind number on the field", () => {
  const store = indexed();
  assert.equal(store.queryStateIndexRange('list', '/tag', {}), null);
  assert.equal(store.queryStateIndexRange('list', '/none', {}), null);
  store.createBranch('side');
  assert.equal(store.view('side').queryStateIndexRange('list', '/t', {}), null);
  store.close();
});

test('A76 a second registerStateFieldIndex for an index that is there and current does nothing', () => {
  const store = indexed();
  const before = store.currentSequence();
  store.registerStateFieldIndex('list', '/t', 'number');
  assert.equal(store.currentSequence(), before);
  assert.deepEqual(store.queryStateIndexRange('list', '/t', {}), [1, 2, 5, 0, 6]);
  store.close();
});

test('A78 T18 queryStateIndexEq takes offset and limit', () => {
  const store = indexed();
  assert.deepEqual(store.queryStateIndexEq('list', '/tag', 'b', {}), [0, 2, 6]);
  assert.deepEqual(store.queryStateIndexEq('list', '/tag', 'b', { offset: 1 }), [2, 6]);
  assert.deepEqual(store.queryStateIndexEq('list', '/tag', 'b', { limit: 2 }), [0, 2]);
  assert.equal(store.queryStateIndexEq('list', '/t', '10', {}), null);
  store.close();
});

test('A79 T19 getStateIndexValueCounts gives each text of the field with how many items have it, and null without an index of the kind string', () => {
  const store = indexed();
  const counts = store.getStateIndexValueCounts('list', '/tag');
  assert.deepEqual(Object.fromEntries(counts.map(({ value, count }) => [value, count])), { a: 2, b: 3, c: 1 });
  assert.equal(store.getStateIndexValueCounts('list', '/t'), null);
  assert.equal(store.getStateIndexValueCounts('list', '/none'), null);
  store.close();
});

test('N9 the entries of a tree come by path, whatever order they were set in', () => {
  const { store } = freshStore();
  store.registerState({ id: 'files', strategy: 'tree' });
  for (const path of ['b', 'a/z', 'c', 'a/b']) store.treeSet('files', path, { blobHash: 'h', size: 1, mode: 0 });
  assert.deepEqual(store.treeList('files').map((entry) => entry.path), ['a/b', 'a/z', 'b', 'c']);
  store.close();
});
