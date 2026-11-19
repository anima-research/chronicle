import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readFileSync, statSync, truncateSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { test } from 'node:test';
import { ErrorCode, JsStore, freshStore, refuses, refusesInAForm } from './support.mjs';

const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex');
const blobFile = (dir, hash) => join(dir, 'blobs', hash.slice(0, 2), hash);

test('A10 T2 append stores the payload as its bytes, under a type of up to 65,535 bytes, and answers with the new record', () => {
  const { store } = freshStore();
  const payload = Buffer.from([0, 1, 2, 0xfe, 0xff, 0x7b]);
  const type = 'é'.repeat(32_767) + 't';
  assert.equal(Buffer.byteLength(type), 65_535);
  const record = store.append(type, payload);
  assert.equal(record.recordType, type);
  assert.deepEqual(record.payload, payload);
  assert.deepEqual(store.getRecord(record.id), record);
  store.close();
});

test("A10 A11 the record goes to the handle's branch", () => {
  const { store } = freshStore();
  store.createBranch('side');
  const view = store.view('side');
  const bytes = view.append('note', Buffer.from('x'));
  const parsed = view.appendJson('note', { n: 1 });
  assert.deepEqual(view.query({}).map((r) => r.id), [bytes.id, parsed.id]);
  assert.deepEqual(store.query({}), []);
  store.close();
});

test('A10 A11 E2 append and appendJson throw InvalidOperation for the type state_update', () => {
  const { store } = freshStore();
  refuses(() => store.append('state_update', Buffer.from('{}')), ErrorCode.InvalidOperation, /^Invalid operation: /);
  refuses(() => store.appendJson('state_update', {}), ErrorCode.InvalidOperation, /^Invalid operation: /);
  assert.equal(store.currentSequence(), 0);
  store.close();
});

test('A12 E2 getRecord throws InvalidOperation for a text that is no whole number from 0 to 2^64 - 1', () => {
  const { store } = freshStore();
  for (const id of ['abc', '-1', '1.5', '18446744073709551616', '']) {
    refuses(() => store.getRecord(id), ErrorCode.InvalidOperation, 'Invalid record ID');
  }
  assert.equal(store.getRecord('18446744073709551615'), null);
  store.close();
});

test('A14 getStateUpdateJson is null for an unknown ID and for a record of another type', () => {
  const { store } = freshStore();
  const note = store.appendJson('note', {});
  assert.equal(store.getStateUpdateJson(note.id), null);
  assert.equal(store.getStateUpdateJson(String(BigInt(note.id) + 1n)), null);
  store.close();
});

test("A14 getStateUpdateJson gives six keys, an operation of one key that shows a value's bytes as numbers, and a record_id that is not the record's ID", () => {
  const { store } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log' });
  store.appendJson('note', {});
  store.appendJson('note', {});
  const record = store.appendToStateJson('list', { n: 1 });
  const update = JSON.parse(store.getStateUpdateJson(record.id));
  assert.deepEqual(Object.keys(update).sort(), ['global_sequence', 'operation', 'prev_update_offset', 'record_id', 'state_id', 'timestamp']);
  assert.equal(update.state_id, 'list');
  assert.deepEqual(Object.keys(update.operation), ['Append']);
  assert.ok(update.operation.Append.every((byte) => Number.isInteger(byte) && byte >= 0 && byte <= 255));
  assert.deepEqual(JSON.parse(Buffer.from(update.operation.Append).toString('utf8')), { n: 1 });
  assert.notEqual(String(update.record_id), record.id);
  store.close();
});

test('A15 getRecordIdsByType gives the ID of each record of the type on every branch, in rising order of the IDs as numbers', () => {
  const { store } = freshStore();
  store.createBranch('side');
  const side = store.view('side');
  const ids = [];
  for (let n = 0; n < 12; n++) {
    ids.push((n % 2 ? side : store).appendJson('wanted', { n }).id);
    store.appendJson('other', { n });
  }
  const sorted = [...ids].sort((x, y) => (BigInt(x) < BigInt(y) ? -1 : 1));
  assert.deepEqual(store.getRecordIdsByType('wanted'), sorted);
  assert.deepEqual(side.getRecordIdsByType('wanted'), sorted);
  assert.deepEqual(store.getRecordIdsByType('absent'), []);
  store.close();
});

test('A16 E2 storeBlob throws InvalidOperation for a content type of more than 65,535 bytes, and stores nothing', () => {
  const { store } = freshStore();
  const content = Buffer.from('content');
  const type = 'é'.repeat(32_768);
  assert.equal(type.length, 32_768);
  refuses(() => store.storeBlob(content, type), ErrorCode.InvalidOperation, /^Invalid operation: /);
  assert.equal(store.stats().blobCount, 0);
  assert.equal(store.getBlob(sha256(content)), null);
  assert.equal(store.storeBlob(content, 'é'.repeat(32_767) + 't'), sha256(content));
  store.close();
});

test('A17 T1 D6 chunked is false when left out, a chunked content is kept in parts, and one that the cut leaves in one part is kept whole', async () => {
  for (const [options, content, whole] of [
    [undefined, Buffer.alloc(200_000, 1), true],
    [{ chunked: false }, Buffer.alloc(200_000, 2), true],
    [{ chunked: true }, Buffer.from('small'), true],
    [{ chunked: true }, Buffer.from(Array.from({ length: 300_000 }, (_, i) => (i * 7919) % 251)), false],
  ]) {
    const { store, dir } = freshStore();
    const hash = await store.storeBlobAsync(content, 'application/octet-stream', options);
    const kept = statSync(blobFile(dir, hash)).size;
    if (whole) {
      assert.equal(store.stats().blobCount, 1, JSON.stringify(options));
      assert.ok(kept >= content.length, `${kept} ${content.length}`);
    } else {
      assert.ok(store.stats().blobCount >= 3, String(store.stats().blobCount));
      assert.ok(kept < content.length / 2, `${kept} ${content.length}`);
    }
    assert.deepEqual(store.getBlob(hash), content);
    store.close();
  }
});

test('A18 E2 getBlob throws InvalidOperation for a hash that is not 64 hexadecimal digits', () => {
  const { store } = freshStore();
  for (const hash of ['zz', 'abcd', '0'.repeat(63), '0'.repeat(65), 'g'.repeat(64)]) {
    refuses(() => store.getBlob(hash), ErrorCode.InvalidOperation, 'Invalid hash');
  }
  store.close();
});

test('A18 getBlob gives the bytes stored under the hash as a Buffer, and null when the store holds no such blob', () => {
  const { store } = freshStore();
  const content = Buffer.from([0, 255, 1]);
  const hash = store.storeBlob(content, 'text/plain');
  const got = store.getBlob(hash);
  assert.ok(Buffer.isBuffer(got));
  assert.deepEqual(got, content);
  assert.equal(store.getBlob('0'.repeat(64)), null);
  store.close();
});

test('A18 E2 getBlob throws when the kept copy is damaged or cut short, with a message in the form of its code', () => {
  const { store, dir } = freshStore();
  const damaged = store.storeBlob(Buffer.alloc(1000, 1), 'application/octet-stream');
  const short = store.storeBlob(Buffer.alloc(1000, 2), 'application/octet-stream');
  store.close();
  const bytes = readFileSync(blobFile(dir, damaged));
  bytes[bytes.length - 5] ^= 0xff;
  writeFileSync(blobFile(dir, damaged), bytes);
  truncateSync(blobFile(dir, short), 500);
  const reopened = JsStore.open({ path: dir });
  for (const hash of [damaged, short]) {
    refusesInAForm(() => reopened.getBlob(hash));
  }
  reopened.close();
});

test('A53 T2 T15 appendWithLinks stores both lists of links in the order given, and leaves out an entry that is no ID', () => {
  const { store } = freshStore();
  const a = store.appendJson('a', {});
  const b = store.appendJson('b', {});
  const record = store.appendWithLinks('c', Buffer.from('x'), { causedBy: [b.id, 'nope', a.id], linkedTo: [a.id, '-1'] });
  assert.deepEqual(record.causedBy, [b.id, a.id]);
  assert.deepEqual(record.linkedTo, [a.id]);
  assert.deepEqual(record.payload, Buffer.from('x'));
  assert.deepEqual(store.getRecord(record.id).causedBy, [b.id, a.id]);
  assert.deepEqual(store.getRecord(record.id).linkedTo, [a.id]);
  store.close();
});

test('A54 appendJsonWithLinks is appendJson with the two lists of links', () => {
  const { store } = freshStore();
  const a = store.appendJson('a', {});
  const record = store.appendJsonWithLinks('c', { n: 1 }, { causedBy: [a.id], linkedTo: [a.id] });
  assert.deepEqual(record.payload, Buffer.from('{"n":1}'));
  assert.deepEqual(record.causedBy, [a.id]);
  assert.deepEqual(record.linkedTo, [a.id]);
  const plain = store.appendJsonWithLinks('d', {}, {});
  assert.deepEqual([plain.causedBy, plain.linkedTo], [[], []]);
  store.close();
});

test("A55 query gives the records appended on the handle's branch, in rising order of sequence, or falling with reverse", () => {
  const { store } = freshStore();
  const before = store.appendJson('note', { n: 0 });
  store.createBranch('side');
  const side = store.view('side');
  const onSide = [side.appendJson('note', { n: 1 }), side.appendJson('note', { n: 2 })];
  const onMain = store.appendJson('note', { n: 3 });
  assert.deepEqual(side.query({}).map((r) => r.id), onSide.map((r) => r.id));
  assert.deepEqual(store.query({}).map((r) => r.id), [before.id, onMain.id]);
  assert.deepEqual(side.query({ reverse: true }).map((r) => r.id), onSide.map((r) => r.id).reverse());
  store.close();
});

test('A55 T16 query takes fromSequence and toSequence as an inclusive range, offset and limit, with every type when types is left out', () => {
  const { store } = freshStore();
  const records = Array.from({ length: 6 }, (_, n) => store.appendJson(n % 2 ? 'odd' : 'even', { n }));
  const ids = (list) => list.map((r) => r.id);
  assert.deepEqual(ids(store.query({ fromSequence: records[1].sequence, toSequence: records[3].sequence })), ids(records.slice(1, 4)));
  assert.deepEqual(ids(store.query({ offset: 2, limit: 3 })), ids(records.slice(2, 5)));
  assert.deepEqual(ids(store.query({ offset: 1, limit: 2, reverse: true })), ids(records.slice(3, 5).reverse()));
  assert.deepEqual(ids(store.query({ types: ['odd'] })), ids(records.filter((_, n) => n % 2)));
  assert.equal(store.query({}).length, 6);
  store.close();
});

test('A55 T16 with no limit, query answers at most 10,000 records', () => {
  const { store } = freshStore();
  for (let n = 0; n < 10_001; n++) store.append('note', Buffer.from('x'));
  assert.equal(store.query({}).length, 10_000);
  assert.equal(store.query({ limit: 10_001 }).length, 10_001);
  store.close();
});

test.todo('A55 L10 a query whose fromSequence is after its toSequence answers no records', () => {
  const script = `
    import { mkdtempSync } from 'node:fs';
    import { tmpdir } from 'node:os';
    import { join } from 'node:path';
    import { JsStore } from ${JSON.stringify(new URL('../index.js', import.meta.url).href)};
    const store = JsStore.openOrCreate({ path: join(mkdtempSync(join(tmpdir(), 'chronicle-spec-')), 'store') });
    for (const n of [1, 2, 3]) store.appendJson('note', { n });
    process.stdout.write(JSON.stringify(store.query({ fromSequence: 2, toSequence: 1 })));
  `;
  const child = spawnSync(process.execPath, ['--input-type=module', '-e', script], { encoding: 'utf8' });
  assert.equal(child.status, 0, child.stderr);
  assert.equal(child.stdout, '[]');
});

test.todo('A55 T16 L1 a filter by type answers each matching record up to the limit, however few of the records match', () => {
  const { store } = freshStore();
  for (let n = 0; n < 200; n++) store.append('common', Buffer.from('x'));
  const rare = store.append('rare', Buffer.from('x'));
  assert.deepEqual(store.query({ types: ['rare'], limit: 1 }).map((r) => r.id), [rare.id]);
  store.close();
});

test('A60 A61 getEffects and getLinksTo give the IDs of the records, on any branch, whose causedBy or linkedTo names the record', () => {
  const { store } = freshStore();
  const cause = store.appendJson('cause', {});
  store.createBranch('side');
  const onMain = store.appendJsonWithLinks('effect', {}, { causedBy: [cause.id] });
  const onSide = store.view('side').appendJsonWithLinks('effect', {}, { causedBy: [cause.id], linkedTo: [cause.id] });
  assert.deepEqual(new Set(store.getEffects(cause.id)), new Set([onMain.id, onSide.id]));
  assert.deepEqual(store.getLinksTo(cause.id), [onSide.id]);
  assert.deepEqual(store.getEffects(onMain.id), []);
  assert.deepEqual(store.getLinksTo(onMain.id), []);
  store.close();
});

test('T2 a record carries the time the store accepted it, in microseconds since 1970 UTC', () => {
  const { store } = freshStore();
  const before = Date.now() * 1000;
  const record = store.appendJson('note', {});
  const after = Date.now() * 1000;
  assert.ok(record.timestamp >= before - 60_000_000 && record.timestamp <= after + 60_000_000, `${before} ${record.timestamp} ${after}`);
  store.close();
});
