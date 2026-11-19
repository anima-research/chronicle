import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, statSync, truncateSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { join } from 'node:path';
import { test } from 'node:test';
import { ErrorCode, JsStore, freshDir, freshStore, refuses } from './support.mjs';

const pkg = createRequire(import.meta.url)('../index.js');

const ARGS = {
  view: ['main'],
  recovery: [],
  snapshotFailures: [],
  append: ['note', Buffer.from('x')],
  appendJson: ['note', {}],
  getRecord: ['1'],
  getRecords: [['1']],
  getStateUpdateJson: ['1'],
  getRecordIdsByType: ['note'],
  storeBlob: [Buffer.from('x'), 'text/plain'],
  storeBlobAsync: [Buffer.from('x'), 'text/plain'],
  getBlob: ['0'.repeat(64)],
  createBranch: ['other'],
  createBranchWithStates: ['other', null, {}],
  createBranchAtWithStates: ['other', 'main', 0, {}],
  createBranchAt: ['other', 'main', 0],
  createEmptyBranch: ['other'],
  switchBranch: ['main'],
  currentBranch: [],
  listBranches: [],
  deleteBranch: ['other'],
  registerState: [{ id: 'new', strategy: 'snapshot' }],
  updateStateStrategy: [{ id: 'list', strategy: 'append_log' }],
  getState: ['list'],
  getStateJson: ['list'],
  setState: ['value', Buffer.from('1')],
  setStateJson: ['value', 1],
  appendToState: ['list', Buffer.from('1')],
  appendToStateJson: ['list', 1],
  appendToStateJsonWithIdentity: ['list', {}, 'id', 'seq'],
  appendManyToStateJson: ['list', [1]],
  appendManyToStateJsonWithIdentity: ['list', [{}], 'id', 'seq'],
  editStateItem: ['list', 0, Buffer.from('1')],
  redactStateItems: ['list', 0, 1],
  getStateLen: ['list'],
  stateHeadOffset: ['list'],
  getStateSlice: ['list', 0, 1],
  getStateItemJson: ['list', 0],
  getStateItemsJson: ['list', [0]],
  getStateTail: ['list', 1],
  setAutoSnapshot: [true],
  autoSnapshotEnabled: [],
  compactState: ['list'],
  compactAllStates: [],
  getCompactionSummary: [],
  stats: [],
  appendWithLinks: ['note', Buffer.from('x'), {}],
  appendJsonWithLinks: ['note', {}, {}],
  query: [{}],
  listStates: [],
  currentSequence: [],
  getStateAt: ['list', 0],
  getStateJsonAt: ['list', 0],
  getEffects: ['1'],
  getLinksTo: ['1'],
  sync: [],
  subscribe: [],
  unsubscribe: ['1'],
  catchUpSubscription: ['1'],
  subscriptionCount: [],
  treeSet: ['tree', 'a', { blobHash: 'h', size: 1, mode: 0 }],
  treeRemove: ['tree', 'a'],
  treeBatch: ['tree', [{ op: 'remove', path: 'a' }]],
  treeGet: ['tree', 'a'],
  treeList: ['tree'],
  treeDiff: ['tree', 0, 0],
  treeSnapshot: ['tree'],
  registerStateFieldIndex: ['list', '/a', 'string'],
  queryStateIndexRange: ['list', '/a', {}],
  queryStateIndexEq: ['list', '/a', 'x', {}],
  getStateIndexValueCounts: ['list', '/a'],
};
const APART = ['close', 'isClosed', 'pinnedBranch', 'pollSubscription', 'pollSubscriptionTimeout'];
const STORE_WIDE = new Set([
  'view', 'recovery', 'snapshotFailures', 'getRecord', 'getRecords', 'getStateUpdateJson', 'getRecordIdsByType',
  'storeBlob', 'storeBlobAsync', 'getBlob', 'createBranchAtWithStates', 'createBranchAt', 'switchBranch', 'listBranches',
  'deleteBranch', 'registerState', 'updateStateStrategy', 'setAutoSnapshot', 'autoSnapshotEnabled', 'stats', 'getEffects',
  'getLinksTo', 'sync', 'subscribe', 'unsubscribe', 'catchUpSubscription', 'subscriptionCount',
]);

function filesOf(dir) {
  const out = {};
  const walk = (at) => {
    for (const entry of readdirSync(at, { withFileTypes: true })) {
      const path = join(at, entry.name);
      if (entry.name === 'LOCK') continue;
      if (entry.isDirectory()) walk(path);
      else out[path] = readFileSync(path);
    }
  };
  walk(dir);
  return out;
}

function populated() {
  const { store, dir } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log' });
  store.registerState({ id: 'value', strategy: 'snapshot' });
  store.registerState({ id: 'tree', strategy: 'tree' });
  store.appendToStateJson('list', { a: 'x' });
  store.setStateJson('value', 1);
  return { store, dir };
}

test('N1 the package gives one class, JsStore, whose three static methods make a store, and a view is a JsStore too', () => {
  const classes = Object.entries(pkg).filter(([, value]) => typeof value === 'function').map(([name]) => name);
  assert.deepEqual(classes, ['JsStore']);
  for (const name of ['create', 'open', 'openOrCreate']) assert.equal(typeof JsStore[name], 'function', name);
  const { store } = freshStore();
  assert.ok(store instanceof JsStore);
  assert.ok(store.view('main') instanceof JsStore);
  store.close();
});

test('N2 each method answers directly or throws at the call, and storeBlobAsync alone answers with a promise', async () => {
  const { store } = populated();
  const id = store.subscribe();
  const calls = { ...ARGS, isClosed: [], pinnedBranch: [], pollSubscription: [id], pollSubscriptionTimeout: [id, 0], close: [] };
  let pending;
  for (const [name, args] of Object.entries(calls)) {
    let answer;
    try {
      answer = store[name](...args);
    } catch {
      continue;
    }
    if (name === 'storeBlobAsync') pending = answer;
    else assert.equal(typeof answer?.then, 'undefined', name);
  }
  assert.ok(pending instanceof Promise);
  assert.match(await pending, /^[0-9a-f]{64}$/);
});

test('N4 the IDs of a record, a branch and a subscription are texts of decimal digits, and a sequence is a number', () => {
  const { store } = freshStore();
  const record = store.appendJson('note', {});
  assert.match(record.id, /^\d+$/);
  assert.equal(typeof record.sequence, 'number');
  assert.match(store.currentBranch().id, /^\d+$/);
  assert.match(store.createBranch('side').id, /^\d+$/);
  assert.match(store.subscribe(), /^\d+$/);
  store.close();
});

test("N4 in an event's data a record's ID is a JSON number", () => {
  const { store } = freshStore();
  const id = store.subscribe({ fromSequence: 0, filter: { includeRecords: true } });
  const record = store.appendJson('note', {});
  store.catchUpSubscription(id);
  const ids = [];
  for (let event = store.pollSubscription(id); event; event = store.pollSubscription(id)) {
    if (event.eventType === 'record') ids.push(JSON.parse(event.data).record.id);
  }
  assert.deepEqual(ids, [Number(record.id)]);
  store.close();
});

test('N4 each method that takes an ID takes it with a plus sign before the digits', () => {
  const { store } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log' });
  const change = store.appendToStateJson('list', 1);
  const cause = store.appendJson('cause', {});
  const effect = store.appendJsonWithLinks('effect', {}, { causedBy: [`+${cause.id}`], linkedTo: [`+${cause.id}`] });
  assert.deepEqual([effect.causedBy, effect.linkedTo], [[cause.id], [cause.id]]);
  const raw = store.appendWithLinks('raw', Buffer.from('x'), { causedBy: [`+${cause.id}`] });
  assert.deepEqual(raw.causedBy, [cause.id]);
  assert.equal(store.getRecord(`+${cause.id}`).id, cause.id);
  assert.deepEqual(store.getRecords([`+${cause.id}`]).map((r) => r.id), [cause.id]);
  assert.equal(JSON.parse(store.getStateUpdateJson(`+${change.id}`)).state_id, 'list');
  assert.deepEqual(store.getEffects(`+${cause.id}`), [effect.id, raw.id]);
  assert.deepEqual(store.getLinksTo(`+${cause.id}`), [effect.id]);
  const id = store.subscribe({ filter: { includeRecords: true } });
  store.catchUpSubscription(`+${id}`);
  assert.equal(store.pollSubscription(`+${id}`).eventType, 'caught_up');
  store.appendJson('note', {});
  assert.equal(store.pollSubscriptionTimeout(`+${id}`, 0).eventType, 'record');
  store.unsubscribe(`+${id}`);
  assert.equal(store.subscriptionCount(), 0);
  store.close();
});

test("N4 a blob's hash is 64 hexadecimal digits in lower case, and a tree entry's hash is the text the caller gave", () => {
  const { store } = freshStore();
  const content = Buffer.from('content');
  const hash = store.storeBlob(content, 'text/plain');
  assert.equal(hash, createHash('sha256').update(content).digest('hex'));
  store.registerState({ id: 'files', strategy: 'tree' });
  store.treeSet('files', 'a', { blobHash: 'Not A Hash', size: 1, mode: 0o644 });
  assert.equal(store.treeGet('files', 'a').blobHash, 'Not A Hash');
  store.close();
});

test("N8 on a closed store, each method but close, isClosed, pinnedBranch and the two polls throws StoreClosed, and so does each of a view's", () => {
  const { store } = populated();
  const view = store.view('main');
  const methods = Object.getOwnPropertyNames(JsStore.prototype).filter((name) => name !== 'constructor');
  assert.deepEqual([...methods].sort(), [...Object.keys(ARGS), ...APART].sort());
  store.close();
  for (const handle of [store, view]) {
    for (const [name, args] of Object.entries(ARGS)) {
      refuses(() => handle[name](...args), ErrorCode.StoreClosed, 'Store has been closed');
    }
    assert.equal(handle.isClosed(), true);
  }
  assert.equal(store.pinnedBranch(), null);
  assert.equal(view.pinnedBranch(), 'main');
});

test("N8 a view that was closed throws StoreClosed with the view's own message", () => {
  const { store } = populated();
  const view = store.view('main');
  view.close();
  for (const [name, args] of Object.entries(ARGS)) {
    refuses(() => view[name](...args), ErrorCode.StoreClosed, 'View has been closed');
  }
  assert.equal(view.pinnedBranch(), 'main');
  assert.equal(store.isClosed(), false);
  store.close();
});

test('N8 on a closed handle a second close does nothing, and a poll, with or without a timeout, first gives the events that still wait', () => {
  const { store } = populated();
  const view = store.view('main');
  const ids = [store.subscribe(), store.subscribe()];
  for (const id of ids) store.catchUpSubscription(id);
  store.close();
  store.close();
  view.close();
  assert.equal(store.pollSubscription(ids[0]).eventType, 'caught_up');
  assert.equal(store.pollSubscriptionTimeout(ids[1], 1).eventType, 'caught_up');
  refuses(() => store.pollSubscription(ids[0]), ErrorCode.StoreClosed, 'Store has been closed');
  refuses(() => store.pollSubscriptionTimeout(ids[1], 1), ErrorCode.StoreClosed, 'Store has been closed');
});

test.todo('N8 L4 on a closed handle, a read of an item with an index below zero throws StoreClosed', () => {
  const { store } = populated();
  const view = store.view('main');
  store.close();
  for (const handle of [store, view]) {
    refuses(() => handle.getStateItemJson('list', -1), ErrorCode.StoreClosed, 'Store has been closed');
    refuses(() => handle.getStateItemsJson('list', [-1]), ErrorCode.StoreClosed, 'Store has been closed');
  }
});

test('N8 A6 through a view whose branch is deleted, each method that depends on a branch throws BranchNotFound, apart from a read of an item with an index below zero (L4)', () => {
  const { store } = populated();
  store.createBranch('gone');
  const view = store.view('gone');
  store.deleteBranch('gone');
  for (const [name, args] of Object.entries(ARGS)) {
    if (STORE_WIDE.has(name)) continue;
    refuses(() => view[name](...args), ErrorCode.BranchNotFound, /^Branch not found: 'gone'/);
  }
  store.close();
});

test('E1 E2 a failure of the operating system is Io, and its message starts "IO error: " and ends with "(os error N)"', { skip: process.platform === 'win32' && 'relies on the POSIX error for a path through a regular file' }, () => {
  const dir = freshDir();
  mkdirSync(join(dir, '..'), { recursive: true });
  writeFileSync(`${dir}.file`, 'not a directory');
  refuses(() => JsStore.open({ path: join(`${dir}.file`, 'store') }), ErrorCode.Io, /^IO error: .*\(os error \d+\)$/);
});

test('E1 E2 a damaged blob stored whole is ChecksumMismatch, "Checksum mismatch: expected …, got …"', () => {
  const { store, dir } = freshStore();
  const hash = store.storeBlob(Buffer.alloc(1000, 7), 'application/octet-stream');
  store.close();
  const file = join(dir, 'blobs', hash.slice(0, 2), hash);
  const bytes = readFileSync(file);
  bytes[bytes.length - 5] ^= 0xff;
  writeFileSync(file, bytes);
  const reopened = JsStore.open({ path: dir });
  refuses(() => reopened.getBlob(hash), ErrorCode.ChecksumMismatch, /^Checksum mismatch: expected .+, got .+$/);
  reopened.close();
});

function logWithDamagedPayload() {
  const { store, dir } = freshStore();
  const marker = 'Q'.repeat(64);
  store.appendJson('note', { marker });
  store.appendJson('note', { n: 2 });
  store.appendJson('note', { n: 3 });
  store.close();
  const log = join(dir, 'records.log');
  const bytes = readFileSync(log);
  const at = bytes.indexOf(Buffer.from(marker));
  assert.ok(at > 0, 'the first payload is in the log');
  bytes[at + 10] = 'R'.charCodeAt(0);
  writeFileSync(log, bytes);
  return dir;
}

test('A2 E1 E2 open throws Corruption, "Corruption detected: …", for a damaged record with valid records after it', () => {
  const dir = logWithDamagedPayload();
  refuses(() => JsStore.open({ path: dir }), ErrorCode.Corruption, /^Corruption detected: /);
});

test.todo('A2 L9 open throws Corruption for a record whose type is damaged, with valid records after it', () => {
  const { store, dir } = freshStore();
  store.appendJson('marked-type', {});
  store.appendJson('note', { n: 2 });
  store.close();
  const log = join(dir, 'records.log');
  const bytes = readFileSync(log);
  bytes[bytes.indexOf(Buffer.from('marked-type'))] = 'M'.charCodeAt(0);
  writeFileSync(log, bytes);
  refuses(() => JsStore.open({ path: dir }), ErrorCode.Corruption, /^Corruption detected: /);
});

test("A1 T10 create makes a store in config.path and returns the store's own handle, which follows the current branch", () => {
  const dir = freshDir();
  const store = JsStore.create({ path: dir });
  assert.ok(existsSync(join(dir, 'MANIFEST')));
  assert.equal(store.pinnedBranch(), null);
  store.createBranch('side');
  store.switchBranch('side');
  assert.equal(store.currentBranch().name, 'side');
  store.appendJson('note', {});
  assert.equal(store.query({}).length, 1);
  store.close();
});

test('A1 E2 create on a store that is open throws Locked first, "Store is locked by another process", and changes nothing', () => {
  const { store, dir } = freshStore();
  store.appendJson('note', {});
  const before = filesOf(dir);
  refuses(() => JsStore.create({ path: dir }), ErrorCode.Locked, 'Store is locked by another process');
  assert.deepEqual(filesOf(dir), before);
  store.close();
});

test("A1 E2 create throws InvalidOperation where the directory holds any one of a store's four data files", () => {
  for (const name of ['MANIFEST', 'records.log', 'state.bin', 'branches.bin']) {
    const dir = freshDir();
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, name), 'kept');
    refuses(() => JsStore.create({ path: dir }), ErrorCode.InvalidOperation, /^Invalid operation: /);
    assert.equal(readFileSync(join(dir, name), 'utf8'), 'kept', name);
  }
});

test("A2 open returns the store's own handle, with what the store holds", () => {
  const { store, dir } = freshStore();
  const record = store.appendJson('note', { n: 1 });
  store.close();
  const reopened = JsStore.open({ path: dir });
  assert.equal(reopened.pinnedBranch(), null);
  assert.equal(reopened.getRecord(record.id).id, record.id);
  reopened.close();
});

test('A3 openOrCreate creates a store in an empty directory', () => {
  const empty = freshDir();
  mkdirSync(empty, { recursive: true });
  const fromEmpty = JsStore.openOrCreate({ path: empty });
  assert.ok(existsSync(join(empty, 'MANIFEST')));
  fromEmpty.close();
});

test('A3 D9 openOrCreate creates a store beside each name of D9', () => {
  for (const name of ['lost+found', '.fseventsd', '.Spotlight-V100', '.Trashes', '.TemporaryItems', 'System Volume Information', '$RECYCLE.BIN', '.DS_Store']) {
    const dir = freshDir();
    mkdirSync(join(dir, name), { recursive: true });
    JsStore.openOrCreate({ path: dir }).close();
    assert.ok(existsSync(join(dir, 'MANIFEST')), name);
  }
});

test('A3 openOrCreate throws Locked as open does', () => {
  const { store, dir } = freshStore();
  refuses(() => JsStore.openOrCreate({ path: dir }), ErrorCode.Locked, 'Store is locked by another process');
  store.close();
});

test("A4 a second close of the store's own handle does nothing and does not throw", () => {
  const { store } = freshStore();
  const view = store.view('main');
  store.close();
  store.close();
  view.close();
  assert.equal(store.isClosed(), true);
});

test('A6 E2 view throws BranchNotFound, "Branch not found: …", for a name that no branch has', () => {
  const { store } = freshStore();
  refuses(() => store.view('nowhere'), ErrorCode.BranchNotFound, /^Branch not found: /);
  store.close();
});

test.todo('A6 L4 through a view whose branch is deleted, a read of an item with an index below zero throws BranchNotFound', () => {
  const { store } = populated();
  store.createBranch('gone');
  const view = store.view('gone');
  store.deleteBranch('gone');
  refuses(() => view.getStateItemJson('list', -1), ErrorCode.BranchNotFound, /^Branch not found: 'gone'/);
  refuses(() => view.getStateItemsJson('list', [-1, -2]), ErrorCode.BranchNotFound, /^Branch not found: 'gone'/);
  store.close();
});

test("A7 pinnedBranch is null on the store's own handle and the view's branch's name as it was when the view was made, also once closed", () => {
  const { store } = freshStore();
  store.createBranch('b');
  const view = store.view('b');
  assert.equal(store.pinnedBranch(), null);
  assert.equal(view.pinnedBranch(), 'b');
  store.deleteBranch('b');
  assert.equal(view.pinnedBranch(), 'b');
  view.close();
  assert.equal(view.pinnedBranch(), 'b');
  store.close();
  assert.equal(store.pinnedBranch(), null);
});

test('A8 recovery is null when the last open found nothing to report', () => {
  const { store, dir } = freshStore();
  assert.equal(store.recovery(), null);
  store.appendJson('note', {});
  store.close();
  const reopened = JsStore.open({ path: dir });
  assert.equal(reopened.recovery(), null);
  reopened.close();
});

test('A8 T7 recovery reports a log cut back at its end: truncatedAt, droppedBytes, validRecords', () => {
  const { store, dir } = freshStore();
  for (const n of [1, 2, 3]) store.appendJson('note', { n });
  store.close();
  const log = join(dir, 'records.log');
  const length = statSync(log).size;
  truncateSync(log, length - 3);
  const reopened = JsStore.open({ path: dir });
  const report = reopened.recovery();
  assert.equal(report.validRecords, 2);
  assert.equal(report.droppedBytes, length - 3 - report.truncatedAt);
  assert.ok(report.droppedBytes > 0);
  assert.equal(statSync(log).size, report.truncatedAt);
  assert.deepEqual(report.unregisteredStates, []);
  assert.equal(report.undecodableStateUpdates, 0);
  assert.equal(report.updatesOnMissingBranches, 0);
  reopened.close();
});

test("A8 T7 T8 an open that brings back a state's changes with no registration for it names the state and the count, and truncatedAt is the log's length when nothing was cut", () => {
  const { store, dir } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log' });
  for (const n of [1, 2, 3]) store.appendToStateJson('list', n);
  store.close();
  rmSync(join(dir, 'state.bin'));
  const reopened = JsStore.open({ path: dir });
  const report = reopened.recovery();
  assert.deepEqual(report.unregisteredStates, [{ id: 'list', updates: 3 }]);
  assert.equal(report.droppedBytes, 0);
  assert.equal(report.truncatedAt, statSync(join(dir, 'records.log')).size);
  assert.equal(report.validRecords, 3);
  reopened.close();
});

function poisoned() {
  const { store, dir } = freshStore();
  store.registerState({ id: 's', strategy: 'append_log', deltaSnapshotEvery: 2, fullSnapshotEvery: 10 });
  store.appendToState('s', Buffer.from('not json'));
  return { store, dir };
}

test('A9 an entry leaves when a later change finds that no short cut is due', () => {
  const { store } = poisoned();
  store.appendToStateJson('s', { n: 1 });
  assert.equal(store.snapshotFailures().length, 1);
  store.editStateItem('s', 0, Buffer.from('{"n":0}'));
  assert.deepEqual(store.snapshotFailures(), []);
  store.close();
});

test('A52 T6 stats reports the whole store, the same from a view', () => {
  const { store } = freshStore();
  store.registerState({ id: 'a', strategy: 'snapshot' });
  store.registerState({ id: 'b', strategy: 'append_log' });
  store.appendJson('note', {});
  store.createBranch('side');
  store.view('side').appendJson('note', {});
  const content = Buffer.alloc(1000, 1);
  store.storeBlob(content, 'application/octet-stream');
  const stats = store.stats();
  assert.equal(stats.recordCount, 2);
  assert.equal(stats.blobCount, 1);
  assert.equal(stats.branchCount, 2);
  assert.equal(stats.stateSlotCount, 2);
  assert.ok(stats.blobSizeBytes >= content.length);
  assert.ok(stats.totalSizeBytes > stats.blobSizeBytes);
  assert.deepEqual(store.view('side').stats(), stats);
  store.close();
});

test('A62 D3 D5 sync saves the tables of states and branches that changed, each as a new file in place of the old, and replaces neither when nothing changed', () => {
  const { store, dir } = freshStore();
  const files = () => ({ states: statSync(join(dir, 'state.bin')).ino, branches: statSync(join(dir, 'branches.bin')).ino });
  store.registerState({ id: 'list', strategy: 'append_log' });
  const registered = files();
  store.appendToStateJson('list', { n: 1 });
  store.appendJson('note', {});
  assert.deepEqual(files(), registered);
  store.sync();
  const synced = files();
  assert.notEqual(synced.states, registered.states);
  assert.notEqual(synced.branches, registered.branches);
  store.sync();
  assert.deepEqual(files(), synced);
  store.close();
});
