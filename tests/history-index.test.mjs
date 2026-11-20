import { test } from 'node:test';
import assert from 'node:assert/strict';
import { cpSync, mkdirSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { JsStore } from '../index.js';

const SHARED = 'messages';
const LOCAL = 'ns/messages';
const slots = [SHARED, LOCAL];
const TIME = '/timestamp';
const TOP = '/metadata/channelId';
const EXT = '/metadata/external/channelId';
const channels = [TOP, EXT];
const MS = 1_710_000_000_000;

function temporaryStore(t, legacy = false) {
  const dir = mkdtempSync(join(tmpdir(), 'chronicle-history-'));
  const path = join(dir, 'store');
  const fixture = {
    reopen() {
      this.store.close();
      return (this.store = JsStore.open({ path }));
    },
  };
  t.after(() => {
    try { fixture.store?.close(); } finally { rmSync(dir, { recursive: true, force: true }); }
  });
  if (legacy) {
    cpSync(new URL('../tools/fixtures/json-updates/', import.meta.url), path, { recursive: true });
    mkdirSync(join(path, 'blobs'), { recursive: true });
    fixture.store = JsStore.open({ path });
  } else {
    fixture.store = JsStore.openOrCreate({ path });
  }
  return fixture;
}

function registerLog(store, id) {
  store.registerState({ id, strategy: 'append_log' });
}
function registerHistory(store, id) {
  store.registerStateFieldIndex(id, TIME, 'number');
  for (const field of channels) store.registerStateFieldIndex(id, field, 'string');
}
function message(content, delta, top, external) {
  const metadata = {};
  if (top !== undefined) metadata.channelId = top;
  if (external !== undefined) metadata.external = { channelId: external };
  return { content, timestamp: MS + delta, metadata };
}
function channelAt(row, field) {
  return field === TOP ? row.metadata?.channelId : row.metadata?.external?.channelId;
}
function positions(rows, predicate) {
  return rows.flatMap((row, i) => predicate(row) ? [i] : []);
}
function expectedEq(rows, field, key) {
  return positions(rows, row => channelAt(row, field) === key);
}
function expectedEntries(rows, field) {
  const values = rows.map(row => channelAt(row, field)).filter(v => typeof v === 'string');
  return [...new Set(values)].sort().map(v => [v, expectedEq(rows, field, v).length]);
}
function expectedRange(rows, { gte = -Infinity, lte = Infinity } = {}) {
  return positions(rows, row => row.timestamp >= gte && row.timestamp <= lte)
    .sort((a, b) => rows[a].timestamp - rows[b].timestamp || a - b);
}
function eq(store, id, field, key) {
  return store.queryStateIndexEq(id, field, key, {});
}
function range(store, id, opts) {
  return store.queryStateIndexRange(id, TIME, opts);
}
function at(delta) {
  return { gte: MS + delta, lte: MS + delta };
}
const ascending = hits => [...hits].sort((a, b) => a - b);
function countPairs(entries) {
  return [...entries].map(({ value, count }) => [value, count])
    .sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0);
}
function logicalHistory(store) {
  return {
    messages: slots.map(id => store.getStateJson(id)),
    records: store.query({}),
    heads: store.listBranches().map(({ name, head }) => [name, head]).sort(),
  };
}

test('Three exact history indexes agree on message ordinals across two state IDs', t => {
  const fixture = temporaryStore(t, true);
  let store = fixture.store;
  const old = Array.from({ length: 50 }, (_, n) => ({ n, text: `message ${n} says hello` }));
  const shared = [
    message('both', 1, 'Room', 'Room'), message('different', 2, 'Room', 'other'),
    message('trailing space', 3, 'Room ', 'Room'), message('null and number', 3, null, 42),
    message('number and null', 4, 42, null), message('no channel', 5),
    message('case matters', 8, undefined, 'roOm'),
  ];
  const local = [
    message('external only match', 5, 'else', 'Room'), message('invalid', 1, 42, null),
    message('distinct schemas', 2, 'Room ', 'Room'), message('one valid', 3, 'local', 42),
    message('absent', 4), message('overlap', 3, 'Room', 'Room'), message('null top', 6, null, 'local'),
  ];
  const rowsById = { [SHARED]: [...old, ...shared], [LOCAL]: local };
  registerLog(store, LOCAL);
  registerHistory(store, LOCAL);
  for (const row of local) store.appendToStateJson(LOCAL, row);
  for (const row of shared) store.appendToStateJson(SHARED, row);
  store.createBranch('before-indexing');
  const before = logicalHistory(store);
  assert.deepEqual(before.messages, slots.map(id => rowsById[id]));
  assert.equal(eq(store, SHARED, TOP, 'Room'), null);
  registerHistory(store, SHARED);
  store.registerStateFieldIndex(SHARED, '/n', 'number');
  assert.deepEqual([
    store.queryStateIndexRange(SHARED, '/n', { gte: 17, lte: 19 }),
    ...slots.map(id => ascending(eq(store, id, TOP, 'Room'))),
  ], [[17, 18, 19], [50, 51], [5]]);
  assert.deepEqual(logicalHistory(store), before);
  store = fixture.reopen();
  assert.deepEqual(logicalHistory(store), before);

  for (const id of slots) {
    registerHistory(store, id);
    const rows = rowsById[id];
    const keys = () => [...new Set(channels.flatMap(field =>
      [...store.getStateIndexValueCounts(id, field)].map(entry => entry.value)))].sort();
    const expectedKeys = () => [...new Set(channels.flatMap(field =>
      expectedEntries(rows, field).map(([v]) => v)))].sort();
    assert.deepEqual(keys(), expectedKeys());
    const later = id === SHARED ? message('later', 2, 'late\nchannel') : message('later local', 2, 'local', 'brand/new');
    rows.push(later);
    store.appendToStateJson(id, later);
    const listedKeys = keys();
    assert.deepEqual(listedKeys, expectedKeys());

    assert.deepEqual(range(store, id, at(2)), expectedRange(rows, at(2)));
    assert.deepEqual(range(store, id, at(1)), id === SHARED ? [50] : [1]);
    assert.deepEqual(ascending(eq(store, id, TOP, 'Room')), id === SHARED ? [50, 51] : [5]);
    assert.deepEqual({
      time: range(store, id, at(3)),
      fields: channels.map(field => [ascending(eq(store, id, field, 'Room')), countPairs(store.getStateIndexValueCounts(id, field))]),
    }, {
      time: expectedRange(rows, at(3)),
      fields: channels.map(field => [expectedEq(rows, field, 'Room'), expectedEntries(rows, field)]),
    });

    for (const field of channels) {
      const expected = expectedEntries(rows, field);
      const expectedValues = expected.map(([v]) => v);
      const entries = [...store.getStateIndexValueCounts(id, field)];
      assert.deepEqual(entries.map(entry => entry.value).sort(), expectedValues);
      assert.deepEqual(countPairs(entries), expected);
      assert.ok(entries.every(({ value }) => typeof value === 'string' && expectedValues.includes(value)));
      assert.deepEqual(ascending(eq(store, id, field, channelAt(later, field) ?? 'Room')),
        expectedEq(rows, field, channelAt(later, field) ?? 'Room'));
      assert.equal(entries.reduce((n, entry) => n + entry.count, 0),
        rows.filter(row => typeof channelAt(row, field) === 'string').length);
      for (const key of new Set([...listedKeys, 'Room', 'Room ', 'roOm', 'room'])) {
        assert.deepEqual(ascending(eq(store, id, field, key)), expectedEq(rows, field, key));
      }
      for (const coerced of ['undefined', 'null', '42']) {
        assert.deepEqual(eq(store, id, field, coerced), []);
      }
    }
    for (const key of listedKeys) {
      const merged = new Set(channels.flatMap(field => eq(store, id, field, key)));
      assert.deepEqual(ascending(merged), positions(rows, row => channels.some(f => channelAt(row, f) === key)));
    }
    const window = { gte: MS + 1, lte: MS + 3 };
    const timeHits = range(store, id, window);
    const topHits = eq(store, id, TOP, 'Room');
    const extHits = eq(store, id, EXT, 'Room');
    assert.deepEqual(timeHits.filter(i => topHits.includes(i) && extHits.includes(i)), id === SHARED ? [50] : [5]);
    assert.deepEqual(ascending(timeHits.filter(i => topHits.includes(i) || extHits.includes(i))),
      positions(rows, row => row.timestamp >= window.gte && row.timestamp <= window.lte &&
        channels.some(field => channelAt(row, field) === 'Room')));
    assert.ok(Array.isArray(timeHits) && timeHits.every(Number.isInteger));
    assert.deepEqual(timeHits.map(i => store.getStateItemJson(id, i)), expectedRange(rows, window).map(i => rows[i]));
    for (const opts of [window, at(3), { lte: MS + 2 }, { gte: undefined, lte: MS + 2 },
      { gte: MS + 4 }, { gte: MS + 4, lte: undefined }, {}, { gte: undefined, lte: undefined }]) {
      assert.deepEqual(range(store, id, opts), expectedRange(rows, opts));
    }
  }
});

test('Registration straight after opening a store older than field indexes indexes its history', t => {
  const { store } = temporaryStore(t, true);
  store.registerStateFieldIndex(SHARED, '/n', 'number');
  store.registerStateFieldIndex(SHARED, '/text', 'string');
  assert.deepEqual([
    store.queryStateIndexRange(SHARED, '/n', { gte: 17, lte: 19 }),
    store.queryStateIndexRange(SHARED, '/n', {}),
    store.queryStateIndexEq(SHARED, '/text', 'message 42 says hello', {}),
  ], [[17, 18, 19], Array.from({ length: 50 }, (_, n) => n), [42]]);
});

function cpuMicrosSince(start) {
  const { user, system } = process.cpuUsage(start);
  return user + system;
}

function narrowRangeTime(messages) {
  const dir = mkdtempSync(join(tmpdir(), 'chronicle-history-cost-'));
  let store;
  try {
    store = JsStore.openOrCreate({ path: join(dir, 'store') });
    registerLog(store, SHARED);
    registerHistory(store, SHARED);
    for (let i = 0; i < messages; i += 4096) {
      store.appendManyToStateJson(SHARED, Array.from({ length: Math.min(4096, messages - i) },
        (_, k) => message(`item ${i + k}`, i + k, 'room', 'room')));
    }
    const firsts = Array.from({ length: 96 }, (_, i) => Math.floor(((i * 0.6180339887498949) % 1) * (messages - 1)));
    const window = first => ({ gte: MS + first, lte: MS + first + 1 });
    const expected = firsts.map(first => [first, first + 1]);
    assert.deepEqual(firsts.map(first => range(store, SHARED, window(first))), expected);
    const times = [];
    for (let sample = 0; sample < 5; ++sample) {
      const found = new Array(firsts.length);
      const start = process.cpuUsage();
      for (let i = 0; i < firsts.length; ++i) found[i] = range(store, SHARED, window(firsts[i]));
      times.push(cpuMicrosSince(start));
      assert.deepEqual(found, expected);
    }
    return Math.min(...times);
  } finally {
    store?.close();
    rmSync(dir, { recursive: true, force: true });
  }
}

test('A narrow time query through the binding costs the same over a long history', () => {
  const small = narrowRangeTime(256);
  const large = narrowRangeTime(32768);
  const floor = 20_000;
  assert.ok(large < 3 * small + floor, `96 narrow queries took ${small} µs CPU over 256 messages, ${large} over 32,768`);
});

test('Shared-handle edits cannot leave queryable stale channel or timestamp hits', t => {
  const { store } = temporaryStore(t);
  registerLog(store, SHARED);
  registerHistory(store, SHARED);
  const writer = store.view('main');
  writer.appendToStateJson(SHARED, message('first', 1, 'first', 'first'));
  const next = store.getStateLen(SHARED);
  const msg = message('second', 2, 'original', 'external');
  const record = writer.appendToStateJsonWithIdentity(SHARED, msg, 'id', 'sequence');
  const stamped = { ...msg, id: record.id, sequence: record.sequence };

  function hits(field, value) {
    const query = () => field === TIME ? range(store, SHARED, at(value)) : eq(store, SHARED, field, value);
    const answer = query();
    if (answer !== null) return answer;
    store.registerStateFieldIndex(SHARED, field, field === TIME ? 'number' : 'string');
    return query();
  }
  const previousHits = () => [hits(TIME, 2), hits(TOP, 'original'), hits(EXT, 'external')];
  assert.deepEqual(previousHits(), [[next], [next], [next]]);
  const edited = { ...stamped, content: [{ text: 'rewritten but in the same channels' }] };
  writer.editStateItem(SHARED, next, Buffer.from(JSON.stringify(edited)));
  const afterEdit = previousHits();
  assert.deepEqual(afterEdit.map(list => list.map(i => [i, store.getStateItemJson(SHARED, i)])),
    [[[next, edited]], [[next, edited]], [[next, edited]]]);

  const moved = { ...edited, timestamp: MS + 9, metadata: { channelId: 'changed', external: { channelId: 'other' } } };
  writer.editStateItem(SHARED, next, Buffer.from(JSON.stringify(moved)));
  const channelHits = () => [hits(TOP, 'original'), hits(EXT, 'external'), hits(TOP, 'changed'), hits(EXT, 'other')];
  assert.deepEqual(channelHits(), [[], [], [next], [next]]);
  assert.deepEqual([hits(TIME, 2), hits(TIME, 9)], [[], [next]]);
  writer.redactStateItems(SHARED, 0, 1);
  assert.deepEqual([hits(TOP, 'first'), hits(EXT, 'first'), ...channelHits()], [[], [], [], [], [next - 1], [next - 1]]);
  const shifted = hits(TIME, 9);
  assert.deepEqual([hits(TIME, 1), hits(TIME, 2), shifted, shifted.map(i => store.getStateItemJson(SHARED, i))],
    [[], [], [next - 1], [moved]]);
});

test("Switching and reusing a branch name cannot revive another identity's indexes", t => {
  const { store } = temporaryStore(t);
  for (const id of slots) registerLog(store, id);
  store.createBranch('side');
  const parent = store.view('main');
  const dead = store.view('side');
  function install(view, tag, base, commonPositions) {
    const model = {};
    for (const [s, id] of slots.entries()) {
      const label = `${tag}-${id}`;
      model[id] = Array.from({ length: 3 }, (_, i) => i === commonPositions[s]
        ? message(`${label} shared hit`, 10, 'common', label)
        : message(`${label} ${i}`, base + s, label, 'external'));
      view.setStateJson(id, model[id]);
      registerHistory(view, id);
    }
    return model;
  }
  const mainRows = install(parent, 'main', 20, [0, 1]);
  const deadRows = install(dead, 'deleted', 30, [2, 0]);
  function check(view, model) {
    for (const id of slots) {
      assert.deepEqual(eq(view, id, TOP, 'common'), expectedEq(model[id], TOP, 'common'));
      assert.deepEqual(range(view, id, at(10)), expectedRange(model[id], at(10)));
      for (const field of channels) {
        const entries = [...view.getStateIndexValueCounts(id, field)];
        assert.deepEqual(entries.map(({ value }) => value).sort(), expectedEntries(model[id], field).map(([value]) => value));
        assert.deepEqual(countPairs(entries), expectedEntries(model[id], field));
      }
    }
  }
  const heads = view => slots.map(id => view.stateHeadOffset(id));
  const parentHeads = heads(parent);
  const sideHeads = heads(dead);
  assert.ok(parentHeads.every((head, s) => head !== sideHeads[s]));
  check(parent, mainRows);
  check(dead, deadRows);
  store.switchBranch('side');
  assert.deepEqual(heads(store), sideHeads);
  check(store, deadRows);
  check(parent, mainRows);
  store.switchBranch('main');
  assert.deepEqual(heads(store), parentHeads);
  check(store, mainRows);
  check(dead, deadRows);

  store.deleteBranch('side');
  store.createBranch('side');
  const replacement = store.view('side');
  for (const id of slots) {
    registerHistory(replacement, id);
    assert.deepEqual(eq(replacement, id, TOP, 'common'), expectedEq(mainRows[id], TOP, 'common'));
  }
  check(replacement, mainRows);
  const replacementRows = install(replacement, 'replacement', 40, [1, 2]);
  for (const [s, id] of slots.entries()) {
    assert.deepEqual(eq(replacement, id, TOP, `deleted-${id}`), []);
    assert.throws(() => range(dead, id, at(30 + s)));
    assert.deepEqual(range(replacement, id, at(30 + s)), []);
    assert.throws(() => eq(dead, id, TOP, `deleted-${id}`));
    assert.throws(() => dead.getStateIndexValueCounts(id, TOP));
  }
  check(replacement, replacementRows);
  store.switchBranch('side');
  check(store, replacementRows);
  check(parent, mainRows);
});

test('Time pages order values first and split tied groups without repeats', t => {
  const { store } = temporaryStore(t);
  registerLog(store, SHARED);
  const times = [6, 2, 7, 4, 4, 9, 1, 4, 5, 3, 4];
  times.forEach((delta, i) => store.appendToStateJson(SHARED, message(`item ${i}`, delta)));
  store.registerStateFieldIndex(SHARED, TIME, 'number');
  for (const reverse of [false, true]) {
    const options = { gte: MS + 2, lte: MS + 7, ...(reverse ? { reverse: true } : {}) };
    const order = reverse ? [2, 0, 8, 10, 7, 4, 3, 9, 1] : [1, 9, 3, 4, 7, 10, 8, 0, 2];
    const full = range(store, SHARED, options);
    const page = offset => range(store, SHARED, { ...options, limit: 4, ...(offset === undefined ? {} : { offset }) });
    const first = page();
    assert.deepEqual([full, first], [order, order.slice(0, 4)]);
    const pages = [first, page(4), page(8)];
    assert.deepEqual(pages.flat().filter(i => times[i] === 4), reverse ? [10, 7, 4, 3] : [3, 4, 7, 10]);
    assert.deepEqual(pages, [order.slice(0, 4), order.slice(4, 8), order.slice(8)]);
  }
});

test('Unpaginated native history results have no ordinary page cap', t => {
  const { store } = temporaryStore(t);
  registerLog(store, SHARED);
  const rows = Array.from({ length: 12_345 }, (_, i) => message(`item ${i}`, i % 4, i % 7 ? 'many' : undefined));
  for (let i = 0; i < rows.length; i += 512) store.appendManyToStateJson(SHARED, rows.slice(i, i + 512));
  registerHistory(store, SHARED);
  assert.deepEqual(ascending(eq(store, SHARED, TOP, 'many')), expectedEq(rows, TOP, 'many'));
  assert.deepEqual(range(store, SHARED, {}), expectedRange(rows));
});

test('Missing, wrong-kind and poisoned indexes differ from successful empty queries', t => {
  const { store } = temporaryStore(t);
  store.setAutoSnapshot(false);
  for (const id of slots) registerLog(store, id);
  store.appendToStateJson(SHARED, message('seed', 1, 'room'));
  store.appendToStateJson(LOCAL, message('sound elsewhere', 3, 'unrelated'));
  store.createBranch('seeded');
  const view = store.view('seeded');
  registerHistory(view, LOCAL);
  const rooms = () => eq(view, SHARED, TOP, 'room');
  const time = () => range(view, SHARED, at(1));
  const counts = () => view.getStateIndexValueCounts(SHARED, TOP);
  assert.equal(rooms(), null);
  assert.equal(counts(), null);
  assert.equal(time(), null);
  view.registerStateFieldIndex(SHARED, TOP, 'string');
  assert.deepEqual(countPairs(counts()), [['room', 1]]);
  assert.deepEqual(rooms(), [0]);
  view.registerStateFieldIndex(SHARED, TIME, 'number');
  assert.deepEqual(time(), [0]);
  view.registerStateFieldIndex(SHARED, EXT, 'string');
  assert.deepEqual(view.getStateIndexValueCounts(SHARED, EXT), []);
  assert.deepEqual(eq(view, SHARED, TOP, 'no such channel'), []);
  assert.deepEqual(range(view, SHARED, at(99)), []);
  assert.equal(eq(view, SHARED, TIME, String(MS + 1)), null);
  assert.equal(view.getStateIndexValueCounts(SHARED, TIME), null);
  assert.equal(view.queryStateIndexRange(SHARED, TOP, {}), null);

  view.appendToState(SHARED, Buffer.from('not JSON'));
  assert.deepEqual([rooms(), time(), counts()], [null, null, null]);
  assert.equal(rooms(), null);
  assert.equal(time(), null);
  assert.equal(counts(), null);
  for (const [field, kind] of [[TIME, 'number'], [TOP, 'string']]) {
    try { view.registerStateFieldIndex(SHARED, field, kind); } catch { /* still unusable */ }
  }
  assert.equal(time(), null);
  assert.equal(rooms(), null);
  assert.equal(counts(), null);
  assert.deepEqual([eq(view, LOCAL, TOP, 'unrelated'), range(view, LOCAL, at(3)),
    countPairs(view.getStateIndexValueCounts(LOCAL, TOP))], [[0], [0], [['unrelated', 1]]]);

  view.setStateJson(SHARED, [message('other', 9, 'other', 'external'), message('repaired', 1, 'room', 'external')]);
  view.registerStateFieldIndex(SHARED, EXT, 'string');
  view.registerStateFieldIndex(SHARED, TOP, 'string');
  assert.deepEqual([rooms(), ascending(eq(view, SHARED, EXT, 'external')), eq(view, LOCAL, TOP, 'unrelated')],
    [[1], [0, 1], [0]]);
  assert.deepEqual(countPairs(counts()), [['other', 1], ['room', 1]]);
  view.registerStateFieldIndex(SHARED, TIME, 'number');
  assert.deepEqual(time(), [1]);
});
