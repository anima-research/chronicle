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
    // The committed 0.2.1 store used by tools/check-tools.mjs. Git omits its empty directory.
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
  // pins registerStateFieldIndex.3 the qualified registration does not also register the shared slot
  assert.equal(eq(store, SHARED, TOP, 'Room'), null);
  registerHistory(store, SHARED);
  // The old fixture predates these three paths; index a field actually present in its original messages.
  store.registerStateFieldIndex(SHARED, '/n', 'number');
  // pins registerStateFieldIndex.7 index original 0.2.1 rows, and rows written on either side of registration
  assert.deepEqual([
    store.queryStateIndexRange(SHARED, '/n', { gte: 17, lte: 19 }),
    ...slots.map(id => ascending(eq(store, id, TOP, 'Room'))),
  ], [[17, 18, 19], [50, 51], [5]]);
  // pins registerStateFieldIndex.13 registration adds neither message mutations, records nor branch-head changes
  assert.deepEqual(logicalHistory(store), before);
  store = fixture.reopen();
  // pins registerStateFieldIndex.13 the same logical history survives reopening
  assert.deepEqual(logicalHistory(store), before);

  for (const id of slots) {
    registerHistory(store, id);
    const rows = rowsById[id];
    const keys = () => [...new Set(channels.flatMap(field =>
      [...store.getStateIndexValueCounts(id, field)].map(entry => entry.value)))].sort();
    const expectedKeys = () => [...new Set(channels.flatMap(field =>
      expectedEntries(rows, field).map(([v]) => v)))].sort();
    // pins getStateIndexValueCounts.4 enumerate the pre-existing channels before a new one arrives
    assert.deepEqual(keys(), expectedKeys());
    const later = id === SHARED ? message('later', 2, 'late\nchannel') : message('later local', 2, 'local', 'brand/new');
    rows.push(later);
    store.appendToStateJson(id, later);
    const listedKeys = keys();
    // pins getStateIndexValueCounts.4 enumeration follows new channels on either schema
    assert.deepEqual(listedKeys, expectedKeys());

    // pins registerStateFieldIndex.4 Node number registration yields ordinals for a narrow millisecond interval
    assert.deepEqual(range(store, id, at(2)), expectedRange(rows, at(2)));
    // pins queryStateIndexRange.2 the same time and pointer address different messages in the two slots
    assert.deepEqual(range(store, id, at(1)), id === SHARED ? [50] : [1]);
    // pins registerStateFieldIndex.3 after both registrations each slot returns only its own hits
    assert.deepEqual(ascending(eq(store, id, TOP, 'Room')), id === SHARED ? [50, 51] : [5]);
    // pins registerStateFieldIndex.5 both nested index memberships and counts coexist with the numeric index
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
      // pins getStateIndexValueCounts.3 iterate the immediate result and read string channel IDs from .value
      assert.deepEqual(entries.map(entry => entry.value).sort(), expectedValues);
      // pins getStateIndexValueCounts.2 counts belong to this slot and this exact nested path
      assert.deepEqual(countPairs(entries), expected);
      // pins getStateIndexValueCounts.5 every entry is from a valid string in that field, none from missing/null/numeric IDs
      assert.ok(entries.every(({ value }) => typeof value === 'string' && expectedValues.includes(value)));
      // pins registerStateFieldIndex.6 skipped values neither add entries nor move the true positions of valid ones
      assert.deepEqual([
        entries.reduce((n, entry) => n + entry.count, 0),
        ascending(eq(store, id, field, channelAt(later, field) ?? 'Room')),
      ], [rows.filter(row => typeof channelAt(row, field) === 'string').length,
        expectedEq(rows, field, channelAt(later, field) ?? 'Room')]);
      for (const key of new Set([...listedKeys, 'Room', 'Room ', 'roOm', 'room'])) {
        // pins queryStateIndexEq.3 exact comparison, including case, whitespace and the other schema's values
        assert.deepEqual(ascending(eq(store, id, field, key)), expectedEq(rows, field, key));
      }
      for (const coerced of ['undefined', 'null', '42']) {
        // pins queryStateIndexEq.5 missing, null and numeric IDs do not match their stringifications
        assert.deepEqual(eq(store, id, field, coerced), []);
      }
    }
    for (const key of listedKeys) {
      const merged = new Set(channels.flatMap(field => eq(store, id, field, key)));
      // pins queryStateIndexEq.9 enumerated keys fed unchanged to both queries recover distinct matching messages
      assert.deepEqual(ascending(merged), positions(rows, row => channels.some(f => channelAt(row, f) === key)));
    }
    const window = { gte: MS + 1, lte: MS + 3 };
    const timeHits = range(store, id, window);
    const topHits = eq(store, id, TOP, 'Room');
    const extHits = eq(store, id, EXT, 'Room');
    // pins queryStateIndexEq.8 the dual-schema message is the same ordinal in both channel indexes and time
    assert.deepEqual(timeHits.filter(i => topHits.includes(i) && extHits.includes(i)), id === SHARED ? [50] : [5]);
    // pins queryStateIndexEq.8 intersect the merged channel ordinals with time without fetching or double-counting content
    assert.deepEqual(ascending(timeHits.filter(i => topHits.includes(i) || extHits.includes(i))),
      positions(rows, row => row.timestamp >= window.gte && row.timestamp <= window.lte &&
        channels.some(field => channelAt(row, field) === 'Room')));
    // pins queryStateIndexRange.3 synchronous integer positions address the complete expected messages
    assert.ok(Array.isArray(timeHits) && timeHits.every(Number.isInteger));
    // pins queryStateIndexRange.3 point reads at the returned positions, not a separate set of known ordinals
    assert.deepEqual(timeHits.map(i => store.getStateItemJson(id, i)), expectedRange(rows, window).map(i => rows[i]));
    for (const opts of [window, at(3), { lte: MS + 2 }, { gte: undefined, lte: MS + 2 },
      { gte: MS + 4 }, { gte: MS + 4, lte: undefined }, {}, { gte: undefined, lte: undefined }]) {
      // pins queryStateIndexRange.7 inclusive Unix-ms bounds; omitted and explicitly undefined ends stay open
      assert.deepEqual(range(store, id, opts), expectedRange(rows, opts));
    }
  }
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

  // As MessageStore does, heal once on literal null. Never re-register over a successful (possibly stale) query.
  function hits(field, value) {
    const query = () => field === TIME ? range(store, SHARED, at(value)) : eq(store, SHARED, field, value);
    const answer = query();
    if (answer !== null) return answer;
    store.registerStateFieldIndex(SHARED, field, field === TIME ? 'number' : 'string');
    return query();
  }
  const previousHits = () => [hits(TIME, 2), hits(TOP, 'original'), hits(EXT, 'external')];
  // pins appendToStateJsonWithIdentity.14 identity append appears at the old length in all matching indexes
  assert.deepEqual(previousHits(), [[next], [next], [next]]);
  const edited = { ...stamped, content: [{ text: 'rewritten but in the same channels' }] };
  writer.editStateItem(SHARED, next, Buffer.from(JSON.stringify(edited)));
  const afterEdit = previousHits();
  // pins editStateItem.16 the unchanged index keys still locate exactly the complete edited message
  assert.deepEqual(afterEdit.map(list => list.map(i => [i, store.getStateItemJson(SHARED, i)])),
    [[[next, edited]], [[next, edited]], [[next, edited]]]);

  const moved = { ...edited, timestamp: MS + 9, metadata: { channelId: 'changed', external: { channelId: 'other' } } };
  writer.editStateItem(SHARED, next, Buffer.from(JSON.stringify(moved)));
  const channelHits = () => [hits(TOP, 'original'), hits(EXT, 'external'), hits(TOP, 'changed'), hits(EXT, 'other')];
  // pins registerStateFieldIndex.12 the observer rejects old channel matches after a same-length sibling edit
  assert.deepEqual(channelHits(), [[], [], [next], [next]]);
  // pins queryStateIndexRange.6 immediate same-branch time searches reject the old key and locate the new one
  assert.deepEqual([hits(TIME, 2), hits(TIME, 9)], [[], [next]]);
  writer.redactStateItems(SHARED, 0, 1);
  // pins registerStateFieldIndex.12 redaction removes earlier hits and shifts the edited row in both schemas
  assert.deepEqual([hits(TOP, 'first'), hits(EXT, 'first'), ...channelHits()], [[], [], [], [], [next - 1], [next - 1]]);
  const shifted = hits(TIME, 9);
  // pins queryStateIndexRange.6 only the shifted current position survives, and there it reads the edited message
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
      // pins queryStateIndexEq.2 common channel hits differ by slot and by the branch selected by this handle
      assert.deepEqual(eq(view, id, TOP, 'common'), expectedEq(model[id], TOP, 'common'));
      // pins queryStateIndexRange.5 even at a common timestamp only this branch's position is returned
      assert.deepEqual(range(view, id, at(10)), expectedRange(model[id], at(10)));
      for (const field of channels) {
        // pins getStateIndexValueCounts.8 current branch enumeration has its own keys/counts and no foreign ones
        assert.deepEqual(countPairs(view.getStateIndexValueCounts(id, field)), expectedEntries(model[id], field));
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
  // pins switchBranch.6 root cache-validation reads follow the destination revision immediately, with no write
  assert.deepEqual(heads(store), sideHeads);
  check(store, deadRows);
  check(parent, mainRows);
  store.switchBranch('main');
  // pins switchBranch.6 switching back restores the source revisions immediately
  assert.deepEqual(heads(store), parentHeads);
  check(store, mainRows);
  check(dead, deadRows);

  store.deleteBranch('side');
  store.createBranch('side');
  const replacement = store.view('side');
  // The replacement forked from main, which differs from the deleted branch.
  // Register before its first write: a Set could rebuild and hide a wrongly recovered index.
  for (const id of slots) {
    registerHistory(replacement, id);
    // pins registerStateFieldIndex.11 immediately indexing the reused name reads the inherited slot, not the dead one
    assert.deepEqual(eq(replacement, id, TOP, 'common'), expectedEq(mainRows[id], TOP, 'common'));
  }
  check(replacement, mainRows);
  const replacementRows = install(replacement, 'replacement', 40, [1, 2]);
  for (const [s, id] of slots.entries()) {
    // pins registerStateFieldIndex.11 registration under the reused name cannot recover the old channel membership
    assert.deepEqual(eq(replacement, id, TOP, `deleted-${id}`), []);
    // pins queryStateIndexRange.5 the dead handle cannot query the new identity and the deleted time is absent there
    assert.throws(() => range(dead, id, at(30 + s)));
    // pins queryStateIndexRange.5 the newly registered numeric index has none of the dead branch's private hits
    assert.deepEqual(range(replacement, id, at(30 + s)), []);
    // pins registerStateFieldIndex.11 the old handle's channel index is not revived by replacement registration
    assert.throws(() => eq(dead, id, TOP, `deleted-${id}`));
    // pins getStateIndexValueCounts.8 old view cannot enumerate either incarnation's channels under a reused name
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
    // pins queryStateIndexRange.8 timestamp order, not append order or reversal of an oldest-first limited page
    assert.deepEqual([full, first], [order, order.slice(0, 4)]);
    const pages = [first, page(4), page(8)];
    // pins queryStateIndexRange.9 the split tie resumes in append order (reverse append order newest-first)
    assert.deepEqual(pages.flat().filter(i => times[i] === 4), reverse ? [10, 7, 4, 3] : [3, 4, 7, 10]);
    // pins queryStateIndexRange.10 paginate ordered, filtered hits, including omitted first offset and short last page
    assert.deepEqual(pages, [order.slice(0, 4), order.slice(4, 8), order.slice(8)]);
  }
});

test('Unpaginated native history results have no ordinary page cap', t => {
  const { store } = temporaryStore(t);
  registerLog(store, SHARED);
  const rows = Array.from({ length: 12_345 }, (_, i) => message(`item ${i}`, i % 4, i % 7 ? 'many' : undefined));
  for (let i = 0; i < rows.length; i += 512) store.appendManyToStateJson(SHARED, rows.slice(i, i + 512));
  registerHistory(store, SHARED);
  // pins queryStateIndexEq.6 empty options return every matching position (including beyond 10,000), exactly once
  assert.deepEqual(ascending(eq(store, SHARED, TOP, 'many')), expectedEq(rows, TOP, 'many'));
  // pins queryStateIndexRange.11 the full unpaginated range includes every tie and channel-less message exactly once
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
  // pins queryStateIndexEq.10 absent string index on the seeded handle signals literal null
  assert.equal(rooms(), null);
  // pins getStateIndexValueCounts.6 absent string index cannot enumerate (not an empty success)
  assert.equal(counts(), null);
  // pins queryStateIndexRange.13 absent numeric index signals literal null
  assert.equal(time(), null);
  view.registerStateFieldIndex(SHARED, TOP, 'string');
  // pins getStateIndexValueCounts.7 the identical counts call immediately sees first registration on the derived handle
  assert.deepEqual(countPairs(counts()), [['room', 1]]);
  // pins queryStateIndexEq.11 same equality call sees the seeded historical position
  assert.deepEqual(rooms(), [0]);
  view.registerStateFieldIndex(SHARED, TIME, 'number');
  // pins queryStateIndexRange.14 immediate retry sees the newly registered historical hit
  assert.deepEqual(time(), [0]);
  view.registerStateFieldIndex(SHARED, EXT, 'string');
  // pins getStateIndexValueCounts.6 usable but valueless channel index is a successful empty array
  assert.deepEqual(view.getStateIndexValueCounts(SHARED, EXT), []);
  // pins queryStateIndexEq.10 usable string miss is an empty array, not null
  assert.deepEqual(eq(view, SHARED, TOP, 'no such channel'), []);
  // pins queryStateIndexRange.13 usable time miss is an empty array, not null
  assert.deepEqual(range(view, SHARED, at(99)), []);
  // pins queryStateIndexEq.10 numeric registration is unusable for equality
  assert.equal(eq(view, SHARED, TIME, String(MS + 1)), null);
  // pins getStateIndexValueCounts.6 numeric registration is unusable for channel counts
  assert.equal(view.getStateIndexValueCounts(SHARED, TIME), null);
  // pins queryStateIndexRange.13 string registration is unusable for numeric range
  assert.equal(view.queryStateIndexRange(SHARED, TOP, {}), null);

  view.appendToState(SHARED, Buffer.from('not JSON'));
  // pins registerStateFieldIndex.10 normally registered indexes that later become unqueryable do not report empty success
  assert.deepEqual([rooms(), time(), counts()], [null, null, null]);
  // pins queryStateIndexEq.10 poisoned string index signals literal null, even for the formerly indexed key
  assert.equal(rooms(), null);
  // pins queryStateIndexRange.13 poisoned number index signals literal null
  assert.equal(time(), null);
  // pins getStateIndexValueCounts.6 poisoned enumeration signals literal null
  assert.equal(counts(), null);
  for (const [field, kind] of [[TIME, 'number'], [TOP, 'string']]) {
    // Materializing malformed bytes may throw. Do not require rebuild to succeed on unreadable history.
    try { view.registerStateFieldIndex(SHARED, field, kind); } catch { /* still unusable */ }
  }
  // pins queryStateIndexRange.14 a repeat query while the slot remains malformed returns literal null again
  assert.equal(time(), null);
  // pins queryStateIndexEq.11 a still-unusable registration does not turn the identical retry into a false miss
  assert.equal(rooms(), null);
  // pins getStateIndexValueCounts.7 repeat enumeration of the unusable field still signals unavailability
  assert.equal(counts(), null);
  // pins queryStateIndexEq.11 the unusable field on the shared slot hasn't disturbed another indexed history
  assert.deepEqual([eq(view, LOCAL, TOP, 'unrelated'), range(view, LOCAL, at(3)),
    countPairs(view.getStateIndexValueCounts(LOCAL, TOP))], [[0], [0], [['unrelated', 1]]]);

  view.setStateJson(SHARED, [message('other', 9, 'other', 'external'), message('repaired', 1, 'room', 'external')]);
  view.registerStateFieldIndex(SHARED, EXT, 'string');
  view.registerStateFieldIndex(SHARED, TOP, 'string');
  // pins queryStateIndexEq.11 identical retry finds repaired positions without disturbing the independent channel field
  assert.deepEqual([rooms(), ascending(eq(view, SHARED, EXT, 'external')), eq(view, LOCAL, TOP, 'unrelated')],
    [[1], [0, 1], [0]]);
  // pins getStateIndexValueCounts.7 the same enumeration now sees the repaired field
  assert.deepEqual(countPairs(counts()), [['other', 1], ['room', 1]]);
  view.registerStateFieldIndex(SHARED, TIME, 'number');
  // pins queryStateIndexRange.14 the same time query finds the repaired row at its new position
  assert.deepEqual(time(), [1]);
});
