import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtempSync, readFileSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { constants as osConstants, tmpdir } from 'node:os';
import { join } from 'node:path';
import { setImmediate as nextTurn } from 'node:timers/promises';
import { ErrorCode, JsStore } from '../index.js';

const MSG = 'messages';
const AUX = 'peer/messages';
const WIN = 'ada/window';
const CTX = 'ada/context';
const STRATEGY = 'strategy/summary';

function fresh(t) {
  const directory = mkdtempSync(join(tmpdir(), 'chronicle-lineage-'));
  const path = join(directory, 'store');
  let store;
  t.after(() => {
    try { store?.close(); } finally { rmSync(directory, { recursive: true, force: true }); }
  });
  store = JsStore.openOrCreate({ path });
  store.setAutoSnapshot(false);
  return {
    directory, path, store,
    reopen() {
      store.close();
      store = JsStore.open({ path });
      store.setAutoSnapshot(false);
      return store;
    },
  };
}

function logs(store, ...ids) {
  for (const id of ids) store.registerState({ id, strategy: 'append_log' });
}

function stamped(store, slot, item, idKey = 'id', seqKey = 'sequence') {
  const record = store.appendToStateJsonWithIdentity(slot, item, idKey, seqKey);
  assert.equal(typeof record.id, 'string');
  assert.ok(Number.isInteger(record.sequence));
  return { ...item, [idKey]: record.id, [seqKey]: record.sequence };
}

const present = (store, item) => stamped(store, WIN, item, 'by', 'at');
const values = (view, slots) => slots.map(slot => view.getStateJson(slot));
const edit = (view, slot, index, item) => view.editStateItem(slot, index, Buffer.from(JSON.stringify(item)));
const listed = (store, name) => store.listBranches().find(b => b.name === name);

test('A sampled non-main parent name works for all three named-parent forks', async t => {
  const { store } = fresh(t);
  logs(store, MSG, AUX, WIN, CTX);
  store.registerState({ id: STRATEGY, strategy: 'snapshot' });
  const row = stamped(store, MSG, { text: 'primary' });
  stamped(store, AUX, { text: 'auxiliary' });
  present(store, { id: row.id });
  store.setStateJson(STRATEGY, { phase: 'initial' });

  const source = store.view(store.createBranch('dendrite/base').name);
  const twin = store.view(store.createBranch('dendrite/twin').name);
  for (const v of [source, twin]) v.appendToStateJson(CTX, { note: 'local to this derivation' });
  const slots = [MSG, AUX, WIN, CTX, STRATEGY];
  const cut = source.currentSequence();
  const atCut = values(source, slots);
  for (const v of [source, twin]) v.setStateJson(STRATEGY, { phase: 'live' });
  const parent = source.currentBranch();
  const head = source.currentSequence();
  const liveSeed = values(source, slots);
  const parentContext = source.stateHeadOffset(CTX);
  assert.ok(Number.isFinite(parentContext));
  assert.notEqual(twin.stateHeadOffset(CTX), parentContext);
  assert.ok(atCut.every((value) => value !== null) && liveSeed.every((value) => value !== null));
  assert.deepEqual([atCut.at(-1), liveSeed.at(-1)], [{ phase: 'initial' }, { phase: 'live' }]);
  assert.deepEqual(values(twin, slots), liveSeed);
  assert.notEqual(twin.currentBranch().id, parent.id);
  assert.notDeepEqual(values(store, slots), liveSeed);

  const plainName = 'recovery/ada/123';
  const liveName = 'dendrite/inspect/ada/1';
  const histName = 'dendrite/inspect/ada/2';
  const filter = { stateIds: [AUX, MSG], statePrefixes: ['strategy/', 'ada/'] };
  const write = nextTurn().then(() => {
    const later = stamped(source, MSG, { text: 'next turn' });
    stamped(source, AUX, { text: 'next auxiliary' });
    present(source, { id: later.id });
    source.appendToStateJson(CTX, { note: 'next context' });
    source.setStateJson(STRATEGY, { phase: 'next' });
  });
  let plain, live, hist;
  try {
    const a = source.createBranchAt(plainName, parent.name, cut);
    plain = source.view(plainName);
    const b = source.createBranchWithStates(liveName, parent.name, filter);
    assert.deepEqual([filter.stateIds, filter.statePrefixes], [[AUX, MSG], ['strategy/', 'ada/']]);
    live = source.view(liveName);
    const c = source.createBranchAtWithStates(histName, parent.name, cut, filter);
    hist = source.view(histName);

    assert.deepEqual([plain, live, hist].map(v => v.currentBranch().parentId), [parent.id, parent.id, parent.id]);
    assert.deepEqual([
      twin.createBranchAt('via-twin/plain', parent.name, cut).parentId,
      twin.createBranchWithStates('via-twin/live', parent.name, filter).parentId,
      twin.createBranchAtWithStates('via-twin/past', parent.name, cut, filter).parentId,
    ], [parent.id, parent.id, parent.id]);
    assert.deepEqual(['via-twin/plain', 'via-twin/live', 'via-twin/past'].map(name => source.view(name).stateHeadOffset(CTX)),
      [parentContext, parentContext, parentContext]);
    for (const v of [plain, store.view(plainName)]) assert.deepEqual([v.currentBranch().id, values(v, slots)], [a.id, atCut]);
    assert.deepEqual([b.parentId, live.currentBranch().branchPoint, live.currentSequence(), live.getStateJson(STRATEGY)],
      [parent.id, head, head, liveSeed.at(-1)]);
    assert.deepEqual([live.currentBranch().id, values(live, slots)], [b.id, liveSeed]);
    assert.deepEqual([hist.currentBranch().id, hist.currentSequence(), values(hist, slots)], [c.id, cut, atCut]);
    assert.deepEqual([hist.currentBranch().parentId, hist.getStateJson(CTX)], [parent.id, [{ note: 'local to this derivation' }]]);
    const link = listed(store, histName);
    assert.deepEqual([link.parentId, link.branchPoint], [parent.id, cut]);

    assert.throws(() => source.createBranchAt(plainName, parent.name, head));
    assert.deepEqual([listed(store, plainName).id, plain.currentBranch().name, values(plain, slots)], [a.id, plainName, atCut]);
    assert.equal(a.name, plainName);
    store.switchBranch(a.name);
    assert.deepEqual([store.currentBranch().id, store.currentSequence()], [a.id, cut]);
  } finally {
    await write;
  }
  assert.ok(source.currentSequence() > head);
  assert.deepEqual([values(plain, slots), values(live, slots), values(hist, slots)], [atCut, liveSeed, atCut]);
});

test('A selective fork of a selectively derived parent leaves out what that parent left out', t => {
  const { store } = fresh(t);
  logs(store, MSG, AUX);
  const row = stamped(store, MSG, { text: 'primary' });
  stamped(store, AUX, { text: 'ledger on main' });
  const parentName = store.createBranchWithStates('dendrite/selective', 'main', { stateIds: [MSG] }).name;
  const parent = store.view(parentName);
  const later = stamped(parent, MSG, { text: 'on the parent' });
  assert.deepEqual([store.getStateJson(AUX).length, parent.getStateJson(AUX), parent.getStateJson(MSG)], [1, null, [row, later]]);
  const cut = parent.currentSequence();
  const filter = { stateIds: [MSG, AUX] };
  const live = store.view(store.createBranchWithStates('grandchild/live', parentName, filter).name);
  const past = store.view(store.createBranchAtWithStates('grandchild/past', parentName, cut, filter).name);
  for (const child of [live, past]) {
    assert.deepEqual([child.getStateJson(AUX), child.getStateJson(MSG)], [null, [row, later]]);
  }
});

test('A selective head fork shares the seed records but subsequent writes diverge', t => {
  const { store: parent } = fresh(t);
  logs(parent, MSG, AUX, WIN);
  const slots = [MSG, AUX, WIN];
  const filter = { stateIds: slots };
  const p = parent.currentBranch().name;
  const first = stamped(parent, MSG, { text: 'first' });
  const aux = stamped(parent, AUX, { text: 'peer' });
  const w1 = present(parent, { id: first.id });
  const earlier = parent.currentSequence();
  const last = stamped(parent, MSG, { text: 'last' });
  const auxLast = stamped(parent, AUX, { text: 'peer last' });
  const w2 = present(parent, { id: last.id });
  const head = parent.currentSequence();
  const seed = values(parent, slots);
  assert.deepEqual(seed, [[first, last], [aux, auxLast], [w1, w2]]);
  const offsets = slots.map(id => parent.stateHeadOffset(id));
  assert.ok(offsets.every(Number.isFinite));
  const records = parent.stats().recordCount;
  assert.ok(records > 0);
  const live = parent.view(parent.createBranchWithStates('live', p, filter).name);
  assert.deepEqual([parent.stats().recordCount, values(live, slots), slots.map(id => live.getStateLen(id)), slots.map(id => live.stateHeadOffset(id))],
    [records, seed, seed.map(items => items.length), offsets]);
  const stamp = ({ id, sequence }) => [id, sequence];
  assert.deepEqual([
    ...[MSG, AUX].map(id => stamp(live.getStateItemJson(id, live.getStateLen(id) - 1))), live.stateHeadOffset(WIN),
  ], [stamp(last), stamp(auxLast), parent.stateHeadOffset(WIN)]);

  const past = parent.view(parent.createBranchAtWithStates('past', p, earlier, filter).name);
  assert.deepEqual([live.currentBranch().branchPoint, values(live, slots), past.currentBranch().branchPoint, values(past, slots)],
    [head, seed, earlier, [[first], [aux], [w1]]]);

  const inherited = values(live, [MSG, AUX]).flat();
  const childOnly = stamped(live, MSG, { text: 'private to child' });
  assert.ok(inherited.every(item => item.sequence <= head));
  assert.deepEqual([childOnly.sequence, live.getStateItemJson(MSG, 2)], [head + 1, childOnly]);
  const onChild = { ...first, text: 'edited on child' };
  edit(live, MSG, 0, onChild);
  assert.deepEqual(parent.getStateJson(MSG), [first, last]);
  const onParent = { ...last, text: 'edited on parent' };
  edit(parent, MSG, 1, onParent);
  const parentOnly = stamped(parent, MSG, { text: 'private to parent' });
  assert.deepEqual([live.getStateJson(MSG), parent.getStateJson(MSG)], [[onChild, last, childOnly], [first, onParent, parentOnly]]);
});

test('Cold historical child keeps its selected prefix, references and later private window', t => {
  const fixture = fresh(t);
  let parent = fixture.store;
  const tree = 'mint-preimage-envelopes';
  const ledger = 'ada/quarantine';
  logs(parent, MSG, WIN, ledger);
  parent.registerState({ id: tree, strategy: 'tree' });
  const note = stamped(parent, ledger, { note: 'not selected' });
  const first = stamped(parent, MSG, { text: 'first' });
  const second = stamped(parent, MSG, { text: 'second' });
  const bytes = Buffer.from([0, 0xfe, 42, 0xff]);
  const requestKey = createHash('sha256').update('authoring request').digest('hex');
  const hash = parent.storeBlob(bytes, 'application/octet-stream');
  parent.treeSet(tree, requestKey, { blobHash: hash, size: bytes.length, mode: 0o644 });
  const cut = parent.currentSequence();
  const oldEnd = parent.stateHeadOffset(MSG);
  assert.ok(Number.isFinite(oldEnd));
  const late = stamped(parent, MSG, { text: 'past the cut' });
  const parentWindow = present(parent, { id: late.id });
  const presentedCut = parent.currentSequence();
  const windowEnd = parent.stateHeadOffset(WIN);
  const retraction = present(parent, { retract: late.id });
  const parentIndex = [parentWindow, retraction];
  assert.ok(Number.isFinite(windowEnd));
  assert.notEqual(parent.stateHeadOffset(WIN), windowEnd);
  assert.notEqual(parent.stateHeadOffset(MSG), oldEnd);
  parent.treeRemove(tree, requestKey);
  const records = parent.stats().recordCount;
  assert.ok(records > 0);
  const name = 'history/ada';
  const branch = parent.createBranchAtWithStates(name, parent.currentBranch().name, cut, { stateIds: [MSG, WIN, tree] });
  const indexedName = 'history/presented';
  parent.createBranchAtWithStates(indexedName, parent.currentBranch().name, presentedCut, { stateIds: [MSG, WIN] });
  assert.equal(parent.stats().recordCount, records);
  parent = fixture.reopen();
  const child = parent.view(name);
  assert.deepEqual([child.getStateJson(MSG), child.stateHeadOffset(MSG)], [[first, second], oldEnd]);
  const indexed = parent.view(indexedName);
  assert.deepEqual([indexed.getStateJson(MSG), indexed.getStateJson(WIN), indexed.stateHeadOffset(WIN)],
    [[first, second, late], [parentWindow], windowEnd]);
  assert.equal(child.getStateJson(WIN), null);
  const entry = child.treeGet(tree, requestKey);
  assert.deepEqual([entry.blobHash, child.getBlob(entry.blobHash)], [hash, bytes]);

  const childOnly = stamped(child, MSG, { text: 'child arrival' });
  assert.ok(second.sequence <= cut && parent.currentSequence() > cut + 1);
  assert.deepEqual([childOnly.sequence, child.getStateItemJson(MSG, 2)], [cut + 1, childOnly]);
  const onChild = { ...first, text: 'changed by child' };
  edit(child, MSG, 0, onChild);
  const childWindow = present(child, { id: childOnly.id });
  assert.deepEqual(values(parent, [MSG, WIN]), [[first, second, late], parentIndex]);
  const onParent = { ...second, text: 'changed by parent' };
  edit(parent, MSG, 1, onParent);
  const parentOnly = stamped(parent, MSG, { text: 'another parent arrival' });
  const childRows = [onChild, second, childOnly];
  const parentRows = [first, onParent, late, parentOnly];
  assert.deepEqual([child.getStateJson(MSG), parent.getStateJson(MSG)], [childRows, parentRows]);

  parent = fixture.reopen();
  const rebound = parent.view(name);
  assert.deepEqual([rebound.currentBranch().id, rebound.getStateJson(MSG), rebound.getStateJson(ledger), rebound.treeGet(tree, requestKey)],
    [branch.id, childRows, null, entry]);
  assert.deepEqual([parent.currentBranch().name, rebound.getStateJson(WIN)], ['main', [childWindow]]);
  assert.deepEqual([values(rebound, [WIN, MSG, ledger]), values(parent, [WIN, MSG, ledger]), parent.view(indexedName).getStateJson(WIN)],
    [[[childWindow], childRows, null], [parentIndex, parentRows, [note]], [parentWindow]]);
});

test('Selective fork input refusals do not allocate or adopt a child', t => {
  const { store } = fresh(t);
  logs(store, MSG);
  const parent = stamped(store, MSG, { text: 'parent row' });
  const occupied = 'occupied';
  store.createBranch(occupied);
  const owner = store.view(occupied);
  const privateRow = stamped(owner, MSG, { text: 'occupant row' });
  const from = store.currentBranch().name;
  const cut = store.currentSequence();
  const filter = { stateIds: [MSG] };
  const observe = () => ({
    branches: store.listBranches().map(({ name, id }) => [name, id]).sort(),
    cursor: store.currentBranch().id,
    occupant: store.view(occupied).getStateJson(MSG),
  });
  const before = observe();
  assert.deepEqual(before.occupant, [parent, privateRow]);
  for (const fail of [
    () => store.createBranchWithStates(occupied, from, filter),
    () => store.createBranchWithStates('missing/live', 'no such parent', filter),
  ]) {
    assert.throws(fail);
    assert.deepEqual(observe(), before);
  }
  for (const fail of [
    () => store.createBranchAtWithStates(occupied, from, cut, filter),
    () => store.createBranchAtWithStates('missing/past', 'no such parent', cut, filter),
    () => store.createBranchAtWithStates('beyond/past', from, cut + 1, filter),
  ]) {
    assert.throws(fail);
    assert.deepEqual(observe(), before);
  }
});

function withStoreDirectoryHidden(path, run) {
  const saved = `${path}-saved`;
  renameSync(path, saved);
  try {
    writeFileSync(path, 'not a directory');
    run();
  } finally {
    rmSync(path, { force: true });
    renameSync(saved, path);
  }
}

function forkUnderFailedCheckpoint(t, fork) {
  const fixture = fresh(t);
  const { store } = fixture;
  logs(store, MSG);
  const row = stamped(store, MSG, { text: 'parent row' });
  const from = store.currentBranch().name;
  const cut = store.currentSequence();
  const names = (handle = store) => handle.listBranches().map(({ name }) => name).sort();
  const original = names();
  store.sync();
  let thrown = null;
  let during;
  withStoreDirectoryHidden(fixture.path, () => {
    try { fork(store, from, cut); } catch (error) { thrown = error; }
    during = names();
  });
  return { fixture, store, row, names, original, thrown, during, retry: () => fork(store, from, cut) };
}

const forks = {
  live: (store, from) => store.createBranchWithStates('derived/live', from, { stateIds: [MSG] }),
  past: (store, from, cut) => store.createBranchAtWithStates('derived/past', from, cut, { stateIds: [MSG] }),
};

test('A selective fork whose own checkpoint fails throws', {
  skip: process.platform === 'win32' && 'renames the open store directory',
}, t => {
  for (const fork of Object.values(forks)) {
    const { thrown } = forkUnderFailedCheckpoint(t, fork);
    assert.deepEqual([thrown?.code, /\(os error (\d+)\)$/.exec(thrown?.message ?? '')?.[1]], [ErrorCode.Io, String(osConstants.errno.ENOTDIR)]);
  }
});

for (const [kind, method] of [['live', 'createBranchWithStates'], ['past', 'createBranchAtWithStates']]) {
  test(`${method} whose own checkpoint fails leaves the name free, and a retry makes the child`, {
    skip: process.platform === 'win32' && 'renames the open store directory',
    todo: `${method} keeps the child it allocated when its own checkpoint fails: the name stays taken, and a retry throws BranchExists.`,
  }, t => {
    const { fixture, row, names, original, thrown, during, retry } = forkUnderFailedCheckpoint(t, forks[kind]);
    assert.deepEqual([thrown?.code, /\(os error (\d+)\)$/.exec(thrown?.message ?? '')?.[1]], [ErrorCode.Io, String(osConstants.errno.ENOTDIR)]);
    assert.deepEqual(during, original);
    assert.deepEqual(names(), original);
    const child = retry();
    assert.equal(child.name, `derived/${kind}`);
    assert.deepEqual(names(), [...original, `derived/${kind}`].sort());
    const reopened = fixture.reopen();
    assert.deepEqual([names(reopened), reopened.view(`derived/${kind}`).getStateJson(MSG)],
      [[...original, `derived/${kind}`].sort(), [row]]);
  });
}

test('Storage, presentation and later writes have distinct inclusive cutoffs', t => {
  const { store } = fresh(t);
  const PLAN = 'workspace/plan';
  const DIRECT = 'workspace/direct';
  const BIND = 'ada/binding';
  logs(store, MSG, AUX, WIN, CTX);
  for (const id of [STRATEGY, PLAN, DIRECT, BIND]) store.registerState({ id, strategy: 'snapshot' });
  const parent = store.view(store.currentBranch().name);
  const other = store.view(store.createBranch('independent').name);
  const visible = stamped(parent, MSG, { text: 'visible' });
  const hidden = stamped(parent, AUX, { text: 'not presented' });
  const legacyMark = parent.currentSequence();
  const runStart = legacyMark;
  const binding = { startedAfter: legacyMark };
  const runEvent = { runStartedAt: runStart };
  parent.setStateJson(BIND, binding);
  parent.appendToStateJson(CTX, runEvent);
  const show = present(parent, { id: visible.id });
  const postPresentation = parent.currentSequence();
  const plan = { path: 'draft.txt', version: 'planned' };
  parent.setStateJson(PLAN, plan);
  const planned = parent.currentSequence();
  const since = { name: parent.currentBranch().name, id: parent.currentBranch().id, head: planned };
  store.switchBranch(other.currentBranch().name);
  const outside1 = stamped(other, MSG, { text: 'other first' });
  const outside2 = stamped(other, AUX, { text: 'other second' });
  const direct = { path: 'draft.txt', operation: 'unlink' };
  parent.setStateJson(DIRECT, direct);
  const afterWrite = parent.currentSequence();
  const laterVisible = stamped(parent, MSG, { text: 'later primary' });
  const laterHidden = stamped(parent, AUX, { text: 'later auxiliary' });
  present(parent, { id: laterVisible.id });
  present(parent, { retract: visible.id });
  parent.appendToStateJson(CTX, { summary: 'after final message' });
  parent.setStateJson(STRATEGY, { through: laterVisible.id });

  const fork = (name, from, cut) => store.view(store.createBranchAt(name, from, cut).name);
  const atVisible = fork('at-first-storage', since.name, visible.sequence);
  const atHidden = fork('at-second-storage', since.name, hidden.sequence);
  const atPresentation = fork('at-presentation', since.name, postPresentation);
  assert.ok(show.at > hidden.sequence && postPresentation >= show.at);
  assert.deepEqual([atVisible.getStateJson(WIN), atHidden.getStateJson(WIN), atPresentation.getStateJson(WIN)], [null, null, [show]]);
  const rows = values(parent, [MSG, AUX]).flat();
  assert.deepEqual(rows.filter(row => row.sequence <= postPresentation), [visible, hidden]);
  assert.deepEqual(rows.filter(row => row.sequence > postPresentation), [laterVisible, laterHidden]);
  assert.deepEqual(values(atPresentation, [MSG, AUX, WIN]), [[visible], [hidden], [show]]);
  const arrival = stamped(atHidden, MSG, { text: 'new on historical fork' });
  assert.deepEqual([values(atHidden, [MSG, AUX]), arrival.sequence], [[[visible, arrival], [hidden]], hidden.sequence + 1]);

  const onPlan = fork('at-plan', since.name, since.head);
  const afterPlan = fork('after-plan', since.name, afterWrite);
  for (const [child, cut, plannedValue, directValue] of [
    [atPresentation, postPresentation, null, null], [onPlan, planned, plan, null], [afterPlan, afterWrite, plan, direct],
  ]) {
    const link = listed(store, child.currentBranch().name);
    assert.deepEqual([link.parentId, link.branchPoint, child.getStateJson(PLAN), child.getStateJson(DIRECT)],
      [since.id, cut, plannedValue, directValue]);
  }

  const allSlots = [MSG, AUX, WIN, CTX, STRATEGY, BIND, PLAN, DIRECT];
  const fullName = 'full';
  const full = fork(fullName, since.name, parent.currentSequence());
  const parentSlots = values(parent, allSlots);
  assert.ok(parentSlots.every((value) => value !== null), 'every slot of the parent is populated');
  assert.deepEqual(values(full, allSlots), parentSlots);
  for (const [name, cut, expected, covered] of [
    ['before-start', visible.sequence, [[visible], null, null, null], [visible]],
    ['at-start', runStart, [[visible], [hidden], null, null], [visible, hidden]],
    ['after-start', laterVisible.sequence, [[visible, laterVisible], [hidden], binding, [runEvent]], [visible, hidden]],
  ]) {
    const v = fork(name, fullName, cut);
    const link = listed(store, name);
    assert.deepEqual([link.parentId, link.branchPoint, values(v, [MSG, AUX, BIND, CTX])], [full.currentBranch().id, cut, expected]);
    const bound = Math.min(legacyMark, link.branchPoint);
    assert.deepEqual(values(v, [MSG, AUX]).flatMap(items => items ?? []).filter(r => r.sequence <= bound), covered);
  }
  const unrelated = fork('unrelated-mark', other.currentBranch().name, legacyMark);
  assert.equal(other.currentSequence(), legacyMark);
  assert.notEqual(outside2.id, hidden.id);
  assert.deepEqual([unrelated.currentBranch().parentId, values(unrelated, [MSG, AUX])], [other.currentBranch().id, [[outside1], [outside2]]]);
});

test('A head can anchor even an empty window', t => {
  const fixture = fresh(t);
  logs(fixture.store, MSG, WIN);
  const branch = fixture.store.createBranch('epoch');
  const view = fixture.store.view(branch.name);
  stamped(view, MSG, { text: 'stored, not presented' });
  assert.equal(view.getStateItemJson(WIN, 0), null);
  const anchor = view.currentSequence();
  assert.ok(Number.isInteger(anchor) && anchor >= 0);
  const file = join(fixture.directory, 'saved-anchor.json');
  writeFileSync(file, JSON.stringify({ branch: branch.name, anchor }));
  const root = fixture.reopen();
  const saved = JSON.parse(readFileSync(file, 'utf8'));
  const again = root.view(saved.branch);
  assert.equal(again.currentSequence(), saved.anchor);
  assert.equal(again.createBranchAt('at-anchor', saved.branch, saved.anchor).branchPoint, saved.anchor);
  assert.throws(() => again.createBranchAt('past-anchor', saved.branch, saved.anchor + 1));
});

test('A redaction on a historical child leaves the source with that row and its rows after the cut', t => {
  const { store } = fresh(t);
  logs(store, MSG);
  const source = store.createBranch('source');
  store.switchBranch(source.name);
  const first = stamped(store, MSG, { text: 'inherited' });
  const second = stamped(store, MSG, { text: 'also inherited' });
  const cut = store.currentSequence();
  const later = stamped(store, MSG, { text: 'after the cut' });
  const child = store.createBranchAt('past', source.name, cut);
  store.switchBranch(child.name);
  store.redactStateItems(MSG, 0, 1);
  assert.deepEqual(store.getStateJson(MSG), [second]);
  store.switchBranch(source.name);
  assert.deepEqual(store.getStateJson(MSG), [first, second, later]);
});

test('A selective historical fork cut at a message append has the message and not its presentation', t => {
  const { store } = fresh(t);
  logs(store, MSG, WIN);
  const row = stamped(store, MSG, { text: 'cut here' });
  const shown = present(store, { id: row.id });
  const child = store.view(store.createBranchAtWithStates('at-message', 'main', row.sequence, { stateIds: [MSG, WIN] }).name);
  assert.deepEqual([child.getStateJson(MSG), child.getStateJson(WIN)], [[row], null]);
  assert.deepEqual(store.getStateJson(WIN), [shown]);
});

test('A selective fork ignores a requested state that was never written and an ID that was never registered', t => {
  const { store } = fresh(t);
  logs(store, MSG, WIN);
  const row = stamped(store, MSG, { text: 'only messages' });
  const index = 'ada/preimage-index';
  const registered = store.listStates().map(({ id }) => id).sort();
  const child = store.view(store.createBranchWithStates('legacy', 'main', { stateIds: [MSG, WIN, index] }).name);
  assert.deepEqual([child.getStateJson(MSG), child.getStateJson(WIN), child.stateHeadOffset(WIN)], [[row], null, null]);
  assert.deepEqual(child.listStates().map(({ id }) => id).sort(), registered);
  assert.equal(child.getStateJson(index), null);
});
