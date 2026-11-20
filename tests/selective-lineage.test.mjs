import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtempSync, readFileSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
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

// Build expectations from the returned record, not a read of the target slot.
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
  // Equal values, but the twin's context row is a record of its own: a
  // child's context head tells which of the two it inherited.
  assert.ok(Number.isFinite(parentContext));
  assert.notEqual(twin.stateHeadOffset(CTX), parentContext);
  // The seeds come from reads, so check first that the writes above are in them.
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
    // pins createBranchWithStates.6: derive can still read both ordered lists on this object.
    assert.deepEqual([filter.stateIds, filter.statePrefixes], [[AUX, MSG], ['strategy/', 'ada/']]);
    live = source.view(liveName);
    const c = source.createBranchAtWithStates(histName, parent.name, cut, filter);
    hist = source.view(histName);

    // pins currentBranch.9: all three sampled-name forks identify this parent, not its equal-data twin.
    assert.deepEqual([plain, live, hist].map(v => v.currentBranch().parentId), [parent.id, parent.id, parent.id]);
    // Also pass the sampled name from a different pinned handle: defaulting to the receiver isn't resolving `from`.
    // pins currentBranch.9: an explicit sampled name identifies the same branch regardless of receiver.
    assert.deepEqual([
      twin.createBranchAt('via-twin/plain', parent.name, cut).parentId,
      twin.createBranchWithStates('via-twin/live', parent.name, filter).parentId,
      twin.createBranchAtWithStates('via-twin/past', parent.name, cut, filter).parentId,
    ], [parent.id, parent.id, parent.id]);
    // pins currentBranch.9: each of those children inherited the named parent's context record, not the receiver's.
    assert.deepEqual(['via-twin/plain', 'via-twin/live', 'via-twin/past'].map(name => source.view(name).stateHeadOffset(CTX)),
      [parentContext, parentContext, parentContext]);
    // pins view.2: the slash-delimited name resolves to the fork and its rows from either handle.
    for (const v of [plain, store.view(plainName)]) assert.deepEqual([v.currentBranch().id, values(v, slots)], [a.id, atCut]);
    // pins createBranchWithStates.2: a named derived parent's head is both the fork point and initial head.
    assert.deepEqual([b.parentId, live.currentBranch().branchPoint, live.currentSequence(), live.getStateJson(STRATEGY)],
      [parent.id, head, head, liveSeed.at(-1)]);
    // pins createBranchWithStates.3: the slash-named view immediately reads the entire pre-turn seed.
    assert.deepEqual([live.currentBranch().id, values(live, slots)], [b.id, liveSeed]);
    // pins createBranchAtWithStates.2: the slash-named historical view is open and populated before yielding.
    assert.deepEqual([hist.currentBranch().id, hist.currentSequence(), values(hist, slots)], [c.id, cut, atCut]);
    // pins createBranchAtWithStates.3: the grandchild inherited the parent's own context row, absent on main.
    assert.deepEqual([hist.currentBranch().parentId, hist.getStateJson(CTX)], [parent.id, [{ note: 'local to this derivation' }]]);
    // pins createBranchAtWithStates.14: the published lineage carries the parent ID and requested inclusive cut.
    const link = listed(store, histName);
    assert.deepEqual([link.parentId, link.branchPoint], [parent.id, cut]);

    // pins createBranchAt.1: creation under the exact recovery name can't adopt or replace the occupant.
    assert.throws(() => source.createBranchAt(plainName, parent.name, head));
    assert.deepEqual([listed(store, plainName).id, plain.currentBranch().name, values(plain, slots)], [a.id, plainName, atCut]);
    // pins createBranchAt.2: the unawaited result's string name already works with switchBranch.
    assert.equal(a.name, plainName);
    store.switchBranch(a.name);
    assert.deepEqual([store.currentBranch().id, store.currentSequence()], [a.id, cut]);
  } finally {
    await write;
  }
  assert.ok(source.currentSequence() > head);
  // pins createBranchWithStates.3: later parent writes to every seed slot miss the synchronous checkpoint.
  assert.deepEqual([values(plain, slots), values(live, slots), values(hist, slots)], [atCut, liveSeed, atCut]);
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
  // pins createBranchWithStates.7: no seed records were added; full values, lengths and chain endpoints are shared.
  assert.deepEqual([parent.stats().recordCount, values(live, slots), slots.map(id => live.getStateLen(id)), slots.map(id => live.stateHeadOffset(id))],
    [records, seed, seed.map(items => items.length), offsets]);
  // pins view.13: each slot's last-item probe matches the parent's stamp and the window revision matches too.
  const stamp = ({ id, sequence }) => [id, sequence];
  assert.deepEqual([
    ...[MSG, AUX].map(id => stamp(live.getStateItemJson(id, live.getStateLen(id) - 1))), live.stateHeadOffset(WIN),
  ], [stamp(last), stamp(auxLast), parent.stateHeadOffset(WIN)]);

  const past = parent.view(parent.createBranchAtWithStates('past', p, earlier, filter).name);
  // pins currentSequence.13: the live head admits the full seed; the earlier cut requires its own cold readings.
  assert.deepEqual([live.currentBranch().branchPoint, values(live, slots), past.currentBranch().branchPoint, values(past, slots)],
    [head, seed, earlier, [[first], [aux], [w1]]]);

  const inherited = values(live, [MSG, AUX]).flat();
  const childOnly = stamped(live, MSG, { text: 'private to child' });
  // pins createBranchWithStates.8: inherited stamps are on the old side of the head, first child stamp exactly one past it.
  assert.ok(inherited.every(item => item.sequence <= head));
  assert.deepEqual([childOnly.sequence, live.getStateItemJson(MSG, 2)], [head + 1, childOnly]);
  const onChild = { ...first, text: 'edited on child' };
  edit(live, MSG, 0, onChild);
  // pins createBranchWithStates.10: neither child's append nor its edit of the first inherited row reached the parent.
  assert.deepEqual(parent.getStateJson(MSG), [first, last]);
  const onParent = { ...last, text: 'edited on parent' };
  edit(parent, MSG, 1, onParent);
  const parentOnly = stamped(parent, MSG, { text: 'private to parent' });
  // pins createBranchWithStates.10: both edits and both appended tails persist only on their own side.
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
  // Contrast the unwritten window with a second historical cut at its presentation record.
  const indexedName = 'history/presented';
  parent.createBranchAtWithStates(indexedName, parent.currentBranch().name, presentedCut, { stateIds: [MSG, WIN] });
  // pins createBranchAtWithStates.9: neither historical fork allocated new log records.
  assert.equal(parent.stats().recordCount, records);
  parent = fixture.reopen();
  const child = parent.view(name);
  // pins createBranchAtWithStates.9: a cold read has the original stamps AND points to the original chain endpoint.
  assert.deepEqual([child.getStateJson(MSG), child.stateHeadOffset(MSG)], [[first, second], oldEnd]);
  const indexed = parent.view(indexedName);
  // pins createBranchAtWithStates.9: a written index still names the same row and ends at the original presentation record.
  assert.deepEqual([indexed.getStateJson(MSG), indexed.getStateJson(WIN), indexed.stateHeadOffset(WIN)],
    [[first, second, late], [parentWindow], windowEnd]);
  // pins createBranchAtWithStates.8: even selected and registered, the window wasn't written until after the cut.
  assert.equal(child.getStateJson(WIN), null);
  // pins createBranchAtWithStates.10: the inherited request key resolves an actual blob from the historical child.
  const entry = child.treeGet(tree, requestKey);
  assert.deepEqual([entry.blobHash, child.getBlob(entry.blobHash)], [hash, bytes]);

  const childOnly = stamped(child, MSG, { text: 'child arrival' });
  // pins createBranchAtWithStates.13: numbering resumes at the cut, not the later parent head.
  assert.ok(second.sequence <= cut && parent.currentSequence() > cut + 1);
  assert.deepEqual([childOnly.sequence, child.getStateItemJson(MSG, 2)], [cut + 1, childOnly]);
  const onChild = { ...first, text: 'changed by child' };
  edit(child, MSG, 0, onChild);
  const childWindow = present(child, { id: childOnly.id });
  // pins createBranchAtWithStates.12: child edits and appends have not reached the live parent.
  assert.deepEqual(values(parent, [MSG, WIN]), [[first, second, late], parentIndex]);
  const onParent = { ...second, text: 'changed by parent' };
  edit(parent, MSG, 1, onParent);
  const parentOnly = stamped(parent, MSG, { text: 'another parent arrival' });
  const childRows = [onChild, second, childOnly];
  const parentRows = [first, onParent, late, parentOnly];
  // pins createBranchAtWithStates.12: a parent edit of a different inherited row and a later append both miss the child.
  assert.deepEqual([child.getStateJson(MSG), parent.getStateJson(MSG)], [childRows, parentRows]);

  parent = fixture.reopen();
  const rebound = parent.view(name);
  // pins createBranchAtWithStates.15: the saved name reopens the same child, with its selected prefix and private changes.
  assert.deepEqual([rebound.currentBranch().id, rebound.getStateJson(MSG), rebound.getStateJson(ledger), rebound.treeGet(tree, requestKey)],
    [branch.id, childRows, null, entry]);
  // pins view.9: root was left on the parent, but the rebound view reads the window as the child left it.
  assert.deepEqual([parent.currentBranch().name, rebound.getStateJson(WIN)], ['main', [childWindow]]);
  // pins appendToStateJsonWithIdentity.1: the stamped window item survived on just the target branch and slot.
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
    // pins createBranchWithStates.13: synchronous refusal, no new ID, replacement, or move to the occupant.
    assert.throws(fail);
    assert.deepEqual(observe(), before);
  }
  for (const fail of [
    () => store.createBranchAtWithStates(occupied, from, cut, filter),
    () => store.createBranchAtWithStates('missing/past', 'no such parent', cut, filter),
    () => store.createBranchAtWithStates('beyond/past', from, cut + 1, filter),
  ]) {
    // pins createBranchAtWithStates.16: all three input refusals throw and leave the existing branches intact.
    assert.throws(fail);
    assert.deepEqual(observe(), before);
  }
});

// Obstruct pathname lookups in the store's directory, not any particular file:
// the open log still takes records, but no checkpoint can be saved. Restore
// it before anything else writes, and on every exit.
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

test('A selective fork whose own checkpoint fails throws, and the store keeps the child',
  { skip: process.platform === 'win32' && 'renames the open store directory' }, t => {
    const fixture = fresh(t);
    const { store } = fixture;
    logs(store, MSG);
    const row = stamped(store, MSG, { text: 'parent row' });
    const from = store.currentBranch().name;
    const cut = store.currentSequence();
    const filter = { stateIds: [MSG] };
    const names = (handle = store) => handle.listBranches().map(({ name }) => name).sort();
    const original = names();
    // The throw comes from the obstructed save, not from a refusal of the input.
    const ioError = error => error?.code === ErrorCode.Io;
    const exists = error => error?.code === ErrorCode.BranchExists;
    const live = 'derived/live';
    const past = 'derived/past';

    // A valid parent and fresh names, and the sync leaves no checkpoint pending:
    // the publication of the call's own child is the first write to fail.
    store.sync();
    withStoreDirectoryHidden(fixture.path, () => {
      // pins createBranchWithStates.13: a failure to publish the new child throws in the call.
      assert.throws(() => store.createBranchWithStates(live, from, filter), ioError);
      // differs from createBranchWithStates.13, which leaves no child to clean up: the store keeps the child it allocated.
      assert.deepEqual(names(), [...original, live].sort());
    });
    // differs from createBranchWithStates.13: the next checkpoint publishes the kept child, so a retry finds the name taken.
    assert.throws(() => store.createBranchWithStates(live, from, filter), exists);
    assert.deepEqual(store.view(live).getStateJson(MSG), [row]);

    store.sync();
    withStoreDirectoryHidden(fixture.path, () => {
      // pins createBranchAtWithStates.16: a failure to publish the new child throws before returning one.
      assert.throws(() => store.createBranchAtWithStates(past, from, cut, filter), ioError);
      // differs from createBranchAtWithStates.16, which leaves no child for derive to clean up: the store keeps the child.
      assert.deepEqual(names(), [...original, live, past].sort());
    });
    // differs from createBranchAtWithStates.16: the next checkpoint publishes the kept child, so a retry finds the name taken.
    assert.throws(() => store.createBranchAtWithStates(past, from, cut, filter), exists);
    assert.deepEqual(store.view(past).getStateJson(MSG), [row]);

    const reopened = fixture.reopen();
    // The kept children outlast the store's restart: a caller has them to clean up.
    assert.deepEqual([names(reopened), reopened.view(live).getStateJson(MSG), reopened.view(past).getStateJson(MSG)],
      [[...original, live, past].sort(), [row], [row]]);
  });

test('Storage, presentation and later writes have distinct inclusive cutoffs', t => {
  const { store } = fresh(t);
  const PLAN = 'workspace/plan';
  const DIRECT = 'workspace/direct';
  const BIND = 'ada/binding';
  logs(store, MSG, AUX, WIN, CTX);
  for (const id of [STRATEGY, PLAN, DIRECT, BIND]) store.registerState({ id, strategy: 'snapshot' });
  const parent = store.view(store.currentBranch().name);
  const other = store.view(store.createBranch('independent').name); // no rows shared
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
  // Simulate the cursor moving during an unlink. The pre-unlink position, not
  // the current cursor, locates the plan a later fork is to recover.
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
  // pins currentSequence.14: rows are stored first; only the post-presentation head includes the window write.
  assert.ok(show.at > hidden.sequence && postPresentation >= show.at);
  assert.deepEqual([atVisible.getStateJson(WIN), atHidden.getStateJson(WIN), atPresentation.getStateJson(WIN)], [null, null, [show]]);
  // pins currentSequence.3: at a mixed visible/hidden cutoff every earlier stored row is <= it, every later row > it.
  const rows = values(parent, [MSG, AUX]).flat();
  assert.deepEqual(rows.filter(row => row.sequence <= postPresentation), [visible, hidden]);
  assert.deepEqual(rows.filter(row => row.sequence > postPresentation), [laterVisible, laterHidden]);
  assert.deepEqual(values(atPresentation, [MSG, AUX, WIN]), [[visible], [hidden], [show]]);
  // pins createBranchAt.4: source stamps survived the plain historical cut; the next private row starts at cut + 1.
  const arrival = stamped(atHidden, MSG, { text: 'new on historical fork' });
  assert.deepEqual([values(atHidden, [MSG, AUX]), arrival.sequence], [[[visible, arrival], [hidden]], hidden.sequence + 1]);

  const onPlan = fork('at-plan', since.name, since.head);
  const afterPlan = fork('after-plan', since.name, afterWrite);
  // pins currentSequence.7: even after a cursor move, the saved plan head is inclusive but the direct write follows it.
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
  // pins createBranchAt.9: the full head includes every populated slot, even retraction, context and strategy after the last row.
  assert.deepEqual(values(full, allSlots), parentSlots);
  // The following cuts are in `full`'s inherited history. The caller's mark and run start are
  // saved numbers; Chronicle supplies the fork points and rows that make them meaningful.
  for (const [name, cut, expected, covered] of [
    ['before-start', visible.sequence, [[visible], null, null, null], [visible]],
    ['at-start', runStart, [[visible], [hidden], null, null], [visible, hidden]],
    ['after-start', laterVisible.sequence, [[visible, laterVisible], [hidden], binding, [runEvent]], [visible, hidden]],
  ]) {
    const v = fork(name, fullName, cut);
    const link = listed(store, name);
    // pins currentSequence.8: before/at/after the start, a fork in inherited history carries exactly its binding and run prefix.
    assert.deepEqual([link.parentId, link.branchPoint, values(v, [MSG, AUX, BIND, CTX])], [full.currentBranch().id, cut, expected]);
    // pins currentSequence.8: the rows within the smaller of legacy mark and fork point are the original covered rows.
    const bound = Math.min(legacyMark, link.branchPoint);
    assert.deepEqual(values(v, [MSG, AUX]).flatMap(items => items ?? []).filter(r => r.sequence <= bound), covered);
  }
  const unrelated = fork('unrelated-mark', other.currentBranch().name, legacyMark);
  // pins currentSequence.8: the very same numeric mark on an independent history describes different stamped rows.
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
  // pins currentSequence.9: a head exists in finite nonnegative integer coordinates with no window row at the ordinal.
  assert.ok(Number.isInteger(anchor) && anchor >= 0);
  const file = join(fixture.directory, 'saved-anchor.json');
  writeFileSync(file, JSON.stringify({ branch: branch.name, anchor }));
  const root = fixture.reopen();
  const saved = JSON.parse(readFileSync(file, 'utf8'));
  const again = root.view(saved.branch);
  // pins currentSequence.9: the persisted anchor still equals this branch's head; only through that number is a valid cut.
  assert.equal(again.currentSequence(), saved.anchor);
  assert.equal(again.createBranchAt('at-anchor', saved.branch, saved.anchor).branchPoint, saved.anchor);
  assert.throws(() => again.createBranchAt('past-anchor', saved.branch, saved.anchor + 1));
});
