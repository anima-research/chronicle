import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ErrorCode, JsStore } from '../index.js';

function freshStore(t) {
  const dir = mkdtempSync(join(tmpdir(), 'chronicle-branch-catalog-'));
  const options = { path: join(dir, 'store') };
  let store;
  t.after(() => {
    try { store?.close(); } finally { rmSync(dir, { recursive: true, force: true }); }
  });
  store = JsStore.openOrCreate(options);
  return {
    store,
    reopen() {
      store.close();
      store = JsStore.open(options);
      return store;
    },
  };
}

const ref = ({ name, id, created }) => ({ name, id, created });
const pair = ({ id, created }) => [id, created];
const named = (rows, name) => rows.find((row) => row.name === name);
const matchesPair = (rows, saved) => rows.filter((b) => b.id === saved.id && b.created === saved.created);
const sortedRefs = (rows) => rows.map(ref).sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0);

// `live` comes from the creations, not from any earlier list result.
function assertInventory(store, live) {
  const before = ref(store.currentBranch());
  const rows = store.listBranches();
  // pins listBranches.1: directly usable JS array, not an iterable or Promise
  assert.ok(Array.isArray(rows));
  // pins listBranches.2: observing the inventory never moves the cursor
  assert.deepEqual(ref(store.currentBranch()), before);
  // pins listBranches.2: exact live membership, including other handles' mutations
  assert.deepEqual(new Set(rows.map((b) => b.id)), new Set(live.map((b) => b.id)));
  // pins listBranches.10: multiset, not just count or set (and no promised order)
  assert.deepEqual(sortedRefs(rows), sortedRefs(live));
  // pins listBranches.5: unique native strings attached to their particular creations
  assert.equal(new Set(rows.map((b) => b.id)).size, rows.length);
  for (const b of live) {
    const row = named(rows, b.name);
    // pins listBranches.5: the saved ID still resolves to this branch after checkout or reopen
    assert.equal(typeof row.id, 'string');
    assert.equal(row.id, b.id);
  }
  // pins currentBranch.8: either lookup key resolves uniquely to the same creation
  assert.deepEqual(rows.filter((b) => b.id === before.id || b.name === before.name).map(ref), [before]);
  return rows;
}

test('A live branch inventory distinguishes creations, not names or equal heads', (t) => {
  const fixture = freshStore(t);
  let store = fixture.store;
  const main = ref(store.currentBranch());
  const draft = store.createBranch('draft');
  const publisher = store.view(main.name);
  let rows = assertInventory(store, [main, draft]);
  // pins listBranches.6: a listed hit is refused without any intervening mutation
  assert.ok(rows.some((b) => b.name === 'draft'));
  assert.throws(() => publisher.createBranch('draft'), { code: ErrorCode.BranchExists });
  // pins listBranches.6: the exact-name miss is available to create
  assert.ok(!store.listBranches().some((b) => b.name === 'Draft'));
  const sibling = publisher.createBranch('Draft');
  assert.equal(sibling.name, 'Draft');
  assert.equal(draft.head, sibling.head);
  assertInventory(store, [main, draft, sibling]);

  store.switchBranch(draft.name);
  const saved = ref(store.currentBranch());
  const oldId = saved.id;
  // pins currentBranch.4: save the native string pin, not a name or a mutable branch object
  assert.equal(typeof oldId, 'string');
  // pins switchBranch.3: the root now identifies the actual creation it selected
  assert.equal(oldId, draft.id);
  store.switchBranch(sibling.name);
  // pins currentBranch.4: even the equal-head sibling has a distinct ID
  assert.notEqual(store.currentBranch().id, oldId);
  // pins switchBranch.3: the equal-head sibling, not just any different branch, was selected
  assert.equal(store.currentBranch().id, sibling.id);
  store.switchBranch(draft.name);
  const receipt = store.appendJson('branch-receipt', saved);
  // pins currentBranch.4: an advancing sequence does not invalidate the ID
  assert.equal(store.currentBranch().id, saved.id);
  store.switchBranch(sibling.name);
  rows = assertInventory(store, [main, draft, sibling]);
  // pins listBranches.11: a saved provenance pair still matches just its own row, not the selected sibling
  assert.deepEqual(matchesPair(rows, saved).map(ref), [saved]);
  const foreign = freshStore(t).store;
  foreign.createBranch(draft.name);
  foreign.switchBranch(draft.name);
  // pins listBranches.11: a reference from another store does not match a local row, even by the same name
  assert.deepEqual(matchesPair(store.listBranches(), ref(foreign.currentBranch())), []);

  store.switchBranch(draft.name);
  // pins switchBranch.3: returning reports the ID of the original creation
  assert.equal(store.currentBranch().id, draft.id);
  // pins currentBranch.4: the ID saved before writing agrees on return
  assert.equal(store.currentBranch().id, oldId);
  store = fixture.reopen();
  store.switchBranch(draft.name);
  // pins currentBranch.4: the saved pin identifies the same branch on restart
  assert.equal(store.currentBranch().id, oldId);
  assertInventory(store, [main, draft, sibling]);

  store.switchBranch(sibling.name);
  const editor = store.view(main.name);
  editor.deleteBranch(draft.name);
  rows = assertInventory(store, [main, sibling]);
  // pins listBranches.11: surviving journal evidence does not keep the old pair live
  assert.deepEqual(JSON.parse(store.getRecord(receipt.id).payload.toString()), saved);
  assert.deepEqual(matchesPair(rows, saved), []);
  const replacement = editor.createBranch(draft.name);
  // pins currentBranch.4: the replacement cannot reuse the saved ID even if its timestamp changes
  assert.notEqual(replacement.id, oldId);
  store.switchBranch(replacement.name);
  // pins switchBranch.3: a checkout by the reused name lands on its new owner
  assert.equal(store.currentBranch().id, replacement.id);
  // pins currentBranch.4: an old ID cannot validate the selected replacement
  assert.notEqual(store.currentBranch().id, oldId);
  store = fixture.reopen();
  rows = assertInventory(store, [main, sibling, replacement]);
  // pins listBranches.5: the same-name row has its own ID, not the saved pin
  assert.notEqual(named(rows, draft.name).id, saved.id);
  // pins listBranches.11: a name resolves to the live owner; the surviving receipt pair does not
  assert.deepEqual(pair(named(rows, draft.name)), pair(replacement));
  assert.deepEqual(matchesPair(rows, saved), []);
  assert.deepEqual(JSON.parse(store.getRecord(receipt.id).payload.toString()), saved);
});

test('Creation metadata is in epoch microseconds and stays with the identity', (t) => {
  const fixture = freshStore(t);
  let store = fixture.store;
  const before = Date.now();
  const born = store.createBranch('dated');
  const after = Date.now();
  store.switchBranch(born.name);
  const selected = ref(store.currentBranch());
  const listed = ref(named(store.listBranches(), born.name));
  // pins currentBranch.11: numeric creation time paired with this creation's ID
  assert.equal(typeof selected.created, 'number');
  assert.deepEqual(pair(selected), pair(born));
  // pins listBranches.9: directly comparable to the timestamp from native creation
  assert.equal(typeof listed.created, 'number');
  assert.equal(listed.created, born.created);

  store.append('update', Buffer.from('after birth'));
  // pins currentBranch.11: the captured pair is independent of the advancing head
  assert.deepEqual(pair(store.currentBranch()), pair(selected));
  store.switchBranch('main');
  // pins listBranches.9: writes and checking out a different branch cannot change the listed timestamp
  assert.equal(named(store.listBranches(), born.name).created, born.created);
  store.switchBranch(born.name);
  // pins currentBranch.11: the original identity and creation time on return
  assert.deepEqual(pair(store.currentBranch()), pair(selected));
  store = fixture.reopen();
  store.switchBranch(born.name);
  // pins currentBranch.11: the same pair survives a close and reopen
  assert.deepEqual(pair(store.currentBranch()), pair(selected));
  // pins listBranches.9: original timestamp after writes and reopening
  assert.equal(named(store.listBranches(), born.name).created, born.created);
  store.switchBranch('main');
  store.deleteBranch(born.name);
  store.createBranch(born.name);
  store.switchBranch(born.name);
  // pins currentBranch.11: it is the pair, not the clock alone, that must distinguish replacements
  assert.notDeepEqual(pair(store.currentBranch()), pair(selected));

  // A window of ten minutes either side tolerates a step of the wall clock, and
  // still tells microseconds from milliseconds or seconds by far.
  const slack = 10 * 60 * 1000;
  const selectedMs = Math.floor(selected.created / 1000);
  // differs from currentBranch.11, which asks for milliseconds: the store gives microseconds since the epoch.
  assert.ok(before - slack <= selectedMs && selectedMs <= after + slack,
    `currentBranch.created = ${selected.created} µs, expected [${before}, ${after}] ms`);
  const listedMs = Math.floor(listed.created / 1000);
  // differs from listBranches.9, which asks for milliseconds: the store gives the same epoch-microsecond clock.
  assert.ok(before - slack <= listedMs && listedMs <= after + slack,
    `listBranches.created = ${listed.created} µs, expected [${before}, ${after}] ms`);
  assert.equal(listed.created, selected.created);
});

test('Listed ancestry describes immediate parents and inclusive cuts, not current heads', (t) => {
  const { store } = freshStore(t);
  const main = store.currentBranch();
  store.registerState({ id: 'at-cut', strategy: 'snapshot' });
  const mainHead = store.setStateJson('at-cut', 'main after zero').sequence;
  const child = store.createBranchAt('child', main.name, 0);
  const c = store.view(child.name);
  const cut = c.setStateJson('at-cut', 'cut here').sequence;
  c.setStateJson('at-cut', 'past the cut');
  const leaf = store.createBranchAt('leaf', child.name, cut);
  const l = store.view(leaf.name);
  const childHead = c.appendJson('step', 'child only').sequence;
  const leafHead = l.appendJson('step', 'leaf only').sequence;
  const heads = [mainHead, childHead, leafHead];
  assert.ok(cut > 0 && childHead > cut && leafHead > cut);
  assert.equal(new Set(heads).size, 3, 'use distinct heads to catch cross-branch lookups');

  store.switchBranch(main.name);
  const rows = store.listBranches(); // main selected; both forks have advanced
  const [rm, rc, rl] = [main, child, leaf].map((b) => named(rows, b.name));
  // pins createBranchAt.5: each fork has an identity distinct from both other rows
  assert.equal(new Set([rm.id, rc.id, rl.id]).size, 3);
  // pins listBranches.13: each row reports its own current numeric sequence
  assert.ok([rm, rc, rl].every((b) => typeof b.head === 'number'));
  assert.deepEqual([rm.head, rc.head, rl.head], heads);
  for (const [row, made, parent, at] of [[rc, child, main, 0], [rl, leaf, child, cut]]) {
    // pins createBranchAt.5: exact-name lookup yields the new identity, its source and its cut
    assert.deepEqual([row.id, row.parentId, row.branchPoint], [made.id, parent.id, at]);
    assert.notEqual(row.id, parent.id);
  }
  // pins listBranches.8: numeric inclusive cut, including zero, not head or missing metadata
  assert.deepEqual([rm.branchPoint ?? null, rc.branchPoint, rl.branchPoint], [null, 0, cut]);
  // pins listBranches.8: the listed cuts identify the last parent state inherited, at its original sequence
  assert.equal(c.getStateJsonAt('at-cut', rc.branchPoint) ?? null, null);
  assert.equal(l.getStateJsonAt('at-cut', rl.branchPoint), 'cut here');
  assert.equal(l.getStateJson('at-cut'), 'cut here');
  // pins listBranches.7: immediate parents lead from leaf through child to the parentless root
  assert.deepEqual([rm.parentId ?? null, rc.parentId, rl.parentId], [null, rm.id, rc.id]);

  for (const [b, parentId, at] of [[main, null, null], [child, main.id, 0], [leaf, child.id, cut]]) {
    store.switchBranch(b.name);
    const selected = store.currentBranch();
    // pins currentBranch.14: immediate native parent ID, nullish only for a root
    assert.equal(selected.parentId ?? null, parentId);
    // pins currentBranch.15: original numeric fork point, distinguishing zero from absence
    assert.equal(selected.branchPoint ?? null, at);
  }
  store.switchBranch(main.name);
  store.deleteBranch(child.name);
  const afterDelete = store.listBranches();
  // pins listBranches.7: the link remains to the deleted parent, whose row no longer exists
  assert.equal(named(afterDelete, leaf.name).parentId, child.id);
  assert.ok(!afterDelete.some((b) => b.id === child.id));
  const newParent = store.createBranch(child.name);
  const afterReuse = store.listBranches();
  assert.equal(named(afterReuse, child.name).id, newParent.id);
  // pins listBranches.7: name reuse must not reparent the old leaf to the new live branch
  assert.equal(named(afterReuse, leaf.name).parentId, child.id);
  assert.ok(!afterReuse.some((b) => b.id === child.id));
});

test('Deletion through a parent view invalidates only the saved selective creation', (t) => {
  const { store } = freshStore(t);
  store.registerState({ id: 'seed', strategy: 'snapshot' });
  const derived = store.createBranch('derived-parent');
  const parent = store.view(derived.name);
  const cut = parent.setStateJson('seed', 'earlier').sequence;
  parent.setStateJson('seed', 'later');
  const filter = { stateIds: ['seed'] };

  for (const [name, create] of [
    ['at-head', (n) => parent.createBranchWithStates(n, 'derived-parent', filter)],
    ['at-cut', (n) => parent.createBranchAtWithStates(n, 'derived-parent', cut, filter)],
  ]) {
    const made = create(name);
    const oldView = parent.view(name);
    const isMine = (rows) => rows.some((row) => row.name === name && row.id === made.id);
    let rows = parent.listBranches();
    // pins createBranchWithStates.12: in the at-head case the synchronous result names the listed creation
    // pins createBranchAtWithStates.17: in the at-cut case the synchronous result names the listed creation
    assert.equal(typeof made.id, 'string');
    assert.deepEqual([made.name, made.id], [name, named(rows, name).id]);
    // pins listBranches.12: the exact (requested name, returned ID) marks the live cleanup target
    assert.ok(isMine(rows));
    assert.equal(oldView.currentBranch().id, made.id);

    parent.deleteBranch(name); // while the child still has its view
    rows = parent.listBranches();
    // pins deleteBranch.6: a derived parent can delete its child despite the open child view
    assert.equal(named(rows, name), undefined);
    assert.throws(() => oldView.getStateJson('seed'), { code: ErrorCode.BranchNotFound });
    // pins listBranches.12: cleanup cannot match an already removed child
    assert.ok(!isMine(rows));

    const next = create(name);
    rows = parent.listBranches();
    // pins createBranchWithStates.12: recreated at-head branch has a new matching ID
    // pins createBranchAtWithStates.17: recreated historical branch has a new matching ID
    assert.equal(typeof next.id, 'string');
    assert.deepEqual([next.name, next.id], [name, named(rows, name).id]);
    assert.notEqual(next.id, made.id);
    // pins listBranches.12: the same name now live is not sufficient for stale cleanup
    assert.ok(!isMine(rows));
    // pins deleteBranch.6: the parent's view still works; the open deleted view does not follow the name
    assert.equal(parent.currentBranch().id, derived.id);
    assert.equal(parent.getStateJson('seed'), 'later');
    assert.throws(() => oldView.getStateJson('seed'), { code: ErrorCode.BranchNotFound });
  }
});

test('A failure after a successful fork and switch does not undo either', async (t) => {
  const { store } = freshStore(t);
  const registration = { id: 'mount-marker', strategy: 'snapshot' };
  store.registerState(registration);
  const source = store.createBranch('source');
  store.switchBranch(source.name);
  const cut = store.appendJson('step', 'earlier').sequence;
  store.appendJson('step', 'later');

  let target;
  const marker = { at: 'destination only' };
  await assert.rejects(async () => {
    target = store.createBranchAt('diagnostic', source.name, cut);
    store.switchBranch(target.name);
    store.setStateJson(registration.id, marker);
    store.registerState(registration); // a later setup step refuses the duplicate
  }, { code: ErrorCode.StateExists });
  // pins currentBranch.6: a setup error after movement reports target rather than source
  assert.equal(store.currentBranch().id, target.id);
  // pins switchBranch.11: the same handle can inspect the destination and its writes without moving again
  assert.deepEqual([store.currentBranch().id, store.getStateJson(registration.id)], [target.id, marker]);

  store.switchBranch(source.name);
  assert.equal(store.currentBranch().id, source.id);
  assert.equal(store.getStateJson(registration.id) ?? null, null);
  // pins createBranchAt.7: after restoration the successful historical creation still exists and is readable
  assert.equal(named(store.listBranches(), target.name)?.id, target.id);
  assert.deepEqual(store.view(target.name).getStateJson(registration.id), marker);
});
