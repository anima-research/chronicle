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

function assertInventory(store, live) {
  const before = ref(store.currentBranch());
  const rows = store.listBranches();
  assert.ok(Array.isArray(rows));
  assert.deepEqual(ref(store.currentBranch()), before);
  assert.deepEqual(new Set(rows.map((b) => b.id)), new Set(live.map((b) => b.id)));
  assert.deepEqual(sortedRefs(rows), sortedRefs(live));
  assert.equal(new Set(rows.map((b) => b.id)).size, rows.length);
  for (const b of live) {
    const row = named(rows, b.name);
    assert.equal(typeof row.id, 'string');
    assert.equal(row.id, b.id);
  }
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
  assert.ok(rows.some((b) => b.name === 'draft'));
  assert.throws(() => publisher.createBranch('draft'), { code: ErrorCode.BranchExists });
  assert.ok(!store.listBranches().some((b) => b.name === 'Draft'));
  const sibling = publisher.createBranch('Draft');
  assert.equal(sibling.name, 'Draft');
  assert.equal(draft.head, sibling.head);
  assertInventory(store, [main, draft, sibling]);

  store.switchBranch(draft.name);
  const saved = ref(store.currentBranch());
  const oldId = saved.id;
  assert.equal(typeof oldId, 'string');
  assert.equal(oldId, draft.id);
  store.switchBranch(sibling.name);
  assert.notEqual(store.currentBranch().id, oldId);
  assert.equal(store.currentBranch().id, sibling.id);
  store.switchBranch(draft.name);
  const receipt = store.appendJson('branch-receipt', saved);
  assert.equal(store.currentBranch().id, saved.id);
  store.switchBranch(sibling.name);
  rows = assertInventory(store, [main, draft, sibling]);
  assert.deepEqual(matchesPair(rows, saved).map(ref), [saved]);
  const foreign = freshStore(t).store;
  foreign.createBranch(draft.name);
  foreign.switchBranch(draft.name);
  assert.deepEqual(matchesPair(store.listBranches(), ref(foreign.currentBranch())), []);

  store.switchBranch(draft.name);
  assert.equal(store.currentBranch().id, draft.id);
  assert.equal(store.currentBranch().id, oldId);
  store = fixture.reopen();
  store.switchBranch(draft.name);
  assert.equal(store.currentBranch().id, oldId);
  assertInventory(store, [main, draft, sibling]);

  store.switchBranch(sibling.name);
  const editor = store.view(main.name);
  editor.deleteBranch(draft.name);
  rows = assertInventory(store, [main, sibling]);
  assert.deepEqual(JSON.parse(store.getRecord(receipt.id).payload.toString()), saved);
  assert.deepEqual(matchesPair(rows, saved), []);
  const replacement = editor.createBranch(draft.name);
  assert.notEqual(replacement.id, oldId);
  store.switchBranch(replacement.name);
  assert.equal(store.currentBranch().id, replacement.id);
  assert.notEqual(store.currentBranch().id, oldId);
  store = fixture.reopen();
  rows = assertInventory(store, [main, sibling, replacement]);
  assert.notEqual(named(rows, draft.name).id, saved.id);
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
  assert.equal(typeof selected.created, 'number');
  assert.deepEqual(pair(selected), pair(born));
  assert.equal(typeof listed.created, 'number');
  assert.equal(listed.created, born.created);

  store.append('update', Buffer.from('after birth'));
  assert.deepEqual(pair(store.currentBranch()), pair(selected));
  store.switchBranch('main');
  assert.equal(named(store.listBranches(), born.name).created, born.created);
  store.switchBranch(born.name);
  assert.deepEqual(pair(store.currentBranch()), pair(selected));
  store = fixture.reopen();
  store.switchBranch(born.name);
  assert.deepEqual(pair(store.currentBranch()), pair(selected));
  assert.equal(named(store.listBranches(), born.name).created, born.created);
  store.switchBranch('main');
  store.deleteBranch(born.name);
  store.createBranch(born.name);
  store.switchBranch(born.name);
  assert.notDeepEqual(pair(store.currentBranch()), pair(selected));

  const slack = 10 * 60 * 1000;
  const selectedMs = Math.floor(selected.created / 1000);
  assert.ok(before - slack <= selectedMs && selectedMs <= after + slack,
    `currentBranch.created = ${selected.created} µs, expected [${before}, ${after}] ms`);
  const listedMs = Math.floor(listed.created / 1000);
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
  const rows = store.listBranches();
  const [rm, rc, rl] = [main, child, leaf].map((b) => named(rows, b.name));
  assert.equal(new Set([rm.id, rc.id, rl.id]).size, 3);
  assert.ok([rm, rc, rl].every((b) => typeof b.head === 'number'));
  assert.deepEqual([rm.head, rc.head, rl.head], heads);
  for (const [row, made, parent, at] of [[rc, child, main, 0], [rl, leaf, child, cut]]) {
    assert.deepEqual([row.id, row.parentId, row.branchPoint], [made.id, parent.id, at]);
    assert.notEqual(row.id, parent.id);
  }
  assert.deepEqual([rm.branchPoint ?? null, rc.branchPoint, rl.branchPoint], [null, 0, cut]);
  assert.equal(c.getStateJsonAt('at-cut', rc.branchPoint) ?? null, null);
  assert.equal(l.getStateJsonAt('at-cut', rl.branchPoint), 'cut here');
  assert.equal(l.getStateJson('at-cut'), 'cut here');
  assert.deepEqual([rm.parentId ?? null, rc.parentId, rl.parentId], [null, rm.id, rc.id]);

  for (const [b, parentId, at] of [[main, null, null], [child, main.id, 0], [leaf, child.id, cut]]) {
    store.switchBranch(b.name);
    const selected = store.currentBranch();
    assert.equal(selected.parentId ?? null, parentId);
    assert.equal(selected.branchPoint ?? null, at);
  }
  store.switchBranch(main.name);
  store.deleteBranch(child.name);
  const orphanParent = (rows) => {
    const link = named(rows, leaf.name).parentId;
    return rows.filter((b) => b.id === link).map((b) => b.name);
  };
  const afterDelete = store.listBranches();
  assert.ok(!afterDelete.some((b) => b.id === child.id));
  assert.deepEqual(orphanParent(afterDelete), []);
  const newParent = store.createBranch(child.name);
  const afterReuse = store.listBranches();
  assert.equal(named(afterReuse, child.name).id, newParent.id);
  assert.ok(!afterReuse.some((b) => b.id === child.id));
  assert.deepEqual(orphanParent(afterReuse), []);
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
    assert.equal(typeof made.id, 'string');
    assert.deepEqual([made.name, made.id], [name, named(rows, name).id]);
    assert.ok(isMine(rows));
    assert.equal(oldView.currentBranch().id, made.id);

    parent.deleteBranch(name);
    rows = parent.listBranches();
    assert.equal(named(rows, name), undefined);
    assert.throws(() => oldView.getStateJson('seed'), { code: ErrorCode.BranchNotFound });
    assert.ok(!isMine(rows));

    const next = create(name);
    rows = parent.listBranches();
    assert.equal(typeof next.id, 'string');
    assert.deepEqual([next.name, next.id], [name, named(rows, name).id]);
    assert.notEqual(next.id, made.id);
    assert.ok(!isMine(rows));
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
    store.registerState(registration);
  }, { code: ErrorCode.StateExists });
  assert.equal(store.currentBranch().id, target.id);
  assert.deepEqual([store.currentBranch().id, store.getStateJson(registration.id)], [target.id, marker]);

  store.switchBranch(source.name);
  assert.equal(store.currentBranch().id, source.id);
  assert.equal(store.getStateJson(registration.id) ?? null, null);
  assert.equal(named(store.listBranches(), target.name)?.id, target.id);
  assert.deepEqual(store.view(target.name).getStateJson(registration.id), marker);
});
