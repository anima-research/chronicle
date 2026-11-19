import assert from 'node:assert/strict';
import { test } from 'node:test';
import { ErrorCode, JsStore, freshStore, refuses } from './support.mjs';

test("A19 with from left out on the store's own handle, the parent is the current branch", () => {
  const { store } = freshStore();
  const side = store.createBranch('side');
  store.switchBranch('side');
  assert.equal(store.createBranch('child').parentId, side.id);
  store.close();
});

test('A19 E2 createBranch throws BranchNotFound for an unknown from, before it looks at the name', () => {
  const { store } = freshStore();
  refuses(() => store.createBranch('new', 'nowhere'), ErrorCode.BranchNotFound, /^Branch not found: /);
  refuses(() => store.createBranch('main', 'nowhere'), ErrorCode.BranchNotFound, /^Branch not found: /);
  store.close();
});

test('A22 createBranchAt starts the branch at the sequence, with each state as it read on the parent there', () => {
  const { store } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log' });
  store.registerState({ id: 'value', strategy: 'snapshot' });
  store.appendToStateJson('list', 1);
  store.setStateJson('value', 'early');
  const at = store.currentSequence();
  store.appendToStateJson('list', 2);
  store.setStateJson('value', 'late');
  const branch = store.createBranchAt('past', 'main', at);
  assert.equal(branch.branchPoint, at);
  assert.equal(branch.head, at);
  const past = store.view('past');
  assert.deepEqual(past.getStateJson('list'), [1]);
  assert.equal(past.getStateJson('value'), 'early');
  store.close();
});

test('A22 E2 createBranchAt throws BranchExists for a name that a branch has, and BranchNotFound for an unknown from', () => {
  const { store } = freshStore();
  refuses(() => store.createBranchAt('main', 'main', 0), ErrorCode.BranchExists, 'Branch already exists: main');
  refuses(() => store.createBranchAt('new', 'nowhere', 0), ErrorCode.BranchNotFound, /^Branch not found: /);
  store.close();
});

test.todo("A23 L3 on the store's own handle with from left out, the new branch's parent is the current branch", () => {
  const { store } = freshStore();
  const side = store.createBranch('side');
  store.switchBranch('side');
  assert.equal(store.createEmptyBranch('empty').parentId, side.id);
  store.close();
});

test('A24 switchBranch answers with the branch it made current', () => {
  const { store } = freshStore();
  const side = store.createBranch('side');
  const answer = store.switchBranch('side');
  assert.deepEqual(answer, store.currentBranch());
  assert.equal(answer.id, side.id);
  store.close();
});

test('A26 listBranches gives every branch of the store, from any handle', () => {
  const { store } = freshStore();
  for (const name of ['a', 'b', 'c']) store.createBranch(name);
  store.deleteBranch('b');
  const names = (handle) => handle.listBranches().map((branch) => branch.name).sort();
  assert.deepEqual(names(store), ['a', 'c', 'main']);
  assert.deepEqual(names(store.view('a')), ['a', 'c', 'main']);
  store.close();
});

test('A27 deleteBranch through a view deletes another branch', () => {
  const { store } = freshStore();
  store.createBranch('a');
  store.createBranch('b');
  store.view('a').deleteBranch('b');
  assert.deepEqual(store.listBranches().map((branch) => branch.name).sort(), ['a', 'main']);
  store.close();
});

test('T4 main has the ID 1 and no parentId or branchPoint, and each new branch gets the next number, also under the name of a deleted one and after a reopen', () => {
  const { store, dir } = freshStore();
  const main = store.currentBranch();
  assert.equal(main.id, '1');
  assert.equal('parentId' in main, false);
  assert.equal('branchPoint' in main, false);
  assert.equal(store.createBranch('b').id, '2');
  store.deleteBranch('b');
  assert.equal(store.createBranch('b').id, '3');
  store.deleteBranch('b');
  store.close();
  const reopened = JsStore.open({ path: dir });
  assert.equal(reopened.createBranch('b').id, '4');
  reopened.close();
});

test("T4 a branch's parentId stays after its parent is deleted, and created is the time in microseconds since 1970 UTC", () => {
  const { store } = freshStore();
  const parent = store.createBranch('parent');
  const before = Date.now() * 1000;
  const child = store.createBranch('child', 'parent');
  const after = Date.now() * 1000;
  assert.ok(child.created >= before - 60_000_000 && child.created <= after + 60_000_000, `${before} ${child.created} ${after}`);
  store.deleteBranch('parent');
  assert.equal(store.listBranches().find((branch) => branch.name === 'child').parentId, parent.id);
  store.close();
});
