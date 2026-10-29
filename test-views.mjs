// Node smoke test for branch-bound handles ("views"), selective forks and
// the createBranchAt snapshot-accounting fix, through the napi binding.
//
//   npx napi build --platform --features napi-bindings && node test-views.mjs
//
// Uses a fresh temporary store and removes it afterwards, so it can be re-run.

import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { JsStore } from './index.js';

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'chronicle-views-'));
const storePath = path.join(dir, 'store');
let step = 0;
const ok = (msg) => console.log(`✓ ${++step}. ${msg}`);
const messages = (handle) => handle.getStateJson('messages') ?? [];
const texts = (handle) => messages(handle).map((m) => m.text);

try {
  const store = JsStore.openOrCreate({ path: storePath });
  store.registerState({ id: 'messages', strategy: 'append_log', deltaSnapshotEvery: 5, fullSnapshotEvery: 100 });
  store.registerState({ id: 'cfg/model', strategy: 'snapshot' });
  store.registerState({ id: 'cfg/tools', strategy: 'snapshot' });
  store.registerState({ id: 'scratch', strategy: 'snapshot' });
  store.registerState({ id: 'files', strategy: 'tree' });

  for (const text of ['m0', 'm1', 'm2']) store.appendToStateJson('messages', { text, ts: step });
  store.setStateJson('cfg/model', { name: 'parent-model' });
  store.setStateJson('cfg/tools', ['a', 'b']);
  store.setStateJson('scratch', { note: 'parent scratch' });
  store.treeSet('files', 'README.md', { blobHash: 'h0', size: 1, mode: 0o644 });

  // --- root handle is a root handle -------------------------------------
  assert.equal(store.pinnedBranch(), null);
  assert.equal(store.currentBranch().name, 'main');
  assert.throws(() => store.view('no-such-branch'), /Branch not found/);
  ok('root handle: pinnedBranch() is null, view() of a missing branch throws');

  // --- two views on sibling branches ------------------------------------
  const a = store.createBranch('a');
  const b = store.createBranch('b');
  const va = store.view('a');
  const vb = store.view('b');
  assert.equal(va.pinnedBranch(), 'a');
  assert.equal(vb.pinnedBranch(), 'b');
  assert.equal(va.currentBranch().name, 'a');
  assert.equal(va.currentBranch().id, a.id);
  assert.equal(vb.currentBranch().name, 'b');
  assert.equal(store.currentBranch().name, 'main');
  assert.deepEqual(texts(va), ['m0', 'm1', 'm2']);
  ok('view(): pinnedBranch/currentBranch report the pinned branch; inherited state is readable');

  const forkSeq = store.currentSequence();
  const ra = [];
  const rb = [];
  for (let i = 0; i < 12; i++) {
    ra.push(va.appendToStateJson('messages', { text: `a${i}` }));
    rb.push(vb.appendToStateJsonWithIdentity('messages', { text: `b${i}` }, 'id', 'seq'));
    assert.equal(store.currentBranch().name, 'main', 'a view must not move the cursor');
  }
  assert.deepEqual(texts(va), ['m0', 'm1', 'm2', ...ra.map((_, i) => `a${i}`)]);
  assert.deepEqual(texts(vb), ['m0', 'm1', 'm2', ...rb.map((_, i) => `b${i}`)]);
  assert.deepEqual(texts(store), ['m0', 'm1', 'm2']);
  assert.equal(store.currentSequence(), forkSeq, 'no write reached main');
  assert.equal(va.getStateLen('messages'), 15);
  assert.equal(vb.getStateItemJson('messages', 14).text, 'b11');
  assert.equal(vb.getStateItemJson('messages', 14).id, rb[11].id);
  assert.equal(JSON.parse(va.getStateTail('messages', 2).toString()).length, 2);
  assert.equal(JSON.parse(va.getStateSlice('messages', 3, 4).toString())[0].text, 'a0');
  ok('two views interleave writes; each reads only its own branch; root cursor and main untouched');

  // Sequences are per branch (siblings overlap); record ids are global.
  assert.equal(ra[0].sequence, forkSeq + 1);
  assert.equal(rb[0].sequence, forkSeq + 1);
  assert.notEqual(ra[0].id, rb[0].id);
  assert.ok(va.currentSequence() > forkSeq);
  assert.equal(va.currentSequence(), va.currentBranch().head);
  assert.equal(store.getRecord(ra[0].id).id, ra[0].id, 'getRecord is store-wide');
  assert.equal(vb.getRecord(ra[0].id).id, ra[0].id, 'getRecord is store-wide on a view too');
  // query() on a view returns the records written on its branch.
  const qa = va.query({});
  assert.ok(qa.length >= 12);
  assert.ok(qa.every((r) => r.sequence > forkSeq));
  assert.ok(!qa.some((r) => rb.some((x) => x.id === r.id)));
  const itemCount = (h) => h.listStates().find((s) => s.id === 'messages').itemCount;
  assert.equal(itemCount(va), 15);
  assert.equal(itemCount(store), 3);
  ok('currentSequence/query/listStates on a view use the pinned branch; sibling sequences overlap, ids do not');

  // Auto-snapshots (delta every 5) happened on the views' branches and the
  // values still reconstruct from the log after the caches are bypassed.
  assert.deepEqual(JSON.parse(va.getStateAt('messages', va.currentSequence()).toString()).map((m) => m.text), texts(va));

  // --- snapshot + tree states are isolated too --------------------------
  va.setStateJson('cfg/model', { name: 'a-model' });
  va.treeSet('files', 'a.txt', { blobHash: 'ha', size: 2, mode: 0o644 });
  va.treeRemove('files', 'README.md');
  vb.treeBatch('files', [{ op: 'set', path: 'b.txt', entry: { blobHash: 'hb', size: 3, mode: 0o644 } }]);
  assert.deepEqual(va.getStateJson('cfg/model'), { name: 'a-model' });
  assert.deepEqual(vb.getStateJson('cfg/model'), { name: 'parent-model' });
  assert.deepEqual(store.getStateJson('cfg/model'), { name: 'parent-model' });
  assert.deepEqual(va.treeList('files').map((e) => e.path), ['a.txt']);
  assert.deepEqual(vb.treeList('files').map((e) => e.path), ['README.md', 'b.txt']);
  assert.deepEqual(store.treeList('files').map((e) => e.path), ['README.md']);
  assert.equal(va.treeGet('files', 'README.md'), null);
  // Parent edits/appends after the fork are invisible to the children.
  store.appendToStateJson('messages', { text: 'main-later' });
  store.editStateItem('messages', 0, Buffer.from(JSON.stringify({ text: 'm0-main-edit' })));
  assert.equal(texts(va)[0], 'm0');
  assert.ok(!texts(vb).includes('main-later'));
  // Child edit/redact are invisible to the parent and the sibling.
  va.editStateItem('messages', 1, Buffer.from(JSON.stringify({ text: 'm1-a-edit' })));
  va.redactStateItems('messages', 2, 3);
  assert.equal(texts(va)[1], 'm1-a-edit');
  assert.equal(va.getStateLen('messages'), 14);
  assert.deepEqual(texts(store), ['m0-main-edit', 'm1', 'm2', 'main-later']);
  assert.equal(texts(vb)[1], 'm1');
  ok('snapshot, tree, edit and redact through views are isolated in both directions');

  // --- stateHeadOffset: a revision key shared by every handle on a branch -
  {
    const again = store.view('main');
    const key = store.stateHeadOffset('messages');
    assert.equal(again.stateHeadOffset('messages'), key, 'two handles on one branch agree');
    assert.equal(store.stateHeadOffset('no-such-state'), null);
    store.setStateJson('cfg/model', { name: 'parent-model' }); // another state: no change
    assert.equal(store.stateHeadOffset('messages'), key);
    again.editStateItem('messages', 0, Buffer.from(JSON.stringify({ text: 'm0-again' }))); // count-preserving edit
    assert.notEqual(store.stateHeadOffset('messages'), key, 'an edit through the other handle moves the key');
    assert.notEqual(va.stateHeadOffset('messages'), store.stateHeadOffset('messages'), 'branches that diverged differ');
    // Put m0 back so the later steps see the value they expect.
    again.editStateItem('messages', 0, Buffer.from(JSON.stringify({ text: 'm0-main-edit' })));
    ok('stateHeadOffset is an exact, handle-independent revision key');
  }

  // --- what a view refuses / redirects ----------------------------------
  assert.throws(() => va.switchBranch('main'), /not available on a view/);
  assert.equal(store.currentBranch().name, 'main');
  const grandchild = va.createBranch('a-child');
  assert.equal(grandchild.parentId, a.id, 'createBranch(name) on a view forks the pinned branch');
  assert.deepEqual(texts(store.view('a-child')), texts(va));
  const emptyFromView = va.createEmptyBranch('a-empty');
  assert.equal(emptyFromView.parentId, a.id);
  assert.equal(store.view('a-empty').getStateJson('messages'), null);
  const viaView = va.view('b');
  assert.equal(viaView.pinnedBranch(), 'b', 'a view can open another view');
  assert.deepEqual(texts(viaView), texts(vb));
  ok('switchBranch throws on a view; createBranch/createEmptyBranch without `from` fork the pinned branch');

  // --- selective fork ----------------------------------------------------
  const sel = store.createBranchWithStates('sel', 'main', { stateIds: ['messages', 'nope'], statePrefixes: ['cfg/'] });
  assert.equal(sel.parentId, store.currentBranch().id);
  const vsel = store.view('sel');
  const vempty = store.view(store.createEmptyBranch('empty', 'main').name);
  assert.deepEqual(texts(vsel), texts(store));
  assert.deepEqual(vsel.getStateJson('cfg/model'), { name: 'parent-model' });
  assert.deepEqual(vsel.getStateJson('cfg/tools'), ['a', 'b']);
  for (const id of ['scratch', 'files']) {
    assert.equal(vsel.getState(id), vempty.getState(id), `${id}: not inherited reads as on an empty branch`);
    assert.equal(vsel.getState(id), null);
    assert.equal(vsel.getStateLen(id), vempty.getStateLen(id));
  }
  assert.deepEqual(vsel.treeList('files'), []);
  assert.equal(itemCount(vsel), 4);
  assert.equal(vsel.listStates().find((s) => s.id === 'scratch').itemCount ?? null, null);
  vsel.setStateJson('scratch', { note: 'child scratch' });
  assert.deepEqual(store.getStateJson('scratch'), { note: 'parent scratch' });
  // From a view, `from` omitted = the pinned branch.
  const selFromView = va.createBranchWithStates('a-sel', undefined, { statePrefixes: ['cfg/'] });
  assert.equal(selFromView.parentId, a.id);
  assert.deepEqual(store.view('a-sel').getStateJson('cfg/model'), { name: 'a-model' });
  assert.equal(store.view('a-sel').getStateJson('messages'), null);
  // Empty filter inherits nothing.
  store.createBranchWithStates('bare', 'main', {});
  assert.equal(store.view('bare').getStateJson('messages'), null);
  // Historical selective fork.
  const histSel = store.createBranchAtWithStates('hist-sel', 'main', forkSeq, { stateIds: ['messages'] });
  assert.equal(histSel.branchPoint, forkSeq);
  assert.deepEqual(texts(store.view('hist-sel')), ['m0', 'm1', 'm2']);
  assert.equal(store.view('hist-sel').getStateJson('cfg/model'), null);
  ok('createBranchWithStates / createBranchAtWithStates inherit exactly the selected states');

  // --- createBranchAt keeps an inherited edit across a child delta snapshot
  store.compactState('messages');
  store.editStateItem('messages', 1, Buffer.from(JSON.stringify({ text: 'm1-edited-after-snapshot' })));
  store.appendToStateJson('messages', { text: 'after-edit' });
  const expectedInherited = texts(store);
  assert.equal(expectedInherited[1], 'm1-edited-after-snapshot');
  store.createBranch('control');
  store.createBranchAt('historical', 'main', store.currentSequence());
  for (const name of ['control', 'historical']) {
    const v = store.view(name);
    for (let i = 0; i < 10; i++) v.appendToStateJson('messages', { text: `${name}-${i}` });
    assert.deepEqual(
      texts(v),
      [...expectedInherited, ...Array.from({ length: 10 }, (_, i) => `${name}-${i}`)],
      `${name}: inherited edit must survive the child's delta snapshot`,
    );
    // And cold, bypassing every cache.
    assert.deepEqual(JSON.parse(v.getStateAt('messages', v.currentSequence()).toString()).map((m) => m.text), texts(v));
  }
  ok('createBranchAt child keeps an inherited Edit after crossing the delta-snapshot threshold');

  // --- field indexes are per branch --------------------------------------
  store.registerStateFieldIndex('messages', '/text', 'string');
  assert.deepEqual(store.queryStateIndexEq('messages', '/text', 'm2', {}), [2]);
  const vidx = store.view(store.createBranch('idx-child').name);
  assert.equal(vidx.queryStateIndexEq('messages', '/text', 'm2', {}), null, 'nothing is built for a new branch');
  vidx.appendToStateJson('messages', { text: 'm2' });
  vidx.editStateItem('messages', 0, Buffer.from(JSON.stringify({ text: 'changed' })));
  assert.deepEqual(store.queryStateIndexEq('messages', '/text', 'm2', {}), [2], "parent's index survives child writes");
  assert.deepEqual(store.queryStateIndexEq('messages', '/text', 'm0-main-edit', {}), [0]);
  vidx.registerStateFieldIndex('messages', '/text', 'string');
  assert.deepEqual(vidx.queryStateIndexEq('messages', '/text', 'm2', {}), [2, vidx.getStateLen('messages') - 1]);
  assert.deepEqual(vidx.queryStateIndexEq('messages', '/text', 'changed', {}), [0]);
  assert.deepEqual(store.queryStateIndexEq('messages', '/text', 'changed', {}), []);
  ok('field indexes: parent index survives child writes; a child has none until it registers its own');

  // --- subscriptions catch up on the pinned branch ----------------------
  const sub = va.subscribe({ fromSequence: forkSeq + 1, filter: { includeRecords: true } });
  va.catchUpSubscription(sub);
  let replayed = 0;
  for (let ev = va.pollSubscription(sub); ev; ev = va.pollSubscription(sub)) {
    if (ev.eventType === 'record') {
      replayed++;
      assert.equal(JSON.parse(ev.data).record.branch, Number(a.id));
    }
  }
  assert.ok(replayed >= 12);
  va.unsubscribe(sub);
  ok('catchUpSubscription on a view replays the pinned branch');

  // --- a view whose branch is deleted fails cleanly ----------------------
  assert.throws(() => vb.deleteBranch('b'), /Cannot delete the branch this view is pinned to/);
  store.deleteBranch('b');
  for (const call of [
    () => vb.getStateJson('messages'),
    () => vb.appendToStateJson('messages', { text: 'x' }),
    () => vb.currentBranch(),
    () => vb.currentSequence(),
    () => vb.treeList('files'),
    () => vb.listStates(),
    () => vb.query({}),
    () => vb.createBranch('from-deleted'),
    () => viaView.getStateLen('messages'),
  ]) {
    assert.throws(call, /Branch not found/);
  }
  assert.equal(vb.pinnedBranch(), 'b');
  assert.ok(vb.listBranches().length > 0, 'store-wide methods still work on that view');
  // A new branch with the same name is a different branch.
  store.createBranch('b');
  assert.throws(() => vb.getStateJson('messages'), /Branch not found/);
  assert.deepEqual(texts(store.view('b')), texts(store));
  ok('a view on a deleted branch throws "Branch not found" (and stays dead if the name is reused)');

  // --- close(): view detaches, root closes everything and frees the lock -
  const disposable = store.view('a');
  assert.equal(disposable.isClosed(), false);
  disposable.close();
  assert.equal(disposable.isClosed(), true);
  assert.throws(() => disposable.getStateJson('messages'), /View has been closed/);
  disposable.close(); // idempotent
  assert.equal(store.isClosed(), false);
  assert.equal(va.isClosed(), false);
  assert.equal(texts(va).length, 14);
  ok('close() on a view detaches only that view');

  const expectA = texts(va);
  const expectSel = texts(vsel);
  assert.throws(() => JsStore.open({ path: storePath }), /lock/i, 'the open store holds the lock');
  store.close();
  assert.equal(store.isClosed(), true);
  assert.equal(va.isClosed(), true, 'views report closed once the root has closed');
  assert.throws(() => va.getStateJson('messages'), /Store has been closed/);
  assert.throws(() => vsel.appendToStateJson('messages', { text: 'x' }), /Store has been closed/);
  assert.throws(() => va.view('a'), /Store has been closed/);

  // The lock is free although `va`, `vsel`, ... are still referenced here.
  const reopened = JsStore.open({ path: storePath });
  assert.equal(reopened.currentBranch().name, 'main', 'views never moved the persisted cursor');
  assert.deepEqual(texts(reopened.view('a')), expectA);
  assert.deepEqual(texts(reopened.view('sel')), expectSel);
  assert.deepEqual(reopened.view('sel').getStateJson('scratch'), { note: 'child scratch' });
  assert.deepEqual(reopened.view('a').treeList('files').map((e) => e.path), ['a.txt']);
  assert.equal(texts(reopened.view('historical'))[1], 'm1-edited-after-snapshot');
  assert.throws(() => va.getStateJson('messages'), /Store has been closed/, 'old views do not attach to the new handle');
  reopened.close();
  ok('root close() closes every view and releases the lock with views still referenced; view-written branches persist');

  console.log('\n✅ All view tests passed!');
} finally {
  fs.rmSync(dir, { recursive: true, force: true });
}
