import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { JsStore } from '../index.js';

function freshStore(t) {
  const path = mkdtempSync(join(tmpdir(), 'chronicle-cadence-'));
  let store;
  t.after(() => {
    try {
      if (store && !store.isClosed()) store.close();
    } finally {
      rmSync(path, { recursive: true, force: true });
    }
  });
  store = JsStore.openOrCreate({ path });
  return { store, reopen() { store = JsStore.open({ path }); return store; } };
}

const appendLog = (id, deltaSnapshotEvery, fullSnapshotEvery) =>
  ({ id, strategy: 'append_log', deltaSnapshotEvery, fullSnapshotEvery });

// Each store below has just one writer. The ordered public update IDs let
// us see snapshots interleaved with the writes, without parsing raw records.
const mark = (store) => store.getRecordIdsByType('state_update').length;
function operations(store, state, since = 0) {
  const found = [];
  for (const id of store.getRecordIdsByType('state_update').slice(since)) {
    const { state_id, operation } = JSON.parse(store.getStateUpdateJson(id));
    if (state_id === state) found.push(operation);
  }
  return found;
}
function operationKinds(store, state, since = 0) {
  return operations(store, state, since).map((op) => Object.keys(op)[0]);
}

// Use the single stamped append that MessageStore.addMessage uses. Keep an
// independent model of the rows rather than copying a possibly short read.
function addMessages(store, id, expected, count) {
  for (let i = 0; i < count; i++) {
    const message = { participant: 'User', content: [{ type: 'text', text: `${id} row ${expected.length}` }] };
    const record = store.appendToStateJsonWithIdentity(id, message, 'id', 'sequence');
    expected.push({ ...message, id: record.id, sequence: record.sequence });
  }
}

test('Toggling automatic snapshots acts immediately without losing disabled-period messages', (t) => {
  // Independent exits from setup: the append proving automatic scheduling
  // is back on must not prepare a snapshot for either of the other exits.
  for (const exit of ['append', 'compact', 'close']) {
    const { store, reopen } = freshStore(t);
    const id = exit === 'close' ? 'bench/messages' : 'messages';
    store.registerState(appendLog(id, 1, 100)); // the very next write is eligible
    const messages = [];
    const start = mark(store);
    store.setAutoSnapshot(false);
    addMessages(store, id, messages, 6);
    // pins setAutoSnapshot.1: from the first eligible append through the loop, no snapshots enter the log.
    assert.deepEqual(operationKinds(store, id, start), Array(6).fill('Append'), exit);

    const endOfSetup = mark(store);
    store.setAutoSnapshot(true);
    if (exit === 'append') {
      addMessages(store, id, messages, 1);
      // pins setAutoSnapshot.2: the next eligible write itself takes an automatic snapshot in the same turn.
      assert.deepEqual(operationKinds(store, id, endOfSetup).slice(-2), ['Append', 'DeltaSnapshot']);
    } else if (exit === 'compact') {
      // Sample just before the call, so that nothing re-enabling wrote is credited to compaction.
      const beforeCompaction = mark(store);
      const headBefore = store.stateHeadOffset(id);
      const compacted = store.compactState(id);
      const headAfter = store.stateHeadOffset(id);
      const bytes = operations(store, id, beforeCompaction).at(-1)?.Snapshot;
      // pins compactState.4: without another append, compaction writes a full snapshot of the accumulated rows.
      assert.deepEqual(bytes && JSON.parse(Buffer.from(bytes).toString('utf8')), messages);
      // pins compactState.4: the snapshot became the slot's head, not merely a detached record: the call wrote
      // that one update and moved the head, and the next write on the slot follows the moved head (below).
      assert.deepEqual({
        written: store.getRecordIdsByType('state_update').slice(beforeCompaction),
        moved: headAfter !== headBefore,
        opsSinceSnapshot: store.listStates().find((state) => state.id === id)?.opsSinceSnapshot,
      }, { written: [compacted?.id], moved: true, opsSinceSnapshot: 0 });
      store.createBranch('child', 'main');
      // pins setAutoSnapshot.3: a child forked at the compacted head inherits every disabled-period row.
      assert.deepEqual(store.view('child').getStateJson(id), messages);
      const next = store.appendToStateJson(id, { participant: 'User', content: [] });
      // pins compactState.4: the next update of the slot links back to the head that compaction installed.
      assert.equal(JSON.parse(store.getStateUpdateJson(next.id)).prev_update_offset, headAfter);
    } else {
      store.close();
      // pins setAutoSnapshot.3: a close straight after re-enabling leaves all the rows readable in order on reopen.
      assert.deepEqual(reopen().getStateJson(id), messages);
    }
  }
});

test('Reapplying a retune on reopen changes scheduling without replacing the named log', (t) => {
  const { store, reopen } = freshStore(t);
  const shared = 'messages', isolated = 'agents/tern/messages';
  const sharedRows = [], isolatedRows = [];
  store.registerState(appendLog(shared, 3, 1));
  store.registerState(appendLog(isolated, 4, 1));
  addMessages(store, shared, sharedRows, 1);
  addMessages(store, isolated, isolatedRows, 4); // start the retune at a snapshot boundary

  const reg = appendLog(isolated, 2, 10);
  const upsert = store.updateStateStrategy;
  const before = mark(store);
  // pins updateStateStrategy.1: the manager's one-object call with the store as receiver accepts an occupied slot.
  assert.doesNotThrow(() => upsert.call(store, reg));
  addMessages(store, isolated, isolatedRows, 2);
  // pins updateStateStrategy.5: without awaiting or yielding, the second write takes the newly scheduled delta.
  assert.deepEqual(operationKinds(store, isolated, before), ['Append', 'Append', 'DeltaSnapshot']);
  // pins updateStateStrategy.4: the occupied slot is still an append log with its pre- and post-retune messages.
  assert.deepEqual({
    strategy: store.listStates().find((s) => s.id === isolated)?.strategy,
    rows: store.getStateJson(isolated),
  }, { strategy: 'append_log', rows: isolatedRows });

  addMessages(store, shared, sharedRows, 2);
  // pins updateStateStrategy.2: retuning the isolated ID left the shared slot on its own full-at-three cadence.
  assert.deepEqual(operationKinds(store, shared, before), ['Append', 'Append', 'Snapshot']);
  store.close();
  const next = reopen();
  const opened = mark(next);
  addMessages(next, isolated, isolatedRows, 2);
  // pins updateStateStrategy.2: the retuned interval persisted, even before the later boot reapplies it.
  assert.deepEqual(operationKinds(next, isolated, opened), ['Append', 'Append', 'DeltaSnapshot']);

  const beforeRepeat = mark(next);
  next.updateStateStrategy(reg);
  addMessages(next, isolated, isolatedRows, 2);
  // pins updateStateStrategy.2: identical settings on reopen continue to schedule writes on the same slot.
  assert.deepEqual(operationKinds(next, isolated, beforeRepeat), ['Append', 'Append', 'DeltaSnapshot']);
  addMessages(next, shared, sharedRows, 3);
  // pins updateStateStrategy.2: the repeated retune still leaves the other ID's registration alone.
  assert.deepEqual(operationKinds(next, shared, beforeRepeat), ['Append', 'Append', 'Append', 'Snapshot']);
  const slots = next.listStates().filter((s) => s.id === shared || s.id === isolated);
  // pins updateStateStrategy.2: each named slot still exists once as an append log, each history once and in order.
  assert.deepEqual({
    ids: slots.map((s) => [s.id, s.strategy]).sort(),
    shared: next.getStateJson(shared),
    isolated: next.getStateJson(isolated),
  }, {
    ids: [[isolated, 'append_log'], [shared, 'append_log']].sort(),
    shared: sharedRows,
    isolated: isolatedRows,
  });
});

// Number each snapshot by the count of writes preceding it on this state.
// An early full can't hide among deltas just because a later boundary also fires.
function appendSchedule(kinds) {
  let writes = 0;
  const boundaries = [];
  for (const kind of kinds) {
    if (kind === 'Append') writes++;
    else boundaries.push([writes, kind]);
  }
  return { writes, boundaries };
}

test('The first full cadence is measured in delta intervals', (t) => {
  const { store } = freshStore(t);
  const journal = 'mcpl/coalescing-journal';
  store.registerState(appendLog(journal, Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER));
  for (let i = 0; i < 120; i++) store.appendToStateJson(journal, { seq: i });
  // pins registerState.14: both very long JS intervals are accepted and the journal sees only ordinary writes.
  assert.deepEqual(appendSchedule(operationKinds(store, journal)), { writes: 120, boundaries: [] });

  // With both fields huge the first delta is never due; a binding that
  // silently defaulted just the full field would pass the check above.
  const longFull = 'short-delta-control';
  store.registerState(appendLog(longFull, 2, Number.MAX_SAFE_INTEGER));
  for (let i = 0; i < 24; i++) store.appendToStateJson(longFull, i);
  // pins registerState.14: even when deltas fire, MAX_SAFE_INTEGER in the full field stays a long interval.
  assert.deepEqual(appendSchedule(operationKinds(store, longFull)), {
    writes: 24, boundaries: Array.from({ length: 12 }, (_, i) => [2 * (i + 1), 'DeltaSnapshot']),
  });

  const id = 'messages';
  store.registerState(appendLog(id, 5, 2));
  store.updateStateStrategy(appendLog(id, 50, 100));
  const start = mark(store);
  for (let i = 0; i < 5000; i++) store.appendToStateJsonWithIdentity(id, { n: i }, 'id', 'sequence');
  const deltas = Array.from({ length: 99 }, (_, i) => [50 * (i + 1), 'DeltaSnapshot']);
  // pins updateStateStrategy.3: delta at 50 and 100, no full through 4,999, first full at 50 × 100 appends.
  assert.deepEqual(appendSchedule(operationKinds(store, id, start)), {
    writes: 5000, boundaries: [...deltas, [5000, 'Snapshot']],
  });
});

test('After the first full snapshot, a growing log spaces the next by its length at the last', (t) => {
  const { store } = freshStore(t);
  const id = 'messages';
  // The caller's 50 and 100 scaled down to 5 and 2, set as the caller sets them,
  // by retuning a registered slot: 10 appends where the caller's make 5,000.
  store.registerState(appendLog(id, 3, 7));
  store.updateStateStrategy(appendLog(id, 5, 2));
  for (let i = 0; i < 80; i++) store.appendToStateJsonWithIdentity(id, { n: i }, 'id', 'sequence');
  const fulls = [10, 20, 40, 80];
  // The store takes the first full snapshot after 10 appends, and each later one when the appends since the last
  // full reach the slot's length at it, so at 20, 40 and 80. Deltas come every 5 appends between.
  assert.deepEqual(appendSchedule(operationKinds(store, id)), {
    writes: 80,
    boundaries: Array.from({ length: 16 }, (_, i) => 5 * (i + 1))
      .map((at) => [at, fulls.includes(at) ? 'Snapshot' : 'DeltaSnapshot']),
  });
});

test('Explicit tree cadence fields drive keyed snapshot boundaries', (t) => {
  const { store } = freshStore(t);
  const id = 'workspace';
  store.registerState({ id, strategy: 'tree', deltaSnapshotEvery: 3, fullSnapshotEvery: 2 });
  const files = Array.from({ length: 6 }, (_, i) => ({
    path: `src/file-${i}`, entry: { blobHash: `hash-${i}`, size: i + 10, mode: 0o644 },
  }));
  for (const { path, entry } of files) store.treeSet(id, path, entry);
  // pins registerState.13: the supplied tree intervals take a delta after three sets and a full after six.
  assert.deepEqual(operationKinds(store, id), [
    'TreeSet', 'TreeSet', 'TreeSet', 'TreeDeltaSnapshot', 'TreeSet', 'TreeSet', 'TreeSet', 'Snapshot',
  ]);
  // pins registerState.13: each keyed write is readable through the configured tree after both boundaries.
  assert.deepEqual(files.map(({ path }) => store.treeGet(id, path)), files.map(({ entry }) => entry));
  // pins registerState.13: listing the tree still yields exactly the intended paths and entries.
  assert.deepEqual(store.treeList(id).sort((a, b) => a.path.localeCompare(b.path)),
    files.map(({ path, entry }) => ({ path, ...entry })));
});
