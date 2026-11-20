import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { JsStore } from '../index.js';

const FILES = 'workspace/mount/tree';
const INTENTS = 'workspace/mount/intent';
const ENVELOPES = 'mint-preimage-envelopes';
const CHECKPOINTS = 'checkpoints';

function fresh(t, ...trees) {
  const parent = mkdtempSync(join(tmpdir(), 'chronicle-tree-'));
  const f = { dir: join(parent, 'store'), mount: join(parent, 'mount') };
  t.after(() => {
    try { f.store?.close(); } finally { rmSync(parent, { recursive: true, force: true }); }
  });
  f.store = JsStore.openOrCreate({ path: f.dir });
  for (const id of trees) f.store.registerState({ id, strategy: 'tree' });
  return f;
}

function diskFile(mount, path, bytes) {
  const name = join(mount, path);
  mkdirSync(dirname(name), { recursive: true });
  writeFileSync(name, bytes);
  return name;
}

function digest(bytes) { return createHash('sha256').update(bytes).digest('hex'); }

function stage(store, value, mode, type = 'application/octet-stream') {
  const bytes = Buffer.from(value);
  return { blobHash: store.storeBlob(bytes, type), size: bytes.byteLength, mode };
}

function intent(store, json) { return stage(store, JSON.stringify(json), 0, 'application/json'); }

// Project only the fields used by callers; new fields needn't break readback.
function fields(e) {
  if (e === null) return null;
  return { blobHash: e.blobHash, size: e.size, mode: e.mode };
}

function point(store, tree, path) { return fields(store.treeGet(tree, path)); }
function row(path, entry) { return { path, ...fields(entry) }; }
function rows(list) { return list.map(e => row(e.path, e)); }

// For unordered inventory comparisons. Keep duplicates; a Map could hide one.
function byPath(list) {
  return list.toSorted((a, b) => a.path < b.path ? -1 : a.path > b.path ? 1 : 0);
}

test('Two exact-key trees preserve byte counts, modes and independent content references', t => {
  const { store, mount } = fresh(t, FILES, INTENTS, CHECKPOINTS);
  const path = 'src/note.md';
  const originalBytes = Buffer.from('初版 🐦\n');
  const name = diskFile(mount, path, originalBytes);
  const original = stage(store, readFileSync(name), 0o644);
  const oldIntent = { origin: 'store' };
  const liveIntent = intent(store, oldIntent);
  const damaged = { blobHash: digest('never staged'), size: 190, mode: 0 };
  const placeholder = stage(store, 'legacy', 0o644);
  store.treeSet(FILES, path, original);
  store.treeSet(FILES, 'legacy.txt', placeholder);
  store.treeSet(INTENTS, path, liveIntent);
  store.treeSet(INTENTS, 'damaged', damaged);
  writeFileSync(name, 'a different disk version');

  const requestKey = digest('authoring request');
  // pins treeGet.13 — an index never registered or written has an exact null miss.
  assert.equal(store.treeGet(ENVELOPES, requestKey), null);
  store.registerState({ id: ENVELOPES, strategy: 'tree' });
  const envelopeBytes = Buffer.from('{"version":1,"segments":[]}');
  const envelope = stage(store, envelopeBytes, 0, 'application/json');
  store.treeSet(FILES, requestKey, original);
  store.treeSet(ENVELOPES, requestKey, envelope);
  store.treeSet(FILES, requestKey, placeholder);
  // pins treeSet.1 — replace just the opaque request key in its tree, not its namesake or neighbors.
  assert.deepEqual([point(store, ENVELOPES, requestKey), point(store, FILES, requestKey), point(store, FILES, path)],
    [envelope, placeholder, original]);

  const hit = store.treeGet(FILES, path);
  // pins treeGet.1 — inspect a present entry directly in the calling turn.
  assert.equal(hit.blobHash, original.blobHash);
  // pins treeGet.2 — exact key and tree, not a normalized spelling, prefix or key from another tree.
  assert.deepEqual([point(store, FILES, path), point(store, INTENTS, path), store.treeGet(FILES, 'src'),
    store.treeGet(FILES, 'src/Note.md'), store.treeGet(FILES, 'src//note.md'), store.treeGet(FILES, 'damaged')],
    [original, liveIntent, null, null, null, null]);
  // pins treeGet.3 — membership is truthy even without backing bytes; only an absent key is null.
  assert.deepEqual([Boolean(hit), Boolean(store.treeGet(INTENTS, 'damaged')), store.treeGet(INTENTS, 'absent')],
    [true, true, null]);
  const indexed = store.treeGet(ENVELOPES, requestKey);
  // pins treeGet.7 — read envelope bytes with the recorded reference, not the lookup digest.
  assert.deepEqual([indexed.blobHash, store.getBlob(indexed.blobHash)], [envelope.blobHash, envelopeBytes]);
  // pins treeGet.8 — recorded byte size, not text length, current disk size or a fresh blob measurement.
  assert.deepEqual([hit.size, store.treeGet(INTENTS, 'damaged').size], [originalBytes.byteLength, damaged.size]);
  // pins treeGet.9 — same full hex SHA-256 as the caller computed from the file bytes.
  assert.equal(hit.blobHash, digest(originalBytes));
  const evidence = { path, ...hit };
  // pins treeGet.14 — spread copies the hash and numeric mode used in evidence.
  assert.deepEqual([evidence.blobHash, evidence.mode], [original.blobHash, original.mode]);

  const newBytes = Buffer.from('révisé 📝\n');
  const revised = stage(store, newBytes, 0o100600);
  store.treeSet(FILES, path, revised);
  const overwritten = store.treeGet(FILES, path);
  // pins treeGet.10 — preserve permission-only and typed regular-file modes across an overwrite.
  assert.deepEqual([hit.mode, overwritten.mode], [0o644, 0o100600]);
  // pins treeSet.3 — store supplied byte counts and modes, including zero and file type bits.
  assert.deepEqual([point(store, FILES, path), point(store, FILES, 'legacy.txt'), point(store, INTENTS, path)],
    [revised, placeholder, liveIntent]);
  const chmod = { blobHash: overwritten.blobHash, size: overwritten.size, mode: 0o100755 };
  store.treeSet(FILES, path, chmod);
  // pins treeSet.11 — mode-only set reuses the old hash and size without another blob write.
  assert.deepEqual(point(store, FILES, path), { ...revised, mode: 0o100755 });

  const files = store.treeList(FILES);
  const intents = store.treeList(INTENTS);
  // pins treeList.1 — full independent inventories even at the shared path that was replaced.
  assert.deepEqual([byPath(rows(files)), byPath(rows(intents))],
    [byPath([row(path, chmod), row('legacy.txt', placeholder), row(requestKey, placeholder)]),
      byPath([row(path, liveIntent), row('damaged', damaged)])]);
  const live = intents.find(e => e.path === path);
  const lost = intents.find(e => e.path === 'damaged');
  const readable = hash => {
    try { return store.getBlob(hash) != null; } catch { return false; }
  };
  // pins treeList.12 — exact usable intent reference; an unreadable reference stays listed.
  assert.deepEqual([live.blobHash, JSON.parse(store.getBlob(live.blobHash).toString()), lost.blobHash, readable(lost.blobHash)],
    [liveIntent.blobHash, oldIntent, damaged.blobHash, false]);
  // pins treeList.13 — replacement lists the SHA-256 of the new workspace bytes.
  assert.equal(files.find(e => e.path === path).blobHash, digest(newBytes));
  // pins treeList.14 — listing distinguishes typed modes from permission-only placeholders.
  assert.deepEqual([files.find(e => e.path === path).mode, files.find(e => e.path === 'legacy.txt').mode], [0o100755, 0o644]);

  const emptyList = Buffer.from('[]');
  store.treeSet(CHECKPOINTS, 'agent', stage(store, emptyList, 0o644, 'application/json'));
  // pins treeSet.15 — a real [] checkpoint is a present, two-byte entry.
  assert.deepEqual(point(store, CHECKPOINTS, 'agent'), { blobHash: digest(emptyList), size: 2, mode: 0o644 });
});

test('A saved tree hit can restore an undecodable intent without touching its blob', t => {
  const { store } = fresh(t, INTENTS);
  const path = 'draft';
  const broken = stage(store, '{not valid JSON', 0, 'application/json');
  const replacement = intent(store, { origin: 'store' });
  store.treeSet(INTENTS, path, broken);
  const saved = store.treeGet(INTENTS, path);
  store.treeSet(INTENTS, path, replacement);
  assert.deepEqual(point(store, INTENTS, path), replacement);
  store.treeRemove(INTENTS, path);
  // pins treeGet.15 — a retained hit stays the original metadata after replacement and removal.
  assert.deepEqual(fields(saved), broken);
  store.treeSet(INTENTS, path, saved);
  // pins treeSet.12 — passing the saved JS object verbatim restores every field without decoding the intent.
  assert.deepEqual(point(store, INTENTS, path), broken);
  store.treeRemove(INTENTS, path);
  // pins treeRemove.10 — clearing the invalid-JSON intent didn't need its blob to decode.
  assert.equal(store.treeGet(INTENTS, path), null);

  const before = store.treeGet(INTENTS, 'new');
  store.treeSet(INTENTS, 'new', replacement);
  assert.deepEqual(point(store, INTENTS, 'new'), replacement);
  if (before === null) store.treeRemove(INTENTS, 'new');
  else store.treeSet(INTENTS, 'new', before);
  // pins treeGet.15 — the saved-null case restores absence, not an empty placeholder.
  assert.deepEqual([before, store.treeGet(INTENTS, 'new')], [null, null]);
});

test('Listing uses exact literal prefixes and ordered original path spellings', t => {
  const { store } = fresh(t, FILES);
  const base = stage(store, 'first version', 0o644);
  const draft = 'drafts/sub/note';
  // Each pair of case variants goes in against path order.
  for (const path of ['src2/f', 'src/sub/f', 'src', 'src/f', 'a//b.txt', 'a//B.txt', 'a/plain', 'gone', draft,
    'docs/readme.md', 'docs/README.md']) {
    store.treeSet(FILES, path, base);
  }
  store.treeSet(FILES, draft, stage(store, 'second', 0o100644));
  store.treeSnapshot(FILES);
  const latest = stage(store, 'final draft', 0o100755);
  store.treeSet(FILES, draft, latest);
  store.treeRemove(FILES, 'gone');

  const all = store.treeList(FILES);
  const paths = list => list.map(e => e.path);
  const names = ['a//B.txt', 'a//b.txt', 'a/plain', 'docs/README.md', 'docs/readme.md', draft, 'src', 'src/f', 'src/sub/f', 'src2/f'];
  const expected = names.map(path => row(path, path === draft ? latest : base));
  // pins treeList.3 — exactly the live off-disk keys, including just the current draft, no removed key or implied directories.
  assert.deepEqual([rows(all), fields(store.treeGet(FILES, draft))], [expected, latest]);
  // pins treeList.4 — a synchronous Array on hit and an empty Array for a prefix with no matches.
  assert.deepEqual([Array.isArray(all), store.treeList(FILES, 'absent/')], [true, []]);
  const spellings = paths(store.treeList(FILES, 'a//'));
  // pins treeList.7 — full original keys, prefix attached, double slash and case preserved.
  assert.deepEqual([...spellings].sort(), ['a//B.txt', 'a//b.txt']);
  // pins treeList.8 — literal src/ includes deep descendants, not src itself or src2/.
  assert.deepEqual(paths(store.treeList(FILES, 'src/')), ['src/f', 'src/sub/f']);
  // pins treeList.9 — omitted, undefined and '' each enumerate the complete multi-depth tree.
  assert.deepEqual([rows(all), rows(store.treeList(FILES, undefined)), rows(store.treeList(FILES, ''))], [expected, expected, expected]);
  // pins treeList.10 — a repeatedly overwritten path straddling a snapshot occurs only once.
  assert.equal(all.filter(e => e.path === draft).length, 1);
  // pins treeList.11 — the first case-folded match is determined by path order, not insertion order.
  assert.deepEqual(paths(all).filter(path => path.toLowerCase() === 'a//b.txt'), ['a//B.txt', 'a//b.txt']);
  // pins treeList.11 — under a directory's prefix, as the spelling step lists it, valid variants come in path order too.
  assert.deepEqual(paths(store.treeList(FILES, 'docs/')), ['docs/README.md', 'docs/readme.md']);
});

test('File and intent removals rewind together and respect separate write order', t => {
  const { store, mount } = fresh(t, FILES, INTENTS);
  store.setAutoSnapshot(false);
  const drop = 'src/drop', guard = 'src/guard', keep = 'top/keep';
  const bytes = Buffer.from('mounted original\n');
  const disk = diskFile(mount, drop, bytes);
  const file = stage(store, readFileSync(disk), 0o100644);
  const guarded = stage(store, 'guarded file', 0o100600);
  const kept = stage(store, 'untouched', 0o644);
  const conflict = { conflict: { kind: 'both-changed', at: 12, disk: null } };
  const prior = intent(store, conflict);
  const origin = intent(store, { origin: 'store' });
  const tomb = intent(store, { ...conflict, tombstone: { hash: file.blobHash } });
  for (const [tree, path, entry] of [[FILES, drop, file], [FILES, guard, guarded], [FILES, keep, kept],
    [INTENTS, drop, prior], [INTENTS, guard, origin], [INTENTS, keep, origin]]) store.treeSet(tree, path, entry);

  const before = store.currentSequence();
  const tombFirst = store.treeSet(INTENTS, drop, tomb).sequence;
  // Resolve the relative key from the real mount while removing it: even an
  // accidental unlink of the raw key would reach the very file we're checking.
  const cwd = process.cwd();
  let dropRemoved;
  try {
    process.chdir(mount);
    dropRemoved = store.treeRemove(FILES, drop).sequence;
  } finally { process.chdir(cwd); }
  // pins treeRemove.7 — the tracked mount-relative path is gone only from the store, not from disk.
  assert.deepEqual([store.treeGet(FILES, drop), readFileSync(disk)], [null, bytes]);
  // pins treeRemove.1 — removing a file leaves its same-key intent and the other files untouched.
  assert.deepEqual([store.treeGet(FILES, drop), point(store, INTENTS, drop), point(store, FILES, guard), point(store, FILES, keep)],
    [null, tomb, guarded, kept]);
  // pins treeRemove.3 — whole-tree and matching-prefix listings lose the removed key but retain surviving matches.
  assert.deepEqual([rows(store.treeList(FILES)), rows(store.treeList(FILES, 'src/'))],
    [[row(guard, guarded), row(keep, kept)], [row(guard, guarded)]]);

  const guardRemoved = store.treeRemove(FILES, guard).sequence;
  store.treeRemove(INTENTS, guard);
  store.treeRemove(INTENTS, keep);
  // pins treeRemove.1 — the inverse operation leaves the same-key workspace file and other intents alone.
  assert.deepEqual([point(store, FILES, keep), store.treeGet(INTENTS, keep), store.treeGet(INTENTS, guard), point(store, INTENTS, drop)],
    [kept, null, null, tomb]);
  // pins treeRemove.3 — the cleared intent disappears from root and prefix scans, leaving the tombstone.
  assert.deepEqual([rows(store.treeList(INTENTS)), rows(store.treeList(INTENTS, 'src/'))], [[row(drop, tomb)], [row(drop, tomb)]]);
  const pair = path => [point(store, FILES, path), point(store, INTENTS, path)];

  // Select on the unpinned handle. Probe the same three keys on both trees at each cut.
  function select(name, sequence, fileEntries, intentEntries) {
    const branch = store.createBranchAt(name, 'main', sequence);
    store.switchBranch(name);
    const keys = [drop, guard, keep];
    const present = entries => entries.flatMap((entry, i) => entry === null ? [] : [row(keys[i], entry)]);
    // pins treeList.2 — the selected branch ID labels exactly the two inventories at this revision.
    assert.deepEqual([store.currentBranch().id, rows(store.treeList(FILES)), rows(store.treeList(INTENTS))],
      [branch.id, present(fileEntries), present(intentEntries)], name);
    // pins treeGet.4 — point hits and exact nulls on the switched handle agree with those inventories and branch ID.
    assert.deepEqual([store.currentBranch().id, keys.map(k => point(store, FILES, k)), keys.map(k => point(store, INTENTS, k))],
      [branch.id, fileEntries, intentEntries], name);
  }
  select('before-delete', before, [file, guarded, kept], [prior, origin, origin]);
  // pins treeRemove.4 — selecting the pre-removal sequence brings back both file and intent as they stood.
  assert.deepEqual([pair(drop), pair(guard)], [[file, prior], [guarded, origin]]);
  select('tombstone-written', tombFirst, [file, guarded, kept], [tomb, origin, origin]);
  // pins treeRemove.9 — the tombstone is already in history while the file is still present.
  assert.deepEqual(pair(drop), [file, tomb]);
  select('file-dropped', dropRemoved, [null, guarded, kept], [tomb, origin, origin]);
  // pins treeRemove.9 — the first revision without this file is guarded by that tombstone.
  assert.deepEqual(pair(drop), [null, tomb]);
  select('guard-not-cleared', guardRemoved, [null, null, kept], [tomb, origin, origin]);
  // pins treeRemove.9 — for a guard-clearing deletion, removal lands before the origin is cleared.
  assert.deepEqual(pair(guard), [null, origin]);
});

test('Staging is not publication and one mixed batch updates only its commit branch', t => {
  const { store } = fresh(t, FILES);
  store.setAutoSnapshot(false);
  const original = stage(store, 'old', 0o644);
  for (const path of ['keep', 'replace', 'delete']) store.treeSet(FILES, path, original);
  const old = [row('delete', original), row('keep', original), row('replace', original)];
  const main = store.currentBranch();
  const commit = store.createBranch('commit');
  const sibling = store.createBranch('sibling');
  store.switchBranch(commit.name);
  const addBytes = Buffer.from([0, 200, 1]);
  const editBytes = Buffer.from('edited 🌳');
  const added = stage(store, addBytes, 0o644);
  const edited = stage(store, editBytes, 0o100750);
  stage(store, 'abandoned before publishing', 0o644);
  // pins storeBlob.8 — staging (including an abandoned edit) publishes no path or replacement.
  assert.deepEqual([rows(store.treeList(FILES)), point(store, FILES, 'replace'), store.treeGet(FILES, 'new/file')],
    [old, original, null]);

  const before = store.currentSequence();
  store.treeBatch(FILES, [
    { op: 'set', path: 'replace', entry: edited },
    { op: 'remove', path: 'delete' },
    { op: 'set', path: 'keep', entry: original }, // already equal
    { op: 'set', path: 'new/file', entry: added },
  ]);
  const updates = store.query({ types: ['state_update'], fromSequence: before + 1 });
  // pins treeBatch.3 — one tree update record for the entire mixed batch with automatic snapshots off.
  assert.deepEqual(updates.map(r => JSON.parse(store.getStateUpdateJson(r.id)).state_id), [FILES]);
  const committed = [row('keep', original), row('new/file', added), row('replace', edited)];
  // pins treeBatch.1 — all requested changes including the already-equal set and removal accepted together.
  assert.deepEqual(rows(store.treeList(FILES)), committed);
  const newHit = store.treeGet(FILES, 'new/file'), editHit = store.treeGet(FILES, 'replace');
  // pins treeBatch.2 — exact supplied hash, byte size and mode, and strict null for the removal.
  assert.deepEqual([fields(newHit), fields(editHit), store.treeGet(FILES, 'delete')], [added, edited, null]);
  // pins storeBlob.8 — the staged identifiers actually published in the tree resolve to the staged bytes.
  assert.deepEqual([store.getBlob(newHit.blobHash), store.getBlob(editHit.blobHash)], [addBytes, editBytes]);

  for (const [branch, expected] of [[sibling, old], [main, old], [commit, committed]]) {
    store.switchBranch(branch.name);
    // pins treeBatch.5 — later switches don't transfer the commit to the active sibling or parent.
    assert.deepEqual([store.currentBranch().id, rows(store.treeList(FILES))], [branch.id, expected]);
  }
});

test('A pre-record write failure leaves every valid batch target unchanged',
  { skip: process.platform === 'win32' && 'uses a POSIX file-size limit in a child process' }, t => {
    const f = fresh(t, FILES);
    const { store, dir } = f;
    const first = stage(store, 'old first', 0o644), second = stage(store, 'old second', 0o644);
    const newFile = stage(store, 'new', 0o100755);
    store.treeSet(FILES, 'edit', first);
    store.treeSet(FILES, 'delete', second);
    const seq = store.currentSequence();
    store.close();

    // Deny regular-file writes only to the process attempting the otherwise valid batch.
    // This needs neither knowledge of the backing files nor permissions tricks that root bypasses.
    const ops = [{ op: 'set', path: 'edit', entry: newFile }, { op: 'remove', path: 'delete' },
      { op: 'set', path: 'new', entry: newFile }];
    const probe = `
      const [moduleUrl, dir, tree, serialized] = process.argv.slice(1);
      const { JsStore } = await import(moduleUrl);
      process.on('SIGXFSZ', () => {});
      const store = JsStore.open({ path: dir });
      store.setAutoSnapshot(false);
      const ops = JSON.parse(serialized);
      let threw = false;
      try { store.treeBatch(tree, ops); } catch { threw = true; }
      const observation = { threw, seq: store.currentSequence(), hits: ops.map(op => store.treeGet(tree, op.path)) };
      // Close before Node tears down the signal handler. The limit still applies
      // to any checkpoint close wants to write; it is not the operation under test.
      try { store.close(); } catch {}
      process.stdout.write(JSON.stringify(observation));
    `;
    const child = spawnSync('sh', ['-c', 'trap "" XFSZ && ulimit -f 0 && exec "$@"', 'sh', process.execPath,
      '--input-type=module', '-e', probe, new URL('../index.js', import.meta.url).href, dir, FILES, JSON.stringify(ops)],
    { encoding: 'utf8', timeout: 30000 });
    assert.ifError(child.error);
    assert.equal(child.status, 0, child.stderr || `child signal: ${child.signal}`);
    const result = JSON.parse(child.stdout);
    // pins treeBatch.3 — a valid batch refused before its record lands leaves every target at the old revision.
    assert.deepEqual([result.threw, result.seq, result.hits.map(fields)], [true, seq, [first, second, null]]);
    f.store = JsStore.open({ path: dir });
    const hits = () => ops.map(op => point(f.store, FILES, op.path));
    // pins treeBatch.3 — no prefix of the refused batch is visible after reopening either.
    assert.deepEqual(hits(), [first, second, null]);
    f.store.treeBatch(FILES, ops); // control: the identical batch is valid without the write limit
    assert.deepEqual(hits(), [newFile, null, newFile]);
  });

test('A malformed live tree cannot masquerade as a miss or empty inventory', t => {
  const f = fresh(t, FILES);
  const file = stage(f.store, 'file', 0o644);
  f.store.treeSet(FILES, 'formerly-present', file);
  // Read the good tree on this handle first, so that a handle that kept it
  // decoded has an old answer to fall back on.
  assert.deepEqual([point(f.store, FILES, 'formerly-present'), rows(f.store.treeList(FILES))],
    [file, [row('formerly-present', file)]]);
  // The fixture makes the tree undecodable through the byte setter. That the
  // store takes this write is what the fixture needs, not a requirement here.
  f.store.setState(FILES, Buffer.from('"a string, not a keyed tree"'));
  const diagnostic = error => error != null && (error instanceof Error ? error.message : String(error)).trim().length > 0;
  const refusals = (store, when) => {
    // pins treeGet.6 — indeterminate point read synchronously throws a reportable cause, not stale data or null.
    assert.throws(() => store.treeGet(FILES, 'formerly-present'), diagnostic, `${when} treeGet`);
    // pins treeList.5 — a decode failure throws a diagnostic instead of an empty, partial or stale inventory.
    assert.throws(() => store.treeList(FILES), diagnostic, `${when} treeList`);
  };
  // As a commit's readback does: the same handle, straight after the write.
  refusals(f.store, 'warm');
  const branch = f.store.currentBranch();
  f.store.close();
  f.store = JsStore.open({ path: f.dir });
  assert.equal(f.store.currentBranch().id, branch.id);
  refusals(f.store, 'cold');
});
