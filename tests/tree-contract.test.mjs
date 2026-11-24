import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { constants as osConstants, tmpdir } from 'node:os';
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

function fields(e) {
  if (e === null) return null;
  return { blobHash: e.blobHash, size: e.size, mode: e.mode };
}

function point(store, tree, path) { return fields(store.treeGet(tree, path)); }
function row(path, entry) { return { path, ...fields(entry) }; }
function rows(list) { return list.map(e => row(e.path, e)); }

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
  assert.equal(store.treeGet(ENVELOPES, requestKey), null);
  store.registerState({ id: ENVELOPES, strategy: 'tree' });
  const envelopeBytes = Buffer.from('{"version":1,"segments":[]}');
  const envelope = stage(store, envelopeBytes, 0, 'application/json');
  store.treeSet(FILES, requestKey, original);
  store.treeSet(ENVELOPES, requestKey, envelope);
  store.treeSet(FILES, requestKey, placeholder);
  assert.deepEqual([point(store, ENVELOPES, requestKey), point(store, FILES, requestKey), point(store, FILES, path)],
    [envelope, placeholder, original]);

  const hit = store.treeGet(FILES, path);
  assert.equal(hit.blobHash, original.blobHash);
  assert.deepEqual([point(store, FILES, path), point(store, INTENTS, path), store.treeGet(FILES, 'src'),
    store.treeGet(FILES, 'src/Note.md'), store.treeGet(FILES, 'src//note.md'), store.treeGet(FILES, 'damaged')],
    [original, liveIntent, null, null, null, null]);
  assert.deepEqual([Boolean(hit), Boolean(store.treeGet(INTENTS, 'damaged')), store.treeGet(INTENTS, 'absent')],
    [true, true, null]);
  const indexed = store.treeGet(ENVELOPES, requestKey);
  assert.deepEqual([indexed.blobHash, store.getBlob(indexed.blobHash)], [envelope.blobHash, envelopeBytes]);
  assert.deepEqual([hit.size, store.treeGet(INTENTS, 'damaged').size], [originalBytes.byteLength, damaged.size]);
  assert.equal(hit.blobHash, digest(originalBytes));
  const evidence = { path, ...hit };
  assert.deepEqual([evidence.blobHash, evidence.mode], [original.blobHash, original.mode]);

  const newBytes = Buffer.from('révisé 📝\n');
  const revised = stage(store, newBytes, 0o100600);
  store.treeSet(FILES, path, revised);
  const overwritten = store.treeGet(FILES, path);
  assert.deepEqual([hit.mode, overwritten.mode], [0o644, 0o100600]);
  assert.deepEqual([point(store, FILES, path), point(store, FILES, 'legacy.txt'), point(store, INTENTS, path)],
    [revised, placeholder, liveIntent]);
  const chmod = { blobHash: overwritten.blobHash, size: overwritten.size, mode: 0o100755 };
  store.treeSet(FILES, path, chmod);
  assert.deepEqual(point(store, FILES, path), { ...revised, mode: 0o100755 });

  const files = store.treeList(FILES);
  const intents = store.treeList(INTENTS);
  assert.deepEqual([byPath(rows(files)), byPath(rows(intents))],
    [byPath([row(path, chmod), row('legacy.txt', placeholder), row(requestKey, placeholder)]),
      byPath([row(path, liveIntent), row('damaged', damaged)])]);
  const live = intents.find(e => e.path === path);
  const lost = intents.find(e => e.path === 'damaged');
  const readable = hash => {
    try { return store.getBlob(hash) != null; } catch { return false; }
  };
  assert.deepEqual([live.blobHash, JSON.parse(store.getBlob(live.blobHash).toString()), lost.blobHash, readable(lost.blobHash)],
    [liveIntent.blobHash, oldIntent, damaged.blobHash, false]);
  assert.equal(files.find(e => e.path === path).blobHash, digest(newBytes));
  assert.deepEqual([files.find(e => e.path === path).mode, files.find(e => e.path === 'legacy.txt').mode], [0o100755, 0o644]);

  const emptyList = Buffer.from('[]');
  store.treeSet(CHECKPOINTS, 'agent', stage(store, emptyList, 0o644, 'application/json'));
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
  assert.deepEqual(fields(saved), broken);
  store.treeSet(INTENTS, path, saved);
  assert.deepEqual(point(store, INTENTS, path), broken);
  store.treeRemove(INTENTS, path);
  assert.equal(store.treeGet(INTENTS, path), null);

  const before = store.treeGet(INTENTS, 'new');
  store.treeSet(INTENTS, 'new', replacement);
  assert.deepEqual(point(store, INTENTS, 'new'), replacement);
  if (before === null) store.treeRemove(INTENTS, 'new');
  else store.treeSet(INTENTS, 'new', before);
  assert.deepEqual([before, store.treeGet(INTENTS, 'new')], [null, null]);
});

test('An intent whose blob is unreadable can be saved, replaced, restored and removed', t => {
  const f = fresh(t, INTENTS);
  const path = 'draft';
  const unreadable = intent(f.store, { origin: 'about to be damaged' });
  f.store.treeSet(INTENTS, path, unreadable);
  f.store.close();
  const file = join(f.dir, 'blobs', unreadable.blobHash.slice(0, 2), unreadable.blobHash);
  const bytes = readFileSync(file);
  bytes[bytes.length - 2] ^= 0xff;
  writeFileSync(file, bytes);
  f.store = JsStore.open({ path: f.dir });
  const { store } = f;
  assert.throws(() => store.getBlob(unreadable.blobHash));
  assert.deepEqual(point(store, INTENTS, path), unreadable);
  const saved = store.treeGet(INTENTS, path);
  const replacement = intent(store, { origin: 'store' });
  store.treeSet(INTENTS, path, replacement);
  assert.deepEqual(point(store, INTENTS, path), replacement);
  store.treeRemove(INTENTS, path);
  assert.equal(store.treeGet(INTENTS, path), null);
  store.treeSet(INTENTS, path, saved);
  assert.deepEqual(point(store, INTENTS, path), unreadable);
  store.treeRemove(INTENTS, path);
  assert.deepEqual([store.treeGet(INTENTS, path), store.treeList(INTENTS)], [null, []]);
});

test('Listing uses exact literal prefixes and ordered original path spellings', t => {
  const { store } = fresh(t, FILES);
  const base = stage(store, 'first version', 0o644);
  const draft = 'drafts/sub/note';
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
  assert.deepEqual([rows(all), fields(store.treeGet(FILES, draft))], [expected, latest]);
  assert.deepEqual([Array.isArray(all), store.treeList(FILES, 'absent/')], [true, []]);
  const spellings = paths(store.treeList(FILES, 'a//'));
  assert.deepEqual([...spellings].sort(), ['a//B.txt', 'a//b.txt']);
  assert.deepEqual(paths(store.treeList(FILES, 'src/')), ['src/f', 'src/sub/f']);
  assert.deepEqual([rows(all), rows(store.treeList(FILES, undefined)), rows(store.treeList(FILES, ''))], [expected, expected, expected]);
  assert.equal(all.filter(e => e.path === draft).length, 1);
  assert.deepEqual(paths(all).filter(path => path.toLowerCase() === 'a//b.txt'), ['a//B.txt', 'a//b.txt']);
  assert.deepEqual(paths(store.treeList(FILES, 'docs/')), ['docs/README.md', 'docs/readme.md']);
});

test('Removing a key leaves a key that starts with it', t => {
  const { store } = fresh(t, FILES);
  const removed = stage(store, 'removed', 0o100644);
  const kept = stage(store, 'kept', 0o100644);
  store.treeSet(FILES, 'src/drop', removed);
  store.treeSet(FILES, 'src/drop.bak', kept);
  store.treeRemove(FILES, 'src/drop');
  assert.deepEqual([store.treeGet(FILES, 'src/drop'), point(store, FILES, 'src/drop.bak'), rows(store.treeList(FILES))],
    [null, fields(kept), [row('src/drop.bak', kept)]]);
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
  const cwd = process.cwd();
  let dropRemoved;
  try {
    process.chdir(mount);
    dropRemoved = store.treeRemove(FILES, drop).sequence;
  } finally { process.chdir(cwd); }
  assert.deepEqual([store.treeGet(FILES, drop), readFileSync(disk)], [null, bytes]);
  assert.deepEqual([store.treeGet(FILES, drop), point(store, INTENTS, drop), point(store, FILES, guard), point(store, FILES, keep)],
    [null, tomb, guarded, kept]);
  assert.deepEqual([rows(store.treeList(FILES)), rows(store.treeList(FILES, 'src/'))],
    [[row(guard, guarded), row(keep, kept)], [row(guard, guarded)]]);

  const guardRemoved = store.treeRemove(FILES, guard).sequence;
  store.treeRemove(INTENTS, guard);
  store.treeRemove(INTENTS, keep);
  assert.deepEqual([point(store, FILES, keep), store.treeGet(INTENTS, keep), store.treeGet(INTENTS, guard), point(store, INTENTS, drop)],
    [kept, null, null, tomb]);
  assert.deepEqual([rows(store.treeList(INTENTS)), rows(store.treeList(INTENTS, 'src/'))], [[row(drop, tomb)], [row(drop, tomb)]]);
  const pair = path => [point(store, FILES, path), point(store, INTENTS, path)];

  function select(name, sequence, fileEntries, intentEntries) {
    const branch = store.createBranchAt(name, 'main', sequence);
    store.switchBranch(name);
    const keys = [drop, guard, keep];
    const present = entries => entries.flatMap((entry, i) => entry === null ? [] : [row(keys[i], entry)]);
    assert.deepEqual([store.currentBranch().id, rows(store.treeList(FILES)), rows(store.treeList(INTENTS))],
      [branch.id, present(fileEntries), present(intentEntries)], name);
    assert.deepEqual([store.currentBranch().id, keys.map(k => point(store, FILES, k)), keys.map(k => point(store, INTENTS, k))],
      [branch.id, fileEntries, intentEntries], name);
  }
  select('before-delete', before, [file, guarded, kept], [prior, origin, origin]);
  assert.deepEqual([pair(drop), pair(guard)], [[file, prior], [guarded, origin]]);
  select('tombstone-written', tombFirst, [file, guarded, kept], [tomb, origin, origin]);
  assert.deepEqual(pair(drop), [file, tomb]);
  select('file-dropped', dropRemoved, [null, guarded, kept], [tomb, origin, origin]);
  assert.deepEqual(pair(drop), [null, tomb]);
  select('guard-not-cleared', guardRemoved, [null, null, kept], [tomb, origin, origin]);
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
  assert.deepEqual([rows(store.treeList(FILES)), point(store, FILES, 'replace'), store.treeGet(FILES, 'new/file')],
    [old, original, null]);

  const before = store.currentSequence();
  store.treeBatch(FILES, [
    { op: 'set', path: 'replace', entry: edited },
    { op: 'remove', path: 'delete' },
    { op: 'set', path: 'keep', entry: original },
    { op: 'set', path: 'new/file', entry: added },
  ]);
  const updates = store.query({ types: ['state_update'], fromSequence: before + 1 });
  assert.deepEqual(updates.map(r => JSON.parse(store.getStateUpdateJson(r.id)).state_id), [FILES]);
  const committed = [row('keep', original), row('new/file', added), row('replace', edited)];
  assert.deepEqual(rows(store.treeList(FILES)), committed);
  const newHit = store.treeGet(FILES, 'new/file'), editHit = store.treeGet(FILES, 'replace');
  assert.deepEqual([fields(newHit), fields(editHit), store.treeGet(FILES, 'delete')], [added, edited, null]);
  assert.deepEqual([store.getBlob(newHit.blobHash), store.getBlob(editHit.blobHash)], [addBytes, editBytes]);

  for (const [branch, expected] of [[sibling, old], [main, old], [commit, committed]]) {
    store.switchBranch(branch.name);
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

    const ops = [{ op: 'set', path: 'edit', entry: newFile }, { op: 'remove', path: 'delete' },
      { op: 'set', path: 'new', entry: newFile }];
    const probe = `
      const [moduleUrl, dir, tree, serialized] = process.argv.slice(1);
      const { JsStore } = await import(moduleUrl);
      process.on('SIGXFSZ', () => {});
      const store = JsStore.open({ path: dir });
      store.setAutoSnapshot(false);
      const ops = JSON.parse(serialized);
      let threw = null;
      try { store.treeBatch(tree, ops); } catch (error) { threw = { error: error instanceof Error, code: error.code, message: error.message }; }
      const observation = { threw, seq: store.currentSequence(), hits: ops.map(op => store.treeGet(tree, op.path)) };
      try { store.close(); } catch {}
      process.stdout.write(JSON.stringify(observation));
    `;
    const child = spawnSync('sh', ['-c', 'trap "" XFSZ && ulimit -f 0 && exec "$@"', 'sh', process.execPath,
      '--input-type=module', '-e', probe, new URL('../index.js', import.meta.url).href, dir, FILES, JSON.stringify(ops)],
    { encoding: 'utf8', timeout: 30000 });
    assert.ifError(child.error);
    assert.equal(child.status, 0, child.stderr || `child signal: ${child.signal}`);
    const result = JSON.parse(child.stdout);
    const errno = result.threw?.code === 'EFBIG' ? osConstants.errno.EFBIG
      : Number(/^IO error: .*\(os error (\d+)\)$/s.exec(result.threw?.message ?? '')?.[1]);
    assert.deepEqual([result.threw?.error, errno, result.seq, result.hits.map(fields)],
      [true, osConstants.errno.EFBIG, seq, [first, second, null]], JSON.stringify(result.threw));
    f.store = JsStore.open({ path: dir });
    const hits = () => ops.map(op => point(f.store, FILES, op.path));
    assert.deepEqual(hits(), [first, second, null]);
    f.store.treeBatch(FILES, ops);
    assert.deepEqual(hits(), [newFile, null, newFile]);
  });

test('A malformed live tree cannot masquerade as a miss or empty inventory', t => {
  const f = fresh(t, FILES);
  const file = stage(f.store, 'file', 0o644);
  f.store.treeSet(FILES, 'formerly-present', file);
  assert.deepEqual([point(f.store, FILES, 'formerly-present'), rows(f.store.treeList(FILES))],
    [file, [row('formerly-present', file)]]);
  f.store.setState(FILES, Buffer.from('"a string, not a keyed tree"'));
  const diagnostic = error => error != null && (error instanceof Error ? error.message : String(error)).trim().length > 0;
  const refusals = (store, when) => {
    assert.throws(() => store.treeGet(FILES, 'formerly-present'), diagnostic, `${when} treeGet`);
    assert.throws(() => store.treeList(FILES), diagnostic, `${when} treeList`);
  };
  refusals(f.store, 'warm');
  const branch = f.store.currentBranch();
  f.store.close();
  f.store = JsStore.open({ path: f.dir });
  assert.equal(f.store.currentBranch().id, branch.id);
  refusals(f.store, 'cold');
});
