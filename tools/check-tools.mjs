// Runs the recovery tools in tools/ on a store this build writes, and on a committed store that chronicle 0.2.1
// wrote. It needs Python 3, as python3, or as python on Windows.
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { cpSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { crc32 } from 'node:zlib';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { JsStore } from '../index.js';

const tools = dirname(fileURLToPath(import.meta.url));
const scratch = mkdtempSync(join(tmpdir(), 'chronicle-tools-'));
const PYTHON = process.platform === 'win32' ? 'python' : 'python3';
const REBUILD_REFUSES = 'this store was written by chronicle 0.3.0 or newer; from 0.5.0, open rebuilds state.bin from '
  + 'records.log for a store with one branch; with more branches, open can refuse the store, or leave a branch without '
  + 'the states it inherited, and no tool here rebuilds it';
const FIXTURE_ITEMS = Array.from({ length: 50 }, (_, n) => ({ n, text: `message ${n} says hello` }));
const NOT_WRITTEN_HERE = { Delta: 'no strategy the JS API registers writes it' };

function python(args) {
  const run = spawnSync(PYTHON, args, { encoding: 'utf8', cwd: tools });
  if (run.error) throw new Error(`${PYTHON} didn't run: ${run.error.message}. This check needs Python 3 as ${PYTHON}.`);
  return { status: run.status, out: run.stdout, err: run.stderr };
}

function exitsZero(name, ...args) {
  const run = python([join(tools, name), ...args]);
  assert.equal(run.status, 0, `${name} ${args.join(' ')} exited ${run.status}:\n${run.out}${run.err}`);
  console.log(`ok: ${name} exits 0`);
  return run;
}

function scanIsClean(store, label, trees = []) {
  const { out } = exitsZero('scan-delta-poison.py', store, '--json');
  const expected = {};
  for (const { state } of updates(store)) expected[state] = (expected[state] ?? 0) + 1;
  const chains = JSON.parse(out).chains;
  assert.deepEqual(chains.map((chain) => chain.state_id).sort(), Object.keys(expected).sort(),
    `scan-delta-poison.py should read one chain for each state of ${label}:\n${out}`);
  for (const chain of chains) {
    const allowed = trees.includes(chain.state_id) ? ['SKIPPED_TREE'] : ['CLEAN', 'HEALED_BUT_HAD_WINDOW', 'OPEN_WINDOW'];
    assert.ok(allowed.includes(chain.status),
      `scan-delta-poison.py reads ${label}'s ${chain.state_id} chain as ${chain.status}${chain.error ? `: ${chain.error}` : ''}`);
    assert.equal(chain.ops, expected[chain.state_id], `scan-delta-poison.py's ${chain.state_id} chain of ${label} should hold each of its updates`);
  }
  console.log(`ok: scan-delta-poison.py reads each chain of ${label}`);
  return Object.fromEntries(chains.map((chain) => [chain.state_id, chain.status]));
}

function copyWithPayload(from, name, update, edit, { checksum = 'rewritten' } = {}) {
  const to = join(scratch, name);
  cpSync(from, to, { recursive: true });
  const log = readFileSync(join(to, 'records.log'));
  const size = log.readUInt32LE(update.payload - 4);
  const payload = edit(Buffer.from(log.subarray(update.payload, update.payload + size)));
  let crcAt = update.payload + size;
  crcAt += 2 + 8 * log.readUInt16LE(crcAt);
  crcAt += 2 + 8 * log.readUInt16LE(crcAt);
  const length = Buffer.alloc(4);
  length.writeUInt32LE(payload.length);
  const crc = Buffer.from(log.subarray(crcAt, crcAt + 4));
  if (checksum === 'rewritten') crc.writeUInt32LE(crc32(payload));
  writeFileSync(join(to, 'records.log'), Buffer.concat([log.subarray(0, update.payload - 4), length, payload,
    log.subarray(update.payload + size, crcAt), crc, log.subarray(crcAt + 4)]));
  return to;
}

function bothToolsStop(store, label, says) {
  for (const name of ['chain-report.py', 'scan-delta-poison.py']) {
    const run = python([join(tools, name), name === 'chain-report.py' ? join(store, 'records.log') : store]);
    assert.equal(run.status, 2, `${name} on ${label} exited ${run.status}:\n${run.out}${run.err}`);
    assert.ok(run.err.includes(says), `${name} on ${label} should say "${says}":\n${run.err}`);
  }
  console.log(`ok: chain-report.py and scan-delta-poison.py stop with exit 2 at ${label}`);
}

function updates(store) {
  const run = python(['-c', `
import json, struct, sys
from chronicle_payload import decode_update
f = open(sys.argv[1], 'rb'); size = f.seek(0, 2); f.seek(0); seen = []
while f.tell() < size:
    record = f.tell(); f.read(6); f.read(32); rtype = f.read(struct.unpack('<H', f.read(2))[0]).decode(); enc = f.read(1)[0]
    size_at = f.tell(); payload = f.read(struct.unpack('<I', f.read(4))[0])
    for _ in range(2): f.read(8 * struct.unpack('<H', f.read(2))[0])
    f.read(4)
    if rtype == 'state_update':
        u = decode_update(enc, payload); op = u['operation']
        seen.append({'state': u['state_id'], 'kind': op if isinstance(op, str) else next(iter(op)),
                     'record': record, 'payload': size_at + 4})
print(json.dumps(seen))`, join(store, 'records.log')]);
  assert.equal(run.status, 0, `reading the updates of ${store}:\n${run.err}`);
  return JSON.parse(run.out);
}

function states(store) {
  return {
    messages: store.getStateJson('messages'),
    settings: store.getStateJson('settings'),
    files: store.treeList('files', null),
  };
}

const REGISTRATIONS = [
  { id: 'messages', strategy: 'append_log', deltaSnapshotEvery: 3, fullSnapshotEvery: 2 },
  { id: 'settings', strategy: 'snapshot' },
  { id: 'files', strategy: 'tree', deltaSnapshotEvery: 2, fullSnapshotEvery: 2 },
];

try {
  const enumBody = readFileSync(join(tools, '..', 'src', 'types.rs'), 'utf8').replace(/\r\n/g, '\n')
    .match(/pub enum StateOperation \{\n([\s\S]*?)\n\}/);
  assert.ok(enumBody, 'src/types.rs has no `pub enum StateOperation`');
  const variants = [...enumBody[1].matchAll(/^[ \t]+([A-Z]\w*)\b/gm)].map((match) => match[1]);
  const lists = JSON.parse(python(['-c', 'import json, chronicle_payload as p; print(json.dumps([p.READ, p.NOT_READ]))']).out);
  const [read, notRead] = [new Set(lists[0]), lists[1]];
  const unplaced = variants.filter((kind) => !read.has(kind) && !(kind in notRead));
  assert.deepEqual(unplaced, [], `StateOperation has kinds that tools/chronicle_payload.py neither reads nor names as not read: ${unplaced.join(', ')}`);
  const listed = [...read, ...Object.keys(notRead)];
  const unknown = listed.filter((kind) => !variants.includes(kind));
  assert.deepEqual(unknown, [], `tools/chronicle_payload.py lists kinds that StateOperation in src/types.rs doesn't have: ${unknown.join(', ')}`);
  assert.equal(variants.length, listed.length, 'StateOperation has as many kinds as READ and NOT_READ list');
  console.log(`ok: each of StateOperation's ${variants.length} kinds is placed`);

  const built = join(scratch, 'built');
  const store = JsStore.create({ path: built });
  for (const registration of REGISTRATIONS) store.registerState(registration);
  for (let n = 0; n < 14; n++) store.appendToStateJson('messages', { n, text: `message ${n}` });
  store.appendManyToStateJson('messages', [{ n: 14, text: 'batched' }, { n: 15, text: 'batched' }]);
  store.editStateItem('messages', 2, Buffer.from(JSON.stringify({ n: 2, text: 'edited' })));
  store.redactStateItems('messages', 4, 6);
  store.setStateJson('settings', { theme: 'dark' });
  store.setStateJson('settings', { theme: 'light', size: 12 });
  const blob = store.storeBlob(Buffer.from('notes'), 'text/plain');
  const file = { blobHash: blob, size: 5, mode: 0o644 };
  store.treeSet('files', 'a.txt', file);
  store.treeSet('files', 'b.txt', file);
  store.treeBatch('files', [{ op: 'set', path: 'c.txt', entry: file }, { op: 'remove', path: 'b.txt' }]);
  store.treeRemove('files', 'a.txt');
  store.treeSet('files', 'd.txt', file);
  const written = states(store);
  store.sync();
  store.close();
  const held = updates(built);
  const kinds = [...new Set(held.map(({ kind }) => kind))].sort();
  assert.deepEqual(kinds, [...read].filter((kind) => !(kind in NOT_WRITTEN_HERE)).sort(),
    'the premise: the store holds each kind the tools read, but those the JS API can\'t write');
  console.log(`ok: the store holds ${kinds.join(', ')}`);

  exitsZero('trim-records-log.py', join(built, 'records.log'));
  const report = exitsZero('chain-report.py', join(built, 'records.log'));
  const messageUpdates = held.filter(({ state }) => state === 'messages').length;
  assert.match(report.out, new RegExp(`^chain0: ops=${messageUpdates} appends=16 `, 'm'),
    `chain-report.py's line for messages should show ${messageUpdates} operations and 16 items:\n${report.out}`);
  console.log(`ok: chain-report.py's line for messages shows its ${messageUpdates} operations and 16 items`);
  const builtStatuses = scanIsClean(built, 'the built store', ['files']);
  exitsZero('unchunk-blobs.py', built);

  const index = readFileSync(join(built, 'state.bin'));
  const rebuild = python([join(tools, 'rebuild-state-index.py'), built]);
  assert.equal(rebuild.status, 2, `rebuild-state-index.py exited ${rebuild.status}:\n${rebuild.out}${rebuild.err}`);
  assert.ok(rebuild.err.includes(REBUILD_REFUSES), `rebuild-state-index.py said:\n${rebuild.out}${rebuild.err}`);
  assert.ok(readFileSync(join(built, 'state.bin')).equals(index), 'rebuild-state-index.py changed state.bin as it refused');
  console.log('ok: rebuild-state-index.py refuses it, exit 2, and leaves state.bin as it was');

  const first = held.find(({ state }) => state === 'messages');
  const undecodable = (payload) => {
    payload[0] = 0xc1;
    return payload;
  };
  const corrupt = copyWithPayload(built, 'corrupt', first, undecodable, { checksum: 'kept' });
  bothToolsStop(corrupt, 'a record that fails its checksum', `the record at offset ${first.record} of records.log fails its checksum`);
  const damaged = copyWithPayload(built, 'damaged', first, undecodable);
  const skips = `typed state_update that don't decode as state updates, the first at offset ${first.record},`;
  const damagedReport = python([join(tools, 'chain-report.py'), join(damaged, 'records.log')]);
  assert.equal(damagedReport.status, 0, `chain-report.py on a record that doesn't decode exited ${damagedReport.status}:\n${damagedReport.err}`);
  assert.ok(damagedReport.err.includes(skips), `chain-report.py should say what it skipped:\n${damagedReport.err}`);
  const damagedScan = python([join(tools, 'scan-delta-poison.py'), damaged, '--json']);
  assert.equal(damagedScan.status, 0, `scan-delta-poison.py on a record that doesn't decode exited ${damagedScan.status}:\n${damagedScan.err}`);
  assert.ok(damagedScan.err.includes(skips), `scan-delta-poison.py should say what it skipped:\n${damagedScan.err}`);
  const statuses = Object.fromEntries(JSON.parse(damagedScan.out).chains.map((chain) => [chain.state_id, chain.status]));
  assert.deepEqual(statuses, { ...builtStatuses, messages: 'PARTIAL_CHAIN' },
    'the scan should show the chain that lost its record as partial, and the others as before');
  console.log('ok: chain-report.py and scan-delta-poison.py skip a record that doesn\'t decode and say so, and the scan shows its chain as partial');
  const unread = 'Field';
  assert.ok(unread in notRead, `the premise: tools/chronicle_payload.py names ${unread} as not read`);
  const renamed = copyWithPayload(built, 'unread', first, (payload) => {
    const at = payload.indexOf(Buffer.from('\xa6Append', 'latin1'));
    assert.ok(at >= 0, 'the premise: the first update of messages is an Append');
    return Buffer.concat([payload.subarray(0, at), Buffer.from(`${String.fromCharCode(0xa0 | unread.length)}${unread}`, 'latin1'),
      payload.subarray(at + 7)]);
  });
  bothToolsStop(renamed, `a state update of kind ${unread}`, `of kind ${unread} at offset ${first.record}, `);

  // The three stores below pin what rebuild-state-index.py's refusal says of open without state.bin: change them with it.
  const bare = join(scratch, 'bare');
  cpSync(built, bare, { recursive: true });
  rmSync(join(bare, 'state.bin'));
  const reopened = JsStore.open({ path: bare });
  for (const registration of REGISTRATIONS) reopened.registerState(registration);
  assert.deepEqual(states(reopened), written, 'a store opened without state.bin reads other states');
  reopened.close();
  console.log('ok: without state.bin, this build reads every state of a one-branch store as written');

  const branched = join(scratch, 'branched');
  const twoBranches = JsStore.create({ path: branched });
  twoBranches.registerState(REGISTRATIONS[0]);
  twoBranches.appendToStateJson('messages', { n: 0, text: 'on main' });
  twoBranches.createBranch('side', 'main');
  twoBranches.switchBranch('side');
  twoBranches.appendToStateJson('messages', { n: 1, text: 'on side' });
  twoBranches.sync();
  twoBranches.close();
  rmSync(join(branched, 'state.bin'));
  assert.throws(() => JsStore.open({ path: branched }), /cannot replay state 'messages'/,
    'a store whose second branch holds a state update should be refused without state.bin');
  console.log('ok: without state.bin, this build refuses a store whose second branch holds a state update');

  const unwritten = join(scratch, 'unwritten');
  const sideUnwritten = JsStore.create({ path: unwritten });
  sideUnwritten.registerState(REGISTRATIONS[0]);
  sideUnwritten.appendToStateJson('messages', { n: 0, text: 'on main' });
  sideUnwritten.createBranch('side', 'main');
  sideUnwritten.sync();
  sideUnwritten.close();
  rmSync(join(unwritten, 'state.bin'));
  const reopenedUnwritten = JsStore.open({ path: unwritten });
  reopenedUnwritten.registerState(REGISTRATIONS[0]);
  assert.deepEqual(reopenedUnwritten.getStateJson('messages'), [{ n: 0, text: 'on main' }], 'main reads its item');
  reopenedUnwritten.switchBranch('side');
  assert.equal(reopenedUnwritten.getStateJson('messages'), null,
    'a branch with no update of its own, without state.bin, should read the state it inherited as null');
  reopenedUnwritten.close();
  console.log('ok: without state.bin, this build opens a store whose second branch has no update, and reads its inherited state as null');

  // Git keeps no empty directory, so each copy gets back the empty blobs/ that 0.2.1 made.
  const fixture = (to) => {
    cpSync(join(tools, 'fixtures', 'json-updates'), to, { recursive: true });
    mkdirSync(join(to, 'blobs'));
    return to;
  };
  const old = fixture(join(scratch, 'old'));
  exitsZero('trim-records-log.py', join(old, 'records.log'));
  exitsZero('chain-report.py', join(old, 'records.log'));
  scanIsClean(old, 'the 0.2.1 store');
  exitsZero('unchunk-blobs.py', old);
  rmSync(join(old, 'state.bin'));
  exitsZero('rebuild-state-index.py', old);
  const rebuilt = JsStore.open({ path: old });
  rebuilt.registerState({ id: 'messages', strategy: 'append_log' });
  assert.equal(rebuilt.getStateLen('messages'), 50, 'the rebuilt store counts 50 items');
  assert.deepEqual(rebuilt.getStateJson('messages'), FIXTURE_ITEMS, 'the rebuilt store reads the 50 items 0.2.1 wrote');
  rebuilt.close();
  console.log('ok: the state.bin rebuild-state-index.py writes opens with the 50 items 0.2.1 wrote');

  const mixed = fixture(join(scratch, 'mixed'));
  const later = JsStore.open({ path: mixed });
  for (let n = 50; n < 55; n++) later.appendToStateJson('messages', { n, text: `message ${n}` });
  later.sync();
  later.close();
  exitsZero('chain-report.py', join(mixed, 'records.log'));
  scanIsClean(mixed, 'the store with both encodings');
  rmSync(join(mixed, 'state.bin'));
  const replayed = JsStore.open({ path: mixed });
  replayed.registerState({ id: 'messages', strategy: 'append_log' });
  assert.deepEqual(replayed.getStateJson('messages'),
    [...FIXTURE_ITEMS, ...Array.from({ length: 5 }, (_, i) => ({ n: 50 + i, text: `message ${50 + i}` }))],
    'the store with both encodings, without state.bin, reads its 55 items');
  replayed.close();
  console.log('ok: without state.bin, this build reads the 55 items of the store with both encodings');
} finally {
  rmSync(scratch, { recursive: true, force: true });
}
