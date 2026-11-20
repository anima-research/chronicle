// Runs the recovery tools in tools/ on a store this build writes, and on a committed store that chronicle 0.2.1
// wrote. It needs Python 3, as python3, or as python on Windows.
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { chmodSync, cpSync, existsSync, lstatSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, renameSync, rmSync, statSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { crc32 } from 'node:zlib';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { JsStore } from '../index.js';

const tools = dirname(fileURLToPath(import.meta.url));
const scratch = mkdtempSync(join(tmpdir(), 'chronicle-tools-'));
const PYTHON = process.platform === 'win32' ? 'python' : 'python3';
const REBUILD_REFUSES = 'this store was written by chronicle 0.3.0 or newer; from 0.5.0, open without state.bin '
  + 'replays the state updates in records.log, and the next save writes state.bin; it refuses a broken chain, and with '
  + 'more than one branch it can refuse the store or leave a branch without the states it inherited; no tool here '
  + 'rebuilds state.bin for it';
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

function withAppendedRecord(from, name, template, payload) {
  const to = join(scratch, name);
  cpSync(from, to, { recursive: true });
  const log = readFileSync(join(to, 'records.log'));
  const header = Buffer.from(log.subarray(template.record, template.payload - 4));
  header.writeBigUInt64LE(1n << 40n, 6);
  const length = Buffer.alloc(4);
  length.writeUInt32LE(payload.length);
  const crc = Buffer.alloc(4);
  crc.writeUInt32LE(crc32(payload));
  writeFileSync(join(to, 'records.log'), Buffer.concat([log, header, length, payload, Buffer.alloc(4), crc]));
  return { store: to, offset: log.length };
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
    record = f.tell(); f.read(6); rid, _, branch, _ = struct.unpack('<QQQq', f.read(32)); rtype = f.read(struct.unpack('<H', f.read(2))[0]).decode(); enc = f.read(1)[0]
    size_at = f.tell(); payload = f.read(struct.unpack('<I', f.read(4))[0])
    for _ in range(2): f.read(8 * struct.unpack('<H', f.read(2))[0])
    f.read(4)
    if rtype == 'state_update':
        u = decode_update(enc, payload); op = u['operation']
        seen.append({'state': u['state_id'], 'kind': op if isinstance(op, str) else next(iter(op)),
                     'record': record, 'payload': size_at + 4, 'branch': branch, 'id': rid,
                     'prev': u.get('prev_update_offset')})
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

const MODULE = join(tools, '..');
const WHOLE = 'whole';
const CONTENT = 'words that state-digests.mjs must not print';
const COMPACT_REGISTRATIONS = [
  { id: WHOLE, strategy: 'snapshot' },
  { id: 'messages', strategy: 'append_log', deltaSnapshotEvery: 3, fullSnapshotEvery: 2 },
  { id: 'files', strategy: 'tree', deltaSnapshotEvery: 2, fullSnapshotEvery: 2 },
];

function compact(store, ...args) {
  return python([join(tools, 'compact-set-states.py'), store, ...args]);
}

function digest(store, label, patterns = [WHOLE]) {
  const run = spawnSync(process.execPath, [join(tools, 'state-digests.mjs'), store, ...patterns.flatMap((p) => ['--state', p])],
    { encoding: 'utf8', env: { ...process.env, CHRONICLE_MODULE: MODULE } });
  assert.equal(run.status, 0, `state-digests.mjs on ${label} exited ${run.status}:\n${run.stdout}${run.stderr}`);
  return run.stdout;
}

// The check opens the store, which saves state.bin, so a check that must leave the store as it is reads a copy.
function digestOfCopy(store, label, patterns) {
  const copy = join(scratch, `copy-${++copies}`);
  cpSync(store, copy, { recursive: true });
  const read = digest(copy, label, patterns);
  rmSync(copy, { recursive: true });
  return read;
}
let copies = 0;

function filesOf(dir) {
  const files = {};
  const walk = (at) => {
    for (const entry of readdirSync(at, { withFileTypes: true })) {
      const path = join(at, entry.name);
      if (entry.isDirectory()) walk(path);
      else files[relative(dir, path)] = `${statSync(path).mode.toString(8)} ${createHash('sha256').update(readFileSync(path)).digest('hex')}`;
    }
  };
  walk(dir);
  return files;
}

function applies(store, label, patterns = [WHOLE]) {
  const before = digest(store, `${label} before the tool`, patterns);
  const size = statSync(join(store, 'records.log')).size;
  const run = compact(store, ...patterns.flatMap((p) => ['--state', p]), '--apply');
  assert.equal(run.status, 0, `compact-set-states.py on ${label} exited ${run.status}:\n${run.out}${run.err}`);
  assert.equal(digest(store, `${label} after the tool`, patterns), before, `state-digests.mjs should read ${label} the same after the tool`);
  assert.ok(statSync(join(store, 'records.log')).size < size, `the tool should make the log of ${label} smaller`);
  console.log(`ok: compact-set-states.py applies to ${label}, its log is smaller, and state-digests.mjs reads it the same`);
}

function refuses(store, label, says, patterns = [WHOLE]) {
  const before = filesOf(store);
  const run = compact(store, ...patterns.flatMap((p) => ['--state', p]), '--apply');
  assert.equal(run.status, 1, `compact-set-states.py on ${label} exited ${run.status}:\n${run.out}${run.err}`);
  assert.ok(run.out.includes(says), `compact-set-states.py on ${label} should say "${says}":\n${run.out}${run.err}`);
  assert.ok(run.out.includes('Nothing in the store changed.'), `compact-set-states.py on ${label} should say that nothing changed:\n${run.out}`);
  assert.deepEqual(filesOf(store), before, `compact-set-states.py changed a file of ${label} as it refused`);
  console.log(`ok: compact-set-states.py refuses ${label}, exit 1, and changes no file`);
}

function toolPython(script, ...args) {
  const run = spawnSync(PYTHON, ['-c', `
import importlib.util, json, os, sys
spec = importlib.util.spec_from_file_location('compact', 'compact-set-states.py')
tool = importlib.util.module_from_spec(spec); spec.loader.exec_module(tool)
${script}`, ...args], { cwd: tools });
  assert.equal(run.status, 0, `running the tool's functions:\n${run.stderr}`);
  return run.stdout;
}

function blockedStateBin(store) {
  return toolPython('sys.stdout.buffer.write(tool.blocked_state_bin(sys.argv[1]))', store);
}

function savedHeads(store) {
  return JSON.parse(toolPython(`
index = tool.read_state_bin(open(os.path.join(sys.argv[1], 'state.bin'), 'rb').read())
print(json.dumps([{'branch': key[0], 'state': key[1], 'offset': head[0]} for key, head in index[0]]))`, store).toString());
}

// state.bin with each head changed by `edit`, a line of Python over `head` and `key`.
function withHeads(store, edit) {
  const sb = join(store, 'state.bin');
  writeFileSync(sb, toolPython(`
index = tool.read_state_bin(open(sys.argv[1], 'rb').read())
for key, head in index[0]:
    ${edit}
sys.stdout.buffer.write(tool.state_bin_bytes(index))`, sb));
}

// Each head this build reads, on a copy, since an open saves state.bin.
function headOffsets(store) {
  const copy = join(scratch, 'heads');
  cpSync(store, copy, { recursive: true });
  const reader = JsStore.open({ path: copy });
  const heads = [];
  for (const { name } of reader.listBranches()) {
    reader.switchBranch(name);
    for (const { id } of reader.listStates()) {
      const offset = reader.stateHeadOffset(id);
      if (offset !== null) heads.push({ branch: name, state: id, offset });
    }
  }
  reader.close();
  rmSync(copy, { recursive: true });
  return heads;
}

function appendRecord(store, template, sequence, payload) {
  const path = join(store, 'records.log');
  const log = readFileSync(path);
  const header = Buffer.from(log.subarray(template.record, template.payload - 4));
  header.writeBigUInt64LE(1n << 40n, 6);
  header.writeBigUInt64LE(BigInt(sequence), 14);
  const length = Buffer.alloc(4);
  length.writeUInt32LE(payload.length);
  const crc = Buffer.alloc(4);
  crc.writeUInt32LE(crc32(payload));
  writeFileSync(path, Buffer.concat([log, header, length, payload, Buffer.alloc(4), crc]));
  return log.length;
}

// MessagePack for the values a state update holds: a map is a Map, an array an Array, bytes a Buffer.
function msgpack(value) {
  const sized = (small, codes, n) => {
    if (n < small.limit) return Buffer.from([small.base | n]);
    const [c8, c16, c32] = codes;
    if (c8 !== null && n < 256) return Buffer.from([c8, n]);
    if (n < 65536) return Buffer.from([c16, n >> 8, n & 255]);
    const out = Buffer.alloc(5); out[0] = c32; out.writeUInt32BE(n, 1); return out;
  };
  if (value === null) return Buffer.from([0xc0]);
  if (typeof value === 'number') {
    const out = Buffer.alloc(9);
    if (value < 0) { out[0] = 0xd3; out.writeBigInt64BE(BigInt(value), 1); } else { out[0] = 0xcf; out.writeBigUInt64BE(BigInt(value), 1); }
    return out;
  }
  if (typeof value === 'string') {
    const text = Buffer.from(value);
    return Buffer.concat([sized({ limit: 32, base: 0xa0 }, [0xd9, 0xda, 0xdb], text.length), text]);
  }
  if (Buffer.isBuffer(value)) return Buffer.concat([sized({ limit: 0, base: 0 }, [0xc4, 0xc5, 0xc6], value.length), value]);
  if (Array.isArray(value)) return Buffer.concat([sized({ limit: 16, base: 0x90 }, [null, 0xdc, 0xdd], value.length), ...value.map(msgpack)]);
  if (value instanceof Map) {
    return Buffer.concat([sized({ limit: 16, base: 0x80 }, [null, 0xde, 0xdf], value.size),
      ...[...value].flatMap(([key, item]) => [msgpack(key), msgpack(item)])]);
  }
  throw new Error(`msgpack: ${typeof value}`);
}

// The payload with its prev_update_offset value replaced by `offset` in the same width, so no record moves.
function withLink(payload, offset) {
  const key = msgpack('prev_update_offset');
  const at = payload.indexOf(key) + key.length;
  const edited = Buffer.from(payload);
  const write = { 0xcc: (o) => edited.writeUInt8(o, at + 1), 0xcd: (o) => edited.writeUInt16BE(o, at + 1),
    0xce: (o) => edited.writeUInt32BE(o, at + 1), 0xcf: (o) => edited.writeBigUInt64BE(BigInt(o), at + 1) }[payload[at]];
  assert.ok(write || (payload[at] < 0x80 && offset < 0x80), `the premise: the link at ${at} takes ${offset} in its width`);
  if (write) write(offset);
  else edited[at] = offset;
  return edited;
}

function branchedStore(name, { goneLast = false } = {}) {
  const path = join(scratch, name);
  const store = JsStore.create({ path });
  for (const registration of COMPACT_REGISTRATIONS) store.registerState(registration);
  const blob = store.storeBlob(Buffer.from('notes'), 'text/plain');
  const write = (branch, from, count) => {
    store.switchBranch(branch);
    for (let n = from; n < from + count; n++) {
      store.setStateJson(WHOLE, { n, branch, text: CONTENT, padding: 'x'.repeat(200) });
      store.appendToStateJson('messages', { n, branch, text: CONTENT });
      store.treeSet('files', `${branch}-${n}.txt`, { blobHash: blob, size: 5, mode: 0o644 });
      store.appendJson('note', { n, branch, text: CONTENT });
    }
  };
  write('main', 0, 5);
  const past = store.currentSequence();
  write('main', 5, 5);
  store.createBranch('side', 'main');
  write('side', 10, 10);
  store.createBranch('deep', 'side');
  write('deep', 20, 10);
  store.createBranch('idle', 'main');
  const gone = store.createBranch('gone', 'main').id;
  if (!goneLast) write('gone', 30, 5);
  write('deep', 35, 5);
  if (goneLast) write('gone', 30, 5);
  store.switchBranch('deep');
  store.deleteBranch('gone');
  store.registerStateFieldIndex('messages', '/n', 'number');
  store.sync();
  store.close();
  return { path, past, gone };
}

function keptByRule(store, states) {
  const held = updates(store);
  const at = new Map(held.map((update) => [update.record, update]));
  const heads = savedHeads(store);
  const newest = Math.max(...heads.map(({ offset }) => offset));
  const starts = [...heads.filter(({ state }) => states.includes(state)).map(({ offset }) => at.get(offset)),
    ...held.filter(({ state, record }) => states.includes(state) && record > newest)];
  const kept = new Set();
  for (let update of starts) {
    while (update && !kept.has(update.id)) {
      kept.add(update.id);
      if (['Set', 'Snapshot'].includes(update.kind) || update.prev === null) break;
      update = at.get(update.prev);
    }
  }
  return [...kept].sort();
}

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
  const ordinary = withAppendedRecord(built, 'ordinary', first,
    Buffer.from('\x82\xa8state_id\xa8messages\xa9operation\x81\xa6Append\xc4\x02{}', 'latin1'));
  const skips = `typed state_update that don't decode as state updates, the first at offset ${ordinary.offset},`;
  const ordinaryReport = python([join(tools, 'chain-report.py'), join(ordinary.store, 'records.log')]);
  assert.equal(ordinaryReport.status, 0, `chain-report.py on a record that isn't a state update exited ${ordinaryReport.status}:\n${ordinaryReport.err}`);
  assert.ok(ordinaryReport.err.includes(skips), `chain-report.py should say what it skipped:\n${ordinaryReport.err}`);
  const ordinaryScan = python([join(tools, 'scan-delta-poison.py'), ordinary.store, '--json']);
  assert.equal(ordinaryScan.status, 0, `scan-delta-poison.py on a record that isn't a state update exited ${ordinaryScan.status}:\n${ordinaryScan.err}`);
  assert.ok(ordinaryScan.err.includes(skips), `scan-delta-poison.py should say what it skipped:\n${ordinaryScan.err}`);
  const statuses = Object.fromEntries(JSON.parse(ordinaryScan.out).chains.map((chain) => [chain.state_id, chain.status]));
  assert.deepEqual(statuses, builtStatuses, 'the scan should read each chain as on the store without the record');
  const keeps = JsStore.open({ path: ordinary.store });
  assert.deepEqual(states(keeps), written, 'this build should keep the record as an ordinary one and read each state as written');
  keeps.close();
  console.log('ok: chain-report.py and scan-delta-poison.py skip a record typed state_update that isn\'t one and say so, as this build keeps it');
  const unread = 'Field';
  assert.ok(unread in notRead, `the premise: tools/chronicle_payload.py names ${unread} as not read`);
  const firstSetting = held.find(({ state }) => state === 'settings');
  const wrapped = copyWithPayload(built, 'unread', firstSetting, (payload) => {
    const at = payload.indexOf(Buffer.from('\xa9operation', 'latin1')) + 10;
    return Buffer.concat([payload.subarray(0, at), Buffer.from('\x81\xa5Field\x82\xa4name\xa1x\xa9operation', 'latin1'),
      payload.subarray(at)]);
  });
  bothToolsStop(wrapped, `a state update of kind ${unread}`, `of kind ${unread} at offset ${firstSetting.record}, `);

  // The stores below pin what rebuild-state-index.py's refusal says of open without state.bin: change them with it.
  const bare = join(scratch, 'bare');
  cpSync(built, bare, { recursive: true });
  rmSync(join(bare, 'state.bin'));
  const reopened = JsStore.open({ path: bare });
  for (const registration of REGISTRATIONS) reopened.registerState(registration);
  assert.deepEqual(states(reopened), written, 'a store opened without state.bin reads other states');
  reopened.close();
  assert.ok(existsSync(join(bare, 'state.bin')), 'the close after an open without state.bin should write state.bin');
  console.log('ok: without state.bin, this build reads every state of a one-branch store as written');

  const log = readFileSync(join(built, 'records.log'));
  const secondRoot = withAppendedRecord(built, 'broken', first,
    log.subarray(first.payload, first.payload + log.readUInt32LE(first.payload - 4)));
  rmSync(join(secondRoot.store, 'state.bin'));
  assert.throws(() => JsStore.open({ path: secondRoot.store }), /cannot replay state 'messages'/,
    'a store whose messages chain has a second root should be refused without state.bin');
  console.log('ok: without state.bin, this build refuses a store with a broken chain');

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

  const compactCases = {
    'the check reads every branch, prints no content, leaves the branch it found, and fails on a failed read': () => {
      const { path } = branchedStore('digest');
      const read = digest(path, 'the store with branches', []);
      for (const branch of ['main', 'side', 'deep', 'idle']) assert.ok(read.includes(`branch ${branch}\t`), `state-digests.mjs should read branch ${branch}:\n${read}`);
      assert.ok(!read.includes(CONTENT), 'state-digests.mjs printed content');
      for (const state of [WHOLE, 'messages']) assert.ok(read.includes(`\t${state}\tat eight sequences `), `state-digests.mjs should read the history of ${state}:\n${read}`);
      assert.ok(!digest(path, 'the store with branches', ['wh?l*']).includes(`\t${WHOLE}\tat eight sequences `), 'state-digests.mjs should leave out the history of a state that a --state glob names');
      const reopened = JsStore.open({ path });
      assert.equal(reopened.currentBranch().name, 'deep', 'state-digests.mjs should leave the store on the branch it found');
      reopened.close();
      const sideHead = headOffsets(path).find(({ branch, state }) => branch === 'side' && state === WHOLE).offset;
      const sideSet = updates(path).find(({ record }) => record === sideHead);
      const broken = copyWithPayload(path, 'digest-broken', sideSet, (payload) => {
        payload[0] = 0xc1;
        return payload;
      });
      const run = spawnSync(process.execPath, [join(tools, 'state-digests.mjs'), broken], { encoding: 'utf8', env: { ...process.env, CHRONICLE_MODULE: MODULE } });
      assert.notEqual(run.status, 0, `state-digests.mjs should exit non-zero when a read of branch side fails:\n${run.stdout}${run.stderr}`);
      const absent = JsStore.create({ path: join(scratch, 'digest-null') });
      absent.registerState({ id: WHOLE, strategy: 'snapshot' });
      absent.close();
      const none = digest(join(scratch, 'digest-null'), 'a store whose state is absent');
      const nulled = JsStore.open({ path: join(scratch, 'digest-null') });
      nulled.setStateJson(WHOLE, null);
      nulled.close();
      assert.notEqual(digest(join(scratch, 'digest-null'), 'a store whose state is null').split('\n').find((line) => line.includes(`\t${WHOLE}\t`)),
        none.split('\n').find((line) => line.includes(`\t${WHOLE}\t`)), 'state-digests.mjs should read an absent state and a null one apart');
      console.log('ok: state-digests.mjs reads every branch and the history of each state, prints no content, leaves the branch it found, tells absent from null, and exits non-zero on a failed read');
    },

    "the check's memory": () => {
      if (!existsSync('/proc/self/status')) {
        console.log("skip: the check's memory case reads its peak from /proc/self/status");
        return;
      }
      const writeStore = (name, size) => {
        const path = join(scratch, name);
        const store = JsStore.create({ path });
        store.registerState({ id: WHOLE, strategy: 'snapshot' });
        const value = 'x'.repeat(size);
        for (let n = 0; n < 3000; n++) store.setStateJson(WHOLE, { n, value });
        store.close();
        return path;
      };
      // VmHWM, since getrusage's maxrss carries over from the parent across exec.
      const peak = (path) => {
        const run = spawnSync(process.execPath, ['--input-type=module', '-e', `
process.argv = [process.argv[0], 'state-digests.mjs', ${JSON.stringify(path)}];
await import(${JSON.stringify(pathToFileURL(join(tools, 'state-digests.mjs')).href)});
const { readFileSync } = await import('node:fs');
console.error('peak ' + readFileSync('/proc/self/status', 'utf8').match(/VmHWM:\\s+(\\d+)/)[1]);`], { encoding: 'utf8', env: { ...process.env, CHRONICLE_MODULE: MODULE } });
        assert.equal(run.status, 0, `state-digests.mjs on ${path} exited ${run.status}:\n${run.stderr}`);
        return Number(run.stderr.match(/peak (\d+)/)[1]) * 1024;
      };
      const large = writeStore('memory-large', 100 << 10);
      const log = statSync(join(large, 'records.log')).size;
      const grown = peak(large) - peak(writeStore('memory-small', 16));
      assert.ok(grown < log / 2, `state-digests.mjs should hold one read at a time: on a log of ${Math.round(log / 2 ** 20)} MB its peak memory grew ${Math.round(grown / 2 ** 20)} MB over a small store's`);
      rmSync(large, { recursive: true });
      console.log(`ok: state-digests.mjs on a log of ${Math.round(log / 2 ** 20)} MB peaks ${Math.round(grown / 2 ** 20)} MB above its peak on a small store`);
    },

    'branches': () => {
      const { path } = branchedStore('branches');
      const kinds = new Set(updates(path).filter(({ state }) => state === 'messages').map(({ kind }) => kind));
      assert.ok(kinds.has('Snapshot') && kinds.has('DeltaSnapshot'), `the premise: messages holds both kinds of snapshot, not ${[...kinds]}`);
      const unchanged = filesOf(path);
      const dry = compact(path, '--state', WHOLE);
      assert.equal(dry.status, 0, `the dry run exited ${dry.status}:\n${dry.out}${dry.err}`);
      assert.ok(dry.out.includes('Dry run: nothing changed.'), `the dry run should say that nothing changed:\n${dry.out}`);
      assert.deepEqual(filesOf(path), unchanged, 'the dry run changed a file');
      const before = digest(path, 'the store with branches before the tool');
      const log = readFileSync(join(path, 'records.log'));
      const index = readFileSync(join(path, 'state.bin'));
      const kept = keptByRule(path, [WHOLE]);
      const run = compact(path, '--state', WHOLE, '--apply');
      assert.equal(run.status, 0, `compact-set-states.py on the store with branches exited ${run.status}:\n${run.out}${run.err}`);
      assert.ok(statSync(join(path, 'records.log')).size < log.length, 'the tool should make the log of the store with branches smaller');
      assert.ok(readFileSync(join(path, 'records.log.pre-compact')).equals(log), 'records.log.pre-compact should be the old log');
      assert.ok(readFileSync(join(path, 'state.bin.pre-compact')).equals(index), 'state.bin.pre-compact should be the old state.bin');
      assert.ok(existsSync(join(path, 'state-indexes.bin.pre-compact')) && !existsSync(join(path, 'state-indexes.bin')), 'state-indexes.bin should be moved to its .pre-compact name');
      assert.equal(digest(path, 'the store with branches after the tool'), before, 'state-digests.mjs should read the store with branches the same after the tool');
      assert.deepEqual(updates(path).filter(({ state }) => state === WHOLE).map(({ id }) => id).sort(), kept,
        `the updates of ${WHOLE} that stay should be those its heads name`);
      console.log(`ok: compact-set-states.py applies to a store with branches, keeps the ${kept.length} sets its heads name, and state-digests.mjs reads it the same`);
    },

    'a named list state and a named tree state': () => {
      const { path } = branchedStore('lists');
      const globs = ['mess*', 'fil?s'];
      const kept = { messages: keptByRule(path, ['messages']), files: keptByRule(path, ['files']) };
      for (const state of ['messages', 'files']) {
        assert.ok(kept[state].length < updates(path).filter((update) => update.state === state).length, `the premise: the tool has updates of ${state} to drop`);
      }
      applies(path, 'a store whose list state and tree state are named', globs);
      for (const state of ['messages', 'files']) {
        assert.deepEqual(updates(path).filter((update) => update.state === state).map(({ id }) => id).sort(), kept[state],
          `the updates of ${state} that stay should be those chronicle's read reaches from its heads`);
      }
      console.log('ok: compact-set-states.py on a list state and a tree state keeps exactly the updates that their reads reach');
    },

    'what counts as a state update': () => {
      const path = join(scratch, 'decode');
      const store = JsStore.create({ path });
      store.registerState({ id: WHOLE, strategy: 'snapshot' });
      for (let n = 0; n < 5; n++) store.setStateJson(WHOLE, { n, padding: 'x'.repeat(200) });
      const sequence = store.currentSequence();
      store.close();
      const head = updates(path).at(-1);
      const partial = msgpack(new Map([['state_id', WHOLE], ['prev_update_offset', head.record],
        ['operation', new Map([['Set', Buffer.from('{"partial":true}')]])]]));
      appendRecord(path, head, sequence + 1, partial);
      const later = JsStore.open({ path });
      assert.equal(later.recovery()?.undecodableStateUpdates, 1, 'the premise: chronicle reads a record with no record_id as an ordinary record');
      for (let n = 5; n < 7; n++) later.setStateJson(WHOLE, { n });
      later.close();
      applies(path, 'a store with a record typed state_update that chronicle does not decode');
      assert.ok(readFileSync(join(path, 'records.log')).includes(partial), 'the record that chronicle does not decode as a state update should stay');

      const positional = join(scratch, 'positional');
      const writer = JsStore.create({ path: positional });
      writer.registerState({ id: WHOLE, strategy: 'snapshot' });
      for (let n = 0; n < 5; n++) writer.setStateJson(WHOLE, { n, padding: 'x'.repeat(200) });
      const top = writer.currentSequence();
      writer.close();
      const last = updates(positional).at(-1);
      appendRecord(positional, last, top + 1, msgpack([1 << 30, top + 1, WHOLE, last.record,
        new Map([['Set', Buffer.from('{"positional":true}')]]), Date.now() * 1000]));
      const replayed = JsStore.open({ path: positional });
      assert.deepEqual(replayed.getStateJson(WHOLE), { positional: true }, 'the premise: chronicle reads the positional form');
      replayed.close();
      refuses(positional, 'a positional update that needs a new link', 'positional MessagePack form and needs a new link');
    },

    'life after the cut': () => {
      const { path, past } = branchedStore('life');
      const before = JsStore.open({ path });
      before.switchBranch('main');
      const pastMessages = before.getStateJsonAt('messages', past);
      const pastWhole = before.getStateJsonAt(WHOLE, past);
      before.switchBranch('deep');
      before.close();
      assert.notEqual(pastWhole, null, `the premise: main holds ${WHOLE} at sequence ${past} before the tool`);
      const twin = join(scratch, 'life-twin');
      cpSync(path, twin, { recursive: true });
      const run = compact(path, '--state', WHOLE, '--apply');
      assert.equal(run.status, 0, `compact-set-states.py exited ${run.status}:\n${run.out}${run.err}`);
      assert.equal(digest(path, 'the store after the tool'), digest(twin, 'its twin that the tool never ran on'),
        'the store after the tool should read as its twin does');
      const cut = updates(path).length;
      for (const store of [path, twin]) {
        const later = JsStore.open({ path: store });
        for (const branch of ['main', 'side', 'deep', 'idle']) {
          later.switchBranch(branch);
          for (let n = 0; n < 7; n++) later.appendToStateJson('messages', { after: n, branch });
          later.setStateJson(WHOLE, { after: true, branch });
        }
        later.switchBranch('deep');
        later.close();
      }
      const kinds = new Set(updates(path).slice(cut).filter(({ state }) => state === 'messages').map(({ kind }) => kind));
      assert.ok(kinds.has('Snapshot') && kinds.has('DeltaSnapshot'), `the premise: the writes after the tool take both kinds of snapshot, not ${[...kinds]}`);
      assert.equal(digest(path, 'the store written after the tool'), digest(twin, 'its twin that the tool never ran on'),
        'the store written after the tool should read as its twin does');
      const branched = JsStore.open({ path });
      branched.createBranchAt('past', 'main', past);
      branched.switchBranch('past');
      assert.equal(branched.getStateJson(WHOLE), null, `a branch made at sequence ${past}, before the cut, should not hold ${WHOLE}`);
      assert.deepEqual(branched.getStateJson('messages'), pastMessages, 'its messages should be what main read at that sequence before the tool');
      branched.switchBranch('deep');
      branched.close();
      console.log('ok: after the tool the store reads as a twin the tool never ran on; then each branch takes appends, snapshots and a set, and reads after a reopen as the twin does; a branch made before the cut holds the list state and not the cut one');
    },

    'the refusals': () => {
      const { path: base } = branchedStore('refusals');
      const copyOf = (name) => {
        const to = join(scratch, name);
        cpSync(base, to, { recursive: true });
        return to;
      };
      const open = copyOf('refuse-open');
      const held = JsStore.open({ path: open });
      try {
        refuses(open, 'an open store', 'the store is open');
      } finally {
        held.close();
      }
      const stale = copyOf('refuse-stale');
      const saved = readFileSync(join(stale, 'state.bin'));
      const writer = JsStore.open({ path: stale });
      writer.setStateJson(WHOLE, { late: true });
      writer.close();
      writeFileSync(join(stale, 'state.bin'), saved);
      refuses(stale, 'a state.bin older than its log', 'older than records.log');
      for (const name of ['records.log.compact', 'state.bin.compact', 'state.bin.blocked', 'records.log.pre-compact', 'state.bin.pre-compact', 'state-indexes.bin.pre-compact']) {
        const earlier = copyOf(`refuse-${name}`);
        writeFileSync(join(earlier, name), 'left');
        refuses(earlier, `a store with ${name}`, name);
      }
      const blocked = copyOf('refuse-blocked');
      writeFileSync(join(blocked, 'state.bin'), blockedStateBin(blocked));
      refuses(blocked, 'a store whose state.bin is blocked', '--go-back');
      const moved = updates(base).filter(({ state }) => state === 'messages').at(-1);
      const corrupt = copyWithPayload(base, 'refuse-checksum', moved, (payload) => {
        const at = payload.indexOf(Buffer.from('deep'));
        payload[at] = 'D'.charCodeAt(0);
        return payload;
      }, { checksum: 'kept' });
      refuses(corrupt, 'a record that fails its checksum', `the record at offset ${moved.record} of records.log fails its checksum`);
      const short = copyOf('refuse-short-index');
      writeFileSync(join(short, 'state.bin'), Buffer.from('STI\0'));
      refuses(short, 'a state.bin cut short', 'state.bin is not a whole state index');
      const nowhere = copyOf('refuse-head');
      withHeads(nowhere, `if key[1] == '${WHOLE}': head[0] += 1`);
      refuses(nowhere, 'a head that names no state update', 'no state update of whole is there');
      const snapshot = copyOf('refuse-snapshot');
      withHeads(snapshot, "if key[1] == 'messages': head[3] = 1");
      refuses(snapshot, 'a snapshot offset that names no record', 'as a snapshot of messages');
      const unlinked = copyWithPayload(base, 'refuse-link', moved, (payload) => withLink(payload, 1));
      refuses(unlinked, 'a link that names no record', `at offset ${moved.record} links to offset 1, where no earlier record is`);
      const reached = new Set(keptByRule(base, ['messages']));
      const walked = updates(base).find((update) => update.state === 'messages' && reached.has(update.id)
        && !['Set', 'Snapshot'].includes(update.kind) && update.prev !== null);
      assert.ok(walked, 'the premise: the read of messages walks a link');
      const crossed = copyWithPayload(base, 'refuse-crossed', walked,
        (payload) => withLink(payload, updates(base).find(({ state }) => state === WHOLE).record));
      refuses(crossed, 'a link to an update of another state', 'where no state update of messages is', ['messages']);
      const linkedLock = copyOf('refuse-lock-link');
      rmSync(join(linkedLock, 'LOCK'));
      writeFileSync(join(scratch, 'elsewhere-LOCK'), '');
      symlinkSync(join(scratch, 'elsewhere-LOCK'), join(linkedLock, 'LOCK'));
      refuses(linkedLock, 'a LOCK that is a symbolic link', 'LOCK is a symbolic link');
      refuses(copyOf('refuse-unnamed'), 'patterns that name no state', 'that --state nothing-* names', ['nothing-*']);
      const empty = join(scratch, 'refuse-no-log');
      mkdirSync(empty);
      refuses(empty, 'a directory with no records.log', 'records.log is missing');

      const json = fixture(join(scratch, 'refuse-json'));
      const before = JsStore.open({ path: json });
      before.registerState({ id: WHOLE, strategy: 'snapshot' });
      before.setStateJson(WHOLE, { n: 0 });
      before.appendToStateJson('messages', { n: 50, text: 'message 50' });
      before.setStateJson(WHOLE, { n: 1 });
      const sequence = before.currentSequence();
      before.close();
      const held2 = updates(json).filter(({ state }) => state === 'messages');
      const appended = appendRecord(json, held2[0], sequence + 1, Buffer.from(JSON.stringify({
        record_id: 0, global_sequence: sequence + 1, state_id: 'messages', prev_update_offset: held2.at(-1).record,
        operation: { Append: [...Buffer.from(JSON.stringify({ n: 51, text: 'message 51' }))] }, timestamp: Date.now() * 1000,
      })));
      rmSync(join(json, 'state.bin'));
      const replayed = JsStore.open({ path: json });
      replayed.registerState({ id: 'messages', strategy: 'append_log' });
      assert.equal(replayed.getStateLen('messages'), 52, 'the premise: this build replays the appended JSON update');
      replayed.close();
      refuses(json, 'a JSON update that needs a new link', `offset ${appended} is in the JSON form and needs a new link`);
    },

    'the old form': () => {
      const old = fixture(join(scratch, 'old-form'));
      const writer = JsStore.open({ path: old });
      writer.registerState({ id: WHOLE, strategy: 'snapshot' });
      for (let n = 0; n < 5; n++) writer.setStateJson(WHOLE, { n, padding: 'x'.repeat(200) });
      writer.close();
      applies(old, 'the store that 0.2.1 began');
      const legacy = branchedStore('legacy-heads').path;
      withHeads(legacy, 'del head[5:]');
      const before = digestOfCopy(legacy, 'a store whose heads have five fields');
      const run = compact(legacy, '--state', WHOLE, '--apply');
      assert.equal(run.status, 0, `compact-set-states.py on a store whose heads have five fields exited ${run.status}:\n${run.out}${run.err}`);
      assert.equal(digest(legacy, 'a store whose heads had five fields, after the tool'), before, 'a store whose heads have five fields should read the same after the tool');
      console.log('ok: compact-set-states.py applies to a store whose heads have the five fields of earlier releases, and it reads the same');
    },

    'a run stopped after each of its steps': () => {
      const { path: base } = branchedStore('stops');
      const original = filesOf(base);
      const before = digestOfCopy(base, 'the store before a run');
      const harness = (store, stopAfter, how) => spawnSync(PYTHON, ['-c', `
import importlib.util, os, sys
spec = importlib.util.spec_from_file_location('compact', 'compact-set-states.py')
tool = importlib.util.module_from_spec(spec); spec.loader.exec_module(tool)
store, stop_after, how = sys.argv[1], int(sys.argv[2]), sys.argv[3]
seen = [0]
def counted(act):
    def step(*args, **kwargs):
        result = act(*args, **kwargs)
        seen[0] += 1
        if seen[0] == stop_after:
            if how == 'kill': os._exit(9)
            raise OSError(5, 'the test stops the run here')
        return result
    return step
tool.write_file = counted(tool.write_file); tool.sync_dir = counted(tool.sync_dir)
os.link = counted(os.link); os.replace = counted(os.replace)
code = tool.main(['compact-set-states.py', store, '--state', '${WHOLE}', '--apply'])
print('steps %d' % seen[0])
sys.exit(code)`, store, String(stopAfter), how], { encoding: 'utf8', cwd: tools });
      cpSync(base, join(scratch, 'stops-count'), { recursive: true });
      const counted = harness(join(scratch, 'stops-count'), 0, 'kill');
      assert.equal(counted.status, 0, `the run with no stop exited ${counted.status}:\n${counted.stdout}${counted.stderr}`);
      const steps = Number(counted.stdout.match(/steps (\d+)/)[1]);
      const blocked = (store) => readFileSync(join(store, 'state.bin')).subarray(0, 9).equals(Buffer.from('BLOCKED: '));
      const live = (files) => Object.fromEntries(['records.log', 'state.bin', 'state-indexes.bin'].map((name) => [name, files[name]]));
      let beforeTheBlock = 0;
      for (let k = 1; k <= steps; k++) {
        const store = join(scratch, `stop-${k}`);
        cpSync(base, store, { recursive: true });
        const stopped = harness(store, k, 'kill');
        assert.equal(stopped.status, 9, `the run stopped after step ${k} exited ${stopped.status}:\n${stopped.stdout}${stopped.stderr}`);
        const left = filesOf(store);
        const staging = ['records.log.compact', 'state.bin.compact', 'state.bin.blocked'].filter((name) => name in left);
        const finished = !blocked(store) && staging.length === 0;
        if (blocked(store)) {
          for (const open of [JsStore.open, JsStore.openOrCreate]) {
            const copy = join(scratch, `stop-${k}-open`);
            cpSync(store, copy, { recursive: true });
            assert.throws(() => open({ path: copy }), /Invalid state index magic/, `after step ${k}, open and openOrCreate should refuse the store`);
            rmSync(copy, { recursive: true });
          }
        } else {
          if (!finished) {
            assert.deepEqual(live(left), live(original), `after step ${k}, before the blocked file, the store's own files should be as they were`);
            beforeTheBlock = k;
          }
          assert.equal(digestOfCopy(store, `the store after step ${k}`), before, `after step ${k}, the store should read as before`);
        }
        refuses(store, `the store after step ${k}`, finished ? 'finished' : '--go-back');
        const back = compact(store, '--go-back');
        if (finished) {
          assert.equal(back.status, 1, `--go-back after a finished run exited ${back.status}:\n${back.out}${back.err}`);
          assert.deepEqual(filesOf(store), left, '--go-back after a finished run should change no file');
        } else {
          assert.equal(back.status, 0, `--go-back after step ${k} exited ${back.status}:\n${back.out}${back.err}`);
          assert.deepEqual(filesOf(store), original, `--go-back after step ${k} should leave every file as before the run, and none of the run`);
        }
      }
      assert.ok(beforeTheBlock > 0, 'the premise: a stop leaves the store before the blocked file');
      const written = join(scratch, 'stop-then-write');
      cpSync(base, written, { recursive: true });
      assert.equal(harness(written, beforeTheBlock, 'kill').status, 9, 'the run stopped before the blocked file should be killed');
      const host = JsStore.open({ path: written });
      host.setStateJson(WHOLE, { late: true });
      host.close();
      const savedByHost = readFileSync(join(written, 'state.bin'));
      const back = compact(written, '--go-back');
      assert.equal(back.status, 0, `--go-back after a stop and a write exited ${back.status}:\n${back.out}${back.err}`);
      assert.ok(readFileSync(join(written, 'state.bin')).equals(savedByHost), '--go-back after a stop before the blocked file should replace no live file');
      const reader = JsStore.open({ path: written });
      assert.deepEqual(reader.getStateJson(WHOLE), { late: true }, '--go-back after a stop before the blocked file should keep a write made since');
      reader.close();
      for (const [k, status, says] of [[1, 1, 'Nothing in the store changed.'], [beforeTheBlock + 2, 3, '--go-back']]) {
        const store = join(scratch, `raise-${k}`);
        cpSync(base, store, { recursive: true });
        const run = harness(store, k, 'raise');
        assert.equal(run.status, status, `a run that fails after step ${k} exited ${run.status}:\n${run.stdout}${run.stderr}`);
        assert.ok(run.stdout.includes(says), `a run that fails after step ${k} should say "${says}":\n${run.stdout}`);
        if (status === 1) assert.deepEqual(filesOf(store), original, 'a run that fails before its swap should leave every file as it was');
      }
      console.log(`ok: a run stopped after each of its ${steps} steps leaves a store that open refuses or that reads as before, the next run refuses it, and --go-back restores it, keeps a write made since a stop before the swap, and changes nothing after a finished run`);
    },

    'a linked log and a linked state.bin': () => {
      const { path } = branchedStore('linked');
      const elsewhere = { 'records.log': join(scratch, 'volume-a'), 'state.bin': join(scratch, 'volume-b') };
      for (const [name, dir] of Object.entries(elsewhere)) {
        mkdirSync(dir);
        renameSync(join(path, name), join(dir, name));
        symlinkSync(join(dir, name), join(path, name));
      }
      applies(path, 'a store whose log and state.bin are links');
      for (const [name, dir] of Object.entries(elsewhere)) {
        assert.ok(lstatSync(join(path, name)).isSymbolicLink(), `${name} should still be a link`);
        assert.ok(existsSync(join(dir, `${name}.pre-compact`)), `${name}.pre-compact should be beside the file the link leads to`);
        assert.ok(!existsSync(join(path, `${name}.pre-compact`)), `${name}.pre-compact should not be in the store's directory`);
      }
      console.log('ok: compact-set-states.py applies to a store whose log and state.bin are links to other directories, and leaves the links');
    },

    'after the newest head': () => {
      const goneLast = branchedStore('gone-last', { goneLast: true });
      const newest = Math.max(...savedHeads(goneLast.path).map(({ offset }) => offset));
      const goneWrites = updates(goneLast.path).filter(({ branch }) => String(branch) === goneLast.gone);
      assert.ok(goneWrites.length && goneWrites.at(-1).record > newest,
        `the premise: the deleted branch's last update lies after the newest head, ${newest}`);
      const tail = (store, after) => updates(store).filter(({ state, record }) => state === WHOLE && record > after).map(({ id }) => id);
      const goneTail = tail(goneLast.path, newest);
      applies(goneLast.path, 'a store whose newest state updates are a deleted branch\'s');
      const stayed = new Set(updates(goneLast.path).map(({ id }) => id));
      assert.deepEqual(goneTail.filter((id) => !stayed.has(id)), [], 'each update of the named state after the newest head should stay');
      const ahead = join(scratch, 'ahead');
      const store = JsStore.create({ path: ahead });
      for (const registration of COMPACT_REGISTRATIONS) store.registerState(registration);
      for (let n = 0; n < 5; n++) store.setStateJson(WHOLE, { n, padding: 'x'.repeat(200) });
      store.createEmptyBranch('fresh', 'main');
      store.sync();
      store.close();
      const saved = readFileSync(join(ahead, 'state.bin'));
      const writer = JsStore.open({ path: ahead });
      writer.switchBranch('fresh');
      for (let n = 0; n < 3; n++) writer.setStateJson(WHOLE, { n, fresh: true });
      writer.switchBranch('main');
      writer.close();
      writeFileSync(join(ahead, 'state.bin'), saved);
      const aheadTail = tail(ahead, Math.max(...savedHeads(ahead).map(({ offset }) => offset)));
      assert.equal(aheadTail.length, 3, 'the premise: the three sets of branch fresh lie after the newest saved head');
      const before = digestOfCopy(ahead, 'a copy of the store whose log is ahead of its state.bin');
      assert.ok(before.includes('branch fresh\t'), `the premise: the check reads branch fresh:\n${before}`);
      const run = compact(ahead, '--state', WHOLE, '--apply');
      assert.equal(run.status, 0, `compact-set-states.py on the store whose log is ahead of its state.bin exited ${run.status}:\n${run.out}${run.err}`);
      const kept = new Set(updates(ahead).map(({ id }) => id));
      assert.deepEqual(aheadTail.filter((id) => !kept.has(id)), [], 'each update of the named state after the newest saved head should stay');
      assert.equal(digest(ahead, 'the store whose log was ahead of its state.bin'), before,
        'the store whose log was ahead of its state.bin should read the same after the tool');
      console.log('ok: compact-set-states.py keeps each update after the newest head, on a store whose newest updates are a deleted branch\'s and on one whose log is ahead of its state.bin, and both read the same');
    },

    'LOCK and the mode': () => {
      const { path } = branchedStore('mode');
      rmSync(join(path, 'LOCK'));
      const unchanged = filesOf(path);
      const dry = compact(path, '--state', WHOLE);
      assert.equal(dry.status, 0, `the dry run on a store with no LOCK exited ${dry.status}:\n${dry.out}${dry.err}`);
      assert.deepEqual(filesOf(path), unchanged, 'the dry run on a store with no LOCK should leave no LOCK');
      for (const name of ['records.log', 'state.bin']) chmodSync(join(path, name), 0o2640);
      assert.equal(statSync(join(path, 'state.bin')).mode & 0o7777, 0o2640, 'the premise: the files take setgid');
      const run = compact(path, '--state', WHOLE, '--apply');
      assert.equal(run.status, 0, `compact-set-states.py exited ${run.status}:\n${run.out}${run.err}`);
      assert.ok(!existsSync(join(path, 'LOCK')), 'the tool should delete the LOCK that it made');
      for (const name of ['records.log', 'state.bin']) {
        assert.equal(statSync(join(path, name)).mode & 0o7777, 0o2640, `the new ${name} should have the old one's mode, setgid included`);
      }
      console.log('ok: the tool leaves no LOCK where it found none, and the new records.log and state.bin have the modes of the old ones');
    },
  };
  if (process.platform === 'win32') console.log(`skip: the ${Object.keys(compactCases).length} cases of compact-set-states.py, which runs on Unix only`);
  else for (const [, run] of Object.entries(compactCases)) run();
} finally {
  rmSync(scratch, { recursive: true, force: true });
}
