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

// The tool with each write, sync, link, rename and removal counted, killed after the stopAfter-th (0: never). It
// prints how many it made when it is not killed.
function stoppedTool(store, stopAfter, ...args) {
  const run = spawnSync(PYTHON, ['-c', `
import importlib.util, os, sys
spec = importlib.util.spec_from_file_location('compact', 'compact-set-states.py')
tool = importlib.util.module_from_spec(spec); spec.loader.exec_module(tool)
stop_after, seen = int(sys.argv[1]), [0]
def counted(act):
    def step(*args, **kwargs):
        result = act(*args, **kwargs)
        seen[0] += 1
        if seen[0] == stop_after: os._exit(9)
        return result
    return step
tool.write_file = counted(tool.write_file); tool.sync_dir = counted(tool.sync_dir)
os.link = counted(os.link); os.replace = counted(os.replace); os.remove = counted(os.remove)
code = tool.main(['compact-set-states.py'] + sys.argv[2:])
print('steps %d' % seen[0])
sys.exit(code)`, String(stopAfter), store, ...args], { encoding: 'utf8', cwd: tools });
  return { status: run.status, out: run.stdout, err: run.stderr, steps: Number(run.stdout.match(/steps (\d+)/)?.[1]) };
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

// state.bin with its strategies changed by `edit`, Python over `strategies`, the MessagePack map of each state's.
function withStrategies(store, edit) {
  const sb = join(store, 'state.bin');
  writeFileSync(sb, toolPython(`
index = tool.read_state_bin(open(sys.argv[1], 'rb').read())
strategies = index[1]
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

function appendRecord(store, template, sequence, payload, { encoding } = {}) {
  const path = join(store, 'records.log');
  const log = readFileSync(path);
  const header = Buffer.from(log.subarray(template.record, template.payload - 4));
  header.writeBigUInt64LE(1n << 40n, 6);
  header.writeBigUInt64LE(BigInt(sequence), 14);
  if (encoding !== undefined) header[header.length - 1] = encoding;
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

function payloadOf(store, update) {
  const log = readFileSync(join(store, 'records.log'));
  return Buffer.from(log.subarray(update.payload, update.payload + log.readUInt32LE(update.payload - 4)));
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
      assert.ok(!digest(path, 'the store with branches', ['wh*e']).includes(`\t${WHOLE}\tat eight sequences `), 'state-digests.mjs should leave out the history of a state that a --state pattern names');
      assert.ok(read.includes('current\tdeep\n'), `state-digests.mjs should print the store's current branch:\n${read}`);
      const reopened = JsStore.open({ path });
      assert.equal(reopened.currentBranch().name, 'deep', 'state-digests.mjs should leave the store on the branch it found');
      reopened.switchBranch('main');
      reopened.close();
      assert.ok(digest(path, 'the store with branches, now on main', []).includes('current\tmain\n'), 'the current line should change with the store\'s branch');
      const killed = spawnSync(process.execPath, ['--input-type=module', '-e', `
import { createRequire } from 'node:module';
const { JsStore } = createRequire(import.meta.url)(${JSON.stringify(MODULE)});
const query = JsStore.prototype.query;
let calls = 0;
JsStore.prototype.query = function (...args) {
  if (++calls === 40) process.exit(9);
  return query.apply(this, args);
};
process.argv = [process.argv[0], 'state-digests.mjs', ${JSON.stringify(path)}];
await import(${JSON.stringify(pathToFileURL(join(tools, 'state-digests.mjs')).href)});`], { encoding: 'utf8', env: { ...process.env, CHRONICLE_MODULE: MODULE } });
      assert.equal(killed.status, 9, `the check stopped partway exited ${killed.status}:\n${killed.stderr}`);
      const after = JsStore.open({ path });
      assert.equal(after.currentBranch().name, 'main', 'a check stopped partway should leave the store on the branch it found');
      after.close();
      // What the open reports goes to stderr, so a check after a run doesn't differ from the check before it for that.
      const check = (store, prelude = '') => spawnSync(process.execPath, ['--input-type=module', '-e', `
import { createRequire } from 'node:module';
const { JsStore } = createRequire(import.meta.url)(${JSON.stringify(MODULE)});
${prelude}
process.argv = [process.argv[0], 'state-digests.mjs', ${JSON.stringify(store)}];
await import(${JSON.stringify(pathToFileURL(join(tools, 'state-digests.mjs')).href)});`], { encoding: 'utf8', env: { ...process.env, CHRONICLE_MODULE: MODULE } });
      const torn = join(scratch, 'digest-torn');
      cpSync(path, torn, { recursive: true });
      writeFileSync(join(torn, 'records.log'), Buffer.concat([readFileSync(join(torn, 'records.log')), Buffer.from('REC\0torn')]));
      const [first, second] = [check(torn), check(torn)];
      assert.equal(first.status, 0, `the check of a store with a torn tail exited ${first.status}:\n${first.stderr}`);
      assert.ok(/This open reported: droppedBytes=[1-9]/.test(first.stderr) && !second.stderr.includes('This open reported'),
        `the first check of a store with a torn tail should report the dropped bytes on stderr, and the second shouldn't:\n${first.stderr}\n${second.stderr}`);
      assert.equal(first.stdout, second.stdout, 'the digest should be the same whether or not the open repaired the store');
      const old = check(path, 'JsStore.prototype.recovery = () => ({ truncatedAt: 100, droppedBytes: 8, validRecords: 3 });');
      assert.equal(old.status, 0, `the check with the recovery report of chronicle 0.4.0 exited ${old.status}:\n${old.stderr}`);
      assert.ok(old.stderr.includes('droppedBytes=8 undecodable=- onMissingBranches=- unregistered=-'),
        `the check should print the fields that a 0.4.0 report lacks as -:\n${old.stderr}`);
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
      const { path, gone } = branchedStore('branches');
      const kinds = new Set(updates(path).filter(({ state }) => state === 'messages').map(({ kind }) => kind));
      assert.ok(kinds.has('Snapshot') && kinds.has('DeltaSnapshot'), `the premise: messages holds both kinds of snapshot, not ${[...kinds]}`);
      const cited = updates(path).find(({ state }) => state === WHOLE);
      const writer = JsStore.open({ path });
      writer.appendJsonWithLinks('receipt', { origin: 'job' }, { causedBy: [String(cited.id)] });
      writer.close();
      const leftovers = updates(path).filter(({ branch }) => String(branch) === gone);
      assert.ok(leftovers.filter(({ state }) => state === WHOLE).length === 5 && !savedHeads(path).some(({ branch }) => String(branch) === gone),
        'the premise: the deleted branch left five sets in the log and no head in state.bin');
      const leftoverBytes = new Map(leftovers.map((update) => [update.id, payloadOf(path, update)]));
      const idAt = new Map(updates(path).map(({ record, id }) => [record, id]));
      const beforeById = new Map(updates(path).map((update) => [update.id, update]));
      const unchanged = filesOf(path);
      const dry = compact(path, '--state', WHOLE);
      assert.equal(dry.status, 0, `the dry run exited ${dry.status}:\n${dry.out}${dry.err}`);
      assert.ok(dry.out.includes('Dry run: nothing changed.'), `the dry run should say that nothing changed:\n${dry.out}`);
      assert.ok(dry.out.includes('1 record(s) that stay name a dropped record in caused_by or linked_to'), `the dry run should count the receipt that names a dropped set:\n${dry.out}`);
      assert.ok(new RegExp(`  ${WHOLE}: drop \\d+ old value\\(s\\), [^;]*; 5 update\\(s\\), [^,]*, stay, since no head reaches them`).test(dry.out),
        `the dry run should count the deleted branch's five sets as staying:\n${dry.out}`);
      assert.deepEqual(filesOf(path), unchanged, 'the dry run changed a file');
      const before = digest(path, 'the store with branches before the tool');
      const log = readFileSync(join(path, 'records.log'));
      const index = readFileSync(join(path, 'state.bin'));
      const at = new Map(updates(path).map((update) => [update.record, update]));
      const chains = new Set();
      for (const { state, offset } of savedHeads(path)) {
        if (state !== WHOLE) continue;
        for (let update = at.get(offset); update && !chains.has(update.record); update = at.get(update.prev)) chains.add(update.record);
      }
      const headIds = new Set(savedHeads(path).filter(({ state }) => state === WHOLE).map(({ offset }) => idAt.get(offset)));
      const kept = updates(path).filter(({ state, record, id }) => state === WHOLE && (!chains.has(record) || headIds.has(id))).map(({ id }) => id).sort();
      assert.ok(kept.length === headIds.size + 5, 'the premise: what stays of the state is its heads and the deleted branch\'s five sets');
      const run = compact(path, '--state', WHOLE, '--apply');
      assert.equal(run.status, 0, `compact-set-states.py on the store with branches exited ${run.status}:\n${run.out}${run.err}`);
      assert.ok(statSync(join(path, 'records.log')).size < log.length, 'the tool should make the log of the store with branches smaller');
      assert.ok(readFileSync(join(path, 'records.log.pre-compact')).equals(log), 'records.log.pre-compact should be the old log');
      assert.ok(readFileSync(join(path, 'state.bin.pre-compact')).equals(index), 'state.bin.pre-compact should be the old state.bin');
      assert.ok(existsSync(join(path, 'state-indexes.bin.pre-compact')) && !existsSync(join(path, 'state-indexes.bin')), 'state-indexes.bin should be moved to its .pre-compact name');
      assert.equal(digest(path, 'the store with branches after the tool'), before, 'state-digests.mjs should read the store with branches the same after the tool');
      assert.deepEqual(updates(path).filter(({ state }) => state === WHOLE).map(({ id }) => id).sort(), kept,
        `the updates of ${WHOLE} that stay should be those its heads name and those no head reaches`);
      const after = new Map(updates(path).map((update) => [update.id, update]));
      assert.ok(leftovers.length > 0 && leftovers.some(({ prev }) => prev !== null && after.get(idAt.get(prev))?.record !== prev),
        'the premise: the run moves a record that an update of the deleted branch links to');
      for (const [id, bytes] of leftoverBytes) assert.ok(payloadOf(path, after.get(id)).equals(bytes), `the deleted branch's update ${id} should keep its bytes`);
      // Each head of the named state links to the new offset of the update it linked to, or to nothing where that went.
      const relinked = savedHeads(path).filter(({ state }) => state === WHOLE);
      for (const { offset } of relinked) {
        const head = [...after.values()].find(({ record }) => record === offset);
        const before = beforeById.get(head.id);
        const linked = before.prev === null ? undefined : after.get(idAt.get(before.prev));
        assert.equal(head.prev, linked ? linked.record : null, `the head ${head.id} of ${WHOLE} should link to ${linked ? `the new offset of ${linked.id}` : 'nothing'}`);
      }
      console.log(`ok: compact-set-states.py applies to a store with branches, keeps the ${headIds.size} sets its heads name and the 5 that no head reaches, counts both and a record that names a dropped one, keeps the bytes of a deleted branch's updates, and state-digests.mjs reads it the same`);
    },

    'a named state that is not whole-value': () => {
      const { path } = branchedStore('not-whole');
      refuses(path, 'a named list state', 'messages is not a state whose every update is a whole value', ['messages']);
      refuses(path, 'a named tree state', 'files is not a state whose every update is a whole value', ['files']);
      // A crafted update after the head, and two sets on top of it where chronicle replays and applies it. Where
      // chronicle does not decode it, the sets go on from the old head, so state.bin is written to name it instead.
      const crafted = (name, edit, { asHead = false } = {}) => {
        const store = join(scratch, name);
        const writer = JsStore.create({ path: store });
        writer.registerState({ id: WHOLE, strategy: 'snapshot' });
        for (let n = 0; n < 3; n++) writer.setStateJson(WHOLE, { n, padding: 'x'.repeat(200) });
        const sequence = writer.currentSequence();
        writer.close();
        const head = updates(store).at(-1);
        const fields = [['record_id', 7], ['global_sequence', sequence + 1], ['state_id', WHOLE], ['prev_update_offset', head.record],
          ['operation', new Map([['Set', Buffer.from('{"crafted":true}')]])], ['timestamp', Date.now() * 1000]];
        const offset = appendRecord(store, head, sequence + 1, edit(fields));
        if (asHead) {
          withHeads(store, `if key[1] == ${JSON.stringify(WHOLE)}: head[:] = [${offset} if v == ${head.record} else v for v in head]`);
          return store;
        }
        const later = JsStore.open({ path: store });
        for (let n = 3; n < 5; n++) later.setStateJson(WHOLE, { n });
        later.close();
        assert.equal(updates(store).find(({ record }) => record > offset)?.prev, offset, `the premise: chronicle applies the crafted update in ${name}`);
        return store;
      };
      const asString = crafted('not-whole-string', (fields) => msgpack(new Map(fields.map(([key, value]) => [key, key === 'operation' ? new Map([['Set', '{"crafted":true}']]) : value]))));
      refuses(asString, 'a set whose value is not bytes', `${WHOLE} is not a state whose every update is a whole value`);
      const twice = crafted('not-whole-twice', (fields) => {
        const map = msgpack(new Map(fields));
        return Buffer.concat([Buffer.from([map[0] + 1]), map.subarray(1), msgpack('timestamp'), msgpack(1)]);
      }, { asHead: true });
      refuses(twice, 'a set with a field two times', `${WHOLE} is not a state whose every update is a whole value`);
    },

    'what a run may change': () => {
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
      writer.registerState({ id: 'other', strategy: 'snapshot' });
      for (let n = 0; n < 5; n++) writer.setStateJson(WHOLE, { n, padding: 'x'.repeat(200) });
      writer.setStateJson('other', { first: true });
      const top = writer.currentSequence();
      writer.close();
      const other = updates(positional).at(-1);
      appendRecord(positional, other, top + 1, msgpack([1 << 30, top + 1, 'other', other.record,
        new Map([['Set', Buffer.from('{"positional":true}')]]), Date.now() * 1000]));
      const replayed = JsStore.open({ path: positional });
      assert.deepEqual(replayed.getStateJson('other'), { positional: true }, 'the premise: chronicle reads the positional form');
      replayed.setStateJson(WHOLE, { n: 5 });
      replayed.close();
      refuses(positional, 'a positional update that needs a new link', 'in the positional MessagePack form, which this tool does not rewrite');

      const tailBase = join(scratch, 'tail-base');
      const base = JsStore.create({ path: tailBase });
      base.registerState({ id: WHOLE, strategy: 'snapshot' });
      for (let n = 0; n < 4; n++) base.setStateJson(WHOLE, { n, padding: 'x'.repeat(200) });
      const last = base.currentSequence();
      base.close();
      const tailHead = updates(tailBase).at(-1);
      const update = (operation) => new Map([['record_id', 9], ['global_sequence', last + 1], ['state_id', WHOLE],
        ['prev_update_offset', tailHead.record], ['operation', operation], ['timestamp', Date.now() * 1000]]);
      const cases = [
        ['no state_id or operation', msgpack(new Map([['hello', 1]])), 1, 'copied'],
        ['a state_id and no operation', msgpack(new Map([['state_id', WHOLE], ['hello', 1]])), 1, 'copied'],
        ['no record_id', msgpack(new Map([...update(new Map([['Set', Buffer.from('{}')]]))].filter(([key]) => key !== 'record_id'))), 1, 'refused'],
        ['a payload that does not parse', Buffer.from([0xc1, 0x00]), 1, 'refused'],
        ['a map with a key that is no string', msgpack(new Map([[[1], 'x'], ['state_id', WHOLE]])), 1, 'refused'],
        ['a set in MessagePack', msgpack(update(new Map([['Set', Buffer.from('{"tail":1}')]]))), 1, 'refused'],
        ['a set in JSON', Buffer.from(JSON.stringify({ record_id: 9, global_sequence: last + 1, state_id: WHOLE, prev_update_offset: tailHead.record,
          operation: { Set: [...Buffer.from('{"tail":2}')] }, timestamp: Date.now() * 1000 })), 2, 'refused'],
      ];
      for (const [label, payload, encoding, expected] of cases) {
        const verdict = join(scratch, `tail-verdict-${cases.findIndex(([name]) => name === label)}`);
        cpSync(tailBase, verdict, { recursive: true });
        appendRecord(verdict, tailHead, last + 1, payload, { encoding });
        const reader = JsStore.open({ path: verdict });
        const replays = !reader.recovery()?.undecodableStateUpdates;
        reader.close();
        const store = join(scratch, `tail-run-${cases.findIndex(([name]) => name === label)}`);
        cpSync(tailBase, store, { recursive: true });
        appendRecord(store, tailHead, last + 1, payload, { encoding });
        if (expected === 'copied') {
          assert.ok(!replays, `the premise: chronicle reads ${label} as an ordinary record`);
          const run = compact(store, '--state', WHOLE, '--apply');
          assert.equal(run.status, 0, `the tool on a store whose tail holds ${label} exited ${run.status}:\n${run.out}${run.err}`);
          assert.ok(readFileSync(join(store, 'records.log')).includes(payload), `${label} should keep its bytes`);
        } else {
          refuses(store, `a store whose tail on a live branch holds ${label}`, 'lies after the newest head');
        }
        console.log(`ok: ${label} after the newest head on a live branch is ${expected}, and chronicle ${replays ? 'replays' : 'does not replay'} it`);
      }
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
      refuses(unlinked, 'a link that names no record', `at offset ${moved.record} links to offset 1, where no state update of messages is`);
      const reached = new Set(keptByRule(base, ['messages']));
      const walked = updates(base).find((update) => update.state === 'messages' && reached.has(update.id)
        && !['Set', 'Snapshot'].includes(update.kind) && update.prev !== null);
      assert.ok(walked, 'the premise: the read of messages walks a link');
      const crossed = copyWithPayload(base, 'refuse-crossed', walked,
        (payload) => withLink(payload, updates(base).find(({ state }) => state === WHOLE).record));
      refuses(crossed, 'a link to an update of another state', 'where no state update of messages is');
      if (process.getuid?.() !== 0) {
        const readOnly = copyOf('refuse-lock-read-only');
        chmodSync(join(readOnly, 'LOCK'), 0o444);
        refuses(readOnly, 'a store whose LOCK this user can\'t write, as chronicle can\'t either', 'could not open LOCK');
      }
      const unlocked = copyOf('refuse-no-lock');
      rmSync(join(unlocked, 'LOCK'));
      refuses(unlocked, 'a store with no LOCK', 'LOCK is missing');
      const version3 = copyOf('refuse-manifest-3');
      writeFileSync(join(version3, 'MANIFEST'), Buffer.from('RST\0\x03', 'latin1'));
      refuses(version3, 'a MANIFEST of version 3', 'names store version 3');
      const feature = copyOf('refuse-manifest-feature');
      const features = Buffer.alloc(13);
      features.write('RST\0', 0, 'latin1');
      features[4] = 2;
      features.writeBigUInt64LE(3n, 5);
      writeFileSync(join(feature, 'MANIFEST'), features);
      refuses(feature, 'a MANIFEST that names a feature this tool does not know', 'names format features 0x3');
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
      refuses(json, 'a JSON update that needs a new link', `offset ${appended} needs a new link, and it is in the JSON form`);
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
        assert.equal(back.status, 0, `--go-back after step ${k} exited ${back.status}:\n${back.out}${back.err}`);
        assert.deepEqual(filesOf(store), original, `--go-back after step ${k} should leave every file as before the run, and none of the run`);
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
      console.log(`ok: a run stopped after each of its ${steps} steps leaves a store that open refuses or that reads as before, the next run refuses it, and --go-back restores it, and keeps a write made since a stop before the swap`);
    },

    'going back after a finished run': () => {
      const { path: base } = branchedStore('finished');
      const original = filesOf(base);
      const trio = (files) => Object.fromEntries(['records.log', 'state.bin', 'state-indexes.bin'].map((name) => [name, files[name]]));
      const before = digestOfCopy(base, 'the store before a finished run');
      const finished = (name, since) => {
        const store = join(scratch, name);
        cpSync(base, store, { recursive: true });
        const run = compact(store, '--state', WHOLE, '--apply');
        assert.equal(run.status, 0, `compact-set-states.py on ${name} exited ${run.status}:\n${run.out}${run.err}`);
        since(store);
        return store;
      };
      const halfway = finished('finished-halfway', () => {});
      const stopped = spawnSync(PYTHON, ['-c', `
import importlib.util, os, sys
spec = importlib.util.spec_from_file_location('compact', 'compact-set-states.py')
tool = importlib.util.module_from_spec(spec); spec.loader.exec_module(tool)
replace = os.replace
def once(*args):
    replace(*args)
    os._exit(9)
os.replace = once
sys.exit(tool.main(['compact-set-states.py', sys.argv[1], '--go-back']))`, halfway], { encoding: 'utf8', cwd: tools });
      assert.equal(stopped.status, 9, `--go-back stopped after its first rename exited ${stopped.status}:\n${stopped.stdout}${stopped.stderr}`);
      assert.ok(readFileSync(join(halfway, 'state.bin')).subarray(0, 9).equals(Buffer.from('BLOCKED: ')), '--go-back after a finished run should block state.bin before it changes anything else');
      assert.equal(compact(halfway, '--go-back').status, 0, '--go-back run again after a stop should finish');
      assert.deepEqual(trio(filesOf(halfway)), trio(original), '--go-back run again after a stop should put back the old files');
      const checked = finished('finished-checked', (store) => digest(store, 'the store after a finished run'));
      const back = compact(checked, '--go-back');
      assert.equal(back.status, 0, `--go-back after a finished run and a check exited ${back.status}:\n${back.out}${back.err}`);
      const left = filesOf(checked);
      assert.deepEqual(trio(left), trio(original), '--go-back after a finished run and a check should put back records.log, state.bin and state-indexes.bin');
      assert.deepEqual(Object.keys(left).sort(), Object.keys(original).sort(), '--go-back after a finished run should leave no file of the run');
      assert.equal(digestOfCopy(checked, 'the store after going back'), before, 'the store should read as before the run after going back');
      for (const [name, change, says] of [
        ['finished-registered', (writer) => writer.registerState({ id: 'later', strategy: 'snapshot' }), 'state.bin has changed'],
        ['finished-written', (writer) => writer.setStateJson(WHOLE, { late: true }), 'records.log has changed'],
      ]) {
        const store = finished(name, (at) => {
          const writer = JsStore.open({ path: at });
          change(writer);
          writer.close();
        });
        const unchanged = filesOf(store);
        const refused = compact(store, '--go-back');
        assert.equal(refused.status, 1, `--go-back on ${name} exited ${refused.status}:\n${refused.out}${refused.err}`);
        assert.ok(refused.out.includes(says), `--go-back on ${name} should say "${says}":\n${refused.out}`);
        assert.deepEqual(filesOf(store), unchanged, `--go-back on ${name} should change no file`);
      }
      const branched = finished('finished-branched', (at) => {
        const writer = JsStore.open({ path: at });
        writer.createEmptyBranch('later', 'main');
        writer.switchBranch('later');
        writer.close();
      });
      const branchedBack = compact(branched, '--go-back');
      assert.equal(branchedBack.status, 0, `--go-back after an empty branch made since the run exited ${branchedBack.status}:\n${branchedBack.out}${branchedBack.err}`);
      const reader = JsStore.open({ path: branched });
      assert.equal(reader.currentBranch().name, 'later', 'an empty branch made since the run should stay selected after --go-back');
      reader.close();
      // A Struct strategy's fields are a Rust HashMap, which a save writes in any order. The binding registers no
      // Struct, so the store gets one by hand, and a reorder stands for the save.
      const struct = "('form', tool.MpMap([('Struct', tool.MpMap([('fields', tool.MpMap([('a', 'Snapshot'), ('b', 'Snapshot'), ('c', 'Snapshot')]))]))]))";
      const fields = "dict(strategies)['form'][0][1][0][1]";
      for (const [name, edit, status, says] of [
        ['finished-reordered', `${fields}.reverse()`, 0, 'went back after a finished run'],
        ['finished-restrategized', `${fields}[1] = ('b', tool.MpMap([('Delta', tool.MpMap([('snapshot_every', 5)]))]))`, 1, 'state.bin has changed'],
      ]) {
        const store = join(scratch, name);
        cpSync(base, store, { recursive: true });
        withStrategies(store, `strategies.append(${struct})`);
        const formed = filesOf(store);
        assert.equal(compact(store, '--state', WHOLE, '--apply').status, 0, `the run on ${name} should finish`);
        withStrategies(store, edit);
        const unchanged = filesOf(store);
        const back = compact(store, '--go-back');
        assert.equal(back.status, status, `--go-back on ${name} exited ${back.status}:\n${back.out}${back.err}`);
        assert.ok(back.out.includes(says), `--go-back on ${name} should say "${says}":\n${back.out}`);
        assert.deepEqual(status === 0 ? trio(filesOf(store)) : filesOf(store), status === 0 ? trio(formed) : unchanged,
          status === 0 ? `--go-back on ${name} should put back the old files` : `--go-back on ${name} should change no file`);
      }
      // The old index deleted by hand after a finished run: going back goes on, and says that the store has no index.
      const lost = join(scratch, 'finished-index-lost');
      cpSync(base, lost, { recursive: true });
      const lostFiles = filesOf(lost);
      assert.ok(lostFiles['state-indexes.bin'], 'the premise: the store has a state-indexes.bin');
      assert.equal(compact(lost, '--state', WHOLE, '--apply').status, 0, 'the run on a store whose old index goes should finish');
      rmSync(join(lost, 'state-indexes.bin.pre-compact'));
      digest(lost, 'a store whose old index was deleted after its run');
      const lostBack = compact(lost, '--go-back');
      assert.equal(lostBack.status, 0, `--go-back after the old index was deleted exited ${lostBack.status}:\n${lostBack.out}${lostBack.err}`);
      assert.ok(lostBack.out.includes('The old state-indexes.bin was not there, so the store has no field index now'),
        `--go-back should say that the store has no field index now:\n${lostBack.out}`);
      const lostLeft = filesOf(lost);
      assert.ok(!lostLeft['state-indexes.bin'] && lostLeft['records.log'] === lostFiles['records.log'] && lostLeft['state.bin'] === lostFiles['state.bin'],
        '--go-back after the old index was deleted should put back records.log and state.bin, and leave no state-indexes.bin');
      const bare = join(scratch, 'finished-no-index');
      cpSync(base, bare, { recursive: true });
      rmSync(join(bare, 'state-indexes.bin'));
      const bareFiles = filesOf(bare);
      assert.equal(compact(bare, '--state', WHOLE, '--apply').status, 0, 'the run on a store with no state-indexes.bin should finish');
      digest(bare, 'a store with no state-indexes.bin after a run');
      assert.ok(existsSync(join(bare, 'state-indexes.bin')), 'the premise: the check makes a state-indexes.bin');
      assert.equal(compact(bare, '--go-back').status, 0, '--go-back on a store that had no state-indexes.bin should go back');
      const bareBack = filesOf(bare);
      assert.deepEqual(trio(bareBack), trio(bareFiles), '--go-back should put back records.log and state.bin, and remove a state-indexes.bin made since the run, where the store had none');
      assert.deepEqual(Object.keys(bareBack).sort(), Object.keys(bareFiles).sort(), '--go-back on a store that had no state-indexes.bin should leave no file of the run');
      console.log('ok: --go-back after a finished run puts back the old files after a check and after a reorder of a strategy\'s map, says that the store has no field index where its old one was deleted, removes a state-indexes.bin made since where there was none, keeps an empty branch made and selected since, and refuses after a registration, a changed strategy or a write');
    },

    'going back stopped, and old files missing': () => {
      const { path: base } = branchedStore('back-stops');
      const original = filesOf(base);
      const blocked = (store) => readFileSync(join(store, 'state.bin')).subarray(0, 9).equals(Buffer.from('BLOCKED: '));
      const count = (name) => {
        const store = join(scratch, name);
        cpSync(base, store, { recursive: true });
        return stoppedTool(store, 0, '--state', WHOLE, '--apply').steps;
      };
      const runSteps = count('back-stops-count');
      // Where the run is left: finished, stopped in the swap with the new log in place and state.bin blocked, or
      // stopped before the swap with both old files' second names made.
      const left = {
        'the second names made': (name) => {
          const store = join(scratch, name);
          cpSync(base, store, { recursive: true });
          assert.equal(stoppedTool(store, runSteps - 8, '--state', WHOLE, '--apply').status, 9, `the run on ${name} should be killed`);
          assert.ok(!blocked(store) && existsSync(join(store, 'records.log.pre-compact')) && existsSync(join(store, 'state.bin.pre-compact')),
            `the premise: after step ${runSteps - 8} the old files have second names and the swap has not begun`);
          return store;
        },
        finished: (name) => {
          const store = join(scratch, name);
          cpSync(base, store, { recursive: true });
          assert.equal(compact(store, '--state', WHOLE, '--apply').status, 0, `the run on ${name} should finish`);
          return store;
        },
        'the new log in place': (name) => {
          const store = join(scratch, name);
          cpSync(base, store, { recursive: true });
          assert.equal(stoppedTool(store, runSteps - 2, '--state', WHOLE, '--apply').status, 9, `the run on ${name} should be killed`);
          assert.ok(blocked(store) && filesOf(store)['records.log'] !== original['records.log'],
            `the premise: after step ${runSteps - 2} the new log is in place and state.bin is blocked`);
          return store;
        },
      };
      for (const [where, make] of Object.entries(left)) {
        const counted = make(`back-${where.replaceAll(' ', '-')}-count`);
        const back = stoppedTool(counted, 0, '--go-back');
        assert.equal(back.status, 0, `--go-back after ${where} exited ${back.status}:\n${back.out}${back.err}`);
        for (let k = 1; k <= back.steps; k++) {
          const store = make(`back-${where.replaceAll(' ', '-')}-${k}`);
          const stopped = stoppedTool(store, k, '--go-back');
          assert.equal(stopped.status, 9, `--go-back after ${where}, stopped after its step ${k}, exited ${stopped.status}:\n${stopped.out}${stopped.err}`);
          const again = compact(store, '--go-back');
          assert.equal(again.status, 0, `--go-back again after ${where} and a stop after step ${k} exited ${again.status}:\n${again.out}${again.err}`);
          assert.deepEqual(filesOf(store), original, `--go-back again after ${where} and a stop after step ${k} should leave every file as before the run, and none of the run`);
        }
        console.log(`ok: --go-back after ${where}, stopped after each of its ${back.steps} steps and run again, leaves every file as before the run`);
        if (where === 'the second names made') continue;
        for (const gone of ['records.log.pre-compact', 'state.bin.pre-compact']) {
          const store = make(`back-${where.replaceAll(' ', '-')}-without-${gone}`);
          rmSync(join(store, gone));
          const unchanged = filesOf(store);
          const refused = compact(store, '--go-back');
          assert.equal(refused.status, 1, `--go-back after ${where} with ${gone} deleted exited ${refused.status}:\n${refused.out}${refused.err}`);
          assert.ok(refused.out.includes('nothing to go back to'), `--go-back after ${where} with ${gone} deleted should say why:\n${refused.out}`);
          assert.deepEqual(filesOf(store), unchanged, `--go-back after ${where} with ${gone} deleted should change no file`);
          if (where === 'finished') {
            const copy = join(scratch, `back-open-${gone}`);
            cpSync(store, copy, { recursive: true });
            JsStore.open({ path: copy }).close();
          }
        }
        console.log(`ok: --go-back after ${where}, with either old file deleted by hand, refuses and changes no file`);
      }
      // A stop inside the write of the run's record: half its bytes are written, and the run is killed.
      const torn = join(scratch, 'back-torn-record');
      cpSync(base, torn, { recursive: true });
      const tearing = spawnSync(PYTHON, ['-c', `
import importlib.util, os, sys
spec = importlib.util.spec_from_file_location('compact', 'compact-set-states.py')
tool = importlib.util.module_from_spec(spec); spec.loader.exec_module(tool)
write_file = tool.write_file
def tearing(path, chunks, like, made=None):
    if 'compact-run' not in path: return write_file(path, chunks, like, made)
    data = b''.join(chunks)
    write_file(path, [data[:len(data) // 2]], like, made)
    os._exit(9)
tool.write_file = tearing
sys.exit(tool.main(['compact-set-states.py', sys.argv[1], '--state', '${WHOLE}', '--apply']))`, torn], { encoding: 'utf8', cwd: tools });
      assert.equal(tearing.status, 9, `the run killed inside the write of its record exited ${tearing.status}:\n${tearing.stdout}${tearing.stderr}`);
      const tornBack = compact(torn, '--go-back');
      assert.equal(tornBack.status, 0, `--go-back after a stop inside the write of the run's record exited ${tornBack.status}:\n${tornBack.out}${tornBack.err}`);
      assert.deepEqual(filesOf(torn), original, '--go-back after a stop inside the write of the run\'s record should leave every file as before the run, and none of the run');
      console.log('ok: --go-back after a stop inside the write of the run\'s record leaves every file as before the run');
      const finished = left.finished('back-finished-to-copy');
      const copy = join(scratch, 'back-finished-copy');
      cpSync(finished, copy, { recursive: true });
      const unchanged = filesOf(copy);
      const refused = compact(copy, '--go-back');
      assert.equal(refused.status, 1, `--go-back on a copy of a finished store exited ${refused.status}:\n${refused.out}${refused.err}`);
      assert.ok(refused.out.includes('as after a copy of the store'), `--go-back on a copy of a finished store should say why:\n${refused.out}`);
      assert.deepEqual(filesOf(copy), unchanged, '--go-back on a copy of a finished store should change no file');
      console.log('ok: --go-back on a copy of a finished store refuses and changes no file');
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
      assert.ok(existsSync(join(elsewhere['state.bin'], 'state.bin.compact-run')), 'the run\'s record should be beside the file state.bin leads to');
      console.log('ok: compact-set-states.py applies to a store whose log and state.bin are links to other directories, and leaves the links');
    },

    'after the newest head': () => {
      const goneLast = branchedStore('gone-last', { goneLast: true });
      const newest = Math.max(...savedHeads(goneLast.path).map(({ offset }) => offset));
      const goneWrites = updates(goneLast.path).filter(({ branch }) => String(branch) === goneLast.gone);
      assert.ok(goneWrites.length && goneWrites.at(-1).record > newest,
        `the premise: the deleted branch's last update lies after the newest head, ${newest}`);
      const goneBytes = new Map(goneWrites.map((update) => [update.id, payloadOf(goneLast.path, update)]));
      applies(goneLast.path, 'a store whose newest state updates are a deleted branch\'s');
      const after = new Map(updates(goneLast.path).map((update) => [update.id, update]));
      for (const [id, bytes] of goneBytes) assert.ok(payloadOf(goneLast.path, after.get(id)).equals(bytes), `the deleted branch's update ${id} should keep its bytes`);
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
      const aheadNewest = Math.max(...savedHeads(ahead).map(({ offset }) => offset));
      assert.equal(updates(ahead).filter(({ state, record }) => state === WHOLE && record > aheadNewest).length, 3,
        'the premise: the three sets of branch fresh lie after the newest saved head');
      refuses(ahead, 'a store whose log is ahead of its state.bin on a live branch', 'lies after the newest head');
      JsStore.open({ path: ahead }).close();
      applies(ahead, 'the store whose log was ahead of its state.bin, after an open and a close');
      console.log('ok: compact-set-states.py keeps the bytes of a deleted branch\'s last writes after the newest head, refuses a live branch\'s, and applies after an open and a close');
    },

    'an unapplied update': () => {
      const path = join(scratch, 'unapplied');
      const writer = JsStore.create({ path });
      writer.registerState({ id: WHOLE, strategy: 'snapshot' });
      writer.registerState({ id: 'other', strategy: 'snapshot' });
      for (let n = 0; n < 5; n++) writer.setStateJson(WHOLE, { n, padding: 'x'.repeat(200) });
      writer.setStateJson('other', { first: true });
      writer.close();
      const sets = updates(path).filter(({ state }) => state === WHOLE);
      const [earlier, newest] = [sets[2], sets[4]];
      withHeads(path, `if key[1] == ${JSON.stringify(WHOLE)}: head[:] = [${earlier.record} if v == ${newest.record} else v for v in head]`);
      const heads = savedHeads(path);
      assert.ok(heads.find(({ state }) => state === WHOLE).offset === earlier.record && heads.find(({ state }) => state === 'other').offset > newest.record,
        `the premise: state.bin names an earlier set as the head of ${WHOLE}, and the head of other lies after its last set`);
      const unapplied = sets.slice(3);
      const bytes = new Map(unapplied.map((update) => [update.id, payloadOf(path, update)]));
      const loose = copyWithPayload(path, 'unapplied-loose', newest, (payload) => {
        const at = [Buffer.from('\xa3Set\xc4', 'latin1'), Buffer.from('\xa8Snapshot\xc4', 'latin1')].map((key) => payload.indexOf(key) + key.length - 1).find((n) => n > 0);
        assert.ok(at > 0, 'the premise: the last set holds its value as bytes');
        payload[at] = 0xd9;
        return payload;
      });
      const looseRun = compact(loose, '--state', WHOLE, '--apply');
      assert.equal(looseRun.status, 0, `the tool on a store whose set that no head reaches holds a string exited ${looseRun.status}:\n${looseRun.out}${looseRun.err}`);
      const before = digestOfCopy(path, 'a store with two unapplied sets');
      const dry = compact(path, '--state', WHOLE);
      assert.ok(dry.out.includes(`  ${WHOLE}: drop 2 old value(s), `) && dry.out.includes('; 2 update(s), '), `the dry run should drop the two sets below the head and count the two after it as staying:\n${dry.out}`);
      const run = compact(path, '--state', WHOLE, '--apply');
      assert.equal(run.status, 0, `compact-set-states.py on a store with two unapplied sets exited ${run.status}:\n${run.out}${run.err}`);
      const after = new Map(updates(path).map((update) => [update.id, update]));
      assert.ok(after.get(earlier.id).record !== earlier.record, 'the premise: the run moves the set that the head names');
      for (const [id, payload] of bytes) assert.ok(payloadOf(path, after.get(id)).equals(payload), `the unapplied set ${id} should keep its bytes`);
      assert.equal(digest(path, 'the store with two unapplied sets, after the tool'), before, 'the store with two unapplied sets should read the same after the tool');
      console.log('ok: compact-set-states.py applies to a store whose state.bin names an earlier set as the head, copies the two sets after it with their bytes, also when one of them is not a whole value, and it reads the same');
    },

    'the patterns': () => {
      const path = join(scratch, 'patterns');
      const writer = JsStore.create({ path });
      const ids = ['a?b', 'axb', 'plot/\u{1F331}', 'line\nbreak', 'plain'];
      for (const id of ids) writer.registerState({ id, strategy: 'snapshot' });
      for (let n = 0; n < 3; n++) for (const id of ids) writer.setStateJson(id, { n, padding: 'x'.repeat(100) });
      writer.close();
      for (const [patterns, expected] of [[['a?b'], ['a?b']], [['*\u{1F331}'], ['plot/\u{1F331}']], [['line*'], ['line\nbreak']], [['*'], ids]]) {
        const dry = compact(path, ...patterns.flatMap((pattern) => ['--state', pattern]));
        assert.equal(dry.status, 0, `the dry run with ${patterns} exited ${dry.status}:\n${dry.out}${dry.err}`);
        assert.ok(dry.out.includes(`${expected.length} named state(s):`), `the tool should name ${expected.length} state(s) for ${patterns}:\n${dry.out}`);
        for (const id of expected) assert.ok(dry.out.includes(`  ${id}: drop `), `the tool should name ${JSON.stringify(id)} for ${patterns}:\n${dry.out}`);
        const read = digest(path, `the store with odd names, with ${patterns}`, patterns);
        for (const id of ids) {
          assert.equal(!read.includes(`\t${id}\tat eight sequences `), expected.includes(id),
            `the check should ${expected.includes(id) ? 'leave out' : 'read'} the history of ${JSON.stringify(id)} for ${patterns}`);
        }
      }
      console.log('ok: the tool and the check name the same states for the same pattern, with ? literal, a name outside the BMP and a name with a line break');
    },

    'the mode': () => {
      const { path } = branchedStore('mode');
      for (const name of ['records.log', 'state.bin']) chmodSync(join(path, name), 0o2640);
      assert.equal(statSync(join(path, 'state.bin')).mode & 0o7777, 0o2640, 'the premise: the files take setgid');
      const run = compact(path, '--state', WHOLE, '--apply');
      assert.equal(run.status, 0, `compact-set-states.py exited ${run.status}:\n${run.out}${run.err}`);
      for (const name of ['records.log', 'state.bin']) {
        assert.equal(statSync(join(path, name)).mode & 0o7777, 0o2640, `the new ${name} should have the old one's mode, setgid included`);
      }
      console.log('ok: the new records.log and state.bin have the modes of the old ones');
    },
  };
  if (process.platform === 'win32') console.log(`skip: the ${Object.keys(compactCases).length} cases of compact-set-states.py, which runs on Unix only`);
  else for (const [, run] of Object.entries(compactCases)) run();
} finally {
  rmSync(scratch, { recursive: true, force: true });
}
