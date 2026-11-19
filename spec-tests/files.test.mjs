import assert from 'node:assert/strict';
import { appendFileSync, existsSync, lstatSync, mkdirSync, readdirSync, readFileSync, renameSync, statSync, symlinkSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { test } from 'node:test';
import { ErrorCode, JsStore, freshDir, freshStore, refuses, refusesInAForm } from './support.mjs';

const CRC_TABLE = Array.from({ length: 256 }, (_, n) => {
  let c = n;
  for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
  return c >>> 0;
});
const crc32 = (bytes) => {
  let c = 0xffffffff;
  for (const byte of bytes) c = CRC_TABLE[(c ^ byte) & 0xff] ^ (c >>> 8);
  return (c ^ 0xffffffff) >>> 0;
};

function readLog(log) {
  const records = [];
  let at = 0;
  const u16 = () => { const v = log.readUInt16LE(at); at += 2; return v; };
  const u64 = () => { const v = log.readBigUInt64LE(at); at += 8; return v; };
  while (at < log.length) {
    const start = at;
    const magic = log.toString('latin1', at, at + 4);
    const version = log[at + 4];
    const flags = log[at + 5];
    at += 6;
    const id = String(u64());
    const sequence = Number(u64());
    const branch = String(u64());
    const time = Number(log.readBigInt64LE(at));
    at += 8;
    const typeLength = u16();
    const type = log.toString('utf8', at, at + typeLength);
    at += typeLength;
    const encoding = log[at++];
    const payloadLength = log.readUInt32LE(at);
    at += 4;
    const payload = log.subarray(at, at + payloadLength);
    at += payloadLength;
    const ids = () => Array.from({ length: u16() }, () => String(u64()));
    const causedBy = ids();
    const linkedTo = ids();
    const crc = log.readUInt32LE(at);
    at += 4;
    records.push({ start, magic, version, flags, id, sequence, branch, time, type, encoding, payload, causedBy, linkedTo, crc });
  }
  return records;
}

function writeRecord(fields) {
  const type = Buffer.from(fields.type, 'utf8');
  const parts = [Buffer.from('REC\0', 'latin1'), Buffer.from([1, 0])];
  for (const n of [fields.id, fields.sequence, fields.branch]) {
    const b = Buffer.alloc(8);
    b.writeBigUInt64LE(BigInt(n));
    parts.push(b);
  }
  const time = Buffer.alloc(8);
  time.writeBigInt64LE(BigInt(fields.time));
  const typeLength = Buffer.alloc(2);
  typeLength.writeUInt16LE(type.length);
  const payloadLength = Buffer.alloc(4);
  payloadLength.writeUInt32LE(fields.payload.length);
  const crc = Buffer.alloc(4);
  crc.writeUInt32LE(crc32(fields.payload));
  parts.push(time, typeLength, type, Buffer.from([fields.encoding]), payloadLength, fields.payload, Buffer.alloc(4), crc);
  return Buffer.concat(parts);
}

function unpack(buffer) {
  let at = 0;
  const take = (n) => { const v = buffer.subarray(at, at + n); at += n; return v; };
  const map = (n) => Object.fromEntries(Array.from({ length: n }, () => [read(), read()]));
  const array = (n) => Array.from({ length: n }, () => read());
  const str = (n) => take(n).toString('utf8');
  const bin = (n) => ({ bin: Buffer.from(take(n)) });
  function read() {
    const b = buffer[at++];
    if (b <= 0x7f) return b;
    if (b >= 0xe0) return b - 0x100;
    if ((b & 0xf0) === 0x80) return map(b & 0x0f);
    if ((b & 0xf0) === 0x90) return array(b & 0x0f);
    if ((b & 0xe0) === 0xa0) return str(b & 0x1f);
    const be = (n) => { const v = buffer.readUIntBE(at, n); at += n; return v; };
    switch (b) {
      case 0xc0: return null;
      case 0xc2: return false;
      case 0xc3: return true;
      case 0xc4: return bin(be(1));
      case 0xc5: return bin(be(2));
      case 0xc6: return bin(be(4));
      case 0xcc: return be(1);
      case 0xcd: return be(2);
      case 0xce: return be(4);
      case 0xcf: { const v = buffer.readBigUInt64BE(at); at += 8; return Number(v); }
      case 0xd0: { const v = buffer.readInt8(at); at += 1; return v; }
      case 0xd1: { const v = buffer.readInt16BE(at); at += 2; return v; }
      case 0xd2: { const v = buffer.readInt32BE(at); at += 4; return v; }
      case 0xd3: { const v = buffer.readBigInt64BE(at); at += 8; return Number(v); }
      case 0xd9: return str(be(1));
      case 0xda: return str(be(2));
      case 0xdb: return str(be(4));
      case 0xdc: return array(be(2));
      case 0xdd: return array(be(4));
      case 0xde: return map(be(2));
      case 0xdf: return map(be(4));
      default: throw new Error(`a MessagePack byte this reader doesn't take: 0x${b.toString(16)}`);
    }
  }
  const value = read();
  assert.equal(at, buffer.length, 'one MessagePack value fills the payload');
  return value;
}

const manifest = (dir) => readFileSync(join(dir, 'MANIFEST'));
const versionTwo = (bits) => {
  const b = Buffer.alloc(13);
  b.write('RST\0', 0, 'latin1');
  b[4] = 2;
  b.writeBigUInt64LE(bits, 5);
  return b;
};

test('D1 a new store\'s MANIFEST is RST\\0 and version 1', () => {
  const { store, dir } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log' });
  store.appendToStateJson('list', 1);
  store.close();
  assert.deepEqual(manifest(dir), Buffer.from('RST\0\x01', 'latin1'));
});

test('D1 just before its first change of several items, the store writes version 2 with feature bit 0 set, as 64 bits with the lowest byte first', () => {
  const { store, dir } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log' });
  store.appendManyToStateJson('list', [1, 2]);
  assert.deepEqual(manifest(dir), versionTwo(1n));
  store.close();
});

test('D1 E2 a build opens version 2 with the bits it knows, and refuses another version, or a bit it does not know, with InvalidFormat', () => {
  const { store, dir } = freshStore();
  store.close();
  writeFileSync(join(dir, 'MANIFEST'), versionTwo(1n));
  JsStore.open({ path: dir }).close();
  for (const bytes of [versionTwo(2n), versionTwo(3n), Buffer.from('RST\0\x03', 'latin1'), Buffer.from('RST\0\x00', 'latin1')]) {
    writeFileSync(join(dir, 'MANIFEST'), bytes);
    refuses(() => JsStore.open({ path: dir }), ErrorCode.InvalidFormat, /^Invalid store format: /);
  }
});

test('D2 T2 the log holds each record as REC\\0, version 1, flags 0, ID, sequence, branch, time, type, encoding, payload, the two lists of links and the CRC-32 of the payload', () => {
  const { store, dir } = freshStore();
  store.createBranch('side');
  const side = store.view('side');
  const a = store.appendJson('note', { n: 1 });
  const b = side.append('bytes é', Buffer.from([0, 1, 255]));
  const c = side.appendJsonWithLinks('linked', { n: 3 }, { causedBy: [b.id, a.id], linkedTo: [a.id] });
  store.close();
  const logged = readLog(readFileSync(join(dir, 'records.log')));
  assert.equal(logged.length, 3);
  for (const record of logged) {
    assert.deepEqual([record.magic, record.version, record.flags], ['REC\0', 1, 0]);
    assert.equal(record.crc, crc32(record.payload));
  }
  const reopened = JsStore.open({ path: dir });
  const sideId = reopened.listBranches().find((branch) => branch.name === 'side').id;
  const mainId = reopened.listBranches().find((branch) => branch.name === 'main').id;
  reopened.close();
  for (const [written, record, branch] of [[a, logged[0], mainId], [b, logged[1], sideId], [c, logged[2], sideId]]) {
    assert.deepEqual(
      { id: record.id, sequence: record.sequence, branch: record.branch, time: record.time, type: record.type, payload: record.payload, causedBy: record.causedBy, linkedTo: record.linkedTo },
      { id: written.id, sequence: written.sequence, branch, time: written.timestamp, type: written.recordType, payload: written.payload, causedBy: written.causedBy, linkedTo: written.linkedTo },
    );
  }
  assert.deepEqual(logged.map((record) => record.encoding), [0, 2, 0]);
});

test('D7 while the store is open, LOCK is a regular file in its directory', () => {
  const { store, dir } = freshStore();
  assert.ok(statSync(join(dir, 'LOCK')).isFile());
  store.close();
  assert.ok(!existsSync(join(dir, 'LOCK')) || statSync(join(dir, 'LOCK')).isFile());
});

test('D8 a change of a state is a state_update record with encoding 1, whose payload is a MessagePack map of record_id, global_sequence, state_id, prev_update_offset, operation and timestamp', () => {
  const { store, dir } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log' });
  const first = store.appendToStateJson('list', { n: 1 });
  const second = store.appendManyToStateJson('list', [{ n: 2 }, { n: 3 }]).record;
  store.close();
  const logged = readLog(readFileSync(join(dir, 'records.log')));
  const changes = logged.filter((record) => record.type === 'state_update');
  assert.deepEqual(changes.map((record) => record.id), [first.id, second.id]);
  const [one, two] = changes.map((record) => {
    assert.equal(record.encoding, 1);
    return unpack(record.payload);
  });
  for (const change of [one, two]) {
    assert.deepEqual(Object.keys(change).sort(), ['global_sequence', 'operation', 'prev_update_offset', 'record_id', 'state_id', 'timestamp']);
    assert.equal(change.state_id, 'list');
    assert.equal(Object.keys(change.operation).length, 1);
  }
  assert.equal(one.prev_update_offset, null);
  assert.equal(two.prev_update_offset, changes[0].start);
  assert.deepEqual(one.operation.Append, { bin: Buffer.from('{"n":1}') });
  assert.deepEqual(two.operation.AppendMany.map((item) => item.bin.toString()), ['{"n":2}', '{"n":3}']);
});

test('D8 T7 A14 an open passes over a record of the type state_update that is no change of a state, and counts it, and getStateUpdateJson throws for it', () => {
  const { store, dir } = freshStore();
  store.appendJson('note', {});
  store.close();
  const log = join(dir, 'records.log');
  const [last] = readLog(readFileSync(log)).slice(-1);
  appendFileSync(log, writeRecord({ id: BigInt(last.id) + 1n, sequence: last.sequence + 1, branch: last.branch, time: last.time, type: 'state_update', encoding: 1, payload: Buffer.from([0xc1, 0xc1]) }));
  const reopened = JsStore.open({ path: dir });
  assert.equal(reopened.recovery().undecodableStateUpdates, 1);
  assert.equal(reopened.recovery().updatesOnMissingBranches, 0);
  refusesInAForm(() => reopened.getStateUpdateJson(String(BigInt(last.id) + 1n)));
  reopened.close();
});

function packMap(entries) {
  const parts = [Buffer.from([0x80 | entries.length])];
  const pack = (value) => {
    if (value === null) return Buffer.from([0xc0]);
    if (typeof value === 'string') return Buffer.concat([Buffer.from([0xa0 | Buffer.byteLength(value)]), Buffer.from(value)]);
    if (typeof value === 'number') {
      const b = Buffer.alloc(9);
      b[0] = 0xcf;
      b.writeBigUInt64BE(BigInt(value), 1);
      return b;
    }
    if (Buffer.isBuffer(value.bin)) {
      const length = Buffer.alloc(4);
      length.writeUInt32BE(value.bin.length);
      return Buffer.concat([Buffer.from([0xc6]), length, value.bin]);
    }
    return packMap(Object.entries(value));
  };
  for (const [key, value] of entries) parts.push(pack(key), pack(value));
  return Buffer.concat(parts);
}

test('D8 A14 a build fails to decode a kind of change that it does not know, and getStateUpdateJson throws for it', () => {
  const { store, dir } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log' });
  store.appendToStateJson('list', 1);
  store.close();
  const log = join(dir, 'records.log');
  const [last] = readLog(readFileSync(log)).slice(-1);
  const known = unpack(last.payload);
  const change = (n, stateId, operation) => writeRecord({
    id: BigInt(last.id) + BigInt(n),
    sequence: last.sequence + n,
    branch: last.branch,
    time: last.time,
    type: 'state_update',
    encoding: 1,
    payload: packMap(Object.entries({ ...known, global_sequence: last.sequence + n, state_id: stateId, prev_update_offset: null, operation })),
  });
  appendFileSync(log, Buffer.concat([change(1, 'known', known.operation), change(2, 'unknown', { Bogus: null })]));
  const reopened = JsStore.open({ path: dir });
  assert.deepEqual(Object.keys(JSON.parse(reopened.getStateUpdateJson(String(BigInt(last.id) + 1n))).operation), ['Append']);
  refusesInAForm(() => reopened.getStateUpdateJson(String(BigInt(last.id) + 2n)));
  reopened.close();
});

test('T7 an open passes over a change of a state whose branch is not in the store, and counts it', () => {
  const { store, dir } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log' });
  store.appendToStateJson('list', 1);
  store.close();
  const log = join(dir, 'records.log');
  const [last] = readLog(readFileSync(log)).slice(-1);
  const payload = packMap(Object.entries({ ...unpack(last.payload), global_sequence: 1, state_id: 'elsewhere', prev_update_offset: null }));
  appendFileSync(log, writeRecord({ id: BigInt(last.id) + 1n, sequence: 1, branch: 99, time: last.time, type: 'state_update', encoding: 1, payload }));
  const reopened = JsStore.open({ path: dir });
  assert.equal(reopened.recovery().updatesOnMissingBranches, 1);
  assert.deepEqual(reopened.recovery().unregisteredStates, []);
  reopened.close();
});

test('D9 an open removes a .chronicle-checkpoint- file that an interrupted save left', () => {
  const { store, dir } = freshStore();
  store.close();
  writeFileSync(join(dir, '.chronicle-checkpoint-left'), 'partial');
  JsStore.open({ path: dir }).close();
  assert.equal(existsSync(join(dir, '.chronicle-checkpoint-left')), false);
});

test('D9 the store leaves no .chronicle-checkpoint- file after its saves, and changes, replaces and removes no entry of another name', () => {
  const { store, dir } = freshStore();
  writeFileSync(join(dir, 'mine.txt'), 'kept');
  mkdirSync(join(dir, 'mine'));
  writeFileSync(join(dir, 'mine', 'inside'), 'kept too');
  const marks = () => ['mine.txt', 'mine', join('mine', 'inside')].map((name) => {
    const { ino, mtimeNs } = statSync(join(dir, name), { bigint: true });
    return [name, ino, mtimeNs];
  });
  const before = marks();
  store.registerState({ id: 'list', strategy: 'append_log' });
  store.registerStateFieldIndex('list', '/n', 'number');
  store.appendManyToStateJson('list', [{ n: 1 }, { n: 2 }]);
  store.storeBlob(Buffer.from('blob'), 'text/plain');
  store.createBranch('side');
  store.sync();
  store.close();
  JsStore.open({ path: dir }).close();
  const own = new Set(['MANIFEST', 'LOCK', 'records.log', 'state.bin', 'state-indexes.bin', 'branches.bin', 'blobs']);
  assert.deepEqual(readdirSync(dir).filter((name) => !own.has(name)).sort(), ['mine', 'mine.txt']);
  assert.equal(readFileSync(join(dir, 'mine.txt'), 'utf8'), 'kept');
  assert.deepEqual(marks(), before);
  assert.deepEqual(readdirSync(join(dir, 'mine')), ['inside']);
  assert.equal(readFileSync(join(dir, 'mine', 'inside'), 'utf8'), 'kept too');
});

test('D9 openOrCreate creates a store beside LOCK, blobs and a name that starts with .chronicle-checkpoint-', () => {
  for (const [name, make] of [['LOCK', 'file'], ['blobs', 'dir'], ['.chronicle-checkpoint-x', 'file']]) {
    const { store, dir } = freshStore();
    store.close();
    const empty = `${dir}-${name}`;
    mkdirSync(empty);
    if (make === 'dir') mkdirSync(join(empty, name));
    else writeFileSync(join(empty, name), '');
    JsStore.openOrCreate({ path: empty }).close();
    assert.ok(existsSync(join(empty, 'MANIFEST')), name);
  }
});

test("D9 a save replaces the file that a link at state.bin leads to, in that file's own directory, the store writes the log through a link at records.log, and both links stay", { skip: process.platform === 'win32' && 'makes a symbolic link' }, () => {
  const { store, dir } = freshStore();
  store.registerState({ id: 'list', strategy: 'append_log' });
  store.close();
  const elsewhere = `${dir}-elsewhere`;
  mkdirSync(elsewhere);
  for (const name of ['state.bin', 'records.log']) {
    renameSync(join(dir, name), join(elsewhere, name));
    symlinkSync(join(elsewhere, name), join(dir, name));
  }
  const reopened = JsStore.open({ path: dir });
  const before = (name) => statSync(join(elsewhere, name), { bigint: true });
  const [states, log] = [before('state.bin'), before('records.log')];
  reopened.appendToStateJson('list', 1);
  reopened.sync();
  reopened.close();
  for (const name of ['state.bin', 'records.log']) assert.ok(lstatSync(join(dir, name)).isSymbolicLink(), name);
  assert.notEqual(before('state.bin').ino, states.ino);
  assert.equal(before('records.log').ino, log.ino);
  assert.ok(before('records.log').size > log.size);
  assert.deepEqual(readdirSync(elsewhere).filter((name) => name.startsWith('.chronicle-checkpoint-')), []);
  const again = JsStore.open({ path: dir });
  assert.deepEqual(again.getStateJson('list'), [1]);
  again.close();
});

test('D9 E2 openOrCreate takes a regular file named blobs by its name, and the create then fails with Io', () => {
  const dir = freshDir();
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, 'blobs'), 'a file');
  refuses(() => JsStore.openOrCreate({ path: dir }), ErrorCode.Io, /^IO error: /);
});

function exportsOf(source) {
  const text = source.replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/[^\n]*/g, '');
  const found = [];
  let depth = 0;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if ('{(['.includes(c)) depth++;
    else if ('})]'.includes(c)) depth--;
    else if (depth === 0 && /^export\b/.test(text.slice(i, i + 7)) && (i === 0 || /\s/.test(text[i - 1]))) {
      const header = text.slice(i).match(/^export\s+(?:declare\s+)?(?:const\s+)?(\w+)\s+(\w+)/);
      const open = text.indexOf('{', i);
      const close = (() => {
        let d = 0;
        for (let k = open; k < text.length; k++) {
          if (text[k] === '{') d++;
          else if (text[k] === '}' && --d === 0) return k;
        }
        return -1;
      })();
      assert.ok(header && close > open, `a whole declaration at ${i}`);
      found.push({ kind: header[1], name: header[2], body: text.slice(open + 1, close) });
      i = close;
    }
  }
  return found;
}

function membersOf(body) {
  const out = [];
  let depth = 0;
  let member = '';
  for (const c of `${body}\n`) {
    if ('{(['.includes(c)) depth++;
    if ('})]'.includes(c)) depth--;
    if (depth === 0 && (c === '\n' || c === ';')) {
      const text = member.replace(/\s+/g, ' ').replace(/([({[]) /g, '$1').replace(/ ([)}\]])/g, '$1').trim();
      if (text) out.push(text);
      member = '';
    } else member += c;
  }
  assert.equal(depth, 0, 'every bracket of the body closes');
  return out.sort();
}

test('N5 the built index.d.ts exports JsStore, ErrorCode and the types of chronicle.spec.md, each method and each type with the name and the signature that the spec gives', () => {
  const spec = readFileSync(new URL('../chronicle.spec.md', import.meta.url), 'utf8');
  const declared = exportsOf(readFileSync(new URL('../index.d.ts', import.meta.url), 'utf8'));
  const types = [...spec.matchAll(/^\*\*T\d+\. `(\w+)`\.\*\* `\{ (.*) \}`/gm)];
  assert.equal(types.length, 27);
  assert.deepEqual(
    declared.map(({ kind, name }) => `${kind} ${name}`).sort(),
    ['class JsStore', 'enum ErrorCode', ...types.map(([, name]) => `interface ${name}`)].sort(),
  );
  const methods = [...spec.matchAll(/^\*\*A\d+\. `(\w+)`\.\*\* `([^`]+)`/gm)];
  assert.equal(methods.length, 79);
  for (const [, name, signature] of methods) assert.equal(signature.match(/^(?:static )?(\w+)\(/)?.[1], name, signature);
  assert.deepEqual(membersOf(declared.find(({ name }) => name === 'JsStore').body), methods.map(([, , signature]) => signature).sort());
  for (const [, name, body] of types) {
    assert.deepEqual(membersOf(declared.find((entry) => entry.name === name).body), body.split('; ').sort(), name);
  }
});
