import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { JsStore } from '../index.js';

const message = (text, n) => ({ channel: 'general', text, n });
const entry = blobHash => ({ blobHash, size: 4, mode: 0o644 });
const utf8 = buffer => (buffer === null || buffer === undefined ? null : buffer.toString('utf8'));
const plain = items => (items === null ? null : items.map(({ id, sequence, ...rest }) => rest));
const payloads = (s, type) => s.getRecordIdsByType(type).map(id => JSON.parse(utf8(s.getRecord(id).payload)));
const branchNames = s => s.listBranches().map(({ name }) => name).sort();
const stateList = s => s.listStates().map(({ id, strategy }) => `${id} ${strategy}`).sort();
const files = s => s.treeList('files').map(({ path, blobHash, size, mode }) => ({ path, blobHash, size, mode }));
const branchStates = name => s => {
  if (!s.listBranches().some(branch => branch.name === name)) return null;
  const view = s.view(name);
  return { messages: view.getStateJson('messages'), mode: view.getStateJson('mode') };
};

const journal = s => payloads(s, 'journal');
const messages = s => s.getStateJson('messages');
const identified = s => plain(s.getStateJson('identified'));
const strategyOf = s => s.listStates().find(({ id }) => id === 'messages')?.strategy ?? null;

function base(s, f) {
  for (const [id, strategy] of [['mode', 'snapshot'], ['raw', 'snapshot'], ['messages', 'append_log'],
    ['identified', 'append_log'], ['files', 'tree']]) {
    s.registerState({ id, strategy });
  }
  s.appendJson('journal', { n: 0 });
  s.setStateJson('mode', { mode: 'old' });
  s.setState('raw', Buffer.from('old bytes'));
  s.appendToStateJson('messages', message('old', 0));
  s.appendToStateJsonWithIdentity('identified', { text: 'old' }, 'id', 'sequence');
  f.blob = s.storeBlob(Buffer.from('blob'), 'text/plain');
  s.treeSet('files', 'a.txt', entry(f.blob));
}

function threeWrites(s) {
  s.appendJson('journal', { n: 1 });
  s.setStateJson('mode', { mode: 'new' });
  s.appendToStateJson('messages', message('new', 1));
}

const longCall = (s, i) => (i % 2 === 0
  ? s.appendJson('journal', { n: i })
  : s.appendToStateJson('messages', message('long', i)));

const writes = {
  appendJson: { call: s => s.appendJson('journal', { n: 1 }), reads: { journal } },
  appendManyToStateJsonWithIdentity: {
    call: s => s.appendManyToStateJsonWithIdentity('identified', [{ text: 'two' }, { text: 'three' }], 'id', 'sequence'),
    reads: { identified },
  },
  appendToStateJson: { call: s => s.appendToStateJson('messages', message('new', 1)), reads: { messages } },
  appendToStateJsonWithIdentity: {
    call: s => s.appendToStateJsonWithIdentity('identified', { text: 'new' }, 'id', 'sequence'),
    reads: { identified },
  },
  compactState: {
    setup: s => [1, 2, 3].forEach(n => s.appendToStateJson('messages', message('more', n))),
    call: s => s.compactState('messages'),
    reads: { messages, length: s => s.getStateLen('messages') },
  },
  createBranch: { call: s => s.createBranch('made'), reads: { branches: branchNames } },
  createBranchAt: {
    setup: (s, f) => { f.at = s.currentSequence(); s.setStateJson('mode', { mode: 'later' }); },
    call: (s, f) => s.createBranchAt('made', 'main', f.at),
    reads: { branches: branchNames, made: branchStates('made') },
  },
  createBranchAtWithStates: {
    setup: (s, f) => { f.at = s.currentSequence(); s.setStateJson('mode', { mode: 'later' }); },
    call: (s, f) => s.createBranchAtWithStates('made', 'main', f.at, { stateIds: ['messages'] }),
    reads: { branches: branchNames, made: branchStates('made') },
  },
  createBranchWithStates: {
    call: s => s.createBranchWithStates('made', 'main', { stateIds: ['messages'] }),
    reads: { branches: branchNames, made: branchStates('made') },
  },
  deleteBranch: {
    setup: s => s.createBranch('gone'),
    call: s => s.deleteBranch('gone'),
    reads: { branches: branchNames },
  },
  editStateItem: {
    call: s => s.editStateItem('messages', 0, Buffer.from(JSON.stringify(message('edited', 0)))),
    reads: { messages },
  },
  redactStateItems: {
    setup: s => s.appendManyToStateJsonWithIdentity('identified', [{ text: 'two' }, { text: 'three' }], 'id', 'sequence'),
    call: s => s.redactStateItems('identified', 0, 2),
    reads: { identified, length: s => s.getStateLen('identified') },
  },
  registerState: { call: s => s.registerState({ id: 'added', strategy: 'snapshot' }), reads: { states: stateList } },
  registerStateFieldIndex: {
    call: s => s.registerStateFieldIndex('messages', '/channel', 'string'),
    reads: { index: s => s.queryStateIndexEq('messages', '/channel', 'general', {}) },
  },
  setAutoSnapshot: { call: s => s.setAutoSnapshot(false), reads: { enabled: s => s.autoSnapshotEnabled() } },
  setState: { call: s => s.setState('raw', Buffer.from('new bytes')), reads: { raw: s => utf8(s.getState('raw')) } },
  setStateJson: { call: s => s.setStateJson('mode', { mode: 'new' }), reads: { mode: s => s.getStateJson('mode') } },
  storeBlob: {
    call: s => s.storeBlob(Buffer.from('fresh blob'), 'text/plain'),
    reads: { blob: (s, f) => utf8(s.getBlob(f.fresh)) },
  },
  switchBranch: {
    setup: s => s.createBranch('other'),
    call: s => s.switchBranch('other'),
    reads: { current: s => s.currentBranch().name },
  },
  treeBatch: {
    call: (s, f) => s.treeBatch('files', [
      { op: 'set', path: 'b.txt', entry: entry(f.blob) },
      { op: 'remove', path: 'a.txt' },
    ]),
    reads: { files },
  },
  treeRemove: { call: s => s.treeRemove('files', 'a.txt'), reads: { files } },
  treeSet: { call: (s, f) => s.treeSet('files', 'b.txt', entry(f.blob)), reads: { files } },
  updateStateStrategy: {
    call: s => s.updateStateStrategy({ id: 'messages', strategy: 'append_log', deltaSnapshotEvery: 5, fullSnapshotEvery: 2 }),
    reads: { strategy: strategyOf, messages },
  },
};

const coldReads = {
  currentBranch: s => s.currentBranch().name,
  currentSequence: s => s.currentSequence(),
  getBlob: (s, f) => utf8(s.getBlob(f.blob)),
  getRecord: (s, f) => {
    const record = s.getRecord(f.record);
    return record && { ...record, payload: utf8(record.payload) };
  },
  getRecordIdsByType: s => s.getRecordIdsByType('journal'),
  getState: s => utf8(s.getState('raw')),
  getStateIndexValueCounts: s => s.getStateIndexValueCounts('messages', '/channel'),
  getStateItemJson: s => s.getStateItemJson('messages', 1),
  getStateJson: s => s.getStateJson('messages'),
  getStateJsonAt: (s, f) => s.getStateJsonAt('mode', f.at),
  getStateLen: s => s.getStateLen('messages'),
  getStateSlice: s => utf8(s.getStateSlice('messages', 1, 2)),
  getStateTail: s => utf8(s.getStateTail('messages', 2)),
  listBranches: branchNames,
  listStates: stateList,
  queryStateIndexEq: s => s.queryStateIndexEq('messages', '/channel', 'general', {}),
  queryStateIndexRange: s => s.queryStateIndexRange('messages', '/n', { gte: 1, lte: 2 }),
  stateHeadOffset: s => s.stateHeadOffset('messages'),
  treeGet: s => s.treeGet('files', 'a.txt'),
  treeList: files,
};

function coldSetup(s, f) {
  [1, 2, 3].forEach(n => s.appendToStateJson('messages', message('more', n)));
  s.registerStateFieldIndex('messages', '/channel', 'string');
  s.registerStateFieldIndex('messages', '/n', 'number');
  f.at = s.currentSequence();
  s.setStateJson('mode', { mode: 'later' });
  f.record = s.getRecordIdsByType('journal')[0];
  s.createBranch('other');
}

const scenarios = [
  ...Object.keys(writes).map(name => ({ name, kind: 'write' })),
  { name: 'sync', kind: 'sync' },
  { name: 'close', kind: 'close' },
  { name: 'long', kind: 'long' },
  ...Object.keys(coldReads).map(name => ({ name, kind: 'cold' })),
];

const longReads = { journal, messages };
const threeReads = { journal, mode: s => s.getStateJson('mode'), messages };

function attempt(work) {
  try {
    const value = work();
    return { ok: true, value: value === undefined ? null : JSON.parse(JSON.stringify(value)) };
  } catch (error) {
    return { ok: false, code: error?.code ?? null, message: String(error?.message ?? error) };
  }
}

const readAll = (reads, s, f) => Object.fromEntries(Object.entries(reads).map(([name, read]) => [name, attempt(() => read(s, f))]));

function copyTree(from, to) {
  mkdirSync(to, { recursive: true });
  for (const item of readdirSync(from, { withFileTypes: true })) {
    if (item.isDirectory()) copyTree(join(from, item.name), join(to, item.name));
    else writeFileSync(join(to, item.name), readFileSync(join(from, item.name)));
  }
}

function freshHash(dir) {
  const scratch = JsStore.create({ path: dir });
  const hash = scratch.storeBlob(Buffer.from('fresh blob'), 'text/plain');
  scratch.close();
  return hash;
}

function run([name, root, outside, index]) {
  const scenario = scenarios.find(item => item.name === name);
  const path = join(root, 'store');
  const f = {};
  const marks = [];
  const out = { marks };
  const mark = label => {
    marks.push(label);
    existsSync(`/chronicle-mark/${label}`);
  };
  const step = (label, work) => {
    mark(`${label}-begin`);
    const outcome = attempt(work);
    mark(`${label}-end`);
    return outcome;
  };
  if (name === 'storeBlob') f.fresh = freshHash(join(outside, 'scratch'));
  mark('begin');
  let s = JsStore.create({ path });
  base(s, f);
  const { kind } = scenario;
  const write = writes[name];
  write?.setup?.(s, f);
  if (kind === 'cold') coldSetup(s, f);
  s.sync();
  mark('setup');

  if (kind === 'long') {
    const at = index === undefined ? Infinity : Number(index);
    let i = 0;
    for (; i < 400 && i < at; i++) {
      if (index === undefined) mark(`append-${i}-begin`);
      longCall(s, i);
      if (index === undefined) mark(`append-${i}-end`);
    }
    if (index === undefined) {
      mark('end');
      return out;
    }
  }
  if (kind === 'sync' || kind === 'close') threeWrites(s);
  if (kind === 'cold') {
    const read = coldReads[name];
    out.expected = attempt(() => read(s, f));
    s.close();
    s = JsStore.open({ path });
    mark('opened');
    out.call = step('call', () => read(s, f));
    out.retry = step('retry', () => read(s, f));
    s.close();
    mark('end');
    return out;
  }

  const reads = kind === 'long' ? longReads : kind === 'write' ? write.reads : threeReads;
  const call = kind === 'long' ? () => longCall(s, Number(index))
    : kind === 'sync' ? () => s.sync()
    : kind === 'close' ? () => s.close()
    : () => write.call(s, f);
  out.old = readAll(reads, s, f);
  out.call = step('call', call);
  if (kind === 'close') {
    out.isClosed = attempt(() => s.isClosed());
    out.reads = [readAll(reads, s, f), readAll(reads, s, f)];
    mark('end');
    return out;
  }
  out.reads = [readAll(reads, s, f), readAll(reads, s, f)];
  mark('reads');
  const gaveOld = Object.entries(out.reads[0]).every(([key, read]) => read.ok && out.old[key].ok
    && JSON.stringify(read.value) === JSON.stringify(out.old[key].value));
  if (gaveOld) {
    out.retry = step('retry', call);
    out.retryReads = readAll(reads, s, f);
    mark('retry-reads');
  }
  out.sync = step('sync', () => s.sync());
  out.syncReads = readAll(reads, s, f);
  copyTree(path, join(outside, 'copy'));
  mark('copied');
  out.close = step('close', () => s.close());
  out.isClosed = attempt(() => s.isClosed());
  out.closedReads = readAll(reads, s, f);
  mark('end');
  return out;
}

function reopen([name, index, ...paths]) {
  const scenario = scenarios.find(item => item.name === name);
  const reads = scenario.kind === 'long' ? longReads : scenario.kind === 'write' ? writes[name].reads : threeReads;
  return paths.map(path => {
    const f = {};
    if (name === 'storeBlob') f.fresh = freshHash(`${path}-scratch`);
    let s = null;
    const opened = attempt(() => { s = JsStore.open({ path }); });
    if (!opened.ok) return { opened };
    const result = { opened, reads: readAll(reads, s, f) };
    s.close();
    return result;
  });
}

if (process.argv[2] === 'list') process.stdout.write(JSON.stringify(scenarios));
else if (process.argv[2] === 'run') process.stdout.write(JSON.stringify(run(process.argv.slice(3))));
else if (process.argv[2] === 'reopen') process.stdout.write(JSON.stringify(reopen(process.argv.slice(3))));
