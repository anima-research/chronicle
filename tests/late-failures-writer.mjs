import { closeSync, existsSync, mkdirSync, openSync, readdirSync, readFileSync, readSync, writeFileSync, writeSync } from 'node:fs';
import { join } from 'node:path';
import { JsStore } from '../index.js';

const message = (text, n) => ({ channel: 'general', text, n });
const entry = blobHash => ({ blobHash, size: 4, mode: 0o644 });
const utf8 = buffer => (buffer === null || buffer === undefined ? null : buffer.toString('utf8'));
const stamped = (id, sequence) => typeof id === 'string' && id !== '' && Number.isSafeInteger(sequence) && sequence >= 0;
const plain = items => (items === null ? null : items.map(({ id, sequence, ...rest }) => ({ ...rest, stamped: stamped(id, sequence) })));
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

function threeWrites(s) {
  s.appendJson('journal', { n: 1 });
  s.setStateJson('mode', { mode: 'new' });
  s.appendToStateJson('messages', message('new', 1));
}

function coldSetup(s, f) {
  [1, 2, 3].forEach(n => s.appendToStateJson('messages', message('more', n)));
  s.registerStateFieldIndex('messages', '/channel', 'string');
  s.registerStateFieldIndex('messages', '/n', 'number');
  f.at = s.currentSequence();
  s.setStateJson('mode', { mode: 'later' });
  f.record = s.getRecordIdsByType('journal')[0];
  s.createBranch('other');
}

const h = {
  message, entry, utf8, branchNames, stateList, files, branchStates, journal, messages, identified, strategyOf,
  threeWrites, threeReads: { journal, mode: s => s.getStateJson('mode'), messages }, coldSetup,
  aimed: process.env.CHRONICLE_LATE_AIMED === '1',
  devNull: () => openSync('/dev/null', 'w'),
  foreignWrite: fd => writeSync(fd, 'x'),
  foreignReads: () => {
    const fd = openSync('/dev/zero', 'r');
    for (let i = 1 + Math.floor(Math.random() * 8); i > 0; i--) readSync(fd, Buffer.alloc(1), 0, 1, null);
    closeSync(fd);
  },
};

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

const FUNCTIONS = ['start', 'setup', 'unsynced', 'call', 'reads', 'read', 'probe', 'method'];

function scenarioAt(path) {
  const spec = JSON.parse(readFileSync(path, 'utf8'));
  const scenario = { ...spec };
  for (const field of FUNCTIONS) if (spec[field] !== undefined) scenario[field] = (0, eval)(`(${spec[field]})`);
  return scenario;
}

function attempt(work) {
  try {
    const value = work();
    return { ok: true, value: value === undefined ? null : JSON.parse(JSON.stringify(value)) };
  } catch (error) {
    return { ok: false, error: error instanceof Error, code: error?.code ?? null, message: String(error?.message ?? error) };
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

function run([scenarioFile, root, outside, index]) {
  const scenario = scenarioAt(scenarioFile);
  const { name } = scenario;
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
  if (scenario.freshBlob) f.fresh = freshHash(join(outside, 'scratch'));
  scenario.start?.(h);
  mark('begin');
  let s = JsStore.create({ path });
  base(s, f);
  const { kind } = scenario;
  scenario.setup?.(s, f, h);
  s.sync();
  mark('setup');

  if (kind === 'long') {
    const at = index === undefined ? Infinity : Number(index);
    let i = 0;
    for (; i < scenario.appends && i < at; i++) {
      if (index === undefined) mark(`append-${i}-begin`);
      scenario.call(s, f, h, i);
      if (index === undefined) mark(`append-${i}-end`);
    }
    if (index === undefined) {
      mark('end');
      return out;
    }
  }
  scenario.unsynced?.(s, f, h);
  if (kind === 'cold') {
    const { read } = scenario;
    out.expected = attempt(() => read(s, f, h));
    s.close();
    s = JsStore.open({ path });
    mark('opened');
    out.call = step('call', () => read(s, f, h));
    out.retry = step('retry', () => read(s, f, h));
    s.close();
    mark('end');
    return out;
  }

  const reads = scenario.reads(h);
  const call = () => scenario.call(s, f, h, Number(index));
  out.method = scenario.method ? scenario.method(Number(index)) : name;
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
  if (!out.call.ok) {
    copyTree(path, join(outside, 'thrown'));
    mark('thrown-copied');
  }
  const gaveOld = !out.call.ok && Object.entries(out.reads[0]).every(([key, read]) => read.ok && out.old[key].ok
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

function reopen([scenarioFile, index, ...paths]) {
  const scenario = scenarioAt(scenarioFile);
  const reads = scenario.reads(h);
  const { probe } = scenario;
  const answers = paths.map(path => {
    const f = {};
    if (scenario.freshBlob) f.fresh = freshHash(`${path}-scratch`);
    let s = null;
    const opened = attempt(() => { s = JsStore.open({ path }); });
    if (!opened.ok) return { opened };
    const result = { opened, reads: readAll(reads, s, f) };
    if (probe) result.probe = attempt(() => probe(s, f, h));
    s.close();
    return result;
  });
  if (!probe) return { answers };
  const s = JsStore.create({ path: `${paths[0]}-without-the-call` });
  base(s, {});
  s.sync();
  const old = attempt(() => probe(s, {}, h));
  s.close();
  return { answers, old };
}

try {
  if (process.argv[2] === 'run') process.stdout.write(JSON.stringify(run(process.argv.slice(3))));
  else if (process.argv[2] === 'reopen') process.stdout.write(JSON.stringify(reopen(process.argv.slice(3))));
  else throw new Error(`late-failures-writer.mjs takes run or reopen, not ${process.argv[2]}`);
} catch (error) {
  process.stderr.write(`${error?.stack ?? error}\n${error}\n`);
  process.exitCode = 1;
}
