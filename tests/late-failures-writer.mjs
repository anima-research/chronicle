import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { JsStore } from '../index.js';
import * as scenarios from './late-failures-scenarios.mjs';

const message = (text, n) => ({ channel: 'general', text, n });
const entry = blobHash => ({ blobHash, size: 4, mode: 0o644 });

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

function known(name) {
  const scenario = Object.hasOwn(scenarios, name) ? scenarios[name] : undefined;
  if (scenario === undefined || scenario.name !== name) throw new Error(`tests/late-failures-scenarios.mjs has no scenario named ${name}`);
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

function run([name, root, outside, index]) {
  const scenario = known(name);
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
  mark('begin');
  let s = JsStore.create({ path });
  base(s, f);
  const { kind } = scenario;
  scenario.setup?.(s, f);
  s.sync();
  mark('setup');

  if (kind === 'long') {
    const at = index === undefined ? Infinity : Number(index);
    let i = 0;
    for (; i < scenario.appends && i < at; i++) {
      if (index === undefined) mark(`append-${i}-begin`);
      scenario.call(s, f, i);
      if (index === undefined) mark(`append-${i}-end`);
    }
    if (index === undefined) {
      mark('end');
      return out;
    }
  }
  scenario.unsynced?.(s);
  if (kind === 'cold') {
    const { read } = scenario;
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

  const { reads } = scenario;
  const call = () => scenario.call(s, f, Number(index));
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

function reopen([name, index, ...paths]) {
  const scenario = known(name);
  const { reads, probe } = scenario;
  const answers = paths.map(path => {
    const f = {};
    if (scenario.freshBlob) f.fresh = freshHash(`${path}-scratch`);
    let s = null;
    const opened = attempt(() => { s = JsStore.open({ path }); });
    if (!opened.ok) return { opened };
    const result = { opened, reads: readAll(reads, s, f) };
    if (probe) result.probe = attempt(() => probe(s));
    s.close();
    return result;
  });
  if (!probe) return { answers };
  const s = JsStore.create({ path: `${paths[0]}-without-the-call` });
  base(s, {});
  s.sync();
  const old = attempt(() => probe(s));
  s.close();
  return { answers, old };
}

if (process.argv[2] === 'run') process.stdout.write(JSON.stringify(run(process.argv.slice(3))));
else if (process.argv[2] === 'reopen') process.stdout.write(JSON.stringify(reopen(process.argv.slice(3))));
else throw new Error(`late-failures-writer.mjs takes run or reopen, not ${process.argv[2]}`);
