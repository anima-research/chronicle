import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { availableParallelism, constants, tmpdir } from 'node:os';
import { join, posix } from 'node:path';
import { after, suite, test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { isDeepStrictEqual } from 'node:util';
import { quoted, readTrace, resolved } from './durability-trace.mjs';

const CALLS = '%file,%desc,mmap,munmap,msync,sync,syncfs,io_uring_setup,io_uring_enter,io_uring_register,io_setup,io_submit';
const WRITER = fileURLToPath(new URL('./late-failures-writer.mjs', import.meta.url));
const WRITES = new Set(['write', 'pwrite64', 'writev', 'pwritev', 'pwritev2']);
const READS = new Set(['read', 'pread64', 'readv', 'preadv', 'preadv2']);
const SYNCS = new Set(['fsync', 'fdatasync']);
const RENAMES = new Set(['rename', 'renameat', 'renameat2']);
const OPENS = new Set(['open', 'openat', 'openat2', 'creat']);
const MKDIRS = new Set(['mkdir', 'mkdirat']);
const BY_DESCRIPTOR = new Set([...WRITES, ...READS, ...SYNCS]);
const NOT_MEASURED = 'The instrument does not measure';
const thrownCopyHoldsChange = problem => problem.thrownCopyHoldsChange === true;
const METADATA_ON_DISK = { rule: 'the reopen', owns: thrownCopyHoldsChange };

const isUnder = (path, dir) => typeof path === 'string' && (path === dir || path.startsWith(`${dir}/`));
const holds = (dir, path) => typeof dir === 'string' && dir !== path && (dir === '/' || path.startsWith(`${dir}/`));
const env = { ...process.env };
delete env.LD_PRELOAD;
delete env.CHRONICLE_LATE_AIMED;

function straceProblem() {
  const probe = spawnSync('strace', ['-f', '-y', '-o', '/dev/null', '-e', `trace=${CALLS}`, '-e', 'inject=fsync:error=EIO:when=1', 'true'],
    { encoding: 'utf8', timeout: 10_000, killSignal: 'SIGKILL' });
  if (probe.error?.code === 'ETIMEDOUT') return 'strace did not trace true within 10 s';
  if (probe.error) return `strace is not on the path (${probe.error.code})`;
  if (probe.status !== 0) return `strace may not trace here: ${probe.stderr.trim().split('\n')[0]}`;
  return null;
}

function splitArgs(body) {
  const args = [];
  let current = '';
  let depth = 0;
  let i = body.indexOf('(') + 1;
  for (; i < body.length; i++) {
    const c = body[i];
    if (c === '"') {
      const end = /^"(?:[^"\\]|\\.)*"(?:\.\.\.)?/s.exec(body.slice(i))[0];
      current += end;
      i += end.length - 1;
    } else if (c === '<' && /(\d|AT_FDCWD)$/.test(current)) {
      const end = body.indexOf('>', i);
      current += body.slice(i, end + 1);
      i = end;
    } else if ('([{'.includes(c)) {
      depth++;
      current += c;
    } else if (')]}'.includes(c)) {
      if (depth === 0) break;
      depth--;
      current += c;
    } else if (c === ',' && depth === 0) {
      args.push(current.trim());
      current = '';
    } else {
      current += c;
    }
  }
  if (current.trim() !== '') args.push(current.trim());
  return { args, rest: body.slice(i + 1) };
}

const decorated = arg => /^(?:-?\d+|AT_FDCWD)<(.*)>$/s.exec(arg ?? '')?.[1] ?? null;

function describe(tid, body, cwd) {
  const name = /^(\w+)\(/.exec(body)[1];
  const { args, rest } = splitArgs(body);
  const call = {
    tid, name, text: body,
    ok: !/^\s*=\s*(-1|\?)/.test(rest),
    injected: /\(INJECTED\)/.test(rest),
    mark: args.map(quoted).map(path => /^\/chronicle-mark\/(.+)$/.exec(path ?? '')?.[1]).find(Boolean) ?? null,
    paths: [],
  };
  if (BY_DESCRIPTOR.has(name)) {
    call.paths = [decorated(args[0])];
    call.undecorated = call.paths[0] === null && /^\d+$/.test(args[0] ?? '');
  }
  else if (name === 'rename') call.paths = [resolved(null, args[0], cwd), resolved(null, args[1], cwd)];
  else if (name === 'renameat' || name === 'renameat2') call.paths = [resolved(args[0], args[1], cwd), resolved(args[2], args[3], cwd)];
  else if (name === 'open' || name === 'creat' || name === 'mkdir' || name === 'unlink' || name === 'rmdir') {
    call.paths = [resolved(null, args[0], cwd)];
  } else if (['openat', 'openat2', 'mkdirat', 'unlinkat', 'statx', 'newfstatat'].includes(name)) {
    call.paths = [resolved(args[0], args[1], cwd)];
  }
  call.flags = name === 'creat' ? 'O_CREAT' : name === 'open' ? args[1] ?? '' : name.startsWith('openat') ? args[2] ?? '' : '';
  return call;
}

function calls(text, cwd) {
  const pending = new Map();
  const out = [];
  for (const line of text.split('\n')) {
    const match = /^(\d+)\s+(.*)$/.exec(line);
    if (!match) continue;
    const tid = match[1];
    let body = match[2];
    const resumed = /^<\.\.\. \w+ resumed>(.*)$/s.exec(body);
    if (resumed) {
      const start = pending.get(tid);
      pending.delete(tid);
      if (start === undefined) continue;
      body = start + resumed[1];
    }
    if (body.endsWith(' <unfinished ...>')) {
      pending.set(tid, body.slice(0, -' <unfinished ...>'.length));
      continue;
    }
    if (/^\w+\(/.test(body)) out.push(describe(tid, body, cwd));
  }
  for (const [tid, body] of pending) out.push(describe(tid, body, cwd));
  return out;
}

function requestsOfCall(text, store, cwd) {
  const all = calls(text, cwd);
  const counts = new Map();
  const existing = new Set();
  const perName = new Map();
  const requests = [];
  let inCall = false;
  for (const call of all) {
    const key = `${call.tid} ${call.name}`;
    counts.set(key, (counts.get(key) ?? 0) + 1);
    call.when = counts.get(key);
    call.main = call.tid === all[0].tid;
    if (call.mark === 'call-begin') inCall = true;
    if (call.mark === 'call-end') inCall = false;
    call.inCall = inCall;
    const [path, target] = call.paths;
    const made = OPENS.has(call.name) && call.ok && call.flags.includes('O_CREAT') && !existing.has(path);
    if (inCall && (isUnder(path, store) || isUnder(target, store) || holds(path, store))) {
      const ordinal = (perName.get(call.name) ?? 0) + 1;
      perName.set(call.name, ordinal);
      requests.push({ id: `${call.name} ${ordinal}`, name: call.name, when: call.when, path, made, call });
    }
    if (call.ok && (OPENS.has(call.name) || MKDIRS.has(call.name) || call.name === 'statx' || call.name === 'newfstatat')) existing.add(path);
    if (call.ok && RENAMES.has(call.name)) {
      existing.delete(path);
      existing.add(target);
    }
    if (call.ok && (call.name === 'unlink' || call.name === 'unlinkat' || call.name === 'rmdir')) existing.delete(path);
  }
  const undecorated = all.filter(call => call.undecorated && call.inCall);
  return { all, requests, undecorated };
}

const firstRead = requests => requests.filter(({ name }) => READS.has(name)).slice(0, 1);

function writePoints(requests) {
  const writes = requests.filter(({ name }) => WRITES.has(name));
  const ends = new Set(writes.flatMap(({ path }) => {
    const ofPath = writes.filter(write => write.path === path);
    return [ofPath[0], ofPath[ofPath.length - 1]];
  }));
  return requests.filter(request => ends.has(request) || SYNCS.has(request.name) || RENAMES.has(request.name)
    || MKDIRS.has(request.name) || request.made);
}

const errnoOf = name => (WRITES.has(name) ? 'ENOSPC' : 'EIO');
const where = (point, store) => (holds(point.path, store) ? point.path : posix.relative(posix.dirname(store), point.path ?? store));

let active = 0;
const queue = [];
async function pooled(work) {
  while (active >= Math.max(1, availableParallelism())) await new Promise(resolve => queue.push(resolve));
  active++;
  try {
    return await work();
  } finally {
    active--;
    queue.shift()?.();
  }
}

function child(command, args, cwd, childEnv = env) {
  return pooled(() => new Promise(resolve => {
    const started = Date.now();
    const proc = spawn(command, args, { cwd, env: childEnv, detached: true });
    let timedOut = false;
    const bound = setTimeout(() => {
      timedOut = true;
      try {
        process.kill(-proc.pid, 'SIGKILL');
      } catch (error) {
        if (error.code !== 'ESRCH') throw error;
      }
      proc.stdout.destroy();
      proc.stderr.destroy();
    }, 20_000);
    const out = [];
    const err = [];
    proc.stdout.on('data', chunk => out.push(chunk));
    proc.stderr.on('data', chunk => err.push(chunk));
    proc.on('error', error => {
      clearTimeout(bound);
      resolve({ status: null, signal: null, timedOut, spawnError: error.message, ms: Date.now() - started, stdout: '', stderr: '' });
    });
    proc.on('close', (status, signal) => {
      clearTimeout(bound);
      resolve({ status, signal, timedOut, ms: Date.now() - started, stdout: Buffer.concat(out).toString('utf8'), stderr: Buffer.concat(err).toString('utf8') });
    });
  }));
}

let base;
let counter = 0;

async function traced(scenario, extra, inject) {
  const dir = join(base, `${scenario.name}-${String(++counter).padStart(4, '0')}`);
  const root = join(dir, 'root');
  const outside = join(dir, 'outside');
  mkdirSync(root, { recursive: true });
  mkdirSync(outside);
  const record = join(dir, 'record');
  const store = join(root, 'store');
  const injection = inject ? ['-e', `inject=${inject.name}:error=${inject.errno}:when=${inject.when}`] : [];
  const paths = inject?.path ? [join(store, inject.path), ...inject.marks.map(label => `/chronicle-mark/${label}`)] : [];
  const result = await child('strace', ['-f', '-y', '-o', record, '-e', `trace=${CALLS}`, ...paths.flatMap(path => ['-P', path]), ...injection,
    process.execPath, '--single-threaded', WRITER, 'run', scenario.file, root, outside, ...extra], dir,
  inject ? { ...env, CHRONICLE_LATE_AIMED: '1' } : env);
  let out = null;
  try {
    out = JSON.parse(result.stdout);
  } catch {}
  let text = '';
  try {
    text = readFileSync(record, 'utf8');
  } catch {}
  return { dir, root, store, copy: join(outside, 'copy'), thrown: join(outside, 'thrown'), result, out, text, inject };
}

function brokenRun(run) {
  if (run.result.spawnError) return `the writer did not start: ${run.result.spawnError}`;
  if (run.result.timedOut) return 'the traced run did not end and close its output within 20 s';
  if (run.result.signal) return `the writer ended by ${run.result.signal}`;
  if (run.result.status !== 0 || run.out === null) {
    const tail = run.result.stderr.trim().split('\n').slice(-3).join(' | ');
    return `the writer ended with status ${run.result.status}: ${tail}`;
  }
  return null;
}

function cleanProblem(kind, clean) {
  const broken = brokenRun(clean);
  if (broken) return `the clean run failed: ${broken}`;
  const { out } = clean;
  if (!out.call.ok) return `the clean run's call threw: ${out.call.message}`;
  return kind.cleanProblem(out);
}

function countBefore(record, name) {
  const at = record.all.findIndex(({ mark }) => mark === 'begin');
  if (at < 0) return null;
  const before = record.all.slice(0, at);
  if (before.some(({ injected }) => injected)) return null;
  return before.filter(call => call.main && call.name === name).length;
}

function nextAim(point, missed, aimed, clean) {
  const refused = missed.all.filter(({ injected }) => injected);
  const inClean = countBefore(clean, point.name);
  if (!point.call.main || refused.length !== 1 || !refused[0].main || inClean === null) return null;
  const counts = [missed, ...aimed.filter(record => record !== missed), clean]
    .map(record => countBefore(record, point.name)).filter(count => count !== null);
  const votes = count => counts.filter(other => other === count).length;
  const count = counts.reduce((best, other) => (votes(other) > votes(best) ? other : best));
  const placeInClean = request => request.when - inClean;
  const own = countBefore(missed, point.name);
  const index = call => missed.all.indexOf(call);
  const callBegin = missed.all.findIndex(({ mark }) => mark === 'call-begin');
  if (own === null || callBegin < 0 || index(refused[0]) < callBegin) return count + placeInClean(point);
  const target = missed.requests.find(({ id }) => id === point.id);
  const ofStore = missed.requests.some(({ call }) => call === refused[0]);
  if (target && (!ofStore || index(target.call) < index(refused[0]))) return target.call.main ? count + target.when - own : null;
  const before = missed.requests.filter(({ name, call }) => name === point.name && index(call) < index(refused[0])).length;
  const next = clean.requests.filter(({ name }) => name === point.name)[before];
  if (!next) return null;
  return count + placeInClean(point) + refused[0].when - own + (ofStore ? 0 : 1) - placeInClean(next);
}

function hit(run) {
  const record = requestsOfCall(run.text, run.store, run.dir);
  const refused = record.all.filter(call => call.injected);
  if (refused.length !== 1) return { why: `the record shows ${refused.length} refused requests`, record };
  const request = record.requests.find(({ call }) => call === refused[0]);
  if (!request) return { why: `the refused request is neither at or under the store's directory nor of a directory that holds it, in the call: ${refused[0].text}`, record };
  return { id: request.id, request, record };
}

function fileAim(point, clean) {
  const onFile = call => call.tid === point.call.tid && call.name === point.name && call.paths[0] === point.path;
  const upTo = clean.all.slice(0, clean.all.indexOf(point.call) + 1).filter(onFile);
  return { path: posix.relative(clean.store, point.path), when: upTo.length, inCall: upTo.filter(call => call.inCall).length };
}

function fileHit(run, point, aim) {
  const record = requestsOfCall(run.text, run.store, run.dir);
  const refused = record.all.filter(call => call.injected);
  if (refused.length !== 1) return { why: `the record shows ${refused.length} refused requests`, record };
  const path = join(run.store, aim.path);
  const onFile = call => call.inCall && call.tid === refused[0].tid && call.name === point.name && call.paths[0] === path;
  if (!onFile(refused[0])) return { why: `the refused call is no ${point.name} of ${aim.path} in the call: ${refused[0].text}`, record };
  const number = record.all.slice(0, record.all.indexOf(refused[0]) + 1).filter(onFile).length;
  if (number !== aim.inCall) return { why: `it refused ${point.name} ${number} of ${aim.path} in the call, where the target is ${point.name} ${aim.inCall}`, record };
  return { id: point.id, request: record.requests.find(({ call }) => call === refused[0]), record };
}

async function aimAll(scenario, kind, clean, extra) {
  const points = kind.points(clean.requests);
  const counted = new Map(points.map(point => [point.id, []]));
  const misses = new Map(points.map(point => [point.id, []]));
  const byFile = new Map(kind.aimsByFile ? points.map(point => [point.id, fileAim(point, clean)]) : []);
  const aims = new Map(points.map(point => [point.id, byFile.get(point.id)?.when ?? point.when]));
  const records = [];
  let aimed = 0;
  let slowest = 0;
  for (let round = 0; round < 3; round++) {
    const open = points.filter(point => counted.get(point.id).length === 0);
    if (open.length === 0) break;
    aimed += open.length;
    const runs = await Promise.all(open.map(point => traced(scenario, extra, {
      name: point.name, errno: errnoOf(point.name), when: aims.get(point.id),
      ...(byFile.has(point.id) && { path: byFile.get(point.id).path, marks: clean.out.marks }),
    })));
    const found = runs.map((run, index) => (byFile.has(open[index].id) ? fileHit(run, open[index], byFile.get(open[index].id)) : hit(run)));
    records.push(...found.map(({ record }) => record));
    slowest = Math.max(slowest, ...runs.map(run => run.result.ms));
    runs.forEach((run, index) => {
      const point = open[index];
      const { id, why, request, record } = found[index];
      if (id && counted.has(id)) counted.get(id).push({ ...run, errno: errnoOf(request.name), refused: request.path });
      if (id === point.id) return;
      const next = byFile.has(point.id) ? null : nextAim(point, record, records, clean);
      misses.get(point.id).push(`${why ?? `it refused ${id}`}${next === null ? `, and the aim stays at ${aims.get(point.id)}` : ''}`);
      if (next !== null) aims.set(point.id, next);
    });
  }
  return { points, counted, misses, aimed, slowest };
}

async function reopened(scenario, kind, extra, runs) {
  const layout = runs.map(run => kind.stores(run));
  const paths = runs.flatMap((run, index) => layout[index].map(where => run[where]));
  if (paths.length === 0) return { each: layout.map(() => ({})) };
  const result = await child(process.execPath, [WRITER, 'reopen', scenario.file, extra[0] ?? '-', ...paths], base);
  if (result.status !== 0) throw new Error(`the reopening process ended with status ${result.status}: ${result.stderr.trim().split('\n').slice(-3).join(' | ')}`);
  const { answers, old } = JSON.parse(result.stdout);
  return {
    old,
    each: layout.map(wheres => Object.fromEntries(wheres.map(where => [where, answers.shift()]))),
  };
}

function errnoCode(failure) {
  if (typeof failure.code === 'string' && /^E[A-Z0-9]+$/.test(failure.code)) return failure.code;
  const match = failure.error && /^IO error: .*\(os error (\d+)\)$/s.exec(failure.message);
  if (match) return Object.keys(constants.errno).find(code => constants.errno[code] === Number(match[1]));
}

const shown = outcome => (outcome.ok ? JSON.stringify(outcome.value) : `a throw (${outcome.code}: ${outcome.message})`);

function barrierProblems(run, store, barriers) {
  const trace = readTrace(run.text, { root: run.root, cwd: run.dir, names: [run.root] });
  const found = trace.marks.map(({ name }) => name);
  if (!isDeepStrictEqual(found, run.out.marks)) return [{ text: `${NOT_MEASURED}: the record's marks are not the writer's marks` }];
  if (trace.problems.length > 0) {
    return [{ text: `the record cannot be judged: ${trace.problems.slice(0, 3).map(({ why, call }) => `${why}: ${call}`).join(' | ')}` }];
  }
  const problems = [];
  for (const barrier of barriers) {
    const mark = trace.marks.find(({ name }) => name === `${barrier}-end`);
    for (const path of mark.files.filter(each => isUnder(each, store))) {
      problems.push({ text: `${barrier} returned before the store asked to put the file ${posix.relative(store, path)} on disk` });
    }
    for (const path of mark.names.filter(each => isUnder(each, run.root))) {
      problems.push({ text: `${barrier} returned before the store asked to put the name ${posix.relative(run.root, path)} on disk` });
    }
  }
  for (const { from, to } of trace.unsyncedRenames.filter(each => isUnder(each.from, store) || isUnder(each.to, store))) {
    problems.push({ text: `the store renamed ${posix.relative(store, from)} to ${posix.relative(store, to)} before it asked to put it on disk` });
  }
  return problems;
}

const RULES = ['the call', 'the errno', 'the reads', 'the second try', 'the barrier', 'close and isClosed', 'the reopen'];

const oldOrNew = (facts, name, value) => isDeepStrictEqual(value, facts.old[name].value) || isDeepStrictEqual(value, facts.fresh[name].value);

function oneState(facts, what, reads) {
  const given = Object.entries(reads).filter(([, read]) => read.ok);
  const asOld = given.filter(([name, read]) => isDeepStrictEqual(read.value, facts.old[name].value)).map(([name]) => name);
  const asNew = given.filter(([name, read]) => isDeepStrictEqual(read.value, facts.fresh[name].value)).map(([name]) => name);
  if (asOld.length < given.length && asNew.length < given.length) {
    return [{ text: `${what} are no one state: ${given.map(([name]) => `${name} ${asNew.includes(name) ? 'new' : asOld.includes(name) ? 'old' : 'neither'}`).join(', ')}` }];
  }
  return [];
}

function whole(facts, what, reads, mustBeNew) {
  const problems = [];
  for (const [name, read] of Object.entries(reads)) {
    if (!read.ok) {
      if (mustBeNew) problems.push({ text: `${what}, ${name} threw after the call returned: ${read.message}` });
    } else if (mustBeNew ? !isDeepStrictEqual(read.value, facts.fresh[name].value) : !oldOrNew(facts, name, read.value)) {
      problems.push({ text: `${what}, ${name} gave ${JSON.stringify(read.value)}, which is not the ${mustBeNew ? '' : 'old or the '}new value ${JSON.stringify(facts.fresh[name].value)}` });
    }
  }
  return problems;
}

const lastGiven = (name, batches) => {
  const reads = batches.filter(Boolean).map(batch => batch[name]);
  return reads.filter(read => read.ok).at(-1) ?? reads.at(-1);
};

const theErrno = {
  rule: 'the errno',
  judge: ({ out, errno }) => (!out.call.ok && out.call.code !== 'DurabilityLost' && errnoCode(out.call) !== errno ? [{
    text: `the call threw without ${errno}: ${shown(out.call)}`,
  }] : []),
};

const theReads = {
  rule: 'the reads',
  judge: facts => {
    const { out } = facts;
    const problems = [
      ...whole(facts, 'the reads after the call', out.reads[0], facts.returned),
      ...whole(facts, 'the second reads after the call', out.reads[1], facts.returned),
      ...oneState(facts, 'the reads after the call', out.reads[0]),
      ...oneState(facts, 'the second reads after the call', out.reads[1]),
    ];
    if (out.syncReads) {
      problems.push(...whole(facts, 'the reads after sync', out.syncReads, false));
      problems.push(...oneState(facts, 'the reads after sync', out.syncReads));
    }
    for (const name of Object.keys(out.reads[0])) {
      if (!isDeepStrictEqual(out.reads[0][name], out.reads[1][name])) {
        problems.push({ text: `two reads of ${name} disagree: ${shown(out.reads[0][name])} and ${shown(out.reads[1][name])}` });
      }
    }
    const unbroken = out.retry ? [['the reads after the same call again', out.retryReads], ['the reads after sync', out.syncReads]]
      : [['the reads after the call', out.reads[0]], ['the second reads after the call', out.reads[1]], ['the reads after sync', out.syncReads]];
    const joined = Object.fromEntries(unbroken.filter(([, reads]) => reads).flatMap(([what, reads]) => Object.entries(reads)
      .map(([name, read]) => [`${name} in ${what}`, read])));
    const asOf = key => key.slice(0, key.indexOf(' in '));
    const given = Object.entries(joined).filter(([, read]) => read.ok);
    const asOldAll = given.filter(([key, read]) => isDeepStrictEqual(read.value, facts.old[asOf(key)].value));
    const asNewAll = given.filter(([key, read]) => isDeepStrictEqual(read.value, facts.fresh[asOf(key)].value));
    if (asOldAll.length < given.length && asNewAll.length < given.length) {
      problems.push({ text: `the handle's reads ${out.retry ? 'from the second try on' : 'after the call'} are no one state: ${given.map(([key]) => `${key} ${asNewAll.some(([each]) => each === key) ? 'new' : asOldAll.some(([each]) => each === key) ? 'old' : 'neither'}`).join(', ')}` });
    }
    return problems;
  },
};

const theSecondTry = {
  rule: 'the second try',
  judge: facts => {
    const { out } = facts;
    const problems = [];
    if (out.retry && !out.retry.ok) {
      problems.push({ text: `the same call again threw: ${shown(out.retry)}` });
    }
    if (out.retry?.ok) problems.push(...whole(facts, 'the reads after the same call again', out.retryReads, true));
    if (out.retryReads) problems.push(...oneState(facts, 'the reads after the same call again', out.retryReads));
    return problems;
  },
};

const theBarrier = { rule: 'the barrier', judge: ({ run, barriers }) => barrierProblems(run, run.store, barriers) };

const closeAndIsClosed = {
  rule: 'close and isClosed',
  judge: ({ out, clean, closing, closedReads }) => {
    const problems = [];
    const add = text => problems.push({ text });
    const closedCode = Object.values(clean.out.closedReads ?? clean.out.reads[0]).find(read => !read.ok)?.code;
    if (closing.ok) {
      if (!out.isClosed.ok || out.isClosed.value !== true) add(`after close returned, isClosed gave ${shown(out.isClosed)}`);
      for (const [name, read] of Object.entries(closedReads)) {
        if (read.ok || read.code !== closedCode) add(`after close returned, ${name} gave ${shown(read)}, not a throw as on a closed handle`);
      }
    } else if (!out.isClosed.ok || typeof out.isClosed.value !== 'boolean') {
      add(`after close threw, isClosed gave ${shown(out.isClosed)}`);
    } else {
      for (const [name, read] of Object.entries(closedReads)) {
        if (out.isClosed.value && (read.ok || read.code !== closedCode)) {
          add(`after close threw and isClosed answered true, ${name} gave ${shown(read)}, not a throw as on a closed handle`);
        }
        if (!out.isClosed.value && !read.ok) add(`after close threw and isClosed answered false, ${name} threw: ${read.message}`);
      }
    }
    return problems;
  },
};

const theReopen = {
  rule: 'the reopen',
  judge: facts => {
    const { out, clean, reopen, barriers, handleReads, storeReads } = facts;
    const problems = [];
    const add = text => problems.push({ text });
    const before = { copy: barriers.filter(barrier => barrier !== 'close'), store: barriers, thrown: [] };
    const lastRead = {
      thrown: name => lastGiven(name, out.reads),
      copy: name => lastGiven(name, handleReads),
      store: name => lastGiven(name, storeReads),
    };
    for (const [where, answer] of Object.entries(reopen)) {
      const which = { copy: 'the copy', store: 'the closed store', thrown: 'the copy made after the throw' }[where];
      if (!answer.opened.ok) {
        add(`${which} did not open: ${answer.opened.message}`);
        continue;
      }
      const passed = before[where].length > 0;
      problems.push(...oneState(facts, `${which}'s reads, opened again,`, answer.reads));
      if (answer.probe) {
        const [oldProbe, newProbe] = [clean.probeOld.value, clean.answer.copy.probe.value];
        const shows = `${which}, opened again, shows ${shown(answer.probe)} operations since a snapshot after five appends`;
        if (!answer.probe.ok || ![oldProbe, newProbe].includes(answer.probe.value)) {
          add(`${shows}, which is neither the old cadence's ${oldProbe} nor the new cadence's ${newProbe}`);
        } else if (passed && (out.call.ok || out.retry?.ok) && answer.probe.value !== newProbe) {
          add(`${shows}: the call returned and ${before[where].join(' and ')} returned, yet the new cadence's count is ${newProbe}`);
        }
      }
      for (const [name, read] of Object.entries(answer.reads)) {
        const handle = lastGiven(name, handleReads);
        if (!read.ok) {
          add(`${which}, opened again, threw for ${name}: ${read.message}`);
        } else if (passed && handle.ok && !isDeepStrictEqual(read.value, handle.value)) {
          add(`${which}, opened again, gave ${JSON.stringify(read.value)} for ${name}, where the handle gave ${JSON.stringify(handle.value)} at ${before[where].join(' and ')}`);
        } else if (!oldOrNew(facts, name, read.value)) {
          add(`${which}, opened again, gave ${JSON.stringify(read.value)} for ${name}, which is not the old or the new value`);
        } else if (!isDeepStrictEqual(read.value, facts.old[name].value)) {
          const last = lastRead[where](name);
          if (!last.ok || !isDeepStrictEqual(last.value, read.value)) {
            problems.push({
              text: `${which}, opened again, gave the new value for ${name}, where the handle last gave ${shown(last)}`,
              thrownCopyHoldsChange: where === 'thrown',
            });
          }
        }
      }
    }
    return problems;
  },
};

const oldAfterThrow = method => ({
  rule: 'the reads',
  judge: ({ out, old }) => {
    if (out.call.ok || (method !== undefined && out.method !== method)) return [];
    const problems = [];
    for (const [what, reads] of [['the reads after the call', out.reads[0]], ['the second reads after the call', out.reads[1]]]) {
      for (const [name, read] of Object.entries(reads)) {
        if (read.ok && !isDeepStrictEqual(read.value, old[name].value)) {
          problems.push({ text: `${what}, ${name} gave ${JSON.stringify(read.value)} after the call threw, not the old value` });
        }
      }
    }
    return problems;
  },
});

const callOf = run => {
  const all = calls(run.text, run.dir);
  return all.slice(all.findIndex(({ mark }) => mark === 'call-begin'), all.findIndex(({ mark }) => mark === 'call-end'));
};

const ofTheNamesPass = run => {
  if (holds(run.refused, run.store) || isUnder(run.refused, `${run.store}/blobs`)) return true;
  if (run.refused !== run.store) return false;
  const inCall = callOf(run);
  const refusedAt = inCall.findIndex(call => !call.ok && call.paths[0] === run.store);
  return refusedAt > 0 && inCall.slice(0, refusedAt).some(call => SYNCS.has(call.name) && holds(call.paths[0], run.store));
};

const aRefusedNameSyncThrows = {
  rule: 'the call',
  judge: ({ out, run }) => (ofTheNamesPass(run) && out.call.ok
    ? [{ text: `the call returned although the sync of ${run.refused} was refused` }] : []),
};

const theSecondTrySyncsItAgain = {
  rule: 'the second try',
  judge: ({ out, run }) => {
    if (!ofTheNamesPass(run) || out.call.ok) return [];
    if (!out.retry) return [{ text: `the reads after the call didn't give the old values, so no second try could sync ${run.refused}` }];
    const all = calls(run.text, run.dir);
    const begin = all.findIndex(({ mark }) => mark === 'retry-begin');
    const end = all.findIndex(({ mark }) => mark === 'retry-end');
    const again = all.slice(begin, end).some(call => SYNCS.has(call.name) && call.ok && call.paths[0] === run.refused);
    return again ? [] : [{ text: `the second try returned without syncing ${run.refused}, whose sync the call had refused` }];
  },
};

const theMessageNamesTheDirectory = {
  rule: 'the call',
  judge: ({ out, run }) => (!out.call.ok && [run.store, `${run.store}/blobs`].includes(run.refused)
    && !out.call.message.includes(`cannot sync ${run.refused},`)
    ? [{ text: `the call threw without naming ${run.refused}, whose sync was refused: ${out.call.message}` }] : []),
};

const theStoreNameWaits = {
  rule: 'the barrier',
  judge: ({ run }) => {
    const trace = readTrace(run.text, { root: run.root, cwd: run.dir, names: [run.root] });
    const begin = trace.marks.find(({ name }) => name === 'call-begin');
    return begin?.names.includes(run.store) ? []
      : [{ text: `${NOT_MEASURED}: before the call, the store's name was not waiting to be put on disk, so the call had no name of it to pay` }];
  },
};

const coldCall = {
  rule: 'the call',
  judge: ({ out }) => (out.call.ok ? [{ text: `the read returned ${JSON.stringify(out.call.value)}, and it did not throw` }] : []),
};
const coldErrno = {
  rule: 'the errno',
  judge: ({ out, errno }) => (!out.call.ok && errnoCode(out.call) !== errno ? [{ text: `the read threw without ${errno}: ${out.call.message}` }] : []),
};
const coldSecondTry = {
  rule: 'the second try',
  judge: ({ out }) => (!out.retry.ok || !isDeepStrictEqual(out.retry.value, out.expected.value)
    ? [{ text: `the same read again gave ${shown(out.retry)}, not ${shown(out.expected)}` }] : []),
};

function handleFacts(clean, run, reopen, { fresh, returned, steps, closing, closedReads, handleReads, storeReads }) {
  const { out } = run;
  const barriers = [...steps, ...(out.sync?.ok ? ['sync'] : []), ...(out.close?.ok ? ['close'] : [])];
  return { out, run, clean, reopen, errno: run.errno, old: clean.out.old, fresh, returned, barriers, closing, closedReads, handleReads, storeReads };
}

const afterCall = (clean, { out }) => ({
  fresh: clean.out.reads[0],
  returned: out.call.ok,
  closing: out.close,
  closedReads: out.closedReads,
  handleReads: [out.reads[0], out.reads[1], out.retryReads, out.syncReads],
  storeReads: [out.reads[0], out.reads[1], out.retryReads, out.syncReads, out.closedReads],
});

const handleStores = run => ['copy', 'store', ...(run.out && !run.out.call.ok ? ['thrown'] : [])];

function readsThrew(out) {
  const threw = Object.entries(out.old).filter(([, read]) => !read.ok).map(([name, read]) => `${name}: ${read.message}`);
  return threw.length > 0 ? `the clean run's reads threw: ${threw.join('; ')}` : null;
}

function reopensAsTheHandle(result) {
  const reopenProblems = Object.entries(result.cleanAnswer).filter(([, answer]) => !answer.opened.ok
    || !isDeepStrictEqual(answer.reads, result.clean.out.syncReads ?? result.clean.out.old));
  assert.deepEqual(reopenProblems.map(([where]) => where), [], 'without a fault, a store opened again reads what the handle last read');
  const { probeOld, answer } = result.clean;
  if (probeOld) {
    assert.ok(probeOld.ok && answer.copy.probe.ok && probeOld.value !== answer.copy.probe.value,
      `${NOT_MEASURED}: the probe gives ${shown(probeOld)} without the call and ${shown(answer.copy.probe)} after it`);
  }
}

const NO_WRITE_POINT = 'in a clean run, the call makes no request at or under the store that is a fault point';
const noExtra = async () => ({ extra: [] });

const writeKind = {
  name: 'write',
  points: writePoints,
  find: noExtra,
  cleanProblem: readsThrew,
  cleanRequests: () => null,
  stores: handleStores,
  noPoints: NO_WRITE_POINT,
  cleanAnswers: reopensAsTheHandle,
  facts: (clean, run, reopen) => handleFacts(clean, run, reopen, { ...afterCall(clean, run), steps: [] }),
  rules: [theErrno, theReads, theSecondTry, theBarrier, closeAndIsClosed, theReopen],
};

const syncKind = {
  name: 'sync',
  points: writePoints,
  find: noExtra,
  cleanProblem: readsThrew,
  cleanRequests: () => null,
  stores: handleStores,
  noPoints: NO_WRITE_POINT,
  cleanAnswers: reopensAsTheHandle,
  rules: [theErrno, theReads, theSecondTry, theBarrier, closeAndIsClosed, theReopen],
  facts: (clean, run, reopen) => handleFacts(clean, run, reopen, {
    ...afterCall(clean, run),
    steps: [...(run.out.call.ok ? ['call'] : []), ...(run.out.retry?.ok ? ['retry'] : [])],
  }),
};

const closeKind = {
  name: 'close',
  points: writePoints,
  find: noExtra,
  cleanProblem: readsThrew,
  cleanRequests: () => null,
  stores: () => ['store'],
  noPoints: NO_WRITE_POINT,
  cleanAnswers: reopensAsTheHandle,
  rules: [theErrno, theReads, theSecondTry, theBarrier, closeAndIsClosed, theReopen],
  facts: (clean, run, reopen) => {
    const { out } = run;
    return handleFacts(clean, run, reopen, {
      fresh: clean.out.old,
      returned: false,
      steps: [...(out.call.ok ? ['call'] : []), ...(out.retry?.ok ? ['retry'] : [])],
      closing: out.call,
      closedReads: out.reads[0],
      handleReads: [out.old],
      storeReads: [out.old, ...out.reads],
    });
  },
};

const longKind = {
  name: 'long',
  points: writePoints,
  cleanProblem: readsThrew,
  stores: handleStores,
  noPoints: NO_WRITE_POINT,
  cleanAnswers: reopensAsTheHandle,
  facts: (clean, run, reopen) => handleFacts(clean, run, reopen, { ...afterCall(clean, run), steps: [] }),
  rules: [theErrno, theReads, theSecondTry, theBarrier, closeAndIsClosed, theReopen],
  find: async scenario => {
    const found = await traced(scenario, [], null);
    const broken = brokenRun(found);
    if (broken) return { failure: `the run that finds the append failed: ${broken}` };
    let inAppend = null;
    for (const call of calls(found.text, found.dir)) {
      const begin = /^append-(\d+)-begin$/.exec(call.mark ?? '');
      if (begin) inAppend = Number(begin[1]);
      else if (call.mark) inAppend = null;
      else if (inAppend !== null && !WRITES.has(call.name) && call.name !== 'lseek'
        && call.paths.some(path => isUnder(path, found.store))) {
        return { extra: [String(inAppend)] };
      }
    }
    return { failure: `no append of the ${scenario.appends} shows a request that is no write and no seek` };
  },
  cleanRequests: (requests, extra) => (requests.every(({ name }) => WRITES.has(name) || name === 'lseek')
    ? `the clean run's append ${extra[0]} shows no request that is no write and no seek` : null),
};

const coldKind = {
  name: 'cold',
  aimsByFile: true,
  points: firstRead,
  find: noExtra,
  cleanProblem: out => (out.retry.ok ? null : `the clean run's second call threw: ${out.retry.message}`),
  cleanRequests: () => null,
  stores: () => [],
  noPoints: 'in a clean run, the read makes no read request at or under the store',
  cleanAnswers: () => {},
  facts: (clean, run) => ({ out: run.out, errno: run.errno }),
  rules: [coldCall, coldErrno, coldSecondTry],
};

function judge(kind, rules, clean, run, reopen) {
  const problems = new Map(RULES.map(rule => [rule, []]));
  const broken = brokenRun(run);
  if (broken) {
    problems.get('the call').push({ text: broken });
    return problems;
  }
  const facts = kind.facts(clean, run, reopen);
  for (const { rule, judge: judged } of [...kind.rules, ...rules]) problems.get(rule).push(...judged(facts));
  return problems;
}

async function scenarioResult(scenario, kind) {
  const { extra, failure } = await kind.find(scenario);
  if (failure) return { failure };
  const clean = await traced(scenario, extra, null);
  const problem = cleanProblem(kind, clean);
  if (problem) return { failure: problem };
  Object.assign(clean, requestsOfCall(clean.text, clean.store, clean.dir));
  if (clean.undecorated.length > 0) {
    return { failure: `${NOT_MEASURED}: in the clean run, the call makes requests whose descriptors the record does not name: ${clean.undecorated.slice(0, 3).map(({ text }) => text).join(' | ')}` };
  }
  const marks = clean.all.filter(({ mark }) => mark).map(({ mark }) => mark);
  if (!isDeepStrictEqual(marks, clean.out.marks)) {
    return { failure: `${NOT_MEASURED}: the clean run's record shows the marks [${marks.join(', ')}], not the writer's [${clean.out.marks.join(', ')}]` };
  }
  const noRequest = kind.cleanRequests(clean.requests, extra);
  if (noRequest) return { failure: noRequest };
  const { points, counted, misses, aimed, slowest } = await aimAll(scenario, kind, clean, extra);
  const runs = [...counted.values()].flat();
  const { old: probeOld, each: answers } = await reopened(scenario, kind, extra, [clean, ...runs]);
  const cleanAnswer = answers.shift();
  Object.assign(clean, { answer: cleanAnswer, probeOld });
  return { clean, points, counted, misses, aimed, slowest: Math.max(slowest, clean.result.ms), answers: new Map(runs.map((run, index) => [run, answers[index]])), cleanAnswer, extra };
}

const asText = value => (typeof value === 'function' ? String(value) : value);

function refused(scenario, kind, { rules = [], todos = [], allows = [], firstAim } = {}) {
  return async t => {
    const file = join(base, `${scenario.name}.json`);
    writeFileSync(file, JSON.stringify({
      ...Object.fromEntries(Object.entries(scenario).map(([field, value]) => [field, asText(value)])),
      kind: kind.name,
    }));
    let result;
    try {
      result = await scenarioResult({ ...scenario, file }, kind);
    } catch (error) {
      result = { failure: error.stack };
    }
    if (result.failure) assert.fail(result.failure);
    const lost = todos.filter(each => each.point !== undefined && !result.points.some(point => point.id === each.point));
    assert.deepEqual(lost.map(({ point, rule }) => `${point}, ${rule}`), [], 'a todo names a fault point that the call no longer has');
    kind.cleanAnswers(result);
    if (result.points.length === 0) {
      assert.ok(!firstAim, `${kind.noPoints}, so the control has nothing to aim at`);
      t.skip(kind.noPoints);
      return;
    }
    t.diagnostic(`${result.points.length} fault points, ${result.aimed} aimed runs, the slowest traced run ${result.slowest} ms`);
    for (const point of result.points) {
      await t.test(`with ${point.id} of the call refused`, async t => {
        const runs = result.counted.get(point.id);
        const at = where(point, result.clean.store);
        if (runs.length === 0) {
          assert.fail(`${NOT_MEASURED}: three aimed runs did not refuse ${point.id} (${at}): ${result.misses.get(point.id).join('; ')}`);
        }
        const missed = result.misses.get(point.id);
        assert.ok(firstAim !== 'miss' || missed.length > 0, `The first aimed run refused ${point.id}, so the control made no miss for the aim to correct`);
        assert.ok(firstAim !== 'hit' || missed.length === 0, `The first aimed run missed ${point.id}: ${missed.join('; ')}`);
        const judged = runs.map(run => [run, judge(kind, rules, result.clean, run, result.answers.get(run))]);
        const here = todos.filter(each => each.point === undefined || each.point === point.id);
        const ownedBy = (rule, problem) => here.find(each => each.rule === rule && each.owns(problem));
        const holds = keep => {
          for (const [run, problems] of judged) {
            const allowed = (rule, problem) => allows.some(each => each.rule === rule && each.owns(problem));
            const found = RULES.flatMap(rule => problems.get(rule).filter(problem => keep(rule, problem) && !allowed(rule, problem)).map(({ text }) => text));
            const gave = run.out ? `, the call gave ${shown(run.out.call)}` : '';
            assert.deepEqual(found, [], `With ${point.id} (${at}) refused${gave}: ${found.join(' | ')}`);
          }
        };
        if (here.length === 0) return holds(() => true);
        for (const each of here) await t.test(each.rule, { todo: each.todo }, () => holds((rule, problem) => ownedBy(rule, problem) === each));
        await t.test('the other rules', () => {
          const stale = here.filter(each => judged.every(([, problems]) => !problems.get(each.rule).some(problem => ownedBy(each.rule, problem) === each)));
          assert.deepEqual(stale.map(({ rule }) => rule), [], `The todo no longer fails here: ${stale.map(({ todo }) => todo).join(' | ')}`);
          holds((rule, problem) => !ownedBy(rule, problem));
        });
      });
    }
  };
}

suite('The aim after a missed run', () => {
  const STORE = '/t/root/store';
  const FDS = { log: `7<${STORE}/records.log>`, event: '19<anon_inode:[eventfd]>', out: '1<pipe:[42]>' };
  const MARKS = { start: 'begin', begin: 'call-begin', end: 'call-end' };
  const textOf = (steps, refusedAt = -1) => steps.map((step, index) => {
    const [what, tid = '1'] = step.split('@');
    if (MARKS[what]) {
      return `${tid} statx(AT_FDCWD, "/chronicle-mark/${MARKS[what]}", AT_STATX_SYNC_AS_STAT, STATX_ALL, 0x7ffd00) = -1 ENOENT (No such file or directory)`;
    }
    return `${tid} write(${FDS[what]}, "x", 1) = ${index === refusedAt ? '-1 ENOSPC (No space left on device) (INJECTED)' : '1'}`;
  }).join('\n');
  const recordOf = (steps, refusedAt = -1) => requestsOfCall(textOf(steps, refusedAt), STORE, '/t');
  const pointOf = (record, id) => record.requests.find(request => request.id === id);
  const aimAfter = (record, id, missed, others = []) => nextAim(pointOf(record, id), missed, [...others, missed], record);
  const clean = recordOf(['out', 'out', 'start', 'out', 'begin', 'log', 'log', 'log', 'event', 'end']);
  const short = recordOf(['out', 'start', 'out', 'begin', 'log', 'log', 'log', 'end']);

  test('After a refused call that is no request of the store, the next aim is where the next request of the store would stand', () => {
    assert.equal(pointOf(clean, 'write 1').when, 4);
    assert.equal(aimAfter(clean, 'write 1', recordOf(['out', 'out', 'start', 'out', 'begin', 'event'], 5)), 5);
    assert.equal(aimAfter(clean, 'write 1', recordOf(['out', 'out', 'start', 'out', 'begin', 'event', 'log', 'log', 'log', 'end'], 5)), 5);
  });

  test('After a refused request of the store, the next aim moves by that request\'s shift against the clean run', () => {
    assert.equal(pointOf(clean, 'write 3').when, 6);
    assert.equal(aimAfter(clean, 'write 3', recordOf(['out', 'out', 'start', 'out', 'begin', 'event', 'log', 'log'], 7)), 7);
  });

  test('After a refused request of the store that the store carried on from, the next aim moves by that request\'s shift', () => {
    assert.equal(aimAfter(clean, 'write 3', recordOf(['out', 'out', 'start', 'out', 'begin', 'event', 'log', 'log', 'out', 'log', 'end'], 7)), 7);
  });

  test('After an aim that overshot, the next aim is the place at which the target stands in the missed run', () => {
    assert.equal(aimAfter(clean, 'write 2', recordOf(['out', 'out', 'start', 'out', 'begin', 'log', 'log', 'log', 'end'], 7)), 5);
  });

  test('After one aimed run with fewer calls before the first mark, where the other runs agree with the clean run, the aim stays', () => {
    const other = recordOf(['out', 'out', 'start', 'out', 'begin', 'log', 'log', 'log', 'end'], 7);
    assert.equal(aimAfter(clean, 'write 1', recordOf(['out', 'start', 'out', 'begin', 'log', 'log', 'log', 'end'], 5), [other]), 4);
  });

  test('After aimed runs with more calls before the first mark than the clean run, every aim moves by the difference', () => {
    const first = recordOf(['out', 'out', 'start', 'out', 'begin', 'log'], 5);
    const second = recordOf(['out', 'out', 'start', 'out', 'begin', 'log', 'log'], 6);
    assert.equal(aimAfter(short, 'write 2', first, [second]), 5);
    assert.equal(aimAfter(short, 'write 3', second, [first]), 6);
  });

  test('When only the clean run and the missed runs vote, an odd aimed run and then a run refused before the call lead the third aim to the target', () => {
    const one = recordOf(['out', 'out', 'start', 'out', 'begin', 'log', 'log', 'end']);
    const odd = recordOf(['out', 'start', 'out', 'begin', 'log', 'log', 'end'], 5);
    assert.equal(aimAfter(one, 'write 1', odd), 3);
    assert.equal(aimAfter(one, 'write 1', recordOf(['out', 'out', 'start', 'out'], 3), [odd]), 4);
  });

  test('When only the clean run and the missed run vote, after an odd clean run the second aim is the target', () => {
    const one = recordOf(['out', 'start', 'out', 'begin', 'log', 'log', 'end']);
    assert.equal(pointOf(one, 'write 1').when, 3);
    assert.equal(aimAfter(one, 'write 1', recordOf(['out', 'out', 'start', 'out'], 3)), 4);
  });

  test('A run refused before the first mark gives no count, and the point takes the count from the other runs', () => {
    const one = recordOf(['out', 'start', 'out', 'begin', 'log', 'end']);
    const other = recordOf(['out', 'out', 'start', 'out', 'begin', 'log', 'end'], 5);
    assert.equal(aimAfter(one, 'write 1', recordOf(['out'], 0), [other]), 4);
  });

  test('A point aimed by its file is counted among the calls of its name on that file, from the start and inside the call', () => {
    const fileClean = { ...recordOf(['out', 'start', 'log', 'event', 'begin', 'log', 'event', 'log', 'end']), store: STORE };
    assert.deepEqual(fileAim(pointOf(fileClean, 'write 2'), fileClean), { path: 'records.log', when: 3, inCall: 2 });
  });

  test('A run aimed by its file counts only when it refused the target\'s number on that file inside the call', () => {
    const fileClean = { ...recordOf(['out', 'start', 'log', 'begin', 'log', 'log', 'end']), store: STORE };
    const point = pointOf(fileClean, 'write 2');
    const aim = fileAim(point, fileClean);
    const run = refusedAt => fileHit({ text: textOf(['start', 'log', 'begin', 'log', 'log', 'end'], refusedAt), store: STORE, dir: '/t' }, point, aim);
    assert.equal(run(4).id, 'write 2');
    assert.equal(run(3).why, 'it refused write 1 of records.log in the call, where the target is write 2');
    assert.match(run(1).why, /^the refused call is no write of records.log in the call/);
  });

  test('A point off the main thread, a refused call off it, or a target shown off it, keeps its aim', () => {
    assert.equal(aimAfter(clean, 'write 1', recordOf(['out', 'out', 'start', 'out', 'begin', 'event', 'log@2', 'end'], 5)), null);
    const threaded = recordOf(['out', 'start', 'begin', 'log@2', 'log@2', 'end']);
    assert.equal(aimAfter(threaded, 'write 1', recordOf(['out', 'start', 'begin', 'log@2', 'log@2', 'end'], 4)), null);
    assert.equal(aimAfter(clean, 'write 1', recordOf(['out', 'out', 'start', 'out', 'begin', 'log@2', 'log', 'log', 'log', 'end'], 5)), null);
  });
});

const notLinux = process.platform !== 'linux' && 'the record of system calls comes from strace, which is for Linux';
function escapedProblem() {
  let dir = tmpdir();
  try {
    dir = realpathSync(dir);
  } catch {}
  return /[\x00-\x1f\x7f-\uffff"\\<>]/.test(dir)
    ? `the temporary directory ${JSON.stringify(dir)} has characters that strace escapes in its record, and the harness reads paths unescaped`
    : null;
}
const missing = notLinux ? null : straceProblem() ?? escapedProblem();
const skip = notLinux || (missing !== null && !process.env.CI && missing);

if (skip) {
  test('A refused request in the middle of a call leaves the caller a throw or a whole value', { skip }, () => {});
} else if (missing !== null) {
  test('strace can trace the writer here', () => assert.fail(missing));
} else {
  base = realpathSync(mkdtempSync(join(tmpdir(), 'chronicle-late-failures-')));
  after(() => rmSync(base, { recursive: true, force: true }));
  suite('A refused request', { concurrency: true }, () => {
    test('A refused request in appendJson leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'appendJson',
        call: (s, f, h) => s.appendJson('journal', { n: 1 }),
        reads: h => ({ journal: h.journal }),
      }, writeKind));
    test('A refused request in appendManyToStateJsonWithIdentity leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'appendManyToStateJsonWithIdentity',
        call: (s, f, h) => s.appendManyToStateJsonWithIdentity('identified', [{ text: 'two' }, { text: 'three' }], 'id', 'sequence'),
        reads: h => ({ identified: h.identified }),
      }, writeKind));
    test('A refused request in appendToStateJson leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'appendToStateJson',
        call: (s, f, h) => s.appendToStateJson('messages', h.message('new', 1)),
        reads: h => ({ messages: h.messages }),
      }, writeKind, {
        rules: [oldAfterThrow()],
      }));
    test('A refused request in appendToStateJsonWithIdentity leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'appendToStateJsonWithIdentity',
        call: (s, f, h) => s.appendToStateJsonWithIdentity('identified', { text: 'new' }, 'id', 'sequence'),
        reads: h => ({ identified: h.identified }),
      }, writeKind));
    test('A refused request in compactState leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'compactState',
        setup: (s, f, h) => [1, 2, 3].forEach(n => s.appendToStateJson('messages', h.message('more', n))),
        call: (s, f, h) => s.compactState('messages'),
        reads: h => ({ messages: h.messages, length: s => s.getStateLen('messages') }),
      }, writeKind));
    test('A refused request in createBranch leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'createBranch',
        call: (s, f, h) => s.createBranch('made'),
        reads: h => ({ branches: h.branchNames }),
      }, writeKind, {
        rules: [oldAfterThrow()],
        allows: [METADATA_ON_DISK],
      }));
    test('A refused request in createBranchAt leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'createBranchAt',
        setup: (s, f, h) => { f.at = s.currentSequence(); s.setStateJson('mode', { mode: 'later' }); },
        call: (s, f, h) => s.createBranchAt('made', 'main', f.at),
        reads: h => ({ branches: h.branchNames, made: h.branchStates('made') }),
      }, writeKind, {
        rules: [oldAfterThrow()],
        allows: [METADATA_ON_DISK],
      }));
    test('A refused request in createBranchAtWithStates leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'createBranchAtWithStates',
        setup: (s, f, h) => { f.at = s.currentSequence(); s.setStateJson('mode', { mode: 'later' }); },
        call: (s, f, h) => s.createBranchAtWithStates('made', 'main', f.at, { stateIds: ['messages'] }),
        reads: h => ({ branches: h.branchNames, made: h.branchStates('made') }),
      }, writeKind, {
        rules: [oldAfterThrow()],
        allows: [METADATA_ON_DISK],
      }));
    test('A refused request in createBranchWithStates leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'createBranchWithStates',
        call: (s, f, h) => s.createBranchWithStates('made', 'main', { stateIds: ['messages'] }),
        reads: h => ({ branches: h.branchNames, made: h.branchStates('made') }),
      }, writeKind, {
        rules: [oldAfterThrow()],
        allows: [METADATA_ON_DISK],
      }));
    test('A refused request in deleteBranch leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'deleteBranch',
        setup: (s, f, h) => s.createBranch('gone'),
        call: (s, f, h) => s.deleteBranch('gone'),
        reads: h => ({ branches: h.branchNames }),
      }, writeKind, {
        rules: [oldAfterThrow()],
        allows: [METADATA_ON_DISK],
      }));
    test('A refused request in editStateItem leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'editStateItem',
        call: (s, f, h) => s.editStateItem('messages', 0, Buffer.from(JSON.stringify(h.message('edited', 0)))),
        reads: h => ({ messages: h.messages }),
      }, writeKind));
    test('A refused request in redactStateItems leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'redactStateItems',
        setup: (s, f, h) => s.appendManyToStateJsonWithIdentity('identified', [{ text: 'two' }, { text: 'three' }], 'id', 'sequence'),
        call: (s, f, h) => s.redactStateItems('identified', 0, 2),
        reads: h => ({ identified: h.identified, length: s => s.getStateLen('identified') }),
      }, writeKind));
    test('A refused request in registerState leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'registerState',
        call: (s, f, h) => s.registerState({ id: 'added', strategy: 'snapshot' }),
        reads: h => ({ states: h.stateList }),
      }, writeKind, {
        rules: [oldAfterThrow()],
        allows: [METADATA_ON_DISK],
      }));
    test('A refused request in registerStateFieldIndex leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'registerStateFieldIndex',
        call: (s, f, h) => s.registerStateFieldIndex('messages', '/channel', 'string'),
        reads: h => ({ index: s => s.queryStateIndexEq('messages', '/channel', 'general', {}) }),
      }, writeKind));
    test('A refused request in setAutoSnapshot leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'setAutoSnapshot',
        call: (s, f, h) => s.setAutoSnapshot(false),
        reads: () => ({}),
      }, writeKind));
    test('A refused request in setState leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'setState',
        call: (s, f, h) => s.setState('raw', Buffer.from('new bytes')),
        reads: h => ({ raw: s => h.utf8(s.getState('raw')) }),
      }, writeKind));
    test('A refused request in setStateJson leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'setStateJson',
        call: (s, f, h) => s.setStateJson('mode', { mode: 'new' }),
        reads: h => ({ mode: s => s.getStateJson('mode') }),
      }, writeKind, {
        rules: [oldAfterThrow()],
      }));
    test('A refused request in storeBlob leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'storeBlob',
        freshBlob: true,
        call: (s, f, h) => s.storeBlob(Buffer.from('fresh blob'), 'text/plain'),
        reads: h => ({ blob: (s, f) => h.utf8(s.getBlob(f.fresh)) }),
      }, writeKind, {
        rules: [theMessageNamesTheDirectory],
      }));
    test('A refused request in switchBranch leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'switchBranch',
        setup: (s, f, h) => s.createBranch('other'),
        call: (s, f, h) => s.switchBranch('other'),
        reads: h => ({ current: s => s.currentBranch().name }),
      }, writeKind, {
        rules: [oldAfterThrow()],
        allows: [METADATA_ON_DISK],
      }));
    test('A refused request in treeBatch leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'treeBatch',
        call: (s, f, h) => s.treeBatch('files', [{ op: 'set', path: 'b.txt', entry: h.entry(f.blob) }, { op: 'remove', path: 'a.txt' }]),
        reads: h => ({ files: h.files }),
      }, writeKind));
    test('The aim finds each point of treeBatch when its aimed runs make a write of their own before the call',
      refused({
        name: 'treeBatchWithForeignWrite',
        method: () => 'treeBatch',
        setup: (s, f, h) => { f.devNull = h.devNull(); },
        call: (s, f, h) => {
          if (h.aimed) h.foreignWrite(f.devNull);
          return s.treeBatch('files', [{ op: 'set', path: 'b.txt', entry: h.entry(f.blob) }, { op: 'remove', path: 'a.txt' }]);
        },
        reads: h => ({ files: h.files }),
      }, writeKind, { firstAim: 'miss' }));
    test('A refused request in treeRemove leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'treeRemove',
        call: (s, f, h) => s.treeRemove('files', 'a.txt'),
        reads: h => ({ files: h.files }),
      }, writeKind, {
        rules: [oldAfterThrow()],
      }));
    test('A refused request in treeSet leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'treeSet',
        call: (s, f, h) => s.treeSet('files', 'b.txt', h.entry(f.blob)),
        reads: h => ({ files: h.files }),
      }, writeKind, {
        rules: [oldAfterThrow()],
      }));
    test('A refused request in updateStateStrategy leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'updateStateStrategy',
        call: (s, f, h) => s.updateStateStrategy({ id: 'messages', strategy: 'append_log', deltaSnapshotEvery: 5, fullSnapshotEvery: 2 }),
        reads: h => ({ strategy: h.strategyOf, messages: h.messages }),
        probe: (s, f, h) => {
          for (let n = 0; n < 5; n++) s.appendToStateJson('messages', h.message('probe', n));
          return s.listStates().find(({ id }) => id === 'messages').opsSinceSnapshot;
        },
      }, writeKind, {
        rules: [oldAfterThrow()],
        allows: [METADATA_ON_DISK],
      }));
    test('A refused request in sync leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'sync',
        unsynced: (s, f, h) => h.threeWrites(s),
        call: (s, f, h) => s.sync(),
        reads: h => h.threeReads,
      }, syncKind));
    test('A refused request in the first sync of a handle leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'firstSync',
        movedIn: true,
        unsynced: (s, f, h) => h.threeWrites(s),
        call: (s, f, h) => s.sync(),
        reads: h => h.threeReads,
      }, syncKind, {
        rules: [aRefusedNameSyncThrows, theSecondTrySyncsItAgain, theStoreNameWaits],
      }));
    test('A refused request in close leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'close',
        unsynced: (s, f, h) => h.threeWrites(s),
        call: (s, f, h) => s.close(),
        reads: h => h.threeReads,
      }, closeKind));
    test('A refused request in the first close of a handle leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'firstClose',
        movedIn: true,
        unsynced: (s, f, h) => h.threeWrites(s),
        call: (s, f, h) => s.close(),
        reads: h => h.threeReads,
      }, closeKind, {
        rules: [aRefusedNameSyncThrows, theStoreNameWaits],
      }));
    test('A refused request in an append of a long run without a barrier leaves a throw or a return, whole values and a store that opens again',
      refused({
        name: 'long',
        appends: 400,
        method: i => (i % 2 === 0 ? 'appendJson' : 'appendToStateJson'),
        call: (s, f, h, i) => (i % 2 === 0 ? s.appendJson('journal', { n: i }) : s.appendToStateJson('messages', h.message('long', i))),
        reads: h => ({ journal: h.journal, messages: h.messages }),
      }, longKind, {
        rules: [oldAfterThrow('appendToStateJson')],
      }));
    test('currentBranch on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused({
        name: 'currentBranch',
        setup: (s, f, h) => h.coldSetup(s, f),
        read: (s, f, h) => s.currentBranch().name,
      }, coldKind));
    test('currentSequence on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused({
        name: 'currentSequence',
        setup: (s, f, h) => h.coldSetup(s, f),
        read: (s, f, h) => s.currentSequence(),
      }, coldKind));
    test('getBlob on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused({
        name: 'getBlob',
        setup: (s, f, h) => h.coldSetup(s, f),
        read: (s, f, h) => h.utf8(s.getBlob(f.blob)),
      }, coldKind));
    test('getRecord on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused({
        name: 'getRecord',
        setup: (s, f, h) => h.coldSetup(s, f),
        read: (s, f, h) => {
          const record = s.getRecord(f.record);
          return record && { ...record, payload: h.utf8(record.payload) };
        },
      }, coldKind));
    test('getRecordIdsByType on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused({
        name: 'getRecordIdsByType',
        setup: (s, f, h) => h.coldSetup(s, f),
        read: (s, f, h) => s.getRecordIdsByType('journal'),
      }, coldKind));
    test('getState on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused({
        name: 'getState',
        setup: (s, f, h) => h.coldSetup(s, f),
        read: (s, f, h) => h.utf8(s.getState('raw')),
      }, coldKind));
    test('getStateIndexValueCounts on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused({
        name: 'getStateIndexValueCounts',
        setup: (s, f, h) => h.coldSetup(s, f),
        read: (s, f, h) => s.getStateIndexValueCounts('messages', '/channel'),
      }, coldKind));
    test('getStateItemJson on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused({
        name: 'getStateItemJson',
        setup: (s, f, h) => h.coldSetup(s, f),
        read: (s, f, h) => s.getStateItemJson('messages', 1),
      }, coldKind));
    test('getStateJson on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused({
        name: 'getStateJson',
        setup: (s, f, h) => h.coldSetup(s, f),
        read: (s, f, h) => s.getStateJson('messages'),
      }, coldKind));
    test('getStateJsonAt on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused({
        name: 'getStateJsonAt',
        setup: (s, f, h) => h.coldSetup(s, f),
        read: (s, f, h) => s.getStateJsonAt('mode', f.at),
      }, coldKind));
    test('getStateLen on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused({
        name: 'getStateLen',
        setup: (s, f, h) => h.coldSetup(s, f),
        read: (s, f, h) => s.getStateLen('messages'),
      }, coldKind));
    test('getStateSlice on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused({
        name: 'getStateSlice',
        setup: (s, f, h) => h.coldSetup(s, f),
        read: (s, f, h) => h.utf8(s.getStateSlice('messages', 1, 2)),
      }, coldKind));
    test('The aim finds the read of getStateSlice when the writer reads another file a random number of times before it begins',
      refused({
        name: 'getStateSliceWithForeignReads',
        start: h => h.foreignReads(),
        setup: (s, f, h) => h.coldSetup(s, f),
        read: (s, f, h) => h.utf8(s.getStateSlice('messages', 1, 2)),
      }, coldKind, { firstAim: 'hit' }));
    test('getStateTail on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused({
        name: 'getStateTail',
        setup: (s, f, h) => h.coldSetup(s, f),
        read: (s, f, h) => h.utf8(s.getStateTail('messages', 2)),
      }, coldKind));
    test('listBranches on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused({
        name: 'listBranches',
        setup: (s, f, h) => h.coldSetup(s, f),
        read: (s, f, h) => h.branchNames(s),
      }, coldKind));
    test('listStates on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused({
        name: 'listStates',
        setup: (s, f, h) => h.coldSetup(s, f),
        read: (s, f, h) => h.stateList(s),
      }, coldKind));
    test('queryStateIndexEq on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused({
        name: 'queryStateIndexEq',
        setup: (s, f, h) => h.coldSetup(s, f),
        read: (s, f, h) => s.queryStateIndexEq('messages', '/channel', 'general', {}),
      }, coldKind));
    test('queryStateIndexRange on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused({
        name: 'queryStateIndexRange',
        setup: (s, f, h) => h.coldSetup(s, f),
        read: (s, f, h) => s.queryStateIndexRange('messages', '/n', { gte: 1, lte: 2 }),
      }, coldKind));
    test('stateHeadOffset on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused({
        name: 'stateHeadOffset',
        setup: (s, f, h) => h.coldSetup(s, f),
        read: (s, f, h) => s.stateHeadOffset('messages'),
      }, coldKind));
    test('treeGet on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused({
        name: 'treeGet',
        setup: (s, f, h) => h.coldSetup(s, f),
        read: (s, f, h) => s.treeGet('files', 'a.txt'),
      }, coldKind));
    test('treeList on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused({
        name: 'treeList',
        setup: (s, f, h) => h.coldSetup(s, f),
        read: (s, f, h) => h.files(s),
      }, coldKind));
  });
}
