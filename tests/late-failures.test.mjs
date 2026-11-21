import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync } from 'node:fs';
import { availableParallelism, constants, tmpdir } from 'node:os';
import { join, posix } from 'node:path';
import { after, suite, test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { isDeepStrictEqual } from 'node:util';
import { quoted, readTrace, resolved } from './durability-trace.mjs';
import {
  appendJson,
  appendManyToStateJsonWithIdentity,
  appendToStateJson,
  appendToStateJsonWithIdentity,
  compactState,
  createBranch,
  createBranchAt,
  createBranchAtWithStates,
  createBranchWithStates,
  deleteBranch,
  editStateItem,
  redactStateItems,
  registerState,
  registerStateFieldIndex,
  setAutoSnapshot,
  setState,
  setStateJson,
  storeBlob,
  switchBranch,
  treeBatch,
  treeRemove,
  treeSet,
  updateStateStrategy,
  sync,
  close,
  long,
  currentBranch,
  currentSequence,
  getBlob,
  getRecord,
  getRecordIdsByType,
  getState,
  getStateIndexValueCounts,
  getStateItemJson,
  getStateJson,
  getStateJsonAt,
  getStateLen,
  getStateSlice,
  getStateTail,
  listBranches,
  listStates,
  queryStateIndexEq,
  queryStateIndexRange,
  stateHeadOffset,
  treeGet,
  treeList,
} from './late-failures-scenarios.mjs';
import * as scenarios from './late-failures-scenarios.mjs';

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
const UNREADABLE_ERRNO = 'The error for a refused sync of the directories that hold a blob has words after its OS error, so a caller cannot read the errno.';
const NEW_BRANCH_AFTER_THROW = 'When a refused request makes the call throw, the handle already shows the new branch.';
const newAfterThrow = problem => problem.newAfterThrow === true;
const wordsAfterOsError = problem => problem.wordsAfterOsError === true;
const nameInRefused = problem => problem.nameInRefused === true;
const syncLost = problem => problem.syncLost === true;

const isUnder = (path, dir) => typeof path === 'string' && (path === dir || path.startsWith(`${dir}/`));
const env = { ...process.env };
delete env.LD_PRELOAD;

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
    if (inCall && (isUnder(path, store) || isUnder(target, store))) {
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
const where = (point, store) => posix.relative(posix.dirname(store), point.path ?? store);

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

function child(command, args, cwd) {
  return pooled(() => new Promise(resolve => {
    const started = Date.now();
    const proc = spawn(command, args, { cwd, env, detached: true });
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
  const dir = join(base, `${scenario.name}-${++counter}`);
  const root = join(dir, 'root');
  const outside = join(dir, 'outside');
  mkdirSync(root, { recursive: true });
  mkdirSync(outside);
  const record = join(dir, 'record');
  const injection = inject ? ['-e', `inject=${inject.name}:error=${inject.errno}:when=${inject.when}`] : [];
  const result = await child('strace', ['-f', '-y', '-o', record, '-e', `trace=${CALLS}`, ...injection,
    process.execPath, '--single-threaded', WRITER, 'run', scenario.name, root, outside, ...extra], dir);
  let out = null;
  try {
    out = JSON.parse(result.stdout);
  } catch {}
  let text = '';
  try {
    text = readFileSync(record, 'utf8');
  } catch {}
  const store = join(root, 'store');
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

function whenInClean(all, call, clean) {
  if (!call.main) return null;
  let index = all.indexOf(call);
  let nth = 0;
  for (; index >= 0 && !all[index].mark; index--) if (all[index].main && all[index].name === call.name) nth++;
  const start = index < 0 ? 0 : clean.all.findIndex(({ mark }) => mark === all[index].mark) + 1;
  if (index >= 0 && start === 0) return null;
  const same = clean.all.slice(start).filter(({ main, name }) => main && name === call.name);
  return same[nth - 1]?.when ?? null;
}

function hit(run, clean) {
  const { all, requests } = requestsOfCall(run.text, run.store, run.dir);
  const refused = all.filter(call => call.injected);
  if (refused.length !== 1) return { why: `the record shows ${refused.length} refused requests`, when: null };
  const when = whenInClean(all, refused[0], clean);
  const request = requests.find(({ call }) => call === refused[0]);
  if (!request) return { why: `the refused request is not at or under the store's directory in the call: ${refused[0].text}`, when };
  return { id: request.id, when, request };
}

async function aimAll(scenario, kind, clean, extra) {
  const points = kind.points(clean.requests);
  const counted = new Map(points.map(point => [point.id, []]));
  const misses = new Map(points.map(point => [point.id, []]));
  const aims = new Map(points.map(point => [point.id, point.when]));
  let aimed = 0;
  let slowest = 0;
  const kept = new Set();
  for (let round = 0; round < 3; round++) {
    const open = points.filter(point => counted.get(point.id).length === 0);
    if (open.length === 0) break;
    aimed += open.length;
    const runs = await Promise.all(open.map(point => traced(scenario, extra,
      { name: point.name, errno: errnoOf(point.name), when: aims.get(point.id) })));
    const found = runs.map(run => hit(run, clean));
    slowest = Math.max(slowest, ...runs.map(run => run.result.ms));
    if (round === 0) {
      for (const name of new Set(open.map(point => point.name))) {
        const ofName = open.flatMap((point, index) => (point.name === name ? [found[index].id === point.id] : []));
        if (ofName.length > 1 && ofName.filter(Boolean).length * 2 >= ofName.length) kept.add(name);
      }
    }
    runs.forEach((run, index) => {
      const point = open[index];
      const { id, why, when, request } = found[index];
      if (id && counted.has(id)) counted.get(id).push({ ...run, errno: errnoOf(request.name), refused: request.path });
      if (id === point.id) return;
      misses.get(point.id).push(why ?? `it refused ${id}`);
      if (!kept.has(point.name) && when !== null) aims.set(point.id, aims.get(point.id) + point.when - when);
    });
  }
  return { points, counted, misses, aimed, slowest };
}

async function reopened(scenario, kind, extra, runs) {
  const layout = runs.map(run => kind.stores(run));
  const paths = runs.flatMap((run, index) => layout[index].map(where => run[where]));
  if (paths.length === 0) return { each: layout.map(() => ({})) };
  const result = await child(process.execPath, [WRITER, 'reopen', scenario.name, extra[0] ?? '-', ...paths], base);
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
    for (const path of mark.names.filter(each => isUnder(each, store) && each !== store)) {
      problems.push({
        text: `${barrier} returned before the store asked to put the name ${posix.relative(store, path)} on disk`,
        nameInRefused: posix.dirname(path) === run.refused,
      });
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
    wordsAfterOsError: out.call.error && Number(/\(os error (\d+)\)./s.exec(out.call.message)?.[1]) === constants.errno[errno],
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
    if (out.retry && !out.retry.ok && !(out.call.code === 'DurabilityLost' && out.retry.code === 'DurabilityLost')) {
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
            add(`${which}, opened again, gave the new value for ${name}, where the handle last gave ${shown(last)}`);
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
          problems.push({ text: `${what}, ${name} gave ${JSON.stringify(read.value)} after the call threw, not the old value`, newAfterThrow: true });
        }
      }
    }
    return problems;
  },
});

const secondSyncReturns = {
  rule: 'the second try',
  judge: ({ out }) => (out.retry && !out.retry.ok && out.call.code === 'DurabilityLost' && out.retry.code === 'DurabilityLost'
    ? [{ text: `sync again threw: ${shown(out.retry)}`, syncLost: true }] : []),
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

const declared = new Set();

function refused(scenario, kind, { rules = [], todos = [] } = {}) {
  declared.add(scenario);
  return async t => {
    assert.ok(scenario !== undefined, 'the test refers to no scenario of tests/late-failures-scenarios.mjs');
    assert.equal(scenario.kind, kind.name, `the scenario ${scenario.name} is of the kind ${scenario.kind}, and its test judges it as ${kind.name}`);
    let result;
    try {
      result = await scenarioResult(scenario, kind);
    } catch (error) {
      result = { failure: error.stack };
    }
    if (result.failure) assert.fail(result.failure);
    const lost = todos.filter(each => each.point !== undefined && !result.points.some(point => point.id === each.point));
    assert.deepEqual(lost.map(({ point, rule }) => `${point}, ${rule}`), [], 'a todo names a fault point that the call no longer has');
    if (result.points.length === 0) {
      t.skip(kind.noPoints);
      return;
    }
    t.diagnostic(`${result.points.length} fault points, ${result.aimed} aimed runs, the slowest traced run ${result.slowest} ms`);
    kind.cleanAnswers(result);
    for (const point of result.points) {
      await t.test(`with ${point.id} of the call refused`, async t => {
        const runs = result.counted.get(point.id);
        const at = where(point, result.clean.store);
        if (runs.length === 0) {
          assert.fail(`${NOT_MEASURED}: three aimed runs did not refuse ${point.id} (${at}): ${result.misses.get(point.id).join('; ')}`);
        }
        const judged = runs.map(run => [run, judge(kind, rules, result.clean, run, result.answers.get(run))]);
        const here = todos.filter(each => each.point === undefined || each.point === point.id);
        const ownedBy = (rule, problem) => here.find(each => each.rule === rule && each.owns(problem));
        const holds = keep => {
          for (const [run, problems] of judged) {
            const found = RULES.flatMap(rule => problems.get(rule).filter(problem => keep(rule, problem)).map(({ text }) => text));
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
      refused(appendJson, writeKind));
    test('A refused request in appendManyToStateJsonWithIdentity leaves a throw or a return, whole values and a store that opens again',
      refused(appendManyToStateJsonWithIdentity, writeKind));
    test('A refused request in appendToStateJson leaves a throw or a return, whole values and a store that opens again',
      refused(appendToStateJson, writeKind, {
        rules: [oldAfterThrow()],
      }));
    test('A refused request in appendToStateJsonWithIdentity leaves a throw or a return, whole values and a store that opens again',
      refused(appendToStateJsonWithIdentity, writeKind));
    test('A refused request in compactState leaves a throw or a return, whole values and a store that opens again',
      refused(compactState, writeKind));
    test('A refused request in createBranch leaves a throw or a return, whole values and a store that opens again',
      refused(createBranch, writeKind));
    test('A refused request in createBranchAt leaves a throw or a return, whole values and a store that opens again',
      refused(createBranchAt, writeKind));
    test('A refused request in createBranchAtWithStates leaves a throw or a return, whole values and a store that opens again',
      refused(createBranchAtWithStates, writeKind, {
        rules: [oldAfterThrow()],
        todos: [
          { rule: 'the reads', todo: NEW_BRANCH_AFTER_THROW, owns: newAfterThrow },
        ],
      }));
    test('A refused request in createBranchWithStates leaves a throw or a return, whole values and a store that opens again',
      refused(createBranchWithStates, writeKind, {
        rules: [oldAfterThrow()],
        todos: [
          { rule: 'the reads', todo: NEW_BRANCH_AFTER_THROW, owns: newAfterThrow },
        ],
      }));
    test('A refused request in deleteBranch leaves a throw or a return, whole values and a store that opens again',
      refused(deleteBranch, writeKind));
    test('A refused request in editStateItem leaves a throw or a return, whole values and a store that opens again',
      refused(editStateItem, writeKind));
    test('A refused request in redactStateItems leaves a throw or a return, whole values and a store that opens again',
      refused(redactStateItems, writeKind));
    test('A refused request in registerState leaves a throw or a return, whole values and a store that opens again',
      refused(registerState, writeKind));
    test('A refused request in registerStateFieldIndex leaves a throw or a return, whole values and a store that opens again',
      refused(registerStateFieldIndex, writeKind));
    test('A refused request in setAutoSnapshot leaves a throw or a return, whole values and a store that opens again',
      refused(setAutoSnapshot, writeKind));
    test('A refused request in setState leaves a throw or a return, whole values and a store that opens again',
      refused(setState, writeKind));
    test('A refused request in setStateJson leaves a throw or a return, whole values and a store that opens again',
      refused(setStateJson, writeKind, {
        rules: [oldAfterThrow()],
      }));
    test('A refused request in storeBlob leaves a throw or a return, whole values and a store that opens again',
      refused(storeBlob, writeKind, {
        todos: [
          { point: 'fsync 1', rule: 'the errno', todo: UNREADABLE_ERRNO, owns: wordsAfterOsError },
          { point: 'fsync 3', rule: 'the errno', todo: UNREADABLE_ERRNO, owns: wordsAfterOsError },
          { point: 'fsync 3', rule: 'the barrier', owns: nameInRefused, todo: "After a refused sync of a blob's directory, sync and close return while the blob's name is not synced." },
        ],
      }));
    test('A refused request in switchBranch leaves a throw or a return, whole values and a store that opens again',
      refused(switchBranch, writeKind, {
        rules: [oldAfterThrow()],
        todos: [
          { rule: 'the reads', owns: newAfterThrow, todo: 'When a refused request makes switchBranch throw, the handle has already switched to the target.' },
        ],
      }));
    test('A refused request in treeBatch leaves a throw or a return, whole values and a store that opens again',
      refused(treeBatch, writeKind));
    test('A refused request in treeRemove leaves a throw or a return, whole values and a store that opens again',
      refused(treeRemove, writeKind, {
        rules: [oldAfterThrow()],
      }));
    test('A refused request in treeSet leaves a throw or a return, whole values and a store that opens again',
      refused(treeSet, writeKind, {
        rules: [oldAfterThrow()],
      }));
    test('A refused request in updateStateStrategy leaves a throw or a return, whole values and a store that opens again',
      refused(updateStateStrategy, writeKind));
    test('A refused request in sync leaves a throw or a return, whole values and a store that opens again',
      refused(sync, syncKind, {
        rules: [secondSyncReturns],
        todos: [{
          point: 'fsync 1', rule: 'the second try', owns: syncLost,
          todo: 'After a refused sync of the record log, sync on the same handle throws DurabilityLost until the store is opened again.',
        }],
      }));
    test('A refused request in close leaves a throw or a return, whole values and a store that opens again',
      refused(close, closeKind));
    test('A refused request in an append of a long run without a barrier leaves a throw or a return, whole values and a store that opens again',
      refused(long, longKind, {
        rules: [oldAfterThrow('appendToStateJson')],
      }));
    test('currentBranch on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused(currentBranch, coldKind));
    test('currentSequence on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused(currentSequence, coldKind));
    test('getBlob on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused(getBlob, coldKind));
    test('getRecord on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused(getRecord, coldKind));
    test('getRecordIdsByType on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused(getRecordIdsByType, coldKind));
    test('getState on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused(getState, coldKind));
    test('getStateIndexValueCounts on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused(getStateIndexValueCounts, coldKind));
    test('getStateItemJson on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused(getStateItemJson, coldKind));
    test('getStateJson on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused(getStateJson, coldKind));
    test('getStateJsonAt on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused(getStateJsonAt, coldKind));
    test('getStateLen on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused(getStateLen, coldKind));
    test('getStateSlice on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused(getStateSlice, coldKind));
    test('getStateTail on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused(getStateTail, coldKind));
    test('listBranches on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused(listBranches, coldKind));
    test('listStates on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused(listStates, coldKind));
    test('queryStateIndexEq on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused(queryStateIndexEq, coldKind));
    test('queryStateIndexRange on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused(queryStateIndexRange, coldKind));
    test('stateHeadOffset on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused(stateHeadOffset, coldKind));
    test('treeGet on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused(treeGet, coldKind));
    test('treeList on a store opened again throws the errno of a refused read, and the same call again gives the true value',
      refused(treeList, coldKind));
  });

  test('Each scenario of tests/late-failures-scenarios.mjs has one test, under its own name', () => {
    const named = Object.entries(scenarios).filter(([key, scenario]) => key !== scenario.name).map(([key]) => key);
    assert.deepEqual(named, [], 'these exports carry another name than their own');
    const untested = Object.values(scenarios).filter(scenario => !declared.has(scenario)).map(({ name }) => name);
    assert.deepEqual(untested, [], 'no test refers to these scenarios');
  });
}
