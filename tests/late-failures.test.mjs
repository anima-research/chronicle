import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync } from 'node:fs';
import { availableParallelism, constants, tmpdir } from 'node:os';
import { join, posix } from 'node:path';
import { after, test } from 'node:test';
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
const UNREADABLE_ERRNO = 'The error for a refused sync of the directories that hold a blob has words after its OS error, so a caller cannot read the errno.';
const TODOS = [
  { scenario: 'storeBlob', point: 'fsync 1', rule: 'the errno', todo: UNREADABLE_ERRNO, owns: () => true },
  { scenario: 'storeBlob', point: 'fsync 3', rule: 'the errno', todo: UNREADABLE_ERRNO, owns: () => true },
  {
    scenario: 'storeBlob', point: 'fsync 3', rule: 'the barrier', owns: problem => problem.nameInRefused === true,
    todo: "After a refused sync of a blob's directory, sync and close return while the blob's name is not synced.",
  },
];

const isUnder = (path, dir) => typeof path === 'string' && (path === dir || path.startsWith(`${dir}/`));
const env = { ...process.env };
delete env.LD_PRELOAD;

function straceProblem() {
  const probe = spawnSync('strace', ['-o', '/dev/null', 'true'], { encoding: 'utf8' });
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
  if (BY_DESCRIPTOR.has(name)) call.paths = [decorated(args[0])];
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
  return { all, requests };
}

function faultPoints(requests, kind) {
  if (kind === 'cold') return requests.filter(({ name }) => READS.has(name)).slice(0, 1);
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
      process.kill(-proc.pid, 'SIGKILL');
    }, 20_000);
    const out = [];
    const err = [];
    proc.stdout.on('data', chunk => out.push(chunk));
    proc.stderr.on('data', chunk => err.push(chunk));
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
  return { dir, root, store, copy: join(outside, 'copy'), result, out, text, inject };
}

function brokenRun(run) {
  if (run.result.timedOut) return 'the writer did not end within 20 s';
  if (run.result.signal) return `the writer ended by ${run.result.signal}`;
  if (run.result.status !== 0 || run.out === null) {
    const tail = run.result.stderr.trim().split('\n').slice(-3).join(' | ');
    return `the writer ended with status ${run.result.status}: ${tail}`;
  }
  return null;
}

function cleanProblem(scenario, clean) {
  const broken = brokenRun(clean);
  if (broken) return `the clean run failed: ${broken}`;
  const { out } = clean;
  if (!out.call.ok) return `the clean run's call threw: ${out.call.message}`;
  if (scenario.kind === 'cold') return out.retry.ok ? null : `the clean run's second call threw: ${out.retry.message}`;
  const threw = Object.entries(out.old).filter(([, read]) => !read.ok).map(([name, read]) => `${name}: ${read.message}`);
  return threw.length > 0 ? `the clean run's reads threw: ${threw.join('; ')}` : null;
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

async function aimAll(scenario, clean, extra) {
  const points = faultPoints(clean.requests, scenario.kind);
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

async function reopened(scenario, extra, runs) {
  if (scenario.kind === 'cold') return { each: runs.map(() => ({})) };
  const paths = runs.flatMap(run => (scenario.kind === 'close' ? [run.store] : [run.copy, run.store]));
  const result = await child(process.execPath, [WRITER, 'reopen', scenario.name, extra[0] ?? '-', ...paths], base);
  if (result.status !== 0) throw new Error(`the reopening process ended with status ${result.status}: ${result.stderr.trim().split('\n').slice(-3).join(' | ')}`);
  const { answers, old } = JSON.parse(result.stdout);
  return {
    old,
    each: runs.map(() => (scenario.kind === 'close' ? { store: answers.shift() } : { copy: answers.shift(), store: answers.shift() })),
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

function judge(scenario, clean, run, reopen) {
  const problems = new Map(RULES.map(rule => [rule, []]));
  const broken = brokenRun(run);
  if (broken) {
    problems.get('the call').push({ text: broken });
    return problems;
  }
  const { out } = run;
  const errno = run.errno;
  const add = (rule, text) => problems.get(rule).push({ text });
  if (scenario.kind === 'cold') {
    if (out.call.ok) add('the call', `the read returned ${JSON.stringify(out.call.value)}, and it did not throw`);
    else if (errnoCode(out.call) !== errno) add('the errno', `the read threw without ${errno}: ${out.call.message}`);
    if (!out.retry.ok || !isDeepStrictEqual(out.retry.value, out.expected.value)) {
      add('the second try', `the same read again gave ${shown(out.retry)}, not ${shown(out.expected)}`);
    }
    return problems;
  }
  const old = clean.out.old;
  const fresh = scenario.kind === 'close' ? clean.out.old : clean.out.reads[0];
  const oldOrNew = (name, value) => isDeepStrictEqual(value, old[name].value) || isDeepStrictEqual(value, fresh[name].value);
  const oneState = (rule, what, reads) => {
    const given = Object.entries(reads).filter(([, read]) => read.ok);
    const asOld = given.filter(([name, read]) => isDeepStrictEqual(read.value, old[name].value)).map(([name]) => name);
    const asNew = given.filter(([name, read]) => isDeepStrictEqual(read.value, fresh[name].value)).map(([name]) => name);
    if (asOld.length < given.length && asNew.length < given.length) {
      add(rule, `${what} are no one state: ${given.map(([name]) => `${name} ${asNew.includes(name) ? 'new' : asOld.includes(name) ? 'old' : 'neither'}`).join(', ')}`);
    }
  };
  const whole = (rule, what, reads, mustBeNew) => {
    for (const [name, read] of Object.entries(reads)) {
      if (!read.ok) {
        if (mustBeNew) add(rule, `${what}, ${name} threw after the call returned: ${read.message}`);
      } else if (mustBeNew ? !isDeepStrictEqual(read.value, fresh[name].value) : !oldOrNew(name, read.value)) {
        add(rule, `${what}, ${name} gave ${JSON.stringify(read.value)}, which is not the ${mustBeNew ? '' : 'old or the '}new value ${JSON.stringify(fresh[name].value)}`);
      }
    }
  };
  if (!out.call.ok && out.call.code !== 'DurabilityLost' && errnoCode(out.call) !== errno) {
    add('the errno', `the call threw without ${errno}: ${shown(out.call)}`);
  }
  const returned = out.call.ok && scenario.kind !== 'close';
  whole('the reads', 'the reads after the call', out.reads[0], returned);
  whole('the reads', 'the second reads after the call', out.reads[1], returned);
  oneState('the reads', 'the reads after the call', out.reads[0]);
  oneState('the reads', 'the second reads after the call', out.reads[1]);
  if (out.syncReads) {
    whole('the reads', 'the reads after sync', out.syncReads, false);
    oneState('the reads', 'the reads after sync', out.syncReads);
  }
  for (const name of Object.keys(out.reads[0])) {
    if (!isDeepStrictEqual(out.reads[0][name], out.reads[1][name])) {
      add('the reads', `two reads of ${name} disagree: ${shown(out.reads[0][name])} and ${shown(out.reads[1][name])}`);
    }
  }
  if (out.retry?.ok) whole('the second try', 'the reads after the same call again', out.retryReads, true);
  if (out.retryReads) oneState('the second try', 'the reads after the same call again', out.retryReads);
  const barriers = [];
  if (scenario.kind === 'sync' || scenario.kind === 'close') {
    if (out.call.ok) barriers.push('call');
    if (out.retry?.ok) barriers.push('retry');
  }
  if (out.sync?.ok) barriers.push('sync');
  if (out.close?.ok) barriers.push('close');
  problems.get('the barrier').push(...barrierProblems(run, run.store, barriers));
  const closing = scenario.kind === 'close' ? out.call : out.close;
  const closedReads = scenario.kind === 'close' ? out.reads[0] : out.closedReads;
  if (!closing.ok) {
    if (!out.isClosed.ok || typeof out.isClosed.value !== 'boolean') {
      add('close and isClosed', `after close threw, isClosed gave ${shown(out.isClosed)}`);
    } else {
      const closedCode = Object.values(clean.out.closedReads ?? clean.out.reads[0]).find(read => !read.ok)?.code;
      for (const [name, read] of Object.entries(closedReads)) {
        if (out.isClosed.value && (read.ok || read.code !== closedCode)) {
          add('close and isClosed', `after close threw and isClosed answered true, ${name} gave ${shown(read)}, not a throw as on a closed handle`);
        }
        if (!out.isClosed.value && !read.ok) {
          add('close and isClosed', `after close threw and isClosed answered false, ${name} threw: ${read.message}`);
        }
      }
    }
  }
  const atBarrier = scenario.kind === 'close' ? out.old : out.syncReads;
  const before = { copy: barriers.filter(barrier => barrier !== 'close'), store: barriers };
  const lastRead = {
    copy: name => out.syncReads[name],
    store: scenario.kind === 'close'
      ? name => (out.reads[1][name].ok ? out.reads[1][name] : out.old[name])
      : name => (out.closedReads[name].ok ? out.closedReads[name] : out.syncReads[name]),
  };
  for (const [where, answer] of Object.entries(reopen)) {
    const which = where === 'copy' ? 'the copy' : 'the closed store';
    if (!answer.opened.ok) {
      add('the reopen', `${which} did not open: ${answer.opened.message}`);
      continue;
    }
    const passed = before[where].length > 0;
    oneState('the reopen', `${which}'s reads, opened again,`, answer.reads);
    if (answer.probe) {
      const [oldProbe, newProbe] = [clean.probeOld.value, clean.answer.copy.probe.value];
      const shows = `${which}, opened again, shows ${shown(answer.probe)} operations since a snapshot after five appends`;
      if (!answer.probe.ok || ![oldProbe, newProbe].includes(answer.probe.value)) {
        add('the reopen', `${shows}, which is neither the old cadence's ${oldProbe} nor the new cadence's ${newProbe}`);
      } else if (passed && (out.call.ok || out.retry?.ok) && answer.probe.value !== newProbe) {
        add('the reopen', `${shows}: the call returned and ${before[where].join(' and ')} returned, yet the new cadence's count is ${newProbe}`);
      }
    }
    for (const [name, read] of Object.entries(answer.reads)) {
      const handle = atBarrier[name];
      if (!read.ok) {
        add('the reopen', `${which}, opened again, threw for ${name}: ${read.message}`);
      } else if (passed && handle.ok && !isDeepStrictEqual(read.value, handle.value)) {
        add('the reopen', `${which}, opened again, gave ${JSON.stringify(read.value)} for ${name}, where the handle gave ${JSON.stringify(handle.value)} at ${before[where].join(' and ')}`);
      } else if (!oldOrNew(name, read.value)) {
        add('the reopen', `${which}, opened again, gave ${JSON.stringify(read.value)} for ${name}, which is not the old or the new value`);
      } else if (!isDeepStrictEqual(read.value, old[name].value)) {
        const last = lastRead[where](name);
        if (!last.ok || !isDeepStrictEqual(last.value, read.value)) {
          add('the reopen', `${which}, opened again, gave the new value for ${name}, where the handle last gave ${shown(last)}`);
        }
      }
    }
  }
  return problems;
}

async function scenarioResult(scenario) {
  let extra = [];
  if (scenario.kind === 'long') {
    const found = await traced(scenario, [], null);
    const broken = brokenRun(found);
    if (broken) return { failure: `the run that finds the append failed: ${broken}` };
    const all = calls(found.text, found.dir);
    let index = null;
    let inAppend = null;
    for (const call of all) {
      const begin = /^append-(\d+)-begin$/.exec(call.mark ?? '');
      if (begin) inAppend = Number(begin[1]);
      else if (call.mark) inAppend = null;
      else if (inAppend !== null && !WRITES.has(call.name) && call.name !== 'lseek'
        && call.paths.some(path => isUnder(path, found.store))) {
        index = inAppend;
        break;
      }
    }
    if (index === null) return { failure: 'no append of the 400 shows a request that is no write and no seek' };
    extra = [String(index)];
  }
  const clean = await traced(scenario, extra, null);
  const problem = cleanProblem(scenario, clean);
  if (problem) return { failure: problem };
  Object.assign(clean, requestsOfCall(clean.text, clean.store, clean.dir));
  const marks = clean.all.filter(({ mark }) => mark).map(({ mark }) => mark);
  if (!isDeepStrictEqual(marks, clean.out.marks)) {
    return { failure: `${NOT_MEASURED}: the clean run's record shows the marks [${marks.join(', ')}], not the writer's [${clean.out.marks.join(', ')}]` };
  }
  if (scenario.kind === 'long' && clean.requests.every(({ name }) => WRITES.has(name) || name === 'lseek')) {
    return { failure: `the clean run's append ${extra[0]} shows no request that is no write and no seek` };
  }
  const { points, counted, misses, aimed, slowest } = await aimAll(scenario, clean, extra);
  const runs = [...counted.values()].flat();
  const { old: probeOld, each: answers } = await reopened(scenario, extra, [clean, ...runs]);
  const cleanAnswer = answers.shift();
  Object.assign(clean, { answer: cleanAnswer, probeOld });
  return { clean, points, counted, misses, aimed, slowest: Math.max(slowest, clean.result.ms), answers: new Map(runs.map((run, index) => [run, answers[index]])), cleanAnswer, extra };
}

const notLinux = process.platform !== 'linux' && 'the record of system calls comes from strace, which is for Linux';
const missing = notLinux ? null : straceProblem();
const skip = notLinux || (missing !== null && !process.env.CI && missing);

if (skip) {
  test('A refused request in the middle of a call leaves the caller a throw or a whole value', { skip }, () => {});
} else if (missing !== null) {
  test('strace can trace the writer', () => assert.fail(missing));
} else {
  base = realpathSync(mkdtempSync(join(tmpdir(), 'chronicle-late-failures-')));
  after(() => rmSync(base, { recursive: true, force: true }));
  const scenarios = JSON.parse(spawnSync(process.execPath, [WRITER, 'list'], { encoding: 'utf8' }).stdout);
  const results = new Map(scenarios.map(scenario => [scenario.name,
    scenarioResult(scenario).catch(error => ({ failure: error.stack }))]));
  for (const scenario of scenarios) {
    const title = scenario.kind === 'cold'
      ? `${scenario.name} on a store opened again throws the errno of a refused read, and the same call again gives the true value`
      : `A refused request in ${scenario.kind === 'long' ? 'an append of a long run without a barrier' : scenario.name} leaves a throw or a return, whole values and a store that opens again`;
    test(title, async t => {
      const result = await results.get(scenario.name);
      if (result.failure) assert.fail(result.failure);
      if (result.points.length === 0) {
        t.skip(scenario.kind === 'cold' ? 'in a clean run, the read makes no read request at or under the store'
          : 'in a clean run, the call makes no request at or under the store that is a fault point');
        return;
      }
      t.diagnostic(`${result.points.length} fault points, ${result.aimed} aimed runs, the slowest traced run ${result.slowest} ms`);
      if (scenario.kind !== 'cold') {
        const reopenProblems = Object.entries(result.cleanAnswer).filter(([, answer]) => !answer.opened.ok
          || !isDeepStrictEqual(answer.reads, result.clean.out.syncReads ?? result.clean.out.old));
        assert.deepEqual(reopenProblems.map(([where]) => where), [], 'without a fault, a store opened again reads what the handle last read');
        const { probeOld, answer } = result.clean;
        if (probeOld) {
          assert.ok(probeOld.ok && answer.copy.probe.ok && probeOld.value !== answer.copy.probe.value,
            `${NOT_MEASURED}: the probe gives ${shown(probeOld)} without the call and ${shown(answer.copy.probe)} after it`);
        }
      }
      for (const point of result.points) {
        await t.test(`with ${point.id} of the call refused`, async t => {
          const runs = result.counted.get(point.id);
          const at = where(point, result.clean.store);
          if (runs.length === 0) {
            assert.fail(`${NOT_MEASURED}: three aimed runs did not refuse ${point.id} (${at}): ${result.misses.get(point.id).join('; ')}`);
          }
          const judged = runs.map(run => [run, judge(scenario, result.clean, run, result.answers.get(run))]);
          const todos = TODOS.filter(each => each.scenario === scenario.name && each.point === point.id);
          const ownedBy = (rule, problem) => todos.find(each => each.rule === rule && each.owns(problem));
          const holds = keep => {
            for (const [run, problems] of judged) {
              const found = RULES.flatMap(rule => problems.get(rule).filter(problem => keep(rule, problem)).map(({ text }) => text));
              const gave = run.out ? `, the call gave ${shown(run.out.call)}` : '';
              assert.deepEqual(found, [], `With ${point.id} (${at}) refused${gave}: ${found.join(' | ')}`);
            }
          };
          if (todos.length === 0) return holds(() => true);
          for (const each of todos) await t.test(each.rule, { todo: each.todo }, () => holds((rule, problem) => ownedBy(rule, problem) === each));
          await t.test('the other rules', () => {
            const stale = todos.filter(each => judged.every(([, problems]) => !problems.get(each.rule).some(problem => ownedBy(each.rule, problem) === each)));
            assert.deepEqual(stale.map(({ rule }) => rule), [], `The todo no longer fails here: ${stale.map(({ todo }) => todo).join(' | ')}`);
            holds((rule, problem) => !ownedBy(rule, problem));
          });
        });
      }
    });
  }
}
