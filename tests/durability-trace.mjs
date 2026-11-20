import { posix } from 'node:path';

const NEUTRAL = new Set([
  'access', 'chmod', 'chown', 'close', 'close_range', 'dup', 'dup2', 'dup3', 'fadvise64', 'fadvise64_64',
  'faccessat', 'faccessat2', 'fchmod', 'fchmodat', 'fchmodat2', 'fchown', 'fchownat', 'fcntl', 'fcntl64',
  'fgetxattr', 'flistxattr', 'flock', 'fstat', 'fstat64', 'fstatat64', 'fstatfs', 'fstatfs64', 'futimesat',
  'getcwd', 'getdents', 'getdents64', 'getxattr', 'lchown', 'lgetxattr', 'listxattr', 'llistxattr', 'llseek',
  '_llseek', 'lseek', 'lstat', 'lstat64', 'mmap', 'msync', 'munmap', 'newfstatat', 'pread64', 'preadv',
  'preadv2', 'read', 'readahead', 'readlink', 'readlinkat', 'readv', 'stat', 'stat64', 'statfs', 'statfs64',
  'statx', 'utime', 'utimensat', 'utimes',
]);

const WRITES_TO_FIRST = new Set([
  'fallocate', 'ftruncate', 'pwrite64', 'pwritev', 'pwritev2', 'sendfile', 'sendfile64', 'write', 'writev',
]);
const WRITES_TO_THIRD = new Set(['copy_file_range', 'splice']);
const OPENS = { open: [null, 0, 1], creat: [null, 0, null], openat: [0, 1, 2], openat2: [0, 1, 2] };
const MARK = /^\/chronicle-mark\/(.+)$/;

const isUnder = (path, dir) => path === dir || path.startsWith(`${dir}/`);

function splitCall(text) {
  const open = text.indexOf('(');
  const args = [];
  let current = '';
  let depth = 0;
  let mode = null;
  let i = open + 1;
  for (; i < text.length; i++) {
    const c = text[i];
    if (mode === 'quote') {
      current += c;
      if (c === '\\') current += text[++i] ?? '';
      else if (c === '"') mode = null;
      continue;
    }
    if (mode === 'path') {
      current += c;
      if (c === '>') mode = null;
      continue;
    }
    if (mode === 'comment') {
      current += c;
      if (c === '/' && text[i - 1] === '*') mode = null;
      continue;
    }
    if (c === '"') mode = 'quote';
    else if (c === '/' && text[i + 1] === '*') mode = 'comment';
    else if (c === '<' && /(\d|AT_FDCWD)$/.test(current)) mode = 'path';
    else if ('([{'.includes(c)) depth++;
    else if (')]}'.includes(c)) {
      if (depth === 0) break;
      depth--;
    } else if (c === ',' && depth === 0) {
      args.push(current.trim());
      current = '';
      continue;
    }
    current += c;
  }
  if (current.trim() !== '') args.push(current.trim());
  const rest = text.slice(i + 1);
  const ret = /^\s*=\s*(\S+)/.exec(rest)?.[1] ?? '?';
  return { name: text.slice(0, open), args, rest, ok: ret !== '?' && !ret.startsWith('-1') };
}

function quoted(arg) {
  if (typeof arg !== 'string' || !arg.startsWith('"')) return null;
  let out = '';
  for (let i = 1; i < arg.length; i++) {
    const c = arg[i];
    if (c === '"') return out;
    if (c !== '\\') {
      out += c;
      continue;
    }
    const n = arg[++i];
    if (/[0-7]/.test(n)) {
      let octal = n;
      while (octal.length < 3 && /[0-7]/.test(arg[i + 1])) octal += arg[++i];
      out += String.fromCharCode(parseInt(octal, 8));
    } else if (n === 'x') {
      out += String.fromCharCode(parseInt(arg.slice(i + 1, i + 3), 16));
      i += 2;
    } else {
      out += { n: '\n', t: '\t', r: '\r', v: '\v', f: '\f' }[n] ?? n;
    }
  }
  return null;
}

function normal(path) {
  const clean = posix.normalize(path);
  return clean.length > 1 && clean.endsWith('/') ? clean.slice(0, -1) : clean;
}

function describedPath(arg, cwd) {
  if (arg === 'AT_FDCWD') return cwd;
  const match = /^(?:-?\d+|AT_FDCWD)<(.*)>$/s.exec(arg ?? '');
  if (!match || match[1].endsWith(' (deleted)')) return null;
  return normal(match[1]);
}

function resolved(dirArg, pathArg, cwd) {
  const path = quoted(pathArg);
  if (path === null) return null;
  if (path.startsWith('/')) return normal(path);
  const base = dirArg === null ? cwd : describedPath(dirArg, cwd);
  return base === null ? null : normal(posix.join(base, path));
}

class Disk {
  constructor(names) {
    this.exists = new Set(names);
    this.files = new Set();
    this.names = new Set();
  }

  wrote(path) {
    this.files.add(path);
  }

  made(path) {
    this.exists.add(path);
    this.names.add(path);
  }

  removed(path) {
    for (const set of [this.exists, this.files, this.names]) {
      for (const entry of [...set]) if (isUnder(entry, path)) set.delete(entry);
    }
  }

  renamed(from, to) {
    if (from === to) return;
    this.removed(to);
    for (const set of [this.exists, this.files, this.names]) {
      for (const entry of [...set]) {
        if (!isUnder(entry, from)) continue;
        set.delete(entry);
        if (set !== this.names || entry !== from) set.add(to + entry.slice(from.length));
      }
    }
    this.made(to);
  }

  synced(path) {
    this.files.delete(path);
    for (const name of [...this.names]) if (posix.dirname(name) === path) this.names.delete(name);
  }

  datasynced(path) {
    this.files.delete(path);
  }

  syncedAll() {
    this.files.clear();
    this.names.clear();
  }
}

function joinedCalls(text) {
  const pending = new Map();
  const calls = [];
  const lines = text.split('\n');
  lines.forEach((line, index) => {
    const match = /^(?:(\d+)\s+)?(.*)$/.exec(line);
    const pid = match[1] ?? '';
    let body = match[2];
    if (body === '' || body.startsWith('+++') || body.startsWith('---')) return;
    const resumed = /^<\.\.\. [\w]+ resumed>(.*)$/.exec(body);
    if (resumed) {
      const start = pending.get(pid);
      pending.delete(pid);
      if (start === undefined) return;
      body = start.text + resumed[1];
    }
    if (body.endsWith(' <unfinished ...>')) {
      pending.set(pid, { text: body.slice(0, -' <unfinished ...>'.length), line: index + 1, position: calls.length });
      return;
    }
    if (!/^\w+\(/.test(body)) return;
    calls.push({ text: body, line: index + 1 });
  });
  return { calls, unfinished: [...pending.values()] };
}

export function readTrace(text, { root, cwd, names = [root] }) {
  const disk = new Disk(names);
  const marks = [];
  const unsyncedRenames = [];
  const problems = [];
  const inRoot = path => path !== null && isUnder(path, root);
  const escapedRoot = root.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const rootText = new RegExp(`[<"]${escapedRoot}(?=[/>"]| \\()`);
  const { calls, unfinished } = joinedCalls(text);
  const marksAt = [];
  const problem = (call, why, at = marks.length) => problems.push({ at, line: call.line, why, call: call.text });

  for (const call of calls) {
    marksAt.push(marks.length);
    const { name, args, rest, ok } = splitCall(call.text);
    const markName = args.map(quoted).map(path => MARK.exec(path ?? '')?.[1]).find(Boolean);
    if (markName) {
      marks.push({ name: markName, files: [...disk.files], names: [...disk.names] });
      continue;
    }
    if (name === 'io_uring_enter') {
      problem(call, 'an io_uring_enter, whose requests the record does not show');
      continue;
    }
    if (name === 'chdir' || name === 'fchdir') {
      problem(call, 'a change of the working directory');
      continue;
    }
    if (name in OPENS) {
      if (!ok) continue;
      const [dirIndex, pathIndex, flagIndex] = OPENS[name];
      const path = resolved(dirIndex === null ? null : args[dirIndex], args[pathIndex], cwd);
      if (path === null) {
        if (rootText.test(call.text)) problem(call, 'an open whose path the record does not resolve');
        continue;
      }
      if (!inRoot(path)) continue;
      const flags = flagIndex === null ? 'O_CREAT|O_WRONLY|O_TRUNC' : args[flagIndex];
      if (flags.includes('O_TMPFILE')) problem(call, 'a file opened with O_TMPFILE');
      else if (flags.includes('O_CREAT') && !disk.exists.has(path)) disk.made(path);
      else if (flags.includes('O_TRUNC') && disk.exists.has(path)) disk.wrote(path);
      continue;
    }
    if (name === 'mkdir' || name === 'mkdirat' || name === 'symlink' || name === 'symlinkat') {
      if (!ok) continue;
      const path = {
        mkdir: () => resolved(null, args[0], cwd),
        mkdirat: () => resolved(args[0], args[1], cwd),
        symlink: () => resolved(null, args[1], cwd),
        symlinkat: () => resolved(args[1], args[2], cwd),
      }[name]();
      if (path === null && rootText.test(call.text)) problem(call, 'a new name that the record does not resolve');
      else if (inRoot(path)) disk.made(path);
      continue;
    }
    if (name === 'link' || name === 'linkat') {
      if (!ok) continue;
      const emptyPath = name === 'linkat' && quoted(args[1]) === '' && (args[4] ?? '').includes('AT_EMPTY_PATH');
      const from = name === 'link' ? resolved(null, args[0], cwd)
        : emptyPath ? describedPath(args[0], cwd) : resolved(args[0], args[1], cwd);
      const to = name === 'link' ? resolved(null, args[1], cwd) : resolved(args[2], args[3], cwd);
      if (from === null || to === null) {
        if (rootText.test(call.text)) problem(call, 'a link that the record does not resolve');
        continue;
      }
      if (!inRoot(to)) continue;
      disk.made(to);
      if (disk.files.has(from)) disk.wrote(to);
      continue;
    }
    if (name === 'rename' || name === 'renameat' || name === 'renameat2') {
      if (!ok) continue;
      const from = name === 'rename' ? resolved(null, args[0], cwd) : resolved(args[0], args[1], cwd);
      const to = name === 'rename' ? resolved(null, args[1], cwd) : resolved(args[2], args[3], cwd);
      if (from === null || to === null) {
        if (rootText.test(call.text)) problem(call, 'a rename that the record does not resolve');
        continue;
      }
      if (!inRoot(from) && !inRoot(to)) continue;
      if (inRoot(from) !== inRoot(to) || /RENAME_EXCHANGE|RENAME_WHITEOUT/.test(args[4] ?? '')) {
        problem(call, 'a rename that the model does not follow');
        continue;
      }
      if (disk.files.has(from)) unsyncedRenames.push({ from, to, at: marks.length, line: call.line });
      disk.renamed(from, to);
      continue;
    }
    if (name === 'unlink' || name === 'unlinkat' || name === 'rmdir') {
      if (!ok) continue;
      const path = name === 'unlinkat' ? resolved(args[0], args[1], cwd) : resolved(null, args[0], cwd);
      if (path === null && rootText.test(call.text)) problem(call, 'a removal that the record does not resolve');
      else if (inRoot(path)) disk.removed(path);
      continue;
    }
    if (name === 'truncate') {
      const path = resolved(null, args[0], cwd);
      if (ok && inRoot(path)) disk.wrote(path);
      continue;
    }
    if (WRITES_TO_FIRST.has(name) || WRITES_TO_THIRD.has(name)) {
      const path = describedPath(args[WRITES_TO_FIRST.has(name) ? 0 : 2], cwd);
      if (ok && inRoot(path)) disk.wrote(path);
      continue;
    }
    if (name === 'fsync' || name === 'fdatasync') {
      const path = describedPath(args[0], cwd);
      if (ok && inRoot(path)) {
        if (name === 'fsync') disk.synced(path);
        else disk.datasynced(path);
      }
      continue;
    }
    if (name === 'sync' || name === 'syncfs') {
      if (ok) disk.syncedAll();
      continue;
    }
    if (name === 'mmap') {
      const shared = /MAP_SHARED/.test(args[3] ?? '') && /PROT_WRITE/.test(args[2] ?? '');
      if (shared && inRoot(describedPath(args[4], cwd))) problem(call, 'a writable shared mapping of a file');
      continue;
    }
    if (NEUTRAL.has(name)) continue;
    if (rootText.test(call.text) || rootText.test(rest)) problem(call, `a call that the model does not know: ${name}`);
  }

  marksAt.push(marks.length);
  for (const call of unfinished) {
    if (rootText.test(call.text)) problem(call, 'a call that did not return', marksAt[call.position]);
  }

  const first = marks.length > 0 ? 1 : Infinity;
  return {
    marks,
    unsyncedRenames,
    problems: problems.filter(({ at }) => at >= first && at < marks.length),
  };
}
