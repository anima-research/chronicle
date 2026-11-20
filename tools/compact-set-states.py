#!/usr/bin/env python3
"""Free the space in records.log that old values of named states hold.

Chronicle's record log is append-only: a state that is rewritten with `Set` on
every write keeps every earlier value on disk. A read of a state walks back from
its head along `prev_update_offset` and stops at the first `Set` or `Snapshot`.
For each state that a --state pattern names, this tool keeps what such a walk
reads, from every head that state.bin holds and from every state update that lies
after the newest head (open replays those), and drops every other update of that
state. Every other record is copied. A state update is rewritten only where its
`prev_update_offset` must change: it then holds the new offset and gets a new
checksum. state.bin gets the new offsets, and every counter in it stays as
chronicle wrote it.

What is lost: the old values of each named state. A read of a named state at an
older sequence can give nothing, and a branch made later at an older point can
lack the state. So name only a state whose old values nothing reads, such as
agent-framework's push coalescer table, `mcpl/coalescing`
(anima-research/agent-framework#1028). A field index that a caller registered is
gone after the run, and the caller registers it again; state-indexes.bin is kept
as state-indexes.bin.pre-compact. Other reads that change:
  * a state update whose link changed holds the new offset in its payload;
  * a query by record type with a limit can return more records, since chronicle
    reads a window of limit * 4 + 100 records before it filters by type;
  * a client of agent-framework's API `subscribe` that resumes from a sequence it
    kept can get its branch's whole history again, where that sequence was a
    dropped update's, since chronicle starts from the log's beginning for a
    sequence where the branch has no record.

records.log, state.bin and state-indexes.bin can each be a symbolic link, as
chronicle allows: the tool works on the file that the link leads to, in that
file's directory, and leaves the link as it is.

It refuses, and changes nothing, when:
  * the store is open (it can't take the lock on LOCK),
  * LOCK is not a regular file, or records.log or state.bin leads to no regular
    file,
  * a head in state.bin does not name the last state update of its branch and
    state, so state.bin is older than records.log (start and stop the store's
    host once, which saves state.bin),
  * state.bin names an offset where its record is not, or a state update links
    to an offset where no update of its state is,
  * the log is torn, or a record fails its checksum,
  * a state update in the JSON form that releases before 0.3.0 wrote, or in the
    positional MessagePack form that no release writes, needs a new link (the
    tool rewrites only the named MessagePack form that chronicle writes),
  * a file of an earlier run is in the store,
  * no state update belongs to a state that a pattern names.
It takes the lock on LOCK as chronicle does. Where LOCK is missing it creates
one, with records.log's owner, and deletes it again before it exits.

The swap: the tool writes the new records.log and state.bin whole, and a blocked
state.bin, which chronicle refuses to open, all beside the old files. Then the
old records.log and state.bin each get a second name, records.log.pre-compact and
state.bin.pre-compact, by a hard link. Up to here the store's own files are as
they were. Then the blocked file takes state.bin's place, state-indexes.bin moves
to state-indexes.bin.pre-compact, the new log takes records.log's place, and the
new state.bin takes the blocked one's place. So a run that stops at any point
leaves a store that opens as before, or one that open refuses.

On a host:
  1. Update the writer, so it no longer writes what the run frees.
  2. Stop the host.
  3. Check:   CHRONICLE_MODULE=<chronicle package> node state-digests.mjs <store-dir> --state <glob> > before.txt
  4. Run:     compact-set-states.py <store-dir> --state <glob>           (a dry run)
              compact-set-states.py <store-dir> --state <glob> --apply
  5. Check:   ... node state-digests.mjs <store-dir> --state <glob> > after.txt
  6. Compare: diff before.txt after.txt shows nothing.
  7. Start the host.
  8. Delete records.log.pre-compact, state.bin.pre-compact and
     state-indexes.bin.pre-compact to free the space.
When a run stopped, run the tool with --go-back, with the host stopped. Where
state.bin is the blocked file, it puts the old files back. Where the swap had not
begun, it removes the run's files and changes no live file. After a run that
finished it changes nothing: undo such a run by hand, by renaming each
.pre-compact file to its name without .pre-compact, which loses every write since
the run.

The tool needs free space for the new log, the size of what it keeps, and a
file system with hard links. It reads chronicle_payload.py from its own
directory.

Usage:
    compact-set-states.py <store-dir> --state <glob> [--state <glob> ...] [--apply]
    compact-set-states.py <store-dir> --go-back

Python 3.9+, Unix. Exit 0: done, a dry run, or nothing to do; 1: refused, and
nothing changed; 2: usage; 3: stopped during the swap (run --go-back).
"""
import errno
import fcntl
import fnmatch
import json
import os
import stat
import struct
import sys
import zlib

from chronicle_payload import NOT_READ, READ

STARTS = ('Set', 'Snapshot')  # where chronicle's read of a chain stops (src/state/chain_walk.rs)
KINDS = frozenset(READ) | frozenset(NOT_READ)
FIELDS = ('record_id', 'global_sequence', 'state_id', 'prev_update_offset', 'operation', 'timestamp')

# ── minimal MessagePack ────────────────────────────────────────────────────

def mp_decode(buf, i=0):
    """Decode one value at buf[i]; return (value, next_index). Bytes stay bytes;
    maps become lists of (key, value) pairs so order and duplicate keys survive."""
    t = buf[i]
    if t <= 0x7F: return t, i + 1
    if t >= 0xE0: return t - 0x100, i + 1
    if 0x80 <= t <= 0x8F: return _mp_map(buf, i + 1, t & 0x0F)
    if 0x90 <= t <= 0x9F: return _mp_arr(buf, i + 1, t & 0x0F)
    if 0xA0 <= t <= 0xBF:
        n = t & 0x1F; return buf[i + 1:i + 1 + n].decode('utf-8'), i + 1 + n
    if t == 0xC0: return None, i + 1
    if t == 0xC2: return False, i + 1
    if t == 0xC3: return True, i + 1
    if t == 0xC4: n = buf[i + 1]; return MpBin(buf[i + 2:i + 2 + n]), i + 2 + n
    if t == 0xC5: n = struct.unpack_from('>H', buf, i + 1)[0]; return MpBin(buf[i + 3:i + 3 + n]), i + 3 + n
    if t == 0xC6: n = struct.unpack_from('>I', buf, i + 1)[0]; return MpBin(buf[i + 5:i + 5 + n]), i + 5 + n
    if t == 0xCA: return struct.unpack_from('>f', buf, i + 1)[0], i + 5
    if t == 0xCB: return struct.unpack_from('>d', buf, i + 1)[0], i + 9
    if t == 0xCC: return buf[i + 1], i + 2
    if t == 0xCD: return struct.unpack_from('>H', buf, i + 1)[0], i + 3
    if t == 0xCE: return struct.unpack_from('>I', buf, i + 1)[0], i + 5
    if t == 0xCF: return struct.unpack_from('>Q', buf, i + 1)[0], i + 9
    if t == 0xD0: return struct.unpack_from('>b', buf, i + 1)[0], i + 2
    if t == 0xD1: return struct.unpack_from('>h', buf, i + 1)[0], i + 3
    if t == 0xD2: return struct.unpack_from('>i', buf, i + 1)[0], i + 5
    if t == 0xD3: return struct.unpack_from('>q', buf, i + 1)[0], i + 9
    if t == 0xD9: n = buf[i + 1]; return buf[i + 2:i + 2 + n].decode('utf-8'), i + 2 + n
    if t == 0xDA: n = struct.unpack_from('>H', buf, i + 1)[0]; return buf[i + 3:i + 3 + n].decode('utf-8'), i + 3 + n
    if t == 0xDB: n = struct.unpack_from('>I', buf, i + 1)[0]; return buf[i + 5:i + 5 + n].decode('utf-8'), i + 5 + n
    if t == 0xDC: n = struct.unpack_from('>H', buf, i + 1)[0]; return _mp_arr(buf, i + 3, n)
    if t == 0xDD: n = struct.unpack_from('>I', buf, i + 1)[0]; return _mp_arr(buf, i + 5, n)
    if t == 0xDE: n = struct.unpack_from('>H', buf, i + 1)[0]; return _mp_map(buf, i + 3, n)
    if t == 0xDF: n = struct.unpack_from('>I', buf, i + 1)[0]; return _mp_map(buf, i + 5, n)
    raise ValueError('unsupported msgpack type 0x%02x at %d' % (t, i))


class MpBin(bytes):
    """Marks a value that was a msgpack bin (not str) so it re-encodes as bin."""


class MpMap(list):
    """Ordered (key, value) pairs that re-encode as a msgpack map."""


def _mp_arr(buf, i, n):
    out = []
    for _ in range(n):
        v, i = mp_decode(buf, i); out.append(v)
    return out, i


def _mp_map(buf, i, n):
    out = MpMap()
    for _ in range(n):
        k, i = mp_decode(buf, i); v, i = mp_decode(buf, i); out.append((k, v))
    return out, i


def mp_encode(v):
    if v is None: return b'\xc0'
    if v is True: return b'\xc3'
    if v is False: return b'\xc2'
    if isinstance(v, MpBin):
        n = len(v)
        if n < 256: return b'\xc4' + bytes([n]) + v
        if n < 65536: return b'\xc5' + struct.pack('>H', n) + v
        return b'\xc6' + struct.pack('>I', n) + v
    if isinstance(v, int):
        if 0 <= v < 128: return bytes([v])
        if -32 <= v < 0: return struct.pack('b', v)
        if v >= 0:
            if v < 256: return b'\xcc' + bytes([v])
            if v < 65536: return b'\xcd' + struct.pack('>H', v)
            if v < 2 ** 32: return b'\xce' + struct.pack('>I', v)
            return b'\xcf' + struct.pack('>Q', v)
        if v >= -128: return b'\xd0' + struct.pack('>b', v)
        if v >= -32768: return b'\xd1' + struct.pack('>h', v)
        if v >= -2 ** 31: return b'\xd2' + struct.pack('>i', v)
        return b'\xd3' + struct.pack('>q', v)
    if isinstance(v, float): return b'\xcb' + struct.pack('>d', v)
    if isinstance(v, str):
        b = v.encode('utf-8'); n = len(b)
        if n < 32: return bytes([0xA0 | n]) + b
        if n < 256: return b'\xd9' + bytes([n]) + b
        if n < 65536: return b'\xda' + struct.pack('>H', n) + b
        return b'\xdb' + struct.pack('>I', n) + b
    if isinstance(v, MpMap):
        n = len(v)
        head = bytes([0x80 | n]) if n < 16 else (b'\xde' + struct.pack('>H', n) if n < 65536 else b'\xdf' + struct.pack('>I', n))
        return head + b''.join(mp_encode(k) + mp_encode(x) for k, x in v)
    if isinstance(v, list):
        n = len(v)
        head = bytes([0x90 | n]) if n < 16 else (b'\xdc' + struct.pack('>H', n) if n < 65536 else b'\xdd' + struct.pack('>I', n))
        return head + b''.join(mp_encode(x) for x in v)
    raise TypeError('cannot encode %r' % type(v))


DECODE_ERRORS = (ValueError, IndexError, TypeError, struct.error, RecursionError)


def is_count(v):
    return isinstance(v, int) and not isinstance(v, bool) and v >= 0


# ── records.log framing ────────────────────────────────────────────────────

MAGIC = b'REC\0'
MESSAGEPACK = 1  # a record's encoding byte; earlier releases wrote state updates as JSON


class Refusal(Exception):
    pass


class Stopped(Exception):
    pass


class Rec:
    __slots__ = ('offset', 'length', 'branch', 'update', 'form', 'sid', 'op', 'prev', 'payload_at', 'prev_at', 'prev_end')

    def __init__(self, offset, length, branch):
        self.offset, self.length, self.branch = offset, length, branch
        self.update = False
        self.form = self.sid = self.op = self.prev = self.payload_at = self.prev_at = self.prev_end = None


def kind_of(operation):
    if isinstance(operation, str): return operation
    if isinstance(operation, MpMap) and len(operation) == 1: return operation[0][0]
    if isinstance(operation, dict) and len(operation) == 1: return next(iter(operation))
    return None


def read_update(rec, enc, payload):
    """Fill in rec as a state update when chronicle's decode (StateUpdateRecord::decode in src/types.rs) reads its
    payload as one: each field it requires, of its type, and an operation of a kind StateOperation names. A record
    typed state_update that doesn't decode stays an ordinary record, as chronicle keeps it."""
    prev_at = prev_end = None
    try:
        if enc == MESSAGEPACK:
            value, end = mp_decode(payload)
            if end != len(payload): return
            if isinstance(value, MpMap):
                form, fields = 'map', dict(value)
                at = 1 if payload[0] <= 0x8F else (3 if payload[0] == 0xDE else 5)
                for key, _ in value:
                    _, value_at = mp_decode(payload, at)
                    _, value_end = mp_decode(payload, value_at)
                    if key == 'prev_update_offset': prev_at, prev_end = value_at, value_end
                    at = value_end
            elif isinstance(value, list) and len(value) == len(FIELDS):
                form, fields = 'positional', dict(zip(FIELDS, value))
            else:
                return
        else:
            form, fields = 'json', json.loads(payload)
            if not isinstance(fields, dict): return
    except DECODE_ERRORS:
        return
    sid, op, prev = fields.get('state_id'), fields.get('operation'), fields.get('prev_update_offset')
    kind = kind_of(op)
    if not (is_count(fields.get('record_id')) and is_count(fields.get('global_sequence'))
            and isinstance(fields.get('timestamp'), int) and not isinstance(fields.get('timestamp'), bool)
            and isinstance(sid, str) and (prev is None or is_count(prev)) and kind in KINDS):
        return
    rec.update, rec.form, rec.sid, rec.op, rec.prev = True, form, sid, kind, prev
    rec.prev_at, rec.prev_end = prev_at, prev_end


def scan_log(path):
    recs = []
    size = os.path.getsize(path)
    with open(path, 'rb') as f:
        def need(n):
            b = f.read(n)
            if len(b) != n:
                raise Refusal('records.log is torn at offset %d. tools/trim-records-log.py reports the torn tail, and '
                              'its --apply cuts it off.' % (f.tell() - len(b)))
            return b
        while f.tell() < size:
            start = f.tell()
            if need(4) != MAGIC:
                raise Refusal('records.log has no record at offset %d. tools/trim-records-log.py reports where the log '
                              'stops being whole, and its --apply cuts off everything from there.' % start)
            ver = need(1)[0]
            if ver != 1: raise Refusal('the record at offset %d of records.log has version %d, which this tool does not read.' % (start, ver))
            need(1)
            _rid, _seq, branch, _ts = struct.unpack('<QQQq', need(32))
            rtype = need(struct.unpack('<H', need(2))[0])
            enc = need(1)[0]
            payload_at = f.tell() + 4 - start
            payload = need(struct.unpack('<I', need(4))[0])
            for _ in range(2): need(8 * struct.unpack('<H', need(2))[0])
            if struct.unpack('<I', need(4))[0] != zlib.crc32(payload) & 0xFFFFFFFF:
                raise Refusal('the record at offset %d of records.log fails its checksum, so chronicle reads nothing past '
                              'it. The store needs a repair first. tools/trim-records-log.py reports the record, and its '
                              '--apply cuts off it and every record after it.' % start)
            rec = Rec(start, f.tell() - start, branch)
            if rtype == b'state_update':
                read_update(rec, enc, payload)
                rec.payload_at = payload_at
            recs.append(rec)
    return recs


# ── state.bin ──────────────────────────────────────────────────────────────

STI_MAGIC = b'STI\0'
STI_VERSION = 2
# A StateChainHead's fields, by position (src/state/manager.rs). chronicle reads a head that a legacy index saved with
# fewer than eight, and gives the missing trailing fields their defaults.
HEAD, LAST_DELTA, LAST_FULL = 0, 3, 4
BLOCKED = b'BLOCKED: '


def head_is_whole(head):
    return (isinstance(head, list) and len(head) > LAST_FULL and is_count(head[HEAD])
            and all(offset is None or is_count(offset) for offset in (head[LAST_DELTA], head[LAST_FULL])))


def read_state_bin(data):
    if data[:4] != STI_MAGIC: raise Refusal('state.bin is not a state index.')
    try:
        if data[4] != STI_VERSION: raise Refusal('state.bin has version %d, which this tool does not read.' % data[4])
        n = struct.unpack_from('<Q', data, 5)[0]
        index, end = mp_decode(data[13:13 + n])
    except DECODE_ERRORS as error:
        raise Refusal('state.bin is not a whole state index (%s).' % error)
    if end != n or len(data) != 13 + n: raise Refusal('state.bin is not the length its index says.')
    # rmp_serde::to_vec: StateIndex = [heads: map{[branch, state_id] -> [fields]}, strategies: map]
    if not (isinstance(index, list) and len(index) == 2 and isinstance(index[0], MpMap)
            and all(isinstance(key, list) and len(key) == 2 and is_count(key[0]) and isinstance(key[1], str)
                    and head_is_whole(head) for key, head in index[0])):
        raise Refusal('state.bin does not hold the state index this tool reads.')
    return index


def state_bin_bytes(index):
    enc = mp_encode(index)
    return STI_MAGIC + bytes([STI_VERSION]) + struct.pack('<Q', len(enc)) + enc


def blocked_state_bin(store):
    """The bytes that stand in for state.bin while the tool swaps the files. They don't begin with STI\\0, so
    chronicle refuses to open the store."""
    return BLOCKED + ('compact-set-states.py stopped while it swapped records.log and state.bin, so this store is '
                      'not whole. With the host stopped, run compact-set-states.py %s --go-back, which puts back the '
                      'files as they were before the run.\n' % store).encode('utf-8')


# ── files ──────────────────────────────────────────────────────────────────

class Files:
    """The paths the tool reads and writes. records.log, state.bin and state-indexes.bin are each resolved through
    their symbolic links, as chronicle's save resolves them (src/atomic_file.rs, atomic_write), and the files the
    tool adds for each are in the directory of the file that it leads to."""

    def __init__(self, store):
        self.store = store
        self.log = resolve(store, 'records.log')
        self.sb = resolve(store, 'state.bin')
        self.indexes = os.path.realpath(os.path.join(store, 'state-indexes.bin'))
        if os.path.exists(self.indexes) and not stat.S_ISREG(os.stat(self.indexes).st_mode):
            raise Refusal('%s is not a regular file.' % self.indexes)
        self.new_log, self.new_sb, self.blocked = self.log + '.compact', self.sb + '.compact', self.sb + '.blocked'
        self.staging = (self.new_log, self.new_sb, self.blocked)
        self.kept = tuple(path + '.pre-compact' for path in (self.log, self.sb, self.indexes))

    def found(self, paths):
        return [path for path in paths if os.path.lexists(path)]


def resolve(store, name):
    path = os.path.join(store, name)
    real = os.path.realpath(path)
    if not os.path.exists(real):
        raise Refusal('%s is missing.' % ('%s, where the link %s leads,' % (real, path) if os.path.islink(path) else path))
    if not stat.S_ISREG(os.stat(real).st_mode): raise Refusal('%s is not a regular file.' % real)
    return real


def go_back_text(store):
    return ('With the host stopped, run compact-set-states.py %s --go-back, which puts back the files as they were '
            'before that run.' % store)


def finished_text(kept):
    return ('If the check after that run was equal to the check before, delete %s to free the space. To undo the run '
            'instead, with the host stopped, rename each to its name without .pre-compact. That loses every write '
            'since the run.' % ', '.join(kept))


def earlier_run(files, state_bin):
    if state_bin.startswith(BLOCKED):
        raise Refusal('state.bin is the blocked file of a compact-set-states.py run that stopped during its swap. %s'
                      % go_back_text(files.store))
    staging, kept = files.found(files.staging), files.found(files.kept)
    if staging:
        raise Refusal('%s of a run that stopped before its swap %s in the store. With the host stopped, run '
                      'compact-set-states.py %s --go-back, which removes them.'
                      % (', '.join(staging + kept), 'is' if len(staging + kept) == 1 else 'are', files.store))
    if kept:
        raise Refusal('%s of an earlier run that finished %s in the store. %s'
                      % (', '.join(kept), 'is' if len(kept) == 1 else 'are', finished_text(kept)))


def take_lock(store, owner):
    """The lock chronicle takes on LOCK (src/store.rs, acquire_lock). Returns (fd, created): where LOCK was missing,
    the tool creates it with the store's owner, and deletes it again before it exits."""
    path = os.path.join(store, 'LOCK')
    created = False
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    try:
        try:
            fd = os.open(path, flags)
        except FileNotFoundError:
            fd = os.open(path, flags | os.O_CREAT | os.O_EXCL, 0o644)
            created = True
            if (os.fstat(fd).st_uid, os.fstat(fd).st_gid) != (owner.st_uid, owner.st_gid):
                os.fchown(fd, owner.st_uid, owner.st_gid)
    except OSError as error:
        if created: os.remove(path)
        if error.errno == errno.ELOOP:
            raise Refusal('LOCK is a symbolic link, which chronicle refuses too. Move it aside.')
        raise Refusal('could not open LOCK (%s).' % error)
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        raise Refusal('LOCK is not a regular file, which chronicle refuses too. Move it aside.')
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        if created: os.remove(path)
        raise Refusal('LOCK is locked: the store is open. Stop its host first.')
    return fd, created


def write_file(path, chunks, like):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'wb') as f:
        for chunk in chunks: f.write(chunk)
        f.flush()
        if (os.fstat(fd).st_uid, os.fstat(fd).st_gid) != (like.st_uid, like.st_gid):
            os.fchown(fd, like.st_uid, like.st_gid)
        os.fchmod(fd, stat.S_IMODE(like.st_mode))
        os.fsync(fd)


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)


def human(n):
    return '%.1f MB' % (n / 1e6)


# ── the plan ───────────────────────────────────────────────────────────────

def plan(recs, heads, patterns):
    by_offset = {r.offset: r for r in recs}
    last = {}
    for r in recs:
        if r.update: last[(r.branch, r.sid)] = r
    for key, head in heads:
        branch, sid = key[0], key[1]
        at = by_offset.get(head[HEAD])
        if at is None or not at.update or at.sid != sid:
            raise Refusal('state.bin names offset %s as the head of %s on branch %d, and no state update of %s is '
                          'there, so state.bin and records.log do not belong together.' % (head[HEAD], sid, branch, sid))
        for snapshot in (head[LAST_DELTA], head[LAST_FULL]):
            if snapshot is not None and snapshot not in by_offset:
                raise Refusal('state.bin names offset %s as a snapshot of %s on branch %d, and no record is there, so '
                              'state.bin and records.log do not belong together.' % (snapshot, sid, branch))
        newest = last.get((branch, sid))
        if newest is not None and newest.offset != at.offset:
            raise Refusal('state.bin is older than records.log: its head of %s on branch %d is at offset %d, and the '
                          'last state update of it there is at offset %d. Start and stop the store\'s host once, '
                          'which saves state.bin, and run again.' % (sid, branch, at.offset, newest.offset))
    named = lambda sid: any(fnmatch.fnmatchcase(sid, p) for p in patterns)
    newest_head = max((head[HEAD] for _, head in heads), default=-1)
    starts = [by_offset[head[HEAD]] for key, head in heads if named(key[1])]
    starts += [r for r in recs if r.update and r.offset > newest_head and named(r.sid)]
    keep = set()
    for r in starts:
        while r is not None and r.offset not in keep:
            keep.add(r.offset)
            if r.op in STARTS or r.prev is None: break
            prior = by_offset.get(r.prev)
            if prior is None or not prior.update or prior.sid != r.sid:
                raise Refusal('the state update of %s at offset %d links to offset %d, where no state update of %s '
                              'is, so chronicle\'s read of %s fails there too.' % (r.sid, r.offset, r.prev, r.sid, r.sid))
            r = prior
    drop = {r.offset for r in recs if r.update and named(r.sid) and r.offset not in keep}

    new_offset, links, at = {}, {}, 0
    for r in recs:
        if r.offset in drop: continue
        new_offset[r.offset] = at
        length = r.length
        if r.update and r.prev is not None:
            if r.prev not in drop and r.prev not in new_offset:
                raise Refusal('the state update of %s at offset %d links to offset %d, where no earlier record is, so '
                              'chronicle\'s read of %s fails there too.' % (r.sid, r.offset, r.prev, r.sid))
            link = None if r.prev in drop else new_offset[r.prev]
            if link != r.prev:
                if r.form != 'map':
                    raise Refusal('the state update of %s at offset %d is in the %s form and needs a new link, and this '
                                  'tool rewrites only the named MessagePack form that chronicle writes. Leave %s out '
                                  'of the --state patterns.'
                                  % (r.sid, r.offset, 'JSON' if r.form == 'json' else 'positional MessagePack', r.sid))
                links[r.offset] = link
                length += len(mp_encode(link)) - (r.prev_end - r.prev_at)
        at += length
    return drop, new_offset, links, at


def relinked(raw, rec, link):
    plen_at = rec.payload_at - 4
    payload = raw[rec.payload_at:rec.payload_at + struct.unpack_from('<I', raw, plen_at)[0]]
    new = payload[:rec.prev_at] + mp_encode(link) + payload[rec.prev_end:]
    tail = raw[rec.payload_at + len(payload):-4]  # caused_by and linked_to
    return raw[:plen_at] + struct.pack('<I', len(new)) + new + tail + struct.pack('<I', zlib.crc32(new) & 0xFFFFFFFF)


# ── the run ────────────────────────────────────────────────────────────────

def locked(store, act):
    lock, created = take_lock(store, os.stat(os.path.join(store, 'records.log')))
    try:
        return act()
    finally:
        if created: os.remove(os.path.join(store, 'LOCK'))
        os.close(lock)


def read_bytes(path):
    with open(path, 'rb') as f:
        return f.read()


def run(store, patterns, apply):
    def act():
        files = Files(store)
        state_bin = read_bytes(files.sb)
        earlier_run(files, state_bin)
        compact(files, patterns, apply, state_bin)
    locked(store, act)


def compact(files, patterns, apply, state_bin):
    print('scanning %s (%s)' % (files.log, human(os.path.getsize(files.log))))
    recs = scan_log(files.log)
    index = read_state_bin(state_bin)
    heads = index[0]
    named = sorted({r.sid for r in recs if r.update and any(fnmatch.fnmatchcase(r.sid, p) for p in patterns)})
    if not named:
        raise Refusal('no state update in records.log belongs to a state that %s names.'
                      % ' or '.join('--state %s' % p for p in patterns))
    drop, new_offset, links, new_size = plan(recs, heads, patterns)

    by_state = {}
    for r in recs:
        if r.offset in drop:
            n, b = by_state.get(r.sid, (0, 0)); by_state[r.sid] = (n + 1, b + r.length)
    print('%d records, %d branches; %d named state(s):' % (len(recs), len({r.branch for r in recs}), len(named)))
    for sid in named:
        n, b = by_state.get(sid, (0, 0))
        print('  %s: drop %d old value(s), %s' % (sid, n, human(b)))
    size = os.path.getsize(files.log)
    print('would drop %d records, %s of %s' % (len(drop), human(size - new_size), human(size)))
    if not drop:
        print('Nothing to drop, so nothing changed.'); return
    if not apply:
        print('Dry run: nothing changed. Add --apply to rewrite.'); return

    def remap(offset):
        return None if offset is None or offset in drop else new_offset[offset]
    new_heads = MpMap()
    for key, head in heads:
        h = list(head)
        h[HEAD], h[LAST_DELTA], h[LAST_FULL] = remap(h[HEAD]), remap(h[LAST_DELTA]), remap(h[LAST_FULL])
        new_heads.append((key, h))

    log_stat, sb_stat = os.stat(files.log), os.stat(files.sb)
    made = []
    try:
        def records():
            with open(files.log, 'rb') as src:
                for r in recs:
                    if r.offset in drop: continue
                    src.seek(r.offset); raw = src.read(r.length)
                    yield relinked(raw, r, links[r.offset]) if r.offset in links else raw
        made.append(files.new_log); write_file(files.new_log, records(), log_stat)
        if os.path.getsize(files.new_log) != new_size: raise RuntimeError('the new log is not the size its plan says')
        made.append(files.new_sb); write_file(files.new_sb, [state_bin_bytes([new_heads, index[1]])], sb_stat)
        made.append(files.blocked); write_file(files.blocked, [blocked_state_bin(files.store)], sb_stat)
        for path in (files.log, files.sb):
            made.append(path + '.pre-compact'); os.link(path, path + '.pre-compact')
        for directory in {os.path.dirname(files.log), os.path.dirname(files.sb)}: sync_dir(directory)
    except BaseException as error:
        for path in made:
            if os.path.lexists(path): os.remove(path)
        raise Refusal('could not write the new files and the old files\' second names (%s: %s).'
                      % (type(error).__name__, error))

    steps = [('put the blocked file in place of state.bin', files.blocked, files.sb)]
    if os.path.exists(files.indexes):
        steps.append(('moved state-indexes.bin to its .pre-compact name', files.indexes, files.indexes + '.pre-compact'))
    steps += [('put the new log in place of records.log', files.new_log, files.log),
              ('put the new state.bin in place of the blocked file', files.new_sb, files.sb)]
    done = 'wrote the new files'
    for step, source, target in steps:
        try:
            os.replace(source, target); sync_dir(os.path.dirname(target))
        except BaseException as error:
            raise Stopped('the swap stopped after it %s (%s: %s). %s'
                          % (done, type(error).__name__, error, go_back_text(files.store)))
        done = step
    print('done: records.log is %s. The old files are %s.'
          % (human(os.path.getsize(files.log)), ', '.join(files.found(files.kept))))
    print('Check with state-digests.mjs and compare with the check from before; then delete them to free the space.')


def go_back(store):
    def act():
        files = Files(store)
        staging, kept = files.found(files.staging), files.found(files.kept)
        blocked = read_bytes(files.sb).startswith(BLOCKED)
        if not (blocked or staging or kept):
            print('No file of a compact-set-states.py run is in %s, so nothing changed.' % store); return
        if blocked:
            if not os.path.lexists(files.sb + '.pre-compact'):
                raise Refusal('state.bin is the blocked file, and the old state.bin, %s.pre-compact, is not there to '
                              'put back.' % files.sb)
            work = [(files.log, True), (files.indexes, True), (files.sb, True)]
            said = 'went back: the files of the store are as they were before the run, and no file of the run is left.'
        elif staging:
            work = [(files.log, False), (files.sb, False)]
            said = 'removed the files of the run, whose swap had not begun. No file of the store changed.'
        else:
            raise Refusal('the run that left %s finished, so --go-back changes nothing. %s'
                          % (', '.join(kept), finished_text(kept)))
        try:
            for live, put_back in work:
                old = live + '.pre-compact'
                if not os.path.lexists(old): continue
                if put_back and not (os.path.lexists(live) and os.path.samestat(os.lstat(old), os.lstat(live))):
                    os.replace(old, live)
                else:
                    os.remove(old)
                sync_dir(os.path.dirname(live))
            for path in files.found(files.staging):
                os.remove(path); sync_dir(os.path.dirname(path))
        except BaseException as error:
            raise Stopped('going back stopped (%s: %s). Run compact-set-states.py %s --go-back again, with the host '
                          'stopped.' % (type(error).__name__, error, store))
        print(said)
    locked(store, act)


def main(argv):
    if len(argv) < 2 or argv[1] in ('-h', '--help'):
        print(__doc__); return 2
    store = argv[1]; patterns = []; apply = back = False
    i = 2
    while i < len(argv):
        if argv[i] == '--state' and i + 1 < len(argv): patterns.append(argv[i + 1]); i += 2
        elif argv[i] == '--apply': apply = True; i += 1
        elif argv[i] == '--go-back': back = True; i += 1
        else: print('unknown argument %s' % argv[i]); return 2
    if back and (patterns or apply):
        print('--go-back takes no other argument'); return 2
    if not back and not patterns:
        print('at least one --state <glob> is required'); return 2
    try:
        if back: go_back(store)
        else: run(store, patterns, apply)
    except Refusal as refusal:
        print('refusing: %s Nothing in the store changed.' % refusal); return 1
    except Stopped as stopped:
        print('stopped: %s' % stopped); return 3
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
