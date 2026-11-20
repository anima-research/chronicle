#!/usr/bin/env python3
"""Free the space in records.log that old values of named states hold.

Chronicle's record log is append-only: a state that is rewritten with `Set` on
every write keeps every earlier value on disk. A read of a state walks back from
its head along `prev_update_offset` and stops at the first `Set` or `Snapshot`.
For each state that a --state pattern names, this tool keeps what such a walk
reads, from every head that state.bin holds and from every state update that lies
after the newest head (open replays those), and drops every other update of that
state. Every other record is copied. A state update is rewritten only where its
`prev_update_offset` must change, and then gets a new checksum. state.bin gets
the new offsets, and every counter in it stays as chronicle wrote it.

What is lost: the old values of each named state. A read of a named state at an
older sequence gives nothing, and a branch made later at an older point does not
hold the state. So name only a state whose old values nothing reads, such as
agent-framework's push coalescer table, `mcpl/coalescing`
(anima-research/agent-framework#1028). A field index that a caller registered is
gone after the run, and the caller registers it again; state-indexes.bin is kept
as state-indexes.bin.pre-compact.

It refuses, and changes nothing, when:
  * the store is open (it can't take the lock on LOCK),
  * a head in state.bin does not name the last state update of its branch and
    state, so state.bin is older than records.log (start and stop the store's
    host once, which saves state.bin),
  * the log is torn, or a record fails its checksum,
  * a state update in the JSON form that releases before 0.3.0 wrote needs a
    new link (the tool rewrites the MessagePack form only),
  * a file of an earlier run is in the store.

The swap: the tool writes the new records.log and state.bin whole, beside the
old ones. Then the old log gets a second name, records.log.pre-compact, by a hard
link, state.bin is copied to state.bin.pre-compact, and state-indexes.bin moves
to state-indexes.bin.pre-compact. A blocked state.bin, which chronicle refuses
to open, takes state.bin's place, then the new log takes records.log's place, and
then the new state.bin takes the blocked one's place. So a run that stops at any
point leaves a store that opens as before, or one that open refuses.

On a host:
  1. Update the writer, so it no longer writes what the run frees.
  2. Stop the host.
  3. Check:   CHRONICLE_MODULE=<chronicle package> node state-digests.mjs <store-dir> > before.txt
  4. Run:     compact-set-states.py <store-dir> --state <glob>           (a dry run)
              compact-set-states.py <store-dir> --state <glob> --apply
  5. Check:   ... node state-digests.mjs <store-dir> > after.txt
  6. Compare: diff before.txt after.txt shows nothing.
  7. Start the host.
  8. Delete records.log.pre-compact, state.bin.pre-compact and
     state-indexes.bin.pre-compact to free the space.
To go back instead, with the host stopped, see go_back() below, or the text of
a blocked state.bin.

The tool needs free space for the new log, the size of what it keeps, and a
file system with hard links.

Usage:
    compact-set-states.py <store-dir> --state <glob> [--state <glob> ...] [--apply]

Python 3.9+, no dependencies. Exit 0: done or dry run; 1: refused; 2: usage.
"""
import fcntl
import fnmatch
import json
import os
import shutil
import stat
import struct
import sys
import zlib

STARTS = ('Set', 'Snapshot')  # where chronicle's read of a chain stops (src/state/chain_walk.rs)

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


def map_get(m, key):
    for k, v in m:
        if k == key: return v
    return None


# ── records.log framing ────────────────────────────────────────────────────

MAGIC = b'REC\0'
MESSAGEPACK = 1  # a record's encoding byte; earlier releases wrote state updates as JSON


class Refusal(Exception):
    pass


class Rec:
    __slots__ = ('offset', 'length', 'branch', 'update', 'json', 'sid', 'op', 'prev', 'payload_at', 'prev_at', 'prev_end')

    def __init__(self, offset, length, branch):
        self.offset, self.length, self.branch = offset, length, branch
        self.update = self.json = False
        self.sid = self.op = self.prev = self.payload_at = self.prev_at = self.prev_end = None


def read_update(rec, enc, payload):
    """Fill in rec as a state update when its payload decodes as one. A record typed
    state_update that doesn't decode stays an ordinary record, as chronicle keeps it."""
    try:
        if enc == MESSAGEPACK:
            m, end = mp_decode(payload)
            if end != len(payload) or not isinstance(m, MpMap): return
            fields = dict(m)
            at = 1 if payload[0] <= 0x8F else (3 if payload[0] == 0xDE else 5)
            for key, _ in m:
                _, value_at = mp_decode(payload, at)
                _, value_end = mp_decode(payload, value_at)
                if key == 'prev_update_offset': rec.prev_at, rec.prev_end = value_at, value_end
                at = value_end
        else:
            fields = json.loads(payload)
            rec.json = True
        sid, op, prev = fields['state_id'], fields['operation'], fields.get('prev_update_offset')
        kind = op if isinstance(op, str) else op[0][0] if isinstance(op, MpMap) else next(iter(op))
    except (ValueError, KeyError, TypeError, IndexError, StopIteration, struct.error, UnicodeDecodeError):
        rec.json = False; rec.prev_at = rec.prev_end = None
        return
    if not isinstance(sid, str) or not (prev is None or isinstance(prev, int)): return
    if enc == MESSAGEPACK and rec.prev_at is None: return
    rec.update, rec.sid, rec.op, rec.prev = True, sid, kind, prev


def scan_log(path):
    recs = []
    size = os.path.getsize(path)
    with open(path, 'rb') as f:
        def need(n):
            b = f.read(n)
            if len(b) != n: raise Refusal('records.log is torn at offset %d; run trim-records-log.py first' % (f.tell() - len(b)))
            return b
        while f.tell() < size:
            start = f.tell()
            if need(4) != MAGIC: raise Refusal('records.log has no record at offset %d; run trim-records-log.py first' % start)
            ver = need(1)[0]
            if ver != 1: raise Refusal('the record at offset %d of records.log has version %d, which this tool does not read' % (start, ver))
            need(1)
            _rid, _seq, branch, _ts = struct.unpack('<QQQq', need(32))
            rtype = need(struct.unpack('<H', need(2))[0])
            enc = need(1)[0]
            payload_at = f.tell() + 4 - start
            payload = need(struct.unpack('<I', need(4))[0])
            for _ in range(2): need(8 * struct.unpack('<H', need(2))[0])
            if struct.unpack('<I', need(4))[0] != zlib.crc32(payload) & 0xFFFFFFFF:
                raise Refusal('the record at offset %d of records.log fails its checksum' % start)
            rec = Rec(start, f.tell() - start, branch)
            if rtype == b'state_update':
                read_update(rec, enc, payload)
                rec.payload_at = payload_at
            recs.append(rec)
    return recs


# ── state.bin ──────────────────────────────────────────────────────────────

STI_MAGIC = b'STI\0'
STI_VERSION = 2
HEAD, LAST_DELTA, LAST_FULL = 0, 3, 4  # offsets in a StateChainHead's eight fields
BLOCKED = b'BLOCKED: '


def read_state_bin(path):
    with open(path, 'rb') as f:
        data = f.read()
    if data[:4] != STI_MAGIC: raise Refusal('state.bin is not a state index')
    if data[4] != STI_VERSION: raise Refusal('state.bin has version %d, which this tool does not read' % data[4])
    n = struct.unpack_from('<Q', data, 5)[0]
    index, end = mp_decode(data[13:13 + n])
    if end != n or len(data) != 13 + n: raise Refusal('state.bin has bytes after its index')
    # rmp_serde::to_vec: StateIndex = [heads: map{[branch, state_id] -> [8 fields]}, strategies: map]
    if not (isinstance(index, list) and len(index) == 2 and isinstance(index[0], MpMap)
            and all(isinstance(h, list) and len(h) == 8 for _, h in index[0])):
        raise Refusal('state.bin does not hold the state index this tool reads')
    return index


def state_bin_bytes(index):
    enc = mp_encode(index)
    return STI_MAGIC + bytes([STI_VERSION]) + struct.pack('<Q', len(enc)) + enc


def go_back(store):
    return ('With the host stopped, in %s: rename records.log.pre-compact to records.log, state.bin.pre-compact to '
            'state.bin and state-indexes.bin.pre-compact to state-indexes.bin, each that is there, and delete '
            'records.log.compact, state.bin.compact and state.bin.blocked. The store is then as it was before the run.'
            % store)


def blocked_state_bin(store):
    """The bytes that stand in for state.bin while the tool swaps the files. They don't begin with STI\\0, so
    chronicle refuses to open the store."""
    return BLOCKED + ('compact-set-states.py stopped while it swapped records.log and state.bin, so this store is '
                      'not whole. Go back: %s\n' % go_back(store)).encode('utf-8')


def blocked_text(data):
    return 'state.bin is the blocked file of a compact-set-states.py run that stopped: ' + data[len(BLOCKED):].decode('utf-8', 'replace').strip()


# ── the run ────────────────────────────────────────────────────────────────

LEFT_BY_A_RUN = ('records.log.compact', 'state.bin.compact', 'state.bin.blocked',
                 'records.log.pre-compact', 'state.bin.pre-compact', 'state-indexes.bin.pre-compact')


def earlier_run(store):
    with open(os.path.join(store, 'state.bin'), 'rb') as f:
        start = f.read(len(BLOCKED) + 4096)
    if start.startswith(BLOCKED): raise Refusal(blocked_text(start))
    found = [name for name in LEFT_BY_A_RUN if os.path.lexists(os.path.join(store, name))]
    if not found: return
    unfinished = [name for name in found if not name.endswith('.pre-compact')]
    if unfinished:
        raise Refusal('%s from a run that stopped before its swap finished %s in the store. %s'
                      % (', '.join(unfinished), 'is' if len(unfinished) == 1 else 'are', go_back(store)))
    raise Refusal('%s from an earlier run %s in the store. If that run finished and its check before and after was '
                  'equal, delete them to free the space. Otherwise go back: %s'
                  % (', '.join(found), 'is' if len(found) == 1 else 'are', go_back(store)))


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
                          'there' % (head[HEAD], sid, branch, sid))
        for snapshot in (head[LAST_DELTA], head[LAST_FULL]):
            if snapshot is not None and snapshot not in by_offset:
                raise Refusal('state.bin names offset %s as a snapshot of %s on branch %d, and no record is there'
                              % (snapshot, sid, branch))
        newest = last.get((branch, sid))
        if newest is not None and newest.offset != at.offset:
            raise Refusal('state.bin is older than records.log: its head of %s on branch %d is at offset %d, and the '
                          'last state update of it there is at offset %d. Start and stop the store\'s host once, '
                          'which saves state.bin, and run again' % (sid, branch, at.offset, newest.offset))
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
                              'is' % (r.sid, r.offset, r.prev, r.sid))
            r = prior
    drop = {r.offset for r in recs if r.update and named(r.sid) and r.offset not in keep}

    new_offset, links, at = {}, {}, 0
    for r in recs:
        if r.offset in drop: continue
        new_offset[r.offset] = at
        length = r.length
        if r.update and r.prev is not None:
            if r.prev not in drop and r.prev not in new_offset:
                raise Refusal('the state update of %s at offset %d links to offset %d, where no earlier record is'
                              % (r.sid, r.offset, r.prev))
            link = None if r.prev in drop else new_offset[r.prev]
            if link != r.prev:
                if r.json:
                    raise Refusal('the JSON state update of %s at offset %d needs a new link, and this tool rewrites '
                                  'the MessagePack form only' % (r.sid, r.offset))
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


def write_file(path, chunks, like):
    with open(path, 'wb') as f:
        for chunk in chunks: f.write(chunk)
        f.flush(); os.fsync(f.fileno())
    os.chmod(path, stat.S_IMODE(like.st_mode))
    if (like.st_uid, like.st_gid) != (os.stat(path).st_uid, os.stat(path).st_gid):
        os.chown(path, like.st_uid, like.st_gid)


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)


def human(n):
    return '%.1f MB' % (n / 1e6)


def run(store, patterns, apply):
    log_path = os.path.join(store, 'records.log'); sb_path = os.path.join(store, 'state.bin')
    for p in (log_path, sb_path):
        if not os.path.exists(p): raise Refusal('%s is missing' % p)
    earlier_run(store)
    lock = open(os.path.join(store, 'LOCK'), 'a+')
    try:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        raise Refusal('%s is locked: the store is open. Stop its host first' % os.path.join(store, 'LOCK'))

    print('scanning %s (%s)' % (log_path, human(os.path.getsize(log_path))))
    recs = scan_log(log_path)
    index = read_state_bin(sb_path)
    heads = index[0]
    drop, new_offset, links, new_size = plan(recs, heads, patterns)

    by_state = {}
    for r in recs:
        if r.offset in drop:
            n, b = by_state.get(r.sid, (0, 0)); by_state[r.sid] = (n + 1, b + r.length)
    named = sorted({r.sid for r in recs if r.update and any(fnmatch.fnmatchcase(r.sid, p) for p in patterns)})
    print('%d records, %d branches; %d named state(s):' % (len(recs), len({r.branch for r in recs}), len(named)))
    for sid in named:
        n, b = by_state.get(sid, (0, 0))
        print('  %s: drop %d old value(s), %s' % (sid, n, human(b)))
    print('would drop %d records, %s of %s' % (len(drop), human(os.path.getsize(log_path) - new_size), human(os.path.getsize(log_path))))
    if not drop: return
    if not apply:
        print('dry run; add --apply to rewrite'); return

    def remap(offset):
        return None if offset is None or offset in drop else new_offset[offset]
    new_heads = MpMap()
    for key, head in heads:
        h = list(head)
        h[HEAD], h[LAST_DELTA], h[LAST_FULL] = remap(h[HEAD]), remap(h[LAST_DELTA]), remap(h[LAST_FULL])
        new_heads.append((key, h))

    log_stat, sb_stat = os.stat(log_path), os.stat(sb_path)
    new_log, new_sb = log_path + '.compact', sb_path + '.compact'
    blocked = sb_path + '.blocked'
    try:
        def records():
            with open(log_path, 'rb') as src:
                for r in recs:
                    if r.offset in drop: continue
                    src.seek(r.offset); raw = src.read(r.length)
                    yield relinked(raw, r, links[r.offset]) if r.offset in links else raw
        write_file(new_log, records(), log_stat)
        if os.path.getsize(new_log) != new_size: raise RuntimeError('the new log is not the size its plan says')
        write_file(new_sb, [state_bin_bytes([new_heads, index[1]])], sb_stat)
        write_file(blocked, [blocked_state_bin(store)], sb_stat)
        os.link(log_path, log_path + '.pre-compact')
    except BaseException as error:
        for path in (new_log, new_sb, blocked):
            if os.path.exists(path): os.remove(path)
        if isinstance(error, OSError):
            raise Refusal('could not write the new files and the old log\'s second name (%s); nothing in the store '
                          'changed' % error)
        raise
    shutil.copy2(sb_path, sb_path + '.pre-compact')
    with open(sb_path + '.pre-compact', 'rb+') as f: os.fsync(f.fileno())
    sync_dir(store)
    indexes = os.path.join(store, 'state-indexes.bin')
    if os.path.exists(indexes):
        os.replace(indexes, indexes + '.pre-compact'); sync_dir(store)
    os.replace(blocked, sb_path); sync_dir(store)
    os.replace(new_log, log_path); sync_dir(store)
    os.replace(new_sb, sb_path); sync_dir(store)
    print('done: records.log is %s. The old files are records.log.pre-compact, state.bin.pre-compact and '
          'state-indexes.bin.pre-compact.' % human(os.path.getsize(log_path)))
    print('Check with state-digests.mjs and compare with the check from before; then delete them to free the space.')


def main(argv):
    if len(argv) < 2 or argv[1] in ('-h', '--help'):
        print(__doc__); return 2
    store = argv[1]; patterns = []; apply = False
    i = 2
    while i < len(argv):
        if argv[i] == '--state' and i + 1 < len(argv): patterns.append(argv[i + 1]); i += 2
        elif argv[i] == '--apply': apply = True; i += 1
        else: print('unknown argument %s' % argv[i]); return 2
    if not patterns:
        print('at least one --state <glob> is required'); return 2
    try:
        run(store, patterns, apply)
    except Refusal as refusal:
        print('refusing: %s' % refusal); return 1
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
