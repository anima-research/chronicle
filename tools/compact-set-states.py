#!/usr/bin/env python3
"""Free the space in records.log that old values of named states hold.

Chronicle's record log is append-only: a state that is rewritten with `Set` on
every write keeps every earlier value on disk. A read of a state walks back from
its head along `prev_update_offset` and stops at the first `Set` or `Snapshot`.
The tool walks the links from each head in state.bin, for every state, to each
chain's end. A state that a --state pattern names must be such a state: each
update of it that the walk reaches must be a Set or a Snapshot. Of it, this tool keeps
what a read reads, the update that each head in state.bin names, and it drops
the other updates of that state that the walk reaches. Every other record is
copied. A state update that the walk reaches is rewritten where its
`prev_update_offset` must change: it then holds the new offset, or no link
where the update it linked to was dropped, and gets a new checksum. Every record that the walk does not reach is copied byte for byte,
whatever it holds, for a named state too. state.bin gets the new offsets, and
every count in a head stays as chronicle wrote it. getStateLen reads a head's
item count, which still describes the value at that head. The other counts
time a state's next snapshot, so a named state's next snapshot can come at
another point. listStates returns one of them as opsSinceSnapshot, which for a
named state can still count updates that the run dropped.

What is lost: the old values of each named state. A read of a named state at an
older sequence can give nothing, and a branch made later at an older point can
lack the state. So name only a state whose old values nothing reads, such as
agent-framework's push coalescer table, `mcpl/coalescing`
(anima-research/agent-framework#1028). A field index that a caller registered is
gone after the run, and the caller registers it again; state-indexes.bin is kept
as state-indexes.bin.pre-compact. Other reads that change:
  * a state update whose link changed holds the new offset in its payload, or
    no link where the update it linked to was dropped;
  * the offset of a record that stays can move, so a stateHeadOffset or a
    cursor kept by offset from before the run names another place;
  * a record that stays and names a dropped update in caused_by or linked_to
    still names it, and a read of that id gives nothing (the dry run counts
    such records);
  * a query by record type with a limit can return more records, since chronicle
    reads a window of limit * 4 + 100 records before it filters by type;
  * a client of agent-framework's API `subscribe` that resumes from a sequence it
    kept can get its branch's whole history again, where that sequence was a
    dropped update's, since chronicle starts from the log's beginning for a
    sequence where the branch has no record;
  * a record typed state_update that no head reaches, such as a deleted
    branch's update, keeps its bytes, so its link can name an offset that
    holds another record now. No read of a live state reaches it, and
    tools/chain-report.py, which reads every update's link, can show a wrong
    chain for it. The old
    values of a named state that no head reaches stay, and no later run frees
    them (the dry run counts them).

records.log, state.bin and state-indexes.bin can each be a symbolic link, as
chronicle allows: the tool works on the file that the link leads to, in that
file's directory, and leaves the link as it is.

It refuses, and changes nothing, when:
  * the store is open (it can't take the lock on LOCK),
  * LOCK is missing or is not a regular file, or records.log or state.bin
    leads to no regular file,
  * MANIFEST names a store version or a format feature that it doesn't read,
  * a named state has an update that the walk reaches and that is not a Set or
    a Snapshot of bytes in the named MessagePack form that chronicle writes,
  * a link from a head leads to a record that is not a state update of the
    same state,
  * a record typed state_update lies after the newest head on a branch that
    branches.bin holds, unless the tool parses it and finds no state_id or no
    operation: it may be a state update that state.bin does not have yet, and
    only chronicle's open settles that (from chronicle 0.5.0 an open of the
    store applies such an update when its link matches the head of its state,
    and refuses to open the store when it does not, and the check before a run
    is such an open; with 0.4.0, which replays nothing at open, or a record
    that chronicle does not decode, a write of any state and a close clear
    it),
  * branches.bin is not a branch index that it reads,
  * state.bin names an offset where its record is not, or a state update links
    to an offset where no record is,
  * the log is torn, or a record fails its checksum,
  * a state update needs a new link, and it is in the JSON form that releases
    before 0.3.0 wrote, or in the positional MessagePack form that no release
    writes,
  * a file of an earlier run is in the store,
  * no state update belongs to a state that a pattern names.
It takes the lock on LOCK as chronicle does.

The swap: the tool writes the new records.log and state.bin whole, and a blocked
state.bin, which chronicle refuses to open, all beside the old files. Then the
old records.log and state.bin each get a second name, records.log.pre-compact and
state.bin.pre-compact, by a hard link. Up to here the store's own files are as
they were. Then the blocked file takes state.bin's place, state-indexes.bin moves
to state-indexes.bin.pre-compact, the new log takes records.log's place, and the
new state.bin takes the blocked one's place. So a run that stops at any point
leaves a store that opens as before, or one that open refuses.

On a host:
  First, do all of this once on a copy of the store that can be thrown away, to
  see the time, the space and the checks.
  1. Update the writer, so it no longer writes what the run frees.
  2. Stop the host.
  3. Check:   CHRONICLE_MODULE=<chronicle package> node state-digests.mjs <store-dir> --state <pattern> > before.txt
     Use the chronicle release that the host uses. A newer release can apply
     updates at open that the host's release doesn't, and it can write a
     MANIFEST that the host's release refuses, which the run's old files don't
     keep. The check opens the store as its host does. That open saves
     state.bin and branches.bin, and it can cut off a torn tail, so the old
     files that the run keeps are from after it.
  4. Run:     compact-set-states.py <store-dir> --state <pattern>          (a dry run)
              compact-set-states.py <store-dir> --state <pattern> --apply
     The dry run names the states and what it would drop, and counts the
     records that stay and name a dropped one.
  5. Check:   ... node state-digests.mjs <store-dir> --state <pattern> > after.txt
  6. Compare: diff before.txt after.txt shows nothing. An equal check is
     evidence for the reads that it lists, and no more.
  7. If they differ, run compact-set-states.py <store-dir> --go-back now, before
     the host starts.
  8. Start the host.
  9. Once the host has run well, delete records.log.pre-compact,
     state.bin.pre-compact, state-indexes.bin.pre-compact and
     state.bin.compact-run to free the space.
A --state pattern is literal text in which each * stands for any text.

--go-back, with the host stopped, finds where the run is from the run's record,
state.bin.compact-run, which the run writes before the old files get their
second names and which --go-back removes last. The record holds the new log's
inode, and the old state-indexes.bin by its size and SHA-256. Where
records.log is the new log, going back needs both old files, and
it refuses without them. After a run that finished, it puts the old
records.log, state.bin and state-indexes.bin back, with the blocked file in
place while it does, only while records.log and state.bin are as the run left
them, and it leaves branches.bin as it is. Where state.bin is the blocked file,
it puts the old files back. Where the swap had not begun, it removes the run's
files and changes no live file. A going back that stops can be run again. A
field index registered since the run is gone after a going back, and where
the old state-indexes.bin is not there as the run left it, the store has no
field index afterwards, which --go-back says. Once
the host has written, it refuses: undoing the run then loses every write since,
and this tool does not do it.

The tool needs free space for the new log, the size of what it keeps, a file
system with hard links, and memory of a few hundred bytes for each record of the
log and a few times the size of the largest record.

Usage:
    compact-set-states.py <store-dir> --state <pattern> [--state <pattern> ...] [--apply]
    compact-set-states.py <store-dir> --go-back

Python 3.9+, Unix. Exit 0: done, a dry run, or nothing to do; 1: refused, and
no file of the store changed; 2: usage; 3: stopped during the swap (run
--go-back).
"""
import errno
import fcntl
import hashlib
import json
import os
import re
import stat
import struct
import sys
import zlib

from chronicle_payload import NOT_READ, READ


STARTS = ('Set', 'Snapshot')  # where chronicle's read of a chain stops (src/state/chain_walk.rs)
KINDS = frozenset(READ) | frozenset(NOT_READ)
FORMS = {'map': 'named MessagePack', 'positional': 'positional MessagePack', 'json': 'JSON'}
FIELDS = ('record_id', 'global_sequence', 'state_id', 'prev_update_offset', 'operation', 'timestamp')


def pattern_matches(pattern, sid):
    """A --state pattern is literal text in which each * stands for any text, in this tool and in state-digests.mjs."""
    return re.fullmatch('.*'.join(re.escape(part) for part in pattern.split('*')), sid, re.DOTALL) is not None

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


def is_uint(v, bits=64):
    return is_count(v) and v < 1 << bits


# ── records.log framing ────────────────────────────────────────────────────

MAGIC = b'REC\0'
MESSAGEPACK = 1  # a record's encoding byte; earlier releases wrote state updates as JSON


class Refusal(Exception):
    pass


class Stopped(Exception):
    pass


class Rec:
    __slots__ = ('offset', 'length', 'rid', 'branch', 'refs', 'shaped', 'update', 'whole', 'form', 'sid', 'op', 'prev',
                 'payload_at', 'prev_at', 'prev_end')

    def __init__(self, offset, length, rid, branch, refs):
        self.offset, self.length, self.rid, self.branch, self.refs = offset, length, rid, branch, refs
        self.shaped = self.update = self.whole = False
        self.form = self.sid = self.op = self.prev = self.payload_at = self.prev_at = self.prev_end = None


def variant_of(value):
    if isinstance(value, MpMap) and len(value) == 1 and isinstance(value[0][0], str): return value[0]
    return None, None


def read_update(rec, enc, payload):
    """Fill in rec as a state update when each field of its payload is of its type and its operation is of a kind that
    StateOperation names (src/types.rs). Whether a record with those is read as one is settled by reach, in plan().
    rec.shaped says that the payload has a state_id and an operation, which chronicle's decode needs."""
    rec.shaped = True  # a payload that the tool can't parse may still be one that chronicle decodes
    try:
        if enc == MESSAGEPACK:
            value, end = mp_decode(payload)
            if end != len(payload): return
            form = 'map' if isinstance(value, MpMap) else 'positional'
        else:
            value, form = json.loads(payload, object_pairs_hook=MpMap), 'json'
    except DECODE_ERRORS:
        return
    if isinstance(value, MpMap) and not all(isinstance(key, str) for key, _ in value):
        return  # a key that is no string: chronicle's decode may still read the map by field numbers
    rec.shaped = False
    if isinstance(value, MpMap):
        keys = [key for key, _ in value]
        rec.shaped = 'state_id' in keys and 'operation' in keys
        fields, once = dict(value), len(keys) == len(set(keys))
    elif isinstance(value, list) and len(value) > FIELDS.index('operation'):
        rec.shaped = True
        if len(value) != len(FIELDS) or form != 'positional': return
        fields, once = dict(zip(FIELDS, value)), True
    else:
        return
    kind, body = variant_of(fields.get('operation'))
    timestamp, prev = fields.get('timestamp'), fields.get('prev_update_offset')
    if not (is_uint(fields.get('record_id')) and is_uint(fields.get('global_sequence'))
            and isinstance(fields.get('state_id'), str) and (prev is None or is_uint(prev))
            and isinstance(timestamp, int) and not isinstance(timestamp, bool) and -1 << 63 <= timestamp < 1 << 63
            and kind in KINDS):
        return
    rec.update, rec.form, rec.sid, rec.op, rec.prev = True, form, fields['state_id'], kind, prev
    rec.whole = form == 'map' and once and kind in STARTS and isinstance(body, MpBin)
    if form == 'map':
        at = 1 if payload[0] <= 0x8F else (3 if payload[0] == 0xDE else 5)
        for key, _ in value:
            _, value_at = mp_decode(payload, at)
            _, value_end = mp_decode(payload, value_at)
            if key == 'prev_update_offset': rec.prev_at, rec.prev_end = value_at, value_end
            at = value_end


# What chronicle's open does with a log that stops being whole (src/records/log.rs, scan_log).
DAMAGE = ('chronicle\'s open cuts off the log from there when no valid record follows, and says how many bytes it '
          'dropped. It refuses to open the store when a valid record follows. The check before a run is such an open: '
          'run it, and then this tool. A store that the check can\'t open needs a repair that this tool doesn\'t make.')


def scan_log(path):
    recs = []
    try:
        size = os.path.getsize(path)
        f = open(path, 'rb')
    except OSError as error:
        raise Refusal('could not read %s (%s).' % (path, error))
    with f:
        def need(n):
            b = f.read(n)
            if len(b) != n:
                raise Refusal('records.log is torn: its last record, at offset %d, is not whole. %s' % (start, DAMAGE))
            return b
        while f.tell() < size:
            start = f.tell()
            if need(4) != MAGIC:
                raise Refusal('records.log stops being whole at offset %d, where no record starts. %s' % (start, DAMAGE))
            ver = need(1)[0]
            if ver != 1: raise Refusal('the record at offset %d of records.log has version %d, which this tool does not read.' % (start, ver))
            need(1)
            rid, _seq, branch, _ts = struct.unpack('<QQQq', need(32))
            rtype = need(struct.unpack('<H', need(2))[0])
            enc = need(1)[0]
            payload_at = f.tell() + 4 - start
            payload = need(struct.unpack('<I', need(4))[0])
            refs = ()
            for _ in range(2):  # caused_by, then linked_to: record ids
                count = struct.unpack('<H', need(2))[0]
                refs += struct.unpack('<%dQ' % count, need(8 * count))
            if struct.unpack('<I', need(4))[0] != zlib.crc32(payload) & 0xFFFFFFFF:
                raise Refusal('the record at offset %d of records.log fails its checksum. %s' % (start, DAMAGE))
            rec = Rec(start, f.tell() - start, rid, branch, refs or None)
            if rtype == b'state_update':
                rec.payload_at = payload_at
                read_update(rec, enc, payload)
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
                    and head_is_whole(head) for key, head in index[0])
            and isinstance(index[1], MpMap) and all(isinstance(key, str) for key, _ in index[1])):
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
        self.marker = self.sb + '.compact-run'
        self.staging = (self.new_log, self.new_sb, self.blocked, self.marker + '.new')
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


def finished_text(files, kept):
    return ('If the check after that run was equal to the check before, and the host has run well since, delete %s to '
            'free the space. If the check was not equal, run compact-set-states.py %s --go-back before the host starts: '
            'it puts the old files back while records.log and state.bin are as the run left them. Once the host has '
            'written, undoing the run loses every write since, and it does not restore branches.bin, so this tool does '
            'not do it.' % (', '.join(kept), files.store))


def earlier_run(files, state_bin):
    if state_bin.startswith(BLOCKED):
        raise Refusal('state.bin is the blocked file of a compact-set-states.py run that stopped during its swap. %s'
                      % go_back_text(files.store))
    staging, kept = files.found(files.staging), files.found(files.kept + (files.marker,))
    if staging:
        raise Refusal('%s of a run that stopped before its swap %s in the store. With the host stopped, run '
                      'compact-set-states.py %s --go-back, which removes them.'
                      % (', '.join(staging + kept), 'is' if len(staging + kept) == 1 else 'are', files.store))
    if kept:
        raise Refusal('%s of an earlier run that finished %s in the store. %s'
                      % (', '.join(kept), 'is' if len(kept) == 1 else 'are', finished_text(files, kept)))


def canonical(value):
    """The value with each map's entries in the order of their encoded keys. chronicle writes some maps of state.bin
    from a Rust HashMap, such as a Struct strategy's fields, and their order changes from one save to the next."""
    if isinstance(value, MpMap):
        return MpMap(sorted(((key, canonical(item)) for key, item in value), key=lambda entry: mp_encode(entry[0])))
    if isinstance(value, list):
        return [canonical(item) for item in value]
    return value


def what_the_run_left(log_inode, log_size, index):
    """What --go-back checks after a finished run, before it overwrites records.log and state.bin: the log by inode
    and size, since a write only adds to it, and state.bin's strategies and each head's branch, state and offsets.
    Counters and the number of a head's fields are left out, so a save by another release still matches, and the
    strategies are compared with each map's entries in one order."""
    heads = sorted([key[0], key[1], head[HEAD], head[LAST_DELTA], head[LAST_FULL]] for key, head in index[0])
    strategies = sorted([sid, mp_encode(canonical(strategy)).hex()] for sid, strategy in index[1])
    return {'records.log': [log_inode, log_size], 'heads': heads, 'strategies': strategies}


def take_lock(store):
    """The lock chronicle takes on LOCK (src/store.rs, acquire_lock)."""
    try:
        # For writing alone, as chronicle opens it: on NFS an exclusive flock is a POSIX lock, which needs that.
        fd = os.open(os.path.join(store, 'LOCK'), os.O_WRONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        raise Refusal('LOCK is missing. Make sure that no host has the store open. Then run the check, whose open '
                      'creates LOCK, and run again.')
    except OSError as error:
        if error.errno == errno.ELOOP:
            raise Refusal('LOCK is a symbolic link, which chronicle refuses too. Move it aside.')
        raise Refusal('could not open LOCK (%s).' % error)
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        raise Refusal('LOCK is not a regular file, which chronicle refuses too. Move it aside.')
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as error:
        os.close(fd)
        if error.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
            raise Refusal('LOCK is locked: the store is open. Stop its host first.')
        raise Refusal('could not lock LOCK (%s).' % error)
    return fd


BRANCH_INDEX_MAGIC = b'BRI\0'


def branch_ids(store):
    """The ids of the branches that branches.bin holds (src/branches/manager.rs, BranchManager::save): BRI\\0, version
    1, the current branch as a u64, the length as a u64, then MessagePack [branches by id, names, next id]."""
    try:
        data = read_bytes(os.path.join(store, 'branches.bin'))
    except FileNotFoundError:
        raise Refusal('the store has no branches.bin.')
    if data[:4] != BRANCH_INDEX_MAGIC or len(data) < 21 or data[4] != 1:
        raise Refusal('branches.bin is not a branch index that this tool reads.')
    n = struct.unpack_from('<Q', data, 13)[0]
    try:
        index, end = mp_decode(data[21:21 + n])
    except DECODE_ERRORS:
        index, end = None, -1
    if end != n or len(data) != 21 + n or not (isinstance(index, list) and len(index) == 3 and isinstance(index[0], MpMap)
                                               and all(is_count(key) for key, _ in index[0])):
        raise Refusal('branches.bin is not a branch index that this tool reads.')
    return {key for key, _ in index[0]}


STORE_MAGIC = b'RST\0'
KNOWN_FEATURES = 1  # AppendMany's bit, the one feature that a version 2 MANIFEST names (src/store.rs)


def read_manifest(store):
    try:
        data = read_bytes(os.path.join(store, 'MANIFEST'))
    except FileNotFoundError:
        raise Refusal('the store has no MANIFEST, so it is not a chronicle store that this tool reads.')
    if data[:4] != STORE_MAGIC or len(data) < 5: raise Refusal('MANIFEST is not a chronicle store\'s.')
    if data[4] == 1: return
    if data[4] == 2 and len(data) >= 13:
        features = struct.unpack_from('<Q', data, 5)[0]
        if features & ~KNOWN_FEATURES == 0: return
        raise Refusal('MANIFEST names format features %#x, and this tool reads only AppendMany\'s (%#x).'
                      % (features, KNOWN_FEATURES))
    raise Refusal('MANIFEST names store version %d, which this tool does not read.' % data[4])


def write_file(path, chunks, like, made=None):
    """Write a new file at path, which must not exist. Once the file exists, its path goes on made, the list of what
    a failed run removes, so a path that someone else made first is never removed."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    if made is not None: made.append(path)
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

def plan(recs, heads, patterns, branches):
    by_offset = {r.offset: r for r in recs}
    # A record after the newest head proves nothing by its place, so the tool changes none of them. chronicle's open
    # replays only an update whose branch branches.bin holds (Store::open in src/store.rs).
    newest_head = max((head[HEAD] for _, head in heads), default=-1)
    for r in recs:
        if r.offset > newest_head and r.shaped and r.branch in branches:
            raise Refusal('the record at offset %d lies after the newest head in state.bin, on branch %d, which '
                          'branches.bin holds, and it may be a state update that state.bin does not have yet. Only '
                          'chronicle\'s open settles that. Run the check with the chronicle release that the host uses, '
                          'which opens and closes the store, and run this tool again. chronicle 0.5.0 or later then '
                          'applies such an update when its link matches the head of its state. When it does not, '
                          'chronicle refuses to open the store, and such a store needs a repair that this tool doesn\'t '
                          'make. If this refusal comes again, chronicle did not apply the record: 0.4.0 replays nothing '
                          'at open, and no release applies a record that it does not decode. The refusal then ends once '
                          'the host has written any state and closed the store. The record then lies before the newest '
                          'head, where no head reaches it, and it is copied.' % (r.offset, r.branch))
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
    # chronicle sets a state update's link to its pair's head (Store::update_state), and each read of a state starts
    # at a head and follows the links. So the updates that the links reach from the heads are real ones, and only
    # those are relinked or dropped. An update that no head reaches is copied, for a named state too.
    reached = set()
    for key, head in heads:
        r = by_offset[head[HEAD]]
        while r.offset not in reached:
            reached.add(r.offset)
            if r.prev is None: break
            prior = by_offset.get(r.prev)
            if prior is None or not prior.update or prior.sid != r.sid:
                raise Refusal('the state update of %s at offset %d links to offset %d, where no state update of %s '
                              'is. chronicle links each update to an update of its own state, so records.log is not '
                              'as chronicle wrote it.' % (r.sid, r.offset, r.prev, r.sid))
            r = prior
    named = lambda sid: any(pattern_matches(p, sid) for p in patterns)
    for r in recs:
        if r.offset in reached and named(r.sid) and not r.whole:
            raise Refusal('%s is not a state whose every update is a whole value: its update at offset %d is %s. This '
                          'tool frees only the old values of a state that each update sets whole, with Set or Snapshot.'
                          % (r.sid, r.offset, 'a %s' % r.op if r.form == 'map' else 'in the %s form' % FORMS[r.form]))
    # chronicle's read of a whole value stops at the head where it starts, so of a named state each head's record
    # stays, and the other updates that the walk reaches go.
    keep = {head[HEAD] for key, head in heads if named(key[1])}
    drop = {offset for offset in reached if named(by_offset[offset].sid) and offset not in keep}

    new_offset, links, at = {}, {}, 0
    for r in recs:
        if r.offset in drop: continue
        new_offset[r.offset] = at
        length = r.length
        if r.update and r.prev is not None and r.offset in reached:
            if r.prev not in drop and r.prev not in new_offset:
                raise Refusal('the state update of %s at offset %d links to offset %d, where no earlier record is. '
                              'chronicle links each update to an earlier one, so records.log is not as chronicle wrote '
                              'it.' % (r.sid, r.offset, r.prev))
            link = None if r.prev in drop else new_offset[r.prev]
            if link != r.prev:
                if r.form != 'map':
                    raise Refusal('the state update of %s at offset %d needs a new link, and it is in the %s form, '
                                  'which this tool does not rewrite.' % (r.sid, r.offset, FORMS[r.form]))
                links[r.offset] = link
                length += len(mp_encode(link)) - (r.prev_end - r.prev_at)
        at += length
    return drop, reached, new_offset, links, at


def relinked(raw, rec, link):
    plen_at = rec.payload_at - 4
    payload = raw[rec.payload_at:rec.payload_at + struct.unpack_from('<I', raw, plen_at)[0]]
    new = payload[:rec.prev_at] + mp_encode(link) + payload[rec.prev_end:]
    tail = raw[rec.payload_at + len(payload):-4]  # caused_by and linked_to
    return raw[:plen_at] + struct.pack('<I', len(new)) + new + tail + struct.pack('<I', zlib.crc32(new) & 0xFFFFFFFF)


# ── the run ────────────────────────────────────────────────────────────────

def locked(store, act):
    resolve(store, 'records.log')
    lock = take_lock(store)
    try:
        return act()
    finally:
        os.close(lock)


def read_bytes(path):
    try:
        with open(path, 'rb') as f:
            return f.read()
    except FileNotFoundError:
        raise
    except OSError as error:
        raise Refusal('could not read %s (%s).' % (path, error))


def run(store, patterns, apply):
    def act():
        files = Files(store)
        state_bin = read_bytes(files.sb)
        earlier_run(files, state_bin)
        read_manifest(store)
        compact(files, patterns, apply, state_bin)
    locked(store, act)


def compact(files, patterns, apply, state_bin):
    print('scanning %s (%s)' % (files.log, human(os.path.getsize(files.log))))
    recs = scan_log(files.log)
    index = read_state_bin(state_bin)
    heads = index[0]
    named = sorted({r.sid for r in recs if r.update and any(pattern_matches(p, r.sid) for p in patterns)})
    if not named:
        raise Refusal('no state update in records.log belongs to a state that %s names.'
                      % ' or '.join('--state %s' % p for p in patterns))
    drop, reached, new_offset, links, new_size = plan(recs, heads, patterns, branch_ids(files.store))

    dropped_by_state, unreached_by_state = {}, {}
    for r in recs:
        if r.update and r.sid in named and r.offset not in reached:
            n, b = unreached_by_state.get(r.sid, (0, 0)); unreached_by_state[r.sid] = (n + 1, b + r.length)
        if r.offset in drop:
            n, b = dropped_by_state.get(r.sid, (0, 0)); dropped_by_state[r.sid] = (n + 1, b + r.length)
    print('%d records, %d branches; %d named state(s):' % (len(recs), len({r.branch for r in recs}), len(named)))
    for sid in named:
        n, b = dropped_by_state.get(sid, (0, 0))
        line = '  %s: drop %d old value(s), %s' % (sid, n, human(b))
        if sid in unreached_by_state:
            n, b = unreached_by_state[sid]
            line += '; %d update(s), %s, stay, since no head reaches them' % (n, human(b))
        print(line)
    dropped = {r.rid for r in recs if r.offset in drop}
    naming = [r for r in recs if r.refs and r.offset not in drop and not dropped.isdisjoint(r.refs)]
    if naming:
        print('%d record(s) that stay name a dropped record in caused_by or linked_to, the first at offset %d. After '
              'the run a read of a dropped record by its id gives nothing.' % (len(naming), naming[0].offset))
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
        write_file(files.new_log, records(), log_stat, made)
        if os.path.getsize(files.new_log) != new_size: raise RuntimeError('the new log is not the size its plan says')
        write_file(files.new_sb, [state_bin_bytes([new_heads, index[1]])], sb_stat, made)
        write_file(files.blocked, [blocked_state_bin(files.store)], sb_stat, made)
        # The record is written whole beside its name and then renamed, so a record under its name is complete.
        left = what_the_run_left(os.stat(files.new_log).st_ino, new_size, [new_heads, index[1]])
        left['state-indexes.bin'] = index_print(files.indexes) if os.path.exists(files.indexes) else None
        write_file(files.marker + '.new', [json.dumps(left).encode('utf-8')], sb_stat, made)
        made.append(files.marker); os.replace(files.marker + '.new', files.marker)
        sync_dir(os.path.dirname(files.marker))
        for path in (files.log, files.sb):
            os.link(path, path + '.pre-compact'); made.append(path + '.pre-compact')
        for directory in {os.path.dirname(files.log), os.path.dirname(files.sb)}: sync_dir(directory)
    except BaseException as error:
        stayed = []
        for path in made:
            try:
                if os.path.lexists(path): os.remove(path)
            except OSError:
                stayed.append(path)
        raise Refusal('could not write the new files and the old files\' second names (%s: %s).%s'
                      % (type(error).__name__, error, ' %s could not be removed. They are files of this run, '
                                                        'and the store\'s own files are as they were, so they can be '
                                                        'removed by hand.' % ', '.join(stayed) if stayed else ''))

    steps = [('put the blocked file in place of state.bin', files.blocked, files.sb)]
    if os.path.exists(files.indexes):
        steps.append(('moved state-indexes.bin to its .pre-compact name', files.indexes, files.indexes + '.pre-compact'))
    steps += [('put the new log in place of records.log', files.new_log, files.log),
              ('put the new state.bin in place of the blocked file', files.new_sb, files.sb)]
    done = 'wrote the new files'
    for step, source, target in steps:
        try:
            os.replace(source, target)
            done = step
            sync_dir(os.path.dirname(target))
        except BaseException as error:
            raise Stopped('the swap stopped after it %s (%s: %s). %s'
                          % (done, type(error).__name__, error, go_back_text(files.store)))
    kept = [path + '.pre-compact' for path in (files.log, files.sb)] + (
        [files.indexes + '.pre-compact'] if left['state-indexes.bin'] is not None else [])
    print('done: records.log is %s. The old files are %s, and the run\'s record is %s.'
          % (human(new_size), ', '.join(kept), files.marker))
    print('Check with state-digests.mjs and compare with the check from before. If they differ, run --go-back before '
          'the host starts. Once the host has run well, delete %s and %s to free the space.'
          % (', '.join(kept), files.marker))


def go_back(store):
    def act():
        files = Files(store)
        old_log, old_sb, old_indexes = files.kept
        staging, kept = files.found(files.staging), files.found(files.kept)
        other = lambda old, live: os.path.lexists(old) and not os.path.samestat(os.lstat(old), os.lstat(live))
        steps = []
        if not os.path.lexists(files.marker):
            # A run writes its record before the old files get their second names, so no live file has changed,
            # unless the record was deleted by hand.
            if read_bytes(files.sb).startswith(BLOCKED):
                found = staging + kept
                raise Refusal('state.bin is the blocked file of a run, and the record of that run, %s, is not there. '
                              'So --go-back can\'t tell where the run stopped, or whether records.log is the old log or '
                              'the new one. It removed nothing. %s The store needs a repair that this tool doesn\'t make.'
                              % (files.marker, 'Of the run\'s files, %s %s there.'
                                 % (', '.join(found), 'is' if len(found) == 1 else 'are')
                                 if found else 'No other file of the run is there.'))
            if kept:
                raise Refusal('%s %s in the store, and the record of the run, %s, is not, so --go-back can\'t tell '
                              'where that run stopped. %s' % (', '.join(kept), 'is' if len(kept) == 1 else 'are',
                                                             files.marker, finished_text(files, kept)))
            if not staging:
                print('No file of a compact-set-states.py run is in %s, so nothing changed.' % store); return
            said = 'removed the files of a run that stopped before it wrote its record. No file of the store changed.'
        else:
            left = read_record(files.marker)
            log_is_new = os.stat(files.log).st_ino == left['records.log'][0]
            blocked = read_bytes(files.sb).startswith(BLOCKED)
            if log_is_new:
                missing = [old for old, live in ((old_log, files.log), (old_sb, files.sb)) if not other(old, live)]
                if missing and blocked:
                    there = os.path.lexists(files.new_sb)
                    raise Refusal('records.log is the log that the run wrote, state.bin is the blocked file, and %s, the '
                                  'old file that going back puts in its place, is not there. So there is nothing to go '
                                  'back to, and chronicle can\'t open the store as it is. %s'
                                  % (' and '.join(missing),
                                     'The run\'s new state.bin is still at %s, and it belongs to this records.log: '
                                     'renaming it to %s finishes the run. Then check the store before a host '
                                     'starts.' % (files.new_sb, files.sb) if there else
                                     'The run\'s new state.bin is not there either. The store needs a repair that this '
                                     'tool doesn\'t make.'))
                if missing:
                    run_files = files.found(files.staging + files.kept + (files.marker,))
                    raise Refusal('records.log is the log that the run wrote, and %s, the old file that going back puts '
                                  'in its place, is not there, so there is nothing to go back to. If the check of the '
                                  'store is equal to the check from before the run, delete %s.'
                                  % (' and '.join(missing), ', '.join(run_files)))
                if blocked:
                    said = 'went back: records.log and state.bin are as they were before the run, and no file of the run is left.'
                else:
                    changed = run_changed(files, left)
                    if changed:
                        raise Refusal('%s has changed since the run finished, so going back would lose what changed. '
                                      'Undoing the run is then outside what this tool does.' % changed)
                    steps.append(('block', files.sb))
                    said = ('went back after a finished run: records.log and state.bin are as they were before it, and '
                            'no file of the run is left. branches.bin is as it is now.')
                steps += [('indexes', files.indexes), ('put back', files.log), ('put back', files.sb)]
            else:
                if other(old_log, files.log):
                    raise Refusal('records.log is not the log that the run wrote, and %s is another file than '
                                  'records.log, so the run\'s record, %s, does not belong to these files, as after a copy '
                                  'of the store.' % (old_log, files.marker))
                if blocked:
                    if not os.path.lexists(old_sb):
                        raise Refusal('state.bin is the blocked file, and the old state.bin, %s, is not there to put '
                                      'back.' % old_sb)
                    steps = [('indexes', files.indexes), ('put back', files.sb)]
                    said = 'went back: records.log and state.bin are as they were before the run, and no file of the run is left.'
                else:
                    said = ('removed the files of the run, whose swap had not begun or had been undone. No file of the '
                            'store changed.')
        try:
            for step, live in steps:
                old = live + '.pre-compact'
                if step == 'block':
                    if os.path.lexists(files.blocked): os.remove(files.blocked)
                    write_file(files.blocked, [blocked_state_bin(store)], os.stat(live))
                    os.replace(files.blocked, live)
                elif step == 'indexes':
                    # A going back leaves a state-indexes.bin only with the bytes that the run recorded: the old
                    # one, also when an earlier going back put it back. Any other goes, one made since the run and a
                    # .pre-compact file changed by hand alike.
                    if os.path.lexists(old) and index_print(old) == left['state-indexes.bin']:
                        os.replace(old, live)
                    else:
                        if os.path.lexists(old): os.remove(old)
                        if os.path.lexists(live) and index_print(live) != left['state-indexes.bin']: os.remove(live)
                elif os.path.lexists(old):
                    os.replace(old, live)
                # A step whose rename an earlier going back made syncs too, since that going back may have stopped
                # before its sync.
                sync_dir(os.path.dirname(live))
            # Whether the store's own index is back, read while the record is still there to run again from.
            index_back = (steps and left['state-indexes.bin'] is not None and os.path.lexists(files.indexes)
                          and index_print(files.indexes) == left['state-indexes.bin'])
            # What is left: the staging files, second names of the live files, and a state.bin.pre-compact that a
            # host made stale by saving state.bin since a stop before the swap. The record goes last.
            for path in files.found(files.staging + (old_log, old_sb, files.marker)):
                os.remove(path); sync_dir(os.path.dirname(path))
        except BaseException as error:
            why = str(error) if isinstance(error, Refusal) else '%s: %s' % (type(error).__name__, error)
            raise Stopped('going back stopped (%s). Run compact-set-states.py %s --go-back again, with the host '
                          'stopped.' % (why, store))
        if steps and left['state-indexes.bin'] is not None:
            if index_back:
                said += ' state-indexes.bin is as it was before the run, too.'
            else:
                said += (' The old state-indexes.bin was not there as the run left it, so the store has no field index '
                         'now, and a caller registers its indexes again.')
        print(said)
    locked(store, act)


def index_print(path):
    """A state-indexes.bin by its bytes: their count and SHA-256."""
    data = read_bytes(path)
    return [len(data), hashlib.sha256(data).hexdigest()]


def read_record(marker):
    try:
        left = json.loads(read_bytes(marker))
    except ValueError:
        left = None
    if not (isinstance(left, dict) and isinstance(left.get('records.log'), list) and len(left['records.log']) == 2
            and 'state-indexes.bin' in left and (left['state-indexes.bin'] is None or (
                isinstance(left['state-indexes.bin'], list) and len(left['state-indexes.bin']) == 2
                and is_count(left['state-indexes.bin'][0]) and isinstance(left['state-indexes.bin'][1], str)))):
        raise Refusal('%s is not the record of a run that this tool reads.' % marker)
    return left


def run_changed(files, left):
    """Which of records.log and state.bin differs from what the finished run left, or None."""
    log = os.stat(files.log)
    now = what_the_run_left(log.st_ino, log.st_size, read_state_bin(read_bytes(files.sb)))
    if now['records.log'] != left.get('records.log'): return 'records.log'
    if [now['heads'], now['strategies']] != [left.get('heads'), left.get('strategies')]: return 'state.bin'
    return None


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
        print('at least one --state <pattern> is required'); return 2
    try:
        if back: go_back(store)
        else: run(store, patterns, apply)
    except Refusal as refusal:
        print('refusing: %s No file of the store changed.' % refusal); return 1
    except Stopped as stopped:
        print('stopped: %s' % stopped); return 3
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
