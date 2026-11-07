#!/usr/bin/env python3
"""Reclaim records.log space held by superseded whole-state writes.

Chronicle's record log is append-only: a state that is rewritten with `Set`
(or re-snapshotted) on every write keeps every previous value on disk
forever, and reconstruction never reads past the newest terminal operation
(`Set`, `Snapshot`, `Delta`). This tool rewrites `records.log` without the
records of the named states that lie BEFORE their newest terminal operation,
re-links the surviving chains, and remaps `state.bin`. Nothing about the
reconstructed value of any state changes; the states not named are copied
untouched, record ids and sequences are preserved (the log simply has gaps),
and `state-indexes.bin` is removed so the store rebuilds it on open.

Only states whose newest operation (or an operation between it and the head)
is terminal can be compacted; an append-log state whose history is all
`Append`s is refused, because every record of it is live.

Safety:
  * refuses while the store is open (flock on LOCK),
  * refuses a multi-branch store (a child branch's chains walk the parent's
    records),
  * refuses when `state.bin` does not agree with the log (head offsets must be
    the last record of each state: stop the agent cleanly first),
  * refuses a torn or corrupt log (run trim-records-log.py first),
  * dry-run by default; `--apply` swaps the files in and keeps the originals
    as `records.log.pre-compact` / `state.bin.pre-compact` until you delete
    them.

Usage:
    compact-set-states.py <store-dir> --state <glob> [--state <glob> ...] [--apply]

Example (kv-unified presentation receipts, context-manager #148):
    compact-set-states.py data/sessions/<id> --state 'agents/*/kvunified:presentation-receipt' --apply

Verify with tools/state-digests.mjs before and after; the digests must match.
Python 3.9+, no dependencies.
"""
import fcntl
import fnmatch
import os
import struct
import sys
import zlib

TERMINAL_OPS = ('Set', 'Snapshot', 'Delta')

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


def map_set(m, key, value):
    for idx, (k, _) in enumerate(m):
        if k == key:
            m[idx] = (k, value); return
    m.append((key, value))


# ── records.log framing ────────────────────────────────────────────────────

MAGIC = b'REC\0'


class Rec:
    __slots__ = ('offset', 'length', 'branch', 'rtype', 'sid', 'op', 'prev', 'seq')

    def __init__(self, offset, length, branch, rtype, sid, op, prev, seq):
        self.offset, self.length, self.branch, self.rtype = offset, length, branch, rtype
        self.sid, self.op, self.prev, self.seq = sid, op, prev, seq


def scan_log(path):
    """Yield Rec for every record; raise on a torn or corrupt log."""
    size = os.path.getsize(path)
    with open(path, 'rb') as f:
        def need(n):
            b = f.read(n)
            if len(b) != n: raise ValueError('short read at %d (torn tail?)' % (f.tell() - len(b)))
            return b
        while f.tell() < size:
            start = f.tell()
            if need(4) != MAGIC: raise ValueError('bad magic at %d' % start)
            ver = need(1)[0]
            if ver != 1: raise ValueError('unknown record version %d at %d' % (ver, start))
            need(1)
            _rid, seq, br, _ts = struct.unpack('<QQQq', need(32))
            tlen = struct.unpack('<H', need(2))[0]; rtype = need(tlen).decode('utf-8')
            enc = need(1)[0]
            plen = struct.unpack('<I', need(4))[0]
            sid = op = prev = None
            if rtype == 'state_update':
                if enc != 1: raise ValueError('state_update at %d is not MessagePack (enc=%d)' % (start, enc))
                payload = need(plen)
                m, _ = mp_decode(payload)
                sid = map_get(m, 'state_id'); prev = map_get(m, 'prev_update_offset')
                opv = map_get(m, 'operation')
                op = opv if isinstance(opv, str) else opv[0][0]
            else:
                f.seek(plen, 1)
            cbc = struct.unpack('<H', need(2))[0]; need(8 * cbc)
            ltc = struct.unpack('<H', need(2))[0]; need(8 * ltc)
            need(4)
            yield Rec(start, f.tell() - start, br, rtype, sid, op, prev, seq)


def read_record_bytes(f, rec):
    f.seek(rec.offset)
    return f.read(rec.length)


def rewrite_prev(raw, new_prev):
    """Return the record bytes with payload.prev_update_offset = new_prev and a fresh CRC."""
    i = 4 + 1 + 1 + 32
    tlen = struct.unpack_from('<H', raw, i)[0]; i += 2 + tlen
    i += 1  # encoding
    plen = struct.unpack_from('<I', raw, i)[0]; pstart = i + 4
    payload = raw[pstart:pstart + plen]
    m, end = mp_decode(payload)
    if end != plen: raise ValueError('payload has trailing bytes')
    map_set(m, 'prev_update_offset', new_prev)
    new_payload = mp_encode(m)
    tail = raw[pstart + plen:len(raw) - 4]  # caused_by + linked_to
    return (raw[:i] + struct.pack('<I', len(new_payload)) + new_payload + tail
            + struct.pack('<I', zlib.crc32(new_payload) & 0xFFFFFFFF))


# ── state.bin ──────────────────────────────────────────────────────────────

STI_MAGIC = b'STI\0'
STI_VERSION = 2


def read_state_bin(path):
    with open(path, 'rb') as f:
        if f.read(4) != STI_MAGIC: raise ValueError('state.bin: bad magic')
        if f.read(1)[0] != STI_VERSION: raise ValueError('state.bin: unsupported version')
        n = struct.unpack('<Q', f.read(8))[0]
        enc = f.read(n)
    index, end = mp_decode(enc)
    if end != n: raise ValueError('state.bin: trailing bytes')
    # rmp_serde::to_vec: StateIndex = [heads: map{[branch, state_id] -> [8 fields]}, strategies: map]
    if not (isinstance(index, list) and len(index) == 2 and isinstance(index[0], MpMap)):
        raise ValueError('state.bin: unexpected StateIndex shape')
    return index


def write_state_bin(path, index):
    enc = mp_encode(index)
    tmp = path + '.tmp'
    with open(tmp, 'wb') as f:
        f.write(STI_MAGIC); f.write(bytes([STI_VERSION])); f.write(struct.pack('<Q', len(enc))); f.write(enc)
        f.flush(); os.fsync(f.fileno())
    os.replace(tmp, path)


# ── main ───────────────────────────────────────────────────────────────────

def human(n):
    return '%.1f MB' % (n / 1e6)


def main(argv):
    if len(argv) < 2 or argv[1] in ('-h', '--help'):
        print(__doc__); return 2
    store = argv[1]; patterns = []; apply = False
    i = 2
    while i < len(argv):
        if argv[i] == '--state': patterns.append(argv[i + 1]); i += 2
        elif argv[i] == '--apply': apply = True; i += 1
        else: print('unknown argument %s' % argv[i]); return 2
    if not patterns:
        print('at least one --state <glob> is required'); return 2
    log_path = os.path.join(store, 'records.log'); sb_path = os.path.join(store, 'state.bin')
    for p in (log_path, sb_path):
        if not os.path.exists(p): print('missing %s' % p); return 2

    # 1. The store must be closed.
    lock_path = os.path.join(store, 'LOCK')
    lock = open(lock_path, 'a+')
    try:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print('refusing: %s is locked (the store is open; stop the agent first)' % lock_path); return 1

    # 2. Scan.
    print('scanning %s (%s)' % (log_path, human(os.path.getsize(log_path))))
    recs = list(scan_log(log_path))
    branches = sorted({r.branch for r in recs})
    if len(branches) > 1:
        print('refusing: %d branches in the log (%s); compaction is single-branch only' % (len(branches), branches)); return 1
    by_offset = {r.offset: r for r in recs}
    last_of_state = {}
    for r in recs:
        if r.rtype == 'state_update': last_of_state[(r.branch, r.sid)] = r

    # 3. state.bin must agree with the log.
    index = read_state_bin(sb_path)
    heads = index[0]
    stale = []
    for key, head in heads:
        br, sid = key[0], key[1]
        last = last_of_state.get((br, sid))
        if last is None or head[0] != last.offset:
            stale.append((sid, head[0], last.offset if last else None))
    if stale:
        print('refusing: state.bin disagrees with the log for %d state(s) (head_offset vs last record):' % len(stale))
        for sid, h, l in stale[:10]: print('  %s: state.bin=%s log=%s' % (sid, h, l))
        print('start and stop the agent cleanly (state.bin is written on close), then retry')
        return 1

    # 4. Choose what to drop: for each matching state, everything before the newest terminal op.
    drop = set(); report = []; kept_after_terminal = {}
    for (br, sid), last in sorted(last_of_state.items()):
        if not any(fnmatch.fnmatchcase(sid, p) for p in patterns): continue
        chain = []; r = last
        while r is not None:
            chain.append(r)
            r = by_offset.get(r.prev) if r.prev is not None else None
        chain.reverse()  # oldest -> newest
        term = max((k for k, c in enumerate(chain) if c.op in TERMINAL_OPS), default=None)
        if term is None:
            print('  skip %s: no terminal op in its %d-record chain (nothing is superseded)' % (sid, len(chain))); continue
        dropped = chain[:term]
        for c in dropped: drop.add(c.offset)
        kept_after_terminal[(br, sid)] = len(chain) - term - 1
        report.append((sid, len(chain), len(dropped), sum(c.length for c in dropped), chain[term].op))
    total_drop = sum(b for _, _, _, b, _ in report)
    print('%d records, %d states; %d matching state(s):' % (len(recs), len(last_of_state), len(report)))
    for sid, n, d, b, op in report:
        print('  %s: %d records, drop %d before the newest %s (%s)' % (sid, n, d, op, human(b)))
    print('would drop %d records, %s of %s' % (len(drop), human(total_drop), human(os.path.getsize(log_path))))
    if not drop:
        return 0
    if not apply:
        print('dry run; add --apply to rewrite'); return 0

    # 5. Rewrite the log.
    out_path = log_path + '.compact'
    new_offset = {}
    chain_prev = {}  # (branch, sid) -> new offset of the previous kept record of that chain
    with open(log_path, 'rb') as src, open(out_path, 'wb') as dst:
        for r in recs:
            if r.offset in drop: continue
            raw = read_record_bytes(src, r)
            if r.rtype == 'state_update':
                if r.prev is None:
                    new_prev = None
                elif r.prev in drop:
                    new_prev = None  # the newest terminal op starts its chain afresh
                else:
                    new_prev = new_offset[r.prev]
                raw = rewrite_prev(raw, new_prev)
            new_offset[r.offset] = dst.tell()
            dst.write(raw)
        dst.flush(); os.fsync(dst.fileno())
    kept = sum(1 for r in recs if r.offset not in drop)
    print('wrote %s: %d records, %s' % (out_path, kept, human(os.path.getsize(out_path))))

    # 6. Remap state.bin offsets (head, last delta snapshot, last full snapshot).
    def remap(off, what, sid):
        if off is None: return None
        if off in drop:
            if what == 'head': raise RuntimeError('head of %s was dropped (bug)' % sid)
            return None  # a snapshot older than the newest terminal op: no longer needed
        return new_offset[off]
    new_heads = MpMap()
    for key, head in heads:
        sid = key[1]; h = list(head)
        h[0] = remap(h[0], 'head', sid); h[3] = remap(h[3], 'delta', sid); h[4] = remap(h[4], 'full', sid)
        if (key[0], sid) in kept_after_terminal:
            # ops_since_delta_snapshot: the newest terminal op is now the chain's base.
            h[1] = kept_after_terminal[(key[0], sid)]
        new_heads.append((key, h))
    new_index = [new_heads, index[1]]

    # 7. Swap in. Originals stay as *.pre-compact.
    os.replace(log_path, log_path + '.pre-compact')
    os.replace(out_path, log_path)
    os.replace(sb_path, sb_path + '.pre-compact')
    write_state_bin(sb_path, new_index)
    fi = os.path.join(store, 'state-indexes.bin')
    if os.path.exists(fi):
        os.replace(fi, fi + '.pre-compact')
    print('done. originals kept as records.log.pre-compact / state.bin.pre-compact / state-indexes.bin.pre-compact;')
    print('verify with state-digests.mjs, then delete them to reclaim the space.')
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
