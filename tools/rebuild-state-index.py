#!/usr/bin/env python3
# Rebuild chronicle state.bin (StateIndex) by scanning records.log.
# Usage: rebuild-state-index.py <store-dir>
#
# Handles broken chains: if the agent ever booted with a lost/empty state
# index, its writes started fresh chains (prev_update_offset=None). We track
# every chain per (branch, state_id) and keep the LONGEST one as the head —
# the tiny post-loss chains (a boot timestamp, a time-module tick) lose.
#
# strategies map is left EMPTY: apps re-register their states on boot.
import sys, struct, zlib, json, os

store = sys.argv[1]
logp = os.path.join(store, 'records.log')
f = open(logp, 'rb'); size = f.seek(0, 2); f.seek(0)

def need(n):
    b = f.read(n)
    if len(b) != n: raise EOFError(f'short read at {f.tell()-len(b)}')
    return b

def new_chain():
    return {'head_offset':0,'ops_delta':0,'deltas_since_full':0,
            'last_delta':None,'last_full':None,'non_append':False,'items':0,'ops':0}

chains = {}   # key -> [chain, ...]
headpos = {}  # (key, current_head_offset) -> chain
count = 0
while f.tell() < size:
    start = f.tell()
    magic = need(4)
    if magic != b'REC\0': raise ValueError(f'bad magic at {start}')
    need(1); need(1)
    rid, seq, br, ts = struct.unpack('<QQQq', need(32))
    tlen = struct.unpack('<H', need(2))[0]; rtype = need(tlen).decode()
    need(1)
    plen = struct.unpack('<I', need(4))[0]; payload = need(plen)
    cbc = struct.unpack('<H', need(2))[0]; need(8*cbc)
    ltc = struct.unpack('<H', need(2))[0]; need(8*ltc)
    crc = struct.unpack('<I', need(4))[0]
    assert crc == zlib.crc32(payload) & 0xffffffff, f'crc fail at {start}'
    count += 1
    if rtype != 'state_update': continue
    upd = json.loads(payload)
    sid = upd['state_id']; op = upd['operation']
    opname = op if isinstance(op, str) else next(iter(op))
    key = (br, sid)
    prev = upd.get('prev_update_offset')
    ch = None
    if prev is not None:
        ch = headpos.pop((key, prev), None)
    if ch is None:
        ch = new_chain()
        chains.setdefault(key, []).append(ch)
    ch['ops'] += 1
    ch['head_offset'] = start
    headpos[(key, start)] = ch
    if opname == 'Snapshot':
        raw = bytes(op['Snapshot'])
        try: ch['items'] = len(json.loads(raw))
        except Exception: ch['items'] = 0
        ch['last_full'] = start; ch['ops_delta'] = 0; ch['deltas_since_full'] = 0; ch['non_append'] = False
    elif opname == 'DeltaSnapshot':
        ch['last_delta'] = start; ch['ops_delta'] = 0; ch['deltas_since_full'] += 1
    elif opname == 'Append':
        ch['items'] += 1; ch['ops_delta'] += 1
    elif opname == 'Redact':
        ch['items'] -= (op['Redact']['end'] - op['Redact']['start']); ch['ops_delta'] += 1; ch['non_append'] = True
    else:  # Edit, Set, Delta, TreeSet, TreeRemove
        ch['ops_delta'] += 1; ch['non_append'] = True
f.close()

heads = {}
print(f'scanned {count} records')
# FORCE_HEAD="messages@12345,other/state@678" pins a specific chain (by its
# head offset) for named states — for when "longest" is the wrong epoch.
force = dict(kv.rsplit('@', 1) for kv in os.environ.get('FORCE_HEAD', '').split(',') if kv)
for key, cl in sorted(chains.items()):
    if key[1] in force:
        want = int(force[key[1]])
        best = next(c for c in cl if c['head_offset'] == want)
        print(f'  FORCED {key}: head@{want}')
    else:
        best = max(cl, key=lambda c: c['ops'])
    if len(cl) > 1:
        print(f'  NOTE {key}: {len(cl)} chains (ops: {sorted((c["ops"] for c in cl), reverse=True)}) -> keeping longest')
    heads[key] = best
    print(f'  branch={key[0]} {key[1]}: head@{best["head_offset"]} ops={best["ops"]} items={best["items"]} full_snap={best["last_full"]}')

# --- minimal msgpack encoder (compact, matches rmp_serde::to_vec) ---
def mp_uint(n):
    if n < 128: return bytes([n])
    if n < 256: return b'\xcc' + bytes([n])
    if n < 65536: return b'\xcd' + struct.pack('>H', n)
    if n < 2**32: return b'\xce' + struct.pack('>I', n)
    return b'\xcf' + struct.pack('>Q', n)
def mp_str(s):
    b = s.encode()
    if len(b) < 32: return bytes([0xa0 | len(b)]) + b
    if len(b) < 256: return b'\xd9' + bytes([len(b)]) + b
    return b'\xda' + struct.pack('>H', len(b)) + b
def mp_arr(n):
    if n < 16: return bytes([0x90 | n])
    return b'\xdc' + struct.pack('>H', n)
def mp_map(n):
    if n < 16: return bytes([0x80 | n])
    return b'\xde' + struct.pack('>H', n)
def mp_opt(v): return b'\xc0' if v is None else mp_uint(v)
def mp_bool(v): return b'\xc3' if v else b'\xc2'

out = bytearray()
out += mp_arr(2)              # StateIndex [heads, strategies]
out += mp_map(len(heads))
for (br, sid), h in heads.items():
    out += mp_arr(2) + mp_uint(br) + mp_str(sid)
    out += mp_arr(7)
    out += mp_uint(h['head_offset'])
    out += mp_uint(h['ops_delta'])
    out += mp_uint(h['deltas_since_full'])
    out += mp_opt(h['last_delta'])
    out += mp_opt(h['last_full'])
    out += mp_bool(h['non_append'])
    out += mp_uint(max(0, h['items']))
out += mp_map(0)              # strategies: empty, apps re-register
enc = bytes(out)
sb = os.path.join(store, 'state.bin')
with open(sb, 'wb') as w:
    w.write(b'STI\0'); w.write(bytes([2])); w.write(struct.pack('<Q', len(enc))); w.write(enc)
print(f'wrote {sb} ({len(enc)} bytes payload)')
