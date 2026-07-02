#!/usr/bin/env python3
# Scan a chronicle records.log; report (and optionally trim) a truncated tail.
# Framing per record: REC\0 | ver(1) | flags(1) | id(8) | seq(8) | branch(8)
#   | ts(8) | type_len(2) | type | enc(1) | payload_len(4) | payload
#   | cb_count(2) | cb*8 | lt_count(2) | lt*8 | crc32(payload)(4)
import sys, struct, zlib
path = sys.argv[1]; apply = '--apply' in sys.argv
f = open(path, 'rb'); data_size = f.seek(0, 2); f.seek(0)
def need(n):
    b = f.read(n)
    if len(b) != n: raise EOFError(f'short read: wanted {n} got {len(b)} at {f.tell()-len(b)}')
    return b
good = 0; count = 0; max_id = 0; last_ts = 0
try:
    while f.tell() < data_size:
        start = f.tell()
        magic = need(4)
        if magic != b'REC\0': raise ValueError(f'bad magic at {start}: {magic!r}')
        ver = need(1)[0]
        if ver != 1: raise ValueError(f'bad version {ver} at {start}')
        need(1)  # flags
        rid, seq, br, ts = struct.unpack('<QQQq', need(32))
        tlen = struct.unpack('<H', need(2))[0]; rtype = need(tlen)
        need(1)  # encoding
        plen = struct.unpack('<I', need(4))[0]
        if plen > 200*1024*1024: raise ValueError(f'absurd payload_len {plen} at {start}')
        payload = need(plen)
        cbc = struct.unpack('<H', need(2))[0]; need(8*cbc)
        ltc = struct.unpack('<H', need(2))[0]; need(8*ltc)
        crc = struct.unpack('<I', need(4))[0]
        if crc != zlib.crc32(payload) & 0xffffffff: raise ValueError(f'crc mismatch for record {rid} at {start}')
        good = f.tell(); count += 1; max_id = max(max_id, rid); last_ts = ts
except (EOFError, ValueError) as e:
    print(f'STOPPED: {e}')
print(f'file size:   {data_size}')
print(f'good bytes:  {good}')
print(f'trailing junk: {data_size - good}')
print(f'records: {count}, max_id: {max_id}, last_ts: {last_ts}')
f.close()
if apply and good < data_size:
    with open(path + '.tail-junk', 'wb') as out, open(path, 'rb') as src:
        src.seek(good); out.write(src.read())
    with open(path, 'r+b') as t: t.truncate(good)
    print(f'TRUNCATED to {good}; junk saved to {path}.tail-junk')
elif apply:
    print('nothing to trim')
