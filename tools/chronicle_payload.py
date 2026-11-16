# A state update's payload as a dict, from either encoding a store holds. A record's encoding byte is 1 for
# MessagePack, which chronicle writes for state updates from 0.3.0; anything else is JSON, as earlier releases wrote.
# The MessagePack reader needs nothing installed.
import json, struct, sys, zlib

# The kinds of state operation that chain-report.py and scan-delta-poison.py read, by the variant names of
# StateOperation in src/types.rs. tools/check-tools.mjs fails unless the enum's variants are the kinds these
# two lists name.
READ = ('Set', 'Delta', 'Append', 'AppendMany', 'Redact', 'Edit', 'Snapshot', 'DeltaSnapshot',
        'TreeSet', 'TreeRemove', 'TreeBatch', 'TreeDeltaSnapshot')
# Kinds that no tool reads, each with its reason. A tool that meets one stops.
NOT_READ = {'Field': "these tools don't replay an operation nested in a field, and chronicle writes Field only in its "
                     'own tests'}

def decode_update(enc, payload):
    if enc != 1: return json.loads(payload)
    value, end = _read(payload, 0)
    if end != len(payload): raise ValueError(f'MessagePack state update has {len(payload) - end} bytes after its value')
    return value

def read_update(enc, payload, offset, skipped):
    try:
        update = decode_update(enc, payload)
        op = update['operation']; kind = op if isinstance(op, str) else next(iter(op))
        for field in ('record_id', 'global_sequence', 'state_id', 'timestamp'): update[field]
    except Exception:
        skipped.append(offset)
        return None, None
    return update, kind

def say_skipped(skipped, tool):
    if skipped:
        print(f"{tool}: skipped {len(skipped)} record(s) typed state_update that don't decode as state updates, the "
              f'first at offset {skipped[0]}, as chronicle skips them; they stay in the log as ordinary records.',
              file=sys.stderr)

def require_checksum(payload, checksum, offset, tool):
    if zlib.crc32(payload) & 0xffffffff != checksum:
        stop(tool, f"the record at offset {offset} of records.log fails its checksum, so chronicle won't read past it: "
                   'open truncates the log there or refuses the store')

def require_read(kind, offset, tool):
    if kind not in READ:
        reason = f' ({NOT_READ[kind]})' if kind in NOT_READ else ''
        stop(tool, f"records.log holds a state update of kind {kind} at offset {offset}, which this tool doesn't "
                   f'read{reason}')

def stop(tool, why):
    print(f'{tool}: {why}, so it stops without a verdict.', file=sys.stderr)
    sys.exit(2)

_FIXED = {0xc0: None, 0xc2: False, 0xc3: True}
_SIZED = {0xc4: ('B', 'bin'), 0xc5: ('>H', 'bin'), 0xc6: ('>I', 'bin'), 0xd9: ('B', 'str'), 0xda: ('>H', 'str'),
          0xdb: ('>I', 'str'), 0xdc: ('>H', 'array'), 0xdd: ('>I', 'array'), 0xde: ('>H', 'map'), 0xdf: ('>I', 'map')}
_NUMBERS = {0xca: '>f', 0xcb: '>d', 0xcc: 'B', 0xcd: '>H', 0xce: '>I', 0xcf: '>Q', 0xd0: 'b', 0xd1: '>h', 0xd2: '>i', 0xd3: '>q'}

def _read(b, at):
    t = b[at]; at += 1
    if t <= 0x7f: return t, at
    if t >= 0xe0: return t - 0x100, at
    if 0xa0 <= t <= 0xbf: return _sized(b, at, t & 0x1f, 'str')
    if 0x90 <= t <= 0x9f: return _sized(b, at, t & 0x0f, 'array')
    if 0x80 <= t <= 0x8f: return _sized(b, at, t & 0x0f, 'map')
    if t in _FIXED: return _FIXED[t], at
    if t in _NUMBERS:
        fmt = _NUMBERS[t]; return struct.unpack_from(fmt, b, at)[0], at + struct.calcsize(fmt)
    if t in _SIZED:
        fmt, kind = _SIZED[t]; return _sized(b, at + struct.calcsize(fmt), struct.unpack_from(fmt, b, at)[0], kind)
    raise ValueError(f'MessagePack type byte 0x{t:02x} at {at - 1} is not one a state update holds')

def _sized(b, at, n, kind):
    if kind in ('bin', 'str'):
        if at + n > len(b): raise ValueError(f'MessagePack value at {at} runs past the payload')
        raw = bytes(b[at:at + n]); return (raw if kind == 'bin' else raw.decode()), at + n
    if kind == 'array':
        items = []
        for _ in range(n): item, at = _read(b, at); items.append(item)
        return items, at
    out = {}
    for _ in range(n): key, at = _read(b, at); out[key], at = _read(b, at)
    return out, at
