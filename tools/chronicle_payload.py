# A state update's payload as a dict, from either encoding a store holds. A record's encoding byte is 1 for
# MessagePack, which chronicle writes for state updates from 0.3.0; anything else is JSON, as earlier releases wrote.
# The MessagePack reader needs nothing installed.
import json, struct, sys

# The kinds of state operation that chain-report.py and scan-delta-poison.py read, by the variant names of
# StateOperation in src/types.rs. tools/check-tools.mjs fails where that enum has a variant in neither list.
READ = ('Set', 'Delta', 'Append', 'AppendMany', 'Redact', 'Edit', 'Snapshot', 'DeltaSnapshot',
        'TreeSet', 'TreeRemove', 'TreeBatch', 'TreeDeltaSnapshot', 'Field')
# Kinds that no tool reads, each with its reason. A tool that meets one stops.
NOT_READ = {}

def decode_update(enc, payload):
    if enc != 1: return json.loads(payload)
    value, end = _read(payload, 0)
    if end != len(payload): raise ValueError(f'MessagePack state update has {len(payload) - end} bytes after its value')
    return value

def operation_kind(update, tool):
    op = update['operation']; kind = op if isinstance(op, str) else next(iter(op))
    if kind not in READ:
        print(f"{tool}: records.log holds a state update of kind {kind}, which this tool doesn't read, so it stops "
              'without a verdict.', file=sys.stderr)
        sys.exit(2)
    return kind

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
