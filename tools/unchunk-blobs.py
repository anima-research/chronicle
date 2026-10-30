#!/usr/bin/env python3
# List a store's chunked blobs (storeBlobAsync with chunked: true), and with --apply rewrite each one whole under the
# same hash, so a chronicle release from before storeBlobAsync can read it. Its chunks stay, as blobs of their own.
#   unchunk-blobs.py <store> [--apply]
import errno, hashlib, os, secrets, struct, sys, zlib
try: from fcntl import fcntl, F_FULLFSYNC
except ImportError: F_FULLFSYNC = None

STAGING_PREFIX = '.chronicle-blob-'

def read_blob(path):
    with open(path, 'rb') as f: data = f.read()
    try:
        if data[:4] != b'BLB\0': raise ValueError('it is not a blob file')
        version = data[4]
        ct_len, = struct.unpack_from('<H', data, 5); at = 7 + ct_len
        content_type = data[7:at]
        content_len, = struct.unpack_from('<Q', data, at); at += 8
        if version == 1:
            content = data[at:at + content_len]; at += content_len
            if len(content) != content_len or len(data) < at + 4: raise ValueError('it is cut short')
            if struct.unpack_from('<I', data, at)[0] != zlib.crc32(content): raise ValueError('its checksum does not match')
            return 1, content_type, content
        if version == 2:
            count, = struct.unpack_from('<I', data, at); at += 4
            if len(data) != at + 32 * count + 4: raise ValueError(f'it names {count} chunks and holds {len(data)} bytes')
            chunks = [data[at + 32 * i:at + 32 * (i + 1)] for i in range(count)]; at += 32 * count
            if struct.unpack_from('<I', data, at)[0] != zlib.crc32(data[5:at]): raise ValueError('its checksum does not match')
            return 2, content_type, (content_len, chunks)
    except (IndexError, struct.error):
        raise ValueError('it is cut short')
    raise ValueError(f'its format version is {version}, which this script does not read')

def sync(fd):
    if F_FULLFSYNC is not None: fcntl(fd, F_FULLFSYNC)
    else: os.fsync(fd)

def write_whole(blobs, path, content_type, content):
    staging = os.path.join(blobs, STAGING_PREFIX + secrets.token_hex(8))
    fd = os.open(staging, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_BINARY', 0), 0o666)
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(b'BLB\0\x01' + struct.pack('<H', len(content_type)) + content_type + struct.pack('<Q', len(content))
                    + content + struct.pack('<I', zlib.crc32(content)))
            f.flush(); sync(f.fileno())
        os.replace(staging, path)
    except BaseException:
        if os.path.exists(staging): os.unlink(staging)
        raise
    if os.name == 'posix':
        try: fd = os.open(os.path.dirname(path), os.O_RDONLY)
        except OSError as e:
            if e.errno in (errno.EACCES, errno.EPERM): return
            raise
        try: sync(fd)
        except OSError as e:
            if e.errno not in (errno.EBADF, errno.EINVAL): raise
        finally: os.close(fd)

def main():
    if len(sys.argv) < 2 or sys.argv[1].startswith('-'): sys.exit('usage: unchunk-blobs.py <store> [--apply]')
    store, apply = sys.argv[1], '--apply' in sys.argv[2:]
    blobs = os.path.join(store, 'blobs')
    found = 0
    for shard in sorted(os.listdir(blobs)):
        shard_dir = os.path.join(blobs, shard)
        if len(shard) != 2 or not os.path.isdir(shard_dir): continue
        for name in sorted(os.listdir(shard_dir)):
            path = os.path.join(shard_dir, name)
            if not os.path.isfile(path): continue
            with open(path, 'rb') as f: head = f.read(5)
            if head != b'BLB\0\x02': continue
            try:
                _, content_type, (content_len, chunks) = read_blob(path)
                pieces, held = [], 0
                for chunk in chunks:
                    try: version, _, piece = read_blob(os.path.join(blobs, chunk.hex()[:2], chunk.hex()))
                    except (OSError, ValueError) as e: raise ValueError(f'its chunk {chunk.hex()} can not be read: {e}')
                    if version != 1: raise ValueError(f'its chunk {chunk.hex()} is itself a file naming chunks')
                    if hashlib.sha256(piece).digest() != chunk: raise ValueError(f'its chunk {chunk.hex()} holds other bytes')
                    held += len(piece)
                    if held > content_len: raise ValueError(f'it names {content_len} bytes of content, and its chunks hold more')
                    pieces.append(piece)
                content = b''.join(pieces)
                if len(content) != content_len or hashlib.sha256(content).hexdigest() != name:
                    raise ValueError('its chunks are not the content it names')
            except (OSError, ValueError) as e:
                sys.exit(f'{path}: {e}. It was left as it is, and nothing after it was listed or rewritten.')
            found += 1
            if apply: write_whole(blobs, path, content_type, content)
            print(f'{name}: {content_len} bytes in {len(chunks)} chunks{", rewritten whole" if apply else ""}')
    print(f'{found} chunked blob(s){" rewritten whole" if apply else "; --apply rewrites them whole"}')

main()
