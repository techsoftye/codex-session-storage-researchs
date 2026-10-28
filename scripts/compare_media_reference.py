"""Compare Codex's current rollout format with a media-reference variant of the same file.

"Their version": the file as Codex stores it after the October 2026 rewrite (a decompressed
copy of the Codex .jsonl.zst, or a recent plain rollout that is not compressed yet).
"Our version": the same bytes with every embedded `data:image/...;base64,...` string replaced
by a fixed-length reference `media://sha256/<hex>`, and each unique image counted once in a
media store at its decoded binary size.

Nothing is written except sizes and counts: both versions are streamed through zstd level 3
in memory and only the compressed byte counts are kept. No image, text or reference value is
written or printed. Inputs are study-folder copies only and are opened read-only.
Requires Python 3.14+ (compression.zstd in the stdlib).
"""
import argparse
import ctypes
import hashlib
import json
import os
import re
import sys
import time
from compression import zstd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'data' / 'reports' / 'media_reference_comparison_v1'
CHUNK = 256 * 1024
LEVEL = 3
HEADER = re.compile(rb'data:image/[A-Za-z0-9.+-]{1,20};base64,')
HEADER_MAX = len(b'data:image/') + 20 + len(b';base64,')
B64_END = re.compile(rb'[^A-Za-z0-9+/=]')
MIN_B64 = 100  # shorter payloads (e.g. a mention of the prefix in text) are left untouched
REF_PREFIX = b'media://sha256/'

DEFAULT_INPUTS = [
    ('pair_01', 'zstd_comparison/pair_01/decompressed_rollout-2026-01-01T00-00-04-00000000-0000-4000-8000-000000000005.jsonl',
     'zstd_comparison/pair_01/store_rollout-2026-01-01T00-00-04-00000000-0000-4000-8000-000000000005.jsonl.zst'),
    ('pair_02', 'zstd_comparison/pair_02/decompressed_rollout-2026-01-01T00-00-05-00000000-0000-4000-8000-000000000006.jsonl',
     'zstd_comparison/pair_02/store_rollout-2026-01-01T00-00-05-00000000-0000-4000-8000-000000000006.jsonl.zst'),
    ('post_oct_10-16', 'samples/post_october/rollout-2026-01-01T00-00-06-00000000-0000-4000-8000-000000000007.jsonl', None),
    ('post_oct_10-20a', 'samples/post_october/rollout-2026-01-01T00-00-07-00000000-0000-4000-8000-000000000008.jsonl', None),
    ('post_oct_10-20b', 'samples/post_october/rollout-2026-01-01T00-00-08-00000000-0000-4000-8000-000000000009.jsonl', None),
    ('post_oct_10-22', 'samples/post_october/rollout-2026-01-01T00-00-09-00000000-0000-4000-8000-000000000010.jsonl', None),
]


class MediaReferencer:
    """Streaming rewriter: feed() bytes, get rewritten bytes back; images are hashed, not kept."""

    def __init__(self, ref_style='sha256'):
        # ref_style 'sha256': media://sha256/<hex>; 'index': @@media:<n>, n = first-seen order in
        # this file, resolved through a per-session link table (session_media).
        self.ref_style = ref_style
        self.order = {}        # sha256 hex -> local index
        self.mimes = {}        # sha256 hex -> image subtype from the data-URL header
        self.header = b''
        self.buf = b''
        self.in_data = False
        self.pending = []      # header + payload held until the payload reaches MIN_B64
        self.committed = False
        self.digest = None
        self.length = 0
        self.tail = b''        # last two payload bytes, for '=' padding
        self.images = {}       # sha256 hex -> decoded bytes
        self.occurrences = 0
        self.base64_bytes = 0
        self.short_left = 0
        self.irregular = 0
        self.eof_inside_payload = 0

    def _finish_image(self, out):
        if self.committed:
            hexdigest = self.digest.hexdigest()
            pad = self.tail.count(b'=') if self.tail.endswith(b'=') else 0
            if self.length % 4:
                self.irregular += 1
            if hexdigest not in self.images:
                self.images[hexdigest] = self.length // 4 * 3 - pad
                self.order[hexdigest] = len(self.order)
                self.mimes[hexdigest] = self.header[len(b'data:image/'):-len(b';base64,')].decode('ascii')
            if self.ref_style == 'index':
                out.append(b'@@media:%d' % self.order[hexdigest])
            else:
                out.append(REF_PREFIX + hexdigest.encode())
            self.occurrences += 1
            self.base64_bytes += self.length
        else:
            out.extend(self.pending)
            self.short_left += 1
        self.in_data = False
        self.pending = []

    def feed(self, chunk):
        out = []
        buf = self.buf + chunk
        i = 0
        while True:
            if not self.in_data:
                match = HEADER.search(buf, i)
                if match is None:
                    keep = max(i, len(buf) - (HEADER_MAX - 1))
                    out.append(buf[i:keep])
                    self.buf = buf[keep:]
                    return b''.join(out)
                out.append(buf[i:match.start()])
                self.in_data = True
                self.header = match.group()
                self.pending = [self.header]
                self.committed = False
                self.digest = hashlib.sha256()
                self.length = 0
                self.tail = b''
                i = match.end()
            end = B64_END.search(buf, i)
            stop = end.start() if end else len(buf)
            payload = buf[i:stop]
            if payload:
                self.digest.update(payload)
                self.length += len(payload)
                self.tail = (self.tail + payload)[-2:]
                if not self.committed:
                    self.pending.append(payload)
                    if self.length >= MIN_B64:
                        self.committed = True
                        self.pending = []
            if end is None:
                self.buf = b''
                return b''.join(out)
            self._finish_image(out)
            i = stop

    def close(self):
        out = []
        if self.in_data:
            self.eof_inside_payload += 1
            self._finish_image(out)
        out.append(self.buf)
        self.buf = b''
        return b''.join(out)


def current_process():
    kernel32 = ctypes.windll.kernel32
    kernel32.GetCurrentProcess.restype = ctypes.c_void_p
    return ctypes.c_void_p(kernel32.GetCurrentProcess())


def set_below_normal_priority():
    if os.name == 'nt' and not ctypes.windll.kernel32.SetPriorityClass(current_process(), 0x00004000):
        raise OSError('SetPriorityClass failed')


def peak_working_set():
    if os.name != 'nt':
        return None

    class Counters(ctypes.Structure):
        _fields_ = [('cb', ctypes.c_ulong), ('PageFaultCount', ctypes.c_ulong),
                    ('PeakWorkingSetSize', ctypes.c_size_t), ('WorkingSetSize', ctypes.c_size_t),
                    ('QuotaPeakPagedPoolUsage', ctypes.c_size_t), ('QuotaPagedPoolUsage', ctypes.c_size_t),
                    ('QuotaPeakNonPagedPoolUsage', ctypes.c_size_t), ('QuotaNonPagedPoolUsage', ctypes.c_size_t),
                    ('PagefileUsage', ctypes.c_size_t), ('PeakPagefileUsage', ctypes.c_size_t)]

    counters = Counters()
    counters.cb = ctypes.sizeof(Counters)
    if not ctypes.windll.psapi.GetProcessMemoryInfo(current_process(), ctypes.byref(counters), counters.cb):
        raise OSError('GetProcessMemoryInfo failed')
    return counters.PeakWorkingSetSize


def measure(label, path, store_zst):
    before = path.stat()
    theirs_z = zstd.ZstdCompressor(level=LEVEL)
    ours_z = zstd.ZstdCompressor(level=LEVEL)
    rewriter = MediaReferencer()
    theirs_raw = theirs_zst = ours_raw = ours_zst = 0
    started = time.perf_counter()
    with open(path, 'rb') as f:
        while chunk := f.read(CHUNK):
            theirs_raw += len(chunk)
            theirs_zst += len(theirs_z.compress(chunk))
            rewritten = rewriter.feed(chunk)
            ours_raw += len(rewritten)
            ours_zst += len(ours_z.compress(rewritten))
    tail = rewriter.close()
    ours_raw += len(tail)
    ours_zst += len(ours_z.compress(tail)) + len(ours_z.flush())
    theirs_zst += len(theirs_z.flush())
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise RuntimeError(f'{label}: source changed during the scan')
    if theirs_raw != before.st_size:
        raise RuntimeError(f'{label}: read {theirs_raw} bytes, expected {before.st_size}')
    if rewriter.irregular or rewriter.eof_inside_payload:
        raise RuntimeError(f'{label}: {rewriter.irregular} irregular payload lengths, '
                           f'{rewriter.eof_inside_payload} payloads cut at end of file')
    media = sum(rewriter.images.values())
    return {
        'label': label,
        'source': str(path.relative_to(ROOT)).replace('\\', '/'),
        'theirs_plain_bytes': theirs_raw,
        'theirs_codex_zst_bytes': store_zst.stat().st_size if store_zst else None,
        'theirs_zstd3_bytes': theirs_zst,
        'ours_plain_bytes': ours_raw,
        'ours_zstd3_bytes': ours_zst,
        'media_store_unique_images': len(rewriter.images),
        'media_store_decoded_bytes': media,
        'ours_zstd3_plus_media_bytes': ours_zst + media,
        'image_occurrences_replaced': rewriter.occurrences,
        'image_base64_bytes_replaced': rewriter.base64_bytes,
        'short_data_urls_left_unchanged': rewriter.short_left,
        'reference_bytes_each': len(REF_PREFIX) + 64,
        'elapsed_seconds': round(time.perf_counter() - started, 1),
    }


def write_atomic(path, text):
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(text, encoding='utf-8')
    os.replace(tmp, path)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--out', type=Path, default=OUT)
    args = parser.parse_args()
    set_below_normal_priority()
    args.out.mkdir(parents=True, exist_ok=True)
    checkpoints = args.out / 'checkpoints'
    checkpoints.mkdir(exist_ok=True)
    started = time.perf_counter()
    results = []
    for label, rel, store_rel in DEFAULT_INPUTS:
        checkpoint = checkpoints / f'{label}.json'
        if checkpoint.exists():
            results.append(json.loads(checkpoint.read_text(encoding='utf-8')))
            continue
        result = measure(label, ROOT / rel, ROOT / store_rel if store_rel else None)
        write_atomic(checkpoint, json.dumps(result, indent=2))
        results.append(result)
        print(json.dumps(result), flush=True)
    summary = {
        'method': 'stream each file once; zstd level 3 in memory for both versions; '
                  f'images with >= {MIN_B64} base64 chars replaced by {REF_PREFIX.decode()}<64 hex>',
        'zstd_level': LEVEL,
        'results': results,
        'runtime_seconds': round(time.perf_counter() - started, 1),
        'peak_working_set_bytes': peak_working_set(),
    }
    write_atomic(args.out / 'summary.json', json.dumps(summary, indent=2))
    print(json.dumps({k: summary[k] for k in ('runtime_seconds', 'peak_working_set_bytes')}))


if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8')
    main()
