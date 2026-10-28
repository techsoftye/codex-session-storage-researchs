"""Stage 1: copy the three largest research JSONL copies and their Codex .jsonl.zst
counterparts into zstd_comparison/, decompress the copied .zst, and compare bytes.

Sources are only read (copied with shutil.copy2). All decompression and comparison
runs on the copies inside zstd_comparison/. Nothing is printed except sizes, hashes,
timestamps and byte offsets. Requires Python 3.14+ (compression.zstd in the stdlib).
"""
import hashlib
import json
import os
import shutil
import sys
import time
from compression import zstd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / '2026'
STORE = Path(os.environ['USERPROFILE']) / '.codex'
OUT = ROOT / 'zstd_comparison'
CHUNK = 1 << 20


def sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as f:
        while chunk := f.read(CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def find_store(name):
    for base in (STORE / 'sessions', STORE / 'archived_sessions'):
        hits = list(base.rglob(name + '.zst'))
        if hits:
            return hits[0]
    return None


def first_difference(a, b):
    """Byte offset of the first difference within the common length, or None."""
    offset = 0
    with open(a, 'rb') as fa, open(b, 'rb') as fb:
        while True:
            ca, cb = fa.read(CHUNK), fb.read(CHUNK)
            if not ca or not cb:
                return None
            if ca != cb:
                n = min(len(ca), len(cb))
                for i in range(n):
                    if ca[i] != cb[i]:
                        return offset + i
                return offset + n if len(ca) != len(cb) else None
            offset += len(ca)


def main():
    started = time.perf_counter()
    OUT.mkdir(exist_ok=True)
    largest = sorted(SAMPLE.rglob('*.jsonl'), key=lambda p: p.stat().st_size, reverse=True)[:3]
    results = []
    for index, source in enumerate(largest, 1):
        pair = OUT / f'pair_{index:02d}'
        pair.mkdir(exist_ok=True)
        zst_source = find_store(source.name)
        row = {'pair': pair.name, 'file_name': source.name,
               'sample_bytes': source.stat().st_size,
               'sample_mtime_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(source.stat().st_mtime)),
               'store_zst_found': zst_source is not None}
        if zst_source is None:
            results.append(row)
            continue
        sample_copy = pair / ('sample_' + source.name)
        zst_copy = pair / ('store_' + zst_source.name)
        decompressed = pair / ('decompressed_' + source.name)
        for src, dst in ((source, sample_copy), (zst_source, zst_copy)):
            if not dst.exists() or dst.stat().st_size != src.stat().st_size:
                shutil.copy2(src, dst)
        row.update(store_zst_bytes=zst_copy.stat().st_size,
                   store_mtime_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(zst_source.stat().st_mtime)),
                   store_created_local=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(zst_source.stat().st_ctime)))
        t0 = time.perf_counter()
        with zstd.open(zst_copy, 'rb') as fin, open(decompressed, 'wb') as fout:
            shutil.copyfileobj(fin, fout, CHUNK)
        row['decompress_seconds'] = round(time.perf_counter() - t0, 2)
        a, b = sample_copy, decompressed
        row.update(decompressed_bytes=b.stat().st_size,
                   size_difference_bytes=b.stat().st_size - a.stat().st_size,
                   compression_ratio=round(a.stat().st_size / zst_copy.stat().st_size, 2),
                   sample_sha256=sha256(a), decompressed_sha256=sha256(b))
        row['identical'] = row['sample_sha256'] == row['decompressed_sha256']
        if not row['identical']:
            row['first_difference_offset'] = first_difference(a, b)
        results.append(row)
        print(json.dumps(row), flush=True)
    summary = {'stage': 1, 'elapsed_seconds': round(time.perf_counter() - started, 1), 'pairs': results}
    (OUT / 'stage1_summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
    print(json.dumps({'elapsed_seconds': summary['elapsed_seconds']}))


if __name__ == '__main__':
    sys.exit(main())
