"""Version 2: measure Codex's stored format against two reference-based variants.

- Variant 1 (images): every embedded `data:image/...;base64,...` string becomes `@@media:<n>`,
  where n is the image's first-seen index in that file. The image is counted once, at its
  decoded size, in a media store. A link table maps (file, n) to the image hash.
- Variant 2 (images + history items): additionally, every item of a compacted record's
  `replacement_history` and `guardian_history` arrays becomes `"@@item:<n>"`. Each distinct
  item (exact bytes after image replacement) is stored once in a per-file item store. A
  snapshot therefore keeps only its list of references, and repeated items are not stored again.

Each source file is read once, read-only, in 256 KiB chunks. Every output stream is compressed
in memory with zstd level 3, and only byte counts are kept. No text, image, item or reference
value is written. The media link tables are built in SQLite from hashes and sizes only, to
measure their size.

Sets:
- `study`: the six study-folder copies (post-migration format; theirs = Codex .zst or zstd-3).
- `corpus`: the 845 files in 2026/ (pre-migration originals; theirs = the Codex .zst sizes
  from zst_counterpart_sizes_v1).

Requires Python 3.14+ (compression.zstd in the stdlib).
"""
import argparse
import csv
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
from compression import zstd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from compare_media_reference import (CHUNK, DEFAULT_INPUTS, LEVEL, ROOT, MediaReferencer,  # noqa: E402
                                     peak_working_set, set_below_normal_priority, write_atomic)

OUT = ROOT / 'data' / 'reports' / 'media_reference_comparison_v2'
SIG_OUT = re.compile(rb'["\[\]{}\n]')
SIG_STR = re.compile(rb'["\\\n]')
TOP_TYPE = re.compile(rb'"type":"([A-Za-z_]+)"')
TARGETS = (b'replacement_history', b'guardian_history')
PREFIX_MAX = 512
KEY_MAX = 32
ITEM_MAX = 256 << 20
MARKERS = (b'@@media:', b'@@item:')


class ItemDeduper:
    """Streaming pass over JSONL that replaces compacted history items with store references."""

    def __init__(self, store):
        self.store = store
        self.mode = 'prefix'   # prefix: reading a line's first bytes; pass: other record; json: compacted
        self.prefix = bytearray()
        self.items = {}
        self.refs = 0
        self.item_bytes = 0
        self.unique_item_bytes = 0
        self.max_item = 0
        self.compacted_lines = 0
        self.images_in_items = 0
        self.truncated_final_line = 0
        self._reset_json()

    def _reset_json(self):
        self.depth = 0
        self.in_str = False
        self.esc = False
        self.key = bytearray()
        self.key_long = False
        self.last_key = None
        self.gap_ok = False
        self.array_depth = None
        self.item = None

    def _emit(self, seg, out):
        if self.item is not None:
            self.item += seg
            if len(self.item) > ITEM_MAX:
                raise RuntimeError('history item exceeds ITEM_MAX')
        else:
            out.append(seg)

    def _start_line(self, out, at_eof=False):
        prefix = bytes(self.prefix)
        self.prefix.clear()
        match = TOP_TYPE.search(prefix)
        payload = prefix.find(b'"payload"')
        if not prefix.strip():
            self.mode = 'pass'
        elif match is None or 0 <= payload < match.start():
            if not at_eof:
                raise RuntimeError('top-level record type not found in line prefix')
            self.truncated_final_line += 1
            self.mode = 'pass'
        elif match.group(1) == b'compacted':
            self.mode = 'json'
            self._reset_json()
            self.compacted_lines += 1
        else:
            self.mode = 'pass'
        self._process(prefix, out)

    def _process(self, data, out):
        i, n = 0, len(data)
        while i < n:
            if self.mode == 'prefix':
                room = PREFIX_MAX - len(self.prefix)
                j = data.find(b'\n', i, i + room)
                end = j + 1 if j >= 0 else min(n, i + room)
                self.prefix += data[i:end]
                i = end
                if j >= 0 or len(self.prefix) >= PREFIX_MAX:
                    self._start_line(out)
            elif self.mode == 'pass':
                j = data.find(b'\n', i)
                if j < 0:
                    out.append(data[i:])
                    return
                out.append(data[i:j + 1])
                i = j + 1
                self.mode = 'prefix'
            else:
                i = self._json(data, i, out)

    def _json(self, data, i, out):
        n = len(data)
        while i < n:
            if self.esc:
                self._emit(data[i:i + 1], out)
                i += 1
                self.esc = False
                continue
            if self.in_str:
                m = SIG_STR.search(data, i)
                stop = m.start() if m else n
                if not self.key_long:
                    self.key += data[i:stop]
                    self.key_long = len(self.key) > KEY_MAX
                if m is None:
                    self._emit(data[i:], out)
                    return n
                c = data[stop]
                if c == 0x0A:
                    raise RuntimeError('raw newline inside a JSON string')
                if c == 0x5C:
                    self.key_long = True
                    if stop + 1 < n:
                        self._emit(data[i:stop + 2], out)
                        i = stop + 2
                    else:
                        self._emit(data[i:], out)
                        self.esc = True
                        i = n
                    continue
                self._emit(data[i:stop + 1], out)
                i = stop + 1
                self.in_str = False
                self.last_key = None if self.key_long else bytes(self.key)
                self.gap_ok = True
                continue
            m = SIG_OUT.search(data, i)
            stop = m.start() if m else n
            if stop > i:
                gap = data[i:stop]
                if self.gap_ok and gap.strip(b' \t\r:'):
                    self.gap_ok = False
                self._emit(gap, out)
            if m is None:
                return n
            c = data[stop]
            token = data[stop:stop + 1]
            i = stop + 1
            if c == 0x22:
                self._emit(token, out)
                self.in_str = True
                self.key = bytearray()
                self.key_long = False
                self.gap_ok = False
            elif c in (0x5B, 0x7B):
                if (c == 0x5B and self.gap_ok and self.item is None and self.array_depth is None
                        and self.depth == 2 and self.last_key in TARGETS):
                    self._emit(token, out)
                    self.depth += 1
                    self.array_depth = self.depth
                elif self.item is None and self.array_depth is not None and self.depth == self.array_depth:
                    if c != 0x7B:
                        raise RuntimeError('history item is not a JSON object')
                    self.item = bytearray(token)
                    self.depth += 1
                else:
                    self._emit(token, out)
                    self.depth += 1
                self.gap_ok = False
            elif c in (0x5D, 0x7D):
                self.depth -= 1
                if self.depth < 0:
                    raise RuntimeError('unbalanced JSON in compacted record')
                self._emit(token, out)
                if self.item is not None and self.depth == self.array_depth:
                    self._finish_item(out)
                elif self.item is None and self.array_depth is not None and self.depth == self.array_depth - 1:
                    self.array_depth = None
                self.gap_ok = False
            else:
                if self.depth or self.item is not None:
                    raise RuntimeError('compacted record ended inside a JSON value')
                self._emit(token, out)
                self.mode = 'prefix'
                return i
        return n

    def _finish_item(self, out):
        item = bytes(self.item)
        self.item = None
        self.refs += 1
        self.item_bytes += len(item)
        self.max_item = max(self.max_item, len(item))
        self.images_in_items += item.count(b'@@media:')
        digest = hashlib.sha256(item).digest()
        index = self.items.get(digest)
        if index is None:
            index = len(self.items)
            self.items[digest] = index
            self.unique_item_bytes += len(item)
            self.store(item + b'\n')
        out.append(b'"@@item:%d"' % index)

    def feed(self, data):
        out = []
        self._process(data, out)
        return b''.join(out)

    def close(self):
        out = []
        if self.mode == 'prefix' and self.prefix:
            self._start_line(out, at_eof=True)
        if self.mode == 'json' and (self.depth or self.item is not None or self.in_str):
            self.truncated_final_line += 1
            if self.item is not None:
                out.append(bytes(self.item))
                self.item = None
        return b''.join(out)


def measure(path, compute_theirs):
    before = path.stat()
    started = time.perf_counter()
    c = {k: 0 for k in ('source_bytes', 'theirs_zstd3_bytes', 'v1_plain_bytes', 'v1_zstd3_bytes',
                        'v2_main_plain_bytes', 'v2_main_zstd3_bytes', 'v2_item_store_plain_bytes',
                        'v2_item_store_zstd3_bytes', 'raw_marker_collisions')}
    theirs_z = zstd.ZstdCompressor(level=LEVEL) if compute_theirs else None
    v1_z, v2_z, store_z = (zstd.ZstdCompressor(level=LEVEL) for _ in range(3))

    def store(data):
        c['v2_item_store_plain_bytes'] += len(data)
        c['v2_item_store_zstd3_bytes'] += len(store_z.compress(data))

    rewriter = MediaReferencer(ref_style='index')
    deduper = ItemDeduper(store)

    def push(data):
        c['v1_plain_bytes'] += len(data)
        c['v1_zstd3_bytes'] += len(v1_z.compress(data))
        main = deduper.feed(data)
        c['v2_main_plain_bytes'] += len(main)
        c['v2_main_zstd3_bytes'] += len(v2_z.compress(main))

    carry = {m: b'' for m in MARKERS}
    with open(path, 'rb') as f:
        while chunk := f.read(CHUNK):
            c['source_bytes'] += len(chunk)
            if theirs_z:
                c['theirs_zstd3_bytes'] += len(theirs_z.compress(chunk))
            for marker in MARKERS:
                window = carry[marker] + chunk
                c['raw_marker_collisions'] += window.count(marker)
                carry[marker] = window[-(len(marker) - 1):]
            push(rewriter.feed(chunk))
    push(rewriter.close())
    tail = deduper.close()
    c['v2_main_plain_bytes'] += len(tail)
    c['v2_main_zstd3_bytes'] += len(v2_z.compress(tail)) + len(v2_z.flush())
    c['v1_zstd3_bytes'] += len(v1_z.flush())
    c['v2_item_store_zstd3_bytes'] += len(store_z.flush())
    if theirs_z:
        c['theirs_zstd3_bytes'] += len(theirs_z.flush())
    else:
        c['theirs_zstd3_bytes'] = None
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise RuntimeError('source changed during the scan')
    if c['source_bytes'] != before.st_size:
        raise RuntimeError('short read')
    c.update({
        'image_occurrences': rewriter.occurrences,
        'image_base64_bytes': rewriter.base64_bytes,
        'short_data_urls_left_unchanged': rewriter.short_left,
        'irregular_base64_lengths': rewriter.irregular,
        'eof_inside_image_payload': rewriter.eof_inside_payload,
        'media': [[h, rewriter.images[h], rewriter.mimes[h]] for h in rewriter.order],
        'media_unique_decoded_bytes': sum(rewriter.images.values()),
        'compacted_records': deduper.compacted_lines,
        'history_items': deduper.refs,
        'history_items_unique': len(deduper.items),
        'history_item_bytes': deduper.item_bytes,
        'history_item_unique_bytes': deduper.unique_item_bytes,
        'history_item_max_bytes': deduper.max_item,
        'images_inside_history_items': deduper.images_in_items,
        'truncated_final_line': deduper.truncated_final_line,
        'elapsed_seconds': round(time.perf_counter() - started, 2),
    })
    return c


def inputs(which):
    if which == 'study':
        for label, rel, store_rel in DEFAULT_INPUTS:
            theirs = (ROOT / store_rel).stat().st_size if store_rel else None
            yield label, ROOT / rel, theirs, theirs is None, None
        return
    with open(ROOT / 'data' / 'reports' / 'zst_counterpart_sizes_v1' / 'files.csv', newline='',
              encoding='utf-8') as f:
        zst = {row['file_name']: int(row['zst_bytes']) if row['zst_bytes'] else None
               for row in csv.DictReader(f)}
    with open(ROOT / 'data' / 'dataset_inventory.csv', newline='', encoding='utf-8-sig') as f:
        rows = sorted((r for r in csv.DictReader(f) if r['Extension'] == '.jsonl'),
                      key=lambda r: r['RelativePath'])
    for number, row in enumerate(rows):
        yield f'{number:04d}', ROOT / row['RelativePath'], zst[row['FileName']], False, int(row['SizeBytes'])


def build_media_tables(path, results):
    if path.exists():
        path.unlink()
    db = sqlite3.connect(path)
    db.execute('CREATE TABLE media (sha256 BLOB PRIMARY KEY, decoded_bytes INTEGER NOT NULL, '
               'mime TEXT NOT NULL) WITHOUT ROWID')
    db.execute('CREATE TABLE session_media (file_no INTEGER NOT NULL, local_index INTEGER NOT NULL, '
               'sha256 BLOB NOT NULL, PRIMARY KEY (file_no, local_index)) WITHOUT ROWID')
    for file_no, result in enumerate(results):
        for local_index, (h, size, mime) in enumerate(result['media']):
            digest = bytes.fromhex(h)
            db.execute('INSERT OR IGNORE INTO media VALUES (?, ?, ?)', (digest, size, mime))
            db.execute('INSERT INTO session_media VALUES (?, ?, ?)', (file_no, local_index, digest))
    db.commit()
    unique_bytes = db.execute('SELECT COUNT(*), SUM(decoded_bytes) FROM media').fetchone()
    links = db.execute('SELECT COUNT(*) FROM session_media').fetchone()[0]
    db.execute('VACUUM')
    db.close()
    return {'unique_images': unique_bytes[0], 'unique_decoded_bytes': unique_bytes[1] or 0,
            'session_media_rows': links, 'sqlite_bytes': path.stat().st_size}


def aggregate(which, results, out):
    keys = ('source_bytes', 'theirs_bytes', 'v1_zstd3_bytes', 'v2_main_zstd3_bytes', 'v2_item_store_zstd3_bytes',
            'v1_plain_bytes', 'v2_main_plain_bytes', 'v2_item_store_plain_bytes', 'media_unique_decoded_bytes',
            'image_occurrences', 'image_base64_bytes', 'compacted_records', 'history_items',
            'history_items_unique', 'history_item_bytes', 'history_item_unique_bytes',
            'images_inside_history_items', 'short_data_urls_left_unchanged', 'irregular_base64_lengths',
            'eof_inside_image_payload', 'truncated_final_line', 'raw_marker_collisions')
    compared = [r for r in results if r['theirs_bytes'] is not None]
    total = {k: sum(r[k] for r in compared) for k in keys}
    total['history_item_max_bytes'] = max(r['history_item_max_bytes'] for r in compared)
    shared = build_media_tables(out / f'media_tables_{which}.sqlite3', compared)
    per_session = total['media_unique_decoded_bytes']
    variants = {
        'theirs': total['theirs_bytes'],
        'ours_v1_media_per_session': total['v1_zstd3_bytes'] + per_session,
        'ours_v1_media_shared': total['v1_zstd3_bytes'] + shared['unique_decoded_bytes'] + shared['sqlite_bytes'],
        'ours_v2_media_per_session': total['v2_main_zstd3_bytes'] + total['v2_item_store_zstd3_bytes'] + per_session,
        'ours_v2_media_shared': (total['v2_main_zstd3_bytes'] + total['v2_item_store_zstd3_bytes']
                                 + shared['unique_decoded_bytes'] + shared['sqlite_bytes']),
    }
    return {
        'files_total': len(results),
        'files_compared': len(compared),
        'files_without_theirs': [r['label'] for r in results if r['theirs_bytes'] is None],
        'source_bytes_without_theirs': sum(r['source_bytes'] for r in results if r['theirs_bytes'] is None),
        'totals': total,
        'shared_media': shared,
        'variants_bytes': variants,
        'theirs_over_variant': {k: round(variants['theirs'] / v, 4) for k, v in variants.items() if k != 'theirs'},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--set', choices=('study', 'corpus'), required=True)
    parser.add_argument('--limit', type=int, help='process only the first N files (pilot)')
    args = parser.parse_args()
    set_below_normal_priority()
    checkpoints = OUT / 'checkpoints' / args.set
    checkpoints.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    results = []
    for count, (label, path, theirs, compute_theirs, expected) in enumerate(inputs(args.set)):
        if args.limit is not None and count >= args.limit:
            break
        checkpoint = checkpoints / f'{label}.json'
        if checkpoint.exists():
            result = json.loads(checkpoint.read_text(encoding='utf-8'))
        else:
            if expected is not None and path.stat().st_size != expected:
                raise RuntimeError(f'{label}: size differs from dataset_inventory.csv')
            result = measure(path, compute_theirs)
            result['label'] = label
            result['theirs_bytes'] = theirs if theirs is not None else result['theirs_zstd3_bytes']
            result['theirs_source'] = 'codex_zst' if theirs is not None else (
                'zstd3_of_source' if compute_theirs else None)
            if args.set == 'corpus' and theirs is None:
                result['theirs_bytes'] = None
            write_atomic(checkpoint, json.dumps(result))
            print(json.dumps({'file': label, 'source_bytes': result['source_bytes'],
                              'elapsed_seconds': result['elapsed_seconds']}), flush=True)
        results.append(result)
    summary = {
        'set': args.set,
        'complete': args.limit is None,
        'zstd_level': LEVEL,
        'chunk_bytes': CHUNK,
        **aggregate(args.set, results, OUT),
        'runtime_seconds_this_invocation': round(time.perf_counter() - started, 1),
        'peak_working_set_bytes_this_invocation': peak_working_set(),
    }
    name = f'summary_{args.set}.json' if args.limit is None else f'pilot_{args.set}.json'
    write_atomic(OUT / name, json.dumps(summary, indent=2))
    print(json.dumps({k: summary[k] for k in ('variants_bytes', 'theirs_over_variant')}, indent=2))


if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8')
    main()
