"""Stage 2: structural comparison of each research-copy JSONL with the decompressed
copy of its Codex .jsonl.zst counterpart, inside zstd_comparison/pair_NN/.

Per record it keeps only: line number, byte length, timestamp, ordinal, type,
payload subtype, compaction window number and a SHA-256 of the line with the
added "ordinal" field removed. Base64 images are identified by SHA-256 of their
base64 text. No content is printed or written; outputs are counts, sizes,
positions and hashes.
"""
import hashlib
import json
import re
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / 'zstd_comparison'
HEAD = 600
TYPE = re.compile(rb'"type":"([A-Za-z_]+)"')
TIMESTAMP = re.compile(rb'"timestamp":"([^"]{1,40})"')
ORDINAL = re.compile(rb'"ordinal":(\d+),')
SUBTYPE = re.compile(rb'"payload":\{"type":"([A-Za-z_]+)"')
WINDOW = re.compile(rb'"window_number":(\d+)')
IMAGE = re.compile(rb'data:image/[A-Za-z0-9.+-]{1,20};base64,')


def scan(path):
    records, images = [], Counter()
    image_bytes = {}
    with open(path, 'rb') as fh:
        for number, line in enumerate(fh, 1):
            head = line[:HEAD]
            ordinal = ORDINAL.search(head)
            normalized = line
            if ordinal:
                normalized = head.replace(ordinal.group(0), b'', 1) + line[HEAD:]
            kind = TYPE.search(head)
            kind = kind.group(1).decode() if kind else None
            sub = SUBTYPE.search(head)
            stamp = TIMESTAMP.search(head)
            window = None
            if kind == 'compacted':
                tail_window = WINDOW.findall(line[-4096:])
                window = int(tail_window[-1]) if tail_window else None
            for match in IMAGE.finditer(line):
                end = line.find(b'"', match.end())
                body = line[match.end():end]
                digest = hashlib.sha256(body).hexdigest()
                images[digest] += 1
                image_bytes[digest] = len(body)
            records.append({
                'line': number, 'bytes': len(line), 'type': kind,
                'subtype': sub.group(1).decode() if sub else None,
                'timestamp': stamp.group(1).decode() if stamp else None,
                'ordinal': int(ordinal.group(1)) if ordinal else None,
                'window': window,
                'hash': hashlib.sha256(normalized).hexdigest(),
                'ordinal_field_bytes': len(ordinal.group(0)) if ordinal else 0,
            })
    return records, images, image_bytes


def by_type(records, select=lambda r: True):
    counts, sizes = Counter(), Counter()
    for r in records:
        if select(r):
            counts[r['type']] += 1
            sizes[r['type']] += r['bytes']
    return {t: {'records': counts[t], 'bytes': sizes[t]} for t in sorted(counts, key=lambda t: -sizes[t])}


def compare(pair):
    sample = next(pair.glob('sample_*.jsonl'))
    decompressed = next(pair.glob('decompressed_*.jsonl'))
    started = time.perf_counter()
    a, a_images, a_image_bytes = scan(sample)
    b, b_images, b_image_bytes = scan(decompressed)

    remaining = Counter(r['hash'] for r in b)
    a_hash_count = Counter(r['hash'] for r in a)
    for r in a:
        if remaining[r['hash']] > 0:
            remaining[r['hash']] -= 1
            r['status'] = 'kept_identical'
        else:
            # An identical copy of this record survives elsewhere in the decompressed file.
            r['status'] = 'removed_duplicate' if r['hash'] in remaining else 'removed_or_changed'
    a_hashes = set(a_hash_count)
    new_in_b = [r for r in b if r['hash'] not in a_hashes]

    # Records missing from the decompressed file whose timestamp/type/subtype reappears there in changed form.
    b_keys = Counter((r['timestamp'], r['type'], r['subtype']) for r in new_in_b)
    for r in a:
        if r['status'] == 'removed_or_changed':
            key = (r['timestamp'], r['type'], r['subtype'])
            if b_keys[key] > 0:
                b_keys[key] -= 1
                r['status'] = 'changed'
            else:
                r['status'] = 'removed_unique'

    status_types = {s: by_type(a, lambda r, s=s: r['status'] == s)
                    for s in ('kept_identical', 'changed', 'removed_duplicate', 'removed_unique')}

    compacted = [r for r in a if r['type'] == 'compacted']
    kept_windows = Counter(r['window'] for r in compacted if r['status'] in ('kept_identical', 'changed'))
    windows = defaultdict(list)
    for index, r in enumerate(compacted):
        windows[r['window']].append((index, r['status'] in ('kept_identical', 'changed')))
    kept_is_last_in_window = sum(1 for w, items in windows.items()
                                 if any(k for _, k in items) and items[-1][1])
    windows_with_kept = sum(1 for items in windows.values() if any(k for _, k in items))

    ordinals = [r['ordinal'] for r in b if r['ordinal'] is not None]
    gaps = sum(1 for x, y in zip(ordinals, ordinals[1:]) if y != x + 1)

    a_unique, b_unique = set(a_images), set(b_images)
    lost = a_unique - b_unique
    result = {
        'pair': pair.name,
        'sample': {'records': len(a), 'bytes': sum(r['bytes'] for r in a), 'by_type': by_type(a)},
        'decompressed': {'records': len(b), 'bytes': sum(r['bytes'] for r in b), 'by_type': by_type(b),
                         'records_not_in_sample': len(new_in_b),
                         'new_records_by_type': by_type(new_in_b)},
        'sample_record_status_by_type': status_types,
        'compacted': {
            'sample_records': len(compacted),
            'kept_records': sum(1 for r in compacted if r['status'] in ('kept_identical', 'changed')),
            'sample_windows': len(windows),
            'windows_with_a_kept_record': windows_with_kept,
            'windows_where_kept_record_is_last_in_window': kept_is_last_in_window,
            'kept_records_per_window_max': max(kept_windows.values()) if kept_windows else 0,
            'sample_window_number_missing': sum(1 for r in compacted if r['window'] is None),
            'kept_positions_in_compacted_sequence': [i for i, r in enumerate(compacted) if r['status'] in ('kept_identical', 'changed')],
        },
        'ordinal': {'records_with_ordinal': len(ordinals), 'first': ordinals[0] if ordinals else None,
                    'last': ordinals[-1] if ordinals else None, 'non_consecutive_steps': gaps,
                    'total_ordinal_field_bytes': sum(r['ordinal_field_bytes'] for r in b)},
        'images': {
            'sample_occurrences': sum(a_images.values()), 'sample_unique': len(a_unique),
            'sample_unique_base64_bytes': sum(a_image_bytes[h] for h in a_unique),
            'decompressed_occurrences': sum(b_images.values()), 'decompressed_unique': len(b_unique),
            'decompressed_unique_base64_bytes': sum(b_image_bytes[h] for h in b_unique),
            'unique_lost': len(lost), 'unique_lost_base64_bytes': sum(a_image_bytes[h] for h in lost),
            'unique_new_in_decompressed': len(b_unique - a_unique),
        },
        'elapsed_seconds': round(time.perf_counter() - started, 1),
    }
    return result


def main():
    results = [compare(pair) for pair in sorted(BASE.glob('pair_*')) if pair.is_dir()]
    (BASE / 'stage2_summary.json').write_text(json.dumps({'stage': 2, 'pairs': results}, indent=2), encoding='utf-8')
    for r in results:
        print(json.dumps({k: r[k] for k in ('pair', 'compacted', 'ordinal', 'images', 'elapsed_seconds')}))
        print(json.dumps({'pair': r['pair'], 'status': {s: {t: v['records'] for t, v in d.items()}
                                                         for s, d in r['sample_record_status_by_type'].items()},
                          'new_in_decompressed': {t: v['records'] for t, v in r['decompressed']['new_records_by_type'].items()}}))


if __name__ == '__main__':
    sys.exit(main())
