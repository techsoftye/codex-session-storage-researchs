"""List the sizes of Codex .jsonl.zst files that correspond to the 845 corpus files.

Metadata only: walks the directory names under %USERPROFILE%\\.codex\\sessions and
archived_sessions and calls stat() on matching .zst files. No file is opened. Corpus file
names and sizes come from data/dataset_inventory.csv, so 2026/ is not touched.
"""
import csv
import json
import os
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STORE = Path(os.environ['USERPROFILE']) / '.codex'
OUT = ROOT / 'data' / 'reports' / 'zst_counterpart_sizes_v1'


def main():
    started = time.perf_counter()
    with open(ROOT / 'data' / 'dataset_inventory.csv', newline='', encoding='utf-8-sig') as f:
        corpus = {row['FileName']: int(row['SizeBytes']) for row in csv.DictReader(f)
                  if row['Extension'] == '.jsonl'}
    found = {}
    for base in ('sessions', 'archived_sessions'):
        for dirpath, _, names in os.walk(STORE / base):
            for name in names:
                if name.endswith('.jsonl.zst') and name[:-4] in corpus:
                    if name[:-4] in found:
                        raise RuntimeError('duplicate .zst counterpart name in store')
                    found[name[:-4]] = (base, os.stat(os.path.join(dirpath, name)).st_size)
    OUT.mkdir(parents=True, exist_ok=True)
    with open(OUT / 'files.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['file_name', 'corpus_bytes', 'zst_bytes', 'store_folder'])
        for name, size in sorted(corpus.items()):
            base, zst = found.get(name, ('', ''))
            writer.writerow([name, size, zst, base])
    matched = sum(corpus[n] for n in found)
    summary = {
        'corpus_files': len(corpus),
        'corpus_bytes': sum(corpus.values()),
        'zst_found': len(found),
        'zst_found_in_archived_sessions': sum(1 for b, _ in found.values() if b == 'archived_sessions'),
        'zst_missing': len(corpus) - len(found),
        'corpus_bytes_of_matched_files': matched,
        'zst_bytes_of_matched_files': sum(z for _, z in found.values()),
        'corpus_bytes_of_missing_files': sum(corpus.values()) - matched,
        'runtime_seconds': round(time.perf_counter() - started, 2),
    }
    (OUT / 'summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
