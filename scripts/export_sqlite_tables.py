"""Export every table of the SQLite files under sqlite/ to one JSON file per table.

Output: sqlite/exported_data/<database file name>/<table name>.json, a JSON array of
row objects in original column order. Values are written as stored: TEXT stays a
string (JSON-looking text is not parsed), INTEGER/REAL stay numbers, NULL is null.
JSON has no binary type, so a BLOB is written as {"$blob_base64": "..."}.

Each database is copied with its -wal/-shm files to a temporary folder before it is
opened, so the files under sqlite/ are never opened for writing and recent WAL
content is included. Rows are streamed one at a time; no table is held in memory.
Nothing is printed except file names, row counts and sizes.
"""
from __future__ import annotations

import argparse
import base64
import json
import shutil
import sqlite3
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'sqlite'
OUTPUT = SOURCE / 'exported_data'


def encode(value):
    if isinstance(value, bytes):
        return {'$blob_base64': base64.b64encode(value).decode('ascii')}
    return value


def export_table(con, table, target):
    columns = [row[1] for row in con.execute(f'PRAGMA table_info("{table}")')]
    count = 0
    with open(target, 'w', encoding='utf-8', newline='\n') as out:
        out.write('[')
        for row in con.execute(f'SELECT * FROM "{table}"'):
            out.write(',\n' if count else '\n')
            json.dump({c: encode(v) for c, v in zip(columns, row)}, out, ensure_ascii=False)
            count += 1
        out.write('\n]\n' if count else ']\n')
    return count


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--overwrite', action='store_true', help='replace existing exported files')
    args = parser.parse_args()

    databases = sorted(p for p in SOURCE.rglob('*')
                       if p.is_file() and p.suffix in ('.db', '.sqlite') and OUTPUT not in p.parents)
    with tempfile.TemporaryDirectory(prefix='sqlite-export-', dir=ROOT / '.tmp') as scratch:
        for db in databases:
            rel = db.relative_to(SOURCE).as_posix()
            copy_dir = Path(scratch) / rel.replace('/', '__')
            copy_dir.mkdir()
            for suffix in ('', '-wal', '-shm'):
                f = Path(str(db) + suffix)
                if f.exists():
                    shutil.copy2(f, copy_dir / (db.name + suffix))
            con = sqlite3.connect(f'file:{(copy_dir / db.name).as_posix()}?mode=ro', uri=True)
            try:
                tables = [r[0] for r in con.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
            except sqlite3.DatabaseError as exc:
                print(f'SKIPPED {rel}: {exc}')
                con.close()
                continue
            target_dir = OUTPUT / rel.replace('/', '__')
            target_dir.mkdir(parents=True, exist_ok=True)
            for table in tables:
                target = target_dir / f'{table}.json'
                if target.exists() and not args.overwrite:
                    print(f'EXISTS  {target.relative_to(ROOT).as_posix()} (use --overwrite)')
                    continue
                expected = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
                written = export_table(con, table, target)
                status = 'OK' if written == expected else f'MISMATCH expected={expected}'
                print(f'{status:<4} {target.relative_to(ROOT).as_posix()} rows={written} '
                      f'bytes={target.stat().st_size}')
            con.close()


if __name__ == '__main__':
    main()
