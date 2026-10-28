"""Measure exact embedded-image duplication using bounded Base64 decoding and SQLite.

No images are rendered, image-codec decoded, saved or transmitted. Only hashes,
byte lengths and source locators are retained. PostgreSQL is read-only.
"""
from __future__ import annotations

import argparse
import base64
import binascii
import csv
import hashlib
import itertools
import json
import sqlite3
import time

from index_session_records import ROOT, json_rows, lower_priority, safe_source_path, utc_now
from profile_response_items import export_locators, peak_memory
from profile_compacted import Profile, CHUNK, HISTORIES
from repair_null_record_types import save

VERSION = '1.0.0'
OUT = ROOT / 'data/reports/compacted_image_duplicates_v1'


class ImageString:
    """Receive JSON-unescaped string chunks, hashing decoded image bytes only."""
    def __init__(self, emit):
        self.emit = emit
        self.header = b''
        self.accepted = None
        self.tail = b''
        self.hasher = hashlib.sha256()
        self.decoded_bytes = 0
        self.padded = False
        self.added_padding = False
        self.error = None

    def feed(self, data):
        if self.accepted is False or self.error:
            return
        if self.accepted is None:
            combined = self.header + data
            prefix = b'data:image/'
            if combined[:min(len(combined),len(prefix))] != prefix[:min(len(combined),len(prefix))]:
                self.accepted = False
                return
            comma = combined.find(b',', 0, 120)
            if comma < 0:
                if len(combined) >= 120:
                    self.accepted = False
                else:
                    self.header = combined
                return
            if not combined[:comma].endswith(b';base64'):
                self.accepted = False
                return
            self.accepted = True
            self.header = b''
            data = combined[comma+1:]
        clean = data.translate(None, b' \t\r\n')
        if self.padded and clean:
            self.error = 'data_after_padding'
            return
        combined = self.tail + clean
        length = len(combined)//4*4
        if not length:
            self.tail = combined
            return
        block, self.tail = combined[:length], combined[length:]
        try:
            decoded = base64.b64decode(block, validate=True)
        except (binascii.Error, ValueError):
            self.error = 'invalid_base64'
            return
        if b'=' in block:
            self.padded = True
            if self.tail:
                self.error = 'data_after_padding'
                return
        self.hasher.update(decoded)
        self.decoded_bytes += len(decoded)

    def finish(self, stored_bytes):
        if self.accepted is not True:
            return
        if not self.error and self.tail:
            if len(self.tail) == 1:
                self.error = 'invalid_base64_length'
            else:
                try:
                    decoded = base64.b64decode(self.tail+b'='*(4-len(self.tail)), validate=True)
                    self.hasher.update(decoded)
                    self.decoded_bytes += len(decoded)
                    self.added_padding = True
                except (binascii.Error, ValueError):
                    self.error = 'invalid_base64'
        if not self.error and not self.decoded_bytes:
            self.error = 'empty_image_data'
        self.emit(None if self.error else self.hasher.digest(), self.decoded_bytes,
                  stored_bytes, self.error, self.added_padding)


def open_index(path, signature):
    conn = sqlite3.connect(path)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute('PRAGMA cache_size=-8192')
    conn.execute('PRAGMA temp_store=FILE')
    conn.execute('PRAGMA mmap_size=0')
    conn.executescript('''
CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS files (session_id INTEGER PRIMARY KEY, signature TEXT NOT NULL,
    records INTEGER NOT NULL, source_bytes INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS records (record_id INTEGER PRIMARY KEY, session_id INTEGER NOT NULL,
    source_offset INTEGER NOT NULL, sequence INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS occurrences (id INTEGER PRIMARY KEY, session_id INTEGER NOT NULL,
    record_id INTEGER NOT NULL, history TEXT NOT NULL, sha256 BLOB,
    decoded_bytes INTEGER NOT NULL, stored_bytes INTEGER NOT NULL,
    error TEXT, added_padding INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS image_hash ON occurrences(sha256, history, id);
CREATE INDEX IF NOT EXISTS image_record ON occurrences(record_id, history, sha256, id);
''')
    serialized = json.dumps(signature,sort_keys=True)
    row = conn.execute("SELECT value FROM metadata WHERE key='run_signature'").fetchone()
    if row and row[0] != serialized:
        conn.close()
        raise RuntimeError('Index signature changed; use a new output directory')
    conn.execute("INSERT OR IGNORE INTO metadata VALUES ('run_signature',?)",(serialized,))
    conn.commit()
    return conn


class OccurrenceWriter:
    def __init__(self, conn, session_id, record_id):
        self.conn, self.session, self.record = conn, session_id, record_id
        self.rows = []

    def factory(self, path):
        for history in HISTORIES:
            if path.startswith('payload.'+history+'[].'):
                def emit(digest, decoded, stored, error, padded, history=history):
                    self.rows.append((self.session,self.record,history,digest,decoded,stored,error,int(padded)))
                    if len(self.rows) >= 256:
                        self.flush()
                return ImageString(emit)
        return None

    def flush(self):
        self.conn.executemany('''INSERT INTO occurrences
            (session_id,record_id,history,sha256,decoded_bytes,stored_bytes,error,added_padding)
            VALUES (?,?,?,?,?,?,?,?)''',self.rows)
        self.rows.clear()


def scope_stats(conn, history=None):
    condition = 'error IS NULL' + (' AND history=?' if history else '')
    params = (history,) if history else ()
    count, stored, decoded = conn.execute(f'''SELECT count(*),coalesce(sum(stored_bytes),0),
        coalesce(sum(decoded_bytes),0) FROM occurrences WHERE {condition}''',params).fetchone()
    unique, unique_decoded, retained = conn.execute(f'''WITH first AS (
        SELECT min(id) id FROM occurrences WHERE {condition} GROUP BY sha256)
        SELECT count(*),coalesce(sum(o.decoded_bytes),0),coalesce(sum(o.stored_bytes),0)
        FROM first JOIN occurrences o ON o.id=first.id''',params).fetchone()
    return dict(occurrences=count, unique_images=unique, unique_decoded_bytes=unique_decoded,
                stored_bytes=stored, decoded_bytes_all_occurrences=decoded,
                retained_first_source_bytes=retained, duplicate_occurrences=count-unique,
                duplicate_stored_bytes=stored-retained, duplicate_decoded_bytes=decoded-unique_decoded)


def summarize(conn):
    scope = {history:scope_stats(conn,history) for history in HISTORIES}
    scope['combined'] = scope_stats(conn)
    shared = conn.execute('''WITH per_history AS (
        SELECT sha256,history,min(decoded_bytes) bytes FROM occurrences WHERE error IS NULL
        GROUP BY sha256,history), common AS (
        SELECT sha256,min(bytes) bytes FROM per_history GROUP BY sha256 HAVING count(*)=2)
        SELECT count(*),coalesce(sum(bytes),0) FROM common''').fetchone()
    between = dict(shared_unique_images=shared[0], shared_unique_decoded_bytes=shared[1],
                   extra_stored_bytes_after_within_history_deduplication=
                   sum(scope[h]['retained_first_source_bytes'] for h in HISTORIES)-scope['combined']['retained_first_source_bytes'])
    # The first-appearance hierarchy partitions duplicates without double counting.
    rows = conn.execute('''WITH ranked AS (SELECT *,
        row_number() OVER (PARTITION BY record_id,history,sha256 ORDER BY id) r,
        row_number() OVER (PARTITION BY session_id,history,sha256 ORDER BY id) s,
        row_number() OVER (PARTITION BY history,sha256 ORDER BY id) h,
        row_number() OVER (PARTITION BY sha256 ORDER BY id) g
        FROM occurrences WHERE error IS NULL), classified AS (
        SELECT *,CASE WHEN r>1 THEN 'within_same_record_and_history'
          WHEN s>1 THEN 'across_compacted_records_same_file_and_history'
          WHEN h>1 THEN 'across_files_same_history'
          WHEN g>1 THEN 'between_histories_extra_copy'
          ELSE 'retained_unique_copy' END category FROM ranked)
        SELECT category,count(*),sum(stored_bytes),sum(decoded_bytes)
        FROM classified GROUP BY category''').fetchall()
    categories = {name:dict(occurrences=n,stored_bytes=b,decoded_bytes=d) for name,n,b,d in rows}
    repetition = conn.execute('''WITH per_record AS (
        SELECT o.session_id,o.history,o.sha256,o.record_id,r.sequence
        FROM occurrences o JOIN records r ON r.record_id=o.record_id WHERE o.error IS NULL
        GROUP BY o.session_id,o.history,o.sha256,o.record_id), spans AS (
        SELECT session_id,history,sha256,count(*) n,min(sequence) first,max(sequence) last
        FROM per_record GROUP BY session_id,history,sha256), limits AS (
        SELECT session_id,max(sequence) last FROM records GROUP BY session_id)
        SELECT count(*),coalesce(sum(CASE WHEN s.n=s.last-s.first+1 THEN 1 ELSE 0 END),0),
          coalesce(sum(CASE WHEN s.n=l.last-s.first+1 THEN 1 ELSE 0 END),0),
          coalesce(max(s.n),0),count(DISTINCT hex(s.sha256))
        FROM spans s JOIN limits l ON l.session_id=s.session_id WHERE s.n>1''').fetchone()
    recurrence = dict(recurrent_image_file_history_groups=repetition[0],
                      groups_present_in_every_record_between_first_and_last=repetition[1],
                      groups_present_in_every_record_from_first_to_file_end=repetition[2],
                      maximum_compacted_records_for_one_image_in_same_file_history=repetition[3],
                      unique_images_repeated_across_records_in_a_file=repetition[4])
    errors = {name:count for name,count in conn.execute('SELECT error,count(*) FROM occurrences WHERE error IS NOT NULL GROUP BY error')}
    # Hash collisions are not expected; a conflicting byte count is an explicit failure.
    conflicts = conn.execute('''SELECT count(*) FROM (SELECT sha256 FROM occurrences
        WHERE error IS NULL GROUP BY sha256 HAVING min(decoded_bytes)<>max(decoded_bytes))''').fetchone()[0]
    assert conflicts == 0
    assert sum(scope[h]['unique_images'] for h in HISTORIES)-shared[0] == scope['combined']['unique_images']
    assert sum(v['stored_bytes'] for v in categories.values()) == scope['combined']['stored_bytes']
    assert sum(v['occurrences'] for v in categories.values()) == scope['combined']['occurrences']
    assert sum(v['stored_bytes'] for k,v in categories.items() if k!='retained_unique_copy') == scope['combined']['duplicate_stored_bytes']
    assert sum(scope[h]['duplicate_stored_bytes'] for h in HISTORIES)+between['extra_stored_bytes_after_within_history_deduplication'] == scope['combined']['duplicate_stored_bytes']
    return dict(scopes=scope,between_histories=between,duplicate_partition=categories,
                recurrence=recurrence,decoding_errors=errors,
                images_with_added_base64_padding=conn.execute('SELECT count(*) FROM occurrences WHERE added_padding=1 AND error IS NULL').fetchone()[0])


def markdown(report):
    lines=['# Embedded-image uniqueness and duplication in compacted histories','',
           'Generated (UTC): '+report['completed_at'],'',
           f"Status: {report['status']}. Source records: {report['records']:,}; source files: {report['files_profiled']:,}.",
           f"Source bytes scanned/reused: {report['source_bytes']:,}. Peak working set: {report['peak_working_set_bytes']/1024**2:.2f} MiB. Elapsed: {report['elapsed_seconds']:.3f} seconds.", '',
           '## Scope and method','',
           'All data:image/...;base64, string values inside replacement_history and guardian_history are processed through JSON unescaping and incremental Base64 decoding, then SHA-256 hashing. Images are never rendered, decoded by an image codec, saved as image files, transmitted or compared visually. External URLs are not fetched. Different binary encodings/resolutions of the same visual image remain distinct.', '',
           'Exact match means the same SHA-256 of decoded bytes. A disk-based SQLite index stores hashes, byte lengths and source record locators only, with an 8 MiB page cache, disk-backed temporary operations, 256 KiB source reads, below-normal priority and atomic per-file checkpoints.', '',
           'Unique binary size sums one decoded byte length per hash. Stored sizes measure the original JSON strings, including quotes, escaping and data URL prefixes. The retained stored representation is the first appearance in source-file database-ID order, then source-record order, then JSON traversal order. Duplicate stored bytes are all remaining appearances. No deduplication was applied to source files.', '',
           '## Size summary (GiB)', '',
           '| Scope | Unique images | Unique binary GiB | Stored JSON GiB | Duplicate JSON GiB | Duplicate share |',
           '|---|---:|---:|---:|---:|---:|']
    for name,v in report['scopes'].items():
        share=v['duplicate_stored_bytes']/v['stored_bytes'] if v['stored_bytes'] else 0
        lines.append(f"| `{name}` | {v['unique_images']:,} | {v['unique_decoded_bytes']/1024**3:.6f} | {v['stored_bytes']/1024**3:.6f} | {v['duplicate_stored_bytes']/1024**3:.6f} | {share:.4%} |")
    lines+=['', '1 GiB = 1,073,741,824 bytes. Binary and JSON sizes use different representations and must not be subtracted from each other.', '',
           '## Unique images and within-scope duplicates','',
           '| Scope | Image occurrences | Unique images | Unique decoded bytes | Stored bytes | Retained first-copy bytes | Duplicate stored bytes |',
           '|---|---:|---:|---:|---:|---:|---:|']
    for name,v in report['scopes'].items():
        lines.append(f"| `{name}` | {v['occurrences']:,} | {v['unique_images']:,} | {v['unique_decoded_bytes']:,} | {v['stored_bytes']:,} | {v['retained_first_source_bytes']:,} | {v['duplicate_stored_bytes']:,} |")
    b=report['between_histories']
    lines+=['','## Overlap between the histories','',
            f"Shared unique images: **{b['shared_unique_images']:,}**; shared unique binary bytes: **{b['shared_unique_decoded_bytes']:,}**.",
            f"Additional stored duplication attributable to keeping one copy in each history after each is deduplicated internally: **{b['extra_stored_bytes_after_within_history_deduplication']:,} bytes**.",'',
            'The two within-history duplicate totals plus this between-history component equal the combined duplicate stored bytes. Shared-image binary bytes describe set overlap and must not be added to stored duplicate totals.','',
            '## Disjoint duplicate categories','',
            '| Category | Occurrences | Stored bytes | Decoded bytes |','|---|---:|---:|---:|']
    for name,v in report['duplicate_partition'].items():
        lines.append(f"| `{name}` | {v['occurrences']:,} | {v['stored_bytes']:,} | {v['decoded_bytes']:,} |")
    lines+=['','## Reappearance across compacted records','',
            'Groups are (source file, history name, image hash). Sequence uses all indexed compacted records within that file. This measures stored snapshots; it does not establish model requests, active context contents or unique runtime compaction operations.','',
            '| Metric | Count |','|---|---:|']
    lines += [f'| `{k}` | {v:,} |' for k,v in report['recurrence'].items()]
    lines+=['','## Validation','',
            f"Prior measured image-string bytes: {report['baseline_image_stored_bytes']:,}; measured here: {sum(v['stored_bytes'] for k,v in report['scopes'].items() if k!='combined'):,}.",
            f"Base64 decoding failures: {sum(report['decoding_errors'].values()):,}. Images requiring missing Base64 padding to be supplied: {report['images_with_added_base64_padding']:,}.",
            'Source sizes and modification times are checked. Record/byte totals, per-history stored image bytes, records containing images, locator fingerprint, shared/union cardinalities and the disjoint byte decomposition are reconciled. No PostgreSQL writes occur.','',
            '## Reproduction','', '`python -B scripts/profile_compacted_images.py --run`','']
    return '\n'.join(lines)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run',action='store_true')
    parser.add_argument('--limit-files',type=int)
    parser.add_argument('--output-dir',default=str(OUT))
    args=parser.parse_args()
    if args.limit_files is not None and args.limit_files<1:
        parser.error('limit-files must be positive')
    output=(ROOT/args.output_dir).resolve()
    if not output.is_relative_to((ROOT/'data/reports').resolve()):
        parser.error('output-dir must be beneath data/reports')
    priority=lower_priority()
    plan=json_rows("""SELECT json_build_object('records',count(*),'source_bytes',sum(source_record_bytes),
        'files',count(DISTINCT research_session_id)) FROM public.research_session_records WHERE record_type='compacted';""")[0]
    print(json.dumps({'plan':plan,'priority':priority,'database_written':False}),flush=True)
    if not args.run:
        return 0
    baseline_path=ROOT/'data/reports/compacted_item_sizes_v1/summary.json'
    baseline=json.loads(baseline_path.read_text(encoding='utf-8'))
    output.mkdir(parents=True,exist_ok=True)
    started,clock=utc_now(),time.monotonic()
    locators=output/'locators.csv'
    fingerprint=export_locators(locators,'compacted')
    assert fingerprint==baseline['locator_sha256']
    assert plan['records']==baseline['records'] and plan['source_bytes']==baseline['source_bytes']
    paths={r['id']:r for r in json_rows("""SELECT json_build_object('id',id,'path',source_relative_path,
        'size',file_size_bytes) FROM public.research_sessions;""")}
    signature={'version':VERSION,'locator_sha256':fingerprint,'plan':plan}
    conn=open_index(output/'image_index.sqlite3',signature)
    files=records=source_bytes=cache_hits=read_bytes=0
    last_progress=time.monotonic()
    try:
        with locators.open(newline='',encoding='utf-8') as handle:
            numbers=(tuple(map(int,r)) for r in csv.reader(handle))
            for session,rows in itertools.groupby(numbers,key=lambda r:r[0]):
                if args.limit_files and files>=args.limit_files:
                    break
                entry=paths[session]
                path=safe_source_path(entry['path'])
                before=path.stat()
                if before.st_size!=entry['size']:
                    raise RuntimeError('Source size changed')
                file_signature=json.dumps({'path':entry['path'],'size':before.st_size,'mtime_ns':before.st_mtime_ns},sort_keys=True)
                cached=conn.execute('SELECT signature,records,source_bytes FROM files WHERE session_id=?',(session,)).fetchone()
                if cached:
                    if cached[0]!=file_signature:
                        raise RuntimeError('Source changed since checkpoint; use a new output directory')
                    for _ in rows:
                        pass
                    count,size=cached[1:]
                    cache_hits+=1
                else:
                    count=size=0
                    previous_end=-1
                    with conn:
                        with path.open('rb') as source:
                            for _,record_id,offset,length in rows:
                                if offset<previous_end or offset<0 or length<=0 or offset+length>before.st_size:
                                    raise RuntimeError('Invalid locator')
                                source.seek(offset)
                                writer=OccurrenceWriter(conn,session,record_id)
                                Profile(source,length,string_observer_factory=writer.factory).run(length)
                                writer.flush()
                                count+=1
                                size+=length
                                read_bytes+=length
                                previous_end=offset+length
                                conn.execute('INSERT INTO records VALUES (?,?,?,?)',(record_id,session,offset,count))
                                if time.monotonic()-last_progress>=15:
                                    print(json.dumps({'files_done':files,'records_done':records+count,
                                        'source_bytes_read':read_bytes,'peak_mib':round(peak_memory()/1024**2,2)}),flush=True)
                                    last_progress=time.monotonic()
                        after=path.stat()
                        if (before.st_size,before.st_mtime_ns)!=(after.st_size,after.st_mtime_ns):
                            raise RuntimeError('Source changed during scan')
                        conn.execute('INSERT INTO files VALUES (?,?,?,?)',(session,file_signature,count,size))
                records+=count
                source_bytes+=size
                files+=1
        summary=summarize(conn)
        expected_by_history={h:sum(v['embedded_image_string_bytes'] for v in baseline['history_item_sizes']['payload.'+h].values()) for h in HISTORIES}
        measured_by_history={h:conn.execute('SELECT coalesce(sum(stored_bytes),0) FROM occurrences WHERE history=?',(h,)).fetchone()[0] for h in HISTORIES}
        image_records=conn.execute('SELECT count(DISTINCT record_id) FROM occurrences').fetchone()[0]
        complete=(records==plan['records'] and source_bytes==plan['source_bytes'] and files==plan['files']
                  and not summary['decoding_errors'] and expected_by_history==measured_by_history
                  and image_records==baseline['features']['embedded_base64_image'])
        report={'version':VERSION,'started_at':started,'completed_at':utc_now(),
                'status':'completed' if complete else 'pilot' if args.limit_files else 'needs_review',
                'plan':plan,'records':records,'source_bytes':source_bytes,'files_profiled':files,
                'cache_hits':cache_hits,'bytes_read_this_run':read_bytes,'locator_sha256':fingerprint,
                'postgresql_written':False,'baseline_image_stored_bytes':sum(expected_by_history.values()),
                'baseline_image_bytes_by_history':expected_by_history,'observed_image_bytes_by_history':measured_by_history,
                'records_with_images':image_records,'limits':{'read_buffer_bytes':CHUNK,'sqlite_cache_kib':8192,'sqlite_temp_store':'file'},
                'elapsed_seconds':round(time.monotonic()-clock,3),'peak_working_set_bytes':peak_memory(),**summary}
        save(output/('pilot.json' if args.limit_files else 'summary.json'),report)
        (output/('pilot.md' if args.limit_files else 'report.md')).write_text(markdown(report),encoding='utf-8')
        conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        print(json.dumps({k:report[k] for k in ('status','records','files_profiled','elapsed_seconds','peak_working_set_bytes')}),flush=True)
        print(json.dumps({'combined':report['scopes']['combined'],'between_histories':report['between_histories'],'decoding_errors':report['decoding_errors']}),flush=True)
        return 0 if complete or args.limit_files and not summary['decoding_errors'] else 1
    finally:
        conn.close()


if __name__=='__main__':
    raise SystemExit(main())
