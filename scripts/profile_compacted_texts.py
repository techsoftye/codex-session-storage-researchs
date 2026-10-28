"""Hash text blocks in compacted histories without retaining conversation text."""
from __future__ import annotations

import argparse
import codecs
import csv
import hashlib
import itertools
import json
import re
import sqlite3
import time

from index_session_records import ROOT, json_rows, lower_priority, safe_source_path, utc_now
from profile_response_items import export_locators, peak_memory
from profile_compacted import Profile, CHUNK, HISTORIES
from repair_null_record_types import save

VERSION = '1.0.0'
OUT = ROOT/'data/reports/compacted_text_duplicates_v1'
TEXT_FIELDS = {'content[].text':'content_text', 'summary[].text':'reasoning_summary',
               'input':'tool_input', 'arguments':'tool_input',
               'output':'tool_output', 'output[].text':'tool_output'}
LABEL_FIELDS = {'type','id','role','status','call_id','name','phase','namespace','author','recipient',
                'content[].type','content[].detail','output[].type','output[].detail','summary[].type'}
SURROGATE = re.compile('[\ud800-\udfff]')
PAIR = re.compile('([\ud800-\udbff])([\udc00-\udfff])')


def classify(path):
    for history in HISTORIES:
        prefix='payload.'+history+'[].'
        if path.startswith(prefix):
            suffix=path[len(prefix):]
            if suffix in TEXT_FIELDS:
                return history, TEXT_FIELDS[suffix]
            if suffix.endswith('encrypted_content'):
                return history, 'encrypted_content'
            if suffix.endswith('image_url'):
                return history, 'image_reference'
            if suffix in LABEL_FIELDS or suffix.startswith(('internal_chat_message_metadata_passthrough.','metadata.')):
                return history, 'technical_metadata'
            return history, 'unclassified'
    return None


class StringMeasure:
    """Canonical UTF-8 hash. JSON surrogate pairs and literal Unicode are equivalent."""
    def __init__(self, category, emit):
        self.category,self.emit=category,emit
        self.selected=category in TEXT_FIELDS.values()
        self.decoder=codecs.getincrementaldecoder('utf-8')('surrogatepass')
        self.hasher=hashlib.sha256()
        self.prefix=b''
        self.pending=''
        self.utf8_bytes=0
        self.error=False
        self.checked=False
        self.media=False
        self.marker=False
        self.marker_tail=b''

    def feed(self, data):
        if not self.selected or self.media or self.error:
            return
        if not self.checked:
            combined=self.prefix+data
            if b'data:'.startswith(combined) or combined.startswith(b'data:') and len(combined)<120 and b',' not in combined:
                self.prefix=combined
                return
            self.checked=True
            self.media=combined.startswith((b'data:image/',b'data:audio/')) and b';base64,' in combined[:120]
            self.prefix=b''
            if self.media:
                return
            data=combined
        self.consume(data)

    def consume(self, data, final=False):
        marked=self.marker_tail+data
        self.marker |= b'data:image/' in marked or b'data:audio/' in marked
        self.marker_tail=marked[-24:]
        text=self.pending+self.decoder.decode(data,final=final)
        self.pending=''
        if not final and text and 0xD800<=ord(text[-1])<=0xDBFF:
            text,self.pending=text[:-1],text[-1]
        if SURROGATE.search(text):
            text=PAIR.sub(lambda m:chr(0x10000+((ord(m[1])-0xD800)<<10)+(ord(m[2])-0xDC00)),text)
            if SURROGATE.search(text):
                self.error=True
                return
        encoded=text.encode('utf-8')
        self.hasher.update(encoded)
        self.utf8_bytes+=len(encoded)

    def finish(self, stored_bytes):
        if self.selected and not self.media and not self.error:
            if self.prefix:
                self.consume(self.prefix)
            self.consume(b'',final=True)
        outcome=('excluded_'+self.category if not self.selected else
                 'excluded_embedded_media' if self.media else
                 'invalid_unicode' if self.error else 'empty_text' if not self.utf8_bytes else 'text')
        self.emit(outcome,self.hasher.digest() if outcome=='text' else None,
                  self.utf8_bytes,stored_bytes,int(self.marker))


def open_index(path, signature):
    conn=sqlite3.connect(path)
    for pragma in ('journal_mode=WAL','synchronous=NORMAL','cache_size=-8192','temp_store=FILE','mmap_size=0'):
        conn.execute('PRAGMA '+pragma)
    conn.executescript('''
CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS fields(id INTEGER PRIMARY KEY,path TEXT UNIQUE,history TEXT,category TEXT);
CREATE TABLE IF NOT EXISTS files(session_id INTEGER PRIMARY KEY,signature TEXT,records INTEGER,source_bytes INTEGER);
CREATE TABLE IF NOT EXISTS records(record_id INTEGER PRIMARY KEY,session_id INTEGER,source_offset INTEGER,sequence INTEGER);
CREATE TABLE IF NOT EXISTS occurrences(id INTEGER PRIMARY KEY,session_id INTEGER,record_id INTEGER,
    field_id INTEGER,sha256 BLOB,utf8_bytes INTEGER,stored_bytes INTEGER,media_marker INTEGER);
CREATE INDEX IF NOT EXISTS text_hash ON occurrences(sha256,id);
CREATE TABLE IF NOT EXISTS field_stats(session_id INTEGER,field_id INTEGER,outcome TEXT,
    occurrences INTEGER,stored_bytes INTEGER,PRIMARY KEY(session_id,field_id,outcome));
''')
    serialized=json.dumps(signature,sort_keys=True)
    row=conn.execute("SELECT value FROM metadata WHERE key='signature'").fetchone()
    if row and row[0]!=serialized:
        conn.close()
        raise RuntimeError('Index signature changed; use a new output directory')
    conn.execute("INSERT OR IGNORE INTO metadata VALUES ('signature',?)",(serialized,))
    conn.commit()
    return conn


class Writer:
    def __init__(self, conn, session):
        self.conn,self.session=conn,session
        self.fields={path:(i,h,c) for i,path,h,c in conn.execute('SELECT * FROM fields')}
        self.stats={}
        self.rows=[]
        self.record=None

    def factory(self, path):
        decision=classify(path)
        if not decision:
            return None
        if path not in self.fields:
            cursor=self.conn.execute('INSERT INTO fields(path,history,category) VALUES (?,?,?)',(path,*decision))
            self.fields[path]=(cursor.lastrowid,*decision)
        field_id,history,category=self.fields[path]
        def emit(outcome,digest,utf8,stored,marker):
            counts=self.stats.setdefault((field_id,outcome),[0,0])
            counts[0]+=1
            counts[1]+=stored
            if outcome=='text':
                self.rows.append((self.session,self.record,field_id,digest,utf8,stored,marker))
                if len(self.rows)>=256:
                    self.flush()
        return StringMeasure(category,emit)

    def flush(self):
        self.conn.executemany('''INSERT INTO occurrences
            (session_id,record_id,field_id,sha256,utf8_bytes,stored_bytes,media_marker) VALUES (?,?,?,?,?,?,?)''',self.rows)
        self.rows.clear()

    def finish_file(self):
        self.flush()
        self.conn.executemany('INSERT INTO field_stats VALUES (?,?,?,?,?)',
            ((self.session,field_id,outcome,n,b) for (field_id,outcome),(n,b) in self.stats.items()))


def metrics():
    return dict(occurrences=0,unique_texts=0,unique_utf8_bytes=0,stored_bytes=0,
                utf8_bytes_all_occurrences=0,retained_first_source_bytes=0)


def add(dest, utf8, stored, first):
    dest['occurrences']+=1
    dest['stored_bytes']+=stored
    dest['utf8_bytes_all_occurrences']+=utf8
    if first:
        dest['unique_texts']+=1
        dest['unique_utf8_bytes']+=utf8
        dest['retained_first_source_bytes']+=stored


def finish_metrics(dest):
    dest['duplicate_occurrences']=dest['occurrences']-dest['unique_texts']
    dest['duplicate_stored_bytes']=dest['stored_bytes']-dest['retained_first_source_bytes']
    dest['duplicate_utf8_bytes']=dest['utf8_bytes_all_occurrences']-dest['unique_utf8_bytes']


def length_band(n):
    return next(label for limit,label in [(128,'1-128 bytes'),(1024,'129-1024 bytes'),
        (16384,'1025-16384 bytes'),(1048576,'16385-1048576 bytes'),(float('inf'),'over 1048576 bytes')] if n<=limit)


def summarize(conn):
    fields={i:(h,c) for i,_,h,c in conn.execute('SELECT * FROM fields')}
    limits=dict(conn.execute('SELECT session_id,max(sequence) FROM records GROUP BY session_id'))
    scopes={h:metrics() for h in (*HISTORIES,'combined')}
    types,bands,partition={},{},{}
    shared=shared_bytes=marker_count=marker_bytes=0
    recurrence=dict(recurrent_text_file_history_groups=0,groups_continuous_between_first_and_last=0,
        groups_continuous_until_file_end=0,maximum_records_for_one_text=0,unique_texts_repeated_across_records_in_a_file=0)
    # Sorted disk index + one hash group at a time, avoiding large SQL window buffers.
    query='''SELECT o.sha256,o.session_id,o.record_id,o.field_id,o.utf8_bytes,o.stored_bytes,o.media_marker,r.sequence
        FROM occurrences o INDEXED BY text_hash JOIN records r ON r.record_id=o.record_id ORDER BY o.sha256,o.id'''
    for digest,rows in itertools.groupby(conn.execute(query),key=lambda row:row[0]):
        seen_histories,seen_records,seen_files,seen_types=set(),set(),set(),set()
        spans={}
        expected_size=None
        for _,session,record,field_id,utf8,stored,marker,sequence in rows:
            if expected_size is None:
                expected_size=utf8
            assert expected_size==utf8,'Conflicting UTF-8 lengths for one digest'
            history,category=fields[field_id]
            record_key,file_key=(record,history),(session,history)
            first=not seen_histories
            add(scopes['combined'],utf8,stored,first)
            add(scopes[history],utf8,stored,history not in seen_histories)
            add(types.setdefault(category,metrics()),utf8,stored,category not in seen_types)
            add(bands.setdefault(length_band(utf8),metrics()),utf8,stored,first)
            classification=('within_same_record_and_history' if record_key in seen_records else
                'across_compacted_records_same_file_and_history' if file_key in seen_files else
                'across_files_same_history' if history in seen_histories else
                'between_histories_extra_copy' if seen_histories else 'retained_unique_copy')
            bucket=partition.setdefault(classification,dict(occurrences=0,stored_bytes=0,utf8_bytes=0))
            bucket['occurrences']+=1
            bucket['stored_bytes']+=stored
            bucket['utf8_bytes']+=utf8
            if record_key not in seen_records:
                span=spans.setdefault(file_key,[0,sequence,sequence])
                span[0]+=1
                span[1]=min(span[1],sequence)
                span[2]=max(span[2],sequence)
            seen_records.add(record_key)
            seen_files.add(file_key)
            seen_histories.add(history)
            seen_types.add(category)
            marker_count+=marker
            marker_bytes+=stored if marker else 0
        if len(seen_histories)==2:
            shared+=1
            shared_bytes+=expected_size
        recurring=False
        for (session,history),(count,first,last) in spans.items():
            if count<2:
                continue
            recurring=True
            recurrence['recurrent_text_file_history_groups']+=1
            recurrence['groups_continuous_between_first_and_last']+=int(count==last-first+1)
            recurrence['groups_continuous_until_file_end']+=int(count==limits[session]-first+1)
            recurrence['maximum_records_for_one_text']=max(recurrence['maximum_records_for_one_text'],count)
        recurrence['unique_texts_repeated_across_records_in_a_file']+=int(recurring)
    for dest in (*scopes.values(),*types.values(),*bands.values()):
        finish_metrics(dest)
    between=dict(shared_unique_texts=shared,shared_unique_utf8_bytes=shared_bytes,
        extra_stored_bytes_after_within_history_deduplication=sum(scopes[h]['retained_first_source_bytes'] for h in HISTORIES)-scopes['combined']['retained_first_source_bytes'])
    field_rows=[dict(path=p,history=h,category=c,outcome=o,occurrences=n,stored_bytes=b)
        for p,h,c,o,n,b in conn.execute('''SELECT f.path,f.history,f.category,s.outcome,sum(s.occurrences),sum(s.stored_bytes)
            FROM field_stats s JOIN fields f ON f.id=s.field_id GROUP BY s.field_id,s.outcome ORDER BY f.path,s.outcome''')]
    outcomes={}
    for row in field_rows:
        m=outcomes.setdefault(row['outcome'],dict(occurrences=0,stored_bytes=0))
        m['occurrences']+=row['occurrences'];m['stored_bytes']+=row['stored_bytes']
    c=scopes['combined']
    assert sum(scopes[h]['unique_texts'] for h in HISTORIES)-shared==c['unique_texts']
    assert sum(v['stored_bytes'] for v in partition.values())==c['stored_bytes']
    assert sum(v['occurrences'] for v in partition.values())==c['occurrences']
    assert sum(scopes[h]['duplicate_stored_bytes'] for h in HISTORIES)+between['extra_stored_bytes_after_within_history_deduplication']==c['duplicate_stored_bytes']
    assert sum(v['stored_bytes'] for v in types.values())==c['stored_bytes']
    assert sum(v['unique_texts'] for v in bands.values())==c['unique_texts']
    assert outcomes.get('text',{}).get('occurrences',0)==c['occurrences']
    return dict(scopes=scopes,between_histories=between,duplicate_partition=partition,by_category=types,
        by_utf8_length=bands,recurrence=recurrence,field_classification=field_rows,outcome_totals=outcomes,
        text_fields_with_media_markers=dict(occurrences=marker_count,stored_bytes=marker_bytes))


def markdown(r):
    lines=['# Exact text-block uniqueness and duplication in compacted histories','',
        f"Completed UTC: {r['completed_at']}. Status: **{r['status']}**.",
        f"Records: {r['records']:,}; files: {r['files_profiled']:,}; source bytes: {r['source_bytes']:,}.",
        f"Peak process RAM: {r['peak_working_set_bytes']/1024**2:.2f} MiB; runtime: {r['elapsed_seconds']:.3f} seconds; reused files: {r['cache_hits']}.",'',
        '## Definition and scope','',
        'The unit is one nonempty selected JSON string, not an entire message or a semantic statement. SHA-256 is computed over canonical UTF-8 after JSON unescaping, combining Unicode surrogate pairs. Whitespace, line endings and Unicode normalization are preserved. Literal Unicode and its equivalent JSON escapes match. Empty values are counted separately. Unique sizes are totals across unique text blocks, not the size of one block.','',
        'Selected suffixes: content[].text (content_text), summary[].text (reasoning_summary), input and arguments (tool_input), output and output[].text (tool_output). These describe field locations, not verified human authorship. Stringified JSON in tool arguments/output is measured as a whole string; it is not recursively parsed. Embedded-media values, image references, encrypted content and technical metadata are excluded and accounted for. Unknown fields and invalid Unicode prevent completed status.','',
        'Physical stored bytes include JSON quotes and escaping. Retaining the first occurrence per digest in source-file database-ID order, source-record order and JSON traversal order defines the remaining duplicate bytes. Unique UTF-8 sizes and physical JSON sizes use different representations. No source files or PostgreSQL rows are changed. No text, images or raw arguments are emitted or stored in the derived index.','',
        '## Main results','',
        '| Scope | Occurrences | Unique text blocks | Unique UTF-8 GiB | Stored JSON GiB | Duplicate JSON GiB |',
        '|---|---:|---:|---:|---:|---:|']
    for k,v in r['scopes'].items():
        lines.append(f"| `{k}` | {v['occurrences']:,} | {v['unique_texts']:,} | {v['unique_utf8_bytes']/1024**3:.6f} | {v['stored_bytes']/1024**3:.6f} | {v['duplicate_stored_bytes']/1024**3:.6f} |")
    b=r['between_histories']
    lines+=['','## Between histories','',
        f"Shared unique blocks: **{b['shared_unique_texts']:,}**; shared unique UTF-8 bytes: **{b['shared_unique_utf8_bytes']:,}**.",
        f"Additional duplicate JSON bytes after each history is deduplicated internally: **{b['extra_stored_bytes_after_within_history_deduplication']:,}** ({b['extra_stored_bytes_after_within_history_deduplication']/1024**3:.6f} GiB).",
        'The two within-history duplicate amounts plus this additional between-history amount equal combined duplication. Do not add shared UTF-8 bytes to physical duplicate bytes.']
    for title,key in [('Disjoint duplicate allocation','duplicate_partition'),('Field categories','by_category'),('UTF-8 length bands','by_utf8_length'),('String classification outcomes','outcome_totals')]:
        lines+=['','## '+title,'','| Category | Occurrences | Stored bytes | Stored GiB |','|---|---:|---:|---:|']
        for k,v in r[key].items():
            lines.append(f"| `{k}` | {v['occurrences']:,} | {v['stored_bytes']:,} | {v['stored_bytes']/1024**3:.6f} |")
    lines+=['','Unique counts within field categories can overlap; they are not additive. UTF-8 length bands partition unique blocks.','',
        '## Validation and limits','',
        f"Baseline string-field occurrence counts reconcile: {r['field_counts_match_baseline']}. Prior image-reference bytes reconcile: {r['image_bytes_match_baseline']}. Prior encrypted-string bytes reconcile: {r['encrypted_bytes_match_baseline']}.",
        f"Nonempty text fields containing data:image/ or data:audio/ markers: {r['text_fields_with_media_markers']['occurrences']:,}. Such composite text fields retain their full text size; media substrings inside arbitrary text are not extracted.",
        'Counts are storage-level text blocks. They are not independent messages, unique requests, semantic equivalence or context-window occupancy. Repeated stored histories and forks may contribute.','',
        'Read buffer: 256 KiB; SQLite page cache: 8 MiB; disk-based temporary operations; one hash group at a time during aggregation; below-normal priority; atomic per-file checkpoints. Full source sizes/mtime and locator fingerprints are checked. The JSON includes exact bytes, per-field decisions, recurrence and resource statistics.','',
        '1 GiB = 1,073,741,824 bytes. Reproduce with `python -B scripts/profile_compacted_texts.py --run`.','']
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
    baseline=json.loads((ROOT/'data/reports/compacted_item_sizes_v1/summary.json').read_text(encoding='utf-8'))
    output.mkdir(parents=True,exist_ok=True)
    started,clock=utc_now(),time.monotonic()
    locators=output/'locators.csv'
    fingerprint=export_locators(locators,'compacted')
    assert fingerprint==baseline['locator_sha256']
    assert plan['records']==baseline['records'] and plan['source_bytes']==baseline['source_bytes']
    paths={r['id']:r for r in json_rows("""SELECT json_build_object('id',id,'path',source_relative_path,
        'size',file_size_bytes) FROM public.research_sessions;""")}
    conn=open_index(output/'text_index.sqlite3',dict(version=VERSION,locator_sha256=fingerprint,plan=plan))
    files=records=source_bytes=cache_hits=read_bytes=0
    last_progress=time.monotonic()
    try:
        if args.limit_files and conn.execute('SELECT count(*) FROM files').fetchone()[0]>args.limit_files:
            raise RuntimeError('Pilot limit is smaller than cached scope; use a separate output directory')
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
                signature=json.dumps(dict(path=entry['path'],size=before.st_size,mtime_ns=before.st_mtime_ns),sort_keys=True)
                cached=conn.execute('SELECT signature,records,source_bytes FROM files WHERE session_id=?',(session,)).fetchone()
                if cached:
                    if cached[0]!=signature:
                        raise RuntimeError('Source changed since checkpoint; use a new output directory')
                    for _ in rows:
                        pass
                    count,size=cached[1:]
                    cache_hits+=1
                else:
                    count=size=0
                    previous_end=-1
                    with conn:
                        writer=Writer(conn,session)
                        with path.open('rb') as source:
                            for _,record_id,offset,length in rows:
                                if offset<previous_end or offset<0 or length<=0 or offset+length>before.st_size:
                                    raise RuntimeError('Invalid locator')
                                source.seek(offset)
                                writer.record=record_id
                                Profile(source,length,string_observer_factory=writer.factory).run(length)
                                count+=1;size+=length;read_bytes+=length
                                previous_end=offset+length
                                conn.execute('INSERT INTO records VALUES (?,?,?,?)',(record_id,session,offset,count))
                                if time.monotonic()-last_progress>=15:
                                    print(json.dumps(dict(files_done=files,records_done=records+count,
                                        source_bytes_read=read_bytes,peak_mib=round(peak_memory()/1024**2,2))),flush=True)
                                    last_progress=time.monotonic()
                        writer.finish_file()
                        after=path.stat()
                        if (before.st_size,before.st_mtime_ns)!=(after.st_size,after.st_mtime_ns):
                            raise RuntimeError('Source changed during scan')
                        conn.execute('INSERT INTO files VALUES (?,?,?,?)',(session,signature,count,size))
                records+=count;source_bytes+=size;files+=1
        print(json.dumps({'stage':'aggregating_hashes','records':records,'files':files}),flush=True)
        summary=summarize(conn)
        expected={k.removesuffix(' : string'):v['occurrences'] for k,v in baseline['fields'].items()
                  if k.endswith(' : string') and classify(k.removesuffix(' : string'))}
        observed={}
        for row in summary['field_classification']:
            observed[row['path']]=observed.get(row['path'],0)+row['occurrences']
        images=sum(row['stored_bytes'] for row in summary['field_classification'] if row['outcome'] in ('excluded_image_reference','excluded_embedded_media'))
        encrypted=summary['outcome_totals'].get('excluded_encrypted_content',{}).get('stored_bytes',0)
        previous_images=sum(v['embedded_image_string_bytes'] for types in baseline['history_item_sizes'].values() for v in types.values())
        previous_encrypted=sum(v['encrypted_content_string_bytes'] for types in baseline['history_item_sizes'].values() for v in types.values())
        issues={k:v for k,v in summary['outcome_totals'].items() if k in ('invalid_unicode','excluded_unclassified')}
        complete=(records==plan['records'] and files==plan['files'] and source_bytes==plan['source_bytes']
                  and expected==observed and images==previous_images and encrypted==previous_encrypted and not issues)
        report=dict(version=VERSION,started_at=started,completed_at=utc_now(),
            status='completed' if complete else 'pilot' if args.limit_files and not issues else 'needs_review',
            plan=plan,records=records,source_bytes=source_bytes,files_profiled=files,cache_hits=cache_hits,
            bytes_read_this_run=read_bytes,locator_sha256=fingerprint,postgresql_written=False,
            field_counts_match_baseline=expected==observed,image_bytes_match_baseline=images==previous_images,
            encrypted_bytes_match_baseline=encrypted==previous_encrypted,issues=issues,
            limits=dict(read_buffer_bytes=CHUNK,sqlite_cache_kib=8192,aggregation='one_hash_group_at_a_time'),
            elapsed_seconds=round(time.monotonic()-clock,3),peak_working_set_bytes=peak_memory(),**summary)
        save(output/('pilot.json' if args.limit_files else 'summary.json'),report)
        (output/('pilot.md' if args.limit_files else 'report.md')).write_text(markdown(report),encoding='utf-8')
        conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        print(json.dumps({k:report[k] for k in ('status','records','files_profiled','elapsed_seconds','peak_working_set_bytes','issues')}),flush=True)
        print(json.dumps(dict(combined=report['scopes']['combined'],between_histories=report['between_histories'])),flush=True)
        return 0 if complete or args.limit_files and not issues else 1
    finally:
        conn.close()


if __name__=='__main__':
    raise SystemExit(main())
