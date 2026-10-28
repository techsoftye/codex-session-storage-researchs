"""Extract every structural token-counter object across all indexed JSONL types.

Default: read-only PostgreSQL plan. --sample: bounded randomized structural pilot.
--apply: stream files and atomically commit observations plus record checkpoints.
No conversation text/base64, token summation, deduplication, or price calculation.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time

from import_session_survey import connection_settings, find_psql
from index_session_records import ROOT, json_rows, lower_priority, utc_now
from extract_record_sample import source_path
from profile_response_items import peak_memory
from repair_null_record_types import save
from token_usage_stream import TokenReader, VERSION, COUNTERS, IDENTIFIERS, CHUNK, encoded

OUT = ROOT / 'data/reports/token_usage_observations_v1'
OBS = 'public.research_token_usage_observations'
STATE = 'public.research_token_usage_scan_state'
COLUMNS = ('research_session_record_id', 'source_json_path', 'source_object_byte_offset',
           'source_object_bytes', 'usage_scope', *IDENTIFIERS, *COUNTERS, 'metadata')


def positive(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError('must be positive')
    return number


def read_query(sql):
    return json_rows("BEGIN READ ONLY; SET LOCAL statement_timeout='120s'; "
                     "SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL work_mem='8MB'; " + sql + '; COMMIT;')


def plan(session_id=None):
    condition = f'WHERE rs.id={session_id}' if session_id else ''
    return read_query(f'''SELECT json_build_object('id',rs.id,
        'source_relative_path',rs.source_relative_path,'file_size_bytes',rs.file_size_bytes,
        'record_count',count(rr.id),'indexed_bytes',coalesce(sum(rr.source_record_bytes),0),
        'state',to_jsonb(st))
        FROM public.research_sessions rs
        LEFT JOIN public.research_session_records rr ON rr.research_session_id=rs.id
        LEFT JOIN {STATE} st ON st.research_session_id=rs.id
        {condition}
        GROUP BY rs.id,st.research_session_id ORDER BY rs.file_size_bytes,rs.id''')


def locators(session_id, after_line, count):
    return read_query(f'''SELECT json_build_object('id',id,'line',source_line_number,
        'offset',source_byte_offset,'bytes',source_record_bytes,'type',record_type)
        FROM public.research_session_records WHERE research_session_id={session_id}
        AND source_line_number>{after_line} ORDER BY source_line_number LIMIT {count}''')


def read_gap(handle, end):
    if handle.tell() > end:
        raise ValueError('overlapping_indexed_records')
    while handle.tell() < end:
        part = handle.read(min(CHUNK, end-handle.tell()))
        if not part:
            raise ValueError('short_source_read')
        if part.strip(b' \t\r\n'):
            raise ValueError('unindexed_nonblank_bytes')


def validate_state(item, stat):
    if stat.st_size != item['file_size_bytes'] or item['indexed_bytes'] > stat.st_size:
        raise ValueError('source_size_or_index_mismatch')
    if not item['record_count'] and stat.st_size:
        raise ValueError('source_has_no_record_index')
    state = item.get('state')
    if state:
        expected = (VERSION, stat.st_size, stat.st_mtime_ns, item['record_count'])
        actual = tuple(state[k] for k in ('scan_version','source_size_bytes','source_mtime_ns','expected_record_count'))
        if actual != expected:
            raise ValueError('checkpoint_signature_changed')
        if not (0 <= state['next_byte_offset'] <= stat.st_size and
                0 <= state['scanned_record_count'] <= item['record_count']):
            raise ValueError('invalid_checkpoint')
        if state['last_record_id']:
            last = read_query(f'''SELECT json_build_object('id',id,'line',source_line_number,
                'end',source_byte_offset+source_record_bytes) FROM public.research_session_records
                WHERE id={state['last_record_id']} AND research_session_id={item['id']}''')
            # A completed file can include trailing blank lines after its last record.
            if len(last) != 1 or last[0]['line'] != state['last_line_number'] or (
                    last[0]['end'] != state['next_byte_offset'] and state['status'] != 'completed'):
                raise ValueError('checkpoint_locator_changed')
        return dict(state)
    return {'last_line_number':0,'last_record_id':None,'next_byte_offset':0,
            'scanned_record_count':0,'observation_count':0,'status':'running'}


def execute_file(path):
    config = connection_settings()
    if config['DB_DATABASE'] != 'codex_session_research':
        raise RuntimeError('unexpected_database')
    environment = os.environ.copy()
    environment.update(PGPASSWORD=config['DB_PASSWORD'], PGCONNECT_TIMEOUT='5', PGCLIENTENCODING='UTF8')
    command = [find_psql(), '-X','-w','-q','-A','-t','-v','ON_ERROR_STOP=1','-v','VERBOSITY=sqlstate',
               '-h',config['DB_HOST'],'-p',config['DB_PORT'],'-U',config['DB_USERNAME'],'-d',config['DB_DATABASE']]
    with path.open('rb') as handle:
        result = subprocess.run(command, stdin=handle, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                env=environment, timeout=180)
    if result.returncode:
        # Do not print COPY input / identifier values from database diagnostics.
        match = re.search(rb'(?:ERROR|FATAL):\s+([0-9A-Z]{5})\b', result.stderr)
        suffix = '_sqlstate_' + match[1].decode().lower() if match else ''
        raise RuntimeError('database_batch_failed_exit_' + str(result.returncode) + suffix)


class Batch:
    def __init__(self, directory, item, state, stat):
        self.directory, self.item, self.state, self.stat = directory, item, state, stat
        self.previous_offset = state['next_byte_offset']
        self.committed_state = dict(state)
        self.count = 0
        self.path = directory / 'batch.csv'
        self.handle = self.path.open('w+', encoding='utf-8', newline='')
        self.writer = csv.writer(self.handle, lineterminator='\n')

    def add(self, observation):
        self.writer.writerow([encoded(observation[key]) if key == 'metadata' else
                              ('\\N' if observation[key] is None else observation[key])
                              for key in COLUMNS])
        self.count += 1

    def close(self):
        self.handle.close()

    def flush(self, status, error_record=None, error_code=None):
        self.handle.flush()
        state, item, stat = self.state, self.item, self.stat
        name_list = ','.join(COLUMNS)
        old_row = ','.join('o.'+x for x in COLUMNS)
        new_row = ','.join('s.'+x for x in COLUMNS)
        error_sql = 'NULL' if error_code is None else "'"+error_code.replace("'", "''")+"'"
        record_sql = str(error_record) if error_record is not None else 'NULL'
        last_record = str(state['last_record_id']) if state['last_record_id'] else 'NULL'
        sql_path = self.directory / 'batch.sql'
        with sql_path.open('wb') as sql:
            def write(text):
                sql.write(text.encode('utf-8'))
            write(f'''BEGIN;
SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='120s';
SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL work_mem='8MB';
INSERT INTO {STATE} (research_session_id,scan_version,source_size_bytes,source_mtime_ns,expected_record_count,status)
VALUES ({item['id']},'{VERSION}',{stat.st_size},{stat.st_mtime_ns},{item['record_count']},'running')
ON CONFLICT (research_session_id) DO NOTHING;
DO $$ BEGIN
PERFORM 1 FROM {STATE} WHERE research_session_id={item['id']}
 AND next_byte_offset={self.previous_offset} AND scan_version='{VERSION}'
 AND source_size_bytes={stat.st_size} AND source_mtime_ns={stat.st_mtime_ns}
 AND expected_record_count={item['record_count']} FOR UPDATE;
IF NOT FOUND THEN RAISE EXCEPTION 'Concurrent or changed checkpoint'; END IF;
END $$;
CREATE TEMP TABLE token_usage_stage AS SELECT {name_list} FROM {OBS} WITH NO DATA;
COPY token_usage_stage ({name_list}) FROM STDIN WITH (FORMAT csv, NULL '\\N');
''')
            with self.path.open('rb') as data:
                shutil.copyfileobj(data, sql, CHUNK)
            write(f'''\\.
DO $$ BEGIN
IF EXISTS (SELECT 1 FROM token_usage_stage s JOIN {OBS} o
 ON o.research_session_record_id=s.research_session_record_id
 AND o.source_object_byte_offset=s.source_object_byte_offset
 WHERE ROW({old_row}) IS DISTINCT FROM ROW({new_row}))
THEN RAISE EXCEPTION 'Existing observation differs'; END IF;
END $$;
INSERT INTO {OBS} ({name_list}) SELECT {name_list} FROM token_usage_stage
ON CONFLICT (research_session_record_id,source_object_byte_offset) DO NOTHING;
UPDATE {STATE} SET status='{status}', last_line_number={state['last_line_number']},
 last_record_id={last_record},next_byte_offset={state['next_byte_offset']},
 scanned_record_count={state['scanned_record_count']},observation_count={state['observation_count']},
 updated_at=CURRENT_TIMESTAMP,completed_at={'CURRENT_TIMESTAMP' if status == 'completed' else 'NULL'},
 error_record_id={record_sql},error_code={error_sql} WHERE research_session_id={item['id']};
COMMIT;
''')
        execute_file(sql_path)
        self.previous_offset = state['next_byte_offset']
        state['status'] = status
        self.committed_state = dict(state)
        self.handle.seek(0); self.handle.truncate(); self.count = 0


def safe_error(error):
    if isinstance(error, UnicodeError):
        return 'invalid_utf8'
    message = str(error)
    if message and len(message) < 100 and all(c in 'abcdefghijklmnopqrstuvwxyz_0123456789' for c in message):
        return message
    return type(error).__name__


def scan_file(item, args, reader, directory, budget=None):
    path = source_path(item['source_relative_path'])
    stat = path.stat()
    state = validate_state(item, stat)
    if state['status'] == 'completed':
        return state, 0
    initial = state['scanned_record_count']
    batch = Batch(directory, item, state, stat)
    since_flush, last_flush, current_record = 0, time.monotonic(), None
    try:
        with path.open('rb', buffering=CHUNK) as source:
            source.seek(state['next_byte_offset'])
            while True:
                if state['scanned_record_count'] == item['record_count']:
                    read_gap(source, stat.st_size)
                    state['next_byte_offset'] = stat.st_size
                    fresh = path.stat()
                    if (fresh.st_size, fresh.st_mtime_ns) != (stat.st_size, stat.st_mtime_ns):
                        raise ValueError('source_changed_during_scan')
                    batch.flush('completed')
                    return state, state['scanned_record_count']-initial
                remaining = None if budget is None else budget-(state['scanned_record_count']-initial)
                if remaining is not None and remaining <= 0:
                    batch.flush('partial')
                    return state, state['scanned_record_count']-initial
                rows = locators(item['id'], state['last_line_number'],
                                min(args.batch_records, remaining) if remaining is not None else args.batch_records)
                if not rows:
                    if state['scanned_record_count'] != item['record_count']:
                        raise ValueError('record_count_mismatch')
                    read_gap(source, stat.st_size)
                    state['next_byte_offset'] = stat.st_size
                    fresh = path.stat()
                    if (fresh.st_size, fresh.st_mtime_ns) != (stat.st_size, stat.st_mtime_ns):
                        raise ValueError('source_changed_during_scan')
                    batch.flush('completed')
                    return state, state['scanned_record_count']-initial
                for row in rows:
                    current_record = row['id']
                    if row['bytes'] <= 0 or row['offset']+row['bytes'] > stat.st_size:
                        raise ValueError('invalid_record_byte_range')
                    read_gap(source, row['offset'])
                    reader.parse(source, row['bytes'])
                    if reader.root_type != row['type']:
                        raise ValueError('indexed_record_type_mismatch')
                    before_row = batch.handle.tell(), batch.count, state['observation_count']
                    try:
                        for observation in reader.observations(row['id'], row['offset']):
                            batch.add(observation)
                            state['observation_count'] += 1
                    except (OSError, ValueError, RuntimeError):
                        batch.handle.seek(before_row[0]); batch.handle.truncate()
                        batch.count, state['observation_count'] = before_row[1:]
                        raise
                    state.update(last_line_number=row['line'],last_record_id=row['id'],
                                 next_byte_offset=row['offset']+row['bytes'])
                    state['scanned_record_count'] += 1
                    since_flush += 1
                    if since_flush >= args.batch_records or time.monotonic()-last_flush >= 5:
                        fresh = path.stat()
                        if (fresh.st_size, fresh.st_mtime_ns) != (stat.st_size, stat.st_mtime_ns):
                            raise ValueError('source_changed_during_scan')
                        batch.flush('running')
                        print(json.dumps({'file_id':item['id'],'scanned_records':state['scanned_record_count'],
                                          'observations':state['observation_count'],
                                          'next_byte_offset':state['next_byte_offset']}), flush=True)
                        since_flush, last_flush = 0, time.monotonic()
                        time.sleep(args.pause_ms/1000)
    except (OSError, ValueError, RuntimeError) as error:
        # Only fully parsed records may be checkpointed. The failing record is retried.
        # An unknown database commit outcome is never followed by another write.
        if not isinstance(error, RuntimeError) or not str(error).startswith('database_'):
            if str(error) == 'source_changed_during_scan':
                state.clear(); state.update(batch.committed_state)
                batch.handle.seek(0); batch.handle.truncate(); batch.count = 0
            batch.flush('error', current_record, safe_error(error))
        raise
    finally:
        batch.close()


def sample(reader, args):
    rows = read_query(f'''WITH picked AS (
        SELECT rr.id,rr.research_session_id,rr.source_byte_offset,rr.source_record_bytes,rr.record_type,
        rs.source_relative_path,rs.file_size_bytes,
        row_number() OVER (PARTITION BY rr.record_type ORDER BY md5(rr.id::text || '{args.seed}')) AS n
        FROM public.research_session_records rr JOIN public.research_sessions rs ON rs.id=rr.research_session_id
        WHERE rr.source_record_bytes BETWEEN 1 AND 4194304)
        SELECT row_to_json(picked) FROM picked WHERE n <= CASE record_type
            WHEN 'token_usage_record' THEN 3 WHEN 'event_msg' THEN 6 WHEN 'compacted' THEN 3 ELSE 1 END
        ORDER BY record_type,n''')
    results, fields, scopes = [], Counter(), Counter()
    for row in rows:
        path = source_path(row['source_relative_path'])
        if path.stat().st_size != row['file_size_bytes']:
            raise ValueError('sample_source_size_changed')
        with path.open('rb') as handle:
            handle.seek(row['source_byte_offset'])
            count = reader.parse(handle, row['source_record_bytes'])
        if reader.root_type != row['record_type']:
            raise ValueError('sample_type_mismatch')
        paths = Counter()
        for value in reader.observations(row['id'],row['source_byte_offset']):
            scopes[value['usage_scope']] += 1
            for key, kind in value['metadata']['field_types'].items():
                fields[(key,kind)] += 1
            # Aggregate structural paths; identifier values are never printed.
            paths[re.sub(r'/[0-9]+(?=/|$)', '/[]', value['source_json_path'])] += 1
        results.append({'record_id':row['id'],'file_id':row['research_session_id'],
                        'month':row['source_relative_path'][:7],'type':row['record_type'],
                        'bytes':row['source_record_bytes'],'observations':count,'paths':dict(paths)})
    return {'mode':'sample','seed':args.seed,'sample_records':len(results),
            'bytes_read':sum(x['bytes'] for x in results),'samples':results,
            'scopes':dict(scopes),'field_types':[{'key':k,'type':t,'count':n} for (k,t),n in sorted(fields.items())],
            'database_written':False,'original_files_modified':False}


class RunLock:
    def __enter__(self):
        path = ROOT / 'data/.token_usage_scan.lock'
        self.handle = path.open('a+b')
        self.handle.seek(0,2)
        if self.handle.tell() == 0:
            self.handle.write(b'0'); self.handle.flush()
        self.handle.seek(0)
        try:
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(self.handle.fileno(),msvcrt.LK_NBLCK,1)
            else:
                import fcntl
                fcntl.flock(self.handle,fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.handle.close()
            raise RuntimeError('another_token_scanner_is_running') from None
        return self

    def __exit__(self, *args):
        self.handle.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--apply',action='store_true')
    mode.add_argument('--sample',action='store_true')
    parser.add_argument('--session-id',type=positive)
    parser.add_argument('--limit-files',type=positive)
    parser.add_argument('--max-records',type=positive)
    parser.add_argument('--batch-records',type=positive,default=1000)
    parser.add_argument('--pause-ms',type=int,default=50)
    parser.add_argument('--seed',type=int,default=20261025)
    args = parser.parse_args()
    if args.batch_records > 5000 or not 0 <= args.pause_ms <= 10000:
        parser.error('batch-records <= 5000 and pause-ms between 0 and 10000 required')
    started = time.monotonic()
    lower_priority()
    config = connection_settings()
    if config['DB_DATABASE'] != 'codex_session_research':
        parser.error('unexpected_database')
    if not args.apply and not args.sample:
        items = plan(args.session_id)
        print(json.dumps({'files':len(items),'source_bytes':sum(x['file_size_bytes'] for x in items),
                          'records':sum(x['record_count'] for x in items),
                          'statuses':dict(Counter((x.get('state') or {}).get('status','pending') for x in items)),
                          'database_written':False},indent=2))
        return 0
    OUT.mkdir(parents=True,exist_ok=True)
    (ROOT / '.tmp').mkdir(exist_ok=True)
    with RunLock(), tempfile.TemporaryDirectory(prefix='token-usage-',dir=ROOT / '.tmp') as temp:
        directory = Path(temp)
        reader = TokenReader(directory / 'record.sqlite3')
        try:
            if args.sample:
                report = sample(reader,args)
                report.update(peak_working_set_bytes=peak_memory(),elapsed_seconds=round(time.monotonic()-started,3))
                save(OUT / 'sample_report.json',report)
                print(json.dumps({k:v for k,v in report.items() if k not in ('samples','field_types')},indent=2))
                return 0
            items = plan(args.session_id)
            if args.session_id and not items:
                raise ValueError('source_session_not_found')
            processed = completed = 0
            files = 0
            try:
                for item in items:
                    if (item.get('state') or {}).get('status') == 'completed':
                        validate_state(item,source_path(item['source_relative_path']).stat())
                        continue
                    if args.limit_files and files >= args.limit_files:
                        break
                    budget = args.max_records-processed if args.max_records else None
                    if budget is not None and budget <= 0:
                        break
                    state,count = scan_file(item,args,reader,directory,budget)
                    processed += count
                    files += 1
                    completed += state['status'] == 'completed'
                    print(json.dumps({'file_id':item['id'],'status':state['status'],
                                      'records':state['scanned_record_count'],
                                      'observations':state['observation_count']}),flush=True)
                status = 'bounded_run_finished' if args.max_records or args.limit_files or args.session_id else 'completed'
            except KeyboardInterrupt:
                status = 'interrupted'
            except (OSError,ValueError,RuntimeError) as error:
                status = 'error'
                print(json.dumps({'status':status,'error_code':safe_error(error)}),flush=True)
            report = {'version':VERSION,'status':status,'completed_at':utc_now(),
                      'records_processed_this_invocation':processed,'files_processed':files,
                      'files_completed_this_invocation':completed,
                      'database_write_requested':True,
                      'database_written':bool(files) if status not in ('error','interrupted') else None,
                      'peak_working_set_bytes':peak_memory(),'elapsed_seconds':round(time.monotonic()-started,3),
                      'counter_sums_computed':False,'duplicate_flags_modified':False}
            save(OUT / 'last_run.json',report)
            print(json.dumps(report,indent=2))
            return 0 if status not in ('error','interrupted') else 1
        finally:
            reader.close()


if __name__ == '__main__':
    raise SystemExit(main())
