"""Audited candidate marking; never delete observations or calculate billed tokens.

Default prepares an immutable CSV plan. --apply commits resumable bounded batches;
--verify checks the entire plan without writes; --rollback restores the initially
unchecked state for this plan only. Does not open any source JSONL/sample files.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import uuid

from import_session_survey import connection_settings, find_psql
from index_session_records import ROOT, lower_priority, utc_now, run_psql
from index_token_usage import read_query, execute_file, RunLock
from profile_response_items import peak_memory
from repair_null_record_types import save
from token_usage_stream import COUNTERS, encoded

VERSION = '1.0.0'
OUT = ROOT / 'data/reports/token_usage_duplicate_candidates_v1'
OBS = 'public.research_token_usage_observations'
RECORDS = 'public.research_session_records'


def sane(alias):
    checks = [f'{alias}.{key} IS NOT NULL AND {alias}.{key}>=0' for key in COUNTERS]
    checks += [f"{alias}.metadata->'issues'='[]'::jsonb",
               f'{alias}.input_tokens::numeric+{alias}.output_tokens::numeric={alias}.total_tokens',
               f'{alias}.cached_input_tokens::numeric+{alias}.cache_write_input_tokens::numeric<={alias}.input_tokens',
               f'{alias}.reasoning_output_tokens<={alias}.output_tokens',f'{alias}.total_tokens>0']
    return ' AND '.join(checks)


def fingerprint(alias='o'):
    return f"md5(((to_jsonb({alias})-ARRAY['is_possible_duplicate','duplicate_checked_at','metadata']) || jsonb_build_object('metadata',{alias}.metadata-'duplicate_check'))::text)"


def policy_sql(observations=OBS, records=RECORDS):
    if observations not in (OBS,'fixture_obs') or records not in (RECORDS,'fixture_records'):
        raise ValueError('unsupported_relation')
    values = lambda alias: ','.join(f'{alias}.{key}' for key in COUNTERS)
    return f'''WITH identified AS (
 SELECT o.id,'response_identity'::text AS rule,o.usage_scope,NULL::bigint AS companion_id,
 jsonb_build_array(o.usage_scope,o.response_id,o.session_id,o.thread_id,o.turn_id,o.root_turn_id,{values('o')}) AS key
 FROM {observations} o JOIN {records} rr ON rr.id=o.research_session_record_id
 WHERE o.response_id IS NOT NULL AND o.usage_scope IN ('response','turn','thread')
 AND {sane('o')}
 AND ((rr.record_type='token_usage_record' AND o.source_json_path='/payload/' ||
      CASE o.usage_scope WHEN 'response' THEN 'usage' WHEN 'turn' THEN 'turn_token_usage' ELSE 'thread_token_usage' END)
   OR (rr.record_type='compacted' AND o.source_json_path='/payload/latest_token_usage_record/' ||
      CASE o.usage_scope WHEN 'response' THEN 'usage' WHEN 'turn' THEN 'turn_token_usage' ELSE 'thread_token_usage' END))
), companions AS (
 SELECT c.*,count(*) OVER (PARTITION BY research_session_record_id) AS companion_count
 FROM {observations} c WHERE source_json_path='/payload/info/total_token_usage'
), last_paired AS (
 SELECT o.id,'paired_last_snapshot'::text AS rule,o.usage_scope,c.id AS companion_id,
 jsonb_build_array(o.usage_scope,{values('o')},{values('c')}) AS key
 FROM {observations} o JOIN {records} rr ON rr.id=o.research_session_record_id
 JOIN companions c ON c.research_session_record_id=o.research_session_record_id
 WHERE rr.record_type='event_msg' AND o.usage_scope='last'
 AND o.source_json_path='/payload/info/last_token_usage'
 AND o.response_id IS NULL AND o.session_id IS NULL AND o.thread_id IS NULL
 AND o.turn_id IS NULL AND o.root_turn_id IS NULL
 AND c.usage_scope='cumulative' AND c.companion_count=1
 AND {sane('o')} AND {sane('c')}
), eligible AS (
 SELECT * FROM identified UNION ALL SELECT * FROM last_paired
), ranked AS (
 SELECT id,rule,usage_scope,companion_id,
 min(id) OVER (PARTITION BY rule,key) AS reference_id,
 count(*) OVER (PARTITION BY rule,key) AS group_size FROM eligible
)
SELECT r.id,{fingerprint()} AS fingerprint,r.rule,r.usage_scope,r.reference_id,r.group_size,
 r.companion_id
FROM ranked r JOIN {observations} o ON o.id=r.id
WHERE r.group_size>1 ORDER BY r.id'''


def table_summary():
    return read_query(f'''SELECT jsonb_build_object('rows',count(*),
      'flagged',count(*) FILTER(WHERE is_possible_duplicate),
      'checked',count(*) FILTER(WHERE duplicate_checked_at IS NOT NULL),
      'audit_keys',count(*) FILTER(WHERE metadata ? 'duplicate_check'),
      'unfinished_files',(SELECT count(*) FROM public.research_sessions rs
        LEFT JOIN public.research_token_usage_scan_state st ON st.research_session_id=rs.id
        WHERE st.status IS DISTINCT FROM 'completed'),
      'states_observations',(SELECT sum(observation_count) FROM public.research_token_usage_scan_state))
      FROM {OBS}''')[0]


def digest_file(path):
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda:handle.read(256*1024),b''):
            digest.update(block)
    return digest.hexdigest()


def export_plan(path):
    c = connection_settings()
    if c['DB_DATABASE'] != 'codex_session_research':
        raise RuntimeError('unexpected_database')
    env = os.environ.copy()
    env.update(PGPASSWORD=c['DB_PASSWORD'],PGCONNECT_TIMEOUT='5',PGCLIENTENCODING='UTF8')
    command=[find_psql(),'-X','-w','-q','-A','-t','-v','ON_ERROR_STOP=1','-v','VERBOSITY=sqlstate',
             '-h',c['DB_HOST'],'-p',c['DB_PORT'],'-U',c['DB_USERNAME'],'-d',c['DB_DATABASE']]
    sql="BEGIN READ ONLY; SET LOCAL statement_timeout='300s'; SET LOCAL work_mem='8MB'; SET LOCAL max_parallel_workers_per_gather=0;\n"
    sql+='COPY ('+policy_sql()+') TO STDOUT WITH (FORMAT csv, HEADER true);\nCOMMIT;'
    temporary=path.with_suffix('.part')
    with temporary.open('wb') as handle:
        result=subprocess.run(command,input=sql.encode(),stdout=handle,stderr=subprocess.PIPE,env=env,timeout=330)
    if result.returncode:
        raise RuntimeError('plan_export_failed')
    temporary.replace(path)


def plan_rows(path):
    previous=0
    with path.open(encoding='utf-8',newline='') as handle:
        for row in csv.DictReader(handle):
            for key in ('id','reference_id','group_size'):
                row[key]=int(row[key])
            row['companion_id']=int(row['companion_id']) if row['companion_id'] else None
            if row['id']<=previous or not 0<row['reference_id']<=row['id'] or row['group_size']<2:
                raise ValueError('invalid_plan_order_or_reference')
            if row['rule'] not in ('response_identity','paired_last_snapshot'):
                raise ValueError('unknown_matching_rule')
            previous=row['id']
            yield row


def summarize_plan(path):
    totals=Counter(); by_rule={}; by_scope={}
    for row in plan_rows(path):
        group='reference' if row['id']==row['reference_id'] else 'candidate'
        totals[group]+=1; totals['rows']+=1
        by_rule.setdefault(row['rule'],Counter())[group]+=1
        by_scope.setdefault(row['usage_scope'],Counter())[group]+=1
    return {'rows':totals['rows'],'candidate_rows':totals['candidate'],'groups':totals['reference'],
            'reference_rows':totals['reference'],'by_rule':by_rule,'by_scope':by_scope}


def patch_for(row, run_id):
    return {'version':VERSION,'run_id':run_id,'rule':row['rule'],
            'group_id':row['rule']+':'+str(row['reference_id']),
            'reference_observation_id':row['reference_id'],'group_size':row['group_size'],
            'classification':'reference' if row['id']==row['reference_id'] else 'possible_duplicate',
            'confidence':'identity_supported' if row['rule']=='response_identity' else 'snapshot_only',
            'companion_observation_id':row['companion_id']}


def write_batch(path, rows, run_id, mode):
    if mode not in ('apply','verify','rollback'):
        raise ValueError('unsupported_mode')
    with path.open('w',encoding='utf-8',newline='') as handle:
        handle.write("BEGIN;\nSET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='120s';\n"
                     "SET LOCAL work_mem='8MB'; SET LOCAL max_parallel_workers_per_gather=0;\n"
                     "SELECT pg_advisory_xact_lock(20261025,2);\n"
                     "CREATE TEMP TABLE duplicate_stage (id bigint PRIMARY KEY,fingerprint text,flag boolean,patch jsonb);\n"
                     "COPY duplicate_stage FROM STDIN WITH (FORMAT csv);\n")
        writer=csv.writer(handle,lineterminator='\n')
        for row in rows:
            writer.writerow([row['id'],row['fingerprint'],row['id']!=row['reference_id'],encoded(patch_for(row,run_id))])
        handle.write(f'''\\.
CREATE TEMP TABLE duplicate_locked AS
SELECT o.id FROM {OBS} o JOIN duplicate_stage s ON s.id=o.id ORDER BY o.id
{'FOR UPDATE OF o' if mode != 'verify' else ''};
DO $$ BEGIN
IF (SELECT count(*) FROM duplicate_locked) <> (SELECT count(*) FROM duplicate_stage)
THEN RAISE EXCEPTION 'Missing planned observations'; END IF;
IF EXISTS (SELECT 1 FROM duplicate_stage s JOIN {OBS} o ON o.id=s.id
 WHERE {fingerprint()}<>s.fingerprint)
THEN RAISE EXCEPTION 'Observation evidence changed'; END IF;
''')
        untouched="(NOT o.is_possible_duplicate AND o.duplicate_checked_at IS NULL AND NOT(o.metadata ? 'duplicate_check'))"
        marked="(o.is_possible_duplicate=s.flag AND o.duplicate_checked_at IS NOT NULL AND o.metadata->'duplicate_check'=s.patch)"
        valid=marked if mode=='verify' else f'({untouched} OR {marked})'
        handle.write(f'''IF EXISTS (SELECT 1 FROM duplicate_stage s JOIN {OBS} o ON o.id=s.id
 WHERE NOT COALESCE({valid},false))
THEN RAISE EXCEPTION 'Conflicting duplicate review'; END IF;
END $$;
''')
        if mode=='apply':
            handle.write(f'''UPDATE {OBS} o SET is_possible_duplicate=s.flag,
duplicate_checked_at=CURRENT_TIMESTAMP,
metadata=jsonb_set(o.metadata,'{{duplicate_check}}',s.patch,true)
FROM duplicate_stage s WHERE o.id=s.id AND {untouched};
''')
        elif mode=='rollback':
            handle.write(f'''UPDATE {OBS} o SET is_possible_duplicate=false,
duplicate_checked_at=NULL,metadata=o.metadata-'duplicate_check'
FROM duplicate_stage s WHERE o.id=s.id AND {marked};
''')
        handle.write('COMMIT;\n' if mode!='verify' else 'ROLLBACK;\n')


def markdown(report):
    p=report['plan']; lines=['# Token-usage duplicate candidates','',
        f"Status: {report['status']}. Rule version: {VERSION}. Run: `{report['run_id']}`.",'',
        '| Metric | Rows/groups |','|---|---:|',
        f"| Original observations | {report['before']['rows']:,} |",
        f"| Candidate groups | {p['groups']:,} |",
        f"| Reference rows | {p['reference_rows']:,} |",
        f"| Possible duplicate rows | {p['candidate_rows']:,} |",
        f"| Outside matching groups | {report['before']['rows']-p['rows']:,} |",'',
        '## Rules and limits','',
        '- Separate usage scopes. All six counters must be present, nonnegative, arithmetically consistent and have a positive total; no extractor issues.',
        '- response_identity: known standalone/compacted usage paths, same response ID, scope, six counters and exact session/thread/turn/root-turn identifier tuple. Missing and present identifiers are not merged.',
        '- paired_last_snapshot: event last usage without any of the five identifiers; identical six last counters AND six counters of one unambiguous cumulative companion in the same source record.',
        '- Minimum observation ID is a deterministic reference, not proof of original chronology. All other group members are candidates.',
        '- Cumulative companion rows are evidence only; this rule does not mark them. No cross-scope deduplication.',
        '- No rows removed, no token totals/prices computed, no source files opened. Unmarked is not certified unique.',
        '- Only is_possible_duplicate, duplicate_checked_at and metadata.duplicate_check are modified. CSV plan records IDs and immutable-evidence hashes; the precondition was false/null/absent for all prior review fields.',
        '- Plan SHA-256 and per-row evidence hashes protect restart/verification. Repeat runs preserve timestamps; conflicts abort the current batch.',
        '- Rollback is available only for this run and verifies the same plan/evidence before restoring the initial review fields.', '',
        '## Per rule','', '| Rule | Candidates | References |','|---|---:|---:|']
    for rule,c in p['by_rule'].items():
        lines.append(f"| {rule} | {c.get('candidate',0):,} | {c.get('reference',0):,} |")
    return '\n'.join(lines)+'\n'


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    modes=parser.add_mutually_exclusive_group()
    for name in ('apply','verify','rollback'):
        modes.add_argument('--'+name,action='store_true')
    parser.add_argument('--batch-rows',type=int,default=2000)
    args=parser.parse_args()
    if not 1<=args.batch_rows<=5000:
        parser.error('batch-rows must be between 1 and 5000')
    lower_priority(); started=time.monotonic()
    OUT.mkdir(parents=True,exist_ok=True)
    plan_path=OUT/'plan.csv'; report_path=OUT/'summary.json'
    with RunLock():
        if report_path.exists():
            report=json.loads(report_path.read_text(encoding='utf-8'))
            if report['version']!=VERSION or digest_file(plan_path)!=report['plan_sha256']:
                raise RuntimeError('existing_plan_changed')
        else:
            before=table_summary()
            if before['unfinished_files'] or before['rows']!=before['states_observations']:
                raise RuntimeError('extraction_incomplete')
            if before['flagged'] or before['checked'] or before['audit_keys']:
                raise RuntimeError('preexisting_review_requires_separate_plan')
            export_plan(plan_path)
            report={'version':VERSION,'run_id':str(uuid.uuid4()),'prepared_at':utc_now(),
                    'before':before,'plan':summarize_plan(plan_path),'plan_sha256':digest_file(plan_path),
                    'status':'prepared','last_applied_id':0}
            save(report_path,report)
        mode='apply' if args.apply else 'verify' if args.verify else 'rollback' if args.rollback else 'plan'
        if mode!='plan':
            current=table_summary()
            if current['rows']!=report['before']['rows'] or current['unfinished_files']:
                raise RuntimeError('dataset_changed')
            path=OUT/'current_batch.sql'; batch=[]; traversed=0
            try:
                for row in plan_rows(plan_path):
                    batch.append(row)
                    if len(batch)<args.batch_rows:
                        continue
                    write_batch(path,batch,report['run_id'],mode); execute_file(path)
                    traversed+=len(batch)
                    if mode=='apply':
                        report['last_applied_id']=batch[-1]['id']; report['status']='applying'
                        save(report_path,report)
                    if traversed % 20000==0:
                        print(json.dumps({'mode':mode,'plan_rows_checked':traversed,'total_plan_rows':report['plan']['rows']}),flush=True)
                    batch.clear(); time.sleep(0.05)
                if batch:
                    write_batch(path,batch,report['run_id'],mode); execute_file(path)
                    traversed+=len(batch)
                report['status']={'apply':'applied','verify':'verified','rollback':'rolled_back'}[mode]
                report['after']=table_summary()
                p=report['plan']; a=report['after']
                if mode in ('apply','verify') and (a['flagged']!=p['candidate_rows'] or a['checked']!=p['rows'] or a['audit_keys']!=p['rows']):
                    raise RuntimeError('aggregate_verification_failed')
                if mode=='rollback' and (a['flagged'] or a['checked'] or a['audit_keys']):
                    raise RuntimeError('rollback_verification_failed')
                report['completed_at']=utc_now()
            except BaseException:
                report['status']=mode+'_interrupted_or_failed'; save(report_path,report)
                raise
            finally:
                path.unlink(missing_ok=True)
        report['last_mode']=mode
        report['elapsed_seconds']=round(time.monotonic()-started,3)
        report['peak_working_set_bytes']=peak_memory()
        save(report_path,report)
        (OUT/'report.md').write_text(markdown(report),encoding='utf-8')
        print(json.dumps({key:report[key] for key in ('status','before','plan','elapsed_seconds','peak_working_set_bytes')},indent=2))


if __name__=='__main__':
    main()
