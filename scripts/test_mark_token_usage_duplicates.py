"""Exercise real PostgreSQL matching/update semantics on rollback-only temp tables."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import mark_token_usage_duplicates as m
from index_session_records import json_rows,run_psql


def fixtures():
    values=(100,80,0,5,2,105)
    cumulative=(1000,800,0,50,20,1050)
    rows=[]; records={}
    def add(i,scope='response',counters=values,record=None,record_type='token_usage_record',**overrides):
        record=record or i
        path={'response':'/payload/usage','turn':'/payload/turn_token_usage','thread':'/payload/thread_token_usage',
              'last':'/payload/info/last_token_usage','cumulative':'/payload/info/total_token_usage'}[scope]
        row=dict(id=i,research_session_record_id=record,source_json_path=path,
                 source_object_byte_offset=i*100,source_object_bytes=10,usage_scope=scope,
                 response_id='resp_a',session_id='session_a',thread_id='thread_a',turn_id='turn_a',root_turn_id='root_a',
                 is_possible_duplicate=False,duplicate_checked_at=None,
                 metadata={'issues':[],'untouched':'test'},extracted_at='2026-10-25T00:00:00Z')
        if scope in ('last','cumulative'):
            row.update({key:None for key in ('response_id','session_id','thread_id','turn_id','root_turn_id')})
            record_type='event_msg'
        if record_type=='compacted':
            row['source_json_path']=path.replace('/payload/','/payload/latest_token_usage_record/')
        row.update(zip(m.COUNTERS,counters)); row.update(overrides)
        rows.append(row); records[record]=record_type
    add(1);add(2)
    add(3,scope='turn')
    add(4,response_id='resp_other')
    add(5,thread_id='conflicting_thread')
    for i in (6,7):add(i,metadata={'issues':['invalid']})
    for i in (8,9):add(i,cache_write_input_tokens=None)
    for i in (10,11):add(i,counters=(0,0,0,0,0,0))
    for i in (12,13):add(i,source_json_path='/payload/arbitrary/usage')
    add(14,record_type='compacted');add(15,record_type='compacted')
    add(16,session_id=None)
    for i in (20,22,24,26,29,31,33):
        add(i,scope='last',record=1000+i,session_id='present_id' if i==29 else None)
        add(i+100,scope='cumulative',record=1000+i,
            counters=(2000,1800,0,50,20,2050) if i==24 else cumulative,
            cache_write_input_tokens=None if i in (31,33) else 0)
    add(27,scope='cumulative',record=1026,counters=cumulative)
    add(40,scope='thread');add(41,scope='thread')
    add(42,root_turn_id='other_root');add(43,root_turn_id='another_root')
    for i in (44,45):add(i,total_tokens=999)
    for i in (46,47):add(i,cache_write_input_tokens=30)
    for i in (48,49):add(i,reasoning_output_tokens=6)
    for i in (50,51):add(i,metadata={})
    data=json.dumps(rows).replace("'","''")
    record_sql=','.join(f"({i},'{kind}')" for i,kind in records.items())
    return f'''BEGIN;
CREATE TEMP TABLE fixture_obs AS SELECT * FROM {m.OBS} WITH NO DATA;
CREATE TEMP TABLE fixture_records (id bigint PRIMARY KEY,record_type text);
INSERT INTO fixture_obs SELECT * FROM json_populate_recordset(NULL::fixture_obs,'{data}');
INSERT INTO fixture_records VALUES {record_sql};
'''


HAS_DB_CONFIG = (Path(__file__).resolve().parents[1] / '.env').exists()


@unittest.skipUnless(HAS_DB_CONFIG, 'requires a local PostgreSQL configuration in .env')
class DatabasePolicyTests(unittest.TestCase):
    def test_matching_boundaries_against_real_postgresql(self):
        query=m.policy_sql('fixture_obs','fixture_records')
        result=json_rows(fixtures()+f"SELECT jsonb_build_object('matches',(SELECT jsonb_agg(q) FROM ({query}) q)); ROLLBACK;")[0]
        found={r['id']:r for r in result['matches']}
        self.assertEqual(set(found),{1,2,14,15,20,22,40,41})
        self.assertTrue(all(found[i]['reference_id']==1 and found[i]['group_size']==4 for i in (1,2,14,15)))
        self.assertEqual(found[20]['companion_id'],120)
        self.assertEqual(found[22]['reference_id'],20)
        self.assertEqual(found[40]['usage_scope'],'thread')

    def batch(self,directory,mode):
        rows=[{'id':i,'fingerprint':'0'*32,'reference_id':1,'rule':'response_identity',
               'usage_scope':'response','group_size':4,'companion_id':None} for i in (1,2)]
        path=directory/'batch.sql'
        with patch.object(m,'OBS','fixture_obs'):
            m.write_batch(path,rows,'fixture-run',mode)
        body=path.read_text().removeprefix('BEGIN;\n')
        body=body.rsplit('COMMIT;',1)[0] if mode!='verify' else body.rsplit('ROLLBACK;',1)[0]
        return body.replace('CREATE TEMP TABLE duplicate_locked AS',
            'UPDATE duplicate_stage s SET fingerprint='+m.fingerprint()+' FROM fixture_obs o WHERE o.id=s.id;\n'
            'CREATE TEMP TABLE duplicate_locked AS')

    def test_apply_resume_verify_and_rollback_preserve_evidence(self):
        with tempfile.TemporaryDirectory(dir=m.ROOT/'.tmp') as name:
            directory=Path(name)
            apply=self.batch(directory,'apply')
            verify=self.batch(directory,'verify')
            rollback=self.batch(directory,'rollback')
        check='''DO $$ BEGIN
IF (SELECT count(*) FROM fixture_obs WHERE is_possible_duplicate)<>1
 OR (SELECT count(*) FROM fixture_obs WHERE duplicate_checked_at IS NOT NULL)<>2
 OR EXISTS (SELECT 1 FROM fixture_obs WHERE id IN(1,2) AND (input_tokens<>100 OR metadata->>'untouched'<>'test'))
THEN RAISE EXCEPTION 'Unexpected marking or data mutation'; END IF;
END $$;'''
        drop='DROP TABLE duplicate_stage,duplicate_locked;\n'
        sql=fixtures()+apply+check+drop
        sql+="UPDATE fixture_obs SET duplicate_checked_at='2020-01-01T00:00:00Z' WHERE id IN(1,2);\n"
        sql+=apply+check+"DO $$ BEGIN IF EXISTS(SELECT 1 FROM fixture_obs WHERE id IN(1,2) AND duplicate_checked_at<>'2020-01-01T00:00:00Z'::timestamptz) THEN RAISE EXCEPTION 'Resume changed timestamp'; END IF; END $$;"+drop
        sql+=verify+drop+rollback
        sql+="SELECT jsonb_build_object('restored',count(*) FILTER(WHERE id IN(1,2) AND NOT is_possible_duplicate AND duplicate_checked_at IS NULL AND NOT(metadata ? 'duplicate_check'))) FROM fixture_obs; ROLLBACK;"
        # Advisory-lock SELECT emits blank lines only; json_rows ignores them.
        result=json_rows(sql)
        self.assertEqual(result[0]['restored'],2)

    def test_conflicting_review_aborts_before_update(self):
        with tempfile.TemporaryDirectory(dir=m.ROOT/'.tmp') as name:
            sql=self.batch(Path(name),'apply')
        with self.assertRaises(RuntimeError):
            run_psql(fixtures()+"UPDATE fixture_obs SET is_possible_duplicate=true WHERE id=1;\n"+sql+'ROLLBACK;')

    def test_changed_evidence_aborts_before_update(self):
        with tempfile.TemporaryDirectory(dir=m.ROOT/'.tmp') as name:
            sql=self.batch(Path(name),'apply')
        sql=sql.replace('CREATE TEMP TABLE duplicate_locked AS',
                        'UPDATE fixture_obs SET input_tokens=999 WHERE id=2;\nCREATE TEMP TABLE duplicate_locked AS')
        with self.assertRaises(RuntimeError):
            run_psql(fixtures()+sql+'ROLLBACK;')


if __name__=='__main__':
    unittest.main()
