"""Synthetic-only tests: no research source, PostgreSQL or image corpus reads."""
import base64
from contextlib import redirect_stdout
import hashlib
import io
import json
from pathlib import Path
import re
import sqlite3
import time
import unittest
from unittest.mock import patch

import analyze_logs_request_capture as a

THREAD='00000000-0000-4000-8000-000000000001'
OTHER='00000000-0000-4000-8000-000000000002'
SECRET='PRIVATE_SYNTHETIC_CONVERSATION_CONTENT'
PATH='C:\\private-fixture\\secret.txt'
ACCOUNT='chatgpt:account-private-fixture:user-private-fixture'
COMMAND='synthetic-secret-shell-command'


def observation(obj, number=1, thread=THREAD, prefix='trace{request: x}: event: '):
    body=prefix+json.dumps(obj,ensure_ascii=False,separators=(',',':'))
    return a.observe({'rowid':number,'thread_id':thread,'ts':number,'ts_nanos':0,'research_session_id':12},body,'a'*64)


def request(number=1,**kw):
    obj={'type':'response.create','model':'gpt-test','input':[],'tools':[],**kw}
    return observation(obj,number)


def response(number=2,rid='resp_test',**kw):
    return observation({'type':'response.completed','response':{'id':rid,**kw}},number)


class CaptureTests(unittest.TestCase):
    def test_exact_utf8_spans_and_escaped_strings(self):
        item={'type':'message','role':'user','content':[{'type':'input_text','text':'عربي " \\ نهاية'}]}
        tools=[{'type':'function','name':'test','description':'نص'}]
        row=request(input=[item],tools=tools)
        self.assertEqual(row['input_items'][0]['logged_json_bytes'],len(json.dumps(item,ensure_ascii=False,separators=(',',':')).encode()))
        self.assertEqual(row['tools_bytes'],len(json.dumps(tools,ensure_ascii=False,separators=(',',':')).encode()))
        self.assertEqual(row['captured_json_bytes'],row['json_end_byte']-row['json_start_byte'])

    def test_structural_export_and_psql_stream_hide_sensitive_values(self):
        binary=b'synthetic-image-private-bytes'
        url='data:image/png;base64,'+base64.b64encode(binary).decode()
        row=request(input=[{'type':'message','role':'user','content':[
            {'type':'input_text','text':SECRET+PATH+ACCOUNT+COMMAND},
            {'type':'input_image','image_url':url}]}],
            instructions=SECRET,client_metadata={'host_id':ACCOUNT},
            tools=[{'type':'function','name':COMMAND,'description':SECRET}],
            **{PATH:SECRET})
        with patch.object(a,'run_psql',return_value='{"rows":1,"inserted_rows":1}') as psql:
            a.persist([row])
        exported=a.encoded(row)+psql.call_args.args[0]
        for secret in (SECRET,PATH,ACCOUNT,COMMAND,url,base64.b64encode(binary).decode()):
            self.assertNotIn(secret,exported)
        self.assertIn('key_sha256:',exported)
        self.assertEqual(row['images'][0]['sha256'],hashlib.sha256(binary).hexdigest())

    def test_parse_failures_do_not_promote_nested_json(self):
        for body,status in (
            ('event: {"type":"response.create","input":[','suspected_truncated_json'),
            ('event: {"type":"response.create","input":"unfinished','suspected_truncated_json'),
            ('event: {"type":"response.create",BAD,{"model":"gpt-test","input":[]}}','invalid_json'),
            ('event: {"type":"a","type":"b"}','invalid_json'),
            ('event: {"type":"response.create","input":NaN}','invalid_json')):
            with self.subTest(status=status):
                obj,_,_,actual=a.extract_protocol(body)
                self.assertIsNone(obj); self.assertEqual(actual,status)

    def test_truncation_markers_separate_from_plain_mentions(self):
        row=request(input=[{'type':'input_text','text':'This instruction mentions truncated and omitted.'}])
        self.assertEqual(row['truncation_evidence'],{'external':[],'embedded':[]})
        row=request(input=[{'type':'input_text','text':'…4228 chars truncated…'}])
        self.assertEqual(row['capture_status'],'complete_json')
        self.assertEqual(row['truncation_evidence']['embedded'][0]['count'],1)
        self.assertFalse(row['truncation_evidence']['external'])
        row=observation({'model':'gpt-test','input':[]},prefix='[output truncated] ')
        self.assertEqual(len(row['truncation_evidence']['external']),1)

    def test_request_response_and_item_classification(self):
        self.assertEqual(request()['classification'],'request')
        self.assertEqual(observation({'model':'gpt-test','input':[]})['classification'],'request')
        self.assertEqual(response()['classification'],'response')
        row=observation({'type':'response.custom_tool_call_input.done','input':SECRET})
        self.assertEqual(row['classification'],'other')
        self.assertEqual(observation({'type':{},'input':[]})['classification'],'other')

    def test_counter_aliases_missing_zero_and_invalid_values(self):
        values,meta=a.counter_values({'usage':{'input_tokens':10,'output_tokens':2,'total_tokens':12,
            'input_tokens_details':{'cached_tokens':0},'output_tokens_details':{'reasoning_tokens':1}}})
        self.assertEqual(values['cached_input_tokens'],0)
        self.assertIsNone(values['cache_write_input_tokens'])
        self.assertEqual(meta['missing_fields'],['cache_write_input_tokens'])
        values,meta=a.counter_values({'usage':{'cached_input_tokens':3,'input_tokens_details':{'cached_tokens':4},'input_tokens':True}})
        self.assertIsNone(values['cached_input_tokens']); self.assertIsNone(values['input_tokens'])
        self.assertIn('cached_input_tokens:conflicting_aliases',meta['issues'])

    def test_previous_id_is_backward_not_current_link(self):
        past=response(1,'resp_old'); req=request(2,previous_response_id='resp_old'); current=response(3,'resp_new')
        a.link_requests([past,req,current])
        self.assertEqual(req['linked_response_id'],'resp_new')
        self.assertEqual(req['link_method'],'order_inferred')

    def test_explicit_current_link(self):
        req=request(1,request_id='req_test')
        rep=response(2,request_id='req_test')
        a.link_requests([req,rep]); self.assertEqual(req['link_method'],'explicit_id')

    def test_intervening_request_and_cross_thread_rejected(self):
        first=request(1); second=request(2); rep=response(3)
        a.link_requests([first,second,rep]); self.assertIsNone(first['link_method'])
        self.assertEqual(second['link_method'],'order_inferred')
        first=request(1); rep['thread_id']=OTHER
        a.link_requests([first,rep]); self.assertIsNone(first['link_method'])

    def test_lifecycle_uses_terminal_without_summing(self):
        req=request(1)
        created=observation({'type':'response.created','response':{'id':'resp_test','usage':None}},2)
        completed=response(3,usage={'input_tokens':10,'output_tokens':2,'total_tokens':12})
        a.link_requests([req,created,completed]); self.assertEqual(req['linked_response_source_rowid'],3)
        self.assertEqual(completed['total_tokens'],12)

    def test_partial_equality_is_not_six_field_equality_or_cross_scope(self):
        rep=response(2,usage={'input_tokens':10,'output_tokens':2,'total_tokens':12,
             'input_tokens_details':{'cached_tokens':0},'output_tokens_details':{'reasoning_tokens':1}})
        evidence={'response_id':'resp_test','research_session_id':12,'usage_scope':'response','observation_count':3,
                  **{k:rep[k] for k in a.COUNTERS}}
        evidence['cache_write_input_tokens']=0
        a.compare_tokens([rep],[evidence]); c=rep['token_comparison']['comparisons'][0]
        self.assertFalse(c['all_six_equal']); self.assertEqual(len(c['compared_fields']),5)
        self.assertEqual(c['differing_fields'],[])
        evidence['usage_scope']='thread'; a.compare_tokens([rep],[evidence])
        self.assertFalse(rep['token_comparison']['comparisons'][0]['response_scope'])

    def test_image_digest_match_and_external_image_not_fetched(self):
        binary=b'synthetic-image-bytes'
        url='data:image/png;base64,'+base64.b64encode(binary).decode()
        row=request(input=[{'type':'input_image','image_url':url},
                           {'type':'input_image','image_url':'https://private.invalid/private'}])
        with sqlite3.connect(':memory:') as con:
            con.execute('CREATE TABLE occurrences (session_id,record_id,sha256,decoded_bytes,error)')
            con.execute('INSERT INTO occurrences VALUES (12,1,?,?,NULL)',(hashlib.sha256(binary).digest(),len(binary)))
            a.match_images([row],con)
        self.assertEqual(row['images'][0]['index_match']['same_session_occurrences'],1)
        self.assertTrue(row['images'][0]['index_match']['same_size'])
        self.assertEqual(row['images'][1]['decode_error'],'external_image_not_fetched')
        self.assertNotIn('https://',a.encoded(row))

    def test_invalid_base64_has_no_digest(self):
        row=request(input=[{'type':'input_image','image_url':'data:image/png;base64,!!!!'}])
        self.assertIsNone(row['images'][0]['sha256'])
        self.assertIsNotNone(row['images'][0]['decode_error'])

    def test_numerical_candidates_never_confirm_identity(self):
        rep=response(2,usage={'input_tokens':10,'output_tokens':2,'total_tokens':12})
        with patch.object(a,'read_pg',return_value=[{'observation_count':2,'distinct_record_count':2,'observations_with_response_id':0}]) as query:
            a.compare_available_counters([rep])
        candidate=rep['token_comparison']['same_file_last_counter_candidates']
        self.assertFalse(candidate['identity_confirmed'])
        self.assertIn("u.usage_scope='last'",query.call_args.args[0])
        self.assertIn('r.research_session_id=12',query.call_args.args[0])
        self.assertNotIn('cache_write_input_tokens=',query.call_args.args[0])
        self.assertIsNone(rep['link_method'])

    def test_body_bound_fails_explicitly(self):
        with patch.object(a,'MAX_BODY',10):
            with self.assertRaisesRegex(ValueError,'log_body_bound_exceeded'): request()

    def test_migration_has_all_persisted_columns(self):
        migration=(a.ROOT/'database/migrations/006_create_logs_request_observations.sql').read_text()
        for column in request(): self.assertRegex(migration,r'(?m)^    '+re.escape(column)+r'\s')
        self.assertIn('UNIQUE (source_database, source_rowid)',migration)


if __name__=='__main__':
    a.common.lower_priority(); start=time.perf_counter()
    result=unittest.TextTestRunner(verbosity=1).run(unittest.defaultTestLoader.loadTestsFromTestCase(CaptureTests))
    print(a.encoded({'tests':result.testsRun,'passed':result.wasSuccessful(),
        'elapsed_seconds':round(time.perf_counter()-start,3),'peak_working_set_bytes':a.common.peak_memory()}))
    raise SystemExit(0 if result.wasSuccessful() else 1)
