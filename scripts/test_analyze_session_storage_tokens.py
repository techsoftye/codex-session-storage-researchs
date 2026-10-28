import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import analyze_session_storage_tokens as study


def usage(n):
    return dict(input_tokens=10*n,cached_input_tokens=5*n,cache_write_input_tokens=0,
                output_tokens=2*n,reasoning_output_tokens=n,total_tokens=12*n)


def event(total,last=None):
    return {'type':'event_msg','payload':{'type':'token_count','info':{
        'total_token_usage':total,'last_token_usage':last}}}


class LimitedRead(io.BytesIO):
    def read(self,size=-1):
        if size<0 or size>4096: raise AssertionError('unbounded read')
        return super().read(size)


class AccountingTests(unittest.TestCase):
    def test_exact_repeat_reset_and_mismatch(self):
        self.assertEqual(study.compare(usage(1),usage(3),usage(2))[0],'increment_equals_last_usage')
        self.assertEqual(study.compare(usage(3),usage(3),usage(2))[0],'unchanged_cumulative_snapshot')
        self.assertEqual(study.compare(usage(3),usage(1),usage(1))[0],'counter_reset_or_scope_change')
        self.assertEqual(study.compare(usage(1),usage(3),usage(1))[0],'increment_does_not_equal_last_usage')

    def test_missing_counters_not_silently_zero_or_ignored(self):
        last=usage(2); last['cache_write_input_tokens']=None
        self.assertEqual(study.compare(usage(1),usage(3),last)[0],'counter_field_set_changed')
        partial=[]
        for n in (1,3,2):
            values=usage(n); values['cache_write_input_tokens']=None; partial.append(values)
        status,delta=study.compare(*partial)
        self.assertEqual(status,'increment_equals_last_usage')
        self.assertIsNone(delta['cache_write_input_tokens'])
        self.assertEqual(study.compare(usage(1),None,usage(1))[0],'invalid_or_missing_counters')


class SourceStudyTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(dir=study.ROOT/'.tmp')
        self.path=Path(self.temp.name)
        self.reader=study.EvidenceReader(self.path/'scratch.sqlite3')

    def tearDown(self):
        self.reader.close(); self.temp.cleanup()

    def analyze(self,records):
        raw=b'\r\n'.join(json.dumps(r,separators=(',',':')).encode() for r in records)+b'\r\n\r\n'
        source=self.path/'fixture.jsonl'; source.write_bytes(raw)
        with patch.object(study,'safe_source_path',return_value=source):
            result=study.analyze_file('synthetic.jsonl',self.reader,self.path,10*1048576)
            validation=study.crosscheck_file(result)
        self.assertEqual(validation['status'],'passed')
        evidence=(study.ROOT/result['evidence_jsonl']).read_text()
        self.assertEqual(result['source_sha256'],hashlib.sha256(raw).hexdigest())
        self.assertEqual(sum(t['bytes'] for t in result['record_types'])+result['blank_line_bytes'],len(raw))
        return result,evidence

    def test_long_strings_are_bounded_and_not_exported(self):
        marker='PRIVATE_SENTINEL'
        content={'type':'compacted','payload':{'replacement_history':[{'text':marker*80000}],
            'guardian_history':[{'image_url':'data:image/png;base64,'+'A'*20000}],
            'latest_token_usage_record':{'usage':usage(1)}}}
        raw=json.dumps(content,separators=(',',':')).encode()
        self.reader.parse(LimitedRead(raw),len(raw),chunk=4096)
        self.assertEqual(self.reader.strings['text_message_field_strings'],len(marker)*80000+2)
        self.assertEqual(self.reader.strings['image_data_url_strings'],20000+len('data:image/png;base64,')+2)
        self.assertNotIn(marker,str(self.reader.headers))
        self.assertEqual(len(list(self.reader.observations(1,0))),1)

    def test_full_file_exact_intervals_repeats_and_compaction_link(self):
        records=[{'type':'session_meta','payload':{'id':'session_a'}},event(usage(1),usage(1)),
            {'type':'response_item','payload':{'type':'message','role':'user','content':[{'type':'input_text','text':'PRIVATE_SENTINEL'}]}},
            {'type':'compacted','payload':{'replacement_history':[],'guardian_history':[],
                'compaction_response_id':'resp_a','latest_token_usage_record':{'response_id':'resp_a','usage':usage(2)}}},
            event(usage(3),usage(2)),event(usage(3),usage(2)),
            {'type':'token_usage_record','payload':{'response_id':'resp_a','usage':usage(2),
                'turn_token_usage':usage(10),'thread_token_usage':usage(100)}},
            event(usage(6),usage(3))]
        result,evidence=self.analyze(records)
        self.assertTrue(result['case_accepted'])
        self.assertEqual(result['verified_interval_tokens'],usage(5))
        self.assertEqual(result['accounting_interval_statuses']['unchanged_cumulative_snapshot'],1)
        self.assertEqual(result['identified_response_count'],1)
        self.assertEqual(result['identified_response_subset_tokens'],usage(2))
        self.assertEqual(len(result['compaction_transitions']),1)
        self.assertIsNone(result['content_category_token_shares'])
        self.assertNotIn('PRIVATE_SENTINEL',evidence)

    def test_invalid_gap_not_bridged_and_session_boundary_not_summed(self):
        records=[event(usage(1),usage(1)),event(None,None),event(usage(4),usage(3)),
                 {'type':'session_meta','payload':{}},event(usage(5),usage(1)),
                 {'type':'session_meta','payload':{}},event(usage(6),usage(1))]
        result,_=self.analyze(records)
        self.assertEqual(result['accounting_interval_statuses']['invalid_or_missing_counters'],2)
        self.assertEqual(result['accounting_interval_statuses']['session_meta_boundary'],1)
        self.assertEqual(result['verified_interval_tokens'],usage(1))
        self.assertFalse(result['case_accepted'])

    def test_conflicting_response_readings_excluded(self):
        result,_=self.analyze([{'type':'token_usage_record','payload':{'response_id':'resp_conflict','usage':usage(n)}} for n in (1,2)])
        self.assertEqual(result['conflicting_response_ids'],1)
        self.assertEqual(result['identified_response_count'],0)
        self.assertIsNone(result['identified_response_subset_tokens'])

    def test_turn_link_uses_explicit_ids_and_response_scope_only(self):
        records=[{'type':'response_item','payload':{'type':'message','role':'user',
                     'internal_chat_message_metadata_passthrough':{'turn_id':'turn_a'},'content':[]}},
                 {'type':'response_item','payload':{'type':'message','role':'user','content':[]}},
                 {'type':'token_usage_record','payload':{'turn_id':'turn_a','response_id':'resp_a',
                     'usage':usage(1),'turn_token_usage':usage(5),'thread_token_usage':usage(20)}},
                 event(usage(1),usage(1))]
        result,_=self.analyze(records)
        self.assertTrue(result['response_sum_matches_final_cumulative'])
        self.assertEqual(len(result['explicit_turn_cases']),1)
        turn=result['explicit_turn_cases'][0]
        self.assertEqual(turn['recorded_response_tokens'],usage(1))
        self.assertEqual(turn['explicitly_tagged_storage']['records'],2)
        self.assertEqual(turn['token_shares']['cached_of_input_percent'],50)

    def test_compaction_nonadditive_snapshot_kept_and_next_valid_found(self):
        snapshot=usage(0); snapshot['total_tokens']=123
        result,_=self.analyze([event(usage(1),usage(1)),{'type':'compacted','payload':{'message':''}},
                               event(usage(1),snapshot),event(usage(3),usage(2))])
        comp=result['compaction_transitions'][0]
        self.assertEqual(comp['previous_usage_line'],1)
        self.assertEqual(comp['next_usage_line'],4)
        self.assertEqual(comp['next_last_input_tokens'],20)
        self.assertEqual(comp['skipped_nonadditive_usage_events'][0]['reported_last'][0]['total_tokens'],123)


if __name__=='__main__':
    unittest.main()
