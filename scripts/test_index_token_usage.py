import argparse
import csv
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import index_token_usage as module
from token_usage_stream import TokenReader, COUNTERS, IDENTIFIERS, VERSION


class CheckpointTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1] / '.tmp')
        self.directory = Path(self.temp.name)
        self.path = self.directory / 'source.jsonl'
        self.raw = b'{"type":"event_msg","payload":{"input_tokens":1}}\n'
        self.path.write_bytes(self.raw*3)
        self.item = {'id':7,'source_relative_path':'2026/fake.jsonl',
                     'file_size_bytes':self.path.stat().st_size,'record_count':3,
                     'indexed_bytes':self.path.stat().st_size,'state':None}
        self.reader = TokenReader(self.directory/'record.sqlite3')
        self.rows = [{'id':i+1,'line':i+1,'offset':i*len(self.raw),'bytes':len(self.raw),'type':'event_msg'} for i in range(3)]
        self.args = argparse.Namespace(batch_records=2,pause_ms=0)

    def tearDown(self):
        self.reader.close(); self.temp.cleanup()

    def scan(self,budget=None):
        def locators(session_id,line,count):
            return [r for r in self.rows if r['line']>line][:count]
        with patch.object(module,'source_path',return_value=self.path), patch.object(module,'locators',side_effect=locators), patch.object(module,'execute_file') as execute:
            result = module.scan_file(self.item,self.args,self.reader,self.directory,budget)
        return result,execute.call_count

    def test_bounded_scan_then_resume_same_file(self):
        (state,done),_ = self.scan(1)
        self.assertEqual((state['status'],done,state['observation_count']),('partial',1,1))
        state.update(scan_version=VERSION,source_size_bytes=self.path.stat().st_size,
                     source_mtime_ns=self.path.stat().st_mtime_ns,expected_record_count=3)
        self.item['state']=state
        with patch.object(module,'read_query',return_value=[{'id':1,'line':1,'end':len(self.raw)}]):
            (state,done),_ = self.scan()
        self.assertEqual((state['status'],done,state['observation_count']),('completed',2,3))

    def test_budget_exactly_eof_completes(self):
        (state,done),_ = self.scan(3)
        self.assertEqual((state['status'],done),('completed',3))

    def test_malformed_record_keeps_previous_checkpoint(self):
        self.path.write_bytes(self.raw+b'{"type":"event_msg",bad}\n'+self.raw)
        size=len(b'{"type":"event_msg",bad}\n')
        self.item.update(file_size_bytes=self.path.stat().st_size,indexed_bytes=self.path.stat().st_size)
        self.rows[1]['bytes']=size
        self.rows[2]['offset']=len(self.raw)+size
        with self.assertRaises(ValueError):
            self.scan()
        sql=(self.directory/'batch.sql').read_text()
        self.assertIn("status='error'",sql)
        self.assertIn('scanned_record_count=1,observation_count=1',sql)

    def test_unindexed_nonblank_tail_stops_completion(self):
        self.path.write_bytes(self.raw*3+b'unindexed')
        self.item['file_size_bytes']=self.path.stat().st_size
        with self.assertRaisesRegex(ValueError,'unindexed_nonblank'):
            self.scan()

    def test_empty_pointer_differs_from_sql_null(self):
        state={'last_line_number':0,'last_record_id':None,'next_byte_offset':0,
               'scanned_record_count':0,'observation_count':0,'status':'running'}
        batch=module.Batch(self.directory,self.item,state,self.path.stat())
        try:
            batch.add({'research_session_record_id':1,'source_json_path':'',
                       'source_object_byte_offset':0,'source_object_bytes':18,'usage_scope':'unknown',
                       **{k:None for k in (*COUNTERS,*IDENTIFIERS)},'metadata':{}})
            batch.handle.flush()
            with batch.path.open(newline='') as f: row=next(csv.reader(f))
            self.assertEqual(row[1],'')
            self.assertEqual(row[5],r'\N')
            with patch.object(module,'execute_file'): batch.flush('partial')
            self.assertIn("NULL '\\N'",(self.directory/'batch.sql').read_text())
        finally:
            batch.close()

    def test_checkpoint_changed_version_rejected(self):
        self.item['state']={'scan_version':'old','source_size_bytes':self.path.stat().st_size,
                            'source_mtime_ns':self.path.stat().st_mtime_ns,'expected_record_count':3}
        with self.assertRaisesRegex(ValueError,'signature_changed'):
            module.validate_state(self.item,self.path.stat())

    def test_partial_observation_iteration_never_commits_partial_record(self):
        original=self.reader.observations
        def fail_after_first(record_id,offset):
            yield from original(record_id,offset)
            raise ValueError('injected_failure')
        with patch.object(self.reader,'observations',side_effect=fail_after_first):
            with self.assertRaisesRegex(ValueError,'injected_failure'):
                self.scan()
        sql=(self.directory/'batch.sql').read_text()
        self.assertIn('scanned_record_count=0,observation_count=0',sql)
        self.assertEqual((self.directory/'batch.csv').stat().st_size,0)

    def test_ambiguous_database_failure_is_not_retried_blindly(self):
        with patch.object(module,'source_path',return_value=self.path), patch.object(module,'locators',return_value=self.rows), patch.object(module,'execute_file',side_effect=RuntimeError('database_batch_failed_exit_3')) as execute:
            with self.assertRaisesRegex(RuntimeError,'database_batch_failed'):
                module.scan_file(self.item,self.args,self.reader,self.directory)
        self.assertEqual(execute.call_count,1)


if __name__ == '__main__':
    unittest.main()
