import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest

from profile_compacted import Profile
from profile_compacted_texts import StringMeasure,Writer,classify,open_index,summarize

SCRATCH=Path(__file__).resolve().parents[1]/'.tmp/text-tests'
SCRATCH.mkdir(parents=True,exist_ok=True)


class TextTests(unittest.TestCase):
    def observe(self, raw, chunk):
        found=[]
        document=b'{"type":"compacted","payload":{"replacement_history":[{"content":[{"text":'+raw+b'}]}]}}'
        Profile(io.BytesIO(document),len(document),chunk=chunk,
                string_observer_factory=lambda path:StringMeasure('content_text',lambda *a:found.append(a)) if path.endswith('.text') else None).run(len(document))
        return found[0]

    def test_unicode_and_surrogate_pairs_match_literal_and_escapes(self):
        text='مرحبا 😀 /\nline\tend'
        expected=hashlib.sha256(text.encode()).digest()
        for ascii_mode in (True,False):
            raw=json.dumps(text,ensure_ascii=ascii_mode).encode()
            for chunk in (1,2,3,7,256):
                outcome,digest,n,stored,marker=self.observe(raw,chunk)
                self.assertEqual((outcome,digest,n,stored),('text',expected,len(text.encode()),len(raw)))

    def test_whitespace_is_preserved_and_empty_is_separate(self):
        hashes={self.observe(json.dumps(s).encode(),1)[1] for s in ('x','x ','x\n','x\r\n')}
        self.assertEqual(len(hashes),4)
        self.assertEqual(self.observe(b'""',1)[0],'empty_text')
        self.assertEqual(self.observe(b'" "',1)[0],'text')

    def test_unpaired_surrogates_are_explicit_errors(self):
        for raw in (b'"\\ud800"',b'"x\\udc00"',b'"\\ud800x"'):
            self.assertEqual(self.observe(raw,1)[0],'invalid_unicode')

    def test_media_and_composite_text_are_distinguished(self):
        self.assertEqual(self.observe(b'"data:image/png;base64,QUJD"',1)[0],'excluded_embedded_media')
        outcome,_,_,_,marker=self.observe(b'"example data:image/png;base64,QUJD"',2)
        self.assertEqual((outcome,marker),('text',1))
        self.assertEqual(self.observe(b'"data:"',1)[0],'text')

    def test_fields_and_unknowns(self):
        prefix='payload.guardian_history[].'
        expected={'content[].text':'content_text','arguments':'tool_input','output[].text':'tool_output',
                  'summary[].text':'reasoning_summary','content[].encrypted_content':'encrypted_content',
                  'id':'technical_metadata','new_field':'unclassified','content[].image_url':'image_reference'}
        for suffix,category in expected.items():
            self.assertEqual(classify(prefix+suffix),('guardian_history',category))

    def test_duplicate_partition_and_checkpoint_reuse(self):
        with tempfile.TemporaryDirectory(dir=SCRATCH) as folder:
            self.assertTrue(Path(folder).resolve().is_relative_to(SCRATCH.resolve()))
            path=Path(folder)/'index.sqlite3'
            conn=open_index(path,{'test':1})
            cases=[(1,101,1,['AAA','AAA','BB'],[]),(1,102,2,['AAA'],['AAA','C']),(2,201,1,['AAA'],['BB'])]
            with conn:
                for session,record,sequence,left,right in cases:
                    writer=Writer(conn,session);writer.record=record
                    doc={'type':'compacted','payload':{h:[{'content':[{'text':s} for s in items]}]
                          for h,items in [('replacement_history',left),('guardian_history',right)]}}
                    raw=json.dumps(doc).encode()
                    Profile(io.BytesIO(raw),len(raw),chunk=3,string_observer_factory=writer.factory).run(len(raw))
                    writer.flush()
                    # Test uses multiple records per file; merge their field counters explicitly.
                    for (field,outcome),(n,b) in writer.stats.items():
                        conn.execute('''INSERT INTO field_stats VALUES (?,?,?,?,?) ON CONFLICT DO UPDATE SET
                            occurrences=occurrences+excluded.occurrences,stored_bytes=stored_bytes+excluded.stored_bytes''',(session,field,outcome,n,b))
                    conn.execute('INSERT INTO records VALUES (?,?,?,?)',(record,session,0,sequence))
            r=summarize(conn)
            self.assertEqual(r['scopes']['combined']['unique_texts'],3)
            self.assertEqual(r['scopes']['combined']['unique_utf8_bytes'],6)
            self.assertEqual(r['scopes']['combined']['occurrences'],8)
            self.assertEqual(r['between_histories']['shared_unique_texts'],2)
            self.assertEqual({k:v['occurrences'] for k,v in r['duplicate_partition'].items()},
                dict(within_same_record_and_history=1,across_compacted_records_same_file_and_history=1,
                     across_files_same_history=1,between_histories_extra_copy=2,retained_unique_copy=3))
            with self.assertRaises(RuntimeError):
                with conn:
                    conn.execute('INSERT INTO records VALUES (999,2,0,2)')
                    raise RuntimeError('interrupt')
            self.assertEqual(conn.execute('SELECT count(*) FROM records WHERE record_id=999').fetchone()[0],0)
            conn.close()
            conn=open_index(path,{'test':1})
            self.assertEqual(summarize(conn)['scopes'],r['scopes'])
            conn.close()
            with self.assertRaises(RuntimeError):
                open_index(path,{'test':2})


if __name__=='__main__':
    unittest.main()
