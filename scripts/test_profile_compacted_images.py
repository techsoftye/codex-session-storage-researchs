import base64
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest

from profile_compacted import Profile
from profile_compacted_images import ImageString, OccurrenceWriter, open_index, summarize

SCRATCH=Path(__file__).resolve().parents[1]/'.tmp/image-tests'
SCRATCH.mkdir(parents=True,exist_ok=True)


class ImageTests(unittest.TestCase):
    def test_chunk_boundaries_whitespace_and_missing_padding(self):
        binary=bytes(range(251))*13
        encoded=base64.b64encode(binary)
        for payload in (encoded,encoded.rstrip(b'='),b'\n'.join(encoded[i:i+61] for i in range(0,len(encoded),61))):
            raw=b'data:image/png;base64,'+payload
            for chunk in (1,2,3,7,64,4096):
                found=[]
                sink=ImageString(lambda *args:found.append(args))
                for start in range(0,len(raw),chunk):
                    sink.feed(raw[start:start+chunk])
                sink.finish(len(raw)+2)
                self.assertEqual(len(found),1)
                digest,size,stored,error,padding=found[0]
                self.assertEqual(digest,hashlib.sha256(binary).digest())
                self.assertEqual(size,len(binary))
                self.assertEqual(stored,len(raw)+2)
                self.assertIsNone(error)

    def test_non_images_and_invalid_base64_do_not_become_unique_images(self):
        for raw in (b'private conversation',b'https://private/image.png',b'data:audio/wav;base64,QUJD'):
            found=[]
            sink=ImageString(lambda *args:found.append(args))
            sink.feed(raw)
            sink.finish(len(raw)+2)
            self.assertEqual(found,[])
        for raw in (b'data:image/png;base64,Q',b'data:image/png;base64,!!!!',b'data:image/png;base64,QQ==QUJD',b'data:image/png;base64,'):
            found=[]
            sink=ImageString(lambda *args:found.append(args))
            for char in raw:
                sink.feed(bytes([char]))
            sink.finish(len(raw)+2)
            self.assertIsNone(found[0][0])
            self.assertIsNotNone(found[0][3])

    def test_json_unescaping_hashes_equal_binary_content(self):
        normal='data:image/png;base64,/w=='
        escaped='data:image\\/png;base64,\\u002fw=='
        raw=('{"type":"compacted","payload":{"replacement_history":[{"type":"message","content":[{"image_url":"'+normal+'"},{"image_url":"'+escaped+'"}]}]}}').encode()
        with tempfile.TemporaryDirectory(dir=SCRATCH) as temp:
            self.assertTrue(Path(temp).resolve().is_relative_to(SCRATCH.resolve()))
            conn=open_index(Path(temp)/'index.sqlite3',{'test':1})
            writer=OccurrenceWriter(conn,1,10)
            Profile(io.BytesIO(raw),len(raw),chunk=1,string_observer_factory=writer.factory).run(len(raw))
            writer.flush()
            rows=conn.execute('SELECT sha256,decoded_bytes,stored_bytes FROM occurrences ORDER BY id').fetchall()
            self.assertEqual(len(rows),2)
            self.assertEqual(rows[0][:2],rows[1][:2])
            self.assertEqual(rows[0][0],hashlib.sha256(b'\xff').digest())
            self.assertEqual(rows[0][2],len(normal)+2)
            self.assertEqual(rows[1][2],len(escaped)+2)
            conn.close()

    def test_duplicate_partition_and_overlap_reconcile(self):
        def image(data):
            return {'image_url':'data:image/png;base64,'+base64.b64encode(data).decode()}
        a,b,c=image(b'AAA'),image(b'BB'),image(b'C')
        with tempfile.TemporaryDirectory(dir=SCRATCH) as temp:
            self.assertTrue(Path(temp).resolve().is_relative_to(SCRATCH.resolve()))
            conn=open_index(Path(temp)/'index.sqlite3',{'test':1})
            cases=[(1,101,1,[a,a,b],[]),(1,102,2,[a],[a,c]),(2,201,1,[a],[b])]
            with conn:
                for session,record,sequence,replacement,guardian in cases:
                    value={'type':'compacted','payload':{'replacement_history':replacement,'guardian_history':guardian}}
                    raw=json.dumps(value).encode()
                    writer=OccurrenceWriter(conn,session,record)
                    Profile(io.BytesIO(raw),len(raw),chunk=13,string_observer_factory=writer.factory).run(len(raw))
                    writer.flush()
                    conn.execute('INSERT INTO records VALUES (?,?,?,?)',(record,session,0,sequence))
            s=summarize(conn)
            self.assertEqual(s['scopes']['combined']['occurrences'],8)
            self.assertEqual(s['scopes']['combined']['unique_images'],3)
            self.assertEqual(s['scopes']['combined']['unique_decoded_bytes'],6)
            self.assertEqual(s['between_histories']['shared_unique_images'],2)
            expected={'within_same_record_and_history':1,'across_compacted_records_same_file_and_history':1,
                      'across_files_same_history':1,'between_histories_extra_copy':2,'retained_unique_copy':3}
            self.assertEqual({k:v['occurrences'] for k,v in s['duplicate_partition'].items()},expected)
            self.assertEqual(s['recurrence']['unique_images_repeated_across_records_in_a_file'],1)
            self.assertEqual(s['recurrence']['groups_present_in_every_record_between_first_and_last'],1)
            conn.close()

    def test_file_transaction_rollback_and_signature_guard(self):
        with tempfile.TemporaryDirectory(dir=SCRATCH) as temp:
            self.assertTrue(Path(temp).resolve().is_relative_to(SCRATCH.resolve()))
            path=Path(temp)/'index.sqlite3'
            conn=open_index(path,{'test':1})
            with self.assertRaises(RuntimeError):
                with conn:
                    conn.execute('INSERT INTO records VALUES (1,1,0,1)')
                    raise RuntimeError('interrupted')
            self.assertEqual(conn.execute('SELECT count(*) FROM records').fetchone()[0],0)
            conn.close()
            with self.assertRaises(RuntimeError):
                open_index(path,{'test':2})


if __name__=='__main__':
    unittest.main()
