import io
import json
import unittest

from profile_compacted import Profile, merge, new_stats


class CompactedTests(unittest.TestCase):
    def profile(self, value, chunk=17):
        raw = json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode('utf-8') + b'\r\n'
        return Profile(io.BytesIO(raw), len(raw), chunk).run(len(raw))

    def test_histories_counts_types_and_exact_block_bytes(self):
        payload = {'message':'private text', 'replacement_history':[
            {'type':'message','role':'user','content':[{'type':'input_text','text':'secret'}]},
            {'type':'function_call','arguments':'private arguments'}],
            'guardian_history':[], 'window_number':3}
        result = self.profile({'type':'compacted','payload':payload})
        for key, value in payload.items():
            expected = len(json.dumps(value,ensure_ascii=False,separators=(',',':')).encode())
            self.assertEqual(result['payload_fields'][key]['value_bytes'],expected)
        self.assertEqual(result['history_lengths']['payload.replacement_history']['sum'],2)
        self.assertEqual(result['history_lengths']['payload.guardian_history']['sum'],0)
        self.assertEqual(result['enums']['payload.replacement_history[].type'],{'message':1,'function_call':1})
        for secret in ('private text','private arguments','secret'):
            self.assertNotIn(secret,json.dumps(result))

    def test_unicode_escapes_and_all_chunk_boundaries(self):
        raw = b'{"payload":{"message":"escaped \\" and \\\\ and \\u0627 ' + 'عربي'.encode() + b'"},"type":"compacted"}\n'
        json.loads(raw)
        for chunk in range(1,30):
            result = Profile(io.BytesIO(raw),len(raw),chunk).run(len(raw))
            self.assertEqual(result['records'],1)
            self.assertEqual(result['source_bytes'],len(raw))

    def test_large_string_is_not_exported_and_bounded_reads(self):
        class Bounded(io.BytesIO):
            def read(self,n=-1):
                if n < 0 or n > 256*1024:
                    raise AssertionError('unbounded read')
                return super().read(n)
        raw = b'{"type":"compacted","payload":{"message":"data:image/png;base64,'+b'A'*(4*1024*1024)+b'"}}'
        result = Profile(Bounded(raw),len(raw)).run(len(raw))
        self.assertEqual(result['features']['embedded_base64_image'],1)
        self.assertLess(len(json.dumps(result)),2000)

    def test_exact_range_and_trailing_bytes(self):
        raw = b'{"type":"compacted","payload":{"guardian_history":null}}'
        source = io.BytesIO(b'prefix'+raw+b'PRIVATE_SUFFIX')
        source.seek(6)
        result = Profile(source,len(raw),3).run(len(raw))
        self.assertEqual(source.tell(),6+len(raw))
        self.assertEqual(result['payload_fields']['guardian_history']['types'],{'null':1})

    def test_invalid_syntax_duplicates_and_unicode_are_rejected(self):
        for raw in (b'{"type":"compacted","type":"compacted","payload":{}}',
                    b'{"type":"compacted","payload":{"a":01}}',
                    b'{"type":"compacted","payload":{"a":true,}}',
                    b'{"type":"compacted","payload":{"a":"\\q"}}',
                    b'{"type":"compacted","payload":{"a":"\xff"}}',
                    b'{"type":"compacted","payload":{}} garbage'):
            with self.assertRaises((ValueError,UnicodeError)):
                Profile(io.BytesIO(raw),len(raw),5).run(len(raw))

    def test_merging_preserves_record_vs_occurrence_counts(self):
        item = {'type':'message','role':'user'}
        record = {'type':'compacted','payload':{'replacement_history':[item,item]}}
        result = new_stats()
        merge(result,self.profile(record))
        merge(result,self.profile(record))
        self.assertEqual(result['fields']['payload.replacement_history[].role : string'],{'records':2,'occurrences':4})
        self.assertEqual(result['history_lengths']['payload.replacement_history'],{'count':2,'sum':4,'min':2,'max':2})

    def test_item_bytes_are_exact_with_type_last_unicode_and_spacing(self):
        first = b'{ "content": [{"image_url":"data:image/png;base64,QUJD","type":"input_image"}], "type": "message" }'
        second = '{"encrypted_content":"private عربي\\n\\\"", "type":"compaction"}'.encode()
        history = b'[\n '+first+b' ,\r\n '+second+b'\n]'
        raw=b'{"type":"compacted","payload":{"replacement_history":'+history+b',"guardian_history":['+second+b']}}'
        json.loads(raw)
        result=Profile(io.BytesIO(raw),len(raw),7).run(len(raw))
        types=result['history_item_sizes']['payload.replacement_history']
        self.assertEqual(types['message']['value_bytes'],len(first))
        self.assertEqual(types['compaction']['value_bytes'],len(second))
        self.assertEqual(types['message']['embedded_image_string_bytes'],len(b'"data:image/png;base64,QUJD"'))
        self.assertEqual(types['compaction']['encrypted_content_string_bytes'],len('"private عربي\\n\\\""'.encode()))
        self.assertEqual(result['history_item_sizes']['payload.guardian_history']['compaction']['items'],1)
        self.assertEqual(result['payload_fields']['replacement_history']['value_bytes']-sum(v['value_bytes'] for v in types.values()),len(history)-len(first)-len(second))

    def test_missing_type_and_non_object_items_are_accounted_for(self):
        result=self.profile({'type':'compacted','payload':{'replacement_history':[{},None,{'type':None}]}})
        sizes=result['history_item_sizes']['payload.replacement_history']
        self.assertEqual(set(sizes),{'[missing_type]','[item_null]','[null_type]'})
        self.assertEqual(sum(v['items'] for v in sizes.values()),3)
        combined=new_stats()
        merge(combined,result)
        merge(combined,result)
        self.assertEqual(combined['history_item_sizes']['payload.replacement_history']['[item_null]']['value_bytes'],8)


if __name__ == '__main__':
    unittest.main()
