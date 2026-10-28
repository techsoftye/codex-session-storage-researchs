import io
import json
from pathlib import Path
import tempfile
import unittest

from token_usage_stream import TokenReader, COUNTERS


class BoundedBytes(io.BytesIO):
    def __init__(self, raw, limit):
        super().__init__(raw)
        self.limit = limit

    def read(self, size=-1):
        if size < 0 or size > self.limit:
            raise AssertionError('unbounded read')
        return super().read(size)


class TokenStreamTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1] / '.tmp')
        self.reader = TokenReader(Path(self.directory.name) / 'record.sqlite3')

    def tearDown(self):
        self.reader.close()
        self.directory.cleanup()

    def parse(self, value, chunk=13):
        raw = value if isinstance(value,bytes) else json.dumps(value,ensure_ascii=False).encode()
        self.reader.parse(BoundedBytes(raw,chunk),len(raw),chunk)
        rows = list(self.reader.observations(123,456))
        for row in rows:
            offset = row['source_object_byte_offset']-456
            obj = json.loads(raw[offset:offset+row['source_object_bytes']])
            self.assertTrue(set(obj).intersection(COUNTERS))
        return rows

    def test_three_scopes_and_later_identifier_fields(self):
        usage = dict(input_tokens=25,cached_input_tokens=20,output_tokens=3,total_tokens=28)
        rows = self.parse({'type':'token_usage_record','payload':{
            'usage':usage,'turn_token_usage':usage,'thread_token_usage':usage,
            'response_id':'resp_a','session_id':'session_a','thread_id':'thread_a',
            'turn_id':'turn_a','root_turn_id':'root_a'}})
        self.assertEqual([r['usage_scope'] for r in rows],['response','turn','thread'])
        self.assertTrue(all(r['response_id']=='resp_a' for r in rows))
        self.assertEqual(rows[0]['metadata']['identifier_sources']['response_id']['path'],'/payload/response_id')

    def test_partial_zero_null_and_wrong_types_preserved(self):
        rows = self.parse({'type':'unseen','payload':[{'input_tokens':0},{'total_tokens':None},
            {'output_tokens':'untrusted text'},{'input_tokens':True},{'cached_input_tokens':{}},
            {'total_tokens':1.5},{'total_tokens':2**90},{'output_tokens':-1}]})
        self.assertEqual(len(rows),8)
        self.assertEqual(rows[0]['input_tokens'],0)
        self.assertIsNone(rows[0]['total_tokens'])
        self.assertEqual(rows[1]['metadata']['field_types'],{'total_tokens':'null'})
        self.assertTrue(rows[2]['metadata']['issues'])
        self.assertNotIn('untrusted text',str(rows))
        self.assertIsNone(rows[3]['input_tokens'])
        self.assertEqual(rows[6]['metadata']['invalid_numeric_values']['total_tokens'],str(2**90))
        self.assertEqual(rows[7]['output_tokens'],-1)

    def test_every_nested_array_position_is_distinct(self):
        rows = self.parse({'payload':{'history':[{'usage':{'input_tokens':10}}]*4}})
        self.assertEqual(len(rows),4)
        self.assertEqual(len({r['source_object_byte_offset'] for r in rows}),4)
        self.assertEqual(rows[-1]['source_json_path'],'/payload/history/3/usage')

    def test_arbitrary_record_type_and_root_counters(self):
        rows = self.parse({'type':'anything','input_tokens':2,'output_tokens':3,'total_tokens':5})
        self.assertEqual(rows[0]['source_json_path'],'')
        self.assertEqual(rows[0]['usage_scope'],'unknown')

    def test_large_string_not_parsed_as_json_or_retained(self):
        text = 'private ' * 250000 + '{"input_tokens":999}'
        rows = self.parse({'payload':{'text':text,'nested':{'input_tokens':7}}},chunk=4096)
        self.assertEqual(len(rows),1)
        self.assertNotIn('private',str(rows))

    def test_nearest_ids_and_explicit_null_block_inheritance(self):
        rows = self.parse({'session_id':'outer','turn_id':'outer_turn','payload':{
            'usage':{'input_tokens':4,'session_id':None},'turn_id':'inner_turn'}})
        self.assertIsNone(rows[0]['session_id'])
        self.assertEqual(rows[0]['turn_id'],'inner_turn')

    def test_sibling_ids_do_not_leak(self):
        rows = self.parse({'payload':[{'session_id':'first','usage':{'input_tokens':1}},
                                     {'usage':{'input_tokens':2}}]})
        self.assertEqual(rows[0]['session_id'],'first')
        self.assertIsNone(rows[1]['session_id'])

    def test_dynamic_long_keys_redacted_but_objects_preserved(self):
        rows = self.parse({'secret / file path':{'x'*2000:{'input_tokens':1}}})
        self.assertEqual(len(rows),1)
        self.assertTrue(rows[0]['metadata']['path_has_redacted_key'])
        self.assertNotIn('secret',str(rows))

    def test_escaped_counter_key_and_multibyte_string_chunks(self):
        raw = '{"text":"نص عربي 😀", "inpu\\u0074_tokens":3}'.encode()
        rows = self.parse(raw,chunk=1)
        self.assertEqual(rows[0]['input_tokens'],3)

    def test_malformed_record_never_yields_observations(self):
        for raw in (b'{"input_tokens":3,}',b'{"input_tokens":3} junk',
                    b'{"input_tokens":3,"input_tokens":4}',b'{"input_tokens":3,"x":"bad\\q"}'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                self.parse(raw)

    def test_depth_limit_is_error_not_silent_skip(self):
        raw = b'{"x":' + b'['*130 + b'{"input_tokens":1}' + b']'*130 + b'}'
        with self.assertRaisesRegex(ValueError,'depth_limit'):
            self.parse(raw)

    def test_counter_objects_inside_counter_value(self):
        rows = self.parse({'input_tokens':{'total_tokens':2}})
        self.assertEqual(len(rows),2)
        self.assertEqual(rows[0]['metadata']['field_types']['input_tokens'],'object')

    def test_nonfinite_numeric_marked_without_invalid_json_report(self):
        rows = self.parse(b'{"input_tokens":1e400}')
        self.assertIsNone(rows[0]['input_tokens'])
        json.dumps(rows,allow_nan=False)

    def test_parse_next_record_clears_previous_context(self):
        self.parse({'response_id':'old','usage':{'input_tokens':1}})
        row = self.parse({'usage':{'input_tokens':2}})[0]
        self.assertIsNone(row['response_id'])

    def test_event_counters_are_distinct_snapshots(self):
        rows = self.parse({'type':'event_msg','payload':{'type':'token_count','info':{
            'last_token_usage':{'input_tokens':10},'total_token_usage':{'input_tokens':10}}}})
        self.assertEqual([r['usage_scope'] for r in rows],['last','cumulative'])


if __name__ == '__main__':
    unittest.main()
