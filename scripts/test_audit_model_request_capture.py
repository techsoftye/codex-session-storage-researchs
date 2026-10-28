import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import audit_model_request_capture as audit


class LimitedRead(io.BytesIO):
    def read(self, size=-1):
        if not 0 <= size <= 4096: raise AssertionError('unbounded read')
        return super().read(size)


class RequestCaptureTests(unittest.TestCase):
    def setUp(self):
        (audit.ROOT / '.tmp').mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=audit.ROOT / '.tmp')
        self.path = Path(self.temp.name)
        self.reader = audit.RequestShapeReader(self.path / 'scratch.sqlite3')

    def tearDown(self):
        self.reader.close(); self.temp.cleanup()

    def parse(self, obj):
        raw = json.dumps(obj, ensure_ascii=False).encode()
        self.reader.parse(LimitedRead(raw), len(raw), chunk=4096)
        return self.reader

    def test_actual_bundle_is_candidate_not_automatic_complete_capture(self):
        r = self.parse({'type': 'model_request', 'payload': {'body': {
            'input': [{'role': 'user', 'content': 'PRIVATE_SENTINEL'}], 'tools': [], 'model': 'model_a'}}})
        self.assertEqual(len(r.candidates), 1)
        self.assertEqual(r.candidates[0]['path'], '/payload/body')
        self.assertFalse(r.candidates[0]['inside_content_or_history'])
        self.assertNotIn('PRIVATE_SENTINEL', json.dumps(r.candidates))

    def test_configuration_and_input_in_separate_objects_not_merged(self):
        r = self.parse({'type': 'event', 'payload': {'settings': {'model': 'model_a'},
            'unrelated': {'input': ['x']}}})
        self.assertEqual(r.candidates, [])

    def test_tool_call_input_not_request_bundle(self):
        r = self.parse({'type': 'response_item', 'payload': {'type': 'custom_tool_call',
            'name': 'tool', 'input': 'PRIVATE_SENTINEL'}})
        self.assertEqual(r.candidates, [])
        self.assertEqual(r.fields['/payload/input']['types']['string'], 1)

    def test_nested_history_bundle_has_content_provenance(self):
        r = self.parse({'type': 'compacted', 'payload': {'guardian_history': [
            {'type': 'message', 'content': [{'body': {'model': 'model_a', 'input': ['x']}}]}]}})
        self.assertEqual(len(r.candidates), 1)
        self.assertTrue(r.candidates[0]['inside_content_or_history'])

    def test_opaque_actual_body_remains_unresolved(self):
        r = self.parse({'type': 'http_request', 'payload': {'body': '{"model":"m","input":"private"}'}})
        self.assertEqual(len(r.opaque_requests), 1)
        self.assertEqual(audit.classify_opaque({**r.opaque_requests[0], 'record_type': 'http_request'}),
                         'unresolved_request_named_string')

    def test_skill_body_classification_uses_exact_context(self):
        r = self.parse({'type': 'world_state', 'payload': {'state': {'host_skills': {'body': 'PRIVATE_SENTINEL'}}}})
        item = {**r.opaque_requests[0], 'record_type': 'world_state'}
        self.assertEqual(audit.classify_opaque(item), 'instruction_state_body_by_structural_path_not_request_capture')
        item['record_type'] = 'http_request'
        self.assertEqual(audit.classify_opaque(item), 'unresolved_request_named_string')

    def test_large_text_skipped_in_bounded_chunks_and_not_exported(self):
        private = 'PRIVATE_SENTINEL' * 150000
        r = self.parse({'type': 'response_item', 'payload': {'type': 'message', 'role': 'user',
            'content': [{'type': 'input_text', 'text': private}], 'image_url': 'data:image/png;base64,' + 'A'*50000}})
        exported = json.dumps({'fields': r.fields, 'objects': r.objects, 'enums': r.enums, 'candidates': r.candidates})
        self.assertNotIn('PRIVATE_SENTINEL', exported)
        self.assertNotIn('data:image/png;base64,', exported)
        self.assertEqual(r.fields['/payload/content/[]/text']['serialized_value_bytes'], len(private)+2)

    def test_complete_file_with_blank_lines_and_misleading_body(self):
        records = [{'type': 'token_usage_record', 'payload': {'response_id': 'resp_private', 'usage': {'input_tokens': 1}}},
            {'type': 'world_state', 'payload': {'state': {'host_skills': {'body': 'PRIVATE_SENTINEL'}}}}]
        raw = b'\r\n' + b'\r\n\r\n'.join(json.dumps(r).encode() for r in records) + b'\r\n'
        source = self.path / 'fixture.jsonl'; source.write_bytes(raw)
        with patch.object(audit, 'safe_source_path', return_value=source): result = audit.scan('fixture.jsonl')
        self.assertEqual(result['records'], 2)
        self.assertEqual(result['source_bytes'], len(raw))
        self.assertEqual(result['source_sha256'], hashlib.sha256(raw).hexdigest())
        self.assertEqual(result['verdict'], 'no_explicit_full_request_capture_found')
        self.assertEqual(result['unresolved_opaque_request_fields'], 0)
        self.assertNotIn('resp_private', json.dumps(result))
        self.assertNotIn('PRIVATE_SENTINEL', json.dumps(result))


if __name__ == '__main__':
    unittest.main()
