import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import analyze_output_token_components as study


def usage(n=1):
    return dict(input_tokens=10*n, cached_input_tokens=5*n, cache_write_input_tokens=0,
                output_tokens=2*n, reasoning_output_tokens=n, total_tokens=12*n)


def event(total, last):
    return {'type': 'event_msg', 'payload': {'type': 'token_count', 'info': {
        'total_token_usage': total, 'last_token_usage': last}}}


def observation(rid='resp_a', n=1, turn='turn_a'):
    return {'type': 'token_usage_record', 'payload': {
        'response_id': rid, 'turn_id': turn, 'thread_id': 'thread_a',
        'session_id': 'session_a', 'root_turn_id': turn, 'usage': usage(n),
        'turn_token_usage': usage(50), 'thread_token_usage': usage(100)}}


def item(kind='message', role='assistant', turn='turn_a', **extra):
    return {'type': 'response_item', 'payload': {'type': kind, 'role': role,
        'internal_chat_message_metadata_passthrough': {'turn_id': turn}, **extra}}


class LimitedRead(io.BytesIO):
    def read(self, size=-1):
        if size < 0 or size > 4096:
            raise AssertionError('unbounded source read')
        return super().read(size)


class OutputAccountingTests(unittest.TestCase):
    def test_split_preserves_unknown_item_attribution_and_missing_values(self):
        result = study.output_split({'output_tokens': 118, 'reasoning_output_tokens': 65})
        self.assertEqual(result['non_reasoning_output_tokens'], 53)
        self.assertIsNone(result['visible_reply_tokens'])
        self.assertIsNone(result['tool_call_tokens'])
        self.assertIsNone(study.output_split({'output_tokens': 118}))
        self.assertIsNone(study.output_split({'output_tokens': 1, 'reasoning_output_tokens': 2}))
        self.assertIsNone(study.output_split({'output_tokens': True, 'reasoning_output_tokens': 0}))
        self.assertIsNone(study.output_split({'output_tokens': 0, 'reasoning_output_tokens': 0})['reasoning_percent_of_output'])

    def test_classification_excludes_non_model_output(self):
        def category(typ, subtype, role=None):
            return study.item_category({'record_type': typ, 'subtype': subtype, 'role': role})
        self.assertEqual(category('response_item', 'message', 'assistant'), 'assistant_message')
        self.assertEqual(category('response_item', 'function_call'), 'tool_call')
        self.assertEqual(category('response_item', 'reasoning'), 'reasoning')
        for role in ('user', 'system', 'developer', 'tool', None):
            self.assertIsNone(category('response_item', 'message', role))
        for subtype in ('function_call_output', 'custom_tool_call_output', 'compaction'):
            self.assertIsNone(category('response_item', subtype))
        self.assertIsNone(category('event_msg', 'message', 'assistant'))


class SourceOutputTests(unittest.TestCase):
    def setUp(self):
        (study.ROOT / '.tmp').mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=study.ROOT / '.tmp')
        self.path = Path(self.temp.name)
        self.reader = study.OutputReader(self.path / 'scratch.sqlite3')

    def tearDown(self):
        self.reader.close()
        self.temp.cleanup()

    def analyze(self, records, blank_lines=False):
        separator = b'\r\n\r\n' if blank_lines else b'\r\n'
        raw = separator + separator.join(json.dumps(r, ensure_ascii=False).encode('utf-8') for r in records) + separator
        self.source = self.path / 'fixture.jsonl'
        self.source.write_bytes(raw)
        with patch.object(study, 'safe_source_path', return_value=self.source):
            evidence = study.read_file('fixture.jsonl', self.reader, 16 * 1048576)
        self.assertEqual(evidence['source_sha256'], hashlib.sha256(raw).hexdigest())
        self.assertEqual(sum(r['bytes'] for r in evidence['rows']) + evidence['blank_line_bytes'], len(raw))
        for row in evidence['rows']:
            parsed = json.loads(raw[row['offset']:row['offset'] + row['bytes']])
            self.assertEqual(parsed['type'], row['record_type'])
        cases, rejected = study.build_cases(evidence)
        return evidence, cases, rejected

    def test_bounded_private_strings_offsets_and_no_content_export(self):
        secret = 'PRIVATE_SENTINEL' * 140000
        record = item('reasoning', encrypted_content=secret, summary=[{'type': 'summary_text', 'text': 'سري\nخاص'}])
        raw = json.dumps(record, ensure_ascii=False).encode('utf-8')
        self.reader.parse(LimitedRead(raw), len(raw), chunk=4096)
        extracted = {'fields': self.reader.fields, 'spans': self.reader.value_spans,
                     'headers': self.reader.headers, 'numeric': self.reader.numeric_output_fields}
        exported = json.dumps(extracted, ensure_ascii=False)
        self.assertNotIn('PRIVATE_SENTINEL', exported)
        self.assertNotIn('سري', exported)
        encrypted = next(s for s in self.reader.value_spans if s['path'] == '/payload/encrypted_content')
        self.assertEqual(encrypted['serialized_bytes'], len(secret) + 2)
        for span in self.reader.value_spans:
            value = json.loads(raw[span['relative_offset']:span['relative_offset'] + span['serialized_bytes']])
            self.assertIsInstance(value, str)

    def test_response_scope_blank_lines_and_independent_verification(self):
        evidence, cases, rejected = self.analyze([
            item(content=[{'type': 'output_text', 'text': 'PRIVATE_SENTINEL'}]),
            observation(), event(usage(), usage())], blank_lines=True)
        self.assertEqual(len(evidence['response_usage']), 1)
        self.assertEqual(len(cases), 1)
        self.assertEqual(rejected, {})
        self.assertEqual(cases[0]['output_split']['output_tokens'], 2)
        self.assertTrue(cases[0]['item_to_response_attribution_inferred'])
        self.assertEqual(cases[0]['component_link_method'], 'explicit_turn_id_with_one_recorded_response')
        self.assertNotIn('PRIVATE_SENTINEL', json.dumps(evidence))
        with patch.object(study, 'safe_source_path', return_value=self.source):
            checked = study.verify_cases(cases)
        self.assertEqual(checked['counter_records_verified'], 1)
        self.assertEqual(checked['status'], 'passed')

    def test_multiple_responses_partition_candidates_not_whole_turn(self):
        _, cases, rejected = self.analyze([
            item('reasoning', id='rs_a', encrypted_content='hidden'),
            observation(), event(usage(), usage()),
            item('custom_tool_call', id='ctc_b', call_id='call_b', input='private'),
            observation('resp_b', 2), event(usage(3), usage(2))])
        self.assertEqual(rejected, {})
        self.assertEqual(len(cases), 2)
        self.assertEqual(cases[0]['output_component_record_counts'], {'reasoning': 1})
        self.assertEqual(cases[1]['output_component_record_counts'], {'tool_call': 1})
        for case in cases:
            self.assertEqual(case['component_link_method'], 'turn_id_and_consecutive_usage_order')
            self.assertTrue(case['item_to_response_attribution_inferred'])
        self.assertEqual(cases[1]['accounting_check'], 'cumulative_increment_matches_response_usage')

    def test_telemetry_and_tool_results_linked_but_excluded_from_output(self):
        records = [item('custom_tool_call', id='ctc_a', call_id='call_a', input='private'),
            {'type': 'event_msg', 'payload': {'type': 'item_completed', 'turn_id': 'turn_a',
                'item': {'id': 'ctc_a', 'type': 'ToolCall'}}}, observation(),
            item('custom_tool_call_output', call_id='call_a', output='private result'), event(usage(), usage())]
        evidence, cases, _ = self.analyze(records)
        case = cases[0]
        self.assertEqual(case['physical_output_record_bytes'], evidence['rows'][0]['bytes'])
        self.assertEqual(len(case['same_item_id_telemetry_records']), 1)
        self.assertEqual(len(case['tool_results_excluded_from_model_output']), 1)
        self.assertEqual(len(case['components']), 1)

    def test_conflicting_response_readings_rejected(self):
        _, cases, rejected = self.analyze([item(), observation(), event(usage(), usage()),
            item(), observation(n=2), event(usage(3), usage(2))])
        self.assertEqual(cases, [])
        self.assertEqual(rejected['conflicting_response_id'], 2)

    def test_missing_last_or_wrong_cumulative_not_accepted(self):
        for snapshots, expected in [([event(usage(), usage(2))], 'no_matching_following_last_usage_snapshot'),
                ([event(usage(2), usage())], 'cumulative_accounting_not_reconciled')]:
            with self.subTest(expected=expected):
                _, cases, rejected = self.analyze([item(), observation(), *snapshots])
                self.assertEqual(cases, [])
                self.assertEqual(rejected[expected], 1)

    def test_direct_response_id_and_conflicting_item_ids(self):
        _, cases, _ = self.analyze([item(response_id='resp_a'),
            item(turn_id='turn_wrong'), observation(), event(usage(), usage())])
        self.assertEqual(len(cases[0]['components']), 1)
        self.assertEqual(cases[0]['component_link_method'], 'explicit_response_id')
        self.assertFalse(cases[0]['item_to_response_attribution_inferred'])

    def test_invalid_counter_gap_is_not_bridged(self):
        _, cases, rejected = self.analyze([event(usage(), usage()), event(None, None),
            item(), observation(n=2), event(usage(3), usage(2))])
        self.assertEqual(cases, [])
        self.assertEqual(rejected['cumulative_accounting_not_reconciled'], 1)

    def test_session_boundary_prevents_cross_epoch_component_link(self):
        _, cases, rejected = self.analyze([item(), {'type': 'session_meta', 'payload': {'id': 'session_a'}},
            observation(), event(usage(), usage())])
        self.assertEqual(cases, [])
        self.assertEqual(rejected['no_model_output_components_in_candidate_range'], 1)

    def test_independent_validation_detects_counter_change(self):
        _, cases, _ = self.analyze([item(), observation(), event(usage(), usage())])
        cases[0]['usage_record']['usage']['output_tokens'] = 99
        with patch.object(study, 'safe_source_path', return_value=self.source):
            with self.assertRaisesRegex(ValueError, 'independent_usage_verification_failed'):
                study.verify_cases(cases)


if __name__ == '__main__':
    unittest.main()
