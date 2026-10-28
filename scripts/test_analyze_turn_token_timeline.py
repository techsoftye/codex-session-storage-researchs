import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import analyze_turn_token_timeline as study


def usage(n):
    return dict(input_tokens=10*n, cached_input_tokens=5*n, cache_write_input_tokens=0,
        output_tokens=2*n, reasoning_output_tokens=n, total_tokens=12*n)


def obs(rid, n, prefix, turn='turn_a'):
    return {'type': 'token_usage_record', 'payload': {'response_id': rid,
        'turn_id': turn, 'root_turn_id': turn, 'thread_id': 'thread_a', 'session_id': 'session_a',
        'usage': usage(n), 'turn_token_usage': usage(prefix), 'thread_token_usage': usage(10+prefix)}}


def event(total, last):
    return {'type': 'event_msg', 'payload': {'type': 'token_count', 'info': {
        'total_token_usage': total, 'last_token_usage': last}}}


def marker(kind, turn='turn_a'):
    return {'type': 'event_msg', 'payload': {'type': kind, 'turn_id': turn}}


def message():
    return {'type': 'response_item', 'payload': {'type': 'message', 'role': 'assistant',
        'internal_chat_message_metadata_passthrough': {'turn_id': 'turn_a'},
        'content': [{'type': 'output_text', 'text': 'PRIVATE_SENTINEL'}]}}


def fixture(compaction=True):
    baseline = obs('resp_prior', 2, 0, turn='turn_prior')
    baseline['payload']['turn_token_usage'] = usage(2)
    records = [baseline, event(usage(10), usage(2)), marker('task_started'),
        message(), obs('resp_a', 1, 1), event(usage(11), usage(1))]
    if compaction:
        compact_usage = obs('resp_compact', 3, 4)
        invalid = usage(0); invalid['total_tokens'] = 99
        records += [compact_usage, {'type': 'compacted', 'payload': {
            'message': 'PRIVATE_SENTINEL', 'replacement_history': [], 'guardian_history': [],
            'compaction_response_id': 'resp_compact',
            'latest_token_usage_record': copy.deepcopy(compact_usage['payload'])}},
            event(usage(11), invalid), message(), obs('resp_b', 2, 6), event(usage(13), usage(2))]
    else:
        records += [message(), obs('resp_b', 1, 2), event(usage(12), usage(1))]
    records.append(marker('task_complete'))
    for index, row in enumerate(records):
        row['timestamp'] = f'2026-09-16T00:00:{index:02d}.000Z'
    return records


class TurnTimelineTests(unittest.TestCase):
    def setUp(self):
        (study.ROOT / '.tmp').mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=study.ROOT / '.tmp')
        self.path = Path(self.temp.name)
        self.reader = study.TurnReader(self.path / 'scratch.sqlite3')

    def tearDown(self):
        self.reader.close(); self.temp.cleanup()

    def scan(self, records):
        self.source = self.path / 'fixture.jsonl'
        raw = b'\r\n' + b'\r\n\r\n'.join(json.dumps(row).encode() for row in records) + b'\r\n'
        self.source.write_bytes(raw)
        with patch.object(study.components, 'safe_source_path', return_value=self.source):
            evidence = study.components.read_file('fixture.jsonl', self.reader, 16*1048576)
        return evidence

    def run_fixture(self, records=None):
        evidence = self.scan(records if records is not None else fixture())
        report, objects, rows = study.build_timeline(evidence, self.reader.accounting_objects, 'turn_a')
        return evidence, report, objects, rows

    def test_compaction_difference_reconciled_and_duplicate_counted_once(self):
        _, report, _, _ = self.run_fixture()
        self.assertEqual(report['status'], 'reconciled_with_documented_event_scope_difference')
        self.assertEqual(report['response_count'], 3)
        self.assertEqual(report['response_usage_observation_count'], 4)
        self.assertEqual(report['duplicate_response_observations_not_added'], 1)
        self.assertEqual(report['totals'], usage(6))
        self.assertEqual(report['event_counter_difference'], usage(3))
        self.assertEqual(report['sum_minus_event_difference'], usage(3))
        self.assertTrue(report['checks']['every_turn_prefix_matches'])
        self.assertTrue(report['checks']['every_thread_prefix_matches'])
        self.assertTrue(report['checks']['event_gap_explained_at_every_response'])
        self.assertFalse(report['checks']['event_difference_equals_sum'])
        self.assertEqual(report['response_last_snapshot_matches'], 2)
        self.assertEqual(report['response_event_increment_matches'], 2)

    def test_equal_values_different_response_ids_remain_distinct(self):
        _, report, _, _ = self.run_fixture(fixture(compaction=False))
        self.assertEqual(report['status'], 'reconciled')
        self.assertEqual(report['response_count'], 2)
        self.assertEqual(report['totals'], usage(2))
        self.assertEqual(report['duplicate_response_observations_not_added'], 0)

    def test_invalid_post_compaction_snapshot_preserved_not_summed(self):
        _, report, _, _ = self.run_fixture()
        self.assertEqual(len(report['invalid_counter_objects']), 1)
        self.assertEqual(report['invalid_counter_objects'][0]['usage']['total_tokens'], 99)
        self.assertEqual(report['totals']['total_tokens'], 72)
        self.assertTrue(report['timeline'][1]['is_explicit_compaction_response'])
        self.assertEqual(report['timeline'][1]['candidate_output_record_bytes'], None)

    def test_missing_baseline_is_not_assumed_zero(self):
        evidence = self.scan(fixture()[2:])
        with self.assertRaisesRegex(ValueError, 'no_observed_pre_turn_cumulative_baseline'):
            study.build_timeline(evidence, self.reader.accounting_objects, 'turn_a')

    def test_duplicate_response_counter_conflict_is_rejected(self):
        records = fixture()
        compact = next(r for r in records if r['type'] == 'compacted')
        compact['payload']['latest_token_usage_record']['usage'] = usage(2)
        evidence = self.scan(records)
        with self.assertRaisesRegex(ValueError, 'conflicting_response_observations'):
            study.build_timeline(evidence, self.reader.accounting_objects, 'turn_a')

    def test_conflicting_compaction_link_is_rejected(self):
        records = fixture()
        compact = next(r for r in records if r['type'] == 'compacted')
        compact['payload']['compaction_response_id'] = 'resp_a'
        evidence = self.scan(records)
        with self.assertRaisesRegex(ValueError, 'conflicting_compaction_response_link'):
            study.build_timeline(evidence, self.reader.accounting_objects, 'turn_a')

    def test_interleaved_turn_is_rejected(self):
        records = fixture(); records.insert(5, marker('task_started', 'turn_other'))
        evidence = self.scan(records)
        with self.assertRaisesRegex(ValueError, 'overlapping_or_interrupted_turn'):
            study.build_timeline(evidence, self.reader.accounting_objects, 'turn_a')

    def test_wrong_prefix_reports_unresolved_not_success(self):
        records = fixture(compaction=False)
        records[-3]['payload']['turn_token_usage'] = usage(3)
        _, report, _, _ = self.run_fixture(records)
        self.assertEqual(report['status'], 'unresolved_accounting')
        self.assertFalse(report['checks']['sum_equals_final_turn_counter'])

    def test_independent_verification_counter_paths_and_identifier_spans(self):
        evidence, report, objects, _ = self.run_fixture()
        with patch.object(study, 'safe_source_path', return_value=self.source):
            result = study.verify(evidence, objects, self.reader.identifier_evidence, report['boundary'])
        self.assertEqual(result['status'], 'passed')
        self.assertEqual(result['counter_objects_verified'], len(objects))
        self.assertGreater(result['identifier_values_verified'], 0)
        self.assertNotIn('PRIVATE_SENTINEL', json.dumps(report))
        self.assertNotIn('PRIVATE_SENTINEL', json.dumps(objects))

    def test_independent_verification_detects_tampered_extraction(self):
        evidence, report, objects, _ = self.run_fixture()
        objects[0]['usage']['input_tokens'] += 1
        with patch.object(study, 'safe_source_path', return_value=self.source):
            with self.assertRaisesRegex(ValueError, 'independent_counter_fragment_mismatch'):
                study.verify(evidence, objects, self.reader.identifier_evidence, report['boundary'])

    def test_storage_span_with_blank_lines_is_accounted(self):
        _, report, _, _ = self.run_fixture()
        self.assertEqual(sum(row['bytes'] for row in report['storage_by_record_type']) + report['boundary']['blank_bytes'],
                         report['boundary']['byte_span'])
        self.assertGreater(report['boundary']['blank_bytes'], 0)


if __name__ == '__main__':
    unittest.main()
