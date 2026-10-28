import io
import json
import os
import unittest

import index_session_records as indexer


class RecordIndexTests(unittest.TestCase):
    def test_streaming_locator_handles_large_payload_and_blank_line(self):
        first = json.dumps({"timestamp": "2026-01-01T00:00:00Z", "type": "response_item",
                            "payload": {"image": "A" * (3 * 1024 * 1024)}}).encode() + b"\r\n"
        blank = b"  \r\n"
        second = json.dumps({"timestamp": "2026-01-01T00:00:01Z", "type": "event_msg",
                             "payload": {"type": "done"}}).encode()
        raw = first + blank + second
        stats = indexer.ScanStats()
        rows = list(indexer.iter_record_locators(io.BytesIO(raw), stats, chunk_bytes=64 * 1024))

        self.assertEqual([row.line_number for row in rows], [1, 3])
        self.assertEqual(rows[0].byte_offset, 0)
        self.assertEqual(rows[0].record_bytes, len(first))
        self.assertEqual(rows[1].byte_offset, len(first) + len(blank))
        self.assertEqual(rows[0].record_timestamp, "2026-01-01T00:00:00Z")
        self.assertEqual(rows[1].record_type, "event_msg")
        self.assertEqual(stats.bytes_scanned, len(raw))
        self.assertEqual(stats.blank_line_count, 1)

    def test_resume_preserves_physical_line_numbers_and_offsets(self):
        first = b'{"timestamp":"a","type":"one","payload":{}}\n'
        second = b'{"timestamp":"b","type":"two","payload":{}}\n'
        stats = indexer.ScanStats(bytes_scanned=len(first), physical_lines=1, record_count=1)
        rows = list(indexer.iter_record_locators(
            io.BytesIO(first + second), stats, start_offset=len(first), previous_line_number=1,
            chunk_bytes=8,
        ))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].line_number, 2)
        self.assertEqual(rows[0].byte_offset, len(first))
        self.assertEqual(rows[0].record_type, "two")

    def test_nested_payload_type_is_not_reported_as_top_level_type(self):
        timestamp, record_type = indexer.extract_top_level_header(
            b'{"timestamp":"x","payload":{"type":"nested"}}'
        )
        self.assertEqual(timestamp, "x")
        self.assertIsNone(record_type)

    def test_invalid_header_still_creates_locator_with_null_fields(self):
        stats = indexer.ScanStats()
        rows = list(indexer.iter_record_locators(io.BytesIO(b"not-json\n"), stats, chunk_bytes=4))
        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0].record_timestamp)
        self.assertIsNone(rows[0].record_type)

    def test_header_survives_utf8_character_split_at_prefix_boundary(self):
        raw = '{"timestamp":"x","type":"response_item","payload":{"text":"ع'.encode()
        self.assertEqual(indexer.extract_top_level_header(raw[:-1]), ("x", "response_item"))

    def test_header_still_rejects_invalid_utf8_inside_prefix(self):
        self.assertEqual(indexer.extract_top_level_header(
            b'{"timestamp":"x","type":"response_item","payload":"\xff"}'
        ), (None, None))

    @unittest.skipUnless(os.environ.get("RECORD_INDEX_DB_TESTS") == "1", "database test disabled")
    def test_batch_insert_and_parent_progress_use_one_transaction(self):
        locator = indexer.RecordLocator(1, 0, 50, "2026-01-01T00:00:00Z", "session_meta")
        sql = """CREATE TEMP TABLE research_sessions
(LIKE public.research_sessions INCLUDING ALL);
CREATE TEMP TABLE research_session_records
(LIKE public.research_session_records INCLUDING ALL);
INSERT INTO pg_temp.research_sessions
(id, file_name, source_relative_path, file_size_bytes, scan_status, scan_complete)
OVERRIDING SYSTEM VALUE VALUES
(1, 'synthetic.jsonl', '2026/01/01/synthetic.jsonl', 50, 'pending', false);
""" + indexer.batch_sql(
            1, [locator], 50, 1,
            records_target="pg_temp.research_session_records",
            sessions_target="pg_temp.research_sessions",
        ) + """
SELECT json_build_object(
  'records', (SELECT count(*) FROM pg_temp.research_session_records),
  'status', (SELECT scan_status FROM pg_temp.research_sessions WHERE id=1),
  'bytes', (SELECT scanned_bytes FROM pg_temp.research_sessions WHERE id=1)
);
"""
        results = [json.loads(line) for line in indexer.run_psql(sql).splitlines() if line.startswith("{")]
        self.assertEqual(results, [{"records": 1, "status": "running", "bytes": 50}])


if __name__ == "__main__":
    unittest.main()
