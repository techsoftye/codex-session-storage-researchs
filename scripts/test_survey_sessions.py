"""Tests use generated fixtures only. Never open the research dataset."""
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import survey_sessions as survey


class SurveyTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix=".survey-test-", dir=Path(__file__).parent)
        self.root = Path(self.temporary.name).resolve()
        self.addCleanup(self.temporary.cleanup)

    def fixture(self, data, name="sample.jsonl"):
        path = self.root / "inputs" / name
        path.parent.mkdir(exist_ok=True)
        path.write_bytes(data)
        return path

    def record(self, kind="event_msg", payload=None, time="2026-01-01T03:00:00Z"):
        return json.dumps({"timestamp": time, "type": kind, "payload": payload or {}}).encode() + b"\n"

    def scan(self, path, **options):
        return survey.survey_file(path, self.root, delay=0, **options)

    def test_complete_identity_context_and_tail(self):
        identifier = "11111111-2222-3333-4444-555555555555"
        data = self.record("session_meta", {"id": identifier, "timestamp": "2026-01-01T05:00:00+03:00",
                                           "source": "vscode", "instructions": "Synthetic instructions"})
        data += self.record("turn_context", {"model": "test-model", "effort": "medium", "sandbox_policy": {"type": "read-only"}})
        data += self.record("response_item", {"type": "message", "role": "user"})
        data += self.record("event_msg", {"type": "user_message"}, "2026-01-01T04:00:00Z")
        path = self.fixture(data, f"rollout-{identifier}.jsonl")
        result = self.scan(path)
        self.assertEqual(result["session_id"], identifier)
        self.assertTrue(result["filename_id_matches"])
        self.assertEqual(result["session_started_at"], "2026-01-01T02:00:00Z")
        self.assertEqual(result["session_last_record_at"], "2026-01-01T04:00:00Z")
        self.assertEqual(result["first_observed_model"], "test-model")
        self.assertEqual(result["scanned_record_count"], 4)
        self.assertEqual(result["user_role_message_count"], 1)
        self.assertEqual(result["user_message_event_count"], 1)
        self.assertTrue(result["scan_complete"])
        self.assertTrue(result["metadata"]["record_parsing_complete"])
        self.assertTrue(result["multiple_payload_shapes_observed"])
        self.assertEqual(path.read_bytes(), data)
        self.assertEqual(result["source_relative_path"], f"inputs/{path.name}")

    def test_preview_counts_only_complete_prefix_records(self):
        first = self.record()
        path = self.fixture(first + self.record(time="2026-01-02T00:00:00Z"))
        result = self.scan(path, head_limit=len(first) + 5)
        self.assertFalse(result["scan_complete"])
        self.assertEqual(result["scan_status"], "partial")
        self.assertEqual(result["scanned_record_count"], 1)
        self.assertTrue(result["metadata"]["partial_record_at_limit"])
        self.assertEqual(result["session_last_record_at"], "2026-01-02T00:00:00Z")
        self.assertEqual(result["last_observed_event_at"], "2026-01-01T03:00:00Z")

    def test_conflicting_session_ids_require_review_without_overwriting_first(self):
        data = self.record("session_meta", {"id": "first-session"})
        data += self.record("session_meta", {"id": "different-session"})
        result = self.scan(self.fixture(data))
        self.assertEqual(result["session_id"], "first-session")
        self.assertTrue(result["metadata"]["conflicting_session_ids_observed"])
        self.assertEqual(result["scan_status"], "needs_review")

    def test_oversized_record_is_skipped_without_losing_next_record(self):
        data = self.record(payload={"text": "x" * 100000}) + self.record()
        result = self.scan(self.fixture(data), full=True, record_limit=256, tail_limit=256)
        self.assertEqual(result["scanned_bytes"], len(data))
        self.assertEqual(result["scanned_record_count"], 2)
        self.assertEqual(result["valid_record_count"], 1)
        self.assertEqual(result["metadata"]["oversized_record_count"], 1)
        self.assertEqual(result["scan_status"], "needs_review")
        self.assertFalse(result["metadata"]["record_parsing_complete"])

    def test_oversized_final_record_has_no_guessed_end_time(self):
        result = self.scan(self.fixture(self.record() + self.record(payload={"text": "x" * 10000})),
                           head_limit=128, tail_limit=128, record_limit=128)
        self.assertIsNone(result["session_last_record_at"])
        self.assertEqual(result["metadata"]["last_record_read_status"], "record_exceeds_tail_limit")

    def test_invalid_last_record_does_not_fall_back_to_previous(self):
        result = self.scan(self.fixture(self.record() + b"{broken}\n"))
        self.assertIsNone(result["session_last_record_at"])
        self.assertEqual(result["invalid_record_count"], 1)
        self.assertEqual(result["metadata"]["last_record_read_status"], "invalid_last_record")

    def test_bom_crlf_blank_lines_and_no_final_newline(self):
        data = b"\xef\xbb\xbf" + self.record().replace(b"\n", b"\r\n") + b"\r\n" + self.record().rstrip(b"\n")
        result = self.scan(self.fixture(data))
        self.assertEqual(result["valid_record_count"], 2)
        self.assertEqual(result["metadata"]["blank_line_count"], 1)
        self.assertEqual(result["metadata"]["last_record_read_status"], "found")

    def test_empty_file(self):
        result = self.scan(self.fixture(b""))
        self.assertTrue(result["scan_complete"])
        self.assertIsNone(result["session_id"])
        self.assertIsNone(result["uniform_envelope_observed"])
        self.assertEqual(result["metadata"]["last_record_read_status"], "empty_file")

    def test_deep_or_nonstandard_json_is_rejected_safely(self):
        result = self.scan(self.fixture(b'{"value": NaN}\n' + b"[" * 2000 + b"0" + b"]" * 2000 + b"\n"))
        self.assertEqual(result["invalid_record_count"], 2)
        self.assertEqual(result["scan_status"], "needs_review")

    def test_cli_output_valid_and_existing_output_preserved(self):
        self.fixture(self.record())
        with patch.object(survey, "PROJECT_ROOT", self.root), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(survey.main(["--source", "inputs"]), 0)
            output = self.root / "data/session_survey_2026.json"
            original = output.read_bytes()
            parsed = json.loads(original)
            self.assertEqual(parsed["files_written"], 1)
            self.assertEqual(parsed["export_status"], "completed")
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                survey.main(["--source", "inputs"])
            self.assertEqual(output.read_bytes(), original)

    def test_dry_run_never_calls_content_reader(self):
        self.fixture(b"private synthetic data")
        with patch.object(survey, "PROJECT_ROOT", self.root), patch.object(survey, "survey_file") as reader:
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(survey.main(["--source", "inputs", "--dry-run"]), 0)
            reader.assert_not_called()
        self.assertFalse((self.root / "data").exists())

    def test_interrupt_produces_valid_partial_export(self):
        path = self.fixture(self.record())
        def interrupted_source(_):
            yield path
            raise KeyboardInterrupt
        with patch.object(survey, "PROJECT_ROOT", self.root), patch.object(survey, "candidate_files", interrupted_source):
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(survey.main(["--source", "inputs"]), 130)
        result = json.loads((self.root / "data/session_survey_2026.json").read_text())
        self.assertEqual(result["export_status"], "interrupted")
        self.assertEqual(result["files_written"], 1)

    def test_paths_outside_project_and_output_in_source_are_rejected(self):
        self.fixture(self.record())
        with patch.object(survey, "PROJECT_ROOT", self.root), contextlib.redirect_stderr(io.StringIO()):
            for arguments in (["--source", "../"], ["--source", "inputs", "--output", "inputs/output.json"]):
                with self.assertRaises(SystemExit):
                    survey.main(arguments)


if __name__ == "__main__":
    unittest.main()
