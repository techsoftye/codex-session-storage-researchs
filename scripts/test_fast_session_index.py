"""Synthetic fast-index tests. No copied research JSONL files are opened."""
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import fast_session_index as fast


class FastIndexTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix=".fast-index-test-", dir=Path(__file__).parent)
        self.root = Path(self.temp.name).resolve()
        self.addCleanup(self.temp.cleanup)

    def write(self, name, records):
        path = self.root / "2026" / "01" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(records)
        return path

    @staticmethod
    def record(kind, payload, moment="2026-01-01T00:00:00Z"):
        return (json.dumps({"timestamp": moment, "type": kind, "payload": payload}) + "\n").encode()

    def test_head_tail_stops_before_middle(self):
        identifier = "11111111-2222-3333-4444-555555555555"
        data = self.record("session_meta", {"id": identifier, "timestamp": "2026-01-01T03:00:00+03:00",
                                            "cli_version": "test"})
        data += self.record("turn_context", {"model": "gpt-test", "effort": "low"})
        data += self.record("response_item", {"type": "message", "content": "x" * 2_000_000})
        data += self.record("event_msg", {}, "2026-01-02T00:00:00Z")
        path = self.write(f"rollout-2026-01-01T03-00-00-{identifier}.jsonl", data)
        item = fast.index_file(path, self.root)
        self.assertEqual(item["identity"]["canonical_session_id"], identifier)
        self.assertEqual(item["identity"]["identity_status"], "verified")
        self.assertEqual(item["session"]["session_started_at"], "2026-01-01T00:00:00Z")
        self.assertEqual(item["session"]["session_last_record_at"], "2026-01-02T00:00:00Z")
        self.assertEqual(item["initial_context"]["first_observed_model"], "gpt-test")
        self.assertLess(item["indexing"]["head_bytes_read"], 1000)
        self.assertLess(item["indexing"]["tail_bytes_read"], 600_000)
        self.assertEqual(item["indexing"]["metadata_index_status"], "completed")
        self.assertEqual(path.read_bytes(), data)

    def test_multiple_filename_ids_do_not_override_metadata(self):
        parent = "11111111-2222-3333-4444-555555555555"
        child = "66666666-7777-8888-9999-aaaaaaaaaaaa"
        data = self.record("session_meta", {"id": parent}) + self.record("turn_context", {"model": "test"})
        item = fast.index_file(self.write(f"rollout-{child}-{parent}.jsonl", data), self.root)
        self.assertEqual(item["identity"]["candidate_session_ids"], [child, parent])
        self.assertEqual(item["identity"]["canonical_session_id"], parent)
        self.assertTrue(item["identity"]["filename_id_matches"])

    def test_fork_history_does_not_override_own_file_identity(self):
        child = "11111111-2222-3333-4444-555555555555"
        parent = "66666666-7777-8888-9999-aaaaaaaaaaaa"
        data = self.record("session_meta", {"id": child, "forked_from_id": parent})
        data += self.record("session_meta", {"id": parent})
        data += self.record("turn_context", {"model": "test"})
        item = fast.index_file(self.write(f"rollout-{child}.jsonl", data), self.root)
        self.assertEqual(item["identity"]["canonical_session_id"], child)
        self.assertEqual(item["identity"]["identity_status"], "verified")
        self.assertEqual(item["identity"]["observed_session_meta_ids"], [child, parent])
        self.assertEqual(item["indexing"]["metadata_index_status"], "completed")

    def test_multiple_meta_ids_without_filename_match_remain_unresolved(self):
        one = "11111111-2222-3333-4444-555555555555"
        two = "66666666-7777-8888-9999-aaaaaaaaaaaa"
        third = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
        data = self.record("session_meta", {"id": one})
        data += self.record("session_meta", {"id": two})
        data += self.record("turn_context", {"model": "test"})
        item = fast.index_file(self.write(f"rollout-{third}.jsonl", data), self.root)
        self.assertIsNone(item["identity"]["canonical_session_id"])
        self.assertEqual(item["identity"]["identity_status"], "conflicting")

    def test_oversized_initial_metadata_has_separate_bounded_allowance(self):
        identifier = "11111111-2222-3333-4444-555555555555"
        data = self.record("session_meta", {"id": identifier, "instructions": "x" * 1_200_000})
        data += self.record("turn_context", {"model": "test"})
        item = fast.index_file(self.write(f"rollout-{identifier}.jsonl", data), self.root,
                               head_limit=1024*1024, meta_limit=2*1024*1024)
        self.assertEqual(item["identity"]["canonical_session_id"], identifier)
        self.assertTrue(item["indexing"]["session_meta_found"])
        self.assertLess(item["indexing"]["head_bytes_read"], 2*1024*1024)

    def test_large_ordinary_record_before_context_is_drained_within_head_budget(self):
        identifier = "11111111-2222-3333-4444-555555555555"
        data = self.record("session_meta", {"id": identifier})
        data += self.record("response_item", {"text": "x" * 700_000})
        data += self.record("turn_context", {"model": "model-after-large-record"})
        item = fast.index_file(self.write(f"rollout-{identifier}.jsonl", data), self.root,
                               head_limit=1024*1024, ordinary_limit=512*1024)
        self.assertEqual(item["initial_context"]["first_observed_model"], "model-after-large-record")
        self.assertTrue(item["indexing"]["turn_context_found"])
        self.assertIn("head_record_skipped", item["indexing"]["error_code"])

    def test_metadata_beyond_safe_limit_is_explicit_and_filename_only(self):
        identifier = "11111111-2222-3333-4444-555555555555"
        data = self.record("session_meta", {"id": identifier, "instructions": "x" * 1_200_000})
        item = fast.index_file(self.write(f"rollout-{identifier}.jsonl", data), self.root,
                               head_limit=1024*1024, meta_limit=1024*1024)
        self.assertEqual(item["identity"]["identity_status"], "filename_only")
        self.assertEqual(item["identity"]["identity_confidence"], "low")
        self.assertIn("session_meta_exceeds_limit", item["indexing"]["error_code"])

    def test_ambiguous_filename_has_no_invented_canonical_id(self):
        one = "11111111-2222-3333-4444-555555555555"
        two = "66666666-7777-8888-9999-aaaaaaaaaaaa"
        item = fast.index_file(self.write(f"rollout-{one}-{two}.jsonl", self.record("event_msg", {})), self.root)
        self.assertIsNone(item["identity"]["canonical_session_id"])
        self.assertEqual(item["identity"]["identity_status"], "unresolved")

    def test_tail_limit_and_trailing_invalid_are_explicit(self):
        head = self.record("session_meta", {"id": "test"}) + self.record("turn_context", {"model": "test"})
        giant = self.record("event_msg", {"body": "x" * 2000})
        item = fast.index_file(self.write("rollout-a.jsonl", head + giant), self.root, tail_limit=100)
        self.assertIsNone(item["session"]["session_last_record_at"])
        self.assertEqual(item["indexing"]["tail_status"], "record_exceeds_tail_limit")
        invalid = fast.index_file(self.write("rollout-b.jsonl", head + b"{broken}\n"), self.root)
        self.assertEqual(invalid["indexing"]["tail_status"], "prior_complete_after_invalid_tail")
        self.assertTrue(invalid["indexing"]["last_record_found"])
        self.assertEqual(invalid["indexing"]["metadata_index_status"], "needs_review")

    def test_export_is_streamed_and_existing_output_is_preserved(self):
        identifier = "11111111-2222-3333-4444-555555555555"
        self.write(f"rollout-{identifier}.jsonl", self.record("session_meta", {"id": identifier}))
        with patch.object(fast, "ROOT", self.root), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(fast.main(["--limit-files", "1"]), 0)
            output = self.root / "data/session_index_2026.json"
            original = output.read_bytes()
            parsed = json.loads(original)
            self.assertEqual(parsed["files_indexed"], 1)
            self.assertEqual(parsed["export_status"], "file_limit_reached")
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                fast.main(["--limit-files", "1"])
            self.assertEqual(output.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
