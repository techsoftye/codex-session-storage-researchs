"""Synthetic evidence tests; no research-session files or database are opened."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import resolve_compound_session_ids as resolver


FIRST = "00000000-0000-4000-8000-000000000011"
SECOND = "00000000-0000-4000-8000-000000000012"


class ResolverTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix=".compound-test-", dir=Path(__file__).parent)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.name = f"rollout-2026-01-01T03-00-10-{FIRST}_{SECOND}.jsonl"
        self.relative = "2026/01/01/" + self.name
        self.source = self.root / self.relative
        self.source.parent.mkdir(parents=True)
        self.meta = {"timestamp": "2026-01-01T00:00:10Z", "type": "session_meta",
                     "payload": {"id": FIRST, "session_id": FIRST,
                                 "timestamp": "2026-01-01T00:00:10Z",
                                 "history_base": {"thread_id": FIRST}, "forked_from_id": "different-parent"}}
        self.event = {"timestamp": "2026-01-01T00:00:11Z", "type": "event_msg",
                      "payload": {"type": "thread_settings_applied", "thread_id": FIRST}}

    def item(self, extra=None):
        records = [self.meta, self.event]
        if extra:
            records += extra
        self.source.write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")
        return {"file": {"source_relative_path": self.relative, "file_name": self.name,
                         "file_size_bytes": self.source.stat().st_size},
                "identity": {"candidate_session_ids": [FIRST, SECOND]}}

    def test_metadata_settings_and_filename_time_verify_first_id(self):
        item = self.item(extra=[{"timestamp": "2026-01-01T00:00:12Z", "type": "response_item",
                                 "payload": {"content": "never exported"}}])
        with patch.object(resolver, "ROOT", self.root):
            result = resolver.examine(item)
        self.assertEqual(result["selected_session_id"], FIRST)
        self.assertEqual(result["decision"], "verified_first_filename_id")
        self.assertEqual(result["filename_timestamp_delta_seconds"], 0)
        self.assertNotIn("never exported", json.dumps(result))
        self.assertLess(result["bytes_read"], self.source.stat().st_size)

    def test_conflicting_settings_id_stays_unresolved(self):
        self.event["payload"]["thread_id"] = SECOND
        item = self.item()
        with patch.object(resolver, "ROOT", self.root):
            result = resolver.examine(item)
        self.assertIsNone(result["selected_session_id"])
        self.assertEqual(result["reason"], "settings_thread_id_matches_second_filename_id")

    def test_history_base_can_reference_an_earlier_segment(self):
        self.meta["payload"]["history_base"]["thread_id"] = SECOND
        item = self.item()
        with patch.object(resolver, "ROOT", self.root):
            result = resolver.examine(item)
        self.assertEqual(result["selected_session_id"], FIRST)
        self.assertEqual(result["history_base_thread_id"], SECOND)

    def test_settings_event_without_thread_id_uses_metadata_and_time(self):
        self.event["payload"].pop("thread_id")
        item = self.item()
        with patch.object(resolver, "ROOT", self.root):
            result = resolver.examine(item)
        self.assertEqual(result["selected_session_id"], FIRST)
        self.assertTrue(result["thread_settings_seen"])

    def test_changed_file_size_stays_unresolved(self):
        item = self.item()
        item["file"]["file_size_bytes"] += 1
        with patch.object(resolver, "ROOT", self.root):
            result = resolver.examine(item)
        self.assertEqual(result["reason"], "file_size_changed_since_index")
        self.assertEqual(result["bytes_read"], 0)


if __name__ == "__main__":
    unittest.main()
