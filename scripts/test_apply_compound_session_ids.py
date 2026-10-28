"""Synthetic plan-validation tests; these never update PostgreSQL."""
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import apply_compound_session_ids as updater
import import_session_survey as common


FIRST = "00000000-0000-4000-8000-000000000011"
SECOND = "00000000-0000-4000-8000-000000000012"


class UpdatePlanTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix=".identity-update-test-", dir=Path(__file__).parent)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.path = f"2026/01/01/rollout-2026-01-01T03-00-10-{FIRST}_{SECOND}.jsonl"
        self.index_item = {
            "file": {"source_relative_path": self.path, "file_size_bytes": 100},
            "identity": {"candidate_session_ids": [FIRST, SECOND], "canonical_session_id": FIRST},
            "session": {"session_started_at": "2026-01-01T00:00:10Z", "session_source": "vscode",
                        "originator": "Codex Desktop", "cli_version": "0.1", "model_provider": "openai"},
            "initial_context": {"first_observed_model": None,
                                "first_observed_reasoning_effort": None,
                                "first_observed_approval_policy": None,
                                "first_observed_sandbox_mode": None},
        }
        self.index = {"index_version": updater.INDEX_VERSION, "export_status": "completed",
                      "source": "2026", "research_root": str(self.root),
                      "files_indexed": 1, "items": [self.index_item]}
        self.proof = {"source_relative_path": self.path, "file_size_bytes": 100,
                      "filename_candidates": [FIRST, SECOND], "selected_session_id": FIRST,
                      "decision": "verified_first_filename_id", "session_meta_id": FIRST,
                      "session_meta_session_id": FIRST, "session_meta_timestamp": "2026-01-01T00:00:10Z",
                      "thread_settings_thread_id": FIRST, "filename_timestamp_delta_seconds": 0,
                      "bytes_read": 80}
        self.report = {"report_version": updater.REPORT_VERSION,
                       "report_scope": "all_compound_names", "filename_time_offset_assumed": "+03:00",
                       "files_examined": 1, "verified_first_id_count": 1, "unresolved_count": 0,
                       "bytes_actually_read": 80, "completed_at": "2026-10-05T00:00:00Z",
                       "source_index": "data/index.json", "items": [self.proof]}
        (self.root / "data").mkdir()

    def load(self):
        (self.root / "data/index.json").write_text(json.dumps(self.index), encoding="utf-8")
        path = self.root / "data/report.json"
        path.write_text(json.dumps(self.report), encoding="utf-8")
        with patch.object(updater, "ROOT", self.root), patch.object(common, "ROOT", self.root), \
                patch.object(updater, "EXPECTED_ROWS", 1):
            return updater.load_plan(path)

    def test_valid_plan_and_validation_does_not_connect(self):
        plan, summary = self.load()
        self.assertEqual(plan[0]["selected_session_id"], FIRST)
        self.assertFalse(summary["database_written"])
        with patch.object(updater, "ROOT", self.root), patch.object(common, "ROOT", self.root), \
                patch.object(updater, "EXPECTED_ROWS", 1), patch.object(common, "run_psql") as db:
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(updater.main(["--report", "data/report.json"]), 0)
            db.assert_not_called()

    def test_second_candidate_rejected(self):
        self.proof["selected_session_id"] = SECOND
        with self.assertRaisesRegex(ValueError, "disagrees"):
            self.load()

    def test_incomplete_report_rejected(self):
        self.report["unresolved_count"] = 1
        with self.assertRaisesRegex(ValueError, "incomplete"):
            self.load()

    def test_mismatched_start_time_rejected(self):
        self.proof["session_meta_timestamp"] = "2026-01-01T00:00:11Z"
        with self.assertRaisesRegex(ValueError, "start time"):
            self.load()


if __name__ == "__main__":
    unittest.main()
