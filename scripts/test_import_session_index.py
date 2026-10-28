"""Synthetic fast-index importer tests; PostgreSQL test uses a rolled-back temp table."""
import contextlib
import copy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import import_session_index as importer
import import_session_survey as common

HAS_DB_CONFIG = (Path(__file__).resolve().parents[1] / '.env').exists()


ID1 = "00000000-0000-4000-8000-000000000013"
ID2 = "00000000-0000-4000-8000-000000000014"


class IndexImportTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix=".index-import-test-", dir=Path(__file__).parent)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.item = {
            "file": {"file_name": f"rollout-2026-01-01T03-00-11-{ID1}.jsonl",
                     "source_relative_path": f"2026/01/01/rollout-2026-01-01T03-00-11-{ID1}.jsonl",
                     "file_size_bytes": 100, "file_format": "jsonl",
                     "file_modified_at": "2026-01-01T00:00:11Z"},
            "identity": {"candidate_session_ids": [ID1], "observed_session_meta_ids": [ID1, ID2],
                         "canonical_session_id": ID1, "identity_source": "session_meta.payload.id",
                         "identity_status": "verified", "filename_id_matches": True,
                         "identity_confidence": "high"},
            "session": {"session_started_at": "2026-01-01T00:00:10Z",
                        "session_last_record_at": "2026-01-01T00:00:11Z",
                        "session_source": "vscode", "originator": "codex_vscode",
                        "cli_version": "0.1", "model_provider": "openai"},
            "initial_context": {"first_observed_model": "test-model",
                                "first_observed_reasoning_effort": "low",
                                "first_observed_approval_policy": "never",
                                "first_observed_sandbox_mode": "read-only"},
            "indexing": {"index_version": importer.VERSION, "index_method": "head_tail",
                         "index_started_at": "2026-10-05T01:32:10Z",
                         "index_completed_at": "2026-10-05T01:32:11Z",
                         "head_bytes_read": 80, "tail_bytes_read": 100,
                         "last_record_found": True, "tail_status": "found",
                         "metadata_index_status": "completed", "full_analysis_status": "pending",
                         "error_code": None},
        }
        self.report = {"index_version": importer.VERSION, "research_root": str(self.root),
                       "source": "2026", "started_at": "2026-10-05T01:32:10Z",
                       "completed_at": "2026-10-05T01:32:11Z", "export_status": "completed",
                       "items": [self.item], "files_indexed": 1,
                       "source_bytes_represented": 100, "bytes_actually_read": 180,
                       "verified_identity_count": 1, "filename_only_identity_count": 0,
                       "unresolved_identity_count": 0, "other_identity_count": 0,
                       "error_count": 0}
        self.path = self.root / "result.json"

    def load(self):
        self.path.write_text(json.dumps(self.report), encoding="utf-8")
        with patch.object(common, "ROOT", self.root):
            return importer.load_export(self.path)

    def test_verified_single_filename_id_and_validation_only(self):
        rows, summary = self.load()
        self.assertEqual(rows[0]["session_id"], ID1)
        self.assertEqual(rows[0]["metadata"]["index_import"]["observed_session_meta_ids"], [ID1, ID2])
        self.assertEqual(summary["identified_session_ids"], 1)
        with patch.object(common, "ROOT", self.root), patch.object(common, "run_psql") as db:
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(importer.main(["--input", "result.json"]), 0)
            db.assert_not_called()

    def test_compound_filename_remains_unresolved(self):
        self.item["identity"]["candidate_session_ids"] = [ID1, ID2]
        self.item["file"]["file_name"] = f"rollout-2026-01-01T03-00-11-{ID1}_{ID2}.jsonl"
        self.item["file"]["source_relative_path"] = "2026/01/01/" + self.item["file"]["file_name"]
        rows, summary = self.load()
        row = rows[0]
        self.assertIsNone(row["session_id"])
        self.assertIsNone(row["session_started_at"])
        self.assertEqual(row["session_last_record_at"], "2026-01-01T00:00:11Z")
        self.assertEqual(row["metadata"]["index_import"]["candidate_session_ids"], [ID1, ID2])
        self.assertEqual(summary["ambiguous_session_ids"], 1)

    def test_duplicate_path_rejected(self):
        self.report["items"].append(copy.deepcopy(self.item))
        self.report["files_indexed"] = 2
        with self.assertRaisesRegex(ValueError, "Duplicate source path"):
            self.load()

    def test_incomplete_export_and_bad_summary_rejected(self):
        self.report["export_status"] = "interrupted"
        with self.assertRaisesRegex(ValueError, "completed index"):
            self.load()
        self.report["export_status"] = "completed"
        self.report["bytes_actually_read"] = 179
        with self.assertRaisesRegex(ValueError, "summary"):
            self.load()

    def test_traversal_and_unverified_identity_rejected(self):
        self.item["file"]["source_relative_path"] = f"2026/../{self.item['file']['file_name']}"
        with self.assertRaisesRegex(ValueError, "relative dataset path"):
            self.load()
        self.item["file"]["source_relative_path"] = "2026/01/01/" + self.item["file"]["file_name"]
        self.item["identity"]["identity_status"] = "conflicting"
        with self.assertRaisesRegex(ValueError, "not verified"):
            self.load()

    def test_duplicate_json_key_rejected(self):
        self.path.write_text('{"export_status":"completed","export_status":"interrupted"}', encoding="utf-8")
        with patch.object(common, "ROOT", self.root), self.assertRaisesRegex(ValueError, "Duplicate JSON"):
            importer.load_export(self.path)

    @unittest.skipUnless(HAS_DB_CONFIG, 'requires a local PostgreSQL configuration in .env')
    def test_temp_postgresql_insert_is_idempotent(self):
        rows, _ = self.load()
        insert = importer.insert_sql(rows, "pg_temp.research_sessions")
        sql = """BEGIN;
SET LOCAL statement_timeout = '30s';
CREATE TEMP TABLE research_sessions (LIKE public.research_sessions INCLUDING ALL) ON COMMIT DROP;
""" + insert + """
UPDATE pg_temp.research_sessions SET researcher_comment = 'keep';
DROP TABLE pg_temp.index_import_stage;
""" + insert + """
SELECT json_build_object('rows', count(*),
    'scan_pending', bool_and(scan_status = 'pending' AND scan_complete = false),
    'review_preserved', bool_and(researcher_comment = 'keep'),
    'metadata_present', bool_and(metadata ? 'index_import'))
FROM pg_temp.research_sessions;
ROLLBACK;
"""
        output = common.run_psql(sql)
        results = [json.loads(line) for line in output.splitlines() if line.startswith("{")]
        self.assertEqual(results[0]["inserted_rows"], 1)
        self.assertEqual(results[1]["inserted_rows"], 0)
        self.assertEqual(results[1]["skipped_existing_rows"], 1)
        self.assertEqual(results[2], {"rows": 1, "scan_pending": True,
                                      "review_preserved": True, "metadata_present": True})


if __name__ == "__main__":
    unittest.main()
