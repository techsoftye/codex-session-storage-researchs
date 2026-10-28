"""Synthetic importer tests; optional PostgreSQL tests use temporary tables only."""
import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import import_session_survey as importer
import survey_sessions as survey


class ImportTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix=".import-test-", dir=Path(__file__).parent)
        self.root = Path(self.temporary.name).resolve()
        self.addCleanup(self.temporary.cleanup)
        self.started = survey.utc_now()
        source = self.root / "2026/04/synthetic.jsonl"
        source.parent.mkdir(parents=True)
        source.write_text(json.dumps({"timestamp": "2026-01-01T00:00:00Z", "type": "session_meta",
                                      "payload": {"id": "synthetic", "source": "مصدر تجريبي ' \\."}}) + "\n", encoding="utf-8")
        self.row = survey.survey_file(source, self.root, delay=0)
        self.report = {"survey_version": survey.VERSION, "research_root": str(self.root), "source": "2026",
                       "started_at": self.started, "completed_at": survey.utc_now(), "export_status": "completed",
                       "files_written": 1, "sessions": [self.row], "file_status_counts": {"completed": 1}}
        self.path = self.root / "result.json"

    def validated(self):
        self.path.write_text(json.dumps(self.report, ensure_ascii=False), encoding="utf-8")
        with patch.object(importer, "ROOT", self.root):
            return importer.load_export(self.path)

    def test_valid_export_provenance_and_no_connection_by_default(self):
        rows, summary = self.validated()
        self.assertEqual(summary["validated_rows"], 1)
        self.assertIn("survey_import", rows[0]["metadata"])
        self.assertFalse(summary["database_written"])
        with patch.object(importer, "ROOT", self.root), patch.object(importer, "run_psql") as db:
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(importer.main(["--input", "result.json"]), 0)
            db.assert_not_called()

    def test_duplicate_paths_rejected(self):
        self.report["sessions"].append(dict(self.row))
        self.report["files_written"] = 2
        with self.assertRaisesRegex(ValueError, "Duplicate source path"):
            self.validated()

    def test_incomplete_export_rejected(self):
        self.report["export_status"] = "interrupted"
        with self.assertRaisesRegex(ValueError, "completed export"):
            self.validated()

    def test_researcher_fields_cannot_be_imported(self):
        self.row["researcher_review_status"] = "approved"
        with self.assertRaisesRegex(ValueError, "unexpected fields"):
            self.validated()

    def test_invalid_counts_rejected(self):
        self.row["scanned_record_count"] += 1
        with self.assertRaisesRegex(ValueError, "Record totals"):
            self.validated()

    def test_path_traversal_rejected(self):
        self.row["source_relative_path"] = "2026/../outside.jsonl"
        with self.assertRaisesRegex(ValueError, "relative dataset path"):
            self.validated()

    def test_unflagged_conflicts_rejected(self):
        self.row["metadata"]["conflicting_session_ids_observed"] = True
        with self.assertRaisesRegex(ValueError, "needs_review"):
            self.validated()

    def test_duplicate_json_keys_rejected(self):
        self.path.write_text('{"export_status":"completed","export_status":"interrupted"}', encoding="utf-8")
        with patch.object(importer, "ROOT", self.root), self.assertRaisesRegex(ValueError, "Duplicate JSON"):
            importer.load_export(self.path)

    @unittest.skipUnless(os.environ.get("SURVEY_IMPORT_DB_TESTS") == "1", "Explicit local temporary-table test switch required")
    def test_postgres_copy_is_idempotent_and_preserves_researcher_review(self):
        rows, _ = self.validated()
        insert = importer.import_sql(rows, "pg_temp.research_sessions")
        # A temporary LIKE table has its own identity sequence. No public INSERT/UPDATE/DELETE.
        sql = """BEGIN;
SET LOCAL statement_timeout = '30s';
CREATE TEMP TABLE research_sessions (LIKE public.research_sessions INCLUDING ALL) ON COMMIT DROP;
""" + insert + """
UPDATE pg_temp.research_sessions
SET researcher_comment = 'keep this review', researcher_review_status = 'approved',
    researcher_reviewed_by = 'synthetic reviewer', researcher_reviewed_at = CURRENT_TIMESTAMP,
    researcher_approved_at = CURRENT_TIMESTAMP, analysis_status = 'completed',
    final_analysis_run_id = 7, approved_analysis_run_id = 7;
DROP TABLE pg_temp.survey_import_stage;
""" + insert + """
SELECT json_build_object('test_rows', count(*),
    'review_preserved', bool_and(researcher_comment = 'keep this review' AND researcher_review_status = 'approved'),
    'source_preserved', bool_and(session_source LIKE '%''%'),
    'metadata_preserved', bool_and(metadata ? 'survey_import'))
FROM pg_temp.research_sessions;
ROLLBACK;
"""
        output = importer.run_psql(sql)
        results = [json.loads(line) for line in output.splitlines() if line.startswith("{")]
        self.assertEqual(results[0]["inserted_rows"], 1)
        self.assertEqual(results[1]["inserted_rows"], 0)
        self.assertEqual(results[1]["skipped_existing_rows"], 1)
        self.assertEqual(results[2], {"test_rows": 1, "review_preserved": True,
                                      "source_preserved": True, "metadata_preserved": True})


if __name__ == "__main__":
    unittest.main()
