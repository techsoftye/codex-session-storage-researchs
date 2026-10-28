"""Validate a survey JSON and optionally import new paths into research_sessions.

Python 3.10+, standard library, and PostgreSQL psql. Does not read source JSONL.
Default: validation only, no database connection. Use --apply to insert.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
MAX_EXPORT_BYTES = 32 * 1024 * 1024
COUNTERS = (
    "scanned_record_count", "valid_record_count", "invalid_record_count", "session_meta_record_count",
    "response_item_record_count", "event_msg_record_count", "turn_context_record_count",
    "user_role_message_count", "assistant_role_message_count", "user_message_event_count",
    "ghost_snapshot_record_count",
)
TEXT_FIELDS = (
    "file_name", "source_relative_path", "file_format", "session_id", "identity_source",
    "session_source", "originator", "cli_version", "model_provider", "first_observed_model",
    "first_observed_reasoning_effort", "first_observed_approval_policy", "first_observed_sandbox_mode",
    "scan_version", "scan_status", "scan_error_code", "scan_error",
)
TIME_FIELDS = (
    "file_modified_at", "session_started_at", "session_last_record_at", "first_observed_event_at",
    "last_observed_event_at", "scan_started_at", "scan_completed_at",
)
BOOL_FIELDS = (
    "filename_id_matches", "instructions_observed", "uniform_envelope_observed",
    "multiple_payload_shapes_observed", "scan_complete",
)
INTEGER_FIELDS = ("file_size_bytes", "scanned_bytes", *COUNTERS)
IMPORT_FIELDS = (*TEXT_FIELDS, *TIME_FIELDS, *BOOL_FIELDS, *INTEGER_FIELDS, "metadata")
ALLOWED_STATUSES = {"completed", "partial", "needs_review"}


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON object key")
        result[key] = value
    return result


def reject_constant(value):
    raise ValueError("Non-standard JSON constant")


def parsed_time(value):
    if not isinstance(value, str) or len(value) > 100:
        raise ValueError("Invalid timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Timestamp must include a timezone")
    return parsed


def project_file(value):
    path = (ROOT / value).resolve()
    if not path.is_relative_to(ROOT):
        raise ValueError("Input must be inside the research project")
    if path.is_relative_to(ROOT / "2026"):
        raise ValueError("Importer accepts derived exports only, not files inside 2026")
    if not path.is_file():
        raise ValueError("Input file does not exist")
    return path


def load_export(path):
    # The derived export is small, unlike source JSONL. Bound the read before decoding.
    with path.open("rb") as handle:
        raw = handle.read(MAX_EXPORT_BYTES + 1)
    if len(raw) > MAX_EXPORT_BYTES:
        raise ValueError("Derived export exceeds the 32 MiB import limit")
    report = json.loads(raw.decode("utf-8-sig"), object_pairs_hook=unique_object, parse_constant=reject_constant)
    if not isinstance(report, dict) or report.get("export_status") != "completed":
        raise ValueError("Only a completed export can be imported")
    if report.get("survey_version") not in {"1.0.0", "1.0.1"}:
        raise ValueError("Unsupported survey version; review the mapping first")
    if Path(report.get("research_root", "")).resolve() != ROOT:
        raise ValueError("Export belongs to a different research root")
    source = report.get("source")
    if not isinstance(source, str) or not (source == "2026" or source.startswith("2026/")):
        raise ValueError("Unexpected export source")
    export_start = parsed_time(report.get("started_at"))
    export_end = parsed_time(report.get("completed_at"))
    if export_end < export_start:
        raise ValueError("Export time range is invalid")
    rows = report.get("sessions")
    if not isinstance(rows, list) or not rows or type(report.get("files_written")) is not int or report["files_written"] != len(rows):
        raise ValueError("Export row count is missing, empty, or inconsistent")
    digest = hashlib.sha256(raw).hexdigest()
    seen = set()
    counts = {}
    for index, row in enumerate(rows, 1):
        try:
            validate_row(row, report, source, export_start, export_end, seen)
        except (TypeError, ValueError, KeyError) as error:
            raise ValueError(f"Row {index} rejected: {error}") from None
        status = row["scan_status"]
        counts[status] = counts.get(status, 0) + 1
        row["metadata"]["survey_import"] = {
            "export_relative_path": path.relative_to(ROOT).as_posix(),
            "export_sha256": digest,
            "survey_version": report["survey_version"],
            "export_completed_at": report["completed_at"],
        }
    if report.get("file_status_counts") != counts:
        raise ValueError("Export status totals do not match its rows")
    return rows, {"validated_rows": len(rows), "status_counts": counts,
                  "export_sha256": digest, "database_written": False}


def validate_row(row, report, source, export_start, export_end, seen):
    if not isinstance(row, dict) or set(row) != set(IMPORT_FIELDS):
        raise ValueError("Missing or unexpected fields; review the export schema")
    for field in TEXT_FIELDS:
        if row[field] is not None and (not isinstance(row[field], str) or "\x00" in row[field]):
            raise ValueError(f"Invalid text field: {field}")
    for field in INTEGER_FIELDS:
        if type(row[field]) is not int or not 0 <= row[field] <= 9223372036854775807:
            raise ValueError(f"Invalid nonnegative integer: {field}")
    for field in BOOL_FIELDS:
        if row[field] is not None and type(row[field]) is not bool:
            raise ValueError(f"Invalid boolean: {field}")
    if type(row["scan_complete"]) is not bool:
        raise ValueError("scan_complete must be boolean")
    for field in TIME_FIELDS:
        if row[field] is not None:
            parsed_time(row[field])
    scan_start = parsed_time(row["scan_started_at"])
    scan_end = parsed_time(row["scan_completed_at"])
    if not export_start <= scan_start <= scan_end <= export_end:
        raise ValueError("Scan times fall outside the export time range")
    relative = row["source_relative_path"]
    if not relative:
        raise ValueError("Missing source path")
    relative = relative.replace("\\", "/")
    parts = relative.split("/")
    if parts[0] != "2026" or any(part in {"", ".", ".."} or ":" in part for part in parts):
        raise ValueError("Source path must be a normalized relative dataset path")
    path = PurePosixPath(relative)
    if path.suffix.lower() != ".jsonl" or row["file_name"] != path.name or row["file_format"] != "jsonl":
        raise ValueError("File name, format, and source path disagree")
    if relative != source and not relative.startswith(source.rstrip("/") + "/"):
        raise ValueError("Row is outside the declared export source")
    if relative in seen:
        raise ValueError("Duplicate source path")
    seen.add(relative)
    row["source_relative_path"] = relative
    if row["scan_version"] != report["survey_version"] or row["scan_status"] not in ALLOWED_STATUSES:
        raise ValueError("Unsupported scan version or status")
    if row["scan_error"] is not None or row["scan_error_code"] is not None:
        raise ValueError("Scan errors require review before import")
    metadata = row["metadata"]
    if not isinstance(metadata, dict) or "survey_import" in metadata:
        raise ValueError("Invalid or already modified metadata")
    skipped = metadata.get("oversized_record_count")
    if type(skipped) is not int or skipped < 0:
        raise ValueError("Missing oversized-record accounting")
    if row["scanned_record_count"] != row["valid_record_count"] + row["invalid_record_count"] + skipped:
        raise ValueError("Record totals disagree")
    if row["scanned_bytes"] > row["file_size_bytes"]:
        raise ValueError("Scanned bytes exceed source size")
    if row["scan_complete"] and row["scanned_bytes"] != row["file_size_bytes"]:
        raise ValueError("Complete scan does not cover the file")
    if row["scan_status"] == "completed" and not row["scan_complete"]:
        raise ValueError("Completed status without complete scan")
    if row["scan_status"] == "partial" and row["scan_complete"]:
        raise ValueError("Partial status with complete scan")
    if (skipped or row["invalid_record_count"] or metadata.get("conflicting_session_ids_observed")) and row["scan_status"] != "needs_review":
        raise ValueError("Data-quality flags require needs_review")


def connection_settings():
    values = {}
    for line in (ROOT / ".env").read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ValueError("Invalid .env entry")
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("\"'")
    required = ("DB_HOST", "DB_PORT", "DB_DATABASE", "DB_USERNAME", "DB_PASSWORD")
    if any(not values.get(key) for key in required):
        raise ValueError("Required database settings are missing from .env")
    if values["DB_DATABASE"] != "codex_session_research" or values["DB_HOST"] not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("Importer is restricted to the local codex_session_research database")
    if not values["DB_PORT"].isdigit() or not 1 <= int(values["DB_PORT"]) <= 65535:
        raise ValueError("Invalid database port")
    return values


def find_psql():
    executable = shutil.which("psql")
    if executable:
        return executable
    local = Path(os.environ.get("ProgramFiles", "C:/Program Files")) / "PostgreSQL/18/bin/psql.exe"
    if local.is_file():
        return str(local)
    raise ValueError("psql was not found; install the PostgreSQL command-line client or add it to PATH")


def import_sql(rows, target="public.research_sessions"):
    # The alternative target is exclusively for isolated integration tests.
    if target not in {"public.research_sessions", "pg_temp.research_sessions"}:
        raise ValueError("Invalid SQL target")
    stream = io.StringIO()
    stream.write("CREATE TEMP TABLE survey_import_stage (row_data jsonb NOT NULL) ON COMMIT DROP;\n")
    stream.write("COPY survey_import_stage (row_data) FROM STDIN WITH (FORMAT csv);\n")
    writer = csv.writer(stream, quoting=csv.QUOTE_ALL, lineterminator="\n")
    for row in rows:
        writer.writerow([json.dumps(row, ensure_ascii=False, allow_nan=False)])
    stream.write("\\.\n")
    fields = ", ".join(IMPORT_FIELDS)
    select_fields = ", ".join("r." + field for field in IMPORT_FIELDS)
    stream.write(f"""
WITH inserted AS (
    INSERT INTO {target} ({fields}, first_inventoried_at, last_inventoried_at)
    SELECT {select_fields}, r.scan_started_at, r.scan_completed_at
    FROM survey_import_stage s
    CROSS JOIN LATERAL jsonb_populate_record(NULL::{target}, s.row_data) r
    ON CONFLICT (source_relative_path) DO NOTHING
    RETURNING id
)
SELECT json_build_object(
    'input_rows', (SELECT count(*) FROM survey_import_stage),
    'inserted_rows', (SELECT count(*) FROM inserted),
    'skipped_existing_rows', (SELECT count(*) FROM survey_import_stage) - (SELECT count(*) FROM inserted)
);
""")
    return stream.getvalue()


def run_psql(sql):
    config = connection_settings()
    env = os.environ.copy()
    env.update(PGPASSWORD=config["DB_PASSWORD"], PGCONNECT_TIMEOUT="5", PGCLIENTENCODING="UTF8")
    command = [find_psql(), "-X", "-w", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=sqlstate",
               "-h", config["DB_HOST"], "-p", config["DB_PORT"], "-U", config["DB_USERNAME"], "-d", config["DB_DATABASE"]]
    process = subprocess.run(command, input=sql, text=True, encoding="utf-8", capture_output=True, env=env, timeout=120)
    if process.returncode:
        raise RuntimeError(f"PostgreSQL operation failed (exit {process.returncode}); transaction was not committed.")
    return process.stdout


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Derived survey JSON path inside the project")
    parser.add_argument("--apply", action="store_true", help="Insert new paths into PostgreSQL; default validates only")
    args = parser.parse_args(argv)
    try:
        rows, summary = load_export(project_file(args.input))
        if args.apply:
            sql = """BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
DO $$ BEGIN
    IF current_database() <> 'codex_session_research' THEN
        RAISE EXCEPTION 'Wrong database';
    END IF;
END $$;
""" + import_sql(rows) + "COMMIT;\n"
            output = run_psql(sql)
            results = [json.loads(line) for line in output.splitlines() if line.strip().startswith("{")]
            if len(results) != 1:
                raise RuntimeError("Unexpected confirmation; inspect database before retrying (existing paths will be skipped).")
            summary.update(results[0], database_written=True)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    except (OSError, ValueError, TypeError, RecursionError, RuntimeError, subprocess.TimeoutExpired) as error:
        print(f"Import stopped: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
