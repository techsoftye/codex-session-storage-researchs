"""Validate a fast metadata index and optionally insert new research_sessions rows.

Reads the derived JSON export only. Default is validation without a database connection.
The index is a file inventory, not a complete JSONL scan or researcher approval.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
from pathlib import PurePosixPath
import re
import sys

import import_session_survey as common

VERSION = "1.0.2"
FIELDS = (
    "file_name", "source_relative_path", "file_size_bytes", "file_format",
    "file_modified_at", "session_id", "identity_source", "filename_id_matches",
    "session_started_at", "session_last_record_at", "session_source", "originator",
    "cli_version", "model_provider", "first_observed_model",
    "first_observed_reasoning_effort", "first_observed_approval_policy",
    "first_observed_sandbox_mode", "first_inventoried_at", "last_inventoried_at",
    "metadata",
)


def nonnegative(value, name):
    if type(value) is not int or not 0 <= value <= 9223372036854775807:
        raise ValueError(f"Invalid nonnegative integer: {name}")
    return value


def optional_time(value, name):
    if value is not None:
        common.parsed_time(value)
    return value


def optional_text(value, name):
    if value is not None and (not isinstance(value, str) or "\x00" in value or len(value) > 2048):
        raise ValueError(f"Invalid text: {name}")
    return value


def identifier_list(value, name):
    if not isinstance(value, list) or not value or len(value) > 64:
        raise ValueError(f"Invalid identifier list: {name}")
    for identifier in value:
        if not isinstance(identifier, str) or not re.fullmatch(
            r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", identifier
        ):
            raise ValueError(f"Invalid identifier in {name}")
    if len({identifier.lower() for identifier in value}) != len(value):
        raise ValueError(f"Repeated identifier in {name}")
    return value


def validate_item(item, source, seen_paths, export_path, digest, version):
    if not isinstance(item, dict) or not all(isinstance(item.get(k), dict) for k in
                                             ("file", "identity", "session", "initial_context", "indexing")):
        raise ValueError("Missing index item sections")
    file, identity, session = item["file"], item["identity"], item["session"]
    context, index = item["initial_context"], item["indexing"]
    relative = file.get("source_relative_path")
    if not isinstance(relative, str) or "\\" in relative or "\x00" in relative:
        raise ValueError("Invalid source path")
    parts = relative.split("/")
    if parts[0] != "2026" or any(not p or p in {".", ".."} or ":" in p for p in parts):
        raise ValueError("Source path must be a normalized relative dataset path")
    if relative != source and not relative.startswith(source.rstrip("/") + "/"):
        raise ValueError("Index item is outside declared source")
    name = file.get("file_name")
    if not isinstance(name, str) or name != PurePosixPath(relative).name or not name.endswith(".jsonl"):
        raise ValueError("File name and path disagree")
    if relative in seen_paths:
        raise ValueError("Duplicate source path")
    seen_paths.add(relative)
    if file.get("file_format") != "jsonl":
        raise ValueError("Unexpected file format")
    size = nonnegative(file.get("file_size_bytes"), "file_size_bytes")
    modified = optional_time(file.get("file_modified_at"), "file_modified_at")
    candidates = identifier_list(identity.get("candidate_session_ids"), "candidate_session_ids")
    observed = identifier_list(identity.get("observed_session_meta_ids"), "observed_session_meta_ids")
    canonical = identity.get("canonical_session_id")
    if (identity.get("identity_status") != "verified" or
            identity.get("identity_source") != "session_meta.payload.id" or
            identity.get("filename_id_matches") is not True or
            not isinstance(canonical, str) or canonical.lower() != observed[0].lower() or
            canonical.lower() not in {candidate.lower() for candidate in candidates}):
        raise ValueError("Index identity is not verified")
    if index.get("index_version") != version or index.get("index_method") != "head_tail":
        raise ValueError("Unsupported item index method/version")
    status = index.get("metadata_index_status")
    if status not in {"completed", "needs_review"} or index.get("full_analysis_status") != "pending":
        raise ValueError("Index item is failed or claims full analysis")
    if index.get("last_record_found") is not True or index.get("tail_status") != "found":
        raise ValueError("Last record was not verified")
    head = nonnegative(index.get("head_bytes_read"), "head_bytes_read")
    tail = nonnegative(index.get("tail_bytes_read"), "tail_bytes_read")
    started = common.parsed_time(index.get("index_started_at"))
    completed = common.parsed_time(index.get("index_completed_at"))
    if completed < started:
        raise ValueError("Index item time range is invalid")
    error_code = optional_text(index.get("error_code"), "error_code")
    if status == "completed" and error_code is not None:
        raise ValueError("Completed index item has an error code")
    if status == "needs_review" and error_code is None:
        raise ValueError("Needs-review index item lacks reason")
    # A compound filename has more than one candidate. Do not promote an
    # inherited parent's first metadata ID to this file's session_id.
    unambiguous = len(candidates) == 1
    for group in (session, context):
        for key, value in group.items():
            if key.endswith("_at"):
                optional_time(value, key)
            else:
                optional_text(value, key)
    metadata = {
        "index_import": {
            "export_relative_path": export_path,
            "export_sha256": digest,
            "index_version": version,
            "candidate_session_ids": candidates,
            "observed_session_meta_ids": observed,
            "identity_status": identity["identity_status"],
            "identity_confidence": identity.get("identity_confidence"),
            "session_id_resolution": "first_metadata_and_single_filename_id" if unambiguous else "ambiguous_compound_filename",
            "metadata_index_status": status,
            "index_error_code": error_code,
            "index_method": index["index_method"],
            "head_bytes_read": head,
            "tail_bytes_read": tail,
            "full_analysis_status": index["full_analysis_status"],
        }
    }
    row = {
        "file_name": name, "source_relative_path": relative, "file_size_bytes": size,
        "file_format": "jsonl", "file_modified_at": modified,
        "session_id": canonical if unambiguous else None,
        "identity_source": identity["identity_source"] if unambiguous else "unresolved_multiple_filename_ids",
        "filename_id_matches": True if unambiguous else None,
        "session_started_at": session.get("session_started_at") if unambiguous else None,
        "session_last_record_at": session.get("session_last_record_at"),
        "session_source": session.get("session_source") if unambiguous else None,
        "originator": session.get("originator") if unambiguous else None,
        "cli_version": session.get("cli_version") if unambiguous else None,
        "model_provider": session.get("model_provider") if unambiguous else None,
        "first_observed_model": context.get("first_observed_model") if unambiguous else None,
        "first_observed_reasoning_effort": context.get("first_observed_reasoning_effort") if unambiguous else None,
        "first_observed_approval_policy": context.get("first_observed_approval_policy") if unambiguous else None,
        "first_observed_sandbox_mode": context.get("first_observed_sandbox_mode") if unambiguous else None,
        "first_inventoried_at": index["index_started_at"],
        "last_inventoried_at": index["index_completed_at"],
        "metadata": metadata,
    }
    return row, status, size, head + tail, unambiguous


def load_export(path):
    with path.open("rb") as handle:
        raw = handle.read(common.MAX_EXPORT_BYTES + 1)
    if len(raw) > common.MAX_EXPORT_BYTES:
        raise ValueError("Derived export exceeds the 32 MiB import limit")
    report = json.loads(raw.decode("utf-8-sig"), object_pairs_hook=common.unique_object,
                        parse_constant=common.reject_constant)
    if not isinstance(report, dict) or report.get("export_status") != "completed":
        raise ValueError("Only a completed index can be imported")
    if report.get("index_version") != VERSION:
        raise ValueError("Unsupported index version")
    if common.Path(report.get("research_root", "")).resolve() != common.ROOT:
        raise ValueError("Index belongs to a different research root")
    source = report.get("source")
    if not isinstance(source, str) or not (source == "2026" or source.startswith("2026/")):
        raise ValueError("Unexpected index source")
    if common.parsed_time(report.get("completed_at")) < common.parsed_time(report.get("started_at")):
        raise ValueError("Index time range is invalid")
    items = report.get("items")
    if not isinstance(items, list) or not items or report.get("files_indexed") != len(items):
        raise ValueError("Index item count is missing or inconsistent")
    digest = hashlib.sha256(raw).hexdigest()
    seen_paths = set()
    rows, statuses = [], {}
    bytes_represented = bytes_read = clear_ids = 0
    for number, item in enumerate(items, 1):
        try:
            row, status, size, read, unambiguous = validate_item(
                item, source, seen_paths, path.relative_to(common.ROOT).as_posix(), digest, VERSION)
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(f"Item {number} rejected: {error}") from None
        rows.append(row)
        statuses[status] = statuses.get(status, 0) + 1
        bytes_represented += size
        bytes_read += read
        clear_ids += unambiguous
    if (report.get("verified_identity_count") != len(rows) or
            report.get("filename_only_identity_count") != 0 or
            report.get("unresolved_identity_count") != 0 or
            report.get("other_identity_count") != 0 or report.get("error_count") != 0 or
            report.get("source_bytes_represented") != bytes_represented or
            report.get("bytes_actually_read") != bytes_read):
        raise ValueError("Index summary does not match its items")
    summary = {"validated_rows": len(rows), "identified_session_ids": clear_ids,
               "ambiguous_session_ids": len(rows) - clear_ids,
               "metadata_index_status_counts": statuses, "export_sha256": digest,
               "database_written": False}
    return rows, summary


def insert_sql(rows, target="public.research_sessions"):
    if target not in {"public.research_sessions", "pg_temp.research_sessions"}:
        raise ValueError("Invalid SQL target")
    stream = io.StringIO()
    stream.write("CREATE TEMP TABLE index_import_stage (row_data jsonb NOT NULL) ON COMMIT DROP;\n")
    stream.write("COPY index_import_stage (row_data) FROM STDIN WITH (FORMAT csv);\n")
    writer = csv.writer(stream, quoting=csv.QUOTE_ALL, lineterminator="\n")
    for row in rows:
        writer.writerow([json.dumps(row, ensure_ascii=False, allow_nan=False)])
    stream.write("\\.\n")
    fields = ", ".join(FIELDS)
    values = ", ".join("r." + field for field in FIELDS)
    stream.write(f"""
WITH inserted AS (
    INSERT INTO {target} ({fields})
    SELECT {values}
    FROM index_import_stage s
    CROSS JOIN LATERAL jsonb_populate_record(NULL::{target}, s.row_data) r
    ON CONFLICT (source_relative_path) DO NOTHING
    RETURNING id
)
SELECT json_build_object(
    'input_rows', (SELECT count(*) FROM index_import_stage),
    'inserted_rows', (SELECT count(*) FROM inserted),
    'skipped_existing_rows', (SELECT count(*) FROM index_import_stage) - (SELECT count(*) FROM inserted)
);
""")
    return stream.getvalue()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Derived fast index JSON inside the project")
    parser.add_argument("--apply", action="store_true", help="Insert new file paths into PostgreSQL")
    args = parser.parse_args(argv)
    try:
        rows, summary = load_export(common.project_file(args.input))
        if args.apply:
            sql = """BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
DO $$ BEGIN
    IF current_database() <> 'codex_session_research' THEN
        RAISE EXCEPTION 'Wrong database';
    END IF;
END $$;
""" + insert_sql(rows) + "COMMIT;\n"
            output = common.run_psql(sql)
            results = [json.loads(line) for line in output.splitlines() if line.strip().startswith("{")]
            if len(results) != 1:
                raise RuntimeError("Unexpected database confirmation; inspect existing rows before retrying")
            summary.update(results[0], database_written=True)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    except (OSError, ValueError, TypeError, RecursionError, RuntimeError,
            common.subprocess.TimeoutExpired) as error:
        print(f"Index import stopped: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
