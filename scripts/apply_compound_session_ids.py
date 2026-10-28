"""Validate compound-name identity evidence and optionally update 18 existing rows.

Reads derived JSON reports only. Default validates without connecting to PostgreSQL.
--apply changes only identity fields on the 18 previously unresolved file rows.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime
import hashlib
import io
import json
from pathlib import Path
import sys

import import_session_survey as common

ROOT = Path(__file__).resolve().parents[1]
REPORT_VERSION = "1.0.1"
INDEX_VERSION = "1.0.2"
EXPECTED_ROWS = 18


def read_json(path):
    with path.open("rb") as handle:
        raw = handle.read(common.MAX_EXPORT_BYTES + 1)
    if len(raw) > common.MAX_EXPORT_BYTES:
        raise ValueError("Derived JSON exceeds the 32 MiB limit")
    value = json.loads(raw.decode("utf-8-sig"), object_pairs_hook=common.unique_object,
                       parse_constant=common.reject_constant)
    if not isinstance(value, dict):
        raise ValueError("Expected a JSON object")
    return value, hashlib.sha256(raw).hexdigest()


def load_plan(report_path):
    report, report_sha = read_json(report_path)
    if (report.get("report_version") != REPORT_VERSION or
            report.get("report_scope") != "all_compound_names" or
            report.get("filename_time_offset_assumed") != "+03:00" or
            report.get("files_examined") != EXPECTED_ROWS or
            report.get("verified_first_id_count") != EXPECTED_ROWS or
            report.get("unresolved_count") != 0):
        raise ValueError("Identity report is incomplete or unsupported")
    items = report.get("items")
    if not isinstance(items, list) or len(items) != EXPECTED_ROWS:
        raise ValueError("Identity report row count disagrees")
    if common.parsed_time(report.get("completed_at")) > datetime.now().astimezone():
        raise ValueError("Identity report completion time is in the future")
    index_relative = report.get("source_index")
    if not isinstance(index_relative, str):
        raise ValueError("Identity report has no source index")
    index_path = common.project_file(index_relative)
    index, index_sha = read_json(index_path)
    if (index.get("index_version") != INDEX_VERSION or index.get("export_status") != "completed" or
            index.get("source") != "2026" or
            Path(index.get("research_root", "")).resolve() != ROOT or
            index.get("files_indexed") != len(index.get("items", []))):
        raise ValueError("Source index is incomplete or belongs to another project")
    compound = {}
    for indexed in index["items"]:
        file = indexed.get("file", {})
        identity = indexed.get("identity", {})
        if len(identity.get("candidate_session_ids", [])) == 2:
            path = file.get("source_relative_path")
            if not isinstance(path, str) or path in compound:
                raise ValueError("Duplicate or invalid compound source path")
            compound[path] = indexed
    if len(compound) != EXPECTED_ROWS:
        raise ValueError("Source index does not contain exactly 18 compound-name files")
    seen, plan = set(), []
    bytes_read = 0
    for number, proof in enumerate(items, 1):
        if not isinstance(proof, dict):
            raise ValueError(f"Evidence row {number} is invalid")
        path = proof.get("source_relative_path")
        if path not in compound or path in seen:
            raise ValueError(f"Evidence row {number} has an unknown or repeated path")
        seen.add(path)
        indexed = compound[path]
        file, identity = indexed["file"], indexed["identity"]
        candidates = identity["candidate_session_ids"]
        selected = proof.get("selected_session_id")
        if (proof.get("decision") != "verified_first_filename_id" or
                proof.get("filename_candidates") != candidates or
                selected != candidates[0] or
                proof.get("session_meta_id") != selected or
                proof.get("session_meta_session_id") != selected or
                proof.get("file_size_bytes") != file["file_size_bytes"] or
                identity.get("canonical_session_id") != selected):
            raise ValueError(f"Evidence row {number} disagrees with the source index")
        event_id = proof.get("thread_settings_thread_id")
        if event_id is not None and event_id != selected:
            raise ValueError(f"Evidence row {number} has a conflicting settings thread ID")
        delta = proof.get("filename_timestamp_delta_seconds")
        if type(delta) not in {int, float} or not 0 <= delta < 2:
            raise ValueError(f"Evidence row {number} lacks filename-time confirmation")
        meta_time = common.parsed_time(proof.get("session_meta_timestamp"))
        index_time = common.parsed_time(indexed["session"].get("session_started_at"))
        if meta_time != index_time:
            raise ValueError(f"Evidence row {number} start time differs from the index")
        read = proof.get("bytes_read")
        if type(read) is not int or read <= 0 or read > 17 * 1024 * 1024:
            raise ValueError(f"Evidence row {number} has invalid read accounting")
        bytes_read += read
        session, context = indexed["session"], indexed["initial_context"]
        plan.append({
            "source_relative_path": path, "file_size_bytes": file["file_size_bytes"],
            "selected_session_id": selected, "filename_candidates": candidates,
            "index_sha256": index_sha, "report_sha256": report_sha,
            "report_relative_path": report_path.relative_to(ROOT).as_posix(),
            "session_started_at": session["session_started_at"],
            "session_source": session.get("session_source"), "originator": session.get("originator"),
            "cli_version": session.get("cli_version"), "model_provider": session.get("model_provider"),
            "first_observed_model": context.get("first_observed_model"),
            "first_observed_reasoning_effort": context.get("first_observed_reasoning_effort"),
            "first_observed_approval_policy": context.get("first_observed_approval_policy"),
            "first_observed_sandbox_mode": context.get("first_observed_sandbox_mode"),
        })
    if seen != set(compound) or report.get("bytes_actually_read") != bytes_read:
        raise ValueError("Identity report does not cover exactly the compound-name files")
    return plan, {"validated_rows": len(plan), "report_sha256": report_sha,
                  "source_index_sha256": index_sha, "database_written": False}


def update_sql(plan, target="public.research_sessions"):
    if target not in {"public.research_sessions", "pg_temp.research_sessions"}:
        raise ValueError("Invalid SQL target")
    stream = io.StringIO()
    stream.write("CREATE TEMP TABLE compound_identity_stage (row_data jsonb NOT NULL) ON COMMIT DROP;\n")
    stream.write("COPY compound_identity_stage (row_data) FROM STDIN WITH (FORMAT csv);\n")
    writer = csv.writer(stream, quoting=csv.QUOTE_ALL, lineterminator="\n")
    for row in plan:
        writer.writerow([json.dumps(row, ensure_ascii=False, allow_nan=False)])
    stream.write("\\.\n")
    stream.write(f"""
DO $$
DECLARE bad_count integer;
BEGIN
    SELECT count(*) INTO bad_count
    FROM compound_identity_stage s
    LEFT JOIN {target} r ON r.source_relative_path = s.row_data->>'source_relative_path'
    WHERE r.id IS NULL
       OR r.file_size_bytes <> (s.row_data->>'file_size_bytes')::bigint
       OR r.session_id IS NOT NULL
       OR r.identity_source IS DISTINCT FROM 'unresolved_multiple_filename_ids'
       OR r.filename_id_matches IS NOT NULL
       OR r.session_started_at IS NOT NULL
       OR r.metadata #>> '{{index_import,session_id_resolution}}' IS DISTINCT FROM 'ambiguous_compound_filename'
       OR r.metadata #>> '{{index_import,export_sha256}}' IS DISTINCT FROM s.row_data->>'index_sha256'
       OR r.metadata #> '{{index_import,candidate_session_ids}}' IS DISTINCT FROM s.row_data->'filename_candidates';
    IF bad_count <> 0 THEN
        RAISE EXCEPTION 'Compound identity preconditions failed for % row(s)', bad_count;
    END IF;
END $$;
CREATE TEMP TABLE compound_identity_result AS
WITH changed AS (
    UPDATE {target} r
    SET session_id = s.row_data->>'selected_session_id',
        identity_source = 'session_meta.payload.id',
        filename_id_matches = true,
        session_started_at = (s.row_data->>'session_started_at')::timestamptz,
        session_source = s.row_data->>'session_source',
        originator = s.row_data->>'originator',
        cli_version = s.row_data->>'cli_version',
        model_provider = s.row_data->>'model_provider',
        first_observed_model = s.row_data->>'first_observed_model',
        first_observed_reasoning_effort = s.row_data->>'first_observed_reasoning_effort',
        first_observed_approval_policy = s.row_data->>'first_observed_approval_policy',
        first_observed_sandbox_mode = s.row_data->>'first_observed_sandbox_mode',
        metadata = jsonb_set(
            jsonb_set(r.metadata, '{{index_import,session_id_resolution}}',
                      '"confirmed_compound_filename_first_id"'::jsonb),
            '{{identity_resolution}}',
            jsonb_build_object('report_relative_path', s.row_data->>'report_relative_path',
                               'report_sha256', s.row_data->>'report_sha256',
                               'decision', 'verified_first_filename_id'), true),
        updated_at = CURRENT_TIMESTAMP
    FROM compound_identity_stage s
    WHERE r.source_relative_path = s.row_data->>'source_relative_path'
    RETURNING r.id
)
SELECT count(*)::integer AS updated_rows FROM changed;
DO $$
BEGIN
    IF (SELECT updated_rows FROM compound_identity_result) <> {len(plan)} THEN
        RAISE EXCEPTION 'Compound identity update count mismatch';
    END IF;
END $$;
SELECT json_build_object('input_rows', (SELECT count(*) FROM compound_identity_stage),
                         'updated_rows', (SELECT updated_rows FROM compound_identity_result));
""")
    return stream.getvalue()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", default="data/compound_session_id_evidence_2026_v101.json")
    parser.add_argument("--apply", action="store_true", help="Update exactly the 18 existing PostgreSQL rows")
    args = parser.parse_args(argv)
    try:
        plan, summary = load_plan(common.project_file(args.report))
        if args.apply:
            sql = """BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
DO $$ BEGIN
    IF current_database() <> 'codex_session_research' THEN
        RAISE EXCEPTION 'Wrong database';
    END IF;
END $$;
""" + update_sql(plan) + "COMMIT;\n"
            output = common.run_psql(sql)
            results = [json.loads(line) for line in output.splitlines() if line.strip().startswith("{")]
            if len(results) != 1 or results[0].get("updated_rows") != EXPECTED_ROWS:
                raise RuntimeError("Unexpected database confirmation; inspect rows before retrying")
            summary.update(results[0], database_written=True)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    except (OSError, ValueError, TypeError, KeyError, RecursionError, RuntimeError,
            common.subprocess.TimeoutExpired) as error:
        print(f"Identity update stopped: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
