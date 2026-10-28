"""Synchronize per-session record counts and export chart-ready statistics.

The script reads PostgreSQL only. It does not open the source JSONL corpus.
Default mode validates the cached counts and exports only when they already
match. Pass --apply to update research_sessions.record_count transactionally
before exporting.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from index_session_records import json_rows, run_psql


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "data" / "reports" / "session_statistics_2026.json"
REPORT_VERSION = "1.0.0"


def project_output(value: str) -> Path:
    path = (ROOT / value).resolve() if not Path(value).is_absolute() else Path(value).resolve()
    if not path.is_relative_to(ROOT):
        raise argparse.ArgumentTypeError("output must be inside the research project")
    return path


def column_exists() -> bool:
    rows = json_rows("""
SELECT json_build_object(
    'exists', EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'research_sessions'
          AND column_name = 'record_count'
    )
);
""")
    return bool(rows[0]["exists"])


def synchronize_counts() -> int:
    output = run_psql("""
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

WITH actual_counts AS (
    SELECT research_session_id, count(*)::bigint AS record_count
    FROM public.research_session_records
    GROUP BY research_session_id
), desired AS (
    SELECT rs.id, coalesce(ac.record_count, 0)::bigint AS record_count
    FROM public.research_sessions AS rs
    LEFT JOIN actual_counts AS ac
      ON ac.research_session_id = rs.id
), changed AS (
    UPDATE public.research_sessions AS rs
    SET record_count = desired.record_count,
        updated_at = CURRENT_TIMESTAMP
    FROM desired
    WHERE rs.id = desired.id
      AND rs.record_count IS DISTINCT FROM desired.record_count
    RETURNING rs.id
)
SELECT count(*) FROM changed;

COMMIT;
""").strip()
    return int(output or 0)


def verification() -> dict:
    rows = json_rows("""
WITH actual_counts AS (
    SELECT research_session_id, count(*)::bigint AS record_count
    FROM public.research_session_records
    GROUP BY research_session_id
), comparison AS (
    SELECT
        rs.id,
        rs.record_count AS cached_count,
        coalesce(ac.record_count, 0)::bigint AS actual_count
    FROM public.research_sessions AS rs
    LEFT JOIN actual_counts AS ac
      ON ac.research_session_id = rs.id
)
SELECT json_build_object(
    'session_count', count(*),
    'null_record_count_rows', count(*) FILTER (WHERE cached_count IS NULL),
    'mismatched_session_rows', count(*) FILTER (WHERE cached_count IS DISTINCT FROM actual_count),
    'cached_record_total', sum(cached_count),
    'actual_record_total', sum(actual_count),
    'source_file_size_bytes', (SELECT sum(file_size_bytes) FROM public.research_sessions)
)
FROM comparison;
""")
    return rows[0]


def session_rows() -> list[dict]:
    return json_rows("""
SELECT json_build_object(
    'research_session_id', rs.id,
    'session_id', rs.session_id,
    'session_started_at', rs.session_started_at,
    'session_source', rs.session_source,
    'originator', rs.originator,
    'first_observed_model', rs.first_observed_model,
    'record_count', rs.record_count,
    'file_size_bytes', rs.file_size_bytes,
    'file_size_mib', round(rs.file_size_bytes / 1048576.0, 6),
    'source_relative_path', rs.source_relative_path
)
FROM public.research_sessions AS rs
ORDER BY rs.id;
""")


def write_report(path: Path, report: dict, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise RuntimeError(f"Output already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.part")
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(report, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        temporary.replace(path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Synchronize research_sessions.record_count and export per-session statistics."
    )
    parser.add_argument("--output", type=project_output, default=DEFAULT_OUTPUT)
    parser.add_argument("--apply", action="store_true", help="Update cached counts before export")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    if not column_exists():
        parser.error("research_sessions.record_count is missing; apply migration 003 first")

    updated_rows = synchronize_counts() if args.apply else 0
    verified = verification()
    if verified["mismatched_session_rows"] != 0 or verified["null_record_count_rows"] != 0:
        parser.error("cached counts are not synchronized; rerun with --apply")
    if verified["cached_record_total"] != verified["actual_record_total"]:
        parser.error("cached and actual record totals disagree")

    sessions = session_rows()
    if len(sessions) != verified["session_count"]:
        parser.error("export row count disagrees with verification summary")

    generated_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    report = {
        "report_version": REPORT_VERSION,
        "generated_at": generated_at,
        "database": "codex_session_research",
        "record_count_source": "public.research_session_records grouped by research_session_id",
        "session_count": verified["session_count"],
        "total_record_count": verified["actual_record_total"],
        "total_file_size_bytes": verified["source_file_size_bytes"],
        "total_file_size_gib": round(verified["source_file_size_bytes"] / 1073741824, 6),
        "record_count_sync_status": "verified",
        "database_rows_updated": updated_rows,
        "sessions": sessions,
    }
    write_report(args.output, report, args.overwrite)
    print(json.dumps({
        "output": str(args.output),
        "session_count": report["session_count"],
        "total_record_count": report["total_record_count"],
        "total_file_size_bytes": report["total_file_size_bytes"],
        "database_rows_updated": updated_rows,
        "record_count_sync_status": "verified",
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
