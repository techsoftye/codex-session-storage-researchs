"""Repair only NULL indexed types, using bounded source headers and audited SQL.

No conversation values are exported. --apply is required for database writes.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re

from index_session_records import (
    ROOT, HEADER_LIMIT_BYTES, extract_top_level_header, json_rows, lower_priority,
    run_psql, safe_source_path, utc_now,
)

REPORT = ROOT / "data/reports/null_record_type_repair_v1.json"
SAFE_TYPE = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,99}\Z")


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def repair_sql(rows):
    values = ",\n".join(
        f"({int(r['id'])},{int(r['research_session_id'])},{int(r['source_byte_offset'])},"
        f"{int(r['source_record_bytes'])},'{r['recovered_type']}')" for r in rows
    )
    return f"""
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
DO $$ BEGIN
 IF current_database() <> 'codex_session_research' THEN
  RAISE EXCEPTION 'Unexpected database';
 END IF;
END $$;
CREATE TEMP TABLE type_repairs (
 id bigint PRIMARY KEY, session_id bigint, byte_offset bigint, record_bytes bigint, new_type text
) ON COMMIT DROP;
INSERT INTO type_repairs VALUES {values};
CREATE TEMP TABLE locked_type_repairs ON COMMIT DROP AS
SELECT rr.id FROM public.research_session_records rr JOIN type_repairs t ON rr.id=t.id
WHERE rr.record_type IS NULL AND rr.research_session_id=t.session_id
 AND rr.source_byte_offset=t.byte_offset AND rr.source_record_bytes=t.record_bytes
ORDER BY rr.id FOR UPDATE OF rr;
DO $$ BEGIN
 IF (SELECT count(*) FROM locked_type_repairs) <> {len(rows)} THEN
  RAISE EXCEPTION 'Indexed rows changed; refusing partial repair';
 END IF;
END $$;
WITH changed AS (
 UPDATE public.research_session_records rr SET record_type=t.new_type
 FROM type_repairs t WHERE rr.id=t.id AND rr.record_type IS NULL RETURNING rr.id
)
SELECT json_build_object('updated_rows',count(*)) FROM changed;
COMMIT;
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    priority = lower_priority()
    rows = json_rows("""
SELECT json_build_object('id',rr.id,'research_session_id',rr.research_session_id,
 'source_byte_offset',rr.source_byte_offset,'source_record_bytes',rr.source_record_bytes,
 'original_timestamp',rr.record_timestamp,'source_relative_path',rs.source_relative_path,
 'file_size_bytes',rs.file_size_bytes)
FROM public.research_session_records rr JOIN public.research_sessions rs ON rs.id=rr.research_session_id
WHERE rr.record_type IS NULL ORDER BY rr.research_session_id,rr.source_byte_offset;
""")
    if not rows:
        print(json.dumps({"remaining_null_rows": 0, "updated_rows": 0, "previous_report_preserved": True}))
        return 0
    report = {"version": "1.0.0", "started_at": utc_now(), "database_written": False,
              "priority": priority, "header_limit_bytes": HEADER_LIMIT_BYTES, "rows": rows}
    errors = 0
    total_read = 0
    for row in rows:
        path = safe_source_path(row["source_relative_path"])
        before = path.stat()
        offset, size = row["source_byte_offset"], row["source_record_bytes"]
        if before.st_size != row["file_size_bytes"] or offset < 0 or size <= 0 or offset+size > before.st_size:
            raise RuntimeError("Source identity or locator range changed")
        with path.open("rb") as source:
            if offset:
                source.seek(offset-1)
                if source.read(1) != b"\n":
                    raise RuntimeError("Locator does not start at a source line boundary")
            source.seek(offset)
            prefix = source.read(min(size, HEADER_LIMIT_BYTES))
        after = path.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise RuntimeError("Source changed during header check")
        total_read += len(prefix)
        cause = "other_header_extraction_issue"
        try:
            prefix.decode("utf-8-sig")
        except UnicodeDecodeError as error:
            if error.reason == "unexpected end of data" and error.end == len(prefix):
                cause = "utf8_character_split_at_header_boundary"
            else:
                cause = "invalid_utf8"
        timestamp, kind = extract_top_level_header(prefix)
        good = isinstance(kind, str) and SAFE_TYPE.fullmatch(kind) is not None
        row.update(recovered_type=kind if good else None, recovered_timestamp=timestamp,
                   diagnosis=cause, header_bytes_read=len(prefix),
                   header_sha256=hashlib.sha256(prefix).hexdigest(),
                   source_mtime_ns=before.st_mtime_ns)
        errors += not good
    report.update(validated_rows=len(rows)-errors, unresolved_rows=errors,
                  bytes_read=total_read, recovered_type_counts=dict(Counter(r["recovered_type"] for r in rows)),
                  diagnosis_counts=dict(Counter(r["diagnosis"] for r in rows)))
    save(REPORT, report)
    if errors:
        raise RuntimeError(f"{errors} headers remain unresolved; no database write performed")
    if args.apply:
        for row in rows:
            state = safe_source_path(row["source_relative_path"]).stat()
            if (state.st_size, state.st_mtime_ns) != (row["file_size_bytes"],row["source_mtime_ns"]):
                raise RuntimeError("Source changed before database repair")
        changes = [json.loads(line) for line in run_psql(repair_sql(rows)).splitlines() if line.startswith("{")]
        report.update(changes[0], database_written=True)
        report["remaining_null_rows"] = json_rows(
            "SELECT json_build_object('count',count(*)) FROM public.research_session_records WHERE record_type IS NULL"
        )[0]["count"]
    report["completed_at"] = utc_now()
    save(REPORT, report)
    print(json.dumps({key:value for key,value in report.items() if key != "rows"}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
