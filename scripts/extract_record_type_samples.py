"""Extract the largest bounded samples for one indexed record type.

The script queries PostgreSQL for candidate record IDs, then delegates exact
byte-range extraction to extract_record_sample.py. Source JSONL files are opened
read-only and copied in bounded chunks; complete source files are never loaded.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re

from extract_record_sample import ROOT, extract
from index_session_records import json_rows


RECORD_TYPE_PATTERN = re.compile(r"^[A-Za-z0-9_]{1,100}$")


def record_type_value(value: str) -> str:
    if value.upper() == "NULL":
        return "NULL"
    if not RECORD_TYPE_PATTERN.fullmatch(value):
        raise argparse.ArgumentTypeError(
            "record type must contain only letters, numbers, and underscores, or be NULL"
        )
    return value


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


def candidates(
    record_type: str,
    count: int,
    max_bytes: int,
    all_records: bool,
) -> tuple[int, list[dict]]:
    type_condition = "record_type IS NULL" if record_type == "NULL" else f"record_type = '{record_type}'"
    size_condition = "" if all_records else f"AND source_record_bytes < {max_bytes}"
    summary = json_rows(f"""
SELECT json_build_object(
    'eligible_record_count', count(*),
    'eligible_record_bytes', coalesce(sum(source_record_bytes), 0)
)
FROM public.research_session_records
WHERE {type_condition}
  {size_condition};
""")[0]
    order_and_limit = "ORDER BY rr.id" if all_records else (
        f"ORDER BY rr.source_record_bytes DESC, rr.id ASC LIMIT {count}"
    )
    rows = json_rows(f"""
SELECT json_build_object(
    'record_id', rr.id,
    'research_session_id', rr.research_session_id,
    'source_line_number', rr.source_line_number,
    'source_byte_offset', rr.source_byte_offset,
    'source_record_bytes', rr.source_record_bytes,
    'record_timestamp', rr.record_timestamp,
    'record_type', rr.record_type,
    'source_relative_path', rs.source_relative_path,
    'file_size_bytes', rs.file_size_bytes
)
FROM public.research_session_records AS rr
JOIN public.research_sessions AS rs
  ON rs.id = rr.research_session_id
WHERE {type_condition.replace('record_type', 'rr.record_type')}
  {size_condition.replace('source_record_bytes', 'rr.source_record_bytes')}
{order_and_limit};
""")
    expected = int(summary["eligible_record_count"]) if all_records else count
    if len(rows) != expected:
        raise RuntimeError(
            f"Expected {expected} eligible {record_type} records; found {len(rows)}"
        )
    return int(summary["eligible_record_count"]), rows


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract the largest N indexed samples below a size ceiling for one record type."
    )
    parser.add_argument("--record-type", required=True, type=record_type_value)
    parser.add_argument("--sample-count", type=positive_int, default=5)
    parser.add_argument("--max-mib", type=positive_int, default=10)
    parser.add_argument("--all-records", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    max_bytes = args.max_mib * 1024 * 1024
    output_name = "null" if args.record_type == "NULL" else args.record_type
    output_dir = (ROOT / "samples" / output_name).resolve()
    samples_root = (ROOT / "samples").resolve()
    if not output_dir.is_relative_to(samples_root):
        parser.error("derived output directory is outside the samples directory")

    try:
        eligible_count, selected = candidates(
            args.record_type,
            args.sample_count,
            max_bytes,
            args.all_records,
        )
        extracted = [
            extract(details, output_dir, args.overwrite)
            for details in selected
        ]
    except (OSError, ValueError, RuntimeError) as error:
        parser.error(str(error))

    result = {
        "record_type": None if args.record_type == "NULL" else args.record_type,
        "selection": "all_records" if args.all_records else "largest_records_below_maximum",
        "maximum_record_bytes_exclusive": None if args.all_records else max_bytes,
        "eligible_record_count": eligible_count,
        "samples_extracted": len(extracted),
        "json_bytes_written": sum(item["json_bytes_written"] for item in extracted),
        "output_directory": str(output_dir),
        "first_record_id": extracted[0]["record_id"] if extracted else None,
        "last_record_id": extracted[-1]["record_id"] if extracted else None,
    }
    if not args.all_records:
        result["samples"] = extracted
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
