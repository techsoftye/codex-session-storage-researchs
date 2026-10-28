"""Index physical JSONL record locations into research_session_records.

Default mode only reports eligible database rows. Use --apply to read source
files and write locator rows. Raw records and payloads are never stored.
"""
from __future__ import annotations

import argparse
import codecs
import csv
import ctypes
from dataclasses import dataclass
from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path, PurePosixPath
import subprocess
import sys
import time

from import_session_survey import connection_settings, find_psql


ROOT = Path(__file__).resolve().parents[1]
DATASET_ROOT = ROOT / "2026"
VERSION = "1.0.0"
DEFAULT_CHUNK_BYTES = 1024 * 1024
HEADER_LIMIT_BYTES = 64 * 1024
DEFAULT_BATCH_RECORDS = 2000
DEFERRED_BATCH = "large_files_second_pass"
REPARSE_POINT = 0x400


@dataclass(frozen=True)
class RecordLocator:
    line_number: int
    byte_offset: int
    record_bytes: int
    record_timestamp: str | None
    record_type: str | None


@dataclass
class ScanStats:
    bytes_scanned: int = 0
    physical_lines: int = 0
    record_count: int = 0
    blank_line_count: int = 0
    blank_line_bytes: int = 0


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def lower_priority() -> str:
    if os.name != "nt":
        return "unchanged"
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.GetCurrentProcess.restype = ctypes.c_void_p
    kernel.SetPriorityClass.argtypes = (ctypes.c_void_p, ctypes.c_uint32)
    return "below_normal" if kernel.SetPriorityClass(kernel.GetCurrentProcess(), 0x4000) else "unchanged"


def run_psql(sql: str, timeout: int = 180) -> str:
    config = connection_settings()
    environment = os.environ.copy()
    environment.update(PGPASSWORD=config["DB_PASSWORD"], PGCONNECT_TIMEOUT="5", PGCLIENTENCODING="UTF8")
    command = [
        find_psql(), "-X", "-w", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1",
        "-v", "VERBOSITY=sqlstate", "-h", config["DB_HOST"], "-p", config["DB_PORT"],
        "-U", config["DB_USERNAME"], "-d", config["DB_DATABASE"],
    ]
    process = subprocess.run(
        command, input=sql, text=True, encoding="utf-8", capture_output=True,
        env=environment, timeout=timeout,
    )
    if process.returncode:
        detail = process.stderr.strip().splitlines()
        suffix = f" PostgreSQL: {detail[-1]}" if detail else ""
        raise RuntimeError(f"PostgreSQL operation failed (exit {process.returncode}).{suffix}")
    return process.stdout


def json_rows(sql: str) -> list[dict]:
    rows = []
    for line in run_psql(sql).splitlines():
        line = line.strip()
        if line:
            value = json.loads(line)
            if not isinstance(value, dict):
                raise RuntimeError("Unexpected PostgreSQL row format")
            rows.append(value)
    return rows


def eligible_files(limit: int | None = None) -> list[dict]:
    limit_sql = f"LIMIT {limit}" if limit else ""
    return json_rows(f"""
SELECT json_build_object(
    'id', id,
    'source_relative_path', source_relative_path,
    'file_size_bytes', file_size_bytes,
    'scan_status', scan_status,
    'scan_complete', scan_complete
)
FROM public.research_sessions
WHERE scan_status IN ('pending', 'running', 'partial')
  AND scan_complete = false
  AND metadata #>> '{{record_index_deferred,batch}}' IS DISTINCT FROM '{DEFERRED_BATCH}'
ORDER BY file_size_bytes, source_relative_path
{limit_sql};
""")


def plan_summary() -> dict:
    rows = json_rows(f"""
SELECT json_build_object(
    'eligible_files', count(*) FILTER (
        WHERE scan_status IN ('pending', 'running', 'partial')
          AND scan_complete = false
          AND metadata #>> '{{record_index_deferred,batch}}' IS DISTINCT FROM '{DEFERRED_BATCH}'
    ),
    'eligible_bytes', coalesce(sum(file_size_bytes) FILTER (
        WHERE scan_status IN ('pending', 'running', 'partial')
          AND scan_complete = false
          AND metadata #>> '{{record_index_deferred,batch}}' IS DISTINCT FROM '{DEFERRED_BATCH}'
    ), 0),
    'deferred_files', count(*) FILTER (
        WHERE metadata #>> '{{record_index_deferred,batch}}' = '{DEFERRED_BATCH}'
    ),
    'completed_files', count(*) FILTER (WHERE scan_status = 'completed' AND scan_complete = true),
    'locator_rows', (SELECT count(*) FROM public.research_session_records)
)
FROM public.research_sessions;
""")
    if len(rows) != 1:
        raise RuntimeError("Could not read indexing plan")
    return rows[0]


def safe_source_path(relative: str) -> Path:
    if not isinstance(relative, str) or "\\" in relative:
        raise ValueError("Source path is not normalized")
    pure = PurePosixPath(relative)
    if not pure.parts or pure.parts[0] != "2026" or pure.suffix.lower() != ".jsonl":
        raise ValueError("Source path is outside the 2026 JSONL dataset")
    if any(part in {"", ".", ".."} or ":" in part for part in pure.parts):
        raise ValueError("Source path contains an unsafe component")
    candidate = ROOT.joinpath(*pure.parts)
    resolved = candidate.resolve(strict=True)
    dataset = DATASET_ROOT.resolve(strict=True)
    if not resolved.is_relative_to(dataset):
        raise ValueError("Resolved source path is outside the dataset")
    current = candidate
    while current != ROOT:
        attributes = getattr(current.lstat(), "st_file_attributes", 0)
        if current.is_symlink() or attributes & REPARSE_POINT:
            raise ValueError("Source path contains a link or reparse point")
        current = current.parent
    if not candidate.is_file():
        raise ValueError("Source is not a regular file")
    return candidate


def _skip_space(text: str, index: int) -> int:
    while index < len(text) and text[index] in " \t\r\n":
        index += 1
    return index


def extract_top_level_header(prefix: bytes) -> tuple[str | None, str | None]:
    """Read top-level timestamp/type without decoding the payload value."""
    try:
        # A byte-limited prefix may end inside a UTF-8 character in the payload.
        # Keep all complete characters without rejecting an otherwise valid header.
        text = codecs.getincrementaldecoder("utf-8-sig")().decode(prefix, final=False)
    except UnicodeDecodeError:
        return None, None
    decoder = json.JSONDecoder()
    timestamp = record_type = None
    index = _skip_space(text, 0)
    if index >= len(text) or text[index] != "{":
        return None, None
    index += 1
    while True:
        index = _skip_space(text, index)
        if index >= len(text) or text[index] == "}":
            break
        try:
            key, index = decoder.raw_decode(text, index)
        except (json.JSONDecodeError, TypeError):
            break
        if not isinstance(key, str):
            break
        index = _skip_space(text, index)
        if index >= len(text) or text[index] != ":":
            break
        index = _skip_space(text, index + 1)
        if key == "payload":
            break
        try:
            value, index = decoder.raw_decode(text, index)
        except (json.JSONDecodeError, TypeError):
            break
        if key == "timestamp" and isinstance(value, str):
            timestamp = value
        elif key == "type" and isinstance(value, str):
            record_type = value
        index = _skip_space(text, index)
        if index >= len(text):
            break
        if text[index] == ",":
            index += 1
            continue
        if text[index] == "}":
            break
        break
    return timestamp, record_type


def iter_record_locators(
    handle,
    stats: ScanStats,
    *,
    start_offset: int = 0,
    previous_line_number: int = 0,
    chunk_bytes: int = DEFAULT_CHUNK_BYTES,
    header_limit: int = HEADER_LIMIT_BYTES,
):
    """Yield nonblank physical lines while retaining at most header_limit bytes per line."""
    handle.seek(start_offset)
    absolute = start_offset
    line_start = start_offset
    line_number = previous_line_number
    record_bytes = 0
    prefix = bytearray()
    nonblank = False

    while True:
        chunk = handle.read(chunk_bytes)
        if not chunk:
            break
        position = 0
        while position < len(chunk):
            newline = chunk.find(b"\n", position)
            end = newline + 1 if newline >= 0 else len(chunk)
            segment = chunk[position:end]
            if not nonblank and segment.strip(b" \t\r\n"):
                nonblank = True
            if len(prefix) < header_limit:
                remaining = header_limit - len(prefix)
                prefix.extend(segment[:remaining])
            size = len(segment)
            record_bytes += size
            absolute += size
            position = end
            if newline < 0:
                continue
            line_number += 1
            stats.physical_lines += 1
            if nonblank:
                timestamp, record_type = extract_top_level_header(bytes(prefix))
                stats.record_count += 1
                yield RecordLocator(line_number, line_start, record_bytes, timestamp, record_type)
            else:
                stats.blank_line_count += 1
                stats.blank_line_bytes += record_bytes
            line_start = absolute
            record_bytes = 0
            prefix.clear()
            nonblank = False

    if record_bytes:
        line_number += 1
        stats.physical_lines += 1
        if nonblank:
            timestamp, record_type = extract_top_level_header(bytes(prefix))
            stats.record_count += 1
            yield RecordLocator(line_number, line_start, record_bytes, timestamp, record_type)
        else:
            stats.blank_line_count += 1
            stats.blank_line_bytes += record_bytes
    stats.bytes_scanned = absolute


def checkpoint(session_id: int) -> dict:
    rows = json_rows(f"""
WITH totals AS (
    SELECT count(*) AS record_count
    FROM public.research_session_records
    WHERE research_session_id = {session_id}
), last_row AS (
    SELECT source_line_number, source_byte_offset, source_record_bytes,
           record_timestamp, record_type
    FROM public.research_session_records
    WHERE research_session_id = {session_id}
    ORDER BY source_line_number DESC
    LIMIT 1
)
SELECT json_build_object(
    'record_count', totals.record_count,
    'last_line_number', last_row.source_line_number,
    'last_byte_offset', last_row.source_byte_offset,
    'last_record_bytes', last_row.source_record_bytes,
    'last_timestamp', last_row.record_timestamp,
    'last_type', last_row.record_type
)
FROM totals
LEFT JOIN last_row ON true;
""")
    if not rows:
        return {"record_count": 0, "last_line_number": None, "last_byte_offset": None,
                "last_record_bytes": None, "last_timestamp": None, "last_type": None}
    if len(rows) != 1:
        raise RuntimeError("Unexpected checkpoint result")
    return rows[0]


def validate_checkpoint(path: Path, file_size: int, state: dict) -> tuple[int, int, int]:
    count = int(state["record_count"])
    if not count:
        return 0, 0, 0
    if any(state[field] is None for field in ("last_line_number", "last_byte_offset", "last_record_bytes")):
        raise ValueError("Database checkpoint is incomplete")
    line_number = int(state["last_line_number"])
    offset = int(state["last_byte_offset"])
    length = int(state["last_record_bytes"])
    end = offset + length
    if offset < 0 or length <= 0 or end > file_size:
        raise ValueError("Stored record checkpoint is outside the source file")
    with path.open("rb", buffering=0) as handle:
        handle.seek(offset)
        prefix = handle.read(min(length, HEADER_LIMIT_BYTES))
        timestamp, record_type = extract_top_level_header(prefix)
        handle.seek(end - 1)
        final_byte = handle.read(1)
    if timestamp != state["last_timestamp"] or record_type != state["last_type"]:
        raise ValueError("Stored checkpoint header no longer matches the source")
    if end < file_size and final_byte != b"\n":
        raise ValueError("Stored checkpoint does not end at a physical line boundary")
    return end, line_number, count


def batch_sql(
    session_id: int,
    rows: list[RecordLocator],
    scanned_bytes: int,
    record_count: int,
    records_target: str = "public.research_session_records",
    sessions_target: str = "public.research_sessions",
) -> str:
    if records_target not in {"public.research_session_records", "pg_temp.research_session_records"}:
        raise ValueError("Invalid records target")
    if sessions_target not in {"public.research_sessions", "pg_temp.research_sessions"}:
        raise ValueError("Invalid sessions target")
    stream = io.StringIO()
    stream.write("BEGIN;\nSET LOCAL lock_timeout = '5s';\nSET LOCAL statement_timeout = '120s';\n")
    stream.write("CREATE TEMP TABLE record_index_stage (research_session_id bigint, source_line_number bigint, source_byte_offset bigint, source_record_bytes bigint, record_timestamp text, record_type text) ON COMMIT DROP;\n")
    stream.write("COPY record_index_stage FROM STDIN WITH (FORMAT csv);\n")
    writer = csv.writer(stream, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
    for row in rows:
        writer.writerow([session_id, row.line_number, row.byte_offset, row.record_bytes,
                         row.record_timestamp, row.record_type])
    stream.write("\\.\n")
    stream.write(f"""
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM record_index_stage s
        JOIN {records_target} r
          ON r.research_session_id = s.research_session_id
         AND r.source_line_number = s.source_line_number
        WHERE r.source_byte_offset <> s.source_byte_offset
           OR r.source_record_bytes <> s.source_record_bytes
           OR r.record_timestamp IS DISTINCT FROM s.record_timestamp
           OR r.record_type IS DISTINCT FROM s.record_type
    ) THEN
        RAISE EXCEPTION 'Existing locator disagrees with source';
    END IF;
END $$;

INSERT INTO {records_target} (
    research_session_id, source_line_number, source_byte_offset,
    source_record_bytes, record_timestamp, record_type
)
SELECT research_session_id, source_line_number, source_byte_offset,
       source_record_bytes, record_timestamp, record_type
FROM record_index_stage
ON CONFLICT (research_session_id, source_line_number) DO NOTHING;

DO $$
DECLARE actual_count bigint;
BEGIN
    SELECT count(*) INTO actual_count
    FROM {records_target}
    WHERE research_session_id = {session_id};
    IF actual_count <> {record_count} THEN
        RAISE EXCEPTION 'Locator count mismatch: expected %, found %', {record_count}, actual_count;
    END IF;
END $$;

DO $$
DECLARE changed integer;
BEGIN
    UPDATE {sessions_target}
    SET scan_version = '{VERSION}',
        scan_status = 'running',
        scan_started_at = coalesce(scan_started_at, CURRENT_TIMESTAMP),
        scan_completed_at = NULL,
        scan_complete = false,
        scanned_bytes = {scanned_bytes},
        scanned_record_count = {record_count},
        scan_error_code = NULL,
        scan_error = NULL,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = {session_id}
      AND scan_status IN ('pending', 'running', 'partial')
      AND scan_complete = false;
    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed <> 1 THEN
        RAISE EXCEPTION 'Source session row is no longer eligible';
    END IF;
END $$;
COMMIT;
""")
    return stream.getvalue()


def finalize_file(session_id: int, file_size: int, record_count: int) -> None:
    run_psql(f"""
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
DO $$
DECLARE actual_count bigint;
BEGIN
    SELECT count(*) INTO actual_count
    FROM public.research_session_records
    WHERE research_session_id = {session_id};
    IF actual_count <> {record_count} THEN
        RAISE EXCEPTION 'Final locator count mismatch: expected %, found %', {record_count}, actual_count;
    END IF;
END $$;
DO $$
DECLARE changed integer;
BEGIN
    UPDATE public.research_sessions
    SET scan_version = '{VERSION}',
        scan_status = 'completed',
        scan_started_at = coalesce(scan_started_at, CURRENT_TIMESTAMP),
        scan_completed_at = CURRENT_TIMESTAMP,
        scan_complete = true,
        scanned_bytes = {file_size},
        scanned_record_count = {record_count},
        scan_error_code = NULL,
        scan_error = NULL,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = {session_id}
      AND scan_status IN ('pending', 'running', 'partial')
      AND scan_complete = false;
    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed <> 1 THEN
        RAISE EXCEPTION 'Source session row is no longer eligible';
    END IF;
END $$;
COMMIT;
""", timeout=60)


def mark_state(session_id: int, status: str, code: str, message: str) -> None:
    if status not in {"partial", "failed"}:
        raise ValueError("Invalid failure status")
    safe_code = code[:80].replace("'", "")
    safe_message = message[:240].replace("'", "''").replace("\x00", "")
    run_psql(f"""
UPDATE public.research_sessions
SET scan_version = '{VERSION}', scan_status = '{status}', scan_complete = false,
    scan_error_code = '{safe_code}', scan_error = '{safe_message}', updated_at = CURRENT_TIMESTAMP
WHERE id = {session_id} AND scan_status <> 'completed';
""", timeout=30)


def index_file(item: dict, batch_records: int, chunk_bytes: int) -> dict:
    session_id = int(item["id"])
    expected_size = int(item["file_size_bytes"])
    path = safe_source_path(item["source_relative_path"])
    before = path.stat()
    if before.st_size != expected_size:
        raise ValueError("Source size differs from research_sessions.file_size_bytes")
    state = checkpoint(session_id)
    resume_offset, previous_line, existing_count = validate_checkpoint(path, expected_size, state)
    stats = ScanStats(bytes_scanned=resume_offset, physical_lines=previous_line, record_count=existing_count)
    batch: list[RecordLocator] = []
    started = time.monotonic()

    with path.open("rb", buffering=0) as handle:
        for locator in iter_record_locators(
            handle, stats, start_offset=resume_offset, previous_line_number=previous_line,
            chunk_bytes=chunk_bytes,
        ):
            batch.append(locator)
            if len(batch) >= batch_records:
                progress = locator.byte_offset + locator.record_bytes
                run_psql(batch_sql(session_id, batch, progress, stats.record_count), timeout=180)
                batch.clear()
        if batch:
            run_psql(batch_sql(session_id, batch, stats.bytes_scanned, stats.record_count), timeout=180)
            batch.clear()

    after = path.stat()
    if before.st_size != after.st_size or before.st_mtime_ns != after.st_mtime_ns:
        raise ValueError("Source changed while it was being indexed")
    if stats.bytes_scanned != expected_size:
        raise ValueError("Indexed byte coverage does not equal source file size")
    finalize_file(session_id, expected_size, stats.record_count)
    return {
        "research_session_id": session_id,
        "source_relative_path": item["source_relative_path"],
        "file_size_bytes": expected_size,
        "record_count": stats.record_count,
        "physical_lines": stats.physical_lines,
        "blank_line_count": stats.blank_line_count,
        "resumed_from_byte": resume_offset,
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }


def positive_int(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("Value must be positive")
    return number


def nonnegative_float(value: str) -> float:
    number = float(value)
    if number < 0:
        raise argparse.ArgumentTypeError("Value cannot be negative")
    return number


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Read eligible files and write locator rows")
    parser.add_argument("--limit-files", type=positive_int, help="Process at most this many eligible files")
    parser.add_argument("--batch-records", type=positive_int, default=DEFAULT_BATCH_RECORDS)
    parser.add_argument("--chunk-mib", type=positive_int, default=1, help="Read chunk size in MiB (1-8)")
    parser.add_argument("--sleep-above-mib", type=positive_int, default=800,
                        help="Pause only after a processed file larger than this size in MiB")
    parser.add_argument("--sleep-seconds", type=nonnegative_float, default=10.0)
    args = parser.parse_args(argv)
    if args.chunk_mib > 8:
        parser.error("--chunk-mib cannot exceed 8")

    try:
        priority = lower_priority()
        plan = plan_summary()
        if not args.apply:
            plan.update(index_version=VERSION, process_priority=priority, database_written=False)
            print(json.dumps(plan, ensure_ascii=False, indent=2))
            return 0

        files = eligible_files(args.limit_files)
        summary = {
            "index_version": VERSION,
            "started_at": utc_now(),
            "process_priority": priority,
            "eligible_at_start": plan["eligible_files"],
            "selected_files": len(files),
            "completed_files": 0,
            "failed_files": 0,
            "indexed_bytes": 0,
            "indexed_records": 0,
            "database_written": True,
        }
        for position, item in enumerate(files, 1):
            try:
                result = index_file(item, args.batch_records, args.chunk_mib * 1024 * 1024)
                summary["completed_files"] += 1
                summary["indexed_bytes"] += result["file_size_bytes"]
                summary["indexed_records"] += result["record_count"]
                print(
                    f"[{position}/{len(files)}] {result['source_relative_path']} | completed | "
                    f"{result['record_count']} records | {result['file_size_bytes']} bytes | "
                    f"{result['elapsed_seconds']}s",
                    flush=True,
                )
            except KeyboardInterrupt:
                try:
                    mark_state(int(item["id"]), "partial", "interrupted", "Record indexing interrupted")
                finally:
                    summary.update(completed_at=utc_now(), status="interrupted")
                    print(json.dumps(summary, ensure_ascii=False, indent=2), file=sys.stderr)
                return 130
            except (OSError, ValueError, TypeError, RuntimeError, subprocess.TimeoutExpired) as error:
                summary["failed_files"] += 1
                try:
                    existing = checkpoint(int(item["id"]))["record_count"]
                    status = "partial" if int(existing) else "failed"
                    mark_state(int(item["id"]), status, type(error).__name__, str(error))
                except Exception:
                    pass
                print(f"[{position}/{len(files)}] {item['source_relative_path']} | failed | {type(error).__name__}", file=sys.stderr, flush=True)

            sleep_threshold = args.sleep_above_mib * 1024 * 1024
            if (int(item["file_size_bytes"]) > sleep_threshold
                    and position < len(files) and args.sleep_seconds):
                print(
                    f"Pausing {args.sleep_seconds:g}s after large file "
                    f"({int(item['file_size_bytes']) / 1024**2:.1f} MiB)...",
                    flush=True,
                )
                time.sleep(args.sleep_seconds)

        summary.update(completed_at=utc_now(), status="completed" if not summary["failed_files"] else "completed_with_failures")
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0 if not summary["failed_files"] else 1
    except (OSError, ValueError, TypeError, RuntimeError, json.JSONDecodeError, subprocess.TimeoutExpired) as error:
        print(f"Record indexing stopped: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
