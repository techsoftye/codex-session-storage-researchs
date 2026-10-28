"""Read-only compaction timing and token-accounting case study.

Reads PostgreSQL metadata plus only indexed event_msg byte ranges in 2026/.
Never prints or writes conversation content, response identifiers, or paths.
"""
from __future__ import annotations

import bisect
import argparse
import csv
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import statistics
import subprocess
import sys
import time
import traceback

from import_session_survey import connection_settings, find_psql, run_psql
from index_session_records import ROOT, safe_source_path
from profile_compacted import Stream, CHUNK


OUT = ROOT / "data/reports/compaction_cost_v1"
SNAPSHOT_SQL = (ROOT / "scripts/compaction_cost_snapshot.sql").read_text(encoding="utf-8")
COPY_EVENTS_SQL = r"""
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL statement_timeout = '0';
SET LOCAL lock_timeout = '5s';
SET LOCAL work_mem = '8MB';
SET LOCAL max_parallel_workers_per_gather = 0;
COPY (
    SELECT r.research_session_id, r.id, r.source_line_number,
           r.source_byte_offset, r.source_record_bytes,
           COALESCE(r.record_timestamp, '')
    FROM public.research_session_records r
    WHERE r.record_type = 'event_msg'
      AND EXISTS (
          SELECT 1 FROM public.research_session_records c
          WHERE c.research_session_id = r.research_session_id
            AND c.record_type = 'compacted'
      )
    ORDER BY r.research_session_id, r.source_line_number
) TO STDOUT WITH (FORMAT csv);
COMMIT;
"""

MAX_DEPTH = 128
SAFE_NUMBERS = {
    "payload.started_at_ms",
    "payload.completed_at_ms",
    "payload.item.started_at_ms",
    "payload.item.completed_at_ms",
    "payload.latest_token_usage_record.usage.input_tokens",
    "payload.latest_token_usage_record.usage.cached_input_tokens",
    "payload.latest_token_usage_record.usage.cache_write_input_tokens",
    "payload.latest_token_usage_record.usage.output_tokens",
    "payload.latest_token_usage_record.usage.reasoning_output_tokens",
    "payload.latest_token_usage_record.usage.total_tokens",
}
SAFE_STRINGS = {"type", "payload.type", "payload.item.type", "payload.compaction_response_id"}
START_PATHS = ("payload.started_at_ms", "payload.item.started_at_ms")
COMPLETED_PATHS = ("payload.completed_at_ms", "payload.item.completed_at_ms")
TIME_PATHS = set(START_PATHS + COMPLETED_PATHS)
TOKEN_FIELDS = (
    "input_tokens", "cached_input_tokens", "cache_write_input_tokens",
    "output_tokens", "reasoning_output_tokens", "total_tokens",
)


class Projection:
    """Streaming JSON projection retaining only selected enum/numeric fields."""

    def __init__(self, source, size):
        self.stream = Stream(source, size, CHUNK)
        self.size = size
        self.values = {}
        self.present = set()

    def parse(self):
        kind, _ = self.value("", 0)
        self.stream.whitespace()
        if kind != "object" or self.stream.peek() is not None or self.stream.position != self.size:
            raise ValueError("invalid_envelope")
        return self.values

    def value(self, path, depth):
        if depth > MAX_DEPTH:
            raise ValueError("depth_limit")
        stream = self.stream
        stream.whitespace()
        if path in TIME_PATHS:
            self.present.add(path)
        char = stream.peek()
        if char in (123, 91):
            is_object = char == 123
            stream.take()
            stream.whitespace()
            close = 125 if is_object else 93
            seen = set()
            if stream.peek() != close:
                while True:
                    if is_object:
                        key, _, key_bytes = stream.string()
                        if key is None or key_bytes > 256:
                            raise ValueError("oversized_key")
                        if key in seen:
                            raise ValueError("duplicate_key")
                        seen.add(key)
                        if len(seen) > 20000:
                            raise ValueError("object_key_limit")
                        stream.whitespace()
                        stream.expect(58)
                        child = f"{path}.{key}" if path else key
                    else:
                        child = f"{path}[]"
                    self.value(child, depth + 1)
                    stream.whitespace()
                    delimiter = stream.take()
                    if delimiter == close:
                        break
                    if delimiter != 44:
                        raise ValueError("invalid_container")
                    stream.whitespace()
            else:
                stream.take()
            return "object" if is_object else "array", None
        if char == 34:
            value, _, _ = stream.string()
            if path in SAFE_STRINGS and value is not None:
                self.values[path] = value
            return "string", None
        kind, value = stream.primitive()
        if path in SAFE_NUMBERS and kind == "number" and not isinstance(value, bool):
            self.values[path] = value
        return kind, value


class LimitedReader:
    def __init__(self, handle, limit):
        self.handle = handle
        self.limit = limit
        self.read_bytes = 0

    def read(self, size=-1):
        if self.read_bytes >= self.limit:
            return b""
        if size is None or size < 0:
            size = self.limit - self.read_bytes
        block = self.handle.read(min(size, self.limit - self.read_bytes))
        self.read_bytes += len(block)
        return block


def parse_timestamp(value):
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def finite_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    low = math.floor(position)
    high = math.ceil(position)
    if low == high:
        return ordered[low]
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def average_ranks(values):
    ordered = sorted(enumerate(values), key=lambda pair: pair[1])
    ranks = [0.0] * len(values)
    index = 0
    while index < len(ordered):
        end = index + 1
        while end < len(ordered) and ordered[end][1] == ordered[index][1]:
            end += 1
        rank = ((index + 1) + end) / 2.0
        for cursor in range(index, end):
            ranks[ordered[cursor][0]] = rank
        index = end
    return ranks


def spearman(x_values, y_values):
    if len(x_values) != len(y_values) or len(x_values) < 2:
        return None
    x = average_ranks(x_values)
    y = average_ranks(y_values)
    mean_x = sum(x) / len(x)
    mean_y = sum(y) / len(y)
    cov = sum((a - mean_x) * (b - mean_y) for a, b in zip(x, y))
    var_x = sum((a - mean_x) ** 2 for a in x)
    var_y = sum((b - mean_y) ** 2 for b in y)
    if not var_x or not var_y:
        return None
    return cov / math.sqrt(var_x * var_y)


def token_valid(row):
    input_tokens = row.get("input_tokens")
    output_tokens = row.get("output_tokens")
    total_tokens = row.get("total_tokens")
    if not all(isinstance(v, int) and v >= 0 for v in (input_tokens, output_tokens, total_tokens)):
        return False
    if input_tokens + output_tokens != total_tokens:
        return False
    cached = row.get("cached_input_tokens")
    if cached is not None and (not isinstance(cached, int) or cached < 0 or cached > input_tokens):
        return False
    cache_write = row.get("cache_write_input_tokens")
    if cache_write is not None and (not isinstance(cache_write, int) or cache_write < 0):
        return False
    reasoning = row.get("reasoning_output_tokens")
    if reasoning is not None and (not isinstance(reasoning, int) or reasoning < 0 or reasoning > output_tokens):
        return False
    return True


def token_distribution(values):
    present = [value for value in values if value is not None]
    return {
        "count": len(present),
        "missing": len(values) - len(present),
        "sum": sum(present) if present else None,
        "min": min(present) if present else None,
        "median": statistics.median(present) if present else None,
        "p90": percentile(present, 0.90),
        "max": max(present) if present else None,
    }


def snapshot():
    raw = run_psql(SNAPSHOT_SQL)
    result = json.loads(raw.strip())
    if result.get("read_only") != "on" or result.get("transaction_read_only") != "on":
        raise RuntimeError("PostgreSQL snapshot was not read-only")
    return result


def copy_command():
    config = connection_settings()
    environment = os.environ.copy()
    environment.update(
        PGPASSWORD=config["DB_PASSWORD"], PGCONNECT_TIMEOUT="5", PGCLIENTENCODING="UTF8"
    )
    command = [
        find_psql(), "-X", "-w", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1",
        "-v", "VERBOSITY=sqlstate", "-h", config["DB_HOST"], "-p", config["DB_PORT"],
        "-U", config["DB_USERNAME"], "-d", config["DB_DATABASE"],
    ]
    return command, environment


def read_event_ranges(files, compacted_by_file, selected_file_ids):
    command, environment = copy_command()
    process = subprocess.Popen(
        command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, encoding="utf-8", env=environment,
    )
    process.stdin.write(COPY_EVENTS_SQL)
    process.stdin.close()
    events_by_file = {file_id: [] for file_id in compacted_by_file}
    total_rows = 0
    total_bytes = 0
    parse_failures = 0
    envelope_mismatches = 0
    active_file_id = None
    active_path = None
    active_handle = None
    expected_sizes = {int(row["file_id"]): int(row["file_size_bytes"]) for row in files}
    paths = {int(row["file_id"]): row["source_relative_path"] for row in files}

    def open_file(file_id):
        nonlocal active_file_id, active_path, active_handle
        if active_handle is not None:
            active_handle.close()
        active_file_id = file_id
        active_path = safe_source_path(paths[file_id])
        if active_path.stat().st_size != expected_sizes[file_id]:
            raise RuntimeError("Indexed source size changed")
        active_handle = active_path.open("rb")

    try:
        for csv_row in csv.reader(process.stdout):
            if len(csv_row) != 6:
                raise RuntimeError("PostgreSQL event locator shape mismatch")
            file_id, record_id, line, offset, size = map(int, csv_row[:5])
            timestamp = csv_row[5] or None
            total_rows += 1
            total_bytes += size
            if file_id not in selected_file_ids:
                raise RuntimeError("Event locator references an unselected file")
            if active_file_id != file_id:
                open_file(file_id)
            if offset < 0 or size <= 0 or offset + size > expected_sizes[file_id]:
                raise RuntimeError("Invalid indexed event byte range")
            active_handle.seek(offset)
            limited = LimitedReader(active_handle, size)
            try:
                projection = Projection(limited, size)
                values = projection.parse()
                if limited.read_bytes != size or values.get("type") != "event_msg":
                    envelope_mismatches += 1
                    continue
                payload_type = values.get("payload.type")
                item_type = values.get("payload.item.type")
                if payload_type == "context_compacted" or item_type == "context_compacted":
                    events_by_file[file_id].append({
                        "kind": "context_compacted", "record_id": record_id,
                        "line": line, "timestamp": timestamp,
                    })
                if payload_type == "item_completed" and item_type == "ContextCompaction":
                    started_path = next((path for path in START_PATHS if path in projection.present), None)
                    completed_path = next((path for path in COMPLETED_PATHS if path in projection.present), None)
                    started = values.get(started_path) if started_path else None
                    completed = values.get(completed_path) if completed_path else None
                    events_by_file[file_id].append({
                        "kind": "exact", "record_id": record_id, "line": line,
                        "timestamp": timestamp, "started_at_ms": started,
                        "completed_at_ms": completed,
                        "started_present": started_path is not None,
                        "completed_present": completed_path is not None,
                        "started_field_path": started_path,
                        "completed_field_path": completed_path,
                    })
            except (ValueError, UnicodeDecodeError, RecursionError):
                parse_failures += 1
            if total_rows % 100000 == 0:
                print(f"event_msg_ranges_read={total_rows} indexed_bytes={total_bytes}", flush=True)
    finally:
        if active_handle is not None:
            active_handle.close()
    process.stdout.close()
    stderr = process.stderr.read()
    process.stderr.close()
    return_code = process.wait()
    if return_code:
        # Do not print psql diagnostics, which can contain query or local data.
        raise RuntimeError("PostgreSQL event range stream failed")
    if stderr.strip():
        raise RuntimeError("PostgreSQL event range stream produced diagnostics")
    return events_by_file, {
        "event_msg_ranges_read": total_rows,
        "indexed_event_msg_bytes_read": total_bytes,
        "parse_failures": parse_failures,
        "envelope_mismatches": envelope_mismatches,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh-existing", action="store_true", help="Rebuild derived report/CSV metrics without reading PostgreSQL or source files")
    args = parser.parse_args()
    if args.refresh_existing:
        refresh_existing_outputs()
        return
    started_clock = time.perf_counter()
    started_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    db = snapshot()
    files = db["files"]
    compacted = db["compacted"]
    latest_usage = db["latest_usage"]
    compacted_by_file = {}
    compacted_by_id = {}
    for row in compacted:
        row["file_id"] = int(row["file_id"])
        row["record_id"] = int(row["record_id"])
        row["source_line_number"] = int(row["source_line_number"])
        row["source_record_bytes"] = int(row["source_record_bytes"])
        prior = row.get("prior_usage")
        if isinstance(prior, dict):
            prior["line"] = int(prior["line"])
        else:
            row["prior_usage"] = None
        compacted_by_file.setdefault(row["file_id"], []).append(row)
        compacted_by_id[row["record_id"]] = row
    for rows in compacted_by_file.values():
        rows.sort(key=lambda row: row["source_line_number"])
    selected_file_ids = set(compacted_by_file)
    events_by_file, scan_stats = read_event_ranges(files, compacted_by_file, selected_file_ids)
    exact_events = []
    context_count = 0
    for file_id, events in events_by_file.items():
        events.sort(key=lambda row: row["line"])
        context_count += sum(event["kind"] == "context_compacted" for event in events)
        rows = compacted_by_file[file_id]
        lines = [row["source_line_number"] for row in rows]
        for event in events:
            if event["kind"] != "exact":
                continue
            started = event.get("started_at_ms")
            completed = event.get("completed_at_ms")
            event["has_started"] = finite_number(started)
            event["has_completed"] = finite_number(completed)
            event["duration_ms"] = None
            if event["has_started"] and event["has_completed"] and completed >= started:
                event["duration_ms"] = completed - started
            index = bisect.bisect_right(lines, event["line"])
            event["linked_compacted"] = rows[index] if index < len(rows) else None
            exact_events.append(event)

    # Approximate upper-bound timing per compacted record. Pick the last
    # context_compacted marker after the selected prior usage and before compaction.
    approx_by_id = {}
    context_by_file = {
        file_id: [event for event in events if event["kind"] == "context_compacted"]
        for file_id, events in events_by_file.items()
    }
    for file_id, rows in compacted_by_file.items():
        markers = context_by_file[file_id]
        marker_lines = [event["line"] for event in markers]
        for row in rows:
            prior = row.get("prior_usage")
            result = {"duration_ms": None, "start_missing": False, "end_missing": False,
                      "no_prior_usage": prior is None, "negative": False,
                      "endpoint_kind": "compacted", "endpoint_record_id": row["record_id"]}
            if prior is None:
                approx_by_id[row["record_id"]] = result
                continue
            start_time = parse_timestamp(prior.get("timestamp"))
            if start_time is None:
                result["start_missing"] = True
            marker_idx = bisect.bisect_right(marker_lines, prior["line"])
            marker_idx = bisect.bisect_left(marker_lines, row["source_line_number"], marker_idx) - 1
            end_value = row.get("record_timestamp")
            if marker_idx >= 0:
                marker = markers[marker_idx]
                result["endpoint_kind"] = "context_compacted"
                result["endpoint_record_id"] = marker["record_id"]
                end_value = marker.get("timestamp")
            end_time = parse_timestamp(end_value)
            if end_time is None:
                result["end_missing"] = True
            if start_time is not None and end_time is not None:
                duration = (end_time - start_time).total_seconds() * 1000.0
                if duration < 0:
                    result["negative"] = True
                else:
                    result["duration_ms"] = duration
            approx_by_id[row["record_id"]] = result

    # Exact events link to the next compacted record. If multiple exact events
    # map to one record, use the nearest event for the paired comparison only.
    exact_events.sort(key=lambda e: (e["linked_compacted"]["record_id"] if e["linked_compacted"] else 0, e["line"]))
    latest_exact_by_compacted = {}
    exact_pairs_for_size = []
    for event in exact_events:
        row = event.get("linked_compacted")
        if row is None:
            continue
        if event["duration_ms"] is not None:
            exact_pairs_for_size.append((event["duration_ms"], row["source_record_bytes"]))
        latest_exact_by_compacted[row["record_id"]] = event

    # Read explicit latest_token_usage_record observations already indexed in
    # PostgreSQL; never inspect compacted source payloads in this analysis.
    obs_by_compacted = {}
    ignored_duplicate_observations = 0
    invalid_token_observations = 0
    for observation in latest_usage:
        record_id = int(observation["record_id"])
        if observation.get("is_possible_duplicate") is not False:
            ignored_duplicate_observations += 1
            continue
        row = {name: observation.get(name) for name in TOKEN_FIELDS}
        if not token_valid(row):
            invalid_token_observations += 1
            continue
        signature = tuple(row[name] for name in TOKEN_FIELDS)
        obs_by_compacted.setdefault(record_id, {})[signature] = row
    token_by_compacted = {}
    ambiguous_token_compactions = 0
    for record_id, candidates in obs_by_compacted.items():
        if len(candidates) == 1:
            token_by_compacted[record_id] = next(iter(candidates.values()))
        else:
            ambiguous_token_compactions += 1

    exact_candidates = len(exact_events)
    started_count = sum(e["started_present"] for e in exact_events)
    completed_count = sum(e["completed_present"] for e in exact_events)
    both_count = sum(e["started_present"] and e["completed_present"] for e in exact_events)
    started_numeric = sum(e["has_started"] for e in exact_events)
    completed_numeric = sum(e["has_completed"] for e in exact_events)
    started_paths = {}
    completed_paths = {}
    for event in exact_events:
        if event.get("started_field_path"):
            started_paths[event["started_field_path"]] = started_paths.get(event["started_field_path"], 0) + 1
        if event.get("completed_field_path"):
            completed_paths[event["completed_field_path"]] = completed_paths.get(event["completed_field_path"], 0) + 1
    exact_valid_events = [e for e in exact_events if e["duration_ms"] is not None]
    exact_durations = [e["duration_ms"] for e in exact_valid_events]
    exact_missing_or_invalid = exact_candidates - len(exact_valid_events)
    exact_linked_valid = [e for e in exact_valid_events if e["linked_compacted"] is not None]

    approx_values = [value["duration_ms"] for value in approx_by_id.values() if value["duration_ms"] is not None]
    approx_missing = sum(value["duration_ms"] is None for value in approx_by_id.values())
    approx_no_prior = sum(value["no_prior_usage"] for value in approx_by_id.values())
    approx_missing_start = sum(value["start_missing"] for value in approx_by_id.values())
    approx_missing_end = sum(value["end_missing"] for value in approx_by_id.values())
    approx_negative = sum(value["negative"] for value in approx_by_id.values())

    paired = []
    for record_id, event in latest_exact_by_compacted.items():
        approx = approx_by_id.get(record_id, {})
        if event.get("duration_ms") is None or approx.get("duration_ms") is None:
            continue
        exact_ms = event["duration_ms"]
        approx_ms = approx["duration_ms"]
        difference = approx_ms - exact_ms
        paired.append({
            "record_id": record_id,
            "exact_ms": exact_ms,
            "approx_ms": approx_ms,
            "difference_ms": difference,
            "difference_percent_of_exact": (difference / exact_ms * 100.0) if exact_ms > 0 else None,
        })

    token_rows = [
        (record_id, row) for record_id, row in token_by_compacted.items()
        if record_id in compacted_by_id
    ]
    token_stats = {
        name: token_distribution([row[name] for _, row in token_rows])
        for name in TOKEN_FIELDS
    }
    exact_token_correlations = {}
    approx_token_correlations = {}
    exact_token_pairs = [(record_id, row, latest_exact_by_compacted[record_id])
                         for record_id, row in token_rows
                         if record_id in latest_exact_by_compacted
                         and latest_exact_by_compacted[record_id].get("duration_ms") is not None]
    approx_token_pairs = [(record_id, row, approx_by_id[record_id])
                          for record_id, row in token_rows
                          if approx_by_id.get(record_id, {}).get("duration_ms") is not None]
    for name in TOKEN_FIELDS:
        pairs = [(row[name], event["duration_ms"]) for _, row, event in exact_token_pairs if row[name] is not None]
        exact_token_correlations[name] = {
            "n": len(pairs), "spearman": spearman([p[0] for p in pairs], [p[1] for p in pairs])
        }
        pairs = [(row[name], approx["duration_ms"]) for _, row, approx in approx_token_pairs if row[name] is not None]
        approx_token_correlations[name] = {
            "n": len(pairs), "spearman": spearman([p[0] for p in pairs], [p[1] for p in pairs])
        }

    rows_for_csv = []
    exact_for_compacted = latest_exact_by_compacted
    for row in compacted:
        record_id = row["record_id"]
        event = exact_for_compacted.get(record_id)
        approx = approx_by_id.get(record_id, {})
        token = token_by_compacted.get(record_id, {})
        prior = row.get("prior_usage") or {}
        exact_duration = event["duration_ms"] if event else None
        approx_duration = approx.get("duration_ms")
        difference = approx_duration - exact_duration if exact_duration is not None and approx_duration is not None else None
        difference_percent = (difference / exact_duration * 100.0) if difference is not None and exact_duration > 0 else None
        rows_for_csv.append({
            "file_internal_id": row["file_id"],
            "compacted_record_internal_id": record_id,
            "compacted_line_number": row["source_line_number"],
            "compacted_record_bytes": row["source_record_bytes"],
            "prior_usage_scope": prior.get("scope"),
            "prior_usage_line_number": prior.get("line"),
            "exact_event_internal_id": event["record_id"] if event else None,
            "exact_event_line_number": event["line"] if event else None,
            "exact_started_at_ms_present": event["started_present"] if event else False,
            "exact_completed_at_ms_present": event["completed_present"] if event else False,
            "exact_duration_ms": exact_duration,
            "approx_endpoint_kind": approx.get("endpoint_kind"),
            "approx_endpoint_internal_id": approx.get("endpoint_record_id"),
            "approx_duration_ms": approx_duration,
            "approx_minus_exact_ms": difference,
            "approx_difference_percent_of_exact": difference_percent,
            "approx_missing_reason": (
                "no_prior_usage" if approx.get("no_prior_usage") else
                "missing_start_time" if approx.get("start_missing") else
                "missing_endpoint_time" if approx.get("end_missing") else
                "negative_chronology" if approx.get("negative") else None
            ),
            **{name: token.get(name) for name in TOKEN_FIELDS},
        })

    completed_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    elapsed = time.perf_counter() - started_clock
    summary = {
        "version": "1.0.0",
        "status": "completed",
        "started_at_utc": started_at,
        "completed_at_utc": completed_at,
        "elapsed_seconds": round(elapsed, 3),
        "read_only": True,
        "scope": {
            "postgres_tables": ["research_sessions", "research_session_records", "research_token_usage_observations"],
            "source_root": "2026/",
            "source_record_types_read": ["event_msg"],
            "source_files_with_compacted_records": len(files),
            "compacted_records": len(compacted),
            **scan_stats,
            "source_bytes_not_read": "All non-event_msg byte ranges, including compacted payload ranges, were not opened.",
        },
        "exact_timing": {
            "event_predicate": "record_type=event_msg AND payload.type=item_completed AND payload.item.type=ContextCompaction",
            "candidate_events": exact_candidates,
            "events_with_started_at_ms": started_count,
            "events_with_completed_at_ms": completed_count,
            "events_with_both": both_count,
            "events_with_numeric_started_at_ms": started_numeric,
            "events_with_numeric_completed_at_ms": completed_numeric,
            "started_at_ms_field_paths": started_paths,
            "completed_at_ms_field_paths": completed_paths,
            "valid_nonnegative_durations": len(exact_valid_events),
            "missing_or_invalid_duration": exact_missing_or_invalid,
            "linked_to_later_compacted_record": sum(e["linked_compacted"] is not None for e in exact_events),
            "valid_linked_pairs_for_spearman": len(exact_linked_valid),
            "unique_compacted_records_linked": len({e["linked_compacted"]["record_id"] for e in exact_events if e["linked_compacted"] is not None}),
            "duration_ms": {
                "count": len(exact_durations), "median": statistics.median(exact_durations) if exact_durations else None,
                "p90_linear": percentile(exact_durations, 0.90), "max": max(exact_durations) if exact_durations else None,
            },
            "spearman_duration_vs_linked_compacted_record_bytes": {
                "n": len(exact_pairs_for_size),
                "spearman": spearman([p[0] for p in exact_pairs_for_size], [p[1] for p in exact_pairs_for_size]),
                "note": "Event-level pairs; if multiple events link to the same compacted row, its byte size is repeated.",
            },
        },
        "approximate_upper_bound_timing": {
            "compacted_records": len(compacted),
            "valid_durations": len(approx_values), "missing_timing_or_usage": approx_missing,
            "no_prior_valid_response_or_last_usage": approx_no_prior,
            "missing_start_timestamp": approx_missing_start,
            "missing_selected_endpoint_timestamp": approx_missing_end,
            "negative_chronology": approx_negative,
            "duration_ms": {
                "count": len(approx_values), "median": statistics.median(approx_values) if approx_values else None,
                "p90_linear": percentile(approx_values, 0.90), "max": max(approx_values) if approx_values else None,
            },
            "endpoint_rule": "Last context_compacted event strictly after prior valid response/last usage and before compacted record; otherwise the compacted record.",
            "interpretation": "Requested upper-bound proxy; observed pairwise comparisons show this interval is not a guaranteed upper bound.",
        },
        "exact_vs_approximate": {
            "paired_compacted_records": len(paired),
            "approx_minus_exact_ms": token_distribution([p["difference_ms"] for p in paired]),
            "difference_percent_of_exact": token_distribution([p["difference_percent_of_exact"] for p in paired if p["difference_percent_of_exact"] is not None]),
            "approx_shorter_than_exact": sum(p["difference_ms"] < 0 for p in paired),
            "approx_is_upper_bound_for_all_pairs": bool(paired) and all(p["difference_ms"] >= 0 for p in paired),
        },
        "explicit_token_usage": {
            "source": "PostgreSQL observations at /payload/latest_token_usage_record/usage attached to compacted records; raw compacted payloads were not read.",
            "compaction_response_id_match_verified": False,
            "reason": "compaction_response_id is not a column in the permitted PostgreSQL observation data; compacted source ranges were outside the authorized file-read scope.",
            "compacted_records_with_one_unambiguous_valid_reading": len(token_by_compacted),
            "duplicate_flagged_or_unclassified_observations_excluded": ignored_duplicate_observations,
            "invalid_observations_excluded": invalid_token_observations,
            "compactions_with_conflicting_valid_readings": ambiguous_token_compactions,
            "readings_with_exact_duration": len({record_id for record_id, _, _ in exact_token_pairs}),
            "readings_with_approximate_duration": len({record_id for record_id, _, _ in approx_token_pairs}),
            "distributions_tokens": token_stats,
            "spearman_with_exact_duration_ms": exact_token_correlations,
            "spearman_with_approximate_duration_ms": approx_token_correlations,
        },
        "interpretation_limits": [
            "Recorded timestamps are not an independent performance benchmark.",
            "The approximate interval is an upper-bound proxy from the preceding valid usage timestamp, not a measured compaction start.",
            "Correlation does not establish causation.",
            "Token readings are those explicitly indexed in PostgreSQL; they are not asserted to match compaction_response_id because source compacted payloads were not read.",
            "A missing value is not zero; nullable token counters remain missing.",
            "The first response after compaction is not used as the prior token reading.",
        ],
    }

    OUT.mkdir(parents=True, exist_ok=True)
    with (OUT / "summary.json").open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2, allow_nan=False)
        handle.write("\n")
    fieldnames = list(rows_for_csv[0]) if rows_for_csv else ["file_internal_id", "compacted_record_internal_id"]
    with (OUT / "compaction_cost.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="raise")
        writer.writeheader()
        writer.writerows(rows_for_csv)
    (OUT / "report.md").write_text(render_report(summary), encoding="utf-8", newline="\n")
    print(json.dumps({
        "status": summary["status"],
        "source_files": len(files),
        "compacted_records": len(compacted),
        "event_msg_ranges_read": scan_stats["event_msg_ranges_read"],
        "indexed_event_msg_bytes_read": scan_stats["indexed_event_msg_bytes_read"],
        "exact_events": exact_candidates,
        "exact_valid_durations": len(exact_valid_events),
        "approx_valid_durations": len(approx_values),
        "paired": len(paired),
        "token_readings": len(token_by_compacted),
        "elapsed_seconds": summary["elapsed_seconds"],
    }, ensure_ascii=False, indent=2))


def refresh_existing_outputs():
    """Recompute derived comparisons from this analysis's already-produced CSV."""
    summary_path = OUT / "summary.json"
    csv_path = OUT / "compaction_cost.csv"
    with summary_path.open(encoding="utf-8") as handle:
        summary = json.load(handle)
    with csv_path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)
    for name in ("approx_minus_exact_ms", "approx_difference_percent_of_exact"):
        if name not in fieldnames:
            fieldnames.append(name)
    paired = []
    exact_token_count = 0
    approx_token_count = 0
    for row in rows:
        exact = float(row["exact_duration_ms"]) if row.get("exact_duration_ms") else None
        approx = float(row["approx_duration_ms"]) if row.get("approx_duration_ms") else None
        difference = approx - exact if approx is not None and exact is not None else None
        percentage = difference / exact * 100.0 if difference is not None and exact > 0 else None
        row["approx_minus_exact_ms"] = difference
        row["approx_difference_percent_of_exact"] = percentage
        has_tokens = bool(row.get("input_tokens"))
        if has_tokens and exact is not None:
            exact_token_count += 1
        if has_tokens and approx is not None:
            approx_token_count += 1
        if difference is not None:
            paired.append({"difference_ms": difference, "difference_percent_of_exact": percentage})
    comparison = summary["exact_vs_approximate"]
    comparison["approx_minus_exact_ms"] = token_distribution([row["difference_ms"] for row in paired])
    comparison["difference_percent_of_exact"] = token_distribution([
        row["difference_percent_of_exact"] for row in paired
        if row["difference_percent_of_exact"] is not None
    ])
    comparison["approx_shorter_than_exact"] = sum(row["difference_ms"] < 0 for row in paired)
    comparison["approx_is_upper_bound_for_all_pairs"] = bool(paired) and all(
        row["difference_ms"] >= 0 for row in paired
    )
    tokens = summary["explicit_token_usage"]
    tokens["readings_with_exact_duration"] = exact_token_count
    tokens["readings_with_approximate_duration"] = approx_token_count
    summary["approximate_upper_bound_timing"]["interpretation"] = (
        "Requested upper-bound proxy; observed pairwise comparisons show this interval is not a guaranteed upper bound."
    )
    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    with summary_path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2, allow_nan=False)
        handle.write("\n")
    (OUT / "report.md").write_text(render_report(summary), encoding="utf-8", newline="\n")
    print(json.dumps({"status": "refreshed_derived_outputs", "paired": len(paired),
                      "token_exact_overlap": exact_token_count,
                      "token_approx_overlap": approx_token_count}, indent=2))


def render_report(summary):
    exact = summary["exact_timing"]
    approx = summary["approximate_upper_bound_timing"]
    comparison = summary["exact_vs_approximate"]
    tokens = summary["explicit_token_usage"]
    lines = [
        "# Compaction timing and token usage",
        "",
        f"Generated: {summary['completed_at_utc']} UTC. Runtime: {summary['elapsed_seconds']:.3f} s.",
        "",
        "## Scope and privacy",
        "",
        f"Read-only PostgreSQL snapshot: {summary['scope']['source_files_with_compacted_records']:,} files, {summary['scope']['compacted_records']:,} compacted records.",
        f"Read exactly {summary['scope']['event_msg_ranges_read']:,} indexed `event_msg` byte ranges ({summary['scope']['indexed_event_msg_bytes_read']:,} bytes). Other record ranges, including compacted payloads, were not opened. No event or conversation content is included.",
        "",
        "## Exact recorded durations",
        "",
        f"Found {exact['candidate_events']:,} `item_completed` / `ContextCompaction` events. `started_at_ms` is present in {exact['events_with_started_at_ms']:,}; `completed_at_ms` in {exact['events_with_completed_at_ms']:,}; both are present in {exact['events_with_both']:,}. Numeric values: started {exact['events_with_numeric_started_at_ms']:,}, completed {exact['events_with_numeric_completed_at_ms']:,}. Valid nonnegative durations: {exact['valid_nonnegative_durations']:,}; missing or invalid: {exact['missing_or_invalid_duration']:,}.",
        f"Field paths observed — start: `{json.dumps(exact['started_at_ms_field_paths'], ensure_ascii=False)}`; completion: `{json.dumps(exact['completed_at_ms_field_paths'], ensure_ascii=False)}`.",
        f"Duration (ms): median {fmt(exact['duration_ms']['median'])}, p90 {fmt(exact['duration_ms']['p90_linear'])}, maximum {fmt(exact['duration_ms']['max'])}.",
        f"Linked to the next compacted record: {exact['linked_to_later_compacted_record']:,}. Spearman with linked compacted-record bytes: n={exact['spearman_duration_vs_linked_compacted_record_bytes']['n']:,}, rho={fmt(exact['spearman_duration_vs_linked_compacted_record_bytes']['spearman'])}.",
        "",
        "## Approximate timing proxy",
        "",
        f"Valid intervals: {approx['valid_durations']:,} of {approx['compacted_records']:,}; missing: {approx['missing_timing_or_usage']:,} (no prior valid usage: {approx['no_prior_valid_response_or_last_usage']:,}; missing start timestamp: {approx['missing_start_timestamp']:,}; missing selected endpoint timestamp: {approx['missing_selected_endpoint_timestamp']:,}; negative chronology: {approx['negative_chronology']:,}).",
        f"Duration (ms): median {fmt(approx['duration_ms']['median'])}, p90 {fmt(approx['duration_ms']['p90_linear'])}, maximum {fmt(approx['duration_ms']['max'])}.",
        "Endpoint is the last `context_compacted` marker after the selected prior usage and before the compacted record; if absent, the compacted record timestamp.",
        "",
        "## Exact versus approximate",
        "",
        f"Both methods are available for {comparison['paired_compacted_records']:,} compacted records. Approximate minus exact duration (ms): minimum {fmt(comparison['approx_minus_exact_ms']['min'])}, median {fmt(comparison['approx_minus_exact_ms']['median'])}, p90 {fmt(comparison['approx_minus_exact_ms']['p90'])}, maximum {fmt(comparison['approx_minus_exact_ms']['max'])}. Percentage difference `(approx − exact) / exact × 100`: median {fmt(comparison['difference_percent_of_exact']['median'])}%, p90 {fmt(comparison['difference_percent_of_exact']['p90'])}%, maximum {fmt(comparison['difference_percent_of_exact']['max'])}%. Approximate was shorter in {comparison['approx_shorter_than_exact']:,}/{comparison['paired_compacted_records']:,} pairs; it therefore did not behave as an upper bound in these paired data.",
        "",
        "## Explicit token readings",
        "",
        f"Found {tokens['compacted_records_with_one_unambiguous_valid_reading']:,} compacted records with one unambiguous, valid PostgreSQL `latest_token_usage_record/usage` reading. Excluded {tokens['duplicate_flagged_or_unclassified_observations_excluded']:,} duplicate-flagged/unclassified and {tokens['invalid_observations_excluded']:,} invalid observations; {tokens['compactions_with_conflicting_valid_readings']:,} records had conflicting readings.",
        f"Only {tokens['readings_with_exact_duration']:,} of these readings overlap a valid exact duration, and {tokens['readings_with_approximate_duration']:,} overlap a valid approximate interval; token-duration association is not estimable from the available matched rows.",
        "",
        "| Counter | n | Median | P90 | Maximum | Sum |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for name in TOKEN_FIELDS:
        stat = tokens["distributions_tokens"][name]
        lines.append(f"| `{name}` | {stat['count']:,} | {fmt(stat['median'])} | {fmt(stat['p90'])} | {fmt(stat['max'])} | {fmt(stat['sum'])} |")
    lines += ["", "Spearman between each token counter and duration:", "", "| Counter | Exact duration n / rho | Approximate duration n / rho |", "|---|---:|---:|"]
    for name in TOKEN_FIELDS:
        exact_corr = tokens["spearman_with_exact_duration_ms"][name]
        approx_corr = tokens["spearman_with_approximate_duration_ms"][name]
        lines.append(f"| `{name}` | {exact_corr['n']:,} / {fmt(exact_corr['spearman'])} | {approx_corr['n']:,} / {fmt(approx_corr['spearman'])} |")
    lines += [
        "",
        "`compaction_response_id` equality was not verified: it is not present as a field in the permitted PostgreSQL token-observation data, and reading compacted source ranges was outside this task's file-read scope. The reported readings are the explicitly indexed latest-usage snapshots attached to compacted rows.",
        "",
        "## Interpretation limits",
        "",
    ]
    lines.extend(f"- {item}" for item in summary["interpretation_limits"])
    lines += ["", "Percentiles use linear interpolation at `(n-1) × p`. Spearman uses average ranks for ties and the Pearson correlation of those ranks. The approximate timestamp proxy was requested as an upper bound, but its observed values were shorter than exact recorded durations in most paired rows; interpret it only as a timestamp interval, not a bound.", ""]
    return "\n".join(lines)


def fmt(value):
    if value is None:
        return "—"
    if isinstance(value, float):
        return f"{value:,.3f}"
    return f"{value:,}" if isinstance(value, int) else str(value)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # Avoid printing exception details that might include sensitive paths or data.
        frames = traceback.extract_tb(error.__traceback__)
        location = " -> ".join(f"{frame.name}:{frame.lineno}" for frame in frames[-3:])
        print(f"analysis_failed: {type(error).__name__} at {location}", file=sys.stderr)
        raise SystemExit(1)
