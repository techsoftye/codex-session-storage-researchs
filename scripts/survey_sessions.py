"""Read-only, bounded-memory JSONL survey. Standard library only, Python 3.10+."""
from __future__ import annotations

import argparse
import ctypes
import json
import os
from pathlib import Path
import re
import stat
import sys
import time
from datetime import datetime, timezone

VERSION = "1.0.1"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
UUID_PATTERN = re.compile(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", re.I)
COUNTERS = (
    "scanned_record_count", "valid_record_count", "invalid_record_count",
    "session_meta_record_count", "response_item_record_count", "event_msg_record_count",
    "turn_context_record_count", "user_role_message_count", "assistant_role_message_count",
    "user_message_event_count", "ghost_snapshot_record_count",
)


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def timestamp(value):
    if not isinstance(value, str) or len(value) > 100:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except ValueError:
        return None


def short_text(value):
    return value if isinstance(value, str) and 0 < len(value) <= 256 else None


def reject_constant(value):
    raise ValueError("Non-standard JSON constant")


def parse_record(raw):
    result = json.loads(raw.decode("utf-8-sig"), parse_constant=reject_constant)
    if not isinstance(result, dict):
        raise ValueError("Expected an object record")
    return result


def is_link(path):
    info = path.lstat()
    return stat.S_ISLNK(info.st_mode) or bool(
        getattr(info, "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 1024)
    )


def candidate_files(source):
    """Do not follow symlinks or Windows junctions, including nested entries."""
    if is_link(source):
        raise ValueError("Source links/reparse points are not allowed")
    if source.is_file():
        if source.suffix.lower() != ".jsonl":
            raise ValueError("Source file must have .jsonl extension")
        yield source
        return
    pending = [source]
    while pending:
        directory = pending.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                path = Path(entry.path)
                if is_link(path):
                    continue
                if entry.is_dir(follow_symlinks=False):
                    pending.append(path)
                elif entry.is_file(follow_symlinks=False) and path.suffix.lower() == ".jsonl":
                    yield path


class Pacer:
    def __init__(self, delay):
        self.delay = delay
        self.bytes = 0
        self.lines = 0

    def tick(self, size):
        self.bytes += size
        self.lines += 1
        if self.bytes >= 256 * 1024 or self.lines >= 128:
            if self.delay:
                time.sleep(self.delay)
            self.bytes = self.lines = 0


def bounded_records(handle, budget, record_limit, pacer, result):
    """Yield complete lines only; discard oversized lines in bounded pieces."""
    while result["scanned_bytes"] < budget:
        remaining = budget - result["scanned_bytes"]
        raw = handle.readline(min(record_limit + 1, remaining))
        if not raw:
            return
        result["scanned_bytes"] += len(raw)
        pacer.tick(len(raw))
        oversized = len(raw) > record_limit
        terminated = raw.endswith(b"\n")
        while oversized and not terminated and result["scanned_bytes"] < budget:
            chunk = handle.readline(min(64 * 1024, budget - result["scanned_bytes"]))
            if not chunk:
                break
            result["scanned_bytes"] += len(chunk)
            pacer.tick(len(chunk))
            terminated = chunk.endswith(b"\n")
        complete = terminated or result["scanned_bytes"] >= result["file_size_bytes"]
        if not complete:
            result["metadata"]["partial_record_at_limit"] = True
            return
        if oversized:
            yield None
        elif raw.strip():
            yield raw
        else:
            result["metadata"]["blank_line_count"] += 1


def read_last_timestamp(handle, size, limit):
    start = max(0, size - limit)
    handle.seek(start)
    raw = handle.read(min(size, limit))
    inspected = len(raw)
    raw = raw.rstrip(b" \t\r\n")
    if not raw:
        return None, "empty_file" if not size else "not_found_within_limit", inspected
    boundary = raw.rfind(b"\n")
    if boundary < 0 and start:
        return None, "record_exceeds_tail_limit", inspected
    try:
        record = parse_record(raw[boundary + 1:])
    except (ValueError, UnicodeError, RecursionError):
        return None, "invalid_last_record", inspected
    value = timestamp(record.get("timestamp"))
    return value, "found" if value else "timestamp_missing_or_invalid", inspected


def observe(record, result, shapes):
    root_shape = tuple(sorted(record))
    if shapes["root"] is None:
        shapes["root"] = root_shape
        result["uniform_envelope_observed"] = True
    elif shapes["root"] != root_shape:
        result["uniform_envelope_observed"] = False
    event_time = timestamp(record.get("timestamp"))
    if event_time:
        if result["first_observed_event_at"] is None:
            result["first_observed_event_at"] = event_time
        result["last_observed_event_at"] = event_time
    kind = record.get("type")
    for known in ("session_meta", "response_item", "event_msg", "turn_context"):
        if kind == known:
            result[known + "_record_count"] += 1
            break
    else:
        result["metadata"]["unknown_record_type_count"] += 1
    payload = record.get("payload")
    if not isinstance(payload, dict):
        result["metadata"]["non_object_payload_count"] += 1
        return
    shape = tuple(sorted(payload))
    if shapes["payload"] is None:
        shapes["payload"] = shape
        result["multiple_payload_shapes_observed"] = False
    elif shape != shapes["payload"]:
        result["multiple_payload_shapes_observed"] = True
    result["instructions_observed"] = bool(
        result["instructions_observed"] or payload.get("instructions") or payload.get("base_instructions")
    )
    if kind == "session_meta":
        identifier = short_text(payload.get("id"))
        if result["session_id"] is None and identifier:
            result["session_id"] = identifier
            result["identity_source"] = "session_meta.payload.id"
            candidates = UUID_PATTERN.findall(result["file_name"])
            result["filename_id_matches"] = identifier.lower() in [s.lower() for s in candidates] if candidates else None
        elif identifier and identifier != result["session_id"]:
            result["metadata"]["conflicting_session_ids_observed"] = True
        for target, source in (("session_source", "source"), ("originator", "originator"),
                               ("cli_version", "cli_version"), ("model_provider", "model_provider")):
            if result[target] is None:
                result[target] = short_text(payload.get(source))
        if result["session_started_at"] is None:
            result["session_started_at"] = timestamp(payload.get("timestamp"))
    if kind == "turn_context":
        for target, source in (("first_observed_model", "model"),
                               ("first_observed_reasoning_effort", "effort"),
                               ("first_observed_approval_policy", "approval_policy")):
            if result[target] is None:
                result[target] = short_text(payload.get(source))
        sandbox = payload.get("sandbox_policy")
        if result["first_observed_sandbox_mode"] is None and isinstance(sandbox, dict):
            result["first_observed_sandbox_mode"] = short_text(sandbox.get("type"))
    subtype = payload.get("type")
    if kind == "response_item" and subtype == "message":
        for role in ("user", "assistant"):
            if payload.get("role") == role:
                result[role + "_role_message_count"] += 1
    if kind == "response_item" and subtype == "ghost_snapshot":
        result["ghost_snapshot_record_count"] += 1
    if kind == "event_msg" and subtype == "user_message":
        result["user_message_event_count"] += 1


def survey_file(path, root, head_limit=1024*1024, tail_limit=256*1024,
                record_limit=256*1024, delay=0.01, full=False):
    result = {
        "file_name": path.name, "source_relative_path": path.relative_to(root).as_posix(),
        "file_size_bytes": None, "file_format": "jsonl", "file_modified_at": None,
        **{key: None for key in (
            "session_id", "identity_source", "filename_id_matches", "session_started_at",
            "session_last_record_at", "session_source", "originator", "cli_version", "model_provider",
            "first_observed_model", "first_observed_reasoning_effort", "first_observed_approval_policy",
            "first_observed_sandbox_mode", "first_observed_event_at", "last_observed_event_at",
            "instructions_observed", "uniform_envelope_observed", "multiple_payload_shapes_observed",
        )},
        "scan_version": VERSION, "scan_started_at": utc_now(), "scan_completed_at": None,
        "scan_complete": False, "scanned_bytes": 0, **dict.fromkeys(COUNTERS, 0),
        "scan_status": "running", "scan_error_code": None, "scan_error": None,
        "metadata": {
            "scan_mode": "full" if full else "preview", "head_limit_bytes": None if full else head_limit,
            "tail_limit_bytes": tail_limit, "max_record_bytes": record_limit,
            "oversized_record_count": 0, "blank_line_count": 0, "partial_record_at_limit": False,
            "unknown_record_type_count": 0, "non_object_payload_count": 0,
            "conflicting_session_ids_observed": False, "tail_bytes_read": 0,
            "last_record_read_status": "not_attempted", "scan_counters_exclude_tail_probe": True,
            "record_parsing_complete": False,
        },
    }
    try:
        if is_link(path):
            raise ValueError("Source links are not allowed")
        before = path.stat()
        result["file_size_bytes"] = before.st_size
        result["file_modified_at"] = datetime.fromtimestamp(before.st_mtime, timezone.utc).isoformat().replace("+00:00", "Z")
        shapes = {"root": None, "payload": None}
        with path.open("rb") as handle:
            budget = before.st_size if full else min(head_limit, before.st_size)
            for raw in bounded_records(handle, budget, record_limit, Pacer(delay), result):
                result["scanned_record_count"] += 1
                if raw is None:
                    result["metadata"]["oversized_record_count"] += 1
                    continue
                try:
                    record = parse_record(raw)
                except (ValueError, UnicodeError, RecursionError):
                    result["invalid_record_count"] += 1
                    continue
                result["valid_record_count"] += 1
                observe(record, result, shapes)
            result["scan_complete"] = result["scanned_bytes"] == before.st_size
            end, status, read_bytes = read_last_timestamp(handle, before.st_size, tail_limit)
            result["session_last_record_at"] = end
            result["metadata"].update(last_record_read_status=status, tail_bytes_read=read_bytes)
        after = path.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            result["scan_complete"] = False
            result.update(scan_status="needs_review", scan_error_code="source_changed",
                          scan_error="Source size or modification time changed during the scan.")
        elif (result["invalid_record_count"] or result["metadata"]["oversized_record_count"]
              or result["metadata"]["conflicting_session_ids_observed"]):
            result["scan_status"] = "needs_review"
        else:
            result["scan_status"] = "completed" if result["scan_complete"] else "partial"
    except (OSError, ValueError) as error:
        result.update(scan_complete=False, scan_status="failed", scan_error_code=type(error).__name__,
                      scan_error="File could not be safely read; no raw content included.")
    result["metadata"]["record_parsing_complete"] = bool(
        result["scan_complete"] and not result["invalid_record_count"]
        and not result["metadata"]["oversized_record_count"]
    )
    result["scan_completed_at"] = utc_now()
    return result


def lower_priority():
    if os.name == "nt":
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.GetCurrentProcess.restype = ctypes.c_void_p
        kernel.SetPriorityClass.argtypes = (ctypes.c_void_p, ctypes.c_uint32)
        return "below_normal" if kernel.SetPriorityClass(kernel.GetCurrentProcess(), 0x4000) else "unchanged"
    return "unchanged"


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("Must be positive")
    return number


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default="2026", help="Research-relative directory or one JSONL file (default: 2026)")
    parser.add_argument("--output", default="data/session_survey_2026.json", help="New output file; never overwritten")
    parser.add_argument("--full", action="store_true", help="Scan all bytes; still skip records above the parsing limit")
    parser.add_argument("--head-mib", type=positive_int, default=1)
    parser.add_argument("--tail-kib", type=positive_int, default=256)
    parser.add_argument("--max-record-kib", type=positive_int, default=256)
    parser.add_argument("--delay-ms", type=positive_int, default=10)
    parser.add_argument("--limit-files", type=positive_int)
    parser.add_argument("--dry-run", action="store_true", help="Filesystem metadata only; no content reads or output writes")
    args = parser.parse_args(argv)
    root = PROJECT_ROOT
    source = Path(os.path.abspath(root / args.source))
    output = Path(os.path.abspath(root / args.output))
    if not source.is_relative_to(root) or not output.is_relative_to(root):
        parser.error("Source and output must stay inside the research project")
    for target in (source, output):
        for component in (target, *target.parents):
            if component == root:
                break
            if component.exists() and is_link(component):
                parser.error("Links and reparse points are not allowed in source/output paths")
    if not source.exists():
        parser.error("Source does not exist")
    if output.is_relative_to(root / "2026") or output == source or (source.is_dir() and output.is_relative_to(source)):
        parser.error("Output must be outside the source dataset")
    if args.tail_kib > 16384 or args.max_record_kib > 16384:
        parser.error("Tail and record limits cannot exceed 16 MiB")
    if output.exists() and not args.dry_run:
        parser.error("Output already exists; choose a new --output name")
    priority = lower_priority()
    if args.dry_run:
        count = total = 0
        for path in candidate_files(source):
            count += 1
            total += path.stat().st_size
            if args.limit_files and count >= args.limit_files:
                break
        print(json.dumps({"candidate_file_count": count, "candidate_bytes": total, "content_read": False}))
        return 0
    output.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    statuses = {}
    started = utc_now()
    export_status = "completed"
    # Streaming output: only the current record and current file summary are held in RAM.
    with output.open("x", encoding="utf-8", newline="\n") as destination:
        header = {"survey_version": VERSION, "research_root": str(root), "source": source.relative_to(root).as_posix(),
                  "started_at": started, "process_priority": priority, "requested_file_limit": args.limit_files}
        destination.write(json.dumps(header, ensure_ascii=False, indent=2)[:-2] + ',\n  "sessions": [\n')
        try:
            for path in candidate_files(source):
                result = survey_file(path, root, args.head_mib * 1024**2, args.tail_kib * 1024,
                                     args.max_record_kib * 1024, args.delay_ms / 1000, args.full)
                serialized = json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False)
                destination.write((",\n" if count else "") + serialized)
                destination.flush()
                count += 1
                status = result["scan_status"]
                statuses[status] = statuses.get(status, 0) + 1
                print(f"[{count}] {result['source_relative_path']} | {status} | {result['scanned_bytes']} bytes", flush=True)
                time.sleep(args.delay_ms / 1000)
                if args.limit_files and count >= args.limit_files:
                    export_status = "file_limit_reached"
                    break
        except KeyboardInterrupt:
            export_status = "interrupted"
        except (OSError, ValueError) as error:
            export_status = "failed"
            print(f"Survey stopped: {type(error).__name__}; see export_status.", file=sys.stderr)
        footer = {"completed_at": utc_now(), "export_status": export_status, "files_written": count, "file_status_counts": statuses}
        destination.write('\n  ],\n' + json.dumps(footer, ensure_ascii=False, indent=2)[2:])
    print(f"Saved {count} summaries to {output}; status={export_status}")
    return 130 if export_status == "interrupted" else (1 if export_status == "failed" or statuses.get("failed") else 0)


if __name__ == "__main__":
    raise SystemExit(main())
