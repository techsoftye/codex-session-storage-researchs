"""Collect bounded identity evidence for compound-name rollout files.

Reads the completed derived index and only the first records of its two-UUID
files. Never reads conversation bodies or writes to PostgreSQL.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path, PurePosixPath
import re
import sys

import fast_session_index as fast
import import_session_survey as common

ROOT = Path(__file__).resolve().parents[1]
META_LIMIT = 16 * 1024 * 1024
EARLY_LIMIT = 1024 * 1024
EARLY_RECORDS = 4
LOCAL_NAME = re.compile(r"^rollout-(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2})-")


def safe_source(relative):
    if not isinstance(relative, str) or "\\" in relative or "\x00" in relative:
        raise ValueError("Invalid source path")
    parts = relative.split("/")
    if parts[0] != "2026" or any(not part or part in {".", ".."} or ":" in part for part in parts):
        raise ValueError("Invalid source path")
    if PurePosixPath(relative).suffix.lower() != ".jsonl":
        raise ValueError("Expected JSONL source")
    path = ROOT.joinpath(*parts)
    for component in (path, *path.parents):
        if component == ROOT:
            break
        if component.exists() and fast.reparse_point(component):
            raise ValueError("Source path contains a link or reparse point")
    return path


def read_record(handle, limit):
    raw = handle.readline(limit + 1)
    if not raw:
        return None, 0, "eof"
    if len(raw) > limit or not raw.endswith(b"\n"):
        return None, len(raw), "record_exceeds_limit"
    try:
        return fast.parse_object(raw), len(raw), None
    except (UnicodeError, ValueError, RecursionError):
        return None, len(raw), "invalid_record"


def examine(item):
    info, identity = item["file"], item["identity"]
    relative = info["source_relative_path"]
    candidates = identity["candidate_session_ids"]
    if len(candidates) != 2 or fast.filename_ids(info["file_name"]) != [c.lower() for c in candidates]:
        raise ValueError("Compound filename candidates are inconsistent")
    path = safe_source(relative)
    result = {"source_relative_path": relative, "file_size_bytes": info["file_size_bytes"],
              "filename_candidates": candidates, "session_meta_id": None,
              "session_meta_session_id": None, "history_base_thread_id": None,
              "thread_settings_seen": False, "thread_settings_thread_id": None,
              "session_meta_timestamp": None, "filename_timestamp_delta_seconds": None,
              "forked_from_id": None,
              "decision": "unresolved", "selected_session_id": None,
              "reason": None, "bytes_read": 0}
    try:
        before = path.stat()
        if before.st_size != info["file_size_bytes"]:
            result["reason"] = "file_size_changed_since_index"
            return result
        with path.open("rb") as handle:
            first, read, error = read_record(handle, META_LIMIT)
            result["bytes_read"] += read
            if error or first.get("type") != "session_meta" or not isinstance(first.get("payload"), dict):
                result["reason"] = error or "first_record_not_session_meta"
                return result
            payload = first["payload"]
            result["session_meta_id"] = fast.short_text(payload.get("id"))
            result["session_meta_session_id"] = fast.short_text(payload.get("session_id"))
            result["session_meta_timestamp"] = fast.utc_timestamp(payload.get("timestamp"))
            result["forked_from_id"] = fast.short_text(payload.get("forked_from_id"))
            history = payload.get("history_base")
            if isinstance(history, dict):
                result["history_base_thread_id"] = fast.short_text(history.get("thread_id"))
            budget = EARLY_LIMIT
            for _ in range(EARLY_RECORDS):
                if budget <= 0:
                    break
                record, read, error = read_record(handle, budget)
                result["bytes_read"] += read
                budget -= read
                if error or record is None:
                    break
                event = record.get("payload")
                if (record.get("type") == "event_msg" and isinstance(event, dict)
                        and event.get("type") == "thread_settings_applied"):
                    result["thread_settings_seen"] = True
                    result["thread_settings_thread_id"] = fast.short_text(event.get("thread_id"))
                    break
        after = path.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            result["reason"] = "source_changed_during_read"
            return result
        first_candidate, second_candidate = (c.lower() for c in candidates)
        match = LOCAL_NAME.match(info["file_name"])
        if match and result["session_meta_timestamp"]:
            file_time = datetime.strptime(match.group(1), "%Y-%m-%dT%H-%M-%S").replace(
                tzinfo=timezone(timedelta(hours=3)))
            meta_time = datetime.fromisoformat(result["session_meta_timestamp"].replace("Z", "+00:00"))
            result["filename_timestamp_delta_seconds"] = round((meta_time - file_time).total_seconds(), 6)
        meta_ids_match = (isinstance(result["session_meta_id"], str)
                          and result["session_meta_id"].lower() == first_candidate
                          and isinstance(result["session_meta_session_id"], str)
                          and result["session_meta_session_id"].lower() == first_candidate)
        settings_id = result["thread_settings_thread_id"]
        settings_consistent = settings_id is None or settings_id.lower() == first_candidate
        delta = result["filename_timestamp_delta_seconds"]
        if meta_ids_match and settings_consistent and delta is not None and 0 <= delta < 2:
            result.update(decision="verified_first_filename_id", selected_session_id=candidates[0],
                          reason="session_meta.id and session_meta.session_id match first filename ID; filename time matches metadata; settings thread ID agrees when present")
        elif settings_id is not None and settings_id.lower() == second_candidate:
            result["reason"] = "settings_thread_id_matches_second_filename_id"
        elif not meta_ids_match:
            result["reason"] = "session_meta_ids_do_not_match_first_filename_id"
        elif not settings_consistent:
            result["reason"] = "settings_thread_id_conflicts_with_session_meta"
        elif delta is None or not 0 <= delta < 2:
            result["reason"] = "filename_time_does_not_match_session_meta"
        else:
            result["reason"] = "identity_evidence_incomplete"
    except OSError:
        result["reason"] = "source_read_failed"
    return result


def run(index_path, output_path, limit=None):
    with index_path.open("rb") as handle:
        raw = handle.read(common.MAX_EXPORT_BYTES + 1)
    if len(raw) > common.MAX_EXPORT_BYTES:
        raise ValueError("Derived index exceeds 32 MiB")
    report = json.loads(raw.decode("utf-8-sig"), object_pairs_hook=common.unique_object,
                        parse_constant=common.reject_constant)
    if (report.get("index_version") != fast.VERSION or report.get("export_status") != "completed"
            or Path(report.get("research_root", "")).resolve() != ROOT or report.get("source") != "2026"
            or report.get("files_indexed") != len(report.get("items", []))):
        raise ValueError("Index is incomplete or belongs to another project")
    selected = [item for item in report["items"] if len(item["identity"]["candidate_session_ids"]) == 2]
    selected.sort(key=lambda item: (item["file"]["file_size_bytes"], item["file"]["source_relative_path"]))
    if not selected:
        raise ValueError("No compound-name files in index")
    if limit is not None:
        selected = selected[:limit]
    priority = fast.lower_priority()
    results = []
    for item in selected:
        result = examine(item)
        results.append(result)
        print(f"[{len(results)}] {result['source_relative_path']} | {result['decision']} | "
              f"{result['bytes_read']} bytes read", flush=True)
    completed = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    result = {"report_version": "1.0.1", "source_index": index_path.relative_to(ROOT).as_posix(),
              "report_scope": "sample" if limit is not None else "all_compound_names",
              "filename_time_offset_assumed": "+03:00", "process_priority": priority,
              "completed_at": completed, "files_examined": len(results),
              "verified_first_id_count": sum(row["decision"] == "verified_first_filename_id" for row in results),
              "unresolved_count": sum(row["decision"] == "unresolved" for row in results),
              "bytes_actually_read": sum(row["bytes_read"] for row in results), "items": results}
    with output_path.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(result, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index", default="data/session_index_2026_v102.json")
    parser.add_argument("--output", default="data/compound_session_id_evidence_2026.json")
    parser.add_argument("--limit-files", type=int, help="Smallest N compound-name files for a sample")
    args = parser.parse_args(argv)
    try:
        if args.limit_files is not None and not 1 <= args.limit_files <= 18:
            raise ValueError("--limit-files must be between 1 and 18")
        index_path = common.project_file(args.index)
        output_path = (ROOT / args.output).resolve()
        if not output_path.is_relative_to(ROOT) or output_path.is_relative_to(ROOT / "2026"):
            raise ValueError("Output must be in the project outside 2026")
        if output_path.exists():
            raise ValueError("Output already exists; choose a new name")
        for component in (output_path, *output_path.parents):
            if component == ROOT:
                break
            if component.exists() and fast.reparse_point(component):
                raise ValueError("Output path contains a link or reparse point")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        result = run(index_path, output_path, args.limit_files)
        print(json.dumps({key: result[key] for key in ("files_examined", "verified_first_id_count",
                                                         "unresolved_count", "bytes_actually_read")}, ensure_ascii=False))
        return 0 if result["unresolved_count"] == 0 else 2
    except (OSError, ValueError, TypeError, RecursionError) as error:
        print(f"Compound identity check stopped: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
