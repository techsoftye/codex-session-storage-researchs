"""Read-only head/tail index of copied Codex rollout files (Python 3.10+)."""
from __future__ import annotations

import argparse
import ctypes
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import stat
import sys

VERSION = "1.0.2"
ROOT = Path(__file__).resolve().parents[1]
UUID = re.compile(r"(?<![0-9a-f])[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}(?![0-9a-f])", re.I)
META_MARKER = re.compile(rb'"type"\s*:\s*"session_meta"')
CHUNK = 64 * 1024


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def utc_timestamp(value):
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


def parse_object(raw):
    def reject_constant(_):
        raise ValueError("Non-standard JSON constant")
    result = json.loads(raw.decode("utf-8-sig"), parse_constant=reject_constant)
    if not isinstance(result, dict):
        raise ValueError("JSONL record is not an object")
    return result


def reparse_point(path):
    info = path.lstat()
    return stat.S_ISLNK(info.st_mode) or bool(
        getattr(info, "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 1024)
    )


def candidates(path):
    if reparse_point(path):
        raise ValueError("Source cannot be a link or reparse point")
    if path.is_file():
        if path.suffix.lower() != ".jsonl":
            raise ValueError("Source file must have .jsonl extension")
        yield path
        return
    pending = [path]
    while pending:
        with os.scandir(pending.pop()) as entries:
            for entry in entries:
                item = Path(entry.path)
                if reparse_point(item):
                    continue
                if entry.is_dir(follow_symlinks=False):
                    pending.append(item)
                elif entry.is_file(follow_symlinks=False) and item.suffix.lower() == ".jsonl":
                    yield item


def filename_ids(name):
    return list(dict.fromkeys(match.group().lower() for match in UUID.finditer(name)))


def read_head_line(handle, remaining, ordinary_limit, meta_limit, result):
    """Read one beginning record, with a larger bounded allowance for session_meta."""
    first = handle.readline(min(CHUNK, remaining))
    result["indexing"]["head_bytes_read"] += len(first)
    if not first:
        return None, "eof"
    is_meta = bool(META_MARKER.search(first[:4096]))
    allowance = meta_limit if is_meta else min(ordinary_limit, remaining)
    raw = bytearray(first)
    if len(raw) > allowance:
        return None, "session_meta_exceeds_limit" if is_meta else "head_budget_reached_in_record"
    while not raw.endswith(b"\n"):
        available = allowance + 1 - len(raw)
        if not is_meta:
            available = min(available, remaining - len(raw))
        if available <= 0:
            if is_meta:
                return None, "session_meta_exceeds_limit"
            # Skip the remainder of a large ordinary record within the HEAD budget.
            consumed = len(raw)
            while consumed < remaining:
                part = handle.readline(min(CHUNK, remaining - consumed))
                result["indexing"]["head_bytes_read"] += len(part)
                consumed += len(part)
                if not part or part.endswith(b"\n"):
                    return None, "head_record_skipped"
            return None, "head_budget_reached_in_record"
        part = handle.readline(min(CHUNK, available))
        result["indexing"]["head_bytes_read"] += len(part)
        if not part:
            break
        raw.extend(part)
        if len(raw) > allowance:
            if is_meta:
                return None, "session_meta_exceeds_limit"
            # The remaining part is drained on the next loop without retaining it.
    if not raw.strip():
        return None, "blank"
    return bytes(raw), "record"


def inspect_head(handle, file_size, head_limit, ordinary_limit, meta_limit, result):
    flags = set()
    identity = result["identity"]
    session = result["session"]
    context = result["initial_context"]
    info = result["indexing"]
    while info["head_bytes_read"] < min(head_limit, file_size):
        remaining = min(head_limit, file_size) - info["head_bytes_read"]
        raw, status = read_head_line(handle, remaining, ordinary_limit, meta_limit, result)
        if status == "eof":
            break
        if status in {"session_meta_exceeds_limit", "head_budget_reached_in_record"}:
            flags.add(status)
            break
        if status == "head_record_skipped":
            flags.add(status)
            continue
        if status == "blank":
            continue
        try:
            record = parse_object(raw)
        except (ValueError, UnicodeError, RecursionError):
            flags.add("invalid_head_record")
            continue
        payload = record.get("payload")
        if not isinstance(payload, dict):
            continue
        kind = record.get("type")
        if kind == "session_meta":
            info["session_meta_found"] = True
            identifier = short_text(payload.get("id"))
            if identifier and identity["observed_session_meta_ids"] == []:
                identity["observed_session_meta_ids"].append(identifier)
            elif identifier and identifier not in identity["observed_session_meta_ids"]:
                identity["observed_session_meta_ids"].append(identifier)
            if session["session_started_at"] is None:
                session["session_started_at"] = utc_timestamp(payload.get("timestamp"))
            for target, source in (("session_source", "source"), ("originator", "originator"),
                                   ("cli_version", "cli_version"), ("model_provider", "model_provider")):
                if session[target] is None:
                    session[target] = short_text(payload.get(source))
        elif kind == "turn_context":
            found = {}
            for target, source in (("first_observed_model", "model"),
                                   ("first_observed_reasoning_effort", "effort"),
                                   ("first_observed_approval_policy", "approval_policy")):
                found[target] = short_text(payload.get(source))
            sandbox = payload.get("sandbox_policy")
            found["first_observed_sandbox_mode"] = short_text(sandbox.get("type")) if isinstance(sandbox, dict) else None
            if any(found.values()):
                info["turn_context_found"] = True
                for key, value in found.items():
                    if context[key] is None:
                        context[key] = value
        if info["session_meta_found"] and info["turn_context_found"]:
            break
    if not info["session_meta_found"] and info["head_bytes_read"] >= head_limit and head_limit < file_size:
        flags.add("session_meta_not_found_within_head_limit")
    if not info["turn_context_found"] and info["head_bytes_read"] >= head_limit and head_limit < file_size:
        flags.add("turn_context_not_found_within_head_limit")
    return flags


def inspect_tail(handle, file_size, tail_limit, result):
    info = result["indexing"]
    start = max(0, file_size - tail_limit)
    handle.seek(start)
    tail = handle.read(min(file_size, tail_limit))
    info["tail_bytes_read"] = len(tail)
    data = tail.rstrip(b" \t\r\n")
    if not data:
        return "empty_file" if file_size == 0 else "last_record_not_found_within_tail"
    # When tail begins mid-record, discard its first incomplete fragment.
    if start:
        boundary = data.find(b"\n")
        if boundary < 0:
            return "record_exceeds_tail_limit"
        data = data[boundary + 1:]
    parts = data.split(b"\n")
    trailing_invalid = False
    for part in reversed(parts):
        if not part.strip():
            continue
        try:
            record = parse_object(part)
        except (ValueError, UnicodeError, RecursionError):
            trailing_invalid = True
            continue
        moment = utc_timestamp(record.get("timestamp"))
        if moment is not None:
            result["session"]["session_last_record_at"] = moment
            info["last_record_found"] = True
            return "prior_complete_after_invalid_tail" if trailing_invalid else "found"
        trailing_invalid = True
    return "invalid_or_missing_tail_timestamp"


def resolve_identity(result):
    identity = result["identity"]
    ids = identity["observed_session_meta_ids"]
    candidates_from_name = identity["candidate_session_ids"]
    if ids and ids[0].lower() in candidates_from_name:
        # The first top-level session_meta identifies this rollout file.
        # Later IDs may be inherited history; keep them as observations.
        identity.update(canonical_session_id=ids[0], identity_source="session_meta.payload.id",
                        identity_status="verified", filename_id_matches=True, identity_confidence="high")
    elif len(ids) > 1:
        identity.update(canonical_session_id=None, identity_source="unresolved",
                        identity_status="conflicting", filename_id_matches=False if candidates_from_name else None,
                        identity_confidence="low")
    elif len(ids) == 1:
        canonical = ids[0]
        match = canonical.lower() in candidates_from_name if candidates_from_name else None
        status = "verified" if match else ("verified_filename_mismatch" if match is False else "session_meta_only")
        confidence = "high" if match else "medium"
        identity.update(canonical_session_id=canonical, identity_source="session_meta.payload.id",
                        identity_status=status, filename_id_matches=match, identity_confidence=confidence)
    elif len(candidates_from_name) == 1:
        identity.update(canonical_session_id=candidates_from_name[0], identity_source="filename_only",
                        identity_status="filename_only", filename_id_matches=None, identity_confidence="low")
    else:
        identity.update(canonical_session_id=None, identity_source="unresolved",
                        identity_status="unresolved", filename_id_matches=None, identity_confidence="low")


def index_file(path, root, head_limit=4*1024*1024, tail_limit=512*1024,
               ordinary_limit=512*1024, meta_limit=16*1024*1024):
    item = {
        "file": {"file_name": path.name, "source_relative_path": path.relative_to(root).as_posix(),
                 "file_format": "jsonl", "file_size_bytes": None, "file_modified_at": None},
        "identity": {"candidate_session_ids": filename_ids(path.name), "observed_session_meta_ids": [],
                     "canonical_session_id": None, "identity_source": "unresolved", "identity_status": "unresolved",
                     "filename_id_matches": None, "identity_confidence": "low"},
        "session": {"session_started_at": None, "session_last_record_at": None,
                    "session_source": None, "originator": None, "cli_version": None, "model_provider": None},
        "initial_context": {"first_observed_model": None, "first_observed_reasoning_effort": None,
                            "first_observed_approval_policy": None, "first_observed_sandbox_mode": None},
        "indexing": {"index_version": VERSION, "index_method": "head_tail", "index_started_at": utc_now(),
                     "index_completed_at": None, "head_bytes_read": 0, "tail_bytes_read": 0,
                     "session_meta_found": False, "turn_context_found": False, "last_record_found": False,
                     "tail_status": "not_attempted", "metadata_index_status": "running",
                     "full_analysis_status": "pending", "error_code": None, "error_message": None},
    }
    flags = set()
    try:
        if reparse_point(path):
            raise ValueError("Source link is not allowed")
        before = path.stat()
        item["file"]["file_size_bytes"] = before.st_size
        item["file"]["file_modified_at"] = datetime.fromtimestamp(
            before.st_mtime, timezone.utc).isoformat().replace("+00:00", "Z")
        with path.open("rb") as handle:
            flags = inspect_head(handle, before.st_size, head_limit, ordinary_limit, meta_limit, item)
            tail_status = inspect_tail(handle, before.st_size, tail_limit, item)
            item["indexing"]["tail_status"] = tail_status
            if tail_status != "found":
                flags.add(tail_status)
        after = path.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            flags.add("source_changed_during_index")
        resolve_identity(item)
        if item["identity"]["identity_status"] != "verified":
            flags.add("identity_" + item["identity"]["identity_status"])
        if not item["indexing"]["turn_context_found"]:
            flags.add("turn_context_not_found")
        if not item["indexing"]["last_record_found"]:
            flags.add("last_record_not_found")
        if flags:
            item["indexing"].update(metadata_index_status="needs_review",
                                    error_code=",".join(sorted(flags)),
                                    error_message="Some metadata could not be verified within configured read limits.")
        else:
            item["indexing"]["metadata_index_status"] = "completed"
    except (OSError, ValueError) as error:
        item["indexing"].update(metadata_index_status="failed", error_code=type(error).__name__,
                                error_message="Source metadata could not be safely read.")
        resolve_identity(item)
    item["indexing"]["index_completed_at"] = utc_now()
    return item


def lower_priority():
    if os.name != "nt":
        return "unchanged"
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.GetCurrentProcess.restype = ctypes.c_void_p
    kernel.SetPriorityClass.argtypes = (ctypes.c_void_p, ctypes.c_uint32)
    return "below_normal" if kernel.SetPriorityClass(kernel.GetCurrentProcess(), 0x4000) else "unchanged"


def bounded_int(value):
    number = int(value)
    if number <= 0 or number > 64:
        raise argparse.ArgumentTypeError("Choose an integer from 1 to 64")
    return number


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default="2026", help="Research-relative JSONL file or directory")
    parser.add_argument("--output", default="data/session_index_2026.json", help="New JSON export; existing files are not overwritten")
    parser.add_argument("--head-mib", type=bounded_int, default=4)
    parser.add_argument("--tail-kib", type=bounded_int, default=512)
    parser.add_argument("--metadata-record-mib", type=bounded_int, default=16)
    parser.add_argument("--limit-files", type=bounded_int, help="Small test batch; capped at 64 files")
    args = parser.parse_args(argv)
    root = ROOT
    source = Path(os.path.abspath(root / args.source))
    output = Path(os.path.abspath(root / args.output))
    if not source.is_relative_to(root / "2026") or not output.is_relative_to(root):
        parser.error("Source must be inside copied 2026 dataset; output must stay inside project")
    if output.is_relative_to(root / "2026"):
        parser.error("Output cannot be written inside copied dataset")
    for target in (source, output):
        for component in (target, *target.parents):
            if component == root:
                break
            if component.exists() and reparse_point(component):
                parser.error("Links and reparse points are not allowed in paths")
    if not source.exists():
        parser.error("Source does not exist")
    if output.exists():
        parser.error("Output already exists; select a new name")
    priority = lower_priority()
    output.parent.mkdir(parents=True, exist_ok=True)
    count = total_size = bytes_read = 0
    identities = {"verified": 0, "filename_only": 0, "unresolved": 0, "other": 0}
    errors = 0
    export_status = "completed"
    with output.open("x", encoding="utf-8", newline="\n") as destination:
        header = {"index_version": VERSION, "research_root": str(root), "source": source.relative_to(root).as_posix(),
                  "started_at": utc_now(), "process_priority": priority}
        destination.write(json.dumps(header, ensure_ascii=False, indent=2)[:-2] + ',\n  "items": [\n')
        try:
            for path in candidates(source):
                item = index_file(path, root, head_limit=args.head_mib * 1024**2,
                                  tail_limit=args.tail_kib * 1024,
                                  meta_limit=args.metadata_record_mib * 1024**2)
                destination.write((",\n" if count else "") + json.dumps(item, ensure_ascii=False, indent=2))
                destination.flush()
                count += 1
                total_size += item["file"]["file_size_bytes"] or 0
                info = item["indexing"]
                bytes_read += info["head_bytes_read"] + info["tail_bytes_read"]
                status = item["identity"]["identity_status"]
                identities[status if status in identities else "other"] += 1
                errors += info["metadata_index_status"] == "failed"
                print(f"[{count}] {item['file']['source_relative_path']} | {info['metadata_index_status']} | "
                      f"{info['head_bytes_read'] + info['tail_bytes_read']} bytes read", flush=True)
                if args.limit_files and count >= args.limit_files:
                    export_status = "file_limit_reached"
                    break
        except KeyboardInterrupt:
            export_status = "interrupted"
        except (OSError, ValueError) as error:
            export_status = "failed"
            print(f"Index stopped: {type(error).__name__}", file=sys.stderr)
        summary = {"completed_at": utc_now(), "export_status": export_status, "files_indexed": count,
                   "source_bytes_represented": total_size, "verified_identity_count": identities["verified"],
                   "filename_only_identity_count": identities["filename_only"],
                   "unresolved_identity_count": identities["unresolved"],
                   "other_identity_count": identities["other"], "error_count": errors,
                   "bytes_actually_read": bytes_read,
                   "read_ratio_percent": round(100 * bytes_read / total_size, 6) if total_size else None}
        destination.write('\n  ],\n' + json.dumps(summary, ensure_ascii=False, indent=2)[2:])
    print(json.dumps(summary, ensure_ascii=False))
    return 130 if export_status == "interrupted" else (1 if export_status == "failed" or errors else 0)


if __name__ == "__main__":
    raise SystemExit(main())
