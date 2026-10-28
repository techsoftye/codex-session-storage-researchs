"""Profile indexed response_item or event_msg structures without exporting content.

Reads exact indexed byte ranges, one record at a time, at below-normal priority.
No database writes. Per-file checkpoints permit restarting an interrupted run.
Only type/role enums, JSON key paths, shapes and counts leave the parser.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
import ctypes
import hashlib
import itertools
import json
import os
from pathlib import Path
import re
import subprocess
import time

from import_session_survey import connection_settings, find_psql, unique_object, reject_constant
from index_session_records import ROOT, json_rows, lower_priority, safe_source_path, utc_now
from repair_null_record_types import save

VERSION = "1.0.0"
OUT = ROOT / "data/reports/response_item_profile_v1"
SAFE_KEY = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,63}\Z")
SAFE_ENUM = re.compile(r"[A-Za-z_][A-Za-z0-9_.:-]{0,79}\Z")
ENUM_PATHS = {
    "payload.role", "payload.status", "payload.phase", "payload.channel",
    "payload.content[].type", "payload.output[].type", "payload.summary[].type",
    "payload.item.type", "payload.item.role", "payload.item.content[].type",
    "payload.content[].content[].type", "payload.output[].content[].type",
}
MAX_DEPTH = 64
MAX_FIELD_SHAPES = 10000
MAX_RECORD_BYTES = 128 * 1024 * 1024
EVENT_ENUM_PATHS = ENUM_PATHS | {
    "payload.stage", "payload.level", "payload.source", "payload.tool_type",
}
EVENT_DYNAMIC_MAPS = {"payload.changes", "payload.statuses"}


def link_field(path):
    key = path.rsplit(".", 1)[-1]
    return not key.endswith("[]") and (key in {"id", "ids"} or key.endswith(("_id", "_ids")))


def kind(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, str):
        return "string"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    return "number"


def enum(value):
    if value is None:
        return "[null]"
    if not isinstance(value, str):
        return "[non_string]"
    return value if SAFE_ENUM.fullmatch(value) else "[non_enum_string]"


def new_stats():
    return {"records": 0, "source_bytes": 0, "failed_records": 0, "failures": [], "subtypes": {}}


def add_record(stats, value, size, record_type="response_item"):
    if not isinstance(value, dict) or value.get("type") != record_type:
        raise ValueError("top_level_type_mismatch")
    payload = value.get("payload")
    subtype = enum(payload.get("type")) if isinstance(payload, dict) else "[payload_" + kind(payload) + "]"
    group = stats["subtypes"].setdefault(subtype, {
        "records": 0, "source_bytes": 0, "enums": {}, "fields": {}, "features": {},
    })
    stats["records"] += 1
    stats["source_bytes"] += size
    group["records"] += 1
    group["source_bytes"] += size
    seen = set()
    features = set()
    links_seen = set()
    enum_paths = EVENT_ENUM_PATHS if record_type == "event_msg" else ENUM_PATHS
    stack = [(payload, "payload", 0)]
    while stack:
        item, path, depth = stack.pop()
        shape = path + " : " + kind(item)
        if shape not in group["fields"] and len(group["fields"]) >= MAX_FIELD_SHAPES:
            features.add("field_shape_limit_reached")
            shape = "[additional_field_shapes]"
        field = group["fields"].setdefault(shape, {"occurrences": 0, "records": 0})
        field["occurrences"] += 1
        if shape not in seen:
            field["records"] += 1
            seen.add(shape)
        if path in enum_paths:
            values = group["enums"].setdefault(path, {})
            name = enum(item)
            values[name] = values.get(name, 0) + 1
        if record_type == "event_msg" and link_field(path):
            coverage = group.setdefault("link_field_coverage", {}).setdefault(path, {
                "records_present": 0, "occurrences": 0, "records_with_null": 0,
                "records_with_nonempty_string": 0, "records_with_empty_string": 0,
                "records_with_other_values": 0,
            })
            category = ("records_with_null" if item is None else
                        "records_with_nonempty_string" if isinstance(item, str) and item else
                        "records_with_empty_string" if isinstance(item, str) else "records_with_other_values")
            coverage["occurrences"] += 1
            for metric in ("records_present", category):
                if (path, metric) not in links_seen:
                    coverage[metric] += 1
                    links_seen.add((path, metric))
        if isinstance(item, str):
            if item.startswith("data:image/") and ";base64," in item[:120]:
                features.add("embedded_base64_image")
            elif item.startswith("data:audio/") and ";base64," in item[:120]:
                features.add("embedded_base64_audio")
            if path.endswith((".image_url", ".image_url.url")):
                features.add("image_reference")
            if path.endswith(".type") and item in {"input_image", "output_image", "image"}:
                features.add("image_content_block")
            if len(item) > 65536:
                features.add("string_over_65536_characters")
        if depth >= MAX_DEPTH:
            if isinstance(item, (list, dict)) and item:
                features.add("depth_limit_reached")
            continue
        if isinstance(item, dict):
            for key, child in item.items():
                dynamic = record_type == "event_msg" and path in EVENT_DYNAMIC_MAPS
                safe = key if not dynamic and SAFE_KEY.fullmatch(key) else "{dynamic_key}"
                stack.append((child, path + "." + safe, depth + 1))
        elif isinstance(item, list):
            stack.extend((child, path + "[]", depth + 1) for child in item)
    for feature in features:
        group["features"][feature] = group["features"].get(feature, 0) + 1


def merge(target, source):
    for key in ("records", "source_bytes", "failed_records"):
        target[key] += source[key]
    target["failures"].extend(source["failures"])
    for subtype, value in source["subtypes"].items():
        group = target["subtypes"].setdefault(subtype, {
            "records": 0, "source_bytes": 0, "enums": {}, "fields": {}, "features": {},
        })
        group["records"] += value["records"]
        group["source_bytes"] += value["source_bytes"]
        for feature, count in value["features"].items():
            group["features"][feature] = group["features"].get(feature, 0) + count
        for category in ("enums", "fields", "link_field_coverage"):
            for path, counts in value.get(category, {}).items():
                dest = group.setdefault(category, {}).setdefault(path, {})
                for name, count in counts.items():
                    dest[name] = dest.get(name, 0) + count


def export_locators(path, record_type="response_item"):
    if record_type not in {"response_item", "event_msg", "compacted"}:
        raise ValueError("Unsupported record type")
    config = connection_settings()
    if config["DB_DATABASE"] != "codex_session_research":
        raise RuntimeError("Unexpected database")
    environment = os.environ.copy()
    environment.update(PGPASSWORD=config["DB_PASSWORD"], PGCONNECT_TIMEOUT="5", PGCLIENTENCODING="UTF8")
    command = [find_psql(), "-X", "-w", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1",
               "-h", config["DB_HOST"], "-p", config["DB_PORT"], "-U", config["DB_USERNAME"],
               "-d", config["DB_DATABASE"]]
    sql = f"""BEGIN READ ONLY;
SET LOCAL max_parallel_workers_per_gather=0;
SET LOCAL work_mem='16MB';
COPY (SELECT research_session_id,id,source_byte_offset,source_record_bytes
 FROM public.research_session_records WHERE record_type='{record_type}'
 ORDER BY research_session_id,source_line_number) TO STDOUT WITH (FORMAT csv);
COMMIT;
"""
    temporary = path.with_suffix(".tmp")
    with temporary.open("wb") as destination:
        result = subprocess.run(command, input=sql.encode(), stdout=destination,
                                stderr=subprocess.PIPE, env=environment, timeout=180)
    if result.returncode:
        raise RuntimeError("Locator export failed; no source files were read")
    temporary.replace(path)
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def peak_memory():
    if os.name != "nt":
        return None
    class Counters(ctypes.Structure):
        _fields_ = [("cb", ctypes.c_ulong), ("faults", ctypes.c_ulong)] + [
            (name, ctypes.c_size_t) for name in ("peak", "working", "pool_peak", "pool",
            "nonpaged_peak", "nonpaged", "pagefile", "pagefile_peak")]
    counters = Counters()
    counters.cb = ctypes.sizeof(counters)
    kernel = ctypes.WinDLL("kernel32")
    kernel.GetCurrentProcess.restype = ctypes.c_void_p
    get_info = ctypes.WinDLL("psapi").GetProcessMemoryInfo
    get_info.argtypes = (ctypes.c_void_p, ctypes.POINTER(Counters), ctypes.c_ulong)
    return counters.peak if get_info(kernel.GetCurrentProcess(), ctypes.byref(counters), counters.cb) else None


def scan_session(source, rows, stats, record_type="response_item"):
    previous_end = -1
    for session_id, record_id, offset, size in rows:
        if offset < 0 or offset < previous_end or size <= 0 or size > MAX_RECORD_BYTES:
            raise RuntimeError("Invalid, overlapping, or oversized locator; nothing skipped silently")
        source.seek(offset)
        raw = source.read(size)
        if len(raw) != size:
            raise RuntimeError("Incomplete source byte range")
        try:
            value = json.loads(raw, object_pairs_hook=unique_object, parse_constant=reject_constant)
            add_record(stats, value, size, record_type)
            del value
        except (ValueError, UnicodeError, RecursionError) as error:
            stats["failed_records"] += 1
            stats["failures"].append({"record_id": record_id, "error_class": type(error).__name__})
        del raw
        previous_end = offset + size


def markdown(report):
    record_type = report.get("record_type", "response_item")
    lines = [f"# {record_type} structural census", "", "Generated (UTC): " + report["completed_at"], "",
             f"Successfully profiled {report['records']:,} physical records; failures: {report['failed_records']}.",
             f"Source bytes: {report['source_bytes']:,}; files: {report['files_profiled']:,}; status: {report['status']}.",
             f"Payload types: {len(report['subtypes'])}; normalized field/type shapes: {sum(len(v['fields']) for v in report['subtypes'].values())}.",
             f"Elapsed: {report['elapsed_seconds']:.3f} seconds; peak working set: {report['peak_working_set_bytes'] or 0:,} bytes.",
             f"Traversal-limit hits: {report.get('traversal_limit_hits', 0)}; database written: {report['database_written']}.",
             "Counts include repeated source histories and are not deduplicated human conversations.", "",
             "Only key paths, JSON types, allowlisted short enums and counts are exported.",
             "Source bytes include each full physical record and its line ending, not database storage size.",
             "Field and link coverage counts records; enum counts measure occurrences. A record may contain multiple array items.",
             "Link fields are candidates identified by field name; their values and cross-record relationships are not examined.", "",
             "| Payload type | Records | Source bytes |", "|---|---:|---:|"]
    for name, row in sorted(report["subtypes"].items(), key=lambda pair: -pair[1]["records"]):
        lines.append(f"| `{name}` | {row['records']:,} | {row['source_bytes']:,} |")
    if record_type == "event_msg":
        item_types = report["subtypes"].get("item_completed", {}).get("enums", {}).get("payload.item.type", {})
        lines.extend(["", "## Interpretation and nested item classification", "",
                      "All counts above are physical event records, including token_count, not token quantities.",
                      "The event subtype error is distinct from a failure of this profiling script.",
                      "Link-field categories can overlap when a field path repeats in an array. Candidate identifiers may belong to tools or external objects, not just turns or threads.",
                      "item_completed contains its own item types. Do not add its UserMessage/AgentMessage counts to standalone message events or response_item messages without matching and deduplication.", "",
                      "| item_completed payload.item.type | Occurrences |", "|---|---:|"])
        for name, count in sorted(item_types.items(), key=lambda pair: -pair[1]):
            lines.append(f"| `{name}` | {count:,} |")
    for name, row in sorted(report["subtypes"].items()):
        lines.extend(["", "## " + name, "", "| Enum path | Value | Occurrences |", "|---|---|---:|"])
        for path, counts in sorted(row["enums"].items()):
            for value, count in sorted(counts.items(), key=lambda pair: -pair[1]):
                lines.append(f"| `{path}` | `{value}` | {count:,} |")
        lines.extend(["", "| Field path and JSON type | Records containing field | Occurrences |",
                      "|---|---:|---:|"])
        for shape, counts in sorted(row["fields"].items()):
            lines.append(f"| `{shape}` | {counts['records']:,} | {counts['occurrences']:,} |")
        if row.get("link_field_coverage"):
            lines.extend(["", "| Candidate link field | Records present | Nonempty string | Null | Empty string | Other value |",
                          "|---|---:|---:|---:|---:|---:|"])
            for path, counts in sorted(row["link_field_coverage"].items()):
                lines.append(f"| `{path}` | {counts['records_present']:,} | {counts['records_with_nonempty_string']:,} | "
                             f"{counts['records_with_null']:,} | {counts['records_with_empty_string']:,} | {counts['records_with_other_values']:,} |")
        if row["features"]:
            lines.extend(["", "Structural features (record counts):", ""])
            lines.extend(f"- `{feature}`: {count:,}" for feature,count in sorted(row["features"].items()))
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--record-type", choices=("response_item", "event_msg"), default="response_item")
    parser.add_argument("--run", action="store_true", help="Run exact-range source profiling; default is DB plan only")
    args = parser.parse_args()
    record_type = args.record_type
    output_dir = ROOT / f"data/reports/{record_type}_profile_v1"
    priority = lower_priority()
    plan = json_rows(f"""SELECT json_build_object('records',count(*),
 'source_bytes',sum(source_record_bytes),'largest_record_bytes',max(source_record_bytes),
 'files',count(DISTINCT research_session_id))
 FROM public.research_session_records WHERE record_type='{record_type}';""")[0]
    print(json.dumps({"plan": plan, "priority": priority, "database_written": False}), flush=True)
    if not args.run:
        return 0
    if (plan["largest_record_bytes"] or 0) > MAX_RECORD_BYTES:
        raise RuntimeError("Largest record exceeds the memory guard; review before proceeding")
    output_dir.mkdir(parents=True, exist_ok=True)
    cache = output_dir / "checkpoints"
    cache.mkdir(exist_ok=True)
    started_at, started_clock = utc_now(), time.monotonic()
    paths = {r["id"]: r for r in json_rows("""SELECT json_build_object('id',id,
 'path',source_relative_path,'size',file_size_bytes) FROM public.research_sessions;""")}
    csv_path = output_dir / "locators.csv"
    locator_hash = export_locators(csv_path, record_type)
    total = new_stats()
    file_count = cache_hits = read_bytes = 0
    last_progress = time.monotonic()
    with csv_path.open(newline="", encoding="utf-8") as locators:
        numbers = (tuple(map(int,row)) for row in csv.reader(locators))
        for session_id, group in itertools.groupby(numbers, key=lambda row: row[0]):
            entry = paths[session_id]
            path = safe_source_path(entry["path"])
            before = path.stat()
            if before.st_size != entry["size"]:
                raise RuntimeError("Source file size changed")
            signature = {"version":VERSION, "locator_sha256":locator_hash, "path":entry["path"],
                         "size":before.st_size, "mtime_ns":before.st_mtime_ns}
            if record_type == "event_msg":
                signature["profile"] = "event_msg_v1"
            checkpoint = cache / f"{session_id}.json"
            cached = json.loads(checkpoint.read_text(encoding="utf-8")) if checkpoint.exists() else None
            # Completed shallow traversals remain exact when the depth ceiling is
            # raised. Re-read only files whose earlier traversal hit a ceiling.
            cached_limited = cached and any(
                subtype["features"].get("depth_limit_reached", 0)
                or subtype["features"].get("field_shape_limit_reached", 0)
                for subtype in cached["stats"]["subtypes"].values()
            )
            if cached and cached["signature"] == signature and not cached["stats"]["failed_records"] and not cached_limited:
                stats = cached["stats"]
                cache_hits += 1
                for unused in group:
                    pass
            else:
                stats = new_stats()
                with path.open("rb") as source:
                    scan_session(source, group, stats, record_type)
                after = path.stat()
                if (before.st_size,before.st_mtime_ns) != (after.st_size,after.st_mtime_ns):
                    raise RuntimeError("Source changed during structural scan")
                read_bytes += stats["source_bytes"]
                save(checkpoint, {"signature":signature, "stats":stats})
            merge(total, stats)
            file_count += 1
            if time.monotonic()-last_progress >= 15 or file_count == plan["files"]:
                print(json.dumps({"files_done":file_count,"files_total":plan["files"],
                                  "records_done":total["records"],"failures":total["failed_records"]}), flush=True)
                last_progress = time.monotonic()
    limited = sum(group["features"].get("depth_limit_reached", 0)
                  + group["features"].get("field_shape_limit_reached", 0)
                  for group in total["subtypes"].values())
    complete = (total["records"] == plan["records"] and total["source_bytes"] == (plan["source_bytes"] or 0)
                and not total["failed_records"] and file_count == plan["files"] and not limited)
    report = {"version":VERSION,"started_at":started_at,"completed_at":utc_now(),
              "status":"completed" if complete else "needs_review", "database_written":False,
              "record_type":record_type, "population":f"physical {record_type} records; no semantic deduplication",
              "traversal_limit_hits":limited,
              "plan":plan,"files_profiled":file_count,"cache_hits":cache_hits,"bytes_read_this_run":read_bytes,
              "locator_sha256":locator_hash,"peak_working_set_bytes":peak_memory(),
              "elapsed_seconds":round(time.monotonic()-started_clock,3),
              "limits":{"shape_depth":MAX_DEPTH,"field_shapes_per_subtype_per_file":MAX_FIELD_SHAPES},
              **total}
    save(output_dir / "summary.json", report)
    (output_dir / "report.md").write_text(markdown(report), encoding="utf-8")
    print(json.dumps({key:value for key,value in report.items() if key not in {"subtypes","failures"}}, indent=2))
    return 0 if complete else 1


if __name__ == "__main__":
    raise SystemExit(main())
