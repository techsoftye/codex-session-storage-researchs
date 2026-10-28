"""Sample one small copied rollout per month and export key/type shapes only.

No JSON value, message text, tool argument, or tool result is written to output.
The default sample has at most ten files, each smaller than 4 MiB.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
import os
from pathlib import Path
import random
import re
import sys

import fast_session_index as fast
import import_session_survey as common

ROOT = Path(__file__).resolve().parents[1]
MAX_EXPORT_BYTES = 32 * 1024 * 1024
MAX_KEY_PATHS = 10000
MAX_NODES_PER_RECORD = 25000
MAX_DEPTH = 32
DYNAMIC_MAP_PATHS = {"payload.changes", "payload.statuses",
                     "payload.dynamic_tools[].inputSchema.properties"}
SAFE_KEY = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,63}\Z")
SAFE_ENUM = re.compile(r"[A-Za-z0-9_.:-]{1,80}\Z")
ENUM_FIELDS = ("type", "payload.type", "payload.role", "payload.phase", "payload.name",
               "payload.status", "payload.source", "payload.thread_source",
               "payload.history_mode", "payload.sandbox_policy.type",
               "payload.content[].type", "payload.item.type", "payload.item.role",
               "payload.item.phase", "payload.item.content[].type",
               "payload.thread_settings.approval_policy")


def safe_key(prefix, key):
    if not isinstance(key, str) or prefix in DYNAMIC_MAP_PATHS or not SAFE_KEY.fullmatch(key):
        return "{dynamic_key}"
    return key


def value_type(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, str):
        return "string"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    return "other"


def key_shape(record):
    found = defaultdict(set)
    visited = [0]
    truncated = [False]

    def visit(value, prefix, depth):
        visited[0] += 1
        if visited[0] > MAX_NODES_PER_RECORD:
            truncated[0] = True
            return
        if depth >= MAX_DEPTH:
            if isinstance(value, (dict, list)):
                truncated[0] = True
            return
        if isinstance(value, dict):
            for key, child in value.items():
                if not isinstance(key, str):
                    truncated[0] = True
                    continue
                normalized = safe_key(prefix, key)
                path = f"{prefix}.{normalized}" if prefix else normalized
                found[path].add(value_type(child))
                visit(child, path, depth + 1)
                if visited[0] > MAX_NODES_PER_RECORD:
                    break
        elif isinstance(value, list):
            path = prefix + "[]"
            for child in value:
                found[path].add(value_type(child))
                visit(child, path, depth + 1)
                if visited[0] > MAX_NODES_PER_RECORD:
                    break

    visit(record, "", 0)
    return found, truncated[0]


def safe_source(relative):
    if not isinstance(relative, str) or "\\" in relative or "\x00" in relative:
        raise ValueError("Invalid indexed source path")
    parts = relative.split("/")
    if parts[0] != "2026" or any(not p or p in {".", ".."} or ":" in p for p in parts):
        raise ValueError("Invalid indexed source path")
    path = ROOT.joinpath(*parts)
    for component in (path, *path.parents):
        if component == ROOT:
            break
        if component.exists() and fast.reparse_point(component):
            raise ValueError("Source path contains a link or reparse point")
    return path


def select_files(index, seed, max_bytes):
    grouped = defaultdict(list)
    for item in index["items"]:
        file = item["file"]
        size = file.get("file_size_bytes")
        relative = file.get("source_relative_path")
        if type(size) is int and 0 < size < max_bytes and isinstance(relative, str):
            grouped[relative.split("/")[1]].append(item)
    rng = random.Random(seed)
    return [rng.choice(sorted(grouped[month], key=lambda x: x["file"]["source_relative_path"]))
            for month in sorted(grouped)]


def scan_file(item, max_bytes, key_paths, top_shapes, payload_shapes):
    file = item["file"]
    path = safe_source(file["source_relative_path"])
    before = path.stat()
    if before.st_size != file["file_size_bytes"] or not 0 < before.st_size < max_bytes:
        raise ValueError("Selected file size changed or exceeds sample limit")
    result = {"source_relative_path": file["source_relative_path"],
              "file_size_bytes": before.st_size, "bytes_read": 0, "record_count": 0,
              "valid_record_count": 0, "invalid_record_count": 0,
              "largest_record_bytes": 0, "truncated_shape_records": 0}
    with path.open("rb") as handle:
        while True:
            raw = handle.readline(max_bytes + 1)
            if not raw:
                break
            if len(raw) > max_bytes:
                raise ValueError("A record exceeds the bounded sample file size")
            result["bytes_read"] += len(raw)
            result["record_count"] += 1
            result["largest_record_bytes"] = max(result["largest_record_bytes"], len(raw))
            try:
                record = fast.parse_object(raw)
            except (UnicodeError, ValueError, RecursionError):
                result["invalid_record_count"] += 1
                continue
            result["valid_record_count"] += 1
            top_shapes[tuple(sorted(safe_key("", key) for key in record))] += 1
            payload = record.get("payload")
            if isinstance(payload, dict):
                payload_shapes[tuple(sorted(safe_key("payload", key) for key in payload))] += 1
            found, truncated = key_shape(record)
            result["truncated_shape_records"] += truncated
            for key, types in found.items():
                if key not in key_paths and len(key_paths) >= MAX_KEY_PATHS:
                    result["truncated_shape_records"] += 1
                    continue
                entry = key_paths[key]
                entry["records"] += 1
                entry["types"].update(types)
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ValueError("Selected source changed during shape scan")
    if result["bytes_read"] != before.st_size:
        raise ValueError("Selected file was not fully read")
    return result


def run(index_path, output_path, seed, max_mib):
    with index_path.open("rb") as handle:
        raw = handle.read(MAX_EXPORT_BYTES + 1)
    if len(raw) > MAX_EXPORT_BYTES:
        raise ValueError("Derived index exceeds 32 MiB")
    index = json.loads(raw.decode("utf-8-sig"), object_pairs_hook=common.unique_object,
                       parse_constant=common.reject_constant)
    if (index.get("index_version") != fast.VERSION or index.get("export_status") != "completed" or
            Path(index.get("research_root", "")).resolve() != ROOT or index.get("source") != "2026" or
            index.get("files_indexed") != len(index.get("items", []))):
        raise ValueError("Index is incomplete or belongs to another project")
    max_bytes = max_mib * 1024 * 1024
    selected = select_files(index, seed, max_bytes)
    if not 5 <= len(selected) <= 10:
        raise ValueError("Sample must contain 5 to 10 distinct months")
    priority = fast.lower_priority()
    key_paths = defaultdict(lambda: {"records": 0, "types": set()})
    top_shapes, payload_shapes = Counter(), Counter()
    files = []
    for item in selected:
        result = scan_file(item, max_bytes, key_paths, top_shapes, payload_shapes)
        files.append(result)
        print(f"[{len(files)}] {result['source_relative_path']} | {result['record_count']} records | "
              f"{result['bytes_read']} bytes read", flush=True)
    output = {"report_version": "1.0.5", "phase": "keys_only", "values_exported": False,
              "source_index": index_path.relative_to(ROOT).as_posix(), "random_seed": seed,
              "selection_method": "one_seeded_random_file_per_month_under_size_limit",
              "max_file_bytes_exclusive": max_bytes, "process_priority": priority,
              "sampled_files": len(files), "total_bytes_read": sum(x["bytes_read"] for x in files),
              "total_records": sum(x["record_count"] for x in files),
              "valid_records": sum(x["valid_record_count"] for x in files),
              "invalid_records": sum(x["invalid_record_count"] for x in files),
              "files": files,
              "top_level_key_sets": [{"keys": list(keys), "record_count": count}
                                     for keys, count in top_shapes.most_common()],
              "payload_key_sets": [{"keys": list(keys), "record_count": count}
                                   for keys, count in payload_shapes.most_common()],
              "key_paths": [{"path": path, "record_count": data["records"],
                             "value_types": sorted(data["types"])}
                            for path, data in sorted(key_paths.items())]}
    with output_path.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(output, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return output


def enum_value(value):
    return value if isinstance(value, str) and SAFE_ENUM.fullmatch(value) else None


def selected_values(record):
    """Return only explicit short categorical fields; never free-text fields."""
    found = []

    def add(path, value):
        if value is not None:
            found.append((path, enum_value(value)))

    add("type", record.get("type"))
    payload = record.get("payload")
    if not isinstance(payload, dict):
        return found
    for key in ("type", "role", "phase", "name", "status", "source", "thread_source", "history_mode"):
        if key in payload and not isinstance(payload[key], (dict, list)):
            add("payload." + key, payload[key])
    sandbox = payload.get("sandbox_policy")
    if isinstance(sandbox, dict):
        add("payload.sandbox_policy.type", sandbox.get("type"))
    settings = payload.get("thread_settings")
    if isinstance(settings, dict):
        add("payload.thread_settings.approval_policy", settings.get("approval_policy"))
    for part in payload.get("content", []) if isinstance(payload.get("content"), list) else []:
        if isinstance(part, dict):
            add("payload.content[].type", part.get("type"))
    item = payload.get("item")
    if isinstance(item, dict):
        for key in ("type", "role", "phase"):
            add("payload.item." + key, item.get(key))
        for part in item.get("content", []) if isinstance(item.get("content"), list) else []:
            if isinstance(part, dict):
                add("payload.item.content[].type", part.get("type"))
    return found


def run_selected_values(index_path, keys_path, output_path, seed, max_mib):
    with keys_path.open("rb") as handle:
        raw_keys = handle.read(MAX_EXPORT_BYTES + 1)
    if len(raw_keys) > MAX_EXPORT_BYTES:
        raise ValueError("Key-only report exceeds 32 MiB")
    key_report = json.loads(raw_keys.decode("utf-8-sig"), object_pairs_hook=common.unique_object,
                            parse_constant=common.reject_constant)
    if (key_report.get("phase") != "keys_only" or key_report.get("values_exported") is not False or
            key_report.get("random_seed") != seed or
            key_report.get("max_file_bytes_exclusive") != max_mib * 1024 * 1024 or
            key_report.get("source_index") != index_path.relative_to(ROOT).as_posix()):
        raise ValueError("Key-only report does not match the requested sample")
    with index_path.open("rb") as handle:
        raw_index = handle.read(MAX_EXPORT_BYTES + 1)
    if len(raw_index) > MAX_EXPORT_BYTES:
        raise ValueError("Derived index exceeds 32 MiB")
    index = json.loads(raw_index.decode("utf-8-sig"), object_pairs_hook=common.unique_object,
                       parse_constant=common.reject_constant)
    if (index.get("index_version") != fast.VERSION or index.get("export_status") != "completed" or
            Path(index.get("research_root", "")).resolve() != ROOT or index.get("source") != "2026" or
            index.get("files_indexed") != len(index.get("items", []))):
        raise ValueError("Source index is incomplete or belongs to another project")
    selected = select_files(index, seed, max_mib * 1024 * 1024)
    expected = [(file["source_relative_path"], file["file_size_bytes"]) for file in key_report["files"]]
    actual = [(item["file"]["source_relative_path"], item["file"]["file_size_bytes"])
              for item in selected]
    if len(actual) != 10 or actual != expected:
        raise ValueError("Selected files differ from the key-only sample")
    priority = fast.lower_priority()
    counts = defaultdict(Counter)
    suppressed = Counter()
    combinations = defaultdict(lambda: [0, 0])
    files = []
    for item in selected:
        file = item["file"]
        path = safe_source(file["source_relative_path"])
        before = path.stat()
        if before.st_size != file["file_size_bytes"] or not 0 < before.st_size < max_mib * 1024 * 1024:
            raise ValueError("Selected file changed or exceeds size limit")
        records = invalid = bytes_read = 0
        with path.open("rb") as handle:
            while True:
                raw = handle.readline(max_mib * 1024 * 1024 + 1)
                if not raw:
                    break
                if len(raw) > max_mib * 1024 * 1024:
                    raise ValueError("Record exceeds bounded sample size")
                bytes_read += len(raw)
                records += 1
                try:
                    record = fast.parse_object(raw)
                except (UnicodeError, ValueError, RecursionError):
                    invalid += 1
                    continue
                extracted = selected_values(record)
                for field, value in extracted:
                    if field not in ENUM_FIELDS:
                        raise ValueError("Unapproved categorical field")
                    if value is None:
                        suppressed[field] += 1
                    else:
                        counts[field][value] += 1
                scalars = dict(extracted)
                signature = tuple(scalars.get(field) or "{missing_or_suppressed}" for field in
                                  ("type", "payload.type", "payload.role", "payload.phase"))
                combinations[signature][0] += 1
                combinations[signature][1] += len(raw)
        after = path.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns) or bytes_read != before.st_size:
            raise ValueError("Selected file changed or was not fully read")
        files.append({"source_relative_path": file["source_relative_path"], "bytes_read": bytes_read,
                      "record_count": records, "invalid_record_count": invalid})
        print(f"[{len(files)}] {file['source_relative_path']} | {records} records | {bytes_read} bytes read",
              flush=True)
    if any(files[i]["record_count"] != expected_file["record_count"] or
           files[i]["invalid_record_count"] != expected_file["invalid_record_count"]
           for i, expected_file in enumerate(key_report["files"])):
        raise ValueError("Second pass record totals differ from the key-only pass")
    output = {"report_version": "1.0.0", "phase": "selected_values",
              "source_keys_report": keys_path.relative_to(ROOT).as_posix(),
              "source_index": index_path.relative_to(ROOT).as_posix(),
              "random_seed": seed, "max_file_bytes_exclusive": max_mib * 1024 * 1024,
              "process_priority": priority, "sampled_files": len(files),
              "total_records": sum(x["record_count"] for x in files),
              "total_bytes_read": sum(x["bytes_read"] for x in files),
              "invalid_records": sum(x["invalid_record_count"] for x in files), "files": files,
              "allowlisted_fields": list(ENUM_FIELDS),
              "categorical_values": {field: [{"value": value, "occurrences": count}
                                            for value, count in values.most_common()]
                                     for field, values in sorted(counts.items())},
              "suppressed_non_enum_or_long_values": dict(sorted(suppressed.items())),
              "record_kind_combinations": [
                  {"top_level_type": key[0], "payload_type": key[1], "role": key[2],
                   "phase": key[3], "record_count": totals[0], "raw_record_bytes": totals[1]}
                  for key, totals in sorted(combinations.items(), key=lambda x: -x[1][1])],
              "free_text_fields_exported": False}
    with output_path.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(output, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return output


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index", default="data/session_index_2026_v102.json")
    parser.add_argument("--output", default="data/record_shapes_sample_2026.json")
    parser.add_argument("--seed", type=int, default=20261005)
    parser.add_argument("--max-file-mib", type=int, default=4)
    parser.add_argument("--phase", choices=("keys", "selected-values"), default="keys")
    parser.add_argument("--keys-report", default="data/record_shapes_sample_2026_v105.json")
    args = parser.parse_args(argv)
    try:
        if not 1 <= args.max_file_mib <= 4:
            raise ValueError("The sample file limit must be between 1 and 4 MiB")
        index_path = common.project_file(args.index)
        output_path = Path(os.path.abspath(ROOT / args.output))
        if not output_path.is_relative_to(ROOT) or output_path.is_relative_to(ROOT / "2026"):
            raise ValueError("Output must remain in the project outside 2026")
        if output_path.exists():
            raise ValueError("Output already exists; choose a new name")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if args.phase == "keys":
            result = run(index_path, output_path, args.seed, args.max_file_mib)
        else:
            result = run_selected_values(index_path, common.project_file(args.keys_report),
                                         output_path, args.seed, args.max_file_mib)
        print(json.dumps({key: result[key] for key in ("sampled_files", "total_records",
                                                         "total_bytes_read", "invalid_records")},
                         ensure_ascii=False))
        return 0
    except (OSError, ValueError, TypeError, RecursionError) as error:
        print(f"Shape sample stopped: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
