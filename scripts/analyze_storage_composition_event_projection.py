"""Project event_msg subtype and byte lengths from indexed ranges only.

The scanner never materializes complete JSON values. It skips unrelated JSON
values as byte spans and decodes only structural keys and the two requested
type labels. Outputs contain aggregate counts/bytes and numeric file IDs only.
"""
from __future__ import annotations

import csv
from datetime import datetime, timezone
import hashlib
from itertools import chain
import json
import os
from pathlib import Path
import re
import statistics
import time

from index_session_records import ROOT, lower_priority, safe_source_path
from profile_response_items import peak_memory

OUT = ROOT / "data/reports/storage_composition_v1"
PROFILE = ROOT / "data/reports/event_msg_profile_v1"
PROFILE_SUMMARY = PROFILE / "summary.json"
LOCATORS = PROFILE / "locators.csv"
CHECKPOINTS = PROFILE / "checkpoints"
REPORT = OUT / "report.md"
SUMMARY = OUT / "summary.json"
FILES_CSV = OUT / "files.csv"
RESPONSE_BREAKDOWN = OUT / "response_item_type_role_phase.csv"
EVENT_TYPE_BY_FILE = OUT / "event_msg_type_by_file.csv"
EVENT_STAGE = OUT / ".event_msg_projection_stage.json"

JSON_SPECIALS = re.compile(rb'["{}\[\]]')
SAFE_LABEL = re.compile(r"[A-Za-z0-9_.:-]{1,80}\Z")
IMAGE_OR_TEXT_DUPLICATES = {"UserMessage", "AgentMessage"}
RESPONSE_PROCESSING_TYPES = {
    "reasoning", "custom_tool_call", "custom_tool_call_output", "function_call",
    "function_call_output", "web_search_call", "tool_search_call",
    "tool_search_output", "image_generation_call",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def skip_ws(raw: bytes, pos: int) -> int:
    while pos < len(raw) and raw[pos] in b" \t\r\n":
        pos += 1
    return pos


def string_end(raw: bytes, pos: int) -> int:
    if pos >= len(raw) or raw[pos] != 34:
        raise ValueError("Expected a JSON string")
    quote = raw.find(b'"', pos + 1)
    while quote >= 0:
        backslash = quote - 1
        while backslash > pos and raw[backslash] == 92:
            backslash -= 1
        if (quote - backslash - 1) % 2 == 0:
            return quote + 1
        quote = raw.find(b'"', quote + 1)
    raise ValueError("Unterminated JSON string")


def decode_small_string(raw: bytes, start: int, end: int) -> str:
    value = json.loads(raw[start:end].decode("utf-8"))
    if not isinstance(value, str):
        raise ValueError("Expected a JSON string value")
    return value


def skip_container(raw: bytes, pos: int) -> int:
    opener = raw[pos]
    if opener not in (123, 91):
        raise ValueError("Expected a JSON object or array")
    stack = [125 if opener == 123 else 93]
    pos += 1
    while stack:
        match = JSON_SPECIALS.search(raw, pos)
        if match is None:
            raise ValueError("Unterminated JSON container")
        token = raw[match.start()]
        if token == 34:
            pos = string_end(raw, match.start())
            continue
        if token == 123:
            stack.append(125)
        elif token == 91:
            stack.append(93)
        elif token in (125, 93):
            if not stack or stack[-1] != token:
                raise ValueError("Mismatched JSON delimiter")
            stack.pop()
        pos = match.end()
    return pos


def skip_value(raw: bytes, pos: int) -> int:
    pos = skip_ws(raw, pos)
    if pos >= len(raw):
        raise ValueError("Missing JSON value")
    if raw[pos] == 34:
        return string_end(raw, pos)
    if raw[pos] in (123, 91):
        return skip_container(raw, pos)
    ends = [raw.find(ch, pos) for ch in (b",", b"}", b"]", b" ", b"\t", b"\r", b"\n")]
    ends = [end for end in ends if end >= 0]
    if not ends:
        return len(raw)
    if min(ends) == pos:
        raise ValueError("Empty JSON scalar")
    return min(ends)


def scalar_string(raw: bytes, pos: int) -> tuple[str | None, int]:
    pos = skip_ws(raw, pos)
    if pos < len(raw) and raw[pos] == 34:
        end = string_end(raw, pos)
        return decode_small_string(raw, pos, end), end
    return None, skip_value(raw, pos)


def parse_target_object(raw: bytes, pos: int, targets: set[str]) -> tuple[dict[str, str | None], int]:
    pos = skip_ws(raw, pos)
    if pos >= len(raw) or raw[pos] != 123:
        return {}, skip_value(raw, pos)
    result: dict[str, str | None] = {}
    seen: set[str] = set()
    pos += 1
    while True:
        pos = skip_ws(raw, pos)
        if pos >= len(raw):
            raise ValueError("Unterminated JSON object")
        if raw[pos] == 125:
            return result, pos + 1
        key_start = pos
        key_end = string_end(raw, pos)
        key = decode_small_string(raw, key_start, key_end)
        if key in targets and key in seen:
            raise ValueError("Duplicate target key in JSON object")
        if key in targets:
            seen.add(key)
        pos = skip_ws(raw, key_end)
        if pos >= len(raw) or raw[pos] != 58:
            raise ValueError("Missing JSON object colon")
        pos = skip_ws(raw, pos + 1)
        if key in targets:
            if key == "payload":
                nested, pos = parse_payload(raw, pos)
                result.update(nested)
            elif key == "item":
                nested, pos = parse_item(raw, pos)
                result.update(nested)
            else:
                value, pos = scalar_string(raw, pos)
                result[key] = value
        else:
            pos = skip_value(raw, pos)
        pos = skip_ws(raw, pos)
        if pos < len(raw) and raw[pos] == 44:
            pos += 1
            continue
        if pos < len(raw) and raw[pos] == 125:
            return result, pos + 1
        raise ValueError("Expected comma or object close")


def parse_payload(raw: bytes, pos: int) -> tuple[dict[str, str | None], int]:
    values, end = parse_target_object(raw, pos, {"type", "item"})
    return {"payload_type": values.get("type"), "item_type": values.get("item_type")}, end


def parse_item(raw: bytes, pos: int) -> tuple[dict[str, str | None], int]:
    values, end = parse_target_object(raw, pos, {"type"})
    return {"item_type": values.get("type")}, end


def extract_types(raw: bytes) -> tuple[str, str, str]:
    values, end = parse_target_object(raw, 0, {"type", "payload"})
    if skip_ws(raw, end) != len(raw):
        raise ValueError("Unexpected data after JSON record")
    root_type = values.get("type")
    payload_type = values.get("payload_type")
    item_type = values.get("item_type")
    if root_type != "event_msg":
        raise ValueError("Indexed event_msg range has another root type")
    if not isinstance(payload_type, str) or not SAFE_LABEL.fullmatch(payload_type):
        payload_type = "[missing_or_other]"
    if not isinstance(item_type, str) or not SAFE_LABEL.fullmatch(item_type):
        item_type = "[missing_or_other]"
    return payload_type, item_type, root_type


def csv_sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def normalize_source_path(value: str) -> Path:
    if not isinstance(value, str):
        raise ValueError("Checkpoint path is missing")
    normalized = value.replace("\\", "/")
    return safe_source_path(normalized)


def percentile90(values: list[float]) -> float:
    ordered = sorted(values)
    pos = (len(ordered) - 1) * 0.9
    low = int(pos)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (pos - low)


def distribution(values: list[float]) -> dict:
    return {
        "n": len(values),
        "mean_unweighted": statistics.fmean(values),
        "median": statistics.median(values),
        "p90_linear_interpolated": percentile90(values),
        "max": max(values),
    }


def write_json(path: Path, value: dict) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2, allow_nan=False)
        handle.write("\n")


def main() -> None:
    started = time.perf_counter()
    started_at = utc_now()
    priority = lower_priority()
    summary = json.loads(SUMMARY.read_text(encoding="utf-8"))
    profile = json.loads(PROFILE_SUMMARY.read_text(encoding="utf-8"))
    if profile.get("status") != "completed" or profile.get("record_type") != "event_msg":
        raise RuntimeError("Existing event_msg locator profile is incomplete")
    if csv_sha256(LOCATORS) != profile.get("locator_sha256"):
        raise RuntimeError("event_msg locator file differs from its completed profile")

    checkpoint_map = {}
    checkpoint_records = checkpoint_bytes = 0
    for checkpoint_path in CHECKPOINTS.glob("*.json"):
        file_id = int(checkpoint_path.stem)
        checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        stats = checkpoint["stats"]
        checkpoint_map[file_id] = {
            "path": normalize_source_path(checkpoint["signature"]["path"]),
            "size": int(checkpoint["signature"]["size"]),
            "mtime_ns": int(checkpoint["signature"]["mtime_ns"]),
            "records": int(stats["records"]),
            "bytes": int(stats["source_bytes"]),
            "subtypes": stats["subtypes"],
        }
        checkpoint_records += int(stats["records"])
        checkpoint_bytes += int(stats["source_bytes"])
    if (len(checkpoint_map) != profile["files_profiled"] or
            checkpoint_records != profile["records"] or checkpoint_bytes != profile["source_bytes"]):
        raise RuntimeError("Existing per-file event_msg checkpoints do not reconcile")

    global_types: dict[str, dict[str, int]] = {}
    global_items: dict[str, dict[str, int]] = {}
    file_types: dict[int, dict[str, dict[str, int]]] = {}
    file_items: dict[int, dict[str, dict[str, int]]] = {}
    file_totals: dict[int, dict[str, int]] = {}
    records_read = bytes_read = 0
    active_id = None
    active_path = None
    active_handle = None
    before = None
    prior_end = -1

    with LOCATORS.open("r", encoding="ascii", newline="") as locator_file:
        first_line = locator_file.readline()
        if first_line.strip() == "research_session_id,id,source_byte_offset,source_record_bytes":
            locator_lines = locator_file
        else:
            locator_lines = chain((first_line,), locator_file)
        for line in locator_lines:
            line = line.rstrip("\r\n")
            first = line.find(",")
            second = line.find(",", first + 1)
            third = line.find(",", second + 1)
            if first < 1 or second < 0 or third < 0 or line.find(",", third + 1) >= 0:
                raise RuntimeError("Malformed event_msg locator row")
            file_id = int(line[:first])
            # The locator's record-id field is deliberately discarded here.
            offset = int(line[second + 1:third])
            size = int(line[third + 1:])
            if file_id not in checkpoint_map or size <= 0:
                raise RuntimeError("Locator references an unknown file or empty range")
            if active_id != file_id:
                if active_handle is not None:
                    active_handle.close()
                    current = active_path.stat()
                    if (current.st_size, current.st_mtime_ns) != (before.st_size, before.st_mtime_ns):
                        raise RuntimeError("Source file changed during the bounded scan")
                checkpoint = checkpoint_map[file_id]
                active_path = checkpoint["path"]
                before = active_path.stat()
                if (before.st_size, before.st_mtime_ns) != (checkpoint["size"], checkpoint["mtime_ns"]):
                    raise RuntimeError("Source file no longer matches indexed checkpoint metadata")
                active_handle = active_path.open("rb")
                active_id = file_id
                prior_end = -1
            if offset < prior_end or offset + size > checkpoint_map[file_id]["size"]:
                raise RuntimeError("Overlapping or out-of-file indexed range")
            active_handle.seek(offset)
            raw = active_handle.read(size)
            if len(raw) != size:
                raise RuntimeError("Short read in an indexed event_msg range")
            prior_end = offset + size
            payload_type, item_type, _ = extract_types(raw)
            records_read += 1
            bytes_read += size
            item_key = item_type if payload_type == "item_completed" else "[not_applicable]"
            file_type = file_types.setdefault(file_id, {}).setdefault(payload_type, {"records": 0, "bytes": 0})
            file_type["records"] += 1
            file_type["bytes"] += size
            global_type = global_types.setdefault(payload_type, {"records": 0, "bytes": 0})
            global_type["records"] += 1
            global_type["bytes"] += size
            file_total = file_totals.setdefault(file_id, {"records": 0, "bytes": 0})
            file_total["records"] += 1
            file_total["bytes"] += size
            if payload_type == "item_completed":
                f_item = file_items.setdefault(file_id, {}).setdefault(item_key, {"records": 0, "bytes": 0})
                f_item["records"] += 1
                f_item["bytes"] += size
                g_item = global_items.setdefault(item_key, {"records": 0, "bytes": 0})
                g_item["records"] += 1
                g_item["bytes"] += size
            del raw
            if records_read % 100000 == 0:
                print(f"event_msg_ranges_read={records_read} indexed_bytes={bytes_read}", flush=True)

    if active_handle is not None:
        active_handle.close()
        current = active_path.stat()
        if (current.st_size, current.st_mtime_ns) != (before.st_size, before.st_mtime_ns):
            raise RuntimeError("Source file changed during the bounded scan")
    if records_read != profile["records"] or bytes_read != profile["source_bytes"]:
        raise RuntimeError("Indexed event_msg scan does not reconcile with its profile")
    event_scan_elapsed = round(time.perf_counter() - started, 3)
    event_scan_peak = peak_memory()

    expected_nested = profile["subtypes"]["item_completed"]["enums"]["payload.item.type"]
    actual_nested = {k: v["records"] for k, v in global_items.items() if k != "[missing_or_other]"}
    if actual_nested != expected_nested:
        raise RuntimeError("item_completed subtype counts differ from the existing profile")
    for file_id, checkpoint in checkpoint_map.items():
        actual = file_totals.get(file_id, {"records": 0, "bytes": 0})
        if actual["records"] != checkpoint["records"] or actual["bytes"] != checkpoint["bytes"]:
            raise RuntimeError("Per-file event_msg range totals differ from their checkpoint")
        actual_types = file_types.get(file_id, {})
        for subtype, expected in checkpoint["subtypes"].items():
            got = actual_types.get(subtype, {"records": 0, "bytes": 0})
            if got["records"] != int(expected["records"]) or got["bytes"] != int(expected["source_bytes"]):
                raise RuntimeError("Per-file event_msg subtype totals differ from their checkpoint")

    # Recovery data contains only the approved grouped projection, never records.
    write_json(EVENT_STAGE, {
        "records": records_read,
        "bytes": bytes_read,
        "elapsed_seconds": event_scan_elapsed,
        "peak_working_set_bytes": event_scan_peak,
        "raw_content_saved": False,
        "record_id_saved": False,
        "by_type": global_types,
        "item_completed_by_type": global_items,
        "files": [
            {"file_internal_id": fid, "records": file_totals[fid]["records"],
             "bytes": file_totals[fid]["bytes"], "by_type": file_types.get(fid, {}),
             "item_completed_by_type": file_items.get(fid, {})}
            for fid in sorted(file_totals)
        ],
    })

    event_type_file_rows = []
    for fid in sorted(file_types):
        for subtype, values in sorted(file_types[fid].items()):
            if subtype == "item_completed":
                for item_type, item_values in sorted(file_items.get(fid, {}).items()):
                    event_type_file_rows.append({
                        "file_internal_id": fid,
                        "payload_type": subtype,
                        "item_type": item_type,
                        "record_count": item_values["records"],
                        "source_record_bytes": item_values["bytes"],
                    })
            else:
                event_type_file_rows.append({
                    "file_internal_id": fid,
                    "payload_type": subtype,
                    "item_type": "",
                    "record_count": values["records"],
                    "source_record_bytes": values["bytes"],
                })

    # Recompute response_item processing using the report's explicit conversion,
    # excluding agent_message and control markers from the processing category.
    response_processing_by_file: dict[int, int] = {}
    response_bytes_by_file: dict[int, int] = {}
    response_records_by_file: dict[int, int] = {}
    response_user_records_by_file: dict[int, int] = {}
    response_assistant_records_by_file: dict[int, int] = {}
    with RESPONSE_BREAKDOWN.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            fid = int(row["file_internal_id"])
            record_bytes = int(row["source_record_bytes"])
            record_count = int(row["records"])
            response_bytes_by_file[fid] = response_bytes_by_file.get(fid, 0) + record_bytes
            response_records_by_file[fid] = response_records_by_file.get(fid, 0) + record_count
            if row["payload_type"] == "message" and row["role"] == "user":
                response_user_records_by_file[fid] = response_user_records_by_file.get(fid, 0) + record_count
            elif row["payload_type"] == "message" and row["role"] == "assistant":
                response_assistant_records_by_file[fid] = response_assistant_records_by_file.get(fid, 0) + record_count
            if row["payload_type"] in RESPONSE_PROCESSING_TYPES:
                response_processing_by_file[fid] = response_processing_by_file.get(fid, 0) + record_bytes
    if sum(response_bytes_by_file.values()) != int(summary["source_scope"]["response_item_source_bytes"]):
        raise RuntimeError("Existing response_item type-role-phase byte totals do not reconcile")
    if sum(response_records_by_file.values()) != int(summary["source_scope"]["response_item_records"]):
        raise RuntimeError("Existing response_item type-role-phase record totals do not reconcile")

    file_rows = []
    with FILES_CSV.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            converted = {}
            for key, value in row.items():
                if not value:
                    converted[key] = 0
                elif key.endswith("_ratio"):
                    converted[key] = float(value)
                else:
                    converted[key] = int(value)
            file_rows.append(converted)
    if len(file_rows) != len(summary["totals_by_file"]):
        raise RuntimeError("Per-file composition CSV does not match the summary")

    total_event = {"records": 0, "bytes": 0}
    event_category_totals = {k: 0 for k in ("processing", "duplicate", "measurements", "other")}
    known_operational_types = set(profile["subtypes"]) - {"user_message", "agent_message", "token_count", "item_completed"}
    files_output = []
    processing_ratios = []
    no_image_ratios = []
    base_ratios = []
    compaction_ratios = []
    for row in file_rows:
        fid = int(row["file_internal_id"])
        type_map = file_types.get(fid, {})
        item_map = file_items.get(fid, {})
        event_bytes = file_totals.get(fid, {}).get("bytes", 0)
        event_records = file_totals.get(fid, {}).get("records", 0)
        duplicate_bytes = sum(type_map.get(t, {}).get("bytes", 0) for t in ("user_message", "agent_message"))
        duplicate_bytes += sum(item_map.get(t, {}).get("bytes", 0) for t in IMAGE_OR_TEXT_DUPLICATES)
        measurement_bytes = type_map.get("token_count", {}).get("bytes", 0)
        event_processing_bytes = sum(type_map.get(t, {}).get("bytes", 0) for t in known_operational_types)
        event_other_bytes = 0
        for nested_type, value in item_map.items():
            if nested_type in IMAGE_OR_TEXT_DUPLICATES:
                continue
            if nested_type in expected_nested:
                event_processing_bytes += value["bytes"]
            else:
                event_other_bytes += value["bytes"]
        known_top = set(profile["subtypes"])
        event_other_bytes += sum(v["bytes"] for t, v in type_map.items() if t not in known_top)
        if duplicate_bytes + measurement_bytes + event_processing_bytes + event_other_bytes != event_bytes:
            raise RuntimeError(f"event_msg category partition failed for internal file {fid}")
        row.update({
            "event_msg_records": event_records,
            "event_msg_bytes": event_bytes,
            "event_duplicate_bytes": duplicate_bytes,
            "event_user_message_bytes": type_map.get("user_message", {}).get("bytes", 0),
            "event_agent_message_bytes": type_map.get("agent_message", {}).get("bytes", 0),
            "event_item_completed_user_message_bytes": item_map.get("UserMessage", {}).get("bytes", 0),
            "event_item_completed_agent_message_bytes": item_map.get("AgentMessage", {}).get("bytes", 0),
            "event_processing_bytes": event_processing_bytes,
            "event_token_count_measurement_bytes": measurement_bytes,
            "event_other_unclassified_bytes": event_other_bytes,
        })
        response_bytes = response_bytes_by_file.get(fid, 0)
        response_proc = response_processing_by_file.get(fid, 0)
        response_other = response_bytes - int(row["user_role_message_bytes"]) - int(row["assistant_message_bytes"]) - response_proc
        if response_other < 0:
            raise RuntimeError(f"response_item category partition failed for internal file {fid}")
        row["response_item_records"] = response_records_by_file.get(fid, 0)
        row["response_item_bytes"] = response_bytes
        row["user_role_message_records"] = response_user_records_by_file.get(fid, 0)
        row["assistant_message_records"] = response_assistant_records_by_file.get(fid, 0)
        row["response_item_processing_bytes"] = response_proc
        row["response_item_other_unclassified_bytes"] = response_other
        processing = response_proc + event_processing_bytes
        base = int(row["base_conversation_bytes"])
        no_image_base = base - int(row["embedded_user_image_base64_bytes"])
        compacted = int(row["compacted_bytes"])
        row["processing_bytes"] = processing
        row["base_conversation_bytes_excluding_counted_image_base64"] = no_image_base
        row["A1_no_image_file_bytes_over_base_ratio"] = int(row["file_size_bytes"]) / no_image_base if no_image_base else None
        row["A2_processing_bytes_over_base_ratio"] = processing / base if base else None
        if base:
            base_ratios.append(int(row["file_size_bytes"]) / base)
            compaction_ratios.append(compacted / base)
            processing_ratios.append(processing / base)
        if no_image_base:
            no_image_ratios.append(int(row["file_size_bytes"]) / no_image_base)
        total_event["records"] += event_records
        total_event["bytes"] += event_bytes
        event_category_totals["duplicate"] += duplicate_bytes
        event_category_totals["measurements"] += measurement_bytes
        event_category_totals["processing"] += event_processing_bytes
        event_category_totals["other"] += event_other_bytes
        files_output.append(row)

    if (total_event["records"] != profile["records"] or total_event["bytes"] != profile["source_bytes"]):
        raise RuntimeError("Merged event_msg totals do not reconcile")
    for category, expected in {
        "user_message": profile["subtypes"]["user_message"]["source_bytes"],
        "agent_message": profile["subtypes"]["agent_message"]["source_bytes"],
        "token_count": profile["subtypes"]["token_count"]["source_bytes"],
        "item_completed": profile["subtypes"]["item_completed"]["source_bytes"],
    }.items():
        actual = sum(v.get("bytes", 0) for types in file_types.values() for t, v in types.items() if t == category)
        if actual != int(expected):
            raise RuntimeError(f"Global event subtype bytes failed reconciliation: {category}")

    base_bytes = sum(int(row["base_conversation_bytes"]) for row in files_output)
    no_image_base_bytes = sum(int(row["base_conversation_bytes_excluding_counted_image_base64"]) for row in files_output)
    corpus_bytes = sum(int(row["file_size_bytes"]) for row in files_output)
    compacted_bytes = sum(int(row["compacted_bytes"]) for row in files_output)
    processing_bytes = sum(int(row["processing_bytes"]) for row in files_output)
    image_bytes = sum(int(row["embedded_user_image_base64_bytes"]) for row in files_output)
    response_processing_total = sum(response_processing_by_file.values())
    if sum(base_ratios) == 0 or no_image_base_bytes <= 0:
        raise RuntimeError("A1 denominator is empty")
    elapsed = event_scan_elapsed
    peak = event_scan_peak

    # Replace previous incomplete rows with the completed aggregate-only rows.
    summary["status"] = "completed"
    summary["completed_at_utc"] = utc_now()
    summary["source_scope"].update({
        "source_record_types_read": ["response_item", "event_msg"],
        "event_msg_records": records_read,
        "event_msg_source_bytes": bytes_read,
        "all_other_source_record_ranges_opened": False,
    })
    summary["event_msg_nested_byte_gap"] = {
        "item_completed_bytes_by_nested_type": True,
        "source_ranges_opened": True,
        "records_read": records_read,
        "bytes_read": bytes_read,
        "nested_type_counts_reconciled": True,
        "raw_content_saved": False,
        "record_id_exported": False,
        "response_item_source_ranges_opened_in_this_run": False,
    }
    summary["event_msg_projection"] = {
        "method": "indexed byte ranges; structural JSON scanner skips non-target values without decoding or retaining them",
        "read_only": True,
        "records_read": records_read,
        "bytes_read": bytes_read,
        "files": len(file_totals),
        "by_payload_type": [
            {"payload_type": key, **value} for key, value in sorted(global_types.items())
        ],
        "item_completed_by_item_type": [
            {"item_type": key, **value} for key, value in sorted(global_items.items())
        ],
        "category_bytes": event_category_totals,
        "by_file_type_csv": "event_msg_type_by_file.csv",
        "raw_content_saved": False,
        "record_id_exported": False,
    }
    response_totals = summary["response_item_totals"]
    response_totals["response_item_processing_bytes"] = response_processing_total
    response_totals["response_item_other_unclassified_bytes"] = sum(
        int(row["response_item_other_unclassified_bytes"]) for row in files_output
    )
    summary["classification_mapping"] = [
        {"category": "Input — user-role", "mapping": "response_item/message/role=user", "status": "measured"},
        {"category": "Visible output", "mapping": "response_item/message/role=assistant; final_answer, commentary, other or missing phase", "status": "measured"},
        {"category": "Processing", "mapping": "listed reasoning/tool response_item types, event_msg execution subtypes, and item_completed types except UserMessage/AgentMessage", "status": "measured"},
        {"category": "Compaction snapshots", "mapping": "compacted, disjointly partitioned by adjacent repeated image, adjacent repeated text, and remaining bytes", "status": "measured"},
        {"category": "Logging duplicates", "mapping": "event_msg user_message, agent_message, and item_completed UserMessage/AgentMessage", "status": "measured"},
        {"category": "Measurements and state", "mapping": "token_count, token_usage_record, turn_context, world_state, session_meta, inter_agent_communication_metadata", "status": "mapped; not included in A2"},
        {"category": "Other / unclassified", "mapping": "unlisted response_item types, role=developer, unknown/null types, and unindexed file bytes", "status": "mapped; not assigned by inference"},
    ]
    summary["note"] = "Byte-size measurements describe stored records only. They do not establish model input or convert to tokens or cost. User-role does not prove a human author."
    summary["inflation_ratios"] = {
        "ratio_units": "multiples of base bytes; storage ratios only, not token or cost estimates",
        "per_file_distribution_method": "unweighted median, linear-interpolated p90, max; arithmetic mean is in the report note only",
        "files_with_zero_base_excluded_from_per_file_ratios": len(files_output) - len(base_ratios),
        "A1": {
            "definition": "sum(file_size_bytes) / sum(user-role and assistant message bytes)",
            "weighted_global": corpus_bytes / base_bytes,
            "per_file": distribution(base_ratios),
        },
        "A1_without_counted_image_base64": {
            "definition": "sum(file_size_bytes) / sum(user-role and assistant message bytes minus embedded image Base64 bytes)",
            "weighted_global": corpus_bytes / no_image_base_bytes,
            "per_file": distribution(no_image_ratios),
        },
        "A2": {
            "definition": "sum(response-item processing bytes + event execution-processing bytes) / sum(user-role and assistant message bytes)",
            "weighted_global": processing_bytes / base_bytes,
            "per_file": distribution(processing_ratios),
        },
        "A3": {
            "definition": "sum(compacted bytes) / sum(user-role and assistant message bytes)",
            "weighted_global": compacted_bytes / base_bytes,
            "per_file": distribution(compaction_ratios),
        },
        "denominators": {
            "base_conversation_bytes": base_bytes,
            "base_conversation_bytes_excluding_counted_image_base64": no_image_base_bytes,
            "embedded_user_image_base64_bytes_subtotal": image_bytes,
        },
    }
    summary["resource_measurements"] = {
        "response_item_scan": {
            "bytes_read": int(summary["source_scope"]["response_item_source_bytes"]),
            "elapsed_seconds": float(summary["elapsed_seconds"]),
            "peak_working_set_bytes": int(summary["peak_working_set_bytes"]),
        },
        "event_msg_scan": {
            "bytes_read": bytes_read,
            "elapsed_seconds": elapsed,
            "peak_working_set_bytes": peak,
            "priority": priority,
        },
        "combined_source_bytes_read": int(summary["source_scope"]["response_item_source_bytes"]) + bytes_read,
        "combined_elapsed_seconds": round(float(summary["elapsed_seconds"]) + elapsed, 3),
        "combined_peak_working_set_bytes": max(int(summary["peak_working_set_bytes"]), int(peak or 0)),
    }
    summary["totals_by_file"] = files_output

    # Preserve only aggregate fields and internal numeric file IDs.
    fields = list(files_output[0].keys())
    with FILES_CSV.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(files_output)
    with EVENT_TYPE_BY_FILE.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=[
            "file_internal_id", "payload_type", "item_type", "record_count", "source_record_bytes"
        ])
        writer.writeheader()
        writer.writerows(event_type_file_rows)

    # Rewrite the report using only aggregate measurements.
    response_bytes = int(summary["source_scope"]["response_item_source_bytes"])
    event_duplicate = event_category_totals["duplicate"]
    event_processing = event_category_totals["processing"]
    event_other = event_category_totals["other"]
    a1_global = corpus_bytes / base_bytes
    a1_no_image_global = corpus_bytes / no_image_base_bytes
    a2_global = processing_bytes / base_bytes
    a3_global = compacted_bytes / base_bytes
    avg_a1 = statistics.fmean(base_ratios)
    avg_no_image = statistics.fmean(no_image_ratios)
    avg_a2 = statistics.fmean(processing_ratios)
    avg_a3 = statistics.fmean(compaction_ratios)

    def ratio_row(label: str, weighted: float, vals: list[float]) -> str:
        return f"| {label} | {weighted:.4f}× | {statistics.median(vals):.4f}× | {percentile90(vals):.4f}× | {max(vals):.4f}× |"

    body = [
        "# تركيب مساحة تخزين سجلات الجلسات", "",
        "**الحالة: مكتمل ضمن نطاق العينة** — فُحصت نطاقات `response_item` و`event_msg` المفهرسة فقط. لم تُفتح أنواع سجلات أخرى.",
        f"نطاق العينة: {len(files_output):,} ملفًا، وحجم الملفات {corpus_bytes:,} بايت ({corpus_bytes/1024**3:.4f} GiB).",
        "استخدم الفحص قارئًا بنيويًا يتجاوز القيم غير المطلوبة ولا يحتفظ بها؛ لم تُحفظ محتويات السجلات أو تُعرض.", "",
        "## أساس القياس: رسائل user-role وassistant", "",
        f"رسائل user-role: {sum(int(row['user_role_message_records']) for row in files_output):,} سجلًا، {sum(int(row['user_role_message_bytes']) for row in files_output):,} بايت. رسائل assistant: {sum(int(row['assistant_message_bytes']) for row in files_output):,} بايت، منها final_answer {sum(int(row['assistant_final_answer_bytes']) for row in files_output):,} وcommentary {sum(int(row['assistant_commentary_bytes']) for row in files_output):,} بايت.",
        f"حجم رسائل user-role وassistant معًا: **{base_bytes:,} بايت**. يحتوي هذا الأساس على **{image_bytes:,} بايت** من أحرف Base64 للصور المضمنة برسائل user-role؛ لم تُفك الصور، والقياس لا يشمل بادئة data URI أو تركيب JSON.",
        f"أساس رسائل user-role وassistant بعد طرح أحرف Base64 المقاسة: **{no_image_base_bytes:,} بايت**. هذا يستبعد بايتات Base64 فقط؛ وتبقى بادئات الصور وتركيب JSON ضمن الأساس.", "",
        "## النسبة الموزونة للعينة والوسيط لكل ملف", "",
        "| المقياس | النسبة الموزونة للعينة | وسيط النسبة لكل ملف | المئين 90 لكل ملف | أعلى نسبة لكل ملف |",
        "|---|---:|---:|---:|---:|",
        ratio_row("A1 — إجمالي بايت الملفات ÷ بايت رسائل user-role وassistant", a1_global, base_ratios),
        ratio_row("A1 دون أحرف Base64 للصور ÷ بايت الأساس المنقّح", a1_no_image_global, no_image_ratios),
        ratio_row("A2 — بايت المعالجة ÷ بايت رسائل user-role وassistant", a2_global, processing_ratios),
        ratio_row("A3 — بايت compacted ÷ بايت رسائل user-role وassistant", a3_global, compaction_ratios), "",
        f"البايتات الأساسية: {base_bytes:,}. وبعد طرح {image_bytes:,} بايت من أحرف Base64 المضمنة يصبح أساس A1 دون الصور **{no_image_base_bytes:,} بايت**. الطرح يزيل أحرف Base64 المقاسة فقط، ولا يزيل بادئة data URI أو تركيب JSON.",
        f"النسبة الموزونة تساوي مجموع البايتات في البسط ÷ مجموع الأساس في المقام. وسيط كل ملف يعطي وصفًا للملف المعتاد دون أن تهيمن الملفات الكبيرة؛ المئين 90 محسوب بالاستيفاء الخطي. **المتوسط الحسابي غير الموزون** لكل ملف — A1 {avg_a1:.4f}×، وA1 دون الصور {avg_no_image:.4f}×، وA2 {avg_a2:.4f}×، وA3 {avg_a3:.4f}× — مذكور للمقارنة فقط، ويتأثر بشدة بالملفات ذات الأساس الصغير.",
        f"استُبعد {len(files_output)-len(base_ratios):,} ملفًا من توزيعات النسب الفردية لأن بايت أساس رسائل user-role وassistant فيها صفر. لا تُحوّل هذه المقاييس التخزينية إلى توكنات أو تكلفة.", "",
        "## أرقام القياس والتصنيف المتنافي", "",
        f"قرأ فحص `response_item` السابق {response_bytes:,} بايت في {summary['resource_measurements']['response_item_scan']['elapsed_seconds']:.3f} ثانية. وقرأ الفحص الحالي {bytes_read:,} بايت من نطاقات `event_msg` في {elapsed:.3f} ثانية. مجموع البايتات المقروءة عبر الفحصين **{summary['resource_measurements']['combined_source_bytes_read']:,} بايت**؛ وذروة الذاكرة المرصودة عبرهما **{summary['resource_measurements']['combined_peak_working_set_bytes']:,} بايت ({summary['resource_measurements']['combined_peak_working_set_bytes']/1024**2:.2f} MiB)**.",
        f"`event_msg`: {records_read:,} سجلًا. وزّعت بايتاته دون تداخل إلى: تكرار تسجيلي {event_duplicate:,}، وأحداث معالجة {event_processing:,}، وقياسات `token_count` {event_category_totals['measurements']:,}، وأخرى/غير مصنفة {event_other:,} بايت.",
        f"بايت المعالجة المستخدم في A2 = {response_processing_total:,} بايت من `response_item` + {event_processing:,} بايت من `event_msg` = **{processing_bytes:,} بايت**.",
        "لم تُحفظ سجلات مفردة أو محتويات؛ المخرجات لا تحتوي إلا إجماليات الأنواع، الأطوال، ومعرّفات الملفات الداخلية الرقمية. معرّفات السجلات ومساراتها ونصوصها وصورها لم تُصدّر.", "",
        "## جدول التحويل الكامل للأنواع", "",
        "| النوع/النوع الفرعي | الفئة | قاعدة الإسناد |", "|---|---|---|",
        "| `response_item/message` بدور `user` | إدخال — user-role | يُسمى user-role ولا يثبت أن الكاتب بشري |",
        "| `response_item/message` بدور `assistant` | إخراج ظاهر | تفصيل `final_answer` و`commentary`، مع فئة مستقلة للطور الآخر/المفقود |",
        "| `response_item/reasoning`, `custom_tool_call`, `custom_tool_call_output`, `function_call`, `function_call_output`, `web_search_call`, `tool_search_call`, `tool_search_output`, `image_generation_call` | معالجة | استدلال واستدعاءات/نتائج أدوات |",
        "| `response_item/compaction`, `agent_message`, `ghost_snapshot`، ورسائل developer/الأدوار الأخرى | أخرى أو غير مصنفة | لا تخلط بعناصر الإدخال أو الإخراج المرئي |",
        "| `compacted` | لقطات الضغط | صور ونصوص معادة في السجل التالي، ثم باقي بايتات compacted؛ فئات متنافية من التقرير السابق |",
        "| `event_msg/user_message`, `event_msg/agent_message` | تكرار تسجيلي | نسخ الرسائل في event stream |",
        "| `event_msg/item_completed` مع `payload.item.type`=`UserMessage` أو `AgentMessage` | تكرار تسجيلي | نسخة الحدث الخاصة بالرسالة |",
        "| `event_msg/item_completed` مع `ContextCompaction`, `Reasoning`, `CommandExecution`, `McpToolCall`, `FileChange`, `Extension`, `Plan`, `DynamicToolCall`, `WebSearch`, `SubAgentActivity`, `CollabAgentToolCall`, `ImageView` | معالجة | أحداث/نتائج تنفيذ |",
        "| `event_msg/agent_reasoning`, `collab_agent_interaction_end`, `collab_agent_spawn_end`, `collab_close_end`, `collab_waiting_end`, `context_compacted`, `dynamic_tool_call_request`, `dynamic_tool_call_response`, `error`, `exec_command_end`, `image_generation_end`, `mcp_tool_call_end`, `patch_apply_end`, `sub_agent_activity`, `task_complete`, `task_started`, `thread_goal_updated`, `thread_name_updated`, `thread_rolled_back`, `thread_settings_applied`, `turn_aborted`, `view_image_tool_call`, `web_search_end` | معالجة | أحداث تشغيل وتنفيذ في event stream |",
        "| `event_msg/token_count`, `token_usage_record` | قياسات وحالة | قياسات توكنات؛ لا تُحوّل إلى توكنات إضافية من حجمها |",
        "| `turn_context`, `world_state`, `session_meta`, `inter_agent_communication_metadata` | قياسات وحالة | حالة وسياق وبيانات وصفية |",
        "| `NULL` أو نوع غير متوقع/بايتات غير مفهرسة | أخرى أو غير مصنفة | لا تُسند إلى فئة بالتخمين |", "",
        "## حدود التفسير", "",
        "البايت يقيس حجم التخزين ولا يساوي توكنًا. دور user لا يثبت أن كاتب الرسالة إنسان، وتكرار التخزين لا يثبت أن المحتوى أُرسل إلى النموذج. لا تعميم خارج ملفات العينة المفهرسة.", "",
        "## المخرجات", "",
        "`summary.json` و`files.csv` (صف لكل ملف ومعرّف داخلي رقمي)، و`response_item_by_file.csv`، و`response_item_type_role_phase.csv`، و`event_msg_type_by_file.csv` (إجماليات حسب الملف والنوع الفرعي). لا تتضمن المخرجات نصوص محادثات أو صورًا أو مسارات أو معرّفات سجلات.",
    ]
    REPORT.write_text("\n".join(body) + "\n", encoding="utf-8", newline="\n")
    summary["resource_measurements"]["event_msg_scan"]["completed_at_utc"] = utc_now()
    write_json(SUMMARY, summary)
    EVENT_STAGE.unlink(missing_ok=True)
    print(json.dumps({
        "status": summary["status"], "event_msg_records": records_read,
        "event_msg_bytes_read": bytes_read,
        "combined_bytes_read": summary["resource_measurements"]["combined_source_bytes_read"],
        "A1_weighted": a1_global, "A1_without_image_base64_weighted": a1_no_image_global,
        "A2_weighted": a2_global, "A3_weighted": a3_global,
        "processing_bytes": processing_bytes,
        "event_msg_elapsed_seconds": elapsed,
        "combined_peak_working_set_bytes": summary["resource_measurements"]["combined_peak_working_set_bytes"],
    }, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
