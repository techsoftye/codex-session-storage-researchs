"""Profile message role/phase byte sizes for the storage-composition study.

Uses only indexed response_item byte ranges and the existing
profile_response_items.py parser. Conversation content is never exported.
"""
from __future__ import annotations

import csv
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import statistics
import time

from import_session_survey import reject_constant, unique_object
from index_session_records import ROOT, json_rows, lower_priority, safe_source_path
from profile_response_items import (
    MAX_RECORD_BYTES,
    OUT as PROFILE_OUT,
    add_record,
    new_stats,
    peak_memory,
)


OUT = ROOT / "data/reports/storage_composition_v1"
PROFILE_SUMMARY = PROFILE_OUT / "summary.json"
LOCATORS = PROFILE_OUT / "locators.csv"
EVENT_SUMMARY = ROOT / "data/reports/event_msg_profile_v1/summary.json"
EVENT_CHECKPOINTS = ROOT / "data/reports/event_msg_profile_v1/checkpoints"
COMPACTED_GROWTH = ROOT / "data/reports/compaction_growth_v1/summary.json"
TEXT_GROWTH = ROOT / "data/reports/compaction_growth_v1/text_growth.json"
EXPECTED_ROLES = {"user", "assistant", "developer"}
EXPECTED_PHASES = {"final_answer", "commentary"}
PROCESSING_RESPONSE_TYPES = {
    "reasoning", "custom_tool_call", "custom_tool_call_output", "function_call",
    "function_call_output", "agent_message", "web_search_call", "tool_search_call",
    "tool_search_output", "image_generation_call",
}


def load_rows(sql):
    sql = "BEGIN READ ONLY;\n" + sql + "\nCOMMIT;\n"
    rows = []
    for line in json_rows(sql):
        rows.append(line)
    return rows


def csv_sha256(path):
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def safe_label(value, allowed, missing="[missing]"):
    if value is None:
        return missing
    if isinstance(value, str) and value in allowed:
        return value
    return "[other]"


def encoded_image_payload_bytes(value):
    """Count embedded data:image Base64 characters without decoding them."""
    total = 0
    occurrences = 0
    stack = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, dict):
            stack.extend(item.values())
        elif isinstance(item, list):
            stack.extend(item)
        elif isinstance(item, str) and item.startswith("data:image/") and ";base64," in item[:160]:
            marker = item.find(",")
            if marker >= 0:
                payload = item[marker + 1:]
                try:
                    total += len(payload.encode("ascii"))
                except UnicodeEncodeError:
                    # Non-ASCII after a Base64 data-URI marker is not counted as Base64.
                    continue
                occurrences += 1
    return total, occurrences


def response_category(payload_type, role, phase):
    if payload_type == "message" and role == "user":
        return "input_user_role"
    if payload_type == "message" and role == "assistant":
        return "assistant_visible_output"
    if payload_type in PROCESSING_RESPONSE_TYPES:
        return "processing"
    return "other_unclassified"


def write_json(path, value):
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2, allow_nan=False)
        handle.write("\n")


def main():
    started_clock = time.perf_counter()
    started_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    priority = lower_priority()
    profile = json.loads(PROFILE_SUMMARY.read_text(encoding="utf-8"))
    event_profile = json.loads(EVENT_SUMMARY.read_text(encoding="utf-8"))
    compacted_growth = json.loads(COMPACTED_GROWTH.read_text(encoding="utf-8"))
    text_growth = json.loads(TEXT_GROWTH.read_text(encoding="utf-8"))
    if profile.get("status") != "completed" or event_profile.get("status") != "completed":
        raise RuntimeError("Required existing profiles are incomplete")
    if csv_sha256(LOCATORS) != profile.get("locator_sha256"):
        raise RuntimeError("Existing response_item locators do not match their profile")

    file_rows = load_rows("""
SELECT json_build_object('file_id',id,'source_relative_path',source_relative_path,
                         'file_size_bytes',file_size_bytes,'record_count',record_count)
FROM public.research_sessions ORDER BY id;
""")
    if not file_rows:
        raise RuntimeError("No research session files found")
    file_map = {int(row["file_id"]): row for row in file_rows}
    grouped_rows = load_rows("""
SELECT json_build_object('file_id',research_session_id,'record_type',record_type,
                         'record_count',count(*),'source_record_bytes',sum(source_record_bytes))
FROM public.research_session_records
GROUP BY research_session_id,record_type
ORDER BY research_session_id,record_type NULLS FIRST;
""")
    db_type_bytes = {}
    db_response_records = db_response_bytes = 0
    db_event_bytes = 0
    for row in grouped_rows:
        file_id = int(row["file_id"])
        typ = row["record_type"] if row["record_type"] is not None else "[null]"
        db_type_bytes.setdefault(file_id, {})[typ] = {
            "records": int(row["record_count"]),
            "bytes": int(row["source_record_bytes"] or 0),
        }
        if typ == "response_item":
            db_response_records += int(row["record_count"])
            db_response_bytes += int(row["source_record_bytes"] or 0)
        elif typ == "event_msg":
            db_event_bytes += int(row["source_record_bytes"] or 0)
    if db_response_records != profile["records"] or db_response_bytes != profile["source_bytes"]:
        raise RuntimeError("Response-item profile no longer matches PostgreSQL locator totals")

    file_metrics = {}
    for file_id, entry in file_map.items():
        file_metrics[file_id] = {
            "file_id": file_id,
            "file_size_bytes": int(entry["file_size_bytes"]),
            "record_count": int(entry["record_count"]),
            "response_item_records": 0,
            "response_item_bytes": 0,
            "user_role_message_records": 0,
            "user_role_message_bytes": 0,
            "user_role_embedded_image_occurrences": 0,
            "user_role_embedded_image_base64_bytes": 0,
            "assistant_message_records": 0,
            "assistant_message_bytes": 0,
            "assistant_final_answer_bytes": 0,
            "assistant_commentary_bytes": 0,
            "assistant_other_or_missing_phase_bytes": 0,
            "response_item_processing_bytes": 0,
            "response_item_other_unclassified_bytes": 0,
        }
        for typ, group in db_type_bytes.get(file_id, {}).items():
            file_metrics[file_id]["indexed_record_bytes"] = file_metrics[file_id].get("indexed_record_bytes", 0) + group["bytes"]
        if file_metrics[file_id].get("indexed_record_bytes", 0) > file_metrics[file_id]["file_size_bytes"]:
            raise RuntimeError("Indexed record bytes exceed source file size")
        file_metrics[file_id]["unindexed_file_bytes"] = (
            file_metrics[file_id]["file_size_bytes"] - file_metrics[file_id].get("indexed_record_bytes", 0)
        )
    breakdown = {}
    per_file_breakdown = {file_id: {} for file_id in file_metrics}
    engine_stats = new_stats()
    scan_records = scan_bytes = image_occurrences = image_payload_bytes = failures = 0
    active_file_id = None
    active_handle = None
    active_path = None
    active_before = None
    previous_end = -1
    expected_file_bytes = {file_id: metric["file_size_bytes"] for file_id, metric in file_metrics.items()}

    with LOCATORS.open("r", encoding="utf-8", newline="") as locators:
        reader = csv.reader(locators)
        for row in reader:
            if len(row) != 4:
                raise RuntimeError("Unexpected existing locator row shape")
            file_id, record_id, offset, size = map(int, row)
            if file_id not in file_map or file_id not in file_metrics:
                raise RuntimeError("Locator references an unknown research file")
            if active_file_id != file_id:
                if active_handle is not None:
                    active_handle.close()
                    after = active_path.stat()
                    if (after.st_size, after.st_mtime_ns) != (active_before.st_size, active_before.st_mtime_ns):
                        raise RuntimeError("Source changed during bounded scan")
                active_file_id = file_id
                active_path = safe_source_path(file_map[file_id]["source_relative_path"])
                active_before = active_path.stat()
                if active_before.st_size != expected_file_bytes[file_id]:
                    raise RuntimeError("Indexed source size changed")
                active_handle = active_path.open("rb")
                previous_end = -1
            if offset < previous_end or size <= 0 or size > MAX_RECORD_BYTES or offset + size > expected_file_bytes[file_id]:
                raise RuntimeError("Invalid or overlapping response_item byte range")
            active_handle.seek(offset)
            raw = active_handle.read(size)
            if len(raw) != size:
                raise RuntimeError("Incomplete indexed response_item byte range")
            previous_end = offset + size
            scan_records += 1
            scan_bytes += size
            try:
                value = json.loads(raw, object_pairs_hook=unique_object, parse_constant=reject_constant)
                add_record(engine_stats, value, size, "response_item")
            except (ValueError, UnicodeError, RecursionError) as error:
                failures += 1
                file_metrics[file_id]["response_item_other_unclassified_bytes"] += size
                del raw
                continue
            payload = value.get("payload") if isinstance(value, dict) else None
            payload = payload if isinstance(payload, dict) else {}
            raw_type = payload.get("type")
            subtype = safe_label(raw_type, set(profile["subtypes"]))
            role = safe_label(payload.get("role"), EXPECTED_ROLES)
            phase = safe_label(payload.get("phase"), EXPECTED_PHASES)
            partition = response_category(subtype, role, phase)
            metric = file_metrics[file_id]
            metric["response_item_records"] += 1
            metric["response_item_bytes"] += size
            metric_key = (subtype, role, phase)
            row_metric = breakdown.setdefault(metric_key, {"records": 0, "bytes": 0})
            row_metric["records"] += 1
            row_metric["bytes"] += size
            per_file_metric = per_file_breakdown[file_id].setdefault(metric_key, {"records": 0, "bytes": 0})
            per_file_metric["records"] += 1
            per_file_metric["bytes"] += size
            if partition == "input_user_role":
                metric["user_role_message_records"] += 1
                metric["user_role_message_bytes"] += size
                image_bytes, found_images = encoded_image_payload_bytes(value)
                metric["user_role_embedded_image_occurrences"] += found_images
                metric["user_role_embedded_image_base64_bytes"] += image_bytes
                image_occurrences += found_images
                image_payload_bytes += image_bytes
            elif partition == "assistant_visible_output":
                metric["assistant_message_records"] += 1
                metric["assistant_message_bytes"] += size
                if phase == "final_answer":
                    metric["assistant_final_answer_bytes"] += size
                elif phase == "commentary":
                    metric["assistant_commentary_bytes"] += size
                else:
                    metric["assistant_other_or_missing_phase_bytes"] += size
            elif partition == "processing":
                metric["response_item_processing_bytes"] += size
            else:
                metric["response_item_other_unclassified_bytes"] += size
            del value, raw
            if scan_records % 100000 == 0:
                print(f"response_item_ranges_read={scan_records} indexed_bytes={scan_bytes}", flush=True)
    if active_handle is not None:
        active_handle.close()
        after = active_path.stat()
        if (after.st_size, after.st_mtime_ns) != (active_before.st_size, active_before.st_mtime_ns):
            raise RuntimeError("Source changed during bounded scan")
    if scan_records != profile["records"] or scan_bytes != profile["source_bytes"]:
        raise RuntimeError("Response-item scan did not reconcile with its existing profile")
    for file_id, metric in file_metrics.items():
        expected = db_type_bytes.get(file_id, {}).get("response_item", {"records": 0, "bytes": 0})
        if metric["response_item_records"] != expected["records"] or metric["response_item_bytes"] != expected["bytes"]:
            raise RuntimeError("Per-file response_item projection did not reconcile with PostgreSQL")
    if failures or engine_stats["failed_records"]:
        raise RuntimeError("Response-item parser rejected one or more indexed records")
    if engine_stats["subtypes"] != profile["subtypes"]:
        raise RuntimeError("Response-item engine output differs from the existing profile")

    if not breakdown or not any(per_file_breakdown.values()):
        raise RuntimeError("No type/role/phase breakdown was produced")

    # Load exact per-file subtype bytes from the existing event_msg checkpoints.
    checkpoint_event_bytes = {}
    checkpoint_event_records = 0
    checkpoint_event_source_bytes = 0
    checkpoint_files = 0
    for checkpoint_path in EVENT_CHECKPOINTS.glob("*.json"):
        checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        file_id = int(checkpoint_path.stem)
        stats = checkpoint["stats"]
        checkpoint_files += 1
        checkpoint_event_records += int(stats["records"])
        checkpoint_event_source_bytes += int(stats["source_bytes"])
        checkpoint_event_bytes[file_id] = {
            subtype: {"records": int(value["records"]), "bytes": int(value["source_bytes"])}
            for subtype, value in stats["subtypes"].items()
        }
    if (checkpoint_files != event_profile["files_profiled"]
            or checkpoint_event_records != event_profile["records"]
            or checkpoint_event_source_bytes != event_profile["source_bytes"]):
        raise RuntimeError("Existing per-file event_msg profile checkpoints failed reconciliation")
    nested_event_gaps = {
        "item_completed_bytes_by_nested_type": False,
        "reason": "The existing event_msg profile records counts for payload.item.type but does not report per-type bytes.",
        "user_message_occurrences": event_profile["subtypes"]["item_completed"]["enums"]["payload.item.type"].get("UserMessage", 0),
        "agent_message_occurrences": event_profile["subtypes"]["item_completed"]["enums"]["payload.item.type"].get("AgentMessage", 0),
    }

    # The authorized response_item scan is complete. Final all-byte inflation
    # ratios require the missing nested event byte split, so mark the report pending.
    elapsed = round(time.perf_counter() - started_clock, 3)
    peak = peak_memory()
    totals = {
        "user_role_message_records": sum(m["user_role_message_records"] for m in file_metrics.values()),
        "user_role_message_bytes": sum(m["user_role_message_bytes"] for m in file_metrics.values()),
        "user_role_embedded_image_occurrences": image_occurrences,
        "user_role_embedded_image_base64_bytes": image_payload_bytes,
        "assistant_message_records": sum(m["assistant_message_records"] for m in file_metrics.values()),
        "assistant_message_bytes": sum(m["assistant_message_bytes"] for m in file_metrics.values()),
        "assistant_final_answer_bytes": sum(m["assistant_final_answer_bytes"] for m in file_metrics.values()),
        "assistant_commentary_bytes": sum(m["assistant_commentary_bytes"] for m in file_metrics.values()),
        "assistant_other_or_missing_phase_bytes": sum(m["assistant_other_or_missing_phase_bytes"] for m in file_metrics.values()),
        "response_item_processing_bytes": sum(m["response_item_processing_bytes"] for m in file_metrics.values()),
        "response_item_other_unclassified_bytes": sum(m["response_item_other_unclassified_bytes"] for m in file_metrics.values()),
    }
    summary = {
        "version": "1.0.0",
        "status": "awaiting_nested_event_msg_byte_split",
        "started_at_utc": started_at,
        "completed_at_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "elapsed_seconds": elapsed,
        "priority": priority,
        "peak_working_set_bytes": peak,
        "read_only": True,
        "source_scope": {
            "source_record_type_read": "response_item",
            "response_item_records": scan_records,
            "response_item_source_bytes": scan_bytes,
            "source_files_with_response_items": profile["files_profiled"],
            "all_other_source_record_ranges_opened": False,
        },
        "response_item_profile_engine": {
            "version": profile["version"],
            "engine_reconciliation": True,
            "locator_sha256": profile["locator_sha256"],
        },
        "response_item_totals": totals,
        "payload_type_role_phase_totals": [
            {"payload_type": key[0], "role": key[1], "phase": key[2], **value}
            for key, value in sorted(breakdown.items())
        ],
        "event_msg_nested_byte_gap": nested_event_gaps,
        "existing_report_scope": {
            "compacted_files": compacted_growth["totals"]["compacted_files"],
            "compacted_records": compacted_growth["totals"]["compacted_records"],
            "compacted_bytes": compacted_growth["totals"]["compacted_bytes"],
            "text_stored_bytes": text_growth["totals"]["text_stored_bytes"],
            "event_profileled_files": checkpoint_files,
            "event_profiled_bytes": checkpoint_event_source_bytes,
            "postgres_event_bytes": db_event_bytes,
        },
        "totals_by_file": list(file_metrics.values()),
        "note": "Final A1/A2/A3 ratios are withheld: item_completed UserMessage/AgentMessage copies are counted in existing reports but their per-record byte split is unavailable without reading event_msg ranges, which this authorization excludes.",
    }
    OUT.mkdir(parents=True, exist_ok=True)
    write_json(OUT / "summary.json", summary)
    with (OUT / "response_item_by_file.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        fieldnames = list(next(iter(file_metrics.values())).keys())
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(file_metrics[file_id] for file_id in sorted(file_metrics))
    with (OUT / "response_item_type_role_phase.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        fieldnames = ["file_internal_id", "payload_type", "role", "phase", "records", "source_record_bytes"]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for file_id in sorted(per_file_breakdown):
            for key, value in sorted(per_file_breakdown[file_id].items()):
                writer.writerow({"file_internal_id": file_id, "payload_type": key[0], "role": key[1],
                                 "phase": key[2], "records": value["records"],
                                 "source_record_bytes": value["bytes"]})
    report = [
        "# Storage composition — response_item measurement stage",
        "",
        f"Status: awaiting authorization for the remaining event byte split. Generated: {summary['completed_at_utc']} UTC.",
        f"Read-only scan time: {elapsed:.3f} s. Priority: {priority}. Peak working set: {peak or 0:,} bytes.",
        "",
        f"The approved profile engine processed {scan_records:,} indexed `response_item` ranges ({scan_bytes:,} bytes) across {profile['files_profiled']:,} files. It read one record range at a time; no other source record type was opened.",
        "",
        "## Message sizes",
        "",
        f"User-role message records: {totals['user_role_message_records']:,}, {totals['user_role_message_bytes']:,} bytes. Embedded image Base64 payload subtotal: {image_occurrences:,} occurrences, {image_payload_bytes:,} bytes (not decoded; excludes the data-URI prefix and JSON syntax).",
        f"Assistant message records: {totals['assistant_message_records']:,}, {totals['assistant_message_bytes']:,} bytes; final_answer {totals['assistant_final_answer_bytes']:,}, commentary {totals['assistant_commentary_bytes']:,}, other/missing phase {totals['assistant_other_or_missing_phase_bytes']:,} bytes.",
        "",
        "## Remaining classification gap",
        "",
        f"The existing `event_msg_profile_v1` report has {nested_event_gaps['user_message_occurrences']:,} `item_completed/UserMessage` and {nested_event_gaps['agent_message_occurrences']:,} `item_completed/AgentMessage` occurrences, but it has no byte totals by nested item type. The requested exclusive split between processing events and registration duplicates, and therefore final A1/A2/A3 ratios, cannot be computed from the authorized reports alone.",
        "",
        "No `event_msg` source ranges were opened. The next smallest sufficient source pass is a projection over indexed `event_msg` ranges to identify payload.type and, only for item_completed, payload.item.type; retain only file ID, subtype labels and source_record_bytes. This is a separate source-read scope and requires researcher approval.",
        "",
        "`response_item_by_file.csv` contains per-file role/phase/processing aggregates. `response_item_type_role_phase.csv` contains the type/role/phase byte breakdown. Neither file contains message text, images, source paths or record IDs.",
        "",
    ]
    (OUT / "report.md").write_text("\n".join(report), encoding="utf-8", newline="\n")
    print(json.dumps({
        "status": summary["status"], "response_item_records": scan_records,
        "response_item_bytes": scan_bytes, "user_role_bytes": totals["user_role_message_bytes"],
        "assistant_bytes": totals["assistant_message_bytes"],
        "embedded_image_base64_bytes": image_payload_bytes,
        "elapsed_seconds": elapsed, "peak_working_set_bytes": peak,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
