"""Extract one indexed JSONL record into an independent JSON sample.

The database is used only to locate the source file and byte range. The source
file is opened read-only and copied in bounded chunks; it is never loaded into
memory as a whole.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath

from index_session_records import json_rows


ROOT = Path(__file__).resolve().parents[1]
DATASET_ROOT = (ROOT / "2026").resolve()
DEFAULT_OUTPUT_DIR = ROOT / "samples"
COPY_CHUNK_BYTES = 1024 * 1024


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


def record_details(record_id: int) -> dict:
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
WHERE rr.id = {record_id};
""")
    if len(rows) != 1:
        raise RuntimeError(f"Record id {record_id} was not found")
    return rows[0]


def source_path(relative_path: str) -> Path:
    relative = PurePosixPath(relative_path)
    if relative.is_absolute() or not relative.parts or relative.parts[0] != "2026":
        raise RuntimeError("Database source path is outside the 2026 dataset")
    path = (ROOT.joinpath(*relative.parts)).resolve()
    if not path.is_relative_to(DATASET_ROOT) or not path.is_file():
        raise RuntimeError("Indexed source file is missing or outside the dataset")
    return path


def payload_length(handle, offset: int, record_bytes: int) -> int:
    tail_size = min(2, record_bytes)
    handle.seek(offset + record_bytes - tail_size)
    tail = handle.read(tail_size)
    if tail.endswith(b"\r\n"):
        return record_bytes - 2
    if tail.endswith((b"\n", b"\r")):
        return record_bytes - 1
    return record_bytes


def extract(details: dict, output_dir: Path, overwrite: bool) -> dict:
    path = source_path(str(details["source_relative_path"]))
    expected_file_size = int(details["file_size_bytes"])
    actual_file_size = path.stat().st_size
    if actual_file_size != expected_file_size:
        raise RuntimeError(
            f"Source file size changed: database={expected_file_size}, actual={actual_file_size}"
        )

    offset = int(details["source_byte_offset"])
    record_bytes = int(details["source_record_bytes"])
    if offset < 0 or record_bytes <= 0 or offset + record_bytes > actual_file_size:
        raise RuntimeError("Indexed byte range is outside the source file")

    output_dir = output_dir.resolve()
    if not output_dir.is_relative_to(ROOT):
        raise RuntimeError("Output directory must be inside the research project")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{details['record_id']}.json"
    temporary_path = output_dir / f".{details['record_id']}.json.part"
    if output_path.exists() and not overwrite:
        raise RuntimeError(f"Output already exists: {output_path}")

    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            json_bytes = payload_length(source, offset, record_bytes)
            if json_bytes <= 0:
                raise RuntimeError("Indexed record contains no JSON bytes")
            source.seek(offset)
            remaining = json_bytes
            with temporary_path.open("xb") as destination:
                while remaining:
                    chunk = source.read(min(COPY_CHUNK_BYTES, remaining))
                    if not chunk:
                        raise RuntimeError("Source file ended before the indexed record range")
                    destination.write(chunk)
                    digest.update(chunk)
                    remaining -= len(chunk)
                destination.flush()
                os.fsync(destination.fileno())

        # A bounded envelope check catches a wrong offset without decoding a
        # potentially very large payload into memory.
        with temporary_path.open("rb") as sample:
            first = sample.read(4096).lstrip()[:1]
            sample.seek(max(0, json_bytes - 4096))
            last = sample.read().rstrip()[-1:]
        if first != b"{" or last != b"}":
            raise RuntimeError("Extracted range does not have a JSON object envelope")

        os.replace(temporary_path, output_path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise

    return {
        "record_id": int(details["record_id"]),
        "research_session_id": int(details["research_session_id"]),
        "record_type": details["record_type"],
        "source_line_number": int(details["source_line_number"]),
        "source_byte_offset": offset,
        "source_record_bytes": record_bytes,
        "json_bytes_written": json_bytes,
        "sha256": digest.hexdigest(),
        "source_relative_path": details["source_relative_path"],
        "output_path": str(output_path),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract one database-indexed JSONL record as a standalone JSON file."
    )
    parser.add_argument("--record-id", required=True, type=positive_int)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    try:
        result = extract(record_details(args.record_id), args.output_dir, args.overwrite)
    except (OSError, ValueError, RuntimeError) as error:
        parser.error(str(error))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
