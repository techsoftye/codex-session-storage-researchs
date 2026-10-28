"""Pretty-format every derived JSON sample without loading JSON values.

The formatter is lexical and streaming: it preserves every non-whitespace byte,
including duplicate object keys and numeric spellings, while replacing only
JSON whitespace outside strings. Each file is replaced atomically.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import json
import os
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SAMPLES = ROOT / "samples"
CHUNK_BYTES = 1024 * 1024
OUTPUT_BUFFER_BYTES = 1024 * 1024
JSON_WHITESPACE = {0x20, 0x09, 0x0A, 0x0D}
OPEN_TO_CLOSE = {0x7B: 0x7D, 0x5B: 0x5D}  # { -> }, [ -> ]


def project_samples_path(value: str) -> Path:
    path = (ROOT / value).resolve() if not Path(value).is_absolute() else Path(value).resolve()
    samples_root = DEFAULT_SAMPLES.resolve()
    if path != samples_root and not path.is_relative_to(samples_root):
        raise argparse.ArgumentTypeError("path must be the samples directory or one of its subdirectories")
    if not path.is_dir():
        raise argparse.ArgumentTypeError("sample directory does not exist")
    return path


def format_file(path: Path, indent_width: int) -> dict:
    temporary = path.with_name(f".{path.name}.formatting")
    temporary.unlink(missing_ok=True)
    stack: list[int] = []
    in_string = False
    escaped = False
    last_token: int | None = None
    output_buffer = bytearray()
    output_bytes = 0
    current_line_bytes = 0
    maximum_line_bytes = 0

    def emit(destination, data: bytes) -> None:
        nonlocal output_bytes, current_line_bytes, maximum_line_bytes
        output_buffer.extend(data)
        output_bytes += len(data)
        if b"\n" in data:
            parts = data.split(b"\n")
            maximum_line_bytes = max(maximum_line_bytes, current_line_bytes + len(parts[0]))
            if len(parts) > 2:
                maximum_line_bytes = max(maximum_line_bytes, *(len(part) for part in parts[1:-1]))
            current_line_bytes = len(parts[-1])
        else:
            current_line_bytes += len(data)
        if len(output_buffer) >= OUTPUT_BUFFER_BYTES:
            destination.write(output_buffer)
            output_buffer.clear()

    def newline_and_indent(destination) -> None:
        emit(destination, b"\n" + (b" " * (indent_width * len(stack))))

    try:
        with path.open("rb") as source, temporary.open("xb") as destination:
            while chunk := source.read(CHUNK_BYTES):
                for byte in chunk:
                    if in_string:
                        emit(destination, bytes((byte,)))
                        if escaped:
                            escaped = False
                        elif byte == 0x5C:  # backslash
                            escaped = True
                        elif byte == 0x22:  # quote
                            in_string = False
                            last_token = byte
                        continue

                    if byte in JSON_WHITESPACE:
                        continue
                    if byte == 0x22:
                        emit(destination, b'"')
                        in_string = True
                        escaped = False
                        last_token = byte
                    elif byte in OPEN_TO_CLOSE:
                        emit(destination, bytes((byte,)))
                        stack.append(byte)
                        last_token = byte
                    elif byte in (0x7D, 0x5D):  # } or ]
                        if not stack or OPEN_TO_CLOSE[stack[-1]] != byte:
                            raise ValueError("mismatched JSON closing delimiter")
                        opening = stack.pop()
                        if last_token != opening:
                            newline_and_indent(destination)
                        emit(destination, bytes((byte,)))
                        last_token = byte
                    elif byte == 0x2C:  # comma
                        emit(destination, b",")
                        newline_and_indent(destination)
                        last_token = byte
                    elif byte == 0x3A:  # colon
                        emit(destination, b": ")
                        last_token = byte
                    else:
                        emit(destination, bytes((byte,)))
                        last_token = byte

            if in_string or escaped:
                raise ValueError("unterminated JSON string")
            if stack:
                raise ValueError("unterminated JSON object or array")
            if last_token is None:
                raise ValueError("empty JSON sample")
            emit(destination, b"\n")
            if output_buffer:
                destination.write(output_buffer)
                output_buffer.clear()
            destination.flush()
            os.fsync(destination.fileno())
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise

    return {
        "output_bytes": output_bytes,
        "maximum_formatted_line_bytes": maximum_line_bytes,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Pretty-format derived JSON samples recursively.")
    parser.add_argument("--samples", type=project_samples_path, default=DEFAULT_SAMPLES)
    parser.add_argument("--indent", type=int, choices=range(1, 9), default=2)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    files = sorted(path for path in args.samples.rglob("*.json") if path.is_file())
    if not files:
        parser.error("no JSON samples were found")
    original_sizes = {path: path.stat().st_size for path in files}
    if not args.apply:
        print(json.dumps({
            "sample_files": len(files),
            "input_bytes": sum(original_sizes.values()),
            "database_written": False,
            "files_written": False,
        }, indent=2))
        return 0

    by_type = defaultdict(lambda: {"files": 0, "input_bytes": 0, "output_bytes": 0})
    maximum_line = 0
    for path in files:
        result = format_file(path, args.indent)
        category = path.parent.name
        by_type[category]["files"] += 1
        by_type[category]["input_bytes"] += original_sizes[path]
        by_type[category]["output_bytes"] += result["output_bytes"]
        maximum_line = max(maximum_line, result["maximum_formatted_line_bytes"])

    print(json.dumps({
        "sample_files": len(files),
        "input_bytes": sum(original_sizes.values()),
        "output_bytes": sum(item["output_bytes"] for item in by_type.values()),
        "maximum_formatted_line_bytes": maximum_line,
        "indent_spaces": args.indent,
        "files_written": True,
        "by_record_type": dict(sorted(by_type.items())),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
