"""Create lightweight, VS Code-friendly views of the derived JSON samples."""
from __future__ import annotations

import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = ROOT / "samples"
DEFAULT_OUTPUT = ROOT / "samples_readable"


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


def reject_constant(value: str):
    raise ValueError(f"non-standard JSON constant: {value}")


def summarize_long_strings(value, threshold: int, stats: dict):
    if isinstance(value, str):
        encoded = value.encode("utf-8")
        if len(encoded) <= threshold:
            return value
        stats["long_strings_replaced"] += 1
        stats["omitted_utf8_bytes"] += len(encoded)
        digest = hashlib.sha256(encoded).hexdigest()
        return (
            f"[LONG STRING OMITTED: characters={len(value)}, "
            f"utf8_bytes={len(encoded)}, sha256={digest}]"
        )
    if isinstance(value, list):
        return [summarize_long_strings(item, threshold, stats) for item in value]
    if isinstance(value, dict):
        return {key: summarize_long_strings(item, threshold, stats) for key, item in value.items()}
    return value


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create readable sample copies with very long strings replaced by metadata."
    )
    parser.add_argument("--max-string-bytes", type=positive_int, default=4096)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    output_root = args.output.resolve()
    if not output_root.is_relative_to(ROOT) or output_root == SOURCE_ROOT.resolve():
        parser.error("output must be a separate directory inside the research project")

    files = sorted(SOURCE_ROOT.rglob("*.json"))
    if not files:
        parser.error("no source samples were found")

    totals = defaultdict(int)
    output_root.mkdir(parents=True, exist_ok=True)
    for source in files:
        relative = source.relative_to(SOURCE_ROOT)
        destination = output_root / relative
        if destination.exists() and not args.overwrite:
            parser.error(f"output already exists: {destination}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(f".{destination.name}.part")
        temporary.unlink(missing_ok=True)
        try:
            with source.open("r", encoding="utf-8-sig") as handle:
                value = json.load(handle, parse_constant=reject_constant)
            file_stats = {"long_strings_replaced": 0, "omitted_utf8_bytes": 0}
            readable = summarize_long_strings(value, args.max_string_bytes, file_stats)
            with temporary.open("x", encoding="utf-8", newline="\n") as handle:
                json.dump(readable, handle, ensure_ascii=False, indent=2)
                handle.write("\n")
            temporary.replace(destination)
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
        totals["files_written"] += 1
        totals["long_strings_replaced"] += file_stats["long_strings_replaced"]
        totals["omitted_utf8_bytes"] += file_stats["omitted_utf8_bytes"]

    totals["output_bytes"] = sum(path.stat().st_size for path in output_root.rglob("*.json"))
    print(json.dumps({
        "source_directory": str(SOURCE_ROOT),
        "output_directory": str(output_root),
        "maximum_preserved_string_bytes": args.max_string_bytes,
        **totals,
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
