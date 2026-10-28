"""Stream indexed compacted records into aggregate structural statistics only.

No conversation values, identifiers, source mutations or database writes.
JSON strings are consumed in bounded chunks; no complete history is materialized.
"""
from __future__ import annotations

import argparse
import codecs
import csv
import itertools
import json
import re
import time

from index_session_records import ROOT, json_rows, lower_priority, safe_source_path, utc_now
from profile_response_items import export_locators, peak_memory, SAFE_KEY, SAFE_ENUM
from repair_null_record_types import save

VERSION = "1.1.0"
OUT = ROOT / "data/reports/compacted_profile_v1"
CHUNK = 256 * 1024
MAX_SHAPES = 20000
MAX_DEPTH = 128
SPECIAL = re.compile(rb'["\\\x00-\x1f]')
SPACE = re.compile(rb'[ \t\r\n]*')
NUMBER = re.compile(rb'-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\Z')
HISTORIES = ("replacement_history", "guardian_history")
HISTORY_ITEM_PATHS = {f"payload.{h}[]" for h in HISTORIES}
ROOT_KEYS = {"timestamp", "ordinal", "type", "payload"}
ENUM_PATHS = {f"payload.{h}[]{p}" for h in HISTORIES for p in
              (".type", ".role", ".content[].type", ".output[].type", ".summary[].type")}


def measure(dest, value):
    dest["count"] = dest.get("count", 0) + 1
    dest["sum"] = dest.get("sum", 0) + value
    dest["min"] = min(dest.get("min", value), value)
    dest["max"] = max(dest.get("max", value), value)


def increment(dest, name, count=1):
    dest[name] = dest.get(name, 0) + count


class Stream:
    def __init__(self, source, size, chunk=CHUNK):
        self.source, self.remaining, self.chunk = source, size, chunk
        self.buffer, self.index, self.base = b"", 0, 0
        self.decoder = codecs.getincrementaldecoder("utf-8")("strict")

    @property
    def position(self):
        return self.base + self.index

    def ready(self):
        if self.index == len(self.buffer) and self.remaining:
            self.base += self.index
            self.buffer = self.source.read(min(self.chunk, self.remaining))
            self.index = 0
            if not self.buffer:
                raise ValueError("incomplete_byte_range")
            self.remaining -= len(self.buffer)
            self.decoder.decode(self.buffer, final=self.remaining == 0)
        return self.index < len(self.buffer)

    def peek(self):
        return self.buffer[self.index] if self.ready() else None

    def take(self):
        value = self.peek()
        if value is None:
            raise ValueError("unexpected_end")
        self.index += 1
        return value

    def expect(self, value):
        if self.take() != value:
            raise ValueError("unexpected_delimiter")

    def whitespace(self):
        while self.ready():
            self.index = SPACE.match(self.buffer, self.index).end()
            if self.index < len(self.buffer):
                return

    def string(self, observer=None):
        self.expect(34)
        start = self.position
        prefix = bytearray()
        while self.ready():
            match = SPECIAL.search(self.buffer, self.index)
            end = match.start() if match else len(self.buffer)
            if observer is not None and end > self.index:
                observer.feed(self.buffer[self.index:end])
            if len(prefix) < 256:
                prefix.extend(self.buffer[self.index:min(end, self.index + 256-len(prefix))])
            self.index = end
            if match is None:
                continue
            char = self.take()
            if char == 34:
                length = self.position - start - 1
                if observer is not None:
                    observer.finish(length + 2)
                small = json.loads(b'"'+prefix+b'"') if length <= 256 else None
                return small, bytes(prefix), length
            if char != 92:
                raise ValueError("unescaped_control_character")
            escaped = bytes((char, self.take()))
            if escaped[1] == 117:
                digits = bytes(self.take() for _ in range(4))
                if not re.fullmatch(rb'[0-9a-fA-F]{4}', digits):
                    raise ValueError("invalid_unicode_escape")
                escaped += digits
            elif escaped[1] not in b'"\\/bfnrt':
                raise ValueError("invalid_escape")
            if observer is not None:
                observer.feed(json.loads(b'"'+escaped+b'"').encode('utf-8', 'surrogatepass'))
            prefix.extend(escaped[:max(0, 256-len(prefix))])
        raise ValueError("unterminated_string")

    def primitive(self):
        token = bytearray()
        while self.peek() is not None and self.peek() not in b' \t\r\n,]}':
            token.append(self.take())
            if len(token) > 128:
                raise ValueError("oversized_primitive")
        raw = bytes(token)
        if raw == b'null':
            return "null", None
        if raw in (b'true', b'false'):
            return "boolean", raw == b'true'
        if not NUMBER.fullmatch(raw):
            raise ValueError("invalid_primitive")
        return "number", json.loads(raw)


def new_stats():
    return {"records":0, "source_bytes":0, "failed_records":0, "failures":[],
            "fields":{}, "enums":{}, "payload_fields":{}, "variants":{},
            "history_lengths":{}, "features":{}, "numeric_ranges":{},
            "history_item_sizes":{}}


class Profile:
    def __init__(self, source, size, chunk=CHUNK, string_observer_factory=None):
        self.stream = Stream(source, size, chunk)
        self.stats = new_stats()
        self.keys = {}
        self.root_type = None
        self.payload_kind = None
        self.features = set()
        self.active_history_item = None
        self.string_observer_factory = string_observer_factory

    def value(self, path="", depth=0):
        if depth > MAX_DEPTH:
            raise ValueError("depth_limit")
        stream = self.stream
        stream.whitespace()
        start = stream.position
        char = stream.peek()
        value = None
        image_string = False
        if path in HISTORY_ITEM_PATHS:
            self.active_history_item = {"path":path, "type":"[missing_type]",
                                        "embedded_image_string_bytes":0,
                                        "encrypted_content_string_bytes":0}
        if char in (123,91):
            object_value = char == 123
            kind = "object" if object_value else "array"
            stream.take()
            stream.whitespace()
            close = 125 if object_value else 93
            seen, count = set(), 0
            if stream.peek() != close:
                while True:
                    if object_value:
                        key, _, key_bytes = stream.string()
                        if key is None or key_bytes > 256:
                            raise ValueError("oversized_object_key")
                        if key in seen:
                            raise ValueError("duplicate_object_key")
                        seen.add(key)
                        if len(seen) > MAX_SHAPES:
                            raise ValueError("object_key_limit")
                        stream.whitespace()
                        stream.expect(58)
                        # Dynamic maps can contain source paths or arbitrary text keys.
                        dynamic = path.endswith((".changes", ".statuses", ".arguments", ".properties", ".structuredContent"))
                        safe = key if SAFE_KEY.fullmatch(key) and not dynamic else "{dynamic_key}"
                        if not path:
                            safe = key if key in ROOT_KEYS else "{other_root_key}"
                        child = path + "." + safe if path else safe
                    else:
                        child = path + "[]"
                    child_kind, child_bytes = self.value(child, depth+1)
                    if path == "payload":
                        self.keys[safe] = child_kind
                        self.stats["payload_fields"][safe] = {
                            "records":1, "types":{child_kind:1}, "value_bytes":child_bytes}
                    count += 1
                    stream.whitespace()
                    delimiter = stream.take()
                    if delimiter == close:
                        break
                    if delimiter != 44:
                        raise ValueError("invalid_container_separator")
                    stream.whitespace()
            else:
                stream.take()
            if not object_value and path in {f"payload.{h}" for h in HISTORIES}:
                measure(self.stats["history_lengths"].setdefault(path, {}), count)
        elif char == 34:
            kind = "string"
            observer = self.string_observer_factory(path) if self.string_observer_factory else None
            value, prefix, length = stream.string(observer)
            if prefix.startswith((b'data:image/', b'data:audio/')) and b';base64,' in prefix[:120]:
                image_string = prefix.startswith(b'data:image/')
                self.features.add("embedded_base64_image" if prefix.startswith(b'data:image/') else "embedded_base64_audio")
                if path.startswith("payload."):
                    self.features.add(path.split(".")[1].split("[")[0] + ":embedded_base64")
            if length > 65536:
                self.features.add("string_over_65536_encoded_bytes")
        else:
            kind, value = stream.primitive()
        size = stream.position - start
        current_item = self.active_history_item
        if current_item is not None:
            if path == current_item['path'] + '.type':
                current_item['type'] = (value if isinstance(value,str) and SAFE_ENUM.fullmatch(value)
                                        else '[null_type]' if kind == 'null' else '[unrecognized_type]')
            if image_string:
                current_item['embedded_image_string_bytes'] += size
            if kind == 'string' and path.endswith('.encrypted_content'):
                current_item['encrypted_content_string_bytes'] += size
            if path == current_item['path']:
                history = path.removesuffix('[]')
                subtype = current_item['type'] if kind == 'object' else '[item_' + kind + ']'
                metrics = self.stats['history_item_sizes'].setdefault(history, {}).setdefault(subtype, {
                    'items':0, 'value_bytes':0, 'embedded_image_string_bytes':0,
                    'encrypted_content_string_bytes':0})
                metrics['items'] += 1
                metrics['value_bytes'] += size
                for key in ('embedded_image_string_bytes','encrypted_content_string_bytes'):
                    metrics[key] += current_item[key]
                self.active_history_item = None
        if path == "type":
            self.root_type = value
        if path == "payload":
            self.payload_kind = kind
        if path.startswith("payload"):
            shape = path + " : " + kind
            if shape not in self.stats["fields"] and len(self.stats["fields"]) >= MAX_SHAPES:
                raise ValueError("field_shape_limit")
            field = self.stats["fields"].setdefault(shape, {"records":1, "occurrences":0})
            field["occurrences"] += 1
            if path in ENUM_PATHS:
                name = (value if isinstance(value,str) and SAFE_ENUM.fullmatch(value) else
                        "[null]" if kind == "null" else "[unrecognized_value]")
                increment(self.stats["enums"].setdefault(path, {}), name)
            if kind == "number" and (path == "payload.window_number" or
                    path.startswith("payload.latest_token_usage_record.") and path.endswith("_tokens")):
                measure(self.stats["numeric_ranges"].setdefault(path, {}), value)
        return kind, size

    def run(self, size):
        root_kind, _ = self.value()
        self.stream.whitespace()
        if self.stream.peek() is not None or self.stream.position != size:
            raise ValueError("trailing_content_or_incomplete_range")
        if root_kind != "object" or self.root_type != "compacted" or self.payload_kind != "object":
            raise ValueError("unexpected_record_envelope")
        self.stats["records"], self.stats["source_bytes"] = 1, size
        variant = "; ".join(f"{key}:{value}" for key,value in sorted(self.keys.items()))
        self.stats["variants"][variant] = {"records":1,"source_bytes":size}
        self.stats["features"] = dict.fromkeys(self.features,1)
        return self.stats


def merge(target, source):
    for key in ("records", "source_bytes", "failed_records"):
        target[key] += source[key]
    target["failures"].extend(source["failures"])
    for history, types in source['history_item_sizes'].items():
        dest_history = target['history_item_sizes'].setdefault(history,{})
        for subtype, metrics in types.items():
            dest = dest_history.setdefault(subtype,{})
            for key,count in metrics.items():
                increment(dest,key,count)
    for category in ("fields", "enums", "variants"):
        for name, metrics in source[category].items():
            dest = target[category].setdefault(name, {})
            for key, count in metrics.items():
                increment(dest, key, count)
    for name, value in source["payload_fields"].items():
        dest = target["payload_fields"].setdefault(name, {"records":0,"types":{},"value_bytes":0})
        dest["records"] += value["records"]
        dest["value_bytes"] += value["value_bytes"]
        for kind, count in value["types"].items():
            increment(dest["types"], kind, count)
    for category in ("history_lengths", "numeric_ranges"):
        for name, value in source[category].items():
            dest = target[category].setdefault(name, {"count":0,"sum":0,"min":value["min"],"max":value["max"]})
            dest["count"] += value["count"]
            dest["sum"] += value["sum"]
            dest["min"], dest["max"] = min(dest["min"],value["min"]), max(dest["max"],value["max"])
    for name, count in source["features"].items():
        increment(target["features"],name,count)
    if len(target["fields"]) > MAX_SHAPES:
        raise ValueError("aggregate_field_shape_limit")


def markdown(report):
    lines = ["# Compacted structural census", "", "Generated (UTC): " + report["completed_at"], "",
             f"Status: {report['status']}. Profiled {report['records']:,} physical compacted records in {report['files_profiled']} files.",
             f"Source bytes: {report['source_bytes']:,} ({report['source_bytes']/1024**3:.4f} GiB). Failed records: {report['failed_records']}.",
             f"Peak working set: {report['peak_working_set_bytes']/1024**2:.2f} MiB. Runtime: {report['elapsed_seconds']:.3f} seconds.", "",
             "## Method and scope", "",
             "Exact byte ranges come from a read-only PostgreSQL query. A single-process streaming parser reads 256 KiB chunks at below-normal priority. Strings and histories are never materialized in full. Per-file checkpoints allow restart. Original files and database rows are unchanged.", "",
             "Counts describe repeated physical histories, not distinct conversations or messages. History overlap and semantic deduplication were not measured. Block bytes include each complete serialized JSON value and its nested keys. Top-level payload keys, separators, the outer envelope and line endings are accounted for separately.", "",
             "## Payload block sizes", "", "| Payload field | Records present | JSON types | Value bytes | GiB |", "|---|---:|---|---:|---:|"]
    for name, value in sorted(report['payload_fields'].items(),key=lambda p:-p[1]['value_bytes']):
        lines.append(f"| `{name}` | {value['records']:,} | {', '.join(sorted(value['types']))} | {value['value_bytes']:,} | {value['value_bytes']/1024**3:.6f} |")
    lines += [f"| Other record bytes | | | {report['other_record_bytes']:,} | {report['other_record_bytes']/1024**3:.6f} |", "",
              "## History lengths", "", "| Array | Records with array | Total items | Minimum per record | Maximum per record |", "|---|---:|---:|---:|---:|"]
    for name,v in report['history_lengths'].items():
        lines.append(f"| `{name}` | {v['count']:,} | {v['sum']:,} | {v['min']:,} | {v['max']:,} |")
    for history, types in report['history_item_sizes'].items():
        history_bytes = report['payload_fields'][history.split('.')[1]]['value_bytes']
        item_bytes = sum(v['value_bytes'] for v in types.values())
        lines += ['', '## Item sizes: ' + history, '',
                  '| Item type | Items | Serialized item bytes | GiB | Share of history bytes |',
                  '|---|---:|---:|---:|---:|']
        lines.extend(f"| `{name}` | {v['items']:,} | {v['value_bytes']:,} | {v['value_bytes']/1024**3:.6f} | {100*v['value_bytes']/history_bytes:.4f}% |"
                     for name,v in sorted(types.items(),key=lambda p:-p[1]['value_bytes']))
        lines += [f"| Array delimiters and inter-item whitespace | | {history_bytes-item_bytes:,} | {(history_bytes-item_bytes)/1024**3:.6f} | {100*(history_bytes-item_bytes)/history_bytes:.4f}% |",
                  f"| Total history | {sum(v['items'] for v in types.values()):,} | {history_bytes:,} | {history_bytes/1024**3:.6f} | 100.0000% |", '',
                  'Item bytes cover the exact original JSON value, including its nested fields and strings. Array brackets, commas and whitespace between elements are accounted for separately. No JSON reserialization or proportional size allocation is used.', '',
                  '| Item type | Embedded image data URL string bytes | Encrypted-content string bytes |',
                  '|---|---:|---:|']
        lines.extend(f"| `{name}` | {v['embedded_image_string_bytes']:,} | {v['encrypted_content_string_bytes']:,} |"
                     for name,v in sorted(types.items(),key=lambda p:-p[1]['value_bytes']))
        lines += ['', 'String-byte metrics are subsets of item bytes and must not be added to the item totals. They include JSON quotes/escaping and data URL prefixes. Images are not decoded, and encrypted content is not decrypted.']
    for path, values in sorted(report['enums'].items()):
        lines += ["", "## " + path, "", "| Value | Occurrences |", "|---|---:|"]
        lines.extend(f"| `{name}` | {count:,} |" for name,count in sorted(values.items(),key=lambda p:-p[1]))
    lines += ["", "## Structural variants", "", "| Payload keys and JSON types | Records | Source bytes |", "|---|---:|---:|"]
    lines.extend(f"| `{name}` | {v['records']:,} | {v['source_bytes']:,} |" for name,v in sorted(report['variants'].items(),key=lambda p:-p[1]['records']))
    lines += ["", "## Content features", "", "| Feature | Records |", "|---|---:|"]
    lines.extend(f"| `{name}` | {count:,} |" for name,count in sorted(report['features'].items()))
    lines += ["", "## Numeric snapshots", "", "Token counters are snapshots, including cumulative turn/thread counters. They are not summed as consumption. Ranges are observed values, not context-window occupancy measurements.", "",
              "| Field | Observations | Minimum | Maximum |", "|---|---:|---:|---:|"]
    lines.extend(f"| `{name}` | {v['count']:,} | {v['min']:,} | {v['max']:,} |" for name,v in sorted(report['numeric_ranges'].items()))
    lines += ["", "## Field shapes", "", "| Path and JSON type | Records | Occurrences |", "|---|---:|---:|"]
    lines.extend(f"| `{name}` | {v['records']:,} | {v['occurrences']:,} |" for name,v in sorted(report['fields'].items()))
    lines += ["", "## Reproduction", "", "`python -B scripts/profile_compacted.py --run`", "",
              "No source text, image data, tool arguments, or identifier values are exported. Short structural enums are allowlisted by path. Unrecognized/dynamic keys are normalized. Parse failures are reported by record ID and error class only.", ""]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--limit-files", type=int, help="Bounded pilot; results are saved separately")
    parser.add_argument("--output-dir", default=str(OUT), help="Report directory beneath data/reports")
    args = parser.parse_args()
    output_dir = (ROOT / args.output_dir).resolve()
    if not output_dir.is_relative_to((ROOT / 'data/reports').resolve()):
        parser.error('output-dir must be beneath data/reports')
    if args.limit_files is not None and args.limit_files < 1:
        parser.error("limit-files must be positive")
    priority = lower_priority()
    plan = json_rows("""SELECT json_build_object('records',count(*),'source_bytes',sum(source_record_bytes),
 'largest_record_bytes',max(source_record_bytes),'files',count(DISTINCT research_session_id))
 FROM public.research_session_records WHERE record_type='compacted';""")[0]
    print(json.dumps({"plan":plan,"priority":priority,"database_written":False}),flush=True)
    if not args.run:
        return 0
    output_dir.mkdir(parents=True,exist_ok=True)
    cache = output_dir / "checkpoints"
    cache.mkdir(exist_ok=True)
    started, clock = utc_now(), time.monotonic()
    paths = {r['id']:r for r in json_rows("""SELECT json_build_object('id',id,
 'path',source_relative_path,'size',file_size_bytes) FROM public.research_sessions;""")}
    locator_path = output_dir / "locators.csv"
    digest = export_locators(locator_path,"compacted")
    total, files, hits, read_bytes = new_stats(), 0, 0, 0
    last_progress = time.monotonic()
    with locator_path.open(newline='',encoding='utf-8') as locators:
        numbers = (tuple(map(int,row)) for row in csv.reader(locators))
        for session, rows in itertools.groupby(numbers,key=lambda r:r[0]):
            if args.limit_files and files >= args.limit_files:
                break
            entry = paths[session]
            source_path = safe_source_path(entry['path'])
            before = source_path.stat()
            if before.st_size != entry['size']:
                raise RuntimeError("Source size changed")
            signature = {"version":VERSION,"locator_sha256":digest,"path":entry['path'],
                         "size":before.st_size,"mtime_ns":before.st_mtime_ns}
            checkpoint = cache / f'{session}.json'
            cached = json.loads(checkpoint.read_text(encoding='utf-8')) if checkpoint.exists() else None
            if cached and cached['signature'] == signature and not cached['stats']['failed_records']:
                stats = cached['stats']
                for _ in rows:
                    pass
                hits += 1
            else:
                stats, previous_end = new_stats(), -1
                with source_path.open('rb') as source:
                    for _, record_id, offset, size in rows:
                        if offset < previous_end or offset < 0 or size <= 0 or offset+size > before.st_size:
                            raise RuntimeError("Invalid locator")
                        source.seek(offset)
                        try:
                            merge(stats,Profile(source,size).run(size))
                        except (ValueError,UnicodeError,RecursionError) as error:
                            stats['failed_records'] += 1
                            stats['failures'].append({'record_id':record_id,'error_class':type(error).__name__})
                        previous_end = offset+size
                        read_bytes += size
                        if time.monotonic()-last_progress >= 15:
                            print(json.dumps({'files_done':files,'files_total':plan['files'],
                                'records_done':total['records']+stats['records'],
                                'failures':total['failed_records']+stats['failed_records'],
                                'bytes_read':read_bytes,'peak_mib':round(peak_memory()/1024**2,2)}),flush=True)
                            last_progress = time.monotonic()
                after = source_path.stat()
                if (before.st_size,before.st_mtime_ns)!=(after.st_size,after.st_mtime_ns):
                    raise RuntimeError("Source changed during scan")
                save(checkpoint,{'signature':signature,'stats':stats})
            merge(total,stats)
            files += 1
    complete = total['records']==plan['records'] and total['source_bytes']==plan['source_bytes'] and not total['failed_records'] and files==plan['files']
    # Do not publish sums of repeated token snapshots as consumption metrics.
    for value in total['numeric_ranges'].values():
        value.pop('sum',None)
    other = total['source_bytes']-sum(v['value_bytes'] for v in total['payload_fields'].values())
    assert other >= 0
    for history, types in total['history_item_sizes'].items():
        assert sum(v['items'] for v in types.values()) == total['history_lengths'][history]['sum']
        assert sum(v['value_bytes'] for v in types.values()) <= total['payload_fields'][history.split('.')[1]]['value_bytes']
    report = {'version':VERSION,'started_at':started,'completed_at':utc_now(),
              'status':'completed' if complete else 'pilot' if args.limit_files else 'needs_review',
              'record_type':'compacted','database_written':False,'plan':plan,'files_profiled':files,
              'cache_hits':hits,'bytes_read_this_run':read_bytes,'locator_sha256':digest,
              'elapsed_seconds':round(time.monotonic()-clock,3),'peak_working_set_bytes':peak_memory(),
              'limits':{'read_buffer_bytes':CHUNK,'depth':MAX_DEPTH,'field_shapes':MAX_SHAPES},
              'other_record_bytes':other,**total}
    stem = 'pilot' if args.limit_files else 'summary'
    save(output_dir/f'{stem}.json',report)
    (output_dir/('pilot.md' if args.limit_files else 'report.md')).write_text(markdown(report),encoding='utf-8')
    print(json.dumps({k:report[k] for k in ('status','records','source_bytes','failed_records','files_profiled','cache_hits','elapsed_seconds','peak_working_set_bytes')},indent=2))
    return 0 if complete or args.limit_files and not total['failed_records'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
