"""Audit one copied rollout for explicit model-request capture, without content export."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
import re
import tempfile
import time

from analyze_output_token_components import WORKFLOW
from analyze_session_storage_tokens import HashingReader
from index_session_records import ROOT, ScanStats, iter_record_locators, safe_source_path, lower_priority, utc_now
from profile_compacted import CHUNK
from profile_response_items import peak_memory, SAFE_ENUM
from repair_null_record_types import save
from token_usage_stream import TokenReader

OUT = ROOT / 'data/reports/model_request_capture_audit_v1'
VERSION = '1.0.0'
LIMIT = 20000
REQUEST_KEYS = {'request', 'model_request', 'api_request', 'http_request', 'request_body',
    'request_payload', 'raw_request', 'raw_body', 'body', 'prompt', 'messages', 'input',
    'model', 'tools', 'dynamic_tools', 'tool_choice', 'instructions', 'system_prompt',
    'developer_instructions', 'base_instructions', 'reasoning', 'text', 'temperature',
    'top_p', 'max_output_tokens', 'max_tokens', 'stream', 'store', 'include',
    'parallel_tool_calls', 'truncation', 'response_id', 'request_id', 'previous_response_id',
    'endpoint', 'url', 'method', 'headers', 'transport', 'payload_json'}
OPAQUE_REQUEST_KEYS = {'request', 'model_request', 'api_request', 'http_request',
    'request_body', 'request_payload', 'raw_request', 'raw_body', 'body', 'payload_json'}
SAFE_KEY = re.compile(r'[A-Za-z_][A-Za-z0-9_.:-]{0,79}\Z')
DYNAMIC_PARENTS = {'properties', 'changes', 'statuses', 'environment_variables', 'env',
                   'headers', 'metadata', 'structuredContent'}
ENUM_PATHS = {'/type', '/payload/type', '/payload/role', '/payload/item/type',
    '/payload/content/[]/type', '/payload/output/[]/type', '/payload/item/content/[]/type'}


def normalize(path):
    result = []; dynamic = False
    for part in path.split('/')[1:]:
        if part.isdigit(): result.append('[]'); continue
        result.append('{dynamic_key}' if dynamic or not SAFE_KEY.fullmatch(part) else part)
        dynamic = part in DYNAMIC_PARENTS
    return '/' + '/'.join(result) if result else ''


def content_context(path):
    parts = set(path.split('/'))
    return bool(parts & {'content', 'output', 'arguments', 'replacement_history',
                        'guardian_history', 'structuredContent', 'summary'})


def bundle_candidate(keys):
    has_input = any(keys.get(k) in ('array', 'string', 'object') for k in ('input', 'messages', 'prompt'))
    return has_input and ('model' in keys or 'tools' in keys or 'tool_choice' in keys or 'previous_response_id' in keys)


def classify_opaque(item):
    if item['record_type'] == 'world_state' and item['path'] == '/payload/state/host_skills/body':
        return 'instruction_state_body_by_structural_path_not_request_capture'
    if item['inside_content_or_history']:
        return 'inside_recorded_content_or_history'
    return 'unresolved_request_named_string'


class RequestShapeReader(TokenReader):
    def parse(self, source, size, chunk=CHUNK):
        self.fields = {}; self.objects = []; self.enums = {}; self.object_frames = []
        self.candidates = []; self.opaque_requests = []
        return super().parse(source, size, chunk)

    def value(self, path, depth):
        self.stream.whitespace(); start = self.stream.position
        is_object = self.stream.peek() == 123
        if is_object:
            self.object_frames.append({'path': path, 'keys': {}, 'start': start})
        kind, value = super().value(path, depth)
        norm = normalize(path); length = self.stream.position - start
        if norm not in self.fields and len(self.fields) >= LIMIT:
            raise ValueError('field_shape_bound')
        info = self.fields.setdefault(norm, {'occurrences': 0, 'types': Counter(), 'serialized_value_bytes': 0})
        info['occurrences'] += 1; info['types'][kind] += 1; info['serialized_value_bytes'] += length
        if norm in ENUM_PATHS and isinstance(value, str) and SAFE_ENUM.fullmatch(value):
            self.enums.setdefault(norm, Counter())[value] += 1
        leaf = path.rsplit('/', 1)[-1]
        if leaf in OPAQUE_REQUEST_KEYS and kind == 'string':
            self.opaque_requests.append({'path': norm, 'relative_offset': start, 'bytes': length,
                'inside_content_or_history': content_context(path)})
        if is_object:
            frame = self.object_frames.pop()
            if bundle_candidate(frame['keys']):
                if len(self.candidates) >= LIMIT: raise ValueError('request_candidate_bound')
                self.candidates.append({'path': norm, 'relative_offset': start, 'bytes': length,
                    'keys': {k: v for k, v in frame['keys'].items() if k in REQUEST_KEYS or k in ('type', 'turn_id')},
                    'inside_content_or_history': content_context(path)})
            if norm in ('', '/payload', '/payload/item'):
                self.objects.append({'path': norm, 'keys': frame['keys']})
        if self.object_frames and path.rsplit('/', 1)[0] == self.object_frames[-1]['path']:
            frame = self.object_frames[-1]
            key = '{dynamic_key}' if frame['path'].rsplit('/', 1)[-1] in DYNAMIC_PARENTS or not SAFE_KEY.fullmatch(leaf) else leaf
            frame['keys'][key] = kind
        return kind, value


def scan(relative):
    source_path = safe_source_path(relative); before = source_path.stat()
    if before.st_size > 16 * 1048576: raise ValueError('source_exceeds_16_mib_audit_bound')
    stats = ScanStats(); aggregate = {}; types = Counter(); subtypes = Counter(); roles = Counter()
    shapes = Counter(); candidates = []; opaque = []; links = Counter(); records = []; enum_totals = {}
    with tempfile.TemporaryDirectory(dir=ROOT / '.tmp', prefix='request-shapes-') as tmp:
        reader = RequestShapeReader(Path(tmp) / 'scratch.sqlite3')
        try:
            with source_path.open('rb') as lines, source_path.open('rb') as source:
                hashed = HashingReader(lines)
                for loc in iter_record_locators(hashed, stats, chunk_bytes=CHUNK, header_limit=256):
                    if len(records) >= LIMIT: raise ValueError('record_count_bound')
                    source.seek(loc.byte_offset); reader.parse(source, loc.record_bytes)
                    typ = reader.root_type if isinstance(reader.root_type, str) and SAFE_ENUM.fullmatch(reader.root_type) else '[unknown]'
                    subtype_values = reader.enums.get('/payload/type', {})
                    sub = next(iter(subtype_values)) if len(subtype_values) == 1 else None
                    types[typ] += 1; subtypes[(typ, sub)] += 1
                    if typ == 'response_item': roles.update(reader.enums.get('/payload/role', {}))
                    payload_keys = next((x['keys'] for x in reader.objects if x['path'] == '/payload'), {})
                    envelope_keys = next((x['keys'] for x in reader.objects if x['path'] == ''), {})
                    shapes[(typ, sub, json.dumps(payload_keys, sort_keys=True))] += 1
                    records.append({'line': loc.line_number, 'offset': loc.byte_offset, 'bytes': loc.record_bytes,
                        'record_type': typ, 'subtype': sub, 'root_keys': envelope_keys})
                    for path, entry in reader.fields.items():
                        key = (typ, path)
                        if key not in aggregate and len(aggregate) >= LIMIT: raise ValueError('aggregate_shape_bound')
                        dest = aggregate.setdefault(key, {'record_count': 0, 'occurrences': 0, 'types': Counter(),
                            'serialized_value_bytes': 0, 'example_lines': []})
                        dest['record_count'] += 1; dest['occurrences'] += entry['occurrences']
                        dest['types'].update(entry['types']); dest['serialized_value_bytes'] += entry['serialized_value_bytes']
                        if len(dest['example_lines']) < 3: dest['example_lines'].append(loc.line_number)
                        if path.rsplit('/', 1)[-1] in ('request_id','response_id','previous_response_id'):
                            links[(typ, path)] += 1
                    for path, counts in reader.enums.items():
                        enum_totals.setdefault((typ, path), Counter()).update(counts)
                    for src, dest in ((reader.candidates, candidates), (reader.opaque_requests, opaque)):
                        for item in src:
                            if len(dest) >= LIMIT: raise ValueError('candidate_evidence_bound')
                            dest.append({**item, 'line': loc.line_number, 'record_type': typ, 'subtype': sub,
                                'offset': loc.byte_offset + item['relative_offset']})
                digest = hashed.digest.hexdigest()
        finally: reader.close()
    after = source_path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns): raise ValueError('source_changed')
    if hashed.bytes != before.st_size or sum(r['bytes'] for r in records) + stats.blank_line_bytes != before.st_size:
        raise ValueError('source_byte_coverage_mismatch')
    fields = [{'record_type': typ, 'path': path, **value} for (typ, path), value in sorted(aggregate.items())]
    eligible = [c for c in candidates if not c['inside_content_or_history'] and c['subtype'] not in ('function_call','custom_tool_call')]
    for item in opaque: item['classification'] = classify_opaque(item)
    outside_opaque = [c for c in opaque if c['classification'] == 'unresolved_request_named_string']
    watched = ('request', 'model_request', 'http_request', 'api_request', 'request_body',
        'request_payload', 'raw_request', 'messages', 'tools', 'tool_choice', 'request_id',
        'previous_response_id', 'endpoint')
    marker_counts = {name: sum(f['occurrences'] for f in fields if f['path'].rsplit('/',1)[-1] == name) for name in watched}
    input_shapes = [f for f in fields if f['path'] == '/payload/input']
    return {'version': VERSION, 'source_relative_path': relative, 'source_bytes': before.st_size,
        'source_sha256': digest, 'source_mtime_ns': before.st_mtime_ns,
        'scan_complete': True, 'records': len(records), 'blank_line_bytes': stats.blank_line_bytes,
        'parse_failures': 0, 'truncated_records': 0, 'record_types': dict(types),
        'record_subtypes': [{'record_type': typ, 'subtype': sub, 'count': n} for (typ,sub),n in subtypes.items()],
        'response_item_roles': dict(roles),
        'payload_shapes': [{'record_type': typ, 'subtype': sub, 'keys': json.loads(keys), 'count': n} for (typ,sub,keys),n in shapes.items()],
        'field_shapes': fields,
        'structural_marker_fields': [f for f in fields if f['path'].rsplit('/',1)[-1] in REQUEST_KEYS],
        'request_like_bundles': candidates, 'request_like_bundles_outside_content': eligible,
        'opaque_request_named_strings': opaque,
        'unresolved_opaque_request_fields': len(outside_opaque),
        'watched_request_key_occurrences': marker_counts,
        'direct_payload_input_shapes': input_shapes,
        'identifier_field_presence': [{'record_type': typ, 'path': path, 'records': n} for (typ,path),n in sorted(links.items())],
        'enums': [{'record_type': typ, 'path': path, 'values': dict(c)} for (typ,path),c in sorted(enum_totals.items())],
        'record_index': records,
        'verdict': 'request_candidates_require_review' if eligible or outside_opaque else 'no_explicit_full_request_capture_found',
        'limits': ['structural_audit_not_a_claim_about_server_side_context',
            'strings_not_interpreted_as_embedded_json_http_or_code',
            'encrypted_or_encoded_content_not_decoded',
            'co_located_input_and_configuration_is_candidate_not_proof_of_complete_sent_request'],
        'database_accessed': False, 'original_files_modified': False, 'conversation_content_exported': False}


def markdown(report):
    lines = ['# Model request capture — structural audit', '',
        f"Source: `{report['source_relative_path']}`", f"SHA-256: `{report['source_sha256']}`", '',
        f"Status: **{report['verdict']}**. Fully parsed {report['records']:,} records / {report['source_bytes']:,} bytes.",
        f"Parse failures: {report['parse_failures']}; truncated records: {report['truncated_records']}.", '',
        '## Conclusion', '',
        ('No explicit complete model-request capture was identified in this file. The observed record schemas describe conversation items, events, context/state, usage and compaction history. They do not establish the exact ordered input and final configuration sent for each response.'
         if report['verdict'] == 'no_explicit_full_request_capture_found' else
         'Candidate request structures or unresolved opaque request fields require inspection before deciding whether complete sent requests were captured.'),
        'This conclusion concerns explicit structured evidence in this one file. It does not claim that the file contains only metadata, or that every recorded context element was sent in every invocation.', '',
        '## Recorded structures', '', '| Record type | Records |', '|---|---:|']
    for name, count in report['record_types'].items(): lines.append(f'| `{name}` | {count:,} |')
    lines += ['', '## Request evidence', '',
        f"- Structured request-like bundles outside recorded content: {len(report['request_like_bundles_outside_content'])}.",
        f"- All structured request-like candidates, including recorded/tool content: {len(report['request_like_bundles'])}.",
        f"- Opaque request-named strings: {len(report['opaque_request_named_strings'])}.",
        f"- Unresolved opaque request fields after structural classification: {report['unresolved_opaque_request_fields']}.",
        '- Bundles are tested structurally for input/messages/prompt plus model/tools/tool_choice/previous_response_id. Matches are candidates, not proof of a sent complete request.',
        '- All JSON key/type shapes, identifier-presence locations and per-record locators are retained in summary.json. No message, instruction, tool argument/result, image, URL or identifier value is exported.', '',
        '## Payload schemas', '', '| Record type / subtype | Count | Direct payload fields and types |', '|---|---:|---|']
    for shape in report['payload_shapes']:
        fields = ', '.join(f"`{k}`: {v}" for k,v in sorted(shape['keys'].items()))
        lines.append(f"| `{shape['record_type']}` / `{shape['subtype'] or ''}` | {shape['count']} | {fields} |")
    lines += ['', '## Field-name ambiguity review', '',
        '| Location | Records / occurrences | Interpretation |', '|---|---:|---|']
    for item in report['opaque_request_named_strings']:
        lines.append(f"| `{item['record_type']}{item['path']}` (line {item['line']}) | 1 | `{item['classification']}` |")
    for item in report['direct_payload_input_shapes']:
        lines.append(f"| `{item['record_type']}{item['path']}` | {item['record_count']} | Value types: `{json.dumps(item['types'], sort_keys=True)}`; inspect subtype schema above, not an automatic request-input label. |")
    lines += ['', '## Watched request keys', '', '| Key | Structural occurrences |', '|---|---:|']
    for key, count in report['watched_request_key_occurrences'].items(): lines.append(f'| `{key}` | {count} |')
    lines += ['', '## Interpretation limits', '',
        'A message history, tool call/result, configuration snapshot or compaction history does not establish the exact input sent for a particular response. A response identifier on a usage record establishes accounting identity, not a saved request body.',
        'The audit does not execute or recursively parse JSON/code/HTTP text inside strings, decode encoded images, or decrypt opaque content. It cannot prove what a server internally received or added. Its conclusion is limited to explicit capture in the inspected structured log.', '',
        '## Resources', '',
        f"256 KiB source chunks; two sequential source passes; peak Python working set {report['resources']['peak_working_set_bytes']/1048576:.2f} MiB; elapsed {report['resources']['elapsed_seconds']:.3f} seconds.",
        'Local read-only source analysis. No PostgreSQL access, external calls or source changes.', '']
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument('--source', default=WORKFLOW)
    args = parser.parse_args(); lower_priority(); started = time.monotonic()
    (ROOT / '.tmp').mkdir(exist_ok=True); OUT.mkdir(parents=True, exist_ok=True)
    result = scan(args.source); result['completed_at'] = utc_now()
    result['resources'] = {'peak_working_set_bytes': peak_memory(), 'elapsed_seconds': round(time.monotonic()-started,3),
        'chunk_bytes': CHUNK, 'source_passes': 2,
        'source_bytes_read': 2*result['source_bytes'] - result['blank_line_bytes']}
    save(OUT / 'summary.json', result); (OUT / 'report.md').write_text(markdown(result), encoding='utf-8')
    print(json.dumps({k: result[k] for k in ('verdict','records','source_bytes','record_types','resources')},indent=2))
    print(json.dumps({'candidate_bundles': len(result['request_like_bundles']),
        'outside_content': len(result['request_like_bundles_outside_content']),
        'opaque_request_fields': len(result['opaque_request_named_strings'])}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
