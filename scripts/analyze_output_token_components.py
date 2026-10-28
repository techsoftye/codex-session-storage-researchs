"""Inspect model-output components and their recorded usage, without content export.

Source-only, bounded streaming; no PostgreSQL, tokenizer, decryption or API calls.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import re
import tempfile
import time

from analyze_session_storage_tokens import EvidenceReader, HashingReader, counters, valid
from index_session_records import ROOT, ScanStats, iter_record_locators, safe_source_path, lower_priority, utc_now
from profile_compacted import CHUNK
from profile_response_items import SAFE_ENUM, peak_memory
from repair_null_record_types import save
from token_usage_stream import SAFE_ID
from survey_sessions import candidate_files

VERSION = '1.0.0'
OUT = ROOT / 'data/reports/output_token_components_v1'
FIRST = '2026/01/01/rollout-2026-01-01T00-00-01-00000000-0000-4000-8000-000000000001.jsonl'
WORKFLOW = '2026/01/01/rollout-2026-01-01T00-00-02-00000000-0000-4000-8000-000000000002.jsonl'
PASSTHROUGH = '/payload/internal_chat_message_metadata_passthrough/'
OUTPUT_TYPES = {'reasoning', 'function_call', 'custom_tool_call', 'web_search_call',
                'computer_call', 'image_generation_call', 'local_shell_call',
                'mcp_call', 'file_search_call', 'code_interpreter_call'}
TEXT_FIELDS = {'text', 'message', 'encrypted_content', 'arguments', 'input', 'output'}


class OutputReader(EvidenceReader):
    def parse(self, source, size, chunk=CHUNK):
        self.fields = {}; self.value_spans = []; self.numeric_output_fields = []
        super().parse(source, size, chunk)

    def value(self, path, depth):
        self.stream.whitespace(); start = self.stream.position
        kind, value = super().value(path, depth)
        leaf = path.rsplit('/', 1)[-1]
        if isinstance(value, str) and (leaf in {'type', 'role', 'channel', 'status', 'id', 'call_id'} or leaf.endswith('_id')):
            if depth <= 5 and (SAFE_ENUM.fullmatch(value) or SAFE_ID.fullmatch(value)):
                if len(self.fields) >= 2000: raise ValueError('identifier_shape_limit')
                self.fields[path] = value
        if kind == 'string' and (leaf in TEXT_FIELDS or path == '/payload/encrypted_content'):
            if len(self.value_spans) >= 20000: raise ValueError('component_span_limit')
            self.value_spans.append({'path': path, 'relative_offset': start,
                                     'serialized_bytes': self.stream.position - start})
        if kind in ('integer', 'number') and 'token' in leaf.lower() and ('output' in leaf.lower() or 'reasoning' in leaf.lower()):
            self.numeric_output_fields.append({'path': path, 'type': kind, 'value': value})
        return kind, value


def resolve_field(fields, name):
    paths = ['/payload/' + name, PASSTHROUGH + name]
    found = {fields[p] for p in paths if fields.get(p)}
    return (next(iter(found)) if len(found) == 1 else None), len(found) > 1


def item_category(row):
    if row['record_type'] != 'response_item': return None
    subtype = row['subtype']
    if subtype == 'message':
        return 'assistant_message' if row['role'] == 'assistant' else None
    if subtype == 'reasoning': return 'reasoning'
    if subtype in OUTPUT_TYPES: return 'tool_call'
    return None


def read_file(relative, reader, max_bytes):
    source_path = safe_source_path(relative); before = source_path.stat()
    if before.st_size > max_bytes: raise ValueError('source_exceeds_size_bound')
    stats = ScanStats(); rows = []; usage_rows = []; types = Counter(); epoch = 0
    model = None; source_model_counts = Counter()
    with source_path.open('rb') as locators, source_path.open('rb') as source:
        hashed = HashingReader(locators)
        for loc in iter_record_locators(hashed, stats, chunk_bytes=CHUNK, header_limit=256):
            if len(rows) >= 100000: raise ValueError('source_record_count_bound')
            source.seek(loc.byte_offset); reader.parse(source, loc.record_bytes)
            typ = reader.root_type
            if not isinstance(typ, str) or not SAFE_ENUM.fullmatch(typ): typ = '[unclassified]'
            if typ == 'session_meta':
                epoch += 1; model = None
            if typ == 'turn_context':
                model = reader.headers.get('/payload/model'); source_model_counts[model or '[unknown]'] += 1
            fields = dict(reader.fields)
            turn, turn_conflict = resolve_field(fields, 'turn_id')
            response, response_conflict = resolve_field(fields, 'response_id')
            row = {'line': loc.line_number, 'offset': loc.byte_offset, 'bytes': loc.record_bytes,
                   'record_type': typ, 'subtype': fields.get('/payload/type'),
                   'role': fields.get('/payload/role'), 'channel': fields.get('/payload/channel'),
                   'id': fields.get('/payload/id'), 'call_id': fields.get('/payload/call_id'),
                   'turn_id': turn, 'response_id': response, 'epoch': epoch,
                   'identifier_conflict': turn_conflict or response_conflict,
                   'fields': fields, 'content_spans': list(reader.value_spans),
                   'numeric_output_fields': list(reader.numeric_output_fields),
                   'usage_snapshots': [],
                   'nearest_model_context': model, 'timestamp': reader.headers.get('/timestamp')}
            row['output_category'] = item_category(row)
            rows.append(row); types[typ] += 1
            for obs in reader.observations(loc.line_number, loc.byte_offset):
                if typ == 'event_msg' and row['subtype'] == 'token_count' and obs['source_json_path'] in (
                        '/payload/info/last_token_usage', '/payload/info/total_token_usage'):
                    row['usage_snapshots'].append({'scope': obs['usage_scope'], 'usage': counters(obs),
                                                   'issues': obs['metadata']['issues']})
                if typ == 'token_usage_record' and obs['source_json_path'] == '/payload/usage':
                    if len(usage_rows) >= 20000: raise ValueError('response_usage_count_bound')
                    usage_rows.append({'line': loc.line_number, 'offset': loc.byte_offset,
                        'bytes': loc.record_bytes, 'epoch': epoch, 'usage': counters(obs),
                        'response_id': obs['response_id'], 'turn_id': obs['turn_id'],
                        'thread_id': obs['thread_id'], 'session_id': obs['session_id'],
                        'root_turn_id': obs['root_turn_id'], 'issues': obs['metadata']['issues'],
                        'source_object_offset': obs['source_object_byte_offset'],
                        'source_object_bytes': obs['source_object_bytes']})
    after = source_path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns): raise ValueError('source_changed')
    if hashed.bytes != before.st_size or sum(r['bytes'] for r in rows) + stats.blank_line_bytes != before.st_size:
        raise ValueError('physical_byte_accounting_failed')
    return {'source_relative_path': relative, 'source_sha256': hashed.digest.hexdigest(),
            'source_bytes': before.st_size, 'source_mtime_ns': before.st_mtime_ns,
            'source_records': len(rows), 'blank_line_bytes': stats.blank_line_bytes,
            'record_types': dict(types), 'model_contexts': dict(source_model_counts),
            'rows': rows, 'response_usage': usage_rows}


def select_sources(max_bytes):
    """Metadata-only selection, biased toward recent logs with richer ID fields."""
    selected = [FIRST]
    pool = []
    for path in candidate_files(ROOT / '2026'):
        size = path.stat().st_size
        relative = path.relative_to(ROOT).as_posix()
        if 512 * 1024 <= size <= max_bytes and relative != FIRST:
            pool.append((relative[:7], size, relative))
    for month in sorted({p[0] for p in pool}, reverse=True):
        selected.extend(p[2] for p in sorted((p for p in pool if p[0] == month), key=lambda p: (-p[1], p[2]))[:8])
    return selected


def snapshot(row, scope):
    found = [x['usage'] for x in row.get('usage_snapshots', []) if x['scope'] == scope and not x['issues']]
    return found[0] if len(found) == 1 and valid(found[0]) else None


def output_split(usage):
    total = usage.get('output_tokens'); reasoning = usage.get('reasoning_output_tokens')
    if type(total) is not int or type(reasoning) is not int or not 0 <= reasoning <= total:
        return None
    return {'output_tokens': total, 'reasoning_output_tokens': reasoning,
            'non_reasoning_output_tokens': total - reasoning,
            'reasoning_percent_of_output': 100 * reasoning / total if total else None,
            'non_reasoning_percent_of_output': 100 * (total - reasoning) / total if total else None,
            'visible_reply_tokens': None, 'tool_call_tokens': None, 'formatting_tokens': None,
            'residual_attribution': 'not_separately_reported'}


def content_bucket(row, span):
    path = span['path']
    if path == '/payload/encrypted_content': return 'encrypted_reasoning_value'
    if re.fullmatch(r'/payload/summary/\d+/text', path): return 'reasoning_summary_text_value'
    if row['output_category'] == 'assistant_message' and re.fullmatch(r'/payload/content/\d+/text', path):
        return 'assistant_text_value'
    if row['output_category'] == 'tool_call' and path in ('/payload/input', '/payload/arguments'):
        return 'tool_arguments_or_input_value'
    return 'other_selected_string_value'


def build_cases(evidence):
    rows = evidence['rows']; usages = evidence['response_usage']
    groups = defaultdict(list); turn_ids = defaultdict(set); rejected = Counter(); cases = []
    for u in usages:
        groups[u['response_id']].append(u)
        if u['response_id'] and u['turn_id']: turn_ids[(u['epoch'], u['turn_id'])].add(u['response_id'])
    emitted = set()
    for index, u in enumerate(usages):
        rid = u['response_id']; turn = u['turn_id']; epoch = u['epoch']
        if not rid or not turn or not valid(u['usage']) or u['issues'] or output_split(u['usage']) is None:
            rejected['invalid_missing_or_incomplete_response_usage'] += 1; continue
        signatures = {json.dumps({k: x[k] for k in ('usage','turn_id','thread_id','session_id','root_turn_id')}, sort_keys=True) for x in groups[rid]}
        if len(signatures) != 1:
            rejected['conflicting_response_id'] += 1; continue
        if rid in emitted:
            rejected['repeated_response_id_observation'] += 1; continue
        emitted.add(rid)
        previous_line = usages[index - 1]['line'] if index and usages[index - 1]['epoch'] == epoch else 0
        next_line = next((x['line'] for x in usages[index + 1:] if x['epoch'] == epoch), rows[-1]['line'] + 1)
        direct = [r for r in rows if r['output_category'] and r['response_id'] == rid and
                  r['epoch'] == epoch and not r['identifier_conflict'] and r['turn_id'] in (None, turn)]
        turn_rows = [r for r in rows if r['epoch'] == epoch and r['turn_id'] == turn]
        if direct:
            components = direct; method = 'explicit_response_id'; inferred = False
        elif len(turn_ids[(epoch, turn)]) == 1:
            components = [r for r in turn_rows if r['output_category'] and not r['identifier_conflict'] and r['response_id'] is None]
            method = 'explicit_turn_id_with_one_recorded_response'; inferred = True
        else:
            components = [r for r in turn_rows if previous_line < r['line'] < u['line'] and
                          r['output_category'] and not r['identifier_conflict'] and r['response_id'] is None]
            method = 'turn_id_and_consecutive_usage_order'; inferred = True
        if not components:
            rejected['no_model_output_components_in_candidate_range'] += 1; continue
        following_events = [r for r in rows if r['epoch'] == epoch and u['line'] < r['line'] < next_line
                            and r['record_type'] == 'event_msg' and r['subtype'] == 'token_count']
        matched = next((r for r in following_events if snapshot(r, 'last') == u['usage']), None)
        if matched is None:
            rejected['no_matching_following_last_usage_snapshot'] += 1; continue
        current_total = snapshot(matched, 'cumulative')
        preceding = [r for r in rows if r['epoch'] == epoch and r['line'] < matched['line'] and
                     r['record_type'] == 'event_msg' and r['subtype'] == 'token_count']
        arithmetic = 'unresolved'; previous_counter_line = None
        if preceding:
            prior = preceding[-1]; previous_counter_line = prior['line']; baseline = snapshot(prior, 'cumulative')
            if baseline and current_total and all((baseline[k] is None and current_total[k] is None and u['usage'][k] is None) or
                (type(baseline[k]) is int and type(current_total[k]) is int and type(u['usage'][k]) is int and current_total[k] - baseline[k] == u['usage'][k]) for k in u['usage']):
                arithmetic = 'cumulative_increment_matches_response_usage'
        elif current_total == u['usage']:
            arithmetic = 'first_available_cumulative_equals_response_usage'
        if arithmetic == 'unresolved':
            rejected['cumulative_accounting_not_reconciled'] += 1; continue
        buckets = Counter(); counts = Counter(); sizes = Counter(); ids = Counter()
        detailed = []; aliases = []
        for component in components:
            category = component['output_category']; counts[category] += 1; sizes[category] += component['bytes']
            if component['id']: ids[component['id']] += 1
            span_totals = Counter()
            for span in component['content_spans']:
                bucket = content_bucket(component, span); span_totals[bucket] += span['serialized_bytes']
            buckets.update(span_totals)
            detailed.append({**{k: component[k] for k in ('line','offset','bytes','subtype','id','call_id','turn_id','response_id')},
                             'category': category, 'serialized_string_value_bytes': dict(span_totals),
                             'content_spans': component['content_spans'], 'item_level_output_token_fields': component['numeric_output_fields'],
                             'tokens_attributed_to_this_item': None})
            if component['id']:
                aliases.extend({'item_line': component['line'], 'event_line': r['line'], 'shared_item_id': component['id'],
                                'event_bytes': r['bytes'], 'evidence': 'same_item_id_and_turn_not_added_to_model_output'}
                               for r in turn_rows if r['record_type'] == 'event_msg' and
                               r['fields'].get('/payload/item/id') == component['id'])
        call_ids = {r['call_id'] for r in components if r['output_category'] == 'tool_call' and r['call_id']}
        tool_results = [{k: r[k] for k in ('line','offset','bytes','subtype','call_id')} for r in rows if
                        r['epoch'] == epoch and r['record_type'] == 'response_item' and
                        r['subtype'] in ('function_call_output','custom_tool_call_output') and r['call_id'] in call_ids]
        cases.append({'source_relative_path': evidence['source_relative_path'], 'source_sha256': evidence['source_sha256'],
            'response_id': rid, 'turn_id': turn, 'usage_record': u,
            'nearest_model_context': components[0]['nearest_model_context'],
            'component_link_method': method, 'item_to_response_attribution_inferred': inferred,
            'link_limit': 'turn_id_does_not_establish_response_id_for_each_item' if inferred else None,
            'candidate_interval': {'start_after_line': previous_line, 'usage_line': u['line']},
            'matching_last_usage_event_line': matched['line'], 'previous_cumulative_event_line': previous_counter_line,
            'accounting_check': arithmetic, 'output_split': output_split(u['usage']),
            'output_component_record_counts': dict(counts), 'output_component_record_bytes': dict(sizes),
            'serialized_string_value_bytes': dict(buckets), 'physical_output_record_bytes': sum(sizes.values()),
            'repeated_item_id_occurrences': sum(n - 1 for n in ids.values()),
            'components': detailed, 'same_item_id_telemetry_records': aliases,
            'tool_results_excluded_from_model_output': tool_results,
            'unknown_response_item_types_in_interval': sorted({r['subtype'] or '[missing]' for r in turn_rows
                if previous_line < r['line'] < u['line'] and r['record_type'] == 'response_item'
                and r['subtype'] not in OUTPUT_TYPES | {'message','function_call_output','custom_tool_call_output','compaction','ghost_snapshot'}})})
    return cases, dict(rejected)


def choose_cases(cases):
    selected = []; used = set()
    def add(label, predicate):
        candidate = next((c for c in cases if c['response_id'] not in used and predicate(c)), None)
        if candidate:
            selected.append({**candidate, 'case_number': len(selected) + 1, 'case_label': label}); used.add(candidate['response_id'])
    count = lambda c, key: c['output_component_record_counts'].get(key, 0)
    add('known_118_output_65_reasoning', lambda c: c['source_relative_path'] == FIRST and
        c['output_split']['output_tokens'] == 118 and c['output_split']['reasoning_output_tokens'] == 65)
    add('assistant_message_zero_reported_reasoning', lambda c: count(c,'assistant_message') and
        not count(c,'reasoning') and not count(c,'tool_call') and c['output_split']['reasoning_output_tokens'] == 0)
    add('reasoning_assistant_message_and_tool_call', lambda c: all(count(c,k) for k in ('reasoning','assistant_message','tool_call')))
    add('reasoning_and_tool_call_without_assistant_message', lambda c: count(c,'reasoning') and count(c,'tool_call') and not count(c,'assistant_message'))
    add('tool_call_zero_reported_reasoning', lambda c: count(c,'tool_call') and not count(c,'reasoning') and
        not count(c,'assistant_message') and c['output_split']['reasoning_output_tokens'] == 0)
    add('multiple_reasoning_records_and_assistant_message', lambda c: count(c,'reasoning') > 1 and count(c,'assistant_message') and not count(c,'tool_call'))
    return selected


def verify_cases(cases):
    counts = Counter()
    for case in cases:
        u = case['usage_record']
        if u['bytes'] > 65536: raise ValueError('usage_verification_size_limit')
        with safe_source_path(case['source_relative_path']).open('rb') as source:
            source.seek(u['offset']); raw = source.read(u['bytes'])
        parsed = json.loads(raw)
        if parsed['type'] != 'token_usage_record' or counters(parsed['payload']['usage']) != u['usage']:
            raise ValueError('independent_usage_verification_failed')
        for key in ('response_id','turn_id','thread_id','session_id','root_turn_id'):
            if parsed['payload'].get(key) != u[key]: raise ValueError('independent_identifier_verification_failed')
        counts['counter_records_verified'] += 1; counts['source_bytes_read'] += len(raw)
        split = case['output_split']
        if split['reasoning_output_tokens'] + split['non_reasoning_output_tokens'] != split['output_tokens']:
            raise ValueError('output_partition_mismatch')
        case['independent_counter_verification'] = 'passed'
    return {'status': 'passed', **counts, 'method': 'stdlib_json_on_original_small_usage_records'}


LABELS = {
    'known_118_output_65_reasoning': 'الحالة الأصلية: استدلال ورسالة',
    'assistant_message_zero_reported_reasoning': 'رسالة مع استدلال مسجّل بصفر',
    'reasoning_assistant_message_and_tool_call': 'استدلال ورسالة واستدعاء أداة',
    'reasoning_and_tool_call_without_assistant_message': 'استدلال واستدعاء أداة',
    'tool_call_zero_reported_reasoning': 'استدعاء أداة مع استدلال مسجّل بصفر',
    'multiple_reasoning_records_and_assistant_message': 'عدة سجلات استدلال ورسالة',
}


def markdown(report):
    lines = ['# نتائج فحص مكوّنات الإخراج', '',
        f"تاريخ الإنشاء: {report['completed_at']}. عدد الحالات: {len(report['cases'])}.", '',
        '## ما الذي ثبت؟', '',
        '- تمّت قراءة ملفات المصدر عبر السكربت، دون قاعدة البيانات أو عرض نصوص المحادثات أو الصور.',
        '- لكل حالة قراءة استخدام تخص معرّف استجابة، ومقارنة ناجحة مع الاستخدام الأخير والعدّاد التراكمي، وتحقق مستقل من سجل الاستخدام الأصلي.',
        '- تقسيم الإخراج إلى استدلال وباقي الإخراج مستخرج من العدّادات. باقي الإخراج = الإخراج الكلي ناقص الاستدلال، ولا يساوي تلقائيًا نص الرد الظاهر.',
        '- لم تُوجد عدّادات إخراج مستقلة داخل عناصر الإخراج المختارة. حصص نص الرد واستدعاء الأداة والتنسيق الداخلي تبقى غير معلومة؛ لم تُقدّر باستخدام الحجم أو الترميز.', '',
        '## حدود الربط', '',
        '- عناصر الإخراج في الحالات المختارة لا تحمل معرّف الاستجابة صراحة. الربط بمعرّف الدور مباشر، لكن إسناد كل عنصر إلى استجابة محددة يحتاج تمييزًا.',
        '- عندما توجد استجابة واحدة مسجّلة للدور: نعرض عناصر ذلك الدور مع هذه القراءة، دون الادعاء بإثبات اكتمال التسجيل على الخادم.',
        '- عندما توجد عدة استجابات للدور: نعرض العناصر الواقعة بين قراءتي استخدام متتاليتين وتحمل معرّف الدور نفسه. هذا ربط استدلالي بترتيب السجلات، وليس ربطًا صريحًا بمعرّف الاستجابة.',
        '- تطابق العدّادات يثبت اتساق الحساب داخل السجل؛ لا يثبت وحده أن جميع العناصر ناتجة عن الاستجابة المعينة أو أن النشاط لم يُنسخ من تاريخ سابق.',
        '- العينة تشخيصية مختارة لتنوع الحالات، وليست تقديرًا لنسب المجموعة البحثية كاملة.', '',
        '## الحالات الست', '',
        '| الحالة | العناصر المرصودة في النطاق المرتبط | إجمالي الإخراج | الاستدلال | نسبته من الإخراج | باقي الإخراج | حجم سجلات العناصر بالبايت |',
        '|---|---|---:|---:|---:|---:|---:|']
    for c in report['cases']:
        split = c['output_split']
        percent = f"{split['reasoning_percent_of_output']:.2f}%" if split['reasoning_percent_of_output'] is not None else 'غير منطبق'
        lines.append(f"| {c['case_number']} | {LABELS[c['case_label']]} | {split['output_tokens']:,} | {split['reasoning_output_tokens']:,} | {percent} | {split['non_reasoning_output_tokens']:,} | {c['physical_output_record_bytes']:,} |")
    lines += ['', 'الأحجام أعلاه تخص سجلات `response_item` المختارة كاملة مع بنية JSON ونهاية السطر. لا تتضمن أحداث التسجيل أو نتائج الأدوات، ولا تمثل حجم البيانات المرسلة للنموذج أو عدد التوكن.',
              '', '## أدلة الحالات', '']
    for c in report['cases']:
        lines += [f"### الحالة {c['case_number']}: {LABELS[c['case_label']]}", '',
            f"المصدر: `{c['source_relative_path']}`", f"SHA-256: `{c['source_sha256']}`",
            f"معرّف الاستجابة: `{c['response_id']}`؛ معرّف الدور: `{c['turn_id']}`.",
            f"سجل الاستخدام: السطر {c['usage_record']['line']}، البايت {c['usage_record']['offset']}. حدث الاستخدام الأخير المطابق: السطر {c['matching_last_usage_event_line']}.",
            f"آلية ربط العناصر: `{c['component_link_method']}`. المقارنة الحسابية: `{c['accounting_check']}`.",
            f"النموذج في سياق الدور الأقرب: `{c['nearest_model_context']}`؛ لا نفترض أن اسم السياق يثبت اسم النموذج لكل عنصر منفرد.", '',
            '| نوع العنصر | السطر | حجم السجل بالبايت | عدد التوكن الخاص بهذا العنصر |',
            '|---|---:|---:|---|']
        for item in c['components']:
            lines.append(f"| `{item['subtype']}` | {item['line']} | {item['bytes']:,} | غير مسجّل منفصلًا |")
        lines += ['', 'أحجام حقول المحتوى المتسلسلة داخل العناصر (جزء من حجم السجل، وتشمل اقتباسات JSON والهروب):', '',
                  '| الحقل المصنف | بايت |', '|---|---:|']
        for key, size in c['serialized_string_value_bytes'].items(): lines.append(f'| `{key}` | {size:,} |')
        lines += ['', f"أحداث تسجيل تحمل معرّف العنصر نفسه ولم تُضف كإخراج جديد: {len(c['same_item_id_telemetry_records'])}.",
                  f"سجلات نتائج أدوات مرتبطة بمعرّف الاستدعاء ومستبعدة من إخراج النموذج: {len(c['tool_results_excluded_from_model_output'])}.", '']
    lines += ['## الموارد والتحقق', '',
        f"ذروة ذاكرة عملية Python: {report['peak_working_set_bytes']/1048576:.2f} MiB. حجم دفعة القراءة: {CHUNK//1024} KiB.",
        f"التحقق المستقل: {report['validation']['counter_records_verified']} سجلات استخدام، {report['validation']['source_bytes_read']:,} بايت، النتيجة `{report['validation']['status']}`.",
        f"ملفات التشغيل النهائي: {len(report['sources'])}. سجل الاستطلاع يحتفظ بـ{len(report['discovery_sources'])} ملفات فُحصت أثناء البحث عن التنوع؛ الملفات الصغيرة الحديثة الأولى كانت مراجعة آلية فقط.",
        'لا تعديل على المصادر أو PostgreSQL. لا فك تشفير، ولا إرسال بيانات إلى خدمة خارجية، ولا حساب تكلفة مالية.', '']
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source')
    parser.add_argument('--max-file-mib', type=int, default=16)
    parser.add_argument('--max-files', type=int, default=12)
    parser.add_argument('--discover', action='store_true', help='Inspect a bounded metadata-selected source pool instead of the two documented study files')
    args = parser.parse_args()
    if not 1 <= args.max_file_mib <= 16 or not 1 <= args.max_files <= 24: parser.error('invalid bounded scan settings')
    started = time.monotonic(); lower_priority(); OUT.mkdir(parents=True, exist_ok=True); (ROOT / '.tmp').mkdir(exist_ok=True)
    sources = [args.source] if args.source else (select_sources(args.max_file_mib * 1048576)[:args.max_files] if args.discover else [FIRST, WORKFLOW])
    all_cases = []; manifest = []; failures = []
    with tempfile.TemporaryDirectory(dir=ROOT / '.tmp', prefix='output-components-') as tmp:
        reader = OutputReader(Path(tmp) / 'scratch.sqlite3')
        try:
            for relative in sources:
                try:
                    evidence = read_file(relative, reader, args.max_file_mib * 1048576)
                    candidates, rejected = build_cases(evidence)
                except (OSError, ValueError, RuntimeError) as error:
                    failures.append({'source_relative_path': relative, 'error_type': type(error).__name__})
                    print(json.dumps(failures[-1]), flush=True); continue
                key = hashlib.sha256(relative.encode()).hexdigest()[:16]
                save(OUT / (key + '_evidence.json'), evidence)
                item = {k: evidence[k] for k in ('source_relative_path','source_sha256','source_bytes','source_mtime_ns','source_records','record_types','model_contexts')}
                item.update(candidate_cases=len(candidates), rejected_candidates=rejected, response_usage_records=len(evidence['response_usage']),
                            evidence_file=(OUT / (key + '_evidence.json')).relative_to(ROOT).as_posix())
                manifest.append(item); all_cases.extend(candidates)
                print(json.dumps({'source': relative, 'records': evidence['source_records'],
                    'usage_rows': len(evidence['response_usage']), 'models': evidence['model_contexts'],
                    'output_categories': dict(Counter(r['output_category'] for r in evidence['rows'] if r['output_category'])),
                    'output_with_response_id': sum(bool(r['response_id']) for r in evidence['rows'] if r['output_category']),
                    'output_with_turn_id': sum(bool(r['turn_id']) for r in evidence['rows'] if r['output_category'])}), flush=True)
        finally: reader.close()
    chosen = choose_cases(all_cases)
    validation = verify_cases(chosen)
    discovery = []
    for path in sorted(OUT.glob('*_evidence.json')):
        evidence = json.loads(path.read_text(encoding='utf-8'))
        discovery.append({k: evidence[k] for k in ('source_relative_path','source_sha256','source_bytes','source_records','model_contexts')})
    for item in manifest:
        current = safe_source_path(item['source_relative_path']).stat()
        if (current.st_size,current.st_mtime_ns) != (item['source_bytes'],item['source_mtime_ns']): raise ValueError('source_changed_during_verification')
    report = {'version': VERSION, 'completed_at': utc_now(),
        'status': 'completed_six_counter_verified_cases' if len(chosen) == 6 and not failures else 'incomplete',
        'cases': chosen, 'sources': manifest, 'discovery_sources': discovery, 'source_failures': failures,
        'candidate_cases': len(all_cases), 'validation': validation,
        'per_item_token_attribution_complete': False, 'database_accessed': False, 'original_files_modified': False,
        'conversation_content_exported': False, 'tokenizer_estimates_used': False,
        'peak_working_set_bytes': peak_memory(), 'elapsed_seconds': round(time.monotonic() - started, 3)}
    save(OUT / 'summary.json', report)
    (OUT / 'report.md').write_text(markdown(report), encoding='utf-8')
    print(json.dumps({'status': report['status'], 'cases': len(chosen), 'candidates': len(all_cases),
        'peak_working_set_bytes': report['peak_working_set_bytes'], 'validation': validation}, indent=2))
    return 0 if report['status'].startswith('completed_') else 1


if __name__ == '__main__':
    raise SystemExit(main())
