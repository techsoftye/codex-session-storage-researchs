"""Source-only token timeline for one explicitly delimited turn; no content export."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime
import json
from pathlib import Path
import tempfile
import time

import analyze_output_token_components as components
from analyze_session_storage_tokens import counters, valid
from index_session_records import ROOT, safe_source_path, lower_priority, utc_now
from profile_compacted import CHUNK
from profile_response_items import peak_memory
from repair_null_record_types import save
from token_usage_stream import COUNTERS, IDENTIFIERS

VERSION = '1.0.0'
OUT = ROOT / 'data/reports/turn_token_timeline_v1'
TURN = '00000000-0000-4000-8000-000000000004'
ID_PATHS = {'/payload/' + k for k in (*IDENTIFIERS, 'compaction_response_id')}
ID_PATHS.update('/payload/latest_token_usage_record/' + k for k in IDENTIFIERS)


class TurnReader(components.OutputReader):
    def __init__(self, spool_path):
        super().__init__(spool_path)
        self.accounting_objects = []
        self.identifier_evidence = {}

    def parse(self, source, size, chunk=CHUNK):
        self.current_identifier_spans = []
        return super().parse(source, size, chunk)

    def value(self, path, depth):
        self.stream.whitespace(); start = self.stream.position
        kind, value = super().value(path, depth)
        if path in ID_PATHS and kind == 'string' and path in self.fields:
            self.current_identifier_spans.append({'path': path, 'relative_offset': start,
                'bytes': self.stream.position - start, 'value': self.fields[path]})
        return kind, value

    def observations(self, record_id, record_offset):
        self.identifier_evidence[record_id] = [
            {**span, 'offset': record_offset + span['relative_offset']}
            for span in self.current_identifier_spans]
        for observation in super().observations(record_id, record_offset):
            if len(self.accounting_objects) >= 50000:
                raise ValueError('counter_observation_limit')
            self.accounting_objects.append({
                'line': record_id, 'record_type': self.root_type,
                **{k: observation[k] for k in ('source_json_path', 'source_object_byte_offset',
                    'source_object_bytes', 'usage_scope', *IDENTIFIERS)},
                'usage': counters(observation), 'issues': observation['metadata']['issues']})
            yield observation


def complete(values):
    return valid(values) and all(type(values.get(k)) is int for k in COUNTERS)


def subtract(end, start):
    return {k: end[k] - start[k] for k in COUNTERS}


def add(values):
    result = dict.fromkeys(COUNTERS, 0)
    for value in values:
        for key in COUNTERS:
            result[key] += value[key]
    return result


def counter_at(objects, line, path):
    matches = [o for o in objects if o['line'] == line and o['source_json_path'] == path]
    return matches[0] if len(matches) == 1 else None


def trusted(observation):
    return observation is not None and not observation['issues'] and complete(observation['usage'])


def locator(row):
    return {k: row[k] for k in ('line', 'offset', 'bytes', 'record_type', 'subtype', 'timestamp')}


RESPONSE_PATHS = {'token_usage_record': '/payload/usage',
                  'compacted': '/payload/latest_token_usage_record/usage'}


def response_observation(obj):
    return RESPONSE_PATHS.get(obj['record_type']) == obj['source_json_path']


def build_timeline(evidence, objects, turn_id):
    rows = evidence['rows']; by_line = {r['line']: r for r in rows}
    starts = [r for r in rows if r['record_type'] == 'event_msg' and
              r['subtype'] == 'task_started' and r['turn_id'] == turn_id]
    ends = [r for r in rows if r['record_type'] == 'event_msg' and
            r['subtype'] == 'task_complete' and r['turn_id'] == turn_id]
    if len(starts) != 1 or len(ends) != 1:
        raise ValueError('ambiguous_or_missing_turn_boundary')
    start, end = starts[0], ends[0]
    if start['epoch'] != end['epoch'] or start['line'] >= end['line']:
        raise ValueError('invalid_turn_boundary')
    selected_rows = [r for r in rows if start['line'] <= r['line'] <= end['line']]
    if any(r['subtype'] in ('task_started', 'task_complete', 'task_aborted', 'turn_aborted')
           and r['line'] not in (start['line'], end['line']) for r in selected_rows):
        raise ValueError('overlapping_or_interrupted_turn')
    epoch = start['epoch']
    selected_objects = [o for o in objects if start['line'] <= o['line'] <= end['line']]
    response_objects = [o for o in selected_objects if response_observation(o)]
    if not response_objects or any(o['turn_id'] != turn_id for o in response_objects):
        raise ValueError('unresolved_or_foreign_response_in_turn')
    if any(response_observation(o) and o['turn_id'] == turn_id and
           not start['line'] <= o['line'] <= end['line'] for o in objects):
        raise ValueError('response_outside_turn_boundary')
    groups = defaultdict(list)
    for obj in response_objects:
        if not trusted(obj) or any(not obj[k] for k in IDENTIFIERS):
            raise ValueError('incomplete_response_counter_or_identity')
        groups[obj['response_id']].append(obj)
    selected = []
    for rid, group in groups.items():
        signatures = {json.dumps({k: o[k] for k in ('usage', *IDENTIFIERS)}, sort_keys=True) for o in group}
        if len(signatures) != 1:
            raise ValueError('conflicting_response_observations')
        canonical = min(group, key=lambda o: (o['record_type'] != 'token_usage_record', o['line']))
        selected.append((canonical, group))
    selected.sort(key=lambda pair: pair[0]['line'])
    if len({(o['thread_id'], o['session_id']) for o, _ in selected}) != 1:
        raise ValueError('multiple_accounting_contexts')
    identity = selected[0][0]

    prior_events = [r for r in rows if r['epoch'] == epoch and r['line'] < start['line'] and
                    r['record_type'] == 'event_msg' and r['subtype'] == 'token_count']
    if not prior_events:
        raise ValueError('no_observed_pre_turn_cumulative_baseline')
    baseline_event_row = prior_events[-1]
    baseline_event = counter_at(objects, baseline_event_row['line'], '/payload/info/total_token_usage')
    prior_threads = [o for o in objects if o['line'] < start['line'] and
        by_line[o['line']]['epoch'] == epoch and o['record_type'] == 'token_usage_record' and
        o['source_json_path'] == '/payload/thread_token_usage' and
        (o['thread_id'], o['session_id']) == (identity['thread_id'], identity['session_id'])]
    if not prior_threads or not trusted(baseline_event) or not trusted(prior_threads[-1]):
        raise ValueError('invalid_or_missing_pre_turn_baseline')
    baseline_thread = prior_threads[-1]
    baseline_gap = subtract(baseline_thread['usage'], baseline_event['usage'])
    events = [r for r in selected_rows if r['record_type'] == 'event_msg' and r['subtype'] == 'token_count']
    if not events:
        raise ValueError('no_turn_counter_events')
    final_event = counter_at(objects, events[-1]['line'], '/payload/info/total_token_usage')
    if not trusted(final_event):
        raise ValueError('invalid_final_cumulative_snapshot')

    compactions = [r for r in selected_rows if r['record_type'] == 'compacted']
    compaction_ids = {r['fields'].get('/payload/compaction_response_id') for r in compactions}
    compaction_ids.discard(None)
    if any(r['fields'].get('/payload/compaction_response_id') not in groups for r in compactions):
        raise ValueError('unlinked_compaction_response')
    for row in compactions:
        embedded = counter_at(objects, row['line'], '/payload/latest_token_usage_record/usage')
        if not trusted(embedded) or embedded['response_id'] != row['fields'].get('/payload/compaction_response_id'):
            raise ValueError('conflicting_compaction_response_link')
    prior_evidence = {**evidence, 'rows': [r for r in rows if baseline_event_row['line'] <= r['line'] <= end['line']],
        'response_usage': [u for u in evidence['response_usage'] if start['line'] <= u['line'] <= end['line']]}
    component_cases, _ = components.build_cases(prior_evidence)
    component_map = {c['response_id']: c for c in component_cases}
    timeline = []; prefix = dict.fromkeys(COUNTERS, 0); compaction_prefix = dict.fromkeys(COUNTERS, 0)
    previous_event = baseline_event; previous_input = None
    for index, (obj, group) in enumerate(selected):
        row = by_line[obj['line']]; rid = obj['response_id']; counts = obj['usage']
        prefix = add([prefix, counts])
        is_compaction = rid in compaction_ids
        if is_compaction: compaction_prefix = add([compaction_prefix, counts])
        parent_path = obj['source_json_path'].rsplit('/', 1)[0]
        turn_counter = counter_at(objects, obj['line'], parent_path + '/turn_token_usage')
        thread_counter = counter_at(objects, obj['line'], parent_path + '/thread_token_usage')
        if not trusted(turn_counter) or not trusted(thread_counter):
            raise ValueError('missing_response_scope_companions')
        for occurrence in group:
            parent = occurrence['source_json_path'].rsplit('/', 1)[0]
            for suffix, reference in (('turn_token_usage', turn_counter), ('thread_token_usage', thread_counter)):
                companion = counter_at(objects, occurrence['line'], parent + '/' + suffix)
                if not trusted(companion) or companion['usage'] != reference['usage'] or any(companion[k] != obj[k] for k in IDENTIFIERS):
                    raise ValueError('conflicting_or_missing_companion_scope')
        before_line = selected[index - 1][0]['line'] if index else start['line']
        next_line = selected[index + 1][0]['line'] if index + 1 < len(selected) else end['line'] + 1
        following = [r for r in events if obj['line'] < r['line'] < next_line]
        last_matches = []
        for event in following:
            last = counter_at(objects, event['line'], '/payload/info/last_token_usage')
            if trusted(last) and last['usage'] == counts: last_matches.append(event['line'])
        current_event = counter_at(objects, following[-1]['line'], '/payload/info/total_token_usage') if following else None
        event_delta = subtract(current_event['usage'], previous_event['usage']) if trusted(current_event) else None
        event_gap = subtract(thread_counter['usage'], current_event['usage']) if trusted(current_event) else None
        if trusted(current_event): previous_event = current_event
        case = component_map.get(rid)
        prior_tools = [r for r in selected_rows if before_line < r['line'] < obj['line'] and
            r['record_type'] == 'response_item' and r['subtype'] in ('function_call_output','custom_tool_call_output')]
        item = {'sequence': index + 1, 'response_id': rid, 'turn_id': turn_id,
            'usage_record': locator(row), 'usage': counts,
            'input_not_reported_as_cached': counts['input_tokens'] - counts['cached_input_tokens'],
            'non_reasoning_output_tokens': counts['output_tokens'] - counts['reasoning_output_tokens'],
            'input_change_from_previous_response': counts['input_tokens'] - previous_input if previous_input is not None else None,
            'is_explicit_compaction_response': is_compaction,
            'observation_locations': [{'line': o['line'], 'path': o['source_json_path'],
                'offset': o['source_object_byte_offset'], 'bytes': o['source_object_bytes']} for o in group],
            'duplicate_observations_not_added': len(group) - 1,
            'turn_counter': turn_counter, 'thread_counter': thread_counter,
            'turn_prefix_matches': turn_counter['usage'] == prefix,
            'thread_increment_from_baseline_matches_prefix': subtract(thread_counter['usage'], baseline_thread['usage']) == prefix,
            'matching_last_usage_event_lines': last_matches,
            'event_cumulative_increment': event_delta,
            'event_increment_matches_response': event_delta == counts,
            'thread_minus_event_cumulative': event_gap,
            'event_gap_matches_baseline_plus_compaction_usage': event_gap == add([baseline_gap, compaction_prefix]),
            'component_link_method': case['component_link_method'] if case else 'no_response_item_attribution',
            'component_association_is_inferred': case['item_to_response_attribution_inferred'] if case else None,
            'output_component_record_counts': case['output_component_record_counts'] if case else {},
            'candidate_output_record_bytes': case['physical_output_record_bytes'] if case else None,
            'candidate_output_record_lines': [c['line'] for c in case['components']] if case else [],
            'preceding_tool_results': [locator(r) for r in prior_tools],
            'tool_results_confirmed_in_next_model_request': None,
            'per_content_input_token_allocation': None}
        timeline.append(item); previous_input = counts['input_tokens']

    transitions = []
    for row in compactions:
        rid = row['fields']['/payload/compaction_response_id']
        comp_entry = next(t for t in timeline if t['response_id'] == rid)
        ordinary_before = [t for t in timeline if t['usage_record']['line'] < row['line'] and not t['is_explicit_compaction_response']]
        after = [t for t in timeline if t['usage_record']['line'] > row['line'] and not t['is_explicit_compaction_response']]
        first_after = after[0] if after else None; last_before = ordinary_before[-1] if ordinary_before else None
        transitions.append({'record': locator(row), 'compaction_response_id': rid,
            'compaction_response_sequence': comp_entry['sequence'],
            'last_ordinary_before_sequence': last_before['sequence'] if last_before else None,
            'first_ordinary_after_sequence': first_after['sequence'] if first_after else None,
            'input_before': last_before['usage']['input_tokens'] if last_before else None,
            'input_after': first_after['usage']['input_tokens'] if first_after else None,
            'input_change': first_after['usage']['input_tokens'] - last_before['usage']['input_tokens'] if first_after and last_before else None,
            'causal_or_financial_savings_claim': False})

    totals = add(t['usage'] for t in timeline)
    compaction_totals = add(t['usage'] for t in timeline if t['is_explicit_compaction_response'])
    event_difference = subtract(final_event['usage'], baseline_event['usage'])
    residual = subtract(totals, event_difference)
    counts_by_type = Counter(r['record_type'] for r in selected_rows)
    bytes_by_type = Counter()
    for row in selected_rows: bytes_by_type[row['record_type']] += row['bytes']
    tool_calls = [r for r in selected_rows if r['output_category'] == 'tool_call']
    tool_results = [r for r in selected_rows if r['record_type'] == 'response_item' and
                    r['subtype'] in ('function_call_output','custom_tool_call_output')]
    span_bytes = end['offset'] + end['bytes'] - start['offset']
    duration = (datetime.fromisoformat(end['timestamp'].replace('Z','+00:00')) -
                datetime.fromisoformat(start['timestamp'].replace('Z','+00:00'))).total_seconds()
    checks = {'every_turn_prefix_matches': all(t['turn_prefix_matches'] for t in timeline),
        'every_thread_prefix_matches': all(t['thread_increment_from_baseline_matches_prefix'] for t in timeline),
        'event_gap_explained_at_every_response': all(t['event_gap_matches_baseline_plus_compaction_usage'] for t in timeline),
        'sum_equals_final_turn_counter': totals == timeline[-1]['turn_counter']['usage'],
        'sum_equals_thread_counter_difference': totals == subtract(timeline[-1]['thread_counter']['usage'], baseline_thread['usage']),
        'event_difference_equals_sum': event_difference == totals,
        'sum_minus_event_difference_equals_compaction_usage': residual == compaction_totals}
    consistent = all(checks[k] for k in checks if k != 'event_difference_equals_sum')
    status = ('reconciled' if checks['event_difference_equals_sum'] else 'reconciled_with_documented_event_scope_difference') if consistent else 'unresolved_accounting'
    report = {'version': VERSION, 'status': status,
        'source': {k: evidence[k] for k in ('source_relative_path','source_sha256','source_bytes','source_mtime_ns','source_records')},
        'turn_id': turn_id, 'session_id': identity['session_id'], 'thread_id': identity['thread_id'],
        'boundary': {'start': locator(start), 'end': locator(end), 'elapsed_seconds': duration,
            'records': len(selected_rows), 'byte_span': span_bytes,
            'blank_bytes': span_bytes - sum(r['bytes'] for r in selected_rows)},
        'response_count': len(timeline), 'response_usage_observation_count': len(response_objects),
        'duplicate_response_observations_not_added': len(response_objects) - len(timeline),
        'compaction_response_count': sum(t['is_explicit_compaction_response'] for t in timeline),
        'totals': totals, 'ordinary_response_totals': subtract(totals, compaction_totals),
        'compaction_response_totals': compaction_totals,
        'input_not_reported_as_cached': totals['input_tokens'] - totals['cached_input_tokens'],
        'cached_input_percent': 100 * totals['cached_input_tokens'] / totals['input_tokens'] if totals['input_tokens'] else None,
        'non_reasoning_output_tokens': totals['output_tokens'] - totals['reasoning_output_tokens'],
        'baseline_event_counter': baseline_event, 'baseline_thread_counter': baseline_thread,
        'final_event_counter': final_event, 'event_counter_difference': event_difference,
        'sum_minus_event_difference': residual, 'checks': checks,
        'response_last_snapshot_matches': sum(bool(t['matching_last_usage_event_lines']) for t in timeline),
        'response_event_increment_matches': sum(t['event_increment_matches_response'] for t in timeline),
        'invalid_counter_objects': [o for o in selected_objects if not trusted(o)],
        'timeline': timeline, 'compaction_transitions': transitions,
        'storage_by_record_type': [{'record_type': k, 'records': counts_by_type[k], 'bytes': bytes_by_type[k]}
            for k in sorted(counts_by_type, key=lambda k: -bytes_by_type[k])],
        'tool_call_records': len(tool_calls), 'distinct_tool_call_ids': len({r['call_id'] for r in tool_calls if r['call_id']}),
        'tool_result_records': len(tool_results),
        'tool_results_with_matching_turn_call_id': sum(r['call_id'] in {c['call_id'] for c in tool_calls if c['call_id']} for r in tool_results),
        'database_accessed': False, 'original_files_modified': False, 'conversation_content_exported': False,
        'tokenizer_estimates_used': False, 'per_content_token_attribution_complete': False}
    selected_object_lines = {o['line'] for o in selected_objects} | {baseline_thread['line'], baseline_event['line']}
    verification_objects = [o for o in objects if o['line'] in selected_object_lines]
    return report, verification_objects, selected_rows


def verify(evidence, objects, identifiers, boundary):
    """Independent small-fragment JSON decoding, including anomalous counters."""
    by_line = {r['line']: r for r in evidence['rows']}
    lines = {o['line'] for o in objects} | {boundary['start']['line'], boundary['end']['line']}
    result = Counter()
    with safe_source_path(evidence['source_relative_path']).open('rb') as source:
        def fragment(offset, size):
            if not 0 < size <= 65536: raise ValueError('independent_verification_fragment_size_limit')
            source.seek(offset); raw = source.read(size)
            if len(raw) != size: raise ValueError('incomplete_verification_fragment')
            result['source_bytes_read'] += size
            return json.loads(raw)
        for obj in objects:
            decoded = fragment(obj['source_object_byte_offset'], obj['source_object_bytes'])
            if counters(decoded) != obj['usage']:
                raise ValueError('independent_counter_fragment_mismatch')
            result['counter_objects_verified'] += 1
        for line in sorted(lines):
            for span in identifiers.get(line, []):
                if fragment(span['offset'], span['bytes']) != span['value']:
                    raise ValueError('independent_identifier_fragment_mismatch')
                result['identifier_values_verified'] += 1
            row = by_line[line]
            if row['record_type'] not in ('token_usage_record', 'event_msg'): continue
            decoded = fragment(row['offset'], row['bytes'])
            if decoded.get('type') != row['record_type'] or decoded['payload'].get('type') != row['subtype']:
                raise ValueError('independent_record_envelope_mismatch')
            if row['record_type'] == 'token_usage_record':
                for key in IDENTIFIERS:
                    if decoded['payload'].get(key) != row['fields'].get('/payload/' + key):
                        raise ValueError('independent_usage_identity_mismatch')
            if line in (boundary['start']['line'], boundary['end']['line']):
                if decoded['payload'].get('turn_id') != row['turn_id']:
                    raise ValueError('independent_turn_boundary_mismatch')
            for obj in (o for o in objects if o['line'] == line):
                value = decoded
                for segment in obj['source_json_path'].strip('/').split('/'):
                    value = value[int(segment)] if isinstance(value, list) else value[segment]
                if counters(value) != obj['usage']:
                    raise ValueError('independent_counter_path_mismatch')
            result['small_records_verified'] += 1
    return {'status': 'passed', **result,
            'method': 'stdlib_json_on_bounded_counter_and_identifier_fragments_and_small_records'}


def markdown(report):
    b = report['boundary']; totals = report['totals']; checks = report['checks']
    lines = ['# تتبّع التوكن لدور كامل', '',
        f"تاريخ التحليل: {report['completed_at']} — الحالة: `{report['status']}`.", '',
        f"المصدر: `{report['source']['source_relative_path']}`.",
        f"SHA-256: `{report['source']['source_sha256']}`.",
        f"معرّف الدور: `{report['turn_id']}`.", '',
        '## النطاق وطريقة الحساب', '',
        f"من `task_started` في السطر {b['start']['line']} ({b['start']['timestamp']}) إلى `task_complete` في السطر {b['end']['line']} ({b['end']['timestamp']}). الزمن المسجل بينهما {b['elapsed_seconds']:.3f} ثانية.",
        f"يضم النطاق {b['records']} سجلًا و{b['byte_span']:,} بايت ({b['byte_span']/1048576:.4f} MiB). هذه أحجام التخزين المحلي، وليست حجم الطلب المرسل للنموذج.",
        f"رُصدت {report['response_usage_observation_count']} قراءة على مستوى الاستجابة، تعود إلى {report['response_count']} معرّف استجابة مختلف. احتُفظ بأدلة التكرار ولم تُجمع {report['duplicate_response_observations_not_added']} قراءة مكررة إضافية.",
        '- لا يُدمج معرّفان مختلفان لمجرد تطابق أرقام التوكن. الدمج الحسابي يقتصر على المعرف نفسه مع تطابق العدّادات الستة ومعرفات السياق.',
        '- جُمعت قراءات `usage` مرة لكل استجابة. استُخدمت `turn_token_usage` و`thread_token_usage` ولقطات `event_msg` للتحقق فقط، ولم تُضف إلى المجموع.',
        '- البداية والنهاية تعرّفان دورًا مسجلًا؛ لم يُقرأ موضوع المحادثة أو يُفترض أنه طلب بشري مستقل.', '',
        '## الإجماليات المثبتة في السجل', '',
        '| القياس | توكن |', '|---|---:|',
        f"| إجمالي الإدخال | {totals['input_tokens']:,} |",
        f"| منه إدخال من الكاش | {totals['cached_input_tokens']:,} |",
        f"| الإدخال غير المسجل ككاش: الفرق | {report['input_not_reported_as_cached']:,} |",
        f"| كتابة الكاش كما وردت | {totals['cache_write_input_tokens']:,} |",
        f"| إجمالي الإخراج | {totals['output_tokens']:,} |",
        f"| منه استدلال | {totals['reasoning_output_tokens']:,} |",
        f"| باقي الإخراج | {report['non_reasoning_output_tokens']:,} |",
        f"| الإدخال + الإخراج | {totals['total_tokens']:,} |", '',
        f"الكاش يمثل {report['cached_input_percent']:.2f}% من الإدخال. هذه نسبة توكن مسجلة، وليست نسبة توفير مالي. الإدخال مجموع عبر {report['response_count']} استجابة، وليس حجم نافذة سياق واحدة؛ والكاش والاستدلال جزءان من الإجمالي وليسا كميتين تُضافان مرة أخرى.", '',
        '## مطابقة العدّادات والفرق المرتبط بالضغط', '',
        '| الحقل | مجموع الاستجابات الفريدة | فرق event_msg | الفرق بينهما | استخدام استجابة الضغط |',
        '|---|---:|---:|---:|---:|']
    for key in COUNTERS:
        lines.append(f"| `{key}` | {totals[key]:,} | {report['event_counter_difference'][key]:,} | {report['sum_minus_event_difference'][key]:,} | {report['compaction_response_totals'][key]:,} |")
    lines += ['', f"طابق مجموع الاستجابات عدّاد الدور النهائي: **{checks['sum_equals_final_turn_counter']}**. وطابق فرق عدّاد الثريد: **{checks['sum_equals_thread_counter_difference']}**.",
        f"طابقت المجاميع المتدرجة عدّادي الدور والثريد عند جميع الاستجابات: **{checks['every_turn_prefix_matches'] and checks['every_thread_prefix_matches']}**.",
        f"طابق الاستخدام الأخير والزيادة في عدّاد الأحداث {report['response_event_increment_matches']} من {report['response_count']} استجابة. الفرق النهائي يساوي قراءة الضغط بكل العدّادات الستة: **{checks['sum_minus_event_difference_equals_compaction_usage']}**.",
        '- في هذه العينة، عدّاد `event_msg.info.total_token_usage` أقل من عدّاد الثريد بعد الضغط بمقدار استخدام استجابة الضغط. هذا اختلاف مُثبت بين مصدرَي القياس في السجل؛ لا يثبت عطلًا عامًا أو كيفية الفوترة.',
        '- قراءة الضغط المستقلة ونسختها داخل `compacted.latest_token_usage_record` تحملان المعرف نفسه والأرقام نفسها؛ احتُسبتا قراءة واحدة.',
        f"- توجد {len(report['invalid_counter_objects'])} لقطة غير صالحة للجمع كاستخدام استجابة. حُفظت بأرقامها وأسبابها في JSON ولم تُحوّل إلى استخدام صفري.", '',
        '## تسلسل الاستجابات', '',
        '| # | سطر الاستخدام | الصفة | الإدخال | منه كاش | غير كاش | الإخراج | منه استدلال | نتائج أدوات مسجلة منذ القراءة السابقة |',
        '|---:|---:|---|---:|---:|---:|---:|---:|---:|']
    for entry in report['timeline']:
        u = entry['usage']; label = 'ضغط — معرف صريح' if entry['is_explicit_compaction_response'] else 'استجابة'
        lines.append(f"| {entry['sequence']} | {entry['usage_record']['line']} | {label} | {u['input_tokens']:,} | {u['cached_input_tokens']:,} | {entry['input_not_reported_as_cached']:,} | {u['output_tokens']:,} | {u['reasoning_output_tokens']:,} | {len(entry['preceding_tool_results'])} |")
    lines += ['', 'عدد نتائج الأدوات هنا يصف ترتيب التسجيل فقط؛ لا يثبت أنها أُرسلت كلها في الطلب التالي أو أن تغيّر الإدخال سببه تلك النتائج وحدها.', '',
              '## موضع الضغط وتغيّر الإدخال', '']
    for transition in report['compaction_transitions']:
        lines += [f"- سجل الضغط: السطر {transition['record']['line']}، الحجم {transition['record']['bytes']:,} بايت، ومعرّف الاستجابة `{transition['compaction_response_id']}`.",
            f"- آخر استجابة عادية قبله: #{transition['last_ordinary_before_sequence']}، الإدخال {transition['input_before']:,}. أول استجابة عادية بعده: #{transition['first_ordinary_after_sequence']}، الإدخال {transition['input_after']:,}.",
            f"- التغير المرصود: {transition['input_change']:,} توكن. لا يمثل هذا بمفرده قياسًا سببيًا أو وفرًا ماليًا؛ عملية الضغط نفسها لها استخدام مسجل ضمن المجموع."]
    lines += ['', '## الأدوات والتخزين', '',
        f"رُصد {report['tool_call_records']} سجل استدعاء أداة و{report['tool_result_records']} سجل نتيجة. تطابق معرّف الاستدعاء لـ{report['tool_results_with_matching_turn_call_id']} نتيجة مع استدعاء داخل الدور. لم تُعامل نتائج الأدوات كإخراج جديد للنموذج.", '',
        '| نوع السجل | العدد | الحجم بالبايت | الحجم MiB |', '|---|---:|---:|---:|']
    for row in report['storage_by_record_type']:
        lines.append(f"| `{row['record_type']}` | {row['records']} | {row['bytes']:,} | {row['bytes']/1048576:.4f} |")
    lines += ['', '## حدود الدليل', '',
        '- ربط استجابة الضغط صريح بمعرفها. ربط عناصر `response_item` بالاستجابات العادية في هذه العينة يستند إلى معرف الدور وترتيب سجلات الاستخدام، ويظل استنتاجًا؛ المعرفات والحدود محفوظة في JSON.',
        '- لم يثبت عدد توكن كل صورة أو نص أو نتيجة أداة. لا تُحوّل نسب التخزين إلى نسب توكن، ولا يُعامل حجم سجل الضغط كله على أنه إخراج عملية الضغط.',
        '- هذه حالة واحدة. إثبات اتساق العدّادات في الملف لا يثبت فواتير الاستخدام أو أن التاريخ لم يُنسخ من جلسة سابقة.', '',
        '## التحقق والموارد', '',
        f"تحقق مستقل: {report['verification']['counter_objects_verified']} كائن عدّادات و{report['verification']['identifier_values_verified']} قيمة معرّف و{report['verification']['small_records_verified']} سجل صغير؛ النتيجة `{report['verification']['status']}`.",
        f"حجم دفعة قراءة المصدر: {CHUNK//1024} KiB. ذروة ذاكرة Python: {report['resources']['peak_working_set_bytes']/1048576:.2f} MiB. زمن التنفيذ: {report['resources']['elapsed_seconds']:.3f} ثانية، وزمن CPU: {report['resources']['cpu_seconds']:.3f} ثانية.",
        'قراءة متسلسلة بأولوية منخفضة، دون عمال متوازين أو اتصالات خارجية. التخزين المؤقت SQLite محلي ومؤقت. لم تُعدّل المصادر أو PostgreSQL، ولم تُعرض أو تُصدّر نصوص المحادثات أو الصور.', '']
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', default=components.WORKFLOW)
    parser.add_argument('--turn-id', default=TURN)
    args = parser.parse_args()
    started = time.monotonic(); cpu_started = time.process_time(); lower_priority()
    OUT.mkdir(parents=True, exist_ok=True); (ROOT / '.tmp').mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(dir=ROOT / '.tmp', prefix='turn-timeline-') as tmp:
        reader = TurnReader(Path(tmp) / 'scratch.sqlite3')
        try:
            evidence = components.read_file(args.source, reader, 16 * 1048576)
            report, verification_objects, selected_rows = build_timeline(evidence, reader.accounting_objects, args.turn_id)
            report['verification'] = verify(evidence, verification_objects, reader.identifier_evidence, report['boundary'])
            evidence_lines = {o['line'] for o in verification_objects} | {r['line'] for r in selected_rows}
            extracted = {'source': report['source'], 'turn_id': args.turn_id,
                'counter_objects': verification_objects,
                'identifiers': {line: spans for line, spans in reader.identifier_evidence.items() if line in evidence_lines and spans},
                'record_index': [{**locator(r), 'turn_id': r['turn_id'], 'response_id': r['response_id'],
                    'item_id': r['id'], 'call_id': r['call_id'], 'role': r['role'],
                    'output_category': r['output_category']} for r in evidence['rows'] if r['line'] in evidence_lines]}
            save(OUT / 'evidence.json', extracted)
        finally: reader.close()
    after = safe_source_path(args.source).stat()
    if (after.st_size, after.st_mtime_ns) != (evidence['source_bytes'], evidence['source_mtime_ns']):
        raise ValueError('source_changed_during_verification')
    report['completed_at'] = utc_now()
    report['resources'] = {'peak_working_set_bytes': peak_memory(),
        'elapsed_seconds': round(time.monotonic() - started, 3),
        'cpu_seconds': round(time.process_time() - cpu_started, 3), 'chunk_bytes': CHUNK,
        'full_source_passes': 2, 'source_bytes': evidence['source_bytes'],
        'bytes_read_including_independent_verification': 2 * evidence['source_bytes'] - evidence['blank_line_bytes'] + report['verification']['source_bytes_read']}
    save(OUT / 'summary.json', report)
    (OUT / 'report.md').write_text(markdown(report), encoding='utf-8')
    print(json.dumps({k: report[k] for k in ('status','response_count','totals','checks','verification','resources')}, indent=2))
    return 0 if report['status'].startswith('reconciled') else 1


if __name__ == '__main__':
    raise SystemExit(main())
