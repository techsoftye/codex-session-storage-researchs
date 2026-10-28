"""Phase 4: derive tables from existing structural observations only.

No database, original SQLite/JSONL, image-index, or network access. Public-source
findings below were reviewed separately at the pinned openai/codex commit.
"""
from collections import Counter, defaultdict
from datetime import datetime, timezone
import ctypes
from ctypes import wintypes
import hashlib
import json
from pathlib import Path
import statistics
import time

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'data/reports/logs_request_capture_v1'
SOURCE = OUT / 'observations.jsonl'
COMMIT = '6aaac694803004a63c6f393d153c96d22521a6b9'
BASE = 'https://github.com/openai/codex/blob/' + COMMIT + '/'
REVIEW = {
    'repository':'openai/codex','release':'rust-v0.164.0','release_published_at':'2026-10-23T04:48:10Z',
    'commit':COMMIT,
    'release_url':'https://github.com/openai/codex/releases/tag/rust-v0.164.0',
    'scope_limit':'This October public release is not established as the June capture build. Conditions explain possible mechanisms, not the cause of any specific observed full request. Server implementation was not inspected.',
    'findings':[
        {'topic':'WebSocket eligibility and incremental input',
         'finding':'Provider support and absence of forced fallback enable WebSockets. With a usable previous response, unchanged compared properties, and matching prior-input-plus-output prefix, only new items and previous_response_id are sent.',
         'sources':[BASE+'codex-rs/core/src/client.rs#L1143-L1154',BASE+'codex-rs/core/src/client.rs#L1530-L1595',BASE+'codex-rs/core/src/client.rs#L2165-L2191']},
        {'topic':'Full input on WebSocket',
         'finding':'Missing continuation state, changed compared properties, shortened or mismatching input select full current input. Closed connections reset continuation state. Full input is still possible over WebSocket.',
         'sources':[BASE+'codex-rs/core/src/client.rs#L430-L457',BASE+'codex-rs/core/src/client.rs#L1711-L1753',BASE+'codex-rs/core/src/client.rs#L2119-L2146']},
        {'topic':'Compaction',
         'finding':'Compaction installs replacement history. If that changes the previously known prefix, incremental reuse fails and the current compacted history is sent. This does not mean resending the entire original archive after every compaction.',
         'sources':[BASE+'codex-rs/core/src/compact.rs#L390-L435',BASE+'codex-rs/core/src/compact_remote_v2_installation.rs#L203-L253',BASE+'codex-rs/core/src/client.rs#L1548-L1564']},
        {'topic':'Transport failure',
         'finding':'HTTP 426 can request fallback. Eligible errors reaching the configured retry budget switch transport; the unbounded-connection-retry branch is an exception. HTTP builds the current request instead of the WebSocket delta. Not every WebSocket error immediately causes HTTP.',
         'sources':[BASE+'codex-rs/core/src/client.rs#L2085-L2108',BASE+'codex-rs/core/src/responses_retry.rs#L87-L139',BASE+'codex-rs/core/src/client.rs#L1865-L1925',BASE+'codex-rs/core/src/client.rs#L2448-L2463']},
        {'topic':'Resume and turn boundaries',
         'finding':'A fresh client starts without continuation state; restored history can therefore require full input. Warmup may carry it first. Ordinary new turns can reuse cached state; a turn boundary alone does not require full input.',
         'sources':[BASE+'codex-rs/core/src/client.rs#L590-L613',BASE+'codex-rs/core/src/client.rs#L690-L706',BASE+'codex-rs/core/src/client.rs#L1457-L1462',BASE+'codex-rs/core/src/client.rs#L2329-L2377']},
    ]}


def resources(lower=False):
    if not hasattr(ctypes,'windll'): return None
    kernel=ctypes.windll.kernel32
    kernel.GetCurrentProcess.restype=wintypes.HANDLE
    process=kernel.GetCurrentProcess()
    if lower:
        kernel.SetPriorityClass.argtypes=[wintypes.HANDLE,wintypes.DWORD]
        kernel.SetPriorityClass(process,0x4000)
    class Counters(ctypes.Structure):
        _fields_=[('cb',wintypes.DWORD),('PageFaultCount',wintypes.DWORD)]+[(name,ctypes.c_size_t) for name in
            ('PeakWorkingSetSize','WorkingSetSize','QuotaPeakPagedPoolUsage','QuotaPagedPoolUsage',
             'QuotaPeakNonPagedPoolUsage','QuotaNonPagedPoolUsage','PagefileUsage','PeakPagefileUsage')]
    info=Counters();info.cb=ctypes.sizeof(info)
    psapi=ctypes.windll.psapi
    psapi.GetProcessMemoryInfo.argtypes=[wintypes.HANDLE,ctypes.c_void_p,wintypes.DWORD]
    return int(info.PeakWorkingSetSize) if psapi.GetProcessMemoryInfo(process,ctypes.byref(info),info.cb) else None


def uncached(total,cached):
    if total is None or cached is None: return None
    if type(total) is not int or type(cached) is not int or not 0<=cached<=total:
        raise ValueError('invalid_cached_subset')
    return total-cached


def strict_sum(values):
    values=list(values)
    return None if any(v is None for v in values) else sum(values)


def request_summary(row,byid):
    groups=defaultdict(lambda:{'count':0,'logged_json_bytes':0})
    for item in row['input_items']:
        g=groups[item['type'] or '[type not available]']
        g['count']+=1;g['logged_json_bytes']+=item['logged_json_bytes']
    if sum(g['count'] for g in groups.values())!=row['input_count']: raise ValueError('input_count_mismatch')
    response=byid.get(row['linked_response_source_rowid'])
    if response:
        assert response['classification']=='response'
        assert response['thread_id']==row['thread_id']
        assert response['response_id']==row['linked_response_id']
        assert (response['ts'],response['ts_nanos']) >= (row['ts'],row['ts_nanos'])
        assert response['protocol_type']=='response.completed'
    usage={k:response[k] if response else None for k in ('input_tokens','cached_input_tokens','output_tokens')}
    images=[{'sha256':im['sha256'],'logged_string_bytes':im['logged_string_bytes'],
             'decoded_bytes':im['decoded_bytes'],'decode_error':im['decode_error']} for im in row['images']]
    return {
        'request_source_rowid':row['source_rowid'],'response_source_rowid':row['linked_response_source_rowid'],
        'thread_id':row['thread_id'],'research_session_id':row['research_session_id'],
        'response_id':row['linked_response_id'],'model':row['model'],
        'mode':'previous_response' if row['previous_response_id'] else 'full_without_previous',
        'transport_shape':'websocket_response_create' if row['protocol_type']=='response.create' else 'http_shaped',
        'link_method':row['link_method'],'link_notes':row['link_notes'],
        'previous_response_id':row['previous_response_id'],
        'captured_request_json_bytes':row['captured_json_bytes'],
        'input_count':row['input_count'],'input_item_bytes':sum(g['logged_json_bytes'] for g in groups.values()),
        'input_by_type':dict(sorted(groups.items())),
        'tools_count':row['tools_count'],'tools_bytes':row['tools_bytes'],
        'image_count':row['image_count'],'image_logged_string_bytes':strict_sum(im['logged_string_bytes'] for im in images),
        'image_decoded_bytes':strict_sum(im['decoded_bytes'] for im in images),'images':images,
        **usage,'uncached_input_tokens_derived':uncached(usage['input_tokens'],usage['cached_input_tokens']),
    }


def describe(values):
    values=[v for v in values if v is not None]
    return {'n':len(values),'min':min(values),'median':statistics.median(values),'max':max(values)} if values else {'n':0,'min':None,'median':None,'max':None}


def derive(rows):
    byid={r['source_rowid']:r for r in rows}
    assert len(byid)==len(rows)
    requests=[request_summary(r,byid) for r in sorted(rows,key=lambda r:(r['ts'],r['ts_nanos'],r['source_rowid'])) if r['classification']=='request']
    linked=[r for r in requests if r['response_source_rowid'] is not None]
    assert len({r['response_id'] for r in linked})==len(linked)
    modes=[]
    for mode in ('previous_response','full_without_previous'):
        all_mode=[r for r in requests if r['mode']==mode]
        paired=[r for r in linked if r['mode']==mode]
        modes.append({'mode':mode,'requests':len(all_mode),'paired_requests':len(paired),
            'unpaired_requests':len(all_mode)-len(paired),'paired_models':dict(Counter(r['model'] for r in paired)),
            **{field:describe(r[field] for r in paired) for field in
               ('captured_request_json_bytes','input_item_bytes','input_tokens','cached_input_tokens','uncached_input_tokens_derived','output_tokens')}})
    special=[r.copy() for r in requests if r['thread_id'].startswith('019eb815')]
    assert len(special)==5 and len({r['thread_id'] for r in special})==1
    previous=None
    for r in special:
        assert r['mode']=='full_without_previous' and r['input_tokens'] is None and r['response_id'] is None
        for field in ('input_count','input_item_bytes','captured_request_json_bytes'):
            r['delta_'+field]=r[field]-previous[field] if previous else None
        previous=r
    hashes={im['sha256'] for r in special for im in r['images']}
    assert len(hashes)==1 and None not in hashes and all(r['image_count']==1 for r in special)
    special_summary={'thread_id':special[0]['thread_id'],'requests':len(special),'token_readings':0,
        'input_count_growth':special[-1]['input_count']-special[0]['input_count'],
        'input_item_bytes_growth':special[-1]['input_item_bytes']-special[0]['input_item_bytes'],
        'request_json_bytes_growth':special[-1]['captured_request_json_bytes']-special[0]['captured_request_json_bytes'],
        'unique_image_count':1,'same_image_sha256':next(iter(hashes)),
        'single_image_decoded_bytes':special[0]['image_decoded_bytes'],
        'single_image_logged_string_bytes':special[0]['image_logged_string_bytes'],
        'five_image_logged_string_bytes':sum(r['image_logged_string_bytes'] for r in special),
        'repeat_image_logged_string_bytes_after_first':sum(r['image_logged_string_bytes'] for r in special[1:])}
    return {'source_observation_rows':len(rows),'requests':len(requests),'linked_requests':len(linked),
        'unlinked_requests':len(requests)-len(linked),'linked_request_table':linked,
        'all_request_table':requests,'mode_comparison':modes,'five_full_requests':special,
        'five_full_requests_summary':special_summary}


def num(value): return 'NULL' if value is None else f'{value:,}'


def markdown(report):
    d=report['analysis'];special=d['five_full_requests_summary']
    lines=['# Phase 4 — logged request size and response token usage','',
        'Research evidence: existing observations.jsonl only. No original session files, SQLite sources, image index or additional database tables were read. No database writes or API experiments were performed.',
        f"Source SHA-256: `{report['source_sha256']}`. {d['linked_requests']} existing request/response pairs; all links remain **order_inferred**, not explicit-ID proof.",'',
        '## Definitions and interpretation','',
        '- **previous_response** sends a reference plus supplied input. Its input_tokens accounts for processed context including prior context reconstructed by the server, not just the new bytes in that request. The exact old/new token split is unavailable.',
        '- **full_without_previous** means the logged request has input and no previous_response_id. It means full current supplied input, not every historical record or all hidden server context. Both modes occur over WebSocket; mode is not transport.',
        '- Request B is captured_json_bytes: the logged JSON object, not measured network bytes. Compression, framing and headers are not measured.',
        '- Input B sums logged_json_bytes of each input item (excluding array separators). Type breakdowns include only actual observed item types.',
        '- Tools B is the logged tools-array size. Image logged B is the JSON string containing its data URL; decoded B is binary-image size. Images are already included inside input/request bytes: do not add them again.',
        '- Uncached input = input_tokens - cached_input_tokens, only when both are present and valid. Cached tokens remain part of input; output is separate. Missing remains NULL. These are counts, not price calculations.',
        '- Bytes are not tokens. Different models, histories, tools and image content confound these observations. This is a descriptive case study, not a controlled comparison or universal conversion ratio.','',
        '## Linked requests and their response usage','',
        'All sizes below are exact bytes. Request/response numbers are source log row IDs. P = previous_response; F = full_without_previous.',
        '', '| Request → response | Mode | Link | Input count | Input B | Request B | Tools count / B | Images count / logged B / decoded B | Input tokens | Cached input | Uncached input | Output tokens |',
        '|---|---|---|---:|---:|---:|---|---|---:|---:|---:|---:|']
    for r in d['linked_request_table']:
        lines.append(f"| {r['request_source_rowid']} → {r['response_source_rowid']} | {'P' if r['mode']=='previous_response' else 'F'} | {r['link_method']} | {num(r['input_count'])} | {num(r['input_item_bytes'])} | {num(r['captured_request_json_bytes'])} | {r['tools_count']} / {num(r['tools_bytes'])} | {r['image_count']} / {num(r['image_logged_string_bytes'])} / {num(r['image_decoded_bytes'])} | {num(r['input_tokens'])} | {num(r['cached_input_tokens'])} | {num(r['uncached_input_tokens_derived'])} | {num(r['output_tokens'])} |")
    lines += ['', '### Input items by type for every linked request','',
              '| Request | Model | Item type | Count | Logged JSON bytes |','|---:|---|---|---:|---:|']
    for r in d['linked_request_table']:
        for kind,g in r['input_by_type'].items():
            lines.append(f"| {r['request_source_rowid']} | {r['model']} | {kind} | {g['count']} | {num(g['logged_json_bytes'])} |")
    lines += ['', '## Descriptive comparison of the two modes','',
              'Ranges and medians below use paired requests only; unpaired requests contribute no token values.',
              '', '| Mode | All requests | Paired | Unpaired | Request B: min / median / max | Input tokens: min / median / max |',
              '|---|---:|---:|---:|---|---|']
    for m in d['mode_comparison']:
        span=lambda field:' / '.join(num(m[field][k]) for k in ('min','median','max'))
        lines.append(f"| {m['mode']} | {m['requests']} | {m['paired_requests']} | {m['unpaired_requests']} | {span('captured_request_json_bytes')} | {span('input_tokens')} |")
    lines += ['', 'The paired modes have **no model in common**: previous_response uses gpt-5.4 (17) and gpt-5.3-codex-spark (2); full input uses gpt-5.5 (1) and gpt-5.4-mini (1). With only two paired full-input requests, no causal ranking or savings percentage is justified.',
        '', 'Example: request 340384 carries 97 B of input items and 22,253 B of logged request JSON, yet its linked response reports 97,548 input tokens (97,152 cached; 396 uncached). A full-input request, 1806069, logs 4,350,704 B including three images but reports 58,781 input tokens. These are different contexts/models and demonstrate why local byte size cannot substitute for usage counters.',
        '', '## Five full-input requests in conversation 019eb815','',
        f"Technical conversation UUID: `{special['thread_id']}`. All five are HTTP-shaped logs, with no paired response or token reading in the allowed evidence. This does not mean zero tokens were consumed.",
        '', '| Request | Input count | Δ count | Input B | Δ input B | Request B | Δ request B | Tools B | Image logged B / decoded B | Input tokens |',
        '|---:|---:|---:|---:|---:|---:|---:|---:|---|---|']
    for r in d['five_full_requests']:
        lines.append(f"| {r['request_source_rowid']} | {num(r['input_count'])} | {num(r['delta_input_count'])} | {num(r['input_item_bytes'])} | {num(r['delta_input_item_bytes'])} | {num(r['captured_request_json_bytes'])} | {num(r['delta_captured_request_json_bytes'])} | {num(r['tools_bytes'])} | {num(r['image_logged_string_bytes'])} / {num(r['image_decoded_bytes'])} | NULL |")
    lines += ['', f"Input grows by {num(special['input_count_growth'])} items and {num(special['input_item_bytes_growth'])} B; logged request JSON grows by {num(special['request_json_bytes_growth'])} B. Growth does not prove unchanged prefixes because no content-item fingerprints were retained.",
        '', f"The same image hash occurs once in every request: `{special['same_image_sha256']}`. Each copy is {num(special['single_image_decoded_bytes'])} decoded bytes and {num(special['single_image_logged_string_bytes'])} logged string bytes. The five logged copies occupy {num(special['five_image_logged_string_bytes'])} B; the four repeats after the first occupy {num(special['repeat_image_logged_string_bytes_after_first'])} B. This establishes repetition in logged request payloads, not a known token cost or exact physical network transfer.",
        '', '### Input-type growth in the five requests','',
        '| Request | Type | Count | Logged JSON bytes |','|---:|---|---:|---:|']
    for r in d['five_full_requests']:
        for kind,g in r['input_by_type'].items(): lines.append(f"| {r['request_source_rowid']} | {kind} | {g['count']} | {num(g['logged_json_bytes'])} |")
    lines += ['', '## Public-code review','',
        f"Reviewed [openai/codex {REVIEW['release']}]({REVIEW['release_url']}), commit `{COMMIT}`, published 2026-10-23. {REVIEW['scope_limit']}",'']
    for finding in REVIEW['findings']:
        links='; '.join(f"[{'/'.join(url.split('/')[7:]).split('#')[0]} {url.split('#')[1]}]({url})" for url in finding['sources'])
        lines.append(f"- **{finding['topic']}:** {finding['finding']} {links}")
    lines += ['', 'The public client deliberately omits known prior input and server-returned output when it can continue a response. This supports the contextual interpretation above; it does not expose the server tokenizer, per-item token attribution, or the reason for the five observed full sends. Compaction-item presence alone does not date a compaction or prove it triggered a resend.',
        '', '## Validation and resources','',
        '- 105 existing observations; 27 requests; 21 unique paired response IDs. Every reused pair has the same thread, chronological order and completed-response type. Type counts and byte totals reconcile. Source file fingerprint is unchanged.',
        '- Synthetic checks cover missing counters, invalid cached subsets and image-size missingness. No new response links were inferred and no database/source rows were changed.',
        f"- Analysis seconds: {report['elapsed_seconds']}; peak Python working set: {num(report['peak_working_set_bytes'])} bytes. Public-source review time is excluded.",
        '- Phase 4 complete. Further analysis requires researcher approval.', '']
    return '\n'.join(lines)


def main():
    resources(lower=True);started=time.perf_counter()
    # Small synthetic checks; all missing-value semantics stay explicit.
    assert uncached(100,80)==20 and uncached(None,0) is None
    assert strict_sum([1,None]) is None and strict_sum([])==0
    try: uncached(1,2)
    except ValueError: pass
    else: raise AssertionError('invalid_cached_subset_not_rejected')
    digest=hashlib.sha256();rows=[]
    with SOURCE.open('rb') as stream:
        for line in stream:
            digest.update(line)
            if line.strip(): rows.append(json.loads(line))
    analysis=derive(rows)
    assert (analysis['source_observation_rows'],analysis['requests'],analysis['linked_requests'])==(105,27,21)
    assert [m['paired_requests'] for m in analysis['mode_comparison']]==[19,2]
    assert all(r['link_method']=='order_inferred' for r in analysis['linked_request_table'])
    assert not set(analysis['mode_comparison'][0]['paired_models']) & set(analysis['mode_comparison'][1]['paired_models'])
    report={'phase':4,'generated_at':datetime.now(timezone.utc).isoformat(),'source':'observations.jsonl',
        'source_sha256':digest.hexdigest(),'analysis':analysis,'public_code_review':REVIEW,
        'elapsed_seconds':round(time.perf_counter()-started,4),'peak_working_set_bytes':resources()}
    with SOURCE.open('rb') as stream:
        assert hashlib.file_digest(stream,'sha256').hexdigest()==digest.hexdigest()
    (OUT/'phase4.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    (OUT/'phase4.md').write_text(markdown(report),encoding='utf-8')
    print(json.dumps({'linked_requests':analysis['linked_requests'],'mode_comparison':analysis['mode_comparison'],
        'five_full_requests_summary':analysis['five_full_requests_summary'],
        'elapsed_seconds':report['elapsed_seconds'],'peak_working_set_bytes':report['peak_working_set_bytes']}))


if __name__=='__main__': main()
