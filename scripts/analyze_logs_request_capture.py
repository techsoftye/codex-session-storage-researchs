"""Private structural case study of copied logs_2; standard library + psql.

Default builds reports with read-only DB access. --apply also persists metadata
to the separately migrated table 006. All SQLite reads use disposable clones.
No source JSONL reads, remote requests, conversation exports, image rendering,
token estimates or cross-scope sums. Bounds fail rather than silently skip data.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import re
import shutil
import sqlite3
import tempfile
import time

import import_codex_sqlite as common
from import_session_survey import run_psql
from profile_compacted_images import ImageString

ROOT = common.ROOT
OUT = ROOT / 'data/reports/logs_request_capture_v1'
IMAGE_INDEX = ROOT / 'data/reports/compacted_image_duplicates_v1/image_index.sqlite3'
VERSION = '1.0.0'
MAX_BODY = 8 * 1024 * 1024
MAX_CANDIDATES = 2000
COUNTERS = ('input_tokens','cached_input_tokens','cache_write_input_tokens',
            'output_tokens','reasoning_output_tokens','total_tokens')
NEEDLES = ('"tools"','"input"','previous_response_id','input_tokens',r'\"tools\"',r'\"input\"')
SAFE_KEYS = frozenset('''type model input tools response usage id status event_id previous_response_id
store instructions stream parallel_tool_calls tool_choice reasoning text include max_output_tokens metadata
service_tier prompt_cache_key temperature top_p input_tokens output_tokens total_tokens input_tokens_details
output_tokens_details cached_tokens reasoning_tokens cache_write_tokens object created_at error incomplete_details
output truncation safety_identifier tool_outputs response_id request_id client_request_id sequence_number
client_metadata item_id output_index item content role summary encrypted_content arguments call_id name
image_url url detail format effort verbosity token_budget max_tokens background billing completed_at max_tool_calls
prompt prompt_cache_retention frequency_penalty presence_penalty seed logprobs top_logprobs user conversation
content_filter_results modalities audio prompt_tokens completion_tokens prompt_tokens_details completion_tokens_details
cached_input_tokens cache_write_input_tokens reasoning_output_tokens function namespace parameters strict description
properties required additionalProperties input_schema tool_input tool_result result code message error_code'''.split())
ITEM_TYPES = frozenset('''message reasoning function_call function_call_output custom_tool_call custom_tool_call_output
input_text output_text input_image image_url input_audio output_audio refusal item_reference compaction
web_search_call computer_call computer_call_output tool_search_call tool_search_output image_generation_call
function custom namespace file_search_call local_shell_call local_shell_call_output mcp_call mcp_list_tools
mcp_approval_request mcp_approval_response input_file'''.split())
PROTOCOL_TYPES = frozenset(('response.create','response.created','response.in_progress','response.completed',
                           'response.failed','response.incomplete','response.output_item.done',
                           'response.custom_tool_call_input.done','response.function_call_arguments.done','error'))
TRUNCATION_PATTERNS = {
    'chars_truncated': re.compile(r'(?:\u2026|\.{3})?\s*\d[\d,]*\s+chars?\s+truncated', re.I),
    'output_truncated': re.compile(r'(?:\[|\u2026|\.{3})\s*(?:output|content|response)\s+(?:was\s+)?truncated', re.I),
    'omitted_count': re.compile(r'(?:\[|\u2026|\.{3})\s*\d[\d,]*\s+(?:chars?|bytes?|lines?|tokens?)\s+omitted', re.I),
}


def encoded(value):
    return json.dumps(value, ensure_ascii=True, separators=(',', ':'), allow_nan=False)


def safe_key(value):
    return value if value in SAFE_KEYS else 'key_sha256:' + hashlib.sha256(value.encode()).hexdigest()


def safe_path(path):
    return '/' + '/'.join(str(p) if isinstance(p, int) else safe_key(p) for p in path)


def uuid(value):
    return value.lower() if isinstance(value, str) and re.fullmatch(common.UUID_PATTERN,value,re.I) else None


def response_id(value):
    return value if isinstance(value,str) and re.fullmatch(r'resp_[A-Za-z0-9_-]{1,180}',value) else None


def request_id(value):
    if uuid(value): return uuid(value)
    return value if isinstance(value,str) and re.fullmatch(r'(?:req_[A-Za-z0-9_-]{1,180}|[0-9a-f]{32,64})',value) else None


def safe_model(value):
    return value if isinstance(value,str) and re.fullmatch(r'(?:gpt-|codex-|o[1-9])[A-Za-z0-9_.-]{0,96}',value) else None


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result: raise ValueError('duplicate_json_key')
        result[key] = value
    return result


def invalid_constant(_):
    raise ValueError('nonfinite_json_number')


DECODER = json.JSONDecoder(object_pairs_hook=unique_object, parse_constant=invalid_constant)
JSON_START = re.compile(r'\{\s*"(?:type|model|input|tools|response|usage|id|event_id)"\s*:')


class Spans:
    """Exact character spans, without decoding/retaining a second copy of strings."""
    def __init__(self, text, start):
        self.text = text; self.pos = start; self.spans = {}; self.nodes = 0
        self.value((), 0)

    def whitespace(self):
        while self.pos < len(self.text) and self.text[self.pos] in ' \t\r\n': self.pos += 1

    def string_end(self):
        start = self.pos; self.pos += 1
        while True:
            quote = self.text.find('"', self.pos)
            if quote < 0: raise ValueError('unterminated_string')
            slash = quote-1
            while slash >= start and self.text[slash] == '\\': slash -= 1
            self.pos = quote+1
            if (quote-slash-1) % 2 == 0: return

    def value(self, path, depth):
        self.nodes += 1
        if depth > 128 or self.nodes > 200000: raise ValueError('structural_bound_exceeded')
        self.whitespace(); start = self.pos; char = self.text[self.pos]
        if char == '"':
            self.string_end()
        elif char in '{[':
            closing = '}' if char == '{' else ']'; self.pos += 1; self.whitespace(); index = 0
            if self.text[self.pos] != closing:
                while True:
                    if char == '{':
                        key_start = self.pos; self.string_end()
                        key = json.loads(self.text[key_start:self.pos])
                        self.whitespace()
                        if self.text[self.pos] != ':': raise ValueError('invalid_colon')
                        self.pos += 1
                    else: key = index
                    self.value(path+(key,), depth+1); index += 1; self.whitespace()
                    if self.text[self.pos] == closing: break
                    if self.text[self.pos] != ',': raise ValueError('invalid_separator')
                    self.pos += 1; self.whitespace()
            self.pos += 1
        else:
            while self.pos < len(self.text) and self.text[self.pos] not in ',}] \t\r\n': self.pos += 1
        self.spans[path] = (start, self.pos)

    def size(self, path):
        a,b = self.spans[path]
        return sum(len(self.text[i:min(i+65536,b)].encode('utf-8')) for i in range(a,b,65536))


def extract_protocol(body):
    match = JSON_START.search(body)
    if not match: return None, None, None, 'no_protocol_json_found'
    try:
        obj,end = DECODER.raw_decode(body, match.start())
    except (ValueError, RecursionError) as error:
        truncated = isinstance(error,json.JSONDecodeError) and (
            error.pos >= len(body.rstrip())-1 or error.msg.startswith('Unterminated'))
        return None, match.start(), None, 'suspected_truncated_json' if truncated else 'invalid_json'
    return obj, match.start(), end, 'complete_json' if not body[end:].strip() else 'json_with_trailing_content'


def counter_values(payload):
    usage = payload.get('usage')
    values = dict.fromkeys(COUNTERS); provenance = {}; issues = []
    aliases = {
        'cached_input_tokens':('input_tokens_details','cached_tokens'),
        'cache_write_input_tokens':('input_tokens_details','cache_write_tokens'),
        'reasoning_output_tokens':('output_tokens_details','reasoning_tokens'),
    }
    if isinstance(usage,dict):
        for name in COUNTERS:
            options = []
            if name in usage: options.append(('usage.'+name,usage[name]))
            if name in aliases:
                group,key = aliases[name]
                if isinstance(usage.get(group),dict) and key in usage[group]:
                    options.append(('usage.'+group+'.'+key,usage[group][key]))
            provenance[name] = [path for path,_ in options]
            if any(type(v) is not int or not 0 <= v < 2**63 for _,v in options):
                issues.append(name+':invalid_value')
            elif options and len({v for _,v in options}) > 1:
                issues.append(name+':conflicting_aliases')
            elif options: values[name] = options[0][1]
        if all(values[k] is not None for k in ('input_tokens','output_tokens','total_tokens')):
            if values['input_tokens']+values['output_tokens'] != values['total_tokens']:
                issues.append('input_output_total_mismatch')
    return values, {'scope':'response' if isinstance(usage,dict) else None,
                    'usage_present':'usage' in payload, 'provenance':provenance,
                    'missing_fields':[k for k,v in values.items() if v is None], 'issues':issues}


def walk(value, path=()):
    yield path,value
    if isinstance(value,dict):
        for key,child in value.items(): yield from walk(child,path+(key,))
    elif isinstance(value,list):
        for index,child in enumerate(value): yield from walk(child,path+(index,))


def image_metadata(value, stored_bytes):
    result = []
    def emit(digest,decoded,stored,error,padded):
        result.append({'sha256':digest.hex() if digest else None,'decoded_bytes':decoded if not error else None,
                       'logged_string_bytes':stored,'decode_error':error,'added_base64_padding':padded})
    decoder = ImageString(emit)
    for i in range(0,len(value),65536): decoder.feed(value[i:i+65536].encode('utf-8'))
    decoder.finish(stored_bytes)
    return result[0] if result else {'sha256':None,'decoded_bytes':None,'logged_string_bytes':stored_bytes,
                                    'decode_error':'unsupported_data_url','added_base64_padding':False}


def observe(source, body, digest):
    raw_size = len(body.encode('utf-8'))
    if raw_size > MAX_BODY: raise ValueError('log_body_bound_exceeded')
    if source.get('thread_id') is not None and uuid(source['thread_id']) is None:
        raise ValueError('non_uuid_thread_identifier')
    row = dict(source_database='logs_2.sqlite',source_rowid=source['rowid'],source_db_sha256=digest,
               research_session_id=source.get('research_session_id'),thread_id=uuid(source.get('thread_id')),
               ts=source['ts'],ts_nanos=source['ts_nanos'],log_body_bytes=raw_size,
               log_body_sha256=hashlib.sha256(body.encode()).hexdigest(),classification='other',
               classification_basis='keyword_only',protocol_type=None,capture_status=None,
               json_start_byte=None,json_end_byte=None,captured_json_bytes=None,
               top_level_keys=[],response_keys=[],truncation_evidence={'external':[],'embedded':[]},
               previous_response_id_present=False,previous_response_id=None,request_context_mode=None,
               store_present=False,store=None,model_present=False,model=None,input_count=None,input_items=[],
               tools_count=None,tools_bytes=None,image_count=0,images=[],response_id=None,request_id=None,
               counter_metadata={},linked_response_id=None,linked_response_source_rowid=None,
               link_method=None,link_notes=[],token_comparison={},analysis_version=VERSION,**dict.fromkeys(COUNTERS))
    obj,start,end,status = extract_protocol(body); row['capture_status']=status
    if not isinstance(obj,dict): return row
    spans = Spans(body,start)
    if spans.pos != end: raise ValueError('span_decoder_disagreement')
    row.update(json_start_byte=len(body[:start].encode()),json_end_byte=len(body[:end].encode()),
               captured_json_bytes=spans.size(()),top_level_keys=sorted(safe_key(k) for k in obj))
    typ = obj.get('type'); row['protocol_type']=typ if isinstance(typ,str) and typ in PROTOCOL_TYPES else None
    if typ == 'response.create':
        row.update(classification='request',classification_basis='response.create')
    elif isinstance(obj.get('input'),list) and isinstance(obj.get('model'),str) and 'response' not in obj:
        row.update(classification='request',classification_basis='model_and_input_request_shape')
    elif typ in ('response.created','response.in_progress','response.completed','response.failed','response.incomplete') and isinstance(obj.get('response'),dict):
        row.update(classification='response',classification_basis='response_event_with_response_object')
    elif row['protocol_type']:
        row['classification_basis']='protocol_item_event_not_model_request'
    payload = obj['response'] if row['classification']=='response' else obj
    base = ('response',) if row['classification']=='response' else ()
    if base: row['response_keys']=sorted(safe_key(k) for k in payload)
    row.update(previous_response_id_present='previous_response_id' in payload,
               previous_response_id=response_id(payload.get('previous_response_id')),
               store_present='store' in payload,store=payload.get('store') if type(payload.get('store')) is bool else None,
               model_present='model' in payload,model=safe_model(payload.get('model')),
               response_id=response_id(payload.get('id')) if row['classification']=='response' else response_id(payload.get('response_id')),
               request_id=request_id(payload.get('request_id')) or request_id(payload.get('client_request_id')) or request_id(obj.get('request_id')))
    if row['classification']=='request':
        row['request_context_mode']='references_previous_response' if row['previous_response_id'] else (
            'input_without_previous_reference' if payload.get('previous_response_id') in (None,'') else 'invalid_previous_reference')
        if isinstance(payload.get('input'),list):
            row['input_count']=len(payload['input'])
            for index,item in enumerate(payload['input']):
                kind=item.get('type') if isinstance(item,dict) else None
                role=item.get('role') if isinstance(item,dict) else None
                row['input_items'].append({'index':index,'type':kind if isinstance(kind,str) and kind in ITEM_TYPES else None,
                    'role':role if role in ('system','developer','user','assistant','tool') else None,
                    'json_type':type(item).__name__,'logged_json_bytes':spans.size(('input',index))})
        if isinstance(payload.get('tools'),list):
            row['tools_count']=len(payload['tools']); row['tools_bytes']=spans.size(('tools',))
    if row['classification']=='response':
        values,meta=counter_values(payload); row.update(values); row['counter_metadata']=meta
    outside=(body[:start],body[end:])
    for name,pattern in TRUNCATION_PATTERNS.items():
        n=sum(len(pattern.findall(text)) for text in outside)
        if n: row['truncation_evidence']['external'].append({'pattern':name,'count':n})
    for path,value in walk(obj):
        if not isinstance(value,str): continue
        for name,pattern in TRUNCATION_PATTERNS.items():
            n=len(pattern.findall(value))
            if n: row['truncation_evidence']['embedded'].append({'path':safe_path(path),'pattern':name,'count':n})
        if value.startswith('data:image/'):
            item=image_metadata(value,spans.size(path)); item['path']=safe_path(path)
            row['images'].append(item)
        elif path and path[-1] in ('image_url','url') and isinstance(value,str) and value.startswith(('https://','http://')):
            # No external image retrieval or URL export.
            if 'image_url' in path:
                row['images'].append({'path':safe_path(path),'sha256':None,'decoded_bytes':None,
                    'logged_string_bytes':spans.size(path),'decode_error':'external_image_not_fetched','added_base64_padding':False})
    row['image_count']=len(row['images'])
    return row


def chronology(row): return row['ts'],row['ts_nanos'],row['source_rowid']


def link_requests(rows):
    groups=defaultdict(list)
    for row in rows:
        if row['classification']=='response' and row['thread_id'] and row['response_id']:
            groups[(row['thread_id'],row['response_id'])].append(row)
    response_groups=[]
    for (thread,rid),events in groups.items():
        events.sort(key=chronology)
        terminal=[e for e in events if e['protocol_type'] in ('response.completed','response.failed','response.incomplete')]
        response_groups.append({'thread_id':thread,'response_id':rid,'first':events[0],
            'terminal':terminal[-1] if terminal else None,'events':events})
    requests=sorted((r for r in rows if r['classification']=='request'),key=chronology)
    for index,row in enumerate(requests):
        row['link_notes']=[]
        if not row['thread_id']:
            row['link_notes']=['missing_thread_id']; continue
        same=[g for g in response_groups if g['thread_id']==row['thread_id']]
        explicit=[g for g in same if (row['response_id'] and g['response_id']==row['response_id']) or
                  (row['request_id'] and any(e['request_id']==row['request_id'] for e in g['events']))]
        if len(explicit)==1:
            match=explicit[0]; method='explicit_id'
            if chronology(match['first']) <= chronology(row):
                row['link_notes']=['explicit_response_precedes_request']; continue
        elif len(explicit)>1:
            row['link_notes']=['conflicting_explicit_identifiers']; continue
        else:
            future=sorted((g for g in same if chronology(g['first'])>chronology(row)),key=lambda g:chronology(g['first']))
            if not future: row['link_notes']=['no_later_response_in_capture']; continue
            match=future[0]
            intervening=any(r['thread_id']==row['thread_id'] and chronology(row)<chronology(r)<chronology(match['first']) for r in requests[index+1:])
            if intervening:
                row['link_notes']=['another_request_before_next_response']; continue
            method='order_inferred'; row['link_notes']=['chronological_same_thread_not_causal_proof']
        event=match['terminal'] or match['first']
        row.update(linked_response_id=match['response_id'],linked_response_source_rowid=event['source_rowid'],link_method=method)
        if match['terminal'] is None: row['link_notes'].append('terminal_response_not_captured')
        readings={tuple(e[k] for k in COUNTERS) for e in match['events']
                  if e['counter_metadata'].get('scope')=='response'}
        if len(readings)>1: row['link_notes'].append('response_usage_snapshots_differ')
    return response_groups


@contextmanager
def copied_image_index(root=ROOT, source=IMAGE_INDEX):
    scratch_root=Path(root)/'.tmp'; common.reject_links(scratch_root); scratch_root.mkdir(exist_ok=True)
    paths=[Path(str(source)+suffix) for suffix in ('','-wal','-shm')]
    before=[common.signature(p) for p in paths]
    if before[0] is None: raise ValueError('image_index_missing')
    with tempfile.TemporaryDirectory(prefix='logs-image-index-',dir=scratch_root) as temporary:
        target=Path(temporary).resolve()
        if target.parent!=scratch_root.resolve(): raise ValueError('unsafe_temporary_directory')
        for path,sig in zip(paths,before):
            if sig is not None: shutil.copyfile(path,target/path.name)
        if before!=[common.signature(p) for p in paths]: raise ValueError('image_index_changed_during_copy')
        con=sqlite3.connect((target/source.name).as_uri()+'?mode=ro',uri=True)
        try:
            con.execute('PRAGMA query_only=ON'); con.execute('PRAGMA cache_size=-2048'); con.execute('PRAGMA mmap_size=0')
            yield con
        finally: con.close()


def match_images(rows, con):
    cache={}
    for row in rows:
        for image in row['images']:
            digest=image['sha256']
            if not digest:
                image['index_match']=None; continue
            if digest not in cache:
                count,files,records,small,large=con.execute('''SELECT count(*),count(DISTINCT session_id),
                    count(DISTINCT record_id),min(decoded_bytes),max(decoded_bytes)
                    FROM occurrences WHERE sha256=? AND error IS NULL''',(bytes.fromhex(digest),)).fetchone()
                cache[digest]={'occurrences':count,'session_files':files,'records':records,'min_decoded_bytes':small,'max_decoded_bytes':large}
            image['index_match']=dict(cache[digest])
            image['index_match']['same_size']=bool(cache[digest]['occurrences'] and cache[digest]['min_decoded_bytes']==cache[digest]['max_decoded_bytes']==image['decoded_bytes'])
            image['index_match']['same_session_occurrences']=con.execute('''SELECT count(*) FROM occurrences
                WHERE sha256=? AND error IS NULL AND session_id=?''',(bytes.fromhex(digest),row['research_session_id'])).fetchone()[0] if row['research_session_id'] else None


def read_pg(sql):
    text=run_psql("BEGIN READ ONLY; SET LOCAL work_mem='8MB'; SET LOCAL max_parallel_workers_per_gather=0; "+sql+'; COMMIT;')
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def compare_tokens(rows, evidence):
    by_id=defaultdict(list)
    for e in evidence: by_id[e['response_id']].append(e)
    for row in rows:
        if row['classification']!='response' or row['counter_metadata'].get('scope')!='response': continue
        groups=by_id.get(row['response_id'],[]); comparisons=[]
        for e in groups:
            shared=[k for k in COUNTERS if row[k] is not None and e[k] is not None]
            comparisons.append({'usage_scope':e['usage_scope'],'research_session_id':e['research_session_id'],
                'observation_count':e['observation_count'],'compared_fields':shared,
                'differing_fields':[k for k in shared if row[k]!=e[k]],
                'log_missing_fields':[k for k in COUNTERS if row[k] is None],
                'database_missing_fields':[k for k in COUNTERS if e[k] is None],
                'same_research_session':row['research_session_id'] is not None and e['research_session_id']==row['research_session_id'],
                'all_six_equal':len(shared)==6 and all(row[k]==e[k] for k in shared),
                'response_scope':e['usage_scope']=='response'})
        row['token_comparison']={'response_id_found':bool(groups),'comparisons':comparisons}
    response_rows={r['source_rowid']:r for r in rows if r['classification']=='response'}
    for row in rows:
        if row['classification']=='request' and row['linked_response_source_rowid'] in response_rows:
            row['token_comparison']={'from_linked_response':row['linked_response_source_rowid'],
                                     **response_rows[row['linked_response_source_rowid']]['token_comparison']}


def compare_available_counters(rows):
    """Secondary numerical evidence only; never promotes a response identity link."""
    for row in rows:
        if row['classification']!='response' or row['input_tokens'] is None or not row['research_session_id']:
            continue
        present=[k for k in COUNTERS if row[k] is not None]
        if not present: continue
        conditions=' AND '.join('u.'+k+'='+str(row[k]) for k in present)
        matched=read_pg('''SELECT json_build_object('observation_count',count(*),
            'distinct_record_count',count(DISTINCT u.research_session_record_id),
            'observations_with_response_id',count(u.response_id))
            FROM public.research_token_usage_observations u
            JOIN public.research_session_records r ON r.id=u.research_session_record_id
            WHERE r.research_session_id='''+str(row['research_session_id'])+
            " AND u.usage_scope='last' AND "+conditions)[0]
        row['token_comparison']['same_file_last_counter_candidates']={
            'basis':'same_file_and_available_counters_only','identity_confirmed':False,
            'usage_scope':'last','compared_fields':present,
            'missing_log_fields':[k for k in COUNTERS if k not in present],**matched}
    response_rows={r['source_rowid']:r for r in rows if r['classification']=='response'}
    for row in rows:
        if row['classification']=='request' and row['linked_response_source_rowid'] in response_rows:
            row['token_comparison']={'from_linked_response':row['linked_response_source_rowid'],
                                     **response_rows[row['linked_response_source_rowid']]['token_comparison']}


def persist(rows):
    if not rows: return {'rows':0,'inserted_rows':0}
    columns=tuple(rows[0]); names=', '.join(common.quoted(k) for k in columns)
    chunks=["BEGIN; SET LOCAL lock_timeout='5s'; SET LOCAL work_mem='8MB'; SET LOCAL max_parallel_workers_per_gather=0;",
            'LOCK TABLE public.logs2_request_observations IN SHARE ROW EXCLUSIVE MODE;',
            'CREATE TEMP TABLE request_import_stage (row_data jsonb NOT NULL) ON COMMIT DROP;',
            'COPY request_import_stage (row_data) FROM STDIN;']
    chunks.extend(common.copy_value(encoded(row)) for row in rows); chunks.append(r'\.')
    chunks.append('CREATE TEMP TABLE request_typed_stage ON COMMIT DROP AS SELECT r.* FROM request_import_stage s CROSS JOIN LATERAL jsonb_populate_record(NULL::public.logs2_request_observations,s.row_data) r;')
    fields=', '.join('s.'+common.quoted(k) for k in columns)
    chunks.append(f'''CREATE TEMP TABLE request_inserted(n bigint) ON COMMIT DROP;
WITH added AS (INSERT INTO public.logs2_request_observations ({names})
 SELECT {fields} FROM request_typed_stage s ON CONFLICT (source_database,source_rowid) DO NOTHING RETURNING 1)
INSERT INTO request_inserted SELECT count(*) FROM added;''')
    differences=' OR '.join('t.'+common.quoted(k)+' IS DISTINCT FROM s.'+common.quoted(k) for k in columns)
    chunks.append(f'''DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM request_typed_stage s LEFT JOIN public.logs2_request_observations t
 USING (source_database,source_rowid) WHERE t.id IS NULL OR {differences})
 OR (SELECT count(*) FROM public.logs2_request_observations) <> {len(rows)} THEN
 RAISE EXCEPTION 'request_observation_verification_failed'; END IF; END $$;
SELECT json_build_object('rows',(SELECT count(*) FROM public.logs2_request_observations),
 'inserted_rows',(SELECT n FROM request_inserted)); COMMIT;''')
    return json.loads(run_psql('\n'.join(chunks)))


def summarize(rows, coverage):
    requests=[r for r in rows if r['classification']=='request']
    responses=[r for r in rows if r['classification']=='response']
    measured=[r for r in responses if r['counter_metadata'].get('scope')=='response']
    images=[im for r in requests for im in r['images']]
    valid_images=[im for im in images if im['sha256']]
    result={'candidate_log_rows':len(rows),'classification_counts':dict(Counter(r['classification'] for r in rows)),
        'total_log_rows':sum(c['log_rows'] for c in coverage),
        'matched_log_rows':sum(c['linked_log_rows'] for c in coverage),
        'log_rows_without_thread_id':sum(c['log_rows'] for c in coverage if c['thread_id'] is None),
        'largest_candidate_body_bytes':max((r['log_body_bytes'] for r in rows),default=0),
        'protocol_type_counts':dict(Counter(r['protocol_type'] or 'untyped' for r in rows)),
        'capture_status_counts':dict(Counter(r['capture_status'] for r in rows)),
        'request_context_modes':dict(Counter(r['request_context_mode'] for r in requests)),
        'request_link_methods':dict(Counter(r['link_method'] or 'unlinked' for r in requests)),
        'unlinked_request_reasons':dict(Counter(n for r in requests if not r['link_method'] for n in r['link_notes'])),
        'response_ids_without_linked_request':len({r['response_id'] for r in responses if r['response_id']}-{r['linked_response_id'] for r in requests if r['linked_response_id']}),
        'distinct_response_ids':len({r['response_id'] for r in responses if r['response_id']}),
        'response_usage_records':len(measured),
        'distinct_response_ids_with_usage':len({r['response_id'] for r in measured if r['response_id']}),
        'response_usage_missing_fields':dict(Counter(k for r in measured for k in r['counter_metadata']['missing_fields'])),
        'response_usage_rows_with_database_id_match':sum(bool(r['token_comparison'].get('response_id_found')) for r in measured),
        'response_usage_rows_with_same_session_present_fields_match':sum(any(c['response_scope'] and c['same_research_session'] and c['compared_fields'] and not c['differing_fields'] for c in r['token_comparison'].get('comparisons',[])) for r in measured),
        'response_usage_rows_with_strict_six_field_match':sum(any(c['response_scope'] and c['same_research_session'] and c['all_six_equal'] for c in r['token_comparison'].get('comparisons',[])) for r in measured),
        'response_usage_rows_in_linked_files':sum(r['research_session_id'] is not None for r in measured),
        'response_usage_rows_with_same_file_last_counter_candidates':sum(bool(r['token_comparison'].get('same_file_last_counter_candidates',{}).get('observation_count')) for r in measured),
        'external_truncation_marker_rows':sum(bool(r['truncation_evidence']['external']) for r in rows),
        'embedded_truncation_marker_rows':sum(bool(r['truncation_evidence']['embedded']) for r in rows),
        'request_image_occurrences':len(images),'decoded_request_image_occurrences':len(valid_images),
        'unique_decoded_request_images':len({im['sha256'] for im in valid_images}),
        'decoded_request_image_bytes':sum(im['decoded_bytes'] for im in valid_images),
        'unique_decoded_request_image_bytes':sum(next(im['decoded_bytes'] for im in valid_images if im['sha256']==h) for h in {im['sha256'] for im in valid_images}),
        'image_occurrences_matching_compacted_index':sum(bool(im.get('index_match',{}).get('same_size')) for im in valid_images),
        'unique_images_matching_compacted_index':len({im['sha256'] for im in valid_images if im.get('index_match',{}).get('same_size')}),
        'image_occurrences_matching_same_session':sum(bool(im.get('index_match',{}).get('same_session_occurrences')) for im in valid_images),
        'image_decode_errors':dict(Counter(im['decode_error'] for im in images if im['decode_error']))}
    for c in coverage:
        subset=[r for r in rows if r['thread_id']==c['thread_id']]
        c.update(candidate_rows=len(subset),request_rows=sum(r['classification']=='request' for r in subset),
                 response_rows=sum(r['classification']=='response' for r in subset),
                 other_rows=sum(r['classification']=='other' for r in subset))
    return result


def markdown(report):
    s=report['summary']
    lines=['# logs_2 request-capture case study','',
        'Scope: the copied research log database for 16 conversations, June 14–18. This is a case study, not a conclusion about the full session corpus.',
        '', '## Findings','',
        f"- Coverage: {s['linked_threads']} of {s['case_threads']} conversations match a research file; {s['linked_threads_with_candidates']} of those have candidate logs ({s['linked_candidate_rows']} candidate rows).",
        f"- The logs contain {s['classification_counts'].get('request',0)} structured model requests. {s['request_context_modes'].get('references_previous_response',0)} reference previous_response_id; the others have no previous-response reference. This does not certify complete reconstructed model input.",
        f"- Request links: {encoded(s['request_link_methods'])}. Unlinked reasons: {encoded(s['unlinked_request_reasons'])}. Another {s['response_ids_without_linked_request']} response IDs have no paired request in this capture; the event sequence is therefore not a complete set of request/response pairs.",
        f"- Shared response-ID matches with the token table: {s['response_usage_rows_with_database_id_match']}. Secondary same-file last-scope numerical candidates: {s['response_usage_rows_with_same_file_last_counter_candidates']}. These match available fields only; they do not confirm response identity.",
        f"- Request images: {s['request_image_occurrences']} occurrences, {s['unique_decoded_request_images']} unique decoded byte strings. {s['unique_images_matching_compacted_index']} unique images match the compacted index, covering {s['image_occurrences_matching_same_session']} occurrences in the same research files.",
        f"- Capture parsing: {encoded(s['capture_status_counts'])}. Detected external/embedded truncation-marker rows: {s['external_truncation_marker_rows']}/{s['embedded_truncation_marker_rows']}. This is detection under the documented patterns, not proof that every upstream string is unabridged.",
        '', '## Coverage','',
        '| Conversation UUID | Log rows | Matched log rows | Matched files | Candidates | Requests | Responses | Other |',
        '|---|---:|---:|---:|---:|---:|---:|---:|']
    for c in report['coverage']:
        lines.append(f"| {c['thread_id'] or '[no thread]'} | {c['log_rows']} | {c['linked_log_rows']} | {c['linked_files']} | {c['candidate_rows']} | {c['request_rows']} | {c['response_rows']} | {c['other_rows']} |")
    lines += ['', '## Measured results','']
    for key,value in s.items(): lines.append(f'- `{key}`: {encoded(value)}')
    lines += ['', '## Existing token observations in files with candidate logs','',
              '| Research file row | Counter scope | Observations | With response_id |',
              '|---:|---|---:|---:|']
    for c in report['token_source_coverage']:
        lines.append(f"| {c['file_id']} | {c['usage_scope']} | {c['observations']} | {c['with_response_id']} |")
    lines += ['', '## Request images by decoded-byte fingerprint','',
              '| SHA-256 | Decoded bytes | Request occurrences | Compacted-index match | Same-file occurrences |',
              '|---|---:|---:|---|---:|']
    for c in report['image_summary']:
        lines.append(f"| {c['sha256']} | {c['decoded_bytes']} | {c['request_occurrences']} | {c['index_match']} | {c['same_file_request_occurrences']} |")
    lines += ['', '## Interpretation and limits','',
        '- Candidate matching uses literal JSON keys tools/input (including escaped quotes), previous_response_id or input_tokens. Keywords alone do not establish a model request.',
        '- Complete JSON means the captured JSON object parses fully. It does not prove the log is the exact wire request, include server-added context, or contain unabridged embedded content.',
        '- A request with previous_response_id relies on previous response context. Its input array is the supplied additional input; the complete reconstructed context is not present in that single log object. Absence of the reference does not certify all server context.',
        '- Embedded truncation markers are distinguished from truncation outside the JSON or malformed JSON. They may originate in earlier tool output. Literal mentions of truncation/omission in instructions are not evidence of shortened logging.',
        '- explicit_id requires a current-response/request ID match. previous_response_id points backward and is never used as the current response ID. Order-inferred links stay within one thread; intervening requests cause an unresolved link.',
        '- Every response lifecycle event is retained. Created/in-progress snapshots are not added to completed usage. No token sums are computed across responses, scopes or copies.',
        '- Missing cache-write or other counters remain NULL. Partial agreement on present counters is reported separately from strict equality of all six counters.',
        '- No shared response_id means the identifier join cannot verify counter agreement; it is not evidence of counter disagreement. The secondary check matches available counters only within the same file and last scope. Such candidates do not establish response identity and are not mixed with cumulative counters.',
        '- Image SHA-256 uses the existing ImageString Base64 decoder on JSON-unescaped data URLs. No image codec, display, file export or URL retrieval occurs. Matches prove identical binary bytes, not visual similarity.',
        '- image_index.sqlite3 covers images inside compacted replacement_history/guardian_history. A miss does not prove absence from other record types or other corpora.',
        '- Byte sizes are exact UTF-8 spans in the logged JSON, including JSON escaping. They are not token counts and are not full transport bytes.',
        '- Read-only snapshots, one bounded body at a time (8 MiB hard limit), 256 KiB image decoding chunks or smaller, low process priority, 2 MiB SQLite cache, and 8 MB PostgreSQL work_mem. No original JSONL was read.',
        '', '## Execution','',f"- Elapsed seconds: {report['elapsed_seconds']}",
        f"- Peak Python working set bytes: {report['peak_working_set_bytes']}",
        f"- Database result: {encoded(report['database'])}",
        '- Memory measurement excludes PostgreSQL service memory. Stop after this phase pending researcher approval.', '']
    return '\n'.join(lines)


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__); parser.add_argument('--apply',action='store_true')
    args=parser.parse_args(argv); start=time.perf_counter(); common.lower_priority()
    try:
        pg_coverage=read_pg('''SELECT json_build_object('thread_id',thread_id,'log_rows',count(*),
            'linked_log_rows',count(*) FILTER (WHERE research_session_id IS NOT NULL),
            'linked_files',count(DISTINCT research_session_id)) FROM public.logs2_logs GROUP BY thread_id''')
        for c in pg_coverage:
            if c['thread_id'] is not None and not uuid(c['thread_id']): raise ValueError('invalid_thread_id')
        metadata={}; rows=[]
        with common.snapshot('logs_2.sqlite') as (con,digest):
            condition=' OR '.join('instr(feedback_log_body,?)>0' for _ in NEEDLES)
            candidate_count,largest=con.execute('SELECT count(*),max(length(CAST(feedback_log_body AS BLOB))) FROM logs WHERE '+condition,NEEDLES).fetchone()
            if candidate_count>MAX_CANDIDATES: raise ValueError('candidate_bound_exceeded')
            if largest is not None and largest>MAX_BODY: raise ValueError('log_body_bound_exceeded')
            selected=[r[0] for r in con.execute('SELECT rowid FROM logs WHERE '+condition,NEEDLES)]
            if len(selected)>MAX_CANDIDATES: raise ValueError('candidate_bound_exceeded')
            if selected:
                ids=','.join(str(int(x)) for x in selected)
                for item in read_pg('''SELECT json_build_object('rowid',source_rowid,'research_session_id',research_session_id,
                    'source_db_sha256',source_db_sha256,'body_bytes',feedback_log_body_length_bytes,
                    'body_sha256',feedback_log_body_sha256) FROM public.logs2_logs WHERE source_rowid IN ('''+ids+')'):
                    metadata[item['rowid']]=item
            if len(metadata)!=len(selected): raise ValueError('imported_source_coverage_mismatch')
            if any(x['source_db_sha256']!=digest for x in metadata.values()): raise ValueError('imported_snapshot_mismatch')
            for rowid,thread,ts,nanos,body in con.execute('SELECT rowid,thread_id,ts,ts_nanos,feedback_log_body FROM logs WHERE '+condition+' ORDER BY ts,ts_nanos,rowid',NEEDLES):
                source={'rowid':rowid,'thread_id':thread,'ts':ts,'ts_nanos':nanos,'research_session_id':metadata[rowid]['research_session_id']}
                if not isinstance(body,str): raise ValueError('body_not_text')
                row=observe(source,body,digest)
                if row['log_body_bytes']!=metadata[rowid]['body_bytes'] or row['log_body_sha256']!=metadata[rowid]['body_sha256']:
                    raise ValueError('imported_body_fingerprint_mismatch')
                rows.append(row)
        link_requests(rows)
        with copied_image_index() as index: match_images(rows,index)
        response_ids=sorted({r['response_id'] for r in rows if r['classification']=='response' and r['response_id']})
        evidence=[]
        if response_ids:
            ids=','.join("'"+rid+"'" for rid in response_ids)
            fields=','.join('u.'+k for k in COUNTERS)
            sql='''SELECT row_to_json(s) FROM (SELECT u.response_id,u.usage_scope,r.research_session_id,'''+fields+''',
                count(*) AS observation_count FROM public.research_token_usage_observations u
                JOIN public.research_session_records r ON r.id=u.research_session_record_id
                WHERE u.response_id IN ('''+ids+') GROUP BY u.response_id,u.usage_scope,r.research_session_id,'+fields+') s LIMIT 10001'
            evidence=read_pg(sql)
            if len(evidence)>10000: raise ValueError('comparison_group_bound_exceeded')
        compare_tokens(rows,evidence)
        compare_available_counters(rows)
        file_ids=sorted({r['research_session_id'] for r in rows if r['research_session_id']})
        token_source_coverage=read_pg('''SELECT json_build_object('file_id',r.research_session_id,
            'usage_scope',u.usage_scope,'observations',count(*),'with_response_id',count(u.response_id))
            FROM public.research_token_usage_observations u
            JOIN public.research_session_records r ON r.id=u.research_session_record_id
            WHERE r.research_session_id IN ('''+','.join(str(i) for i in file_ids)+''')
            GROUP BY r.research_session_id,u.usage_scope ORDER BY r.research_session_id,u.usage_scope''') if file_ids else []
        images=defaultdict(list)
        for row in rows:
            if row['classification']=='request':
                for im in row['images']:
                    if im['sha256']: images[im['sha256']].append(im)
        image_summary=[{'sha256':h,'decoded_bytes':ims[0]['decoded_bytes'],'request_occurrences':len(ims),
            'index_match':ims[0]['index_match']['same_size'],
            'same_file_request_occurrences':sum(bool(im['index_match']['same_session_occurrences']) for im in ims)} for h,ims in sorted(images.items())]
        coverage=sorted(pg_coverage,key=lambda c:c['thread_id'] or '')
        summary=summarize(rows,coverage)
        summary.update(case_threads=sum(c['thread_id'] is not None for c in coverage),
                       linked_threads=sum(bool(c['thread_id'] and c['linked_files']) for c in coverage),
                       linked_threads_with_candidates=sum(bool(c['linked_files'] and c['candidate_rows']) for c in coverage),
                       linked_candidate_rows=sum(c['candidate_rows'] for c in coverage if c['linked_files']))
        database={'written':True,**persist(rows)} if args.apply else {'written':False,'mode':'read_only'}
        report={'version':VERSION,'date':'2026-10-27','status':'completed','summary':summary,'coverage':coverage,
                'token_source_coverage':token_source_coverage,'image_summary':image_summary,
                'candidate_definition':list(NEEDLES),'source_db_sha256':digest,'database':database,
                'elapsed_seconds':round(time.perf_counter()-start,3),'peak_working_set_bytes':common.peak_memory()}
        OUT.mkdir(parents=True,exist_ok=True)
        (OUT/'coverage.json').write_text(json.dumps({'coverage':coverage,'summary':{k:summary[k] for k in ('case_threads','linked_threads','linked_threads_with_candidates','linked_candidate_rows','candidate_log_rows')}},indent=2)+'\n',encoding='utf-8')
        with (OUT/'observations.jsonl').open('w',encoding='utf-8',newline='\n') as output:
            for row in rows: output.write(encoded(row)+'\n')
        (OUT/'summary.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
        (OUT/'token_source_coverage.json').write_text(json.dumps(token_source_coverage,indent=2)+'\n',encoding='utf-8')
        (OUT/'report.md').write_text(markdown(report),encoding='utf-8')
        print(encoded({'status':'completed','summary':summary,'database':database,
                       'elapsed_seconds':report['elapsed_seconds'],'peak_working_set_bytes':report['peak_working_set_bytes']}))
        return 0
    except (Exception,KeyboardInterrupt) as error:
        print(encoded({'status':'failed','error_class':type(error).__name__,'elapsed_seconds':round(time.perf_counter()-start,3),
                       'peak_working_set_bytes':common.peak_memory()}))
        return 1


if __name__=='__main__': raise SystemExit(main())
