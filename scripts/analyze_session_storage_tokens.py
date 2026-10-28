"""File-only case study: stored bytes versus recorded token-accounting evidence.

No PostgreSQL connection, conversation export, tokenizer estimate or pricing.
Parses original copied JSONL in bounded chunks. Every attempted file is reported;
unresolved accounting is retained. A case qualifies after two exact increments,
not after proving a preferred hypothesis about storage or token causality.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import random
import re
import tempfile
import time

from index_session_records import ROOT, ScanStats, iter_record_locators, lower_priority, safe_source_path, utc_now
from profile_compacted import Stream, CHUNK
from profile_response_items import peak_memory, SAFE_ENUM
from repair_null_record_types import save
from survey_sessions import candidate_files
from token_usage_stream import TokenReader, COUNTERS, SAFE_ID

VERSION='1.0.0'
OUT=ROOT/'data/reports/storage_token_case_study_v1'
FIRST='2026/01/01/rollout-2026-01-01T00-00-03-00000000-0000-4000-8000-000000000003.jsonl'
CORE=('input_tokens','output_tokens','total_tokens')
HEADERS={'/timestamp','/payload/type','/payload/role','/payload/model','/payload/model_provider',
         '/payload/turn_id','/payload/thread_id','/payload/session_id','/payload/response_id',
         '/payload/compaction_response_id','/payload/id','/payload/window_id','/payload/previous_window_id',
         '/payload/internal_chat_message_metadata_passthrough/turn_id'}


class HashingReader:
    def __init__(self,handle):
        self.handle=handle; self.digest=hashlib.sha256(); self.bytes=0
    def seek(self,offset):
        if offset!=0 or self.bytes:
            raise ValueError('hash_reader_requires_complete_forward_pass')
        return self.handle.seek(offset)
    def read(self,size):
        block=self.handle.read(size); self.digest.update(block); self.bytes+=len(block)
        return block


class EvidenceStream(Stream):
    def string(self,observer=None):
        result=super().string(observer)
        self.last_string=result
        return result


class EvidenceReader(TokenReader):
    def parse(self,source,size,chunk=CHUNK):
        self.db.execute('DELETE FROM frames'); self.db.execute('DELETE FROM candidates')
        self.stream=EvidenceStream(source,size,chunk)
        self.stack=[]; self.root_type=None; self.redacted_keys=0
        self.headers={}; self.strings=Counter(); self.blocks={}; self.token_fields={}
        kind,_=self.value('',0)
        self.stream.whitespace()
        if kind!='object' or self.stream.peek() is not None or self.stream.position!=size:
            raise ValueError('invalid_record_envelope_or_trailing_content')

    def value(self,path,depth):
        self.stream.whitespace(); start=self.stream.position; char=self.stream.peek()
        kind,value=super().value(path,depth)
        length=self.stream.position-start
        if path in HEADERS and isinstance(value,str):
            if path=='/timestamp' and re.fullmatch(r'[0-9T:.+Z-]{10,40}',value):
                self.headers[path]=value
            elif SAFE_ENUM.fullmatch(value) or (path.endswith('_id') and SAFE_ID.fullmatch(value)):
                self.headers[path]=value
        if path in ('/payload/replacement_history','/payload/guardian_history','/payload/latest_token_usage_record'):
            self.blocks[path]=length
        if char==34:
            _,prefix,_=self.stream.last_string
            leaf=path.rsplit('/',1)[-1]
            if prefix.startswith(b'data:image/') and b';base64,' in prefix[:120]:
                category='image_data_url_strings'
            elif leaf=='encrypted_content': category='encrypted_strings'
            elif 'instruction' in leaf: category='instruction_field_strings'
            elif leaf in ('arguments','input'): category='argument_input_field_strings'
            elif leaf=='output': category='output_field_strings'
            elif leaf in ('text','message'): category='text_message_field_strings'
            else: category='other_string_values'
            self.strings[category]+=length
        if kind in ('integer','number') and ('token' in path.rsplit('/',1)[-1].lower()):
            norm=re.sub(r'/[0-9]+(?=/|$)','/[]',path)
            if len(self.token_fields)>5000: raise ValueError('token_field_shape_limit')
            self.token_fields[norm]=kind
        return kind,value


def counters(observation):
    return {key:observation.get(key) for key in COUNTERS}


def valid(values):
    if values is None or any(type(values.get(k)) is not int or values[k]<0 for k in CORE): return False
    if values['input_tokens']+values['output_tokens']!=values['total_tokens']: return False
    for key in COUNTERS:
        if values.get(key) is not None and (type(values[key]) is not int or values[key]<0): return False
    if values.get('cached_input_tokens') is not None and values['cached_input_tokens']>values['input_tokens']: return False
    if values.get('reasoning_output_tokens') is not None and values['reasoning_output_tokens']>values['output_tokens']: return False
    return True


def compare(previous,current,last):
    if not all(valid(v) for v in (previous,current,last)): return 'invalid_or_missing_counters',None
    keys=[k for k in COUNTERS if previous.get(k) is not None and current.get(k) is not None and last.get(k) is not None]
    if any(len({v.get(k) is None for v in (previous,current,last)})!=1 for k in COUNTERS):
        return 'counter_field_set_changed',None
    delta={k:current[k]-previous[k] if k in keys else None for k in COUNTERS}
    if any(delta[k]<0 for k in keys): return 'counter_reset_or_scope_change',delta
    if all(delta[k]==0 for k in keys): return 'unchanged_cumulative_snapshot',delta
    if all(delta[k]==last[k] for k in keys): return 'increment_equals_last_usage',delta
    return 'increment_does_not_equal_last_usage',delta


def response_category(subtype,role):
    if subtype=='message': return 'message_role_'+(role or 'unknown')
    if subtype in ('function_call','custom_tool_call'): return 'tool_call'
    if subtype in ('function_call_output','custom_tool_call_output'): return 'tool_output'
    if subtype=='reasoning': return 'reasoning'
    return subtype or 'unknown'


def type_table(counts,bytes_by_type,total):
    return [{'type':k,'records':counts[k],'bytes':n,'share_of_file_percent':round(100*n/total,6)}
            for k,n in sorted(bytes_by_type.items(),key=lambda x:-x[1])]


def analyze_file(relative,reader,output_dir,max_bytes):
    path=safe_source_path(relative); before=path.stat()
    if before.st_size>max_bytes: raise ValueError('case_file_exceeds_bound')
    key=hashlib.sha256(relative.encode()).hexdigest()[:16]
    evidence=output_dir/(key+'.jsonl'); partial=evidence.with_suffix('.part')
    counts=Counter(); sizes=Counter(); response_sizes=Counter(); string_sizes=Counter()
    fields=Counter(); models=Counter(); statuses=Counter(); token_sums=Counter(); token_coverage=Counter()
    segments=Counter(); segment_records=Counter(); segment_response=Counter(); segment_strings=Counter()
    compactions=[]; direct_responses={}; compaction_usages=[]; conflicts=set()
    previous=None; previous_last=None; previous_line=None; segment_compactions=[]
    first_usage=None; last_usage=None; first_line=None; last_line=None
    epoch=0; previous_epoch=None; seen_session_meta=False; model=None
    examples={}; compaction_transitions=[]; locator_stats=ScanStats(); observations_count=0
    usage_events=0; verified_bytes=0; verified_types=Counter()
    previous_timestamp=None; previous_model=None
    recent_valid_usage=None; pending_compactions=[]; turn_storage={}
    def response_observation(o,line,origin):
        rid=o['response_id']; value=counters(o)
        if not rid or not valid(value) or o['metadata']['issues']: return
        identity={k:o[k] for k in ('turn_id','root_turn_id','session_id','thread_id')}
        if rid in direct_responses and (direct_responses[rid]['usage']!=value or direct_responses[rid]['identity']!=identity): conflicts.add(rid)
        elif rid not in direct_responses:
            direct_responses[rid]={'usage':value,'identity':identity,'first_line':line,'origins':[origin],'origin_first_lines':{origin:line},'occurrences':0}
        if rid in direct_responses:
            direct_responses[rid]['occurrences']+=1
            direct_responses[rid]['origin_first_lines'].setdefault(origin,line)
            if origin not in direct_responses[rid]['origins']: direct_responses[rid]['origins'].append(origin)
    with path.open('rb') as index_handle,path.open('rb') as source,partial.open('w',encoding='utf-8') as output:
        hashed=HashingReader(index_handle)
        for loc in iter_record_locators(hashed,locator_stats,chunk_bytes=CHUNK,header_limit=256):
            source.seek(loc.byte_offset); reader.parse(source,loc.record_bytes)
            typ=reader.root_type or '[missing_type]'; h=reader.headers
            subtype=h.get('/payload/type'); role=h.get('/payload/role')
            turn_ids={h[p] for p in ('/payload/turn_id','/payload/internal_chat_message_metadata_passthrough/turn_id') if h.get(p)}
            explicit_turn=next(iter(turn_ids)) if len(turn_ids)==1 else None
            if explicit_turn:
                group=turn_storage.setdefault(explicit_turn,{'records':0,'bytes':0,'bytes_by_record_type':Counter(),'records_by_type':Counter(),'string_value_bytes':Counter(),'first_line':loc.line_number,'last_line':loc.line_number})
                group['records']+=1; group['bytes']+=loc.record_bytes; group['last_line']=loc.line_number
                group['bytes_by_record_type'][typ]+=loc.record_bytes; group['string_value_bytes'].update(reader.strings)
                group['records_by_type'][typ]+=1
            for pending in pending_compactions:
                pending['bytes_since_compaction_including_it']+=loc.record_bytes
                pending['records_since_compaction'][typ]+=1
            if typ=='session_meta':
                if seen_session_meta: epoch+=1
                seen_session_meta=True
            if typ=='turn_context' and h.get('/payload/model'):
                model=h['/payload/model']; models[model]+=1
            counts[typ]+=1; sizes[typ]+=loc.record_bytes
            segments[typ]+=loc.record_bytes; segment_records[typ]+=1
            string_sizes.update(reader.strings); segment_strings.update(reader.strings)
            if typ=='response_item':
                category=response_category(subtype,role)
                response_sizes[category]+=loc.record_bytes; segment_response[category]+=loc.record_bytes
            for shape,kind in reader.token_fields.items(): fields[(shape,kind)]+=1
            usage=[]
            for observation in reader.observations(loc.line_number,loc.byte_offset):
                if len(usage)>=20000: raise ValueError('case_observation_limit')
                usage.append(observation)
            observations_count+=len(usage)
            safe_usage=[{'path':o['source_json_path'],'scope':o['usage_scope'],'usage':counters(o),
                         'response_id':o['response_id'],'turn_id':o['turn_id'],
                         'issues':o['metadata']['issues']} for o in usage]
            record={'kind':'source_record','line':loc.line_number,'offset':loc.byte_offset,'bytes':loc.record_bytes,
                    'record_type':typ,'subtype':subtype,'role':role,'timestamp':h.get('/timestamp'),
                    'model_if_explicit':h.get('/payload/model'),'string_value_bytes':dict(reader.strings),
                    'explicit_turn_id':explicit_turn,
                    'payload_block_bytes':reader.blocks,'usage_observations':safe_usage}
            output.write(json.dumps(record,ensure_ascii=True,separators=(',',':'))+'\n')
            for o in usage:
                if typ=='token_usage_record' and o['source_json_path']=='/payload/usage':
                    response_observation(o,loc.line_number,'standalone')
            if typ=='compacted':
                comp={'line':loc.line_number,'bytes':loc.record_bytes,'block_bytes':dict(reader.blocks),
                      'image_string_bytes':reader.strings['image_data_url_strings'],
                      'compaction_response_id':h.get('/payload/compaction_response_id')}
                compactions.append(comp); segment_compactions.append(comp)
                pending_compactions.append({'compaction':comp,'before':recent_valid_usage,
                    'bytes_since_compaction_including_it':loc.record_bytes,'records_since_compaction':Counter({typ:1}),
                    'skipped_nonadditive_usage_events':[]})
                for o in usage:
                    if o['source_json_path']=='/payload/latest_token_usage_record/usage':
                        matched=bool(comp['compaction_response_id'] and comp['compaction_response_id']==o['response_id'])
                        detail={'line':loc.line_number,'response_id':o['response_id'],
                                'matches_compaction_response_id':matched,'usage':counters(o)}
                        compaction_usages.append(detail)
                        if matched: response_observation(o,loc.line_number,'compaction_id_matched_snapshot')
            if typ=='event_msg' and subtype=='token_count':
                usage_events+=1
                totals=[counters(o) if not o['metadata']['issues'] else None for o in usage if o['source_json_path']=='/payload/info/total_token_usage']
                lasts=[counters(o) if not o['metadata']['issues'] else None for o in usage if o['source_json_path']=='/payload/info/last_token_usage']
                total=totals[0] if len(totals)==1 else None
                last=lasts[0] if len(lasts)==1 else None
                if valid(total) and valid(last) and last['total_tokens']>0:
                    for pending in pending_compactions:
                        before_usage=pending['before']
                        transition={'previous_usage_line':before_usage['line'] if before_usage else None,
                            'next_usage_line':loc.line_number,'compaction_lines':[pending['compaction']['line']],
                            'stored_compacted_bytes':pending['compaction']['bytes'],
                            'previous_last_input_tokens':before_usage['last']['input_tokens'] if before_usage else None,
                            'next_last_input_tokens':last['input_tokens'],
                            'previous_last_usage':before_usage['last'] if before_usage else None,'next_last_usage':last,
                            'same_session_meta_epoch':before_usage['epoch']==epoch if before_usage else False,
                            'previous_model_context':before_usage['model'] if before_usage else None,'next_model_context':model,
                            'records_since_compaction_including_it':dict(pending['records_since_compaction']),
                            'bytes_since_compaction_including_it':pending['bytes_since_compaction_including_it'],
                            'skipped_nonadditive_usage_events':pending['skipped_nonadditive_usage_events'],
                            'causal_attribution':'not_established_by_temporal_order'}
                        compaction_transitions.append(transition)
                    pending_compactions=[]
                    recent_valid_usage={'line':loc.line_number,'last':last,'total':total,'epoch':epoch,'model':model}
                elif pending_compactions:
                    for pending in pending_compactions:
                        pending['skipped_nonadditive_usage_events'].append({'line':loc.line_number,
                            'reported_last':[counters(o) for o in usage if o['source_json_path']=='/payload/info/last_token_usage'],
                            'issues':[o['metadata']['issues'] for o in usage if o['source_json_path']=='/payload/info/last_token_usage']})
                if first_usage is None and valid(total): first_usage=total; first_line=loc.line_number
                if valid(total): last_usage=total; last_line=loc.line_number
                if previous_line is not None:
                    status,delta=compare(previous,total,last) if epoch==previous_epoch else ('session_meta_boundary',None)
                    if status=='unchanged_cumulative_snapshot' and last!=previous_last:
                        status='unchanged_total_but_changed_last_snapshot'
                    statuses[status]+=1
                    interval={'kind':'usage_interval','start_after_line':previous_line,'end_at_line':loc.line_number,
                              'status':status,'previous_total':previous,'current_total':total,
                              'current_last':last,'delta':delta,'stored_bytes':sum(segments.values()),
                              'bytes_by_record_type':dict(segments),'records_by_type':dict(segment_records),
                              'response_item_bytes_by_category':dict(segment_response),
                              'string_value_bytes':dict(segment_strings),'nearest_model_context':model,
                              'previous_timestamp':previous_timestamp,'current_timestamp':h.get('/timestamp'),
                              'previous_nearest_model_context':previous_model,
                              'model_assignment':'preceding_turn_context_only_not_verified_per_call',
                              'compaction_lines':[x['line'] for x in segment_compactions]}
                    output.write(json.dumps(interval,ensure_ascii=True,separators=(',',':'))+'\n')
                    if status not in examples: examples[status]=interval
                    if status=='increment_equals_last_usage':
                        verified_bytes+=interval['stored_bytes']; verified_types.update(segments)
                        for k,v in delta.items():
                            if v is not None: token_sums[k]+=v; token_coverage[k]+=1
                previous=total; previous_last=last; previous_line=loc.line_number; previous_epoch=epoch
                previous_timestamp=h.get('/timestamp'); previous_model=model
                segments.clear(); segment_records.clear(); segment_response.clear(); segment_strings.clear(); segment_compactions=[]
    after=path.stat()
    if (before.st_size,before.st_mtime_ns)!=(after.st_size,after.st_mtime_ns): raise ValueError('source_changed')
    if hashed.bytes!=before.st_size or sum(sizes.values())+locator_stats.blank_line_bytes!=before.st_size:
        raise ValueError('byte_accounting_mismatch')
    partial.replace(evidence)
    verified=statuses['increment_equals_last_usage']
    measured={k:token_sums[k] if token_coverage[k]==verified and verified else None for k in COUNTERS}
    share={}
    if measured['total_tokens']:
        share={'input_share_of_total_percent':100*measured['input_tokens']/measured['total_tokens'],
               'output_share_of_total_percent':100*measured['output_tokens']/measured['total_tokens'],
               'cached_share_of_input_percent':100*measured['cached_input_tokens']/measured['input_tokens'] if measured['input_tokens'] and measured['cached_input_tokens'] is not None else None,
               'reasoning_share_of_output_percent':100*measured['reasoning_output_tokens']/measured['output_tokens'] if measured['output_tokens'] and measured['reasoning_output_tokens'] is not None else None}
    token_paths=[{'path':p,'type':t,'records':n} for (p,t),n in sorted(fields.items())]
    attribution_candidates=[x for x in token_paths if re.search(r'(image|text|tool|content|item).*token|token.*(image|text|tool|content|item)',x['path'],re.I)]
    eligible_responses=[r for rid,r in direct_responses.items() if rid not in conflicts]
    response_totals={k:sum(r['usage'][k] for r in eligible_responses)
                     if eligible_responses and all(r['usage'][k] is not None for r in eligible_responses) else None for k in COUNTERS}
    turn_cases=[]
    for turn_id,storage in turn_storage.items():
        responses={rid:r for rid,r in direct_responses.items() if rid not in conflicts and r['identity']['turn_id']==turn_id}
        if not responses: continue
        values={k:sum(r['usage'][k] for r in responses.values())
                if all(r['usage'][k] is not None for r in responses.values()) else None for k in COUNTERS}
        turn_cases.append({'turn_id':turn_id,'explicitly_tagged_storage':storage,
            'unique_response_count':len(responses),'responses':responses,'recorded_response_tokens':values,
            'token_shares':{'input_of_total_percent':100*values['input_tokens']/values['total_tokens'] if values['total_tokens'] else None,
                'output_of_total_percent':100*values['output_tokens']/values['total_tokens'] if values['total_tokens'] else None,
                'cached_of_input_percent':100*values['cached_input_tokens']/values['input_tokens'] if values['input_tokens'] and values['cached_input_tokens'] is not None else None,
                'reasoning_of_output_percent':100*values['reasoning_output_tokens']/values['output_tokens'] if values['output_tokens'] and values['reasoning_output_tokens'] is not None else None},
            'scope':'records_with_explicit_turn_id_only_not_complete_topic_or_prompt'})
    summary={'source_relative_path':relative,'source_sha256':hashed.digest.hexdigest(),
             'source_bytes':before.st_size,'source_mtime_ns':before.st_mtime_ns,
             'source_records':sum(counts.values()),'blank_line_bytes':locator_stats.blank_line_bytes,
             'source_bytes_read_two_forward_passes':before.st_size+sum(sizes.values()),
             'case_accepted':verified>=2,'case_acceptance':'at_least_two_exact_accounting_increments',
             'record_types':type_table(counts,sizes,before.st_size),
             'response_item_bytes_by_category':dict(response_sizes),
             'string_value_bytes':dict(string_sizes),
             'remaining_structure_and_nonstring_value_bytes':before.st_size-sum(string_sizes.values()),
             'model_contexts':dict(models),'usage_observations':observations_count,
             'usage_events':usage_events,'verified_interval_stored_bytes':verified_bytes,
             'verified_interval_bytes_by_record_type':dict(verified_types),
             'measured_token_coverage':'verified_intervals_only_not_full_file_or_topic',
             'accounting_interval_statuses':dict(statuses),'verified_interval_tokens':measured,
             'verified_interval_token_shares':share,'first_cumulative_snapshot':first_usage,
             'last_cumulative_snapshot':last_usage,'first_usage_line':first_line,'last_usage_line':last_line,
             'interval_examples':examples,'compactions':compactions,'compaction_transitions':compaction_transitions,
             'unresolved_compaction_followups':[p['compaction']['line'] for p in pending_compactions],
             'explicit_turn_cases':turn_cases,
             'records_with_explicit_turn_id':sum(v['records'] for v in turn_storage.values()),
             'response_sum_matches_final_cumulative':bool(eligible_responses and last_usage and response_totals==last_usage),
             'identified_response_count':len(direct_responses)-len(conflicts),
             'conflicting_response_ids':len(conflicts),'identified_response_subset_tokens':response_totals if eligible_responses else None,
             'compaction_usage_links':compaction_usages,'numeric_token_fields':token_paths,
             'candidate_content_token_breakdown_fields':attribution_candidates,
             'content_category_token_shares':None,
             'content_attribution_status':'requires_field_semantic_validation' if attribution_candidates else 'not_reported_in_structural_numeric_fields',
             'evidence_jsonl':str(evidence.relative_to(ROOT)).replace('\\','/')}
    save(output_dir/(key+'.json'),summary)
    return summary


def selected_files(seed,max_bytes):
    pool=[]
    for path in candidate_files(ROOT/'2026'):
        size=path.stat().st_size
        if 256*1024<=size<=max_bytes:
            pool.append(path.relative_to(ROOT).as_posix())
    rng=random.Random(seed); groups=defaultdict(list)
    for relative in sorted(pool): groups[relative[:7]].append(relative)
    for group in groups.values(): rng.shuffle(group)
    chosen=[]
    if FIRST in pool: chosen.append(FIRST)
    months=sorted(groups,reverse=True)
    while any(groups.values()):
        for month in months:
            if groups[month]:
                item=groups[month].pop()
                if item not in chosen: chosen.append(item)
    return chosen


def crosscheck_file(summary):
    """Independently parse only small counter records with Python's JSON parser."""
    checks=defaultdict(list)
    example=summary.get('interval_examples',{}).get('increment_equals_last_usage')
    if example:
        checks[example['start_after_line']].append(('/payload/info/total_token_usage',example['previous_total']))
        checks[example['end_at_line']].extend([('/payload/info/total_token_usage',example['current_total']),
                                             ('/payload/info/last_token_usage',example['current_last'])])
    for turn in summary['explicit_turn_cases']:
        for response in turn['responses'].values():
            if 'standalone' in response['origins']:
                checks[response['origin_first_lines']['standalone']].append(('/payload/usage',response['usage']))
    for transition in summary['compaction_transitions']:
        for line,values in ((transition['previous_usage_line'],transition['previous_last_usage']),
                            (transition['next_usage_line'],transition['next_last_usage'])):
            if line is not None: checks[line].append(('/payload/info/last_token_usage',values))
    refs={}
    with (ROOT/summary['evidence_jsonl']).open(encoding='utf-8') as evidence:
        for line in evidence:
            row=json.loads(line)
            if row['kind']=='source_record' and row['line'] in checks: refs[row['line']]=row
    if set(refs)!=set(checks): raise ValueError('independent_check_missing_locator')
    compared=0; read_bytes=0; skipped=[]
    with safe_source_path(summary['source_relative_path']).open('rb') as source:
        for line,expected in checks.items():
            ref=refs[line]
            if ref['bytes']>65536:
                skipped.append(line); continue
            source.seek(ref['offset']); raw=source.read(ref['bytes']); read_bytes+=len(raw)
            obj=json.loads(raw)
            for pointer,values in expected:
                found=obj
                for key in pointer.strip('/').split('/'): found=found[key]
                if counters(found)!=values: raise ValueError('independent_counter_check_mismatch')
                compared+=1
    return {'method':'standard_library_json_parser_on_small_original_counter_records',
            'counter_objects_compared':compared,'source_records_read':len(refs)-len(skipped),
            'source_bytes_read':read_bytes,'skipped_large_record_lines':skipped,
            'status':'passed' if not skipped else 'partial_bounded_check'}


def markdown(report):
    accepted=[x for x in report['files'] if x.get('case_accepted')]
    lines=['# File-only storage and token-accounting case study','',
        f"Generated: {report['completed_at']}. Status: {report['status']}.",
        f"Attempted files: {len(report['files'])}; qualifying cases: {len(accepted)}.",
        f"Peak Python working set: {report['peak_working_set_bytes']/1048576:.2f} MiB.",'',
        '## Findings','',
        f"The script read {sum(f.get('source_records',0) for f in report['files']):,} records from {sum(f.get('source_bytes',0) for f in report['files']):,} source bytes. It verified {sum(f.get('accounting_interval_statuses',{}).get('increment_equals_last_usage',0) for f in accepted)} cumulative increments and found {sum(len(f['explicit_turn_cases']) for f in accepted)} operational turns linked by explicit identifiers to response usage.",
        f"Independent standard-library JSON parsing rechecked {sum(f.get('independent_validation',{}).get('counter_objects_compared',0) for f in accepted)} counter objects directly from small source records. Byte accounting covers each complete file; source SHA-256 hashes and line/byte locators accompany the results.",
        'Observed storage composition and token-accounting composition are different measurements. Record types describe the log; the token counters do not allocate input usage among those record types, text, images or tools in these files. Their token percentages are therefore unknown, not zero.',
        'The explicit turn tables establish a measurable operational unit. They do not establish the boundaries of a human topic or identify all context supplied to each model request.', '',
        '## Evidence boundaries','',
        '- Original copied JSONL files are the only data source. No PostgreSQL queries, model calls, tokenizers, prices or conversation exports.',
        '- Storage shares measure serialized physical bytes, including JSON/Base64 and repeated history. They are not input-token shares.',
        '- A verified increment means the change between two recorded cumulative snapshots equals the recorded last usage in every available counter. It establishes internal log-accounting consistency, not independent server billing or causal attribution.',
        '- Inherited or replayed history may repeat earlier activity. Interval sums are sums of log readings, not independently verified unique executions; a source file is not assumed to be one uninterrupted logical session.',
        '- Window byte counts include records after the preceding token event through the current token event. They are logged activity, not a reconstructed model input request.',
        '- Input can include earlier context; response/tool records and event telemetry may represent the same activity. Do not sum alternative usage sources or scopes.',
        '- Per-content token shares remain null unless authoritative item-level attribution is available and validated. Missing cache-write counters stay unknown, not zero.',
        '- Token sums below cover verified intervals only; they exclude the unanchored first snapshot, unresolved intervals and the final tail. They are not a whole-file or topic total.',
        '- Compaction transitions are temporal comparisons. Extra records/model changes/input changes prevent claiming a controlled causal experiment.',
        '- Selection is a reproducible diagnostic case search, not a representative corpus estimate. Failed/nonqualifying files are preserved in the JSON report.', '',
        '## Cases','',
        '| Case | File MiB | Largest stored type | Share | Verified intervals | Measured input tokens | Measured output tokens | Compacted records |',
        '|---|---:|---|---:|---:|---:|---:|---:|']
    for i,f in enumerate(accepted,1):
        top=f['record_types'][0]; usage=f['verified_interval_tokens']
        lines.append(f"| {i} | {f['source_bytes']/1048576:.4f} | {top['type']} | {top['share_of_file_percent']:.2f}% | {f['accounting_interval_statuses'].get('increment_equals_last_usage',0)} | {usage['input_tokens']:,} | {usage['output_tokens']:,} | {len(f['compactions'])} |")
    for i,f in enumerate(accepted,1):
        lines+=['',f'## Case {i}', '',f"Source: `{f['source_relative_path']}`",f"SHA-256: `{f['source_sha256']}`",f"Evidence: `{f['evidence_jsonl']}`",'',
                '| Record type | Records | Bytes | Share of file |','|---|---:|---:|---:|']
        for t in f['record_types']:
            lines.append(f"| {t['type']} | {t['records']:,} | {t['bytes']:,} | {t['share_of_file_percent']:.4f}% |")
        e=f['interval_examples']['increment_equals_last_usage']
        lines+=['',f"Exact example, lines {e['start_after_line']} → {e['end_at_line']}: cumulative total {e['previous_total']['total_tokens']:,} → {e['current_total']['total_tokens']:,}; increment {e['delta']['total_tokens']:,} = reported last total {e['current_last']['total_tokens']:,}.",
                f"Stored interval: {e['stored_bytes']:,} bytes. Record-type bytes: `{json.dumps(e['bytes_by_record_type'])}`.",
                f"Accounting statuses: `{json.dumps(f['accounting_interval_statuses'])}`.",
                f"Independent counter validation: `{json.dumps(f.get('independent_validation',{}))}`.",
                f"Token shares within verified increments: `{json.dumps(f['verified_interval_token_shares'])}`.",
                f"Models explicitly observed in turn contexts: `{json.dumps(f['model_contexts'])}`. `codex-auto-review` cases represent review-agent activity, not a human topic budget.",
                f"Image data-URL string bytes: {f['string_value_bytes'].get('image_data_url_strings',0):,}; content-token attribution: `{f['content_attribution_status']}`."]
        if f['explicit_turn_cases']:
            lines+=['','### Explicit identifier links','',
                f"Nonconflicting response IDs: {f['identified_response_count']}; their usage sum equals the final cumulative snapshot in all counter fields: **{f['response_sum_matches_final_cumulative']}**.",
                'Each row uses explicit turn IDs and unique response IDs within this file. Stored bytes include only records explicitly carrying that turn ID; untagged records and previously stored context are excluded. A turn is an operational unit, not an independently established human topic. Response totals and cumulative increments are alternative checks and must not be added together.',
                '', '| Turn ID | Responses | Tagged record bytes | Input tokens | Output tokens | Cached / input | Reasoning / output |',
                '|---|---:|---:|---:|---:|---:|---:|']
            for turn in f['explicit_turn_cases']:
                u=turn['recorded_response_tokens']; shares=turn['token_shares']
                pct=lambda value: f'{value:.2f}%' if value is not None else 'unknown'
                lines.append(f"| `{turn['turn_id']}` | {turn['unique_response_count']} | {turn['explicitly_tagged_storage']['bytes']:,} | {u['input_tokens']:,} | {u['output_tokens']:,} | {pct(shares['cached_of_input_percent'])} | {pct(shares['reasoning_of_output_percent'])} |")
            lines+=['','Storage composition for the first linked turn (bytes, not token attribution):','',
                    '| Record type | Bytes | Share of explicitly tagged turn records |','|---|---:|---:|']
            t=f['explicit_turn_cases'][0]['explicitly_tagged_storage']
            for typ,n in sorted(t['bytes_by_record_type'].items(),key=lambda x:-x[1]):
                lines.append(f"| {typ} | {n:,} | {100*n/t['bytes']:.4f}% |")
        for c in f['compaction_transitions'][:3]:
            lines.append(f"Compaction lines {c['compaction_lines']}: {c['stored_compacted_bytes']:,} stored bytes; adjacent last-input counts {c['previous_last_input_tokens']} → {c['next_last_input_tokens']}. Causality is not established by this ordering.")
            for skipped in c['skipped_nonadditive_usage_events'][:3]:
                lines.append(f"Non-additive snapshot at line {skipped['line']}, preserved but excluded from additive usage: `{json.dumps(skipped['reported_last'])}`.")
    return '\n'.join(lines)+'\n'


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source',help='One project-relative copied JSONL file; no database lookup')
    parser.add_argument('--target-cases',type=int,default=6)
    parser.add_argument('--max-files',type=int,default=24)
    parser.add_argument('--max-file-mib',type=int,default=4)
    parser.add_argument('--seed',type=int,default=20261025)
    args=parser.parse_args()
    if not 1<=args.target_cases<=24 or not 1<=args.max_files<=48 or not 1<=args.max_file_mib<=64:
        parser.error('invalid bounded study settings')
    lower_priority(); start=time.monotonic(); OUT.mkdir(parents=True,exist_ok=True); (ROOT/'.tmp').mkdir(exist_ok=True)
    sources=[args.source] if args.source else selected_files(args.seed,args.max_file_mib*1048576)
    files=[]; accepted=0; transitions=0
    with tempfile.TemporaryDirectory(prefix='storage-token-',dir=ROOT/'.tmp') as temp:
        reader=EvidenceReader(Path(temp)/'record.sqlite3')
        try:
            for relative in sources[:args.max_files]:
                try:
                    result=analyze_file(relative,reader,OUT,args.max_file_mib*1048576)
                    result['independent_validation']=crosscheck_file(result)
                    save(OUT/(hashlib.sha256(relative.encode()).hexdigest()[:16]+'.json'),result)
                except (OSError,ValueError,RuntimeError) as error:
                    result={'source_relative_path':relative,'case_accepted':False,'error_type':type(error).__name__}
                files.append(result)
                accepted+=bool(result.get('case_accepted'))
                transitions+=len(result.get('compaction_transitions',[]))
                print(json.dumps({'files_attempted':len(files),'accepted_cases':accepted,
                                  'compaction_transitions':transitions,'source':relative,
                                  'records':result.get('source_records'),'error_type':result.get('error_type')}),flush=True)
                interim={'version':VERSION,'files':files,'seed':args.seed,'status':'in_progress'}
                save(OUT/'summary.json',interim)
                if not args.source and accepted>=args.target_cases and transitions:
                    break
        finally:
            reader.close()
    report={'version':VERSION,'completed_at':utc_now(),'seed':args.seed,
            'status':'cases_collected' if accepted>=args.target_cases else 'bounded_search_incomplete',
            'target_cases':args.target_cases,'accepted_cases':accepted,'attempted_files':len(files),
            'selection':{'first_source':args.source or FIRST,'remaining':'seeded_month_stratified_order' if not args.source else None,
                         'max_files':args.max_files,'max_file_bytes':args.max_file_mib*1048576,
                         'minimum_candidate_file_bytes':256*1024 if not args.source else None},
            'compaction_transitions':transitions,'files':files,'database_accessed':False,
            'compaction_transition_observed':bool(transitions),
            'original_files_modified':False,'per_content_token_allocation_estimated':False,
            'peak_working_set_bytes':peak_memory(),'elapsed_seconds':round(time.monotonic()-start,3)}
    save(OUT/'summary.json',report)
    (OUT/'report.md').write_text(markdown(report),encoding='utf-8')
    print(json.dumps({k:v for k,v in report.items() if k!='files'},indent=2))
    return 0 if accepted>=args.target_cases or args.source else 1


if __name__=='__main__':
    raise SystemExit(main())
