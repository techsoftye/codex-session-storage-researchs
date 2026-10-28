"""PostgreSQL-only, read-only endpoint and storage case study; stdlib + psql."""
from collections import Counter
from datetime import datetime, timezone
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics
import time

from import_session_survey import run_psql
from analyze_logs_request_comparison import resources

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'data/reports/tokens_vs_storage_v1'
CUTOFF=datetime(2026,6,19,tzinfo=timezone.utc)
SCOPES=('thread','cumulative')
MEASURES=('total','input','uncached_input','output')
STORAGE=('file_size_bytes','compacted_bytes','compacted_record_count','record_count')
GROUP_CODES={'unknown':0,'user':1,'subagent':2,'conflict':3}
TYPE_NAMES=('event_msg','response_item','token_usage_record','turn_context','world_state',
            'session_meta','compacted','inter_agent_communication_metadata','[NULL]','[other]')
SIZE_BINS=('0–<1 MiB','1–<10 MiB','10–<100 MiB','100–<1024 MiB','>=1024 MiB')
TOKEN_BINS=('0','1–<10k','10k–<100k','100k–<1M','1M–<10M','>=10M','missing')


def nonnegative(v): return type(v) is int and v>=0


def uncached(total,cached):
    return total-cached if nonnegative(total) and nonnegative(cached) and cached<=total else None


def average_ranks(values):
    order=sorted(range(len(values)),key=values.__getitem__)
    ranks=[0.0]*len(values);i=0
    while i<len(order):
        j=i+1
        while j<len(order) and values[order[j]]==values[order[i]]: j+=1
        rank=(i+1+j)/2
        for k in order[i:j]: ranks[k]=rank
        i=j
    return ranks


def spearman(pairs):
    pairs=[(x,y) for x,y in pairs if nonnegative(x) and nonnegative(y)]
    n=len(pairs)
    if n<2: return {'n':n,'rho':None,'reason':'fewer_than_two_pairs'}
    x,y=zip(*pairs);rx=average_ranks(x);ry=average_ranks(y);center=(n+1)/2
    xx=math.fsum((r-center)**2 for r in rx);yy=math.fsum((r-center)**2 for r in ry)
    if not xx or not yy:return {'n':n,'rho':None,'reason':'constant_rank_variable'}
    rho=math.fsum((a-center)*(b-center) for a,b in zip(rx,ry))/math.sqrt(xx*yy)
    return {'n':n,'rho':max(-1.0,min(1.0,rho)),'reason':None,
            'distinct_x':len(set(x)),'distinct_y':len(set(y))}


def match_class(a,b):
    if not nonnegative(a) or not nonnegative(b): return 'missing_or_invalid'
    if a==b:return 'exact'
    return 'near' if abs(a-b)*100<=max(a,b) else 'different'


def describe(values):
    values=sorted(v for v in values if nonnegative(v))
    if not values:return {'n':0,'min':None,'median':None,'max':None,'zero_count':0}
    return {'n':len(values),'min':values[0],'median':statistics.median(values),'max':values[-1],
            'zero_count':values.count(0)}


def size_bin(value):
    return sum(value>=b*1048576 for b in (1,10,100,1024))


def token_bin(value):
    if not nonnegative(value):return 6
    if value==0:return 0
    return 1+sum(value>=b for b in (10000,100000,1000000,10000000))


def prepare(data):
    files=[];excluded=[]
    for original in data['files']:
        f=dict(original)
        reasons=[]
        if f['indexed_bytes']!=f['file_size_bytes'] or f['indexed_record_count']!=f['record_count']:
            reasons.append('index_count_or_byte_mismatch')
        if f['max_recorded_at'] and datetime.fromisoformat(f['max_recorded_at'])>=CUTOFF:
            reasons.append('a_record_timestamp_is_after_cutoff')
        if reasons:
            excluded.append({'file_id':f['file_id'],'reasons':reasons});continue
        assert sum(v['bytes'] for v in f['by_type'].values())==f['file_size_bytes']
        assert sum(v['records'] for v in f['by_type'].values())==f['record_count']
        sub=f['has_parent_edge'] or f['source_category']=='subagent'
        f['group']='conflict' if sub and f['source_category']=='user' else (
            'subagent' if sub else 'user' if f['source_category']=='user' else 'unknown')
        compact=f['by_type'].get('compacted',{'bytes':0,'records':0})
        f['compacted_bytes']=compact['bytes'];f['compacted_record_count']=compact['records']
        for scope in SCOPES:
            endpoint=f['endpoints'].get(scope,{})
            for measure in ('total','input','cached_input','output'):
                f[scope+'_'+measure]=endpoint.get(measure)
            f[scope+'_uncached_input']=uncached(endpoint.get('input'),endpoint.get('cached_input'))
        files.append(f)
    assert len({f['file_id'] for f in files})==len(files)
    return files,excluded


def compare_endpoints(files):
    results=[]
    for scope in SCOPES:
        for measure in MEASURES:
            counts=Counter(match_class(f['tokens_used'],f[scope+'_'+measure]) for f in files)
            n=len(files)-counts['missing_or_invalid']
            results.append({'scope':scope,'measure':measure,'comparable_files':n,
                'missing_or_invalid':counts['missing_or_invalid'],
                **{label:counts[label] for label in ('exact','near','different')},
                **{label+'_percent':100*counts[label]/n if n else None for label in ('exact','near','different')}})
    return results


def matrix(files,token_field):
    cells=[[0]*len(TOKEN_BINS) for _ in SIZE_BINS]
    for f in files:cells[size_bin(f['file_size_bytes'])][token_bin(f[token_field])]+=1
    assert sum(map(sum,cells))==len(files)
    return {'token_field':token_field,'size_bins':SIZE_BINS,'token_bins':TOKEN_BINS,'counts':cells}


def analyze(data):
    files,excluded=prepare(data);byid={f['file_id']:f for f in files}
    metrics=['tokens_used']+[scope+'_'+measure for scope in SCOPES for measure in MEASURES
                            if any(nonnegative(f[scope+'_'+measure]) for f in files)]
    correlations=[{'token_field':m,'storage_field':s,**spearman((f[m],f[s]) for f in files)}
                  for m in metrics for s in STORAGE]
    groups=[{'group':group,'files':sum(f['group']==group for f in files),
             **{key:describe(f[key] for f in files if f['group']==group)
                for key in ('tokens_used','cumulative_total','file_size_bytes','compacted_bytes','record_count')}}
            for group in GROUP_CODES if any(f['group']==group for f in files)]
    pairs=[];seen=set();edge_diagnostics=Counter()
    for edge in data['edges']:
        a,b=edge['parent_file_id'],edge['child_file_id']
        if a is None or b is None:edge_diagnostics['missing_file_on_one_or_both_sides']+=1;continue
        edge_diagnostics['both_files_linked']+=1
        if a not in byid or b not in byid:edge_diagnostics['outside_analysis_cohort']+=1;continue
        if a==b:edge_diagnostics['same_file_edge_excluded']+=1;continue
        if (a,b) in seen:edge_diagnostics['repeated_pair_excluded']+=1;continue
        seen.add((a,b));p,c=byid[a],byid[b]
        pairs.append({'edge_row_id':edge['edge_row_id'],'parent_file_id':a,'child_file_id':b,
                      **{prefix+'_'+key:f[key] for prefix,f in (('parent',p),('child',c))
                         for key in ('tokens_used','cumulative_total','file_size_bytes','record_count','compacted_bytes')},
                      'child_parent_tokens_used_ratio':c['tokens_used']/p['tokens_used'] if p['tokens_used']>0 else None,
                      'child_parent_size_ratio':c['file_size_bytes']/p['file_size_bytes'] if p['file_size_bytes']>0 else None})
    parent_ids={p['parent_file_id'] for p in pairs};child_ids={p['child_file_id'] for p in pairs}
    pair_summary={'source_edges':len(data['edges']),**edge_diagnostics,'eligible_pairs':len(pairs),
        'distinct_parents':len(parent_ids),'distinct_children':len(child_ids),
        'parents':{key:describe(byid[i][key] for i in parent_ids) for key in ('tokens_used','cumulative_total','file_size_bytes')},
        'children':{key:describe(byid[i][key] for i in child_ids) for key in ('tokens_used','cumulative_total','file_size_bytes')},
        'paired_tokens_comparison':dict(Counter('child_less' if p['child_tokens_used']<p['parent_tokens_used'] else 'equal' if p['child_tokens_used']==p['parent_tokens_used'] else 'child_more' for p in pairs)),
        'paired_size_comparison':dict(Counter('child_less' if p['child_file_size_bytes']<p['parent_file_size_bytes'] else 'equal' if p['child_file_size_bytes']==p['parent_file_size_bytes'] else 'child_more' for p in pairs))}
    diagnostics={scope:{'endpoint_files':sum(scope in f['endpoints'] for f in files),
        'nested_history_endpoints':sum(bool(f['endpoints'].get(scope,{}).get('nested_history')) for f in files),
        'newer_flagged_endpoint':sum(bool(f['endpoints'].get(scope,{}).get('newer_flagged_endpoint')) for f in files),
        'reported_thread_mismatch':sum(f['endpoints'].get(scope,{}).get('thread_id_matches') is False for f in files),
        'reported_thread_identity_missing':sum(scope in f['endpoints'] and f['endpoints'][scope]['thread_id_matches'] is None for f in files),
        'reported_session_mismatch':sum(f['endpoints'].get(scope,{}).get('session_id_matches') is False for f in files)} for scope in SCOPES}
    totals={kind:{'bytes':sum(f['by_type'].get(kind,{}).get('bytes',0) for f in files),
                  'records':sum(f['by_type'].get(kind,{}).get('records',0) for f in files)} for kind in TYPE_NAMES}
    return {'coverage':{**data['coverage'],'analysis_files':len(files),'quality_exclusions':excluded,
        'groups':dict(Counter(f['group'] for f in files)),'model_missing':sum(f['model'] is None for f in files),
        'models':dict(Counter(f['model'] or '[missing]' for f in files)),
        'files_with_unparseable_record_times':sum(f['unparseable_times']>0 for f in files),
        'unparseable_record_times':sum(f['unparseable_times'] for f in files),
        'catalog_tail_mismatch':sum(not f['catalog_tail_matches'] for f in files)},
        'usage_observation_counts':data['usage_counts'],'endpoint_diagnostics':diagnostics,
        'total_disagreements':data['total_disagreements'],
        'endpoint_comparisons':compare_endpoints(files),'spearman':correlations,
        'size_token_matrices':[matrix(files,'tokens_used'),matrix(files,'cumulative_total')],
        'group_comparisons':groups,'parent_child_summary':pair_summary,'parent_child_pairs':pairs,
        'record_type_totals':totals,'files':files}


def number(v,digits=3):
    return 'NA' if v is None else f'{v:,.{digits}f}' if isinstance(v,float) else f'{v:,}'


def write_csv(result):
    fields=['file_id','thread_row_id','group_code','agent_role_present','has_parent_edge',
            'tokens_used','file_size_bytes','record_count','compacted_bytes','compacted_record_count']
    fields += ['bytes_'+kind.replace('[','').replace(']','').lower() for kind in TYPE_NAMES]
    fields += [scope+'_'+m for scope in SCOPES for m in ('observation_id','total','input','cached_input','uncached_input','output')]
    with (OUT/'sessions.csv').open('w',encoding='utf-8',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=fields);writer.writeheader()
        for f in result['files']:
            row={k:f.get(k) for k in fields};row['group_code']=GROUP_CODES[f['group']]
            row['agent_role_present']=int(f['agent_role_present']);row['has_parent_edge']=int(f['has_parent_edge'])
            for kind in TYPE_NAMES:row['bytes_'+kind.replace('[','').replace(']','').lower()]=f['by_type'].get(kind,{}).get('bytes',0)
            for scope in SCOPES:row[scope+'_observation_id']=f['endpoints'].get(scope,{}).get('observation_id')
            writer.writerow(row)
    pairs=result['parent_child_pairs']
    fields=list(pairs[0]) if pairs else ['edge_row_id','parent_file_id','child_file_id']
    with (OUT/'parent_child_pairs.csv').open('w',encoding='utf-8',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=fields);writer.writeheader();writer.writerows(pairs)


def markdown(report):
    a=report['analysis'];c=a['coverage'];p=a['parent_child_summary']
    lines=['# Token counters versus session-file storage','',
        '## Scope and method','',
        f"PostgreSQL snapshot: {report['database_time']}. Read-only repeatable-read transaction. {c['analysis_files']} files qualify out of {c['linked_files']} linked files ({c['state_thread_rows']} imported thread rows). Whole-file final physical record must be before 2026-06-19 00:00:00 UTC, i.e. through June 18 inclusive. No later files were truncated into the sample.",
        f"The Riyadh-day cutoff yields {c['eligible_by_tail_riyadh']} files. {c['later_than_cutoff']} linked files end later; {c['unknown_last_record_time']} have unknown final timestamps. Index totals reconcile with file size and catalog record count. Additional quality exclusions: {len(c['quality_exclusions'])}.",
        '- Research sources: state5_threads, research_sessions, research_session_records, research_token_usage_observations and state5_thread_spawn_edges only. No original session files, SQLite databases or external services were accessed.',
        '- For each file and each scope separately, choose the last unflagged observation by physical line, object byte offset, then row ID. is_possible_duplicate must be false. Do not choose the largest counter or sum counters. An endpoint with a missing field stays missing; do not fall back to an earlier value.',
        '- Compare tokens_used with total, input, input minus cached input, and output. Derived uncached input requires valid nonnegative counters and cached <= input.',
        '- Exact: equal integers. Near: unequal, with absolute difference <= 1% of the larger value. Different: greater difference. These categories are disjoint; percentages use comparable files only. Missing/invalid files are counted separately. Both zero is exact.',
        '- Spearman is Pearson correlation of average ranks, calculated with the Python standard library. Ties receive their mean rank. Constant variables or fewer than two pairs return NA. No p-values, causal estimates or byte-to-token conversions are calculated.',
        '- User means explicit thread_source=user with no child evidence; subagent means a child edge or explicit subagent source. Missing source is unknown unless an edge establishes child status. agent_role presence alone is not proof. Models/roles are not exported in CSV.',
        '', '## Meaning of tokens_used: endpoint agreement','',
        '| Scope | Endpoint comparison | Comparable | Exact n / % | Near n / % | Different n / % | Missing/invalid |',
        '|---|---|---:|---|---|---|---:|']
    for m in a['endpoint_comparisons']:
        lines.append(f"| {m['scope']} | {m['measure']} | {m['comparable_files']} | {m['exact']} / {number(m['exact_percent'])} | {m['near']} / {number(m['near_percent'])} | {m['different']} / {number(m['different_percent'])} | {m['missing_or_invalid']} |")
    valid=[m for m in a['endpoint_comparisons'] if m['comparable_files']]
    if valid:
        best=max(valid,key=lambda m:(m['exact_percent'],m['near_percent']))
        lines += ['', f"Highest exact agreement in this snapshot: **{best['scope']}.{best['measure']}**, {best['exact']}/{best['comparable_files']} ({number(best['exact_percent'])}%). This is an empirical match, not an assumed or universal definition of tokens_used. Endpoint gaps, state-snapshot timing and duplicated/inherited history can affect disagreements."]
    if a['total_disagreements']:
        lines += ['', '### Total-counter exceptions','',
                  '| Internal file ID | Scope | tokens_used | Latest total | Latest line | Earlier exact readings | Last earlier exact line / time |',
                  '|---:|---|---:|---:|---:|---:|---|']
        for d in a['total_disagreements']:
            previous=d['latest_earlier_exact']
            lines.append(f"| {d['file_id']} | {d['usage_scope']} | {number(d['tokens_used'])} | {number(d['latest_total'])} | {d['latest_line']} | {d['earlier_exact_matches']} | {str(previous['line_number'])+' / '+str(previous['recorded_at']) if previous else 'NA'} |")
        lines += ['', 'An earlier exact reading supports a possible snapshot-timing difference, but does not establish when state5_threads was updated. The primary comparison remains against the latest reading; the exception was not reclassified as exact.']
    lines += ['', '| Scope | Files with endpoint | Nested-history endpoints | Missing thread ID | Conflicting thread ID | Newer flagged endpoint skipped |',
              '|---|---:|---:|---:|---:|---:|']
    for scope,d in a['endpoint_diagnostics'].items():
        lines.append(f"| {scope} | {d['endpoint_files']} | {d['nested_history_endpoints']} | {d['reported_thread_identity_missing']} | {d['reported_thread_mismatch']} | {d['newer_flagged_endpoint']} |")
    lines += ['', 'Missing counter-level thread IDs mean linkage relies on the indexed file FK, not an explicit ID inside the counter object.',
              '', '| Scope | Observations | Flagged excluded | Retained | Files with retained observations |',
              '|---|---:|---:|---:|---:|']
    for u in a['usage_observation_counts']:
        lines.append(f"| {u['usage_scope']} | {u['observations']} | {u['excluded_flagged']} | {u['retained']} | {u['files_retained']} |")
    lines += [
        '', '## Rank association with storage','',
        '| Token variable | Storage variable | Paired files | Spearman rho | Status |',
        '|---|---|---:|---:|---|']
    for r in a['spearman']:
        lines.append(f"| {r['token_field']} | {r['storage_field']} | {r['n']} | {number(r['rho'],6)} | {r['reason'] or 'computed'} |")
    lines += ['', 'Each token variable is analyzed independently. tokens_used correlations retain that field name; they do not silently redefine it as total or uncached tokens. Storage metrics include nested/duplicated stored content. Rank association does not establish which bytes entered a model request.',
              '', '## File-size bands versus token bands','']
    for m in a['size_token_matrices']:
        lines += [f"### {m['token_field']}",'','| File size | '+' | '.join(m['token_bins'])+' | Total |',
                  '|---|'+'---:|'*(len(m['token_bins'])+1)]
        for label,counts in zip(m['size_bins'],m['counts']):lines.append('| '+label+' | '+' | '.join(str(n) for n in counts)+f' | {sum(counts)} |')
        lines.append('')
    lines += ['## Explicit user conversations versus subagents','',
              '| Group | Files | tokens_used n / median | Cumulative total n / median | File bytes median | Compacted bytes median | Record count median |',
              '|---|---:|---|---|---:|---:|---:|']
    for g in a['group_comparisons']:
        lines.append(f"| {g['group']} | {g['files']} | {g['tokens_used']['n']} / {number(g['tokens_used']['median'])} | {g['cumulative_total']['n']} / {number(g['cumulative_total']['median'])} | {number(g['file_size_bytes']['median'])} | {number(g['compacted_bytes']['median'])} | {number(g['record_count']['median'])} |")
    lines += ['', f"Unknown/conflicting sources are not counted as user conversations. Model availability: {c['model_missing']} missing; observed categories: `{json.dumps(c['models'],separators=(',',':'))}`. Comparisons are descriptive and unadjusted for duration, model, task or inherited history.",
              '', '## Parents and children with files on both sides','',
              f"Source edges: {p['source_edges']}; both files linked: {p.get('both_files_linked',0)}; eligible distinct pairs: {p['eligible_pairs']}; distinct parents: {p['distinct_parents']}; distinct children: {p['distinct_children']}.",
              '', '| Side, unique files | tokens_used n / median | Cumulative total n / median | File bytes n / median |',
              '|---|---|---|---|']
    for side in ('parents','children'):
        lines.append('| '+side+' | '+' | '.join(f"{p[side][k]['n']} / {number(p[side][k]['median'])}" for k in ('tokens_used','cumulative_total','file_size_bytes'))+' |')
    lines += ['', f"Per-pair tokens_used comparison: `{json.dumps(p['paired_tokens_comparison'])}`. Per-pair file size: `{json.dumps(p['paired_size_comparison'])}`.",
        'The pair CSV retains each distinct parent/child pair and both readings. A parent can occur in several pairs; side medians above use unique files to avoid weighting a parent by its child count. Parent and child counters are never added; inherited context can overlap. Ratios with zero parent values remain blank.',
        '', '## Storage by record type in the eligible files','',
        '| Record type | Records | Bytes |','|---|---:|---:|']
    for kind,v in sorted(a['record_type_totals'].items(),key=lambda kv:kv[1]['bytes'],reverse=True):
        lines.append(f"| {kind} | {number(v['records'])} | {number(v['bytes'])} |")
    lines += ['', '## Boundaries and reproducibility','',
        '- The cutoff selects whole files by their final physical record timestamp; maximum valid record time is also checked. It does not reconstruct the state database at an exact historical instant. Imported tokens_used and the file endpoint can differ in snapshot timing.',
        f"- Unparseable or missing record timestamps: {c['unparseable_record_times']} across {c['files_with_unparseable_record_times']} eligible files. Final timestamps are known; content inside opaque/nested fields was not read or dated.",
        '- No thread-scope endpoint is inferred from cumulative scope or vice versa. No last/response counters are accumulated into totals. Missing observations are not zeros. A duplicate flag is a prior heuristic, not proof that all remaining rows are semantically independent.',
        '- The file is the statistical unit. Files related by spawning/forking can share history, so samples are not necessarily independent. No generalization beyond these files through June 18, and no monetary estimate.',
        '- sessions.csv uses numeric internal file/thread-row/observation IDs only, numeric group codes and metrics. Group codes: 0 unknown, 1 user, 2 subagent, 3 conflict. Empty numeric fields mean missing; zero means observed/derived zero. No names, paths, conversation UUIDs or account identifiers.',
        '- parent_child_pairs.csv uses internal numeric edge/file IDs. summary.json includes numeric provenance and endpoint diagnostics; no raw content or original UUIDs.',
        '- Validation: tied-rank, perfect/reversed association, constant/missing inputs, near-threshold boundaries, uncached arithmetic and band boundaries are covered by seven synthetic tests. CSV reconciliation independently recomputes rank correlations with statistics.correlation.',
        f"- Query SHA-256: `{report['query_sha256']}`. Snapshot SHA-256: `{report['snapshot_sha256']}`. Transaction mode: {report['transaction_read_only']} / {report['transaction_isolation']}.",
        f"- Query seconds: {report['query_seconds']}; analysis/output seconds: {report['elapsed_seconds']}; Python peak working set: {number(report['peak_working_set_bytes'])} B.",
        '- Memory is the Python process peak, excluding psql and PostgreSQL service RAM. PostgreSQL parallel workers were disabled and work_mem was 8 MB per operation, not a global server memory limit. Python ran at below-normal priority.',
        '- Stop after this report pending researcher approval.', '']
    return '\n'.join(lines)


def main():
    resources(lower=True);start=time.perf_counter()
    query=(ROOT/'scripts/tokens_vs_storage_query.sql').read_text(encoding='utf-8')
    query_start=time.perf_counter();data=json.loads(run_psql(query));query_seconds=time.perf_counter()-query_start
    assert data['transaction_read_only']=='on' and data['transaction_isolation']=='repeatable read'
    result=analyze(data)
    OUT.mkdir(parents=True,exist_ok=True)
    write_csv(result)
    report={'version':'1.0.0','created_at':datetime.now(timezone.utc).isoformat(),
        **{k:data[k] for k in ('database_time','transaction_read_only','transaction_isolation')},
        'query_sha256':hashlib.sha256(query.encode()).hexdigest(),
        'snapshot_sha256':hashlib.sha256(json.dumps(data,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
        'cutoff_exclusive_utc':CUTOFF.isoformat(),'near_threshold':{'percent':1,'denominator':'max(tokens_used, compared_counter)','exclusive_of_exact':True},
        'analysis':result,'query_seconds':round(query_seconds,4),
        'elapsed_seconds':round(time.perf_counter()-start,4),'peak_working_set_bytes':resources()}
    (OUT/'summary.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    (OUT/'report.md').write_text(markdown(report),encoding='utf-8')
    print(json.dumps({'coverage':result['coverage'],'endpoint_comparisons':result['endpoint_comparisons'],
        'spearman':result['spearman'],'groups':result['group_comparisons'],'parents_children':result['parent_child_summary'],
        'query_seconds':report['query_seconds'],'elapsed_seconds':report['elapsed_seconds'],
        'peak_working_set_bytes':report['peak_working_set_bytes']}))


if __name__=='__main__':main()
