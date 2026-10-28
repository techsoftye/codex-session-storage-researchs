"""Length-control extension: fixed prior cohort, PostgreSQL measurements, stdlib."""
from datetime import datetime, timezone
from fractions import Fraction
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics
import time

from import_session_survey import run_psql
from analyze_tokens_vs_storage import average_ranks
from analyze_logs_request_comparison import resources

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'data/reports/tokens_vs_storage_v1'
NORMALIZED=('tokens_per_record','bytes_per_record','compacted_byte_fraction','compacted_per_1000_records')
CUTOFF=datetime(2026,6,19,tzinfo=timezone.utc)


def pearson(x,y):
    if len(x)!=len(y):raise ValueError('unequal_series_lengths')
    if len(x)<2:return None
    mx=math.fsum(x)/len(x);my=math.fsum(y)/len(y)
    dx=[v-mx for v in x];dy=[v-my for v in y]
    xx=math.fsum(v*v for v in dx);yy=math.fsum(v*v for v in dy)
    if xx==0 or yy==0:return None
    return max(-1.0,min(1.0,math.fsum(a*b for a,b in zip(dx,dy))/math.sqrt(xx*yy)))


def spearman(x,y):
    return pearson(average_ranks(x),average_ranks(y))


def residuals(values,control):
    """OLS with intercept on one ranked control variable."""
    mv=math.fsum(values)/len(values);mc=math.fsum(control)/len(control)
    vc=[v-mv for v in values];zc=[z-mc for z in control]
    ss=math.fsum(z*z for z in zc)
    if ss==0:return None
    slope=math.fsum(v*z for v,z in zip(vc,zc))/ss
    errors=[v-slope*z for v,z in zip(vc,zc)]
    return {'residuals':errors,'slope':slope,'intercept':mv-slope*mc,
        'residual_sum_squares':math.fsum(e*e for e in errors),
        'original_sum_squares':math.fsum(v*v for v in vc),
        'residual_sum':math.fsum(errors),'residual_dot_centered_control':math.fsum(e*z for e,z in zip(errors,zc))}


def partial_spearman(x,y,z):
    if not len(x)==len(y)==len(z):raise ValueError('unequal_series_lengths')
    if len(x)<3:return {'n':len(x),'rho':None,'reason':'fewer_than_three_complete_cases'}
    rx,ry,rz=map(average_ranks,(x,y,z))
    ex=residuals(rx,rz);ey=residuals(ry,rz)
    base={'n':len(x),'unadjusted_spearman':pearson(rx,ry),'x_control_spearman':pearson(rx,rz),
          'y_control_spearman':pearson(ry,rz)}
    if ex is None or ey is None:return {**base,'rho':None,'reason':'constant_control_rank'}
    if any(e['residual_sum_squares']<=1e-12*max(1,e['original_sum_squares']) for e in (ex,ey)):
        return {**base,'rho':None,'reason':'zero_or_negligible_residual_variance'}
    rho=pearson(ex['residuals'],ey['residuals'])
    raw,xz,yz=(base[k] for k in ('unadjusted_spearman','x_control_spearman','y_control_spearman'))
    formula=(raw-xz*yz)/math.sqrt((1-xz*xz)*(1-yz*yz))
    if abs(rho-formula)>1e-10:raise ValueError('partial_identity_crosscheck_failed')
    return {**base,'rho':rho,'reason':None,'formula_crosscheck_rho':formula,
        'residual_models':{name:{k:v for k,v in model.items() if k!='residuals'} for name,model in (('x',ex),('y',ey))}}


def stratum_bounds(records):
    if type(records) is not int or records<=0:raise ValueError('nonpositive_record_count')
    lower=1 << (records.bit_length()-1)
    return lower,lower*2


def normalize(row):
    r=dict(row)
    for key in ('tokens_used','file_size_bytes','record_count','compacted_bytes','compacted_record_count'):
        if type(r[key]) is not int or r[key]<0:raise ValueError('missing_or_invalid_numeric_input')
    if r['record_count']==0 or r['file_size_bytes']==0:raise ValueError('undefined_normalization_denominator')
    if r['compacted_bytes']>r['file_size_bytes'] or r['compacted_record_count']>r['record_count']:
        raise ValueError('invalid_subset_totals')
    r.update(tokens_per_record=Fraction(r['tokens_used'],r['record_count']),
        bytes_per_record=Fraction(r['file_size_bytes'],r['record_count']),
        compacted_byte_fraction=Fraction(r['compacted_bytes'],r['file_size_bytes']),
        compacted_per_1000_records=Fraction(1000*r['compacted_record_count'],r['record_count']),
        has_compacted=r['compacted_record_count']>0)
    r['stratum_lower'],r['stratum_upper_exclusive']=stratum_bounds(r['record_count'])
    return r


def group_summary(rows):
    return {'n':len(rows),'median_tokens_per_record':float(statistics.median(r['tokens_per_record'] for r in rows)) if rows else None,
        'median_record_count':statistics.median(r['record_count'] for r in rows) if rows else None,
        'min_record_count':min((r['record_count'] for r in rows),default=None),
        'max_record_count':max((r['record_count'] for r in rows),default=None)}


def calculate(files):
    rows=[normalize(f) for f in files]
    x=[r['tokens_per_record'] for r in rows]
    normalized=[{'x':'tokens_per_record','y':name,'n':len(rows),
                 'rho':spearman(x,[r[name] for r in rows]),'distinct_x':len(set(x)),
                 'distinct_y':len({r[name] for r in rows})} for name in NORMALIZED[1:]]
    partial=[{'x':'tokens_used','y':name,'control':'record_count',
              **partial_spearman([r['tokens_used'] for r in rows],[r[name] for r in rows],[r['record_count'] for r in rows])}
             for name in ('file_size_bytes','compacted_bytes')]
    strata=[]
    for lower in sorted({r['stratum_lower'] for r in rows}):
        within=[r for r in rows if r['stratum_lower']==lower]
        yes=group_summary([r for r in within if r['has_compacted']])
        no=group_summary([r for r in within if not r['has_compacted']])
        strata.append({'lower_inclusive':lower,'upper_exclusive':2*lower,'files':len(within),
            'with_compacted':yes,'without_compacted':no,
            'median_difference_with_minus_without':yes['median_tokens_per_record']-no['median_tokens_per_record'] if yes['n'] and no['n'] else None,
            'both_groups_present':bool(yes['n'] and no['n']),
            'minimum_group_count':min(yes['n'],no['n'])})
    exported=[{k:float(v) if isinstance(v,Fraction) else v for k,v in r.items()} for r in rows]
    return {'files':len(rows),'with_compacted':sum(r['has_compacted'] for r in rows),
        'without_compacted':sum(not r['has_compacted'] for r in rows),
        'zero_tokens_used':sum(r['tokens_used']==0 for r in rows),
        'normalized_spearman':normalized,'partial_spearman':partial,'record_count_strata':strata,
        'mixed_strata':sum(s['both_groups_present'] for s in strata),
        'mixed_strata_at_least_five_per_group':sum(s['minimum_group_count']>=5 for s in strata),
        'normalized_rows':exported}


def fmt(v,digits=3):
    return 'NA' if v is None else f'{v:,.{digits}f}' if isinstance(v,float) else f'{v:,}'


def markdown(report):
    a=report['analysis']
    lines=['# Length-control extension: tokens and storage','',
        f"The same **{a['files']} files** as the accepted analysis are retained through June 18, 2026. Every measurement was read from PostgreSQL in a read-only repeatable-read transaction. The earlier summary was used only as an internal-file-ID membership manifest, never as a measurement source.",
        '', '## Measures and fixed methods','',
        '| Measure | Definition |','|---|---|',
        '| tokens_per_record | tokens_used / record_count |',
        '| bytes_per_record | file_size_bytes / record_count |',
        '| compacted_byte_fraction | compacted_bytes / file_size_bytes, on a 0–1 scale |',
        '| compacted_per_1000_records | 1000 × compacted_record_count / record_count |',
        '', '- Ratios use exact rational numbers for rank calculations, so equivalent fractions tie exactly. Exported decimals are rounded binary floating-point representations; original integer numerators and denominators are retained for reconstruction.',
        '- No zero or missing denominator is silently replaced. The run fails on invalid inputs; all 189 files were valid. Observed zero tokens_used values are retained.',
        '- Normalized associations are Spearman correlations, using average ranks for ties.',
        '- Partial Spearman: rank tokens_used (X), the storage measure (Y), and record_count (Z); regress each of rank(X) and rank(Y) on an intercept plus rank(Z). Compute Pearson correlation of the two residual vectors. **Do not rank the residuals again.**',
        '- Cross-check the residual result with (rho_XY - rho_XZ*rho_YZ) / sqrt((1-rho_XZ^2)*(1-rho_YZ^2)). Constant control ranks or negligible residual variance yield NA.',
        '- Record-count strata were specified before outcome comparison as powers of two: [2^k, 2^(k+1)). Empty strata are omitted; boundaries are not moved after viewing outcomes. Both subgroup medians and subgroup record-count medians are shown.',
        '', '## Spearman correlations of normalized measures','',
        '| X | Y | Files | Spearman rho |','|---|---|---:|---:|']
    for r in a['normalized_spearman']:lines.append(f"| {r['x']} | {r['y']} | {r['n']} | {fmt(r['rho'],6)} |")
    lines += ['', '## Partial Spearman controlling record count','',
        '| X | Y | Control | Files | Unadjusted rho | Partial rho |',
        '|---|---|---|---:|---:|---:|']
    for r in a['partial_spearman']:
        lines.append(f"| {r['x']} | {r['y']} | {r['control']} | {r['n']} | {fmt(r.get('unadjusted_spearman'),6)} | {fmt(r['rho'],6)} |")
    lines += ['', '## Compacted versus no compacted, within record-count strata','',
        f"Files with at least one compacted record: **{a['with_compacted']}**; without: **{a['without_compacted']}**. Mixed strata: {a['mixed_strata']}; mixed strata with at least five files in each group: {a['mixed_strata_at_least_five_per_group']}.",
        '', '| Records, inclusive range | With compacted n | Without n | Median tokens/record, with | Median tokens/record, without | Median records, with | Median records, without | With − without |',
        '|---|---:|---:|---:|---:|---:|---:|---:|']
    for s in a['record_count_strata']:
        yes,no=s['with_compacted'],s['without_compacted']
        lines.append(f"| {s['lower_inclusive']:,}–{s['upper_exclusive']-1:,} | {yes['n']} | {no['n']} | {fmt(yes['median_tokens_per_record'])} | {fmt(no['median_tokens_per_record'])} | {fmt(yes['median_record_count'])} | {fmt(no['median_record_count'])} | {fmt(s['median_difference_with_minus_without'])} |")
    mixed=[s for s in a['record_count_strata'] if s['both_groups_present']]
    lines += ['', 'NA means a subgroup is absent. Differences are descriptive, not treatment effects. Sparse groups are shown rather than merged or removed; inspect subgroup counts before interpreting a median. No averaging of layer effects or token totals across scopes was performed.',
        '', '## Findings within this cohort','']
    for p in a['partial_spearman']:
        lines.append(f"- `{p['y']}`: raw rank correlation {fmt(p.get('unadjusted_spearman'),6)} becomes {fmt(p['rho'],6)} after controlling ranked record_count. This is a conditional rank association, not the causal effect of stored bytes.")
    lines += [f"- Only {len(mixed)} strata, containing {sum(s['files'] for s in mixed)} files, support a within-stratum comparison. The remaining {a['files']-sum(s['files'] for s in mixed)} files have no opposite group in their stratum.",
        '- In the 256–511 stratum, the compacted group has only three files and a median of 508 records versus 315 in the other group. The groups are not closely matched even within this fixed band.',
        '- The 512–1023 stratum has eight files per group: median tokens/record is 14,104.701 with compacted versus 13,426.284 without; median record counts are 806.5 versus 760.5. This single small comparison cannot establish a compaction effect.',
        '- The positive normalized associations involving compacted share/frequency remain descriptive: they do not additionally control record_count, and presence of compacted is strongly separated by length in these files. They should not be substituted for the partial-rank results.',
        '', '## Interpretation limits','',
        '- record_count is a proxy for recorded activity/length, not elapsed session duration, number of model requests, or the amount of model-visible context. A large compacted record is still one record.',
        '- Partial rank correlation removes a linear association with ranked record_count. It does not fully isolate session length or eliminate all nonlinear patterns and confounding.',
        '- tokens_per_record and bytes_per_record share a denominator, which can induce association. compacted measures also reuse file/record totals. Normalization is descriptive rather than proof of causality.',
        '- Compaction can occur because sessions are already long or intensive. Models, instructions, tasks, image content, inherited history and repeated storage remain potential confounders. Related files are not necessarily independent.',
        '- The earlier empirical interpretation of tokens_used is retained as a limitation: 179/180 available endpoints matched cumulative total; one matched an earlier reading. This extension uses the recorded tokens_used values for all 189 files, including nine zeros. It does not infer token attribution to compacted content.',
        '- No byte-to-token conversion, monetary estimate, causal claim, or generalization beyond this cohort through June 18. No new source files or SQLite databases were opened and no database rows were changed.',
        '', '## Artifacts, verification and resources','',
        '- length_control.json contains methods, correlations, residual-model diagnostics, strata and every normalized row. length_control.csv contains internal numeric IDs, original measurements and derived ratios; no names, file paths or conversation UUIDs.',
        f"- Cohort ID SHA-256: `{report['cohort_ids_sha256']}`; query SHA-256: `{report['query_sha256']}`; numeric measurement SHA-256: `{report['measurement_sha256']}`.",
        f"- Transaction: {report['transaction_read_only']} / {report['transaction_isolation']}; database snapshot time: {report['database_time']}.",
        '- Prior membership, exact cohort count, valid timestamps/cutoff, record-count and byte reconciliation, rank ties, residual orthogonality and the partial-correlation identity are checked. Synthetic tests cover degenerate cases and bin boundaries.',
        f"- PostgreSQL query: {report['query_seconds']} seconds; analysis and CSV generation: {report['elapsed_seconds']} seconds; peak Python working set: {report['peak_working_set_bytes']:,} B.",
        '- Memory excludes psql and PostgreSQL service processes. Python runs below normal priority; SQL parallel workers are disabled, work_mem is 8 MB per operation, and the query has a 60-second timeout.',
        '- Stop here pending researcher approval.', '']
    return '\n'.join(lines)


def main():
    resources(lower=True);started=time.perf_counter()
    # Identity manifest only: the prior file supplies no measurements to calculations.
    with (OUT/'summary.json').open(encoding='utf-8') as stream:
        manifest=json.load(stream)
    previous_ids=sorted(f['file_id'] for f in manifest['analysis']['files'])
    del manifest
    assert len(previous_ids)==len(set(previous_ids))==189
    query=(ROOT/'scripts/length_control_query.sql').read_text(encoding='utf-8')
    began=time.perf_counter();data=json.loads(run_psql(query));query_seconds=time.perf_counter()-began
    assert data['transaction_read_only']=='on' and data['transaction_isolation']=='repeatable read'
    files=data['files']
    assert sorted(f['file_id'] for f in files)==previous_ids
    for f in files:
        assert f['record_count']==f['indexed_record_count'] and f['file_size_bytes']==f['indexed_bytes']
        assert datetime.fromisoformat(f['last_record_at'])<CUTOFF
        assert f['max_recorded_at'] is not None and datetime.fromisoformat(f['max_recorded_at'])<CUTOFF
    # No dates or other content are needed in per-file output.
    measurements=[{k:f[k] for k in ('file_id','thread_row_id','tokens_used','file_size_bytes','record_count',
                                   'compacted_bytes','compacted_record_count')} for f in files]
    result=calculate(measurements)
    with (OUT/'length_control.csv').open('w',encoding='utf-8',newline='') as stream:
        fields=list(result['normalized_rows'][0]);writer=csv.DictWriter(stream,fieldnames=fields);writer.writeheader()
        for row in result['normalized_rows']:
            writer.writerow({k:int(v) if isinstance(v,bool) else v for k,v in row.items()})
    report={'version':'1.0.0','created_at':datetime.now(timezone.utc).isoformat(),
        **{k:data[k] for k in ('database_time','transaction_read_only','transaction_isolation')},
        'cohort_ids_sha256':hashlib.sha256(json.dumps(previous_ids,separators=(',',':')).encode()).hexdigest(),
        'query_sha256':hashlib.sha256(query.encode()).hexdigest(),
        'measurement_sha256':hashlib.sha256(json.dumps(measurements,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
        'methods':{'partial':'Pearson correlation of OLS residuals of average ranks, controlling rank(record_count) with intercept',
                   'normalized_ranks':'exact rational values; average ranks for ties','strata':'[2^k,2^(k+1)), fixed in advance',
                   'cutoff_exclusive_utc':CUTOFF.isoformat(),'manifest_use':'prior report internal file IDs only'},
        'analysis':result,'query_seconds':round(query_seconds,4),
        'elapsed_seconds':round(time.perf_counter()-started,4),'peak_working_set_bytes':resources()}
    (OUT/'length_control.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    (OUT/'length_control.md').write_text(markdown(report),encoding='utf-8')
    print(json.dumps({k:result[k] for k in ('files','with_compacted','without_compacted','zero_tokens_used',
        'normalized_spearman','partial_spearman','mixed_strata','mixed_strata_at_least_five_per_group','record_count_strata')}))
    print(json.dumps({k:report[k] for k in ('query_seconds','elapsed_seconds','peak_working_set_bytes')}))


if __name__=='__main__':main()
