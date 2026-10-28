"""Read-only hash-index extension. No session sources, text or images opened.

--stage-text reads PostgreSQL and text_index only.
--finalize uses that stage and prior image metrics. --read-image-index is an
explicit, separately authorized opt-in for exact image disappearance measures.
"""
import argparse
from collections import Counter, defaultdict
import csv
from datetime import datetime, timezone
import itertools
import json
from pathlib import Path
import sqlite3
import time

from analyze_compaction_growth import ROOT, OUT, digest, describe, write_csv
from analyze_logs_request_comparison import resources
from import_session_survey import run_psql

TEXT_INDEX = ROOT/'data/reports/compacted_text_duplicates_v1/text_index.sqlite3'
IMAGE_INDEX = ROOT/'data/reports/compacted_image_duplicates_v1/image_index.sqlite3'
STAGE = OUT/'text_growth_stage.json'


def readonly(path):
    c = sqlite3.connect(path.as_uri()+'?mode=ro', uri=True)
    for sql in ('PRAGMA query_only=ON','PRAGMA cache_size=-8192','PRAGMA mmap_size=0','PRAGMA temp_store=FILE','BEGIN'):
        c.execute(sql)
    return c


def collapse(rows):
    """sha -> [occurrences, stored bytes, canonical byte length, first stored size]."""
    result = {}
    for sha, canonical, stored in rows:
        if not isinstance(sha,bytes) or len(sha)!=32 or canonical<=0 or stored<=0:
            raise ValueError('invalid_hash_index_value')
        if sha not in result:
            result[sha] = [0,0,canonical,stored]
        item = result[sha]
        if item[2]!=canonical:
            raise ValueError('hash_length_conflict')
        item[0]+=1
        item[1]+=stored
    return result


def compare(previous, current):
    """Current repetition uses current bytes; disappearance uses previous bytes."""
    result = dict(unique_blocks=len(current), occurrences=sum(v[0] for v in current.values()),
                  stored_bytes=sum(v[1] for v in current.values()))
    names = ('adjacent_hash_matches','adjacent_repeated_occurrences','adjacent_repeated_stored_bytes',
             'disappeared_unique_blocks','disappeared_occurrences','disappeared_stored_bytes',
             'appeared_unique_blocks','appeared_occurrences','appeared_stored_bytes',
             'shared_copy_count_reduction','shared_copy_count_increase','shared_stored_bytes_decrease',
             'net_stored_bytes_decrease')
    if previous is None:
        # No transition: repetition is zero; disappearance/deltas are unavailable.
        result.update({k:0 if k.startswith('adjacent_') else None for k in names})
        return result
    shared = previous.keys() & current.keys()
    lost = previous.keys() - current.keys()
    added = current.keys() - previous.keys()
    result.update(adjacent_hash_matches=len(shared),
        adjacent_repeated_occurrences=sum(current[h][0] for h in shared),
        adjacent_repeated_stored_bytes=sum(current[h][1] for h in shared),
        disappeared_unique_blocks=len(lost),disappeared_occurrences=sum(previous[h][0] for h in lost),
        disappeared_stored_bytes=sum(previous[h][1] for h in lost),
        appeared_unique_blocks=len(added),appeared_occurrences=sum(current[h][0] for h in added),
        appeared_stored_bytes=sum(current[h][1] for h in added),
        shared_copy_count_reduction=sum(max(previous[h][0]-current[h][0],0) for h in shared),
        shared_copy_count_increase=sum(max(current[h][0]-previous[h][0],0) for h in shared),
        shared_stored_bytes_decrease=sum(previous[h][1]-current[h][1] for h in shared),
        net_stored_bytes_decrease=sum(v[1] for v in previous.values())-result['stored_bytes'])
    if result['disappeared_stored_bytes']-result['appeared_stored_bytes']+result['shared_stored_bytes_decrease']!=result['net_stored_bytes_decrease']:
        raise ValueError('transition_byte_identity_failed')
    return result


def check_index(c, db_records, files):
    actual = {r[0]:r[1:] for r in c.execute('SELECT record_id,session_id,source_offset,sequence FROM records')}
    expected = {r['record_id']:(r['file_id'],r['source_byte_offset'],r['compacted_ordinal']) for r in db_records}
    if actual!=expected:
        raise ValueError('index_record_coverage_or_locators_mismatch')
    actual_files = {r[0]:r[1:] for r in c.execute('SELECT session_id,records,source_bytes FROM files')}
    expected_files = {f['file_id']:(f['compacted_records'],f['compacted_bytes']) for f in files}
    if actual_files!=expected_files:
        raise ValueError('index_file_totals_mismatch')


def text_stream(c, records):
    """Single table scan; fail if index insertion order differs from source order."""
    order = {r['record_id']:(r['file_id'],r['compacted_ordinal']) for r in records}
    cursor = iter(c.execute('SELECT session_id,record_id,sha256,utf8_bytes,stored_bytes FROM occurrences ORDER BY id'))
    pending = next(cursor,None)
    last_key = None
    for record in records:
        target = order[record['record_id']]
        def rows():
            nonlocal pending,last_key
            while pending is not None:
                file_id, rid, sha, canonical, stored = pending
                key = order.get(rid)
                if key is None or key[0]!=file_id or (last_key is not None and key<last_key) or key<target:
                    raise ValueError('text_index_order_or_identity_mismatch')
                if key!=target:
                    break
                last_key=key
                yield sha,canonical,stored
                pending=next(cursor,None)
        yield record,collapse(rows())
    if pending is not None:
        raise ValueError('unmatched_text_occurrence')


def verify_by_hash(c, records):
    """Independent second pass via existing text_hash index; bounded one-hash map."""
    order={r['record_id']:(r['file_id'],r['compacted_ordinal']) for r in records}
    next_keys={(r['file_id'],r['compacted_ordinal']) for r in records if r['compacted_ordinal']>1}
    decrease_keys={(r['file_id'],r['compacted_ordinal']) for r in records if (r['record_size_decrease_bytes'] or 0)>0}
    totals=Counter()
    query='SELECT sha256,session_id,record_id,utf8_bytes,stored_bytes FROM occurrences ORDER BY sha256,id'
    for sha,group in itertools.groupby(c.execute(query),key=lambda r:r[0]):
        nodes={}
        canonical=None
        for _,fid,rid,length,stored in group:
            key=order[rid]
            if key[0]!=fid or (canonical is not None and canonical!=length):
                raise ValueError('hash_verifier_identity_conflict')
            canonical=length
            node=nodes.setdefault(key,[0,0])
            node[0]+=1;node[1]+=stored
        totals['global_unique_text_blocks']+=1
        totals['global_unique_utf8_bytes']+=canonical
        for (fid,seq),(count,stored) in nodes.items():
            totals['occurrences']+=count;totals['stored_bytes']+=stored
            if (fid,seq-1) in nodes:
                totals['adjacent_hash_matches']+=1
                totals['adjacent_repeated_occurrences']+=count
                totals['adjacent_repeated_stored_bytes']+=stored
            if (fid,seq+1) in next_keys and (fid,seq+1) not in nodes:
                totals['disappeared_unique_blocks']+=1
                totals['disappeared_occurrences']+=count
                totals['disappeared_stored_bytes']+=stored
                if (fid,seq+1) in decrease_keys:
                    totals['decrease_disappeared_unique_blocks']+=1
                    totals['decrease_disappeared_stored_bytes']+=stored
    return dict(totals)


def save(path, value):
    path.write_text(json.dumps(value,indent=2,allow_nan=False)+'\n',encoding='utf-8')


def stage_text():
    if STAGE.exists():raise ValueError('text_stage_already_exists')
    clock=time.monotonic();resources(lower=True)
    query=Path(__file__).with_name('compaction_growth_query.sql').read_text(encoding='utf-8')
    raw=run_psql(query);db=json.loads(raw)
    query_seconds=time.monotonic()-clock
    if db['transaction_read_only']!='on':raise ValueError('postgres_not_read_only')
    for f in db['files']:
        if f['file_size_bytes']!=f['indexed_bytes'] or f['record_count']!=f['indexed_records']:
            raise ValueError('postgres_file_totals_mismatch')
    source_records=db['records'];files=[];previous_file=None;previous_size=None;ordinal=0
    sizes={f['file_id']:f['file_size_bytes'] for f in db['files']}
    for r in source_records:
        if r['file_id']!=previous_file:ordinal=0;previous_size=None
        ordinal+=1;r['compacted_ordinal']=ordinal
        r['record_size_decrease_bytes']=previous_size-r['source_record_bytes'] if previous_size is not None else None
        previous_file=r['file_id'];previous_size=r['source_record_bytes']
    for fid,group in itertools.groupby(source_records,key=lambda r:r['file_id']):
        rows=list(group)
        files.append(dict(file_id=fid,file_size_bytes=sizes[fid],compacted_records=len(rows),
                          compacted_bytes=sum(r['source_record_bytes'] for r in rows)))
    before=digest(TEXT_INDEX);c=readonly(TEXT_INDEX);check_index(c,source_records,files)
    outcomes={name:dict(occurrences=n,stored_bytes=b) for name,n,b in c.execute('SELECT outcome,sum(occurrences),sum(stored_bytes) FROM field_stats GROUP BY outcome')}
    if any(k in outcomes for k in ('invalid_unicode','excluded_unclassified')):
        raise ValueError('unresolved_text_coverage')
    current_file=None;prior=None;seen={};first_bytes=0;output=[];file_details={}
    def finish_file():
        if current_file is not None:
            file_details[current_file]=dict(text_unique_blocks=len(seen),text_unique_utf8_bytes=sum(seen.values()),
                                           text_retained_first_copy_stored_bytes=first_bytes)
    for r,current in text_stream(c,source_records):
        if r['file_id']!=current_file:
            finish_file();current_file=r['file_id'];prior=None;seen={};first_bytes=0
        for sha,values in current.items():
            if sha not in seen:seen[sha]=values[2];first_bytes+=values[3]
            elif seen[sha]!=values[2]:raise ValueError('within_file_hash_length_conflict')
        measured=compare(prior,current)
        if measured['stored_bytes']>r['source_record_bytes']:raise ValueError('text_bytes_exceed_record')
        output.append({**r,**{'text_'+k:v for k,v in measured.items()}})
        prior=current
    finish_file()
    verification=verify_by_hash(c,source_records)
    for key in ('occurrences','stored_bytes','adjacent_hash_matches','adjacent_repeated_occurrences',
                'adjacent_repeated_stored_bytes','disappeared_unique_blocks','disappeared_occurrences','disappeared_stored_bytes'):
        if verification.get(key,0)!=sum(r['text_'+key] or 0 for r in output):
            raise ValueError('independent_hash_verification_failed_'+key)
    if verification['stored_bytes']!=outcomes['text']['stored_bytes'] or verification['occurrences']!=outcomes['text']['occurrences']:
        raise ValueError('text_field_totals_mismatch')
    c.close()
    if digest(TEXT_INDEX)!=before:raise ValueError('text_index_changed')
    for f in files:f.update(file_details[f['file_id']])
    result=dict(version='1.0.0',generated_at=datetime.now(timezone.utc).isoformat(),
        database_time=db['database_time'],postgres_read_only=True,text_index_read_only=True,
        text_index_sha256=before,records=output,files=files,verification=verification,field_outcomes=outcomes,
        corpus_bytes=sum(f['file_size_bytes'] for f in db['files']),corpus_files=len(db['files']),
        resources=dict(stage_text_seconds=round(time.monotonic()-clock,6),postgres_seconds=round(query_seconds,6),
                       peak_python_bytes=resources(),measurement_scope='Through source validation; final stage serialization excluded'))
    save(STAGE,result)
    print(json.dumps({k:result[k] for k in ('verification','resources')},indent=2))


def images_from_index(stage):
    c=readonly(IMAGE_INDEX);check_index(c,stage['records'],stage['files'])
    result={};fid=None;prior=None
    for r in stage['records']:
        if r['file_id']!=fid:prior=None;fid=r['file_id']
        current=collapse(c.execute('SELECT sha256,decoded_bytes,stored_bytes FROM occurrences WHERE record_id=? ORDER BY id',(r['record_id'],)))
        result[r['record_id']]=compare(prior,current)
        prior=current
    c.close();return result


def dominant(parts):
    if any(v is None for v in parts.values()):return 'unresolved'
    maximum=max(parts.values());winners=[k for k,v in parts.items() if v==maximum]
    return winners[0] if len(winners)==1 else 'tie'


def render(s):
    t=s['totals'];d=s['decreases'];rs=s['resources']
    lines=['# Text recurrence and compacted size decreases','',f"Generated: {s['generated_at']}.",'',
      ('Text recurrence, combined storage and net-drop decomposition are complete. Exact vanished-image byte and occurrence counts await permission to read the image index.'
       if not s['source']['image_index_read'] else 'All requested text and image transition measures are complete.'),'',
      f"Coverage: **{t['files']:,} files, {t['records']:,} compacted records, {t['compacted_bytes']/1024**3:.4f} GiB**. "
      'The original session files were not opened. Only numeric locators and existing hash indexes/derived metrics were processed.', '',
      '## Disjoint allocation of all compacted bytes','',
      '| Allocation | Bytes | GiB | % of compacted |','| --- | ---: | ---: | ---: |']
    for r in s['combined_allocation']:
        lines.append(f"| {r['category']} | {r['bytes']:,} | {r['bytes']/1024**3:.6f} | {r['percent_of_compacted']:.4f} |")
    lines += ['', 'The first two rows count every current-record occurrence whose hash appeared in the immediately previous compacted record of the same file. '
      'The remainder includes first-record content, newly appearing/nonadjacent text and images, and all other bytes. '
      '**Remainder does not mean unique content.** These three rows are disjoint and sum to all compacted bytes. '
      'Within-record duplicates and whole-file duplicate totals are not added again.', '',
      'Text and image metrics use physical JSON string bytes including quotes/escaping. Text is hashed after JSON unescaping into UTF-8, '
      'without whitespace or Unicode normalization. Selected nonempty content text, reasoning summaries and tool input/output strings are covered. '
      'Image-reference strings are excluded from text. Text strings containing media markers are still whole text blocks; embedded substrings are not separately counted as image occurrences. '
      'Both histories are combined. A changed text block is a disappeared hash and an appeared hash, not proven semantic removal.', '',
      '## Text recurrence','',
      f"{t['text_occurrences']:,} text occurrences; {t['global_unique_text_blocks']:,} global exact unique text blocks. "
      f"Adjacent recurrence: {t['text_adjacent_hash_matches']:,} distinct hash-transition matches, "
      f"{t['text_adjacent_repeated_occurrences']:,} current occurrences, {t['text_adjacent_repeated_stored_bytes']/1024**3:.6f} GiB. "
      'Per-file counts and storage fractions are in text_growth_files.csv.', '',
      '## Size decreases','',
      f"**{d['count']:,} decreasing transitions**. Median drop: **{d['bytes_distribution']['median']:,.0f} bytes**; "
      f"p90: **{d['bytes_distribution']['p90']:,.1f} bytes**; maximum: **{d['bytes_distribution']['max']:,.0f} bytes**. "
      f"Sum of observed drops: **{d['total_drop_bytes']/1024**3:.6f} GiB**. "
      'This sums transitions, including repeated shrink/grow cycles; it is not a recoverable-space estimate.', '',
      '| Drop size (bytes, inclusive lower / exclusive upper) | Transitions | Sum of drops (bytes) |',
      '| --- | ---: | ---: |']
    for b in d['bands']:lines.append(f"| {b['lower']} to {b['upper_exclusive'] if b['upper_exclusive'] is not None else 'unbounded'} | {b['count']} | {b['drop_bytes']:,} |")
    lines += ['', '| Largest positive net decrease component | Transitions |','| --- | ---: |']
    for k,v in d['dominant_component_counts'].items():lines.append(f'| {k} | {v:,} |')
    lines += ['', '| Component over decreasing transitions | Disappeared hash-transition pairs | Disappeared occurrences | Previous stored bytes of disappeared hashes | Signed net byte decrease |',
      '| --- | ---: | ---: | ---: | ---: |']
    for k,v in d['components'].items():
        def fmt(x):return 'unavailable' if x is None else f'{x:,}'
        lines.append(f"| {k} | {fmt(v['disappeared_unique_blocks'])} | {fmt(v['disappeared_occurrences'])} | {fmt(v['disappeared_stored_bytes'])} | {fmt(v['net_stored_bytes_decrease'])} |")
    lines += ['', '| Component | % of aggregate net drop |','| --- | ---: |']
    for k,v in d['components'].items():
        lines.append(f"| {k} | {100*v['net_stored_bytes_decrease']/d['total_drop_bytes']:.4f} |")
    lines += ['', 'Disappeared means a hash present in the preceding record is entirely absent from the next; its bytes are measured in the preceding record, including all copies there. '
      'If a hash remains but has fewer copies, it is not counted as disappeared. Separate copy-count and shared-byte changes are exported. '
      'Within each component: **net decrease = disappeared bytes − appeared bytes + shared-hash byte decrease**. '
      'The last term can reflect copy-count or JSON-representation changes. Additions can offset disappearances. '
      '**Image net decrease + text net decrease + other net decrease = full-record decrease**. '
      'Negative component decreases mean that component grew. Dominance uses the largest positive net component, including other bytes; it is not a causal explanation.', '',
      '## Scope and verification','',
      s['image_disappearance_scope'], '',
      'PostgreSQL was read-only. File/record counts, byte totals, IDs, offsets and sequence reconcile with the text index. '
      'A second streaming pass ordered by text hash independently verified recurrence and disappearance totals. '
      'Existing image metrics were matched by file ID, record ID, line, ordinal and size. '
      'Text and image physical string locations are separate by the profiler rules. No texts, image bytes, source names/paths or account identifiers are exported.', '',
      'No claim about model context, requests, tokens, money or causes is established by storage recurrence. Findings are limited to these indexed records. '
      'Unavailable quantities are NULL, not zero. Empty text and excluded encrypted/technical fields remain in other bytes.', '',
      '## Files and resources','',
      '- text_growth.json: combined totals, decrease distribution, coverage and per-file summaries.',
      '- text_growth_records.csv: every compacted record, text/image recurrence and transition measures.',
      '- text_growth_files.csv: per-file text recurrence and duplication.',
      '- compacted_decreases.csv: every decreasing transition and its measured components.',
      '- decrease_distribution.csv: fixed drop-size bands.', '',
      f"Text analysis/validation: {rs['stage_text_seconds']:.3f} s; combination/image comparison: {rs['finalize_seconds']:.3f} s. "
      f"Maximum measured Python working set across these runs: {rs['peak_python_bytes']/1024**2:.2f} MiB. "
      'Includes in-process SQLite, excludes PostgreSQL/psql. Measurements precede final JSON/MD serialization. '
      'Below-normal priority, 8 MiB SQLite cache, no mmap; occurrence rows streamed and text hashes retained only for the current file.', '',
      'Further analysis awaits researcher instructions.','']
    return '\n'.join(lines)


def finalize(read_image_index=False):
    clock=time.monotonic();resources(lower=True)
    for name in ('text_growth.json','text_growth.md','text_growth_records.csv','text_growth_files.csv','compacted_decreases.csv','decrease_distribution.csv'):
        if (OUT/name).exists():
            # Allow only completion of this report's explicitly pending image step.
            saved=OUT/'text_growth.json'
            if not read_image_index or not saved.exists() or json.loads(saved.read_text()).get('status')!='complete_except_image_disappearance_bytes_pending_permission':
                raise ValueError('output_already_exists')
    stage=json.loads(STAGE.read_text(encoding='utf-8'))
    # Prior approved derived image results; no raw source reads.
    old={int(r['record_id']):r for r in csv.DictReader((OUT/'records.csv').open(encoding='utf-8'))}
    if set(old)!={r['record_id'] for r in stage['records']}:raise ValueError('prior_image_result_coverage_mismatch')
    image_hash_before=digest(IMAGE_INDEX) if read_image_index else None
    image_results=images_from_index(stage) if read_image_index else None
    if read_image_index and digest(IMAGE_INDEX)!=image_hash_before:raise ValueError('image_index_changed')
    output=[];prior_image=None;prior_other=None;prior_file=None
    for row in stage['records']:
        r={k:v for k,v in row.items() if k!='source_byte_offset'};base=old[r['record_id']]
        for a,b in [('file_id','file_id'),('source_line_number','source_line_number'),('compacted_ordinal','compacted_ordinal'),('source_record_bytes','record_bytes')]:
            if r[a]!=int(base[b]):raise ValueError('prior_image_result_locator_mismatch')
        if r['file_id']!=prior_file:prior_image=None;prior_other=None
        im = image_results[r['record_id']] if image_results is not None else dict(
            unique_blocks=int(base['unique_images_in_record']),occurrences=int(base['image_occurrences']),
            stored_bytes=int(base['image_stored_bytes']),adjacent_hash_matches=int(base['adjacent_image_hash_reappearances']),
            adjacent_repeated_occurrences=int(base['adjacent_repeated_image_occurrences']),
            adjacent_repeated_stored_bytes=int(base['adjacent_repeated_image_stored_bytes']))
        for key,col in [('occurrences','image_occurrences'),('stored_bytes','image_stored_bytes'),('unique_blocks','unique_images_in_record'),('adjacent_hash_matches','adjacent_image_hash_reappearances'),('adjacent_repeated_stored_bytes','adjacent_repeated_image_stored_bytes')]:
            if im[key]!=int(base[col]):raise ValueError('prior_image_metrics_mismatch')
        if image_results is None:
            im.update(disappeared_unique_blocks=prior_image['unique_blocks']-im['adjacent_hash_matches'] if prior_image is not None else None,
                disappeared_occurrences=None,disappeared_stored_bytes=None,
                net_stored_bytes_decrease=prior_image['stored_bytes']-im['stored_bytes'] if prior_image is not None else None)
        r.update({'image_'+k:v for k,v in im.items()})
        other=r['source_record_bytes']-r['image_stored_bytes']-r['text_stored_bytes']
        if other<0:raise ValueError('text_image_overlap_or_size_mismatch')
        r['other_stored_bytes']=other
        r['other_net_stored_bytes_decrease']=prior_other-other if prior_other is not None else None
        if prior_other is not None:
            if r['record_size_decrease_bytes']!=r['image_net_stored_bytes_decrease']+r['text_net_stored_bytes_decrease']+r['other_net_stored_bytes_decrease']:
                raise ValueError('net_decrease_partition_mismatch')
        r['dominant_decrease_component']=dominant({k:r[k+'_net_stored_bytes_decrease'] for k in ('image','text','other')}) if (r['record_size_decrease_bytes'] or 0)>0 else ''
        output.append(r);prior_image=im;prior_other=other;prior_file=r['file_id']
    drops=[r for r in output if (r['record_size_decrease_bytes'] or 0)>0]
    total_bytes=sum(r['source_record_bytes'] for r in output)
    image_adj=sum(r['image_adjacent_repeated_stored_bytes'] for r in output)
    text_adj=sum(r['text_adjacent_repeated_stored_bytes'] for r in output)
    allocation=[dict(category=k,bytes=v,percent_of_compacted=100*v/total_bytes) for k,v in (
        ('Images reappearing in next compacted record',image_adj),('Text reappearing in next compacted record',text_adj),
        ('All remaining compacted bytes',total_bytes-image_adj-text_adj))]
    bounds=[1,1024,10240,102400,1048576,10485760,104857600]
    bands=[]
    for i,lo in enumerate(bounds):
        hi=bounds[i+1] if i+1<len(bounds) else None
        chosen=[r for r in drops if r['record_size_decrease_bytes']>=lo and (hi is None or r['record_size_decrease_bytes']<hi)]
        bands.append(dict(lower=lo,upper_exclusive=hi,count=len(chosen),drop_bytes=sum(r['record_size_decrease_bytes'] for r in chosen)))
    components={}
    for kind in ('image','text','other'):
        components[kind]={}
        for field in ('disappeared_unique_blocks','disappeared_occurrences','disappeared_stored_bytes','net_stored_bytes_decrease'):
            vals=[r.get(kind+'_'+field) for r in drops]
            components[kind][field]=sum(vals) if all(v is not None for v in vals) else None
    files=[]
    for f in stage['files']:
        rows=[r for r in output if r['file_id']==f['file_id']]
        summary={**f}
        for field in ('occurrences','stored_bytes','adjacent_hash_matches','adjacent_repeated_occurrences','adjacent_repeated_stored_bytes'):
            summary['text_'+field]=sum(r['text_'+field] for r in rows)
        summary['text_duplicate_stored_bytes']=summary['text_stored_bytes']-summary['text_retained_first_copy_stored_bytes']
        summary['text_duplicate_occurrences']=summary['text_occurrences']-summary['text_unique_blocks']
        summary['text_adjacent_repeat_fraction_of_file']=summary['text_adjacent_repeated_stored_bytes']/f['file_size_bytes']
        files.append(summary)
    totals=dict(files=len(files),records=len(output),compacted_bytes=total_bytes,
        global_unique_text_blocks=stage['verification']['global_unique_text_blocks'],
        **{'text_'+k:sum(r['text_'+k] for r in output) for k in ('occurrences','stored_bytes','adjacent_hash_matches','adjacent_repeated_occurrences','adjacent_repeated_stored_bytes')})
    if sum(r['text_disappeared_unique_blocks'] for r in drops)!=stage['verification'].get('decrease_disappeared_unique_blocks',0) or sum(r['text_disappeared_stored_bytes'] for r in drops)!=stage['verification'].get('decrease_disappeared_stored_bytes',0):
        raise ValueError('decrease_text_hash_validation_failed')
    write_csv(OUT/'text_growth_records.csv',output);write_csv(OUT/'compacted_decreases.csv',drops)
    write_csv(OUT/'text_growth_files.csv',files);write_csv(OUT/'decrease_distribution.csv',bands)
    s=dict(version='1.0.0',generated_at=datetime.now(timezone.utc).isoformat(),totals=totals,
        status='completed' if read_image_index else 'complete_except_image_disappearance_bytes_pending_permission',
        combined_allocation=allocation,files=files,
        decreases=dict(count=len(drops),total_drop_bytes=sum(r['record_size_decrease_bytes'] for r in drops),
            bytes_distribution=describe([r['record_size_decrease_bytes'] for r in drops]),
            relative_drop_distribution=describe([r['record_size_decrease_bytes']/(r['source_record_bytes']+r['record_size_decrease_bytes']) for r in drops]),
            bands=bands,dominant_component_counts=dict(Counter(r['dominant_decrease_component'] for r in drops)),components=components),
        source=dict(postgres_read_only=True,text_index_sha256=stage['text_index_sha256'],image_index_read=read_image_index,
                    image_index_sha256=image_hash_before,source_sessions_opened=False,existing_image_csv_sha256=digest(OUT/'records.csv')),
        image_disappearance_scope=('The separately approved image hash index was read to measure exact vanished-image bytes and occurrence counts.' if read_image_index else
            'Image index was not read. Prior results determine vanished-image identity counts and image net byte changes, but not exact vanished-image occurrence counts or bytes; these remain NULL.'),
        validation=dict(independent_text_hash_pass=True,all_index_locators_match=True,prior_image_metrics_match=True,
                        disjoint_combined_allocation=True,per_transition_net_byte_partition=True,text_index_unchanged=True),
        resources={**stage['resources'], 'finalize_seconds':round(time.monotonic()-clock,6),
            'peak_python_bytes':max(stage['resources']['peak_python_bytes'],resources())},
        limits=['Exact whole-block hashes, not semantic similarity.','Shared hashes can lose copies without disappearing.',
                'Repeated storage is not evidence of model context or token consumption.','No text or source image data read or displayed.'])
    save(OUT/'text_growth.json',s);(OUT/'text_growth.md').write_text(render(s),encoding='utf-8')
    print(json.dumps({k:s[k] for k in ('totals','combined_allocation','decreases','resources')},indent=2))


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    mode=p.add_mutually_exclusive_group(required=True)
    mode.add_argument('--stage-text',action='store_true');mode.add_argument('--finalize',action='store_true')
    p.add_argument('--read-image-index',action='store_true')
    a=p.parse_args()
    if a.read_image_index and not a.finalize:p.error('--read-image-index requires --finalize')
    stage_text() if a.stage_text else finalize(a.read_image_index)
