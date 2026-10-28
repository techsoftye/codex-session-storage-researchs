"""Read-only locator and image-index analysis. Never opens session source files."""
from collections import Counter, defaultdict
from datetime import datetime, timezone
from fractions import Fraction
import csv
import hashlib
import itertools
import json
from pathlib import Path
import sqlite3
import statistics
import time

from import_session_survey import run_psql
from analyze_length_control import spearman
from analyze_logs_request_comparison import resources

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'data/reports/compaction_growth_v1'
INDEX = ROOT / 'data/reports/compacted_image_duplicates_v1/image_index.sqlite3'
PARTS = ('retained_first_file_copy', 'within_record_extra_copy',
         'adjacent_record_first_copy_repeat', 'nonadjacent_record_first_copy_repeat')


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(262144), b''):
            h.update(block)
    return h.hexdigest()


def describe(values):
    v = sorted(values)
    if not v:
        return dict(n=0, min=None, median=None, p90=None, max=None)
    # Linear interpolation at (n-1)*p; no distributional assumption.
    pos = Fraction(9 * (len(v)-1), 10)
    lo = int(pos)
    p90 = v[lo] + (pos-lo)*(v[min(lo+1, len(v)-1)]-v[lo])
    return dict(n=len(v), min=float(v[0]), median=float(statistics.median(v)),
                p90=float(p90), max=float(v[-1]))


def growth(records):
    sizes = [r['source_record_bytes'] for r in records]
    if not sizes or any(type(n) is not int or n <= 0 for n in sizes):
        raise ValueError('invalid_compacted_sizes')
    lines = [r['source_line_number'] for r in records]
    if any(b <= a for a, b in zip(lines, lines[1:])):
        raise ValueError('source_line_order_invalid')
    up = sum(b > a for a, b in zip(sizes, sizes[1:]))
    down = sum(b < a for a, b in zip(sizes, sizes[1:]))
    equal = len(sizes)-1-up-down
    return dict(compacted_records=len(sizes), first_compacted_bytes=sizes[0],
                last_compacted_bytes=sizes[-1], max_compacted_bytes=max(sizes),
                compacted_bytes=sum(sizes), sum_div_last_bytes=sum(sizes)/sizes[-1],
                increases=up, decreases=down, equal_transitions=equal,
                strictly_increasing=(up == len(sizes)-1) if len(sizes)>1 else None,
                nondecreasing=(down == 0) if len(sizes)>1 else None)


class Images:
    """Keep only hash summaries for one file; process one indexed record at a time."""
    def __init__(self):
        self.seen = {}
        self.previous = set()
        self.parts = {p: dict(occurrences=0, stored_bytes=0) for p in PARTS}
        self.adjacent_hash_reappearances = 0
        self.adjacent_occurrences = 0
        self.adjacent_stored_bytes = 0
        self.image_record_count = 0

    def add(self, rows):
        # rows: sha256, decoded_bytes, stored_bytes; already in source traversal order.
        current = set()
        adjacent_count = adjacent_bytes = 0
        total_count = total_bytes = 0
        for sha, decoded, stored in rows:
            if not isinstance(sha, bytes) or len(sha)!=32 or decoded<=0 or stored<=0:
                raise ValueError('invalid_image_index_value')
            if sha in self.seen and self.seen[sha]['decoded_bytes'] != decoded:
                raise ValueError('conflicting_binary_lengths_for_hash')
            if sha in current:
                part = 'within_record_extra_copy'
            elif sha in self.seen:
                part = ('adjacent_record_first_copy_repeat' if sha in self.previous
                        else 'nonadjacent_record_first_copy_repeat')
                self.seen[sha]['record_count'] += 1
            else:
                part = 'retained_first_file_copy'
                self.seen[sha] = dict(decoded_bytes=decoded, record_count=1)
            self.parts[part]['occurrences'] += 1
            self.parts[part]['stored_bytes'] += stored
            if sha in self.previous:
                adjacent_count += 1
                adjacent_bytes += stored
            current.add(sha)
            total_count += 1
            total_bytes += stored
        overlap = len(current & self.previous)
        self.adjacent_hash_reappearances += overlap
        self.adjacent_occurrences += adjacent_count
        self.adjacent_stored_bytes += adjacent_bytes
        self.image_record_count += bool(current)
        self.previous = current  # Empty compacted records break adjacency too.
        return dict(image_occurrences=total_count, unique_images_in_record=len(current),
                    image_stored_bytes=total_bytes, adjacent_image_hash_reappearances=overlap,
                    adjacent_repeated_image_occurrences=adjacent_count,
                    adjacent_repeated_image_stored_bytes=adjacent_bytes)

    def summary(self, file_bytes):
        total = sum(p['stored_bytes'] for p in self.parts.values())
        duplicates = total-self.parts['retained_first_file_copy']['stored_bytes']
        return dict(unique_images=len(self.seen),
                    unique_decoded_image_bytes=sum(v['decoded_bytes'] for v in self.seen.values()),
                    recurrent_unique_images=sum(v['record_count']>1 for v in self.seen.values()),
                    image_bearing_compacted_records=self.image_record_count,
                    image_occurrences=sum(p['occurrences'] for p in self.parts.values()),
                    image_stored_bytes=total, duplicate_image_stored_bytes=duplicates,
                    duplicate_image_fraction_of_file=duplicates/file_bytes,
                    adjacent_image_hash_reappearances=self.adjacent_hash_reappearances,
                    adjacent_repeated_image_occurrences=self.adjacent_occurrences,
                    adjacent_repeated_image_stored_bytes=self.adjacent_stored_bytes,
                    adjacent_image_repeat_fraction_of_file=self.adjacent_stored_bytes/file_bytes,
                    image_duplicate_partition=self.parts)


def write_csv(path, rows):
    with path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def flatten_file(row):
    flat = {k: (int(v) if isinstance(v, bool) else v) for k, v in row.items()
            if k != 'image_duplicate_partition'}
    for part, values in row['image_duplicate_partition'].items():
        for key, value in values.items():
            flat[f'image_{part}_{key}'] = value
    return flat


def report(s):
    t, g, im = s['totals'], s['growth'], s['images']
    lines = ['# Compacted record growth', '', f"Generated: {s['generated_at']}.", '',
        f"The full indexed corpus contains **{t['corpus_files']:,} files** and "
        f"**{t['corpus_bytes']/1024**3:.4f} GiB**. This analysis covers all "
        f"**{t['compacted_files']:,} files with {t['compacted_records']:,} compacted records**, "
        f"totalling **{t['compacted_bytes']/1024**3:.4f} GiB**. It is not restricted to the previous 189-file cohort or June cutoff.", '',
        'Only PostgreSQL locator metadata and the existing image hash index were read. '
        'No session source, conversation text, image bytes, title, path or account identifier was read or exported.', '',
        '## Growth and recurrence', '',
        f"Across {g['transitions']:,} consecutive compacted-record pairs: "
        f"**{g['increases']:,} increases**, **{g['decreases']:,} decreases**, "
        f"**{g['equal_transitions']:,} equal sizes**. Among {g['files_with_two_or_more']:,} files with at least two records, "
        f"{g['strictly_increasing_files']:,} strictly increase and {g['nondecreasing_files']:,} never decrease. "
        f"Single-record files ({g['single_record_files']:,}) have no growth comparison.", '',
        f"Per-file sum / last-record-size: median **{s['sum_div_last_distribution']['median']:.3f}**, "
        f"90th percentile **{s['sum_div_last_distribution']['p90']:.3f}**, maximum **{s['sum_div_last_distribution']['max']:.3f}**. "
        'This is a storage-volume multiple relative to the final snapshot, not a content-equivalence or duplicate-byte estimate. '
        'A smaller final record can increase the multiple.', '',
        f"Spearman correlation between compacted-record count and compacted bytes / file bytes: "
        f"**{s['association']['rho']:.6f}**, n={s['association']['n']}. Average ranks handle ties. "
        'The share is ranked as an exact rational number. Files without compacted are excluded from this correlation.', '',
        '## Distribution of compacted counts', '',
        '| Compacted records per file | Files |', '| --- | ---: |']
    lines += [f"| {b['label']} | {b['files']:,} |" for b in s['count_bands']]
    lines += ['', 'The exact frequency distribution is also saved in summary.json and count_distribution.csv. '
              'Percentiles use linear interpolation at (n−1)×p.', '',
        '## Ten largest files by total file bytes', '',
        '| File ID | File GiB | Count | First MiB | Last MiB | Max MiB | Compacted GiB | Share % | Sum / last | Up | Down | Equal |',
        '| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |']
    for r in s['top10_by_file_size']:
        lines.append(f"| {r['file_id']} | {r['file_size_bytes']/1024**3:.4f} | {r['compacted_records']} | "
            f"{r['first_compacted_bytes']/1024**2:.3f} | {r['last_compacted_bytes']/1024**2:.3f} | "
            f"{r['max_compacted_bytes']/1024**2:.3f} | {r['compacted_bytes']/1024**3:.4f} | "
            f"{100*r['compacted_fraction_of_file']:.2f} | {r['sum_div_last_bytes']:.3f} | "
            f"{r['increases']} | {r['decreases']} | {r['equal_transitions']} |")
    lines += ['', '## Image repetition', '',
        'Image identity is exact SHA-256 equality of decoded binary data, as already recorded by the image profiler. '
        'Here no image was decoded or opened. The index covers embedded data images in replacement_history and guardian_history only. '
        'Different encodings of visually identical images remain different identities; external images and other fields are not measured.', '',
        f"There are **{im['global_unique_images']:,} globally unique binary images** and "
        f"**{im['file_unique_image_sum']:,} file–image pairs** across **{im['files_with_images']:,} files**. "
        'File-level deduplication restarts at each file. Global uniqueness and the sum of per-file uniqueness must not be confused.', '',
        '| Measure | Count | Stored GiB | % of all corpus bytes |',
        '| --- | ---: | ---: | ---: |',
        f"| All indexed image occurrences | {im['image_occurrences']:,} | {im['image_stored_bytes']/1024**3:.4f} | {100*im['image_stored_bytes']/t['corpus_bytes']:.4f} |",
        f"| Duplicate image copies within each file, all positions | {im['duplicate_image_occurrences']:,} | {im['duplicate_image_stored_bytes']/1024**3:.4f} | {100*im['duplicate_image_stored_bytes']/t['corpus_bytes']:.4f} |",
        f"| Later occurrences also present in immediately previous compacted record | {im['adjacent_repeated_image_occurrences']:,} | {im['adjacent_repeated_image_stored_bytes']/1024**3:.4f} | {100*im['adjacent_repeated_image_stored_bytes']/t['corpus_bytes']:.4f} |", '',
        f"**{im['adjacent_image_hash_reappearances']:,} distinct-image transition matches** were observed. "
        'For adjacency, the two history lists are combined and each hash is counted once per transition. '
        'The byte measure counts every current-record occurrence whose hash appeared in the preceding compacted record. '
        'Intervening non-compacted rows do not interrupt this sequence; a compacted record without that image does.', '',
        '**Do not add the duplicate and adjacent rows:** adjacent repetition is a subset of all within-file duplicates. '
        'The following partition is disjoint and sums to every indexed image occurrence:', '',
        '| Partition | Occurrences | Stored GiB |', '| --- | ---: | ---: |']
    for name, values in im['partition'].items():
        lines.append(f"| {name} | {values['occurrences']:,} | {values['stored_bytes']/1024**3:.6f} |")
    lines += ['', 'A retained copy is the first occurrence in file-line / JSON traversal order. '
        'Within-record extra copies include duplication across the two histories. For a later record, its first copy of an old hash '
        'is classified as adjacent or nonadjacent; all additional copies in that record belong to the within-record group. '
        'Stored bytes include JSON string quotes, escaping, Base64 and data-URL prefix. '
        'Decoded byte counts are separate and are not added to file storage.', '',
        '## Outputs and checks', '',
        '- files.csv: all per-file growth and image metrics, exact bytes and fractions; internal numeric IDs only.',
        '- records.csv: one row per compacted record, line number, ordinal, bytes, cumulative bytes, size delta and image recurrence.',
        '- count_distribution.csv: exact distribution over all corpus files, including zero compacted records.',
        '- summary.json: definitions, coverage, exact totals, distribution, top ten and all per-file summaries.', '',
        'Every PostgreSQL file count/byte total was reconciled to its locator rows. Every image-index record ID, file ID, '
        'offset, ordinal and per-file compacted count/byte total matched PostgreSQL. '
        'Independent SQLite window queries confirmed image duplicates and adjacency totals. '
        'The index SHA-256 remained unchanged. No database or source files were modified.', '',
        '## Interpretation limits', '',
        'Repeated storage does not prove that these images or histories entered the model context, were sent again, '
        'or consumed additional tokens. The data contain recorded snapshots, not a reconstruction of requests. '
        'Equal sizes do not prove equal content; growth and decreases do not identify their cause. '
        'Correlation is descriptive, not causal. No byte-to-token or monetary conversion is made, and findings apply only to these indexed files.', '',
        '## Resources', '',
        f"Analysis and CSV export time: {s['resources']['elapsed_seconds']:.3f} seconds; PostgreSQL query: {s['resources']['postgres_seconds']:.3f} seconds. "
        f"Peak Python working set: {s['resources']['peak_python_bytes']/1024**2:.2f} MiB. "
        'Measured through CSV export, before final JSON/Markdown serialization and the separate validation pass. '
        'Memory includes the in-process SQLite connection, but excludes psql/PostgreSQL service memory. '
        'Below-normal Python priority; SQLite cache 8 MiB and mmap disabled; PostgreSQL work_mem 8 MB per operation and parallel workers disabled. '
        'Only compact numeric locator data are held in memory, and image occurrences are streamed per record.', '',
        'Analysis complete. Further work awaits researcher approval.', '']
    return '\n'.join(lines)


def main():
    started = time.monotonic()
    resources(lower=True)
    query = Path(__file__).with_name('compaction_growth_query.sql').read_text(encoding='utf-8')
    qstart = time.monotonic()
    raw = run_psql(query)
    db = json.loads(raw)
    qseconds = time.monotonic()-qstart
    if db['transaction_read_only']!='on':
        raise ValueError('postgres_read_only_not_active')
    for f in db['files']:
        if f['record_count']!=f['indexed_records'] or f['file_size_bytes']!=f['indexed_bytes']:
            raise ValueError('file_index_totals_mismatch')
    byfile = {f['file_id']: f for f in db['files']}
    if len(byfile)!=len(db['files']):
        raise ValueError('duplicate_file_id')
    allrecords = db.pop('records')
    pgids = {r['record_id'] for r in allrecords}
    if len(pgids)!=len(allrecords):
        raise ValueError('duplicate_record_id')
    index_hash = digest(INDEX)
    conn = sqlite3.connect(INDEX.as_uri()+'?mode=ro', uri=True)
    conn.execute('PRAGMA query_only=ON')
    conn.execute('PRAGMA cache_size=-8192')
    conn.execute('PRAGMA mmap_size=0')
    conn.execute('PRAGMA temp_store=FILE')
    conn.execute('BEGIN')
    indexed = {r[0]: r[1:] for r in conn.execute('SELECT record_id,session_id,source_offset,sequence FROM records')}
    if set(indexed)!=pgids:
        raise ValueError('image_index_record_coverage_mismatch')
    indexed_files = {r[0]: r[1:] for r in conn.execute('SELECT session_id,records,source_bytes FROM files')}
    if conn.execute('SELECT count(*) FROM occurrences WHERE error IS NOT NULL OR sha256 IS NULL').fetchone()[0]:
        raise ValueError('image_hash_coverage_incomplete')
    if conn.execute('SELECT count(*) FROM occurrences o LEFT JOIN records r USING(record_id) '
                    'WHERE r.record_id IS NULL OR r.session_id<>o.session_id').fetchone()[0]:
        raise ValueError('orphan_image_occurrence')
    OUT.mkdir(parents=True, exist_ok=True)
    if any((OUT/n).exists() for n in ('summary.json','report.md','files.csv','records.csv','count_distribution.csv')):
        raise ValueError('output_already_exists')
    files = []
    global_images = {}
    with (OUT/'records.csv').open('w', encoding='utf-8', newline='') as handle:
        writer = None
        for file_id, group in itertools.groupby(allrecords, key=lambda r: r['file_id']):
            rows = list(group)
            f = byfile[file_id]
            g = growth(rows)
            if indexed_files.get(file_id)!=(g['compacted_records'],g['compacted_bytes']):
                raise ValueError('image_index_file_totals_mismatch')
            if g['compacted_bytes']>f['file_size_bytes']:
                raise ValueError('compacted_bytes_exceed_file')
            images = Images()
            cumulative = 0
            previous_size = None
            for ordinal, r in enumerate(rows, 1):
                if indexed[r['record_id']]!=(file_id,r['source_byte_offset'],ordinal):
                    raise ValueError('image_record_locator_mismatch')
                measured = images.add(conn.execute('SELECT sha256,decoded_bytes,stored_bytes FROM occurrences '
                    'WHERE record_id=? ORDER BY id', (r['record_id'],)))
                if measured['image_stored_bytes']>r['source_record_bytes']:
                    raise ValueError('image_bytes_exceed_record')
                cumulative += r['source_record_bytes']
                out = dict(file_id=file_id, record_id=r['record_id'], source_line_number=r['source_line_number'],
                    compacted_ordinal=ordinal, record_bytes=r['source_record_bytes'],
                    cumulative_compacted_bytes=cumulative,
                    change_from_previous_bytes=(r['source_record_bytes']-previous_size) if previous_size is not None else None,
                    **measured)
                if writer is None:
                    writer = csv.DictWriter(handle, fieldnames=list(out))
                    writer.writeheader()
                writer.writerow(out)
                previous_size = r['source_record_bytes']
            for sha, values in images.seen.items():
                if sha in global_images and global_images[sha]!=values['decoded_bytes']:
                    raise ValueError('cross_file_image_length_conflict')
                global_images[sha] = values['decoded_bytes']
            files.append(dict(file_id=file_id,file_size_bytes=f['file_size_bytes'],record_count=f['record_count'],
                **g, compacted_fraction_of_file=g['compacted_bytes']/f['file_size_bytes'],
                **images.summary(f['file_size_bytes'])))
    if set(indexed_files)!={f['file_id'] for f in files}:
        raise ValueError('image_index_file_coverage_mismatch')
    partition = {p: {k:sum(f['image_duplicate_partition'][p][k] for f in files)
                     for k in ('occurrences','stored_bytes')} for p in PARTS}
    # Independent SQLite aggregation; no writes or source JSON reads.
    sql_duplicates = conn.execute('''WITH numbered AS (
        SELECT o.stored_bytes,row_number() OVER(PARTITION BY o.session_id,o.sha256 ORDER BY r.sequence,o.id) AS n
        FROM occurrences o JOIN records r USING(record_id))
        SELECT count(*),sum(stored_bytes),sum(n>1),sum(CASE WHEN n>1 THEN stored_bytes ELSE 0 END)
        FROM numbered''').fetchone()
    # Use index source sequence to identify adjacent compacted records, including image-free gaps.
    sql_adjacent = conn.execute('''WITH per_record AS (
        SELECT o.session_id,o.sha256,r.sequence,count(*) AS n,sum(o.stored_bytes) AS b
        FROM occurrences o JOIN records r USING(record_id)
        GROUP BY o.session_id,o.sha256,r.sequence), previous AS (
        SELECT *,lag(sequence) OVER(PARTITION BY session_id,sha256 ORDER BY sequence) AS prior FROM per_record)
        SELECT count(*),coalesce(sum(n),0),coalesce(sum(b),0) FROM previous WHERE sequence=prior+1''').fetchone()
    totals = dict(corpus_files=len(byfile), corpus_bytes=sum(f['file_size_bytes'] for f in byfile.values()),
                  corpus_records=sum(f['record_count'] for f in byfile.values()),compacted_files=len(files),
                  compacted_records=len(allrecords), compacted_bytes=sum(f['compacted_bytes'] for f in files),
                  files_without_compacted=len(byfile)-len(files),
                  bytes_of_files_with_compacted=sum(f['file_size_bytes'] for f in files))
    image_totals = {k:sum(f[k] for f in files) for k in ('image_occurrences','image_stored_bytes',
        'duplicate_image_stored_bytes','adjacent_image_hash_reappearances','adjacent_repeated_image_occurrences',
        'adjacent_repeated_image_stored_bytes','recurrent_unique_images')}
    image_totals.update(global_unique_images=len(global_images),
        global_unique_decoded_bytes=sum(global_images.values()), file_unique_image_sum=sum(f['unique_images'] for f in files),
        files_with_images=sum(f['unique_images']>0 for f in files), partition=partition,
        duplicate_image_occurrences=image_totals['image_occurrences']-sum(f['unique_images'] for f in files))
    image_totals['recurrent_file_image_pairs'] = image_totals.pop('recurrent_unique_images')
    expected = tuple(image_totals[k] for k in ('image_occurrences','image_stored_bytes',
                      'duplicate_image_occurrences','duplicate_image_stored_bytes'))
    if expected!=sql_duplicates:
        raise ValueError('independent_duplicate_check_failed')
    if sql_adjacent!=tuple(image_totals[k] for k in ('adjacent_image_hash_reappearances',
                               'adjacent_repeated_image_occurrences','adjacent_repeated_image_stored_bytes')):
        raise ValueError('independent_adjacency_check_failed')
    conn.close()
    if digest(INDEX)!=index_hash:
        raise ValueError('image_index_changed_during_analysis')
    counts = Counter(f['compacted_records'] for f in files)
    counts[0] = len(byfile)-len(files)
    bands = [('0',0,0),('1',1,1),('2–5',2,5),('6–10',6,10),('11–25',11,25),
             ('26–50',26,50),('51–100',51,100),('101+',101,None)]
    distribution = [dict(compacted_records=n,files=count) for n,count in sorted(counts.items())]
    write_csv(OUT/'files.csv', [flatten_file(f) for f in files])
    write_csv(OUT/'count_distribution.csv', distribution)
    seconds = time.monotonic()-started
    s = dict(version='1.0.0', generated_at=datetime.now(timezone.utc).isoformat(),
        source=dict(postgres_tables=['research_sessions','research_session_records'],
            postgres_read_only=True, database_time=db['database_time'],
            postgres_export_sha256=hashlib.sha256(raw.encode()).hexdigest(), image_index_sha256=index_hash,
            sqlite_read_only=True, source_sessions_opened=False),
        totals=totals,
        growth=dict(transitions=len(allrecords)-len(files),
            **{k:sum(f[k] for f in files) for k in ('increases','decreases','equal_transitions')},
            files_with_two_or_more=sum(f['compacted_records']>1 for f in files),
            single_record_files=sum(f['compacted_records']==1 for f in files),
            strictly_increasing_files=sum(f['strictly_increasing'] is True for f in files),
            nondecreasing_files=sum(f['nondecreasing'] is True for f in files),
            files_with_any_decrease=sum(f['decreases']>0 for f in files)),
        count_distribution=distribution,
        count_summary_positive=describe([f['compacted_records'] for f in files]),
        count_bands=[dict(label=label,files=sum(v for n,v in counts.items() if n>=lo and (hi is None or n<=hi)))
                     for label,lo,hi in bands],
        sum_div_last_distribution=describe([Fraction(f['compacted_bytes'],f['last_compacted_bytes']) for f in files]),
        association=dict(x='compacted_records',y='compacted_fraction_of_file',n=len(files),method='Spearman average ranks',
            rho=spearman([f['compacted_records'] for f in files],
                         [Fraction(f['compacted_bytes'],f['file_size_bytes']) for f in files])),
        images=image_totals, top10_by_file_size=sorted(files,key=lambda f:(-f['file_size_bytes'],f['file_id']))[:10],
        files=files,
        resources=dict(elapsed_seconds=round(seconds,6),postgres_seconds=round(qseconds,6),
                       peak_python_bytes=resources(),memory_excludes='psql and PostgreSQL service',
                       measurement_scope='Through CSV exports; excludes final JSON/Markdown serialization and separate validation'),
        validation=dict(postgres_file_totals_match=True,index_locators_and_totals_match=True,
            image_hash_errors=0, independent_sqlite_duplicates_match=True,
            independent_sqlite_adjacency_match=True, image_index_unchanged=True),
        definitions=dict(sequence='source_line_number ascending within each file; compacted records only',
            sum_div_last_bytes='sum of all compacted record bytes / last compacted record bytes; not proven repeated content',
            adjacent_repeated_image_stored_bytes='all current occurrences of hashes present in the immediately previous compacted record; subset of within-file duplicates',
            duplicate_image_stored_bytes='all image occurrences except first copy of each decoded hash within each file',
            stored_bytes='source JSON image string bytes, including quotes, escapes, Base64 and data URL prefix',
            unique_images='decoded-binary SHA-256 identity, combining replacement_history and guardian_history',
            partition='within-record extra copies take precedence; first current copy classified by earlier-file presence and adjacency',
            first_record_delta_and_single_record_monotonicity='NULL; no preceding comparison'),
        limits=['Recorded storage does not establish model context or transmitted request content.',
                'No byte-to-token or money conversion; correlation does not establish causation.',
                'Only the indexed corpus and indexed embedded images in the two histories are covered.'])
    (OUT/'summary.json').write_text(json.dumps(s,indent=2,ensure_ascii=False,allow_nan=False)+'\n',encoding='utf-8')
    (OUT/'report.md').write_text(report(s),encoding='utf-8')
    print(json.dumps(dict(totals=totals,growth=s['growth'],association=s['association'],
                         image_totals=image_totals,resources=s['resources']),indent=2))


if __name__=='__main__':
    main()
