-- Only locator metadata and numeric internal identifiers leave PostgreSQL.
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL statement_timeout = '60s';
SET LOCAL lock_timeout = '5s';
SET LOCAL work_mem = '8MB';
SET LOCAL max_parallel_workers_per_gather = 0;
WITH totals AS (
    SELECT research_session_id AS file_id, count(*) AS indexed_records,
           sum(source_record_bytes) AS indexed_bytes
    FROM public.research_session_records GROUP BY research_session_id
), files AS (
    SELECT s.id AS file_id, s.file_size_bytes, s.record_count,
           t.indexed_records, t.indexed_bytes
    FROM public.research_sessions s LEFT JOIN totals t ON t.file_id=s.id
), compacted AS (
    SELECT research_session_id AS file_id, id AS record_id,
           source_line_number, source_byte_offset, source_record_bytes
    FROM public.research_session_records WHERE record_type='compacted'
)
SELECT json_build_object(
    'database_time', CURRENT_TIMESTAMP,
    'transaction_read_only', current_setting('transaction_read_only'),
    'files', (SELECT json_agg(f ORDER BY file_id) FROM files f),
    'records', (SELECT json_agg(c ORDER BY file_id,source_line_number) FROM compacted c)
);
COMMIT;
