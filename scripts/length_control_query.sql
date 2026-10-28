-- New measurements only from PostgreSQL; no DDL/DML or temporary tables.
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL statement_timeout = '60s';
SET LOCAL lock_timeout = '5s';
SET LOCAL work_mem = '8MB';
SET LOCAL max_parallel_workers_per_gather = 0;

WITH linked AS MATERIALIZED (
    SELECT s.id AS file_id,t.source_rowid AS thread_row_id,t.tokens_used,
           s.file_size_bytes,s.record_count,
           CASE WHEN tail.record_timestamp ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt][0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]+)?([Zz]|[+-][0-9]{2}:[0-9]{2})$'
                     AND pg_input_is_valid(tail.record_timestamp,'timestamp with time zone')
                THEN tail.record_timestamp::timestamptz END AS last_record_at
    FROM public.state5_threads t
    JOIN public.research_sessions s ON s.id=t.research_session_id
    LEFT JOIN LATERAL (
        SELECT record_timestamp FROM public.research_session_records r
        WHERE r.research_session_id=s.id ORDER BY source_line_number DESC LIMIT 1
    ) tail ON true
), eligible AS MATERIALIZED (
    SELECT * FROM linked WHERE last_record_at < '2026-06-19T00:00:00Z'::timestamptz
), totals AS (
    SELECT r.research_session_id AS file_id,count(*) AS indexed_record_count,
           sum(r.source_record_bytes) AS indexed_bytes,
           COALESCE(sum(r.source_record_bytes) FILTER(WHERE r.record_type='compacted'),0) AS compacted_bytes,
           count(*) FILTER(WHERE r.record_type='compacted') AS compacted_record_count,
           max(CASE WHEN r.record_timestamp ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt][0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]+)?([Zz]|[+-][0-9]{2}:[0-9]{2})$'
                         AND pg_input_is_valid(r.record_timestamp,'timestamp with time zone')
                    THEN r.record_timestamp::timestamptz END) AS max_recorded_at
    FROM public.research_session_records r JOIN eligible e ON e.file_id=r.research_session_id
    GROUP BY r.research_session_id
), files AS (
    SELECT e.*,a.indexed_record_count,a.indexed_bytes,a.compacted_bytes,a.compacted_record_count,a.max_recorded_at
    FROM eligible e JOIN totals a USING(file_id)
)
SELECT json_build_object('database_time',CURRENT_TIMESTAMP,
    'transaction_read_only',current_setting('transaction_read_only'),
    'transaction_isolation',current_setting('transaction_isolation'),
    'files',COALESCE((SELECT json_agg(f ORDER BY file_id) FROM files f),'[]'::json));
COMMIT;
