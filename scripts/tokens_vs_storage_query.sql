-- SELECT-only analysis of the five authorized research tables.
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL statement_timeout = '90s';
SET LOCAL lock_timeout = '5s';
SET LOCAL work_mem = '8MB';
SET LOCAL max_parallel_workers_per_gather = 0;

WITH linked AS MATERIALIZED (
    SELECT t.id AS private_thread_uuid, t.source_rowid AS thread_row_id,
           t.research_session_id AS file_id, t.tokens_used,
           CASE WHEN t.model ~ '^(gpt-|codex-|o[1-9])[A-Za-z0-9_.-]{0,96}$' THEN t.model END AS model,
           CASE WHEN t.thread_source IN ('user','subagent') THEN t.thread_source
                WHEN t.thread_source IS NULL THEN 'missing' ELSE 'other' END AS source_category,
           t.agent_role IS NOT NULL AS agent_role_present,
           EXISTS (SELECT 1 FROM public.state5_thread_spawn_edges e WHERE e.child_thread_id=t.id) AS has_parent_edge,
           s.file_size_bytes, s.record_count, s.session_last_record_at AS catalog_last_at,
           tail.source_line_number AS last_line_number,
           CASE WHEN tail.record_timestamp ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt][0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]+)?([Zz]|[+-][0-9]{2}:[0-9]{2})$'
                     AND pg_input_is_valid(tail.record_timestamp,'timestamp with time zone')
                THEN tail.record_timestamp::timestamptz END AS last_record_at
    FROM public.state5_threads t
    JOIN public.research_sessions s ON s.id=t.research_session_id
    LEFT JOIN LATERAL (
        SELECT source_line_number,record_timestamp FROM public.research_session_records r
        WHERE r.research_session_id=s.id ORDER BY source_line_number DESC LIMIT 1
    ) tail ON true
), eligible AS MATERIALIZED (
    SELECT * FROM linked WHERE last_record_at < '2026-06-19T00:00:00Z'::timestamptz
), records AS MATERIALIZED (
    SELECT r.id,r.research_session_id AS file_id,r.source_line_number,r.source_record_bytes,
           CASE WHEN r.record_type IN ('event_msg','response_item','token_usage_record','turn_context',
               'world_state','session_meta','compacted','inter_agent_communication_metadata') THEN r.record_type
                WHEN r.record_type IS NULL THEN '[NULL]' ELSE '[other]' END AS record_type,
           CASE WHEN r.record_timestamp ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt][0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]+)?([Zz]|[+-][0-9]{2}:[0-9]{2})$'
                     AND pg_input_is_valid(r.record_timestamp,'timestamp with time zone')
                THEN r.record_timestamp::timestamptz END AS recorded_at
    FROM public.research_session_records r JOIN eligible e ON e.file_id=r.research_session_id
), type_totals AS MATERIALIZED (
    SELECT file_id,record_type,count(*) AS records,sum(source_record_bytes) AS bytes,
           max(recorded_at) AS max_recorded_at,count(*) FILTER(WHERE recorded_at IS NULL) AS unparseable_times
    FROM records GROUP BY file_id,record_type
), file_totals AS (
    SELECT file_id,sum(records) AS indexed_record_count,sum(bytes) AS indexed_bytes,
           max(max_recorded_at) AS max_recorded_at,sum(unparseable_times) AS unparseable_times,
           jsonb_object_agg(record_type,jsonb_build_object('records',records,'bytes',bytes)) AS by_type
    FROM type_totals GROUP BY file_id
), usage_rows AS MATERIALIZED (
    SELECT r.file_id,r.source_line_number,r.record_type,r.recorded_at,
           u.id,u.research_session_record_id,u.source_object_byte_offset,u.usage_scope,u.is_possible_duplicate,
           u.total_tokens,u.input_tokens,u.cached_input_tokens,u.output_tokens,
           CASE WHEN u.thread_id IS NULL THEN NULL ELSE u.thread_id=e.private_thread_uuid END AS thread_id_matches,
           CASE WHEN u.session_id IS NULL THEN NULL ELSE u.session_id=e.private_thread_uuid END AS session_id_matches,
           position('history' in u.source_json_path)>0 AS nested_history
    FROM records r JOIN public.research_token_usage_observations u ON u.research_session_record_id=r.id
    JOIN eligible e ON e.file_id=r.file_id
    WHERE u.usage_scope IN ('thread','cumulative')
), latest_any AS (
    SELECT DISTINCT ON(file_id,usage_scope) file_id,usage_scope,id,is_possible_duplicate
    FROM usage_rows ORDER BY file_id,usage_scope,source_line_number DESC,source_object_byte_offset DESC,id DESC
), latest_kept AS (
    SELECT DISTINCT ON(file_id,usage_scope) * FROM usage_rows WHERE is_possible_duplicate IS FALSE
    ORDER BY file_id,usage_scope,source_line_number DESC,source_object_byte_offset DESC,id DESC
), endpoints AS (
    SELECT k.file_id,k.usage_scope,jsonb_build_object(
        'observation_id',k.id,'record_id',k.research_session_record_id,'line_number',k.source_line_number,
        'source_object_byte_offset',k.source_object_byte_offset,'record_type',k.record_type,
        'recorded_at',k.recorded_at,'total',k.total_tokens,'input',k.input_tokens,
        'cached_input',k.cached_input_tokens,'output',k.output_tokens,
        'thread_id_matches',k.thread_id_matches,'session_id_matches',k.session_id_matches,
        'nested_history',k.nested_history,'newer_flagged_endpoint',a.id<>k.id
    ) AS endpoint FROM latest_kept k JOIN latest_any a USING(file_id,usage_scope)
), endpoint_files AS (
    SELECT file_id,jsonb_object_agg(usage_scope,endpoint) AS endpoints FROM endpoints GROUP BY file_id
), file_rows AS (
    SELECT e.file_id,e.thread_row_id,e.tokens_used,e.model,e.source_category,e.agent_role_present,e.has_parent_edge,
           e.file_size_bytes,e.record_count,e.last_line_number,e.last_record_at,
           e.catalog_last_at IS NOT DISTINCT FROM e.last_record_at AS catalog_tail_matches,
           f.indexed_record_count,f.indexed_bytes,f.max_recorded_at,f.unparseable_times,f.by_type,
           COALESCE(p.endpoints,'{}'::jsonb) AS endpoints
    FROM eligible e JOIN file_totals f USING(file_id) LEFT JOIN endpoint_files p USING(file_id)
), edges AS (
    SELECT e.source_rowid AS edge_row_id,p.source_rowid AS parent_thread_row_id,c.source_rowid AS child_thread_row_id,
           p.research_session_id AS parent_file_id,c.research_session_id AS child_file_id
    FROM public.state5_thread_spawn_edges e
    LEFT JOIN public.state5_threads p ON p.id=e.parent_thread_id
    LEFT JOIN public.state5_threads c ON c.id=e.child_thread_id
), usage_counts AS (
    SELECT usage_scope,count(*) AS observations,
           count(*) FILTER(WHERE is_possible_duplicate IS TRUE) AS excluded_flagged,
           count(*) FILTER(WHERE is_possible_duplicate IS FALSE) AS retained,
           count(DISTINCT file_id) FILTER(WHERE is_possible_duplicate IS FALSE) AS files_retained
    FROM usage_rows GROUP BY usage_scope
), total_disagreements AS (
    SELECT e.file_id,k.usage_scope,e.tokens_used,k.total_tokens AS latest_total,
           k.source_line_number AS latest_line,k.recorded_at AS latest_recorded_at,
           (SELECT count(*) FROM usage_rows u WHERE u.file_id=e.file_id AND u.usage_scope=k.usage_scope
            AND u.is_possible_duplicate IS FALSE AND u.total_tokens=e.tokens_used) AS earlier_exact_matches,
           (SELECT jsonb_build_object('line_number',u.source_line_number,'recorded_at',u.recorded_at,
                                     'observation_id',u.id)
            FROM usage_rows u WHERE u.file_id=e.file_id AND u.usage_scope=k.usage_scope
            AND u.is_possible_duplicate IS FALSE AND u.total_tokens=e.tokens_used
            ORDER BY u.source_line_number DESC,u.source_object_byte_offset DESC,u.id DESC LIMIT 1) AS latest_earlier_exact
    FROM eligible e JOIN latest_kept k ON k.file_id=e.file_id
    WHERE e.tokens_used>=0 AND k.total_tokens>=0 AND e.tokens_used<>k.total_tokens
)
SELECT json_build_object(
    'transaction_read_only',current_setting('transaction_read_only'),
    'transaction_isolation',current_setting('transaction_isolation'),
    'database_time',CURRENT_TIMESTAMP,
    'coverage',json_build_object(
        'state_thread_rows',(SELECT count(*) FROM public.state5_threads),
        'linked_thread_rows',(SELECT count(*) FROM linked),
        'linked_files',(SELECT count(DISTINCT file_id) FROM linked),
        'eligible_by_tail_utc',(SELECT count(*) FROM eligible),
        'eligible_by_tail_riyadh',(SELECT count(*) FROM linked WHERE last_record_at<'2026-06-18T21:00:00Z'::timestamptz),
        'unknown_last_record_time',(SELECT count(*) FROM linked WHERE last_record_at IS NULL),
        'later_than_cutoff',(SELECT count(*) FROM linked WHERE last_record_at>='2026-06-19T00:00:00Z'::timestamptz)),
    'files',COALESCE((SELECT json_agg(f ORDER BY file_id) FROM file_rows f),'[]'::json),
    'usage_counts',COALESCE((SELECT json_agg(u ORDER BY usage_scope) FROM usage_counts u),'[]'::json),
    'total_disagreements',COALESCE((SELECT json_agg(d ORDER BY file_id,usage_scope) FROM total_disagreements d),'[]'::json),
    'edges',COALESCE((SELECT json_agg(e ORDER BY edge_row_id) FROM edges e),'[]'::json)
);
COMMIT;
