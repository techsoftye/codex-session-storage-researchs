-- Read-only metadata and pre-compaction usage snapshot. No payload or source text.
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL statement_timeout = '90s';
SET LOCAL lock_timeout = '5s';
SET LOCAL work_mem = '8MB';
SET LOCAL max_parallel_workers_per_gather = 0;
WITH compacted AS MATERIALIZED (
    SELECT s.id AS file_id, s.source_relative_path, s.file_size_bytes,
           r.id AS record_id, r.source_line_number, r.source_byte_offset,
           r.source_record_bytes, r.record_timestamp
    FROM public.research_sessions s
    JOIN public.research_session_records r ON r.research_session_id=s.id
    WHERE r.record_type='compacted'
), with_prior_usage AS (
    SELECT c.*,
           CASE WHEN u.id IS NOT NULL THEN json_build_object(
             'scope',u.usage_scope,'line',ur.source_line_number,
             'timestamp',ur.record_timestamp,'input_tokens',u.input_tokens,
             'cached_input_tokens',u.cached_input_tokens,
             'cache_write_input_tokens',u.cache_write_input_tokens,
             'output_tokens',u.output_tokens,
             'reasoning_output_tokens',u.reasoning_output_tokens,
             'total_tokens',u.total_tokens) END AS prior_usage
    FROM compacted c
    LEFT JOIN LATERAL (
        SELECT x.* FROM public.research_token_usage_observations x
        JOIN public.research_session_records urx ON urx.id=x.research_session_record_id
        WHERE urx.research_session_id=c.file_id
          AND urx.source_line_number<c.source_line_number
          AND x.usage_scope IN ('response','last')
          AND x.is_possible_duplicate IS FALSE
          AND x.input_tokens IS NOT NULL AND x.input_tokens>=0
          AND x.output_tokens IS NOT NULL AND x.output_tokens>=0
          AND x.total_tokens IS NOT NULL AND x.total_tokens>=0
          AND x.input_tokens+x.output_tokens=x.total_tokens
          AND (x.cached_input_tokens IS NULL OR
               (x.cached_input_tokens>=0 AND x.cached_input_tokens<=x.input_tokens))
          AND (x.cache_write_input_tokens IS NULL OR x.cache_write_input_tokens>=0)
          AND (x.reasoning_output_tokens IS NULL OR
               (x.reasoning_output_tokens>=0 AND x.reasoning_output_tokens<=x.output_tokens))
        ORDER BY urx.source_line_number DESC,x.source_object_byte_offset DESC,x.id DESC
        LIMIT 1
    ) u ON true
    LEFT JOIN public.research_session_records ur ON ur.id=u.research_session_record_id
), latest_usage AS (
    SELECT c.file_id,c.record_id,c.source_line_number,
           u.id AS observation_id,u.is_possible_duplicate,
           u.response_id,u.input_tokens,u.cached_input_tokens,u.cache_write_input_tokens,
           u.output_tokens,u.reasoning_output_tokens,u.total_tokens
    FROM compacted c
    JOIN public.research_token_usage_observations u
      ON u.research_session_record_id=c.record_id
     AND u.source_json_path='/payload/latest_token_usage_record/usage'
)
SELECT json_build_object(
    'database_time',CURRENT_TIMESTAMP,
    'transaction_read_only',current_setting('transaction_read_only'),
    'files',COALESCE((SELECT json_agg(f ORDER BY file_id) FROM (
        SELECT DISTINCT file_id,source_relative_path,file_size_bytes FROM compacted
    ) f),'[]'::json),
    'compacted',COALESCE((SELECT json_agg(r ORDER BY file_id,source_line_number)
                          FROM with_prior_usage r),'[]'::json),
    'latest_usage',COALESCE((SELECT json_agg(u ORDER BY file_id,source_line_number,observation_id)
                             FROM latest_usage u),'[]'::json),
    'read_only',current_setting('transaction_read_only'));
COMMIT;
