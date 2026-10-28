-- Structural case-study observations only; no message text, commands, paths or image data.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
DO $$ BEGIN
    IF current_database() <> 'codex_session_research' THEN
        RAISE EXCEPTION 'wrong_research_database';
    END IF;
END $$;
CREATE TABLE public.logs2_request_observations (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_database text NOT NULL CHECK (source_database = 'logs_2.sqlite'),
    source_rowid bigint NOT NULL,
    source_db_sha256 text NOT NULL,
    research_session_id bigint REFERENCES public.research_sessions(id),
    thread_id text,
    ts bigint NOT NULL,
    ts_nanos bigint NOT NULL,
    log_body_bytes bigint NOT NULL,
    log_body_sha256 text NOT NULL,
    classification text NOT NULL CHECK (classification IN ('request','response','other')),
    classification_basis text NOT NULL,
    protocol_type text,
    capture_status text NOT NULL,
    json_start_byte bigint,
    json_end_byte bigint,
    captured_json_bytes bigint,
    top_level_keys jsonb NOT NULL,
    response_keys jsonb NOT NULL,
    truncation_evidence jsonb NOT NULL,
    previous_response_id_present boolean NOT NULL,
    previous_response_id text,
    request_context_mode text,
    store_present boolean NOT NULL,
    store boolean,
    model_present boolean NOT NULL,
    model text,
    input_count bigint,
    input_items jsonb NOT NULL,
    tools_count bigint,
    tools_bytes bigint,
    image_count bigint NOT NULL,
    images jsonb NOT NULL,
    response_id text,
    request_id text,
    input_tokens bigint,
    cached_input_tokens bigint,
    cache_write_input_tokens bigint,
    output_tokens bigint,
    reasoning_output_tokens bigint,
    total_tokens bigint,
    counter_metadata jsonb NOT NULL,
    linked_response_id text,
    linked_response_source_rowid bigint,
    link_method text CHECK (link_method IN ('explicit_id','order_inferred')),
    link_notes jsonb NOT NULL,
    token_comparison jsonb NOT NULL,
    analysis_version text NOT NULL,
    analyzed_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_database, source_rowid),
    FOREIGN KEY (source_database, source_rowid)
        REFERENCES public.logs2_logs(source_database, source_rowid)
);
CREATE INDEX logs2_request_observations_response_idx
    ON public.logs2_request_observations(response_id) WHERE response_id IS NOT NULL;
CREATE INDEX logs2_request_observations_thread_idx
    ON public.logs2_request_observations(thread_id, ts, ts_nanos, source_rowid);
COMMENT ON TABLE public.logs2_request_observations IS
    'One structural observation per keyword-candidate log row. Complete JSON is not proof of full model context. No token sums across scopes.';
COMMENT ON COLUMN public.logs2_request_observations.input_items IS
    'Positions, allowlisted types/roles and exact UTF-8 byte spans in logged JSON. Never content values.';
COMMENT ON COLUMN public.logs2_request_observations.images IS
    'Decoded-binary SHA-256 and lengths, decoding status, and read-only compacted-image-index match counts. No URLs or binary data.';
COMMENT ON COLUMN public.logs2_request_observations.link_method IS
    'explicit_id requires a current response/request identifier match. previous_response_id never establishes the current response link.';
COMMIT;
