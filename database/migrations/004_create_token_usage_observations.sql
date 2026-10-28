-- Physical observations, including repeated snapshots. No semantic deduplication.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
DO $$ BEGIN
    IF current_database() <> 'codex_session_research' THEN
        RAISE EXCEPTION 'This migration targets codex_session_research only';
    END IF;
END $$;

CREATE TABLE public.research_token_usage_observations (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    research_session_record_id bigint NOT NULL REFERENCES public.research_session_records(id),
    source_json_path text NOT NULL,
    source_object_byte_offset bigint NOT NULL CHECK (source_object_byte_offset >= 0),
    source_object_bytes bigint NOT NULL CHECK (source_object_bytes > 0),
    usage_scope text NOT NULL,
    response_id text,
    turn_id text,
    root_turn_id text,
    thread_id text,
    session_id text,
    input_tokens bigint,
    cached_input_tokens bigint,
    cache_write_input_tokens bigint,
    output_tokens bigint,
    reasoning_output_tokens bigint,
    total_tokens bigint,
    is_possible_duplicate boolean NOT NULL DEFAULT false,
    duplicate_checked_at timestamptz,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    extracted_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT token_usage_source_position_unique
        UNIQUE (research_session_record_id, source_object_byte_offset)
);

COMMENT ON TABLE public.research_token_usage_observations IS
    'One row per JSON object containing at least one of the six exact token counter keys, across all record types. Physical repeats are retained; not a billing ledger.';
COMMENT ON COLUMN public.research_token_usage_observations.source_json_path IS
    'RFC 6901 pointer, including array indexes. Arbitrary/long property names are SHA-256 redacted; exact object offsets remain authoritative.';
COMMENT ON COLUMN public.research_token_usage_observations.source_object_byte_offset IS
    'Absolute byte offset of the opening object brace in the original JSONL file.';
COMMENT ON COLUMN public.research_token_usage_observations.metadata IS
    'Counter presence/types, validation issues, identifier provenance and extraction version. No conversation text or raw payload.';
COMMENT ON COLUMN public.research_token_usage_observations.session_id IS
    'Identifier reported in the JSON context. The source-file relationship is through research_session_record_id.';
COMMENT ON CONSTRAINT token_usage_source_position_unique ON public.research_token_usage_observations IS
    'Restart idempotency only: distinct source positions remain separate even with identical counters and identifiers.';

CREATE TABLE public.research_token_usage_scan_state (
    research_session_id bigint PRIMARY KEY REFERENCES public.research_sessions(id),
    scan_version text NOT NULL,
    source_size_bytes bigint NOT NULL,
    source_mtime_ns bigint NOT NULL,
    expected_record_count bigint NOT NULL,
    status text NOT NULL CHECK (status IN ('running','partial','completed','error')),
    last_line_number bigint NOT NULL DEFAULT 0,
    last_record_id bigint,
    next_byte_offset bigint NOT NULL DEFAULT 0,
    scanned_record_count bigint NOT NULL DEFAULT 0,
    observation_count bigint NOT NULL DEFAULT 0,
    started_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at timestamptz,
    error_record_id bigint,
    error_code text
);
COMMENT ON TABLE public.research_token_usage_scan_state IS
    'Independent token-extraction checkpoints, committed atomically with observations. Does not change original record-index scan_status.';
COMMIT;
