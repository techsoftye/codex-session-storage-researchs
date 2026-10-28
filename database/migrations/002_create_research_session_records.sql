-- One locator row per physical JSONL record; source content remains in the file.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DO $$
BEGIN
    IF current_database() <> 'codex_session_research' THEN
        RAISE EXCEPTION 'This migration targets codex_session_research only';
    END IF;
END;
$$;

CREATE TABLE public.research_session_records (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    research_session_id bigint NOT NULL REFERENCES public.research_sessions (id),
    source_line_number bigint NOT NULL CHECK (source_line_number > 0),
    source_byte_offset bigint NOT NULL CHECK (source_byte_offset >= 0),
    source_record_bytes bigint NOT NULL CHECK (source_record_bytes > 0),
    record_timestamp text,
    record_type text,
    is_payload_analysis boolean NOT NULL DEFAULT false,
    payload_source_format jsonb DEFAULT NULL,
    indexed_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT research_session_records_session_line_uniq
        UNIQUE (research_session_id, source_line_number)
);

COMMENT ON TABLE public.research_session_records IS
    'One locator per source JSONL record. No raw record or payload content is stored.';
COMMENT ON COLUMN public.research_session_records.research_session_id IS
    'References the research_sessions row for the source file, not the non-unique textual session_id.';
COMMENT ON COLUMN public.research_session_records.source_byte_offset IS
    'Zero-based byte offset of the source line in its file.';
COMMENT ON COLUMN public.research_session_records.source_record_bytes IS
    'Physical source line length in bytes, including its line ending when present.';
COMMENT ON COLUMN public.research_session_records.record_timestamp IS
    'Source timestamp as text; no timestamp conversion during record indexing.';
COMMENT ON COLUMN public.research_session_records.is_payload_analysis IS
    'False until a later analysis stage processes this record payload.';
COMMENT ON COLUMN public.research_session_records.payload_source_format IS
    'Reserved nullable JSONB field for later payload analysis; unused during indexing.';

COMMIT;
