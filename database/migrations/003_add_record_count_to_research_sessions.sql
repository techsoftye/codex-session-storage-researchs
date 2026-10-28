-- Cached count of indexed physical records for each registered source file.
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

ALTER TABLE public.research_sessions
    ADD COLUMN IF NOT EXISTS record_count bigint;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.research_sessions'::regclass
          AND conname = 'research_sessions_record_count_nonnegative'
    ) THEN
        ALTER TABLE public.research_sessions
            ADD CONSTRAINT research_sessions_record_count_nonnegative
            CHECK (record_count >= 0);
    END IF;
END;
$$;

COMMENT ON COLUMN public.research_sessions.record_count IS
    'Cached count of research_session_records rows for this source-file row; NULL until synchronized.';

COMMIT;
