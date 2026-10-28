-- Apply once to codex_session_research. Does not read or import source files.
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

CREATE TABLE public.research_sessions (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    file_name text NOT NULL CHECK (btrim(file_name) <> ''),
    source_relative_path text NOT NULL UNIQUE CHECK (btrim(source_relative_path) <> ''),
    file_size_bytes bigint NOT NULL CHECK (file_size_bytes >= 0),
    file_format text NOT NULL DEFAULT 'jsonl' CHECK (btrim(file_format) <> ''),
    file_created_at timestamptz,
    file_modified_at timestamptz,
    session_id text,
    identity_source text,
    filename_id_matches boolean,
    session_started_at timestamptz,
    session_last_record_at timestamptz,
    session_source text,
    originator text,
    cli_version text,
    model_provider text,
    first_observed_model text,
    first_observed_reasoning_effort text,
    first_observed_approval_policy text,
    first_observed_sandbox_mode text,
    first_inventoried_at timestamptz,
    last_inventoried_at timestamptz,
    latest_scan_run_id bigint CHECK (latest_scan_run_id > 0),
    scan_version text,
    scan_status text NOT NULL DEFAULT 'pending'
        CHECK (scan_status IN ('pending', 'running', 'partial', 'completed', 'failed', 'interrupted', 'needs_review')),
    scan_started_at timestamptz,
    scan_completed_at timestamptz,
    scan_complete boolean NOT NULL DEFAULT false,
    scanned_bytes bigint CHECK (scanned_bytes >= 0),
    scanned_record_count bigint CHECK (scanned_record_count >= 0),
    valid_record_count bigint CHECK (valid_record_count >= 0),
    invalid_record_count bigint CHECK (invalid_record_count >= 0),
    first_observed_event_at timestamptz,
    last_observed_event_at timestamptz,
    session_meta_record_count bigint CHECK (session_meta_record_count >= 0),
    response_item_record_count bigint CHECK (response_item_record_count >= 0),
    event_msg_record_count bigint CHECK (event_msg_record_count >= 0),
    turn_context_record_count bigint CHECK (turn_context_record_count >= 0),
    user_role_message_count bigint CHECK (user_role_message_count >= 0),
    assistant_role_message_count bigint CHECK (assistant_role_message_count >= 0),
    user_message_event_count bigint CHECK (user_message_event_count >= 0),
    ghost_snapshot_record_count bigint CHECK (ghost_snapshot_record_count >= 0),
    instructions_observed boolean,
    uniform_envelope_observed boolean,
    multiple_payload_shapes_observed boolean,
    scan_error_code text,
    scan_error text,
    analysis_status text NOT NULL DEFAULT 'pending'
        CHECK (analysis_status IN ('pending', 'running', 'completed', 'failed', 'interrupted', 'needs_review')),
    analysis_started_at timestamptz,
    analysis_completed_at timestamptz,
    final_analysis_run_id bigint CHECK (final_analysis_run_id > 0),
    researcher_review_status text NOT NULL DEFAULT 'pending'
        CHECK (researcher_review_status IN ('pending', 'approved', 'rejected', 'needs_more_data', 'needs_review')),
    researcher_comment text,
    researcher_reviewed_by text,
    researcher_reviewed_at timestamptz,
    researcher_approved_at timestamptz,
    approved_analysis_run_id bigint CHECK (approved_analysis_run_id > 0),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT research_sessions_scan_time_order CHECK (
        scan_completed_at IS NULL OR
        (scan_started_at IS NOT NULL AND scan_completed_at >= scan_started_at)
    ),
    CONSTRAINT research_sessions_analysis_time_order CHECK (
        analysis_completed_at IS NULL OR
        (analysis_started_at IS NOT NULL AND analysis_completed_at >= analysis_started_at)
    ),
    CONSTRAINT research_sessions_valid_approval CHECK (
        researcher_review_status <> 'approved' OR (
            analysis_status = 'completed'
            AND final_analysis_run_id IS NOT NULL
            AND approved_analysis_run_id IS NOT NULL
            AND approved_analysis_run_id = final_analysis_run_id
            AND researcher_reviewed_by IS NOT NULL
            AND btrim(researcher_reviewed_by) <> ''
            AND researcher_reviewed_at IS NOT NULL
            AND researcher_approved_at IS NOT NULL
        )
    )
);

-- Session identifiers are deliberately non-unique: one session may have several copied files.
CREATE INDEX research_sessions_session_id_idx ON public.research_sessions (session_id);
CREATE INDEX research_sessions_started_at_idx ON public.research_sessions (session_started_at);

COMMENT ON TABLE public.research_sessions IS
    'One row per copied research file. Stores derived inventory, scan summaries, and researcher review; no raw conversation content.';
COMMENT ON COLUMN public.research_sessions.source_relative_path IS
    'Unique file registration key relative to the research root; importer must normalize separators to forward slashes.';
COMMENT ON COLUMN public.research_sessions.session_id IS
    'Session identifier extracted from source metadata; not inferred as authoritative from a filename and not unique across files.';
COMMENT ON COLUMN public.research_sessions.file_created_at IS
    'Filesystem creation time of the copied file, not the session start time.';
COMMENT ON COLUMN public.research_sessions.session_last_record_at IS
    'Timestamp of the last non-empty source JSON record, not proof of task completion.';
COMMENT ON COLUMN public.research_sessions.last_observed_event_at IS
    'Timestamp of the last event observed in the scanned region; may differ from the final source record.';
COMMENT ON COLUMN public.research_sessions.scan_complete IS
    'True only when the full file was scanned. False includes not yet scanned; see scan_status.';
COMMENT ON COLUMN public.research_sessions.scanned_bytes IS
    'Source bytes covered by the scan; counters describe that scan, not necessarily the whole file. Unknown metrics remain NULL.';
COMMENT ON COLUMN public.research_sessions.latest_scan_run_id IS
    'Reserved reference to analysis_runs.id. Foreign key deferred until that table is created.';
COMMENT ON COLUMN public.research_sessions.final_analysis_run_id IS
    'Reserved reference to the final consolidated analysis run. Foreign key deferred until analysis_runs exists.';
COMMENT ON COLUMN public.research_sessions.approved_analysis_run_id IS
    'Specific result approved by the researcher. A new final result requires renewed review. Foreign key deferred.';
COMMENT ON COLUMN public.research_sessions.scan_error IS
    'Redacted diagnostic only; do not persist raw session text or secrets.';
COMMENT ON COLUMN public.research_sessions.metadata IS
    'Extensible JSON object containing derived metadata only; no raw conversations or secrets.';
COMMENT ON COLUMN public.research_sessions.updated_at IS
    'Writer must update this timestamp on modification; default applies only on insertion.';

COMMIT;
