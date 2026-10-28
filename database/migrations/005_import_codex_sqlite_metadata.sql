-- Phase 1 design. Apply separately only after researcher approval.
-- INTEGER -> bigint; REAL -> double precision; source epochs remain integers.
-- source_db_sha256 is the versioned DB+WAL snapshot fingerprint (see README).
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
DO $$ BEGIN IF current_database() <> 'codex_session_research' THEN
    RAISE EXCEPTION 'wrong_research_database'; END IF; END $$;

CREATE TABLE public.state5_threads (
    "id" text,
    "rollout_path_length_bytes" bigint,
    "rollout_path_sha256" text,
    "rollout_file_name" text,
    "created_at" bigint,
    "updated_at" bigint,
    "source" text,
    "model_provider" text,
    "cwd_length_bytes" bigint,
    "cwd_sha256" text,
    "title_length_bytes" bigint,
    "title_sha256" text,
    "sandbox_policy_length_bytes" bigint,
    "sandbox_policy_sha256" text,
    "sandbox_policy_type" text,
    "approval_mode" text,
    "tokens_used" bigint,
    "has_user_event" bigint,
    "archived" bigint,
    "archived_at" bigint,
    "git_sha" text,
    "git_branch" text,
    "git_origin_url_length_bytes" bigint,
    "git_origin_url_sha256" text,
    "cli_version" text,
    "first_user_message_length_bytes" bigint,
    "first_user_message_sha256" text,
    "agent_nickname" text,
    "agent_role" text,
    "memory_mode" text,
    "model" text,
    "reasoning_effort" text,
    "agent_path" text,
    "created_at_ms" bigint,
    "updated_at_ms" bigint,
    "thread_source" text,
    "preview_length_bytes" bigint,
    "preview_sha256" text,
    source_database text NOT NULL CHECK (source_database = 'state_5.sqlite'),
    source_rowid bigint NOT NULL,
    source_db_sha256 text NOT NULL CHECK (source_db_sha256 ~ '^[0-9a-f]{64}$'),
    imported_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    research_session_id bigint REFERENCES public.research_sessions(id),
    PRIMARY KEY (source_database, source_rowid),
    CHECK ((title_length_bytes IS NULL) = (title_sha256 IS NULL)),
    CHECK (title_length_bytes >= 0),
    CHECK (title_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK ((first_user_message_length_bytes IS NULL) = (first_user_message_sha256 IS NULL)),
    CHECK (first_user_message_length_bytes >= 0),
    CHECK (first_user_message_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK ((preview_length_bytes IS NULL) = (preview_sha256 IS NULL)),
    CHECK (preview_length_bytes >= 0),
    CHECK (preview_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK ((cwd_length_bytes IS NULL) = (cwd_sha256 IS NULL)),
    CHECK (cwd_length_bytes >= 0),
    CHECK (cwd_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK ((git_origin_url_length_bytes IS NULL) = (git_origin_url_sha256 IS NULL)),
    CHECK (git_origin_url_length_bytes >= 0),
    CHECK (git_origin_url_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK ((rollout_path_length_bytes IS NULL) = (rollout_path_sha256 IS NULL)),
    CHECK (rollout_path_length_bytes >= 0),
    CHECK (rollout_path_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK ((sandbox_policy_length_bytes IS NULL) = (sandbox_policy_sha256 IS NULL)),
    CHECK (sandbox_policy_length_bytes >= 0),
    CHECK (sandbox_policy_sha256 ~ '^[0-9a-f]{64}$')
);
CREATE INDEX state5_threads_research_session_idx
    ON public.state5_threads (research_session_id) WHERE research_session_id IS NOT NULL;
COMMENT ON TABLE public.state5_threads IS
    'Research SQLite metadata copy; selected sensitive values replaced by byte length and SHA-256. Original records retained.';

CREATE TABLE public.state5_thread_spawn_edges (
    "parent_thread_id" text,
    "child_thread_id" text,
    "status" text,
    source_database text NOT NULL CHECK (source_database = 'state_5.sqlite'),
    source_rowid bigint NOT NULL,
    source_db_sha256 text NOT NULL CHECK (source_db_sha256 ~ '^[0-9a-f]{64}$'),
    imported_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    research_session_id bigint REFERENCES public.research_sessions(id),
    PRIMARY KEY (source_database, source_rowid)
);
CREATE INDEX state5_thread_spawn_edges_research_session_idx
    ON public.state5_thread_spawn_edges (research_session_id) WHERE research_session_id IS NOT NULL;
COMMENT ON TABLE public.state5_thread_spawn_edges IS
    'Research SQLite metadata copy; selected sensitive values replaced by byte length and SHA-256. Original records retained.';

CREATE TABLE public.state5__sqlx_migrations (
    "version" bigint,
    "description" text,
    "installed_on" timestamp without time zone,
    "success" boolean,
    "execution_time" bigint,
    source_database text NOT NULL CHECK (source_database = 'state_5.sqlite'),
    source_rowid bigint NOT NULL,
    source_db_sha256 text NOT NULL CHECK (source_db_sha256 ~ '^[0-9a-f]{64}$'),
    imported_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    research_session_id bigint REFERENCES public.research_sessions(id),
    PRIMARY KEY (source_database, source_rowid)
);
CREATE INDEX state5__sqlx_migrations_research_session_idx
    ON public.state5__sqlx_migrations (research_session_id) WHERE research_session_id IS NOT NULL;
COMMENT ON TABLE public.state5__sqlx_migrations IS
    'Research SQLite metadata copy; selected sensitive values replaced by byte length and SHA-256. Original records retained.';

CREATE TABLE public.codexdev_local_thread_catalog (
    "host_kind" text,
    "host_id_sha256" text,
    "thread_id" text,
    "display_title_length_bytes" bigint,
    "display_title_sha256" text,
    "source_created_at" double precision,
    "source_updated_at" double precision,
    "cwd_length_bytes" bigint,
    "cwd_sha256" text,
    "source_kind" text,
    "source_detail" text,
    "model_provider" text,
    "git_branch" text,
    "observation_sequence" bigint,
    "missing_candidate" bigint,
    "thread_source" text,
    "source_recency_at" double precision,
    "pending_observed_title" bigint,
    "project_id" text,
    "conversation_origin" text,
    "trial_conversation_type" text,
    "chatgpt_async_status" bigint,
    source_database text NOT NULL CHECK (source_database = 'codex-dev.db'),
    source_rowid bigint NOT NULL,
    source_db_sha256 text NOT NULL CHECK (source_db_sha256 ~ '^[0-9a-f]{64}$'),
    imported_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    research_session_id bigint REFERENCES public.research_sessions(id),
    PRIMARY KEY (source_database, source_rowid),
    CHECK ((display_title_length_bytes IS NULL) = (display_title_sha256 IS NULL)),
    CHECK (display_title_length_bytes >= 0),
    CHECK (display_title_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK ((cwd_length_bytes IS NULL) = (cwd_sha256 IS NULL)),
    CHECK (cwd_length_bytes >= 0),
    CHECK (cwd_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (host_kind IN ('chatgpt', 'local')),
    CHECK (host_kind IS NULL OR host_id_sha256 IS NOT NULL),
    CHECK (host_id_sha256 ~ '^[0-9a-f]{64}$')
);
CREATE INDEX codexdev_local_thread_catalog_research_session_idx
    ON public.codexdev_local_thread_catalog (research_session_id) WHERE research_session_id IS NOT NULL;
COMMENT ON TABLE public.codexdev_local_thread_catalog IS
    'Research SQLite metadata copy; selected sensitive values replaced by byte length and SHA-256. Original records retained.';

CREATE TABLE public.codexdev_thread_timeline_ledger (
    "host_kind" text,
    "host_id_sha256" text,
    "thread_id" text,
    "sequence" bigint,
    "record_id" text,
    "payload_json_length_bytes" bigint,
    "payload_json_sha256" text,
    "payload_type" text,
    source_database text NOT NULL CHECK (source_database = 'codex-dev.db'),
    source_rowid bigint NOT NULL,
    source_db_sha256 text NOT NULL CHECK (source_db_sha256 ~ '^[0-9a-f]{64}$'),
    imported_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    research_session_id bigint REFERENCES public.research_sessions(id),
    PRIMARY KEY (source_database, source_rowid),
    CHECK ((payload_json_length_bytes IS NULL) = (payload_json_sha256 IS NULL)),
    CHECK (payload_json_length_bytes >= 0),
    CHECK (payload_json_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (host_kind IN ('chatgpt', 'local')),
    CHECK (host_kind IS NULL OR host_id_sha256 IS NOT NULL),
    CHECK (host_id_sha256 ~ '^[0-9a-f]{64}$')
);
CREATE INDEX codexdev_thread_timeline_ledger_research_session_idx
    ON public.codexdev_thread_timeline_ledger (research_session_id) WHERE research_session_id IS NOT NULL;
COMMENT ON TABLE public.codexdev_thread_timeline_ledger IS
    'Research SQLite metadata copy; selected sensitive values replaced by byte length and SHA-256. Original records retained.';

CREATE TABLE public.codexdev_codex_schema_migrations (
    "id" text,
    "predecessor_id" text,
    "checksum" text,
    "applied_at" bigint,
    "app_version" text,
    "build_flavor" text,
    "build_commit" text,
    source_database text NOT NULL CHECK (source_database = 'codex-dev.db'),
    source_rowid bigint NOT NULL,
    source_db_sha256 text NOT NULL CHECK (source_db_sha256 ~ '^[0-9a-f]{64}$'),
    imported_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    research_session_id bigint REFERENCES public.research_sessions(id),
    PRIMARY KEY (source_database, source_rowid)
);
CREATE INDEX codexdev_codex_schema_migrations_research_session_idx
    ON public.codexdev_codex_schema_migrations (research_session_id) WHERE research_session_id IS NOT NULL;
COMMENT ON TABLE public.codexdev_codex_schema_migrations IS
    'Research SQLite metadata copy; selected sensitive values replaced by byte length and SHA-256. Original records retained.';

CREATE TABLE public.memories1_stage1_outputs (
    "thread_id" text,
    "source_updated_at" bigint,
    "raw_memory_length_bytes" bigint,
    "raw_memory_sha256" text,
    "rollout_summary_length_bytes" bigint,
    "rollout_summary_sha256" text,
    "rollout_slug_length_bytes" bigint,
    "rollout_slug_sha256" text,
    "generated_at" bigint,
    "usage_count" bigint,
    "last_usage" bigint,
    "selected_for_phase2" bigint,
    "selected_for_phase2_source_updated_at" bigint,
    source_database text NOT NULL CHECK (source_database = 'memories_1.sqlite'),
    source_rowid bigint NOT NULL,
    source_db_sha256 text NOT NULL CHECK (source_db_sha256 ~ '^[0-9a-f]{64}$'),
    imported_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    research_session_id bigint REFERENCES public.research_sessions(id),
    PRIMARY KEY (source_database, source_rowid),
    CHECK ((raw_memory_length_bytes IS NULL) = (raw_memory_sha256 IS NULL)),
    CHECK (raw_memory_length_bytes >= 0),
    CHECK (raw_memory_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK ((rollout_summary_length_bytes IS NULL) = (rollout_summary_sha256 IS NULL)),
    CHECK (rollout_summary_length_bytes >= 0),
    CHECK (rollout_summary_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK ((rollout_slug_length_bytes IS NULL) = (rollout_slug_sha256 IS NULL)),
    CHECK (rollout_slug_length_bytes >= 0),
    CHECK (rollout_slug_sha256 ~ '^[0-9a-f]{64}$')
);
CREATE INDEX memories1_stage1_outputs_research_session_idx
    ON public.memories1_stage1_outputs (research_session_id) WHERE research_session_id IS NOT NULL;
COMMENT ON TABLE public.memories1_stage1_outputs IS
    'Research SQLite metadata copy; selected sensitive values replaced by byte length and SHA-256. Original records retained.';

CREATE TABLE public.memories1_jobs (
    "kind" text,
    "job_key" text,
    "status" text,
    "worker_id" text,
    "ownership_token" text,
    "started_at" bigint,
    "finished_at" bigint,
    "lease_until" bigint,
    "retry_at" bigint,
    "retry_remaining" bigint,
    "last_error" text,
    "input_watermark" bigint,
    "last_success_watermark" bigint,
    source_database text NOT NULL CHECK (source_database = 'memories_1.sqlite'),
    source_rowid bigint NOT NULL,
    source_db_sha256 text NOT NULL CHECK (source_db_sha256 ~ '^[0-9a-f]{64}$'),
    imported_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    research_session_id bigint REFERENCES public.research_sessions(id),
    PRIMARY KEY (source_database, source_rowid)
);
CREATE INDEX memories1_jobs_research_session_idx
    ON public.memories1_jobs (research_session_id) WHERE research_session_id IS NOT NULL;
COMMENT ON TABLE public.memories1_jobs IS
    'Research SQLite metadata copy; selected sensitive values replaced by byte length and SHA-256. Original records retained.';

CREATE TABLE public.logs2_logs (
    "id" bigint,
    "ts" bigint,
    "ts_nanos" bigint,
    "level" text,
    "target" text,
    "feedback_log_body_length_bytes" bigint,
    "feedback_log_body_sha256" text,
    "module_path" text,
    "file" text,
    "line" bigint,
    "thread_id" text,
    "process_uuid" text,
    "estimated_bytes" bigint,
    source_database text NOT NULL CHECK (source_database = 'logs_2.sqlite'),
    source_rowid bigint NOT NULL,
    source_db_sha256 text NOT NULL CHECK (source_db_sha256 ~ '^[0-9a-f]{64}$'),
    imported_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    research_session_id bigint REFERENCES public.research_sessions(id),
    PRIMARY KEY (source_database, source_rowid),
    CHECK ((feedback_log_body_length_bytes IS NULL) = (feedback_log_body_sha256 IS NULL)),
    CHECK (feedback_log_body_length_bytes >= 0),
    CHECK (feedback_log_body_sha256 ~ '^[0-9a-f]{64}$')
);
CREATE INDEX logs2_logs_research_session_idx
    ON public.logs2_logs (research_session_id) WHERE research_session_id IS NOT NULL;
COMMENT ON TABLE public.logs2_logs IS
    'Research SQLite metadata copy; selected sensitive values replaced by byte length and SHA-256. Original records retained.';

COMMENT ON COLUMN public.state5_thread_spawn_edges.research_session_id IS
    'Matched child_thread_id only. Parent remains separately identifiable.';
COMMENT ON COLUMN public.codexdev_thread_timeline_ledger.source_rowid IS
    'Derived one-based ordinal ordered by source PK (host_id, thread_id, sequence). Source is WITHOUT ROWID; this is snapshot-scoped, not a SQLite rowid.';
COMMENT ON COLUMN public.memories1_jobs.research_session_id IS
    'Only exact UUID job_key with kind memory_stage1 is eligible.';
COMMENT ON COLUMN public.logs2_logs.feedback_log_body_sha256 IS
    'Hash of the entire stored body, not evidence of a complete model request.';
COMMIT;
