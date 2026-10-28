"""Import an explicit SQLite metadata allowlist. Default: local read-only plan.

Only temporary copies under .tmp are opened with SQLite. --apply streams redacted
rows to psql in one transaction; migration 005 must have been applied separately.
No source JSONL, exported_data, backups, live Codex store, or log-body exports.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path, PureWindowsPath
import re
import sqlite3
import subprocess
import tempfile
import time

from import_session_survey import connection_settings, find_psql
from index_session_records import lower_priority
from profile_response_items import peak_memory

ROOT = Path(__file__).resolve().parents[1]
VERSION = '1.0.2'
CHUNK = 256 * 1024
MAX_METADATA_BYTES = 1024 * 1024
UUID_PATTERN = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
PREFIXES = {'codex-dev.db': 'codexdev_', 'state_5.sqlite': 'state5_',
            'logs_2.sqlite': 'logs2_', 'memories_1.sqlite': 'memories1_',
            'goals_1.sqlite': 'goals1_'}
TYPE_MAP = {'TEXT': 'text', 'INTEGER': 'bigint', 'BIGINT': 'bigint',
            'REAL': 'double precision', 'BOOLEAN': 'boolean',
            'TIMESTAMP': 'timestamp without time zone'}


class ImportFailure(Exception):
    """Message must be a fixed diagnostic code, never source data or an exception."""


@dataclass(frozen=True)
class Table:
    database: str
    name: str
    definition: str
    sensitive: tuple[str, ...] = ()
    thread_column: str | None = None
    ordered_key: tuple[str, ...] = ()

    @property
    def columns(self):
        return tuple(tuple(part.split(':')) for part in self.definition.split())

    @property
    def target(self):
        return PREFIXES[self.database] + self.name

    @property
    def destination_columns(self):
        result = []
        for name, kind in self.columns:
            if kind == 'BLOB' and name == 'checksum':
                continue
            if name in self.sensitive:
                if name == 'host_id':
                    result.extend((('host_kind', 'text'), ('host_id_sha256', 'text')))
                    continue
                result.extend(((name + '_length_bytes', 'bigint'),
                               (name + '_sha256', 'text')))
                if name == 'rollout_path':
                    result.append(('rollout_file_name', 'text'))
                if name == 'payload_json':
                    result.append(('payload_type', 'text'))
                if name == 'sandbox_policy':
                    result.append(('sandbox_policy_type', 'text'))
            else:
                result.append((name, TYPE_MAP[kind]))
        return tuple(result)


# Fixed schema contract transcribed from SQLITE_SCHEMA.md. No source discovery.
TABLES = (
    Table('state_5.sqlite', 'threads', '''
        id:TEXT rollout_path:TEXT created_at:INTEGER updated_at:INTEGER source:TEXT
        model_provider:TEXT cwd:TEXT title:TEXT sandbox_policy:TEXT approval_mode:TEXT
        tokens_used:INTEGER has_user_event:INTEGER archived:INTEGER archived_at:INTEGER
        git_sha:TEXT git_branch:TEXT git_origin_url:TEXT cli_version:TEXT first_user_message:TEXT
        agent_nickname:TEXT agent_role:TEXT memory_mode:TEXT model:TEXT reasoning_effort:TEXT
        agent_path:TEXT created_at_ms:INTEGER updated_at_ms:INTEGER thread_source:TEXT preview:TEXT
        ''', ('title', 'first_user_message', 'preview', 'cwd', 'git_origin_url', 'rollout_path',
              'sandbox_policy'), 'id'),
    Table('state_5.sqlite', 'thread_spawn_edges',
          'parent_thread_id:TEXT child_thread_id:TEXT status:TEXT', (), 'child_thread_id'),
    Table('state_5.sqlite', '_sqlx_migrations', '''
        version:BIGINT description:TEXT installed_on:TIMESTAMP success:BOOLEAN
        checksum:BLOB execution_time:BIGINT'''),
    Table('codex-dev.db', 'local_thread_catalog', '''
        host_id:TEXT thread_id:TEXT display_title:TEXT source_created_at:REAL source_updated_at:REAL
        cwd:TEXT source_kind:TEXT source_detail:TEXT model_provider:TEXT git_branch:TEXT
        observation_sequence:INTEGER missing_candidate:INTEGER thread_source:TEXT source_recency_at:REAL
        pending_observed_title:INTEGER project_id:TEXT conversation_origin:TEXT trial_conversation_type:TEXT
        chatgpt_async_status:INTEGER''', ('display_title', 'cwd', 'host_id'), 'thread_id'),
    Table('codex-dev.db', 'thread_timeline_ledger',
          'host_id:TEXT thread_id:TEXT sequence:INTEGER record_id:TEXT payload_json:TEXT',
          ('payload_json', 'host_id'), 'thread_id', ('host_id', 'thread_id', 'sequence')),
    Table('codex-dev.db', 'codex_schema_migrations', '''
        id:TEXT predecessor_id:TEXT checksum:TEXT applied_at:INTEGER
        app_version:TEXT build_flavor:TEXT build_commit:TEXT'''),
    Table('memories_1.sqlite', 'stage1_outputs', '''
        thread_id:TEXT source_updated_at:INTEGER raw_memory:TEXT rollout_summary:TEXT rollout_slug:TEXT
        generated_at:INTEGER usage_count:INTEGER last_usage:INTEGER selected_for_phase2:INTEGER
        selected_for_phase2_source_updated_at:INTEGER''',
          ('raw_memory', 'rollout_summary', 'rollout_slug'), 'thread_id'),
    Table('memories_1.sqlite', 'jobs', '''
        kind:TEXT job_key:TEXT status:TEXT worker_id:TEXT ownership_token:TEXT started_at:INTEGER
        finished_at:INTEGER lease_until:INTEGER retry_at:INTEGER retry_remaining:INTEGER
        last_error:TEXT input_watermark:INTEGER last_success_watermark:INTEGER''', (), 'job_key'),
    Table('logs_2.sqlite', 'logs', '''
        id:INTEGER ts:INTEGER ts_nanos:INTEGER level:TEXT target:TEXT feedback_log_body:TEXT
        module_path:TEXT file:TEXT line:INTEGER thread_id:TEXT process_uuid:TEXT estimated_bytes:INTEGER
        ''', ('feedback_log_body',), 'thread_id'),
)
PROVENANCE = (('source_database', 'text'), ('source_rowid', 'bigint'),
              ('source_db_sha256', 'text'), ('research_session_id', 'bigint'))


def quoted(name):
    if not re.fullmatch(r'[a-z_][a-z_0-9]*', name):
        raise ImportFailure('invalid_static_identifier')
    return '"' + name + '"'


def render_migration():
    """Deterministic generation for review/tests only; never executed by main."""
    out = ['-- Phase 1 design. Apply separately only after researcher approval.',
           '-- INTEGER -> bigint; REAL -> double precision; source epochs remain integers.',
           '-- source_db_sha256 is the versioned DB+WAL snapshot fingerprint (see README).',
           'BEGIN;', "SET LOCAL lock_timeout = '5s';", "SET LOCAL statement_timeout = '30s';",
           "DO $$ BEGIN IF current_database() <> 'codex_session_research' THEN",
           "    RAISE EXCEPTION 'wrong_research_database'; END IF; END $$;", '']
    for table in TABLES:
        declarations = [f'    {quoted(n)} {t}' for n, t in table.destination_columns]
        declarations += [
            f"    source_database text NOT NULL CHECK (source_database = '{table.database}')",
            '    source_rowid bigint NOT NULL',
            "    source_db_sha256 text NOT NULL CHECK (source_db_sha256 ~ '^[0-9a-f]{64}$')",
            '    imported_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP',
            '    research_session_id bigint REFERENCES public.research_sessions(id)',
            '    PRIMARY KEY (source_database, source_rowid)',
        ]
        for column in table.sensitive:
            if column == 'host_id':
                declarations.append("    CHECK (host_kind IN ('chatgpt', 'local'))")
                declarations.append('    CHECK (host_kind IS NULL OR host_id_sha256 IS NOT NULL)')
            else:
                declarations.append(f'    CHECK (({column}_length_bytes IS NULL) = '
                                    f'({column}_sha256 IS NULL))')
                declarations.append(f'    CHECK ({column}_length_bytes >= 0)')
            declarations.append(f"    CHECK ({column}_sha256 ~ '^[0-9a-f]{{64}}$')")
        out += [f'CREATE TABLE public.{table.target} (', ',\n'.join(declarations), ');',
                f'CREATE INDEX {table.target}_research_session_idx',
                f'    ON public.{table.target} (research_session_id) WHERE research_session_id IS NOT NULL;',
                f"COMMENT ON TABLE public.{table.target} IS",
                "    'Research SQLite metadata copy; selected sensitive values replaced by byte length and SHA-256. Original records retained.';", '']
    out += ["COMMENT ON COLUMN public.state5_thread_spawn_edges.research_session_id IS",
            "    'Matched child_thread_id only. Parent remains separately identifiable.';",
            "COMMENT ON COLUMN public.codexdev_thread_timeline_ledger.source_rowid IS",
            "    'Derived one-based ordinal ordered by source PK (host_id, thread_id, sequence). Source is WITHOUT ROWID; this is snapshot-scoped, not a SQLite rowid.';",
            "COMMENT ON COLUMN public.memories1_jobs.research_session_id IS",
            "    'Only exact UUID job_key with kind memory_stage1 is eligible.';",
            "COMMENT ON COLUMN public.logs2_logs.feedback_log_body_sha256 IS",
            "    'Hash of the entire stored body, not evidence of a complete model request.';",
            'COMMIT;', '']
    return '\n'.join(out)


def reject_links(path):
    if path.is_symlink() or (hasattr(path, 'is_junction') and path.is_junction()):
        raise ImportFailure('linked_path_rejected')


def signature(path):
    if not path.exists():
        return None
    reject_links(path)
    st = path.stat()
    if not path.is_file():
        raise ImportFailure('source_is_not_regular_file')
    return st.st_size, st.st_mtime_ns, st.st_ino


@contextmanager
def snapshot(database, root=ROOT):
    """Copy a static research DB bundle, open only its clone, clean up even on failure.

    Fingerprint: SHA256(tag || framed filename/size/content for DB and WAL).
    SHM is copied but excluded: it is a volatile WAL index, not logical row data.
    No immutable=1: that can omit uncheckpointed WAL rows.
    """
    if database not in {t.database for t in TABLES}:
        raise ImportFailure('database_not_allowlisted')
    root = Path(root).resolve()
    source = root / 'sqlite'; scratch_root = root / '.tmp'
    reject_links(source); reject_links(scratch_root)
    scratch_root.mkdir(exist_ok=True)
    paths = [source / (database + suffix) for suffix in ('', '-wal', '-shm')]
    signatures = [signature(p) for p in paths]
    if signatures[0] is None:
        raise ImportFailure('required_database_missing')
    with tempfile.TemporaryDirectory(prefix='codex-sqlite-import-', dir=scratch_root) as temporary:
        scratch = Path(temporary).resolve()
        if scratch.parent != scratch_root.resolve():
            raise ImportFailure('temporary_directory_outside_workspace')
        digest = hashlib.sha256(b'codex-sqlite-db-wal-v1\0')
        for index, (src, sig) in enumerate(zip(paths, signatures)):
            if sig is None:
                continue
            if index < 2:
                name = src.name.encode('ascii')
                digest.update(len(name).to_bytes(4, 'big') + name + sig[0].to_bytes(8, 'big'))
            with src.open('rb') as incoming, (scratch / src.name).open('xb') as outgoing:
                while chunk := incoming.read(CHUNK):
                    outgoing.write(chunk)
                    if index < 2:
                        digest.update(chunk)
        if signatures != [signature(p) for p in paths]:
            raise ImportFailure('source_changed_during_copy')
        connection = None
        try:
            connection = sqlite3.connect((scratch / database).as_uri() + '?mode=ro', uri=True)
            connection.execute('PRAGMA query_only=ON')
            connection.execute('PRAGMA cache_size=-2048')
            connection.execute('PRAGMA mmap_size=0')
            connection.execute('PRAGMA temp_store=FILE')
            connection.execute('BEGIN')
            if connection.execute('PRAGMA encoding').fetchone()[0] != 'UTF-8':
                raise ImportFailure('unsupported_database_encoding')
            connection.create_function('research_file_name', 1, safe_file_name)
            connection.create_function('research_payload_type', 1, safe_payload_type)
            connection.create_function('research_sandbox_policy_type', 1, safe_sandbox_policy_type)
            yield connection, digest.hexdigest()
        finally:
            if connection is not None:
                connection.close()
        # Copies are closed before the context manager deletes this exact scratch folder.


def safe_file_name(value):
    if value is None:
        return None
    if not isinstance(value, str) or len(value.encode('utf-8')) > 32768:
        raise ImportFailure('invalid_rollout_path')
    name = PureWindowsPath(value).name
    # Do not export arbitrary filenames containing private text.
    if not re.fullmatch(r'rollout-[0-9T_.:-]+-' + UUID_PATTERN +
                        r'(?:[_-]' + UUID_PATTERN + r')*\.jsonl', name, re.IGNORECASE):
        raise ImportFailure('nonstandard_rollout_file_name')
    return name


def safe_payload_type(value):
    if isinstance(value, str) and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}', value):
        return value
    return None


def safe_sandbox_policy_type(value):
    if isinstance(value, str) and re.fullmatch(r'[A-Za-z][A-Za-z0-9_-]{0,63}', value):
        return value
    return None


def validate_schema(con, table):
    rows = con.execute(f'PRAGMA table_info({quoted(table.name)})').fetchall()
    actual = tuple((r[1], r[2].upper().strip()) for r in rows)
    if actual != table.columns:
        raise ImportFailure('source_schema_mismatch')
    without_rowid = con.execute(
        "SELECT wr FROM pragma_table_list WHERE schema='main' AND name=?", (table.name,)
    ).fetchone()[0]
    primary_key = tuple(r[1] for r in sorted(rows, key=lambda r:r[5]) if r[5])
    if bool(without_rowid) != bool(table.ordered_key) or (table.ordered_key and primary_key != table.ordered_key):
        raise ImportFailure('source_row_identity_layout_mismatch')
    if not table.ordered_key:
        con.execute(f'SELECT rowid FROM {quoted(table.name)} LIMIT 0')
    return con.execute(f'SELECT count(*) FROM {quoted(table.name)}').fetchone()[0]


def hash_cell(con, table, column, rowid, length):
    if length is None:
        return None
    digest = hashlib.sha256(); read = 0
    if table.ordered_key:
        # WITHOUT ROWID has no incremental blob handle. Fetch bounded BLOB slices
        # at the same PK-ordered position; raw (potentially sensitive) keys stay in SQLite.
        order = ', '.join(quoted(k) for k in table.ordered_key)
        sql = (f'SELECT substr(CAST({quoted(column)} AS BLOB), ?, ?) FROM {quoted(table.name)} '
               f'ORDER BY {order} LIMIT 1 OFFSET ?')
        while read < length:
            result = con.execute(sql, (read + 1, min(CHUNK, length-read), rowid-1)).fetchone()
            if result is None or not result[0] or not isinstance(result[0], bytes):
                raise ImportFailure('sensitive_short_read')
            chunk = result[0]
            if len(chunk) > min(CHUNK, length-read):
                raise ImportFailure('sensitive_length_mismatch')
            read += len(chunk); digest.update(chunk)
        return digest.hexdigest()
    with con.blobopen(table.name, column, rowid, readonly=True) as blob:
        if len(blob) != length:
            raise ImportFailure('sensitive_length_mismatch')
        while chunk := blob.read(CHUNK):
            read += len(chunk)
            digest.update(chunk)
    if read != length:
        raise ImportFailure('sensitive_short_read')
    return digest.hexdigest()


def validate_scalar(value, kind):
    if value is None:
        return None
    if kind in ('INTEGER', 'BIGINT'):
        if type(value) is not int or not -(2**63) <= value < 2**63:
            raise ImportFailure('invalid_integer_storage_class')
    elif kind == 'BOOLEAN':
        if type(value) is not int or value not in (0, 1):
            raise ImportFailure('invalid_boolean_storage_class')
        return bool(value)
    elif kind == 'REAL':
        if type(value) not in (int, float) or not math.isfinite(value):
            raise ImportFailure('invalid_real_storage_class')
    elif kind in ('TEXT', 'TIMESTAMP'):
        if not isinstance(value, str) or '\0' in value:
            raise ImportFailure('invalid_text_storage_class')
    return value


def iter_rows(con, table, digest):
    """One row at a time; sensitive TEXT is never selected into Python as text."""
    expressions = [('NULL' if table.ordered_key else 'rowid') + ' AS source_rowid']
    selected = []; ordinary = []
    for name, kind in table.columns:
        column = quoted(name)
        if name == 'checksum' and kind == 'BLOB':
            continue
        if name in table.sensitive:
            expressions.extend((f'length(CAST({column} AS BLOB)) AS {name}_length_bytes',
                                f'typeof({column}) AS {name}_storage_type'))
            selected.extend((name + '_length_bytes', name + '_storage_type'))
            if name == 'rollout_path':
                expressions.append('CASE WHEN length(CAST(rollout_path AS BLOB)) <= 32768 '
                                   'THEN research_file_name(rollout_path) ELSE NULL END AS rollout_file_name')
                selected.append('rollout_file_name')
            if name in ('payload_json', 'sandbox_policy'):
                alias, function, limit = (
                    ('payload_type', 'research_payload_type', 128) if name == 'payload_json' else
                    ('sandbox_policy_type', 'research_sandbox_policy_type', 64))
                expressions.append(f"CASE WHEN json_valid({column}) THEN CASE "
                                   f"WHEN json_type({column}, '$.type') = 'text' "
                                   f"AND length(CAST(json_extract({column}, '$.type') AS BLOB)) <= {limit} "
                                   f"THEN {function}(json_extract({column}, '$.type')) "
                                   f"END END AS {alias}")
                selected.append(alias)
            if name == 'host_id':
                # SQLite returns only a literal allowlisted prefix, never the raw ID.
                expressions.append("CASE (CASE WHEN instr(host_id, ':') > 0 "
                                   "THEN substr(host_id, 1, instr(host_id, ':')-1) ELSE host_id END) "
                                   "WHEN 'chatgpt' THEN 'chatgpt' WHEN 'local' THEN 'local' "
                                   "ELSE NULL END AS host_kind")
                selected.append('host_kind')
        else:
            expressions.extend((f'CASE WHEN length(CAST({column} AS BLOB)) <= {MAX_METADATA_BYTES} '
                                f'THEN {column} ELSE NULL END AS {column}',
                                f'length(CAST({column} AS BLOB)) AS {name}_source_bytes'))
            selected.extend((name, name + '_source_bytes')); ordinary.append((name, kind))
    order = ', '.join(quoted(k) for k in table.ordered_key) if table.ordered_key else 'rowid'
    cursor = con.execute('SELECT ' + ', '.join(expressions) +
                         f' FROM {quoted(table.name)} ORDER BY {order}')
    for ordinal, values in enumerate(cursor, 1):
        rowid = ordinal if table.ordered_key else values[0]
        row = dict(zip(selected, values[1:]))
        for name, kind in ordinary:
            size = row.pop(name + '_source_bytes')
            if size is not None and size > MAX_METADATA_BYTES:
                raise ImportFailure('oversized_unredacted_metadata')
            row[name] = validate_scalar(row[name], kind)
        for name in table.sensitive:
            storage = row.pop(name + '_storage_type')
            if storage not in ('text', 'null'):
                raise ImportFailure('invalid_sensitive_storage_class')
            length = row[name + '_length_bytes']
            row[name + '_sha256'] = hash_cell(con, table, name, rowid, length)
            if name == 'host_id':
                del row[name + '_length_bytes']
            if name == 'rollout_path' and length is not None and row['rollout_file_name'] is None:
                raise ImportFailure('invalid_rollout_file_name')
        row.update(source_database=table.database, source_rowid=rowid,
                   source_db_sha256=digest, research_session_id=None)
        yield row


def copy_value(value):
    if value is None:
        return r'\N'
    if isinstance(value, bool):
        return 'true' if value else 'false'
    text = str(value)
    if '\0' in text:
        raise ImportFailure('nul_not_supported_in_postgres_text')
    return text.replace('\\', '\\\\').replace('\t', r'\t').replace('\n', r'\n').replace('\r', r'\r')


def transaction_sql():
    # Only research_sessions metadata is read. UUIDs/file names never leave psql.
    return f"""BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='120s';
SET LOCAL work_mem='8MB';
SET LOCAL max_parallel_workers_per_gather=0;
SET LOCAL client_min_messages=ERROR;
DO $$ BEGIN
  IF current_database() <> 'codex_session_research' THEN RAISE EXCEPTION 'wrong_database'; END IF;
  IF NOT pg_try_advisory_xact_lock(260026005) THEN RAISE EXCEPTION 'import_already_running'; END IF;
END $$;
CREATE TEMP TABLE sqlite_import_session_matches ON COMMIT DROP AS
WITH candidates AS (
  SELECT lower(session_id) AS uuid, id FROM public.research_sessions
  WHERE lower(session_id) ~ '^{UUID_PATTERN}$'
  UNION
  SELECT lower(m[1]), rs.id FROM public.research_sessions rs
  CROSS JOIN LATERAL regexp_matches(lower(rs.file_name), '({UUID_PATTERN})', 'g') AS m
)
SELECT uuid, min(id) AS research_session_id FROM candidates
GROUP BY uuid HAVING count(DISTINCT id)=1;
CREATE UNIQUE INDEX ON sqlite_import_session_matches(uuid);
"""


def start_table_sql(table):
    columns = ', '.join(quoted(n) for n, _ in table.destination_columns + PROVENANCE)
    return (f'LOCK TABLE public.{table.target} IN SHARE ROW EXCLUSIVE MODE;\n'
            f'CREATE TEMP TABLE sqlite_import_stage (LIKE public.{table.target} INCLUDING DEFAULTS) ON COMMIT DROP;\n'
            f'COPY sqlite_import_stage ({columns}) FROM STDIN;\n')


def finish_table_sql(table, count):
    columns = [n for n, _ in table.destination_columns + PROVENANCE]
    fields = ', '.join(quoted(n) for n in columns)
    comparisons = ' OR '.join(f't.{quoted(n)} IS DISTINCT FROM s.{quoted(n)}' for n in columns)
    link = ''
    if table.thread_column:
        source = 's.' + quoted(table.thread_column)
        condition = " AND s.kind='memory_stage1'" if table.name == 'jobs' else ''
        link = (f'UPDATE sqlite_import_stage s SET research_session_id=m.research_session_id '
                f'FROM sqlite_import_session_matches m WHERE lower({source})=m.uuid{condition};\n')
    return f"""\\.
{link}DO $$ BEGIN
  IF (SELECT count(*) FROM sqlite_import_stage) <> {count} THEN
    RAISE EXCEPTION 'staged_count_mismatch'; END IF;
END $$;
CREATE TEMP TABLE sqlite_import_insert_count (n bigint) ON COMMIT DROP;
WITH inserted AS (
 INSERT INTO public.{table.target} ({fields}) SELECT {fields} FROM sqlite_import_stage
 ON CONFLICT (source_database, source_rowid) DO NOTHING RETURNING 1
) INSERT INTO sqlite_import_insert_count SELECT count(*) FROM inserted;
DO $$ BEGIN
  IF (SELECT count(*) FROM public.{table.target} WHERE source_database='{table.database}') <> {count}
     OR EXISTS (SELECT 1 FROM sqlite_import_stage s
       LEFT JOIN public.{table.target} t USING (source_database,source_rowid)
       WHERE t.source_rowid IS NULL OR {comparisons}) THEN
    RAISE EXCEPTION 'snapshot_or_value_mismatch'; END IF;
END $$;
SELECT json_build_object('table','{table.target}', 'source_rows',{count},
  'destination_rows',(SELECT count(*) FROM public.{table.target} WHERE source_database='{table.database}'),
  'inserted_rows',(SELECT n FROM sqlite_import_insert_count),
  'matched_session_rows',(SELECT count(*) FROM sqlite_import_stage WHERE research_session_id IS NOT NULL));
DROP TABLE sqlite_import_stage;
DROP TABLE sqlite_import_insert_count;
"""


class PsqlStream:
    """No SQL/data spool, no stderr leakage, one atomic transaction for nine tables."""
    def __enter__(self):
        config = connection_settings()
        env = os.environ.copy()
        env.update(PGPASSWORD=config['DB_PASSWORD'], PGCONNECT_TIMEOUT='5', PGCLIENTENCODING='UTF8',
                   PGAPPNAME='research_sqlite_metadata_import')
        command = [find_psql(), '-X', '-w', '-q', '-A', '-t', '-v', 'ON_ERROR_STOP=1',
                   '-v', 'VERBOSITY=sqlstate', '-v', 'ECHO=none', '-v', 'ECHO_HIDDEN=off',
                   '-h', config['DB_HOST'], '-p', config['DB_PORT'],
                   '-U', config['DB_USERNAME'], '-d', config['DB_DATABASE']]
        options = {'creationflags': subprocess.BELOW_NORMAL_PRIORITY_CLASS} if os.name == 'nt' else {}
        self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.DEVNULL, text=True, encoding='utf-8', env=env,
                                        **options)
        return self

    def write(self, text):
        self.process.stdin.write(text)

    def finish(self):
        self.process.stdin.close(); self.process.stdin = None
        output, _ = self.process.communicate(timeout=1800)
        if self.process.returncode:
            raise ImportFailure('postgres_transaction_failed')
        if len(output) > 65536:
            raise ImportFailure('unexpected_postgres_output')
        rows = [json.loads(line) for line in output.splitlines() if line.strip()]
        expected = {t.target for t in TABLES}
        if len(rows) != len(TABLES) or {r.get('table') for r in rows} != expected:
            raise ImportFailure('invalid_postgres_verification_output')
        for row in rows:
            if set(row) != {'table','source_rows','destination_rows','inserted_rows','matched_session_rows'}:
                raise ImportFailure('invalid_postgres_verification_output')
            if any(type(v) is not int or v < 0 for k, v in row.items() if k != 'table'):
                raise ImportFailure('invalid_postgres_verification_output')
            if row['source_rows'] != row['destination_rows']:
                raise ImportFailure('destination_count_mismatch')
        return rows

    def __exit__(self, *exc):
        if self.process.poll() is None:
            self.process.kill(); self.process.communicate()
        for handle in (self.process.stdin, self.process.stdout):
            if handle is not None:
                handle.close()


def run_import(apply=False, root=ROOT, writer=None):
    plan = []
    if apply and writer is None:
        raise ImportFailure('writer_required')
    if apply:
        writer.write(transaction_sql())
    for database in dict.fromkeys(t.database for t in TABLES):
        with snapshot(database, root) as (con, digest):
            for table in (t for t in TABLES if t.database == database):
                count = validate_schema(con, table)
                plan.append({'table':table.target, 'source_rows':count})
                if not apply:
                    continue
                writer.write(start_table_sql(table)); streamed = 0
                columns = [n for n, _ in table.destination_columns + PROVENANCE]
                for row in iter_rows(con, table, digest):
                    writer.write('\t'.join(copy_value(row[n]) for n in columns) + '\n')
                    streamed += 1
                if streamed != count:
                    raise ImportFailure('source_row_count_changed')
                writer.write(finish_table_sql(table, count))
    if apply:
        writer.write('COMMIT;\n')
        return writer.finish()
    return plan


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true', help='Write redacted metadata; requires migration 005')
    args = parser.parse_args(argv)
    started = time.perf_counter(); priority = lower_priority(); success = False
    try:
        if args.apply:
            with PsqlStream() as writer:
                tables = run_import(True, writer=writer)
        else:
            tables = run_import()
        result = {'version':VERSION, 'mode':'apply' if args.apply else 'plan',
                  'status':'completed', 'tables':tables,
                  'source_rows':sum(r['source_rows'] for r in tables),
                  'database_written':args.apply}
        success = True
    except (Exception, KeyboardInterrupt) as error:
        # Never print sqlite/psql messages, paths, SQL parameters, or source values.
        result = {'version':VERSION, 'status':'failed', 'mode':'apply' if args.apply else 'plan',
                  'error_code':str(error) if isinstance(error, ImportFailure) else
                  ('interrupted' if isinstance(error, KeyboardInterrupt) else 'operation_failed'),
                  'database_written':None if args.apply else False}
    result.update(elapsed_seconds=round(time.perf_counter()-started, 3),
                  peak_working_set_bytes=peak_memory(), priority=priority)
    print(json.dumps(result, ensure_ascii=True))
    return 0 if success else 1


if __name__ == '__main__':
    raise SystemExit(main())
