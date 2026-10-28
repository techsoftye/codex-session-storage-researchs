"""Synthetic-only tests. No project SQLite, PostgreSQL, .env, or JSONL access."""
import contextlib
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch

import import_codex_sqlite as importer


THREAD = '11111111-1111-4111-8111-111111111111'
PRIVATE = 'SYNTHETIC_PRIVATE_CONTENT_أبجد'
FILE_NAME = f'rollout-2026-01-01T03-00-13-{THREAD}.jsonl'
PRIVATE_PATH = 'C:\\synthetic-private\\sessions\\' + FILE_NAME


def default_values(table):
    row = {}
    for name, kind in table.columns:
        row[name] = {'TEXT':'fixture', 'INTEGER':1, 'BIGINT':1, 'REAL':1.25,
                     'BOOLEAN':1, 'BLOB':b'fixture', 'TIMESTAMP':'2026-06-14 12:00:00'}[kind]
    for name in table.sensitive:
        row[name] = PRIVATE
    if table.thread_column:
        row[table.thread_column] = THREAD
    if table.name == 'threads':
        row['rollout_path'] = PRIVATE_PATH
        row['sandbox_policy'] = json.dumps({'type':'workspace-write', 'writable_roots':[PRIVATE_PATH]})
    if 'host_id' in table.sensitive:
        row['host_id'] = 'chatgpt:synthetic-account:synthetic-user'
    if table.name == 'thread_spawn_edges':
        row['parent_thread_id'] = '22222222-2222-4222-8222-222222222222'
    if table.name == 'thread_timeline_ledger':
        row['payload_json'] = json.dumps({'type':'transcript-segment','text':PRIVATE})
    if table.name == 'jobs':
        row['kind'] = 'memory_stage1'
    return row


def insert(con, table, row):
    columns = ','.join(importer.quoted(n) for n, _ in table.columns)
    con.execute(f'INSERT INTO {importer.quoted(table.name)} ({columns}) VALUES (' +
                ','.join('?' for _ in row) + ')', tuple(row[n] for n, _ in table.columns))


class SyntheticFixture(unittest.TestCase):
    def setUp(self):
        scratch = importer.ROOT / '.tmp'
        scratch.mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix='sqlite-metadata-tests-', dir=scratch)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / 'sqlite').mkdir()
        (self.root / '.tmp').mkdir()
        for database in dict.fromkeys(t.database for t in importer.TABLES):
            with sqlite3.connect(self.root / 'sqlite' / database) as con:
                for table in (t for t in importer.TABLES if t.database == database):
                    definition = ','.join(importer.quoted(n) + ' ' + kind for n, kind in table.columns)
                    suffix = ''
                    if table.ordered_key:
                        definition += ', PRIMARY KEY (' + ','.join(importer.quoted(n) for n in table.ordered_key) + ')'
                        suffix = ' WITHOUT ROWID'
                    con.execute(f'CREATE TABLE {importer.quoted(table.name)} ({definition}){suffix}')
                    insert(con, table, default_values(table))
            con.close()

    def table(self, name):
        return next(t for t in importer.TABLES if t.name == name)

    def source_signature(self):
        return {p.name:hashlib.sha256(p.read_bytes()).hexdigest()
                for p in (self.root / 'sqlite').iterdir() if p.is_file()}

    def capture_import_stream(self):
        class Sink:
            def __init__(self): self.output = io.StringIO()
            def write(self, text): self.output.write(text)
            def finish(self): return []
        sink = Sink()
        importer.run_import(True, self.root, sink)
        return sink.output.getvalue()

    def test_plan_uses_only_allowlist_counts_without_postgres(self):
        (self.root / 'sqlite' / 'goals_1.sqlite').write_bytes(b'ignored invalid database')
        (self.root / 'sqlite' / 'db-backups').mkdir()
        (self.root / 'sqlite' / 'exported_data').mkdir()
        with patch.object(importer, 'PsqlStream') as pg:
            plan = importer.run_import(root=self.root)
        pg.assert_not_called()
        self.assertEqual(len(plan), 9)
        self.assertEqual(sum(x['source_rows'] for x in plan), 9)
        self.assertEqual(list((self.root / '.tmp').iterdir()), [])

    def test_open_only_readonly_clone_and_preserve_source(self):
        before = self.source_signature()
        real_connect = sqlite3.connect
        opened = []
        def checked_connect(database, **kwargs):
            self.assertIn('/.tmp/codex-sqlite-import-', database)
            self.assertTrue(database.endswith('?mode=ro'))
            self.assertTrue(kwargs['uri'])
            opened.append(database)
            return real_connect(database, **kwargs)
        with patch.object(importer.sqlite3, 'connect', side_effect=checked_connect):
            with importer.snapshot('state_5.sqlite', self.root) as (con, digest):
                self.assertEqual(len(digest), 64)
                with self.assertRaises(sqlite3.OperationalError):
                    con.execute('DELETE FROM threads')
        self.assertEqual(len(opened), 1)
        self.assertEqual(before, self.source_signature())
        self.assertEqual(list((self.root / '.tmp').iterdir()), [])

    def test_sensitive_hashes_null_empty_and_basename(self):
        table = self.table('threads')
        row = default_values(table); row['title'] = ''; row['preview'] = None
        with sqlite3.connect(self.root / 'sqlite' / table.database) as con:
            con.execute('DELETE FROM threads'); insert(con, table, row)
        con.close()
        with importer.snapshot(table.database, self.root) as (con, digest):
            result = next(importer.iter_rows(con, table, digest))
        self.assertEqual(result['title_length_bytes'], 0)
        self.assertEqual(result['title_sha256'], hashlib.sha256(b'').hexdigest())
        self.assertIsNone(result['preview_length_bytes'])
        self.assertIsNone(result['preview_sha256'])
        self.assertEqual(result['cwd_length_bytes'], len(PRIVATE.encode()))
        self.assertEqual(result['cwd_sha256'], hashlib.sha256(PRIVATE.encode()).hexdigest())
        self.assertEqual(result['rollout_file_name'], FILE_NAME)
        for name in table.sensitive:
            self.assertNotIn(name, result)
        self.assertNotIn(PRIVATE, json.dumps(result, ensure_ascii=False))
        self.assertNotIn(PRIVATE_PATH, json.dumps(result))

    def test_large_sensitive_body_hashes_in_bounded_chunks(self):
        table = self.table('logs')
        body = (PRIVATE * 160000) + '\x00end'
        expected_hash = hashlib.sha256(body.encode()).hexdigest(); expected_length = len(body.encode())
        with sqlite3.connect(self.root / 'sqlite' / table.database) as con:
            con.execute('UPDATE logs SET feedback_log_body=?', (body,))
        con.close(); del body
        with importer.snapshot(table.database, self.root) as (con, digest):
            sizes = []
            class BlobProxy:
                def __init__(self, blob): self.blob = blob
                def __enter__(self): return self
                def __exit__(self, *exc): self.blob.close()
                def __len__(self): return len(self.blob)
                def read(self, size):
                    sizes.append(size)
                    return self.blob.read(size)
            class ConnectionProxy:
                def execute(self, *args): return con.execute(*args)
                def blobopen(self, *args, **kwargs): return BlobProxy(con.blobopen(*args, **kwargs))
            result = next(importer.iter_rows(ConnectionProxy(), table, digest))
        self.assertEqual(result['feedback_log_body_length_bytes'], expected_length)
        self.assertEqual(result['feedback_log_body_sha256'], expected_hash)
        self.assertGreater(len(sizes), 10)
        self.assertLessEqual(max(sizes), importer.CHUNK)
        self.assertNotIn('feedback_log_body', result)

    def test_wal_rows_are_included_and_fingerprint_tracks_wal(self):
        table = self.table('logs'); path = self.root / 'sqlite' / table.database
        con = sqlite3.connect(path)
        self.addCleanup(con.close)
        con.execute('PRAGMA journal_mode=WAL')
        con.execute('PRAGMA wal_autocheckpoint=0')
        con.execute('UPDATE logs SET id=2'); con.commit()
        main_before = hashlib.sha256(path.read_bytes()).hexdigest()
        bundle_before = self.source_signature()
        with importer.snapshot(table.database, self.root) as (copy, first_hash):
            self.assertEqual(copy.execute('SELECT id FROM logs').fetchone()[0], 2)
        self.assertEqual(bundle_before, self.source_signature())
        con.execute('UPDATE logs SET id=3'); con.commit()
        self.assertEqual(main_before, hashlib.sha256(path.read_bytes()).hexdigest())
        with importer.snapshot(table.database, self.root) as (copy, second_hash):
            self.assertEqual(copy.execute('SELECT id FROM logs').fetchone()[0], 3)
        self.assertNotEqual(first_hash, second_hash)
        with importer.snapshot(table.database, self.root) as (_, repeated_hash):
            self.assertEqual(second_hash, repeated_hash)

    def test_cleanup_after_failure(self):
        with self.assertRaisesRegex(importer.ImportFailure, 'synthetic_abort'):
            with importer.snapshot('logs_2.sqlite', self.root):
                raise importer.ImportFailure('synthetic_abort')
        self.assertEqual(list((self.root / '.tmp').iterdir()), [])

    def test_without_rowid_uses_stable_pk_order_and_hashes_correct_rows(self):
        table = self.table('thread_timeline_ledger')
        first = default_values(table); first['sequence'] = 2; first['payload_json'] = '{"type":"second"}'
        second = default_values(table); second['sequence'] = 1
        second['payload_json'] = json.dumps({'type':'first','text':PRIVATE * 30000})
        with sqlite3.connect(self.root / 'sqlite' / table.database) as con:
            con.execute('DELETE FROM thread_timeline_ledger')
            insert(con, table, first); insert(con, table, second)
        con.close()
        for _ in range(2):
            with importer.snapshot(table.database, self.root) as (con, digest):
                self.assertEqual(importer.validate_schema(con, table), 2)
                with self.assertRaises(sqlite3.OperationalError):
                    con.execute('SELECT rowid FROM thread_timeline_ledger')
                rows = list(importer.iter_rows(con, table, digest))
            self.assertEqual([r['source_rowid'] for r in rows], [1,2])
            self.assertEqual([r['sequence'] for r in rows], [1,2])
            self.assertEqual([r['payload_type'] for r in rows], ['first','second'])
            for result, source in zip(rows, (second,first)):
                self.assertEqual(result['payload_json_sha256'], hashlib.sha256(source['payload_json'].encode()).hexdigest())

    def test_unexpected_rowid_layout_is_rejected(self):
        table = self.table('thread_timeline_ledger')
        with sqlite3.connect(self.root / 'sqlite' / table.database) as con:
            con.execute('DROP TABLE thread_timeline_ledger')
            con.execute('CREATE TABLE thread_timeline_ledger (' + ','.join(
                importer.quoted(n)+' '+kind for n,kind in table.columns) + ')')
        con.close()
        with importer.snapshot(table.database, self.root) as (con,_):
            with self.assertRaisesRegex(importer.ImportFailure, 'identity_layout_mismatch'):
                importer.validate_schema(con, table)

    def test_changed_copy_detected_and_cleaned_up(self):
        real_signature = importer.signature; calls = 0
        def changed(path):
            nonlocal calls
            calls += 1
            result = real_signature(path)
            if calls == 4 and result:
                return result[0], result[1] + 1, result[2]
            return result
        with patch.object(importer, 'signature', side_effect=changed):
            with self.assertRaisesRegex(importer.ImportFailure, 'source_changed'):
                with importer.snapshot('logs_2.sqlite', self.root):
                    self.fail('unstable snapshot opened')
        self.assertEqual(list((self.root / '.tmp').iterdir()), [])

    def test_unknown_database_and_schema_changes_fail_closed(self):
        with self.assertRaisesRegex(importer.ImportFailure, 'not_allowlisted'):
            with importer.snapshot('goals_1.sqlite', self.root): pass
        table = self.table('logs')
        with sqlite3.connect(self.root / 'sqlite' / table.database) as con:
            con.execute('ALTER TABLE logs ADD COLUMN unknown_sensitive TEXT')
        con.close()
        with self.assertRaisesRegex(importer.ImportFailure, 'schema_mismatch'):
            importer.run_import(root=self.root)

    def test_payload_type_only_and_blob_checksum_omission(self):
        table = self.table('thread_timeline_ledger')
        with importer.snapshot(table.database, self.root) as (con, digest):
            result = next(importer.iter_rows(con, table, digest))
        self.assertEqual(result['payload_type'], 'transcript-segment')
        self.assertNotIn('payload_json', result)
        table = self.table('_sqlx_migrations')
        with importer.snapshot(table.database, self.root) as (con, digest):
            result = next(importer.iter_rows(con, table, digest))
        self.assertNotIn('checksum', result)
        self.assertIs(result['success'], True)
        table = self.table('codex_schema_migrations')
        with importer.snapshot(table.database, self.root) as (con, digest):
            result = next(importer.iter_rows(con, table, digest))
        self.assertEqual(result['checksum'], 'fixture')

    def test_invalid_payload_is_preserved_as_hash_with_unknown_type(self):
        table = self.table('thread_timeline_ledger')
        for body in ('{broken', '{"type": null}', '{"type": "private sentence here"}'):
            with sqlite3.connect(self.root / 'sqlite' / table.database) as con:
                con.execute('UPDATE thread_timeline_ledger SET payload_json=?', (body,))
            con.close()
            with importer.snapshot(table.database, self.root) as (con, digest):
                row = next(importer.iter_rows(con, table, digest))
            self.assertIsNone(row['payload_type'])
            self.assertEqual(row['payload_json_sha256'], hashlib.sha256(body.encode()).hexdigest())

    def test_apply_streams_only_redacted_rows_and_finishes_after_all_tables(self):
        class Sink:
            def __init__(self): self.parts = []; self.finished = False
            def write(self, text): self.parts.append(text)
            def finish(self): self.finished = True; return []
        sink = Sink()
        importer.run_import(True, self.root, sink)
        stream = ''.join(sink.parts)
        self.assertTrue(sink.finished)
        self.assertTrue(stream.endswith('COMMIT;\n'))
        self.assertEqual(stream.count('COPY sqlite_import_stage'), 9)
        self.assertEqual(stream.count('ON CONFLICT (source_database, source_rowid) DO NOTHING'), 9)
        self.assertNotIn(PRIVATE, stream)
        self.assertNotIn(PRIVATE_PATH, stream)
        # Hash/metadata only for sensitive columns; no raw bodies in the psql stream.
        self.assertEqual(stream.count('snapshot_or_value_mismatch'), 9)

    def test_failure_before_finish_never_sends_commit(self):
        table = self.table('logs')
        with sqlite3.connect(self.root / 'sqlite' / table.database) as con:
            con.execute('UPDATE logs SET level=?', ('bad\x00text',))
        con.close()
        class Sink:
            def __init__(self): self.parts = []
            def write(self, text): self.parts.append(text)
            def finish(self): raise AssertionError('must not finish')
        sink = Sink()
        with self.assertRaises(importer.ImportFailure):
            importer.run_import(True, self.root, sink)
        self.assertNotIn('COMMIT;\n', sink.parts)

    def test_sandbox_local_path_is_absent_from_psql_stream(self):
        table = self.table('threads')
        path = 'C:\\SYNTHETIC_SANDBOX_ONLY\\private-project'
        body = json.dumps({'type':'workspace-write', 'writable_roots':[path]})
        with sqlite3.connect(self.root / 'sqlite' / table.database) as con:
            con.execute('UPDATE threads SET sandbox_policy=?', (body,))
        con.close()
        with importer.snapshot(table.database, self.root) as (con, digest):
            row = next(importer.iter_rows(con, table, digest))
        self.assertEqual(row['sandbox_policy_type'], 'workspace-write')
        self.assertEqual(row['sandbox_policy_length_bytes'], len(body.encode()))
        self.assertEqual(row['sandbox_policy_sha256'], hashlib.sha256(body.encode()).hexdigest())
        self.assertNotIn('sandbox_policy', row)
        stream = self.capture_import_stream()
        self.assertNotIn('SYNTHETIC_SANDBOX_ONLY', stream)
        self.assertNotIn(path, stream)
        self.assertNotIn(importer.copy_value(body), stream)
        self.assertIn(row['sandbox_policy_sha256'], stream)

    def test_account_and_user_ids_are_absent_from_both_psql_streams(self):
        host_id = 'chatgpt:SYNTHETIC_ACCOUNT_PRIVATE:SYNTHETIC_USER_PRIVATE'
        expected = hashlib.sha256(host_id.encode()).hexdigest()
        for name in ('local_thread_catalog', 'thread_timeline_ledger'):
            table = self.table(name)
            with sqlite3.connect(self.root / 'sqlite' / table.database) as con:
                con.execute(f'UPDATE {name} SET host_id=?', (host_id,))
            con.close()
            with importer.snapshot(table.database, self.root) as (con, digest):
                row = next(importer.iter_rows(con, table, digest))
            self.assertEqual(row['host_kind'], 'chatgpt')
            self.assertEqual(row['host_id_sha256'], expected)
            self.assertNotIn('host_id', row)
            self.assertNotIn('host_id_length_bytes', row)
        stream = self.capture_import_stream()
        self.assertNotIn('SYNTHETIC_ACCOUNT_PRIVATE', stream)
        self.assertNotIn('SYNTHETIC_USER_PRIVATE', stream)
        self.assertEqual(stream.count(expected), 2)

    def test_sandbox_type_rejects_nonclassifications_and_preserves_null_empty(self):
        table = self.table('threads')
        cases = [(None, None), ('', None), ('{broken', None), ('[]', None),
                 ('{"type":null}', None), ('{"type":1}', None),
                 ('{"type":{}}', None), ('{"type":"/synthetic/private"}', None),
                 ('{"type":"C:private"}', None), ('{"type":"two words"}', None),
                 (json.dumps({'type':'x'*65}), None),
                 ('{"type":"read-only"}', 'read-only'),
                 ('{"type":"danger-full-access"}', 'danger-full-access')]
        for body, expected_type in cases:
            with sqlite3.connect(self.root / 'sqlite' / table.database) as con:
                con.execute('UPDATE threads SET sandbox_policy=?', (body,))
            con.close()
            with importer.snapshot(table.database, self.root) as (con, digest):
                row = next(importer.iter_rows(con, table, digest))
            self.assertEqual(row['sandbox_policy_type'], expected_type)
            self.assertEqual(row['sandbox_policy_length_bytes'], None if body is None else len(body.encode()))
            self.assertEqual(row['sandbox_policy_sha256'], None if body is None else hashlib.sha256(body.encode()).hexdigest())

    def test_host_kind_first_segment_only_and_hash_null_empty_unknown(self):
        cases = [(None,None), ('',None), ('chatgpt:account:user','chatgpt'),
                 ('local:host:rest','local'), ('local','local'), ('chatgpt','chatgpt'),
                 ('ChatGPT:account',None), ('remote:local',None), (':chatgpt',None),
                 ('chatgpt-other:account',None), ('local/path:account',None)]
        for name in ('local_thread_catalog', 'thread_timeline_ledger'):
            table = self.table(name)
            for host_id, expected_kind in cases:
                if host_id is None and table.ordered_key:
                    continue  # WITHOUT ROWID primary-key columns cannot contain NULL.
                with contextlib.closing(sqlite3.connect(self.root / 'sqlite' / table.database)) as con:
                    with con:
                        con.execute(f'UPDATE {name} SET host_id=?', (host_id,))
                with importer.snapshot(table.database, self.root) as (con, digest):
                    row = next(importer.iter_rows(con, table, digest))
                self.assertEqual(row['host_kind'], expected_kind)
                self.assertEqual(row['host_id_sha256'], None if host_id is None else hashlib.sha256(host_id.encode()).hexdigest())
                self.assertNotIn('host_id', row)
                self.assertNotIn('host_id_length_bytes', row)


class ContractTests(unittest.TestCase):
    def test_migration_matches_contract_and_has_no_006_or_unrequested_tables(self):
        path = importer.ROOT / 'database/migrations/005_import_codex_sqlite_metadata.sql'
        self.assertEqual(path.read_text(encoding='utf-8'), importer.render_migration())
        sql = importer.render_migration()
        self.assertEqual(sql.count('CREATE TABLE public.'), 9)
        self.assertEqual(sql.count('PRIMARY KEY (source_database, source_rowid)'), 9)
        self.assertNotIn('remote_control_enrollments', sql)
        self.assertNotIn('logs2_request_observations', sql)
        self.assertNotIn('"sandbox_policy" text', sql)
        self.assertNotIn('"host_id" text', sql)
        self.assertNotIn('host_id_length_bytes', sql)
        self.assertEqual(sql.count('"host_kind" text'), 2)
        self.assertEqual(sql.count('"host_id_sha256" text'), 2)
        self.assertIn('"sandbox_policy_type" text', sql)
        self.assertIn('"sandbox_policy_length_bytes" bigint', sql)
        self.assertIn('"sandbox_policy_sha256" text', sql)

    def test_uuid_join_is_exact_unique_child_owned_and_job_scoped(self):
        sql = importer.transaction_sql()
        self.assertIn('count(DISTINCT id)=1', sql)
        self.assertIn('lower(session_id)', sql)
        self.assertIn('rs.file_name', sql)
        self.assertNotIn('source_relative_path', sql)
        tables = {t.name:t for t in importer.TABLES}
        self.assertIn('lower(s."child_thread_id")=m.uuid', importer.finish_table_sql(tables['thread_spawn_edges'], 1))
        self.assertIn("s.kind='memory_stage1'", importer.finish_table_sql(tables['jobs'], 1))
        self.assertNotIn('UPDATE sqlite_import_stage', importer.finish_table_sql(tables['_sqlx_migrations'], 1))

    def test_copy_escapes_sql_like_input_newlines_null_and_backslash(self):
        self.assertEqual(importer.copy_value(None), r'\N')
        self.assertEqual(importer.copy_value('\\N'), r'\\N')
        self.assertEqual(importer.copy_value('\n\\.\nDROP TABLE x;'), r'\n\\.\nDROP TABLE x;')
        self.assertEqual(importer.copy_value('a\tb\rc'), r'a\tb\rc')
        with self.assertRaises(importer.ImportFailure): importer.copy_value('\0')

    def test_invalid_types_are_not_coerced_silently(self):
        for value, kind in [('2','INTEGER'), (2,'BOOLEAN'), (float('inf'),'REAL'), (b'abc','TEXT')]:
            with self.assertRaises(importer.ImportFailure): importer.validate_scalar(value, kind)

    def test_psql_result_is_counts_only_and_failure_is_sanitized(self):
        class Process:
            def __init__(self, output, code=0):
                self.stdin = io.StringIO(); self.output = output; self.returncode = code
            def communicate(self, **kwargs): return self.output, None
        rows = [{'table':t.target, 'source_rows':1, 'destination_rows':1,
                 'inserted_rows':0, 'matched_session_rows':0} for t in importer.TABLES]
        stream = importer.PsqlStream()
        stream.process = Process('\n'.join(json.dumps(r) for r in rows))
        self.assertEqual(stream.finish(), rows)
        rows[0]['unexpected_source_value'] = PRIVATE
        stream.process = Process('\n'.join(json.dumps(r) for r in rows))
        with self.assertRaisesRegex(importer.ImportFailure, 'invalid_postgres_verification_output'):
            stream.finish()
        stream.process = Process(PRIVATE, code=1)
        with self.assertRaisesRegex(importer.ImportFailure, '^postgres_transaction_failed$'):
            stream.finish()

    def test_main_plan_no_pg_and_errors_do_not_expose_data(self):
        output = io.StringIO()
        with patch.object(importer, 'run_import', return_value=[{'table':'logs2_logs','source_rows':2}]), \
             patch.object(importer, 'PsqlStream') as pg, contextlib.redirect_stdout(output):
            self.assertEqual(importer.main([]), 0)
        pg.assert_not_called()
        self.assertFalse(json.loads(output.getvalue())['database_written'])
        output = io.StringIO()
        with patch.object(importer, 'run_import', side_effect=RuntimeError(PRIVATE_PATH + PRIVATE)), \
             contextlib.redirect_stdout(output):
            self.assertEqual(importer.main([]), 1)
        self.assertNotIn(PRIVATE, output.getvalue())
        self.assertEqual(json.loads(output.getvalue())['error_code'], 'operation_failed')


if __name__ == '__main__':
    started = time.perf_counter()
    importer.lower_priority()
    suite = unittest.defaultTestLoader.loadTestsFromModule(__import__(__name__))
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    print(json.dumps({'synthetic_only':True, 'tests_run':result.testsRun,
                      'failures':len(result.failures), 'errors':len(result.errors),
                      'elapsed_seconds':round(time.perf_counter()-started,3),
                      'peak_working_set_bytes':importer.peak_memory()}))
    raise SystemExit(0 if result.wasSuccessful() else 1)
