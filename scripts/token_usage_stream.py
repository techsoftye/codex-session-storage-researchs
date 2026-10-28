"""Bounded JSON traversal. Retain only token counters and identifier provenance.

Objects are discovered structurally, never by searching strings for token names.
Selected objects/ancestor identifiers spool to disk so keys following a usage
block resolve correctly without retaining its enclosing conversation/history.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import re
import sqlite3

from profile_compacted import Stream, CHUNK

VERSION = '1.0.0'
COUNTERS = ('input_tokens', 'cached_input_tokens', 'cache_write_input_tokens',
            'output_tokens', 'reasoning_output_tokens', 'total_tokens')
IDENTIFIERS = ('response_id', 'turn_id', 'root_turn_id', 'thread_id', 'session_id')
SAFE_KEY = re.compile(r'[A-Za-z_][A-Za-z0-9_.:-]{0,255}\Z')
SAFE_ID = re.compile(r'[A-Za-z0-9_.:-]{1,256}\Z')
BIGINT_MIN, BIGINT_MAX = -(2**63), 2**63-1


def encoded(value):
    return json.dumps(value, ensure_ascii=True, separators=(',', ':'), allow_nan=False)


class KeyHash:
    def __init__(self):
        self.digest = hashlib.sha256()

    def feed(self, data):
        self.digest.update(data)

    def finish(self, length):
        pass


@dataclass
class Frame:
    offset: int
    parent: int | None
    path: str
    needed: bool = False
    ids: dict = field(default_factory=dict)
    counters: dict = field(default_factory=dict)
    types: dict = field(default_factory=dict)
    issues: list = field(default_factory=list)
    invalid_numbers: dict = field(default_factory=dict)


class TokenReader:
    def __init__(self, spool_path):
        self.db = sqlite3.connect(spool_path)
        self.db.execute('PRAGMA journal_mode=OFF')
        self.db.execute('PRAGMA synchronous=OFF')
        self.db.execute('PRAGMA cache_size=-2048')
        self.db.execute('PRAGMA temp_store=FILE')
        self.db.execute('PRAGMA mmap_size=0')
        self.db.executescript('''
CREATE TABLE IF NOT EXISTS frames (offset INTEGER PRIMARY KEY, parent INTEGER,
    path TEXT NOT NULL, ids TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS candidates (offset INTEGER PRIMARY KEY, length INTEGER,
    path TEXT NOT NULL, counters TEXT NOT NULL, metadata TEXT NOT NULL);
''')
        self.stack = []

    def close(self):
        self.db.close()

    def parse(self, source, size, chunk=CHUNK):
        self.db.execute('DELETE FROM frames')
        self.db.execute('DELETE FROM candidates')
        self.stream = Stream(source, size, chunk)
        self.stack = []
        self.root_type = None
        self.redacted_keys = 0
        kind, _ = self.value('', 0)
        self.stream.whitespace()
        if kind != 'object' or self.stream.peek() is not None or self.stream.position != size:
            raise ValueError('invalid_record_envelope_or_trailing_content')
        return self.db.execute('SELECT count(*) FROM candidates').fetchone()[0]

    def value(self, path, depth):
        if depth > 128:
            raise ValueError('json_depth_limit_128')
        s = self.stream
        s.whitespace()
        start = s.position
        char = s.peek()
        if char == 123:
            frame = Frame(start, self.stack[-1].offset if self.stack else None, path)
            self.stack.append(frame)
            s.take(); s.whitespace()
            seen = set()
            if s.peek() != 125:
                while True:
                    digest = KeyHash()
                    key, _, _ = s.string(digest)
                    identity = (key if key is not None else digest.digest.hexdigest())
                    if identity in seen:
                        raise ValueError('duplicate_json_object_key')
                    seen.add(identity)
                    if len(seen) > 20000:
                        raise ValueError('json_object_key_limit_20000')
                    if key is not None and SAFE_KEY.fullmatch(key):
                        segment = key
                    else:
                        segment = '{key_sha256=' + digest.digest.hexdigest() + '}'
                        self.redacted_keys += 1
                    child_path = path + '/' + segment
                    s.whitespace(); s.expect(58)
                    kind, value = self.value(child_path, depth+1)
                    if not path and key == 'type':
                        self.root_type = value
                    if key in COUNTERS:
                        frame.types[key] = kind
                        frame.counters[key] = None
                        if kind == 'integer' and BIGINT_MIN <= value <= BIGINT_MAX:
                            frame.counters[key] = value
                            if value < 0:
                                frame.issues.append(key + ':negative')
                        elif kind != 'null':
                            frame.issues.append(key + ':not_bigint')
                            if kind in ('integer', 'number'):
                                frame.invalid_numbers[key] = str(value)
                    if key in IDENTIFIERS:
                        valid = kind == 'string' and value is not None and SAFE_ID.fullmatch(value)
                        frame.ids[key] = {'value': value if valid else None, 'type': kind}
                        if kind != 'null' and not valid:
                            frame.ids[key]['issue'] = 'invalid_or_oversized_identifier'
                    s.whitespace()
                    delimiter = s.take()
                    if delimiter == 125:
                        break
                    if delimiter != 44:
                        raise ValueError('invalid_object_separator')
                    s.whitespace()
            else:
                s.take()
            if frame.counters:
                for ancestor in self.stack:
                    ancestor.needed = True
                values = frame.counters
                if all(type(values.get(k)) is int for k in ('input_tokens','output_tokens','total_tokens')):
                    if values['input_tokens'] + values['output_tokens'] != values['total_tokens']:
                        frame.issues.append('input_plus_output_differs_from_total')
                for part, whole in (('cached_input_tokens','input_tokens'),
                                    ('reasoning_output_tokens','output_tokens')):
                    if type(values.get(part)) is int and type(values.get(whole)) is int and values[part] > values[whole]:
                        frame.issues.append(part + ':exceeds_' + whole)
                metadata = {'extractor_version': VERSION, 'field_types': frame.types,
                            'issues': frame.issues, 'invalid_numeric_values': frame.invalid_numbers}
                self.db.execute('INSERT INTO candidates VALUES (?,?,?,?,?)',
                                (start, s.position-start, path, encoded(values), encoded(metadata)))
            if frame.needed:
                self.db.execute('INSERT INTO frames VALUES (?,?,?,?)',
                                (start, frame.parent, path, encoded(frame.ids)))
            self.stack.pop()
            return 'object', None
        if char == 91:
            s.take(); s.whitespace()
            index = 0
            if s.peek() != 93:
                while True:
                    self.value(path + '/' + str(index), depth+1)
                    index += 1
                    s.whitespace()
                    delimiter = s.take()
                    if delimiter == 93:
                        break
                    if delimiter != 44:
                        raise ValueError('invalid_array_separator')
                    s.whitespace()
            else:
                s.take()
            return 'array', None
        if char == 34:
            value, _, _ = s.string()
            return 'string', value
        kind, value = s.primitive()
        return ('integer' if type(value) is int else kind), value

    def observations(self, record_id, record_offset):
        for offset, length, path, counters, meta in self.db.execute(
                'SELECT offset,length,path,counters,metadata FROM candidates ORDER BY offset'):
            counters, metadata = json.loads(counters), json.loads(meta)
            identifiers, provenance = {}, {}
            current = offset
            while current is not None:
                row = self.db.execute('SELECT parent,path,ids FROM frames WHERE offset=?', (current,)).fetchone()
                if row is None:
                    raise RuntimeError('missing_identifier_context')
                parent, context_path, ids = row
                for name, detail in json.loads(ids).items():
                    if name not in identifiers:
                        identifiers[name] = detail['value']
                        provenance[name] = {'path': context_path + '/' + name, 'type': detail['type']}
                        if 'issue' in detail:
                            metadata['issues'].append(name + ':' + detail['issue'])
                current = parent
            leaf = path.rsplit('/', 1)[-1]
            scope = {'turn_token_usage': 'turn', 'thread_token_usage': 'thread',
                     'last_token_usage': 'last', 'total_token_usage': 'cumulative'}.get(leaf, 'unknown')
            if leaf == 'usage' and (self.root_type == 'token_usage_record' or '/latest_token_usage_record/' in path):
                scope = 'response'
            metadata['identifier_sources'] = provenance
            metadata['scope_basis'] = 'field_name' if scope != 'unknown' else 'unclassified'
            metadata['path_has_redacted_key'] = '{key_sha256=' in path
            yield {'research_session_record_id': record_id, 'source_json_path': path,
                   'source_object_byte_offset': record_offset+offset, 'source_object_bytes': length,
                   'usage_scope': scope, **{key: identifiers.get(key) for key in IDENTIFIERS},
                   **{key: counters.get(key) for key in COUNTERS}, 'metadata': metadata}
