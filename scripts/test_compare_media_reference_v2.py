import base64
import hashlib
import json
import re
import unittest

from compare_media_reference import MediaReferencer
from compare_media_reference_v2 import ItemDeduper


def run(data, size):
    store = []
    rewriter = MediaReferencer(ref_style='index')
    deduper = ItemDeduper(store.append)
    out = []
    for i in range(0, len(data), size):
        out.append(deduper.feed(rewriter.feed(data[i:i + size])))
    out.append(deduper.feed(rewriter.close()))
    out.append(deduper.close())
    return b''.join(out), store, rewriter, deduper


def restore(main, store, rewriter, urls):
    items = [s[:-1] for s in store]
    data = re.sub(rb'"@@item:(\d+)"', lambda m: items[int(m.group(1))], main)
    by_index = {rewriter.order[h]: urls[h] for h in rewriter.order}
    return re.sub(rb'@@media:(\d+)', lambda m: by_index[int(m.group(1))], data)


def line(obj):
    return json.dumps(obj, separators=(',', ':'), ensure_ascii=False).encode() + b'\n'


class ItemDeduperTest(unittest.TestCase):
    def setUp(self):
        b64_a = base64.b64encode(bytes(range(256)) * 2).decode()
        b64_b = base64.b64encode(b'y' * 200).decode()
        self.urls = {}
        for mime, b64 in (('png', b64_a), ('jpeg', b64_b)):
            self.urls[hashlib.sha256(b64.encode()).hexdigest()] = f'data:image/{mime};base64,{b64}'.encode()
        url_a, url_b = (u.decode() for u in self.urls.values())
        user = {'type': 'message', 'role': 'user',
                'content': [{'type': 'input_text', 'text': 'quote " brace } [x] \\ é'},
                            {'type': 'input_image', 'image_url': url_a}]}
        dev = {'type': 'message', 'role': 'developer', 'content': [{'type': 'input_text', 'text': 'rules'}]}
        new = {'type': 'message', 'role': 'user', 'content': [{'type': 'input_image', 'image_url': url_b}]}
        summary = {'type': 'compaction', 'encrypted_content': 'abc'}
        self.data = b''.join([
            line({'timestamp': 't0', 'type': 'session_meta', 'payload': {'replacement_history': [1, 2]}}),
            line({'timestamp': 't1', 'type': 'response_item', 'payload': user}),
            line({'timestamp': 't2', 'type': 'compacted', 'payload': {
                'message': '', 'replacement_history': [dev, user, summary],
                'nested': {'replacement_history': [dev]}}}),
            line({'ordinal': 7, 'timestamp': 't3', 'type': 'compacted', 'payload': {
                'replacement_history': [dev, user, new, summary], 'guardian_history': [user],
                'window_number': 2}}),
            b'\n',
            line({'timestamp': 't4', 'type': 'event_msg', 'payload': {'type': 'x'}})[:-1],
        ])

    def test_round_trip_and_counts_for_any_chunk_size(self):
        for size in (1, 2, 5, 17, 100, 4096, 1 << 20):
            main, store, rewriter, deduper = run(self.data, size)
            self.assertEqual(restore(main, store, rewriter, self.urls), self.data, size)
            self.assertEqual(deduper.compacted_lines, 2)
            self.assertEqual(deduper.refs, 8)                 # 3 + 4 + 1 guardian; nested array untouched
            self.assertEqual(len(deduper.items), 4)           # dev, user, summary, new
            self.assertEqual(deduper.images_in_items, 4)
            self.assertEqual(rewriter.occurrences, 5)
            self.assertEqual(len(rewriter.images), 2)
            self.assertEqual(deduper.truncated_final_line, 0)
            self.assertIn(b'"nested":{"replacement_history":[{', main)

    def test_truncated_compacted_line_is_reported_not_dropped(self):
        data = self.data.split(b'\n')[2][:60]
        main, store, rewriter, deduper = run(data, 7)
        self.assertEqual(deduper.truncated_final_line, 1)
        self.assertEqual(restore(main, store, rewriter, self.urls), data)

    def test_missing_type_raises(self):
        with self.assertRaises(RuntimeError):
            run(b'{"a":"' + b'x' * 600 + b'"}\n', 64)


if __name__ == '__main__':
    unittest.main()
