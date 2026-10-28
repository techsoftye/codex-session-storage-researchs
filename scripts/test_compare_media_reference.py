import base64
import hashlib
import unittest

from compare_media_reference import MIN_B64, REF_PREFIX, MediaReferencer


def run(data, size):
    rewriter = MediaReferencer()
    out = b''.join(rewriter.feed(data[i:i + size]) for i in range(0, len(data), size))
    return out + rewriter.close(), rewriter


class MediaReferencerTest(unittest.TestCase):
    def setUp(self):
        self.img_a = base64.b64encode(bytes(range(256)) * 3)       # no padding
        self.img_b = base64.b64encode(b'x' * 301)                   # '==' padding
        self.url_a = b'data:image/png;base64,' + self.img_a
        self.url_b = b'data:image/jpeg;base64,' + self.img_b
        self.data = (b'{"a":"' + self.url_a + b'"}\n{"b":"' + self.url_b + b'","c":"'
                     + self.url_a + b'"}\n{"t":"data:image/png;base64,${x}"}\n')
        ref = lambda b64: REF_PREFIX + hashlib.sha256(b64).hexdigest().encode()
        self.expected = (b'{"a":"' + ref(self.img_a) + b'"}\n{"b":"' + ref(self.img_b)
                         + b'","c":"' + ref(self.img_a)
                         + b'"}\n{"t":"data:image/png;base64,${x}"}\n')

    def test_rewrite_is_independent_of_chunk_boundaries(self):
        for size in (1, 2, 3, 7, 13, 64, 1 << 20):
            out, rewriter = run(self.data, size)
            self.assertEqual(out, self.expected, size)
            self.assertEqual(rewriter.occurrences, 3)
            self.assertEqual(len(rewriter.images), 2)
            self.assertEqual(sum(rewriter.images.values()), 768 + 301)
            self.assertEqual(rewriter.short_left, 1)

    def test_short_payload_is_left_unchanged(self):
        data = b'"data:image/png;base64,' + b'A' * (MIN_B64 - 4) + b'"'
        for size in (1, 5, 1 << 20):
            out, rewriter = run(data, size)
            self.assertEqual(out, data)
            self.assertEqual(rewriter.occurrences, 0)

    def test_text_without_images_passes_through(self):
        data = b'plain data:image/ text;base64, and more ' * 50
        for size in (1, 9, 1 << 20):
            self.assertEqual(run(data, size)[0], data)


if __name__ == '__main__':
    unittest.main()
