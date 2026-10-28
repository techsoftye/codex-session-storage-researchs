"""Synthetic tests: no connection and no research data read."""
import unittest
from fractions import Fraction
from analyze_compaction_growth import Images, growth, describe
from analyze_length_control import spearman

A = b'a'*32
B = b'b'*32


class GrowthTests(unittest.TestCase):
    def records(self, sizes):
        return [dict(source_line_number=i*10, source_record_bytes=b) for i,b in enumerate(sizes,1)]

    def test_sizes_can_fall_and_tie(self):
        r = growth(self.records([100,200,200,50]))
        self.assertEqual((r['increases'],r['decreases'],r['equal_transitions']), (1,1,1))
        self.assertEqual(r['sum_div_last_bytes'],11)
        self.assertFalse(r['nondecreasing'])

    def test_single_has_no_growth_evidence(self):
        r = growth(self.records([50]))
        self.assertIsNone(r['strictly_increasing'])
        self.assertIsNone(r['nondecreasing'])
        self.assertEqual(r['sum_div_last_bytes'],1)

    def test_invalid_sizes_and_order(self):
        for sizes in ([],[0],[-1]):
            with self.assertRaises(ValueError): growth(self.records(sizes))
        with self.assertRaises(ValueError): growth(list(reversed(self.records([1,2]))))

    def test_monotonic_and_exact_rank_ties(self):
        self.assertTrue(growth(self.records([1,2,3]))['strictly_increasing'])
        self.assertTrue(growth(self.records([1,1,3]))['nondecreasing'])
        self.assertAlmostEqual(spearman([1,1,2],[Fraction(1,2),Fraction(2,4),Fraction(3,4)]),1)

    def test_adjacency_and_disjoint_partition(self):
        images = Images()
        images.add([(A,10,20),(A,10,21),(B,12,25)])
        second = images.add([(A,10,22),(A,10,23),(B,12,26)])
        self.assertEqual(second['adjacent_image_hash_reappearances'],2)
        self.assertEqual(second['adjacent_repeated_image_occurrences'],3)
        self.assertEqual(second['adjacent_repeated_image_stored_bytes'],71)
        s = images.summary(1000)
        self.assertEqual(s['unique_images'],2)
        self.assertEqual(s['duplicate_image_stored_bytes'],92)
        self.assertEqual(s['image_duplicate_partition']['within_record_extra_copy']['stored_bytes'],44)
        self.assertEqual(s['image_duplicate_partition']['adjacent_record_first_copy_repeat']['stored_bytes'],48)
        self.assertEqual(sum(p['stored_bytes'] for p in s['image_duplicate_partition'].values()),137)

    def test_empty_and_missing_image_break_adjacency(self):
        images = Images()
        images.add([(A,10,20)])
        images.add([])
        self.assertEqual(images.add([(A,10,30)])['adjacent_image_hash_reappearances'],0)
        images.add([(B,12,40)])
        images.add([(A,10,50)])
        s = images.summary(1000)
        self.assertEqual(s['image_duplicate_partition']['nonadjacent_record_first_copy_repeat']['occurrences'],2)
        self.assertEqual(s['recurrent_unique_images'],1)

    def test_hash_length_conflict_rejected(self):
        images = Images()
        images.add([(A,10,20)])
        with self.assertRaises(ValueError): images.add([(A,11,20)])

    def test_no_images_and_quantiles(self):
        images = Images()
        images.add([])
        self.assertEqual(images.summary(100)['unique_images'],0)
        self.assertEqual(describe([1,2,3,4])['p90'],3.7)
        self.assertIsNone(describe([])['median'])


if __name__=='__main__':
    unittest.main()
