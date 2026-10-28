"""Synthetic transition tests; no research sources or databases opened."""
import unittest
from analyze_compaction_text_growth import collapse,compare,dominant

A=b'a'*32
B=b'b'*32
C=b'c'*32


class TransitionTests(unittest.TestCase):
    def test_first_record_is_not_transition(self):
        r=compare(None,collapse([(A,10,20)]))
        self.assertEqual(r['adjacent_repeated_stored_bytes'],0)
        self.assertIsNone(r['disappeared_stored_bytes'])
        self.assertIsNone(r['net_stored_bytes_decrease'])

    def test_whole_hash_disappearance_counts_all_previous_copies(self):
        p=collapse([(A,10,20),(A,10,21),(B,11,30)])
        c=collapse([(B,11,31)])
        r=compare(p,c)
        self.assertEqual((r['disappeared_unique_blocks'],r['disappeared_occurrences'],r['disappeared_stored_bytes']),(1,2,41))
        self.assertEqual(r['adjacent_repeated_stored_bytes'],31)
        self.assertEqual(r['shared_stored_bytes_decrease'],-1)
        self.assertEqual(r['net_stored_bytes_decrease'],40)

    def test_fewer_copies_does_not_mean_disappearance(self):
        p=collapse([(A,10,20),(A,10,20),(A,10,20)])
        c=collapse([(A,10,20)])
        r=compare(p,c)
        self.assertEqual(r['disappeared_unique_blocks'],0)
        self.assertEqual(r['shared_copy_count_reduction'],2)
        self.assertEqual(r['net_stored_bytes_decrease'],40)

    def test_new_content_offsets_disappearance(self):
        r=compare(collapse([(A,10,100)]),collapse([(B,10,150)]))
        self.assertEqual(r['disappeared_stored_bytes'],100)
        self.assertEqual(r['appeared_stored_bytes'],150)
        self.assertEqual(r['net_stored_bytes_decrease'],-50)

    def test_empty_record_breaks_recurrence(self):
        a=collapse([(A,10,20)])
        self.assertEqual(compare(a,{})['disappeared_unique_blocks'],1)
        self.assertEqual(compare({},a)['adjacent_hash_matches'],0)
        self.assertEqual(compare({},a)['appeared_unique_blocks'],1)

    def test_representation_can_change_without_copy_change(self):
        r=compare(collapse([(A,10,30)]),collapse([(A,10,20)]))
        self.assertEqual(r['shared_copy_count_reduction'],0)
        self.assertEqual(r['shared_stored_bytes_decrease'],10)
        self.assertEqual(r['disappeared_unique_blocks'],0)

    def test_conflicting_lengths_rejected(self):
        with self.assertRaises(ValueError):collapse([(A,10,20),(A,11,20)])

    def test_dominance_includes_other_and_ties(self):
        self.assertEqual(dominant(dict(image=-10,text=5,other=20)),'other')
        self.assertEqual(dominant(dict(image=10,text=10,other=-2)),'tie')
        self.assertEqual(dominant(dict(image=None,text=10,other=2)),'unresolved')


if __name__=='__main__':unittest.main()
