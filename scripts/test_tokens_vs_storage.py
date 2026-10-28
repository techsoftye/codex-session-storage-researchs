"""Small synthetic checks; no database or research inputs."""
import math
import unittest
from analyze_tokens_vs_storage import average_ranks,spearman,match_class,uncached,size_bin,token_bin


class StatisticsTests(unittest.TestCase):
    def test_average_tied_ranks(self):
        self.assertEqual(average_ranks([3,1,1,2]),[4,1.5,1.5,3])

    def test_perfect_and_reverse(self):
        self.assertAlmostEqual(spearman([(1,3),(2,5),(3,9)])['rho'],1)
        self.assertAlmostEqual(spearman([(1,9),(2,5),(3,3)])['rho'],-1)

    def test_tied_spearman_hand_computed(self):
        # Ranks: [1.5,1.5,3] versus [1,2.5,2.5]; centered cross-product = .75.
        self.assertAlmostEqual(spearman([(1,1),(1,2),(2,2)])['rho'],0.5)

    def test_constant_and_missing(self):
        self.assertIsNone(spearman([(1,2),(1,3)])['rho'])
        self.assertEqual(spearman([(None,1),(1,2)])['n'],1)
        self.assertIsNone(spearman([(None,1),(1,2)])['rho'])

    def test_match_threshold_and_zero(self):
        self.assertEqual(match_class(0,0),'exact')
        self.assertEqual(match_class(10000,9900),'near')
        self.assertEqual(match_class(10000,9899),'different')
        self.assertEqual(match_class(0,1),'different')
        self.assertEqual(match_class(None,0),'missing_or_invalid')

    def test_uncached_preserves_missing(self):
        self.assertEqual(uncached(100,90),10)
        self.assertIsNone(uncached(100,None))
        self.assertIsNone(uncached(90,100))

    def test_bands(self):
        self.assertEqual(size_bin(1048575),0)
        self.assertEqual(size_bin(1048576),1)
        self.assertEqual(size_bin(1024*1048576),4)
        self.assertEqual([token_bin(x) for x in (0,1,9999,10000,10000000,None)],[0,1,1,2,5,6])


if __name__=='__main__':unittest.main()
