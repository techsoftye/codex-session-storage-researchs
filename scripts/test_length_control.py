from fractions import Fraction
import math
import unittest
from analyze_length_control import average_ranks,spearman,residuals,partial_spearman,stratum_bounds,normalize,group_summary


class LengthControlTests(unittest.TestCase):
    def test_equivalent_ratios_tie_exactly(self):
        self.assertEqual(average_ranks([Fraction(1,3),Fraction(2,6),Fraction(1,2)]),[1.5,1.5,3])
        self.assertAlmostEqual(spearman([Fraction(1,3),Fraction(2,6),Fraction(1,2)],[1,2,2]),0.5)

    def test_residuals_orthogonal_to_intercept_and_control(self):
        r=residuals([1,3,2,5,4],[1,2,3,4,5])
        self.assertAlmostEqual(r['residual_sum'],0)
        self.assertAlmostEqual(r['residual_dot_centered_control'],0)

    def test_partial_hand_computed_ranks(self):
        p=partial_spearman([1,2,3,4],[1,3,2,4],[1,4,2,3])
        self.assertAlmostEqual(p['rho'],0.48/math.sqrt(0.84*0.36))

    def test_partial_undefined_cases(self):
        self.assertIsNone(partial_spearman([1,2,3],[3,1,2],[1,1,1])['rho'])
        self.assertIsNone(partial_spearman([1,2,3],[3,1,2],[1,2,3])['rho'])
        self.assertIsNone(partial_spearman([1,2],[2,1],[1,2])['rho'])

    def test_stratum_boundaries(self):
        self.assertEqual(stratum_bounds(127),(64,128))
        self.assertEqual(stratum_bounds(128),(128,256))
        with self.assertRaises(ValueError):stratum_bounds(0)

    def test_normalization_and_invalid_denominator(self):
        row={'tokens_used':10,'file_size_bytes':100,'record_count':3,'compacted_bytes':40,'compacted_record_count':1}
        n=normalize(row)
        self.assertEqual(n['tokens_per_record'],Fraction(10,3))
        self.assertEqual(n['compacted_byte_fraction'],Fraction(2,5))
        self.assertEqual(n['compacted_per_1000_records'],Fraction(1000,3))
        with self.assertRaises(ValueError):normalize({**row,'record_count':0})

    def test_absent_group_is_missing_not_zero(self):
        self.assertIsNone(group_summary([])['median_tokens_per_record'])


if __name__=='__main__':unittest.main()
