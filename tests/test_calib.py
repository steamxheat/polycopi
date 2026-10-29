"""scripts/calib.py — калибровка модели по корзинам."""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import calib  # noqa: E402


def mk(p, won, slug="s1", side="Up", vwap=0.60, shares=5.0):
    return {"p": p, "won": won, "vwap": vwap, "shares": shares,
            "slug": slug, "side": side, "tok": "T"}


def write(trades) -> Path:
    d = Path(tempfile.mkdtemp())
    (d / "state.json").write_text(json.dumps({"variants": {"V": {"trades": [
        dict(t, pnl=1.0, t=1.0) for t in trades]}}}))
    return d


class WilsonTests(unittest.TestCase):
    def test_empty_is_fully_uncertain(self):
        self.assertEqual(calib.wilson(0, 0), (0.0, 1.0))

    def test_small_sample_is_wide(self):
        lo, hi = calib.wilson(7, 10)
        self.assertLess(lo, 0.45)
        self.assertGreater(hi, 0.88)

    def test_large_sample_is_narrow(self):
        lo, hi = calib.wilson(700, 1000)
        self.assertGreater(lo, 0.67)
        self.assertLess(hi, 0.73)

    def test_all_wins_does_not_claim_certainty(self):
        lo, hi = calib.wilson(5, 5)
        self.assertLess(lo, 1.0)


class ClusterTests(unittest.TestCase):
    """Пять покупок одного исхода — одно наблюдение, а не пять проверок."""

    def test_repeat_buys_collapse(self):
        tr = [mk(0.7, False, slug="eth15") for _ in range(5)]
        obs = calib.cluster(tr)
        self.assertEqual(len(obs), 1)
        self.assertEqual(obs[0]["n_buys"], 5)

    def test_different_outcomes_stay_separate(self):
        tr = [mk(0.7, False, slug="a"), mk(0.7, True, slug="b")]
        self.assertEqual(len(calib.cluster(tr)), 2)

    def test_same_market_other_side_is_separate(self):
        tr = [mk(0.7, False, slug="a", side="Up"), mk(0.7, True, slug="a", side="Down")]
        self.assertEqual(len(calib.cluster(tr)), 2)

    def test_probability_weighted_by_size(self):
        tr = [mk(0.60, True, shares=1.0), mk(0.80, True, shares=3.0)]
        self.assertAlmostEqual(calib.cluster(tr)[0]["p"], 0.75)

    def test_todays_nine_buys_are_three_observations(self):
        """29.10: 5 x ETH-15m Up, 3 x BTC-15m Up, 1 x ETH-5m Down."""
        tr = ([mk(0.9, False, slug="eth15")] * 5 + [mk(0.9, False, slug="btc15")] * 3
              + [mk(0.9, False, slug="eth5", side="Down")])
        self.assertEqual(len(calib.cluster(tr)), 3)


class VerdictTests(unittest.TestCase):
    def test_no_verdict_below_the_minimum(self):
        """Вердикт на девяти наблюдениях — ровно та ошибка, от которой
        этот скрипт защищает."""
        tr = [mk(0.65, i % 3 == 0, slug=f"s{i}") for i in range(9)]
        txt = calib.report(write(tr), "V", 0.0, 1.01, False)
        for w in ("ЗАВЫШАЕТ", "ЗАНИЖАЕТ"):
            self.assertNotIn(w, txt)

    def test_overstating_model_is_caught(self):
        """Модель обещает 80-90%, выигрывает 40% на большой выборке."""
        tr = [mk(0.85, i % 10 < 4, slug=f"s{i}") for i in range(200)]
        txt = calib.report(write(tr), "V", 0.0, 1.01, False)
        self.assertIn("ЗАВЫШАЕТ", txt)
        self.assertIn("переплата опасна", txt)

    def test_understating_model_is_named_too(self):
        tr = [mk(0.55, i % 10 < 9, slug=f"s{i}") for i in range(200)]
        txt = calib.report(write(tr), "V", 0.0, 1.01, False)
        self.assertIn("ЗАНИЖАЕТ", txt)

    def test_small_sample_says_cannot_tell_not_fine(self):
        tr = [mk(0.65, i % 3 == 0, slug=f"s{i}") for i in range(9)]
        txt = calib.report(write(tr), "V", 0.0, 1.01, False)
        self.assertIn("не отличить", txt)
        self.assertIn("Данных мало для вывода", txt)
        self.assertNotIn("ЗАВЫШАЕТ", txt)
        self.assertIn(f"нужно хотя бы {calib.MIN_N}", txt)

    def test_average_alone_would_hide_opposite_biases(self):
        """Завышение в одной корзине и занижение в другой дают верное среднее.
        Именно поэтому средняя калибровка бесполезна."""
        tr = ([mk(0.85, i % 10 < 5, slug=f"a{i}") for i in range(200)]
              + [mk(0.55, i % 10 < 9, slug=f"b{i}") for i in range(200)])
        txt = calib.report(write(tr), "V", 0.0, 1.01, False)
        self.assertIn("ЗАВЫШАЕТ", txt)
        self.assertIn("ЗАНИЖАЕТ", txt)

    def test_price_filter_limits_to_the_working_range(self):
        tr = ([mk(0.65, True, slug=f"a{i}", vwap=0.60) for i in range(20)]
              + [mk(0.65, False, slug=f"b{i}", vwap=0.10) for i in range(20)])
        txt = calib.report(write(tr), "V", 0.50, 0.70, False)
        self.assertIn("в цене 0.50-0.70: 20", txt)

    def test_raw_mode_is_marked_as_ignoring_dependence(self):
        tr = [mk(0.7, False, slug="one")] * 5
        txt = calib.report(write(tr), "V", 0.0, 1.01, True)
        self.assertIn("зависимость НЕ учтена", txt)

    def test_missing_variant_lists_what_exists(self):
        with self.assertRaises(SystemExit) as e:
            calib.report(write([mk(0.7, True)]), "нет", 0.0, 1.01, False)
        self.assertIn("Есть: V", str(e.exception))


if __name__ == "__main__":
    unittest.main()
