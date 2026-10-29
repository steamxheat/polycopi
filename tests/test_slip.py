"""scripts/slip.py — запас прочности по цене билета."""
from __future__ import annotations

import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import slip  # noqa: E402


def mk(vwap, win, shares=5.0, t=1.0):
    return {"v": "V", "shares": shares, "vwap": vwap, "win": win,
            "edge": 0.0, "t": f"{t:.3f}", "ts": t, "what": (f"s{t}", "Up")}


class ArithmeticTests(unittest.TestCase):
    def test_fee_is_biggest_in_the_middle(self):
        self.assertGreater(slip.fee(0.5), slip.fee(0.1))
        self.assertAlmostEqual(slip.fee(0.5), 0.07 * 0.25)

    def test_winning_ticket_pays_the_rest_of_the_dollar(self):
        self.assertAlmostEqual(slip.per_share(mk(0.60, True)), 0.40 - slip.fee(0.6), places=6)

    def test_losing_ticket_costs_its_price(self):
        self.assertAlmostEqual(slip.per_share(mk(0.60, False)), -0.60 - slip.fee(0.6), places=6)

    def test_slack_equals_profit_per_ticket(self):
        """Переплата в s центов снижает прибыль ровно на s — значит средняя
        прибыль на билет и есть запас прочности."""
        tr = [mk(0.60, True)] * 3 + [mk(0.60, False)]
        s = slip.stats(tr)
        moved = slip.stats([{**t, "vwap": t["vwap"] + 0.05} for t in tr])
        # не ровно 0.05: сбор тоже зависит от цены
        self.assertAlmostEqual(s["slack"] - moved["slack"], 0.05, places=2)

    def test_fair_price_leaves_no_margin(self):
        """60% побед по цене 0.60 — запас отрицательный из-за сбора."""
        tr = [mk(0.60, True)] * 6 + [mk(0.60, False)] * 4
        self.assertLess(slip.stats(tr)["slack"], 0)

    def test_edge_shows_up_as_margin(self):
        """74% побед по 0.60 — около 12¢ на билет."""
        tr = [mk(0.60, True)] * 74 + [mk(0.60, False)] * 26
        self.assertAlmostEqual(slip.stats(tr)["slack"], 0.12, places=2)

    def test_lower_bound_is_below_the_average(self):
        tr = [mk(0.60, True)] * 74 + [mk(0.60, False)] * 26
        s = slip.stats(tr)
        self.assertLess(s["lo"], s["slack"])


class FillTests(unittest.TestCase):
    def test_reads_survival_by_bucket(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            with (d / "survive.csv").open("w", newline="") as fh:
                w = csv.DictWriter(fh, ["ask0", "okt_200", "ok_200"])
                w.writeheader()
                w.writerow({"ask0": "0.60", "okt_200": "1", "ok_200": "1"})
                w.writerow({"ask0": "0.65", "okt_200": "0", "ok_200": "0"})
                w.writerow({"ask0": "0.10", "okt_200": "1", "ok_200": "0"})
            f = slip.fills(d)
            self.assertEqual(f[(0.50, 0.70)], [2, 1])
            self.assertEqual(f[(0.0, 0.15)], [1, 1])

    def test_no_file_is_not_a_crash(self):
        with tempfile.TemporaryDirectory() as td:
            self.assertEqual(slip.fills(Path(td)), {})


class ReportTests(unittest.TestCase):
    def _dir(self, trades):
        td = tempfile.mkdtemp()
        d = Path(td)
        (d / "state.json").write_text(json.dumps({"variants": {"V": {"trades": [
            {"shares": t["shares"], "vwap": t["vwap"], "won": t["win"], "pnl": 1.0,
             "t": t["ts"], "slug": t["what"][0], "side": "Up", "edge_fill": 0.0}
            for t in trades]}}}))
        return d

    def test_cheap_bucket_called_empty(self):
        tr = [mk(0.10, True)] * 4 + [mk(0.10, False)] * 28
        txt = slip.report(self._dir(tr), "V", None)
        self.assertIn("ПУСТО", txt)
        self.assertIn("торговать нечем", txt)

    def test_core_bucket_good_when_there_are_enough_trades(self):
        tr = ([mk(0.60, True)] * 74 + [mk(0.60, False)] * 26) * 4      # 400 сделок
        txt = slip.report(self._dir(tr), "V", None)
        self.assertIn("ЕСТЬ", txt)
        self.assertIn("0.50-0.70", txt)
        self.assertIn("торговать 0.50-0.70", txt)

    def test_core_on_a_hundred_trades_is_only_thin(self):
        """74% по 0.60 на 100 сделках: в среднем 12¢, но наверняка лишь ~4¢ —
        ровно столько, сколько мы сами переплачиваем."""
        tr = [mk(0.60, True)] * 74 + [mk(0.60, False)] * 26
        txt = slip.report(self._dir(tr), "V", None)
        self.assertIn("ТОНКО", txt)
        self.assertIn("переплату надо срезать", txt)

    def test_barely_above_fair_is_called_empty(self):
        """64% по 0.60 — в среднем плюс, но нижняя граница ниже нуля."""
        tr = [mk(0.60, True)] * 64 + [mk(0.60, False)] * 36
        txt = slip.report(self._dir(tr), "V", None)
        self.assertIn("ПУСТО", txt)
        self.assertNotIn("ЕСТЬ ", txt)

    def test_thin_bucket_suggests_a_smaller_markup(self):
        tr = [mk(0.60, True)] * 74 + [mk(0.60, False)] * 26
        txt = slip.report(self._dir(tr), "V", None)
        self.assertIn("limit_extra_max", txt)
        self.assertNotIn("ЕСТЬ ", txt)

    def test_missing_variant_says_what_is_there(self):
        txt = slip.report(self._dir([mk(0.6, True)]), "нетакого", None)
        self.assertIn("Есть: V", txt)


if __name__ == "__main__":
    unittest.main()
