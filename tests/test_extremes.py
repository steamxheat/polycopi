import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import extremes  # noqa: E402


def trade(pnl, cost=4.0, shares=5.0, vwap=0.8, won=None, t=1000.0, tau=120,
          slug="btc-updown-5m-900", side="Up"):
    return {"pnl": pnl, "cost": cost, "shares": shares, "vwap": vwap,
            "won": won if won is not None else pnl > 0, "t": t, "tau": tau,
            "slug": slug, "side": side}


def write(d: Path, trades, name="A"):
    (d / "state.json").write_text(json.dumps({"variants": {name: {"trades": trades}}}))


class LoadTests(unittest.TestCase):
    def setUp(self):
        self.d = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.d, ignore_errors=True)

    def test_skips_unsettled(self):
        write(self.d, [trade(1.0), {**trade(0), "pnl": None}])
        self.assertEqual(len(extremes.load(self.d, "A")["A"]), 1)

    def test_filters_by_variant(self):
        (self.d / "state.json").write_text(json.dumps(
            {"variants": {"A": {"trades": [trade(1.0)]}, "B": {"trades": [trade(2.0)]}}}))
        self.assertEqual(list(extremes.load(self.d, "B")), ["B"])
        self.assertEqual(sorted(extremes.load(self.d, None)), ["A", "B"])

    def test_broken_rows_ignored(self):
        write(self.d, [trade(1.0), {"pnl": "ой", "cost": 1}])
        self.assertEqual(len(extremes.load(self.d, "A")["A"]), 1)


class ReportTests(unittest.TestCase):
    def setUp(self):
        self.d = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.d, ignore_errors=True)

    def test_extremes_and_stake_range(self):
        ts = [trade(0.9, cost=4.0, vwap=0.80) for _ in range(10)]
        ts.append(trade(7.5, cost=2.0, shares=10, vwap=0.20))       # крупный выигрыш
        ts.append(trade(-9.0, cost=9.0, shares=10, vwap=0.90))      # крупный проигрыш
        write(self.d, ts)
        txt = extremes.report(self.d, "A", top=2)
        self.assertIn("ставка:  от 2.00$ до 9.00$", txt)
        self.assertIn("цена:    от 0.20 до 0.90", txt)
        best = txt[txt.index("крупных выигрышей"):txt.index("крупных проигрышей")]
        self.assertIn("+7.50$", best)
        worst = txt[txt.index("крупных проигрышей"):]
        self.assertIn("-9.00$", worst)

    def test_concentration_warning_when_few_trades_carry_all(self):
        ts = [trade(-1.0) for _ in range(20)] + [trade(30.0)]
        write(self.d, ts)
        txt = extremes.report(self.d, "A", top=1)
        self.assertIn("Больше 100%", txt)
        self.assertIn("хрупко", txt)

    def test_spread_profit_is_called_even(self):
        ts = [trade(1.0) for _ in range(50)]
        write(self.d, ts)
        txt = extremes.report(self.d, "A", top=5)
        self.assertIn("размазана", txt)

    def test_longest_losing_streak_and_worst_single(self):
        ts = [trade(1.0), trade(-4.0), trade(-4.0), trade(-4.0), trade(1.0), trade(-9.9)]
        write(self.d, ts)
        txt = extremes.report(self.d, "A", top=2)
        self.assertIn("Худшая серия подряд без выигрыша: 3 сделок", txt)
        self.assertIn("Самый большой убыток за одну сделку: -9.90$", txt)

    def test_no_data(self):
        (self.d / "state.json").write_text(json.dumps({"variants": {}}))
        self.assertIn("Нет рассчитанных сделок", extremes.report(self.d, "A", 5))


if __name__ == "__main__":
    unittest.main()
