import csv
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import sens  # noqa: E402


def write(d: Path, trades, outcomes):
    with (d / "outcomes.csv").open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["slug", "winner"])
        w.writerows(outcomes)
    cols = ["variant", "slug", "side", "shares", "fill_vwap", "edge_fill", "time_utc"]
    with (d / "trades.csv").open("w", newline="") as fh:
        w = csv.DictWriter(fh, cols)
        w.writeheader()
        w.writerows(trades)


class SensTests(unittest.TestCase):
    def setUp(self):
        self.d = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.d, ignore_errors=True)

    def test_pnl_and_slip(self):
        ts = [{"shares": 10, "vwap": 0.90, "win": True, "edge": 0.01}]
        tot, roi, wins = sens.pnl(ts, 0.0)
        self.assertAlmostEqual(tot, 10 - (9.0 + 0.07 * 0.9 * 0.1 * 10), places=6)
        self.assertEqual(wins, 1)
        worse, _, _ = sens.pnl(ts, 0.02)          # цена хуже -> прибыль меньше
        self.assertLess(worse, tot)

    def test_drop_best_removes_top_edge(self):
        ts = [{"edge": e} for e in (0.05, 0.01, 0.03, 0.02)]
        kept = sens.drop_best(ts, 0.25)
        self.assertEqual(len(kept), 3)
        self.assertNotIn(0.05, [t["edge"] for t in kept])
        self.assertEqual(sens.drop_best(ts, 0.0), ts)

    def test_zscore_and_threshold(self):
        self.assertEqual(sens.zscore([1.0] * 50), 99.0)       # разброса нет, плюс устойчив
        self.assertEqual(sens.zscore([-1.0] * 50), 0.0)
        self.assertAlmostEqual(sens.zscore([1, -1] * 50), 0.0)
        self.assertGreater(sens.z_threshold(16), sens.z_threshold(1))   # перебор -> строже

    def test_eff_k_counts_only_independent_variants(self):
        def v(*pairs):
            return [{"t": "1", "vwap": 0.5, "what": pr} for pr in pairs]
        same = v(("s1", "Up"), ("s2", "Up"))
        self.assertEqual(sens.eff_k({"a": same, "b": v(("s1", "Up"), ("s2", "Up"))}), 1.0)
        self.assertEqual(sens.eff_k({"a": same, "b": v(("s3", "Up"), ("s4", "Up"))}), 2.0)
        part = sens.eff_k({"a": same, "b": v(("s1", "Up"), ("s9", "Up"))})
        self.assertTrue(1.0 < part < 2.0, part)            # половина сделок общая
        self.assertEqual(sens.eff_k({"a": same}), 1.0)
        # метка времени у каждого варианта своя -> на неё опираться нельзя
        self.assertEqual(sens.eff_k({"a": [{"t": "1.001", "vwap": 0.5, "what": ("s1", "Up")}],
                                     "b": [{"t": "1.004", "vwap": 0.5, "what": ("s1", "Up")}]}), 1.0)
        self.assertEqual(sens.z_threshold(1), 2.0)
        self.assertGreater(sens.z_threshold(16), sens.z_threshold(2))

    def test_top_share_and_halves(self):
        self.assertAlmostEqual(sens.top_share([10, 1, 1, 1, 1, 1], 3), 80.0)
        self.assertEqual(sens.top_share([-1, -2], 3), 0.0)
        ts = [{"shares": 10, "vwap": 0.5, "win": True, "edge": 0, "t": "2026-10-28T01:00:00"},
              {"shares": 10, "vwap": 0.5, "win": False, "edge": 0, "t": "2026-10-28T02:00:00"}]
        h1, h2, n1, n2 = sens.halves(ts)
        self.assertEqual((n1, n2), (1, 1))
        self.assertGreater(h1, 0)
        self.assertLess(h2, 0)

    def test_load_skips_unsettled_and_report(self):
        outcomes = [[f"s{i}", "Up"] for i in range(40)] + [["sX", ""]]
        trades = [{"variant": "A", "slug": f"s{i}", "side": "Up", "shares": 10,
                   "fill_vwap": 0.80, "edge_fill": 0.01 * (i % 5),
                   "time_utc": f"2026-10-28T{i // 2:02d}:00:00"} for i in range(40)]
        trades.append({"variant": "A", "slug": "sX", "side": "Up", "shares": 10,
                       "fill_vwap": 0.80, "edge_fill": 0.9,
                       "time_utc": "2026-10-28T23:00:00"})       # окно не рассчитано
        write(self.d, trades, outcomes)
        self.assertEqual(len(sens.load(self.d)["A"]), 40)
        txt = sens.report(self.d, min_n=30)
        self.assertIn("A: 40 сделок", txt)
        self.assertIn("Прошли проверку на значимость: 1 из 1", txt)
        self.assertNotIn("шум", txt)

    def test_state_json_is_preferred_and_full(self):
        """В trades.csv только часть истории, в state.json — вся. Берём вторую."""
        outcomes = [[f"s{i}", "Up"] for i in range(5)]
        trades = [{"variant": "A", "slug": f"s{i}", "side": "Up", "shares": 10,
                   "fill_vwap": 0.80, "edge_fill": 0.01,
                   "time_utc": f"2026-10-28T{i:02d}:00:00"} for i in range(5)]
        write(self.d, trades, outcomes)
        st = {"variants": {"A": {"trades": [
            {"slug": f"s{i}", "tok": "T", "side": "Up", "t": 1000.0 + i, "shares": 10,
             "vwap": 0.80, "cost": 8.1, "edge_fill": 0.01, "pnl": 1.9, "won": True}
            for i in range(40)] + [
            {"slug": "sOpen", "tok": "T", "side": "Up", "t": 2000.0, "shares": 10,
             "vwap": 0.80, "cost": 8.1, "edge_fill": 0.01, "pnl": None, "won": False}]}}}
        (self.d / "state.json").write_text(json.dumps(st))
        self.assertEqual(len(sens.load_state(self.d)["A"]), 40)   # нерассчитанная пропущена
        self.assertEqual(len(sens.load(self.d)["A"]), 5)
        txt = sens.report(self.d, min_n=30)
        self.assertIn("state.json", txt)
        self.assertIn("A: 40 сделок", txt)
        csv_txt = sens.report(self.d, min_n=3, source="csv")
        self.assertIn("trades.csv", csv_txt)
        self.assertIn("A: 5 сделок", csv_txt)

    def test_state_json_missing_falls_back_to_csv(self):
        outcomes = [[f"s{i}", "Up"] for i in range(40)]
        trades = [{"variant": "A", "slug": f"s{i}", "side": "Up", "shares": 10,
                   "fill_vwap": 0.80, "edge_fill": 0.01,
                   "time_utc": f"2026-10-28T{i // 2:02d}:00:00"} for i in range(40)]
        write(self.d, trades, outcomes)
        self.assertEqual(sens.load_state(self.d), {})
        self.assertIn("trades.csv", sens.report(self.d, min_n=30))

    def test_losing_variant_marked(self):
        outcomes = [[f"s{i}", "Up" if i % 2 else "Down"] for i in range(40)]
        trades = [{"variant": "B", "slug": f"s{i}", "side": "Up", "shares": 10,
                   "fill_vwap": 0.80, "edge_fill": 0.01,
                   "time_utc": f"2026-10-28T{i // 2:02d}:00:00"} for i in range(40)]
        write(self.d, trades, outcomes)
        self.assertIn("шум, не доказано", sens.report(self.d, min_n=30))


if __name__ == "__main__":
    unittest.main()
