import csv
import random
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import backtest  # noqa: E402

COLS = ["t", "asset", "window", "slug", "tau", "ratio_bps", "sigma", "p_spot", "p_t60", "p_wt",
        "up_bid", "up_ask", "down_bid", "down_ask", "sources"]


class BacktestTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        rnd = random.Random(1)
        with (self.tmp / "snapshots.csv").open("w", newline="") as fh, \
                (self.tmp / "outcomes.csv").open("w", newline="") as fo:
            w = csv.DictWriter(fh, fieldnames=COLS)
            w.writeheader()
            o = csv.writer(fo)
            o.writerow(["slug", "asset", "window", "start", "winner", "d_spot_bps", "d_t60_bps", "d_wt_bps"])
            for k in range(80):
                slug = f"btc-updown-5m-{1000 + k * 300}"
                win = "Up" if rnd.random() < 0.5 else "Down"
                # рынок честный (аск = шанс), а модель «знает» итог в 2/3 случаев
                for s in range(0, 300, 10):
                    p = 0.5
                    pm = (0.7 if win == "Up" else 0.3) if k % 3 else 0.5
                    w.writerow({"t": 1000 + k * 300 + s, "asset": "btc", "window": "5m", "slug": slug,
                                "tau": 300 - s, "ratio_bps": 0, "sigma": "1e-5", "p_spot": pm, "p_t60": pm,
                                "p_wt": pm, "up_bid": p - 0.01, "up_ask": p, "down_bid": p - 0.01,
                                "down_ask": p, "sources": 2})
                o.writerow([slug, "btc", "5m", 0, win, 0, 0, 0])

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_report(self):
        snaps, winners = backtest.load(self.tmp)
        self.assertEqual(len(snaps), 80)
        strategies = backtest.build()
        self.assertGreaterEqual(len(strategies), 90)
        txt = backtest.report(self.tmp, min_n=10, top=10)
        self.assertIn("модель", txt)
        self.assertIn("✅", txt)            # «знающая» модель должна пройти фильтры
        self.assertIn("Прошли все фильтры", txt)


if __name__ == "__main__":
    unittest.main()
