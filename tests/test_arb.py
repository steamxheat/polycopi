import asyncio
import csv
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import arbwatch  # noqa: E402
from arbwatch import Mkt, Watcher, pair_fill, fee  # noqa: E402

COEF = 0.07


class FakeHttp:
    def __init__(self):
        self.books = {}

    async def aget(self, url, params=None):
        if url.endswith("/book"):
            return self.books[params["token_id"]]
        return []


class PairFillTests(unittest.TestCase):
    def test_no_arb(self):
        self.assertEqual(pair_fill({0.52: 100}, {0.50: 100}, COEF)[0], 0)

    def test_arb_walks_levels(self):
        # 0.45 + 0.45 + комиссии < 1; второй уровень Down 0.56 уже не выгоден
        pairs, gain, avg = pair_fill({0.45: 10, 0.46: 10}, {0.45: 5, 0.56: 50}, COEF)
        self.assertAlmostEqual(pairs, 5)
        c = 0.9 + 2 * fee(0.45, COEF)
        self.assertAlmostEqual(gain, 5 * (1 - c))
        self.assertAlmostEqual(avg, c)


class WatcherTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.t = 1000.0
        self.http = FakeHttp()
        cfg = dict(arbwatch.DEFAULTS)
        self.w = Watcher(cfg, self.tmp, http=self.http, now=lambda: self.t)
        mk = Mkt("btc-updown-5m-900", "btc", "5m", 900, 1200, "U", "D")
        self.w.markets[mk.slug] = mk
        self.w.tok["U"] = self.w.tok["D"] = mk
        self.mk = mk

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    async def test_episode_detect_confirm_and_report(self):
        w = self.w
        w.on_event({"event_type": "book", "asset_id": "U", "asks": [{"price": "0.55", "size": "20"}]})
        w.on_event({"event_type": "book", "asset_id": "D", "asks": [{"price": "0.50", "size": "20"}]})
        self.assertIsNone(self.mk.ep)                       # 1.05 — арбитража нет
        self.http.books = {"U": {"asks": [{"price": "0.45", "size": "8"}]},
                           "D": {"asks": [{"price": "0.47", "size": "30"}]}}
        # продавец Up опустил цену до 0.45 -> 0.45 + 0.50 + комиссии < 1
        w.on_event({"event_type": "price_change", "price_changes": [
            {"asset_id": "U", "price": "0.45", "size": "10", "side": "SELL"}]})
        self.assertIsNotNone(self.mk.ep)
        await asyncio.sleep(0.01)                           # перепроверка стакана
        self.t += 0.4
        w.on_event({"event_type": "price_change", "price_changes": [
            {"asset_id": "U", "price": "0.45", "size": "0", "side": "SELL"}]})   # кто-то забрал
        self.assertIsNone(self.mk.ep)
        await asyncio.sleep(0.01)
        rows = list(csv.DictReader((self.tmp / "episodes.csv").open()))
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertAlmostEqual(float(r["duration_s"]), 0.4, places=3)
        self.assertAlmostEqual(float(r["max_pairs_top"]), 10)
        self.assertAlmostEqual(float(r["confirm_pairs"]), 8)
        self.assertGreater(float(r["confirm_gain_usd"]), 0)
        txt = arbwatch.report(self.tmp)
        self.assertIn("BTC 5m", txt)
        self.assertIn("ИТОГО", txt)

    async def test_buy_side_ignored_and_market_cleanup(self):
        w = self.w
        w.on_event({"event_type": "price_change", "price_changes": [
            {"asset_id": "U", "price": "0.10", "size": "10", "side": "BUY"}]})
        self.assertNotIn("U", w.asks)
        self.t = 1300
        await w.discover()                                  # рынок закончился -> удалён
        self.assertNotIn(self.mk.slug, w.markets)
        self.assertNotIn("U", w.tok)


if __name__ == "__main__":
    unittest.main()
