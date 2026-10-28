import sys
import shutil
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import feedlat  # noqa: E402


class ParserTests(unittest.TestCase):
    def test_parsers(self):
        self.assertEqual(feedlat.p_binance({"data": {"s": "BTCUSDT", "p": "100", "T": 1000}}), [("BTC", 100.0, 1.0)])
        self.assertEqual(feedlat.p_bybit({"topic": "publicTrade.ETHUSDT", "data": [{"s": "ETHUSDT", "p": "5", "T": 2000}]}),
                         [("ETH", 5.0, 2.0)])
        self.assertEqual(feedlat.p_okx({"arg": {"channel": "trades"}, "data": [{"instId": "BTC-USDT-SWAP", "px": "7", "ts": "3000"}]}),
                         [("BTC", 7.0, 3.0)])
        cb = feedlat.p_coinbase({"type": "match", "product_id": "ETH-USD", "price": "9", "time": "1970-01-01T00:00:04Z"})
        self.assertEqual(cb, [("ETH", 9.0, 4.0)])
        kr = feedlat.p_kraken({"channel": "trade", "data": [{"symbol": "BTC/USD", "price": 11, "timestamp": "1970-01-01T00:00:05Z"}]})
        self.assertEqual(kr, [("BTC", 11.0, 5.0)])
        bs = feedlat.p_bitstamp({"event": "trade", "channel": "live_trades_btcusd", "data": {"price": 12, "microtimestamp": "6000000"}})
        self.assertEqual(bs, [("BTC", 12.0, 6.0)])
        rt = feedlat.p_rtds({"topic": "crypto_prices_chainlink", "payload": {"symbol": "btc/usd", "value": 13, "timestamp": 7000}})
        self.assertEqual(rt, [("BTC", 13.0, 7.0)])
        self.assertEqual(feedlat.p_binance({"result": None}), [])


class RaceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_race_ranks_sources(self):
        r = feedlat.Race(dict(feedlat.DEFAULTS), self.tmp)
        t = 1000.0
        for i in range(30):                                   # 3 с спокойной цены у всех
            for src in ("fast", "mid", "slow"):
                r.on_price(src, "BTC", 100000.0, None, t + i * 0.1)
        t += 3.0
        r.on_price("fast", "BTC", 100050.0, t - 0.005, t)        # +5 б.п. — рывок
        r.on_price("mid", "BTC", 100040.0, None, t + 0.040)
        r.on_price("slow", "BTC", 100045.0, None, t + 0.150)
        r.on_price("fast", "BTC", 100050.0, None, t + 4.0)       # закрывает событие
        txt = feedlat.report(self.tmp)
        self.assertIn("1 рывков", txt)
        lines = {l.split()[0]: l for l in txt.splitlines() if l.split() and l.split()[0] in ("fast", "mid", "slow")}
        self.assertIn("100%", lines["fast"])
        self.assertIn("40мс", lines["mid"])
        self.assertIn("150мс", lines["slow"])
        r.status()
        self.assertIn("Доставка", feedlat.report(self.tmp))


if __name__ == "__main__":
    unittest.main()
