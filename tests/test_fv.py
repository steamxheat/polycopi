import asyncio
import csv
import json
import math
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import fvsim  # noqa: E402
import research  # noqa: E402

START = 1793112000
SLUG = f"btc-updown-5m-{START}"
UP, DN = "111", "222"


def gamma_market(winner=None):
    prices = ["0.5", "0.5"] if winner is None else (["1", "0"] if winner == "Up" else ["0", "1"])
    return {"slug": SLUG, "conditionId": "0xc", "closed": winner is not None,
            "outcomes": json.dumps(["Up", "Down"]), "outcomePrices": json.dumps(prices),
            "clobTokenIds": json.dumps([UP, DN])}


def book(asks=(), bids=()):
    return {"bids": [{"price": str(p), "size": str(s)} for p, s in bids],
            "asks": [{"price": str(p), "size": str(s)} for p, s in asks]}


class FakeApi:
    def __init__(self):
        self.t = START - 700.0
        self.open = {SLUG: gamma_market()}
        self.closed = {}
        self.books = {}
        self.book_calls = 0

    def now(self):
        return self.t

    async def get_market(self, slug, closed=None):
        return (self.closed if closed else self.open).get(slug)

    async def get_book(self, tok):
        self.book_calls += 1
        return self.books.get(tok, book())


def make_engine(tmp, **over):
    cfg = fvsim.load_config(Path("/nonexistent"))
    cfg.update({"assets": {"btc": "BTCUSDT"}, "windows": {"5m": 300},
                "variants": [{"name": "e2", "min_edge": 0.02}, {"name": "e10", "min_edge": 0.10}]})
    cfg.update(over)
    fvsim.setup_logging(tmp, cfg, quiet=True)
    api = FakeApi()
    return fvsim.Engine(cfg, api, tmp, quiet=True), api


def feed(eng, api, until, price_fn, sources=("coinbase_ws", "okx_ws")):
    """Посекундно подаём цены от до until (сэмплер + старт окна)."""
    while api.t < until:
        for i, src in enumerate(sources):
            eng.prices.add("BTCUSDT", api.t, price_fn(api.t) * (1 + i * 1e-4), src)
        eng.sampler.tick(api.t)
        eng.capture_s0()
        api.t += 1


class ModelTests(unittest.TestCase):
    def test_p_up(self):
        self.assertAlmostEqual(fvsim.p_up(1.0, 1e-4, 100), 0.5)
        self.assertGreater(fvsim.p_up(1.001, 1e-4, 100), 0.8)       # +10 б.п. при σ√τ=10 б.п.
        self.assertAlmostEqual(fvsim.p_up(1.001, 1e-4, 100), fvsim.phi(1.0), places=3)
        self.assertEqual(fvsim.p_up(1.01, 1e-4, 0), 1.0)
        self.assertEqual(fvsim.p_up(0.99, 1e-4, 0), 0.0)
        self.assertAlmostEqual(research.p_up(101, 100, 1e-3, 100), fvsim.p_up(1.01, 1e-3, 100), places=9)

    def test_fee(self):
        self.assertAlmostEqual(fvsim.fee(0.5, 0.07), 0.0175)


class RuleTests(unittest.TestCase):
    def test_rules(self):
        import fvmodel
        d = {t: math.log(100.0) for t in range(0, 1000)}
        for t in range(300, 401):
            d[t] = math.log(101.0)                 # +1% только в последние 100 с из 300
        at, avg = fvmodel.series_funcs(d)
        st, en = 100, 400
        self.assertGreater(fvmodel.final_diff("spot", at, avg, st, en), 0.009)
        self.assertGreater(fvmodel.final_diff("t60", at, avg, st, en), 0.009)
        self.assertAlmostEqual(fvmodel.final_diff("wt", at, avg, st, en), math.log(1.01) / 3, places=6)
        # за 10 с до конца по t60 почти всё решено даже если цена сейчас откатилась
        p = fvmodel.prob_up("t60", at, avg, st, en, en - 10, math.log(100.0), 1e-4)
        self.assertGreater(p, 0.99)
        p = fvmodel.prob_up("spot", at, avg, st, en, en - 10, math.log(100.0), 1e-4)
        self.assertAlmostEqual(p, 0.5, places=6)


class EngineTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp)

    async def test_full_cycle_buy_and_settle(self):
        eng, api = make_engine(self.tmp)
        wiggle = lambda t: 100000 * (1 + 2e-5 * math.sin(t))           # σ ≈ 1.4 б.п./с
        feed(eng, api, START - 10, wiggle)
        await eng.discover()
        self.assertIn(SLUG, eng.markets)
        feed(eng, api, START + 200, lambda t: wiggle(t) if t < START + 150 else 100000 * 1.003)
        m = eng.markets[SLUG]
        self.assertEqual(set(m.s0), {"coinbase_ws", "okx_ws"})
        pr, r, sig, n = eng.model(m)
        self.assertGreater(pr["spot"], 0.97)                            # +30 б.п., 100 с до конца
        self.assertGreater(pr["t60"], 0.97)
        self.assertGreater(pr["wt"], 0.9)
        self.assertEqual(n, 2)
        # рынок отстаёт: Up продают по 0.80
        eng.book.on_event({"event_type": "price_change", "price_changes": [
            {"asset_id": UP, "best_bid": "0.78", "best_ask": "0.80"},
            {"asset_id": DN, "best_bid": "0.19", "best_ask": "0.21"}]}, api.t)
        api.books[UP] = book(asks=[(0.80, 3), (0.81, 100), (0.90, 100)])
        eng.decide()
        await asyncio.sleep(0.01)
        self.assertEqual(api.book_calls, 1)                             # один запрос стакана на оба варианта
        for name in ("e2", "e10"):
            v = eng.variants[name]
            self.assertEqual(len(v.trades), 1)
            t = v.trades[0]
            self.assertEqual(t["side"], "Up")
            self.assertAlmostEqual(t["shares"], 5.0)                    # минимум 5 акций
            self.assertAlmostEqual(t["vwap"], (0.8 * 3 + 0.81 * 2) / 5, places=4)
        # повторно сразу не входим (пауза между входами)
        eng.decide()
        await asyncio.sleep(0.01)
        self.assertEqual(api.book_calls, 1)
        # расчёт (цены докручиваем до конца окна)
        feed(eng, api, START + 305, lambda t: 100000 * 1.003)
        api.t = START + 320
        api.closed[SLUG] = gamma_market("Up")
        await eng.resolve()
        self.assertNotIn(SLUG, eng.markets)
        v = eng.variants["e2"]
        self.assertGreater(v.realized, 0)
        self.assertTrue(v.trades[0]["won"])
        self.assertAlmostEqual(v.cash, 100 + v.realized, places=6)
        rows = list(csv.DictReader((self.tmp / "trades.csv").open()))
        self.assertEqual(len(rows), 2)
        self.assertTrue((self.tmp / "snapshots.csv").exists())
        out = list(csv.DictReader((self.tmp / "outcomes.csv").open()))
        self.assertEqual(out[0]["winner"], "Up")
        self.assertGreater(float(out[0]["d_spot_bps"]), 25)          # +30 б.п. к концу
        self.assertEqual(eng.counters["rule_t60_ok"], 1)
        snap = list(csv.DictReader((self.tmp / "snapshots.csv").open()))
        self.assertIn("p_wt", snap[0])
        eng.save()
        txt = fvsim.report(self.tmp)
        self.assertIn("e10", txt)
        self.assertIn("калибровка", txt)
        self.assertIn("какое правило", txt)

    async def test_lag_variant_needs_fresh_move_and_stale_ask(self):
        eng, api = make_engine(self.tmp, variants=[
            {"name": "lag", "min_edge": 0.0, "move_bps": 3, "move_s": 1, "ask_stale_s": 1}])
        wiggle = lambda t: 100000 * (1 + 2e-5 * math.sin(t))
        feed(eng, api, START - 10, wiggle)
        await eng.discover()
        feed(eng, api, START + 150, lambda t: wiggle(t) if t < START + 100 else 100000 * 1.003)
        eng.book.on_event({"event_type": "price_change", "price_changes": [
            {"asset_id": UP, "best_bid": "0.78", "best_ask": "0.80"},
            {"asset_id": DN, "best_bid": "0.19", "best_ask": "0.21"}]}, api.t - 5)
        api.books[UP] = book(asks=[(0.80, 100)])
        # цена давно стоит на месте — рывка нет -> не входим
        eng.decide()
        await asyncio.sleep(0.01)
        self.assertEqual(api.book_calls, 0)
        # рывок +5 б.п. за секунду, аск Polymarket не менялся 5 с -> входим в Up
        for src in ("coinbase_ws", "okx_ws"):
            eng.prices.add("BTCUSDT", api.t, 100000 * 1.0035, src)
        eng.decide()
        await asyncio.sleep(0.01)
        self.assertEqual(api.book_calls, 1)
        self.assertEqual(eng.variants["lag"].trades[0]["side"], "Up")

    async def test_twap_variant_and_csv_rotation(self):
        (self.tmp / "snapshots.csv").write_text("old,header\n1,2\n")
        eng, api = make_engine(self.tmp, variants=[{"name": "t60", "min_edge": 0.05, "rule": "t60"},
                                                    {"name": "spot", "min_edge": 0.05}])
        # минуту перед стартом цена была на 20 б.п. ниже, на старте прыгнула вверх и стоит
        feed(eng, api, START - 10, lambda t: 100000 * (1 + 2e-5 * math.sin(t)) * (0.998 if t < START - 1 else 1))
        await eng.discover()
        feed(eng, api, START + 30, lambda t: 100000 * (1 + 2e-5 * math.sin(t)))
        pr, *_ = eng.model(eng.markets[SLUG])
        self.assertAlmostEqual(pr["spot"], 0.5, delta=0.15)     # по споту — «ничего не было»
        self.assertGreater(pr["t60"], 0.9)                       # по TWAP база ниже -> Up почти точно
        eng.book.on_event({"event_type": "book", "asset_id": UP, "bids": [],
                           "asks": [{"price": "0.70", "size": "50"}]}, api.t)
        api.books[UP] = book(asks=[(0.70, 50)])
        eng.decide()
        await asyncio.sleep(0.01)
        self.assertEqual(len(eng.variants["t60"].trades), 1)
        self.assertEqual(len(eng.variants["spot"].trades), 0)
        self.assertTrue(list(self.tmp.glob("snapshots_old_*.csv")))
        head = (self.tmp / "snapshots.csv").read_text().splitlines()[0]
        self.assertIn("p_t60", head)

    async def test_no_trade_without_edge_and_miss_when_book_moved(self):
        eng, api = make_engine(self.tmp)
        feed(eng, api, START - 10, lambda t: 100000 * (1 + 2e-5 * math.sin(t)))
        await eng.discover()
        feed(eng, api, START + 200, lambda t: 100000 * (1 + 2e-5 * math.sin(t)))
        # модель ≈0.5, рынок 0.50 — перевеса нет
        eng.book.on_event({"event_type": "book", "asset_id": UP, "bids": [{"price": "0.49", "size": "9"}],
                           "asks": [{"price": "0.50", "size": "9"}]}, api.t)
        eng.decide()
        await asyncio.sleep(0.01)
        self.assertEqual(api.book_calls, 0)
        # рынок вдруг продаёт Up по 0.30 (перевес ~20¢), но пока мы ходили за стаканом — уже 0.45
        eng.book.on_event({"event_type": "book", "asset_id": UP, "bids": [],
                           "asks": [{"price": "0.30", "size": "9"}]}, api.t)
        api.books[UP] = book(asks=[(0.45, 100)])
        eng.decide()
        await asyncio.sleep(0.01)
        self.assertEqual(api.book_calls, 1)
        self.assertEqual(eng.variants["e2"].trades, [])
        self.assertEqual(eng.counters.get("miss_e2"), 1)

    async def test_window_joined_midway_is_skipped(self):
        eng, api = make_engine(self.tmp)
        api.t = START + 100                         # бот стартовал посреди окна
        await eng.discover()
        feed(eng, api, START + 200, lambda t: 100000.0)
        self.assertEqual(eng.markets[SLUG].s0, {})
        self.assertIsNone(eng.model(eng.markets[SLUG]))

    async def test_state_persists(self):
        eng, api = make_engine(self.tmp)
        eng.variants["e2"].cash = 77.0
        eng.markets[SLUG] = fvsim.Mkt(SLUG, "btc", "5m", START, START + 300, UP, DN)
        eng.markets[SLUG].s0 = {"x": 1.0}
        eng.save()
        eng2, _ = make_engine(self.tmp)
        self.assertEqual(eng2.variants["e2"].cash, 77.0)
        self.assertEqual(eng2.markets[SLUG].s0, {"x": 1.0})


class ResearchTests(unittest.IsolatedAsyncioTestCase):
    def test_analyse(self):
        rows = []
        for i in range(40):
            won = i % 4 != 0
            rows.append({"price": 0.7, "win": won, "pnl_ps": (0.3 if won else -0.7), "cost": 7.0,
                         "pnl": (3 if won else -7), "tau": 20 + i, "edge": 0.05, "z_side": 1.0,
                         "taker": i % 2 == 0, "p_model": 0.75})
        txt = research.analyse(rows)
        self.assertIn("Сделок: 40", txt)
        self.assertIn("винрейт 75%", txt)
        self.assertIn("калибровка", txt)

    async def test_run_against_fake_http(self):
        start = START
        slug = f"btc-updown-5m-{start}"
        acts = [{"slug": slug, "side": "BUY", "price": 0.6, "size": 10, "usdcSize": 6.0, "outcome": "Up",
                 "timestamp": start + 250},
                {"slug": "some-sports-market", "side": "BUY", "price": 0.5, "size": 1, "timestamp": 1}]

        class FakeHttp:
            def __init__(self, *a, **k):
                pass

            async def aget(self, url, params=None):
                if url.endswith("/activity"):
                    return acts if params["offset"] == 0 else []
                if url.endswith("/markets"):
                    return [{"outcomes": '["Up","Down"]', "outcomePrices": '["1","0"]'}]
                if url.endswith("/klines"):
                    t0 = params["startTime"] // 1000
                    return [[(t0 + i) * 1000, 0, 0, 0, str(100000 * (1 + 1e-5 * math.sin(t0 + i))
                                                           + (50 if t0 + i > start + 200 else 0))]
                            for i in range(1000) if t0 + i < start + 400]
                raise AssertionError(url)

        old, oldhere = research.Http, research.HERE
        tmp = Path(tempfile.mkdtemp())
        research.Http, research.HERE = FakeHttp, tmp
        try:
            args = type("A", (), {"wallet": "0xabc", "name": "x", "max_trades": 100, "data": "d",
                                  "gamma": "g", "binance": "b"})()
            self.assertEqual(await research.run(args), 0)
            rows = [json.loads(x) for x in (tmp / "research_trades.jsonl").read_text().splitlines()]
            self.assertEqual(len(rows), 1)
            self.assertTrue(rows[0]["win"])
            self.assertGreater(rows[0]["p_model"], 0.9)          # цена ушла на +5 б.п. и держится
            self.assertAlmostEqual(rows[0]["tau"], 50)
            self.assertIsNotNone(rows[0]["p_t60"])
            rep = (tmp / "research_report.txt").read_text()
            self.assertIn("КАКОЕ ПРАВИЛО", rep)
            self.assertIn("1/1 = 100.0%", rep)
        finally:
            research.Http, research.HERE = old, oldhere
            shutil.rmtree(tmp)


if __name__ == "__main__":
    unittest.main()


class KlinesFailTests(unittest.IsolatedAsyncioTestCase):
    async def test_dead_host_rotates_and_failure_is_skipped(self):
        calls = []

        class H:
            async def aget(self, url, params=None):
                calls.append(url)
                if "data-api" in url:
                    raise TimeoutError("read timed out")
                t0 = params["startTime"] // 1000
                if t0 >= 1200:
                    raise TimeoutError("all dead here")
                return [[(t0 + i) * 1000, 0, 0, 0, "100"] for i in range(params["limit"])]

        orig = research.asyncio.sleep

        async def nosleep(_):
            await orig(0)
        research.asyncio.sleep = nosleep
        try:
            kl = research.Klines(H())
            await kl.load("BTCUSDT", 0, 1500, progress=False)
        finally:
            research.asyncio.sleep = orig
        self.assertIn(0, kl.close["BTCUSDT"])
        self.assertIn(1199, kl.close["BTCUSDT"])
        self.assertEqual(kl.failed, 1)                   # кусок 1200-1500 пропущен, но без падения
        self.assertTrue(any("api.binance.com" in c for c in calls))


class StudyTests(unittest.TestCase):
    def test_study_map(self):
        tmp = Path(tempfile.mkdtemp())
        try:
            with (tmp / "outcomes.csv").open("w") as f:
                f.write("slug,asset,window,start,winner,d_spot_bps,d_t60_bps,d_wt_bps\nA,btc,5m,0,Up,,,\nB,btc,5m,0,Down,,,\n")
            cols = fvsim.SNAP_COLS
            with (tmp / "snapshots.csv").open("w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=cols)
                w.writeheader()
                w.writerow({"slug": "A", "tau": 20, "p_t60": 0.85, "up_ask": 0.80, "down_ask": 0.22})
                w.writerow({"slug": "B", "tau": 200, "p_t60": 0.85, "up_ask": 0.80, "down_ask": 0.22})
            txt = fvsim.study(tmp)
            self.assertIn("снимков 2", txt)
            self.assertIn("<30с", txt)
            self.assertIn("[1]", txt)
        finally:
            shutil.rmtree(tmp)

    def test_min_p_filter(self):
        self.assertTrue(any(v.get("min_p") for v in fvsim.DEFAULTS["variants"]))
