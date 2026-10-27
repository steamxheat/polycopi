import asyncio
import csv
import json
import shutil
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from core import (Portfolio, end_ts_from_slug, parse_trade, resolution_payouts,  # noqa: E402
                  walk_book)
from net import Http, WebSocket, WsClosed  # noqa: E402
import simbot  # noqa: E402
from simbot import Api, Engine, FakeApi, load_config, poll_loop, report, resim, rtds_loop  # noqa: E402
import fake_polymarket as fp  # noqa: E402

GF = "0x0ca4e9d27ff826a109ecbf74754fb66b93f277a6"
UP, DOWN = "111", "222"
SLUG = "btc-updown-5m-1793093100"          # закрывается в 1793093400


def trade(side="BUY", price=0.84, size=5, asset=DOWN, ts=1793093199, tx="0xaa", usdc=None,
          outcome="Down", wallet=GF, slug=SLUG):
    """Формат — как в реальном ответе data-api.polymarket.com/activity."""
    return {"proxyWallet": wallet, "timestamp": ts, "conditionId": "0xcond", "type": "TRADE",
            "size": size, "usdcSize": usdc if usdc is not None else size * price,
            "transactionHash": tx, "price": price, "asset": asset, "side": side,
            "outcomeIndex": 1, "title": "Bitcoin Up or Down - October 27, 5:25AM-5:30AM ET",
            "slug": slug, "eventSlug": slug, "outcome": outcome, "name": "GoodFallen"}


def book(asks=(), bids=(), mos="5"):
    # Как у CLOB: уровни строками, asks по убыванию (лучший — последний)
    return {"asset_id": "x", "min_order_size": mos, "tick_size": "0.01",
            "asks": [{"price": str(p), "size": str(s)} for p, s in sorted(asks, reverse=True)],
            "bids": [{"price": str(p), "size": str(s)} for p, s in sorted(bids)]}


def market(winner=None, closed=True):
    prices = ["0.5", "0.5"] if winner is None else (["1", "0"] if winner == "Up" else ["0", "1"])
    return {"slug": SLUG, "closed": closed, "endDate": "2026-10-27T09:30:00Z",
            "clobTokenIds": json.dumps([UP, DOWN]), "outcomePrices": json.dumps(prices)}


def cfg(**kw):
    c = load_config(ROOT / "config.json")
    c.update({"leaders": {"goodfallen": GF}, "resolve_every_s": 0}, **kw)
    return c


# ================================================================== core

class CoreTests(unittest.TestCase):
    def test_parse_real_sample_and_fee(self):
        # Реальная сделка goodfallen: 5 акций Down по 0.84, заплатил 4.24704
        t = parse_trade(trade(usdc=4.24704))
        self.assertEqual(t.side, "BUY")
        self.assertAlmostEqual(t.implied_fee_coef, 0.07, places=6)
        t2 = parse_trade(trade(price=0.16, size=7.5, usdc=1.27056, asset=UP))
        self.assertAlmostEqual(t2.implied_fee_coef, 0.07, places=6)
        self.assertIsNone(parse_trade({"type": "REDEEM", "side": "BUY"}))
        self.assertIsNone(parse_trade({"side": "BUY"}))

    def test_slug_end(self):
        self.assertEqual(end_ts_from_slug("btc-updown-5m-1793091900"), 1793092200)
        self.assertEqual(end_ts_from_slug("eth-updown-15m-1793091600"), 1793092500)
        self.assertEqual(end_ts_from_slug("btc-updown-4h-1793088000"), 1793102400)
        self.assertIsNone(end_ts_from_slug("bitcoin-up-or-down-on-october-27-2026"))

    def test_walk_book(self):
        b = book(asks=[(0.85, 3), (0.86, 4), (0.90, 100)], bids=[(0.80, 2), (0.82, 3)])
        f = walk_book(b, "BUY", 6, limit=0.86, fee_coef=0.07)
        self.assertAlmostEqual(f.shares, 6)
        self.assertAlmostEqual(f.notional, 3 * 0.85 + 3 * 0.86)
        self.assertEqual(f.best, 0.85)
        self.assertAlmostEqual(f.fee, 0.07 * (3 * .85 * .15 + 3 * .86 * .14))
        f = walk_book(b, "BUY", 100, limit=0.86, fee_coef=0)      # частичное исполнение
        self.assertAlmostEqual(f.shares, 7)
        f = walk_book(b, "BUY", 5, limit=0.84, fee_coef=0)         # выше лимита — ничего
        self.assertEqual(f.shares, 0)
        f = walk_book(b, "SELL", 4, limit=0.80, fee_coef=0)        # SELL с лучшего бида
        self.assertAlmostEqual(f.notional, 3 * 0.82 + 1 * 0.80)

    def test_portfolio(self):
        p = Portfolio(100, 100)
        t = parse_trade(trade())
        p.buy(t, 10, 8.5, 0.1, None)
        self.assertAlmostEqual(p.cash, 91.4)
        pnl = p.sell(DOWN, 5, 4.6, 0.0)          # половину продали
        self.assertAlmostEqual(pnl, 4.6 - 4.3)
        self.assertAlmostEqual(p.settle(DOWN, 1), 5 - 4.3)
        self.assertEqual((p.wins, p.losses), (1, 0))
        self.assertAlmostEqual(p.cash, 100 - 8.6 + 4.6 + 5)
        p2 = Portfolio.from_dict(json.loads(json.dumps(p.to_dict())))
        self.assertAlmostEqual(p2.realized, p.realized)

    def test_resolution(self):
        self.assertEqual(resolution_payouts(market("Up")), {UP: 1, DOWN: 0})
        self.assertIsNone(resolution_payouts(market(None)))           # цены ещё не 0/1
        self.assertIsNone(resolution_payouts(market("Up", closed=False)))
        m = market("Down")
        m["clobTokenIds"], m["outcomePrices"] = [UP, DOWN], ["0", "1"]   # и списки тоже
        self.assertEqual(resolution_payouts(m), {UP: 0, DOWN: 1})

    def test_report_forecast_after_twelve_hours(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            real = Portfolio(100, 100)
            real.realized, real.wins, real.losses = 3, 20, 10
            ideal = Portfolio(100, 100)
            (out / "state.json").write_text(json.dumps({
                "started": 0,
                "saved": 12 * 3600,
                "real": real.to_dict(),
                "ideal": ideal.to_dict(),
                "counters": {},
                "stats": {"lag": [], "book_ms": [], "slip": [], "fee_coef": []},
            }))
            self.assertIn("Прогноз на 30 дней", report(out))


# ================================================================== engine

class EngineTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def _engine(self, **kw):
        api = FakeApi()
        api.t = 1793093200.0            # узнали через 1 с после сделки лидера
        return api, Engine(cfg(**kw), api, self.tmp, quiet=True)

    async def test_buy_merge_dedup_settle(self):
        api, e = self._engine()
        t1 = trade(tx="0x1")
        api.current_key = parse_trade(t1).key
        api.books[api.current_key] = book(asks=[(0.85, 3), (0.86, 50)])
        await e.on_trade(t1, "rtds")
        await e.on_trade(t1, "poll")                        # тот же fill из опроса -> дубль
        await e.on_trade(trade(tx="0x2", size=7), "rtds")   # дочернее исполнение -> сведено
        self.assertEqual(e.counters["copied_buy"], 1)
        self.assertEqual(e.counters["dup_poll"], 1)
        self.assertEqual(e.counters["merged_fill"], 1)
        pos = e.leaders["goodfallen"].real.positions[DOWN]
        # $2 / 0.84 = 2.38 акций < мин. 5 -> поднимаем до 5; 3 по 0.85 + 2 по 0.86
        self.assertAlmostEqual(pos.shares, 5)
        self.assertAlmostEqual(e.leaders["goodfallen"].stats["slip"][0], (0.854 - 0.84) * 100, places=6)

        api.t = 1793093420
        api.markets[SLUG] = [(1793093410, market("Down"))]
        await e.resolve_once()
        self.assertEqual(e.leaders["goodfallen"].real.positions, {})
        cost = 3 * .85 + 2 * .86 + 0.07 * (3 * .85 * .15 + 2 * .86 * .14)
        self.assertAlmostEqual(e.leaders["goodfallen"].real.realized, 5 - cost, places=6)
        ideal_cost = 5 * .84 + 0.07 * 5 * .84 * .16
        self.assertAlmostEqual(e.leaders["goodfallen"].ideal.realized, 5 - ideal_cost, places=6)

        e.save_state()
        rows = list(csv.DictReader((self.tmp / "trades.csv").open()))
        self.assertEqual([r["action"] for r in rows], ["BUY", "SETTLE"])
        txt = report(self.tmp)
        self.assertIn("Реализованный PnL: +0.6", txt)
        self.assertIn("цена опоздания", txt)
        # перезапуск подхватывает состояние
        e2 = Engine(cfg(), api, self.tmp, quiet=True)
        self.assertAlmostEqual(e2.leaders["goodfallen"].real.realized, e.leaders["goodfallen"].real.realized)

    async def test_skips(self):
        api, e = self._engine()
        # слишком близко к концу: рынок закрывается в ...400, сейчас ...395
        api.t = 1793093395
        t = trade(tx="0x3", ts=1793093394)
        api.current_key = parse_trade(t).key
        api.books[api.current_key] = book(asks=[(0.85, 10)])
        await e.on_trade(t, "rtds")
        self.assertEqual(e.counters.get("skip_too_close_to_end"), 1)
        # нет ликвидности в пределах +2¢
        api.t = 1793093200
        t = trade(tx="0x4", asset=UP, outcome="Up", price=0.16)
        api.current_key = parse_trade(t).key
        api.books[api.current_key] = book(asks=[(0.30, 100)])
        await e.on_trade(t, "rtds")
        self.assertEqual(e.counters.get("skip_no_liquidity_in_limit"), 1)
        # не тот кошелёк — игнор
        await e.on_trade(trade(wallet="0xdead", tx="0x5"), "rtds")
        self.assertNotIn("first_rtds", {k for k in e.counters if e.counters[k] > 2})

    async def test_no_cash(self):
        api, e = self._engine(start_cash=3)
        t = trade(tx="0x6")
        api.current_key = parse_trade(t).key
        api.books[api.current_key] = book(asks=[(0.85, 100)])
        await e.on_trade(t, "rtds")
        self.assertEqual(e.counters.get("skip_no_cash"), 1)
        self.assertAlmostEqual(e.leaders["goodfallen"].real.cash, 3)

    async def test_sell_proportional(self):
        api, e = self._engine()
        b = trade(tx="0x7", size=100)
        api.current_key = parse_trade(b).key
        api.books[api.current_key] = book(asks=[(0.84, 100)])
        await e.on_trade(b, "rtds")
        s = trade(tx="0x8", side="SELL", size=50, price=0.90, ts=1793093250)
        api.t = 1793093251
        api.current_key = parse_trade(s).key
        api.books[api.current_key] = book(bids=[(0.89, 100)])
        await e.on_trade(s, "rtds")
        self.assertAlmostEqual(e.leaders["goodfallen"].real.positions[DOWN].shares, 2.5)  # лидер продал половину
        self.assertEqual(e.counters["copied_sell"], 1)

    async def test_resim_from_recording(self):
        api, e = self._engine()
        for i, (tx, asset, outc, price) in enumerate([("0xa", DOWN, "Down", .84),
                                                       ("0xb", UP, "Up", .16)]):
            t = trade(tx=tx, asset=asset, outcome=outc, price=price, ts=1793093199 + i * 5)
            api.t = t["timestamp"] + 1
            api.current_key = parse_trade(t).key
            api.books[api.current_key] = book(asks=[(price + .01, 3), (price + .03, 100)])
            await e.on_trade(t, "rtds")
        api.t = 1793093420
        api.markets[SLUG] = [(1793093410, market("Down"))]
        await e.resolve_once()
        e.save_state()
        base = e.leaders["goodfallen"].real.realized
        out = await resim(cfg(), self.tmp, self.tmp / "resim")
        self.assertIn("Реализованный PnL", out)
        r = json.loads((self.tmp / "resim" / "state.json").read_text())
        self.assertAlmostEqual(r["leaders"]["goodfallen"]["real"]["realized"], base, places=6)   # тот же результат
        await resim(cfg(max_slippage=0.005), self.tmp, self.tmp / "resim")
        r2 = json.loads((self.tmp / "resim" / "state.json").read_text())
        self.assertNotAlmostEqual(r2["leaders"]["goodfallen"]["real"]["realized"], base, places=6)  # другие настройки


# ================================================================== network (локально)

class NetTests(unittest.IsolatedAsyncioTestCase):
    async def test_websocket_client(self):
        async def handler(reader, writer):
            writer.write(fp.ws_frame(0x9, b"hi"))                     # ping от сервера
            writer.write(fp.ws_frame(0x1, b'{"a":', fin=False))       # фрагменты
            writer.write(fp.ws_frame(0x0, b'"b"}'))
            big = ("x" * 70000).encode()
            writer.write(fp.ws_frame(0x1, big))                       # 64-битная длина
            await writer.drain()
            op, masked, data = await fp.ws_read(reader)               # ожидаем pong
            assert op == 0xA and masked and data == b"hi", (op, masked, data)
            op, masked, data = await fp.ws_read(reader)               # эхо
            writer.write(fp.ws_frame(0x1, data))
            writer.write(fp.ws_frame(0x8, b""))
            await writer.drain()
            await asyncio.sleep(0.1)

        srv, url = await fp.start_ws(handler)
        ws = await WebSocket.connect(url)
        self.assertEqual(await ws.recv(), '{"a":"b"}')
        self.assertEqual(len(await ws.recv()), 70000)
        await ws.send("привет")
        self.assertEqual(await ws.recv(), "привет")
        with self.assertRaises(WsClosed):
            await ws.recv()
        await ws.close()
        srv.close()

    def test_http_keepalive(self):
        st = fp.FakeState()
        st.books["9"] = book(asks=[(0.5, 1)])
        srv, base = fp.start_http(st)
        h = Http()
        for _ in range(5):
            self.assertEqual(h.get_json(base + "/book", {"token_id": "9"})["asks"][0]["price"], "0.5")
        self.assertEqual(st.connections, 1)          # одно соединение на 5 запросов
        srv.shutdown()

    async def test_full_loop_against_fake_polymarket(self):
        """Весь бот целиком: настоящие сетевые циклы против локального Polymarket."""
        st = fp.FakeState()
        old = trade(tx="0xold", ts=1793093000)
        st.activity[GF] = [old]                      # история до старта — не копируется
        now = time.time()
        slug = f"btc-updown-5m-{int(now) // 300 * 300}"
        st.books[DOWN] = book(asks=[(0.85, 3), (0.86, 50)])
        st.books[UP] = book(asks=[(0.17, 100)])
        srv, base = fp.start_http(st)

        sent = asyncio.Event()

        async def rtds(reader, writer):
            op, _, data = await fp.ws_read(reader)
            assert json.loads(data)["subscriptions"][0]["topic"] == "activity"
            other = trade(wallet="0xother", tx="0xz", slug=slug)
            writer.write(fp.ws_frame(0x1, json.dumps({"topic": "activity", "type": "trades",
                                                      "payload": other}).encode()))
            t = trade(tx="0xrtds", ts=int(time.time()), slug=slug)
            writer.write(fp.ws_frame(0x1, json.dumps({"topic": "activity", "type": "trades",
                                                      "timestamp": int(time.time() * 1000),
                                                      "payload": t}).encode()))
            await writer.drain()
            sent.set()
            while True:
                await fp.ws_read(reader)

        wsrv, wurl = await fp.start_ws(rtds)
        tmp = Path(tempfile.mkdtemp())
        c = cfg(clob_url=base, data_url=base, gamma_url=base, rtds_url=wurl,
                poll_interval_s=0.2, min_seconds_left=0)
        e = Engine(c, Api(c), tmp, quiet=True)
        stop = asyncio.Event()
        tasks = [asyncio.ensure_future(rtds_loop(e, stop)),
                 asyncio.ensure_future(poll_loop(e, GF, stop))]
        await asyncio.wait_for(sent.wait(), 5)
        await asyncio.sleep(0.5)
        # новая сделка появляется в Data API (её видит только опрос)
        st.activity[GF].insert(0, trade(tx="0xpoll", asset=UP, outcome="Up", price=0.16,
                                        ts=int(time.time()), slug=slug))
        await asyncio.sleep(0.8)
        stop.set()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        wsrv.close()
        srv.shutdown()
        self.assertEqual(e.counters.get("first_rtds"), 1)
        self.assertEqual(e.counters.get("first_poll"), 1)
        self.assertEqual(e.counters.get("copied_buy"), 2)
        self.assertIn(DOWN, e.leaders["goodfallen"].real.positions)
        self.assertIn(UP, e.leaders["goodfallen"].real.positions)
        self.assertNotIn("0xold", json.dumps(e.counters))
        self.assertEqual(sum(1 for r in st.requests if r == "/book"), 2)
        shutil.rmtree(tmp)



# ================================================================== v2: несколько трейдеров, логи, миграция

AL = "0x3725d52f3c252e8374999cc8617292ea2608ad88"
PR = "0xd38b71f3e8ed1af71983e5c309eac3dfa9b35029"


def cfg6(**kw):
    """Боевой config.json из репозитория (6 трейдеров)."""
    c = load_config(ROOT / "config.json")
    c.update({"resolve_every_s": 0}, **kw)
    return c


class MultiLeaderTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_config_normalize(self):
        c = cfg6()
        self.assertEqual(len(c["leaders"]), 6)
        self.assertEqual(c["leaders"]["primm"], PR)
        self.assertEqual(c["leader_poll"][PR], 3)
        self.assertNotIn(GF, c["leader_poll"])          # строковая запись — общий интервал

    async def test_separate_accounts_same_market(self):
        api = FakeApi()
        api.t = 1793093200.0
        e = Engine(cfg6(), api, self.tmp, quiet=True)
        self.assertEqual(set(e.leaders), {"goodfallen", "almach", "primm", "kch123",
                                          "BreakTheBank", "omnibus-076daa"})
        for w, tx in ((GF, "0xg"), (AL, "0xa")):
            t = trade(wallet=w, tx=tx)
            api.current_key = parse_trade(t).key
            api.books[api.current_key] = book(asks=[(0.85, 100)])
            await e.on_trade(t, "rtds")
        gf, al = e.leaders["goodfallen"], e.leaders["almach"]
        self.assertIn(DOWN, gf.real.positions)
        self.assertIn(DOWN, al.real.positions)
        self.assertAlmostEqual(gf.real.cash, al.real.cash)
        self.assertAlmostEqual(e.leaders["primm"].real.cash, 100)   # чужие сделки не трогают
        api.t = 1793093420
        api.markets[SLUG] = [(1793093410, market("Up"))]
        await e.resolve_once()
        self.assertEqual(gf.real.losses, 1)
        self.assertEqual(al.real.losses, 1)
        e.save_state()
        txt = report(self.tmp)
        self.assertIn("трейдер", txt)
        for n in ("goodfallen", "almach", "primm", "omnibus-076daa"):
            self.assertIn(f"--- {n}", txt)
        one = report(self.tmp, "almach")
        self.assertIn("--- almach", one)
        self.assertNotIn("--- goodfallen", one)

    async def test_sports_market_not_skipped_after_gamma_end(self):
        """У спорта endDate из gamma может быть в прошлом, а рынок ещё торгуется."""
        api = FakeApi()
        api.t = 1793093200.0
        e = Engine(cfg6(), api, self.tmp, quiet=True)
        slug = "cfb-texas-tech-cincinnati-2026-10-25"
        m = market(None, closed=False)
        m.update(slug=slug, endDate="2026-10-25T00:00:00Z")
        api.markets[slug] = [(0, m)]
        t = trade(wallet=PR, tx="0xp", price=0.60, slug=slug, outcome="Cincinnati")
        api.current_key = parse_trade(t).key
        api.books[api.current_key] = book(asks=[(0.61, 100)], bids=[(0.59, 100)])
        await e.on_trade(t, "poll")
        self.assertEqual(e.leaders["primm"].counters.get("copied_buy"), 1)

    async def test_resolver_backoff(self):
        api = FakeApi()
        api.t = 1793093200.0
        e = Engine(cfg6(resolve_every_s=20), api, self.tmp, quiet=True)
        t = trade(tx="0xq")
        api.current_key = parse_trade(t).key
        api.books[api.current_key] = book(asks=[(0.85, 100)])
        await e.on_trade(t, "rtds")
        api.t = 1793093500
        api.markets[SLUG] = [(0, market(None))]          # ещё не рассчитан
        await e.resolve_once()
        first = e.resolve_next[SLUG] - api.t
        api.t = e.resolve_next[SLUG]
        await e.resolve_once()
        self.assertGreater(e.resolve_next[SLUG] - api.t, first)   # интервал растёт

    async def test_detailed_logs(self):
        simbot.setup_logging(self.tmp, cfg6(), quiet=True)
        api = FakeApi()
        api.t = 1793093200.0
        e = Engine(cfg6(), api, self.tmp)
        t = trade(tx="0xlog", usdc=4.24704)
        api.current_key = parse_trade(t).key
        api.books[api.current_key] = book(asks=[(0.85, 3), (0.86, 50)], bids=[(0.83, 10)])
        await e.on_trade(t, "rtds")
        t2 = trade(tx="0xlog2", asset=UP, outcome="Up", price=0.16)
        api.current_key = parse_trade(t2).key
        api.books[api.current_key] = book(asks=[(0.40, 100)])
        await e.on_trade(t2, "rtds")
        e.log_status()
        for h in simbot.log.handlers:
            h.flush()
        txt = (self.tmp / "bot.log").read_text()
        for needle in ("FILL  goodfallen [rtds] BUY Down @0.84", "fee_coef 0.070",
                       "BOOK  goodfallen Down bid 0.83 / ask 0.85", "BUY  goodfallen",
                       "SKIP goodfallen", "no_liquidity_in_limit", "СТАТУС"):
            self.assertIn(needle, txt)
        decs = [json.loads(l) for l in (self.tmp / "decisions.jsonl").read_text().splitlines()]
        self.assertEqual([d["action"] for d in decs], ["BUY", "SKIP"])
        self.assertEqual(decs[0]["book"]["best_ask"], 0.85)
        self.assertEqual(decs[0]["book"]["spread_c"], 2.0)
        self.assertEqual(decs[0]["levels_taken"], 2)
        rows = list(csv.DictReader((self.tmp / "trades.csv").open()))
        self.assertEqual(rows[0]["best_bid"], "0.83")
        self.assertEqual(rows[0]["leader_fee_coef"], "0.0700")
        self.assertEqual(rows[0]["tx"], "0xlog")
        simbot.log.handlers.clear()

    async def test_migration_from_v1_state(self):
        """Старый общий счёт с сервера раскладывается по трейдерам без потерь."""
        p_gf = {"asset": DOWN, "shares": 5, "cost": 4.3, "slug": SLUG, "condition_id": "c",
                "outcome": "Down", "title": "BTC", "leader": "goodfallen", "end_ts": None}
        p_al = dict(p_gf, asset=UP, outcome="Up", cost=1.0, leader="almach")
        v1 = {"started": 1793000000, "saved": 1793090000,
              "real": {"cash": 100 - 4.3 - 1.0 + 0.7 - 0.4, "start_cash": 100,
                       "positions": {DOWN: p_gf, UP: p_al}, "realized": 0.3, "fees": 0.1,
                       "wins": 1, "losses": 1},
              "ideal": {"cash": 95, "start_cash": 100, "positions": {DOWN: p_gf}, "realized": 0.5,
                        "fees": 0, "wins": 1, "losses": 0},
              "counters": {"signals": 4}, "stats": {"lag": [1], "book_ms": [], "slip": [1.2],
                                                     "fee_coef": []},
              "end_cache": {}}
        (self.tmp / "state.json").write_text(json.dumps(v1))
        old_cols = ["time_utc", "leader", "source", "action", "reason", "market", "outcome",
                    "leader_price", "leader_size", "lag_s", "book_ms", "best_price", "our_vwap",
                    "slip_cents", "our_shares", "our_cost", "fee", "pnl", "cash", "equity_real",
                    "equity_ideal"]
        with (self.tmp / "trades.csv").open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=old_cols)
            w.writeheader()
            w.writerow({"leader": "goodfallen", "action": "BUY", "fee": "0.05"})
            w.writerow({"leader": "goodfallen", "action": "SETTLE", "reason": "win", "pnl": "0.7"})
            w.writerow({"leader": "almach", "action": "BUY", "fee": "0.05"})
            w.writerow({"leader": "almach", "action": "SETTLE", "reason": "loss", "pnl": "-0.4"})
        api = FakeApi()
        api.t = 1793093200.0
        e = Engine(cfg6(), api, self.tmp, quiet=True)
        gf, al = e.leaders["goodfallen"], e.leaders["almach"]
        self.assertAlmostEqual(gf.real.realized, 0.7)
        self.assertAlmostEqual(al.real.realized, -0.4)
        self.assertEqual((gf.real.wins, al.real.losses), (1, 1))
        self.assertAlmostEqual(gf.real.cash, 100 + 0.7 - 4.3)
        self.assertAlmostEqual(al.real.cash, 100 - 0.4 - 1.0)
        self.assertIn(DOWN, gf.real.positions)
        self.assertIn(UP, al.real.positions)
        self.assertIn(DOWN, gf.ideal.positions)
        self.assertAlmostEqual(e.leaders["primm"].real.cash, 100)
        self.assertEqual(e.counters["signals"], 4)
        self.assertTrue(list(self.tmp.glob("state_v1_backup_*.json")))   # старое не потеряно
        self.assertTrue((self.tmp / "trades_v1.csv").exists())
        self.assertFalse((self.tmp / "trades.csv").exists())
        e.save_state()
        e2 = Engine(cfg6(), api, self.tmp, quiet=True)                   # повторный старт
        self.assertAlmostEqual(e2.leaders["goodfallen"].real.realized, 0.7)
        self.assertEqual(len(list(self.tmp.glob("state_v1_backup_*.json"))), 1)
        txt = report(self.tmp)
        self.assertIn("разделён по трейдерам", txt)


if __name__ == "__main__":
    unittest.main(verbosity=2)
