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

import mmsim  # noqa: E402
from mmsim import Engine, PriceFeed, floor_tick, load_config, report  # noqa: E402
import fake_polymarket as fp  # noqa: E402

START = 1793112000                      # начало 5-минутного окна
SLUG = f"btc-updown-5m-{START}"
UP, DN = "111", "222"
COND = "0xcond"


def gamma_market(closed=False, winner=None, slug=SLUG, up=UP, dn=DN, cond=COND):
    prices = ["0.5", "0.5"] if winner is None else (["1", "0"] if winner == "Up" else ["0", "1"])
    return {"slug": slug, "conditionId": cond, "closed": closed, "outcomes": json.dumps(["Up", "Down"]),
            "outcomePrices": json.dumps(prices), "clobTokenIds": json.dumps([up, dn]),
            "orderPriceMinTickSize": 0.01}


def book(bids=(), asks=()):
    return {"bids": [{"price": str(p), "size": str(s)} for p, s in sorted(bids)],
            "asks": [{"price": str(p), "size": str(s)} for p, s in sorted(asks, reverse=True)]}


def tr(token, price, size, ts, tx="0x"):
    return {"asset": token, "price": price, "size": size, "timestamp": ts, "transactionHash": tx,
            "side": "SELL", "proxyWallet": "0xw"}


class FakeApi:
    def __init__(self):
        self.t = START + 10.0
        self.markets = {}            # (slug, closed) -> market
        self.books = {UP: book(bids=[(0.49, 100)], asks=[(0.51, 100)]),
                      DN: book(bids=[(0.49, 100)], asks=[(0.51, 100)])}
        self.trades = {}             # cond -> list

    def now(self):
        return self.t

    async def get_market(self, slug, closed=None):
        m = self.markets.get(slug)
        if not m:
            return None
        if closed is None:                       # как gamma: без параметра закрытых не видно
            return None if m["closed"] else m
        return m if bool(m["closed"]) == closed else None

    async def get_book(self, token):
        return self.books[token]

    async def get_trades(self, cond, limit=500):
        return list(self.trades.get(cond, []))

    async def get_prices(self, syms):
        return {s: 100000.0 for s in syms}


def cfg(**kw):
    c = load_config(ROOT / "mm_config.json")
    c.update({"assets": {"btc": "BTCUSDT"}, "resolve_every_s": 0}, **kw)
    return c


class MMTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.api = FakeApi()
        self.api.markets[SLUG] = gamma_market()

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def eng(self, **kw):
        return Engine(cfg(**kw), self.api, self.tmp, quiet=True)

    async def test_t60_fair_mode(self):
        import math
        e = self.eng(fair_mode="t60")
        # цены биржи: минуту до старта на 20 б.п. ниже, со старта — выше и стоят
        t = START - 700
        while t <= START + 10:
            px = 100000 * (1 + 2e-5 * math.sin(t)) * (0.998 if t < START - 1 else 1.0)
            for src in ("coinbase_ws", "okx_ws"):
                e.prices.add("BTCUSDT", float(t), px, src)
            self.api.t = float(t)
            e.sampler.tick(float(t))
            t += 1
        self.api.t = START + 10.0
        await e.step()
        mk = e.markets[SLUG]
        fair = e.model_fair(mk)
        self.assertGreater(fair, 0.75)                     # по t60 Up почти решён, стакан 0.50 этого «не знает»
        self.assertEqual(len(mk.pairs), 0)                 # >0.8 -> за пределами max_mid? проверим ниже
        e2 = self.eng(fair_mode="t60", max_mid=0.99)
        e2.prices, e2.sampler = e.prices, e.sampler
        await e2.step()
        p = e2.pairs[e2.markets[SLUG].pairs[0]]
        ou, od = e2.orders[p.up], e2.orders[p.down]
        self.assertEqual(ou.price, 0.50)                   # остаёмся мейкером: на тик ниже ask 0.51
        self.assertLess(od.price, 0.2)                     # Down котируем дёшево, по модели
        self.assertGreater(p.fair_up, 0.75)

    async def test_guard_mode(self):
        import math
        e = self.eng(fair_mode="guard", max_mid=0.99)
        t = START - 700
        while t <= START + 10:
            px = 100000 * (1 + 2e-5 * math.sin(t)) * (0.998 if t < START - 1 else 1.0)
            for src in ("coinbase_ws", "okx_ws"):
                e.prices.add("BTCUSDT", float(t), px, src)
            self.api.t = float(t)
            e.sampler.tick(float(t))
            t += 1
        self.api.t = START + 10.0
        await e.step()                                    # модель ~0.9, стакан 0.50 -> опасно, не котируем
        self.assertEqual(len(e.markets[SLUG].pairs), 0)
        self.assertGreaterEqual(e.counters.get("skip_guard", 0), 1)
        e2 = self.eng(fair_mode="guard", guard_max_dev=0.6)  # модель и стакан «согласны» -> котируем вокруг стакана
        e2.prices, e2.sampler = e.prices, e.sampler
        await e2.step()
        p = e2.pairs[e2.markets[SLUG].pairs[0]]
        self.assertEqual((e2.orders[p.up].price, e2.orders[p.down].price), (0.48, 0.48))

    async def test_floor_tick(self):
        self.assertEqual(floor_tick(0.479999, 0.01), 0.47)
        self.assertEqual(floor_tick(0.48, 0.01), 0.48)

    async def test_quote_and_complete_pair_with_queue(self):
        e = self.eng()
        await e.step()
        self.assertIn(SLUG, e.markets)
        mk = e.markets[SLUG]
        self.assertEqual(len(mk.pairs), 1)
        p = e.pairs[mk.pairs[0]]
        ou, od = e.orders[p.up], e.orders[p.down]
        # справедливая Up = 0.50, edge 0.04 -> 0.48 / 0.48
        self.assertEqual((ou.price, od.price), (0.48, 0.48))
        self.assertAlmostEqual(e.acct.reserved, 0.48 * 5 * 2)
        self.assertEqual(ou.queue_ahead, 0)          # на 0.48 в стакане никого
        # сделка в ту же секунду, что выставление, не считается (пессимистично)
        self.api.trades[COND] = [tr(UP, 0.47, 50, START + 10, "0xa")]
        await e.step()
        self.assertEqual(ou.filled, 0)
        # сделка ниже нашей цены на следующей секунде -> Up исполнен
        self.api.t = START + 12
        self.api.trades[COND].append(tr(UP, 0.47, 50, START + 11, "0xb"))
        await e.step()
        self.assertEqual(ou.filled, 5)
        self.assertEqual(p.state, "one_leg")
        # сделка ровно по 0.48 по Down — очередь пустая -> исполнен -> пара полная
        self.api.t = START + 20
        self.api.trades[COND].append(tr(DN, 0.48, 3, START + 19, "0xc"))
        self.api.trades[COND].append(tr(DN, 0.48, 3, START + 19, "0xd"))
        await e.step()
        self.assertEqual(od.filled, 5)
        self.assertEqual(p.state, "complete")
        # пара закрыта -> бот сразу выставил следующую (резерв только под неё)
        nxt = e.pairs[mk.pairs[-1]]
        self.assertNotEqual(nxt.id, p.id)
        self.assertAlmostEqual(e.acct.reserved, 5 * (e.orders[nxt.up].price + e.orders[nxt.down].price))
        # рынок закрылся, победил Down
        self.api.t = START + 320
        self.api.markets[SLUG] = gamma_market(closed=True, winner="Down")
        await e.step()
        self.assertAlmostEqual(e.acct.realized, 5 * (1 - 0.96), places=6)
        self.assertAlmostEqual(e.acct.cash, 100 + 0.2, places=6)
        self.assertEqual(e.counters["out_complete"], 1)
        rows = list(csv.DictReader((self.tmp / "pairs.csv").open()))
        self.assertEqual(rows[0]["outcome"], "complete")
        self.assertNotIn(SLUG, e.markets)            # память очищена

    async def test_queue_ahead_must_be_eaten_first(self):
        self.api.books[UP] = book(bids=[(0.48, 20), (0.47, 5)], asks=[(0.52, 10)])
        e = self.eng()
        await e.step()
        mk = e.markets[SLUG]
        p = e.pairs[mk.pairs[0]]
        ou = e.orders[p.up]
        self.assertEqual(ou.price, 0.48)
        self.assertEqual(ou.queue_ahead, 20)
        self.api.t = START + 15
        self.api.trades[COND] = [tr(UP, 0.48, 18, START + 13, "0x1")]
        await e.step()
        self.assertEqual(ou.filled, 0)
        self.assertAlmostEqual(ou.queue_ahead, 2)
        self.api.trades[COND].append(tr(UP, 0.48, 4, START + 14, "0x2"))
        await e.step()
        self.assertAlmostEqual(ou.filled, 2)         # 2 ушли на очередь, 2 нам

    async def test_one_leg_loss_and_report(self):
        e = self.eng()
        await e.step()
        p = e.pairs[e.markets[SLUG].pairs[0]]
        self.api.t = START + 30
        self.api.trades[COND] = [tr(UP, 0.40, 50, START + 29, "0x1")]
        await e.step()
        self.api.t = START + 320
        self.api.markets[SLUG] = gamma_market(closed=True, winner="Down")
        await e.step()
        self.assertAlmostEqual(e.acct.realized, -0.48 * 5, places=6)
        self.assertAlmostEqual(e.acct.cash + e.acct.reserved, 100 - 2.4, places=6)
        self.assertEqual(e.counters["out_one_leg_loss"], 1)
        e.save()
        txt = report(self.tmp)
        self.assertIn("одна нога: 1 (100%)", txt)
        self.assertIn("ГЛАВНОЕ ЧИСЛО", txt)

    async def test_move_cancels_unfilled_but_keeps_hedge_leg(self):
        e = self.eng()
        await e.step()
        mk = e.markets[SLUG]
        p1 = e.pairs[mk.pairs[0]]
        self.api.t += 3                                     # заявка стоит 3 секунды
        e.prices.add("BTCUSDT", self.api.t - 2, 100000)
        e.prices.add("BTCUSDT", self.api.t, 100100)          # +10 б.п.
        e.check_moves()
        self.assertEqual(p1.state, "canceled")
        self.assertEqual(e.orders[p1.up].cancel_reason, "move")
        self.assertEqual(e.counters["canceled_on_move"], 1)
        # сделка в ту же секунду, что и снятие, засчитывается (пессимистично)
        cancel_ts = e.orders[p1.down].canceled_ts
        self.api.t += 1
        self.api.trades[COND] = [tr(DN, 0.45, 50, int(cancel_ts), "0xadv")]
        await e.step()
        self.assertEqual(e.orders[p1.down].filled, 5)
        self.assertEqual(p1.state, "one_leg")                 # пара вернулась с одной ногой
        self.assertEqual(e.counters["late_fills"], 1)
        # пауза после рывка: новые котировки не ставим
        self.assertEqual(len([x for x in mk.pairs]), 1)
        # теперь пара с одной ногой: второй рывок её НЕ снимает
        self.api.t += 20
        e.prices.add("BTCUSDT", self.api.t - 1, 100100)
        e.prices.add("BTCUSDT", self.api.t, 99900)
        e.check_moves()
        self.assertEqual(p1.state, "one_leg")

    async def test_requote_when_fair_moves(self):
        e = self.eng()
        await e.step()
        mk = e.markets[SLUG]
        first = mk.pairs[0]
        self.api.books[UP] = book(bids=[(0.55, 100)], asks=[(0.57, 100)])
        self.api.books[DN] = book(bids=[(0.43, 100)], asks=[(0.45, 100)])
        self.api.t += 2
        await e.step()
        self.assertEqual(e.pairs[first].state, "canceled")
        new = e.pairs[mk.pairs[-1]]
        self.assertNotEqual(new.id, first)
        self.assertEqual((e.orders[new.up].price, e.orders[new.down].price), (0.54, 0.42))

    async def test_maker_only_and_extreme_mid(self):
        self.api.books[UP] = book(bids=[(0.85, 100)], asks=[(0.87, 100)])
        e = self.eng()
        await e.step()
        self.assertEqual(e.markets[SLUG].pairs, [])
        self.assertEqual(e.counters["skip_mid_extreme"], 1)

    async def test_stop_before_end_and_cancel_all(self):
        e = self.eng()
        await e.step()
        mk = e.markets[SLUG]
        p = e.pairs[mk.pairs[0]]
        self.api.t = START + 296
        await e.step()
        self.assertEqual(e.orders[p.up].cancel_reason, "end")
        self.assertEqual(len(mk.pairs), 1)            # новых не ставим

    async def test_chase_second_leg(self):
        e = self.eng(chase_enabled=True, chase_after_s=10)
        await e.step()
        mk = e.markets[SLUG]
        p = e.pairs[mk.pairs[0]]
        self.api.t = START + 20
        self.api.trades[COND] = [tr(UP, 0.40, 50, START + 19, "0x1")]
        await e.step()
        self.assertEqual(p.state, "one_leg")
        self.api.books[DN] = book(bids=[(0.50, 10)], asks=[(0.53, 10)])
        self.api.t = START + 40
        await e.step()
        self.assertTrue(p.chased)
        self.assertEqual(e.orders[p.down].price, 0.51)   # min(0.99-0.48, 0.53-0.01)

    async def test_no_cash(self):
        e = self.eng(start_cash=3)
        await e.step()
        self.assertEqual(e.markets[SLUG].pairs, [])
        self.assertEqual(e.counters["skip_no_cash"], 1)
        self.assertAlmostEqual(e.acct.cash, 3)

    async def test_state_persists(self):
        e = self.eng()
        await e.step()
        e.save()
        e2 = self.eng()
        self.assertEqual(set(e2.pairs), set(e.pairs))
        self.assertAlmostEqual(e2.acct.reserved, e.acct.reserved)

    def test_price_feed(self):
        f = PriceFeed(["BTCUSDT"])
        f.add("BTCUSDT", 100, 100000)
        f.add("BTCUSDT", 101, 100050)
        self.assertAlmostEqual(f.move_bps("BTCUSDT", 101, 3), 5.0, places=3)
        self.assertAlmostEqual(f.move_bps("BTCUSDT", 110, 3), 0.0)


class MMNetTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_api_against_fake_servers(self):
        """Настоящие сетевые вызовы mmsim.Api против локальных серверов."""
        import http.server
        import threading
        from urllib.parse import urlsplit, parse_qs

        mkt = gamma_market()
        state = {"closed": False}

        class H(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def do_GET(self):
                u = urlsplit(self.path)
                q = {k: v[0] for k, v in parse_qs(u.query).items()}
                if u.path == "/markets":
                    want = q.get("closed") == "true"
                    m = dict(mkt, closed=state["closed"])
                    body = [m] if (state["closed"] == want or ("closed" not in q and not state["closed"])) else []
                elif u.path == "/book":
                    body = book(bids=[(0.49, 10)], asks=[(0.51, 10)])
                elif u.path == "/trades":
                    assert q["market"] == COND and q["takerOnly"] == "true"
                    body = [tr(UP, 0.47, 10, 1, "0x")]
                elif u.path == "/api/v3/ticker/price":
                    syms = json.loads(q["symbols"])
                    body = [{"symbol": s, "price": "79761.13"} for s in syms]
                else:
                    body = {}
                data = json.dumps(body).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        base = f"http://127.0.0.1:{srv.server_address[1]}"
        c = cfg(gamma_url=base, clob_url=base, data_url=base, binance_rest=base)
        api = mmsim.Api(c)
        try:
            self.assertEqual((await api.get_market(SLUG))["slug"], SLUG)
            self.assertIsNone(await api.get_market(SLUG, closed=True))
            state["closed"] = True
            self.assertIsNone(await api.get_market(SLUG))
            self.assertTrue((await api.get_market(SLUG, closed=True))["closed"])
            self.assertEqual(len(await api.get_trades(COND)), 1)
            self.assertEqual(await api.get_prices(["BTCUSDT", "ETHUSDT"]),
                             {"BTCUSDT": 79761.13, "ETHUSDT": 79761.13})
        finally:
            srv.shutdown()

    async def test_binance_ws_loop(self):
        async def handler(reader, writer):
            msgs = [{"stream": "btcusdt@aggTrade",
                     "data": {"e": "aggTrade", "s": "BTCUSDT", "p": "100000.0", "T": int(time.time() * 1000)}},
                    {"stream": "btcusdt@aggTrade",
                     "data": {"e": "aggTrade", "s": "BTCUSDT", "p": "100200.0", "T": int(time.time() * 1000)}}]
            for m in msgs:
                writer.write(fp.ws_frame(0x1, json.dumps(m).encode()))
            await writer.drain()
            await asyncio.sleep(1)

        srv, url = await fp.start_ws(handler)
        tmp = Path(tempfile.mkdtemp())
        try:
            c = cfg(binance_ws=url + "/stream")
            e = Engine(c, mmsim.Api(c), tmp, quiet=True)
            stop = asyncio.Event()
            task = asyncio.ensure_future(mmsim.binance_ws_loop(e, stop))
            await asyncio.sleep(0.5)
            stop.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            self.assertEqual(e.prices.last("BTCUSDT"), 100200.0)
            self.assertEqual(e.prices.source, "binance_ws")
            self.assertGreater(e.prices.move_bps("BTCUSDT", time.time(), 3), 19)
        finally:
            srv.close()
            shutil.rmtree(tmp)



class ClobWsTests(unittest.IsolatedAsyncioTestCase):
    async def test_clob_ws_fills_precisely_and_dedupes_api(self):
        got_sub = {}
        q = asyncio.Queue()

        async def handler(reader, writer):
            op, _, data = await fp.ws_read(reader)
            got_sub["msg"] = json.loads(data)
            while True:
                m = await q.get()
                writer.write(fp.ws_frame(0x1, json.dumps(m).encode()))
                await writer.drain()

        srv, url = await fp.start_ws(handler)
        tmp = Path(tempfile.mkdtemp())
        api = FakeApi()
        api.t = time.time()
        start = int(api.t // 300 * 300)
        slug = f"btc-updown-5m-{start}"
        api.markets[slug] = gamma_market(slug=slug)
        api.now = time.time
        try:
            c = cfg(clob_ws=url, start_after_s=0, stop_new_pairs_before_end_s=0,
                    cancel_all_before_end_s=0, order_latency_s=0.3)
            e = Engine(c, api, tmp, quiet=True)
            await e.step()
            mk = e.markets[slug]
            p = e.pairs[mk.pairs[0]]
            ou = e.orders[p.up]
            stop = asyncio.Event()
            task = asyncio.ensure_future(mmsim.clob_ws_loop(e, stop))
            for _ in range(50):
                if got_sub:
                    break
                await asyncio.sleep(0.05)
            self.assertEqual(set(got_sub["msg"]["assets_ids"]), {UP, DN})
            self.assertEqual(got_sub["msg"]["type"], "market")
            # сделка через 0.1 с после выставления — ещё до того, как заявка «дошла» -> не наша
            ms = lambda t: str(int(t * 1000))
            await q.put([{"event_type": "last_trade_price", "asset_id": UP, "price": "0.40",
                          "size": "50", "timestamp": ms(ou.placed_ts + 0.1)}])
            await q.put({"event_type": "last_trade_price", "asset_id": UP, "price": "0.40",
                         "size": "50", "timestamp": ms(ou.placed_ts + 0.5)})
            await q.put({"event_type": "price_change", "asset_id": UP})
            for _ in range(50):
                if ou.filled:
                    break
                await asyncio.sleep(0.05)
            self.assertEqual(ou.filled, 5)
            self.assertEqual(e.counters["trades_ws"], 2)
            # та же сделка из data-api (секундное время) не учитывается второй раз
            await asyncio.sleep(2.1)
            await q.put({"event_type": "price_change", "asset_id": UP})     # лента жива
            await asyncio.sleep(0.2)
            a, b = e.ws_intervals[-1]
            sec = int(a) + 1                                  # секунда целиком внутри периода
            self.assertTrue(e.ws_covered(sec))
            self.assertFalse(e.ws_covered(int(a) - 5))         # до подключения — не покрыто
            api.trades[COND] = [tr(UP, 0.40, 50, sec, "0xdup")]
            await e.poll_trades(mk)
            self.assertEqual(e.counters.get("trades_api", 0), 0)
            stop.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        finally:
            srv.close()
            shutil.rmtree(tmp)



class RtdsPriceTests(unittest.IsolatedAsyncioTestCase):
    def test_parse(self):
        self.assertEqual(mmsim.parse_rtds_price({"topic": "crypto_prices", "type": "update",
                                                 "payload": {"symbol": "btcusdt", "timestamp": 1,
                                                             "value": 79761.13}}),
                         [("BTCUSDT", 79761.13, 1)])
        snap = mmsim.parse_rtds_price({"topic": "crypto_prices", "type": "subscribe",
                                       "payload": {"symbol": "ethusdt",
                                                   "data": [{"timestamp": 1, "value": 2300},
                                                            {"timestamp": 2, "value": "2301.5"}]}})
        self.assertEqual(snap[-1], ("ETHUSDT", 2301.5, 2))
        self.assertEqual(mmsim.parse_rtds_price({"topic": "activity", "payload": {}}), [])
        self.assertEqual(mmsim.parse_rtds_price({"topic": "crypto_prices",
                                                 "payload": {"symbol": "dogeusdt", "value": 1}}), [])

    async def test_rtds_loop_feeds_prices(self):
        got = {}

        async def handler(reader, writer):
            op, _, data = await fp.ws_read(reader)
            got["sub"] = json.loads(data)
            for v in (100000, 100150):
                writer.write(fp.ws_frame(0x1, json.dumps(
                    {"topic": "crypto_prices", "type": "update",
                     "payload": {"symbol": "btcusdt", "timestamp": 1, "value": v}}).encode()))
            await writer.drain()
            while True:
                await fp.ws_read(reader)

        srv, url = await fp.start_ws(handler)
        tmp = Path(tempfile.mkdtemp())
        try:
            c = cfg(rtds_url=url)
            e = Engine(c, mmsim.Api(c), tmp, quiet=True)
            stop = asyncio.Event()
            task = asyncio.ensure_future(mmsim.rtds_price_loop(e, stop))
            await asyncio.sleep(0.4)
            stop.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            self.assertEqual(got["sub"]["subscriptions"],
                             [{"topic": "crypto_prices", "type": "update", "filters": "btcusdt"}])
            self.assertEqual(e.prices.last("BTCUSDT"), 100150)
            self.assertEqual(e.prices.source, "rtds")
            self.assertGreater(e.prices.move_bps("BTCUSDT", time.time(), 3), 14)
        finally:
            srv.close()
            shutil.rmtree(tmp)

    def test_http_no_keepalive(self):
        st = fp.FakeState()
        st.books["9"] = {"bids": [], "asks": []}
        srv, base = fp.start_http(st)
        from net import Http
        h = Http(keepalive=False)
        for _ in range(3):
            h.get_json(base + "/book", {"token_id": "9"})
        self.assertEqual(st.connections, 3)          # каждый раз новое соединение
        srv.shutdown()



class MoreFeedsTests(unittest.IsolatedAsyncioTestCase):
    def test_parsers(self):
        self.assertEqual(mmsim.parse_coinbase({"type": "ticker", "product_id": "BTC-USD", "price": "79800.5"}),
                         [("BTCUSDT", 79800.5)])
        self.assertEqual(mmsim.parse_coinbase({"type": "subscriptions"}), [])
        self.assertEqual(mmsim.parse_okx({"arg": {"channel": "tickers", "instId": "ETH-USDT"},
                                          "data": [{"instId": "ETH-USDT", "last": "2310.1"}]}),
                         [("ETHUSDT", 2310.1)])
        self.assertEqual(mmsim.parse_okx({"event": "subscribe"}), [])

    def test_sources_kept_apart(self):
        f = PriceFeed(["BTCUSDT"])
        f.add("BTCUSDT", 100, 80000, "a")
        f.add("BTCUSDT", 100.5, 80020, "b")        # другая биржа на 2.5 б.п. дороже — не рывок
        self.assertLess(f.move_bps("BTCUSDT", 101, 3), 0.01)
        f.add("BTCUSDT", 101, 80060, "a")          # +7.5 б.п. на одной бирже — рывок
        self.assertAlmostEqual(f.move_bps("BTCUSDT", 101, 3), 7.5, places=2)
        self.assertEqual(f.sources_alive(101), ["a", "b"])

    async def test_generic_ws_loop(self):
        async def handler(reader, writer):
            await fp.ws_read(reader)
            writer.write(fp.ws_frame(0x1, json.dumps(
                {"type": "ticker", "product_id": "BTC-USD", "price": "80000"}).encode()))
            await writer.drain()
            while True:
                await fp.ws_read(reader)

        srv, url = await fp.start_ws(handler)
        tmp = Path(tempfile.mkdtemp())
        try:
            c = cfg(coinbase_ws=url)
            e = Engine(c, mmsim.Api(c), tmp, quiet=True)
            stop = asyncio.Event()
            task = asyncio.ensure_future(mmsim.coinbase_loop(e, stop))
            await asyncio.sleep(0.3)
            stop.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            self.assertEqual(e.prices.last("BTCUSDT"), 80000)
            self.assertIn("coinbase_ws", e.prices.sources_alive(time.time()))
        finally:
            srv.close()
            shutil.rmtree(tmp)


class BookMoveTests(unittest.IsolatedAsyncioTestCase):
    async def test_book_move_cancels_unfilled_pair(self):
        tmp = Path(tempfile.mkdtemp())
        api = FakeApi()
        api.markets[SLUG] = gamma_market()
        try:
            e = Engine(cfg(), api, tmp, quiet=True)
            await e.step()
            mk = e.markets[SLUG]
            p = e.pairs[mk.pairs[0]]
            toks = {UP: mk, DN: mk}
            e.on_book_event({"event_type": "price_change", "price_changes": [
                {"asset_id": UP, "best_bid": "0.49", "best_ask": "0.51"}]}, toks)
            api.t += 1
            # Down: bid .45/.47 -> Up эквивалент 0.54, сдвиг 4¢ >= 4 тиков
            e.on_book_event({"event_type": "price_change", "price_changes": [
                {"asset_id": DN, "best_bid": "0.45", "best_ask": "0.47"}]}, toks)
            self.assertEqual(p.state, "canceled")
            self.assertEqual(e.orders[p.up].cancel_reason, "book_move")
            self.assertEqual(e.counters["book_moves"], 1)
            api.t += 1
            await e.step()                              # пауза: новых котировок нет
            self.assertEqual(mk.pairs, [])
            # снимок стакана тоже понимается
            e.on_book_event({"event_type": "book", "asset_id": UP,
                             "bids": [{"price": "0.30", "size": "5"}],
                             "asks": [{"price": "0.32", "size": "5"}]}, toks)
        finally:
            shutil.rmtree(tmp)


if __name__ == "__main__":
    unittest.main(verbosity=2)


class ExamTests(unittest.TestCase):
    def test_exam_and_rebates(self):
        from mmsim import exam, Account
        a = Account(100.0)
        a.rebates = 1.5
        base = 1793000000
        done = []
        for day in range(9):
            for k in range(40):
                win = k % 3 != 0
                done.append({"asset": "btc" if k % 2 else "eth", "t": base + day * 86400 + k * 600,
                             "pnl": 0.2 if k % 4 else (0.5 if win else -0.4),
                             "outcome": "complete" if k % 4 else ("one_leg_win" if win else "one_leg_loss")})
        d = {"account": a.to_dict(), "counters": {"markets_resolved": 400, "fills": 1500},
             "done_pairs": done, "started": base, "saved": base + 9 * 86400}
        txt = exam(d)
        self.assertIn("ЭКЗАМЕН", txt)
        self.assertIn("9/9", txt)
        d["done_pairs"] = done[:10]
        self.assertIn("❌", exam(d))
        self.assertEqual(Account.from_dict(a.to_dict()).rebates, 1.5)
