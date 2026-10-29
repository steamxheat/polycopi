import asyncio
import json
import shutil
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import fillcheck  # noqa: E402
import liveprobe  # noqa: E402
from liveprobe import Prober  # noqa: E402
from tests.test_fillcheck import FakeApi  # noqa: E402


class RecordTrader:
    """Запоминает заявки и отдаёт заранее заданный ответ."""

    mode = "live"

    def __init__(self, reply=None, fail=False):
        self.sent = []
        self.reply = reply or {"ok": True, "status": "matched", "filled": 5.0,
                               "avg_price": 0.61, "spent": 5.0 * 0.61, "error": "",
                               "note": "", "sign_ms": 5.0, "post_ms": 40.0}
        self.fail = fail

    async def start(self):
        pass

    async def buy(self, tok, limit, shares):
        self.sent.append((tok, limit, shares))
        if self.fail:
            raise RuntimeError("биржа недоступна")
        return dict(self.reply)

    async def close(self):
        pass


def mkp(tmp, trader, **over):
    cfg = dict(liveprobe.DEFAULTS)
    cfg["use_model"] = False        # эти тесты про механику заявок, модель отдельно
    cfg.update(over)
    api = FakeApi()
    pr = Prober(cfg, api, tmp, trader)
    mk = fvsim_mkt()
    pr.markets[mk.slug] = mk
    pr.tok_mkt["U"] = pr.tok_mkt["D"] = mk
    api.t = mk.start + 60
    pr.book.on_event({"event_type": "book", "asset_id": "U",
                      "asks": [{"price": "0.60", "size": "50"}]}, api.t - 2)
    sym = cfg["assets"]["btc"]
    pr.prices.on_add = None
    pr.prices.add(sym, api.t - 2.0, 100.0, "s0")
    pr.prices.on_add = pr._on_price
    pr.prices.add(sym, api.t, 100.06, "s0")          # рывок +6 б.п.
    return pr, api, mk


def fvsim_mkt():
    import fvsim
    return fvsim.Mkt("btc-updown-5m-900", "btc", "5m", 900, 1200, "U", "D")


class SendTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    async def test_signal_sends_order_with_limit_and_size(self):
        t = RecordTrader()
        pr, _, _ = mkp(self.tmp, t, lead_sources=None, shares=5.0, limit_extra=0.01)
        await asyncio.sleep(0.01)
        self.assertEqual(len(t.sent), 1)
        tok, limit, shares = t.sent[0]
        self.assertEqual(tok, "U")
        self.assertAlmostEqual(limit, 0.61)           # увиденный аск + допуск
        self.assertEqual(shares, 5.0)
        self.assertAlmostEqual(pr.spent, 5.0 * 0.61, places=4)
        r = fillcheck.rows(self.tmp / "orders.csv")[0]
        self.assertEqual((r["ok"], r["mode"], r["side"]), ("1", "live", "Up"))
        self.assertAlmostEqual(float(r["ask_seen"]), 0.60)

    async def test_crash_in_trader_is_recorded_not_fatal(self):
        t = RecordTrader(fail=True)
        pr, _, _ = mkp(self.tmp, t, lead_sources=None)
        await asyncio.sleep(0.01)
        r = fillcheck.rows(self.tmp / "orders.csv")[0]
        self.assertEqual(r["ok"], "0")
        self.assertIn("биржа недоступна", r["error"])
        self.assertEqual(pr.spent, 0.0)

    async def test_unfilled_order_spends_nothing(self):
        t = RecordTrader({"ok": True, "status": "unmatched", "filled": 0.0,
                          "avg_price": None, "spent": 0.0, "error": "", "note": ""})
        pr, _, _ = mkp(self.tmp, t, lead_sources=None)
        await asyncio.sleep(0.01)
        self.assertEqual(pr.spent, 0.0)
        self.assertEqual(pr.counters["order_empty"], 1)


class LimitTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    async def test_order_count_cap(self):
        t = RecordTrader({"ok": True, "status": "x", "filled": 0.0, "avg_price": None,
                          "spent": 0.0, "error": "", "note": ""})
        pr, api, _ = mkp(self.tmp, t, lead_sources=None, max_orders=2, signal_gap_s=0.0)
        for _ in range(5):
            api.t += 0.05
            pr.prices.add(pr.cfg["assets"]["btc"], api.t, 100.06, "s0")
            await asyncio.sleep(0)
        await asyncio.sleep(0.01)
        self.assertEqual(len(t.sent), 2)
        self.assertIn("лимит заявок", pr.halted)

    async def test_spend_cap_blocks_before_sending(self):
        t = RecordTrader()
        pr, api, _ = mkp(self.tmp, t, lead_sources=None, max_spend_usd=3.0, signal_gap_s=0.0)
        await asyncio.sleep(0.01)
        self.assertEqual(len(t.sent), 0)              # 5 * 0.61 = $3.05 > $3 -> не шлём
        self.assertEqual(pr.spent, 0.0)

    async def test_stop_file_halts_orders(self):
        t = RecordTrader()
        (self.tmp / "STOP").write_text("")
        pr, _, _ = mkp(self.tmp, t, lead_sources=None)
        await asyncio.sleep(0.01)
        self.assertEqual(len(t.sent), 0)
        self.assertIn("STOP", pr.halted)

    async def test_price_cap_respected(self):
        t = RecordTrader()
        pr, api, _ = mkp(self.tmp, t, lead_sources=None, max_price=0.55)
        await asyncio.sleep(0.01)
        self.assertEqual(len(t.sent), 0)              # аск 0.60 дороже предела

    async def test_no_two_orders_for_same_token_at_once(self):
        class Slow(RecordTrader):
            async def buy(self, tok, limit, shares):
                self.sent.append((tok, limit, shares))
                await asyncio.sleep(0.05)
                return dict(self.reply)
        t = Slow()
        pr, api, _ = mkp(self.tmp, t, lead_sources=None, signal_gap_s=0.0)
        for _ in range(3):
            api.t += 0.02
            pr.prices.add(pr.cfg["assets"]["btc"], api.t, 100.06, "s0")
            await asyncio.sleep(0)
        self.assertEqual(len(t.sent), 1)              # одна в полёте — вторую не шлём
        await asyncio.sleep(0.1)


class ModelGateTests(unittest.IsolatedAsyncioTestCase):
    """Заявка уходит только если модель согласна — как у lag3-1s-t60."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    async def test_no_order_when_model_silent(self):
        t = RecordTrader()
        pr, _, _ = mkp(self.tmp, t, lead_sources=None, use_model=True)
        await asyncio.sleep(0.01)
        self.assertEqual(len(t.sent), 0)              # модели нет -> не стреляем
        self.assertGreaterEqual(pr.counters["no_model"], 1)

    async def test_no_order_when_edge_too_small(self):
        t = RecordTrader()
        pr, _, _ = mkp(self.tmp, t, lead_sources=None, use_model=True, min_edge=0.02,
                       signal_gap_s=0.0)
        pr.model_edge = lambda mk, side, ask: 0.005      # перевес всего полцента
        pr.check_signals()
        await asyncio.sleep(0.01)
        self.assertEqual(len(t.sent), 0)
        self.assertGreaterEqual(pr.counters["low_edge"], 1)

    async def test_order_goes_when_edge_is_enough(self):
        t = RecordTrader()
        pr, _, _ = mkp(self.tmp, t, lead_sources=None, use_model=True, min_edge=0.02,
                       signal_gap_s=0.0)
        pr.model_edge = lambda mk, side, ask: 0.05       # перевес 5 центов
        pr.check_signals()
        await asyncio.sleep(0.01)
        self.assertEqual(len(t.sent), 1)
        r = fillcheck.rows(self.tmp / "orders.csv")[0]
        self.assertAlmostEqual(float(r["edge"]), 0.05)

    def test_measurement_off_in_live_mode(self):
        """Иначе лишняя работа в цикле задерживает отправку заявки."""
        self.assertFalse(liveprobe.DEFAULTS["measure"])

    def test_model_borrowed_from_working_bot(self):
        """Расчёт должен быть ТОТ ЖЕ, что у fvsim, а не копией."""
        import fvsim
        self.assertIs(liveprobe.Prober.model, fvsim.Engine.model)
        self.assertIs(liveprobe.Prober.capture_s0, fvsim.Engine.capture_s0)


class SettlementTests(unittest.IsolatedAsyncioTestCase):
    """Замер тратит настоящие деньги — он обязан знать, чем кончились сделки."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    async def filled_order(self, won: bool):
        t = RecordTrader({"ok": True, "status": "matched", "filled": 5.0,
                          "avg_price": 0.60, "spent": 3.0, "error": "", "note": ""})
        pr, api, mk = mkp(self.tmp, t, lead_sources=None)
        await asyncio.sleep(0.01)
        self.assertEqual(pr.pos["U"], [5.0, 3.0])          # позиция записана
        # ответ gamma-api закрытого рынка: цены исходов ровно 0/1
        api.closed[mk.slug] = {"closed": True, "clobTokenIds": '["U", "D"]',
                               "outcomePrices": '["1", "0"]' if won else '["0", "1"]'}
        api.t = mk.end + 60
        return pr, api, mk

    async def test_win_is_credited(self):
        pr, _, _ = await self.filled_order(won=True)
        await pr.resolve_markets()
        self.assertAlmostEqual(pr.realized, 5.0 - 3.0)      # 5 билетов по $1 минус $3
        self.assertNotIn("U", pr.pos)
        self.assertEqual(pr.settled, 1)

    async def test_loss_is_recorded(self):
        pr, _, _ = await self.filled_order(won=False)
        await pr.resolve_markets()
        self.assertAlmostEqual(pr.realized, -3.0)
        self.assertEqual(pr.settled, 1)

    async def test_state_json_readable_by_app(self):
        """Формат симулятора — тогда приложение читает замер без правок шлюза."""
        pr, _, _ = await self.filled_order(won=True)
        await pr.resolve_markets()
        st = json.loads((self.tmp / "state.json").read_text())
        self.assertIn("probe", st["variants"])
        v = st["variants"]["probe"]
        for k in ("cash", "start_cash", "realized", "pos", "trades"):
            self.assertIn(k, v)
        self.assertAlmostEqual(v["realized"], 2.0)
        self.assertTrue(any(t.get("pnl") is not None or t.get("shares") for t in v["trades"]))

    async def test_trades_csv_in_simulator_format(self):
        await self.filled_order(won=True)
        rows = fillcheck.rows(self.tmp / "trades.csv")
        self.assertEqual(rows[0]["variant"], "probe")
        self.assertEqual(rows[0]["side"], "Up")
        self.assertAlmostEqual(float(rows[0]["shares"]), 5.0)
        self.assertAlmostEqual(float(rows[0]["cost"]), 3.0)

    async def test_unfilled_order_makes_no_position(self):
        t = RecordTrader({"ok": True, "status": "unmatched", "filled": 0.0,
                          "avg_price": None, "spent": 0.0, "error": "", "note": ""})
        pr, _, _ = mkp(self.tmp, t, lead_sources=None)
        await asyncio.sleep(0.01)
        self.assertEqual(pr.pos, {})
        self.assertFalse((self.tmp / "trades.csv").exists())


class DryTests(unittest.IsolatedAsyncioTestCase):
    async def test_dry_trader_sends_nothing(self):
        d = liveprobe.DryTrader()
        await d.start()
        r = await d.buy("U", 0.61, 5)
        self.assertEqual((r["ok"], r["filled"], r["status"]), (True, 0.0, "dry"))
        await d.close()


class CheckTests(unittest.TestCase):
    def test_check_reports_missing_library(self):
        txt = liveprobe.check()
        self.assertIn("polymarket-client", txt)
        self.assertIn("ЗАБРОШЕНА", txt)              # предупреждение про старую библиотеку

    def test_credentials_reads_env(self):
        import os
        os.environ["POLYMARKET_PRIVATE_KEY"] = "секрет-для-теста"
        try:
            cr = liveprobe.LiveTrader.credentials()
            self.assertEqual(cr["POLYMARKET_PRIVATE_KEY"], "секрет-для-теста")
        finally:
            del os.environ["POLYMARKET_PRIVATE_KEY"]

    def test_key_never_appears_in_check_output(self):
        import os
        os.environ["POLYMARKET_PRIVATE_KEY"] = "ОЧЕНЬ-СЕКРЕТНЫЙ-КЛЮЧ"
        try:
            self.assertNotIn("ОЧЕНЬ-СЕКРЕТНЫЙ-КЛЮЧ", liveprobe.check())
        finally:
            del os.environ["POLYMARKET_PRIVATE_KEY"]


class ParseReplyTests(unittest.TestCase):
    """Разбор ответа биржи. Самое скользкое — единицы измерения сумм:
    если библиотека отдаёт их не в долларах, учёт трат поедет."""

    class Accepted:
        ok = True
        order_id = "abc"
        status = "matched"
        taking_amount = "5"          # полученные билеты
        making_amount = "3.05"       # потраченные деньги

    class Rejected:
        ok = False
        code = "NOT_ENOUGH"
        message = "insufficient balance"

    def test_accepted_order(self):
        r = liveprobe.parse_reply(self.Accepted(), limit=0.61, shares=5)
        self.assertTrue(r["ok"])
        self.assertEqual(r["filled"], 5.0)
        self.assertAlmostEqual(r["avg_price"], 0.61, places=3)
        self.assertAlmostEqual(r["spent"], 3.05, places=3)
        self.assertEqual(r["order_id"], "abc")
        self.assertEqual(r["note"], "")

    def test_rejected_order(self):
        r = liveprobe.parse_reply(self.Rejected(), limit=0.61, shares=5)
        self.assertFalse(r["ok"])
        self.assertIn("insufficient", r["error"])
        self.assertEqual(r["filled"], 0.0)
        self.assertEqual(r["spent"], 0.0)

    def test_micro_units_are_converted(self):
        """Как отдаёт библиотека на самом деле: обе суммы в микроединицах.
        Баланс 10480989 = $10.48 — отсюда и видно масштаб."""
        class Micro:
            ok = True
            order_id = "x"
            status = "matched"
            taking_amount = "5000000"        # 5 билетов
            making_amount = "3050000"        # $3.05
        r = liveprobe.parse_reply(Micro(), limit=0.61, shares=5)
        self.assertAlmostEqual(r["filled"], 5.0)
        self.assertAlmostEqual(r["spent"], 3.05, places=4)
        self.assertAlmostEqual(r["avg_price"], 0.61, places=4)
        self.assertEqual(r["note"], "")

    def test_plain_units_still_work(self):
        class Plain:
            ok = True
            order_id = "x"
            status = "matched"
            taking_amount = "5"
            making_amount = "3.05"
        r = liveprobe.parse_reply(Plain(), limit=0.61, shares=5)
        self.assertAlmostEqual(r["filled"], 5.0)
        self.assertAlmostEqual(r["spent"], 3.05, places=4)

    def test_truly_impossible_price_is_rejected(self):
        """Цена билета не может быть больше доллара — такому ответу не верим."""
        class Bad:
            ok = True
            order_id = "x"
            status = "matched"
            taking_amount = "5"
            making_amount = "50"             # $10 за билет — так не бывает
        r = liveprobe.parse_reply(Bad(), limit=0.61, shares=5)
        self.assertIsNone(r["avg_price"])
        self.assertIn("единицы ответа непонятны", r["note"])
        self.assertAlmostEqual(r["spent"], 5 * 0.61, places=3)   # считаем по пределу

    def test_to_units_and_usd(self):
        self.assertAlmostEqual(liveprobe.to_units(10480989, 1e4), 10.480989)
        self.assertAlmostEqual(liveprobe.to_units(10.48, 1e4), 10.48)   # уже в долларах
        self.assertIsNone(liveprobe.to_units(None, 5))
        self.assertEqual(liveprobe.usd(10480989), "$10.48")
        self.assertEqual(liveprobe.usd(None), "?")

    def test_partial_fill(self):
        class Part:
            ok = True
            order_id = "x"
            status = "matched"
            taking_amount = "2"
            making_amount = "1.18"
        r = liveprobe.parse_reply(Part(), limit=0.61, shares=5)
        self.assertEqual(r["filled"], 2.0)
        self.assertAlmostEqual(r["avg_price"], 0.59, places=2)

    def test_missing_fields(self):
        class Empty:
            ok = True
        r = liveprobe.parse_reply(Empty(), limit=0.61, shares=5)
        self.assertEqual(r["filled"], 0.0)
        self.assertEqual(r["spent"], 0.0)
        self.assertIsNone(r["avg_price"])


class CheckIntrospectionTests(unittest.TestCase):
    """check должен ПОКАЗЫВАТЬ настоящие подписи, а не верить документации:
    в версии 0.14.0 у place_limit_order нет order_type, и заявка повисла бы как GTC."""

    def setUp(self):
        import types
        self.mod = types.ModuleType("polymarket")
        self.mod.__version__ = "0.14.0"
        self.saved = sys.modules.get("polymarket")

    def tearDown(self):
        if self.saved is None:
            sys.modules.pop("polymarket", None)
        else:
            sys.modules["polymarket"] = self.saved

    def install(self, cls):
        self.mod.AsyncSecureClient = cls
        sys.modules["polymarket"] = self.mod
        return liveprobe.check()

    def test_lists_order_methods_with_signatures(self):
        class C:
            def place_limit_order(self, *, asset_id=None, price=0, size=0, side="BUY",
                                  post_only=False, expiration=None): ...
            def place_market_order(self, *, asset_id=None, amount=0, side="BUY",
                                   order_type="FAK"): ...
            def post_order(self, o): ...
            def setup_trading_approvals(self): ...
        txt = self.install(C)
        self.assertIn("place_limit_order(self, *, asset_id=None", txt)
        self.assertIn("place_market_order", txt)
        # признаки «не оставлять висеть» должны быть найдены там, где они есть
        hint = txt[txt.index("можно ли НЕ оставлять"):]
        self.assertIn("order_type", hint)
        self.assertIn("post_only", hint)

    def test_missing_base_method_blocks_live(self):
        class C:
            def place_limit_order(self, **k): ...
        txt = self.install(C)
        self.assertIn("ЖИВОЙ РЕЖИМ НЕ ЗАПУСКАТЬ", txt)
        self.assertIn("post_order", txt)

    def test_no_take_and_leave_option_is_reported(self):
        class C:
            def place_order(self, asset_id, size, side): ...
            def post_order(self, o): ...
            def setup_trading_approvals(self): ...
        txt = self.install(C)
        self.assertIn("подходящих признаков не найдено", txt)

    def test_wrong_library_detected(self):
        class Nothing:
            pass
        self.mod.AsyncSecureClient = None
        sys.modules["polymarket"] = self.mod
        self.assertIn("НЕТ класса AsyncSecureClient", liveprobe.check())

    def test_response_fields_printed_from_pydantic_like_model(self):
        import types
        resp = types.ModuleType("polymarket.models.clob.order_response")

        class AcceptedOrder:
            model_fields = {"order_id": 1, "status": 2, "size_matched": 3}

        class RejectedOrder:
            model_fields = {"message": 1, "error_code": 2}
        resp.AcceptedOrder, resp.RejectedOrder = AcceptedOrder, RejectedOrder
        clob = types.ModuleType("polymarket.models.clob")
        clob.order_response = resp
        models = types.ModuleType("polymarket.models")
        models.clob = clob
        for name, m in (("polymarket.models", models), ("polymarket.models.clob", clob),
                        ("polymarket.models.clob.order_response", resp)):
            sys.modules[name] = m
        try:
            class C:
                def place_limit_order(self, **k): ...
                def post_order(self, o): ...
                def setup_trading_approvals(self): ...
            txt = self.install(C)
            self.assertIn("AcceptedOrder: order_id, size_matched, status", txt)
            self.assertIn("RejectedOrder: error_code, message", txt)
        finally:
            for name in ("polymarket.models", "polymarket.models.clob",
                         "polymarket.models.clob.order_response"):
                sys.modules.pop(name, None)


class CredentialsTests(unittest.TestCase):
    """Набор ключей решает, какой способ подключения доступен."""

    def setUp(self):
        import types
        self.mod = types.ModuleType("polymarket")
        self.mod.AsyncSecureClient = object

        class Relayer:
            def __init__(self, key, address):
                self.key, self.address = key, address

        class Builder:
            def __init__(self, key, secret, passphrase):
                self.key = key
        self.mod.RelayerApiKey, self.mod.BuilderApiKey = Relayer, Builder
        self.saved = sys.modules.get("polymarket")
        sys.modules["polymarket"] = self.mod

    def tearDown(self):
        if self.saved is None:
            sys.modules.pop("polymarket", None)
        else:
            sys.modules["polymarket"] = self.saved

    def test_private_key_only(self):
        kw = liveprobe.LiveTrader.client_kwargs({"POLYMARKET_PRIVATE_KEY": "k"})
        self.assertEqual(set(kw), {"private_key"})

    def test_relayer_key_is_passed(self):
        kw = liveprobe.LiveTrader.client_kwargs({
            "POLYMARKET_PRIVATE_KEY": "k", "POLYMARKET_WALLET_ADDRESS": "0xабв",
            "POLYMARKET_RELAYER_API_KEY": "r", "POLYMARKET_RELAYER_API_KEY_ADDRESS": "0xкл"})
        self.assertEqual(kw["wallet"], "0xабв")
        self.assertEqual(kw["api_key"].key, "r")
        self.assertEqual(kw["api_key"].address, "0xкл")

    def test_builder_key_drops_wallet(self):
        """С ключом Builder кошелёк выводится сам — передавать его нельзя."""
        kw = liveprobe.LiveTrader.client_kwargs({
            "POLYMARKET_PRIVATE_KEY": "k", "POLYMARKET_WALLET_ADDRESS": "0xабв",
            "POLYMARKET_BUILDER_API_KEY": "b", "POLYMARKET_BUILDER_SECRET": "s",
            "POLYMARKET_BUILDER_PASSPHRASE": "p"})
        self.assertNotIn("wallet", kw)
        self.assertEqual(kw["api_key"].key, "b")

    def test_check_lists_what_is_missing(self):
        import os
        os.environ["POLYMARKET_PRIVATE_KEY"] = "СЕКРЕТ"
        try:
            txt = liveprobe.check()
            self.assertIn("✅ приватный ключ подписи", txt)
            self.assertIn("— ключ Relayer", txt)
            self.assertIn("путь EOA", txt)
            self.assertNotIn("СЕКРЕТ", txt)          # значения не показываем
        finally:
            del os.environ["POLYMARKET_PRIVATE_KEY"]

    def test_check_says_set_is_complete(self):
        import os
        os.environ["POLYMARKET_PRIVATE_KEY"] = "k"
        os.environ["POLYMARKET_RELAYER_API_KEY"] = "r"
        try:
            self.assertIn("Набор полный", liveprobe.check())
        finally:
            del os.environ["POLYMARKET_PRIVATE_KEY"]
            del os.environ["POLYMARKET_RELAYER_API_KEY"]


class BuyArgsTests(unittest.IsolatedAsyncioTestCase):
    """Биржа требует для покупки сумму в долларах, а не количество билетов."""

    async def test_buy_sends_amount_and_price_cap(self):
        sent = {}

        class Client:
            async def create_market_order(self, **kw):
                sent.update(kw)
                return "подписанная-заявка"

            async def post_order(self, signed):
                sent["posted"] = signed
                class R:
                    ok = True
                    order_id = "x"
                    status = "matched"
                    taking_amount = "5000000"
                    making_amount = "3050000"
                return R()
        t = liveprobe.LiveTrader(dict(liveprobe.DEFAULTS))
        t.client = Client()
        r = await t.buy("TOK", 0.61, 5.0)
        self.assertEqual(sent["posted"], "подписанная-заявка")
        self.assertIn("sign_ms", r)
        self.assertIn("post_ms", r)
        self.assertEqual(sent["side"], "BUY")
        self.assertEqual(sent["asset_id"], "TOK")
        self.assertEqual(sent["amount"], "3.05")          # 5 * 0.61
        self.assertEqual(sent["max_price"], "0.61")
        self.assertEqual(sent["order_type"], "FAK")
        self.assertNotIn("shares", sent)
        self.assertAlmostEqual(r["filled"], 5.0)

    async def test_small_order_raised_to_minimum(self):
        sent = {}

        class Client:
            async def create_market_order(self, **kw):
                sent.update(kw)
                raise RuntimeError("не важно")
        cfg = dict(liveprobe.DEFAULTS)
        cfg["min_order_usd"] = 1.0
        t = liveprobe.LiveTrader(cfg)
        t.client = Client()
        r = await t.buy("TOK", 0.10, 5.0)                 # 5 * 0.10 = $0.50 — мало
        # Минимум биржи — 5 билетов, значит $0.50, а не выдуманный $1.
        self.assertEqual(sent["amount"], "0.50")
        self.assertIn("подпись", r["error"])               # упало на подписи, не на отправке


class StatusTests(unittest.TestCase):
    def test_status_is_in_commands_and_sends_no_orders(self):
        """status обязан быть только для чтения: ни одного вызова заявок."""
        src = (ROOT / "liveprobe.py").read_text()
        i = src.index("\nasync def status(")          # именно функция, не метод класса
        body = src[i:src.index("\nasync def approvals(", i)]
        self.assertGreater(len(body), 200, "не нашли тело команды status")
        for forbidden in ("place_market_order", "place_limit_order", "post_order", "buy("):
            self.assertNotIn(forbidden, body, f"status не должен вызывать {forbidden}")
        self.assertIn("get_balance_allowance", body)
        self.assertIn("get_trading_approvals_state", body)
        self.assertIn('"status"', src)          # есть в списке команд


class ReportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_report_without_data(self):
        self.assertIn("Нет заявок", liveprobe.report(self.tmp))

    def test_report_live_numbers(self):
        import csv
        with (self.tmp / "orders.csv").open("w", newline="") as fh:
            w = csv.DictWriter(fh, liveprobe.ORDER_COLS)
            w.writeheader()
            for i in range(10):
                filled = 5.0 if i < 7 else 0.0            # 7 из 10 исполнились
                w.writerow({"time_utc": "x", "mode": "live", "asset": "btc", "window": "5m",
                            "side": "Up", "tok": "U", "ask_seen": 0.60, "limit": 0.61,
                            "shares": 5.0, "send_ms": 40 + i, "ok": 1, "status": "m",
                            "filled": filled, "avg_price": 0.605 if filled else "",
                            "spent": filled * 0.605, "move_bps": 4.0, "ask_age_s": 1.5,
                            "error": ""})
        txt = liveprobe.report(self.tmp)
        self.assertIn("исполнено хоть частично: 7/10 = 70%", txt)
        self.assertIn("медиана 44мс", txt)
        self.assertIn("+0.50¢", txt)                      # 0.605 против 0.60

    def test_report_marks_dry_only(self):
        import csv
        with (self.tmp / "orders.csv").open("w", newline="") as fh:
            w = csv.DictWriter(fh, liveprobe.ORDER_COLS)
            w.writeheader()
            w.writerow({"mode": "dry", "send_ms": 10, "ok": 1, "filled": 0, "shares": 5,
                        "ask_seen": 0.6, "spent": 0})
        self.assertIn("только сухой режим", liveprobe.report(self.tmp))


if __name__ == "__main__":
    unittest.main()


class OrderThreadTests(unittest.IsolatedAsyncioTestCase):
    """Отправка должна жить в своём потоке: иначе разбор потока стаканов
    задерживает заявку (подпись прыгала с 8 до 151 мс — ждала очереди)."""

    def _trader(self):
        import threading
        seen = {}

        class C:
            async def create_market_order(self, **kw):
                seen["sign_thread"] = threading.current_thread().name
                await asyncio.sleep(0.01)
                return {"ok": True}

            async def post_order(self, signed):
                seen["post_thread"] = threading.current_thread().name
                await asyncio.sleep(0.01)
                return {"status": "unmatched", "makingAmount": "0", "takingAmount": "0"}

        t = liveprobe.LiveTrader(dict(liveprobe.DEFAULTS))
        t.client = C()
        t._start_thread()
        return t, seen

    async def test_client_calls_run_off_the_main_loop(self):
        import threading
        t, seen = self._trader()
        try:
            await t.buy("TOK", 0.61, 5.0)
        finally:
            t.loop.call_soon_threadsafe(t.loop.stop)
        here = threading.current_thread().name
        self.assertEqual(seen["sign_thread"], "orders")
        self.assertEqual(seen["post_thread"], "orders")
        self.assertNotEqual(seen["sign_thread"], here)

    async def test_busy_main_loop_does_not_inflate_measurement(self):
        """Главный цикл занят 300 мс — замер заявки обязан остаться честным."""
        t, _ = self._trader()
        try:
            task = asyncio.ensure_future(t.buy("TOK", 0.61, 5.0))
            await asyncio.sleep(0)
            time.sleep(0.3)                     # блокируем главный цикл
            r = await task
        finally:
            t.loop.call_soon_threadsafe(t.loop.stop)
        self.assertLess(r["sign_ms"], 150)
        self.assertLess(r["post_ms"], 150)


class PriceSlackTests(unittest.TestCase):
    """Сколько готовы переплатить к увиденному аску."""

    def _p(self, **over):
        cfg = dict(liveprobe.DEFAULTS); cfg.update(over)
        pr = liveprobe.Prober.__new__(liveprobe.Prober)
        pr.cfg = cfg
        return pr

    def test_small_edge_pays_only_the_floor(self):
        self.assertAlmostEqual(self._p().price_slack(0.02), 0.01)

    def test_big_edge_pays_half_of_what_is_free(self):
        # перевес 8¢, запас 2¢ -> свободно 6¢ -> отдаём 3¢ (потолок поднят)
        self.assertAlmostEqual(self._p(limit_extra_max=0.04).price_slack(0.08), 0.03)

    def test_never_above_the_cap(self):
        self.assertAlmostEqual(self._p(limit_extra_max=0.04).price_slack(0.50), 0.04)

    def test_default_cap_is_one_cent(self):
        """По умолчанию потолок 1¢: доказанный запас всего 1.9¢ на билет."""
        self.assertAlmostEqual(self._p().price_slack(0.50), 0.01)

    def test_never_below_the_floor(self):
        self.assertAlmostEqual(self._p().price_slack(0.0), 0.01)


class BalanceCapTests(unittest.IsolatedAsyncioTestCase):
    """Заявка не должна быть больше денег на счёте: иначе отказ выглядит как
    «не успели по скорости», хотя дело в балансе."""

    def _trader(self, balance):
        got = {}

        class C:
            async def create_market_order(self, **kw):
                got.update(kw)
                return {"ok": True}

            async def post_order(self, signed):
                return {"status": "unmatched", "makingAmount": "0", "takingAmount": "0"}

        cfg = dict(liveprobe.DEFAULTS, risk_frac=1.0)   # проверяем не долю риска
        t = liveprobe.LiveTrader(cfg)
        t.client = C()
        t.balance = balance
        return t, got

    async def test_order_trimmed_to_balance(self):
        t, got = self._trader(1.93)                  # на счёте $1.93
        r = await t.buy("TOK", 0.93, 5.0)            # 5 билетов = $4.65
        # Урезать нельзя: минимум биржи — 5 билетов, а денег на них нет.
        self.assertEqual(r["status"], "no_funds")
        self.assertEqual(got, {})

    async def test_order_trimmed_when_above_the_share_minimum(self):
        t, got = self._trader(10.0)                  # хватает на 10 билетов по 0.95
        r = await t.buy("TOK", 0.95, 50.0)           # хотели 50
        self.assertEqual(got["amount"], "9.50")      # 10.0 * 0.95 = $9.50
        self.assertAlmostEqual(r["shares_sent"], 10.0, places=2)

    async def test_full_size_when_money_is_there(self):
        t, got = self._trader(50.0)
        await t.buy("TOK", 0.60, 5.0)
        self.assertEqual(got["amount"], "3.00")

    async def test_no_order_at_all_below_minimum(self):
        t, got = self._trader(0.40)                  # даже на $1 не хватает
        r = await t.buy("TOK", 0.60, 5.0)
        self.assertEqual(r["status"], "no_funds")
        self.assertEqual(got, {})                    # бирже ничего не послали
        self.assertIn("не хватает денег", r["error"])

    async def test_unknown_balance_does_not_block(self):
        t, got = self._trader(None)
        await t.buy("TOK", 0.60, 5.0)
        self.assertEqual(got["amount"], "3.00")

    async def test_balance_is_reread_from_the_exchange(self):
        """Свой счётчик врёт при частичном исполнении — спрашиваем биржу."""
        t, _ = self._trader(10.0)
        t.client.get_balance_allowance = lambda **k: _done(type("B", (), {"balance": 7000000})())
        await t.buy("TOK", 0.60, 5.0)
        self.assertAlmostEqual(t.balance, 7.0)


class ConfirmTests(unittest.IsolatedAsyncioTestCase):
    """Горький урок: биржа отвечала «встречных нет», мы писали «потрачено $0»,
    а со счёта ушло $9. Ответу не верим — сверяем со счётом."""

    def _trader(self, after_balance, reply, trades=()):
        class C:
            async def create_market_order(self, **kw):
                return {"ok": True}

            async def post_order(self, signed):
                if isinstance(reply, Exception):
                    raise reply
                return reply

            def get_balance_allowance(self, **k):
                return _done(type("B", (), {"balance": after_balance * 1_000_000})())

            def get_trades(self):
                return _done(list(trades))

        cfg = dict(liveprobe.DEFAULTS, risk_frac=1.0)   # проверяем сверку со счётом
        t = liveprobe.LiveTrader(cfg)
        t.client = C()
        t.balance = 10.48
        return t

    async def test_error_reply_but_money_left_the_account(self):
        err = RuntimeError("no orders found to match with FAK order")
        t = self._trader(9.41, err, trades=[{"asset_id": "TOK", "size": 20.0, "price": 0.05}])
        r = await t.buy("TOK", 0.05, 5.0)
        self.assertTrue(r["ok"])                       # не промах!
        self.assertAlmostEqual(r["spent"], 1.07, places=2)
        self.assertAlmostEqual(r["filled"], 20.0)
        self.assertAlmostEqual(r["avg_price"], 0.05)
        self.assertIn("ОТВЕТ СОВРАЛ", r["note"])

    async def test_real_miss_stays_a_miss(self):
        err = RuntimeError("no orders found to match with FAK order")
        t = self._trader(10.48, err)                   # счёт не дрогнул
        r = await t.buy("TOK", 0.60, 5.0)
        self.assertFalse(r["ok"])
        self.assertEqual(r["spent"], 0.0)
        self.assertNotIn("СОВРАЛ", r.get("note") or "")

    async def test_honest_reply_is_left_alone(self):
        ok = _reply(making=3_000_000, taking=5_000_000)
        t = self._trader(7.48, ok)
        r = await t.buy("TOK", 0.60, 5.0)
        self.assertAlmostEqual(r["spent"], 3.0)
        self.assertNotIn("СОВРАЛ", r.get("note") or "")

    async def test_spent_taken_from_the_account_not_the_reply(self):
        """Ответ занизил трату — верим счёту."""
        low = _reply(making=100_000, taking=5_000_000)
        t = self._trader(8.48, low)                    # ушло $2.00, а в ответе $0.10
        r = await t.buy("TOK", 0.60, 5.0)
        self.assertAlmostEqual(r["spent"], 2.0)
        self.assertIn("СОВРАЛ", r["note"])

    async def test_unreadable_account_is_said_out_loud(self):
        class Boom(Exception):
            pass

        t = self._trader(0, RuntimeError("мимо"))
        t.client.get_balance_allowance = lambda **k: (_ for _ in ()).throw(Boom())
        r = await t.buy("TOK", 0.60, 5.0)
        self.assertIn("счёт не сверен", r["note"])


class MinPriceTests(unittest.TestCase):
    """Дешёвые билеты не торгуем: все 9 настоящих покупок ушли по 0.05-0.20,
    а это диапазон, который в разборе держался на случайных попаданиях."""

    def test_floor_is_in_defaults(self):
        self.assertGreaterEqual(liveprobe.DEFAULTS["min_price"], 0.30)

    def test_cheap_signal_is_skipped(self):
        src = (Path(__file__).resolve().parent.parent / "liveprobe.py").read_text()
        self.assertIn('if sig["ask0"] < self.cfg["min_price"]', src)


async def _done(v):
    return v


def _reply(making: int, taking: int, status: str = "matched"):
    """Ответ библиотеки — объект с полями, а не словарь."""
    return type("Accepted", (), {"ok": True, "status": status, "order_id": "x",
                                 "making_amount": making, "taking_amount": taking,
                                 "message": ""})()


class ProvenRangeTests(unittest.TestCase):
    """Настройки обязаны совпадать с тем, что доказал запас прочности
    (scripts/slip.py на 422 сделках): торгуем 0.50-0.70, переплата 1¢."""

    def test_only_the_proven_range_is_tradable(self):
        self.assertAlmostEqual(liveprobe.DEFAULTS["min_price"], 0.50)
        self.assertAlmostEqual(liveprobe.DEFAULTS["max_price"], 0.70)

    def test_markup_does_not_eat_the_margin(self):
        # доказано 1.9¢ наверняка — отдавать больше половины нельзя
        self.assertLessEqual(liveprobe.DEFAULTS["limit_extra_max"], 0.01)

    def test_dear_signal_is_skipped_too(self):
        src = (Path(__file__).resolve().parent.parent / "liveprobe.py").read_text()
        self.assertIn('if limit > self.cfg["max_price"]', src)


class RiskLimitTests(unittest.IsolatedAsyncioTestCase):
    """Урок $8.98: минимум заявки $1 при счёте $10.48 — это 10% счёта на
    сделку, девять сделок = 90% счёта за 23 минуты. Считаем в долях счёта."""

    def _trader(self, balance, **over):
        got = {}

        class C:
            async def create_market_order(self, **kw):
                got.update(kw)
                return {"ok": True}

            async def post_order(self, signed):
                return {"status": "unmatched"}

            def get_balance_allowance(self, **k):
                return _done(type("B", (), {"balance": balance * 1_000_000})())

        cfg = dict(liveprobe.DEFAULTS); cfg.update(over)
        t = liveprobe.LiveTrader(cfg)
        t.client = C()
        t.balance = balance
        return t, got

    async def test_stake_trimmed_to_the_risk_share(self):
        t, got = self._trader(150.0)                 # 2% = $3.00
        await t.buy("TOK", 0.60, 50.0)               # хотели $30
        self.assertEqual(got["amount"], "3.00")

    async def test_no_trade_when_the_minimum_breaks_the_risk_share(self):
        """Ровно наш случай: 2% от $10.48 = $0.21, а минимум заявки $1."""
        t, got = self._trader(10.48)
        r = await t.buy("TOK", 0.05, 5.0)
        self.assertEqual(r["status"], "too_big")
        self.assertEqual(got, {})                    # бирже ничего не послали
        self.assertIn("не влезает в риск", r["error"])
        self.assertIn("5 билетов", r["error"])
        self.assertIn("$12", r["error"])             # по цене 0.05 хватило бы $12

    async def test_the_old_behaviour_is_gone(self):
        """Раньше $0.25 раздувалось до $1 — 10% счёта. Больше нет."""
        t, got = self._trader(10.48)
        await t.buy("TOK", 0.05, 5.0)
        self.assertNotIn("amount", got)

    async def test_minimum_lot_in_the_working_range_needs_a_real_account(self):
        """5 билетов по 0.60 = $3.00. При риске 2% это требует счёта $150 —
        ровно весь бюджет. С $80 торговать в рабочем диапазоне нельзя."""
        t, got = self._trader(80.0)                  # 2% = $1.60, а лот $3.00
        r = await t.buy("TOK", 0.60, 5.0)
        self.assertEqual(r["status"], "too_big")
        self.assertIn("$150", r["error"])
        self.assertEqual(got, {})

    async def test_full_account_can_trade_the_working_range(self):
        t, got = self._trader(150.0)                 # 2% = $3.00 = ровно лот
        await t.buy("TOK", 0.60, 5.0)
        self.assertEqual(got["amount"], "3.00")


class LossCapTests(unittest.TestCase):
    """Лимит в $150 при счёте $10.48 не защищал ни от чего."""

    def _probe(self, start, spent, **over):
        cfg = dict(liveprobe.DEFAULTS); cfg.update(over)
        pr = liveprobe.Prober.__new__(liveprobe.Prober)
        pr.cfg = cfg
        pr.spent = spent
        pr.orders = 0
        pr.halted = ""
        pr.out = Path("/нет/такого")
        pr.trader = type("T", (), {"start_balance": start})()
        return pr

    def test_stops_at_the_share_of_the_account(self):
        pr = self._probe(10.48, 1.60)                # 15% от 10.48 = $1.57
        self.assertIn("% счёта", pr.budget_left())

    def test_keeps_going_below_the_share(self):
        self.assertEqual(self._probe(10.48, 1.00).budget_left(), "")

    def test_cap_scales_with_the_account(self):
        self.assertAlmostEqual(self._probe(150.0, 0).loss_cap(), 22.5)
        self.assertAlmostEqual(self._probe(10.48, 0).loss_cap(), 1.572)

    def test_unknown_balance_leaves_the_dollar_limit(self):
        self.assertIsNone(self._probe(None, 0).loss_cap())

    def test_the_old_dollar_limit_would_not_have_saved_us(self):
        """$9 потерь при лимите $150 — старый предел молчал, новый кричит."""
        pr = self._probe(10.48, 8.98)
        self.assertLess(pr.spent, pr.cfg["max_spend_usd"])
        self.assertIn("% счёта", pr.budget_left())


class ExchangeMinimumTests(unittest.IsolatedAsyncioTestCase):
    """Минимум заявки у Polymarket задан в БИЛЕТАХ (minOrderSize в стакане),
    а не в долларах. Выдуманный минимум $1 по цене 0.05 превращал 5 билетов
    в 20 и умножил потерю втрое."""

    def test_fallback_is_five_shares(self):
        t = liveprobe.LiveTrader(dict(liveprobe.DEFAULTS))
        self.assertAlmostEqual(t.min_shares("нет-такого"), 5.0)

    def test_exchange_value_wins_over_ours(self):
        t = liveprobe.LiveTrader(dict(liveprobe.DEFAULTS))
        t.min_size["TOK"] = 20.0
        self.assertAlmostEqual(t.min_shares("TOK"), 20.0)

    def test_zero_from_the_exchange_is_ignored(self):
        t = liveprobe.LiveTrader(dict(liveprobe.DEFAULTS))
        t.min_size["TOK"] = 0.0
        self.assertAlmostEqual(t.min_shares("TOK"), 5.0)

    async def test_minimum_read_when_a_market_appears(self):
        """Читаем при появлении рынка, а не перед заявкой: перед заявкой
        дорога каждая миллисекунда."""
        pr = liveprobe.Prober.__new__(liveprobe.Prober)
        pr.cfg = dict(liveprobe.DEFAULTS)
        pr.trader = liveprobe.LiveTrader(dict(liveprobe.DEFAULTS))
        asked = []

        class Api:
            async def get_book(self, tok):
                asked.append(tok)
                return {"min_order_size": "15"}

        pr.api = Api()
        mk = type("M", (), {"up": "U", "down": "D"})()
        await pr.on_new_market(mk)
        self.assertEqual(asked, ["U", "D"])
        self.assertAlmostEqual(pr.trader.min_shares("U"), 15.0)

    async def test_unreadable_book_leaves_our_fallback(self):
        pr = liveprobe.Prober.__new__(liveprobe.Prober)
        pr.cfg = dict(liveprobe.DEFAULTS)
        pr.trader = liveprobe.LiveTrader(dict(liveprobe.DEFAULTS))

        class Api:
            async def get_book(self, tok):
                raise RuntimeError("нет связи")

        pr.api = Api()
        await pr.on_new_market(type("M", (), {"up": "U", "down": "D"})())
        self.assertAlmostEqual(pr.trader.min_shares("U"), 5.0)

    async def test_dry_mode_does_not_crash_on_new_market(self):
        pr = liveprobe.Prober.__new__(liveprobe.Prober)
        pr.cfg = dict(liveprobe.DEFAULTS)
        pr.trader = liveprobe.DryTrader()
        await pr.on_new_market(type("M", (), {"up": "U", "down": "D"})())
