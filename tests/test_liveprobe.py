import asyncio
import json
import shutil
import sys
import tempfile
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
                               "note": ""}
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
            async def place_market_order(self, **kw):
                sent.update(kw)
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
            async def place_market_order(self, **kw):
                sent.update(kw)
                raise RuntimeError("не важно")
        cfg = dict(liveprobe.DEFAULTS)
        cfg["min_order_usd"] = 1.0
        t = liveprobe.LiveTrader(cfg)
        t.client = Client()
        await t.buy("TOK", 0.10, 5.0)                     # 5 * 0.10 = $0.50 — мало
        self.assertEqual(sent["amount"], "1.00")


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
