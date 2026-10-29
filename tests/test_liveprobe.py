import asyncio
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
                               "avg_price": 0.61, "error": ""}
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
                          "avg_price": None, "error": ""})
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
                          "error": ""})
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


class CheckSignatureTests(unittest.TestCase):
    """check должен ловить не только пропавшие методы, но и не те аргументы:
    без order_type заявка осталась бы висеть на рынке как GTC."""

    def setUp(self):
        import types
        self.mod = types.ModuleType("polymarket")
        self.mod.__version__ = "9.9.9"
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

    def test_full_signature_passes(self):
        class Good:
            def place_limit_order(self, asset_id, side, price, size, order_type="GTC"): ...
            def post_order(self, o): ...
            def setup_trading_approvals(self): ...
        txt = self.install(Good)
        self.assertIn("можно dry, потом live", txt)
        self.assertNotIn("НЕ ЗАПУСКАТЬ", txt)

    def test_missing_order_type_blocks_live(self):
        class Bad:
            def place_limit_order(self, asset_id, side, price, size): ...
            def post_order(self, o): ...
            def setup_trading_approvals(self): ...
        txt = self.install(Bad)
        self.assertIn("НЕ ЗАПУСКАТЬ", txt)
        self.assertIn("нет order_type", txt)

    def test_missing_method_blocks_live(self):
        class Bad:
            def post_order(self, o): ...
        txt = self.install(Bad)
        self.assertIn("НЕ ЗАПУСКАТЬ", txt)
        self.assertIn("отсутствуют методы", txt)

    def test_kwargs_signature_accepted(self):
        class Flexible:
            def place_limit_order(self, **kwargs): ...
            def post_order(self, o): ...
            def setup_trading_approvals(self): ...
        self.assertNotIn("НЕ ЗАПУСКАТЬ", self.install(Flexible))


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
