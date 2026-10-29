import asyncio
import csv
import logging
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import fillcheck  # noqa: E402
from fillcheck import HORIZONS_MS, Watcher  # noqa: E402


class FakeApi:
    def __init__(self, t=1000.0):
        self.t = t
        self.books = {}
        self.closed = {}          # slug -> ответ gamma-api закрытого рынка

    def now(self):
        return self.t

    async def get_book(self, tok):
        return self.books.get(tok, {})

    async def get_market(self, slug, closed=False):
        return self.closed.get(slug)


def mkw(tmp, **over):
    cfg = dict(fillcheck.DEFAULTS)
    cfg.update(over)
    api = FakeApi()
    w = Watcher(cfg, api, tmp)
    mk = fillcheck.fvsim.Mkt("btc-updown-5m-900", "btc", "5m", 900, 1200, "U", "D")
    w.markets[mk.slug] = mk
    w.tok_mkt["U"] = w.tok_mkt["D"] = mk
    return w, api, mk


def feed(w, api, *bps):
    """Кладёт в ленту цены так, чтобы движение за секунду было ровно bps."""
    sym = w.cfg["assets"]["btc"]
    for i, b in enumerate(bps):
        src = f"s{i}"
        # цена ДО окна замера (нужна как точка отсчёта) и цена сейчас
        w.prices.add(sym, api.t - 2.0, 100.0, src)
        w.prices.add(sym, api.t, 100.0 * (1 + b / 1e4), src)


class MoveTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_move_direction_matters(self):
        w, api, _ = mkw(self.tmp, lead_sources=None, lead_agree=1, move_bps=3.0)
        feed(w, api, 5.0)                        # цена ушла ВВЕРХ на 5 б.п.
        self.assertTrue(w.moved("btc", "Up"))
        self.assertFalse(w.moved("btc", "Down"))
        self.assertAlmostEqual(w.move_bps("btc"), 5.0, places=1)

    def test_move_needs_enough_sources(self):
        w, api, _ = mkw(self.tmp, lead_sources=None, lead_agree=2, move_bps=3.0)
        feed(w, api, 5.0, 0.0)                   # согласен только один источник
        self.assertFalse(w.moved("btc", "Up"))
        w2, api2, _ = mkw(self.tmp, lead_sources=None, lead_agree=2, move_bps=3.0)
        feed(w2, api2, 5.0, 4.0)
        self.assertTrue(w2.moved("btc", "Up"))

    def test_small_move_ignored(self):
        w, api, _ = mkw(self.tmp, lead_sources=None, move_bps=3.0)
        feed(w, api, 1.0)
        self.assertFalse(w.moved("btc", "Up"))


class LifetimeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_level_lifetime_measured(self):
        w, api, _ = mkw(self.tmp)
        w.book.on_event({"event_type": "book", "asset_id": "U",
                         "asks": [{"price": "0.60", "size": "10"}]}, api.t)
        api.t += 0.35                                   # заявка простояла 350 мс
        w.book.on_event({"event_type": "book", "asset_id": "U",
                         "asks": [{"price": "0.62", "size": "10"}]}, api.t)
        self.assertEqual(len(w.lives), 1)
        life, after = w.lives[0]
        self.assertAlmostEqual(life, 350, delta=1)
        self.assertFalse(after)                         # рывка не было
        rs = fillcheck.rows(self.tmp / "levels.csv")
        self.assertEqual(rs[0]["price"], "0.6")
        self.assertEqual(rs[0]["after_move"], "0")

    def test_level_marked_after_move(self):
        w, api, _ = mkw(self.tmp, lead_sources=None, move_bps=3.0)
        feed(w, api, 6.0)
        w.book.on_event({"event_type": "book", "asset_id": "U",
                         "asks": [{"price": "0.60", "size": "10"}]}, api.t)
        api.t += 0.1
        w.book.on_event({"event_type": "book", "asset_id": "U",
                         "asks": [{"price": "0.61", "size": "10"}]}, api.t)
        self.assertTrue(w.lives[0][1])                  # уровень появился после рывка

    def test_unknown_token_not_tracked(self):
        w, api, _ = mkw(self.tmp)
        w.book.on_event({"event_type": "book", "asset_id": "ZZZ",
                         "asks": [{"price": "0.60", "size": "10"}]}, api.t)
        self.assertNotIn("ZZZ", w.open_level)


class MemoryTests(unittest.TestCase):
    """За ночь уровней набирается сотни тысяч: в памяти их держать нельзя."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_lives_bounded_but_counted(self):
        w, api, _ = mkw(self.tmp)
        cap = w.lives.maxlen
        for i in range(cap + 500):
            api.t += 0.01
            w.book.on_event({"event_type": "book", "asset_id": "U",
                             "asks": [{"price": f"{0.10 + (i % 80) * 0.01:.2f}", "size": "10"}]},
                            api.t)
        self.assertLessEqual(len(w.lives), cap)          # память не растёт без предела
        self.assertGreater(w.n_lives, cap)               # но счётчик точный
        self.assertIn(str(w.n_lives), self._status(w))

    def _status(self, w):
        rec = []
        h = logging.Handler()
        h.emit = lambda r: rec.append(r.getMessage())
        lvl = fillcheck.log.level
        fillcheck.log.addHandler(h)
        fillcheck.log.setLevel(logging.INFO)
        try:
            w.status()
        finally:
            fillcheck.log.removeHandler(h)
            fillcheck.log.setLevel(lvl)
        return " ".join(rec)

    def test_dead_token_marks_are_dropped(self):
        w, api, mk = mkw(self.tmp)
        w.last_signal["U"] = api.t
        api.t = mk.end + 1000     # окно давно закончилось (порог очистки 900 с:
        #                           итоги у Polymarket появляются не сразу)
        asyncio.run(w.discover())
        self.assertNotIn("U", w.last_signal)
        self.assertNotIn("U", w.tok_mkt)


class MeasureOffTests(unittest.TestCase):
    """В живом режиме замеры рынка выключены: лишняя работа в цикле задерживает
    отправку заявок (750 мс против 208 мс «чистой» отправки)."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_no_level_tracking(self):
        w, api, _ = mkw(self.tmp, measure=False)
        w.book.on_event({"event_type": "book", "asset_id": "U",
                         "asks": [{"price": "0.60", "size": "10"}]}, api.t)
        api.t += 0.2
        w.book.on_event({"event_type": "book", "asset_id": "U",
                         "asks": [{"price": "0.62", "size": "10"}]}, api.t)
        self.assertEqual(len(w.lives), 0)
        self.assertEqual(w.n_lives, 0)

    def test_no_survival_checks_but_signal_still_fires(self):
        w, api, mk = mkw(self.tmp, measure=False, lead_sources=None, move_bps=3.0)
        fired = []
        w.on_signal = fired.append
        api.t = mk.start + 60
        w.book.on_event({"event_type": "book", "asset_id": "U",
                         "asks": [{"price": "0.60", "size": "10"}]}, api.t - 2)
        feed(w, api, 6.0)
        w.check_signals()
        self.assertEqual(len(fired), 1)        # сигнал есть
        self.assertEqual(len(w.pending), 0)    # а проверок выживания нет

    def test_measure_on_by_default(self):
        w, api, _ = mkw(self.tmp)
        w.book.on_event({"event_type": "book", "asset_id": "U",
                         "asks": [{"price": "0.60", "size": "10"}]}, api.t)
        api.t += 0.2
        w.book.on_event({"event_type": "book", "asset_id": "U",
                         "asks": [{"price": "0.62", "size": "10"}]}, api.t)
        self.assertEqual(w.n_lives, 1)


class SurvivalTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def arm(self, **over):
        w, api, mk = mkw(self.tmp, lead_sources=None, move_bps=3.0, ask_stale_s=1.0, **over)
        api.t = mk.start + 60
        w.book.on_event({"event_type": "book", "asset_id": "U",
                         "asks": [{"price": "0.60", "size": "10"}]}, api.t - 2)
        feed(w, api, 6.0)
        return w, api, mk

    def test_signal_requires_stale_ask(self):
        w, api, mk = self.arm()
        w.book._set("U", None, 0.61, api.t)             # аск только что переставили
        self.assertAlmostEqual(w.book.ask_t["U"], api.t)
        w.check_signals()
        self.assertEqual(len(w.pending), 0)

    def test_price_survives_all_horizons(self):
        w, api, _ = self.arm()
        w.check_signals()
        self.assertEqual(len(w.pending), 1)
        for _ in range(40):                             # цена не меняется
            api.t += 0.05
            w.resolve_pending()
        self.assertEqual(len(w.surv), 1)
        ask0, oks = w.surv[0]
        self.assertAlmostEqual(ask0, 0.60)
        self.assertEqual(oks, [1] * len(HORIZONS_MS))

    def test_price_taken_after_120ms(self):
        w, api, _ = self.arm()
        w.check_signals()
        api.t += 0.12
        w.book.on_event({"event_type": "book", "asset_id": "U",
                         "asks": [{"price": "0.70", "size": "10"}]}, api.t)
        for _ in range(30):
            api.t += 0.05
            w.resolve_pending()
        _, oks = w.surv[0]
        by = dict(zip(HORIZONS_MS, oks))
        self.assertEqual((by[50], by[100]), (1, 1))
        self.assertEqual((by[200], by[500], by[1000]), (0, 0, 0))
        r = fillcheck.rows(self.tmp / "survive.csv")[0]
        self.assertEqual((r["ok_100"], r["ok_200"], r["side"]), ("1", "0", "Up"))

    def test_better_price_counts_as_filled(self):
        w, api, _ = self.arm()
        w.check_signals()
        api.t += 0.06
        w.book.on_event({"event_type": "book", "asset_id": "U",
                         "asks": [{"price": "0.55", "size": "10"}]}, api.t)   # стало дешевле
        for _ in range(30):
            api.t += 0.05
            w.resolve_pending()
        self.assertEqual(w.surv[0][1], [1] * len(HORIZONS_MS))

    def test_new_price_triggers_check(self):
        """Сигнал должен появляться на новой цене, не дожидаясь цикла."""
        w, api, mk = mkw(self.tmp, lead_sources=None, move_bps=3.0, ask_stale_s=1.0)
        api.t = mk.start + 60
        w.book.on_event({"event_type": "book", "asset_id": "U",
                         "asks": [{"price": "0.60", "size": "10"}]}, api.t - 2)
        sym = w.cfg["assets"]["btc"]
        w.prices.on_add = None                        # заполняем прошлое без триггера
        w.prices.add(sym, api.t - 2.0, 100.0, "s0")
        w.prices.on_add = w._on_price
        self.assertEqual(len(w.pending), 0)
        w.prices.add(sym, api.t, 100.06, "s0")        # рывок +6 б.п. -> сигнал сразу
        self.assertEqual(len(w.pending), 1)
        self.assertGreaterEqual(w.counters["fast_trigger"], 1)

    def test_within_tolerance_counts_only_in_tol_column(self):
        """Цена ушла на полцента: строго — мимо, но бот бы купил (допуск 1¢)."""
        w, api, _ = self.arm(fill_tolerance=0.01)
        w.check_signals()
        api.t += 0.06
        w.book.on_event({"event_type": "book", "asset_id": "U",
                         "asks": [{"price": "0.605", "size": "10"}]}, api.t)
        for _ in range(30):
            api.t += 0.05
            w.resolve_pending()
        r = fillcheck.rows(self.tmp / "survive.csv")[0]
        self.assertEqual((r["ok_200"], r["okt_200"]), ("0", "1"))
        self.assertEqual((r["ok_50"], r["okt_50"]), ("1", "1"))   # на 50 мс цена была старой

    def test_beyond_tolerance_misses_both(self):
        w, api, _ = self.arm(fill_tolerance=0.01)
        w.check_signals()
        api.t += 0.06
        w.book.on_event({"event_type": "book", "asset_id": "U",
                         "asks": [{"price": "0.65", "size": "10"}]}, api.t)
        for _ in range(30):
            api.t += 0.05
            w.resolve_pending()
        r = fillcheck.rows(self.tmp / "survive.csv")[0]
        self.assertEqual((r["ok_200"], r["okt_200"]), ("0", "0"))

    def test_late_poll_does_not_distort_horizons(self):
        """Опрос пришёл с опозданием на 2 с, но цена менялась на 300 мс —
        горизонты до 300 мс должны остаться успешными."""
        w, api, _ = self.arm()
        w.check_signals()
        api.t += 0.30
        w.book.on_event({"event_type": "book", "asset_id": "U",
                         "asks": [{"price": "0.80", "size": "10"}]}, api.t)
        api.t += 2.0                                  # единственный опрос — сильно позже
        w.resolve_pending()
        r = fillcheck.rows(self.tmp / "survive.csv")[0]
        self.assertEqual([r[f"ok_{t}"] for t in HORIZONS_MS], ["1", "1", "1", "0", "0"])

    def test_signal_not_repeated_within_gap(self):
        w, api, _ = self.arm(signal_gap_s=5.0)
        w.check_signals()
        api.t += 0.5
        feed(w, api, 6.0)
        w.check_signals()
        self.assertEqual(len(w.pending) + len(w.surv), 1)


class GhostTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    async def test_ghost_detected_when_rest_cheaper(self):
        w, api, _ = mkw(self.tmp)
        w.book._set("U", None, 0.70, api.t)                       # живой стакан: 70¢
        api.books["U"] = {"asks": [{"price": "0.60", "size": "10"}],
                          "timestamp": str(int(api.t * 1000) - 1500)}
        await w.check_rest()
        self.assertEqual((w.rest_n, w.ghost_n), (1, 1))
        r = fillcheck.rows(self.tmp / "ghosts.csv")[0]
        self.assertEqual(r["ghost"], "1")
        self.assertAlmostEqual(float(r["diff_c"]), 10.0, places=1)
        self.assertAlmostEqual(float(r["rest_age_ms"]), 1500, delta=2)

    async def test_no_ghost_when_rest_matches(self):
        w, api, _ = mkw(self.tmp)
        w.book._set("U", None, 0.60, api.t)
        api.books["U"] = {"asks": [{"price": "0.60", "size": "10"}],
                          "timestamp": str(int(api.t * 1000))}
        await w.check_rest()
        self.assertEqual((w.rest_n, w.ghost_n), (1, 0))

    async def test_report_runs(self):
        w, api, _ = mkw(self.tmp)
        w.book._set("U", None, 0.70, api.t)
        api.books["U"] = {"asks": [{"price": "0.60", "size": "10"}],
                          "timestamp": str(int(api.t * 1000))}
        await w.check_rest()
        w.book.on_event({"event_type": "book", "asset_id": "U",
                         "asks": [{"price": "0.60", "size": "10"}]}, api.t)
        api.t += 0.2
        w.book.on_event({"event_type": "book", "asset_id": "U",
                         "asks": [{"price": "0.62", "size": "10"}]}, api.t)
        txt = fillcheck.report(self.tmp)
        self.assertIn("СКОЛЬКО ЖИВЁТ", txt)
        self.assertIn("ПРИЗРАКИ", txt)
        w.status()

    async def test_report_without_data(self):
        self.assertIn("Нет данных", fillcheck.report(self.tmp))


class LoggingTests(unittest.TestCase):
    def test_uses_project_logger(self):
        """Строки статуса должны попадать в тот же журнал, что у остальных ботов.
        Отдельный логгер остаётся без обработчиков, и сообщения молча теряются."""
        import mmsim
        self.assertIs(fillcheck.log, mmsim.log)

    def test_status_failure_does_not_kill_loop(self):
        tmp = Path(tempfile.mkdtemp())
        try:
            w, api, _ = mkw(tmp)
            w.prices = None                      # сломаем статус намеренно
            with self.assertRaises(Exception):
                w.status()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


class AdverseSelectionTests(unittest.TestCase):
    """Разбивка по силе рывка: слабые рывки доходят, сильные забирают до нас."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_breakdown_appears_with_enough_rows(self):
        cols = fillcheck.SURV_COLS
        rows = []
        for i in range(40):                       # слабые рывки — попадаем
            rows.append({"move_bps": 3.0 + i * 0.01, "ask0": 0.6,
                         **{f"ok_{t}": 1 for t in HORIZONS_MS},
                         **{f"okt_{t}": 1 for t in HORIZONS_MS}})
        for i in range(40):                       # сильные рывки — не попадаем
            rows.append({"move_bps": 12.0 + i * 0.01, "ask0": 0.6,
                         **{f"ok_{t}": 0 for t in HORIZONS_MS},
                         **{f"okt_{t}": 0 for t in HORIZONS_MS}})
        with (self.tmp / "survive.csv").open("w", newline="") as fh:
            w = csv.DictWriter(fh, cols)
            w.writeheader()
            for r in rows:
                w.writerow({c: r.get(c, "") for c in cols})
        txt = fillcheck.report(self.tmp)
        self.assertIn("сила рывка", txt)
        self.assertIn("слабее всех", txt)
        self.assertIn("сильнее всех", txt)
        # слабые рывки доходят целиком, сильные не доходят вовсе
        line_weak = next(l for l in txt.splitlines() if "слабее всех" in l)
        line_strong = next(l for l in txt.splitlines() if "сильнее всех" in l)
        self.assertIn("100%", line_weak)
        self.assertIn("0%", line_strong)

    def test_breakdown_with_identical_move_values(self):
        """У многих сигналов сила рывка совпадает — сортировка не должна падать."""
        cols = fillcheck.SURV_COLS
        with (self.tmp / "survive.csv").open("w", newline="") as fh:
            w = csv.DictWriter(fh, cols)
            w.writeheader()
            for i in range(80):
                r = {"move_bps": 3.2, "ask0": 0.6,            # одно и то же значение
                     **{f"ok_{t}": i % 2 for t in HORIZONS_MS},
                     **{f"okt_{t}": i % 2 for t in HORIZONS_MS}}
                w.writerow({c: r.get(c, "") for c in cols})
        txt = fillcheck.report(self.tmp)
        self.assertIn("сила рывка", txt)
        self.assertIn("(3.2-3.2)", txt)

    def test_breakdown_hidden_when_too_few(self):
        cols = fillcheck.SURV_COLS
        with (self.tmp / "survive.csv").open("w", newline="") as fh:
            w = csv.DictWriter(fh, cols)
            w.writeheader()
            w.writerow({c: 1 for c in cols})
        self.assertIn("мало сигналов для разбивки", fillcheck.report(self.tmp))


class DepthTests(unittest.TestCase):
    """Потолок по объёму: стакан 30 билетов по 60¢, 50 по 61¢, 100 по 65¢."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.w, _, _ = mkw(self.tmp)
        self.bk = {"asks": [{"price": "0.65", "size": "100"},
                            {"price": "0.60", "size": "30"},
                            {"price": "0.61", "size": "50"}]}

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_depth_by_price_limit(self):
        d = self.w.depth(self.bk, 0.60)
        self.assertAlmostEqual(d["usd_at_best"], 30 * 0.60, places=2)
        self.assertAlmostEqual(d["usd_within_1c"], 30 * 0.60 + 50 * 0.61, places=2)
        self.assertAlmostEqual(d["usd_within_3c"], d["usd_within_1c"], places=2)  # 65¢ вне +3¢

    def test_vwap_of_100_usd_is_worse_than_best(self):
        d = self.w.depth(self.bk, 0.60)
        self.assertGreater(d["vwap_100usd"], 0.60)        # цена поехала вверх
        self.assertLess(d["vwap_100usd"], 0.65)

    def test_empty_book(self):
        d = self.w.depth({"asks": []}, 0.60)
        self.assertEqual(d["usd_at_best"], 0)
        self.assertEqual(d["vwap_100usd"], "")


class PctTests(unittest.TestCase):
    def test_pct(self):
        xs = list(range(1, 101))
        self.assertEqual(fillcheck.pct(xs, 0), 1)
        self.assertEqual(fillcheck.pct(xs, 100), 100)
        self.assertEqual(fillcheck.pct([], 50), 0.0)
        self.assertTrue(40 <= fillcheck.pct(xs, 50) <= 60)


if __name__ == "__main__":
    unittest.main()
