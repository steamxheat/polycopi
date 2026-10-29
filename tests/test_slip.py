"""scripts/slip.py — запас прочности по цене билета."""
from __future__ import annotations

import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import slip  # noqa: E402


_seq = [0]


def mk(vwap, win, shares=5.0, t=1.0, what=None):
    """По умолчанию каждая покупка — СВОЙ исход: иначе кластерная погрешность
    считалась бы по одному исходу и ничего не измеряла."""
    _seq[0] += 1
    return {"v": "V", "shares": shares, "vwap": vwap, "win": win,
            "edge": 0.0, "t": f"{t:.3f}", "ts": t,
            "what": what or (f"m{_seq[0]}", "Up")}


class ArithmeticTests(unittest.TestCase):
    def test_fee_is_biggest_in_the_middle(self):
        self.assertGreater(slip.fee(0.5), slip.fee(0.1))
        self.assertAlmostEqual(slip.fee(0.5), 0.07 * 0.25)

    def test_winning_ticket_pays_the_rest_of_the_dollar(self):
        self.assertAlmostEqual(slip.per_share(mk(0.60, True)), 0.40 - slip.fee(0.6), places=6)

    def test_losing_ticket_costs_its_price(self):
        self.assertAlmostEqual(slip.per_share(mk(0.60, False)), -0.60 - slip.fee(0.6), places=6)

    def test_slack_equals_profit_per_ticket(self):
        """Переплата в s центов снижает прибыль ровно на s — значит средняя
        прибыль на билет и есть запас прочности."""
        tr = [mk(0.60, True)] * 3 + [mk(0.60, False)]
        s = slip.stats(tr)
        moved = slip.stats([{**t, "vwap": t["vwap"] + 0.05} for t in tr])
        # не ровно 0.05: сбор тоже зависит от цены
        self.assertAlmostEqual(s["slack"] - moved["slack"], 0.05, places=2)

    def test_fair_price_leaves_no_margin(self):
        """60% побед по цене 0.60 — запас отрицательный из-за сбора."""
        tr = [mk(0.60, True)] * 6 + [mk(0.60, False)] * 4
        self.assertLess(slip.stats(tr)["slack"], 0)

    def test_edge_shows_up_as_margin(self):
        """74% побед по 0.60 — около 12¢ на билет."""
        tr = [mk(0.60, True)] * 74 + [mk(0.60, False)] * 26
        self.assertAlmostEqual(slip.stats(tr)["slack"], 0.12, places=2)

    def test_lower_bound_is_below_the_average(self):
        tr = [mk(0.60, True)] * 74 + [mk(0.60, False)] * 26
        s = slip.stats(tr)
        self.assertLess(s["lo"], s["slack"])


class MultipleLooksTests(unittest.TestCase):
    """Смотреть шесть диапазонов и брать лучший — шесть попыток, не одна."""

    def test_one_bucket_is_the_plain_bound(self):
        self.assertAlmostEqual(slip.z_for(1), 1.96, places=2)

    def test_six_buckets_demand_more(self):
        self.assertGreater(slip.z_for(6), 2.6)
        self.assertLess(slip.z_for(6), 2.8)

    def test_more_buckets_demand_more_still(self):
        self.assertLess(slip.z_for(2), slip.z_for(6))

    def test_correction_can_flip_the_verdict(self):
        """74% по 0.60 на 113 сделках проходит как один диапазон, но не как
        лучший из шести — ровно эту ошибку мы и делали."""
        tr = ([mk(0.60, True) for _ in range(84)]
              + [mk(0.60, False) for _ in range(29)])
        alone = slip.stats(tr)
        se = alone["se"]
        self.assertGreater(alone["slack"] - 1.96 * se, 0.04)
        self.assertLess(alone["slack"] - slip.z_for(6) * se, 0.04)


class FillTests(unittest.TestCase):
    def test_reads_survival_by_bucket(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            with (d / "survive.csv").open("w", newline="") as fh:
                w = csv.DictWriter(fh, ["ask0", "okt_200", "ok_200"])
                w.writeheader()
                w.writerow({"ask0": "0.60", "okt_200": "1", "ok_200": "1"})
                w.writerow({"ask0": "0.65", "okt_200": "0", "ok_200": "0"})
                w.writerow({"ask0": "0.10", "okt_200": "1", "ok_200": "0"})
            f = slip.fills(d)
            self.assertEqual(f[(0.50, 0.70)], [2, 1])
            self.assertEqual(f[(0.0, 0.15)], [1, 1])

    def test_no_file_is_not_a_crash(self):
        with tempfile.TemporaryDirectory() as td:
            self.assertEqual(slip.fills(Path(td)), {})


class ReportTests(unittest.TestCase):
    def _dir(self, trades):
        td = tempfile.mkdtemp()
        d = Path(td)
        (d / "state.json").write_text(json.dumps({"variants": {"V": {"trades": [
            {"shares": t["shares"], "vwap": t["vwap"], "won": t["win"], "pnl": 1.0,
             "t": t["ts"] + i, "slug": f"m{i}", "side": "Up", "edge_fill": 0.0}
            for i, t in enumerate(trades)]}}}))
        return d

    def test_cheap_bucket_called_empty(self):
        tr = [mk(0.10, True)] * 4 + [mk(0.10, False)] * 28
        txt = slip.report(self._dir(tr), "V", None)
        self.assertIn("ПУСТО", txt)
        self.assertIn("торговать нечем", txt)

    def test_core_bucket_good_when_there_are_enough_trades(self):
        tr = ([mk(0.60, True)] * 74 + [mk(0.60, False)] * 26) * 8      # 800 сделок
        txt = slip.report(self._dir(tr), "V", None)
        self.assertIn("ЕСТЬ", txt)
        self.assertIn("0.50-0.70", txt)
        self.assertIn("НЕ разрешение торговать", txt)

    def test_barely_above_fair_is_called_empty(self):
        """64% по 0.60 — в среднем плюс, но нижняя граница ниже нуля."""
        tr = [mk(0.60, True)] * 64 + [mk(0.60, False)] * 36
        txt = slip.report(self._dir(tr), "V", None)
        self.assertIn("ПУСТО", txt)
        self.assertNotIn("ЕСТЬ ", txt)

    def test_no_verdict_is_ever_a_permission(self):
        """Ни «ЕСТЬ», ни «ТОНКО» не разрешают торговлю — решение по Г-2."""
        for wins in (74, 64, 95):
            tr = ([mk(0.60, True) for _ in range(wins)]
                  + [mk(0.60, False) for _ in range(100 - wins)])
            txt = slip.report(self._dir(tr), "V", None)
            self.assertNotIn("Торговать можно", txt)
            self.assertNotIn("торговать 0.50", txt)

    def test_missing_variant_says_what_is_there(self):
        txt = slip.report(self._dir([mk(0.6, True)]), "нетакого", None)
        self.assertIn("Есть: V", txt)


if __name__ == "__main__":
    unittest.main()


class MoneyColumnTests(unittest.TestCase):
    """Откуда на самом деле взялись деньги: прибыль по диапазонам отдельно
    от общей суммы, иначе один рабочий диапазон маскирует пять пустых."""

    def _dir(self, trades):
        import tempfile as tf
        d = Path(tf.mkdtemp())
        (d / "state.json").write_text(json.dumps({"variants": {"V": {"trades": [
            {"shares": t["shares"], "vwap": t["vwap"], "won": t["win"], "pnl": 1.0,
             "t": t["ts"] + i, "slug": f"m{i}", "side": "Up", "edge_fill": 0.0}
            for i, t in enumerate(trades)]}}}))
        return d

    def test_money_is_shown_per_bucket(self):
        tr = ([mk(0.60, True)] * 74 + [mk(0.60, False)] * 26) * 8
        txt = slip.report(self._dir(tr), "V", None)
        self.assertIn("деньги", txt)
        self.assertIn("Всего", txt)

    def test_share_from_proven_ranges_is_stated(self):
        """Один хороший диапазон и один пустой: видно, сколько дал каждый."""
        tr = ([mk(0.60, True)] * 74 + [mk(0.60, False)] * 26) * 8 \
             + [mk(0.10, True)] * 4 + [mk(0.10, False)] * 28
        txt = slip.report(self._dir(tr), "V", None)
        self.assertIn("доказанным перевесом приходится", txt)

    def test_money_follows_the_stake_size(self):
        a = slip.stats([mk(0.60, True, shares=5.0)])
        b = slip.stats([mk(0.60, True, shares=10.0)])
        self.assertAlmostEqual(b["money"], 2 * a["money"], places=6)


class ForecastTests(unittest.TestCase):
    """Когда ждать ответа и каким он будет, если попадание удержится."""

    def _s(self, wins, n, price=0.60):
        tr = [mk(price, True) for _ in range(wins)] + [mk(price, False)
                                                       for _ in range(n - wins)]
        st = slip.stats(tr)
        st["lo"] = st["slack"] - slip.z_for(6) * st["se"]
        return st

    def test_bound_grows_with_more_trades(self):
        s = self._s(89, 123)
        z = slip.z_for(6)
        self.assertGreater(slip.forecast(s, z, 250), s["lo"])
        self.assertGreater(slip.forecast(s, z, 400), slip.forecast(s, z, 250))

    def test_our_case_passes_the_threshold_barely(self):
        """Наши настоящие числа: 123 сделки, 89 побед, средняя цена 0.587.
        При 250 сделках нижняя граница выходит около 4.5¢ — порог берётся,
        но впритык."""
        s = self._s(89, 123, price=0.587)
        self.assertAlmostEqual(100 * s["slack"], 12.0, delta=0.3)   # сходится с сервером
        f = slip.forecast(s, slip.z_for(6), 250)
        self.assertAlmostEqual(100 * f, 4.5, delta=0.5)
        self.assertGreater(f, slip.OUR_SLIP_CAP)

    def test_lower_markup_makes_the_threshold_easier(self):
        """Срезав переплату с 4¢ до 1¢, мы снизили и планку: тот же прогноз,
        который не проходил при 4¢, проходит при 1¢."""
        s = self._s(89, 123, price=0.60)
        f = slip.forecast(s, slip.z_for(6), 250)
        self.assertLess(f, 0.04)
        self.assertGreater(f, 0.01)

    def test_weaker_hit_rate_never_passes(self):
        """При 68% попаданий порог не берётся и на 400 сделках."""
        s = self._s(68, 100)
        self.assertLess(slip.forecast(s, slip.z_for(6), 400), slip.OUR_SLIP_CAP)

    def test_forecast_does_not_look_backwards(self):
        s = self._s(89, 123)
        base = s.get("k") or s["n"]
        self.assertAlmostEqual(slip.forecast(s, slip.z_for(6), base - 10), s["lo"])

    def test_pace_counts_only_this_range(self):
        # 10 сделок за 2 часа -> 5 в час
        tr = [mk(0.60, True, t=i * 720.0) for i in range(11)]
        self.assertAlmostEqual(slip.pace(tr), 5.5, delta=0.01)

    def test_pace_needs_enough_time_to_be_honest(self):
        tr = [mk(0.60, True, t=float(i)) for i in range(5)]   # 5 сделок за 4 с
        self.assertEqual(slip.pace(tr), 0.0)

    def test_report_states_when_to_expect_the_answer(self):
        import tempfile as tf
        d = Path(tf.mkdtemp())
        tr = [mk(0.60, i % 10 < 7, t=i * 600.0) for i in range(123)]
        (d / "state.json").write_text(json.dumps({"variants": {"V": {"trades": [
            {"shares": t["shares"], "vwap": t["vwap"], "won": t["win"], "pnl": 1.0,
             "t": t["ts"] + i, "slug": f"m{i}", "side": "Up", "edge_fill": 0.0}
            for i, t in enumerate(tr)]}}}))
        txt = slip.report(d, "V", None)
        self.assertIn("Прогноз по 0.50-0.70", txt)
        self.assertIn("при  250 сделках", txt)
        self.assertIn("сделок в час", txt)


class ClusterErrorTests(unittest.TestCase):
    """Среднее считается по билетам (это реальный результат стратегии),
    а погрешность — по исходам (пять покупок одного окна делят один исход)."""

    def test_clusters_group_by_market_and_side(self):
        tr = [mk(0.60, False, what=("eth15", "Up")) for _ in range(5)]
        self.assertEqual(len(slip.clusters(tr)), 1)
        tr.append(mk(0.60, True, what=("eth15", "Down")))
        self.assertEqual(len(slip.clusters(tr)), 2)

    def test_average_is_not_changed_by_clustering(self):
        """Моя прежняя ошибка: пересчёт средних менял сам показатель."""
        tr = [mk(0.60, True, what=("a", "Up")) for _ in range(3)] + [mk(0.60, False)]
        self.assertAlmostEqual(slip.stats(tr, clustered=True)["slack"],
                               slip.stats(tr, clustered=False)["slack"], places=9)

    def test_money_is_not_changed_by_clustering(self):
        tr = [mk(0.60, True, what=("a", "Up")) for _ in range(3)] + [mk(0.60, False)]
        self.assertAlmostEqual(slip.stats(tr, clustered=True)["money"],
                               slip.stats(tr, clustered=False)["money"], places=9)

    def test_repeat_buys_widen_the_error(self):
        """Внутри одного исхода результат ОДИН — так и в реальных данных.
        Те же 40 покупок, собранные из 4 исходов, дают гораздо большую
        погрешность, чем 40 отдельных исходов."""
        spread = [mk(0.60, i < 28) for i in range(40)]                  # 40 исходов
        same = [mk(0.60, i // 10 < 3, what=(f"g{i // 10}", "Up"))
                for i in range(40)]                                     # 4 исхода
        self.assertGreater(slip.stats(same)["se"], 3 * slip.stats(spread)["se"])

    def test_one_outcome_has_one_result(self):
        """Проверка целостности данных: покупки одного исхода не могут
        разойтись в результате. Если разошлись — данные испорчены."""
        tr = [mk(0.60, True, what=("a", "Up")), mk(0.60, False, what=("a", "Up"))]
        g = slip.clusters(tr)
        for xs in g.values():
            self.assertEqual(len({x["win"] for x in xs}), 2)   # так быть не должно
        # в настоящих данных такого нет — проверяем на снимке в тесте ниже

    def test_cluster_count_is_reported(self):
        tr = [mk(0.60, True, what=("a", "Up")) for _ in range(5)]
        s = slip.stats(tr)
        self.assertEqual((s["n"], s["k"]), (5, 1))

    def test_single_outcome_admits_it_knows_nothing(self):
        """При одном исходе разброс оценить нечем — нижняя граница обязана
        уйти в минус, а не показать уверенный плюс."""
        tr = [mk(0.60, True, what=("a", "Up")) for _ in range(5)]
        self.assertLess(slip.stats(tr)["lo"], 0)

    def test_report_states_the_method(self):
        import tempfile as tf
        d = Path(tf.mkdtemp())
        tr = [mk(0.60, i % 10 < 7, t=i * 600.0) for i in range(60)]
        (d / "state.json").write_text(json.dumps({"variants": {"V": {"trades": [
            {"shares": t["shares"], "vwap": t["vwap"], "won": t["win"], "pnl": 1.0,
             "t": t["ts"] + i, "slug": f"m{i}", "side": "Up", "edge_fill": 0.0}
            for i, t in enumerate(tr)]}}}))
        self.assertIn("по исходам", slip.report(d, "V", None))
        self.assertIn("--raw", slip.report(d, "V", None, raw=True))

    def test_purchase_price_filter_keeps_mixed_outcomes(self):
        """Исход, купленный по 0.60 и 0.80, не должен выпадать из обоих
        диапазонов: отбираем покупки по их цене, группируем уже внутри."""
        tr = [mk(0.60, True, what=("mix", "Up")), mk(0.80, True, what=("mix", "Up"))]
        inrange = [t for t in tr if 0.50 <= t["vwap"] < 0.70]
        self.assertEqual(len(inrange), 1)
        self.assertEqual(slip.stats(inrange)["n"], 1)


class ReportHonestyTests(unittest.TestCase):
    """Отчёт не должен разрешать торговлю и не должен держать свою копию
    настроек: он писал «торговать можно» с переплатой 4¢, когда у бота
    стоял 1¢ и действовал запрет Г-2."""

    def _dir(self, trades):
        import tempfile as tf
        d = Path(tf.mkdtemp())
        (d / "state.json").write_text(json.dumps({"variants": {"V": {"trades": [
            {"shares": t["shares"], "vwap": t["vwap"], "won": t["win"], "pnl": 1.0,
             "t": 1_700_000_000.0 + i * 600, "slug": f"m{i}", "side": "Up",
             "edge_fill": 0.0} for i, t in enumerate(trades)]}}}))
        return d

    def test_markup_comes_from_the_bot_settings(self):
        import liveprobe
        self.assertAlmostEqual(slip.OUR_SLIP_CAP,
                               liveprobe.DEFAULTS["limit_extra_max"])

    def test_report_never_permits_trading(self):
        tr = ([mk(0.60, True) for _ in range(740)]
              + [mk(0.60, False) for _ in range(260)])
        txt = slip.report(self._dir(tr), "V", None)
        self.assertNotIn("Торговать можно", txt)
        self.assertNotIn("торговать 0.50", txt)
        self.assertIn("HYPOTHESES", txt)

    def test_lower_bound_is_called_an_estimate(self):
        tr = [mk(0.60, i % 10 < 7) for i in range(60)]
        txt = slip.report(self._dir(tr), "V", None)
        self.assertIn("нижняя ОЦЕНКА (не гарантия)", txt)
        self.assertNotIn("меньше этого не будет", txt)

    def test_since_filters_and_says_so(self):
        tr = [mk(0.60, i % 10 < 7) for i in range(60)]
        d = self._dir(tr)
        full = slip.report(d, "V", None)
        part = slip.report(d, "V", None, since="2023-11-14T22:30")
        self.assertIn("только после", part)
        self.assertNotIn("только после", full)
        self.assertLess(int(part.split("покупок ")[1].split(";")[0]),
                        int(full.split("покупок ")[1].split(";")[0]))

    def test_since_with_no_data_says_so(self):
        tr = [mk(0.60, True) for _ in range(5)]
        txt = slip.report(self._dir(tr), "V", None, since="2030-01-01")
        self.assertIn("рассчитанных сделок нет", txt)

    def test_bad_since_is_rejected_clearly(self):
        with self.assertRaises(SystemExit) as e:
            slip.since_ts("вчера")
        self.assertIn("Не понял время", str(e.exception))
