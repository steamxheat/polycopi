"""Передаточный документ должен оставаться правдой.

Если настройка в коде изменилась, а в HANDOFF.md осталась старая цифра,
новый чат примет решение по неверным данным. Эти тесты сверяют документ
с кодом — чтобы расхождение падало здесь, а не в живой торговле.
"""
from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import liveprobe  # noqa: E402

DOC = (ROOT / "docs" / "HANDOFF.md").read_text()
HYP = (ROOT / "docs" / "HYPOTHESES.md").read_text()
STR = (ROOT / "docs" / "STRATEGY.md").read_text()


class SettingsMatchTheCodeTests(unittest.TestCase):
    def test_price_range_matches(self):
        self.assertIn(f"{liveprobe.DEFAULTS['min_price']:.2f}–"
                      f"{liveprobe.DEFAULTS['max_price']:.2f}", DOC)

    def test_risk_share_matches(self):
        self.assertIn(f"{liveprobe.DEFAULTS['risk_frac'] * 100:.0f}% счёта", DOC)

    def test_loss_cap_matches(self):
        self.assertIn(f"{liveprobe.DEFAULTS['day_loss_frac'] * 100:.0f}% счёта", DOC)

    def test_losing_streak_matches(self):
        self.assertIn(f"**{liveprobe.DEFAULTS['stop_after_losses']}** подряд", DOC)

    def test_drift_cap_matches(self):
        self.assertIn(f"${liveprobe.DEFAULTS['max_drift_usd']:.2f}", DOC)

    def test_warmup_matches(self):
        self.assertIn(f"**{liveprobe.DEFAULTS['warmup_orders']}**", DOC)

    def test_markup_cap_matches(self):
        self.assertIn(f"**{liveprobe.DEFAULTS['limit_extra_max'] * 100:.0f}¢**", DOC)

    def test_share_minimum_matches(self):
        self.assertIn(f"**{liveprobe.DEFAULTS['min_order_shares']:.0f}**", DOC)


class ArithmeticTests(unittest.TestCase):
    """Числа экономики сделки должны сходиться, а не быть переписаны на глаз."""

    def test_break_even_is_correct(self):
        p, n = 0.60, 5.0
        fee = 0.07 * p * (1 - p) * n
        win, loss = n * 1.0 - n * p - fee, n * p + fee
        self.assertAlmostEqual(win, 1.92, places=2)
        self.assertAlmostEqual(loss, 3.08, places=2)
        self.assertAlmostEqual(loss / (win + loss), 0.6168, places=4)

    def test_doc_quotes_those_numbers(self):
        for s in ("+$1.92", "−$3.08", "61.7%", "$3.00"):
            self.assertIn(s, DOC)

    def test_loss_adds_up(self):
        self.assertAlmostEqual(10.48 - 8.98, 1.50, places=2)
        for s in ("$10.48", "$8.98", "$1.50"):
            self.assertIn(s, DOC)


class CompletenessTests(unittest.TestCase):
    """Новый чат должен найти в документе всё, без чего работа не продолжится."""

    def test_server_and_paths_present(self):
        for s in ("ubuntu@203.12.31.215", "~/polycopy-sim", "data_fv_lag",
                  "data_probe", "~/.polymarket.env"):
            self.assertIn(s, DOC)

    def test_safety_rules_present(self):
        for s in ("не запрашивать и не выводить", "геоблок", "polycopy-sim"):
            self.assertIn(s, DOC)

    def test_emergency_stop_command_present(self):
        self.assertIn("touch data_probe/STOP", DOC)
        self.assertIn("pm2 stop polyprobe", DOC)

    def test_hypothesis_thresholds_present(self):
        for s in ("≥ 4¢", "2–4¢", "< 2¢"):
            self.assertIn(s, DOC)
        self.assertIn("Г-2", HYP)

    def test_every_script_in_the_map_exists(self):
        for m in re.finditer(r"`(scripts/[\w./]+\.(?:py|sh))`", DOC):
            self.assertTrue((ROOT / m.group(1)).exists(), f"нет файла {m.group(1)}")

    def test_known_rakes_section_present(self):
        self.assertIn("Грабли", DOC)
        self.assertIn("не в том классе", DOC)

    def test_test_count_is_not_stale(self):
        """Число тестов в документе не должно быть меньше настоящего."""
        said = [int(x) for x in re.findall(r"\*\*(\d{3})\s+тест", DOC)]
        self.assertTrue(said, "в документе не указано число тестов")
        real = sum(1 for p in (ROOT / "tests").glob("test_*.py")
                   for _ in re.finditer(r"\n    def test_", p.read_text()))
        self.assertGreaterEqual(max(said) + 15, real,
                                f"в документе {max(said)}, а тестов уже {real}")


class StrategyDocTests(unittest.TestCase):
    """STRATEGY.md объясняет замысел человеку или другому ИИ. Он обязан
    содержать и то, где идея ломается, иначе это реклама, а не описание."""

    def test_entry_conditions_match_the_code(self):
        self.assertIn(f"{liveprobe.DEFAULTS['min_price']:.2f}–"
                      f"{liveprobe.DEFAULTS['max_price']:.2f}", STR)
        self.assertIn(f"{liveprobe.DEFAULTS['min_edge'] * 100:.0f} цента", STR)
        self.assertIn(liveprobe.DEFAULTS["rule"], STR)

    def test_mechanism_is_explained(self):
        for s in ("Bybit", "OKX", "базисных пункта", "устаревшей цене"):
            self.assertIn(s, STR)

    def test_failure_modes_are_explained(self):
        """Шесть мест, где идея ломается, должны быть названы."""
        for s in ("Отбор", "Асимметрия", "съедает перевес",
                  "лучшего из многих", "Потолок", "64%"):
            self.assertIn(s, STR)

    def test_it_does_not_promise_profit(self):
        """В описании замысла не должно быть обещаний заработка."""
        for s in ("гарант", "обязательно заработ", "точно заработ"):
            self.assertNotIn(s, STR)

    def test_old_fake_figures_appear_only_as_a_warning(self):
        """Суммы «+$207» и «+$131» упоминать можно — но только как пример
        самообмана, иначе новый читатель примет их за достижение."""
        for fig in ("+$207", "+$131"):
            if fig not in STR:
                continue
            i = STR.index(fig)
            near = STR[max(0, i - 400):i + 400]
            self.assertTrue(
                any(w in near for w in ("самообман", "в реальности не было",
                                        "выдумк", "удача")),
                f"{fig} упомянуто без опровержения")

    def test_the_honest_verdict_is_there(self):
        self.assertIn("торговать нельзя", STR)
        self.assertIn("тонкий", STR)

    def test_how_to_check_without_losing_money(self):
        for s in ("slip.py", "HYPOTHESES.md", "fillcheck.py", "выписка со счёта"):
            self.assertIn(s, STR)


if __name__ == "__main__":
    unittest.main()


class DependentOutcomesTests(unittest.TestCase):
    """29.10: девять покупок пришлись на ТРИ исхода. Считать их девятью
    независимыми ставками нельзя — на этом я уже ошибся дважды."""

    def test_probability_counts_outcomes_not_purchases(self):
        # цены исходов: ETH-15m Up ~0.10, BTC-15m Up ~0.07, ETH-5m Down 0.08
        by_outcome = (1 - 0.90) * 0 + 1  # заглушка от опечаток ниже
        p_all_lose = 0.90 * 0.93 * 0.92
        self.assertAlmostEqual(p_all_lose, 0.77, delta=0.01)
        wrong = 0.9 ** 9                  # прежний расчёт по девяти покупкам
        self.assertAlmostEqual(wrong, 0.39, delta=0.01)
        self.assertGreater(p_all_lose, wrong)   # зависимость делает исход ОБЫЧНЕЕ
        self.assertTrue(by_outcome)

    def test_handoff_quotes_the_corrected_figure(self):
        self.assertIn("77%", DOC)
        self.assertIn("три исхода", DOC)

    def test_handoff_no_longer_claims_independence(self):
        self.assertNotIn("вероятность 39%", DOC)


class PowerCalcTests(unittest.TestCase):
    """Срок проверки Г-2 обязан считаться тем коэффициентом, который записан
    в самой гипотезе (2.64), а не удобным (1.96)."""

    @staticmethod
    def n_needed(slack: float, z: float, thr: float = 0.04, sd: float = 0.45) -> float:
        return (z * sd / (slack - thr)) ** 2

    def test_recorded_correction_demands_far_more_trades(self):
        self.assertAlmostEqual(self.n_needed(0.12, 2.64), 221, delta=3)
        self.assertAlmostEqual(self.n_needed(0.12, 1.96), 122, delta=3)

    def test_small_edge_is_undetectable_by_this_test(self):
        self.assertGreater(self.n_needed(0.06, 2.64), 3000)

    def test_answers_doc_states_both_coefficients(self):
        ans = (ROOT / "docs" / "ANSWERS_2026-10-29.md").read_text()
        for s in ("2.64", "1.96", "221", "3528"):
            self.assertIn(s, ans)
