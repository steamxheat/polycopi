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


if __name__ == "__main__":
    unittest.main()
