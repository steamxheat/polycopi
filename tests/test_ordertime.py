import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


class OrderTimeSafetyTests(unittest.TestCase):
    """Скрипт замера НЕ должен отправлять заявки — только подписывать."""

    def setUp(self):
        self.src = (ROOT / "scripts" / "ordertime.py").read_text()

    def test_never_posts_an_order(self):
        """Ищем именно ВЫЗОВ (со скобкой): в описании эти имена упоминаться могут."""
        for forbidden in ("post_order(", "place_market_order(", "place_limit_order(",
                          "post_orders("):
            self.assertNotIn(forbidden, self.src,
                             f"ordertime.py не должен вызывать {forbidden}")

    def test_only_signs_and_reads(self):
        self.assertIn("create_market_order", self.src)
        self.assertIn("get_book", self.src)

    def test_imports_cleanly(self):
        sys.path.insert(0, str(ROOT / "scripts"))
        import ordertime
        self.assertTrue(callable(ordertime.run))
        self.assertTrue(callable(ordertime.pick_token))


if __name__ == "__main__":
    unittest.main()
