import re
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


class OrderTimeSafetyTests(unittest.TestCase):
    """Замер не должен отправлять заявки, кроме явно затребованных безопасных."""

    def setUp(self):
        self.src = (ROOT / "scripts" / "ordertime.py").read_text()
        i = self.src.index("async def post_probe(")
        self.probe = self.src[i:self.src.index("async def run(", i)]
        self.rest = self.src[:i] + self.src[self.src.index("async def run("):]

    def test_posting_only_inside_post_probe(self):
        self.assertNotIn("post_order(", self.rest,
                         "отправка заявки допустима только внутри post_probe")
        self.assertIn("post_order(", self.probe)

    def test_probe_price_cannot_fill(self):
        """Цена 1 цент при рыночной 0.60 — встречных заявок быть не может."""
        self.assertIn('max_price="0.01"', self.probe)
        self.assertIn('side="BUY"', self.probe)
        self.assertIn('order_type="FAK"', self.probe)   # остаток не повиснет

    def test_probe_is_off_by_default(self):
        self.assertIn('"--post", type=int, default=0', self.src)
        self.assertIn("if post:", self.src)

    def test_probe_warns_if_it_ever_fills(self):
        self.assertIn("ПРОВЕРЬ", self.probe)

    def test_no_other_order_methods(self):
        for forbidden in ("place_market_order(", "place_limit_order(", "post_orders("):
            self.assertNotIn(forbidden, self.src)

    def test_imports_cleanly(self):
        sys.path.insert(0, str(ROOT / "scripts"))
        import ordertime
        self.assertTrue(callable(ordertime.post_probe))


if __name__ == "__main__":
    unittest.main()
