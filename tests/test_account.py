"""scripts/account.py — только чтение, и читает оно правильно."""
from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import account  # noqa: E402


class SafetyTests(unittest.TestCase):
    def test_never_sends_an_order(self):
        src = (Path(__file__).resolve().parent.parent / "scripts" / "account.py").read_text()
        for bad in ("post_order", "create_market_order", "place_limit_order", "cancel"):
            self.assertNotIn(bad, src, f"скрипт только читает, а нашёлся {bad}")


class PageItemsTests(unittest.IsolatedAsyncioTestCase):
    async def test_plain_list(self):
        self.assertEqual(await account.page_items([1, 2]), [1, 2])

    async def test_awaitable(self):
        async def f():
            return [7]
        self.assertEqual(await account.page_items(f()), [7])

    async def test_paginator(self):
        class Page:
            items = ["a", "b"]

        class Pg:
            async def first_page(self):
                return Page()

        self.assertEqual(await account.page_items(Pg()), ["a", "b"])

    async def test_async_iterator(self):
        class It:
            async def __aiter__(self):
                for x in (1, 2, 3):
                    yield x

        self.assertEqual(await account.page_items(It()), [1, 2, 3])

    async def test_nothing(self):
        self.assertEqual(await account.page_items(None), [])


class BriefTests(unittest.TestCase):
    def test_shows_the_fields_that_matter(self):
        s = account.brief({"side": "BUY", "price": "0.61", "size": "5", "junk": "x"})
        self.assertIn("side=BUY", s)
        self.assertIn("price=0.61", s)
        self.assertNotIn("junk", s)

    def test_money_shown_in_dollars_not_microunits(self):
        # биржа отдаёт 1930000 = $1.93
        self.assertIn("$1.93", account.brief({"amount": 1930000}))

    def test_empty_fields_skipped(self):
        self.assertNotIn("price", account.brief({"side": "BUY", "price": 0}))


class SummingTests(unittest.IsolatedAsyncioTestCase):
    async def test_totals_use_dollars(self):
        rows = [{"amount": 1930000}, {"amount": 2000000}]
        tot = 0.0
        for r in rows:
            tot += account.to_units(account._num(r["amount"]), 1e4)
        self.assertAlmostEqual(tot, 3.93)


if __name__ == "__main__":
    unittest.main()
