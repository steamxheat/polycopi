import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import wherehost  # noqa: E402


def probe(city, total, dns=2, tcp=4, tls=30, status=None):
    """ВНИМАНИЕ: Globalping отдаёт время в МИЛЛИСЕКУНДАХ."""
    r = {"probe": {"city": city, "country": "XX"}, "result": {}}
    if total is None:
        r["result"]["status"] = status or "failed"
    else:
        r["result"]["timings"] = {"dns": dns, "tcp": tcp, "tls": tls, "total": total}
    return r


class StartTests(unittest.TestCase):
    def test_three_probes_per_city(self):
        seen = {}

        def fake(url, body, timeout=20):
            seen["body"] = body
            return {"id": "m"}
        wherehost.start("x", ["Dublin"], poster=fake)
        self.assertEqual(seen["body"]["locations"][0]["limit"], 3)

    def test_request_body(self):
        seen = {}

        def fake(url, body, timeout=20):
            seen["url"], seen["body"] = url, body
            return {"id": "m123"}
        mid = wherehost.start("clob.polymarket.com", ["Dublin", "Amsterdam"], poster=fake)
        self.assertEqual(mid, "m123")
        self.assertEqual(seen["url"], wherehost.API)
        self.assertEqual(seen["body"]["type"], "http")
        self.assertEqual(seen["body"]["target"], "clob.polymarket.com")
        self.assertEqual([l["city"] for l in seen["body"]["locations"]],
                         ["Dublin", "Amsterdam"])
        self.assertEqual(seen["body"]["measurementOptions"]["protocol"], "HTTPS")


class WaitTests(unittest.TestCase):
    def test_polls_until_finished(self):
        seq = [{"status": "in-progress"}, {"status": "in-progress"}, {"status": "finished", "ok": 1}]
        calls = []

        def fake(url, timeout=20):
            calls.append(url)
            return seq[min(len(calls) - 1, len(seq) - 1)]
        d = wherehost.wait("m1", getter=fake, sleep=lambda s: None)
        self.assertEqual(d["ok"], 1)
        self.assertEqual(len(calls), 3)

    def test_gives_up_eventually(self):
        d = wherehost.wait("m1", getter=lambda u, timeout=20: {"status": "in-progress"},
                           sleep=lambda s: None, tries=3)
        self.assertEqual(d["status"], "in-progress")


class RowsTests(unittest.TestCase):
    def test_sorted_and_warm_time_excludes_handshakes(self):
        d = {"results": [probe("Amsterdam", 50), probe("Dublin", 45)]}
        rs = wherehost.rows(d)
        self.assertEqual([r[0] for r in rs], ["Dublin", "Amsterdam"])   # быстрее первым
        _city, total, warm, _n, _ = rs[0]
        self.assertAlmostEqual(total, 45)
        self.assertAlmostEqual(warm, 45 - 2 - 4 - 30)

    def test_takes_best_probe_per_city(self):
        """Один плохой узел не должен портить город: берём лучший."""
        d = {"results": [probe("Amsterdam", 297), probe("Amsterdam", 49),
                         probe("Amsterdam", 60)]}
        rs = wherehost.rows(d)
        self.assertEqual(len(rs), 1)
        _city, total, _warm, n, _ = rs[0]
        self.assertAlmostEqual(total, 49)
        self.assertEqual(n, 3)

    def test_warm_never_negative(self):
        d = {"results": [probe("Dublin", 10, dns=5, tcp=5, tls=20)]}
        self.assertEqual(wherehost.rows(d)[0][2], 0.0)

    def test_failed_probe_goes_last(self):
        d = {"results": [probe("Dublin", None, status="timeout"), probe("Amsterdam", 50)]}
        rs = wherehost.rows(d)
        self.assertEqual(rs[0][0], "Amsterdam")
        self.assertEqual(rs[-1][4], "timeout")

    def test_missing_fields_do_not_crash(self):
        self.assertEqual(wherehost.rows({}), [])
        self.assertEqual(wherehost.rows({"results": [{}]})[0][0], "?")


class ReportTests(unittest.TestCase):
    def test_milliseconds_printed_as_is(self):
        """Ловушка: если время принять за секунды, в отчёте будет 48000мс."""
        txt = wherehost.report({"results": [probe("Dublin", 48)]})
        line = next(l for l in txt.splitlines() if l.startswith("Dublin"))
        self.assertIn("48мс", line)
        self.assertNotIn("48000", txt)

    def test_marks_blocked_countries(self):
        d = {"results": [probe("London", 40), probe("Amsterdam", 50)]}
        txt = wherehost.report(d)
        line = next(l for l in txt.splitlines() if l.startswith("London"))
        self.assertIn("❌", line)                       # быстрый, но торговать нельзя
        self.assertIn("Лучший из РАЗРЕШЁННЫХ: Amsterdam", txt)

    def test_recommends_move_when_gain_is_big(self):
        d = {"results": [probe("Dublin", 60), probe("Amsterdam", 100)]}
        txt = wherehost.report(d)
        self.assertIn("Лучший из РАЗРЕШЁННЫХ: Dublin", txt)
        self.assertIn("Выигрыш от переезда: 40мс", txt)
        self.assertIn("есть смысл проверить", txt)

    def test_rejects_move_when_gain_is_small(self):
        d = {"results": [probe("Dublin", 47), probe("Amsterdam", 50)]}
        txt = wherehost.report(d)
        self.assertIn("не стоит возни", txt)

    def test_amsterdam_already_best(self):
        d = {"results": [probe("Amsterdam", 40), probe("Dublin", 60)]}
        txt = wherehost.report(d)
        self.assertIn("переезжать некуда", txt)

    def test_empty(self):
        self.assertIn("Пусто", wherehost.report({"results": []}))


if __name__ == "__main__":
    unittest.main()
