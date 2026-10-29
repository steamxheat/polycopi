import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import wherehost  # noqa: E402


def probe(city, total, dns=0.002, tcp=0.004, tls=0.03, status=None):
    r = {"probe": {"city": city, "country": "XX"}, "result": {}}
    if total is None:
        r["result"]["status"] = status or "failed"
    else:
        r["result"]["timings"] = {"dns": dns, "tcp": tcp, "tls": tls, "total": total}
    return r


class StartTests(unittest.TestCase):
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
        self.assertEqual(seen["body"]["locations"],
                         [{"city": "Dublin", "limit": 1}, {"city": "Amsterdam", "limit": 1}])
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
        d = {"results": [probe("Amsterdam", 0.050), probe("Dublin", 0.020)]}
        rs = wherehost.rows(d)
        self.assertEqual([r[0] for r in rs], ["Dublin", "Amsterdam"])   # быстрее первым
        city, total, warm, _, _ = rs[0]
        self.assertAlmostEqual(warm, 0.020 - 0.002 - 0.004 - 0.03, places=6)

    def test_failed_probe_goes_last(self):
        d = {"results": [probe("Dublin", None, status="timeout"), probe("Amsterdam", 0.05)]}
        rs = wherehost.rows(d)
        self.assertEqual(rs[0][0], "Amsterdam")
        self.assertEqual(rs[-1][4], "timeout")

    def test_missing_fields_do_not_crash(self):
        self.assertEqual(wherehost.rows({}), [])
        self.assertEqual(wherehost.rows({"results": [{}]})[0][0], "?")


class ReportTests(unittest.TestCase):
    def test_marks_blocked_countries(self):
        d = {"results": [probe("London", 0.020), probe("Amsterdam", 0.050)]}
        txt = wherehost.report(d)
        line = next(l for l in txt.splitlines() if l.startswith("London"))
        self.assertIn("❌", line)                       # быстрый, но торговать нельзя
        self.assertIn("Лучший из РАЗРЕШЁННЫХ: Amsterdam", txt)

    def test_recommends_move_when_gain_is_big(self):
        d = {"results": [probe("Dublin", 0.020), probe("Amsterdam", 0.060)]}
        txt = wherehost.report(d)
        self.assertIn("Лучший из РАЗРЕШЁННЫХ: Dublin", txt)
        self.assertIn("Выигрыш от переезда: 40мс", txt)
        self.assertIn("есть смысл проверить", txt)

    def test_rejects_move_when_gain_is_small(self):
        d = {"results": [probe("Dublin", 0.047), probe("Amsterdam", 0.050)]}
        txt = wherehost.report(d)
        self.assertIn("не стоит возни", txt)

    def test_amsterdam_already_best(self):
        d = {"results": [probe("Amsterdam", 0.040), probe("Dublin", 0.060)]}
        txt = wherehost.report(d)
        self.assertIn("переезжать некуда", txt)

    def test_empty(self):
        self.assertIn("Пусто", wherehost.report({"results": []}))


if __name__ == "__main__":
    unittest.main()
