import asyncio
import http.server
import json
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from urllib.parse import urlsplit, parse_qs

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import scout  # noqa: E402
from scout import compute, flags_for, score  # noqa: E402

NOW = 1793100000.0
DAY = 86400


def closed(pnl, avg, bought, ts, slug="nba-lal-bos", cid="c"):
    return {"conditionId": cid, "asset": cid + "a", "avgPrice": avg, "totalBought": bought,
            "realizedPnl": pnl, "timestamp": ts, "slug": slug, "title": slug}


def openp(cur, cash, init, end, slug="nba-x", cid="o"):
    return {"conditionId": cid, "asset": cid + "a", "curPrice": cur, "cashPnl": cash,
            "initialValue": init, "avgPrice": 0.5, "size": init / 0.5, "endDate": end,
            "slug": slug, "title": slug}


def act(ts, usd, price=0.5, size=None, slug="btc-updown-5m-1793091900", side="BUY"):
    size = size if size is not None else usd / price
    return {"type": "TRADE", "timestamp": ts, "usdcSize": usd, "price": price, "size": size,
            "side": side, "slug": slug, "title": "Bitcoin Up or Down"}


class ComputeTests(unittest.TestCase):
    def test_hidden_losses_counted(self):
        # на профиле видно 3 выигрыша; ещё 2 проигрыша «висят» в открытых по цене 0
        c = [closed(10, 0.5, 20, NOW - DAY, cid=f"w{i}") for i in range(3)]
        o = [openp(0, -10, 10, "2026-10-26", cid=f"l{i}") for i in range(2)]
        o.append(openp(0.55, 1, 10, "2026-11-30", cid="live"))        # ещё идёт — не считаем
        o.append(openp(1, 10, 10, "2026-10-26", cid="unredeemed"))    # выигрыш без выкупа
        st = compute("0xw", "t", c, o, [], NOW, 30)
        self.assertEqual(st.resolved, 6)
        self.assertEqual(st.wins, 4)
        self.assertEqual(st.hidden_losses, 2)
        self.assertAlmostEqual(st.winrate, 4 / 6)
        self.assertAlmostEqual(st.pnl, 30 - 20 + 10)
        self.assertAlmostEqual(st.invested, 30 + 20 + 10)

    def test_window_and_dedupe(self):
        c = [closed(5, 0.5, 10, NOW - 40 * DAY, cid="old"), closed(5, 0.5, 10, NOW - DAY, cid="new")]
        o = [openp(1, 5, 5, "2026-10-26", cid="new")]          # та же позиция — не дублируем
        st = compute("0xw", "t", c, o, [], NOW, 30)
        self.assertEqual(st.resolved, 1)

    def test_favorite_and_crypto_and_taker(self):
        c = [closed(0.5, 0.95, 10, NOW - DAY, slug="btc-updown-5m-1793091900", cid=f"f{i}")
             for i in range(40)]
        # тейкер: заплатил комиссию 0.07*p*(1-p) на акцию
        a = [act(NOW - i * 60, 5 * 0.5 + 0.07 * 5 * 0.25, price=0.5, size=5) for i in range(20)]
        st = compute("0xw", "t", c, [], a, NOW, 30)
        self.assertAlmostEqual(st.avg_entry, 0.95)
        self.assertEqual(st.crypto_share, 1.0)
        self.assertEqual(st.fast_share, 1.0)
        self.assertEqual(st.taker_share, 1.0)
        self.assertIn("фаворит", flags_for(st))
        busy = compute("0xw", "t", c, [], [act(NOW - i * 60, 5, 0.5, 10) for i in range(400)], NOW, 30)
        self.assertIn("частый", flags_for(busy))
        maker = compute("0xw", "t", c, [], [act(NOW - i * 60, 2.5, 0.5, 5) for i in range(20)], NOW, 30)
        self.assertIn("мейкер", flags_for(maker))
        self.assertLess(maker.days_since_trade, 0.01)

    def test_combo_mm_flagged(self):
        a = [dict(act(NOW - i * 60, 300, 0.9, 333), isCombo=True, outcomeIndex=999) for i in range(30)]
        c = [closed(30, 0.9, 333, NOW - DAY, cid=f"k{i}") for i in range(40)]
        st = compute("0xw", "t", c, [], a, NOW, 30)
        self.assertGreater(st.combo_share, 0.3)
        self.assertIn("комбо-MM", flags_for(st))

    def test_score_prefers_real_edge(self):
        a = scout.Stats("a", "a", resolved=100, invested=100, roi=0.10, per_day=5)
        b = scout.Stats("b", "b", resolved=100, invested=100, roi=0.02, per_day=5)
        c = scout.Stats("c", "c", resolved=10, invested=100, roi=0.50)
        self.assertGreater(score(a, 30), score(b, 30))
        self.assertLess(score(c, 30), -1e8)                    # мало позиций — не оцениваем


class ScoutRunTests(unittest.TestCase):
    def test_full_run_against_fake_api_and_apply(self):
        W_GOOD, W_MAKER = "0x" + "a" * 40, "0x" + "b" * 40
        now = time.time()

        class H(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def do_GET(self):
                u = urlsplit(self.path)
                q = {k: v[0] for k, v in parse_qs(u.query).items()}
                if u.path == "/v1/leaderboard":
                    body = [{"proxyWallet": W_GOOD, "userName": "good", "pnl": 5000},
                            {"proxyWallet": W_MAKER, "userName": "maker", "pnl": 9000}] \
                        if q.get("offset") == "0" else []
                elif u.path == "/closed-positions":
                    off = int(q["offset"])
                    win = q["user"] == W_GOOD
                    body = [closed(8 if win or i % 2 else -10, 0.6, 20, now - 3600 * (off + i),
                                   cid=f"{q['user'][:4]}{off + i}") for i in range(50)] if off < 100 else []
                elif u.path == "/positions":
                    body = []
                elif u.path == "/activity":
                    fee = q["user"] == W_GOOD
                    body = [act(now - i * 300, 3 + (0.07 * 6 * 0.24 if fee else 0), 0.6, 5)
                            for i in range(40)]
                else:
                    body = []
                d = json.dumps(body).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(d)))
                self.end_headers()
                self.wfile.write(d)

        srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        tmp = Path(tempfile.mkdtemp())
        try:
            for f in ("scout.py", "net.py", "core.py", "simbot.py", "config.json"):
                shutil.copy(ROOT / f, tmp / f)
            out = subprocess.run([sys.executable, "scout.py", "--base",
                                  f"http://127.0.0.1:{srv.server_address[1]}", "--apply", "5",
                                  "--min-winrate", "0.8"],
                                 cwd=tmp, capture_output=True, text=True, timeout=60)
            self.assertEqual(out.returncode, 0, out.stderr)
            self.assertIn("good", out.stdout)
            extra = json.loads((tmp / "leaders_extra.json").read_text())
            self.assertEqual(list(extra), ["good"])            # мейкер с 50% винрейтом отсеян
            self.assertEqual(extra["good"]["address"], W_GOOD)
            cands = json.loads((tmp / "scout_candidates.json").read_text())
            self.assertEqual({c["name"] for c in cands}, {"good", "maker"})
            # копи-бот подхватывает новых трейдеров
            sys.path.insert(0, str(tmp))
            import importlib
            sb = importlib.import_module("simbot")
            cfg = sb.load_config(tmp / "config.json")
            self.assertEqual(cfg["leaders"]["good"], W_GOOD)
            self.assertEqual(cfg["leader_poll"][W_GOOD], 5)
            self.assertIn("primm", cfg["leaders"])
        finally:
            srv.shutdown()
            shutil.rmtree(tmp)


if __name__ == "__main__":
    unittest.main(verbosity=2)
