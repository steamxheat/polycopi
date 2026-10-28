#!/usr/bin/env python3
"""Наблюдатель чистого арбитража на крипто-рынках Polymarket «Up or Down».

Чистый арбитраж: купить ОБА исхода (Up и Down) по лучшим ценам продавцов дешевле $1
с учётом комиссии тейкера. Один из них точно выплатит $1 -> прибыль без риска.

Бот ничего не покупает. Он держит стаканы всех текущих рынков (лента CLOB) и записывает
каждый «эпизод», когда Up_ask + Down_ask + комиссии < 1:
  * сколько длился, какая была выгода на акцию и сколько акций было доступно;
  * сразу же перепроверяет оба стакана запросом (как будто мы пошли покупать) —
    осталось ли что-то через наши ~50–100 мс, и сколько бы мы реально заработали.

Команды:
  python3 arbwatch.py run      [--out data_arb]
  python3 arbwatch.py report   [--out data_arb]
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import logging.handlers
import signal
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

from core import _as_list
from net import Http, WebSocket

HERE = Path(__file__).resolve().parent
log = logging.getLogger("arbwatch")

DEFAULTS = {
    "assets": ["btc", "eth", "sol", "xrp", "doge", "bnb", "hype"],
    "windows": {"5m": 300, "15m": 900},
    "fee_coef": 0.07,               # комиссия тейкера на акцию: fee_coef * p * (1 - p)
    "min_gain": 0.0,                # выгода на пару акций, начиная с которой пишем эпизод
    "confirm": True,                # перепроверять стаканы запросом при начале эпизода
    "discover_every_s": 20,
    "status_every_s": 60,
    "ws_silence_s": 30,
    "gamma_url": "https://gamma-api.polymarket.com",
    "clob_url": "https://clob.polymarket.com",
    "clob_ws": "wss://ws-subscriptions-clob.polymarket.com/ws/market",
}

EP_COLS = ["start_utc", "slug", "asset", "window", "sec_in_window", "duration_s", "updates",
           "best_gain", "first_up_ask", "first_down_ask", "max_pairs_top", "potential_usd",
           "confirm_ms", "confirm_pairs", "confirm_gain_usd", "confirm_avg_cost"]


def utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def fee(p: float, coef: float) -> float:
    return coef * p * (1 - p)


def levels(raw) -> dict[float, float]:
    out = {}
    for x in raw or []:
        try:
            p, s = float(x["price"]), float(x["size"])
        except (KeyError, TypeError, ValueError):
            continue
        if s > 0:
            out[p] = s
    return out


def pair_fill(asks_u: dict, asks_d: dict, coef: float, max_pairs: float = 1e9):
    """Сколько пар (по акции Up + акции Down) можно купить, пока каждая следующая
    пара стоит меньше $1 с комиссией. -> (пар, выгода $, средняя цена пары)."""
    lu = sorted(asks_u.items())
    ld = sorted(asks_d.items())
    i = j = 0
    ru = lu[0][1] if lu else 0.0
    rd = ld[0][1] if ld else 0.0
    pairs = gain = cost = 0.0
    while i < len(lu) and j < len(ld) and pairs < max_pairs:
        pu, pd = lu[i][0], ld[j][0]
        c = pu + pd + fee(pu, coef) + fee(pd, coef)
        if c >= 1:
            break
        q = min(ru, rd, max_pairs - pairs)
        pairs += q
        gain += q * (1 - c)
        cost += q * c
        ru -= q
        rd -= q
        if ru <= 1e-9:
            i += 1
            ru = lu[i][1] if i < len(lu) else 0.0
        if rd <= 1e-9:
            j += 1
            rd = ld[j][1] if j < len(ld) else 0.0
    return pairs, gain, (cost / pairs if pairs else None)


class Mkt:
    def __init__(self, slug, asset, window, start, end, up, down):
        self.slug, self.asset, self.window = slug, asset, window
        self.start, self.end, self.up, self.down = start, end, up, down
        self.ep: dict | None = None          # открытый эпизод арбитража


class Watcher:
    def __init__(self, cfg: dict, out: Path, http: Http | None = None, now=time.time):
        self.cfg, self.out, self.now = cfg, out, now
        out.mkdir(parents=True, exist_ok=True)
        self.http = http or Http(timeout=5.0)
        self.markets: dict[str, Mkt] = {}
        self.tok: dict[str, Mkt] = {}
        self.asks: dict[str, dict[float, float]] = {}
        self.c: dict[str, float] = {"msgs": 0, "episodes": 0, "potential": 0.0, "confirmed": 0.0,
                                     "confirmed_eps": 0, "reconnects": 0}
        self.pending: set = set()

    # ------------------------------------------------------------ markets
    async def discover(self):
        now = self.now()
        for asset in self.cfg["assets"]:
            for win, dur in self.cfg["windows"].items():
                start = int(now // dur * dur)
                for st in (start, start + dur):
                    slug = f"{asset}-updown-{win}-{st}"
                    if slug in self.markets:
                        continue
                    try:
                        res = await self.http.aget(f"{self.cfg['gamma_url']}/markets", {"slug": slug})
                    except Exception as e:  # noqa: BLE001
                        log.debug(f"discover {slug}: {e}")
                        continue
                    m = res[0] if isinstance(res, list) and res else None
                    if not m or m.get("closed"):
                        continue
                    toks = [str(t) for t in _as_list(m.get("clobTokenIds"))]
                    outs = [str(o).lower() for o in _as_list(m.get("outcomes"))]
                    if len(toks) != 2 or outs != ["up", "down"]:
                        continue
                    mk = Mkt(slug, asset, win, st, st + dur, toks[0], toks[1])
                    self.markets[slug] = mk
                    self.tok[mk.up] = self.tok[mk.down] = mk
        for slug in [s for s, m in self.markets.items() if m.end < now - 5]:
            m = self.markets.pop(slug)
            if m.ep:
                self.close_ep(m, now)
            for t in (m.up, m.down):
                self.tok.pop(t, None)
                self.asks.pop(t, None)

    # ------------------------------------------------------------ book
    def on_event(self, ev: dict):
        et = ev.get("event_type")
        touched = set()
        if et == "book":
            t = str(ev.get("asset_id"))
            if t in self.tok:
                self.asks[t] = levels(ev.get("asks"))
                touched.add(t)
        elif et == "price_change":
            for it in ev.get("price_changes") or ([ev] if ev.get("price") is not None else []):
                t = str(it.get("asset_id") or ev.get("asset_id") or "")
                if t not in self.tok or str(it.get("side", "")).upper() != "SELL":
                    continue
                try:
                    p, s = float(it["price"]), float(it["size"])
                except (KeyError, TypeError, ValueError):
                    continue
                book = self.asks.setdefault(t, {})
                if s > 0:
                    book[p] = s
                else:
                    book.pop(p, None)
                touched.add(t)
        for t in touched:
            self.check(self.tok[t])

    def check(self, mk: Mkt):
        now = self.now()
        au, ad = self.asks.get(mk.up), self.asks.get(mk.down)
        if not au or not ad:
            if mk.ep:
                self.close_ep(mk, now)
            return
        pu, pd = min(au), min(ad)
        coef = self.cfg["fee_coef"]
        gain = 1 - (pu + pd + fee(pu, coef) + fee(pd, coef))
        if gain > self.cfg["min_gain"]:
            pairs, usd, _ = pair_fill(au, ad, coef)
            if mk.ep is None:
                mk.ep = {"t0": now, "updates": 0, "best_gain": gain, "pu": pu, "pd": pd,
                         "max_pairs": pairs, "potential": usd, "confirm": None}
                self.c["episodes"] += 1
                if self.cfg["confirm"] and mk.slug not in self.pending:
                    self.pending.add(mk.slug)
                    asyncio.ensure_future(self.confirm(mk, mk.ep))
            ep = mk.ep
            ep["updates"] += 1
            ep["best_gain"] = max(ep["best_gain"], gain)
            if usd > ep["potential"]:
                ep["potential"], ep["max_pairs"] = usd, pairs
        elif mk.ep:
            self.close_ep(mk, now)

    async def confirm(self, mk: Mkt, ep: dict):
        """Сходить за обоими стаканами, как будто покупаем, и посчитать реальную выгоду."""
        t0 = time.perf_counter()
        try:
            bu, bd = await asyncio.gather(
                self.http.aget(f"{self.cfg['clob_url']}/book", {"token_id": mk.up}),
                self.http.aget(f"{self.cfg['clob_url']}/book", {"token_id": mk.down}))
            pairs, usd, avg = pair_fill(levels(bu.get("asks")), levels(bd.get("asks")), self.cfg["fee_coef"])
            ep["confirm"] = (round((time.perf_counter() - t0) * 1000), pairs, usd, avg)
        except Exception as e:  # noqa: BLE001
            log.debug(f"confirm {mk.slug}: {e}")
            ep["confirm"] = (round((time.perf_counter() - t0) * 1000), None, None, None)
        finally:
            self.pending.discard(mk.slug)
            if mk.ep is not ep:          # эпизод уже закрыт до ответа — дописываем сейчас
                self._write(mk, ep, ep.get("t_end", self.now()))

    def close_ep(self, mk: Mkt, now: float):
        ep, mk.ep = mk.ep, None
        ep["t_end"] = now
        self.c["potential"] += ep["potential"]
        if ep["confirm"] is not None or not self.cfg["confirm"] or mk.slug not in self.pending:
            self._write(mk, ep, now)

    def _write(self, mk: Mkt, ep: dict, now: float):
        if ep.get("written"):
            return
        ep["written"] = True
        cf = ep["confirm"] or (None, None, None, None)
        if cf[2]:
            self.c["confirmed"] += cf[2]
            self.c["confirmed_eps"] += 1
        row = {"start_utc": utc(ep["t0"]), "slug": mk.slug, "asset": mk.asset, "window": mk.window,
               "sec_in_window": round(ep["t0"] - mk.start, 1), "duration_s": round(now - ep["t0"], 3),
               "updates": ep["updates"], "best_gain": round(ep["best_gain"], 4),
               "first_up_ask": ep["pu"], "first_down_ask": ep["pd"],
               "max_pairs_top": round(ep["max_pairs"], 2), "potential_usd": round(ep["potential"], 4),
               "confirm_ms": cf[0], "confirm_pairs": None if cf[1] is None else round(cf[1], 2),
               "confirm_gain_usd": None if cf[2] is None else round(cf[2], 4),
               "confirm_avg_cost": None if cf[3] is None else round(cf[3], 4)}
        path = self.out / "episodes.csv"
        new = not path.exists()
        with path.open("a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=EP_COLS)
            if new:
                w.writeheader()
            w.writerow(row)
        log.info(f"АРБИТРАЖ {mk.asset.upper()} {mk.window} {row['sec_in_window']:.0f}с окна: "
                 f"Up {ep['pu']:.2f} + Down {ep['pd']:.2f}, выгода до {ep['best_gain'] * 100:.1f}¢/пара, "
                 f"держался {row['duration_s']:.2f}с, на верхних уровнях ${ep['potential']:.2f}"
                 + (f" | перепроверка через {cf[0]} мс: {cf[1]:.0f} пар, ${cf[2]:.2f}"
                    if cf[1] is not None else ""))

    def status(self):
        c = self.c
        log.info(f"СТАТУС рынков {len(self.markets)}, сообщений {int(c['msgs'])}, эпизодов {c['episodes']}, "
                 f"возможная выгода ${c['potential']:.2f}, подтверждено запросом ${c['confirmed']:.2f} "
                 f"({c['confirmed_eps']} эп.), переподключений {c['reconnects']}")


# ======================================================================= loops

async def discover_loop(w: Watcher, stop: asyncio.Event):
    while not stop.is_set():
        await w.discover()
        try:
            await asyncio.wait_for(stop.wait(), w.cfg["discover_every_s"])
        except asyncio.TimeoutError:
            pass


async def ws_loop(w: Watcher, stop: asyncio.Event):
    backoff = 1
    while not stop.is_set():
        tokens = set(w.tok)
        if not tokens:
            try:
                await asyncio.wait_for(stop.wait(), 1)
            except asyncio.TimeoutError:
                pass
            continue
        ws = pinger = None
        try:
            ws = await WebSocket.connect(w.cfg["clob_ws"])
            await ws.send(json.dumps({"assets_ids": sorted(tokens), "type": "market"}))
            backoff = 1

            async def ping():
                while True:
                    await asyncio.sleep(10)
                    await ws.send("PING")
            pinger = asyncio.ensure_future(ping())
            while not stop.is_set():
                if set(w.tok) != tokens:
                    break
                raw = await asyncio.wait_for(ws.recv(), w.cfg["ws_silence_s"])
                w.c["msgs"] += 1
                if raw.strip().upper() == "PONG":
                    continue
                if '"ask' not in raw and '"SELL"' not in raw and '"book"' not in raw:
                    continue                       # нам нужны только продавцы
                try:
                    data = json.loads(raw)
                except ValueError:
                    continue
                for ev in (data if isinstance(data, list) else [data]):
                    if isinstance(ev, dict):
                        w.on_event(ev)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            w.c["reconnects"] += 1
            log.debug(f"CLOB WS обрыв: {type(e).__name__}: {e}")
        finally:
            if pinger:
                pinger.cancel()
            if ws:
                await ws.close()
        # после переподключения стаканы придут заново снимками
        for mk in list(w.markets.values()):
            if mk.ep:
                w.close_ep(mk, w.now())
        w.asks.clear()
        try:
            await asyncio.wait_for(stop.wait(), backoff if w.c["reconnects"] else 0)
        except asyncio.TimeoutError:
            pass
        backoff = min(backoff * 2, 30)


async def status_loop(w: Watcher, stop: asyncio.Event):
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), w.cfg["status_every_s"])
        except asyncio.TimeoutError:
            w.status()


def setup_logging(out: Path):
    out.mkdir(parents=True, exist_ok=True)
    log.setLevel(logging.DEBUG)
    log.handlers.clear()
    fmt = logging.Formatter("[%(asctime)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    fmt.converter = time.gmtime
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)
    fh = logging.handlers.RotatingFileHandler(out / "arb.log", maxBytes=5_000_000, backupCount=2)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    log.addHandler(sh)
    log.addHandler(fh)


async def run(cfg: dict, out: Path):
    setup_logging(out)
    w = Watcher(cfg, out)
    log.info(f"Старт наблюдателя арбитража: {', '.join(a.upper() for a in cfg['assets'])}, "
             f"окна {', '.join(cfg['windows'])}, комиссия {cfg['fee_coef']}·p·(1−p)")
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, stop.set)
        except NotImplementedError:
            pass
    tasks = [asyncio.ensure_future(x) for x in (discover_loop(w, stop), ws_loop(w, stop), status_loop(w, stop))]
    await stop.wait()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    w.status()


# ======================================================================= report

def report(out: Path) -> str:
    path = out / "episodes.csv"
    if not path.exists():
        return f"Нет эпизодов в {path} — либо арбитража не было, либо бот ещё не запускался."
    rows = list(csv.DictReader(path.open()))
    if not rows:
        return "Эпизодов пока нет."

    def f(x):
        try:
            return float(x)
        except (TypeError, ValueError):
            return None
    t0, t1 = rows[0]["start_utc"], rows[-1]["start_utc"]
    L = [f"=== Чистый арбитраж Up+Down < $1: {len(rows)} эпизодов ({t0} … {t1} UTC) ===", ""]
    L.append(f"{'рынок':<10} {'эпиз.':>5} {'медиана длит.':>13} {'макс выгода':>11} "
             f"{'на верхах $':>11} {'подтв. эп.':>10} {'подтв. $':>9}")
    groups: dict[str, list] = {}
    for r in rows:
        groups.setdefault(f"{r['asset'].upper()} {r['window']}", []).append(r)
    tot_pot = tot_conf = 0.0
    tot_conf_n = 0
    for k in sorted(groups):
        g = groups[k]
        dur = [f(r["duration_s"]) for r in g if f(r["duration_s"]) is not None]
        pot = sum(f(r["potential_usd"]) or 0 for r in g)
        conf = [f(r["confirm_gain_usd"]) for r in g if (f(r["confirm_gain_usd"]) or 0) > 0]
        tot_pot += pot
        tot_conf += sum(conf)
        tot_conf_n += len(conf)
        L.append(f"{k:<10} {len(g):>5} {statistics.median(dur) if dur else 0:>12.2f}с "
                 f"{max(f(r['best_gain']) or 0 for r in g) * 100:>9.1f}¢ {pot:>11.2f} "
                 f"{len(conf):>10} {sum(conf):>9.2f}")
    L.append("")
    L.append(f"ИТОГО: если бы забирали всё на верхних уровнях мгновенно — ${tot_pot:.2f}; "
             f"реально осталось к моменту перепроверки (~наша задержка) — ${tot_conf:.2f} "
             f"в {tot_conf_n} эпизодах из {len(rows)}.")
    durs = sorted(f(r["duration_s"]) or 0 for r in rows)
    L.append(f"Длительность эпизодов: медиана {statistics.median(durs):.2f}с, "
             f"90% — до {durs[int(len(durs) * 0.9)]:.2f}с, максимум {durs[-1]:.2f}с.")
    ms = [f(r["confirm_ms"]) for r in rows if f(r["confirm_ms"]) is not None]
    if ms:
        L.append(f"Перепроверка стакана занимала: медиана {statistics.median(ms):.0f} мс.")
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description="Наблюдатель чистого арбитража Up+Down < $1")
    ap.add_argument("cmd", choices=["run", "report"])
    ap.add_argument("--out", default=str(HERE / "data_arb"))
    ap.add_argument("--config", default=str(HERE / "arb_config.json"))
    a = ap.parse_args()
    cfg = json.loads(json.dumps(DEFAULTS))
    p = Path(a.config)
    if p.exists():
        cfg.update(json.loads(p.read_text()))
    if a.cmd == "run":
        try:
            asyncio.run(run(cfg, Path(a.out)))
        except KeyboardInterrupt:
            pass
    else:
        print(report(Path(a.out)))


if __name__ == "__main__":
    main()
