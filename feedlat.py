#!/usr/bin/env python3
"""Гонка источников цен: какая биржа первой приносит рывок BTC/ETH на НАШ сервер.

Подключается сразу ко многим биржам (сделки в реальном времени) и для каждого рывка
(цена сдвинулась на ≥ jump_bps за 1 с хоть у одного источника) замеряет, через сколько
миллисекунд ПОСЛЕ самого быстрого источника то же движение увидел каждый другой.

Ничего не торгует. Команды:
  python3 feedlat.py run     [--out data_lat]
  python3 feedlat.py report  [--out data_lat]
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
from collections import deque
from datetime import datetime
from pathlib import Path

from net import WebSocket, fast_loads

HERE = Path(__file__).resolve().parent
log = logging.getLogger("feedlat")

DEFAULTS = {
    "jump_bps": 3.0,          # рывок: изменение за jump_window_s у любого источника
    "jump_window_s": 1.0,
    "cross_bps": 1.5,         # источник «увидел» рывок, когда сдвинулся на столько же в ту же сторону
    "follow_s": 3.0,          # сколько ждём остальных
    "cooldown_s": 3.0,        # не чаще одного события на монету
    "status_every_s": 60,
    "ws_silence_s": 30,
}


def _ts_iso(s: str) -> float | None:
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except (ValueError, AttributeError):
        return None


# ------------------------------------------------------------------ источники
# имя -> (url, сообщение подписки, разбор -> [(монета, цена, время биржи или None)], ping-текст, раз в сек)

def p_binance(m):
    d = m.get("data") if isinstance(m, dict) else None
    if isinstance(d, dict) and d.get("p"):
        s = str(d.get("s", ""))
        coin = "BTC" if s.startswith("BTC") else "ETH" if s.startswith("ETH") else None
        if coin:
            return [(coin, float(d["p"]), (d.get("T") or d.get("E") or 0) / 1000 or None)]
    return []


def p_bybit(m):
    out = []
    if isinstance(m, dict) and str(m.get("topic", "")).startswith("publicTrade."):
        for d in m.get("data") or []:
            s = str(d.get("s", ""))
            coin = "BTC" if s.startswith("BTC") else "ETH" if s.startswith("ETH") else None
            if coin and d.get("p"):
                out.append((coin, float(d["p"]), (d.get("T") or 0) / 1000 or None))
    return out


def p_okx(m):
    out = []
    if isinstance(m, dict) and (m.get("arg") or {}).get("channel") == "trades":
        for d in m.get("data") or []:
            coin = str(d.get("instId", "")).split("-")[0]
            if coin in ("BTC", "ETH") and d.get("px"):
                out.append((coin, float(d["px"]), int(d.get("ts", 0)) / 1000 or None))
    return out


def p_coinbase(m):
    if isinstance(m, dict) and m.get("type") in ("match", "last_match") and m.get("price"):
        coin = str(m.get("product_id", "")).split("-")[0]
        if coin in ("BTC", "ETH"):
            return [(coin, float(m["price"]), _ts_iso(m.get("time")))]
    return []


def p_kraken(m):
    out = []
    if isinstance(m, dict) and m.get("channel") == "trade":
        for d in m.get("data") or []:
            coin = str(d.get("symbol", "")).split("/")[0]
            if coin in ("BTC", "ETH") and d.get("price") is not None:
                out.append((coin, float(d["price"]), _ts_iso(d.get("timestamp"))))
    return out


def p_bitstamp(m):
    if isinstance(m, dict) and m.get("event") == "trade":
        ch = str(m.get("channel", ""))
        coin = "BTC" if "btc" in ch else "ETH" if "eth" in ch else None
        d = m.get("data") or {}
        if coin and d.get("price") is not None:
            mt = d.get("microtimestamp")
            return [(coin, float(d["price"]), int(mt) / 1e6 if mt else None)]
    return []


def p_rtds(m):
    """Polymarket RTDS: цены Binance и Chainlink, которыми пользуется сам Polymarket."""
    out = []
    if isinstance(m, dict) and str(m.get("topic", "")).startswith("crypto_prices"):
        p = m.get("payload") or {}
        sym = str(p.get("symbol", "")).lower()
        coin = "BTC" if sym.startswith("btc") else "ETH" if sym.startswith("eth") else None
        if coin and p.get("value") is not None:
            out.append((coin, float(p["value"]), (p.get("timestamp") or 0) / 1000 or None))
    return out


def sources() -> dict:
    j = json.dumps
    return {
        "binance_spot": ("wss://data-stream.binance.vision/stream?streams=btcusdt@trade/ethusdt@trade",
                         None, p_binance, None, 0),
        "binance_fut": ("wss://fstream.binance.com/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade",
                        None, p_binance, None, 0),
        "bybit_spot": ("wss://stream.bybit.com/v5/public/spot",
                       j({"op": "subscribe", "args": ["publicTrade.BTCUSDT", "publicTrade.ETHUSDT"]}),
                       p_bybit, j({"op": "ping"}), 20),
        "bybit_perp": ("wss://stream.bybit.com/v5/public/linear",
                       j({"op": "subscribe", "args": ["publicTrade.BTCUSDT", "publicTrade.ETHUSDT"]}),
                       p_bybit, j({"op": "ping"}), 20),
        "okx_spot": ("wss://ws.okx.com:8443/ws/v5/public",
                     j({"op": "subscribe", "args": [{"channel": "trades", "instId": "BTC-USDT"},
                                                    {"channel": "trades", "instId": "ETH-USDT"}]}),
                     p_okx, "ping", 20),
        "okx_perp": ("wss://ws.okx.com:8443/ws/v5/public",
                     j({"op": "subscribe", "args": [{"channel": "trades", "instId": "BTC-USDT-SWAP"},
                                                    {"channel": "trades", "instId": "ETH-USDT-SWAP"}]}),
                     p_okx, "ping", 20),
        "coinbase": ("wss://ws-feed.exchange.coinbase.com",
                     j({"type": "subscribe", "product_ids": ["BTC-USD", "ETH-USD"], "channels": ["matches"]}),
                     p_coinbase, None, 0),
        "kraken": ("wss://ws.kraken.com/v2",
                   j({"method": "subscribe", "params": {"channel": "trade", "symbol": ["BTC/USD", "ETH/USD"]}}),
                   p_kraken, j({"method": "ping"}), 20),
        "bitstamp": ("wss://ws.bitstamp.net",
                     [j({"event": "bts:subscribe", "data": {"channel": "live_trades_btcusd"}}),
                      j({"event": "bts:subscribe", "data": {"channel": "live_trades_ethusd"}})],
                     p_bitstamp, None, 0),
        "pm_rtds": ("wss://ws-live-data.polymarket.com",
                    j({"action": "subscribe", "subscriptions": [
                        {"topic": "crypto_prices", "type": "update", "filters": "btcusdt"},
                        {"topic": "crypto_prices", "type": "update", "filters": "ethusdt"},
                        {"topic": "crypto_prices_chainlink", "type": "*", "filters": ""}]}),
                    p_rtds, "ping", 5),
    }


# ------------------------------------------------------------------ гонка

class Race:
    def __init__(self, cfg: dict, out: Path, now=time.time):
        self.cfg, self.out, self.now = cfg, out, now
        self.hist: dict[tuple, deque] = {}           # (источник, монета) -> (время прихода, цена)
        self.last: dict[tuple, float] = {}
        self.stats: dict[str, dict] = {}             # источник -> msgs, transport[], reconnects
        self.events: dict[str, dict] = {}            # монета -> открытое событие
        self.cool: dict[str, float] = {}
        self.n_events = 0

    def st(self, src):
        return self.stats.setdefault(src, {"msgs": 0, "transport": deque(maxlen=2000), "reconnects": 0})

    def on_price(self, src: str, coin: str, px: float, exch_ts: float | None, t: float | None = None):
        t = self.now() if t is None else t
        s = self.st(src)
        s["msgs"] += 1
        if exch_ts and abs(t - exch_ts) < 30:
            s["transport"].append((t - exch_ts) * 1000)
        key = (src, coin)
        h = self.hist.setdefault(key, deque())
        h.append((t, px))
        while h and t - h[0][0] > 5:
            h.popleft()
        self.last[key] = px
        ev = self.events.get(coin)
        if ev:
            self._update(ev, src, coin, px, t)
        elif t >= self.cool.get(coin, 0):
            ref = None
            for ts, p in h:                              # цена ~1 с назад у этого источника
                if t - ts <= self.cfg["jump_window_s"]:
                    break
                ref = p
            if ref:
                bps = (px / ref - 1) * 1e4
                if abs(bps) >= self.cfg["jump_bps"]:
                    self._open(coin, 1 if bps > 0 else -1, t)
                    self._update(self.events[coin], src, coin, px, t)
        self._close_old(t)

    def _open(self, coin, direction, t):
        # база каждого источника — его цена до начала рывка
        base = {}
        for (src, c), h in self.hist.items():
            if c != coin or not h:
                continue
            b = None
            for ts, p in h:
                if ts <= t - self.cfg["jump_window_s"]:
                    b = p
            base[src] = b if b is not None else h[0][1]
        self.events[coin] = {"t0": t, "dir": direction, "base": base, "seen": {}}
        self.cool[coin] = t + self.cfg["cooldown_s"]

    def _update(self, ev, src, coin, px, t):
        if src in ev["seen"]:
            return
        b = ev["base"].get(src)
        if b is None:
            ev["base"][src] = b = px
            return
        if ev["dir"] * (px / b - 1) * 1e4 >= self.cfg["cross_bps"]:
            ev["seen"][src] = t

    def _close_old(self, t):
        for coin in [c for c, e in self.events.items() if t - e["t0"] > self.cfg["follow_s"]]:
            ev = self.events.pop(coin)
            seen = ev["seen"]
            if len(seen) < 2:
                continue
            first = min(seen.values())
            self.n_events += 1
            path = self.out / "races.csv"
            new = not path.exists()
            with path.open("a", newline="") as f:
                w = csv.writer(f)
                if new:
                    w.writerow(["t", "coin", "dir", "source", "lag_ms", "rank"])
                for rank, (src, ts) in enumerate(sorted(seen.items(), key=lambda x: x[1]), 1):
                    w.writerow([round(ev["t0"], 3), coin, ev["dir"], src, round((ts - first) * 1000, 1), rank])

    def status(self):
        parts = []
        for src, s in sorted(self.stats.items()):
            tr = s["transport"]
            parts.append(f"{src} {s['msgs']} сообщ."
                         + (f", доставка ~{statistics.median(tr):.0f} мс" if tr else "")
                         + (f", обрывов {s['reconnects']}" if s["reconnects"] else ""))
        log.info(f"СТАТУС рывков {self.n_events} | " + " | ".join(parts))
        (self.out / "transport.json").write_text(json.dumps(
            {src: {"msgs": s["msgs"], "reconnects": s["reconnects"],
                   "transport_ms_median": round(statistics.median(s["transport"]), 1) if s["transport"] else None,
                   "transport_ms_p90": round(sorted(s["transport"])[int(len(s["transport"]) * .9)], 1)
                   if s["transport"] else None}
             for src, s in self.stats.items()}, ensure_ascii=False, indent=1))


async def source_loop(race: Race, name: str, spec, stop: asyncio.Event):
    url, sub, parser, ping_text, ping_every = spec
    backoff = 1
    while not stop.is_set():
        ws = pinger = None
        try:
            ws = await WebSocket.connect(url)
            for s in ([sub] if isinstance(sub, str) else (sub or [])):
                await ws.send(s)
            backoff = 1
            if ping_text:
                async def ping():
                    while True:
                        await asyncio.sleep(ping_every)
                        await ws.send(ping_text)
                pinger = asyncio.ensure_future(ping())
            while not stop.is_set():
                raw = await asyncio.wait_for(ws.recv(), race.cfg["ws_silence_s"])
                t = time.time()
                if len(raw) < 8:
                    continue
                try:
                    msg = fast_loads(raw)
                except ValueError:
                    continue
                for coin, px, ets in parser(msg):
                    race.on_price(name, coin, px, ets, t)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            race.st(name)["reconnects"] += 1
            n = race.st(name)["reconnects"]
            (log.info if n in (1, 2, 3, 10) or n % 50 == 0 else log.debug)(
                f"{name}: обрыв #{n}: {type(e).__name__}: {e}")
        finally:
            if pinger:
                pinger.cancel()
            if ws:
                await ws.close()
        try:
            await asyncio.wait_for(stop.wait(), backoff)
        except asyncio.TimeoutError:
            pass
        backoff = min(backoff * 2, 60)


async def status_loop(race: Race, stop: asyncio.Event):
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), race.cfg["status_every_s"])
        except asyncio.TimeoutError:
            race.status()


def setup_logging(out: Path):
    out.mkdir(parents=True, exist_ok=True)
    log.setLevel(logging.DEBUG)
    log.handlers.clear()
    fmt = logging.Formatter("[%(asctime)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    fmt.converter = time.gmtime
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)
    fh = logging.handlers.RotatingFileHandler(out / "lat.log", maxBytes=5_000_000, backupCount=1)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    log.addHandler(sh)
    log.addHandler(fh)


async def run(cfg: dict, out: Path):
    setup_logging(out)
    race = Race(cfg, out)
    src = sources()
    log.info(f"Старт гонки источников: {', '.join(src)}")
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, stop.set)
        except NotImplementedError:
            pass
    tasks = [asyncio.ensure_future(source_loop(race, n, sp, stop)) for n, sp in src.items()]
    tasks.append(asyncio.ensure_future(status_loop(race, stop)))
    await stop.wait()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    race.status()


def report(out: Path) -> str:
    L = []
    tp = out / "transport.json"
    if tp.exists():
        d = json.loads(tp.read_text())
        L.append("Доставка (время биржи → наш сервер; зависит и от часов биржи, ориентир):")
        for src, s in sorted(d.items(), key=lambda x: (x[1]["transport_ms_median"] is None,
                                                         x[1]["transport_ms_median"] or 0)):
            med = s["transport_ms_median"]
            L.append(f"  {src:<13} {('%.0f мс' % med) if med is not None else 'нет метки':>9}"
                     f"  (90%: {s['transport_ms_p90'] if s['transport_ms_p90'] is not None else '-'})"
                     f"  сообщений {s['msgs']}, обрывов {s['reconnects']}")
        L.append("")
    path = out / "races.csv"
    if not path.exists():
        return "\n".join(L + ["Рывков пока не было — подождите несколько минут."])
    with path.open() as fh:
        rows = list(csv.DictReader(fh))
    events = {(r["t"], r["coin"]) for r in rows}
    L.append(f"=== Гонка: {len(events)} рывков. Отставание от самого быстрого источника ===")
    L.append(f"{'источник':<13} {'участвовал':>10} {'первым':>7} {'медиана':>9} {'75%':>8} {'90%':>8}")
    by: dict[str, list] = {}
    for r in rows:
        by.setdefault(r["source"], []).append(r)
    order = sorted(by, key=lambda s: statistics.median(float(x["lag_ms"]) for x in by[s]))
    for src in order:
        lags = sorted(float(x["lag_ms"]) for x in by[src])
        first = sum(1 for x in by[src] if x["rank"] == "1")
        L.append(f"{src:<13} {len(lags):>10} {first / len(lags) * 100:>6.0f}% {statistics.median(lags):>7.0f}мс "
                 f"{lags[int(len(lags) * .75)]:>6.0f}мс {lags[int(len(lags) * .9)]:>6.0f}мс")
    L.append("")
    L.append("Чем меньше медиана и больше «первым» — тем раньше этот источник приносит нам рывок.")
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description="Гонка источников цен")
    ap.add_argument("cmd", choices=["run", "report"])
    ap.add_argument("--out", default=str(HERE / "data_lat"))
    a = ap.parse_args()
    cfg = dict(DEFAULTS)
    if a.cmd == "run":
        try:
            asyncio.run(run(cfg, Path(a.out)))
        except KeyboardInterrupt:
            pass
    else:
        print(report(Path(a.out)))


if __name__ == "__main__":
    main()
