#!/usr/bin/env python3
"""Polymarket copy-trading SIMULATOR.

Следит за сделками выбранных кошельков, для каждой сделки берёт РЕАЛЬНЫЙ стакан
в момент, когда вы бы о ней узнали, и считает, по какой цене вы бы вошли.
Ведёт виртуальный счёт. Реальных ордеров не отправляет и ключей не требует.

Команды:
  python3 simbot.py check            проверить доступ к Polymarket и задержки
  python3 simbot.py run              запустить симуляцию (Ctrl+C — стоп, состояние сохраняется)
  python3 simbot.py report           отчёт по накопленным данным
  python3 simbot.py resim --max-slippage 0.01 --usd-per-trade 3
                                     пересчитать записанную историю с другими настройками
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import signal
import statistics
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from core import (LeaderTrade, Portfolio, end_ts_from_slug, market_end_ts, parse_trade,
                  resolution_payouts, walk_book, fee)
from net import Http, HttpError, WebSocket, WsClosed

HERE = Path(__file__).resolve().parent

DEFAULTS = {
    "leaders": {},
    "start_cash": 100.0,
    "usd_per_trade": 2.0,
    "max_usd_per_trade": 5.0,
    "max_slippage": 0.02,
    "max_entry_price": 0.97,
    "min_seconds_left": 10,
    "group_window_s": 3,
    "taker_fee_coef": 0.07,
    "use_rtds": True,
    "use_polling": True,
    "poll_interval_s": 1.5,
    "status_every_s": 60,
    "resolve_every_s": 20,
    "clob_url": "https://clob.polymarket.com",
    "data_url": "https://data-api.polymarket.com",
    "gamma_url": "https://gamma-api.polymarket.com",
    "rtds_url": "wss://ws-live-data.polymarket.com",
}


def load_config(path: Path) -> dict:
    cfg = dict(DEFAULTS)
    if path.exists():
        cfg.update(json.loads(path.read_text()))
    cfg["leaders"] = {k: v.lower() for k, v in cfg["leaders"].items()}
    return cfg


def utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# ======================================================================= API

class Api:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.http = Http(timeout=5.0)

    def now(self) -> float:
        return time.time()

    async def get_book(self, asset: str) -> dict:
        return await self.http.aget(f"{self.cfg['clob_url']}/book", {"token_id": asset})

    async def get_market(self, slug: str) -> dict | None:
        res = await self.http.aget(f"{self.cfg['gamma_url']}/markets", {"slug": slug})
        if isinstance(res, list):
            return res[0] if res else None
        return res

    async def get_activity(self, wallet: str, limit: int = 50) -> list:
        res = await self.http.aget(f"{self.cfg['data_url']}/activity",
                                   {"user": wallet, "type": "TRADE", "limit": limit})
        return res if isinstance(res, list) else []


class FakeApi:
    """Подставные данные из записи: для тестов и для resim."""

    def __init__(self):
        self.t = 0.0
        self.books: dict[str, dict] = {}       # trade_key -> book
        self.markets: dict[str, list] = {}     # slug -> [(t, market)]
        self.current_key = ""

    def now(self) -> float:
        return self.t

    async def get_book(self, asset: str) -> dict:
        b = self.books.get(self.current_key)
        if b is None:
            raise HttpError("no recorded book")
        return b

    async def get_market(self, slug: str):
        best = None
        for t, m in self.markets.get(slug, []):
            if t <= self.t:
                best = m
        return best


# ======================================================================= ENGINE

CSV_COLS = ["time_utc", "leader", "source", "action", "reason", "market", "outcome",
            "leader_price", "leader_size", "lag_s", "book_ms", "best_price", "our_vwap",
            "slip_cents", "our_shares", "our_cost", "fee", "pnl", "cash", "equity_real",
            "equity_ideal"]


class Engine:
    def __init__(self, cfg: dict, api, out: Path, record: bool = True, quiet: bool = False):
        self.cfg = cfg
        self.api = api
        self.out = out
        self.record = record
        self.quiet = quiet
        out.mkdir(parents=True, exist_ok=True)
        self.names = {v: k for k, v in cfg["leaders"].items()}
        self.state_path = out / "state.json"
        self.csv_path = out / "trades.csv"
        self.rec_path = out / "recording.jsonl"
        self.real = Portfolio(cfg["start_cash"], cfg["start_cash"])
        self.ideal = Portfolio(cfg["start_cash"], cfg["start_cash"])
        self.counters: dict[str, int] = {}
        self.stats: dict[str, list] = {"lag": [], "book_ms": [], "slip": [], "fee_coef": []}
        self.started = api.now()
        self.seen: set[str] = set()
        self._seen_q: deque = deque()
        self.leader_net: dict[tuple, float] = {}
        self.recent_group: dict[tuple, float] = {}
        self.end_cache: dict[str, float] = {}
        self.last_resolve_try: dict[str, float] = {}
        self._load_state()

    # ------------------------------------------------------------ persistence
    def _load_state(self):
        if not self.state_path.exists():
            return
        d = json.loads(self.state_path.read_text())
        self.real = Portfolio.from_dict(d["real"])
        self.ideal = Portfolio.from_dict(d["ideal"])
        self.counters = d.get("counters", {})
        self.stats = {k: d.get("stats", {}).get(k, []) for k in self.stats}
        self.started = d.get("started", self.started)
        self.end_cache = d.get("end_cache", {})

    def save_state(self):
        d = {"started": self.started, "saved": self.api.now(), "config": self.cfg,
             "real": self.real.to_dict(), "ideal": self.ideal.to_dict(),
             "counters": self.counters,
             "stats": {k: v[-20000:] for k, v in self.stats.items()},
             "end_cache": self.end_cache}
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(d))
        os.replace(tmp, self.state_path)

    def _rec(self, kind: str, **kw):
        if not self.record:
            return
        with self.rec_path.open("a") as f:
            f.write(json.dumps({"t": self.api.now(), "kind": kind, **kw}) + "\n")

    def _row(self, **kw):
        new = not self.csv_path.exists()
        row = {c: kw.get(c, "") for c in CSV_COLS}
        row["time_utc"] = utc(self.api.now())
        row["cash"] = f"{self.real.cash:.2f}"
        row["equity_real"] = f"{self.real.equity_at_cost():.2f}"
        row["equity_ideal"] = f"{self.ideal.equity_at_cost():.2f}"
        with self.csv_path.open("a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=CSV_COLS)
            if new:
                w.writeheader()
            w.writerow(row)

    def _count(self, k: str):
        self.counters[k] = self.counters.get(k, 0) + 1

    def log(self, msg: str):
        if not self.quiet:
            print(f"[{utc(self.api.now())}] {msg}", flush=True)

    def mark_seen(self, t: LeaderTrade) -> bool:
        """True, если сделка новая."""
        if t.key in self.seen:
            return False
        self.seen.add(t.key)
        self._seen_q.append(t.key)
        if len(self._seen_q) > 200000:
            self.seen.discard(self._seen_q.popleft())
        return True

    # ------------------------------------------------------------ signals
    async def on_trade(self, raw: dict, source: str):
        t = parse_trade(raw)
        if t is None or t.wallet not in self.names:
            return
        if not self.mark_seen(t):
            self._count(f"dup_{source}")
            return
        self._count(f"first_{source}")
        t.name = self.names[t.wallet]
        now = self.api.now()
        lag = max(0.0, now - t.ts)
        self._rec("trade", source=source, data=raw)

        fc = t.implied_fee_coef
        if fc is not None:
            self.stats["fee_coef"].append(round(fc, 4))

        nk = (t.wallet, t.asset)
        held_before = self.leader_net.get(nk, 0.0)
        self.leader_net[nk] = held_before + (t.size if t.side == "BUY" else -t.size)

        g = (t.wallet, t.asset, t.side)
        first = self.recent_group.get(g)
        if first is not None and t.ts - first <= self.cfg["group_window_s"]:
            self._count("merged_fill")      # дочерние исполнения того же ордера лидера
            return
        self.recent_group[g] = t.ts
        self._count("signals")
        self.stats["lag"].append(round(lag, 3))

        if t.side == "BUY":
            await self._copy_buy(t, lag, source)
        else:
            await self._copy_sell(t, lag, source, held_before)

    async def _end_ts(self, slug: str):
        if slug in self.end_cache:
            return self.end_cache[slug]
        e = end_ts_from_slug(slug)
        if e is None:
            try:
                m = await self.api.get_market(slug)
                if m:
                    self._rec("market", slug=slug, data=m)
                e = market_end_ts(m)
            except Exception:  # noqa: BLE001
                e = None
        if e is not None:
            self.end_cache[slug] = e
        return e

    def _skip(self, t: LeaderTrade, reason: str, lag: float, source: str, **kw):
        self._count(f"skip_{reason}")
        self._row(leader=t.name, source=source, action="SKIP", reason=reason, market=t.title,
                  outcome=t.outcome, leader_price=t.price, leader_size=t.size,
                  lag_s=f"{lag:.2f}", **kw)

    async def _record_book_only(self, t: LeaderTrade):
        try:
            book = await self.api.get_book(t.asset)
            self._rec("book", trade_key=t.key, asset=t.asset, data=book)
        except Exception:  # noqa: BLE001
            pass

    async def _copy_buy(self, t: LeaderTrade, lag: float, source: str):
        cfg = self.cfg
        end_task = asyncio.ensure_future(self._end_ts(t.slug))
        t0 = time.perf_counter()
        try:
            book = await self.api.get_book(t.asset)
        except Exception as e:  # noqa: BLE001
            end_task.cancel()
            self._skip(t, "book_error", lag, source)
            self.log(f"book error {t.asset[:10]}: {e}")
            return
        book_ms = (time.perf_counter() - t0) * 1000
        self.stats["book_ms"].append(round(book_ms, 1))
        self._rec("book", trade_key=t.key, asset=t.asset, data=book)
        end_ts = await end_task
        now = self.api.now()

        if end_ts is not None and end_ts - now < cfg["min_seconds_left"]:
            return self._skip(t, "too_close_to_end", lag, source, book_ms=f"{book_ms:.0f}")
        if t.price > cfg["max_entry_price"]:
            return self._skip(t, "price_too_high", lag, source, book_ms=f"{book_ms:.0f}")

        min_sz = float(book.get("min_order_size") or 5)
        limit = min(t.price + cfg["max_slippage"], cfg["max_entry_price"])
        shares = cfg["usd_per_trade"] / max(t.price, 0.01)
        if shares < min_sz:
            shares = min_sz
        if shares * t.price > cfg["max_usd_per_trade"]:
            return self._skip(t, "min_order_too_big", lag, source, book_ms=f"{book_ms:.0f}")

        # идеальный двойник: та же сделка по цене лидера
        ideal_cost = shares * t.price + fee(shares, t.price, cfg["taker_fee_coef"])
        if self.ideal.cash >= ideal_cost:
            self.ideal.buy(t, shares, shares * t.price,
                           fee(shares, t.price, cfg["taker_fee_coef"]), end_ts)

        per_share_max = limit + fee(1, limit, cfg["taker_fee_coef"])
        affordable = self.real.cash / per_share_max if per_share_max > 0 else 0
        if affordable < min_sz:
            return self._skip(t, "no_cash", lag, source, book_ms=f"{book_ms:.0f}")
        shares = min(shares, affordable)

        f = walk_book(book, "BUY", shares, limit, cfg["taker_fee_coef"])
        if f.shares <= 0:
            return self._skip(t, "no_liquidity_in_limit", lag, source,
                              book_ms=f"{book_ms:.0f}",
                              best_price="" if f.best is None else f.best)
        slip = (f.vwap - t.price) * 100
        self.stats["slip"].append(round(slip, 3))
        self.real.buy(t, f.shares, f.notional, f.fee, end_ts)
        self._count("copied_buy")
        self._row(leader=t.name, source=source, action="BUY", market=t.title, outcome=t.outcome,
                  leader_price=t.price, leader_size=t.size, lag_s=f"{lag:.2f}",
                  book_ms=f"{book_ms:.0f}", best_price=f.best, our_vwap=f"{f.vwap:.4f}",
                  slip_cents=f"{slip:.2f}", our_shares=f"{f.shares:.2f}",
                  our_cost=f"{f.notional + f.fee:.4f}", fee=f"{f.fee:.4f}")
        self.log(f"BUY  {t.name:<12} {t.outcome:<5} лидер {t.price:.3f} -> мы {f.vwap:.3f} "
                 f"({slip:+.1f}¢) x{f.shares:.1f} | лаг {lag:.1f}с | {t.title[:50]}")

    async def _copy_sell(self, t: LeaderTrade, lag: float, source: str, leader_held: float):
        cfg = self.cfg
        pos = self.real.positions.get(t.asset)
        ipos = self.ideal.positions.get(t.asset)
        if not pos and not ipos:
            self._count("skip_sell_not_held")
            asyncio.ensure_future(self._record_book_only(t)) if self.record else None
            return
        frac = min(1.0, t.size / leader_held) if leader_held > 1e-9 else 1.0
        if ipos:
            sh = ipos.shares * frac
            self.ideal.sell(t.asset, sh, sh * t.price, fee(sh, t.price, cfg["taker_fee_coef"]))
        if not pos:
            return
        t0 = time.perf_counter()
        try:
            book = await self.api.get_book(t.asset)
        except Exception:  # noqa: BLE001
            return self._skip(t, "book_error", lag, source)
        book_ms = (time.perf_counter() - t0) * 1000
        self._rec("book", trade_key=t.key, asset=t.asset, data=book)
        limit = max(t.price - cfg["max_slippage"], 0.001)
        f = walk_book(book, "SELL", pos.shares * frac, limit, cfg["taker_fee_coef"])
        if f.shares <= 0:
            return self._skip(t, "no_bids_in_limit", lag, source, book_ms=f"{book_ms:.0f}")
        pnl = self.real.sell(t.asset, f.shares, f.notional, f.fee)
        slip = (t.price - f.vwap) * 100
        self._count("copied_sell")
        self._row(leader=t.name, source=source, action="SELL", market=t.title, outcome=t.outcome,
                  leader_price=t.price, leader_size=t.size, lag_s=f"{lag:.2f}",
                  book_ms=f"{book_ms:.0f}", best_price=f.best, our_vwap=f"{f.vwap:.4f}",
                  slip_cents=f"{slip:.2f}", our_shares=f"{f.shares:.2f}",
                  fee=f"{f.fee:.4f}", pnl=f"{pnl:.4f}")
        self.log(f"SELL {t.name:<12} {t.outcome:<5} лидер {t.price:.3f} -> мы {f.vwap:.3f} "
                 f"PnL {pnl:+.2f}$ | {t.title[:50]}")

    # ------------------------------------------------------------ settlement
    async def resolve_once(self):
        now = self.api.now()
        slugs: dict[str, list[str]] = {}
        for pf in (self.real, self.ideal):
            for a, p in pf.positions.items():
                slugs.setdefault(p.slug, [])
                if a not in slugs[p.slug]:
                    slugs[p.slug].append(a)
        for slug, assets in slugs.items():
            end = self.end_cache.get(slug) or end_ts_from_slug(slug)
            if end is not None and now < end + 5:
                continue
            if now - self.last_resolve_try.get(slug, 0) < self.cfg["resolve_every_s"]:
                continue
            self.last_resolve_try[slug] = now
            try:
                m = await self.api.get_market(slug)
            except Exception as e:  # noqa: BLE001
                self.log(f"resolve error {slug}: {e}")
                continue
            if m and slug not in self.end_cache:
                e2 = market_end_ts(m)
                if e2:
                    self.end_cache[slug] = e2
            pay = resolution_payouts(m) if m else None
            if not pay:
                continue
            self._rec("market", slug=slug, data=m)
            for a in assets:
                if a not in pay:
                    continue
                pos = self.real.positions.get(a)
                self.ideal.settle(a, pay[a])
                if pos:
                    pnl = self.real.settle(a, pay[a])
                    self._count("settled")
                    self._row(leader=pos.leader, action="SETTLE", market=pos.title,
                              outcome=pos.outcome, our_shares=f"{pos.shares:.2f}",
                              our_cost=f"{pos.cost:.4f}", pnl=f"{pnl:.4f}",
                              reason="win" if pay[a] else "loss")
                    self.log(f"ИТОГ {pos.leader:<12} {pos.outcome:<5} "
                             f"{'выигрыш' if pay[a] else 'проигрыш'} {pnl:+.2f}$ | {pos.title[:50]}")

    # ------------------------------------------------------------ status
    def status_line(self) -> str:
        r, i = self.real, self.ideal
        return (f"Счёт: ${r.equity_at_cost():.2f} (свободно ${r.cash:.2f}, в позициях "
                f"{len(r.positions)}) | реализ. PnL {r.realized:+.2f}$ | идеал по ценам лидера "
                f"{i.realized:+.2f}$ | сигналов {self.counters.get('signals', 0)}, "
                f"скопировано {self.counters.get('copied_buy', 0)}")


# ======================================================================= FEEDS

async def rtds_loop(engine: Engine, stop: asyncio.Event):
    url = engine.cfg["rtds_url"]
    addrs = list(engine.names)
    backoff = 1
    while not stop.is_set():
        ws = None
        try:
            ws = await WebSocket.connect(url)
            await ws.send(json.dumps({"action": "subscribe",
                                      "subscriptions": [{"topic": "activity", "type": "trades"}]}))
            engine.log("RTDS: подключено, слушаю ленту сделок")
            backoff = 1

            async def pinger():
                while True:
                    await asyncio.sleep(5)
                    await ws.send("ping")
            ping_task = asyncio.ensure_future(pinger())
            try:
                while not stop.is_set():
                    msg = await ws.recv()
                    low = msg.lower()
                    if not any(a in low for a in addrs):
                        continue
                    try:
                        obj = json.loads(msg)
                    except ValueError:
                        continue
                    payload = obj.get("payload") if isinstance(obj, dict) else None
                    items = payload if isinstance(payload, list) else [payload]
                    for p in items:
                        if isinstance(p, dict):
                            asyncio.ensure_future(engine.on_trade(p, "rtds"))
            finally:
                ping_task.cancel()
        except Exception as e:  # noqa: BLE001
            engine._count("rtds_reconnect")
            engine.log(f"RTDS: обрыв ({e}), переподключение через {backoff}с")
        finally:
            if ws:
                await ws.close()
        try:
            await asyncio.wait_for(stop.wait(), backoff)
        except asyncio.TimeoutError:
            pass
        backoff = min(backoff * 2, 30)


async def poll_loop(engine: Engine, wallet: str, stop: asyncio.Event):
    api = engine.api
    primed = False
    while not stop.is_set():
        try:
            items = await api.get_activity(wallet)
            items.sort(key=lambda d: d.get("timestamp", 0))
            for d in items:
                t = parse_trade(d)
                if t is None:
                    continue
                if not primed:
                    engine.mark_seen(t)       # историю до запуска не копируем
                else:
                    asyncio.ensure_future(engine.on_trade(d, "poll"))
            if not primed:
                engine.log(f"Опрос {engine.names[wallet]}: старт, история пропущена ({len(items)})")
            primed = True
        except Exception as e:  # noqa: BLE001
            engine._count("poll_error")
            engine.log(f"Опрос {engine.names.get(wallet)}: ошибка {e}")
        try:
            await asyncio.wait_for(stop.wait(), engine.cfg["poll_interval_s"])
        except asyncio.TimeoutError:
            pass


async def periodic(engine: Engine, stop: asyncio.Event):
    last_status = 0.0
    while not stop.is_set():
        try:
            await engine.resolve_once()
        except Exception as e:  # noqa: BLE001
            engine.log(f"resolver: {e}")
        if time.time() - last_status >= engine.cfg["status_every_s"]:
            engine.save_state()
            engine.log(engine.status_line())
            last_status = time.time()
        try:
            await asyncio.wait_for(stop.wait(), 5)
        except asyncio.TimeoutError:
            pass


async def run(cfg: dict, out: Path):
    if not cfg["leaders"]:
        sys.exit("В config.json нет leaders.")
    engine = Engine(cfg, Api(cfg), out)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, stop.set)
        except NotImplementedError:
            pass
    engine.log(f"Старт симуляции. Лидеры: {', '.join(cfg['leaders'])}. Счёт ${engine.real.cash:.2f}")
    tasks = [asyncio.ensure_future(periodic(engine, stop))]
    if cfg["use_polling"]:
        tasks += [asyncio.ensure_future(poll_loop(engine, w, stop)) for w in engine.names]
    if cfg["use_rtds"]:
        tasks.append(asyncio.ensure_future(rtds_loop(engine, stop)))
    await stop.wait()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    engine.save_state()
    engine.log("Остановлено, состояние сохранено. Отчёт: python3 simbot.py report")


# ======================================================================= CHECK

async def check(cfg: dict):
    api = Api(cfg)
    ok = True

    async def timed(name, coro):
        nonlocal ok
        t0 = time.perf_counter()
        try:
            res = await coro
            print(f"  OK   {name:<34} {(time.perf_counter() - t0) * 1000:7.0f} мс")
            return res
        except Exception as e:  # noqa: BLE001
            ok = False
            print(f"  FAIL {name:<34} {e}")
            return None

    print("Проверка доступа к Polymarket:")
    try:
        geo = await api.http.aget("https://polymarket.com/api/geoblock")
        print(f"  Гео: {geo}")
    except Exception as e:  # noqa: BLE001
        print(f"  Гео: не удалось проверить ({e})")
    for name, w in cfg["leaders"].items():
        acts = await timed(f"Data API: сделки {name}", api.get_activity(w, 5))
        if acts:
            last = parse_trade(acts[0])
            if last:
                print(f"       последняя: {utc(last.ts)} {last.side} {last.outcome} "
                      f"{last.price} — {last.title[:45]}")
                book = await timed("CLOB: стакан (1-й запрос)", api.get_book(last.asset))
                await timed("CLOB: стакан (повтор, keep-alive)", api.get_book(last.asset))
                if book:
                    print(f"       bids {len(book.get('bids', []))}, asks {len(book.get('asks', []))}, "
                          f"min_order_size {book.get('min_order_size')}")
                await timed("Gamma: рынок", api.get_market(last.slug))
    print("  RTDS: подключаюсь и слушаю 10 секунд...")
    try:
        t0 = time.perf_counter()
        ws = await WebSocket.connect(cfg["rtds_url"])
        print(f"  OK   RTDS подключение {(time.perf_counter() - t0) * 1000:22.0f} мс")
        await ws.send(json.dumps({"action": "subscribe",
                                  "subscriptions": [{"topic": "activity", "type": "trades"}]}))
        n, lags, end = 0, [], time.time() + 10
        while time.time() < end:
            try:
                msg = await asyncio.wait_for(ws.recv(), end - time.time())
            except asyncio.TimeoutError:
                break
            try:
                p = json.loads(msg).get("payload") or {}
            except (ValueError, AttributeError):
                continue
            if isinstance(p, dict) and p.get("timestamp"):
                n += 1
                ts = float(p["timestamp"])
                ts = ts / 1000 if ts > 1e12 else ts
                lags.append(time.time() - ts)
        await ws.close()
        if n:
            print(f"  OK   RTDS: {n} сделок за 10 с, задержка ленты медиана "
                  f"{statistics.median(lags):.2f} с")
        else:
            ok = False
            print("  FAIL RTDS: подключились, но сделок не пришло (формат мог измениться)")
    except Exception as e:  # noqa: BLE001
        ok = False
        print(f"  FAIL RTDS: {e}")
    print("Итог:", "всё работает, можно запускать run" if ok else
          "есть проблемы (см. FAIL выше). Если упал только RTDS — "
          "поставьте \"use_rtds\": false, бот будет работать на опросе")
    return ok


# ======================================================================= REPORT

def _pct(xs, q):
    if not xs:
        return float("nan")
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))]


def report(out: Path) -> str:
    sp = out / "state.json"
    if not sp.exists():
        return f"Нет данных в {out}. Сначала запустите: python3 simbot.py run"
    d = json.loads(sp.read_text())
    real, ideal = Portfolio.from_dict(d["real"]), Portfolio.from_dict(d["ideal"])
    c, s = d["counters"], d["stats"]
    hours = max((d["saved"] - d["started"]) / 3600, 1e-9)
    L = []
    a = L.append
    a(f"=== Отчёт симуляции копирования ({hours:.1f} ч наблюдения) ===")
    a(f"Старт ${real.start_cash:.2f}  ->  сейчас ${real.equity_at_cost():.2f} "
      f"(свободно ${real.cash:.2f} + {len(real.positions)} открытых позиций по себестоимости)")
    a(f"Реализованный PnL: {real.realized:+.2f}$   (рынков рассчитано: {real.wins + real.losses}, "
      f"в плюс {real.wins}, в минус {real.losses})")
    a(f"Комиссии уплачено: ${real.fees:.2f}")
    a(f"Если бы входили по цене лидера: PnL {ideal.realized:+.2f}$  "
      f"-> цена опоздания {real.realized - ideal.realized:+.2f}$")
    a("")
    a(f"Сигналов (ордеров лидеров): {c.get('signals', 0)}, исполнений сведено в них: "
      f"{c.get('merged_fill', 0)}")
    a(f"Скопировано: покупок {c.get('copied_buy', 0)}, продаж {c.get('copied_sell', 0)}")
    skips = {k[5:]: v for k, v in c.items() if k.startswith("skip_")}
    if skips:
        a("Пропущено: " + ", ".join(f"{k} {v}" for k, v in sorted(skips.items(), key=lambda x: -x[1])))
    a(f"Кто узнал первым: лента RTDS {c.get('first_rtds', 0)}, опрос {c.get('first_poll', 0)}")
    if s["lag"]:
        a(f"Задержка до сигнала: медиана {statistics.median(s['lag']):.2f} с, "
          f"90% {_pct(s['lag'], .9):.2f} с")
    if s["book_ms"]:
        a(f"Запрос стакана: медиана {statistics.median(s['book_ms']):.0f} мс")
    if s["slip"]:
        a(f"Проскальзывание против цены лидера: среднее {statistics.mean(s['slip']):+.2f}¢, "
          f"медиана {statistics.median(s['slip']):+.2f}¢, 90% {_pct(s['slip'], .9):+.2f}¢")
    if s["fee_coef"]:
        a(f"Комиссия, которую платит сам лидер (коэф.): медиана {statistics.median(s['fee_coef']):.3f} "
          f"(0 = мейкер, ~0.07 = тейкер)")
    a("")
    days = hours / 24
    if days >= 1 and (real.wins + real.losses) >= 30:
        a(f"Прогноз на 30 дней при том же темпе: {real.realized / days * 30:+.2f}$ "
          f"(грубая экстраполяция, не гарантия)")
    else:
        a("Для прогноза на месяц нужно минимум сутки и 30+ рассчитанных рынков.")
    return "\n".join(L)


# ======================================================================= RESIM

async def resim(cfg: dict, src: Path, dst: Path) -> str:
    rec = src / "recording.jsonl"
    if not rec.exists():
        return f"Нет записи {rec}"
    events = [json.loads(l) for l in rec.read_text().splitlines() if l.strip()]
    api = FakeApi()
    for e in events:
        if e["kind"] == "book":
            api.books.setdefault(e["trade_key"], e["data"])
        elif e["kind"] == "market":
            api.markets.setdefault(e["slug"], []).append((e["t"], e["data"]))
    if dst.exists():
        for f in ("state.json", "trades.csv"):
            (dst / f).unlink(missing_ok=True)
    cfg = dict(cfg, resolve_every_s=0)
    eng = Engine(cfg, api, dst, record=False, quiet=True)
    trades = [e for e in events if e["kind"] == "trade"]
    if trades:
        api.t = trades[0]["t"]
        eng.started = api.t
    for e in trades:
        api.t = e["t"]
        await eng.resolve_once()
        t = parse_trade(e["data"])
        api.current_key = t.key if t else ""
        await eng.on_trade(e["data"], e.get("source", "replay"))
    if api.markets:
        api.t = max(t for v in api.markets.values() for t, _ in v) + 1
        await eng.resolve_once()
    eng.save_state()
    return report(dst)


# ======================================================================= CLI

def main():
    ap = argparse.ArgumentParser(description="Симулятор копитрейдинга Polymarket")
    ap.add_argument("cmd", choices=["run", "check", "report", "resim"])
    ap.add_argument("--config", default=str(HERE / "config.json"))
    ap.add_argument("--out", default=str(HERE / "data"))
    for k in ("usd_per_trade", "max_slippage", "max_entry_price", "min_seconds_left",
              "taker_fee_coef", "start_cash"):
        ap.add_argument("--" + k.replace("_", "-"), type=float)
    a = ap.parse_args()
    cfg = load_config(Path(a.config))
    for k in ("usd_per_trade", "max_slippage", "max_entry_price", "min_seconds_left",
              "taker_fee_coef", "start_cash"):
        v = getattr(a, k)
        if v is not None:
            cfg[k] = v
    out = Path(a.out)
    if a.cmd == "run":
        asyncio.run(run(cfg, out))
    elif a.cmd == "check":
        sys.exit(0 if asyncio.run(check(cfg)) else 1)
    elif a.cmd == "report":
        print(report(out))
    elif a.cmd == "resim":
        print(asyncio.run(resim(cfg, out, out / "resim")))


if __name__ == "__main__":
    main()
