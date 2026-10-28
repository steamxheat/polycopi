#!/usr/bin/env python3
"""Polymarket MARKET-MAKER SIMULATOR for 5-minute BTC/ETH "Up or Down" markets.

Виртуально ставит лимитные заявки на покупку ОБЕИХ сторон (Up и Down) так, чтобы
сумма цен была меньше $1 (например 0.48 + 0.48 = 0.96). Если исполнятся обе — пара
гарантированно выплатит $1 → прибыль. Если исполнится одна («одна нога») — исход
зависит от рынка.

Исполнение моделируется по РЕАЛЬНЫМ сделкам Polymarket (data-api /trades):
  * сделка по нашему токену ниже нашей цены → нас исполнили (цена прошла сквозь нас);
  * сделка ровно по нашей цене → сначала съедается очередь, стоявшая перед нами
    (её объём берётся из реального стакана в момент выставления), потом мы.
Пока исполнилась только одна нога или ни одной, при резком движении BTC/ETH на Binance
заявки снимаются (если не исполнено ничего). Реальных ордеров нет, ключи не нужны.

Команды:
  python3 mmsim.py run       запустить (Ctrl+C — стоп, состояние сохраняется)
  python3 mmsim.py report    отчёт
  python3 mmsim.py check     проверить доступ к Polymarket и Binance
Логи: data_mm/mm.log (подробно), data_mm/events.jsonl, data_mm/pairs.csv
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import logging.handlers
import math
import os
import signal
import statistics
import sys
import time
from collections import deque, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path

import fvmodel
from core import resolution_payouts, _as_list
from net import Http, HttpError, WebSocket

HERE = Path(__file__).resolve().parent
log = logging.getLogger("polymm")

DEFAULTS = {
    "assets": {"btc": "BTCUSDT", "eth": "ETHUSDT"},
    "start_cash": 100.0,
    "shares": 5,                    # объём каждой ноги (минимальный ордер Polymarket)
    "edge": 0.04,                   # Up+Down = 1 - edge; 0.04 -> пара стоит 0.96
    "tick": 0.01,
    "min_mid": 0.20,                # не котировать, если рынок уже почти решён
    "max_mid": 0.80,
    "start_after_s": 5,             # с какой секунды окна начинаем
    "stop_new_pairs_before_end_s": 60,
    "cancel_all_before_end_s": 5,
    "max_pairs_per_market": 3,
    "max_open_pairs_per_market": 1,  # новая пара — только когда прошлая закрыта полностью
    "requote_ticks": 2,             # переставить, если справедливая цена ушла на N тиков
    "move_window_s": 3,             # окно для детекции резкого движения
    "move_threshold_bps": 5,        # 5 б.п. = 0.05% за move_window_s
    "book_move_ticks": 4,           # рывок по стакану Polymarket: середина Up сдвинулась на N тиков
    "book_move_window_s": 2,
    "book_cooldown_s": 3,           # пауза после рывка стакана (короче, чем после рывка биржи)
    "cooldown_after_move_s": 10,
    "chase_enabled": False,         # догонять вторую ногу, если первая исполнилась
    "chase_after_s": 30,
    "chase_max_total": 0.99,        # максимум суммарной цены пары при догонянии
    "maker_fee_coef": 0.0,          # мейкер на Polymarket комиссию не платит
    # ребейты: Polymarket раздаёт мейкерам 20% комиссии тейкеров (крипто). Доля мейкера ≈ его
    # fee-equivalent = shares * 0.07 * p * (1-p), так что ребейт ≈ 0.2 * это. В счёт НЕ добавляем,
    # показываем отдельно: правила программы могут поменяться.
    "rebate_share": 0.20,
    "taker_fee_rate": 0.07,
    # справедливая цена: "book" — середина стакана (первая версия), "t60" — модель по правилу
    # расчёта Polymarket (средняя Chainlink за последнюю минуту против минуты перед стартом)
    # "guard" — цена по стакану (рынок откалиброван), а модель t60 = сигнал опасности:
    # расхождение модели и середины стакана > guard_max_dev -> не котируем и снимаем заявки
    "fair_mode": "book",
    "guard_max_dev": 0.05,
    "rule": "t60",
    "sigma_window_s": 900,
    "sigma_floor": 2e-5,
    "retro_window_s": 15,           # сколько ждём «запоздавшие» сделки по снятым заявкам
    "loop_s": 1.0,
    "trades_poll_s": 1.0,
    "status_every_s": 60,
    "resolve_every_s": 15,
    "log_max_mb": 20,
    "log_backups": 10,
    "clob_url": "https://clob.polymarket.com",
    "data_url": "https://data-api.polymarket.com",
    "gamma_url": "https://gamma-api.polymarket.com",
    "binance_ws": "wss://data-stream.binance.vision/stream",
    "clob_ws": "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    "use_clob_ws": True,            # сделки в реальном времени (data-api отстаёт на десятки секунд)
    "order_latency_s": 0.3,         # заявка начинает работать через N с после решения
    "cancel_latency_s": 0.3,        # снятие срабатывает через N с
    "move_watch_s": 0.2,            # как часто проверять рывки бирж (fast-версия: 0.05)
    "ws_silence_s": 10,
    "use_rtds_prices": False,       # RTDS crypto_prices: Polymarket его выключает
    "use_coinbase_ws": True,
    "use_okx_ws": True,
    "coinbase_ws": "wss://ws-feed.exchange.coinbase.com",
    "okx_ws": "wss://ws.okx.com:8443/ws/v5/public",
    "use_binance_ws": True,         # прямой поток Binance (может резаться сетью сервера)
    "rtds_url": "wss://ws-live-data.polymarket.com",
    "rest_fallback_after_s": 2,     # опрос REST, если потоки молчат дольше N с
    "binance_rest": "https://data-api.binance.vision",
}


def utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def floor_tick(x: float, tick: float) -> float:
    return round(math.floor(x / tick + 1e-9) * tick, 4)


def load_config(path: Path) -> dict:
    cfg = dict(DEFAULTS)
    if path.exists():
        cfg.update(json.loads(path.read_text()))
    return cfg


def setup_logging(out: Path, cfg: dict, quiet: bool = False):
    log.handlers.clear()
    log.setLevel(logging.DEBUG)
    log.propagate = False
    if not quiet:
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        ch.setFormatter(logging.Formatter("[%(asctime)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
        ch.formatter.converter = time.gmtime
        log.addHandler(ch)
    out.mkdir(parents=True, exist_ok=True)
    fh = logging.handlers.RotatingFileHandler(out / "mm.log", maxBytes=int(cfg["log_max_mb"] * 2**20),
                                              backupCount=int(cfg["log_backups"]), encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s.%(msecs)03d %(levelname)-5s %(message)s",
                                      "%Y-%m-%d %H:%M:%S"))
    fh.formatter.converter = time.gmtime
    log.addHandler(fh)


# ======================================================================= model

@dataclass
class Order:
    id: int
    pair_id: int
    slug: str
    leg: str                 # "Up" / "Down"
    token: str
    price: float
    shares: float
    placed_ts: float
    queue_ahead: float
    filled: float = 0.0
    canceled_ts: float | None = None
    cancel_reason: str = ""
    first_fill_ts: float | None = None
    released: bool = False   # резерв денег освобождён

    @property
    def remaining(self) -> float:
        return max(0.0, self.shares - self.filled)



@dataclass
class Pair:
    id: int
    slug: str
    asset: str
    created_ts: float
    up: int                  # order ids
    down: int
    fair_up: float
    state: str = "quoting"   # quoting | one_leg | complete | canceled | settled
    close_reason: str = ""
    chased: bool = False
    pnl: float | None = None
    outcome: str = ""        # complete | one_leg_win | one_leg_loss | no_fill | partial
    feat: dict = field(default_factory=dict)   # «фото рынка»: q — при котировке, f — при первом исполнении


@dataclass
class Market:
    slug: str
    asset: str
    condition_id: str
    up_token: str
    down_token: str
    start_ts: float
    end_ts: float
    tick: float = 0.01
    pairs: list = field(default_factory=list)
    resolved: bool = False
    winner: str = ""
    trades_seen: int = 0


class Account:
    def __init__(self, cash: float):
        self.start_cash = cash
        self.cash = cash                    # свободно
        self.reserved = 0.0                 # под заявками
        self.positions: dict[str, list] = {}   # token -> [shares, cost]
        self.realized = 0.0
        self.rebates = 0.0                  # оценка ребейтов мейкера (в счёт не входит)

    def reserve(self, amount: float) -> bool:
        if amount > self.cash + 1e-9:
            return False
        self.cash -= amount
        self.reserved += amount
        return True

    def release(self, amount: float):
        amount = min(amount, self.reserved)
        self.reserved -= amount
        self.cash += amount

    def fill(self, token: str, shares: float, price: float, fee: float, reserved_part: float):
        """reserved_part — сколько из резерва покрывает эту покупку."""
        cost = shares * price + fee
        self.reserved -= reserved_part
        self.cash -= (cost - reserved_part)
        p = self.positions.setdefault(token, [0.0, 0.0])
        p[0] += shares
        p[1] += cost

    def settle(self, token: str, payout: float) -> float:
        p = self.positions.pop(token, None)
        if not p:
            return 0.0
        pnl = p[0] * payout - p[1]
        self.cash += p[0] * payout
        self.realized += pnl
        return pnl

    def equity_at_cost(self) -> float:
        return self.cash + self.reserved + sum(c for _, c in self.positions.values())

    def to_dict(self):
        return {"start_cash": self.start_cash, "cash": self.cash, "reserved": self.reserved,
                "positions": self.positions, "realized": self.realized, "rebates": self.rebates}

    @classmethod
    def from_dict(cls, d):
        a = cls(d["start_cash"])
        a.cash, a.reserved, a.realized = d["cash"], d["reserved"], d["realized"]
        a.positions = {k: list(v) for k, v in d["positions"].items()}
        a.rebates = d.get("rebates", 0.0)
        return a


# ======================================================================= price feed

class PriceFeed:
    """Цены BTC/ETH из нескольких источников. История хранится по каждому источнику
    отдельно (у бирж цены чуть различаются), рывок = максимум по источникам."""

    def __init__(self, symbols: list[str], keep_s: float = 30):
        self.symbols = set(symbols)
        self.hist: dict[tuple, deque] = {}
        self.keep_s = keep_s
        self.last_update = 0.0
        self.source = ""
        self.seen: dict[str, float] = {}       # источник -> время последней цены

    def add(self, symbol: str, ts: float, price: float, source: str = "?"):
        if symbol not in self.symbols or not price:
            return
        h = self.hist.setdefault((symbol, source), deque())
        h.append((ts, price))
        while h and ts - h[0][0] > self.keep_s:
            h.popleft()
        self.last_update = max(self.last_update, ts)
        self.seen[source] = ts
        self.source = source

    def last(self, symbol: str):
        best = None
        for (sym, _), h in self.hist.items():
            if sym == symbol and h and (best is None or h[-1][0] > best[0]):
                best = h[-1]
        return best[1] if best else None

    def ret_bps(self, symbol: str, now: float, window_s: float, fresh_s: float = 3.0):
        """Изменение цены за последние window_s секунд, б.п. (медиана по источникам; + = рост)."""
        vals = []
        for (sym, _), h in self.hist.items():
            if sym != symbol or not h or now - h[-1][0] > fresh_s:
                continue
            past = None
            for ts, px in reversed(h):
                if ts <= now - window_s:
                    past = px
                    break
            if past:
                vals.append((h[-1][1] / past - 1) * 1e4)
        return round(statistics.median(vals), 2) if vals else None

    def move_bps(self, symbol: str, now: float, window_s: float) -> float:
        """Максимальное отклонение последней цены от любой цены за окно, в б.п."""
        mx = 0.0
        for (sym, _), h in self.hist.items():
            if sym != symbol or not h or now - h[-1][0] > window_s:
                continue
            last = h[-1][1]
            for t, p in h:
                if now - t <= window_s:
                    mx = max(mx, abs(last - p) / p * 1e4)
        return mx

    def sources_alive(self, now: float, within: float = 10) -> list[str]:
        return sorted(k for k, t in self.seen.items() if now - t <= within)


# ======================================================================= API

class Api:
    def __init__(self, cfg):
        self.cfg = cfg
        self.http = Http(timeout=5.0)
        # Binance REST без keep-alive: на некоторых сетях длинное соединение «замерзает»
        self.http_px = Http(timeout=3.0, keepalive=False)

    def now(self):
        return time.time()

    async def get_market(self, slug: str, closed: bool | None = None):
        params = {"slug": slug}
        if closed is not None:
            params["closed"] = "true" if closed else "false"
        res = await self.http.aget(f"{self.cfg['gamma_url']}/markets", params)
        if isinstance(res, list):
            return res[0] if res else None
        return res or None

    async def get_book(self, token: str):
        return await self.http.aget(f"{self.cfg['clob_url']}/book", {"token_id": token})

    async def get_trades(self, condition_id: str, limit: int = 500):
        res = await self.http.aget(f"{self.cfg['data_url']}/trades",
                                   {"market": condition_id, "limit": limit, "takerOnly": "true"})
        return res if isinstance(res, list) else []

    async def get_prices(self, symbols: list[str]):
        res = await self.http_px.aget(f"{self.cfg['binance_rest']}/api/v3/ticker/price",
                                   {"symbols": json.dumps(symbols, separators=(",", ":"))})
        return {d["symbol"]: float(d["price"]) for d in res}


def best(book: dict, side: str):
    lv = []
    for x in book.get(side) or []:
        try:
            p, s = float(x["price"]), float(x["size"])
        except (KeyError, TypeError, ValueError):
            continue
        if s > 0:
            lv.append((p, s))
    if not lv:
        return None, 0.0, []
    lv.sort(key=lambda z: -z[0] if side == "bids" else z[0])
    return lv[0][0], lv[0][1], lv


def size_at(levels, price) -> float:
    return sum(s for p, s in levels if abs(p - price) < 1e-9)


# ======================================================================= engine

PAIR_COLS = ["pair_id", "asset", "slug", "created_utc", "fair_up", "up_price", "down_price",
             "pair_cost", "up_filled", "down_filled", "outcome", "close_reason", "winner",
             "pnl", "first_fill_s", "second_fill_s", "chased"]


class Engine:
    def __init__(self, cfg: dict, api, out: Path, quiet: bool = False):
        self.cfg, self.api, self.out, self.quiet = cfg, api, out, quiet
        out.mkdir(parents=True, exist_ok=True)
        self.state_path = out / "state.json"
        self.acct = Account(cfg["start_cash"])
        self.orders: dict[int, Order] = {}
        self.pairs: dict[int, Pair] = {}
        self.markets: dict[str, Market] = {}
        self.next_id = 1
        self.started = api.now()
        self.counters: dict[str, int] = {}
        self.cooldown: dict[str, float] = {}
        self.trade_seen: dict[str, set] = {}
        self.resolve_next: dict[str, float] = {}
        self.prices = PriceFeed(list(cfg["assets"].values()))
        self.trade_lag: dict[str, deque] = {"ws": deque(maxlen=2000), "api": deque(maxlen=2000)}
        self.book_hist: dict[str, deque] = {}
        self.book_cooldown: dict[str, float] = {}
        self.run_started = api.now()
        self.ws_intervals: list = []      # [начало, конец] периодов, когда лента CLOB работала
        self.ws_last_msg = 0.0
        self.done_pairs: list[dict] = []      # итоги рассчитанных пар (для отчёта)
        self.sampler = fvmodel.Sampler(self.prices)
        self.s0: dict[str, dict] = {}         # slug -> {источник: цена на старте окна}
        self.tob: dict[str, tuple] = {}       # токен -> (лучший бид, лучший аск, время) из ленты CLOB
        self.flow: dict[str, deque] = {}      # slug -> (время, токен, объём) сделок рынка
        self._load()

    # ------------------------------------------------------------ io
    def info(self, m):
        (log.debug if self.quiet else log.info)(m)

    def event(self, **kw):
        kw = {"t": round(self.api.now(), 3), "time_utc": utc(self.api.now()), **kw}
        with (self.out / "events.jsonl").open("a") as f:
            f.write(json.dumps(kw, ensure_ascii=False, default=str) + "\n")

    def count(self, k, n=1):
        self.counters[k] = self.counters.get(k, 0) + n

    def _load(self):
        if not self.state_path.exists():
            return
        d = json.loads(self.state_path.read_text())
        self.acct = Account.from_dict(d["account"])
        self.orders = {int(k): Order(**v) for k, v in d["orders"].items()}
        self.pairs = {int(k): Pair(**v) for k, v in d["pairs"].items()}
        for k, v in d["markets"].items():
            v = dict(v)
            v["pairs"] = list(v.get("pairs", []))
            self.markets[k] = Market(**v)
        self.next_id = d["next_id"]
        self.started = d["started"]
        self.counters = d.get("counters", {})
        self.done_pairs = d.get("done_pairs", [])

    def save(self):
        live = {m.slug for m in self.markets.values() if not m.resolved}
        d = {"version": 1, "started": self.started, "saved": self.api.now(), "config": self.cfg,
             "account": self.acct.to_dict(), "next_id": self.next_id, "counters": self.counters,
             "orders": {k: asdict(o) for k, o in self.orders.items() if o.slug in live},
             "pairs": {k: asdict(p) for k, p in self.pairs.items() if p.slug in live},
             "markets": {k: asdict(m) for k, m in self.markets.items() if m.slug in live},
             "done_pairs": self.done_pairs[-50000:]}
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(d))
        os.replace(tmp, self.state_path)

    def _nid(self):
        self.next_id += 1
        return self.next_id - 1

    # ------------------------------------------------------------ markets
    async def discover(self):
        now = self.api.now()
        start = int(now // 300 * 300)
        for asset in self.cfg["assets"]:
            slug = f"{asset}-updown-5m-{start}"
            if slug in self.markets:
                continue
            try:
                m = await self.api.get_market(slug)
            except Exception as e:  # noqa: BLE001
                log.debug(f"discover {slug}: {e}")
                continue
            if not m or m.get("closed"):
                continue
            toks = [str(t) for t in _as_list(m.get("clobTokenIds"))]
            outs = [str(o) for o in _as_list(m.get("outcomes"))]
            if len(toks) != 2 or [o.lower() for o in outs] != ["up", "down"]:
                log.debug(f"discover {slug}: неожиданный формат {outs}")
                continue
            mk = Market(slug, asset, str(m.get("conditionId", "")), toks[0], toks[1],
                        float(start), float(start + 300),
                        float(m.get("orderPriceMinTickSize") or self.cfg["tick"]))
            self.markets[slug] = mk
            self.trade_seen[slug] = set()
            self.info(f"РЫНОК {asset.upper()} {utc(mk.start_ts)[11:16]}–{utc(mk.end_ts)[11:16]} UTC "
                      f"| {slug}")

    def market_orders(self, mk: Market):
        for pid in mk.pairs:
            p = self.pairs.get(pid)
            if p:
                yield p, self.orders[p.up], self.orders[p.down]

    # ------------------------------------------------------------ orders
    def _place(self, mk: Market, pair_id: int, leg: str, price: float, queue: float) -> Order | None:
        sh = float(self.cfg["shares"])
        cost = sh * price * (1 + 0)  # мейкер без комиссии
        if not self.acct.reserve(cost):
            return None
        o = Order(self._nid(), pair_id, mk.slug, leg, mk.up_token if leg == "Up" else mk.down_token,
                  price, sh, self.api.now(), queue)
        self.orders[o.id] = o
        return o

    def _cancel(self, o: Order, reason: str):
        if o.canceled_ts is None and o.remaining > 1e-9:
            o.canceled_ts = self.api.now()
            o.cancel_reason = reason

    def _release_expired(self):
        """Освобождает деньги снятых заявок, когда прошло retro_window_s."""
        now = self.api.now()
        for o in self.orders.values():
            if (not o.released and o.canceled_ts is not None
                    and now - o.canceled_ts >= self.cfg["retro_window_s"]):
                self.acct.release(o.remaining * o.price)
                o.released = True

    def model_fair(self, mk: Market):
        """P(Up) по модели (правило cfg['rule']); None, если данных не хватает
        (например, бот запущен посреди окна и не видел цену на старте)."""
        sym = self.cfg["assets"][mk.asset]
        now = self.api.now()
        if now < mk.start_ts:
            return None
        s0 = self.s0.setdefault(mk.slug, {})
        for src in self.sampler.sources(sym):
            if src not in s0:
                v = self.sampler.value_at(sym, src, int(mk.start_ts), tol=2)
                if v is not None:
                    s0[src] = v
        if not s0:
            return None
        return fvmodel.market_prob(self.sampler, self.prices, sym, mk.start_ts, mk.end_ts, s0, now,
                                   self.cfg["rule"], self.cfg["sigma_window_s"], self.cfg["sigma_floor"])

    def snapshot(self, mk: Market, books: dict | None = None) -> dict:
        """«Фото рынка» для разбора, какие условия приводят к проигрышной одной ноге."""
        now = self.api.now()
        sym = self.cfg["assets"][mk.asset]
        f = {"t": round(now, 2), "sec": round(now - mk.start_ts, 1), "left": round(mk.end_ts - now, 1)}
        for k in (1, 2, 5, 10, 30):
            f[f"r{k}"] = self.prices.ret_bps(sym, now, k)
        try:
            sig = self.sampler.sigma(sym, now, self.cfg["sigma_window_s"])
            f["sig_bps"] = round(sig * 1e4, 3) if sig else None
        except Exception:  # noqa: BLE001
            f["sig_bps"] = None
        try:
            m = self.model_fair(mk)
            f["model"] = round(m, 4) if m is not None else None
        except Exception:  # noqa: BLE001
            f["model"] = None
        for leg, tok in (("u", mk.up_token), ("d", mk.down_token)):
            b = (books or {}).get(tok)
            if b is not None:
                bb, bsz, _ = best(b, "bids")
                ba, asz, _ = best(b, "asks")
                f[f"{leg}_bid"], f[f"{leg}_ask"], f[f"{leg}_bsz"], f[f"{leg}_asz"] = bb, ba, bsz, asz
            elif tok in self.tob:
                bb, ba, ts = self.tob[tok]
                f[f"{leg}_bid"], f[f"{leg}_ask"], f[f"{leg}_tob_age"] = bb, ba, round(now - ts, 2)
        h = self.book_hist.get(mk.slug)
        if h:
            f["mid"] = round(h[-1][1], 4)
            f["mid_chg"] = round(h[-1][1] - h[0][1], 4)
        fl = self.flow.get(mk.slug)
        if fl:
            for k in (5, 30):
                f[f"vol_u{k}"] = round(sum(sz for ts, tok, sz in fl if tok == mk.up_token and ts >= now - k), 1)
                f[f"vol_d{k}"] = round(sum(sz for ts, tok, sz in fl if tok == mk.down_token and ts >= now - k), 1)
        return f

    def _write_features(self, row: dict, p: Pair):
        rec = dict(row)
        rec.update(p.feat)
        with (self.out / "features.jsonl").open("a") as fh:
            fh.write(json.dumps(rec, separators=(",", ":")) + "\n")

    async def quote(self, mk: Market):
        cfg, now = self.cfg, self.api.now()
        if now < mk.start_ts + cfg["start_after_s"] or now > mk.end_ts - cfg["stop_new_pairs_before_end_s"]:
            return
        if now < self.cooldown.get(mk.asset, 0):
            return
        open_pairs = [p for p, _, _ in self.market_orders(mk) if p.state in ("quoting", "one_leg")]
        if any(p.state == "quoting" for p in open_pairs) or \
                len(open_pairs) >= cfg["max_open_pairs_per_market"]:
            return
        if len(mk.pairs) >= cfg["max_pairs_per_market"]:
            return
        try:
            bu, bd = await asyncio.gather(self.api.get_book(mk.up_token), self.api.get_book(mk.down_token))
        except Exception as e:  # noqa: BLE001
            log.debug(f"book {mk.slug}: {e}")
            return
        bb, _, bids_u = best(bu, "bids")
        ba, _, _ = best(bu, "asks")
        dbb, _, bids_d = best(bd, "bids")
        dba, _, _ = best(bd, "asks")
        if bb is None or ba is None:
            self.count("skip_empty_book")
            return
        book_mid = (bb + ba) / 2
        fair = book_mid
        if cfg["fair_mode"] == "guard":
            model = self.model_fair(mk)
            if model is None:
                self.count("skip_no_model")
                return
            if abs(model - book_mid) > cfg["guard_max_dev"]:
                self.count("skip_guard")
                log.debug(f"GUARD {mk.slug}: модель {model:.3f} vs стакан {book_mid:.3f} — не котирую")
                return
        elif cfg["fair_mode"] != "book":
            fair = self.model_fair(mk)
            if fair is None:
                self.count("skip_no_model")
                return
        if not (cfg["min_mid"] <= fair <= cfg["max_mid"]):
            self.count("skip_mid_extreme")
            log.debug(f"SKIP {mk.slug}: fair Up {fair:.3f} вне [{cfg['min_mid']}, {cfg['max_mid']}]")
            return
        tick = mk.tick
        qu = floor_tick(fair - cfg["edge"] / 2, tick)
        qd = round(1 - cfg["edge"] - qu, 4)          # сумма пары ровно 1 - edge
        # остаёмся мейкером: строго ниже лучшего предложения
        if ba is not None:
            qu = min(qu, floor_tick(ba - tick, tick))
        if dba is not None:
            qd = min(qd, floor_tick(dba - tick, tick))
        if qu < tick or qd < tick:
            return
        pid = self._nid()
        ou = self._place(mk, pid, "Up", qu, size_at(bids_u, qu))
        od = self._place(mk, pid, "Down", qd, size_at(bids_d, qd)) if ou else None
        if not ou or not od:
            if ou:
                self.acct.release(ou.shares * ou.price)
                del self.orders[ou.id]
            self.count("skip_no_cash")
            return
        p = Pair(pid, mk.slug, mk.asset, now, ou.id, od.id, round(fair, 4))
        try:
            p.feat["q"] = self.snapshot(mk, {mk.up_token: bu, mk.down_token: bd})
        except Exception as e:  # noqa: BLE001
            log.debug(f"snapshot: {e}")
        self.pairs[pid] = p
        mk.pairs.append(pid)
        self.count("pairs_created")
        sec = now - mk.start_ts
        self.info(f"КОТИРУЮ {mk.asset.upper()} #{pid} Up {qu:.2f} (очередь {ou.queue_ahead:.0f}) + "
                  f"Down {qd:.2f} (очередь {od.queue_ahead:.0f}) = {qu + qd:.2f} | справедл. Up {fair:.3f} "
                  f"| {sec:.0f}с окна | своб. ${self.acct.cash:.2f}")
        self.event(ev="quote", pair=pid, asset=mk.asset, slug=mk.slug, up=qu, down=qd, fair_up=fair,
                   book_mid=book_mid,
                   up_bid=bb, up_ask=ba, down_bid=dbb, down_ask=dba, q_up=ou.queue_ahead,
                   q_down=od.queue_ahead, sec_in_window=round(sec, 1))

    async def manage(self, mk: Market):
        """Переставить устаревшие котировки, догнать вторую ногу, снять всё перед концом."""
        cfg, now = self.cfg, self.api.now()
        for p, ou, od in list(self.market_orders(mk)):
            if p.state not in ("quoting", "one_leg"):
                continue
            if now >= mk.end_ts - cfg["cancel_all_before_end_s"]:
                for o in (ou, od):
                    self._cancel(o, "end")
                continue
            if p.state == "quoting" and ou.filled == 0 and od.filled == 0:
                if cfg["fair_mode"] == "t60":
                    fair = self.model_fair(mk)
                    if fair is None:
                        continue
                else:
                    try:
                        bu = await self.api.get_book(mk.up_token)
                    except Exception:  # noqa: BLE001
                        continue
                    bb, _, _ = best(bu, "bids")
                    ba, _, _ = best(bu, "asks")
                    if bb is None or ba is None:
                        continue
                    fair = (bb + ba) / 2
                    if cfg["fair_mode"] == "guard":
                        model = self.model_fair(mk)
                        if model is not None and abs(model - fair) > cfg["guard_max_dev"]:
                            for o in (ou, od):
                                self._cancel(o, "guard")
                            p.state, p.close_reason = "canceled", "guard"
                            mk.pairs.remove(p.id)
                            self.count("canceled_on_guard")
                            log.debug(f"GUARD-СНЯТИЕ #{p.id}: модель {model:.3f} vs стакан {fair:.3f}")
                            continue
                if abs(fair - p.fair_up) >= cfg["requote_ticks"] * mk.tick - 1e-9:
                    for o in (ou, od):
                        self._cancel(o, "requote")
                    p.state, p.close_reason = "canceled", "requote"
                    mk.pairs.remove(p.id)          # освобождает слот пары в рынке
                    self.count("requoted")
                    log.debug(f"REQUOTE #{p.id}: справедл. Up {p.fair_up:.3f} -> {fair:.3f}")
                    self.event(ev="requote", pair=p.id, old=p.fair_up, new=fair)
            elif p.state == "one_leg" and cfg["chase_enabled"] and not p.chased:
                filled, other = (ou, od) if ou.filled > od.filled else (od, ou)
                if filled.first_fill_ts and now - filled.first_fill_ts >= cfg["chase_after_s"]:
                    try:
                        bk = await self.api.get_book(other.token)
                    except Exception:  # noqa: BLE001
                        continue
                    oa, _, _ = best(bk, "asks")
                    _, _, obids = best(bk, "bids")
                    target = floor_tick(cfg["chase_max_total"] - filled.price, mk.tick)
                    if oa is not None:
                        target = min(target, floor_tick(oa - mk.tick, mk.tick))
                    if target > other.price + 1e-9 and other.canceled_ts is None:
                        rem = other.remaining
                        self._cancel(other, "chase")
                        no = self._place(mk, p.id, other.leg, target, size_at(obids, target))
                        if no:
                            no.shares = rem
                            self.acct.release((self.cfg["shares"] - rem) * target)
                            if other.leg == "Up":
                                p.up = no.id
                            else:
                                p.down = no.id
                            p.chased = True
                            self.count("chased")
                            self.info(f"ДОГОНЯЮ #{p.id} {other.leg}: {other.price:.2f} -> {target:.2f}")
                            self.event(ev="chase", pair=p.id, leg=other.leg, old=other.price, new=target)

    def on_book_event(self, ev: dict, tokens: dict):
        """Середина стакана Up из ленты CLOB; резкий сдвиг = снять неисполненные пары рынка."""
        items = ev.get("price_changes") if isinstance(ev.get("price_changes"), list) else [ev]
        now = self.api.now()
        for it in items:
            tok = str(it.get("asset_id") or ev.get("asset_id") or "")
            mk = tokens.get(tok)
            if mk is None:
                continue
            bb, ba = it.get("best_bid"), it.get("best_ask")
            if (bb is None or ba is None) and ev.get("event_type") == "book":
                b1, _, _ = best(ev, "bids")
                a1, _, _ = best(ev, "asks")
                bb, ba = b1, a1
            try:
                bb, ba = float(bb), float(ba)
            except (TypeError, ValueError):
                continue
            if bb <= 0 or ba <= 0 or ba < bb:
                continue
            self.tob[tok] = (bb, ba, now)
            mid = (bb + ba) / 2
            if tok == mk.down_token:
                mid = 1 - mid                       # приводим к цене Up
            h = self.book_hist.setdefault(mk.slug, deque())
            h.append((now, mid))
            while h and now - h[0][0] > self.cfg["book_move_window_s"]:
                h.popleft()
            lo, hi = min(p for _, p in h), max(p for _, p in h)
            if (hi - lo) >= self.cfg["book_move_ticks"] * mk.tick - 1e-9:
                self.on_book_move(mk, hi - lo)
                h.clear()

    def on_book_move(self, mk: Market, delta: float):
        now = self.api.now()
        if now < self.book_cooldown.get(mk.slug, 0):
            return
        self.book_cooldown[mk.slug] = now + self.cfg["book_cooldown_s"]
        n = 0
        for p, ou, od in list(self.market_orders(mk)):
            if p.state == "quoting" and ou.filled == 0 and od.filled == 0 and ou.canceled_ts is None:
                self._cancel(ou, "book_move")
                self._cancel(od, "book_move")
                p.state, p.close_reason = "canceled", "book_move"
                mk.pairs.remove(p.id)
                n += 1
        self.cooldown[mk.asset] = max(self.cooldown.get(mk.asset, 0), now + self.cfg["book_cooldown_s"])
        self.count("book_moves")
        if n:
            self.count("canceled_on_book_move", n)
        self.info(f"РЫВОК СТАКАНА {mk.asset.upper()} Up сдвинулся на {delta * 100:.1f}¢ "
                  f"за {self.cfg['book_move_window_s']}с — снято пар: {n}")
        self.event(ev="book_move", asset=mk.asset, slug=mk.slug, delta=round(delta, 4), canceled_pairs=n)

    def on_price_move(self, asset: str, bps: float):
        cfg, now = self.cfg, self.api.now()
        if now < self.cooldown.get(asset, 0):
            return
        self.cooldown[asset] = now + cfg["cooldown_after_move_s"]
        n = 0
        for mk in self.markets.values():
            if mk.asset != asset or mk.resolved:
                continue
            for p, ou, od in list(self.market_orders(mk)):
                if p.state == "quoting" and ou.filled == 0 and od.filled == 0 \
                        and ou.canceled_ts is None:
                    self._cancel(ou, "move")
                    self._cancel(od, "move")
                    p.state, p.close_reason = "canceled", "move"
                    mk.pairs.remove(p.id)
                    n += 1
        self.count("moves")
        if n:
            self.count("canceled_on_move", n)
        self.info(f"РЫВОК {asset.upper()} {bps:.1f} б.п. за {cfg['move_window_s']}с — снято пар: {n}, "
                  f"пауза {cfg['cooldown_after_move_s']}с")
        self.event(ev="move", asset=asset, bps=round(bps, 2), canceled_pairs=n)

    # ------------------------------------------------------------ fills
    def ws_covered(self, ts: float) -> bool:
        """Секунда ts целиком попала в период работы ленты CLOB -> сделка уже учтена из неё."""
        return any(a <= ts and ts + 1 <= b for a, b in self.ws_intervals)

    def on_trade(self, mk: Market, tr: dict, source: str = "api"):
        try:
            token, price, size = str(tr["asset"]), float(tr["price"]), float(tr["size"])
            ts = float(tr["timestamp"])
        except (KeyError, TypeError, ValueError):
            return
        if ts > 1e12:
            ts /= 1000
        if source == "api" and self.ws_covered(ts):
            return                                   # эту секунду уже видели в ленте CLOB
        key = f"{source}:{tr.get('transactionHash')}:{token}:{size}:{price}:{ts}:{tr.get('proxyWallet')}"
        seen = self.trade_seen.setdefault(mk.slug, set())
        if key in seen:
            return
        seen.add(key)
        mk.trades_seen += 1
        fl = self.flow.setdefault(mk.slug, deque())
        fl.append((ts, token, size))
        while fl and fl[0][0] < ts - 60:
            fl.popleft()
        self.count(f"trades_{source}")
        if ts >= self.run_started:
            self.trade_lag[source].append(self.api.now() - ts)
        avail = size
        lat_o, lat_c = self.cfg["order_latency_s"], self.cfg["cancel_latency_s"]
        if source == "ws":       # точное время (мс)
            alive = lambda o: (ts > o.placed_ts + lat_o and
                               (o.canceled_ts is None or ts <= o.canceled_ts + lat_c))
        else:                    # время до секунды: пессимистично в обе стороны
            alive = lambda o: (ts >= math.floor(o.placed_ts) + 1 and
                               (o.canceled_ts is None or ts <= o.canceled_ts))
        cands = [o for o in self.orders.values()
                 if o.slug == mk.slug and o.token == token and o.remaining > 1e-9 and alive(o)]
        cands.sort(key=lambda o: (-o.price, o.placed_ts))
        for o in cands:
            if avail <= 1e-9:
                break
            if price < o.price - 1e-9:
                got = min(o.remaining, avail)
            elif abs(price - o.price) < 1e-9:
                eat = min(o.queue_ahead, avail)
                o.queue_ahead -= eat
                rest = avail - eat
                got = min(o.remaining, rest)
                if got <= 1e-9:
                    continue
            else:
                continue
            avail -= got
            self._apply_fill(mk, o, got, ts, price)

    def _apply_fill(self, mk: Market, o: Order, shares: float, ts: float, trade_price: float):
        fee = self.cfg["maker_fee_coef"] * shares * o.price * (1 - o.price)
        # резерв этой заявки ещё не освобождён -> покупка покрывается резервом
        reserved_part = 0.0 if o.released else shares * o.price
        self.acct.fill(o.token, shares, o.price, fee, reserved_part)
        self.acct.rebates += (self.cfg["rebate_share"] * self.cfg["taker_fee_rate"]
                              * shares * o.price * (1 - o.price))
        o.filled += shares
        if o.first_fill_ts is None:
            o.first_fill_ts = self.api.now()
        p = self.pairs[o.pair_id]
        ou, od = self.orders[p.up], self.orders[p.down]
        late = " (заявка уже снималась — исполнили раньше снятия)" if o.canceled_ts else ""
        self.count("fills")
        if o.canceled_ts:
            self.count("late_fills")
        if p.state == "canceled":                 # сняли, но сделка успела раньше
            p.state = "quoting"
            if p.id not in mk.pairs:
                mk.pairs.append(p.id)
        if ou.filled >= ou.shares - 1e-9 and od.filled >= od.shares - 1e-9:
            if p.state != "complete":
                p.state = "complete"
                self.count("pairs_complete")
                locked = (1 - ou.price - od.price) * min(ou.filled, od.filled)
                self.info(f"ПАРА ✔ #{p.id} {mk.asset.upper()} Up {ou.price:.2f} + Down {od.price:.2f} "
                          f"= {ou.price + od.price:.2f} → зафиксировано +${locked:.2f}{late}")
        elif p.state == "quoting":
            try:
                p.feat["f"] = self.snapshot(mk)
                p.feat["f"]["leg"] = o.leg
                p.feat["f"]["fill_lag"] = round(self.api.now() - ts, 2)
            except Exception as e:  # noqa: BLE001
                log.debug(f"snapshot: {e}")
            p.state = "one_leg"
            self.count("first_legs")
            sec_left = mk.end_ts - self.api.now()
            self.info(f"НОГА  #{p.id} {mk.asset.upper()} {o.leg} {o.price:.2f} x{shares:g} "
                      f"(сделка {trade_price:.2f}), жду вторую | до конца {sec_left:.0f}с{late}")
        self.event(ev="fill", pair=p.id, leg=o.leg, price=o.price, shares=shares,
                   trade_price=trade_price, trade_ts=ts, canceled_before=bool(o.canceled_ts),
                   state=p.state)

    async def poll_trades(self, mk: Market, force: bool = False):
        if not mk.condition_id:
            return
        if not force and self.api.now() - self.ws_last_msg < 5:
            return                       # лента CLOB жива — data-api не нужен
        try:
            trades = await self.api.get_trades(mk.condition_id)
        except Exception as e:  # noqa: BLE001
            self.count("trades_error")
            log.debug(f"trades {mk.slug}: {e}")
            return
        for tr in sorted(trades, key=lambda d: d.get("timestamp", 0)):
            self.on_trade(mk, tr)

    # ------------------------------------------------------------ settlement
    async def resolve(self, mk: Market):
        now = self.api.now()
        if mk.resolved or now < mk.end_ts + 10 or now < self.resolve_next.get(mk.slug, 0):
            return
        self.resolve_next[mk.slug] = now + self.cfg["resolve_every_s"]
        try:
            m = await self.api.get_market(mk.slug, closed=True)
        except Exception as e:  # noqa: BLE001
            log.debug(f"resolve {mk.slug}: {e}")
            return
        pay = resolution_payouts(m) if m else None
        if not pay:
            log.debug(f"resolve {mk.slug}: ещё не рассчитан")
            return
        # все сделки до конца рынка уже должны быть учтены (секунды, покрытые лентой, пропускаются)
        await self.poll_trades(mk, force=True)
        mk.resolved = True
        mk.winner = "Up" if pay.get(mk.up_token) == 1 else "Down"
        for o in self.orders.values():
            if o.slug == mk.slug and not o.released:
                self.acct.release(o.remaining * o.price)
                o.released = True
        total = 0.0
        for tok in (mk.up_token, mk.down_token):
            total += self.acct.settle(tok, pay.get(tok, 0))
        rows = []
        mine = sorted(pid for pid, p in self.pairs.items() if p.slug == mk.slug)
        for pid in mine:
            p = self.pairs[pid]
            ou, od = self.orders[p.up], self.orders[p.down]
            matched = min(ou.filled, od.filled)
            pnl = matched * (1 - ou.price - od.price)
            extra_up, extra_dn = ou.filled - matched, od.filled - matched
            pnl += extra_up * (pay.get(mk.up_token, 0) - ou.price)
            pnl += extra_dn * (pay.get(mk.down_token, 0) - od.price)
            if ou.filled == 0 and od.filled == 0:
                outcome = "no_fill"
            elif extra_up < 1e-9 and extra_dn < 1e-9:
                outcome = "complete"
            else:
                leg = "Up" if extra_up > 0 else "Down"
                outcome = "one_leg_win" if leg == mk.winner else "one_leg_loss"
                if matched > 0:
                    outcome = "partial_" + outcome
            p.state, p.pnl, p.outcome = "settled", round(pnl, 6), outcome
            self.count(f"out_{outcome}")
            ffs = [x.first_fill_ts for x in (ou, od) if x.first_fill_ts]
            row = {"pair_id": p.id, "asset": p.asset, "slug": p.slug, "created_utc": utc(p.created_ts),
                   "fair_up": p.fair_up, "up_price": ou.price, "down_price": od.price,
                   "pair_cost": round(ou.price + od.price, 4), "up_filled": ou.filled,
                   "down_filled": od.filled, "outcome": outcome,
                   "close_reason": ou.cancel_reason or od.cancel_reason, "winner": mk.winner,
                   "pnl": round(pnl, 4),
                   "first_fill_s": round(min(ffs) - p.created_ts, 1) if ffs else "",
                   "second_fill_s": round(max(ffs) - p.created_ts, 1) if len(ffs) == 2 else "",
                   "chased": p.chased}
            rows.append(row)
            if outcome != "no_fill" or p.feat:
                try:
                    self._write_features(row, p)
                except OSError as e:
                    log.debug(f"features: {e}")
            self.done_pairs.append({"pair_id": p.id, "asset": p.asset, "outcome": outcome,
                                    "pnl": round(pnl, 6), "t": now,
                                    "pair_cost": round(ou.price + od.price, 4)})
        self._write_pairs(rows)
        self.count("markets_resolved")
        self.info(f"ИТОГ  {mk.asset.upper()} {utc(mk.start_ts)[11:16]} победил {mk.winner}: пар {len(rows)}, "
                  f"PnL {total:+.2f}$ | счёт ${self.acct.equity_at_cost():.2f}")
        self.event(ev="resolve", slug=mk.slug, winner=mk.winner, pnl=round(total, 4), pairs=rows)
        # чистим память
        for pid in mine:
            p = self.pairs.pop(pid, None)
            if p:
                self.orders.pop(p.up, None)
                self.orders.pop(p.down, None)
        for oid in [k for k, o in self.orders.items() if o.slug == mk.slug]:
            self.orders.pop(oid)
        self.trade_seen.pop(mk.slug, None)
        self.flow.pop(mk.slug, None)
        for tok in (mk.up_token, mk.down_token):
            self.tob.pop(tok, None)

    def _write_pairs(self, rows):
        if not rows:
            return
        path = self.out / "pairs.csv"
        new = not path.exists()
        with path.open("a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=PAIR_COLS)
            if new:
                w.writeheader()
            w.writerows(rows)

    def drop_old(self):
        for slug in [s for s, m in self.markets.items() if m.resolved]:
            del self.markets[slug]
            self.s0.pop(slug, None)

    # ------------------------------------------------------------ cycle
    async def step(self):
        self.sampler.tick(self.api.now())
        await self.discover()
        live = [m for m in self.markets.values() if not m.resolved]
        for mk in live:
            if self.api.now() <= mk.end_ts + 30:
                await self.poll_trades(mk)
        self._release_expired()
        for mk in live:
            await self.manage(mk)
            await self.quote(mk)
            await self.resolve(mk)
        self.drop_old()

    def check_moves(self):
        now = self.api.now()
        for asset, sym in self.cfg["assets"].items():
            bps = self.prices.move_bps(sym, now, self.cfg["move_window_s"])
            if bps >= self.cfg["move_threshold_bps"]:
                self.on_price_move(asset, bps)

    def status(self):
        a, c = self.acct, self.counters
        out = {k[4:]: v for k, v in c.items() if k.startswith("out_")}
        self.info(f"СТАТУС счёт ${a.equity_at_cost():.2f} (своб. ${a.cash:.2f}, под заявками "
                  f"${a.reserved:.2f}, в позициях ${sum(x[1] for x in a.positions.values()):.2f}) "
                  f"| PnL {a.realized:+.2f}$ | котировок {c.get('pairs_created', 0)}, полных пар "
                  f"{c.get('pairs_complete', 0)} | итоги: одна нога +{out.get('one_leg_win', 0)}/"
                  f"-{out.get('one_leg_loss', 0)}, рынков {c.get('markets_resolved', 0)} "
                  f"| рывков {c.get('moves', 0)} | цены: "
                  f"{','.join(self.prices.sources_alive(self.api.now())) or 'нет цен!'}"
                  f" | рывков по стакану {c.get('book_moves', 0)}"
                  f" (обрывов {c.get('binance_ws_reconnect', 0)}) | сделки: лента CLOB "
                  f"{c.get('trades_ws', 0)} (задержка {self._lag('ws')}, обрывов "
                  f"{c.get('clob_ws_reconnect', 0)}), data-api {c.get('trades_api', 0)} "
                  f"(задержка {self._lag('api')}) | поздних исполнений {c.get('late_fills', 0)}")

    def _lag(self, src: str) -> str:
        xs = self.trade_lag[src]
        return f"{statistics.median(xs):.1f}с" if xs else "-"


# ======================================================================= feeds

async def binance_ws_loop(eng: Engine, stop: asyncio.Event):
    syms = list(eng.cfg["assets"].values())
    url = eng.cfg["binance_ws"] + "?streams=" + "/".join(f"{s.lower()}@aggTrade" for s in syms)
    backoff = 1
    while not stop.is_set():
        ws = None
        try:
            ws = await WebSocket.connect(url)
            eng.info("Binance: поток подключён")
            backoff = 1
            while not stop.is_set():
                msg = json.loads(await asyncio.wait_for(ws.recv(), eng.cfg["ws_silence_s"]))
                d = msg.get("data", msg)
                if d.get("e") == "aggTrade":
                    # время приёма по своим часам: при расхождении часов сервера и биржи
                    # окно «рывка» иначе оказывается пустым и рывки не ловятся
                    eng.prices.add(d["s"], eng.api.now(), float(d["p"]), "binance_ws")
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            eng.count("binance_ws_reconnect")
            n = eng.counters["binance_ws_reconnect"]
            msg = f"Binance WS: обрыв #{n}: {type(e).__name__}: {e}"
            (eng.info if n in (1, 2, 3, 10) or n % 50 == 0 else log.debug)(msg)
        finally:
            if ws:
                await ws.close()
        try:
            await asyncio.wait_for(stop.wait(), backoff)
        except asyncio.TimeoutError:
            pass
        backoff = min(backoff * 2, 60)


def _live_tokens(eng: Engine) -> dict:
    out = {}
    for mk in eng.markets.values():
        if not mk.resolved and eng.api.now() <= mk.end_ts + 10:
            out[mk.up_token] = mk
            out[mk.down_token] = mk
    return out


async def clob_ws_loop(eng: Engine, stop: asyncio.Event):
    """Лента CLOB: сделки (last_trade_price) в реальном времени по токенам текущих рынков."""
    backoff = 1
    while not stop.is_set():
        tokens = _live_tokens(eng)
        if not tokens:
            try:
                await asyncio.wait_for(stop.wait(), 1)
            except asyncio.TimeoutError:
                pass
            continue
        ws, interval, pinger = None, None, None
        try:
            ws = await WebSocket.connect(eng.cfg["clob_ws"])
            await ws.send(json.dumps({"assets_ids": list(tokens), "type": "market"}))
            interval = [eng.api.now(), eng.api.now()]
            eng.ws_intervals.append(interval)
            eng.ws_intervals[:] = eng.ws_intervals[-200:]
            log.debug(f"CLOB WS: подписка на {len(tokens)} токенов")
            backoff = 1

            async def ping():
                while True:
                    await asyncio.sleep(10)
                    await ws.send("PING")
            pinger = asyncio.ensure_future(ping())
            while not stop.is_set():
                if set(_live_tokens(eng)) != set(tokens):
                    break                                   # новый рынок -> переподписка
                raw = await asyncio.wait_for(ws.recv(), eng.cfg["ws_silence_s"])
                now = eng.api.now()
                interval[1] = now
                eng.ws_last_msg = now
                if raw.strip().upper() == "PONG":
                    continue
                try:
                    data = json.loads(raw)
                except ValueError:
                    continue
                for ev in (data if isinstance(data, list) else [data]):
                    if not isinstance(ev, dict):
                        continue
                    et = ev.get("event_type")
                    if et in ("price_change", "book"):
                        eng.on_book_event(ev, tokens)
                        continue
                    if et != "last_trade_price":
                        continue
                    mk = tokens.get(str(ev.get("asset_id")))
                    if mk is None:
                        continue
                    eng.on_trade(mk, {"asset": ev.get("asset_id"), "price": ev.get("price"),
                                      "size": ev.get("size"), "timestamp": ev.get("timestamp"),
                                      "transactionHash": ev.get("transaction_hash", ""),
                                      "proxyWallet": ev.get("side", "")}, "ws")
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            eng.count("clob_ws_reconnect")
            n = eng.counters["clob_ws_reconnect"]
            msg = f"CLOB WS: обрыв #{n}: {type(e).__name__}: {e}"
            (eng.info if n in (1, 2, 3, 10) or n % 50 == 0 else log.debug)(msg)
        finally:
            if pinger:
                pinger.cancel()
            if ws:
                await ws.close()
        try:
            await asyncio.wait_for(stop.wait(), backoff if eng.counters.get("clob_ws_reconnect") else 0)
        except asyncio.TimeoutError:
            pass
        backoff = min(backoff * 2, 30)


RTDS_SYMBOL = {"btcusdt": "BTCUSDT", "ethusdt": "ETHUSDT", "solusdt": "SOLUSDT", "xrpusdt": "XRPUSDT",
               "dogeusdt": "DOGEUSDT", "bnbusdt": "BNBUSDT"}


def parse_rtds_price(msg: dict):
    """-> [(SYMBOL, price, ts_ms|None)] из сообщения RTDS crypto_prices."""
    if not isinstance(msg, dict) or msg.get("topic") != "crypto_prices":
        return []
    p = msg.get("payload") or {}
    sym = RTDS_SYMBOL.get(str(p.get("symbol", "")).lower())
    if not sym:
        return []
    out = []
    if "value" in p or "price" in p:
        out.append((sym, float(p.get("value", p.get("price"))), p.get("timestamp")))
    for pt in p.get("data") or []:                       # снимок при подписке
        if isinstance(pt, dict) and "value" in pt:
            out.append((sym, float(pt["value"]), pt.get("timestamp")))
    return out


async def rtds_price_loop(eng: Engine, stop: asyncio.Event):
    """Цены Binance через RTDS Polymarket: сервер Polymarket доступен даже там, где Binance режут."""
    wanted = {v: k for k, v in RTDS_SYMBOL.items()}
    syms = [wanted[s] for s in eng.cfg["assets"].values() if s in wanted]
    backoff = 1
    while not stop.is_set():
        ws, pinger = None, None
        try:
            ws = await WebSocket.connect(eng.cfg["rtds_url"])
            await ws.send(json.dumps({"action": "subscribe", "subscriptions": [
                {"topic": "crypto_prices", "type": "update", "filters": s} for s in syms]}))
            backoff = 1

            async def ping():
                while True:
                    await asyncio.sleep(5)
                    await ws.send("ping")
            pinger = asyncio.ensure_future(ping())
            got_any = False
            while not stop.is_set():
                raw = await asyncio.wait_for(ws.recv(), eng.cfg["ws_silence_s"])
                try:
                    msg = json.loads(raw)
                except ValueError:
                    continue
                pts = parse_rtds_price(msg)
                for sym, px, _ in pts[-1:]:                 # снимок: берём последнюю точку
                    eng.prices.add(sym, eng.api.now(), px, "rtds")
                if pts and not got_any:
                    got_any = True
                    eng.info("RTDS: цены идут")
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            eng.count("rtds_price_reconnect")
            n = eng.counters["rtds_price_reconnect"]
            msg = f"RTDS цены: обрыв #{n}: {type(e).__name__}: {e}"
            (eng.info if n in (1, 2, 3, 10) or n % 50 == 0 else log.debug)(msg)
        finally:
            if pinger:
                pinger.cancel()
            if ws:
                await ws.close()
        try:
            await asyncio.wait_for(stop.wait(), backoff)
        except asyncio.TimeoutError:
            pass
        backoff = min(backoff * 2, 30)


CB_PRODUCT = {"BTCUSDT": "BTC-USD", "ETHUSDT": "ETH-USD", "SOLUSDT": "SOL-USD", "XRPUSDT": "XRP-USD",
              "DOGEUSDT": "DOGE-USD"}
OKX_INST = {"BTCUSDT": "BTC-USDT", "ETHUSDT": "ETH-USDT", "SOLUSDT": "SOL-USDT", "XRPUSDT": "XRP-USDT",
            "DOGEUSDT": "DOGE-USDT", "BNBUSDT": "BNB-USDT"}


def parse_coinbase(msg: dict):
    if isinstance(msg, dict) and msg.get("type") == "ticker" and msg.get("price"):
        inv = {v: k for k, v in CB_PRODUCT.items()}
        sym = inv.get(msg.get("product_id"))
        if sym:
            return [(sym, float(msg["price"]))]
    return []


def parse_okx(msg: dict):
    out = []
    if isinstance(msg, dict) and (msg.get("arg") or {}).get("channel") == "tickers":
        inv = {v: k for k, v in OKX_INST.items()}
        for d in msg.get("data") or []:
            sym = inv.get(d.get("instId"))
            if sym and d.get("last"):
                out.append((sym, float(d["last"])))
    return out


async def generic_price_ws(eng: Engine, stop: asyncio.Event, name: str, url: str, sub: str,
                           parser, ping_text: str | None, ping_every: float):
    backoff = 1
    while not stop.is_set():
        ws, pinger = None, None
        try:
            ws = await WebSocket.connect(url)
            await ws.send(sub)
            backoff = 1
            if ping_text:
                async def ping():
                    while True:
                        await asyncio.sleep(ping_every)
                        await ws.send(ping_text)
                pinger = asyncio.ensure_future(ping())
            while not stop.is_set():
                raw = await asyncio.wait_for(ws.recv(), eng.cfg["ws_silence_s"])
                if raw in ("pong", "PONG"):
                    continue
                try:
                    msg = json.loads(raw)
                except ValueError:
                    continue
                for sym, px in parser(msg):
                    eng.prices.add(sym, eng.api.now(), px, name)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            key = f"{name}_reconnect"
            eng.count(key)
            n = eng.counters[key]
            m = f"{name}: обрыв #{n}: {type(e).__name__}: {e}"
            (eng.info if n in (1, 2, 3, 10) or n % 50 == 0 else log.debug)(m)
        finally:
            if pinger:
                pinger.cancel()
            if ws:
                await ws.close()
        try:
            await asyncio.wait_for(stop.wait(), backoff)
        except asyncio.TimeoutError:
            pass
        backoff = min(backoff * 2, 30)


def coinbase_loop(eng: Engine, stop: asyncio.Event):
    prods = [CB_PRODUCT[s] for s in eng.cfg["assets"].values() if s in CB_PRODUCT]
    sub = json.dumps({"type": "subscribe", "product_ids": prods, "channels": ["ticker"]})
    return generic_price_ws(eng, stop, "coinbase_ws", eng.cfg["coinbase_ws"], sub, parse_coinbase, None, 0)


def okx_loop(eng: Engine, stop: asyncio.Event):
    args = [{"channel": "tickers", "instId": OKX_INST[s]} for s in eng.cfg["assets"].values() if s in OKX_INST]
    sub = json.dumps({"op": "subscribe", "args": args})
    return generic_price_ws(eng, stop, "okx_ws", eng.cfg["okx_ws"], sub, parse_okx, "ping", 20)


async def binance_rest_loop(eng: Engine, stop: asyncio.Event):
    """Запасной вариант: опрос REST раз в секунду, если потоки молчат."""
    syms = list(eng.cfg["assets"].values())
    errors = 0
    while not stop.is_set():
        if eng.api.now() - eng.prices.last_update > eng.cfg["rest_fallback_after_s"]:
            try:
                px = await eng.api.get_prices(syms)
                now = eng.api.now()
                for s, p in px.items():
                    eng.prices.add(s, now, p, "binance_rest")
                errors = 0
            except Exception as e:  # noqa: BLE001
                errors += 1
                if errors in (1, 10) or errors % 100 == 0:
                    eng.info(f"Binance REST: ошибка #{errors}: {e}")
        try:
            await asyncio.wait_for(stop.wait(), 0.5)
        except asyncio.TimeoutError:
            pass


async def main_loop(eng: Engine, stop: asyncio.Event):
    last_status = 0.0
    while not stop.is_set():
        try:
            eng.check_moves()
            await eng.step()
        except Exception as e:  # noqa: BLE001
            log.exception(f"step error: {e}")
        if time.time() - last_status >= eng.cfg["status_every_s"]:
            eng.save()
            eng.status()
            last_status = time.time()
        try:
            await asyncio.wait_for(stop.wait(), eng.cfg["loop_s"])
        except asyncio.TimeoutError:
            pass


async def move_watch(eng: Engine, stop: asyncio.Event):
    """Чаще основного цикла: реагировать на рывок за доли секунды."""
    while not stop.is_set():
        eng.check_moves()
        try:
            await asyncio.wait_for(stop.wait(), eng.cfg["move_watch_s"])
        except asyncio.TimeoutError:
            pass


async def run(cfg: dict, out: Path):
    setup_logging(out, cfg)
    eng = Engine(cfg, Api(cfg), out)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, stop.set)
        except NotImplementedError:
            pass
    eng.info(f"Старт маркет-мейкер-симулятора [справедливая цена: "
             f"{ {'book': 'стакан', 'guard': 'стакан + охрана по модели ' + cfg['rule']}.get(cfg['fair_mode'], 'модель ' + cfg['rule'])}]: "
             f"{', '.join(a.upper() for a in cfg['assets'])}, "
             f"{cfg['shares']} акций на ногу, пара = 1 − {cfg['edge']}, счёт ${eng.acct.cash:.2f}")
    log.debug("Настройки: " + json.dumps(cfg, ensure_ascii=False))
    loops = [main_loop(eng, stop), move_watch(eng, stop), binance_rest_loop(eng, stop)]
    if cfg["use_binance_ws"]:
        loops.append(binance_ws_loop(eng, stop))
    if cfg["use_rtds_prices"]:
        loops.append(rtds_price_loop(eng, stop))
    if cfg["use_coinbase_ws"]:
        loops.append(coinbase_loop(eng, stop))
    if cfg["use_okx_ws"]:
        loops.append(okx_loop(eng, stop))
    if cfg["use_clob_ws"]:
        loops.append(clob_ws_loop(eng, stop))
    tasks = [asyncio.ensure_future(x) for x in loops]
    await stop.wait()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    eng.save()
    eng.info("Остановлено, состояние сохранено. Отчёт: python3 mmsim.py report")


# ======================================================================= report

def exam(d: dict, start_cash: float | None = None) -> str:
    """Экзамен перед реальными деньгами: каждый пункт ✅ или ❌ (планка из плана)."""
    a = Account.from_dict(d["account"])
    c = d.get("counters", {})
    cash0 = start_cash or a.start_cash
    done = sorted((x for x in d.get("done_pairs", []) if x["outcome"] != "no_fill"), key=lambda x: x.get("t", 0))
    hours = max((d["saved"] - d["started"]) / 3600, 0)
    days = defaultdict(float)
    for x in done:
        days[utc(x.get("t", 0))[:10]] += x["pnl"]
    full_days = sorted(days)[1:-1] if len(days) > 2 else []      # первый и последний день неполные
    pnl = sum(x["pnl"] for x in done)
    top5 = sum(sorted((x["pnl"] for x in done), reverse=True)[:5])
    peak = eq = dd = 0.0
    for x in done:
        eq += x["pnl"]
        peak = max(peak, eq)
        dd = min(dd, eq - peak)
    win = sum(1 for x in done if x["outcome"].endswith("one_leg_win"))
    loss = sum(1 for x in done if x["outcome"].endswith("one_leg_loss"))
    by_asset = defaultdict(float)
    for x in done:
        by_asset[x["asset"]] += x["pnl"]
    checks = [
        ("7 дней подряд", hours >= 7 * 24, f"{hours / 24:.1f} дн"),
        ("≥300 окон", c.get("markets_resolved", 0) >= 300, f"{c.get('markets_resolved', 0)}"),
        ("≥1000 исполнений", c.get("fills", 0) >= 1000, f"{c.get('fills', 0)}"),
        ("итог ≥ +5% без ребейтов", pnl >= 0.05 * cash0, f"{pnl:+.2f}$ ({pnl / cash0 * 100:+.1f}%)"),
        ("плюс ≥5 из 7 полных дней", len(full_days) >= 7 and sum(days[k] > 0 for k in full_days[-7:]) >= 5,
         f"{sum(days[k] > 0 for k in full_days[-7:])} из {min(len(full_days), 7)}"),
        ("плюс без 5 лучших пар", pnl - top5 > 0, f"{pnl - top5:+.2f}$"),
        ("одна нога: проигрышей ≤1.3× выигрышей", win > 0 and loss <= 1.3 * win, f"{win} выигр. / {loss} проигр."),
        ("плюс и на BTC, и на ETH", len(by_asset) >= 2 and all(v > 0 for v in by_asset.values()),
         ", ".join(f"{k.upper()} {v:+.2f}$" for k, v in sorted(by_asset.items())) or "-"),
        ("просадка не глубже −15%", dd >= -0.15 * cash0, f"{dd:+.2f}$ ({dd / cash0 * 100:+.1f}%)"),
    ]
    ok = sum(1 for _, good, _ in checks if good)
    L = [f"=== ЭКЗАМЕН ПЕРЕД РЕАЛЬНЫМИ ДЕНЬГАМИ: {ok}/{len(checks)} ==="]
    for name, good, val in checks:
        L.append(f"  {'✅' if good else '❌'} {name:<38} {val}")
    L.append("  (+ вручную: 48 ч без падений — колонка ↺ в pm2 не растёт)")
    L.append("  Реальные деньги — только при 9/9. " + ("ВСЁ ✅ — можно обсуждать живой тест на $30–50."
                                                     if ok == len(checks) else "Пока рано."))
    return "\n".join(L)


def report(out: Path) -> str:
    sp = out / "state.json"
    if not sp.exists():
        return f"Нет данных в {out}. Сначала: python3 mmsim.py run"
    d = json.loads(sp.read_text())
    a = Account.from_dict(d["account"])
    c = d.get("counters", {})
    done = [x for x in d.get("done_pairs", []) if x["outcome"] != "no_fill"]
    hours = max((d["saved"] - d["started"]) / 3600, 1e-9)
    L = [f"=== Маркет-мейкер (симуляция), {hours:.1f} ч ===",
         f"Счёт: ${a.start_cash:.2f} -> ${a.equity_at_cost():.2f}  (реализованный PnL {a.realized:+.2f}$, "
         f"{a.realized / a.start_cash * 100:+.1f}%)",
         f"Рынков рассчитано: {c.get('markets_resolved', 0)}", ""]
    for asset in ["все"] + sorted({x["asset"] for x in done}):
        xs = done if asset == "все" else [x for x in done if x["asset"] == asset]
        if not xs and asset != "все":
            continue
        by = {}
        for x in xs:
            k = x["outcome"].replace("partial_", "")
            by.setdefault(k, []).append(x["pnl"])
        n = len(xs)
        comp = by.get("complete", [])
        win, loss = by.get("one_leg_win", []), by.get("one_leg_loss", [])
        one = len(win) + len(loss)
        L.append(f"--- {asset.upper()} ---")
        L.append(f"Пар с исполнением: {n}")
        if n:
            L.append(f"  полных пар (обе ноги): {len(comp)} ({len(comp) / n * 100:.0f}%), заработали "
                     f"{sum(comp):+.2f}$ (в среднем {statistics.mean(comp) if comp else 0:+.3f}$)")
            L.append(f"  одна нога: {one} ({one / n * 100:.0f}%) → повезло {len(win)} ({sum(win):+.2f}$), "
                     f"не повезло {len(loss)} ({sum(loss):+.2f}$)")
            L.append(f"  итого: {sum(x['pnl'] for x in xs):+.2f}$")
        L.append("")
    L.append(f"Котировок выставлено: {c.get('pairs_created', 0)}, переставлено {c.get('requoted', 0)}, "
             f"снято на рывке биржи {c.get('canceled_on_move', 0)} (рывков {c.get('moves', 0)}), "
             f"на рывке стакана {c.get('canceled_on_book_move', 0)} (рывков {c.get('book_moves', 0)}), "
             f"догоняли {c.get('chased', 0)}")
    skips = {k[5:]: v for k, v in c.items() if k.startswith("skip_")}
    if skips:
        L.append("Не котировали: " + ", ".join(f"{k} {v}" for k, v in skips.items()))
    L.append("")
    comp_n = sum(1 for x in done if x["outcome"].endswith("complete"))
    one_n = len(done) - comp_n
    if done:
        L.append(f"ГЛАВНОЕ ЧИСЛО: одна нога в {one_n / len(done) * 100:.0f}% пар с исполнением. "
                 f"При паре 0.96 стратегия в плюсе, только если убытки «одной ноги» меньше "
                 f"заработка полных пар — смотрите строку «итого».")
    L.append(f"Ребейты мейкера (оценка, в счёт не входят): +${a.rebates:.2f}")
    L += ["", exam(d)]
    if hours >= 12 and c.get("markets_resolved", 0) >= 50:
        L.append(f"Прогноз на 30 дней: {a.realized / hours * 24 * 30:+.2f}$ (грубая экстраполяция)")
    else:
        L.append(f"Прогноз: нужно ≥12 ч и ≥50 рынков (сейчас {hours:.1f} ч, {c.get('markets_resolved', 0)})")
    return "\n".join(L)


# ======================================================================= check

async def check(cfg: dict) -> bool:
    api = Api(cfg)
    ok = True
    now = time.time()
    start = int(now // 300 * 300)
    print("Проверка:")
    for asset in cfg["assets"]:
        slug = f"{asset}-updown-5m-{start}"
        try:
            m = await api.get_market(slug)
            toks = _as_list(m.get("clobTokenIds")) if m else []
            print(f"  OK   рынок {slug}: {'найден' if m else 'НЕ найден'}, токенов {len(toks)}")
            if m:
                b = await api.get_book(str(toks[0]))
                bb, _, _ = best(b, "bids")
                ba, _, _ = best(b, "asks")
                print(f"  OK   стакан Up: bid {bb} / ask {ba}")
                tr = await api.get_trades(str(m.get("conditionId")))
                print(f"  OK   сделок в рынке: {len(tr)}")
            else:
                ok = False
        except Exception as e:  # noqa: BLE001
            ok = False
            print(f"  FAIL {slug}: {e}")
    try:
        px = await api.get_prices(list(cfg["assets"].values()))
        print(f"  OK   Binance REST: {px}")
    except Exception as e:  # noqa: BLE001
        ok = False
        print(f"  FAIL Binance REST: {e}")
    syms = list(cfg["assets"].values())
    url = cfg["binance_ws"] + "?streams=" + "/".join(f"{s.lower()}@aggTrade" for s in syms)
    try:
        ws = await WebSocket.connect(url)
        n, end = 0, time.time() + 5
        while time.time() < end:
            try:
                await asyncio.wait_for(ws.recv(), end - time.time())
                n += 1
            except asyncio.TimeoutError:
                break
        await ws.close()
        print(f"  {'OK  ' if n else 'WARN'} Binance WS: {n} сообщений за 5 с"
              + ("" if n else " (бот будет работать через REST)"))
    except Exception as e:  # noqa: BLE001
        print(f"  WARN Binance WS: {e} (бот будет работать через REST)")
    print("Итог:", "можно запускать" if ok else "есть проблемы, см. FAIL")
    return ok


async def diag(cfg: dict, seconds: int = 60):
    """Минутная диагностика сети: идут ли потоки без «замираний»."""
    api = Api(cfg)
    now = time.time()
    start = int(now // 300 * 300)
    toks = []
    for asset in cfg["assets"]:
        m = await api.get_market(f"{asset}-updown-5m-{start}")
        if m:
            toks += [str(t) for t in _as_list(m.get("clobTokenIds"))]
    syms = list(cfg["assets"].values())
    burl = cfg["binance_ws"] + "?streams=" + "/".join(f"{x.lower()}@aggTrade" for x in syms)
    buckets = {"binance_ws": [0] * (seconds // 10), "coinbase_ws": [0] * (seconds // 10),
               "okx_ws": [0] * (seconds // 10), "rtds_prices": [0] * (seconds // 10),
               "clob_ws": [0] * (seconds // 10), "clob_trades": [0] * (seconds // 10)}
    t0 = time.time()
    errors = []

    async def reader(name, url, sub):
        try:
            ws = await WebSocket.connect(url)
            if sub:
                await ws.send(sub)
            while time.time() - t0 < seconds:
                try:
                    raw = await asyncio.wait_for(ws.recv(), seconds - (time.time() - t0))
                except asyncio.TimeoutError:
                    break
                i = min(int((time.time() - t0) // 10), len(buckets[name]) - 1)
                if name == "rtds_prices":            # считаем только сообщения с ценой
                    try:
                        if not parse_rtds_price(json.loads(raw)):
                            continue
                    except ValueError:
                        continue
                buckets[name][i] += 1
                if name == "clob_ws" and "last_trade_price" in raw:
                    buckets["clob_trades"][i] += raw.count("last_trade_price")
            await ws.close()
        except Exception as e:  # noqa: BLE001
            errors.append(f"{name}: {type(e).__name__}: {e} на {time.time() - t0:.0f}с")

    async def rest():
        out = []
        while time.time() - t0 < seconds:
            a = time.perf_counter()
            try:
                await api.get_prices(syms)
                out.append(f"{(time.perf_counter() - a) * 1000:.0f}")
            except Exception as e:  # noqa: BLE001
                out.append(f"ERR({type(e).__name__})")
            await asyncio.sleep(5)
        return out

    print(f"Диагностика {seconds} с (сообщений за каждые 10 с)...")
    wanted = {v: k for k, v in RTDS_SYMBOL.items()}
    rsub = json.dumps({"action": "subscribe", "subscriptions": [
        {"topic": "crypto_prices", "type": "update", "filters": wanted[x]} for x in syms if x in wanted]})
    cb_sub = json.dumps({"type": "subscribe", "channels": ["ticker"],
                         "product_ids": [CB_PRODUCT[x] for x in syms if x in CB_PRODUCT]})
    okx_sub = json.dumps({"op": "subscribe",
                          "args": [{"channel": "tickers", "instId": OKX_INST[x]} for x in syms if x in OKX_INST]})
    res = await asyncio.gather(
        reader("binance_ws", burl, None),
        reader("coinbase_ws", cfg["coinbase_ws"], cb_sub),
        reader("okx_ws", cfg["okx_ws"], okx_sub),
        reader("rtds_prices", cfg["rtds_url"], rsub),
        reader("clob_ws", cfg["clob_ws"], json.dumps({"assets_ids": toks, "type": "market"}) if toks else None),
        rest())
    for k, v in buckets.items():
        print(f"  {k:<12} {v}")
    print(f"  binance REST, мс: {res[5]}")
    for e in errors:
        print(f"  ОШИБКА {e}")
    frozen = [k for k, v in buckets.items() if k != "clob_trades" and v and v[0] > 0 and 0 in v[1:]]
    print("Итог:", "потоки замирают: " + ", ".join(frozen) + " — похоже на блокировку/фильтрацию сети "
          "на стороне сервера" if frozen else "потоки идут без замираний")


# ======================================================================= learn

def _load_features(outs: list[Path]) -> list[dict]:
    rows = []
    for o in outs:
        path = o / "features.jsonl"
        if not path.exists():
            continue
        for line in path.open():
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def _bucket_table(title: str, items: list[tuple], edges: list[float], fmt: str = "{:+g}") -> list[str]:
    """items: (значение, выиграла_ли_нога, pnl). Таблица по корзинам значения."""
    L = [f"  {title}:"]
    xs = [it for it in items if it[0] is not None]
    if not xs:
        return L + ["    (нет данных)"]
    bounds = [-math.inf] + edges + [math.inf]
    for lo, hi in zip(bounds, bounds[1:]):
        b = [it for it in xs if lo <= it[0] < hi]
        if not b:
            continue
        w = sum(1 for it in b if it[1])
        pnl = sum(it[2] for it in b)
        name = (f"< {fmt.format(hi)}" if lo == -math.inf else
                f">= {fmt.format(lo)}" if hi == math.inf else f"{fmt.format(lo)}..{fmt.format(hi)}")
        L.append(f"    {name:<16} ног {len(b):4d} | выигр. {w / len(b) * 100:3.0f}% | PnL {pnl:+8.2f}$")
    return L


def learn_report(outs: list[Path]) -> str:
    """Разбор одиноких ног: при каких условиях рынка они проигрывают."""
    rows = _load_features(outs)
    if not rows:
        return "Нет features.jsonl — данные появятся после первых рассчитанных рынков с новой версией."
    legs = [r for r in rows if "one_leg" in r.get("outcome", "") and isinstance(r.get("f"), dict)]
    comp = [r for r in rows if r.get("outcome") == "complete"]
    nof = [r for r in rows if r.get("outcome") == "no_fill"]
    L = [f"=== Разбор рынка: пар {len(rows)}, полных {len(comp)}, одиноких ног {len(legs)}, "
         f"без исполнения {len(nof)} ==="]
    if not legs:
        return "\n".join(L + ["Одиноких ног с данными пока нет."])
    w = sum(1 for r in legs if "win" in r["outcome"])
    L.append(f"Одинокие ноги: выиграли {w} из {len(legs)} ({w / len(legs) * 100:.0f}%), "
             f"PnL {sum(r['pnl'] for r in legs):+.2f}$")
    L.append("«За ногу» = цена на бирже шла в сторону исполненной ноги (Up и рост, Down и падение).")

    def it(key, src="f", orient=True):
        out = []
        for r in legs:
            f = r.get(src) or {}
            v = f.get(key)
            if v is not None and orient:
                leg = (r.get("f") or {}).get("leg", "Up")
                v = v if leg == "Up" else -v
            out.append((v, "win" in r["outcome"], r["pnl"]))
        return out

    L.append("")
    L.append("1) Движение биржи в момент исполнения (б.п., «+» = за ногу, «−» = против):")
    for k in (2, 5, 30):
        L += _bucket_table(f"за {k}с до исполнения", it(f"r{k}"), [-3, -1, 0, 1, 3])
    L.append("")
    L.append("2) Движение биржи в момент котировки:")
    for k in (5, 30):
        L += _bucket_table(f"за {k}с до котировки", it(f"r{k}", "q"), [-3, -1, 0, 1, 3])
    L.append("")
    L.append("3) Модель против стакана при исполнении (модель P(нога) − цена ноги):")
    edge = []
    for r in legs:
        f = r.get("f") or {}
        m, leg = f.get("model"), f.get("leg", "Up")
        price = r["up_price"] if leg == "Up" else r["down_price"]
        if m is not None:
            pm = m if leg == "Up" else 1 - m
            edge.append((pm - price, "win" in r["outcome"], r["pnl"]))
        else:
            edge.append((None, False, 0))
    L += _bucket_table("модель − цена", edge, [-0.1, -0.03, 0, 0.03, 0.1])
    L.append("")
    L.append("4) Когда в окне случилось исполнение:")
    L += _bucket_table("секунда окна", it("sec", orient=False), [60, 120, 180, 240], "{:g}")
    L.append("")
    L.append("5) Волатильность (σ за секунду, б.п.):")
    L += _bucket_table("σ", it("sig_bps", "q", orient=False), [0.5, 1, 1.5, 2.5], "{:g}")
    L.append("")
    L.append("6) Цена исполненной ноги:")
    pr = []
    for r in legs:
        leg = (r.get("f") or {}).get("leg", "Up")
        pr.append((r["up_price"] if leg == "Up" else r["down_price"], "win" in r["outcome"], r["pnl"]))
    L += _bucket_table("цена", pr, [0.3, 0.4, 0.5, 0.6], "{:g}")
    L.append("")
    L.append("7) По монетам:")
    for a in sorted({r["asset"] for r in legs}):
        b = [r for r in legs if r["asset"] == a]
        ww = sum(1 for r in b if "win" in r["outcome"])
        cc = [r for r in comp if r["asset"] == a]
        L.append(f"    {a.upper():<5} ног {len(b):4d} | выигр. {ww / len(b) * 100:3.0f}% | PnL ног "
                 f"{sum(r['pnl'] for r in b):+8.2f}$ | полных пар {len(cc)} (+{sum(r['pnl'] for r in cc):.2f}$)")
    if len(legs) < 200:
        L.append("")
        L.append(f"⚠️ Ног пока {len(legs)} — выводы ненадёжны, нужно хотя бы 200–300.")
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description="Симулятор маркет-мейкера Polymarket 5m BTC/ETH")
    ap.add_argument("cmd", choices=["run", "report", "check", "diag", "learn"])
    ap.add_argument("--config", default=str(HERE / "mm_config.json"))
    ap.add_argument("--out", default=str(HERE / "data_mm"),
                    help="папка данных; для learn можно несколько через запятую")
    a = ap.parse_args()
    cfg = load_config(Path(a.config))
    if a.cmd == "run":
        asyncio.run(run(cfg, Path(a.out)))
    elif a.cmd == "report":
        print(report(Path(a.out)))
    elif a.cmd == "learn":
        print(learn_report([Path(x) for x in a.out.split(",")]))
    elif a.cmd == "diag":
        asyncio.run(diag(cfg))
    else:
        sys.exit(0 if asyncio.run(check(cfg)) else 1)


if __name__ == "__main__":
    main()
