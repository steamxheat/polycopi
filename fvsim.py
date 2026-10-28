#!/usr/bin/env python3
"""Симулятор стратегии «справедливой цены» для крипто-рынков Up/Down (5 и 15 минут).

Идея (так, судя по всему, зарабатывают крипто-боты): исход окна — это вопрос
«будет ли цена в конце выше стартовой». Зная старт S0, текущую цену S, оставшееся
время τ и типичную волатильность σ, можно посчитать честную вероятность:

    P(Up) = Φ( ln(S/S0) / (σ·√τ) )

Если Polymarket продаёт исход заметно дешевле этой вероятности (коэффициенты
отстают от биржи), бот покупает. Мы НЕ копируем чужие сделки, а сами видим сигнал.

Что честно учтено:
  * цена биржи — медиана по нескольким биржам (Coinbase, OKX, Binance), каждая
    сравнивается со СВОЕЙ стартовой ценой (разница между биржами не даёт ложных сигналов);
  * вход — тейкером по РЕАЛЬНОМУ стакану, запрошенному ПОСЛЕ решения (наша сетевая
    задержка попадает в цену), с лимитом «увиденная цена + допуск»;
  * комиссия тейкера 0.07·p·(1−p) за акцию;
  * несколько вариантов порога входа работают параллельно, у каждого свой счёт $100;
  * каждую секунду пишется снимок рынка (модель vs стакан) — датасет для обучения.

  python3 fvsim.py run | report | check
Данные: data_fv/ (fv.log, state.json, trades.csv, snapshots.csv, outcomes.csv)
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
from pathlib import Path

import fvmodel
import mmsim
from core import resolution_payouts, walk_book, _as_list
from mmsim import Api, PriceFeed, best, utc
from net import WebSocket, fast_loads

HERE = Path(__file__).resolve().parent
log = mmsim.log            # общие петли цен пишут в этот логгер

DEFAULTS = {
    "assets": {"btc": "BTCUSDT", "eth": "ETHUSDT"},
    "windows": {"5m": 300, "15m": 900},
    "start_cash": 100.0,
    "stake_usd": 2.0,
    "variants": [
        {"name": "edge2", "min_edge": 0.02},
        {"name": "edge4", "min_edge": 0.04},
        {"name": "edge7", "min_edge": 0.07},
        {"name": "edge10", "min_edge": 0.10},
        {"name": "edge4-late", "min_edge": 0.04, "max_tau": 90},
        {"name": "edge4-early", "min_edge": 0.04, "min_tau": 90},
        # те же пороги, но вероятность по правилам TWAP (см. fvmodel.py)
        {"name": "t60-edge4", "min_edge": 0.04, "rule": "t60"},
        {"name": "t60-edge7", "min_edge": 0.07, "rule": "t60"},
        {"name": "wt-edge4", "min_edge": 0.04, "rule": "wt"},
        {"name": "wt-edge7", "min_edge": 0.07, "rule": "wt"},
        # t60 подтверждён как настоящее правило (98-99% совпадений) — смотрим, где у него перевес
        {"name": "t60-edge10", "min_edge": 0.10, "rule": "t60"},
        {"name": "t60-early", "min_edge": 0.05, "rule": "t60", "min_tau": 90},
        {"name": "t60-late", "min_edge": 0.05, "rule": "t60", "max_tau": 90},
        # разбор goodfallen/almach: перевес у фаворитов (рынок недооценивает исход с P≥0.7 по t60),
        # длинные шансы (дешёвые исходы) системно переоценены — их не покупаем
        {"name": "fav70", "min_edge": 0.03, "rule": "t60", "min_p": 0.70},
        {"name": "fav70-late", "min_edge": 0.03, "rule": "t60", "min_p": 0.70, "max_tau": 120},
        {"name": "fav85", "min_edge": 0.03, "rule": "t60", "min_p": 0.85},
    ],
    "min_tau": 8,
    "min_price": 0.05,
    "max_price": 0.95,
    "max_entries_per_side": 2,
    "entry_gap_s": 15,
    "fill_tolerance": 0.01,       # готовы взять до «увиденная цена + 1¢»
    "taker_fee_coef": 0.07,
    "sigma_window_s": 900,
    "sigma_floor": 2e-5,          # минимум σ за секунду (≈0.002%) — защита от тихих минут
    "min_sources": 1,
    "loop_s": 0.25,
    "snapshot_every_s": 1.0,
    "status_every_s": 60,
    "resolve_every_s": 15,
    "ws_silence_s": 10,
    "rest_fallback_after_s": 2,
    "use_binance_ws": True, "use_coinbase_ws": True, "use_okx_ws": True,
    "log_max_mb": 20, "log_backups": 10,
    "clob_url": "https://clob.polymarket.com",
    "gamma_url": "https://gamma-api.polymarket.com",
    "clob_ws": "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    "binance_ws": "wss://data-stream.binance.vision/stream",
    "binance_rest": "https://data-api.binance.vision",
    "coinbase_ws": "wss://ws-feed.exchange.coinbase.com",
    "okx_ws": "wss://ws.okx.com:8443/ws/v5/public",
    "use_bybit_perp_ws": False, "use_okx_perp_ws": False,
    "bybit_perp_ws": "wss://stream.bybit.com/v5/public/linear",
    # по каким источникам ловим «рывок» для запаздывающих вариантов (None = все) и сколько должны согласиться
    "lead_sources": None,
    "lead_agree": 1,
}


def phi(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def p_up(ratio: float, sigma_1s: float, tau: float) -> float:
    """ratio = S/S0. Броуновское движение без сноса."""
    if ratio <= 0:
        return 0.5
    if tau <= 0:
        return 1.0 if ratio > 1 else 0.0
    return phi(math.log(ratio) / (sigma_1s * math.sqrt(tau)))


def fee(p: float, coef: float) -> float:
    return coef * p * (1 - p)


def load_config(path: Path) -> dict:
    cfg = json.loads(json.dumps(DEFAULTS))
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
    fh = logging.handlers.RotatingFileHandler(out / "fv.log", maxBytes=int(cfg["log_max_mb"] * 2**20),
                                              backupCount=int(cfg["log_backups"]), encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s.%(msecs)03d %(levelname)-5s %(message)s",
                                      "%Y-%m-%d %H:%M:%S"))
    fh.formatter.converter = time.gmtime
    log.addHandler(fh)


# ======================================================================= цены

Sampler = fvmodel.Sampler


# ======================================================================= модель данных

class Mkt:
    def __init__(self, slug, asset, window, start, end, up, down, cond=""):
        self.slug, self.asset, self.window = slug, asset, window
        self.start, self.end, self.up, self.down, self.cond = start, end, up, down, cond
        self.s0: dict[str, float] = {}       # источник -> стартовая цена
        self.resolved = False
        self.winner = ""

    def to_dict(self):
        return dict(self.__dict__)

    @classmethod
    def from_dict(cls, d):
        m = cls(d["slug"], d["asset"], d["window"], d["start"], d["end"], d["up"], d["down"], d.get("cond", ""))
        m.s0, m.resolved, m.winner = d.get("s0", {}), d.get("resolved", False), d.get("winner", "")
        return m


class Book:
    """Лучшие цены по токенам из ленты CLOB."""

    def __init__(self):
        self.bid: dict[str, float] = {}
        self.ask: dict[str, float] = {}
        self.t: dict[str, float] = {}
        self.ask_t: dict[str, float] = {}       # когда лучший аск последний раз менялся
        self.ask_h: dict[str, deque] = {}       # история аска (время, цена) за последние секунды

    def on_event(self, ev: dict, now: float):
        et = ev.get("event_type")
        if et == "book":
            tok = str(ev.get("asset_id"))
            b, _, _ = best(ev, "bids")
            a, _, _ = best(ev, "asks")
            self._set(tok, b, a, now)
        elif et == "price_change":
            items = ev.get("price_changes") if isinstance(ev.get("price_changes"), list) else [ev]
            for it in items:
                tok = str(it.get("asset_id") or ev.get("asset_id") or "")
                try:
                    b = float(it["best_bid"]) if it.get("best_bid") not in (None, "") else None
                    a = float(it["best_ask"]) if it.get("best_ask") not in (None, "") else None
                except (TypeError, ValueError):
                    continue
                self._set(tok, b, a, now)

    def _set(self, tok, b, a, now):
        if not tok:
            return
        if b is not None and b > 0:
            self.bid[tok] = b
        if a is not None and 0 < a < 1:
            if self.ask.get(tok) != a:
                self.ask_t[tok] = now
                h = self.ask_h.setdefault(tok, deque())
                h.append((now, a))
                while h and now - h[0][0] > 10:
                    h.popleft()
            self.ask[tok] = a

    def ask_at(self, tok: str, t: float):
        """Аск, который стоял в момент t (None, если не знаем)."""
        best_ = None
        for ts, a in self.ask_h.get(tok, ()):
            if ts <= t:
                best_ = a
            else:
                break
        return best_
        self.t[tok] = now


class Variant:
    def __init__(self, spec: dict, cash: float):
        self.name = spec["name"]
        self.spec = spec
        self.cash = cash
        self.start_cash = cash
        self.pos: dict[str, list] = {}         # token -> [shares, cost]
        self.entries: dict[str, list] = {}     # token -> [время входов]
        self.realized = 0.0
        self.trades: list[dict] = []           # рассчитанные сделки (для отчёта)

    def to_dict(self):
        return {"name": self.name, "spec": self.spec, "cash": self.cash, "start_cash": self.start_cash,
                "pos": self.pos, "entries": self.entries, "realized": self.realized,
                "trades": self.trades[-20000:]}

    @classmethod
    def from_dict(cls, d):
        v = cls(d["spec"], d["start_cash"])
        v.cash, v.pos, v.entries = d["cash"], d["pos"], d["entries"]
        v.realized, v.trades = d["realized"], d.get("trades", [])
        return v

    def equity_at_cost(self):
        return self.cash + sum(c for _, c in self.pos.values())


# ======================================================================= движок

TRADE_COLS = ["time_utc", "variant", "rule", "asset", "window", "slug", "side", "tau", "ratio_bps", "sigma",
              "p_model", "seen_ask", "fill_vwap", "shares", "cost", "fee", "edge_seen", "edge_fill",
              "book_ms", "sources"]
SNAP_COLS = ["t", "asset", "window", "slug", "tau", "ratio_bps", "sigma", "p_spot", "p_t60", "p_wt",
             "up_bid", "up_ask", "down_bid", "down_ask", "sources"]
OUT_COLS = ["slug", "asset", "window", "start", "winner", "d_spot_bps", "d_t60_bps", "d_wt_bps"]


class Engine:
    def __init__(self, cfg: dict, api, out: Path, quiet: bool = False):
        self.cfg, self.api, self.out, self.quiet = cfg, api, out, quiet
        out.mkdir(parents=True, exist_ok=True)
        self.prices = PriceFeed(list(cfg["assets"].values()))
        self.sampler = Sampler(self.prices)
        # «запаздывающие» варианты решают сразу на каждой новой цене, не дожидаясь цикла
        lag = [v for v in cfg["variants"] if "move_bps" in v]
        if lag:
            self._trig_bps = min(v["move_bps"] for v in lag)
            self._trig_s = max(v.get("move_s", 1.0) for v in lag)
            self._last_trig = 0.0
            self.prices.on_add = self._on_price
        self.book = Book()
        self.markets: dict[str, Mkt] = {}
        self.variants = {v["name"]: Variant(v, cfg["start_cash"]) for v in cfg["variants"]}
        self.counters: dict[str, int] = {}
        self.started = api.now()
        self.ws_last = 0.0
        self.pending: set = set()
        self.last_snap = 0.0
        self.resolve_next: dict[str, float] = {}
        self._csv_checked: set = set()
        self._load()

    # ---- служебное
    def info(self, m):
        (log.debug if self.quiet else log.info)(m)

    def count(self, k, n=1):
        self.counters[k] = self.counters.get(k, 0) + n

    def _csv(self, name, cols, row):
        p = self.out / name
        if name not in self._csv_checked:
            self._csv_checked.add(name)
            if p.exists():
                with p.open() as f:
                    head = f.readline().strip().split(",")
                if head != cols:                     # формат поменялся — старый файл в сторону
                    os.replace(p, p.with_name(p.stem + f"_old_{int(self.api.now())}" + p.suffix))
        new = not p.exists()
        with p.open("a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            if new:
                w.writeheader()
            w.writerow({c: row.get(c, "") for c in cols})

    def _load(self):
        sp = self.out / "state.json"
        if not sp.exists():
            return
        d = json.loads(sp.read_text())
        self.started = d.get("started", self.started)
        self.counters = d.get("counters", {})
        for k, v in d.get("variants", {}).items():
            if k in self.variants:
                self.variants[k] = Variant.from_dict(v)
        for k, m in d.get("markets", {}).items():
            self.markets[k] = Mkt.from_dict(m)

    def save(self):
        d = {"started": self.started, "saved": self.api.now(), "config": self.cfg,
             "counters": self.counters,
             "variants": {k: v.to_dict() for k, v in self.variants.items()},
             "markets": {k: m.to_dict() for k, m in self.markets.items() if not m.resolved}}
        tmp = self.out / "state.tmp"
        tmp.write_text(json.dumps(d))
        os.replace(tmp, self.out / "state.json")

    # ---- рынки
    async def discover(self):
        now = self.api.now()
        for asset in self.cfg["assets"]:
            for win, dur in self.cfg["windows"].items():
                start = int(now // dur * dur)
                for st in (start, start + dur):          # текущее и следующее окно
                    slug = f"{asset}-updown-{win}-{st}"
                    if slug in self.markets:
                        continue
                    if st > now + 20:                    # следующее — только за 20 с до старта
                        continue
                    try:
                        m = await self.api.get_market(slug)
                    except Exception:  # noqa: BLE001
                        continue
                    if not m:
                        continue
                    toks = [str(t) for t in _as_list(m.get("clobTokenIds"))]
                    outs = [str(o).lower() for o in _as_list(m.get("outcomes"))]
                    if len(toks) != 2 or outs != ["up", "down"]:
                        continue
                    self.markets[slug] = Mkt(slug, asset, win, st, st + dur, toks[0], toks[1],
                                             str(m.get("conditionId", "")))
                    log.debug(f"РЫНОК {slug}")

    def capture_s0(self):
        for m in self.markets.values():
            if m.resolved or self.api.now() < m.start:
                continue
            sym = self.cfg["assets"][m.asset]
            for src in self.sampler.sources(sym):
                if src not in m.s0:
                    # только если у источника есть цена РОВНО на старте окна (±2 с);
                    # источник, появившийся позже, в этом окне не участвует
                    v = self.sampler.value_at(sym, src, int(m.start), tol=2)
                    if v is not None:
                        m.s0[src] = v

    def _src_funcs(self, m: Mkt, sym: str, src: str, now: float):
        at0, avg = self.sampler.funcs(sym, src, now)
        s0 = m.s0.get(src)

        def at(t):
            if int(t) == int(m.start) and s0:
                return math.log(s0)              # старт сохраняется в state.json (переживает рестарт)
            return at0(t)
        return at, avg

    def model(self, m: Mkt):
        """-> ({правило: P(Up)}, ratio, sigma, n_sources) или None.
        Источник участвует, только если у него есть цена на старте окна."""
        sym = self.cfg["assets"][m.asset]
        now = self.api.now()
        sig = self.sampler.sigma(sym, now, self.cfg["sigma_window_s"])
        if sig is None:
            return None
        sig = max(sig, self.cfg["sigma_floor"])
        ratios, probs = [], {r: [] for r in fvmodel.RULES}
        for src, s0 in m.s0.items():
            h = self.prices.hist.get((sym, src))
            if not (h and now - h[-1][0] <= 3 and s0 > 0):
                continue
            ratios.append(h[-1][1] / s0)
            at, avg = self._src_funcs(m, sym, src, now)
            x = math.log(h[-1][1])
            for rule in fvmodel.RULES:
                p = fvmodel.prob_up(rule, at, avg, m.start, m.end, now, x, sig)
                if p is not None:
                    probs[rule].append(p)
        if len(ratios) < self.cfg["min_sources"]:
            return None
        pr = {r: statistics.median(v) for r, v in probs.items() if v}
        return pr, statistics.median(ratios), sig, len(ratios)

    # ---- решения
    def _on_price(self, symbol: str, ts: float):
        now = self.api.now()
        if now - self._last_trig < self.cfg.get("trigger_gap_s", 0.02):
            return
        rets = self.prices.rets_bps(symbol, now, self._trig_s, self.cfg.get("lead_sources"))
        agree = self.cfg.get("lead_agree", 1)
        if (sum(1 for x in rets if x >= self._trig_bps) >= agree
                or sum(1 for x in rets if -x >= self._trig_bps) >= agree):
            self._last_trig = now
            self.count("fast_trigger")
            try:
                self.decide()
            except Exception as e:  # noqa: BLE001
                log.debug(f"fast decide: {e}")

    def decide(self):
        now = self.api.now()
        cfg = self.cfg
        snap = now - self.last_snap >= cfg["snapshot_every_s"]
        if snap:
            self.last_snap = now
        for m in list(self.markets.values()):
            if m.resolved or now < m.start or now > m.end:
                continue
            tau = m.end - now
            mo = self.model(m)
            if mo is None:
                self.count("no_model")
                continue
            pr, r, sig, nsrc = mo
            if snap:
                row = {"t": round(now, 2), "asset": m.asset, "window": m.window, "slug": m.slug,
                       "tau": round(tau, 1), "ratio_bps": round((r - 1) * 1e4, 3), "sigma": f"{sig:.3e}",
                       "up_bid": self.book.bid.get(m.up, ""), "up_ask": self.book.ask.get(m.up, ""),
                       "down_bid": self.book.bid.get(m.down, ""), "down_ask": self.book.ask.get(m.down, ""),
                       "sources": nsrc}
                for rule in fvmodel.RULES:
                    row[f"p_{rule}"] = round(pr[rule], 4) if rule in pr else ""
                self._csv("snapshots.csv", SNAP_COLS, row)
            if tau < cfg["min_tau"]:
                continue
            for side, tok in (("Up", m.up), ("Down", m.down)):
                ask = self.book.ask.get(tok)
                if ask is None or not (cfg["min_price"] <= ask <= cfg["max_price"]):
                    continue
                if now - self.book.t.get(tok, 0) > 30:
                    continue                                   # стакан давно не обновлялся
                f_ask = fee(ask, cfg["taker_fee_coef"])
                want = []
                for v in self.variants.values():
                    sp = v.spec
                    rule = sp.get("rule", "spot")
                    if rule not in pr:
                        continue
                    p = pr[rule] if side == "Up" else 1 - pr[rule]
                    edge = p - ask - f_ask
                    if edge < sp["min_edge"] or edge > sp.get("max_edge", 9):
                        continue
                    if "assets" in sp and m.asset not in sp["assets"]:
                        continue
                    if "windows" in sp and m.window not in sp["windows"]:
                        continue
                    if "move_bps" in sp:
                        # «запаздывание»: биржа уже сдвинулась в нашу сторону, а аск на Polymarket ещё нет
                        sym = cfg["assets"][m.asset]
                        rets = self.prices.rets_bps(sym, now, sp.get("move_s", 1.0), cfg.get("lead_sources"))
                        sgn = 1 if side == "Up" else -1
                        if sum(1 for x in rets if sgn * x >= sp["move_bps"]) < cfg.get("lead_agree", 1):
                            continue
                        if "ask_stale_s" in sp and now - self.book.ask_t.get(tok, now) < sp["ask_stale_s"]:
                            continue
                        if sp.get("ask_not_up"):
                            # Polymarket ещё не подорожал: аск сейчас не выше, чем до рывка
                            before = self.book.ask_at(tok, now - sp.get("move_s", 1.0))
                            if before is None or ask > before + 1e-9:
                                continue
                    if not (sp.get("min_p", 0) <= p <= sp.get("max_p", 1)):
                        continue
                    if tau < sp.get("min_tau", 0) or tau > sp.get("max_tau", 1e9):
                        continue
                    ent = v.entries.get(tok, [])
                    if len(ent) >= cfg["max_entries_per_side"] or (ent and now - ent[-1] < cfg["entry_gap_s"]):
                        continue
                    want.append((v, p, edge))
                if want and (tok not in self.pending):
                    self.pending.add(tok)
                    for v, _, _ in want:
                        v.entries.setdefault(tok, []).append(now)   # бронируем, чтобы не дублировать
                    asyncio.ensure_future(self.execute(m, side, tok, ask, r, sig, nsrc, tau, want))

    async def execute(self, m, side, tok, ask, r, sig, nsrc, tau, items):
        cfg = self.cfg
        try:
            t0 = time.perf_counter()
            try:
                bk = await self.api.get_book(tok)          # реальная задержка до биржи
            except Exception as e:  # noqa: BLE001
                self.count("book_error")
                log.debug(f"book {tok[:10]}: {e}")
                return
            ms = (time.perf_counter() - t0) * 1000
            limit = min(ask + cfg["fill_tolerance"], cfg["max_price"])
            for v, p, edge in items:
                shares = max(5.0, cfg["stake_usd"] / ask)
                f = walk_book(bk, "BUY", shares, limit, cfg["taker_fee_coef"])
                if f.shares <= 0:
                    self.count(f"miss_{v.name}")
                    log.debug(f"МИМО {v.name} {m.slug} {side}: стакан ушёл выше {limit:.2f} "
                              f"(лучшая {f.best}) за {ms:.0f} мс")
                    continue
                cost = f.notional + f.fee
                if cost > v.cash:
                    self.count(f"no_cash_{v.name}")
                    continue
                v.cash -= cost
                pos = v.pos.setdefault(tok, [0.0, 0.0])
                pos[0] += f.shares
                pos[1] += cost
                self.count(f"buy_{v.name}")
                ef = p - f.vwap - f.fee / f.shares
                v.trades.append({"slug": m.slug, "tok": tok, "side": side, "t": self.api.now(),
                                 "tau": round(tau, 1), "p": round(p, 4), "seen": ask,
                                 "vwap": round(f.vwap, 4), "shares": round(f.shares, 4),
                                 "cost": round(cost, 4), "edge_seen": round(edge, 4),
                                 "edge_fill": round(ef, 4), "pnl": None})
                self._csv("trades.csv", TRADE_COLS, {
                    "time_utc": utc(self.api.now()), "variant": v.name,
                    "rule": v.spec.get("rule", "spot"), "asset": m.asset,
                    "window": m.window, "slug": m.slug, "side": side, "tau": round(tau, 1),
                    "ratio_bps": round((r - 1) * 1e4, 2), "sigma": f"{sig:.3e}", "p_model": round(p, 4),
                    "seen_ask": ask, "fill_vwap": round(f.vwap, 4), "shares": round(f.shares, 2),
                    "cost": round(cost, 4), "fee": round(f.fee, 4), "edge_seen": round(edge, 4),
                    "edge_fill": round(ef, 4), "book_ms": round(ms), "sources": nsrc})
                self.info(f"BUY {v.name:<12} {m.asset.upper()} {m.window} {side:<4} модель {p:.2f} "
                          f"видели {ask:.2f} -> взяли {f.vwap:.3f} (перевес {ef * 100:+.1f}¢) | "
                          f"{tau:.0f}с до конца | BTC/ETH {(r - 1) * 1e4:+.1f} б.п. | счёт ${v.cash:.2f}")
        finally:
            self.pending.discard(tok)

    def rule_check(self, m: Mkt) -> dict:
        """Итог окна по каждому правилу из НАШИХ цен (медиана по источникам), в б.п."""
        sym = self.cfg["assets"][m.asset]
        out = {}
        for rule in fvmodel.RULES:
            ds = []
            for src in m.s0:
                at, avg = self._src_funcs(m, sym, src, self.api.now())
                d = fvmodel.final_diff(rule, at, avg, m.start, m.end)
                if d is not None:
                    ds.append(d)
            if ds:
                out[rule] = statistics.median(ds) * 1e4
        return out

    # ---- итоги
    async def resolve(self):
        now = self.api.now()
        for m in list(self.markets.values()):
            if m.resolved or now < m.end + 10 or now < self.resolve_next.get(m.slug, 0):
                continue
            self.resolve_next[m.slug] = now + self.cfg["resolve_every_s"]
            try:
                mk = await self.api.get_market(m.slug, closed=True)
            except Exception:  # noqa: BLE001
                continue
            pay = resolution_payouts(mk) if mk else None
            if not pay:
                continue
            m.resolved = True
            m.winner = "Up" if pay.get(m.up) == 1 else "Down"
            rc = self.rule_check(m)
            row = {"slug": m.slug, "asset": m.asset, "window": m.window, "start": m.start,
                   "winner": m.winner}
            for rule in fvmodel.RULES:
                if rule in rc:
                    d = rc[rule]
                    ok = (d >= 0) == (m.winner == "Up")
                    row[f"d_{rule}_bps"] = round(d, 3)
                    self.count(f"rule_{rule}_n")
                    self.count(f"rule_{rule}_ok", int(ok))
                    if abs(d) >= 1.0:                       # «явные» окна, где правило не на грани
                        self.count(f"rule_{rule}_clear_n")
                        self.count(f"rule_{rule}_clear_ok", int(ok))
            self._csv("outcomes.csv", OUT_COLS, row)
            tot = []
            for v in self.variants.values():
                pnl_m = 0.0
                for tok in (m.up, m.down):
                    pos = v.pos.pop(tok, None)
                    v.entries.pop(tok, None)
                    if not pos:
                        continue
                    payout = pos[0] * pay.get(tok, 0)
                    pnl = payout - pos[1]
                    v.cash += payout
                    v.realized += pnl
                    pnl_m += pnl
                    for t in v.trades:
                        if t["tok"] == tok and t["pnl"] is None:
                            t["pnl"] = round(t["shares"] * pay.get(tok, 0) - t["cost"], 4)
                            t["won"] = bool(pay.get(tok, 0))
                if pnl_m:
                    tot.append(f"{v.name} {pnl_m:+.2f}")
            self.count("resolved")
            if tot:
                self.info(f"ИТОГ {m.asset.upper()} {m.window} {utc(m.start)[11:16]} победил {m.winner}: "
                          + ", ".join(tot))
        for k in [k for k, m in self.markets.items() if m.resolved]:
            del self.markets[k]

    def status(self):
        a = self.sampler
        srcs = ",".join(self.prices.sources_alive(self.api.now())) or "НЕТ ЦЕН"
        parts = [f"{v.name} ${v.equity_at_cost():.2f} ({v.realized:+.2f}, "
                 f"{sum(1 for t in v.trades if t['pnl'] is not None)} сд.)" for v in self.variants.values()]
        self.info(f"СТАТУС рынков {sum(1 for m in self.markets.values() if not m.resolved)}, "
                  f"рассчитано {self.counters.get('resolved', 0)} | цены: {srcs} | стакан CLOB "
                  f"{self.api.now() - self.ws_last:.0f}с назад | " + " | ".join(parts))


# ======================================================================= петли

async def clob_book_loop(eng: Engine, stop: asyncio.Event):
    backoff = 1
    while not stop.is_set():
        toks = sorted({t for m in eng.markets.values() if not m.resolved for t in (m.up, m.down)})
        if not toks:
            await asyncio.sleep(1)
            continue
        ws, pinger = None, None
        try:
            ws = await WebSocket.connect(eng.cfg["clob_ws"])
            await ws.send(json.dumps({"assets_ids": toks, "type": "market"}))
            backoff = 1

            async def ping():
                while True:
                    await asyncio.sleep(10)
                    await ws.send("PING")
            pinger = asyncio.ensure_future(ping())
            while not stop.is_set():
                cur = sorted({t for m in eng.markets.values() if not m.resolved for t in (m.up, m.down)})
                if cur != toks:
                    break
                raw = await asyncio.wait_for(ws.recv(), eng.cfg["ws_silence_s"])
                eng.ws_last = eng.api.now()
                if raw.strip().upper() == "PONG":
                    continue
                try:
                    data = fast_loads(raw)
                except ValueError:
                    continue
                for ev in (data if isinstance(data, list) else [data]):
                    if isinstance(ev, dict):
                        eng.book.on_event(ev, eng.api.now())
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            eng.count("clob_reconnect")
            log.debug(f"CLOB WS: {type(e).__name__}: {e}")
        finally:
            if pinger:
                pinger.cancel()
            if ws:
                await ws.close()
        await asyncio.sleep(backoff if eng.counters.get("clob_reconnect") else 0)
        backoff = min(backoff * 2, 30)


async def main_loop(eng: Engine, stop: asyncio.Event):
    last_disc, last_status, last_sample = 0.0, 0.0, 0.0
    while not stop.is_set():
        now = time.time()
        try:
            if now - last_sample >= 1:
                eng.sampler.tick(eng.api.now())
                eng.capture_s0()
                last_sample = now
            if now - last_disc >= 5:
                await eng.discover()
                last_disc = now
            eng.decide()
            await eng.resolve()
        except Exception as e:  # noqa: BLE001
            log.exception(f"loop: {e}")
        if now - last_status >= eng.cfg["status_every_s"]:
            eng.save()
            eng.status()
            last_status = now
        try:
            await asyncio.wait_for(stop.wait(), eng.cfg["loop_s"])
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
    eng.info(f"Старт: стратегия справедливой цены, {', '.join(a.upper() for a in cfg['assets'])}, "
             f"окна {', '.join(cfg['windows'])}, варианты: "
             + ", ".join(f"{v['name']}(≥{v['min_edge'] * 100:.0f}¢)" for v in cfg["variants"]))
    loops = [main_loop(eng, stop), clob_book_loop(eng, stop), mmsim.binance_rest_loop(eng, stop)]
    if cfg["use_coinbase_ws"]:
        loops.append(mmsim.coinbase_loop(eng, stop))
    if cfg["use_okx_ws"]:
        loops.append(mmsim.okx_loop(eng, stop))
    if cfg["use_binance_ws"]:
        loops.append(mmsim.binance_ws_loop(eng, stop))
    loops += mmsim.extra_feed_loops(eng, cfg, stop)
    tasks = [asyncio.ensure_future(x) for x in loops]
    await stop.wait()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    eng.save()
    eng.info("Остановлено. Отчёт: python3 fvsim.py report")


# ======================================================================= отчёт

TAU_B = [(0, 30, "<30с"), (30, 60, "30-60с"), (60, 120, "1-2м"), (120, 300, "2-5м"), (300, 1e9, ">5м")]


def study(out: Path, rule: str = "t60", fee_coef: float = 0.07) -> str:
    """Карта перевеса по снимкам: если бы мы покупали по ask каждую секунду —
    сколько бы выиграли в каждой клетке «вероятность модели × время до конца».
    Снимки не зависят от наших порогов, поэтому это честная проверка модели."""
    op = out / "outcomes.csv"
    sp = out / "snapshots.csv"
    if not op.exists() or not sp.exists():
        return "Нет snapshots.csv/outcomes.csv — бот должен поработать хотя бы час."
    win = {r["slug"]: r["winner"] for r in csv.DictReader(op.open())}
    cells = defaultdict(lambda: [0, 0, 0.0, 0.0, set()])   # n, won, sum ask, sum pnl, рынки
    col = f"p_{rule}"
    used = 0
    with sp.open() as f:
        for r in csv.DictReader(f):
            w = win.get(r["slug"])
            if not w or not r.get(col):
                continue
            pu = float(r[col])
            tau = float(r["tau"])
            used += 1
            for side, p, ask in (("Up", pu, r["up_ask"]), ("Down", 1 - pu, r["down_ask"])):
                if not ask:
                    continue
                ask = float(ask)
                if not 0.02 <= ask <= 0.98:
                    continue
                pb = min(int(p * 10), 9)
                tb = next(lbl for a, b, lbl in TAU_B if a <= tau < b)
                c = cells[(pb, tb)]
                won = (w == side)
                c[0] += 1
                c[1] += won
                c[2] += ask
                c[3] += (1.0 if won else 0.0) - ask - fee_coef * ask * (1 - ask)
                c[4].add(r["slug"])
    if not used:
        return "Пока нет рассчитанных рынков со снимками."
    L = [f"=== Карта перевеса по снимкам (правило {rule}), снимков {used}, рынков {len(win)} ===",
         "В клетке: перевес на акцию после комиссии тейкера, если покупать по ask (¢), и [рынков].",
         "Плюс в клетке с ≥30 рынками — кандидат в стратегию. Меньше 30 рынков — шум.", "",
         f"{'P модели':<10}" + "".join(f"{lbl:>16}" for _, _, lbl in TAU_B)]
    for pb in range(10):
        row = f"{pb / 10:.1f}-{(pb + 1) / 10:.1f}  "
        for _, _, lbl in TAU_B:
            c = cells.get((pb, lbl))
            row += f"{f'{c[3] / c[0] * 100:+.1f}¢ [{len(c[4])}]':>16}" if c and c[0] else f"{'-':>16}"
        L.append(row)
    L += ["", f"{'P модели':<10}{'снимков':>9}{'выиграл':>9}{'ср.ask':>8}{'перевес':>9}{'рынков':>8}"]
    for pb in range(10):
        n = sum(cells[(pb, lbl)][0] for _, _, lbl in TAU_B if (pb, lbl) in cells)
        if not n:
            continue
        wn = sum(cells[(pb, lbl)][1] for _, _, lbl in TAU_B if (pb, lbl) in cells)
        sa = sum(cells[(pb, lbl)][2] for _, _, lbl in TAU_B if (pb, lbl) in cells)
        sp_ = sum(cells[(pb, lbl)][3] for _, _, lbl in TAU_B if (pb, lbl) in cells)
        mk = set().union(*(cells[(pb, lbl)][4] for _, _, lbl in TAU_B if (pb, lbl) in cells))
        L.append(f"{pb / 10:.1f}-{(pb + 1) / 10:.1f}   {n:>9}{wn / n:>8.0%} {sa / n:>7.3f}"
                 f"{sp_ / n * 100:>+8.1f}¢{len(mk):>8}")
    return "\n".join(L)


def report(out: Path) -> str:
    sp = out / "state.json"
    if not sp.exists():
        return f"Нет данных в {out}. Сначала: python3 fvsim.py run"
    d = json.loads(sp.read_text())
    hours = max((d["saved"] - d["started"]) / 3600, 1e-9)
    L = [f"=== Справедливая цена (симуляция), {hours:.1f} ч, рассчитано рынков "
         f"{d.get('counters', {}).get('resolved', 0)} ===", "",
         f"{'вариант':<14}{'счёт':>9}{'PnL':>9}{'сделок':>8}{'винрейт':>9}{'ROI':>8}"
         f"{'перевес видели':>16}{'перевес взяли':>15}{'промахов':>10}"]
    c = d.get("counters", {})
    for name, v in d["variants"].items():
        tr = [t for t in v["trades"] if t.get("pnl") is not None]
        cost = sum(t["cost"] for t in tr)
        pnl = sum(t["pnl"] for t in tr)
        eq = v["cash"] + sum(x[1] for x in v["pos"].values())
        wr = f"{sum(t['won'] for t in tr) / len(tr):.0%}" if tr else "-"
        roi = f"{pnl / cost:+.1%}" if cost else "-"
        es = f"{statistics.mean(t['edge_seen'] for t in tr) * 100:+.1f}¢" if tr else "-"
        ef = f"{statistics.mean(t['edge_fill'] for t in tr) * 100:+.1f}¢" if tr else "-"
        L.append(f"{name:<14}{eq:>9.2f}{v['realized']:>+9.2f}{len(tr):>8}{wr:>9}{roi:>8}"
                 f"{es:>16}{ef:>15}{c.get('miss_' + name, 0):>10}")
    if c.get("rule_spot_n") or c.get("rule_t60_n") or c.get("rule_wt_n"):
        L += ["", "--- какое правило расчёта совпадает с реальными итогами (по нашим ценам бирж) ---",
              f"{'правило':<10}{'совпало':>14}{'в явных окнах (≥1 б.п.)':>26}"]
        names = {"spot": "spot (цена в конце)", "t60": "t60 (средн. 60с)", "wt": "wt (средн. окна)"}
        for rule in ("spot", "t60", "wt"):
            n, ok = c.get(f"rule_{rule}_n", 0), c.get(f"rule_{rule}_ok", 0)
            cn, cok = c.get(f"rule_{rule}_clear_n", 0), c.get(f"rule_{rule}_clear_ok", 0)
            if n:
                cl = f"{cok}/{cn} = {cok / cn:.0%}" if cn else "-"
                L.append(f"{names[rule]:<22}{f'{ok}/{n} = {ok / n:.0%}':>12}{cl:>16}")
        L.append("Правило с совпадением ~95-100% — то, по которому реально считает Polymarket.")
    # калибровка модели по всем сделкам всех вариантов (уникальные по времени/токену)
    seen, rows = set(), []
    for v in d["variants"].values():
        for t in v["trades"]:
            k = (t["tok"], round(t["t"], 1))
            if t.get("pnl") is not None and k not in seen:
                seen.add(k)
                rows.append(t)
    if rows:
        L += ["", "--- калибровка: вероятность модели vs как вышло (по сделкам) ---",
              f"{'P модели':<12}{'сделок':>7}{'выиграли':>10}{'ср.цена':>9}"]
        g = defaultdict(list)
        for t in rows:
            g[min(int(t["p"] * 10), 9)].append(t)
        for k in range(10):
            xs = g.get(k)
            if xs:
                L.append(f"{k / 10:.1f}-{(k + 1) / 10:.1f}     {len(xs):>7}"
                         f"{sum(x['won'] for x in xs) / len(xs):>9.0%} "
                         f"{statistics.mean(x['vwap'] for x in xs):>8.3f}")
    L += ["", "Как читать: «перевес видели» — насколько модель была выше цены в момент решения;",
          "«перевес взяли» — после реального входа по стакану (наша задержка). Если ROI > 0 на",
          "сотнях сделок и калибровка ровная (выиграли ≈ вероятности модели) — есть что развивать.",
          "Снимки рынков для обучения: data_fv/snapshots.csv + outcomes.csv"]
    return "\n".join(L)


async def check(cfg: dict) -> bool:
    api = Api(cfg)
    now = time.time()
    ok = True
    for asset in cfg["assets"]:
        for win, dur in cfg["windows"].items():
            slug = f"{asset}-updown-{win}-{int(now // dur * dur)}"
            try:
                m = await api.get_market(slug)
                print(f"  {'OK  ' if m else 'FAIL'} {slug}: {'найден' if m else 'не найден'}")
                ok &= bool(m)
            except Exception as e:  # noqa: BLE001
                ok = False
                print(f"  FAIL {slug}: {e}")
    return ok


def main():
    ap = argparse.ArgumentParser(description="Симулятор справедливой цены для крипто Up/Down")
    ap.add_argument("cmd", choices=["run", "report", "check", "study"])
    ap.add_argument("--rule", default="t60", choices=["spot", "t60", "wt"])
    ap.add_argument("--config", default=str(HERE / "fv_config.json"))
    ap.add_argument("--out", default=str(HERE / "data_fv"))
    a = ap.parse_args()
    cfg = load_config(Path(a.config))
    if a.cmd == "run":
        asyncio.run(run(cfg, Path(a.out)))
    elif a.cmd == "study":
        print(study(Path(a.out), a.rule))
    elif a.cmd == "report":
        print(report(Path(a.out)))
    else:
        sys.exit(0 if asyncio.run(check(cfg)) else 1)


if __name__ == "__main__":
    main()
