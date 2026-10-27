#!/usr/bin/env python3
"""Polymarket copy-trading SIMULATOR.

Следит за сделками выбранных кошельков, для каждой сделки берёт РЕАЛЬНЫЙ стакан
в момент, когда вы бы о ней узнали, и считает, по какой цене вы бы вошли.
У КАЖДОГО трейдера свой виртуальный счёт (start_cash), чтобы их можно было сравнить.
Реальных ордеров не отправляет и ключей не требует.

Команды:
  python3 simbot.py check                 проверить доступ к Polymarket и задержки
  python3 simbot.py run                   запустить симуляцию (Ctrl+C — стоп, состояние сохраняется)
  python3 simbot.py report                отчёт: сводка + блок по каждому трейдеру
  python3 simbot.py report --leader primm подробный отчёт по одному трейдеру
  python3 simbot.py resim --max-slippage 0.01
                                          пересчитать записанную историю с другими настройками
Логи: data/bot.log (подробно, с ротацией), data/decisions.jsonl (каждое решение),
      data/trades.csv (сделки), data/recording.jsonl (сырые данные для resim).
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import logging.handlers
import os
import shutil
import signal
import statistics
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from core import (LeaderTrade, Portfolio, end_ts_from_slug, market_end_ts, parse_trade,
                  resolution_payouts, walk_book, fee, _levels)
from net import Http, HttpError, WebSocket, WsClosed

HERE = Path(__file__).resolve().parent
STATE_VERSION = 2
STATS_KEYS = ("lag", "book_ms", "slip", "fee_coef")

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
    "resolve_max_backoff_s": 600,
    "log_level_console": "INFO",
    "log_max_mb": 20,
    "log_backups": 10,
    "clob_url": "https://clob.polymarket.com",
    "data_url": "https://data-api.polymarket.com",
    "gamma_url": "https://gamma-api.polymarket.com",
    "rtds_url": "wss://ws-live-data.polymarket.com",
}

log = logging.getLogger("polycopy")


def load_config(path: Path) -> dict:
    cfg = dict(DEFAULTS)
    if path.exists():
        cfg.update(json.loads(path.read_text()))
    return normalize_leaders(cfg)


def normalize_leaders(cfg: dict) -> dict:
    """leaders: {"имя": "0x.."} или {"имя": {"address": "0x..", "poll_interval_s": 3}}."""
    leaders, poll = {}, {}
    for name, v in cfg.get("leaders", {}).items():
        if isinstance(v, dict):
            addr = str(v["address"]).lower()
            if "poll_interval_s" in v:
                poll[addr] = float(v["poll_interval_s"])
        else:
            addr = str(v).lower()
        leaders[name] = addr
    cfg["leaders"] = leaders
    cfg["leader_poll"] = {**cfg.get("leader_poll", {}), **poll}
    return cfg


def utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def setup_logging(out: Path, cfg: dict, quiet: bool = False) -> None:
    """Консоль (pm2 logs) — INFO; файл data/bot.log — всё (DEBUG) с ротацией."""
    log.handlers.clear()
    log.setLevel(logging.DEBUG)
    log.propagate = False
    if not quiet:
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(getattr(logging, str(cfg.get("log_level_console", "INFO")).upper(), logging.INFO))
        ch.setFormatter(logging.Formatter("[%(asctime)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
        ch.formatter.converter = time.gmtime
        log.addHandler(ch)
    out.mkdir(parents=True, exist_ok=True)
    fh = logging.handlers.RotatingFileHandler(
        out / "bot.log", maxBytes=int(cfg["log_max_mb"] * 1024 * 1024),
        backupCount=int(cfg["log_backups"]), encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s.%(msecs)03d %(levelname)-5s %(message)s",
                                      "%Y-%m-%d %H:%M:%S"))
    fh.formatter.converter = time.gmtime
    log.addHandler(fh)


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
        """Gamma по умолчанию НЕ отдаёт закрытые рынки: без closed=true ответ пустой.
        Поэтому сначала спрашиваем закрытый (нужен для расчёта), потом открытый."""
        for extra in ({"closed": "true"}, {}):
            res = await self.http.aget(f"{self.cfg['gamma_url']}/markets", {"slug": slug, **extra})
            if isinstance(res, list):
                if res:
                    return res[0]
            elif res:
                return res
        return None

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


# ======================================================================= helpers

def book_summary(book: dict, limit: float | None = None) -> dict:
    """Короткая сводка стакана для логов."""
    asks = sorted(_levels(book.get("asks")), key=lambda x: x[0])
    bids = sorted(_levels(book.get("bids")), key=lambda x: -x[0])
    s = {"best_bid": bids[0][0] if bids else None, "best_ask": asks[0][0] if asks else None,
         "ask_levels": len(asks), "bid_levels": len(bids),
         "min_order": book.get("min_order_size")}
    if s["best_bid"] is not None and s["best_ask"] is not None:
        s["spread_c"] = round((s["best_ask"] - s["best_bid"]) * 100, 2)
    if limit is not None:
        sh = sum(q for p, q in asks if p <= limit + 1e-9)
        usd = sum(p * q for p, q in asks if p <= limit + 1e-9)
        s["ask_depth_in_limit_sh"] = round(sh, 2)
        s["ask_depth_in_limit_usd"] = round(usd, 2)
    return s


class LeaderBook:
    """Всё, что относится к одному трейдеру: два портфеля, счётчики, статистика."""

    def __init__(self, name: str, start_cash: float, now: float):
        self.name = name
        self.real = Portfolio(start_cash, start_cash)
        self.ideal = Portfolio(start_cash, start_cash)
        self.since = now                 # с какого момента учитываем этого трейдера
        self.ideal_since = now           # с какого момента корректен «идеальный двойник»
        self.real_base = 0.0             # real.realized в момент ideal_since
        self.counters: dict[str, int] = {}
        self.stats: dict[str, list] = {k: [] for k in STATS_KEYS}

    def count(self, k: str):
        self.counters[k] = self.counters.get(k, 0) + 1

    def to_dict(self) -> dict:
        return {"real": self.real.to_dict(), "ideal": self.ideal.to_dict(), "since": self.since,
                "ideal_since": self.ideal_since, "real_base": self.real_base,
                "counters": self.counters,
                "stats": {k: v[-5000:] for k, v in self.stats.items()}}

    @classmethod
    def from_dict(cls, name: str, d: dict) -> "LeaderBook":
        lb = cls(name, d["real"]["start_cash"], d.get("since", 0))
        lb.real = Portfolio.from_dict(d["real"])
        lb.ideal = Portfolio.from_dict(d["ideal"])
        lb.ideal_since = d.get("ideal_since", lb.since)
        lb.real_base = d.get("real_base", 0.0)
        lb.counters = d.get("counters", {})
        lb.stats = {k: d.get("stats", {}).get(k, []) for k in STATS_KEYS}
        return lb


# ======================================================================= ENGINE

CSV_COLS = ["time_utc", "leader", "source", "action", "reason", "market", "outcome",
            "leader_price", "leader_size", "leader_usdc", "leader_fee_coef", "lag_s", "book_ms",
            "sec_to_end", "best_bid", "best_ask", "spread_cents", "ask_depth_in_limit_usd",
            "our_vwap", "slip_cents", "our_shares", "our_cost", "fee", "pnl",
            "cash", "equity_real", "equity_ideal", "slug", "asset", "tx"]


def migrate_v1(d: dict, cfg: dict, csv_path: Path, now: float) -> dict:
    """Старый state (один общий счёт) -> по счёту на каждого трейдера.

    Открытые позиции раскладываются по полю leader. Реализованный PnL, комиссии
    и выигрыши/проигрыши каждого восстанавливаются из trades.csv.
    «Идеальный двойник» считается заново с момента миграции.
    """
    start = float(cfg["start_cash"])
    old_real, old_ideal = Portfolio.from_dict(d["real"]), Portfolio.from_dict(d["ideal"])
    per: dict[str, dict] = {n: {"realized": 0.0, "fees": 0.0, "wins": 0, "losses": 0}
                            for n in cfg["leaders"]}
    if csv_path.exists():
        with csv_path.open(newline="") as f:
            for r in csv.DictReader(f):
                n = r.get("leader", "")
                if n not in per:
                    continue
                try:
                    if r.get("pnl"):
                        per[n]["realized"] += float(r["pnl"])
                    if r.get("fee") and r.get("action") in ("BUY", "SELL"):
                        per[n]["fees"] += float(r["fee"])
                except ValueError:
                    pass
                if r.get("action") == "SETTLE":
                    per[n]["wins" if r.get("reason") == "win" else "losses"] += 1
    leaders = {}
    for name in cfg["leaders"]:
        lb = LeaderBook(name, start, d.get("started", now))
        lb.ideal_since = now
        for a, p in old_real.positions.items():
            if p.leader == name:
                lb.real.positions[a] = p
        for a, p in old_ideal.positions.items():
            if p.leader == name:
                lb.ideal.positions[a] = p
        x = per[name]
        lb.real.realized, lb.real.fees = x["realized"], x["fees"]
        lb.real.wins, lb.real.losses = x["wins"], x["losses"]
        lb.real.cash = start + lb.real.realized - lb.real.open_cost()
        lb.ideal.cash = start - lb.ideal.open_cost()
        lb.real_base = lb.real.realized
        leaders[name] = lb.to_dict()
    return {"version": STATE_VERSION, "started": d.get("started", now), "saved": now,
            "config": cfg, "leaders": leaders, "counters": d.get("counters", {}),
            "legacy_stats": d.get("stats", {}), "end_cache": d.get("end_cache", {}),
            "migrated_from_v1": now}


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
        self.dec_path = out / "decisions.jsonl"
        self.leaders: dict[str, LeaderBook] = {}
        self.counters: dict[str, int] = {}
        self.legacy_stats: dict = {}
        self.started = api.now()
        self.seen: set[str] = set()
        self._seen_q: deque = deque()
        self.leader_net: dict[tuple, float] = {}
        self.recent_group: dict[tuple, float] = {}
        self.end_cache: dict[str, float] = {}
        self.resolve_next: dict[str, float] = {}
        self.resolve_tries: dict[str, int] = {}
        self.feed: dict[str, float] = {"rtds_msgs": 0, "rtds_matched": 0}
        self._load_state()
        for name in cfg["leaders"]:
            if name not in self.leaders:
                self.leaders[name] = LeaderBook(name, cfg["start_cash"], api.now())
                self.info(f"Новый трейдер {name}: свой счёт ${cfg['start_cash']:.2f}")
        self._prepare_csv()

    # ------------------------------------------------------------ logging
    def info(self, msg: str):
        if not self.quiet:
            log.info(msg)
        else:
            log.debug(msg)

    def debug(self, msg: str):
        log.debug(msg)

    def decision(self, **kw):
        if not self.record:
            return
        kw = {"t": round(self.api.now(), 3), "time_utc": utc(self.api.now()), **kw}
        with self.dec_path.open("a") as f:
            f.write(json.dumps(kw, ensure_ascii=False, default=str) + "\n")

    # ------------------------------------------------------------ persistence
    def _load_state(self):
        if not self.state_path.exists():
            return
        d = json.loads(self.state_path.read_text())
        if d.get("version", 1) < 2:
            backup = self.out / f"state_v1_backup_{int(self.api.now())}.json"
            shutil.copy2(self.state_path, backup)
            d = migrate_v1(d, self.cfg, self.csv_path, self.api.now())
            self.info(f"Состояние переведено на раздельные счета, копия старого: {backup.name}")
        self.started = d.get("started", self.started)
        self.counters = d.get("counters", {})
        self.legacy_stats = d.get("legacy_stats", {})
        self.end_cache = d.get("end_cache", {})
        for name, ld in d.get("leaders", {}).items():
            self.leaders[name] = LeaderBook.from_dict(name, ld)
        self._migrated = d.get("migrated_from_v1")

    def save_state(self):
        d = {"version": STATE_VERSION, "started": self.started, "saved": self.api.now(),
             "config": self.cfg, "counters": self.counters, "legacy_stats": self.legacy_stats,
             "leaders": {n: lb.to_dict() for n, lb in self.leaders.items()},
             "end_cache": self.end_cache, "feed": self.feed}
        if getattr(self, "_migrated", None):
            d["migrated_from_v1"] = self._migrated
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(d))
        os.replace(tmp, self.state_path)

    def _prepare_csv(self):
        """Если у старого trades.csv другие колонки — он сохраняется как trades_v1.csv."""
        if not self.csv_path.exists():
            return
        with self.csv_path.open(newline="") as f:
            header = next(csv.reader(f), [])
        if header != CSV_COLS:
            dst = self.out / "trades_v1.csv"
            n = 1
            while dst.exists():
                n += 1
                dst = self.out / f"trades_v1_{n}.csv"
            self.csv_path.rename(dst)
            self.info(f"trades.csv: новые колонки, старый файл сохранён как {dst.name}")

    def _rec(self, kind: str, **kw):
        if not self.record:
            return
        with self.rec_path.open("a") as f:
            f.write(json.dumps({"t": self.api.now(), "kind": kind, **kw}) + "\n")

    def _row(self, lb: LeaderBook, **kw):
        new = not self.csv_path.exists()
        row = {c: kw.get(c, "") for c in CSV_COLS}
        row["time_utc"] = utc(self.api.now())
        row["leader"] = lb.name
        row["cash"] = f"{lb.real.cash:.2f}"
        row["equity_real"] = f"{lb.real.equity_at_cost():.2f}"
        row["equity_ideal"] = f"{lb.ideal.equity_at_cost():.2f}"
        with self.csv_path.open("a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=CSV_COLS)
            if new:
                w.writeheader()
            w.writerow(row)

    def _count(self, k: str, lb: LeaderBook | None = None):
        self.counters[k] = self.counters.get(k, 0) + 1
        if lb:
            lb.count(k)

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
        name = self.names[t.wallet]
        lb = self.leaders[name]
        if not self.mark_seen(t):
            self._count(f"dup_{source}", lb)
            self.debug(f"DUP   {name} {source} {t.side} {t.outcome} {t.price} x{t.size} tx={t.tx[:12]}")
            return
        self._count(f"first_{source}", lb)
        t.name = name
        now = self.api.now()
        lag = max(0.0, now - t.ts)
        self._rec("trade", source=source, data=raw)

        fc = t.implied_fee_coef
        if fc is not None:
            lb.stats["fee_coef"].append(round(fc, 4))

        nk = (t.wallet, t.asset)
        held_before = self.leader_net.get(nk, 0.0)
        self.leader_net[nk] = held_before + (t.size if t.side == "BUY" else -t.size)

        self.debug(f"FILL  {name} [{source}] {t.side} {t.outcome} @{t.price} x{t.size:g} "
                   f"(${t.usdc:.2f}, fee_coef {'' if fc is None else f'{fc:.3f}'}) "
                   f"lag {lag:.2f}s | {t.title} | slug={t.slug} tx={t.tx[:14]}")

        g = (t.wallet, t.asset, t.side)
        first = self.recent_group.get(g)
        if first is not None and t.ts - first <= self.cfg["group_window_s"]:
            self._count("merged_fill", lb)       # дочерние исполнения того же ордера лидера
            return
        self.recent_group[g] = t.ts
        self._count("signals", lb)
        lb.stats["lag"].append(round(lag, 3))

        if t.side == "BUY":
            await self._copy_buy(lb, t, lag, source)
        else:
            await self._copy_sell(lb, t, lag, source, held_before)

    async def _end_ts(self, slug: str):
        """(время закрытия, точное ли). Точное — только из слага 5m/15m/1h/4h."""
        e = end_ts_from_slug(slug)
        if e is not None:
            return e, True
        if slug in self.end_cache:
            return self.end_cache[slug], False
        try:
            m = await self.api.get_market(slug)
            if m:
                self._rec("market", slug=slug, data=m)
            e = market_end_ts(m)
        except Exception as ex:  # noqa: BLE001
            self.debug(f"gamma error {slug}: {ex}")
            e = None
        if e is not None:
            self.end_cache[slug] = e
        return e, False

    def _base(self, t: LeaderTrade, lag: float, source: str) -> dict:
        return dict(source=source, market=t.title, outcome=t.outcome, leader_price=t.price,
                    leader_size=t.size, leader_usdc=f"{t.usdc:.4f}",
                    leader_fee_coef="" if t.implied_fee_coef is None else f"{t.implied_fee_coef:.4f}",
                    lag_s=f"{lag:.2f}", slug=t.slug, asset=t.asset, tx=t.tx)

    def _skip(self, lb: LeaderBook, t: LeaderTrade, reason: str, lag: float, source: str,
              extra: dict | None = None, **kw):
        self._count(f"skip_{reason}", lb)
        row = self._base(t, lag, source)
        row.update(kw)
        self._row(lb, action="SKIP", reason=reason, **row)
        self.decision(leader=lb.name, action="SKIP", side=t.side, reason=reason, **row,
                      **(extra or {}))
        self.info(f"SKIP {lb.name:<14} {t.side:<4} {t.outcome:<8} @{t.price:.3f} "
                  f"причина: {reason} | лаг {lag:.1f}с | {t.title[:55]}")

    async def _record_book_only(self, t: LeaderTrade):
        try:
            book = await self.api.get_book(t.asset)
            self._rec("book", trade_key=t.key, asset=t.asset, data=book)
        except Exception:  # noqa: BLE001
            pass

    async def _copy_buy(self, lb: LeaderBook, t: LeaderTrade, lag: float, source: str):
        cfg = self.cfg
        end_task = asyncio.ensure_future(self._end_ts(t.slug))
        t0 = time.perf_counter()
        try:
            book = await self.api.get_book(t.asset)
        except Exception as e:  # noqa: BLE001
            end_task.cancel()
            self._skip(lb, t, "book_error", lag, source, extra={"error": str(e)})
            return
        book_ms = (time.perf_counter() - t0) * 1000
        lb.stats["book_ms"].append(round(book_ms, 1))
        self._rec("book", trade_key=t.key, asset=t.asset, data=book)
        end_ts, precise = await end_task
        now = self.api.now()
        limit = min(t.price + cfg["max_slippage"], cfg["max_entry_price"])
        bs = book_summary(book, limit)
        sec_to_end = None if end_ts is None else round(end_ts - now, 1)
        ctx = dict(book_ms=f"{book_ms:.0f}", sec_to_end="" if sec_to_end is None else sec_to_end,
                   best_bid=bs["best_bid"], best_ask=bs["best_ask"],
                   spread_cents=bs.get("spread_c", ""),
                   ask_depth_in_limit_usd=bs.get("ask_depth_in_limit_usd", ""))
        extra = {"book": bs, "limit": limit, "end_ts": end_ts, "end_precise": precise}
        self.debug(f"BOOK  {lb.name} {t.outcome} bid {bs['best_bid']} / ask {bs['best_ask']} "
                   f"спред {bs.get('spread_c')}¢, в лимите {limit:.3f}: "
                   f"{bs.get('ask_depth_in_limit_sh')} акц (${bs.get('ask_depth_in_limit_usd')}), "
                   f"{book_ms:.0f} мс, до конца {sec_to_end}с{'' if precise else ' (по gamma)'}")

        if precise and end_ts is not None and end_ts - now < cfg["min_seconds_left"]:
            return self._skip(lb, t, "too_close_to_end", lag, source, extra, **ctx)
        if t.price > cfg["max_entry_price"]:
            return self._skip(lb, t, "price_too_high", lag, source, extra, **ctx)

        min_sz = float(book.get("min_order_size") or 5)
        shares = cfg["usd_per_trade"] / max(t.price, 0.01)
        if shares < min_sz:
            shares = min_sz
        if shares * t.price > cfg["max_usd_per_trade"]:
            return self._skip(lb, t, "min_order_too_big", lag, source, extra, **ctx)

        # идеальный двойник: та же сделка по цене лидера
        ifee = fee(shares, t.price, cfg["taker_fee_coef"])
        if lb.ideal.cash >= shares * t.price + ifee:
            lb.ideal.buy(t, shares, shares * t.price, ifee, end_ts)

        per_share_max = limit + fee(1, limit, cfg["taker_fee_coef"])
        affordable = lb.real.cash / per_share_max if per_share_max > 0 else 0
        if affordable < min_sz:
            return self._skip(lb, t, "no_cash", lag, source, extra, **ctx)
        shares = min(shares, affordable)

        f = walk_book(book, "BUY", shares, limit, cfg["taker_fee_coef"])
        if f.shares <= 0:
            return self._skip(lb, t, "no_liquidity_in_limit", lag, source, extra, **ctx)
        slip = (f.vwap - t.price) * 100
        lb.stats["slip"].append(round(slip, 3))
        lb.real.buy(t, f.shares, f.notional, f.fee, end_ts)
        self._count("copied_buy", lb)
        row = self._base(t, lag, source)
        row.update(ctx, our_vwap=f"{f.vwap:.4f}", slip_cents=f"{slip:.2f}",
                   our_shares=f"{f.shares:.2f}", our_cost=f"{f.notional + f.fee:.4f}",
                   fee=f"{f.fee:.4f}")
        self._row(lb, action="BUY", **row)
        self.decision(leader=lb.name, action="BUY", side="BUY", **row, **extra,
                      levels_taken=f.levels, wanted_shares=round(shares, 4),
                      cash_after=round(lb.real.cash, 4))
        self.info(f"BUY  {lb.name:<14} {t.outcome:<8} лидер {t.price:.3f} -> мы {f.vwap:.3f} "
                  f"({slip:+.1f}¢) x{f.shares:.1f} ${f.notional + f.fee:.2f} | лаг {lag:.1f}с "
                  f"| свободно ${lb.real.cash:.2f} | {t.title[:50]}")

    async def _copy_sell(self, lb: LeaderBook, t: LeaderTrade, lag: float, source: str,
                         leader_held: float):
        cfg = self.cfg
        pos = lb.real.positions.get(t.asset)
        ipos = lb.ideal.positions.get(t.asset)
        if not pos and not ipos:
            self._count("skip_sell_not_held", lb)
            self.debug(f"SELL  {lb.name} {t.outcome} @{t.price} — у нас нет позиции, пропуск")
            if self.record:
                asyncio.ensure_future(self._record_book_only(t))
            return
        frac = min(1.0, t.size / leader_held) if leader_held > 1e-9 else 1.0
        if ipos:
            sh = ipos.shares * frac
            lb.ideal.sell(t.asset, sh, sh * t.price, fee(sh, t.price, cfg["taker_fee_coef"]))
        if not pos:
            return
        t0 = time.perf_counter()
        try:
            book = await self.api.get_book(t.asset)
        except Exception as e:  # noqa: BLE001
            return self._skip(lb, t, "book_error", lag, source, extra={"error": str(e)})
        book_ms = (time.perf_counter() - t0) * 1000
        self._rec("book", trade_key=t.key, asset=t.asset, data=book)
        limit = max(t.price - cfg["max_slippage"], 0.001)
        bs = book_summary(book)
        ctx = dict(book_ms=f"{book_ms:.0f}", best_bid=bs["best_bid"], best_ask=bs["best_ask"],
                   spread_cents=bs.get("spread_c", ""))
        extra = {"book": bs, "limit": limit, "fraction": round(frac, 4),
                 "leader_held_before": round(leader_held, 4)}
        f = walk_book(book, "SELL", pos.shares * frac, limit, cfg["taker_fee_coef"])
        if f.shares <= 0:
            return self._skip(lb, t, "no_bids_in_limit", lag, source, extra, **ctx)
        pnl = lb.real.sell(t.asset, f.shares, f.notional, f.fee)
        slip = (t.price - f.vwap) * 100
        self._count("copied_sell", lb)
        row = self._base(t, lag, source)
        row.update(ctx, our_vwap=f"{f.vwap:.4f}", slip_cents=f"{slip:.2f}",
                   our_shares=f"{f.shares:.2f}", fee=f"{f.fee:.4f}", pnl=f"{pnl:.4f}")
        self._row(lb, action="SELL", **row)
        self.decision(leader=lb.name, action="SELL", side="SELL", **row, **extra)
        self.info(f"SELL {lb.name:<14} {t.outcome:<8} лидер {t.price:.3f} -> мы {f.vwap:.3f} "
                  f"доля {frac:.0%} PnL {pnl:+.2f}$ | свободно ${lb.real.cash:.2f} | {t.title[:50]}")

    # ------------------------------------------------------------ settlement
    async def resolve_once(self):
        now = self.api.now()
        slugs: dict[str, set] = {}
        for lb in self.leaders.values():
            for pf in (lb.real, lb.ideal):
                for a, p in pf.positions.items():
                    slugs.setdefault(p.slug, set()).add(a)
        for slug in slugs:
            end = end_ts_from_slug(slug) or self.end_cache.get(slug)
            if end is not None and now < end + 5:
                continue
            if now < self.resolve_next.get(slug, 0):
                continue
            n = self.resolve_tries.get(slug, 0)
            self.resolve_tries[slug] = n + 1
            delay = min(self.cfg["resolve_every_s"] * (2 ** min(n, 10)),
                        self.cfg["resolve_max_backoff_s"])
            self.resolve_next[slug] = now + delay
            try:
                m = await self.api.get_market(slug)
            except Exception as e:  # noqa: BLE001
                self.debug(f"RESOLVE {slug}: ошибка {e}")
                continue
            if m and slug not in self.end_cache:
                e2 = market_end_ts(m)
                if e2:
                    self.end_cache[slug] = e2
            pay = resolution_payouts(m) if m else None
            if not pay:
                self.debug(f"RESOLVE {slug}: ещё не рассчитан (попытка {n + 1}, "
                           f"следующая через {delay:.0f}с)")
                continue
            self.resolve_next.pop(slug, None)
            self.resolve_tries.pop(slug, None)
            self._rec("market", slug=slug, data=m)
            for lb in self.leaders.values():
                for a in list(lb.ideal.positions):
                    if a in pay:
                        lb.ideal.settle(a, pay[a])
                for a in list(lb.real.positions):
                    if a not in pay:
                        continue
                    pos = lb.real.positions[a]
                    pnl = lb.real.settle(a, pay[a])
                    self._count("settled", lb)
                    won = bool(pay[a])
                    self._row(lb, action="SETTLE", market=pos.title, outcome=pos.outcome,
                              our_shares=f"{pos.shares:.2f}", our_cost=f"{pos.cost:.4f}",
                              pnl=f"{pnl:.4f}", reason="win" if won else "loss",
                              slug=pos.slug, asset=a)
                    self.decision(leader=lb.name, action="SETTLE", market=pos.title,
                                  outcome=pos.outcome, won=won, shares=pos.shares,
                                  cost=pos.cost, pnl=pnl, slug=pos.slug, asset=a,
                                  cash_after=round(lb.real.cash, 4))
                    self.info(f"ИТОГ {lb.name:<14} {pos.outcome:<8} "
                              f"{'выигрыш' if won else 'проигрыш'} {pnl:+.2f}$ "
                              f"| счёт ${lb.real.equity_at_cost():.2f} | {pos.title[:50]}")

    # ------------------------------------------------------------ status
    def status_lines(self) -> list[str]:
        L = []
        for lb in self.leaders.values():
            r = lb.real
            L.append(f"  {lb.name:<14} счёт ${r.equity_at_cost():7.2f} (своб. ${r.cash:6.2f}, "
                     f"позиций {len(r.positions):3d}) | PnL {r.realized:+7.2f}$ "
                     f"| рынков {r.wins}+/{r.losses}- | сигн. {lb.counters.get('signals', 0)}, "
                     f"куплено {lb.counters.get('copied_buy', 0)}")
        return L

    def log_status(self):
        self.info(f"СТАТУС: RTDS сообщений {int(self.feed['rtds_msgs'])}, наших "
                  f"{int(self.feed['rtds_matched'])}, переподключений "
                  f"{self.counters.get('rtds_reconnect', 0)}, ошибок опроса "
                  f"{self.counters.get('poll_error', 0)}, ждут расчёта {len(self.resolve_tries)}")
        for line in self.status_lines():
            self.info(line)


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
            engine.info("RTDS: подключено, слушаю ленту сделок")
            backoff = 1

            async def pinger():
                while True:
                    await asyncio.sleep(5)
                    await ws.send("ping")
            ping_task = asyncio.ensure_future(pinger())
            try:
                while not stop.is_set():
                    msg = await ws.recv()
                    engine.feed["rtds_msgs"] += 1
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
                            engine.feed["rtds_matched"] += 1
                            asyncio.ensure_future(engine.on_trade(p, "rtds"))
            finally:
                ping_task.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            engine._count("rtds_reconnect")
            engine.info(f"RTDS: обрыв ({e}), переподключение через {backoff}с")
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
    name = engine.names[wallet]
    interval = engine.cfg["leader_poll"].get(wallet, engine.cfg["poll_interval_s"])
    primed = False
    errors_in_row = 0
    while not stop.is_set():
        try:
            t0 = time.perf_counter()
            items = await api.get_activity(wallet)
            ms = (time.perf_counter() - t0) * 1000
            items.sort(key=lambda d: d.get("timestamp", 0))
            new = 0
            for d in items:
                t = parse_trade(d)
                if t is None:
                    continue
                if not primed:
                    engine.mark_seen(t)       # историю до запуска не копируем
                elif t.key not in engine.seen:
                    new += 1
                    asyncio.ensure_future(engine.on_trade(d, "poll"))
            if not primed:
                engine.info(f"Опрос {name}: старт (каждые {interval:g}с), "
                            f"история пропущена ({len(items)})")
            elif new:
                engine.debug(f"POLL  {name}: {new} новых сделок, запрос {ms:.0f} мс")
            primed = True
            if errors_in_row:
                engine.info(f"Опрос {name}: восстановлен после {errors_in_row} ошибок")
            errors_in_row = 0
        except Exception as e:  # noqa: BLE001
            engine._count("poll_error")
            errors_in_row += 1
            if errors_in_row in (1, 10) or errors_in_row % 100 == 0:
                engine.info(f"Опрос {name}: ошибка #{errors_in_row}: {e}")
            else:
                engine.debug(f"Опрос {name}: ошибка #{errors_in_row}: {e}")
        try:
            await asyncio.wait_for(stop.wait(), interval)
        except asyncio.TimeoutError:
            pass


async def periodic(engine: Engine, stop: asyncio.Event):
    last_status = 0.0
    while not stop.is_set():
        try:
            await engine.resolve_once()
        except Exception as e:  # noqa: BLE001
            engine.info(f"resolver: {e}")
        if time.time() - last_status >= engine.cfg["status_every_s"]:
            engine.save_state()
            engine.log_status()
            last_status = time.time()
        try:
            await asyncio.wait_for(stop.wait(), 5)
        except asyncio.TimeoutError:
            pass


async def run(cfg: dict, out: Path):
    if not cfg["leaders"]:
        sys.exit("В config.json нет leaders.")
    setup_logging(out, cfg)
    engine = Engine(cfg, Api(cfg), out)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, stop.set)
        except NotImplementedError:
            pass
    engine.info(f"Старт симуляции. Трейдеров: {len(cfg['leaders'])} "
                f"({', '.join(cfg['leaders'])}), у каждого свой счёт.")
    engine.debug("Настройки: " + json.dumps({k: v for k, v in cfg.items()
                                             if not k.endswith("_url")}, ensure_ascii=False))
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
    engine.info("Остановлено, состояние сохранено. Отчёт: python3 simbot.py report")


# ======================================================================= CHECK

async def check(cfg: dict):
    api = Api(cfg)
    ok = True

    async def timed(name, coro):
        nonlocal ok
        t0 = time.perf_counter()
        try:
            res = await coro
            print(f"  OK   {name:<40} {(time.perf_counter() - t0) * 1000:7.0f} мс")
            return res
        except Exception as e:  # noqa: BLE001
            ok = False
            print(f"  FAIL {name:<40} {e}")
            return None

    print("Проверка доступа к Polymarket:")
    try:
        geo = await api.http.aget("https://polymarket.com/api/geoblock")
        print(f"  Гео: {geo}")
    except Exception as e:  # noqa: BLE001
        print(f"  Гео: не удалось проверить ({e})")
    for name, w in cfg["leaders"].items():
        acts = await timed(f"Data API: сделки {name}", api.get_activity(w, 5))
        if acts is not None and not acts:
            print("       сделок нет (трейдер давно не торговал?)")
        if acts:
            last = parse_trade(acts[0])
            if last:
                age_h = (time.time() - last.ts) / 3600
                print(f"       последняя: {utc(last.ts)} ({age_h:.1f} ч назад) {last.side} "
                      f"{last.outcome} {last.price} — {last.title[:45]}")
                book = await timed("CLOB: стакан", api.get_book(last.asset))
                if book:
                    print(f"       bids {len(book.get('bids', []))}, asks {len(book.get('asks', []))}, "
                          f"min_order_size {book.get('min_order_size')}")
    print("  RTDS: подключаюсь и слушаю 10 секунд...")
    try:
        t0 = time.perf_counter()
        ws = await WebSocket.connect(cfg["rtds_url"])
        print(f"  OK   RTDS подключение {(time.perf_counter() - t0) * 1000:28.0f} мс")
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


def _load_report_state(d: dict) -> dict[str, LeaderBook]:
    """v2 -> как есть; v1 (общий счёт) -> один блок «все»."""
    if d.get("version", 1) >= 2:
        return {n: LeaderBook.from_dict(n, x) for n, x in d.get("leaders", {}).items()}
    lb = LeaderBook("все", d["real"]["start_cash"], d.get("started", 0))
    lb.real, lb.ideal = Portfolio.from_dict(d["real"]), Portfolio.from_dict(d["ideal"])
    lb.counters = d.get("counters", {})
    lb.stats = {k: d.get("stats", {}).get(k, []) for k in STATS_KEYS}
    return {"все": lb}


def _leader_block(lb: LeaderBook, saved: float) -> list[str]:
    r, i, c, s = lb.real, lb.ideal, lb.counters, lb.stats
    hours = max((saved - lb.since) / 3600, 1e-9)
    settled = r.wins + r.losses
    L = [f"--- {lb.name}  ({hours:.1f} ч наблюдения) ---",
         f"Счёт: ${r.start_cash:.2f} -> ${r.equity_at_cost():.2f} (свободно ${r.cash:.2f}, "
         f"{len(r.positions)} открытых по себестоимости ${r.open_cost():.2f})",
         f"Реализованный PnL: {r.realized:+.2f}$ ({r.realized / r.start_cash * 100:+.1f}%), "
         f"рынков рассчитано {settled}: +{r.wins} / -{r.losses}"
         + (f", винрейт {r.wins / settled * 100:.0f}%" if settled else ""),
         f"Комиссии: ${r.fees:.2f}"]
    since_txt = "" if lb.ideal_since <= lb.since + 1 else f" (с {utc(lb.ideal_since)} UTC)"
    late = (r.realized - lb.real_base) - i.realized
    L.append(f"По цене лидера было бы: {i.realized:+.2f}$ -> цена опоздания {late:+.2f}$"
             + since_txt)
    L.append(f"Сигналов {c.get('signals', 0)} (сведено исполнений {c.get('merged_fill', 0)}), "
             f"куплено {c.get('copied_buy', 0)}, продано {c.get('copied_sell', 0)}")
    skips = {k[5:]: v for k, v in c.items() if k.startswith("skip_")}
    if skips:
        L.append("Пропуски: " + ", ".join(f"{k} {v}" for k, v in
                                          sorted(skips.items(), key=lambda x: -x[1])))
    L.append(f"Кто узнал первым: RTDS {c.get('first_rtds', 0)}, опрос {c.get('first_poll', 0)}")
    if s["lag"]:
        L.append(f"Задержка: медиана {statistics.median(s['lag']):.2f} с, "
                 f"90% {_pct(s['lag'], .9):.2f} с")
    if s["book_ms"]:
        L.append(f"Стакан: медиана {statistics.median(s['book_ms']):.0f} мс")
    if s["slip"]:
        L.append(f"Проскальзывание: среднее {statistics.mean(s['slip']):+.2f}¢, "
                 f"медиана {statistics.median(s['slip']):+.2f}¢, 90% {_pct(s['slip'], .9):+.2f}¢")
    if s["fee_coef"]:
        L.append(f"Комиссия лидера (коэф.): медиана {statistics.median(s['fee_coef']):.3f} "
                 f"(0 = мейкер, ~0.07 = тейкер)")
    if hours >= 12 and settled >= 30:
        L.append(f"Прогноз на 30 дней: {r.realized / (hours / 24) * 30:+.2f}$ "
                 f"(грубая экстраполяция, не гарантия)")
    else:
        L.append(f"Прогноз: нужно ≥12 ч и ≥30 рассчитанных рынков (сейчас {hours:.1f} ч, {settled})")
    return L


def report(out: Path, leader: str | None = None) -> str:
    sp = out / "state.json"
    if not sp.exists():
        return f"Нет данных в {out}. Сначала запустите: python3 simbot.py run"
    d = json.loads(sp.read_text())
    books = _load_report_state(d)
    saved = d.get("saved", time.time())
    if leader:
        if leader not in books:
            return f"Нет трейдера {leader}. Есть: {', '.join(books)}"
        return "\n".join(_leader_block(books[leader], saved))
    hours = max((saved - d.get("started", saved)) / 3600, 1e-9)
    L = [f"=== Отчёт симуляции копирования ({hours:.1f} ч с первого запуска) ===", "",
         f"{'трейдер':<16}{'счёт':>9}{'PnL':>9}{'%':>7}{'рынков':>8}{'винрейт':>9}"
         f"{'слип¢':>7}{'опозд.$':>9}"]
    rows = sorted(books.values(), key=lambda b: -b.real.realized)
    for lb in rows:
        r = lb.real
        st = r.wins + r.losses
        slip = f"{statistics.mean(lb.stats['slip']):+.2f}" if lb.stats["slip"] else "-"
        late = (r.realized - lb.real_base) - lb.ideal.realized
        L.append(f"{lb.name:<16}{r.equity_at_cost():>9.2f}{r.realized:>+9.2f}"
                 f"{r.realized / r.start_cash * 100:>+6.1f}%{st:>8}"
                 f"{(f'{r.wins / st * 100:.0f}%' if st else '-'):>9}"
                 f"{slip:>7}{late:>+9.2f}")
    L.append("")
    if d.get("migrated_from_v1"):
        L.append(f"(Общий счёт разделён по трейдерам {utc(d['migrated_from_v1'])} UTC; "
                 f"«опоздание» для старых трейдеров считается с этого момента.)")
        L.append("")
    for lb in rows:
        L += _leader_block(lb, saved) + [""]
    return "\n".join(L).rstrip()


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
    trades = [e for e in events if e["kind"] == "trade"]
    if trades:
        api.t = trades[0]["t"]
    eng = Engine(cfg, api, dst, record=False, quiet=True)
    eng.started = api.t
    for lb in eng.leaders.values():
        lb.since = lb.ideal_since = api.t
    for e in trades:
        api.t = e["t"]
        await eng.resolve_once()
        t = parse_trade(e["data"])
        api.current_key = t.key if t else ""
        await eng.on_trade(e["data"], e.get("source", "replay"))
    if api.markets:
        api.t = max(t for v in api.markets.values() for t, _ in v) + 1
        eng.resolve_next.clear()
        await eng.resolve_once()
    eng.save_state()
    return report(dst)


# ======================================================================= CLI

def main():
    ap = argparse.ArgumentParser(description="Симулятор копитрейдинга Polymarket")
    ap.add_argument("cmd", choices=["run", "check", "report", "resim"])
    ap.add_argument("--config", default=str(HERE / "config.json"))
    ap.add_argument("--out", default=str(HERE / "data"))
    ap.add_argument("--leader", help="для report: подробно по одному трейдеру")
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
        print(report(out, a.leader))
    elif a.cmd == "resim":
        print(asyncio.run(resim(cfg, out, out / "resim")))


if __name__ == "__main__":
    main()
