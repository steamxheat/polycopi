#!/usr/bin/env python3
"""Разведчик: ищет на Polymarket трейдеров, которых имеет смысл копировать.

1. Собирает кандидатов из таблиц лидеров по всем категориям и периодам.
2. Для каждого считает ЧЕСТНЫЕ метрики за последние N дней:
   * винрейт = выигранные / все рассчитанные позиции, включая ПРОИГРАННЫЕ,
     которые Polymarket не показывает в «закрытых» (они висят в открытых с ценой 0);
   * ROI = прибыль / вложено (именно он показывает, есть ли перевес;
     винрейт 90% при покупке по 0.95 — это ноль);
   * средняя цена входа, позиций в день, доля крипты и 5-минуток,
     доля сделок тейкером (мейкеров копировать бесполезно), дни с последней сделки.
3. Ранжирует и печатает таблицу. С --apply N записывает лучших N
   в leaders_extra.json — копи-бот подхватит их после перезапуска.

  python3 scout.py                         # отчёт (несколько минут)
  python3 scout.py --apply 20              # отчёт + добавить 20 лучших в копи-бот
  python3 scout.py --min-winrate 0.7 --days 14
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import re
import statistics
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path

from net import Http

HERE = Path(__file__).resolve().parent
DATA = "https://data-api.polymarket.com"
CATEGORIES = ["CRYPTO", "OVERALL", "SPORTS", "ESPORTS", "POLITICS", "ECONOMICS", "FINANCE",
              "TECH", "WEATHER", "CULTURE", "MENTIONS"]
PERIODS = ["WEEK", "MONTH"]
CRYPTO_RE = re.compile(r"(updown|up-or-down|bitcoin|ethereum|solana|\bxrp\b|\bbtc\b|\beth\b|\bsol\b|"
                       r"dogecoin|\bbnb\b|crypto|hyperliquid)", re.I)
FAST_RE = re.compile(r"-(5m|15m|1h|4h)-\d{9,11}$")


@dataclass
class Stats:
    wallet: str
    name: str
    sources: str = ""
    lb_pnl_month: float = 0.0
    resolved: int = 0
    wins: int = 0
    winrate: float = 0.0
    invested: float = 0.0
    pnl: float = 0.0
    roi: float = 0.0
    avg_entry: float = 0.0
    per_day: float = 0.0
    crypto_share: float = 0.0
    fast_share: float = 0.0
    taker_share: float = -1.0
    median_trade_usd: float = 0.0
    trades_seen: int = 0
    days_since_trade: float = 999.0
    hidden_losses: int = 0
    combo_share: float = 0.0
    score: float = 0.0
    flags: str = ""


def _f(x, d=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return d


def _ts(v) -> float:
    """timestamp (сек/мс) или дата 'YYYY-MM-DD...' -> unix-секунды."""
    if v is None or v == "":
        return 0.0
    if isinstance(v, (int, float)) or (isinstance(v, str) and v.replace(".", "", 1).isdigit()):
        t = float(v)
        return t / 1000 if t > 1e12 else t
    from datetime import datetime, timezone
    s = str(v).replace("Z", "+00:00")
    try:
        d = datetime.fromisoformat(s if "T" in s or "+" in s else s + "T00:00:00+00:00")
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d.timestamp()
    except ValueError:
        return 0.0


def compute(wallet: str, name: str, closed: list, open_pos: list, activity: list,
            now: float, days: float) -> Stats:
    """Чистая функция: все метрики из ответов API."""
    st = Stats(wallet, name)
    since = now - days * 86400
    seen = set()
    rows = []          # (pnl, cost, avg_price, slug_or_title)
    for p in closed:
        t = _ts(p.get("timestamp")) or _ts(p.get("endDate"))
        if t < since:
            continue
        key = (p.get("conditionId"), p.get("asset"))
        seen.add(key)
        cost = _f(p.get("avgPrice")) * _f(p.get("totalBought"))
        rows.append((_f(p.get("realizedPnl")), cost, _f(p.get("avgPrice")),
                     f"{p.get('slug', '')} {p.get('title', '')}"))
    # рассчитанные, но не закрытые: проигранные (цена 0) и невыкупленные выигрыши (цена 1)
    for p in open_pos:
        cur = _f(p.get("curPrice"), -1)
        if not (cur <= 0.0005 or cur >= 0.9995):
            continue
        t = _ts(p.get("endDate"))
        if t and t < since:
            continue
        key = (p.get("conditionId"), p.get("asset"))
        if key in seen:
            continue
        seen.add(key)
        cost = _f(p.get("initialValue")) or _f(p.get("avgPrice")) * _f(p.get("size"))
        pnl = _f(p.get("cashPnl")) + _f(p.get("realizedPnl"))
        if cur <= 0.0005:
            st.hidden_losses += 1
            pnl = pnl if pnl < 0 else -cost
        rows.append((pnl, cost, _f(p.get("avgPrice")), f"{p.get('slug', '')} {p.get('title', '')}"))
    st.resolved = len(rows)
    if rows:
        st.wins = sum(1 for r in rows if r[0] > 0)
        st.winrate = st.wins / st.resolved
        st.invested = sum(r[1] for r in rows)
        st.pnl = sum(r[0] for r in rows)
        st.roi = st.pnl / st.invested if st.invested > 0 else 0.0
        w = [(r[2], r[1]) for r in rows if r[1] > 0 and 0 < r[2] < 1]
        st.avg_entry = (sum(p * c for p, c in w) / sum(c for _, c in w)) if w else 0.0
        st.crypto_share = sum(1 for r in rows if CRYPTO_RE.search(r[3])) / st.resolved
        st.fast_share = sum(1 for r in rows if FAST_RE.search(r[3].split(" ")[0])) / st.resolved
    trades = [a for a in activity if str(a.get("type", "TRADE")) == "TRADE"]
    st.trades_seen = len(trades)
    is_combo = lambda x: bool(x.get("isCombo")) or str(x.get("outcomeIndex")) == "999"
    combo_n = sum(1 for a in trades if is_combo(a)) + sum(1 for p in closed + open_pos if is_combo(p))
    total_n = len(trades) + len(closed) + len(open_pos)
    st.combo_share = combo_n / total_n if total_n else 0.0
    if trades:
        tss = [_ts(a.get("timestamp")) for a in trades]
        st.days_since_trade = max(0.0, (now - max(tss)) / 86400)
        span_days = max((now - min(tss)) / 86400, 1 / 24)
        st.per_day = len(trades) / span_days
        st.median_trade_usd = statistics.median(_f(a.get("usdcSize")) for a in trades)
        buys = [a for a in trades if str(a.get("side")).upper() == "BUY" and 0 < _f(a.get("price")) < 1
                and _f(a.get("size")) > 0]
        fee_buys = [a for a in buys if CRYPTO_RE.search(f"{a.get('slug', '')} {a.get('title', '')}")]
        if len(fee_buys) >= 10:     # комиссия есть только на крипто-рынках -> только по ним
            taker = 0
            for a in fee_buys:
                p, s = _f(a["price"]), _f(a["size"])
                coef = (_f(a.get("usdcSize")) - p * s) / (s * p * (1 - p))
                taker += coef > 0.02
            st.taker_share = taker / len(fee_buys)
    return st


def score(st: Stats, min_resolved: int) -> float:
    """Перевес (ROI) с поправкой на размер выборки + бонусы за крипту и частоту."""
    if st.resolved < min_resolved or st.invested <= 0:
        return -1e9
    z = st.roi * math.sqrt(min(st.resolved, 400))          # «уверенность» в перевесе
    bonus = 1 + 0.3 * st.crypto_share + 0.2 * min(st.per_day / 20, 1)
    return z * bonus


def flags_for(st: Stats) -> str:
    f = []
    if st.avg_entry >= 0.85:
        f.append("фаворит")          # высокий винрейт за счёт покупки почти решённого
    if st.taker_share >= 0 and st.taker_share < 0.3:
        f.append("мейкер")           # копировать бесполезно
    if st.fast_share > 0.5 and st.per_day > 200:
        f.append("бот-HFT")
    if st.combo_share > 0.3:
        f.append("комбо-MM")         # продаёт парлеи через RFQ — скопировать нельзя
    if st.per_day > 400:
        f.append("частый")           # сотни сделок в день: копия всегда опаздывает
    if st.median_trade_usd > 5000:
        f.append("кит")
    if st.days_since_trade > 3:
        f.append("неактивен")
    if st.hidden_losses and st.resolved and st.hidden_losses / st.resolved > 0.3:
        f.append("скрыв.проигр")
    return ",".join(f)


class Scout:
    def __init__(self, http: Http | None = None, base: str = DATA, pause: float = 0.05):
        self.http = http or Http(timeout=10)
        self.base = base
        self.pause = pause
        self.sem = asyncio.Semaphore(4)
        self.requests = 0
        self.errors = 0

    async def get(self, path: str, params: dict):
        async with self.sem:
            self.requests += 1
            for attempt in range(3):
                try:
                    res = await self.http.aget(f"{self.base}{path}", params)
                    await asyncio.sleep(self.pause)
                    return res
                except Exception:  # noqa: BLE001
                    await asyncio.sleep(1 + attempt * 2)
            self.errors += 1
            return []

    async def candidates(self, pages: int = 2) -> dict:
        out: dict[str, dict] = {}
        jobs = []
        for cat in CATEGORIES:
            for per in PERIODS:
                for page in range(pages):
                    jobs.append((cat, per, page))

        async def one(cat, per, page):
            rows = await self.get("/v1/leaderboard", {"category": cat, "timePeriod": per,
                                                      "orderBy": "PNL", "limit": 50,
                                                      "offset": page * 50})
            for r in rows or []:
                w = str(r.get("proxyWallet", "")).lower()
                if not w.startswith("0x"):
                    continue
                c = out.setdefault(w, {"name": r.get("userName") or w[:10], "src": set(),
                                       "pnl_month": 0.0})
                c["src"].add(f"{cat[:4]}/{per[0]}")
                if per == "MONTH":
                    c["pnl_month"] = max(c["pnl_month"], _f(r.get("pnl")))
        await asyncio.gather(*(one(*j) for j in jobs))
        return out

    async def analyse(self, wallet: str, info: dict, days: float, now: float) -> Stats:
        closed = []
        for off in range(0, 200, 50):
            page = await self.get("/closed-positions", {"user": wallet, "limit": 50, "offset": off,
                                                        "sortBy": "TIMESTAMP", "sortDirection": "DESC"})
            closed += page or []
            if not page or len(page) < 50 or _ts(page[-1].get("timestamp")) < now - days * 86400:
                break
        open_pos = await self.get("/positions", {"user": wallet, "limit": 500, "sizeThreshold": 0})
        act = await self.get("/activity", {"user": wallet, "type": "TRADE", "limit": 500})
        st = compute(wallet, info["name"], closed or [], open_pos or [], act or [], now, days)
        st.sources = " ".join(sorted(info["src"]))
        st.lb_pnl_month = info["pnl_month"]
        return st


def fmt_table(rows: list[Stats]) -> str:
    L = [f"{'#':>2} {'трейдер':<22}{'винрейт':>8}{'ROI':>8}{'позиций':>8}{'в день':>7}"
         f"{'вход':>6}{'крипта':>7}{'тейкер':>7}{'сделка$':>8}{'дней':>5}  метки"]
    for i, s in enumerate(rows, 1):
        tk = "-" if s.taker_share < 0 else f"{s.taker_share:.0%}"
        L.append(f"{i:>2} {s.name[:21]:<22}{s.winrate:>7.0%} {s.roi:>+7.1%}{s.resolved:>8}"
                 f"{s.per_day:>7.0f}{s.avg_entry:>6.2f}{s.crypto_share:>6.0%} {tk:>6}"
                 f"{s.median_trade_usd:>8.0f}{s.days_since_trade:>5.1f}  {s.flags}")
    return "\n".join(L)


async def run(args) -> int:
    sc = Scout(base=args.base)
    now = time.time()
    t0 = time.time()
    print("Собираю кандидатов из таблиц лидеров...", flush=True)
    cands = await sc.candidates(pages=args.pages)
    print(f"  кандидатов: {len(cands)} (запросов {sc.requests})", flush=True)
    order = sorted(cands.items(), key=lambda kv: (-len(kv[1]["src"]), -kv[1]["pnl_month"]))
    order = order[:args.max]
    print(f"Анализирую {len(order)} трейдеров за последние {args.days:g} дней...", flush=True)
    results: list[Stats] = []
    done = 0

    async def one(w, info):
        nonlocal done
        st = await sc.analyse(w, info, args.days, now)
        st.flags = flags_for(st)
        st.score = score(st, args.min_resolved)
        results.append(st)
        done += 1
        if done % 20 == 0:
            print(f"  {done}/{len(order)}...", flush=True)

    await asyncio.gather(*(one(w, i) for w, i in order))
    out = HERE / "scout_candidates.json"
    out.write_text(json.dumps([asdict(r) for r in sorted(results, key=lambda r: -r.score)],
                              ensure_ascii=False, indent=1))
    excluded = {"мейкер", "бот-HFT", "неактивен", "кит", "комбо-MM", "частый", "фаворит"}
    good = [r for r in results
            if r.score > -1e8 and r.winrate >= args.min_winrate and r.roi >= args.min_roi
            and not (set(r.flags.split(",")) & excluded)]
    good.sort(key=lambda r: -r.score)
    print(f"\nГотово за {time.time() - t0:.0f} с, запросов {sc.requests}, ошибок {sc.errors}.")
    print(f"Прошли фильтр (винрейт ≥ {args.min_winrate:.0%}, ROI ≥ {args.min_roi:.0%}, "
          f"≥ {args.min_resolved} позиций за {args.days:g} дн., без меток "
          f"мейкер/HFT/частый/фаворит/кит/комбо/неактивен): {len(good)}\n")
    print(fmt_table(good[:args.show]))
    print("\nМетки: фаворит = в среднем покупает по ≥0.85 (высокий винрейт мало что значит), "
          "скрыв.проигр = много проигрышей, которых не видно на профиле.")
    print("Все кандидаты с метриками: scout_candidates.json")
    if args.apply:
        pick = good[:args.apply]
        extra = {r.name or r.wallet[:10]: {"address": r.wallet, "poll_interval_s": args.poll}
                 for r in pick}
        path = HERE / "leaders_extra.json"
        path.write_text(json.dumps(extra, ensure_ascii=False, indent=1))
        print(f"\nЗаписано в {path.name}: {len(extra)} трейдеров. "
              f"Перезапустите копи-бот: pm2 restart polycopy-sim")
    return 0


def main():
    ap = argparse.ArgumentParser(description="Поиск трейдеров для копирования")
    ap.add_argument("--days", type=float, default=30)
    ap.add_argument("--min-winrate", type=float, default=0.8)
    ap.add_argument("--min-resolved", type=int, default=30)
    ap.add_argument("--max", type=int, default=600, help="сколько кандидатов анализировать")
    ap.add_argument("--min-roi", type=float, default=0.05,
                    help="минимальный ROI: тоньше перевес съест проскальзывание копии")
    ap.add_argument("--pages", type=int, default=2, help="страниц по 50 на каждую таблицу лидеров")
    ap.add_argument("--show", type=int, default=40)
    ap.add_argument("--apply", type=int, default=0, help="добавить N лучших в копи-бот")
    ap.add_argument("--poll", type=float, default=5, help="интервал опроса для новых трейдеров, с")
    ap.add_argument("--base", default=DATA, help=argparse.SUPPRESS)
    sys.exit(asyncio.run(run(ap.parse_args())))


if __name__ == "__main__":
    main()
