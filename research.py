#!/usr/bin/env python3
"""Разбор стратегии крипто-ботов (goodfallen, almach) на 5/15-минутных рынках Up/Down.

Для каждой их сделки восстанавливает, что было известно в тот момент:
  * сколько секунд оставалось до конца окна;
  * стартовую цену окна и текущую цену BTC/ETH (посекундные свечи Binance);
  * движение от старта в б.п. и его «силу» z = ln(S/S0) / (σ·√τ);
  * вероятность исхода по модели P(Up) = Φ(z) и «перевес модели» = P(исход) − цена;
  * чем закончилось (итог рынка) и сколько он заработал с акции (с учётом комиссии).

Потом группирует сделки и показывает, ГДЕ у них прибыль: по времени в окне, по цене,
по силе движения, по перевесу модели, мейкер/тейкер. И проверяет саму модель:
совпадает ли её вероятность с реальной частотой (калибровка) — если да, модель
можно ставить в своего бота.

  python3 research.py                       # goodfallen + almach, последние сделки
  python3 research.py --wallet 0x... --name someone --max-trades 5000
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
from collections import defaultdict
from pathlib import Path

import fvmodel
from net import Http

HERE = Path(__file__).resolve().parent
DATA = "https://data-api.polymarket.com"
GAMMA = "https://gamma-api.polymarket.com"
BINANCE = "https://data-api.binance.vision"
SLUG_RE = re.compile(r"^(btc|eth|sol|xrp)-updown-(5m|15m|1h|4h)-(\d{9,11})$")
DUR = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400}
SYMBOL = {"btc": "BTCUSDT", "eth": "ETHUSDT", "sol": "SOLUSDT", "xrp": "XRPUSDT"}
DEFAULT_WALLETS = {"goodfallen": "0x0ca4e9d27ff826a109ecbf74754fb66b93f277a6",
                   "almach": "0x3725d52f3c252e8374999cc8617292ea2608ad88"}


def phi(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def p_up(s: float, s0: float, sigma_1s: float, tau: float) -> float:
    """Вероятность, что цена в конце окна выше старта (броуновское движение без сноса)."""
    if s <= 0 or s0 <= 0:
        return 0.5
    if tau <= 0:
        return 1.0 if s > s0 else 0.0
    sd = max(sigma_1s, 1e-6) * math.sqrt(tau)
    return phi(math.log(s / s0) / sd)


class Klines:
    """Посекундные цены закрытия из Binance (кэш по символу)."""

    def __init__(self, http: Http, base: str = BINANCE):
        self.http, self.base = http, base
        self.close: dict[str, dict[int, float]] = defaultdict(dict)
        self.requests = 0
        self._logs: dict[str, dict[int, float]] = {}

    def funcs(self, symbol: str):
        d = self._logs.get(symbol)
        if d is None or len(d) != len(self.close[symbol]):
            d = {k: math.log(v) for k, v in self.close[symbol].items() if v > 0}
            self._logs[symbol] = d
        return fvmodel.series_funcs(d)

    async def load(self, symbol: str, start: int, end: int):
        t = start
        while t < end:
            rows = await aget_retry(self.http, f"{self.base}/api/v3/klines",
                                        {"symbol": symbol, "interval": "1s",
                                         "startTime": t * 1000, "limit": 1000})
            self.requests += 1
            if not rows:
                t += 1000
                continue
            for r in rows:
                self.close[symbol][int(r[0]) // 1000] = float(r[4])
            t = int(rows[-1][0]) // 1000 + 1

    def at(self, symbol: str, ts: int, back: int = 5):
        c = self.close[symbol]
        for d in range(back + 1):
            if ts - d in c:
                return c[ts - d]
        return None

    def sigma(self, symbol: str, ts: int, window: int = 600) -> float | None:
        c = self.close[symbol]
        xs = [c[t] for t in range(ts - window, ts) if t in c]
        if len(xs) < window * 0.5:
            return None
        rets = [math.log(b / a) for a, b in zip(xs, xs[1:]) if a > 0 and b > 0]
        return statistics.pstdev(rets) if len(rets) > 10 else None


def bucket(x, edges, labels):
    for e, l in zip(edges, labels):
        if x < e:
            return l
    return labels[-1]


TAU_B = ([15, 30, 60, 120, 200, 1e9], ["<15с", "15-30с", "30-60с", "1-2м", "2-3.3м", ">3.3м"])
PRICE_B = ([0.2, 0.4, 0.6, 0.8, 0.9, 0.95, 1.01], ["<0.2", "0.2-0.4", "0.4-0.6", "0.6-0.8",
                                                 "0.8-0.9", "0.9-0.95", "≥0.95"])
EDGE_B = ([-0.10, -0.03, 0.0, 0.03, 0.10, 9], ["<-10¢", "-10..-3¢", "-3..0¢", "0..3¢", "3..10¢", ">10¢"])
Z_B = ([-1.5, -0.5, 0.5, 1.5, 99], ["сильно против", "против", "около нуля", "за", "сильно за"])


def rule_check(kl, markets: dict, winners: dict) -> str:
    """Какое правило расчёта совпадает с реальными итогами (по 1-секундным ценам Binance)."""
    stat = {r: [0, 0, 0, 0] for r in fvmodel.RULES}     # n, ok, n явных, ok явных
    for slug, t in markets.items():
        win = winners.get(slug)
        if win not in ("Up", "Down"):
            continue
        at, avg = kl.funcs(SYMBOL[t["asset"]])
        for rule in fvmodel.RULES:
            d = fvmodel.final_diff(rule, at, avg, t["start"], t["end"])
            if d is None:
                continue
            ok = (d >= 0) == (win == "Up")
            st = stat[rule]
            st[0] += 1
            st[1] += ok
            if abs(d) * 1e4 >= 1:
                st[2] += 1
                st[3] += ok
    L = ["\n--- КАКОЕ ПРАВИЛО РАСЧЁТА СОВПАДАЕТ С РЕАЛЬНЫМИ ИТОГАМИ (цены Binance 1с) ---",
         f"{'правило':<24}{'совпало':>16}{'в явных окнах (≥1 б.п.)':>26}"]
    names = {"spot": "spot (цена в конце)", "t60": "t60 (средн. 60 с)", "wt": "wt (средн. всего окна)"}
    for rule, (n, ok, cn, cok) in stat.items():
        if n:
            cl = f"{cok}/{cn} = {cok / cn:.1%}" if cn else "-"
            L.append(f"{names[rule]:<24}{f'{ok}/{n} = {ok / n:.1%}':>16}{cl:>22}")
    return "\n".join(L)


def analyse(rows: list[dict]) -> str:
    """rows: сделки с полями price, win, pnl_ps, cost, tau, edge, z_side, taker, p_model."""
    L = []

    def table(title, key, order):
        g = defaultdict(list)
        for r in rows:
            g[key(r)].append(r)
        L.append(f"\n--- {title} ---")
        L.append(f"{'группа':<16}{'сделок':>7}{'винрейт':>9}{'ср.цена':>9}{'PnL/акц':>9}"
                 f"{'ROI':>8}{'$ итого':>10}")
        for k in order:
            xs = g.get(k)
            if not xs:
                continue
            cost = sum(x["cost"] for x in xs)
            pnl = sum(x["pnl"] for x in xs)
            L.append(f"{k:<16}{len(xs):>7}{sum(x['win'] for x in xs) / len(xs):>8.0%} "
                     f"{statistics.mean(x['price'] for x in xs):>8.3f}"
                     f"{statistics.mean(x['pnl_ps'] for x in xs):>+9.3f}"
                     f"{(pnl / cost if cost else 0):>+8.1%}{pnl:>+10.0f}")

    n = len(rows)
    cost = sum(r["cost"] for r in rows)
    pnl = sum(r["pnl"] for r in rows)
    L.append(f"Сделок: {n}, вложено ${cost:,.0f}, PnL ${pnl:+,.0f} (ROI {pnl / cost if cost else 0:+.2%}), "
             f"винрейт {sum(r['win'] for r in rows) / n:.0%}, тейкером {sum(r['taker'] for r in rows) / n:.0%}")
    table("по времени до конца окна", lambda r: bucket(r["tau"], *TAU_B), TAU_B[1])
    table("по цене покупки", lambda r: bucket(r["price"], *PRICE_B), PRICE_B[1])
    table("по движению BTC от старта (в сторону его ставки)", lambda r: bucket(r["z_side"], *Z_B), Z_B[1])
    table("по перевесу модели (P(исход) − цена)", lambda r: bucket(r["edge"], *EDGE_B), EDGE_B[1])
    table("мейкер / тейкер", lambda r: "тейкер" if r["taker"] else "мейкер", ["мейкер", "тейкер"])
    # калибровка модели: P_model vs реальная частота
    L.append("\n--- калибровка модели (вероятность модели vs как вышло на самом деле) ---")
    L.append(f"{'P модели':<12}{'сделок':>7}{'реально':>9}{'цена рынка':>12}")
    g = defaultdict(list)
    for r in rows:
        g[min(int(r["p_model"] * 10), 9)].append(r)
    for k in range(10):
        xs = g.get(k)
        if xs:
            L.append(f"{k / 10:.1f}-{(k + 1) / 10:.1f}     {len(xs):>7}{sum(x['win'] for x in xs) / len(xs):>8.0%} "
                     f"{statistics.mean(x['price'] for x in xs):>11.3f}")
    for key, title in (("p_t60", "правило t60"), ("p_wt", "правило wt")):
        xs_all = [r for r in rows if r.get(key) is not None]
        if not xs_all:
            continue
        L.append(f"\n--- калибровка: {title} ---")
        L.append(f"{'P модели':<12}{'сделок':>7}{'реально':>9}{'цена рынка':>12}")
        g = defaultdict(list)
        for r in xs_all:
            g[min(int(r[key] * 10), 9)].append(r)
        for k in range(10):
            xs = g.get(k)
            if xs:
                L.append(f"{k / 10:.1f}-{(k + 1) / 10:.1f}     {len(xs):>7}{sum(x['win'] for x in xs) / len(xs):>8.0%} "
                         f"{statistics.mean(x['price'] for x in xs):>11.3f}")
    return "\n".join(L)


async def aget_retry(http, url, params, tries: int = 8):
    """Повтор с нарастающей паузой: Polymarket и Binance отвечают 429 при частых запросах."""
    delay = 2.0
    for i in range(tries):
        try:
            return await http.aget(url, params)
        except Exception as e:  # noqa: BLE001
            if i == tries - 1:
                raise
            msg = str(e)
            wait = delay * (3 if "429" in msg else 1)
            print(f"    повтор через {wait:.0f} с ({msg[:60]})", flush=True)
            await asyncio.sleep(wait)
            delay = min(delay * 2, 60)


async def collect(http: Http, wallet: str, max_trades: int, base_data: str) -> list:
    out, off = [], 0
    while off <= 5000 and len(out) < max_trades:
        page = await aget_retry(http, f"{base_data}/activity", {"user": wallet, "type": "TRADE",
                                                                "limit": 500, "offset": off})
        await asyncio.sleep(0.5)
        if not page:
            break
        out += page
        off += 500
        if len(page) < 500:
            break
    return out[:max_trades]


async def run(args) -> int:
    http = Http(timeout=15)
    wallets = {args.name: args.wallet.lower()} if args.wallet else DEFAULT_WALLETS
    kl = Klines(http, args.binance)
    winners: dict[str, str | None] = {}
    report = []
    dataset = []
    for name, w in wallets.items():
        t0 = time.time()
        print(f"\n=== {name}: загружаю сделки...", flush=True)
        acts = await collect(http, w, args.max_trades, args.data)
        trades = []
        for a in acts:
            m = SLUG_RE.match(str(a.get("slug") or ""))
            if not m or str(a.get("side")).upper() != "BUY":
                continue
            price, size = float(a.get("price") or 0), float(a.get("size") or 0)
            if not (0 < price < 1) or size <= 0:
                continue
            asset, dur, start = m.group(1), DUR[m.group(2)], int(m.group(3))
            trades.append({"slug": a["slug"], "asset": asset, "start": start, "end": start + dur,
                           "ts": int(float(a["timestamp"])), "price": price, "size": size,
                           "usdc": float(a.get("usdcSize") or price * size),
                           "outcome": str(a.get("outcome", ""))})
        if not trades:
            print("  крипто-сделок не найдено")
            continue
        print(f"  сделок на крипто-окнах: {len(trades)} из {len(acts)}; "
              f"период {time.strftime('%d.%m %H:%M', time.gmtime(min(t['ts'] for t in trades)))}"
              f" – {time.strftime('%d.%m %H:%M', time.gmtime(max(t['ts'] for t in trades)))} UTC", flush=True)
        # итоги рынков
        slugs = sorted({t["slug"] for t in trades} - set(winners))
        print(f"  итоги рынков: {len(slugs)}...", flush=True)
        sem = asyncio.Semaphore(6)

        async def res(slug):
            async with sem:
                try:
                    r = await aget_retry(http, f"{args.gamma}/markets", {"slug": slug, "closed": "true"}, 5)
                    mk = r[0] if isinstance(r, list) and r else None
                except Exception:  # noqa: BLE001
                    mk = None
                win = None
                if mk:
                    outs = mk.get("outcomes")
                    prices = mk.get("outcomePrices")
                    outs = json.loads(outs) if isinstance(outs, str) else outs
                    prices = json.loads(prices) if isinstance(prices, str) else prices
                    try:
                        for o, p in zip(outs or [], prices or []):
                            if float(p) > 0.99:
                                win = str(o)
                    except (TypeError, ValueError):
                        pass
                winners[slug] = win
        await asyncio.gather(*(res(s) for s in slugs))
        # цены Binance
        for asset in sorted({t["asset"] for t in trades}):
            sym = SYMBOL[asset]
            # грузим только нужные куски: [старт окна − 10 мин, конец окна], склеенные
            spans = sorted({(t["start"] - 660, t["end"] + 2) for t in trades if t["asset"] == asset})
            merged = []
            for a, b in spans:
                if merged and a <= merged[-1][1]:
                    merged[-1][1] = max(merged[-1][1], b)
                else:
                    merged.append([a, b])
            total = sum(b - a for a, b in merged)
            print(f"  свечи {sym} 1с: {total / 3600:.1f} ч в {len(merged)} кусках...", flush=True)
            for a, b in merged:
                await kl.load(sym, a, b)
        rows = []
        skipped = defaultdict(int)
        for t in trades:
            win = winners.get(t["slug"])
            if win is None:
                skipped["рынок не рассчитан"] += 1
                continue
            sym = SYMBOL[t["asset"]]
            s0 = kl.at(sym, t["start"])
            s = kl.at(sym, t["ts"] - 1)            # что было известно секундой раньше
            sig = kl.sigma(sym, t["ts"] - 1)
            if not s0 or not s or not sig:
                skipped["нет свечей"] += 1
                continue
            tau = t["end"] - t["ts"]
            pu = p_up(s, s0, sig, max(tau, 0))
            is_up = t["outcome"].lower() == "up"
            p_side = pu if is_up else 1 - pu
            z = math.log(s / s0) / (sig * math.sqrt(max(tau, 1)))
            fee_coef = (t["usdc"] - t["price"] * t["size"]) / (t["size"] * t["price"] * (1 - t["price"]))
            taker = fee_coef > 0.02
            fee_ps = max(0.0, (t["usdc"] - t["price"] * t["size"]) / t["size"])
            won = (win.lower() == t["outcome"].lower())
            pnl_ps = (1 - t["price"] if won else -t["price"]) - fee_ps
            at, avg = kl.funcs(sym)
            pr = {}
            for rule in fvmodel.RULES:
                p = fvmodel.prob_up(rule, at, avg, t["start"], t["end"], t["ts"] - 1, math.log(s), sig)
                if p is not None:
                    pr[rule] = p if is_up else 1 - p
            rows.append({"p_t60": pr.get("t60"), "p_wt": pr.get("wt"),
                         "price": t["price"], "win": won, "pnl_ps": pnl_ps, "cost": t["usdc"],
                         "pnl": pnl_ps * t["size"], "tau": tau, "edge": p_side - t["price"],
                         "z_side": z if is_up else -z, "taker": taker, "p_model": p_side,
                         "move_bps": (s / s0 - 1) * 1e4, "sigma": sig, "asset": t["asset"],
                         "slug": t["slug"], "outcome": t["outcome"], "ts": t["ts"], "who": name})
        print(f"  разобрано {len(rows)} (пропущено: {dict(skipped)}), {time.time() - t0:.0f} с", flush=True)
        if rows:
            txt = f"\n################ {name} ################\n" + analyse(rows)
            txt += "\n" + rule_check(kl, {t["slug"]: t for t in trades}, winners)
            print(txt, flush=True)
            report.append(txt)
            dataset += rows
    (HERE / "research_report.txt").write_text("\n".join(report))
    with (HERE / "research_trades.jsonl").open("w") as f:
        for r in dataset:
            f.write(json.dumps(r) + "\n")
    print(f"\nОтчёт: research_report.txt, все сделки с признаками: research_trades.jsonl "
          f"(запросов к Binance: {kl.requests})")
    return 0


def main():
    ap = argparse.ArgumentParser(description="Разбор стратегии крипто-ботов Polymarket")
    ap.add_argument("--wallet")
    ap.add_argument("--name", default="trader")
    ap.add_argument("--max-trades", type=int, default=5500)
    ap.add_argument("--data", default=DATA, help=argparse.SUPPRESS)
    ap.add_argument("--gamma", default=GAMMA, help=argparse.SUPPRESS)
    ap.add_argument("--binance", default=BINANCE, help=argparse.SUPPRESS)
    sys.exit(asyncio.run(run(ap.parse_args())))


if __name__ == "__main__":
    main()
