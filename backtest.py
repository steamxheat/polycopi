#!/usr/bin/env python3
"""Массовая проверка ~100 простых стратегий тейкера на уже записанных данных.

Берёт посекундные «снимки» рынков, которые пишет polyfv-sim (data_fv/snapshots*.csv:
цены Up/Down, вероятности модели, время до конца, движение цены от старта) и итоги
окон (outcomes.csv). Для каждой стратегии: в каждом окне — не больше одного входа на
сторону (первый момент, когда сработало правило), покупка по аску + комиссия тейкера,
держим до расчёта.

Защита от самообмана при 100 стратегиях (часть «выиграет» случайно):
  * данные делятся по времени на две половины — стратегия должна быть в плюсе в ОБЕИХ;
  * z = средний результат / его погрешность — нужно z ≥ 2;
  * минимум сделок в каждой половине.

  python3 backtest.py [--data data_fv] [--min-n 30] [--top 25]
"""
from __future__ import annotations

import argparse
import csv
import glob
import math
import statistics
from pathlib import Path

HERE = Path(__file__).resolve().parent
FEE = 0.07


def fee(p: float) -> float:
    return FEE * p * (1 - p)


def f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def load(data: Path):
    snaps: dict[str, list] = {}
    for path in sorted(glob.glob(str(data / "snapshots*.csv"))):
        with open(path, newline="") as fh:
            for r in csv.DictReader(fh):
                row = {"t": f(r.get("t")), "tau": f(r.get("tau")), "ratio": f(r.get("ratio_bps")),
                       "ua": f(r.get("up_ask")), "da": f(r.get("down_ask")),
                       "p_spot": f(r.get("p_spot")), "p_t60": f(r.get("p_t60")), "p_wt": f(r.get("p_wt")),
                       "asset": r.get("asset"), "window": r.get("window")}
                if row["t"] is None or row["tau"] is None:
                    continue
                snaps.setdefault(r["slug"], []).append(row)
    winners = {}
    for path in sorted(glob.glob(str(data / "outcomes*.csv"))):
        with open(path, newline="") as fh:
            for r in csv.DictReader(fh):
                if r.get("winner") in ("Up", "Down"):
                    winners[r["slug"]] = r["winner"]
    for rows in snaps.values():
        rows.sort(key=lambda x: x["t"])
    return snaps, winners


# ------------------------------------------------------------------ стратегии
# правило: (снимок, сторона) -> True, если покупаем эту сторону сейчас.

def side_vals(r, side):
    ask = r["ua"] if side == "Up" else r["da"]
    pm = {k: (r[k] if side == "Up" else (1 - r[k] if r[k] is not None else None)) for k in ("p_spot", "p_t60", "p_wt")}
    mv = r["ratio"] if side == "Up" else (-r["ratio"] if r["ratio"] is not None else None)
    return ask, pm, mv


def tau_ok(tau, band):
    lo, hi = band
    return lo <= tau < hi


TAUS = {"все": (8, 1e9), "ранн>180": (180, 1e9), "сред60-180": (60, 180), "поздн<60": (8, 60)}


def build():
    S = []

    # A. перевес модели (разные правила расчёта, пороги, время окна)
    for rule in ("p_spot", "p_t60", "p_wt"):
        for edge in (0.02, 0.04, 0.06, 0.08):
            for tname, band in TAUS.items():
                def rule_fn(r, side, rule=rule, edge=edge, band=band):
                    ask, pm, _ = side_vals(r, side)
                    p = pm[rule]
                    return (ask is not None and p is not None and 0.05 <= ask <= 0.95 and tau_ok(r["tau"], band)
                            and p - ask - fee(ask) >= edge)
                S.append((f"модель {rule[2:]} ≥{edge * 100:.0f}¢ {tname}", rule_fn))

    # B. фавориты в конце окна (рынок часто «недоверяет» почти решённому исходу)
    for lo, hi in ((0.80, 0.90), (0.90, 0.95), (0.95, 0.985)):
        for tmax in (30, 60, 120):
            for agree in (False, True):
                def fav(r, side, lo=lo, hi=hi, tmax=tmax, agree=agree):
                    ask, pm, _ = side_vals(r, side)
                    if ask is None or not (lo <= ask < hi) or not (8 <= r["tau"] < tmax):
                        return False
                    return not agree or (pm["p_t60"] is not None and pm["p_t60"] >= ask + fee(ask))
                S.append((f"фаворит {lo:.2f}-{hi:.2f} <{tmax}с{' +модель' if agree else ''}", fav))

    # C. дешёвые «лонгшоты»
    for lo, hi in ((0.03, 0.10), (0.10, 0.20), (0.20, 0.30)):
        for tname in ("ранн>180", "сред60-180", "поздн<60"):
            band = TAUS[tname]

            def longshot(r, side, lo=lo, hi=hi, band=band):
                ask, _, _ = side_vals(r, side)
                return ask is not None and lo <= ask < hi and tau_ok(r["tau"], band)
            S.append((f"лонгшот {lo:.2f}-{hi:.2f} {tname}", longshot))

    # D. по движению (цена уже ушла от старта -> покупаем ту же сторону, если ещё не дорого)
    for x in (2, 5, 10):
        for cap in (0.60, 0.75, 0.90):
            def mom(r, side, x=x, cap=cap):
                ask, _, mv = side_vals(r, side)
                return ask is not None and mv is not None and mv >= x and ask <= cap and r["tau"] >= 60
            S.append((f"движение ≥{x}бп, аск ≤{cap:.2f}", mom))

    # E. против движения (ставка на откат)
    for x in (3, 6, 10):
        for cap in (0.30, 0.40):
            def rev(r, side, x=x, cap=cap):
                ask, _, mv = side_vals(r, side)
                return ask is not None and mv is not None and mv <= -x and ask <= cap and r["tau"] >= 90
            S.append((f"откат: против ≥{x}бп, аск ≤{cap:.2f}", rev))

    # F. по монетам и длине окна — лучшие базовые правила отдельно
    for asset in ("btc", "eth"):
        for win in ("5m", "15m"):
            for edge in (0.04, 0.06):
                def split(r, side, asset=asset, win=win, edge=edge):
                    if r["asset"] != asset or r["window"] != win:
                        return False
                    ask, pm, _ = side_vals(r, side)
                    p = pm["p_t60"]
                    return ask is not None and p is not None and 0.05 <= ask <= 0.95 and r["tau"] >= 60 \
                        and p - ask - fee(ask) >= edge
                S.append((f"модель t60 ≥{edge * 100:.0f}¢ {asset.upper()} {win}", split))
    return S


# ------------------------------------------------------------------ прогон

def run(snaps, winners, strategies):
    slugs = sorted((s for s in snaps if s in winners), key=lambda s: snaps[s][0]["t"])
    if not slugs:
        return [], 0, None
    mid_t = snaps[slugs[len(slugs) // 2]][0]["t"]
    res = []
    for name, fn in strategies:
        trades = []
        for slug in slugs:
            win = winners[slug]
            for side in ("Up", "Down"):
                for r in snaps[slug]:
                    try:
                        ok = fn(r, side)
                    except (TypeError, KeyError):
                        ok = False
                    if ok:
                        ask = r["ua"] if side == "Up" else r["da"]
                        cost = ask + fee(ask)
                        pnl = (1.0 if side == win else 0.0) - cost
                        trades.append((r["t"] < mid_t, pnl, cost))
                        break
        res.append((name, trades))
    return res, len(slugs), mid_t


def summarize(trades):
    if not trades:
        return None
    pn = [p for _, p, _ in trades]
    cost = sum(c for _, _, c in trades)
    n = len(pn)
    sd = statistics.pstdev(pn) if n > 1 else 0
    z = statistics.mean(pn) / (sd / math.sqrt(n)) if sd > 0 else 0
    halves = []
    for h in (True, False):
        xs = [(p, c) for first, p, c in trades if first == h]
        halves.append((len(xs), sum(p for p, _ in xs) / sum(c for _, c in xs) * 100 if xs else None))
    return {"n": n, "win": sum(1 for p in pn if p > 0) / n * 100, "roi": sum(pn) / cost * 100,
            "pnl": sum(pn), "z": z, "h1": halves[0], "h2": halves[1]}


def report(data: Path, min_n: int, top: int) -> str:
    snaps, winners = load(data)
    strategies = build()
    res, n_mk, _ = run(snaps, winners, strategies)
    if not res:
        return f"Нет данных: снимков рынков {len(snaps)}, итогов {len(winners)} в {data}"
    rows = []
    for name, trades in res:
        s = summarize(trades)
        if s:
            rows.append((name, s))
    rows.sort(key=lambda x: -x[1]["roi"])
    L = [f"=== {len(strategies)} стратегий на {n_mk} рассчитанных окнах ({data}) ===",
         "Покупка по аску + комиссия, 1 вход на сторону в окне, держим до итога. PnL — на 1 акцию.",
         f"✅ = в плюсе в ОБЕИХ половинах по времени, ≥{min_n} сделок в каждой, z ≥ 2", "",
         f"{'стратегия':<40} {'сделок':>6} {'выигр':>6} {'ROI':>7} {'1-я пол.':>12} {'2-я пол.':>12} {'z':>5}"]
    good = 0
    for name, s in rows[:top]:
        (n1, r1), (n2, r2) = s["h1"], s["h2"]
        ok = (r1 is not None and r2 is not None and r1 > 0 and r2 > 0 and n1 >= min_n and n2 >= min_n
              and s["z"] >= 2)
        good += ok
        h1 = f"{r1:+.1f}%/{n1}" if r1 is not None else "-"
        h2 = f"{r2:+.1f}%/{n2}" if r2 is not None else "-"
        L.append(f"{name[:40]:<40} {s['n']:>6} {s['win']:>5.0f}% {s['roi']:>+6.1f}% {h1:>12} {h2:>12} "
                 f"{s['z']:>5.1f} {'✅' if ok else ''}")
    tot_ok = sum(1 for _, s in rows if s["h1"][1] and s["h2"][1] and s["h1"][1] > 0 and s["h2"][1] > 0
                 and s["h1"][0] >= min_n and s["h2"][0] >= min_n and s["z"] >= 2)
    L += ["", f"Прошли все фильтры: {tot_ok} из {len(rows)} стратегий со сделками.",
          "Напоминание: из 100 случайных стратегий ~2–5 могут пройти фильтры по чистой удаче —",
          "прошедшие идут в живую проверку на НОВЫХ данных, только потом можно им верить."]
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description="Массовая проверка стратегий тейкера на записанных снимках")
    ap.add_argument("--data", default=str(HERE / "data_fv"))
    ap.add_argument("--min-n", type=int, default=30)
    ap.add_argument("--top", type=int, default=25)
    a = ap.parse_args()
    print(report(Path(a.data), a.min_n, a.top))


if __name__ == "__main__":
    main()
