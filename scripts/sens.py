#!/usr/bin/env python3
"""Проверка на чувствительность: что останется от прибыли в реальности.

Симулятор оптимистичен: он покупает по цене из стакана и забирает все сделки.
В жизни хуже по двум причинам:
  1) ЦЕНА. Между решением и исполнением проходит время, цена уползает.
     Проверяем сдвиг цены исполнения на +0 / +1 / +2 цента.
  2) КОНКУРЕНЦИЯ. Самые выгодные билеты заберут более быстрые боты.
     Проверяем выброс 25% и 50% сделок с наибольшим перевесом (edge_fill).

Если плюс держится при +1 центе и выбросе 25% — стратегия с запасом.
Если пропадает — она жила на тонкой кромке, реальные деньги ставить нельзя.

Запуск:  python3 scripts/sens.py [папка_данных] [--min-n 30]
"""
from __future__ import annotations

import argparse
import csv
import math
import statistics
from pathlib import Path

FEE_COEF = 0.07


def fee(p: float, shares: float) -> float:
    return FEE_COEF * p * (1 - p) * shares


def load(d: Path):
    win: dict[str, str] = {}
    for p in sorted(d.glob("outcomes*.csv")):
        with p.open() as fh:
            for r in csv.DictReader(fh):
                if r.get("winner") in ("Up", "Down"):
                    win[r["slug"]] = r["winner"]
    trades: dict[str, list[dict]] = {}
    with (d / "trades.csv").open() as fh:
        for r in csv.DictReader(fh):
            w = win.get(r["slug"])
            if w is None:
                continue                       # окно ещё не рассчитано
            try:
                t = {"v": r["variant"], "shares": float(r["shares"]),
                     "vwap": float(r["fill_vwap"]), "win": r["side"] == w,
                     "edge": float(r.get("edge_fill") or 0.0),
                     "t": r.get("time_utc") or ""}
            except (TypeError, ValueError):
                continue
            trades.setdefault(t["v"], []).append(t)
    return trades


def pnl(ts: list[dict], slip: float):
    """Итог группы сделок при сдвиге цены исполнения на slip (в долях доллара)."""
    tot = cost_tot = 0.0
    wins = 0
    for t in ts:
        p = min(0.99, t["vwap"] + slip)
        cost = t["shares"] * p + fee(p, t["shares"])
        got = t["shares"] if t["win"] else 0.0
        tot += got - cost
        cost_tot += cost
        wins += t["win"]
    roi = tot / cost_tot * 100 if cost_tot else 0.0
    return tot, roi, wins


def each_pnl(ts: list[dict], slip: float = 0.0) -> list[float]:
    out = []
    for t in ts:
        p = min(0.99, t["vwap"] + slip)
        out.append((t["shares"] if t["win"] else 0.0) - (t["shares"] * p + fee(p, t["shares"])))
    return out


def zscore(xs: list[float]) -> float:
    """Во сколько раз средний результат больше своей погрешности.
    z около 0 — результат объясняется случайностью."""
    n = len(xs)
    if n < 2:
        return 0.0
    sd = statistics.pstdev(xs)
    m = statistics.mean(xs)
    if sd <= 0:                      # все сделки одинаковы — разброса нет
        return 99.0 if m > 0 else 0.0
    return m / (sd / math.sqrt(n))


def z_threshold(k: int) -> float:
    """Порог z с поправкой на перебор k вариантов: при k вариантах самый удачливый
    почти всегда даёт z около 2 просто так. Грубая, но честная поправка."""
    k = max(1, k)
    return 2.0 + 0.55 * math.log(k)


def top_share(xs: list[float], m: int = 3) -> float:
    """Какую долю всего плюса дают m самых крупных выигрышей.
    Близко к 100% — плюс держится на нескольких случайных сделках."""
    tot = sum(xs)
    if tot <= 0:
        return 0.0
    return min(999.0, sum(sorted(xs, reverse=True)[:m]) / tot * 100)


def halves(ts: list[dict]) -> tuple[float, float, int, int]:
    """Делит сделки по времени на две половины: настоящее преимущество есть в обеих."""
    s = sorted(ts, key=lambda t: t["t"])
    half = len(s) // 2
    a, b = each_pnl(s[:half]), each_pnl(s[half:])
    return sum(a), sum(b), len(a), len(b)


def drop_best(ts: list[dict], frac: float):
    """Убирает frac сделок с наибольшим перевесом — их заберут быстрые боты."""
    if frac <= 0:
        return ts
    s = sorted(ts, key=lambda t: -t["edge"])
    return s[int(len(s) * frac):]


def report(d: Path, min_n: int) -> str:
    trades = load(d)
    if not trades:
        return f"Нет рассчитанных сделок в {d}"
    big = {v: ts for v, ts in trades.items() if len(ts) >= min_n}
    thr = z_threshold(len(trades))
    slips = (0.0, 0.01, 0.02)
    drops = (0.0, 0.25, 0.50)
    L = [f"=== Чувствительность и значимость: {d} ===",
         f"Вариантов всего {len(trades)}, из них с ≥{min_n} сделок — {len(big)}.",
         "",
         "ЧАСТЬ 1. Это вообще не случайность?",
         "  z — во сколько раз средний результат больше своей погрешности.",
         f"  Мы перебрали {len(trades)} вариантов, поэтому порог поднят до z ≥ {thr:.1f}:",
         "  при таком переборе самый удачливый вариант даёт z около 2 просто по удаче.",
         "  «топ-3» — какую долю плюса дают три самых крупных выигрыша.",
         "  «половины» — результат первой и второй половины сделок по времени.",
         "",
         f"{'вариант':<20} {'сделок':>6} {'PnL $':>8} {'z':>6} {'топ-3':>7} "
         f"{'1-я пол.':>10} {'2-я пол.':>10}  вывод"]
    proven = []
    for v in sorted(big):
        ts = big[v]
        xs = each_pnl(ts)
        z = zscore(xs)
        h1, h2, n1, n2 = halves(ts)
        ok = z >= thr and h1 > 0 and h2 > 0
        if ok:
            proven.append(v)
        L.append(f"{v:<20} {len(ts):>6} {sum(xs):>+8.1f} {z:>6.1f} {top_share(xs):>6.0f}% "
                 f"{h1:>+10.1f} {h2:>+10.1f}  {'доказано' if ok else 'шум, не доказано'}")
    L += ["", f"Прошли проверку на значимость: {len(proven)} из {len(big)}.", ""]
    if not proven:
        L += ["Ни один вариант не доказал преимущества. Плюс у лучших объясняется",
              "тем, что вариантов много и сделок мало. Реальные деньги ставить нельзя —",
              "нужно копить данные дальше и проверять на НОВЫХ окнах.", ""]
    L += ["ЧАСТЬ 2. Что будет, если исполнение хуже.",
          "  Строка — сколько сделок отобрала конкуренция (убираем лучшие по перевесу),",
          "  столбец — насколько хуже цена исполнения. В клетке: PnL $ / ROI %.",
          "  У настоящего преимущества числа падают ПЛАВНО сверху вниз.",
          "  Если строка «−50%» лучше строки «−25%» — результат держится на случайных",
          "  сделках, а не на правиле.", ""]
    for v in sorted(big):
        ts = big[v]
        base_pnl, base_roi, wins = pnl(ts, 0.0)
        L.append(f"--- {v}: {len(ts)} сделок, выигрышей {wins / len(ts) * 100:.0f}%, "
                 f"как есть {base_pnl:+.2f}$ ({base_roi:+.1f}%)")
        L.append(f"{'конкуренция':<14}" + "".join(f"{f'+{s * 100:.0f}¢':>16}" for s in slips))
        cells = {}
        for dr in drops:
            kept = drop_best(ts, dr)
            row = f"{f'−{dr * 100:.0f}% лучших':<14}"
            for s in slips:
                if len(kept) < 5:
                    row += f"{'мало':>16}"
                    continue
                p_, r_, _ = pnl(kept, s)
                cells[(dr, s)] = p_
                row += f"{f'{p_:+.1f}$ / {r_:+.1f}%':>16}"
            L.append(row)
        a, b, c = cells.get((0.0, 0.0)), cells.get((0.25, 0.0)), cells.get((0.50, 0.0))
        if None not in (a, b, c) and not (a >= b >= c):
            L.append("  ! порядок нарушен: выбросили больше лучших — стало лучше. Это шум.")
        L.append("")
    L += ["Важно: это те же данные, на которых варианты и выбирались.",
          "Ни значимость, ни чувствительность не заменяют экзамен на НОВЫХ окнах."]
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description="Чувствительность результатов тейкера к цене и конкуренции")
    ap.add_argument("data", nargs="?", default="data_fv_lag")
    ap.add_argument("--min-n", type=int, default=30)
    a = ap.parse_args()
    print(report(Path(a.data), a.min_n))


if __name__ == "__main__":
    main()
