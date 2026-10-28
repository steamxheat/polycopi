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
                     "edge": float(r.get("edge_fill") or 0.0)}
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
    slips = (0.0, 0.01, 0.02)
    drops = (0.0, 0.25, 0.50)
    L = [f"=== Чувствительность: {d} ===",
         "Строка — сколько сделок убрала конкуренция, столбец — насколько хуже цена.",
         "В клетке: PnL $ / ROI %. Нужен плюс в клетке «−25% / +1 цент» и правее.", ""]
    survivors = []
    for v in sorted(trades):
        ts = trades[v]
        if len(ts) < min_n:
            continue
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
        key = cells.get((0.25, 0.01))
        hard = cells.get((0.50, 0.02))
        verdict = ("ЗАПАС ЕСТЬ" if key and key > 0 and hard and hard > 0 else
                   "кромка: держится только при мягких допущениях" if key and key > 0 else
                   "НЕ ДЕРЖИТСЯ")
        L.append(f"  вывод: {verdict}")
        L.append("")
        if key and key > 0:
            survivors.append((v, key, len(ts)))
    L.append(f"Пережили «−25% лучших + 1 цент»: {len(survivors)} вариантов "
             f"(с ≥{min_n} сделок).")
    for v, p_, n in sorted(survivors, key=lambda x: -x[1]):
        L.append(f"  {v:<22} {p_:+.2f}$ на {n} сделках")
    L += ["", "Важно: это всё ещё те же данные, на которых варианты выбирались.",
          "Проверка на чувствительность не заменяет экзамен на НОВЫХ данных."]
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description="Чувствительность результатов тейкера к цене и конкуренции")
    ap.add_argument("data", nargs="?", default="data_fv_lag")
    ap.add_argument("--min-n", type=int, default=30)
    a = ap.parse_args()
    print(report(Path(a.data), a.min_n))


if __name__ == "__main__":
    main()
