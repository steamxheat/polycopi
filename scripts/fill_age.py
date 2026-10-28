#!/usr/bin/env python3
"""Делит сделки тейкера по «возрасту» стакана в момент покупки и считает результат каждой группы.
Если плюс дают только сделки со «старым» стаканом — плюс подозрительный.
Запуск: python3 scripts/fill_age.py [папка_данных]   (по умолчанию data_fv_lag)"""
import csv
import sys
from pathlib import Path

d = Path(sys.argv[1] if len(sys.argv) > 1 else "data_fv_lag")
win = {}
for p in d.glob("outcomes*.csv"):
    with p.open() as fh:
        for r in csv.DictReader(fh):
            win[r["slug"]] = r.get("winner")
groups = {"свежий <200мс": [], "200мс–1с": [], "старый >1с": [], "без метки": []}
with (d / "trades.csv").open() as fh:
    for r in csv.DictReader(fh):
        w = win.get(r["slug"])
        if w not in ("Up", "Down"):
            continue                      # окно ещё не рассчитано
        pnl = float(r["shares"]) * (1.0 if r["side"] == w else 0.0) - float(r["cost"])
        try:
            age = float(r.get("book_age_ms") or "x")
        except ValueError:
            groups["без метки"].append((pnl, float(r["cost"])))
            continue
        key = "свежий <200мс" if age < 200 else "200мс–1с" if age < 1000 else "старый >1с"
        groups[key].append((pnl, float(r["cost"])))
print(f"{'стакан':<15} {'сделок':>6} {'выигр':>6} {'PnL $':>8} {'ROI':>7}")
for k, xs in groups.items():
    if not xs:
        continue
    pnl, cost = sum(p for p, _ in xs), sum(c for _, c in xs)
    print(f"{k:<15} {len(xs):>6} {sum(1 for p, _ in xs if p > 0) / len(xs) * 100:>5.0f}% "
          f"{pnl:>+8.2f} {pnl / cost * 100:>+6.1f}%")
