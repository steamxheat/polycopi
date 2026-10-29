#!/usr/bin/env python3
"""Крайние сделки варианта: самые крупные выигрыши и проигрыши, размах ставок.

Средние числа скрывают главное: ровная ли кривая счёта или всё держится на
нескольких удачах. Здесь видно каждую крайность — сколько вложили, по какой
цене, сколько получили.

  python3 scripts/extremes.py [папка] [вариант] [--top 5]
  python3 scripts/extremes.py data_fv_lag lag3-1s-t60
"""
from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path

HERE = Path(__file__).resolve().parent


def load(d: Path, variant: str | None):
    st = json.loads((d / "state.json").read_text())
    out = {}
    for name, v in st.get("variants", {}).items():
        if variant and name != variant:
            continue
        ts = []
        for t in v.get("trades", []):
            if t.get("pnl") is None:
                continue
            try:
                ts.append({"pnl": float(t["pnl"]), "cost": float(t["cost"]),
                           "shares": float(t["shares"]), "vwap": float(t["vwap"]),
                           "won": bool(t.get("won")), "t": float(t.get("t") or 0),
                           "slug": t.get("slug", ""), "side": t.get("side", ""),
                           "tau": float(t.get("tau") or 0)})
            except (TypeError, ValueError, KeyError):
                continue
        if ts:
            out[name] = ts
    return out


def line(t: dict) -> str:
    kind = "выигрыш" if t["won"] else "сгорел "
    asset = t["slug"].split("-")[0].upper() if t["slug"] else "?"
    win = t["slug"].split("-")[2] if t["slug"].count("-") >= 2 else ""
    return (f"  {kind} {t['pnl']:>+7.2f}$  вложено {t['cost']:>5.2f}$  "
            f"{t['shares']:>5.1f} шт по {t['vwap']:.2f}  {asset} {win} {t['side']:<4} "
            f"за {t['tau']:.0f}с до конца")


BUCKETS = [(0.00, 0.15, "дешёвые 5-15¢"), (0.15, 0.30, "15-30¢"), (0.30, 0.50, "30-50¢"),
           (0.50, 0.70, "50-70¢"), (0.70, 0.85, "70-85¢"), (0.85, 1.01, "фавориты 85-95¢")]


def by_price(ts: list[dict]) -> list[str]:
    """Откуда на самом деле берётся прибыль. Крупные выигрыши всегда выпадают на
    дешёвые билеты просто потому, что они платят в 20 раз больше вложенного —
    поэтому смотреть надо не на крайности, а на итог каждой ценовой группы."""
    tot = sum(t["pnl"] for t in ts)
    L = ["  --- откуда берётся прибыль (по цене покупки) ---",
         f"  {'цена':<18}{'сделок':>7}{'выигр':>7}{'итог $':>9}{'ROI':>8}{'доля прибыли':>14}"]
    for lo, hi, name in BUCKETS:
        g = [t for t in ts if lo <= t["vwap"] < hi]
        if not g:
            continue
        pnl = sum(t["pnl"] for t in g)
        cost = sum(t["cost"] for t in g)
        wr = sum(1 for t in g if t["won"]) / len(g)
        share = pnl / tot * 100 if tot else 0
        L.append(f"  {name:<18}{len(g):>7}{wr:>6.0%}{pnl:>+9.2f}"
                 f"{pnl / cost * 100 if cost else 0:>+7.0f}%{share:>13.0f}%")
    return L


def report(d: Path, variant: str | None, top: int) -> str:
    data = load(d, variant)
    if not data:
        return f"Нет рассчитанных сделок в {d}" + (f" у варианта {variant}" if variant else "")
    L = []
    for name, ts in sorted(data.items()):
        pnl = [t["pnl"] for t in ts]
        costs = [t["cost"] for t in ts]
        prices = [t["vwap"] for t in ts]
        wins = [t for t in ts if t["won"]]
        tot = sum(pnl)
        L += [f"=== {name}: {len(ts)} сделок, выигрышей {len(wins) / len(ts):.0%}, "
              f"итог {tot:+.2f}$ ===", ""]
        L += [f"  ставка:  от {min(costs):.2f}$ до {max(costs):.2f}$  "
              f"(медиана {statistics.median(costs):.2f}$)",
              f"  цена:    от {min(prices):.2f} до {max(prices):.2f}  "
              f"(медиана {statistics.median(prices):.2f})", ""]
        L += by_price(ts) + [""]
        best = sorted(ts, key=lambda t: -t["pnl"])[:top]
        worst = sorted(ts, key=lambda t: t["pnl"])[:top]
        L += [f"  --- {len(best)} самых крупных выигрышей ---"] + [line(t) for t in best]
        L += ["", f"  --- {len(worst)} самых крупных проигрышей ---"] + [line(t) for t in worst]
        if tot > 0:
            share = sum(t["pnl"] for t in best) / tot * 100
            L += ["", f"  Эти {len(best)} выигрышей дают {share:.0f}% всей прибыли."]
            L.append("  Больше 100% — значит остальные сделки в сумме в минусе, и весь"
                     if share > 100 else
                     "  Меньше ~40% — прибыль размазана, кривая счёта ровная." if share < 40 else
                     "  40-100% — заметная часть прибыли на нескольких сделках.")
            if share > 100:
                L.append("  плюс держится на этой горстке. Это хрупко.")
        streak = cur = 0
        for t in ts:
            cur = cur + 1 if not t["won"] else 0
            streak = max(streak, cur)
        L += ["", f"  Худшая серия подряд без выигрыша: {streak} сделок.",
              f"  Самый большой убыток за одну сделку: {min(pnl):.2f}$.", ""]
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description="Крайние сделки: самые крупные выигрыши и проигрыши")
    ap.add_argument("data", nargs="?", default=str(HERE.parent / "data_fv_lag"))
    ap.add_argument("variant", nargs="?", default="lag3-1s-t60")
    ap.add_argument("--top", type=int, default=5)
    ap.add_argument("--all", action="store_true", help="все варианты, не один")
    a = ap.parse_args()
    print(report(Path(a.data), None if a.all else a.variant, a.top))


if __name__ == "__main__":
    main()
