#!/usr/bin/env python3
"""Запас прочности по ценовым диапазонам. Денег не тратит, ничего не шлёт.

Дешёвый диапазон нас обманул: симулятор дал там +$76 на 23 сделках, а живой
бот проиграл 9 из 9. Вопрос — есть ли такой же обман в ядре 50-70¢.

Мерим не "сколько заработали", а СКОЛЬКО МОЖЕМ ПОТЕРЯТЬ НА ХОДУ И ОСТАТЬСЯ
В ПЛЮСЕ. Арифметика простая: если платим за билет на s дороже, прибыль
с билета падает ровно на s. Значит средняя прибыль на билет И ЕСТЬ запас:
столько центов проскальзывания диапазон выдерживает до нуля.

Это честнее winrate: диапазон может выигрывать 74% и всё равно не иметь
запаса, если билеты дорогие.

  python3 scripts/slip.py data_fv_lag [--variant lag3-1s-t60] [--fill data_fill]
"""
from __future__ import annotations

import argparse
import csv
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sens import load_state  # noqa: E402

BUCKETS = ((0.0, 0.15), (0.15, 0.30), (0.30, 0.50),
           (0.50, 0.70), (0.70, 0.85), (0.85, 1.01))
FEE_K = 0.07              # сбор берущего: 0.07 * p * (1-p)
OUR_SLIP_CAP = 0.04       # выше аск+4¢ мы не платим никогда (limit_extra_max)


def fee(p: float) -> float:
    return FEE_K * p * (1.0 - p)


def per_share(t: dict) -> float:
    """Прибыль с одного билета: выиграл -> 1-цена, проиграл -> -цена, минус сбор."""
    p = t["vwap"]
    return (1.0 - p if t["win"] else -p) - fee(p)


def stats(trades: list[dict]) -> dict:
    n = len(trades)
    if not n:
        return {"n": 0}
    xs = [per_share(t) for t in trades]
    m = sum(xs) / n
    sd = math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1)) if n > 1 else 0.0
    se = sd / math.sqrt(n) if n else 0.0
    return {"n": n, "wins": sum(1 for t in trades if t["win"]),
            "price": sum(t["vwap"] for t in trades) / n,
            "slack": m, "se": se,
            "lo": m - 1.96 * se, "money": sum(x * t["shares"] for x, t in zip(xs, trades))}


def fills(path: Path) -> dict:
    """Доля заявок, доживших до нашей задержки, по ценовым диапазонам."""
    p = path / "survive.csv"
    if not p.exists():
        return {}
    out: dict[tuple, list[int]] = {}
    with p.open() as fh:
        for r in csv.DictReader(fh):
            try:
                a = float(r["ask0"])
            except (TypeError, ValueError, KeyError):
                continue
            key = next((b for b in BUCKETS if b[0] <= a < b[1]), None)
            if key is None:
                continue
            # 200 мс — ближайший замеренный горизонт к нашим настоящим 241 мс;
            # okt_ учитывает, что мы согласны переплатить.
            v = r.get("okt_200") or r.get("ok_200")
            if v in (None, ""):
                continue
            c = out.setdefault(key, [0, 0])
            c[0] += 1
            c[1] += int(float(v) > 0)
    return out


def report(d: Path, variant: str, fillpath: Path | None) -> str:
    hist = load_state(d)
    tr = hist.get(variant)
    if not tr:
        have = ", ".join(sorted(hist)) or "ничего"
        return f"Нет сделок варианта {variant} в {d}. Есть: {have}"
    fl = fills(fillpath) if fillpath else {}
    L = [f"Запас прочности по цене билета — {variant}, сделок {len(tr)}", ""]
    L.append("  цена билета  сделок  попаданий   средняя   запас, ¢   наверняка   "
             "после переплаты   исполнение")
    L.append("  " + "-" * 100)
    rows = []
    for b in BUCKETS:
        part = [t for t in tr if b[0] <= t["vwap"] < b[1]]
        s = stats(part)
        if not s["n"]:
            continue
        f = fl.get(b)
        fs = f"{100 * f[1] / f[0]:.0f}% ({f[0]})" if f and f[0] else "нет данных"
        net = s["lo"] - OUR_SLIP_CAP
        L.append(f"  {b[0]:.2f}-{b[1]:.2f}    {s['n']:5d}   {s['wins']:4d}"
                 f" ({100 * s['wins'] / s['n']:3.0f}%)   {s['price']:.2f}"
                 f"   {100 * s['slack']:+7.1f}   {100 * s['lo']:+7.1f}"
                 f"        {100 * net:+7.1f}       {fs}")
        rows.append((b, s))
    L += ["", "  запас — на сколько центов можем переплачивать за билет и остаться в нуле.",
          "  наверняка — тот же запас по нижней границе (95%): меньше этого не будет.",
          f"  после переплаты — что останется, если платим наши "
          f"{100 * OUR_SLIP_CAP:.0f}¢ сверх аска.", ""]

    good = [(b, s) for b, s in rows if s["lo"] - OUR_SLIP_CAP > 0]
    thin = [(b, s) for b, s in rows if 0 < s["lo"] <= OUR_SLIP_CAP]
    bad = [(b, s) for b, s in rows if s["lo"] <= 0]
    for b, s in bad:
        L.append(f"  ПУСТО {b[0]:.2f}-{b[1]:.2f}: в среднем {100 * s['slack']:+.1f}¢, "
                 f"но наверняка только {100 * s['lo']:+.1f}¢ — перевес не доказан, "
                 f"торговать нельзя ({s['n']} сделок).")
    for b, s in thin:
        can = s["lo"] / 2
        L.append(f"  ТОНКО {b[0]:.2f}-{b[1]:.2f}: наверняка {100 * s['lo']:.1f}¢, "
                 f"а переплачиваем до {100 * OUR_SLIP_CAP:.0f}¢ — свой же запас и съедаем. "
                 f"Можно позволить не больше {100 * can:.0f}¢ "
                 f"(limit_extra_max {can:.2f}), либо набрать сделок.")
    for b, s in good:
        L.append(f"  ЕСТЬ  {b[0]:.2f}-{b[1]:.2f}: наверняка {100 * s['lo']:.1f}¢ на билет, "
                 f"после переплаты остаётся {100 * (s['lo'] - OUR_SLIP_CAP):.1f}¢ "
                 f"({s['n']} сделок).")
    if not good and not thin:
        L += ["", "  ИТОГ: ни в одном диапазоне перевес не доказан. "
                  "Живыми деньгами торговать нечем."]
    elif not good:
        best = max(thin, key=lambda x: x[1]["lo"])
        L += ["", f"  ИТОГ: перевес есть только тонкий. Лучший — "
                  f"{best[0][0]:.2f}-{best[0][1]:.2f} ({100 * best[1]['lo']:.1f}¢ наверняка). "
                  f"Торговать можно, но переплату надо срезать до "
                  f"{100 * best[1]['lo'] / 2:.0f}¢ и копить сделки."]
    else:
        lo, hi = good[0][0][0], good[-1][0][1]
        L += ["", f"  ИТОГ: торговать {lo:.2f}-{hi:.2f}, остальное закрыть "
                  f"порогами min_price/max_price."]
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description="Запас прочности по цене билета")
    ap.add_argument("dir", type=Path)
    ap.add_argument("--variant", default="lag3-1s-t60")
    ap.add_argument("--fill", type=Path, default=Path("data_fill"))
    a = ap.parse_args()
    print(report(a.dir, a.variant, a.fill if a.fill.exists() else None))


if __name__ == "__main__":
    main()
