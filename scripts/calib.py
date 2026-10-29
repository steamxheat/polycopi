#!/usr/bin/env python3
"""Врёт ли модель. Калибровка по корзинам, без денег.

Главный вопрос стратегии: когда модель говорит «вероятность 70%», выигрывает
ли это в 70% случаев. Среднее по всем сделкам на этот вопрос НЕ отвечает:
занижение в одной корзине гасит завышение в другой.

Две оговорки, из-за которых прежние ответы были неверны:

1. Повторные покупки одного исхода — это ОДНО наблюдение. 29.10 бот купил
   ETH Up пять раз в одном окне; считать это пятью проверками модели нельзя.
   По умолчанию объединяем в кластеры (рынок, сторона).
2. Доля выигрышей в корзине из 20 наблюдений гуляет на десятки процентов.
   Поэтому к каждой корзине даётся интервал (Уилсона), и вывод делается
   только там, где обещание модели вне интервала.

  python3 scripts/calib.py data_fv_lag [--variant lag3-1s-t60]
                           [--min-price 0.50] [--max-price 0.70] [--raw]
"""
from __future__ import annotations

import argparse
import math
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

BUCKETS = ((0.0, 0.5), (0.5, 0.6), (0.6, 0.7), (0.7, 0.8), (0.8, 0.9), (0.9, 1.01))
Z = 1.96
MIN_N = 30                # меньше этого сводный вердикт не выносим


def wilson(wins: int, n: int, z: float = Z) -> tuple[float, float]:
    """Интервал для доли. На малых выборках обычная формула врёт."""
    if n == 0:
        return (0.0, 1.0)
    p = wins / n
    d = 1 + z * z / n
    c = p + z * z / (2 * n)
    r = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return max(0.0, (c - r) / d), min(1.0, (c + r) / d)


def load(d: Path, variant: str) -> list[dict]:
    """Сделки варианта с вероятностью модели и исходом."""
    import json                                            # noqa: PLC0415

    sp = d / "state.json"
    if not sp.exists():
        raise SystemExit(f"Нет {sp}")
    st = json.loads(sp.read_text())
    v = st.get("variants", {}).get(variant)
    if v is None:
        raise SystemExit(f"Нет варианта {variant}. Есть: {', '.join(st.get('variants', {}))}")
    out = []
    for t in v.get("trades", []):
        if t.get("pnl") is None or t.get("p") is None:
            continue
        try:
            out.append({"p": float(t["p"]), "won": bool(t.get("won")),
                        "vwap": float(t["vwap"]), "shares": float(t["shares"]),
                        "slug": str(t.get("slug", "")), "side": str(t.get("side", "")),
                        "tok": str(t.get("tok", ""))})
        except (TypeError, ValueError):
            continue
    return out


def cluster(trades: list[dict]) -> list[dict]:
    """Повторные покупки одного исхода -> одно наблюдение.

    Исход у них общий, поэтому как проверки модели они не независимы.
    Вероятность модели берём средневзвешенную по размеру покупки.
    """
    g: dict[tuple, list[dict]] = defaultdict(list)
    for t in trades:
        g[(t["slug"] or t["tok"], t["side"])].append(t)
    out = []
    for xs in g.values():
        w = sum(x["shares"] for x in xs) or float(len(xs))
        out.append({"p": sum(x["p"] * x["shares"] for x in xs) / w,
                    "won": xs[0]["won"], "n_buys": len(xs),
                    "vwap": sum(x["vwap"] * x["shares"] for x in xs) / w})
    return out


def report(d: Path, variant: str, lo_p: float, hi_p: float, raw: bool) -> str:
    tr = load(d, variant)
    if not tr:
        return f"У {variant} нет рассчитанных сделок с вероятностью модели."
    in_range = [t for t in tr if lo_p <= t["vwap"] < hi_p]
    obs = in_range if raw else cluster(in_range)
    allobs = tr if raw else cluster(tr)

    L = [f"Калибровка модели — {variant}",
         f"Сделок всего {len(tr)}, в цене {lo_p:.2f}-{hi_p:.2f}: {len(in_range)}."]
    if not raw:
        L.append(f"Повторные покупки одного исхода объединены: наблюдений "
                 f"{len(obs)} (из {len(in_range)} покупок), по всем ценам {len(allobs)}.")
    else:
        L.append("Режим --raw: каждая покупка считается отдельно (зависимость НЕ учтена).")
    L += ["", "  модель обещала   наблюдений   выиграло   интервал 95%      вердикт",
          "  " + "-" * 74]

    g = defaultdict(list)
    for t in obs:
        b = next((x for x in BUCKETS if x[0] <= t["p"] < x[1]), None)
        if b:
            g[b].append(t)

    verdicts = []
    for b in BUCKETS:
        xs = g.get(b)
        if not xs:
            continue
        n = len(xs)
        w = sum(1 for x in xs if x["won"])
        lo, hi = wilson(w, n)
        mid = (b[0] + b[1]) / 2
        if hi < b[0]:
            v, tag = "ЗАВЫШАЕТ", -1
        elif lo > b[1]:
            v, tag = "ЗАНИЖАЕТ", 1
        else:
            v, tag = "не отличить", 0
        if n < MIN_N:
            tag = 0                                  # по корзине тоже не судим
        verdicts.append((b, n, w, tag))
        L.append(f"  {b[0]:.2f}-{b[1]:.2f}        {n:9d}   {w:4d} ({100 * w / n:3.0f}%)"
                 f"   {100 * lo:3.0f}-{100 * hi:3.0f}%      {v}"
                 + ("" if tag or n >= 30 else f"  (мало: нужно ~{30 - n} ещё)"))

    # сводно: средний прогноз против факта, с интервалом
    n = len(obs)
    if n:
        pred = sum(t["p"] for t in obs) / n
        w = sum(1 for t in obs if t["won"])
        lo, hi = wilson(w, n)
        L += ["", f"  В целом по {lo_p:.2f}-{hi_p:.2f}: модель обещала {100 * pred:.1f}%, "
                  f"выиграло {100 * w / n:.1f}% ({w} из {n}), интервал "
                  f"{100 * lo:.1f}-{100 * hi:.1f}%."]
        if n < MIN_N:
            # На девяти наблюдениях интервал узким не бывает, и «вердикт» по нему —
            # ровно та ошибка, из-за которой этот скрипт и появился.
            L.append(f"  Наблюдений {n} — для вывода о смещении нужно хотя бы {MIN_N}. "
                     f"Данных мало для вывода.")
        elif pred < lo:
            L.append("  Модель ЗАНИЖАЕТ: факт выше обещанного, и обещание вне интервала.")
        elif pred > hi:
            L.append("  Модель ЗАВЫШАЕТ: факт ниже обещанного. Перевес, на который мы "
                     "соглашаемся переплачивать, частично выдуман.")
        else:
            L.append("  Смещения не видно: обещание модели внутри интервала. Это НЕ значит, "
                     "что калибровка хорошая — значит, данных мало для вывода.")

    bad = [v for v in verdicts if v[3] == -1]
    if bad:
        L += ["", "  Корзины с завышением — именно там переплата опасна:"]
        for b, n, w, _ in bad:
            L.append(f"    {b[0]:.2f}-{b[1]:.2f}: обещано ≥{100 * b[0]:.0f}%, "
                     f"вышло {100 * w / n:.0f}% на {n} наблюдениях.")
    L += ["", "  Как читать: вердикт ставится только когда обещание модели ВНЕ интервала.",
          "  «не отличить» — данных мало, а не «калибровка в порядке»."]
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description="Калибровка модели по корзинам")
    ap.add_argument("dir", type=Path)
    ap.add_argument("--variant", default="lag3-1s-t60")
    ap.add_argument("--min-price", type=float, default=0.0)
    ap.add_argument("--max-price", type=float, default=1.01)
    ap.add_argument("--raw", action="store_true",
                    help="не объединять повторные покупки одного исхода")
    a = ap.parse_args()
    print(report(a.dir, a.variant, a.min_price, a.max_price, a.raw))


if __name__ == "__main__":
    main()
