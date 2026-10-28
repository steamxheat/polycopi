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
import json
import os
import math
import statistics
from pathlib import Path

FEE_COEF = 0.07


def fee(p: float, shares: float) -> float:
    return FEE_COEF * p * (1 - p) * shares


def load_state(d: Path):
    """Полная история сделок из state.json бота (включая те, что были до смены
    формата trades.csv — тот файл бот отложил в trades_old_*.csv)."""
    sp = d / "state.json"
    if not sp.exists():
        return {}
    st = json.loads(sp.read_text())
    out: dict[str, list[dict]] = {}
    for name, v in st.get("variants", {}).items():
        for t in v.get("trades", []):
            if t.get("pnl") is None:
                continue                       # окно ещё не рассчитано
            try:
                out.setdefault(name, []).append(
                    {"v": name, "shares": float(t["shares"]), "vwap": float(t["vwap"]),
                     "win": bool(t.get("won")), "edge": float(t.get("edge_fill") or 0.0),
                     "t": f"{float(t['t']):.3f}", "what": (t.get("slug", ""), t.get("side", ""))})
            except (TypeError, ValueError, KeyError):
                continue
    return out


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
                     "t": r.get("time_utc") or "",
                     "what": (r["slug"], r["side"])}
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


def eff_k(trades: dict[str, list[dict]]) -> float:
    """Сколько НЕЗАВИСИМЫХ проверок мы на самом деле сделали.

    Варианты торгуют во многом одни и те же сделки, поэтому 16 вариантов — это не
    16 независимых экспериментов. Оцениваем по пересечению сделок: если все варианты
    торгуют одно и то же, независимая проверка одна.
    """
    vs = list(trades.values())
    if len(vs) < 2:
        return float(len(vs) or 1)
    # одна и та же возможность = тот же рынок и та же сторона. Метка времени не
    # годится: бот берёт её заново для каждого варианта, она отличается на микросекунды.
    keys = [{t.get("what") or (t["t"],) for t in ts} for ts in vs]
    sims = []
    for i in range(len(keys)):
        for j in range(i + 1, len(keys)):
            u = len(keys[i] | keys[j])
            sims.append(len(keys[i] & keys[j]) / u if u else 0.0)
    overlap = statistics.mean(sims) if sims else 0.0
    return max(1.0, len(vs) * (1 - overlap))


def z_threshold(k: float) -> float:
    """Порог z с поправкой на перебор k НЕЗАВИСИМЫХ проверок.
    При одной проверке — обычные 2.0; дальше поправка Бонферрони, но мягкая,
    потому что k уже уменьшено на пересечение сделок между вариантами."""
    return 2.0 + 0.55 * math.log(max(1.0, k))


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


def report(d: Path, min_n: int, source: str = "auto") -> str:
    trades, src = {}, ""
    if source in ("auto", "state"):
        trades, src = load_state(d), "state.json (полная история)"
    if not trades and source in ("auto", "csv"):
        trades, src = load(d), "trades.csv (только после последней смены формата)"
    if not trades:
        return f"Нет рассчитанных сделок в {d}"
    big = {v: ts for v, ts in trades.items() if len(ts) >= min_n}
    k = eff_k(trades)
    thr = z_threshold(k)
    span = hours(d)
    slips = (0.0, 0.01, 0.02)
    drops = (0.0, 0.25, 0.50)
    L = [f"=== Чувствительность и значимость: {d} ===",
         f"Источник: {src}",
         f"Вариантов всего {len(trades)}, из них с ≥{min_n} сделок — {len(big)}."
         + (f" Бот работает {span:.1f} ч." if span else ""),
         "",
         "ЧАСТЬ 1. Это вообще не случайность?",
         "  z — во сколько раз средний результат больше своей погрешности.",
         f"  Вариантов {len(trades)}, но они торгуют во многом одни и те же сделки,",
         f"  поэтому независимых проверок всего ~{k:.1f} -> порог z ≥ {thr:.1f}",
         "  (при одной проверке хватило бы 2.0; поправка нужна из-за перебора).",
         "  «нужно сделок» — сколько их нужно для порога, если эффект того же размера.",
         "  «топ-3» — какую долю плюса дают три самых крупных выигрыша.",
         "  «половины» — результат первой и второй половины сделок по времени.",
         "",
         f"{'вариант':<20} {'сделок':>6} {'PnL $':>8} {'z':>6} {'топ-3':>7} "
         f"{'1-я пол.':>10} {'2-я пол.':>10} {'нужно':>7} {'ещё':>8}  вывод"]
    proven = []
    for v in sorted(big):
        ts = big[v]
        xs = each_pnl(ts)
        z = zscore(xs)
        h1, h2, n1, n2 = halves(ts)
        ok = z >= thr and h1 > 0 and h2 > 0
        if ok:
            proven.append(v)
        need = len(ts) * (thr / z) ** 2 if z > 0 else 0
        nd = f"{need:,.0f}".replace(",", " ") if 0 < need < 1e5 else ("-" if z <= 0 else "много")
        eta = (f"{(need - len(ts)) / (len(ts) / span):.0f} ч"
               if span and z > 0 and 0 < need < 1e5 and need > len(ts) else "-")
        verdict = ("доказано" if ok else
                   "похоже на правду, не доказано" if z >= 2.0 and h1 > 0 and h2 > 0 else
                   "шум, не доказано")
        L.append(f"{v:<20} {len(ts):>6} {sum(xs):>+8.1f} {z:>6.1f} {top_share(xs):>6.0f}% "
                 f"{h1:>+10.1f} {h2:>+10.1f} {nd:>7} {eta:>8}  {verdict}")
    pos = sum(1 for ts in big.values() if sum(each_pnl(ts)) > 0)
    L += ["", f"Прошли проверку на значимость: {len(proven)} из {len(big)}.",
          f"В плюсе: {pos} из {len(big)}. Если бы это была чистая случайность, "
          f"ожидалось бы около половины.", ""]
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


def hours(d: Path) -> float:
    try:
        st = json.loads((d / "state.json").read_text())
        return max((st["saved"] - st["started"]) / 3600, 0.0)
    except (OSError, ValueError, KeyError):
        return 0.0


def main():
    ap = argparse.ArgumentParser(description="Чувствительность результатов тейкера к цене и конкуренции")
    ap.add_argument("data", nargs="?", default="data_fv_lag")
    ap.add_argument("--min-n", type=int, default=30)
    ap.add_argument("--source", choices=("auto", "state", "csv"), default="auto",
                    help="state.json — вся история; csv — только текущий trades.csv")
    a = ap.parse_args()
    try:
        print(report(Path(a.data), a.min_n, a.source))
    except BrokenPipeError:            # вывод обрезали через | head — это не ошибка
        os._exit(0)


if __name__ == "__main__":
    main()
