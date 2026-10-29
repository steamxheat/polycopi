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
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sens import load_state  # noqa: E402

BUCKETS = ((0.0, 0.15), (0.15, 0.30), (0.30, 0.50),
           (0.50, 0.70), (0.70, 0.85), (0.85, 1.01))
FEE_K = 0.07              # сбор берущего: 0.07 * p * (1-p)
def _our_cap() -> float:
    """Нашу переплату берём из настроек бота, а не держим копию здесь.

    В отчёте стояло 4¢, когда у бота уже был 1¢, и один проект выдавал
    разрешающий и запрещающий вывод одновременно.
    """
    try:
        import liveprobe                                 # noqa: PLC0415
        return float(liveprobe.DEFAULTS["limit_extra_max"])
    except Exception:                                    # noqa: BLE001
        return 0.01


OUR_SLIP_CAP = _our_cap()


def fee(p: float) -> float:
    return FEE_K * p * (1.0 - p)


def clusters(trades: list[dict]) -> dict[tuple, list[dict]]:
    """Группы покупок с общим исходом: (рынок, сторона)."""
    g: dict[tuple, list[dict]] = defaultdict(list)
    for t in trades:
        g[(t.get("what") or (t.get("t"),))].append(t)
    return g


def per_share(t: dict) -> float:
    """Прибыль с одного билета: выиграл -> 1-цена, проиграл -> -цена, минус сбор."""
    p = t["vwap"]
    return (1.0 - p if t["win"] else -p) - fee(p)


def z_for(k: int) -> float:
    """Запас по нижней границе при выборе лучшего из k диапазонов.

    Смотреть шесть диапазонов и брать лучший — это шесть попыток, а не одна.
    Ровно так нас обманул дешёвый диапазон. Делим риск ошибки на k
    (поправка Бонферрони): 1 диапазон -> 1.96 сигмы, 6 -> около 2.64.
    """
    k = max(int(k), 1)
    if k == 1:
        return 1.96
    try:
        from statistics import NormalDist             # noqa: PLC0415
        return NormalDist().inv_cdf(1 - 0.05 / (2 * k))
    except Exception:                                 # noqa: BLE001
        return 1.96 + 0.52 * math.log(k)


def stats(trades: list[dict], clustered: bool = True) -> dict:
    """Прибыль на билет и её погрешность.

    Среднее считается ПО БИЛЕТАМ: это реальный результат стратегии, и
    пересчитывать его с равным весом исходов нельзя — так меняется сам
    измеряемый показатель, а не его точность (моя прежняя ошибка: средние
    8.0¢ вместо настоящих 12.0¢).

    А вот ПОГРЕШНОСТЬ обязана считаться по исходам: пять покупок одного окна
    делят один исход и независимыми наблюдениями не являются. Это кластерная
    оценка — среднее остаётся прежним, растёт только разброс.
    """
    n = len(trades)
    if not n:
        return {"n": 0}
    w_all = sum(t["shares"] for t in trades) or 1.0
    m = sum(per_share(t) * t["shares"] for t in trades) / w_all      # на билет

    if clustered:
        gs = clusters(trades)
        k = len(gs)
        # остаток по каждому исходу: сколько он дал сверх среднего
        res = [sum((per_share(t) - m) * t["shares"] for t in xs) for xs in gs.values()]
        if k < 2:
            # Один исход — разброс оценить НЕЧЕМ. Любая цифра здесь была бы
            # выдумкой, поэтому объявляем полную неопределённость.
            se = abs(m) + 1.0
        else:
            var = sum(r * r for r in res) / (w_all * w_all)
            var *= k / (k - 1)                  # поправка на малое число исходов
            se = math.sqrt(max(var, 0.0))
    else:
        k = n
        xs = [per_share(t) for t in trades]
        sd = math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1)) if n > 1 else 0.0
        se = sd / math.sqrt(n)

    return {"n": n, "k": k, "wins": sum(1 for t in trades if t["win"]),
            "price": sum(t["vwap"] * t["shares"] for t in trades) / w_all,
            "slack": m, "se": se, "lo": m - 1.96 * se,
            "money": sum(per_share(t) * t["shares"] for t in trades)}


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


def forecast(s: dict, z: float, target: int) -> float:
    """Какой станет нижняя граница при target сделках, если попадание
    удержится. Неопределённость падает как корень из числа сделок —
    значит граница растёт сама, без улучшения стратегии.
    """
    base = s.get("k") or s["n"]          # считаем в исходах, не в покупках
    if base < 2 or target <= base:
        return s["lo"]
    return s["slack"] - z * s["se"] * math.sqrt(base / target)


def pace(trades: list[dict]) -> float:
    """Сделок в час: считаем по рабочему диапазону, а не по всем сделкам.
    Ошибка на этом месте дала оценку «к утру» вместо «через сутки»."""
    ts = sorted(t["ts"] for t in trades if t.get("ts") is not None)
    if len(ts) < 2:
        return 0.0
    hours = (ts[-1] - ts[0]) / 3600
    return len(ts) / hours if hours > 0.5 else 0.0


def since_ts(text: str) -> float:
    """«2026-10-29T15:30» (UTC) -> секунды. Пустая строка -> 0."""
    if not text:
        return 0.0
    from datetime import datetime, timezone                # noqa: PLC0415
    t = text.strip().replace(" ", "T")
    for f in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(t, f).replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            continue
    raise SystemExit(f"Не понял время «{text}». Нужно 2026-10-29T15:30 (UTC).")


def report(d: Path, variant: str, fillpath: Path | None, raw: bool = False,
           since: str = "") -> str:
    hist = load_state(d)
    tr = hist.get(variant)
    if not tr:
        have = ", ".join(sorted(hist)) or "ничего"
        return f"Нет сделок варианта {variant} в {d}. Есть: {have}"
    cut = since_ts(since)
    if cut:
        tr = [t for t in tr if (t.get("ts") or 0) >= cut]
        if not tr:
            return f"После {since} у {variant} рассчитанных сделок нет."
    fl = fills(fillpath) if fillpath else {}
    raw_n = len(tr)
    nb = sum(1 for b in BUCKETS if any(b[0] <= t["vwap"] < b[1] for t in tr))
    L = [f"Запас прочности по цене билета — {variant}, покупок {raw_n}"
         + (" (зависимость НЕ учтена: --raw)" if raw else
            "; погрешность считается по исходам")
         + (f"; только после {since} UTC" if since else ""),
         f"Диапазонов с данными {nb}, поэтому нижняя граница берётся с запасом "
         f"{z_for(nb):.2f} сигмы (выбираем лучший из {nb}, а не один заранее названный).",
         ""]
    L.append("  цена билета  сделок  попаданий   средняя   запас, ¢   наверняка   "
             "после переплаты    деньги   исполнение")
    L.append("  " + "-" * 112)
    rows = []
    seen = [b for b in BUCKETS if any(b[0] <= t["vwap"] < b[1] for t in tr)]
    z = z_for(len(seen))
    for b in BUCKETS:
        # Отбираем ПОКУПКИ по их цене, и лишь затем группируем по исходам:
        # иначе исход, купленный по 0.60 и 0.80, выпадал из обоих диапазонов.
        part = [t for t in tr if b[0] <= t["vwap"] < b[1]]
        s = stats(part, clustered=not raw)
        if not s["n"]:
            continue
        s["lo"] = s["slack"] - z * s["se"]           # с поправкой на выбор лучшего
        f = fl.get(b)
        fs = f"{100 * f[1] / f[0]:.0f}% ({f[0]})" if f and f[0] else "нет данных"
        net = s["lo"] - OUR_SLIP_CAP
        L.append(f"  {b[0]:.2f}-{b[1]:.2f}    {s['n']:5d}   {s['wins']:4d}"
                 f" ({100 * s['wins'] / s['n']:3.0f}%)   {s['price']:.2f}"
                 f"   {100 * s['slack']:+7.1f}   {100 * s['lo']:+7.1f}"
                 f"        {100 * net:+7.1f}   {s['money']:+8.2f}$   {fs}")
        if not raw and s.get("k") and s["k"] < s["n"]:
            L.append(f"               ^ {s['n']} покупок на {s['k']} исходах")
        rows.append((b, s))
    tot = sum(s["money"] for _, s in rows)
    L += ["", f"  деньги — сколько эта строка принесла в симуляции. Всего {tot:+.2f}$.",
          "  запас — на сколько центов можем переплачивать за билет и остаться в нуле.",
          f"  наверняка — нижняя граница с поправкой на выбор лучшего из "
          f"{nb}. Это нижняя ОЦЕНКА (не гарантия): она говорит, что данные "
          f"не противоречат такому запасу, а не что меньше не будет.",
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
        L += ["", f"  ИТОГ: перевес тонкий. Лучший — "
                  f"{best[0][0]:.2f}-{best[0][1]:.2f} ({100 * best[1]['lo']:.1f}¢ "
                  f"по нижней оценке). Торговля НЕ разрешена: нужно либо срезать "
                  f"переплату до {100 * best[1]['lo'] / 2:.0f}¢, либо копить наблюдения."]
    else:
        lo, hi = good[0][0][0], good[-1][0][1]
        L += ["", f"  ИТОГ: перевес установлен в {lo:.2f}-{hi:.2f}. Это НЕ разрешение "
                  f"торговать: решение принимается по условиям в docs/HYPOTHESES.md."]
    # Прогноз по лучшему диапазону: когда ждать ответа и каким он будет
    if rows:
        b, best = max(rows, key=lambda x: x[1]["lo"])
        part = [t for t in tr if b[0] <= t["vwap"] < b[1]]
        ph = pace(part)
        L += ["", f"  Прогноз по {b[0]:.2f}-{b[1]:.2f}, если попадание удержится "
                  f"на {100 * best['wins'] / best['n']:.1f}%:"]
        for tgt in (180, 250, 400):
            if tgt <= best["n"]:
                continue
            f = forecast(best, z, tgt)
            when = f"{(tgt - best['n']) / ph:.0f} ч" if ph else "?"
            mark = "хватит" if f - OUR_SLIP_CAP > 0 else "мало"
            L.append(f"    при {tgt:4d} сделках -> наверняка {100 * f:+.1f}¢ "
                     f"({mark}), ждать ~{when}")
        if ph:
            L.append(f"    темп {ph:.1f} сделок в час в этом диапазоне")
    if tot:
        pr = sum(s["money"] for b, s in rows if s["lo"] > 0)
        L.append(f"  Из {tot:+.2f}$ на диапазоны с доказанным перевесом приходится "
                 f"{pr:+.2f}$ ({100 * pr / tot:.0f}%), остальное — недоказанное.")
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description="Запас прочности по цене билета")
    ap.add_argument("dir", type=Path)
    ap.add_argument("--variant", default="lag3-1s-t60")
    ap.add_argument("--fill", type=Path, default=Path("data_fill"))
    ap.add_argument("--raw", action="store_true",
                    help="считать по покупкам, не по исходам (завышает точность)")
    ap.add_argument("--since", default="",
                    help="только сделки позже этого времени UTC (2026-10-29T15:30)")
    a = ap.parse_args()
    print(report(a.dir, a.variant, a.fill if a.fill.exists() else None, a.raw, a.since))


if __name__ == "__main__":
    main()
