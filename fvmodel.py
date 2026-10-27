"""Правила расчёта крипто-окон Up/Down и вероятность исхода для каждого правила.

Polymarket пишет: «Up, если TWAP Chainlink (поток ETH/USD TWAP-60s) за отрезок ≥ цены
в начале отрезка». Это можно понять по-разному, поэтому считаем три версии и
проверяем на фактах, какая совпадает с реальными итогами:

  spot — цена в конце окна против цены на старте (наша первая модель);
  t60  — средняя за последние 60 с окна против средней за 60 с перед стартом;
  wt   — средняя за всё окно против цены на старте.

Все цены — в логарифмах. at(t) -> лог-цена в секунду t (или None),
avg(a, b) -> средняя лог-цена за секунды [a, b) (или None).
Будущее — броуновское движение с σ за секунду; средняя БД за отрезок длины T
имеет дисперсию σ²T/3.
"""
from __future__ import annotations

import math

RULES = ("spot", "t60", "wt")
TW = 60


def phi(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def final_diff(rule, at, avg, start, end):
    """Итог окна по правилу (лог-разница «итог − база»), когда все цены известны."""
    if rule == "spot":
        a, b = at(end), at(start)
    elif rule == "t60":
        a, b = avg(end - TW, end), avg(start - TW, start)
    elif rule == "wt":
        a, b = avg(start, end), at(start)
    else:
        raise ValueError(rule)
    if a is None or b is None:
        return None
    return a - b


def prob_up(rule, at, avg, start, end, now, x_now, sigma):
    """P(Up) по правилу в момент now при текущей лог-цене x_now."""
    tau = end - now
    if rule == "spot":
        ref, mean = at(start), x_now
        sd = sigma * math.sqrt(max(tau, 0))
    elif rule == "t60":
        ref = avg(start - TW, start)
        if tau >= TW:
            mean = x_now
            sd = sigma * math.sqrt(tau - TW + TW / 3)
        else:
            done = TW - tau                       # секунд последней минуты уже прошло
            a = avg(end - TW, int(now)) if done >= 1 else x_now
            if a is None:
                return None
            mean = (done * a + tau * x_now) / TW
            sd = (tau / TW) * sigma * math.sqrt(max(tau, 0) / 3)
    elif rule == "wt":
        ref = at(start)
        dur = end - start
        el = now - start
        a = avg(start, int(now)) if el >= 1 else x_now
        if a is None:
            return None
        mean = (el * a + tau * x_now) / dur
        sd = (tau / dur) * sigma * math.sqrt(max(tau, 0) / 3)
    else:
        raise ValueError(rule)
    if ref is None:
        return None
    if sd <= 0:
        return 1.0 if mean >= ref else 0.0
    return phi((mean - ref) / sd)


def series_funcs(d: dict, tol: int = 2, min_cover: float = 0.6):
    """at/avg поверх словаря {секунда: лог-цена}."""
    def at(t):
        t = int(t)
        for k in range(tol + 1):
            if t - k in d:
                return d[t - k]
        return None

    def avg(a, b):
        a, b = int(a), int(b)
        if b <= a:
            return None
        xs = [d[k] for k in range(a, b) if k in d]
        if len(xs) < max(1, (b - a) * min_cover):
            return None
        return sum(xs) / len(xs)
    return at, avg
