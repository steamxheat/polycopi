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
import statistics
from collections import defaultdict

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


class Sampler:
    """Раз в секунду снимает последнюю цену каждого источника -> посекундные ряды."""

    def __init__(self, prices, keep_s: int = 1800):
        self.prices = prices
        self.series: dict[tuple, dict[int, float]] = defaultdict(dict)   # (sym, src) -> {сек: цена}
        self.logs: dict[tuple, dict[int, float]] = defaultdict(dict)     # то же в логарифмах
        self.keep_s = keep_s
        self._sig_cache: dict = {}
        self._avg_cache: dict = {}
        self._cache_sec = 0

    def tick(self, now: float, fresh_s: float = 2.5):
        sec = int(now)
        for (sym, src), h in self.prices.hist.items():
            if h and now - h[-1][0] <= fresh_s:
                d = self.series[(sym, src)]
                d[sec] = h[-1][1]
                self.logs[(sym, src)][sec] = math.log(h[-1][1])
                if len(d) > self.keep_s + 60:
                    for dd in (d, self.logs[(sym, src)]):
                        for k in [k for k in dd if k < sec - self.keep_s]:
                            del dd[k]

    def value_at(self, sym: str, src: str, sec: int, tol: int = 2):
        d = self.series.get((sym, src), {})
        for k in range(tol + 1):
            if sec - k in d:
                return d[sec - k]
        return None

    def sources(self, sym: str):
        return [src for (s, src) in self.series if s == sym]

    def funcs(self, sym: str, src: str, now: float):
        """at/avg по лог-ценам источника; средние кэшируются в пределах секунды."""
        sec = int(now)
        if sec != self._cache_sec:
            self._cache_sec, self._avg_cache = sec, {}
        at, avg0 = series_funcs(self.logs.get((sym, src), {}))
        cache = self._avg_cache

        def avg(a, b):
            k = (sym, src, int(a), int(b))
            if k not in cache:
                cache[k] = avg0(a, b)
            return cache[k]
        return at, avg

    def sigma(self, sym: str, now: float, window: int) -> float | None:
        k = (sym, int(now), window)
        if k not in self._sig_cache:
            if len(self._sig_cache) > 50:
                self._sig_cache.clear()
            self._sig_cache[k] = self._sigma(sym, now, window)
        return self._sig_cache[k]

    def _sigma(self, sym: str, now: float, window: int) -> float | None:
        vals = []
        sec = int(now)
        for src in self.sources(sym):
            d = self.series[(sym, src)]
            xs = [(k, d[k]) for k in range(sec - window, sec + 1) if k in d]
            if len(xs) < window * 0.3:
                continue
            rets = []
            for (k1, a), (k2, b) in zip(xs, xs[1:]):
                if a > 0 and b > 0 and k2 > k1:
                    rets.append(math.log(b / a) / math.sqrt(k2 - k1))
            if len(rets) > 30:
                vals.append(statistics.pstdev(rets))
        return statistics.median(vals) if vals else None


def market_prob(sampler, prices, sym, start, end, s0: dict, now, rule="t60",
                sigma_window=900, sigma_floor=2e-5, fresh_s=3.0):
    """P(Up) по правилу: медиана по источникам, у которых есть цена на старте окна.
    prices — PriceFeed (нужен .hist), s0 — {источник: цена на старте}."""
    sig = sampler.sigma(sym, now, sigma_window)
    if sig is None:
        return None
    sig = max(sig, sigma_floor)
    ps = []
    for src, p0 in s0.items():
        h = prices.hist.get((sym, src))
        if not (h and now - h[-1][0] <= fresh_s and p0 > 0):
            continue
        at0, avg = sampler.funcs(sym, src, now)

        def at(t, at0=at0, p0=p0):
            return math.log(p0) if int(t) == int(start) else at0(t)
        p = prob_up(rule, at, avg, start, end, now, math.log(h[-1][1]), sig)
        if p is not None:
            ps.append(p)
    return statistics.median(ps) if ps else None
